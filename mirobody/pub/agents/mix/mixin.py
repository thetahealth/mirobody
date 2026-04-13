"""
MixMixin - Two-phase model fusion capabilities.

Provides core streaming logic for:
- Phase 1: Data collection with tool calls (orchestrator model)
- Phase 2: Response generation with collected context (responder model)

This mixin can be combined with DeepAgent or other agent base classes.
"""

import json
import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any, AsyncGenerator
from zoneinfo import ZoneInfo

from jinja2 import Environment
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import BaseTool

from ....utils import get_req_ctx
from ..deep.utils import StreamConverter, TokenUsageCallback

logger = logging.getLogger(__name__)

# Phase 2 auto-retry on empty response (DashScope/Gemini intermittent empty output)
MAX_PHASE2_RETRIES = 3


class MixMixin:
    """
    Two-phase response generation mixin.

    Provides:
    - Phase 2 LLM initialization from @responder providers
    - Phase 2 prompt building
    - Phase 1 stream processing (collecting tool messages)
    - Phase 2 streaming output
    - Cost statistics merging

    Host class must provide:
    - agent_name: str
    - timezone: str
    - recursion_limit: int
    - user_info (optional, with user_name attribute)
    """

    # Attributes provided by host class
    agent_name: str
    timezone: str
    recursion_limit: int

    # Default tools to filter (intermediate process tools)
    # Supports both plain names and prefixed names (e.g., "user-search_health_indicators")
    DEFAULT_FILTER_TOOL_NAMES = {
        "search_health_indicators",
        "get_apple_watch_indicators",
        "ls", "grep", "glob",
        "fetch_remote_files",
        "write_todos",
        "generate_answer",
    }

    def _init_mix_mixin(
        self,
        responder_providers: dict[str, Any] = None,
        responder_configs: dict[str, dict] = None,
        prompt_templates: dict[str, str] = None,
        group_responder_map: dict[str, list[str]] = None,
    ):
        """
        Initialize MixMixin attributes. Call this in subclass __init__.

        Args:
            responder_providers: Dict of provider_name -> LLM client for @responder providers
            responder_configs: Dict of provider_name -> config dict (with 'response_with_tools' field)
            prompt_templates: Dict of prompt_key -> prompt_content (e.g., {"orchestrator": "...", "responder": "..."})
            group_responder_map: Dict of group_name -> list of responder provider names
        """
        self._responder_providers = responder_providers or {}
        self._responder_configs = responder_configs or {}
        self._prompt_templates = prompt_templates or {}
        self._group_responder_map = group_responder_map or {}
        self._phase2_llm_cache: dict[str, Any] = {}

        logger.info(f"MixMixin initialized with {len(self._responder_providers)} responder providers, {len(self._group_responder_map)} groups, {len(self._prompt_templates)} prompt templates")

    # === Phase 2 LLM Selection ===

    def _get_responder_for_mode(self, has_tools: bool, group: str | None = None) -> tuple[Any, str] | tuple[None, None]:
        """
        Get the responder LLM client based on whether Phase 1 had tool calls.

        Selection priority:
        1. Filter by group (if specified and group mapping exists)
        2. Exact match: response_with_tools == has_tools (True/False)
        3. Flexible responder: response_with_tools is None (used for both cases)
        4. Fallback: First available responder with warning

        Args:
            has_tools: True if Phase 1 made tool calls, False otherwise
            group: Provider group name (e.g., "sonnet&gemini") to filter responders

        Returns:
            Tuple of (llm_client, provider_name) or (None, None) if not found
        """
        # Filter responder candidates by group if specified
        if group and self._group_responder_map:
            allowed_names = set(self._group_responder_map.get(group, []))
            if allowed_names:
                candidates = {k: v for k, v in self._responder_providers.items() if k in allowed_names}
                logger.info(f"Filtered responders by group '{group}': {list(candidates.keys())}")
            else:
                candidates = self._responder_providers
                logger.warning(f"Group '{group}' not found in group_responder_map, using all responders")
        else:
            candidates = self._responder_providers

        flexible_responder = None
        flexible_name = None

        # First pass: look for exact match or flexible responder
        for provider_name, client in candidates.items():
            config = self._responder_configs.get(provider_name, {})
            response_with_tools = config.get("response_with_tools")

            # Exact match (response_with_tools == True/False)
            if response_with_tools == has_tools:
                logger.info(f"Selected responder '{provider_name}' (exact match: has_tools={has_tools}, group='{group}')")
                return client, provider_name

            # Flexible responder (response_with_tools is None)
            if response_with_tools is None and flexible_responder is None:
                flexible_responder = client
                flexible_name = provider_name

        # Use flexible responder if found
        if flexible_responder:
            logger.info(f"Selected flexible responder '{flexible_name}' (response_with_tools=None, group='{group}')")
            return flexible_responder, flexible_name

        # Fallback to first responder with warning
        if candidates:
            first_name = next(iter(candidates))
            logger.warning(f"No matching responder for has_tools={has_tools}, group='{group}', using fallback: {first_name}")
            return candidates[first_name], first_name

        return None, None

    # === Phase 1 Message Filtering ===

    def _filter_tool_messages(
        self,
        collected_messages: list[BaseMessage],
        filter_failed: bool = True,
        filter_tool_names: set[str] | None = None,
    ) -> list[BaseMessage]:
        """
        Filter tool call messages before passing to Phase 2.

        Removes:
        1. Failed tool calls (success: false in JSON response)
        2. Intermediate process tools (e.g., search_health_indicators)

        Args:
            collected_messages: Messages collected from Phase 1
            filter_failed: Whether to filter failed tool calls
            filter_tool_names: Tool names to filter out (default: DEFAULT_FILTER_TOOL_NAMES)

        Returns:
            Filtered message list
        """
        if not collected_messages:
            return []

        filter_tool_names = filter_tool_names or self.DEFAULT_FILTER_TOOL_NAMES

        # Step 1: Build tool_call_id to tool_name mapping from AIMessages
        tool_call_id_to_name: dict[str, str] = {}
        for msg in collected_messages:
            if isinstance(msg, AIMessage) and msg.tool_calls:
                for tc in msg.tool_calls:
                    tc_id = tc.get("id", "")
                    tc_name = tc.get("name", "")
                    if tc_id:
                        tool_call_id_to_name[tc_id] = tc_name

        # Step 2: Identify tool_call_ids to remove
        ids_to_remove: set[str] = set()

        # 2a: Remove by tool name (exact match or suffix match for prefixed names like "user-search_health_indicators")
        for tc_id, tc_name in tool_call_id_to_name.items():
            if tc_name in filter_tool_names:
                ids_to_remove.add(tc_id)
                continue
            # Support prefixed tool names: "user-search_health_indicators" matches "search_health_indicators"
            if "-" in tc_name:
                base_name = tc_name.split("-", 1)[1]
                if base_name in filter_tool_names:
                    ids_to_remove.add(tc_id)

        # 2b: Remove failed tool results
        for msg in collected_messages:
            if not isinstance(msg, ToolMessage):
                continue

            tool_call_id = msg.tool_call_id
            if not tool_call_id or tool_call_id in ids_to_remove:
                continue

            if filter_failed:
                content = msg.content if isinstance(msg.content, str) else str(msg.content)
                if self._is_failed_tool_response(content):
                    ids_to_remove.add(tool_call_id)
                    logger.debug(f"Filtering failed tool call: {tool_call_id_to_name.get(tool_call_id, '?')}")

        if not ids_to_remove:
            logger.debug("No tool calls filtered")
            return collected_messages

        removed_names = {tool_call_id_to_name.get(tid, "?") for tid in ids_to_remove}
        logger.info(f"_filter_tool_messages: removing {len(ids_to_remove)} tool_call_ids: {removed_names}")

        # Step 3: Rebuild message list
        filtered: list[BaseMessage] = []

        for msg in collected_messages:
            if isinstance(msg, AIMessage) and msg.tool_calls:
                # Filter out removed tool_calls
                remaining_calls = [
                    tc for tc in msg.tool_calls
                    if tc.get("id", "") not in ids_to_remove
                ]
                if remaining_calls:
                    # Create new AIMessage with filtered tool_calls
                    filtered.append(AIMessage(
                        content=msg.content,
                        tool_calls=remaining_calls,
                        additional_kwargs=msg.additional_kwargs,
                    ))
                # If no remaining calls, skip this AIMessage entirely

            elif isinstance(msg, ToolMessage):
                if msg.tool_call_id not in ids_to_remove:
                    filtered.append(msg)

            else:
                filtered.append(msg)

        logger.info(
            f"Filtered {len(ids_to_remove)} tool calls: "
            f"{len(collected_messages)} -> {len(filtered)} messages"
        )
        return filtered

    def _is_failed_tool_response(self, content: str) -> bool:
        """
        Check if tool response indicates failure or has no useful data.

        Filters:
        1. Explicit failure: success: false
        2. Empty data patterns (success: true but no actual data)

        Args:
            content: Tool response content (JSON string)

        Returns:
            True if the response should be filtered
        """
        if not content:
            return False

        try:
            data = json.loads(content)
            if not isinstance(data, dict):
                return False

            # 1. Explicit failure
            if data.get("success") is False:
                return True

            # 2. Empty data patterns (success: true but no useful data)
            if data.get("success") is True:
                inner_data = data.get("data")

                # data.total_records == 0 or data.total_indicators == 0
                if isinstance(inner_data, dict):
                    if inner_data.get("total_records") == 0:
                        return True
                    if inner_data.get("total_indicators") == 0:
                        return True

                # data is string (e.g., "No data...")
                if isinstance(inner_data, str):
                    return True

                # indicators is string
                indicators = data.get("indicators")
                if isinstance(indicators, str):
                    return True

        except (json.JSONDecodeError, TypeError):
            pass

        return False

    def _prepare_messages_for_gemini(
        self, collected_messages: list[BaseMessage]
    ) -> list[BaseMessage]:
        """
        Prepare messages for Gemini, ensuring correct AIMessage and ToolMessage order.

        Key modifications (see test_gemini_tool_injection.py):
        1. AIMessage.content must be empty string "", not None
        2. Preserves existing signatures but does NOT inject new ones
           (fake signatures cause Gemini 3.1 Pro to return empty output)
        3. ToolMessage order must match AIMessage tool_calls order!
           - Phase 1 tools execute in parallel, return order may differ
           - Gemini expects strict tool_call -> tool_result pairing order
        """
        if not collected_messages:
            return []

        # Group messages by round: each round = 1 AIMessage + N ToolMessages
        prepared: list[BaseMessage] = []
        current_ai_msg: AIMessage | None = None
        current_tool_call_ids: list[str] = []  # tool_call order in current AIMessage
        pending_tool_messages: dict[str, ToolMessage] = {}  # tool_call_id -> ToolMessage

        for msg in collected_messages:
            if isinstance(msg, AIMessage) and msg.tool_calls:
                # Flush previous round first
                if current_ai_msg and pending_tool_messages:
                    self._flush_round(prepared, current_ai_msg, current_tool_call_ids, pending_tool_messages)

                # Start new round
                current_ai_msg = msg
                current_tool_call_ids = [tc.get("id", "") for tc in msg.tool_calls if tc.get("id")]
                pending_tool_messages = {}

            elif isinstance(msg, ToolMessage) and current_ai_msg:
                # Collect ToolMessage, will reorder later
                tool_call_id = msg.tool_call_id
                if tool_call_id:
                    pending_tool_messages[tool_call_id] = msg

        # Flush last round
        if current_ai_msg and pending_tool_messages:
            self._flush_round(prepared, current_ai_msg, current_tool_call_ids, pending_tool_messages)

        logger.debug(f"Prepared {len(prepared)} messages for Gemini (reordered)")
        return prepared

    def _flush_round(
        self,
        prepared: list[BaseMessage],
        ai_msg: AIMessage,
        tool_call_ids: list[str],
        tool_messages: dict[str, ToolMessage],
    ) -> None:
        """
        Process one tool call round: add AIMessage and correctly ordered ToolMessages.
        """
        # 1. Process AIMessage — preserve existing kwargs but don't inject signatures
        new_kwargs = dict(ai_msg.additional_kwargs) if ai_msg.additional_kwargs else {}

        new_ai_msg = AIMessage(
            content="",  # Must be empty string!
            tool_calls=ai_msg.tool_calls,
            additional_kwargs=new_kwargs,
        )
        prepared.append(new_ai_msg)
        logger.debug(f"Added AIMessage with {len(ai_msg.tool_calls)} tool_calls")

        # 2. Add ToolMessages in tool_call order
        for tool_call_id in tool_call_ids:
            if tool_call_id in tool_messages:
                prepared.append(tool_messages[tool_call_id])
            else:
                logger.warning(f"Missing ToolMessage for tool_call_id={tool_call_id}")

    # === Phase 2 Prompt Building ===

    async def _build_phase2_prompt(
        self,
        has_tools: bool,
        language: str = "en",
        chart_context: list[dict[str, str]] | None = None,
        prompt_dir: str | None = None,
    ) -> str:
        """
        Build Phase 2 system prompt.

        Args:
            has_tools: Whether there are tool calls (passed to template for conditional rendering)
            language: Language code
            chart_context: Chart info list [{id, url, title}, ...]
            prompt_dir: Custom prompt directory (overrides default, for backward compatibility)

        Note:
            Does not pass user_id to template to protect privacy.
            user_name is a nickname and can be safely passed.

        Prompt Resolution Order:
            1. self._prompt_templates["responder"] (from PROMPTS_MIX config)
            2. prompt_dir + "responder.jinja" (if prompt_dir provided)
            3. Default path: mirobody/pub/agents/mix/prompts/responder.jinja
        """
        base_prompt = ""

        # Priority 1: Use prompt from _prompt_templates if available
        if self._prompt_templates and "responder" in self._prompt_templates:
            base_prompt = self._prompt_templates["responder"]
            logger.debug("Using responder prompt from prompt_templates")
        else:
            # Priority 2/3: Load from file
            template_name = "responder.jinja"
            if prompt_dir:
                template_path = os.path.join(prompt_dir, template_name)
            else:
                template_path = os.path.join(os.path.dirname(__file__), "prompts", template_name)

            try:
                with open(template_path, "r", encoding="utf-8") as f:
                    base_prompt = f.read()
            except FileNotFoundError:
                logger.warning(f"Prompt template not found: {template_path}, using empty prompt")

        current_time = datetime.now(ZoneInfo(self.timezone)).strftime(
            "%A, %B %d, %Y, at %I:00 %p %Z (UTC%z)"
        )

        try:
            template = Environment(enable_async=True).from_string(base_prompt)
            rendered_prompt = await template.render_async(
                agent_name=self.agent_name,
                user_name=getattr(self, 'user_info', None) and self.user_info.user_name or "",
                current_time=current_time,
                language=language,
                has_tools=has_tools,
                available_charts=chart_context or [],
            )
        except Exception as e:
            logger.warning(f"Failed to render Phase 2 prompt template: {e}")
            rendered_prompt = base_prompt

        return rendered_prompt

    # === Phase 1 Stream Processing ===

    async def _stream_agent(
        self,
        agent: Any,
        messages: list,
        config: dict,
        chat_context: Any = None,
        collect_tool_context: bool = False,
        stop_on_generate_answer: bool = False,
        skip_tool_names: set[str] | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Phase 1 stream processing (updates mode), collecting raw LangChain messages for Phase 2.

        Uses stream_mode="updates" to receive complete step updates, no token-by-token output needed.

        Output behavior:
        - AIMessage content always output as thinking (Phase 1 is thinking stage)
        - With tool calls: collect AIMessage (without content) + ToolMessage for Phase 2
        - Without tool calls: collect content to ai_partial_content, pass to Phase 2 as context

        Args:
            collect_tool_context: Collect tool messages (AIMessage + ToolMessage)
            stop_on_generate_answer: Stop after generate_answer
            skip_tool_names: Tool names to skip (don't show, don't collect)

        Yields:
            thinking/queryTitle/queryDetail events + final _metadata event
        """
        # Collection state
        collected_messages: list[BaseMessage] = []  # Raw LangChain messages
        pending_tool_ids: set[str] = set()
        generate_answer_triggered = False
        has_tools = False

        # Collect AI output content (for Phase 2 context when no tools)
        ai_content_parts: list[str] = []

        # Collect chart info (Phase 2 uses placeholders to reference)
        chart_context: list[dict[str, str]] = []
        chart_index = 0

        # Skipped tool IDs (for filtering)
        skipped_tool_ids: set[str] = set()

        trace_id = get_req_ctx("trace_id") or str(uuid.uuid4())
        logger.info(f"Starting Phase 1 stream (updates mode), trace_id={trace_id}")

        try:
            # Only use updates mode, receive complete step updates
            async for update in agent.astream(
                {"messages": messages},
                context=chat_context,
                stream_mode="updates",
                config=config
            ):
                for step, step_data in update.items():
                    if step not in ("model", "tools"):
                        continue
                    if not step_data or not isinstance(step_data, dict):
                        continue
                    if "messages" not in step_data or not step_data["messages"]:
                        continue

                    # Process all messages in this step
                    for msg in step_data["messages"]:
                        # === Process AIMessage (model output) ===
                        if isinstance(msg, AIMessage):
                            content = ""
                            if msg.content:
                                content = msg.content if isinstance(msg.content, str) else str(msg.content)

                            # Phase 1 AI content always output as thinking
                            if content and not generate_answer_triggered:
                                yield {"type": "thinking", "content": content}

                            # Process tool calls
                            if msg.tool_calls:
                                # Filter tools to skip
                                filtered_calls = msg.tool_calls
                                if skip_tool_names:
                                    filtered_calls = [
                                        tc for tc in msg.tool_calls
                                        if tc.get("name") not in skip_tool_names
                                    ]
                                    # Record skipped tool IDs
                                    for tc in msg.tool_calls:
                                        if tc.get("name") in skip_tool_names:
                                            skipped_tool_ids.add(tc.get("id", ""))

                                for tc in filtered_calls:
                                    tool_name = tc.get("name", "")
                                    tool_id = tc.get("id", "")

                                    # Detect generate_answer
                                    if stop_on_generate_answer and tool_name == "generate_answer":
                                        generate_answer_triggered = True
                                        logger.info(f"generate_answer triggered, pending tools: {len(pending_tool_ids)}")
                                        yield {"type": "queryTitle", "content": tool_name, "tool_id": tool_id}
                                        if not pending_tool_ids:
                                            break
                                        continue

                                    has_tools = True
                                    if collect_tool_context:
                                        pending_tool_ids.add(tool_id)

                                    # Output tool call title
                                    yield {"type": "queryTitle", "content": tool_name, "tool_id": tool_id}

                                # Collect messages (filtered, without content to keep compact)
                                if collect_tool_context and filtered_calls:
                                    collected_messages.append(AIMessage(
                                        content="",  # Don't pass content, Phase 2 only needs tool_calls
                                        tool_calls=filtered_calls,
                                    ))
                            else:
                                # No tool calls: collect content for Phase 2
                                if content:
                                    ai_content_parts.append(content)

                        # === Process ToolMessage (tool result) ===
                        elif isinstance(msg, ToolMessage):
                            tool_id = msg.tool_call_id

                            # Skip filtered tool results
                            if tool_id in skipped_tool_ids:
                                continue

                            tool_content = msg.content if isinstance(msg.content, str) else str(msg.content)

                            # Detect chart result and simplify for Phase 2
                            is_chart = False
                            if collect_tool_context and tool_content:
                                chart_info = self._extract_chart_info(tool_content, chart_index)
                                if chart_info:
                                    chart_context.append(chart_info)
                                    chart_index += 1
                                    is_chart = True

                            pending_tool_ids.discard(tool_id)

                            # Output tool result
                            yield {"type": "queryDetail", "content": tool_content, "tool_id": tool_id}

                            # Collect message (simplify chart results to save Phase 2 tokens)
                            if collect_tool_context:
                                if is_chart:
                                    simplified = json.dumps({
                                        "chart_id": chart_info["id"],
                                        "url": chart_info.get("url", ""),
                                        "chart_title": chart_info["title"],
                                    }, ensure_ascii=False)
                                    collected_messages.append(ToolMessage(
                                        content=simplified,
                                        tool_call_id=tool_id,
                                    ))
                                else:
                                    collected_messages.append(msg)

                    # Check if should exit
                    if generate_answer_triggered and not pending_tool_ids:
                        break

                # Outer loop also check exit condition
                if generate_answer_triggered and not pending_tool_ids:
                    break

        except Exception as e:
            logger.error(f"Phase 1 streaming error: {e}", exc_info=True)
            yield {"type": "error", "content": f"Phase 1 error: {e}"}
            return

        logger.info(f"Phase 1 complete: {len(collected_messages)} messages, {len(chart_context)} charts")

        # Return collected metadata
        if collect_tool_context:
            ai_partial_content = "".join(ai_content_parts)

            yield {
                "type": "_metadata",
                "collected_messages": collected_messages,  # Raw LangChain messages
                "ai_partial_content": ai_partial_content,
                "has_tools": has_tools,
                "chart_context": chart_context,
            }

    def _extract_chart_info(self, tool_content: str, index: int) -> dict[str, Any] | None:
        """
        Extract chart info from tool result (reuses StreamConverter).

        Args:
            tool_content: Tool output (JSON string or dict)
            index: Chart index

        Returns:
            {id, url, title, event} or None
            - id: For placeholder matching [[CHART:chart_0]]
            - url: Chart image URL
            - title: For responder.jinja template display
            - event: Complete image event, yield directly
        """
        # Reuse StreamConverter's chart detection logic
        chart_event = StreamConverter.extract_chart_data(tool_content, "")
        if not chart_event:
            return None

        # Parse content to get title and url (for template), keep complete event
        chart_data = json.loads(chart_event["content"])
        return {
            "id": f"chart_{index}",
            "url": chart_data.get("url", ""),
            "title": chart_data.get("title", ""),
            "event": chart_event,  # Complete event, yield directly
        }

    # === Phase 2 Message Preparation ===

    def _clean_tool_output_for_phase2(self, tool_name: str, raw_content: str) -> str:
        """Clean a single tool result for Phase 2 responder.

        Removes metadata noise while preserving ALL actual data:
        - 'desc' fields in health data (indicator names suffice for the health expert responder)
        - 'success' field (failures already filtered upstream by _filter_tool_messages)
        - Chart noise fields (filename, file_key, is_chart, message) — keeps chart_id + chart_title + url
        """
        try:
            data = json.loads(raw_content) if isinstance(raw_content, str) else raw_content
        except (json.JSONDecodeError, TypeError):
            return raw_content

        if not isinstance(data, dict):
            return raw_content

        data.pop("success", None)

        if tool_name.endswith("fetch_health_data") and "indicators" in data:
            for indicator_data in data["indicators"].values():
                if isinstance(indicator_data, dict):
                    indicator_data.pop("desc", None)

        if "chart" in tool_name:
            data = {k: v for k, v in data.items() if k in ("chart_id", "chart_title", "url")}

        return json.dumps(data, ensure_ascii=False)

    def _flatten_tool_calls_to_text(self, messages: list[BaseMessage]) -> list[BaseMessage]:
        """Convert AIMessage(tool_calls) + ToolMessage pairs into plain-text AIMessages.

        Phase 2 responder has NO tools — it must only see data, not tool-call format.
        Passing tool_call format causes models (especially Gemini) to attempt function
        calls instead of generating text.

        Each tool round (1 AIMessage + N ToolMessages) becomes a single AIMessage:
            [tool_name] result:
            <cleaned tool output>
        """
        if not messages:
            return []

        result: list[BaseMessage] = []
        current_ai: AIMessage | None = None
        tc_id_to_name: dict[str, str] = {}
        pending_tools: list[tuple[str, str]] = []

        def flush_round() -> None:
            nonlocal current_ai, tc_id_to_name, pending_tools
            if current_ai is None:
                return
            if not pending_tools:
                current_ai = None
                tc_id_to_name = {}
                return

            parts = []
            for tool_name, content in pending_tools:
                cleaned = self._clean_tool_output_for_phase2(tool_name, content)
                parts.append(f"[{tool_name}] result:\n{cleaned}")

            result.append(AIMessage(content="\n\n".join(parts)))

            current_ai = None
            tc_id_to_name = {}
            pending_tools = []

        for msg in messages:
            if isinstance(msg, AIMessage) and msg.tool_calls:
                flush_round()
                current_ai = msg
                tc_id_to_name = {tc["id"]: tc["name"] for tc in msg.tool_calls if tc.get("id")}
                pending_tools = []
            elif isinstance(msg, ToolMessage) and current_ai is not None:
                tool_name = tc_id_to_name.get(msg.tool_call_id, "unknown_tool")
                content = msg.content if isinstance(msg.content, str) else str(msg.content)
                pending_tools.append((tool_name, content))
            else:
                flush_round()
                result.append(msg)

        flush_round()

        logger.info(
            f"_flatten_tool_calls_to_text: {len(messages)} msgs -> {len(result)} "
            f"(tool rounds converted to plain text)"
        )
        return result

    def _restructure_phase2_messages(self, messages: list[BaseMessage]) -> list[BaseMessage]:
        """Restructure Phase 2 messages for non-Gemini models.

        Multiple consecutive AI/tool messages (collected data from Phase 1) are restructured into:
            SystemMessage(...)
            HumanMessage(<original query + files>)
            AIMessage("I found the following health data for your query:")
            HumanMessage(<all collected data merged>)

        This avoids issues with consecutive same-role messages that cause empty responses
        on some model providers (OpenRouter, DeepSeek, etc.).
        """
        system_msgs: list[SystemMessage] = []
        user_msgs: list[HumanMessage] = []
        model_texts: list[str] = []

        for msg in messages:
            if isinstance(msg, SystemMessage):
                system_msgs.append(msg)
            elif isinstance(msg, HumanMessage):
                user_msgs.append(msg)
            else:
                text = msg.content if isinstance(msg.content, str) else str(msg.content)
                if text:
                    model_texts.append(text)

        result: list[BaseMessage] = []
        result.extend(system_msgs)
        result.extend(user_msgs)

        if model_texts:
            result.append(AIMessage(content="I found the following health data for your query:"))
            result.append(HumanMessage(content="\n\n".join(model_texts)))

        return result

    # === Gemini Direct Streaming ===

    async def _gemini_direct_stream(
        self,
        model_name: str,
        messages: list[BaseMessage],
        thinking_level: str = "low",
        max_output_tokens: int = 65536,
    ) -> AsyncGenerator[dict, None]:
        """Stream Phase 2 response using the official Google GenAI SDK directly.

        Yields dicts: {"type": "text"/"thinking"/"usage", ...}
        Bypasses LangChain adapter which causes flaky empty responses with Gemini 3/3.1.
        """
        from google.genai import types

        client = self._get_or_create_genai_client()
        system_instruction, contents = self._lc_messages_to_genai_contents(messages)

        config = types.GenerateContentConfig(
            system_instruction=system_instruction if system_instruction else None,
            thinking_config=types.ThinkingConfig(thinking_level=thinking_level),
            max_output_tokens=max_output_tokens,
        )

        total_input_tokens = 0
        total_output_tokens = 0

        stream = await client.aio.models.generate_content_stream(
            model=model_name,
            contents=contents,
            config=config,
        )
        async for chunk in stream:
            if chunk.usage_metadata:
                total_input_tokens = chunk.usage_metadata.prompt_token_count or 0
                total_output_tokens = chunk.usage_metadata.candidates_token_count or 0

            if not chunk.candidates:
                continue

            for candidate in chunk.candidates:
                if not candidate.content or not candidate.content.parts:
                    continue
                for part in candidate.content.parts:
                    if part.thought and part.text:
                        yield {"type": "thinking", "thinking": part.text}
                    elif part.text:
                        yield {"type": "text", "text": part.text}

        yield {"type": "usage", "input_tokens": total_input_tokens, "output_tokens": total_output_tokens}

    def _get_or_create_genai_client(self):
        """Get or create a cached Google GenAI client."""
        if not hasattr(self, '_genai_client') or self._genai_client is None:
            from google import genai

            from ....utils.config import safe_read_cfg
            api_key = safe_read_cfg("GOOGLE_API_KEY") or os.environ.get("GOOGLE_API_KEY", "")
            self._genai_client = genai.Client(api_key=api_key)
        return self._genai_client

    def _lc_messages_to_genai_contents(
        self, messages: list[BaseMessage]
    ) -> tuple[str, list]:
        """Convert LangChain BaseMessages to Google GenAI Content objects.

        Returns (system_instruction, contents).
        Multiple consecutive model messages are restructured into user/model turns.
        """
        from google.genai import types

        system_instruction = ""
        user_parts: list[types.Part] = []
        model_texts: list[str] = []

        for msg in messages:
            if isinstance(msg, SystemMessage):
                system_instruction = msg.content if isinstance(msg.content, str) else str(msg.content)
                continue

            if isinstance(msg, HumanMessage):
                if isinstance(msg.content, list):
                    for block in msg.content:
                        if isinstance(block, dict):
                            block_type = block.get("type", "")
                            if block_type == "text":
                                user_parts.append(types.Part(text=block.get("text", "")))
                            elif block_type == "image_url":
                                # Image URL — pass as text reference (GenAI handles URLs natively)
                                url = block.get("image_url", {}).get("url", "")
                                if url:
                                    user_parts.append(types.Part(text=f"[Image: {url}]"))
                        elif isinstance(block, str):
                            user_parts.append(types.Part(text=block))
                elif isinstance(msg.content, str) and msg.content:
                    user_parts.append(types.Part(text=msg.content))
            else:
                # AIMessage, ToolMessage — collect as model text
                text = msg.content if isinstance(msg.content, str) else str(msg.content)
                if text:
                    model_texts.append(text)

        contents: list[types.Content] = []
        if user_parts:
            contents.append(types.Content(role="user", parts=user_parts))

        if model_texts:
            contents.append(types.Content(
                role="model",
                parts=[types.Part(text="I found the following health data for your query:")],
            ))
            contents.append(types.Content(
                role="user",
                parts=[types.Part(text="\n\n".join(model_texts))],
            ))

        return system_instruction, contents

    # === Phase 2 Streaming ===

    async def _stream_phase2_response(
        self,
        has_tools: bool,
        system_prompt: str,
        user_messages: list[dict[str, Any]],
        collected_messages: list[BaseMessage],
        chart_context: list[dict[str, str]],
        bind_tools_mode: str = "none",
        all_tools: list[BaseTool] | None = None,
        group: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Phase 2 streaming output.

        Automatically selects the best streaming strategy:
        - Gemini models: Uses Google GenAI SDK directly (bypasses LangChain flaky adapter)
        - Other models: Uses LangChain astream with message restructuring

        Message preparation:
        - Gemini direct: Converts to GenAI Content objects with restructured turns
        - Non-Gemini: Flattens tool calls to plain text + restructures same-role messages

        Args:
            has_tools: Whether Phase 1 made tool calls (used to select responder)
            system_prompt: System prompt
            user_messages: User history messages (dict format)
            collected_messages: Phase 1 collected LangChain messages (AIMessage + ToolMessage)
            chart_context: Chart info list [{id, title, event}, ...]
            bind_tools_mode: Tool binding mode ("none"/"used"/"all")
            all_tools: All available tools list (when bind_tools_mode != "none")
            group: Provider group name for selecting the correct responder

        Yields:
            reply/thinking/image events, last one is _cost_metadata event
        """
        # Build chart ID to event mapping
        chart_map = {c["id"]: c["event"] for c in chart_context}

        # Placeholder regex: [[CHART:chart_0]] or [[CHART:chart_1]]
        chart_pattern = re.compile(r'\[\[CHART:(\w+)\]\]')

        # Buffer for detecting cross-chunk placeholders
        buffer = ""

        # Get responder LLM based on whether Phase 1 had tool calls (filtered by group)
        llm, provider_name = self._get_responder_for_mode(has_tools, group=group)
        if not llm:
            logger.error(f"No responder LLM found for has_tools={has_tools}")
            yield {"type": "error", "content": f"No responder configured for has_tools={has_tools}"}
            return

        config = self._responder_configs.get(provider_name, {})
        model_name = config.get("model", provider_name)

        # Detect if responder is Gemini — use direct streaming to bypass LangChain adapter issues
        _use_direct_gemini = isinstance(model_name, str) and model_name.startswith("gemini")

        # Handle tool binding (only for LangChain path)
        if not _use_direct_gemini:
            if bind_tools_mode == "all" and all_tools:
                llm = llm.bind_tools(all_tools)
                logger.info(f"Phase 2: Bound {len(all_tools)} tools (all)")
            elif bind_tools_mode == "used" and all_tools and collected_messages:
                used_tool_names: set[str] = set()
                for msg in collected_messages:
                    if isinstance(msg, AIMessage) and msg.tool_calls:
                        for tc in msg.tool_calls:
                            used_tool_names.add(tc.get("name", ""))
                used_tools = [t for t in all_tools if t.name in used_tool_names]
                if used_tools:
                    llm = llm.bind_tools(used_tools)
                    logger.info(f"Phase 2: Bound {len(used_tools)} tools (used: {used_tool_names})")

        # Build message list
        messages: list[BaseMessage] = [SystemMessage(content=system_prompt)]

        # Add user history messages
        for msg in user_messages:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "user":
                messages.append(HumanMessage(content=content))
            elif role == "assistant":
                messages.append(AIMessage(content=content))

        # Prepare Phase 1 collected messages based on streaming strategy
        if collected_messages:
            if _use_direct_gemini:
                # Gemini direct: flatten tool calls to text (no tool_call format needed)
                flattened = self._flatten_tool_calls_to_text(collected_messages)
                messages.extend(flattened)
            else:
                # Non-Gemini: also flatten, then restructure to avoid consecutive same-role issues
                flattened = self._flatten_tool_calls_to_text(collected_messages)
                messages.extend(flattened)

        # For non-Gemini: restructure to avoid consecutive same-role messages
        if not _use_direct_gemini:
            messages = self._restructure_phase2_messages(messages)
            logger.info(f"Phase 2: Restructured to {len(messages)} msgs (non-Gemini)")

        # Token statistics
        token_callback = TokenUsageCallback()
        stream_config = {"callbacks": [token_callback]}

        # Log message details for debugging
        msg_summary = []
        for i, m in enumerate(messages):
            m_type = type(m).__name__
            m_len = len(m.content) if isinstance(m.content, str) else len(str(m.content)) if m.content else 0
            extra = ""
            if isinstance(m, AIMessage) and m.tool_calls:
                extra = f", tool_calls={len(m.tool_calls)}"
            if hasattr(m, 'additional_kwargs') and m.additional_kwargs:
                extra += f", additional_kwargs={list(m.additional_kwargs.keys())}"
            msg_summary.append(f"[{i}]{m_type}({m_len}ch{extra})")
        logger.info(f"Phase 2: Starting stream, model={model_name}, direct_gemini={_use_direct_gemini}, messages={len(messages)}, bind_tools={bind_tools_mode}")
        logger.info(f"Phase 2: Message details: {', '.join(msg_summary)}")

        if hasattr(llm, 'model_kwargs') and llm.model_kwargs:
            logger.info(f"Phase 2: LLM model_kwargs={llm.model_kwargs}")

        import time as _time

        for _attempt in range(1, MAX_PHASE2_RETRIES + 1):
            _phase2_stream_start = _time.monotonic()
            _chunk_count = 0
            _empty_chunk_count = 0
            _first_chunk_time = None
            _has_yielded = False
            buffer = ""

            try:
                # Select streaming backend
                if _use_direct_gemini:
                    _stream_iter = self._gemini_direct_stream(
                        model_name=model_name, messages=messages, thinking_level="low",
                    )
                else:
                    _stream_iter = llm.astream(messages, config=stream_config)

                async for chunk in _stream_iter:
                    _chunk_count += 1
                    text = ""

                    if _use_direct_gemini:
                        # Gemini direct stream yields dicts
                        if chunk.get("type") == "usage":
                            token_callback.total_input_tokens = chunk.get("input_tokens", 0)
                            token_callback.total_output_tokens = chunk.get("output_tokens", 0)
                            continue
                        elif chunk.get("type") == "thinking":
                            _has_yielded = True
                            yield {"type": "thinking", "content": chunk["thinking"]}
                            continue
                        elif chunk.get("type") == "text":
                            text = chunk.get("text", "")
                    else:
                        # LangChain chunk processing
                        content = chunk.content
                        if not content:
                            _empty_chunk_count += 1
                            if _empty_chunk_count <= 3:
                                logger.info(f"Phase 2: Empty chunk #{_chunk_count}, content={repr(content)}")
                            continue

                        if isinstance(content, list):
                            for block in content:
                                if isinstance(block, dict):
                                    block_type = block.get('type')
                                    if block_type == 'thinking':
                                        thinking_text = block.get('thinking', '')
                                        if thinking_text.strip():
                                            _has_yielded = True
                                            yield {"type": "thinking", "content": thinking_text}
                                    elif block_type == 'text':
                                        text += block.get('text', '')
                                elif isinstance(block, str) and block:
                                    text += block
                        elif isinstance(content, str):
                            text = content

                    if not text:
                        continue

                    if _first_chunk_time is None:
                        _first_chunk_time = _time.monotonic()
                        logger.info(
                            f"Phase 2: First text after {_first_chunk_time - _phase2_stream_start:.2f}s"
                        )

                    buffer += text

                    # Process chart placeholders in buffer
                    while True:
                        match = chart_pattern.search(buffer)
                        if not match:
                            break

                        pre_text = buffer[:match.start()]
                        if pre_text:
                            _has_yielded = True
                            yield {"type": "reply", "content": pre_text}

                        chart_id = match.group(1)
                        chart_event = chart_map.get(chart_id)
                        if chart_event:
                            _has_yielded = True
                            yield chart_event
                            logger.info(f"Phase 2: Emitted chart image, id={chart_id}")
                        else:
                            logger.warning(f"Phase 2: Unknown chart id={chart_id}")
                            yield {"type": "reply", "content": match.group(0)}

                        buffer = buffer[match.end():]

                    # Output safe buffer content (keep possibly incomplete placeholders)
                    safe_len = len(buffer) - 15
                    if safe_len > 0:
                        _has_yielded = True
                        yield {"type": "reply", "content": buffer[:safe_len]}
                        buffer = buffer[safe_len:]

            except Exception as e:
                _elapsed = _time.monotonic() - _phase2_stream_start
                logger.error(
                    f"Phase 2 stream error (attempt {_attempt}/{MAX_PHASE2_RETRIES}) after {_elapsed:.2f}s, "
                    f"chunks={_chunk_count}(empty={_empty_chunk_count}): {e}",
                    exc_info=True
                )
                if not _has_yielded and _attempt < MAX_PHASE2_RETRIES:
                    logger.info("Phase 2: Retrying after stream error (nothing yielded yet)...")
                    continue
                yield {"type": "error", "content": f"Phase 2 generation failed: {e}"}
                return

            # --- Retry decision ---
            _elapsed = _time.monotonic() - _phase2_stream_start
            _ttft_str = f"{_first_chunk_time - _phase2_stream_start:.2f}s" if _first_chunk_time else "N/A"
            _should_retry = (
                not _has_yielded
                and not buffer.strip()
                and _attempt < MAX_PHASE2_RETRIES
            )

            logger.info(
                f"Phase 2 stream done (attempt {_attempt}/{MAX_PHASE2_RETRIES}): {_elapsed:.2f}s, TTFT={_ttft_str}, "
                f"chunks={_chunk_count}(empty={_empty_chunk_count}), "
                f"has_yielded={_has_yielded}, buffer={len(buffer)}"
            )

            if _should_retry:
                logger.warning(
                    f"Phase 2: Empty response from {model_name}, "
                    f"retrying ({_attempt + 1}/{MAX_PHASE2_RETRIES})..."
                )
                continue

            if not _has_yielded and not buffer.strip():
                logger.warning(f"Phase 2: Empty response from {model_name} after all {MAX_PHASE2_RETRIES} attempts!")

            break

        # Output remaining buffer
        if buffer:
            while True:
                match = chart_pattern.search(buffer)
                if not match:
                    break
                pre_text = buffer[:match.start()]
                if pre_text:
                    yield {"type": "reply", "content": pre_text}

                chart_id = match.group(1)
                chart_event = chart_map.get(chart_id)
                if chart_event:
                    yield chart_event
                else:
                    yield {"type": "reply", "content": match.group(0)}
                buffer = buffer[match.end():]

            if buffer:
                yield {"type": "reply", "content": buffer}

        # Return token statistics
        logger.info(
            f"Phase 2 tokens: input={token_callback.total_input_tokens}, "
            f"output={token_callback.total_output_tokens}, "
            f"cache_read={token_callback.cache_read_tokens}"
        )

        yield {
            "type": "_cost_metadata",
            "input_tokens": token_callback.total_input_tokens,
            "output_tokens": token_callback.total_output_tokens,
            "model": model_name,
            "cache_read_tokens": token_callback.cache_read_tokens,
            "cache_creation_tokens": token_callback.cache_creation_tokens,
        }

    # === Cost Statistics ===

    def _merge_cost_statistics(
        self, phase1: dict[str, Any] | None, phase2: dict[str, Any] | None
    ) -> dict[str, Any]:
        """
        Merge cost statistics from two costStatistics events.

        Args:
            phase1: Phase 1 costStatistics event
            phase2: Phase 2 costStatistics event
        """
        p1 = phase1.get("content", {}) if phase1 else {}
        p2 = phase2.get("content", {}) if phase2 else {}

        p1_model = p1.get("model", "unknown")
        p2_model = p2.get("model", "unknown")
        p1_in = int(p1.get("input_tokens", 0))
        p1_out = int(p1.get("output_tokens", 0))
        p2_in = int(p2.get("input_tokens", 0))
        p2_out = int(p2.get("output_tokens", 0))

        p1_cost_str = p1.get("total_cost", "0")
        p2_cost_str = p2.get("total_cost", "0")

        total_cost = None
        try:
            p1_cost = float(p1_cost_str) if p1_cost_str != "unrecognized model" else 0.0
            p2_cost = float(p2_cost_str) if p2_cost_str != "unrecognized model" else 0.0
            if p1_cost > 0 or p2_cost > 0:
                total_cost = p1_cost + p2_cost
        except (ValueError, TypeError):
            pass

        total_tokens = p1_in + p1_out + p2_in + p2_out

        content = {
            "model": f"{p1_model} | {p2_model}",
            "input_tokens": str(p1_in + p2_in),
            "output_tokens": str(p1_out + p2_out),
            "total_tokens": str(total_tokens),
            "total_cost": f"{total_cost:.6f}" if total_cost else "unrecognized model",
        }

        # Merge cache metrics from both phases
        try:
            p1_cache_read = int(p1.get("cache_read_tokens", 0))
            p2_cache_read = int(p2.get("cache_read_tokens", 0))
            total_cache_read = p1_cache_read + p2_cache_read
            if total_cache_read > 0:
                content["cache_read_tokens"] = str(total_cache_read)
                p1_cost_saved = float(p1.get("cost_saved", 0) or 0)
                p2_cost_saved = float(p2.get("cost_saved", 0) or 0)
                total_cost_saved = p1_cost_saved + p2_cost_saved
                if total_cost_saved > 0:
                    content["cost_saved"] = f"{total_cost_saved:.6f}"
        except (ValueError, TypeError):
            pass

        return {"type": "costStatistics", "content": content}


__all__ = ["MixMixin"]
