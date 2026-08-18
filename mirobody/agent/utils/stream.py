import ast
import json
import logging
from typing import Dict, Any, AsyncGenerator

from langchain_core.callbacks import AsyncCallbackHandler

logger = logging.getLogger(__name__)


# LangGraph stream node names that contribute to the final user-visible output.
FINAL_OUTPUT_NODES: set[str] = {"tools", "model"}


# costStatistics deliberately reports TOKENS ONLY. It used to also compute
# dollar amounts from a hardcoded MODEL_PRICING table; provider prices change
# faster than any table gets refreshed, so the amounts drifted into fiction
# while looking authoritative. Tokens are facts from the API; prices are not.


class StreamConverter:
    """
    Pure static utility class for converting LangGraph stream output to unified format.
    
    Uses stream_mode="messages" for real-time LLM token streaming.
    Tool calls and results are handled via stream_mode="updates" in deep_agent.py.
    
    Supported output types: reply, thinking, costStatistics
    
    Core features:
    - Real-time processing of LLM token streams
    - Stateless pure functions
    - Complete data pass-through to upper layers
    """
    
    @staticmethod
    async def convert_message_chunk(
        chunk: Any,
        metadata: Dict[str, Any],
        trace_id: str = None,
        is_subagent: bool = False,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Convert LangGraph message chunk to unified format.

        Only processes AIMessageChunk for text/thinking content.
        Tool calls are handled separately via updates mode.

        Args:
            chunk: LangChain AIMessageChunk object
            metadata: Metadata containing langgraph_step, langgraph_node, etc.
            trace_id: Optional trace ID for logging
            is_subagent: True when the chunk originates from a subagent subgraph
                (non-empty stream namespace). Subagent text is surfaced as
                ``thinking`` rather than ``reply`` so a delegated agent's narration
                streams into the process/thinking channel instead of being glued
                into the main assistant answer (the subagent's final report still
                arrives as the ``task`` tool's ``queryDetail``).

        Yields:
            Unified format event dictionary with type: reply or thinking
        """
        try:
            # Skip summarization-internal model calls — they are not user-facing output.
            # SummarizationMiddleware marks these runs with lc_source="summarization" in
            # the config metadata, which LangGraph propagates into stream event metadata.
            if metadata.get('lc_source') == 'summarization':
                return

            chunk_type = type(chunk).__name__

            # Extract metadata information
            node_info = StreamConverter._extract_metadata_info(metadata)
            node_name = node_info.get("node")
            
            # Only process AIMessageChunk for text content from final output nodes
            if chunk_type == "AIMessageChunk" and node_name in FINAL_OUTPUT_NODES:
                async for event in StreamConverter._handle_ai_message_chunk(
                    chunk, node_info, trace_id, is_subagent=is_subagent
                ):
                    yield event
            
        except Exception as e:
            logger.error(f"Error converting message chunk: {str(e)}", exc_info=True)
    
    @staticmethod
    async def _handle_ai_message_chunk(
        chunk: Any,
        node_info: Dict[str, Any],
        trace_id: str = None,
        is_subagent: bool = False,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Handle AIMessageChunk - only process text content for real-time streaming.

        Tool calls are handled separately via updates mode in deep_agent.py.

        When ``is_subagent`` is True the chunk comes from a delegated subagent
        subgraph; its text is emitted as ``thinking`` (process channel) instead of
        ``reply`` so it does not merge into the main assistant answer.
        """
        # Subagent text streams into the thinking channel; main-agent text is reply.
        text_event_type = "thinking" if is_subagent else "reply"
        content = getattr(chunk, 'content', '')
        if content:
            # Handle list format from Gemini (e.g., [{'type': 'text', 'text': '...', 'index': 0}])
            if isinstance(content, list):
                text_parts = []
                thinking_parts = []

                for block in content:
                    if isinstance(block, dict):
                        block_type = block.get('type')
                        if block_type == 'text':
                            text_parts.append(block.get('text', ''))
                        elif block_type == 'thinking':
                            # Gemini uses 'thinking' field for thinking content
                            thinking_parts.append(block.get('thinking', ''))

                # Output thinking content separately with type="thinking"
                if thinking_parts:
                    thinking_content = ''.join(thinking_parts)
                    if thinking_content:
                        yield {
                            "type": "thinking",
                            "content": thinking_content,
                            **node_info
                        }

                content = ''.join(text_parts)

            # Regular text: reply for main agent, thinking for a subagent.
            if content:
                yield {
                    "type": text_event_type,
                    "content": content,
                    **node_info
                }
    
    @staticmethod
    def _extract_metadata_info(metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key information from metadata.
        
        Args:
            metadata: Metadata provided by LangGraph
            
        Returns:
            Dictionary containing node, step, and other information
        """
        return {
            "node": metadata.get('langgraph_node'),
            "step": metadata.get('langgraph_step'),
            "model": metadata.get('ls_model_name'),
        }
    
    @staticmethod
    async def process_stream_event(
        stream_type: str,
        stream_event: Any,
        trace_id: str = None,
        namespace: Any = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a single stream event from LangGraph agent.astream().

        Handles both "messages" and "updates" stream modes:
        - messages: Real-time token-by-token text streaming
        - updates: Tool calls and tool results (complete messages)

        Args:
            stream_type: Either "messages" or "updates"
            stream_event: The event data from LangGraph stream
            trace_id: Optional trace ID for logging
            namespace: LangGraph subgraph namespace tuple (from astream
                ``subgraphs=True``). Empty/None means the main agent; a non-empty
                tuple means a subagent subgraph, whose message text is surfaced as
                ``thinking`` instead of ``reply``.

        Yields:
            Unified format event dictionaries with types:
            - reply, thinking (from messages mode)
            - queryTitle (tool call info)
            - queryDetail, image (tool results)
        """
        from langchain_core.messages import AIMessage, ToolMessage

        # Non-empty namespace => event came from a subagent subgraph.
        is_subagent = bool(namespace)

        try:
            # Handle text streaming (real-time token-by-token)
            if stream_type == "messages":
                try:
                    chunk, chunk_metadata = stream_event
                except (TypeError, ValueError) as e:
                    logger.warning(
                        f"Invalid messages event format: {type(stream_event).__name__}, "
                        f"error: {str(e)}, trace_id={trace_id}"
                    )
                    return

                async for event in StreamConverter.convert_message_chunk(
                    chunk, chunk_metadata, trace_id=trace_id, is_subagent=is_subagent
                ):
                    if event:
                        yield event
            
            # Handle tool calls and results (complete messages)
            elif stream_type == "updates":
                for step, step_data in stream_event.items():
                    # Only process model and tools steps
                    if step not in FINAL_OUTPUT_NODES:
                        continue
                    
                    # Validate step_data
                    if step_data is None or not isinstance(step_data, dict):
                        logger.debug(
                            f"Skipping node '{step}' with invalid step_data type: "
                            f"{type(step_data).__name__}, trace_id={trace_id}"
                        )
                        continue
                    
                    # Validate messages field
                    if 'messages' not in step_data:
                        logger.debug(
                            f"Skipping node '{step}' without messages field, trace_id={trace_id}"
                        )
                        continue
                    
                    if not step_data['messages'] or not isinstance(step_data['messages'], list):
                        logger.debug(
                            f"Skipping node '{step}' with invalid messages: "
                            f"{type(step_data.get('messages')).__name__}, trace_id={trace_id}"
                        )
                        continue
                    
                    # Get the last message in this step (latest update)
                    last_message = step_data['messages'][-1]
                    
                    # Step "model": Extract AIMessage content and tool_calls
                    if step == "model":
                        if isinstance(last_message, AIMessage):
                            # Handle tool calls
                            if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
                                for tc in last_message.tool_calls:
                                    tool_name = tc.get('name', '')
                                    tool_id = tc.get('id', '')
                                    tool_args = tc.get('args', {})
                                    
                                    if tool_name and tool_id:
                                        # Log complete tool call
                                        logger.info(
                                            f"[Tool Call] trace_id={trace_id},"
                                            f"tool={tool_name}, args={tool_args}, id={tool_id}"
                                        )
                                        
                                        # Emit queryTitle with display name
                                        yield {
                                            "type": "queryTitle",
                                            "content": tool_name,
                                            "tool_id": tool_id,
                                        }
                                        # Emit queryArguments separately, bound by tool_id
                                        yield {
                                            "type": "queryArguments",
                                            "content": json.dumps(tool_args, ensure_ascii=False),
                                            "tool_id": tool_id,
                                        }
                    
                    # Step "tools": Extract ToolMessage results
                    elif step == "tools":
                        if isinstance(last_message, ToolMessage):
                            tool_content = last_message.content 
                            tool_call_id = last_message.tool_call_id
                            
                            if tool_content:
                                # Log complete tool result
                                logger.info(
                                    f"[Tool Result] trace_id={trace_id}, "
                                    f"tool_id={tool_call_id}, result={tool_content}"
                                )
                                
                                # Emit queryDetail (no truncation, pass complete data to upper layer)
                                yield {
                                    "type": "queryDetail",
                                    "content": tool_content,
                                    "tool_id": tool_call_id,
                                }


        except Exception as e:
            logger.error(
                f"Error processing stream event: {str(e)}, "
                f"stream_type={stream_type}, "
                f"event_type={type(stream_event).__name__}, "
                f"trace_id={trace_id}", 
                exc_info=True
            )
    
    @staticmethod
    def _coerce_tool_result(tool_content: Any) -> Dict[str, Any] | None:
        """Best-effort parse of a tool result into a dict (JSON, then Python-literal)."""
        if isinstance(tool_content, dict):
            return tool_content
        if isinstance(tool_content, str):
            try:
                parsed = json.loads(tool_content)
            except (json.JSONDecodeError, TypeError):
                try:
                    parsed = ast.literal_eval(tool_content)
                except (ValueError, SyntaxError):
                    return None
            return parsed if isinstance(parsed, dict) else None
        return None

    @staticmethod
    def create_cost_statistics(
        input_tokens: int,
        output_tokens: int,
        model_name: str = "unknown",
        cache_read_tokens: int = 0,
        cache_creation_tokens: int = 0
    ) -> Dict[str, Any] | None:
        """
        Token-usage statistics for the turn. Tokens only, no dollar amounts —
        see the module-level note.

        Args:
            input_tokens: Non-cached input tokens
            output_tokens: Output tokens
            model_name: Model name (display only)
            cache_read_tokens: Tokens read from cache (prompt caching hit)
            cache_creation_tokens: Tokens written to cache (prompt caching miss)

        Returns:
            costStatistics event dictionary or None on error
        """
        try:
            # Detect format:
            # - OpenAI/OpenRouter: input_tokens includes cache_read_tokens (input >= cache_read)
            # - Anthropic: input_tokens is non-cached only (input < cache_read typically)
            is_openai_format = input_tokens >= cache_read_tokens and cache_read_tokens > 0

            if is_openai_format:
                # OpenAI format: input_tokens already includes cache_read
                total_input = input_tokens + cache_creation_tokens
            else:
                # Anthropic format: input_tokens is non-cached only
                total_input = input_tokens + cache_read_tokens + cache_creation_tokens

            total_tokens = total_input + output_tokens

            # Build response (all values as strings for stability)
            content = {
                "model": model_name,
                "input_tokens": str(total_input),
                "output_tokens": str(output_tokens),
                "total_tokens": str(total_tokens),
            }

            # Cache info only if cache was involved
            if cache_read_tokens > 0:
                content["cache_read_tokens"] = str(cache_read_tokens)
            if cache_creation_tokens > 0:
                content["cache_creation_tokens"] = str(cache_creation_tokens)

            return {
                "type": "costStatistics",
                "content": content
            }
        except Exception as e:
            logger.error(f"Failed to create cost statistics: {e}")
            return None

class TokenUsageCallback(AsyncCallbackHandler):
    """
    Callback handler to track token usage across LLM calls.

    Supports multiple providers:
    - Anthropic (direct API): cache_read_input_tokens, cache_creation_input_tokens
    - OpenRouter: prompt_tokens_details.cached_tokens
    - OpenAI: prompt_tokens_details.cached_tokens
    - Gemini: usage_metadata.input_tokens
    """

    def __init__(self):
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.cache_read_tokens = 0
        self.cache_creation_tokens = 0

    async def on_llm_end(self, response, **kwargs):
        # Track if we've already extracted tokens to avoid double counting
        tokens_extracted = False
        cache_extracted = False

        # Debug: log full response structure
        logger.debug(f"[TokenUsage] llm_output keys: {response.llm_output.keys() if response.llm_output else 'None'}")

        # Debug: the whole final message, not just usage. Exists because a turn
        # ended with output tokens spent but zero streamed text and zero tool
        # calls — only the full message shows where those tokens went
        # (finish_reason, invalid_tool_calls, non-text content blocks).
        try:
            for gens in response.generations:
                for gen in gens:
                    msg = getattr(gen, "message", None)
                    if msg is not None:
                        logger.debug(
                            "[LLMEnd] content=%r additional_kwargs=%r invalid_tool_calls=%r response_metadata=%r",
                            getattr(msg, "content", None),
                            getattr(msg, "additional_kwargs", None),
                            getattr(msg, "invalid_tool_calls", None),
                            getattr(msg, "response_metadata", None),
                        )
        except Exception as e:
            logger.debug(f"[LLMEnd] introspection failed: {e}")

        # Method 1: response.llm_output["token_usage"] (OpenAI/OpenRouter format)
        if response.llm_output and "token_usage" in response.llm_output:
            usage = response.llm_output["token_usage"]
            logger.debug(f"[TokenUsage] token_usage: {usage}")

            # Extract cache tokens first (needed for input calculation)
            cached_tokens = 0
            if not cache_extracted:
                # Anthropic format
                self.cache_read_tokens += usage.get("cache_read_input_tokens", 0)
                self.cache_creation_tokens += usage.get("cache_creation_input_tokens", 0)

                # OpenRouter/OpenAI format: prompt_tokens_details.cached_tokens
                prompt_details = usage.get("prompt_tokens_details") or {}
                if prompt_details:
                    logger.debug(f"[TokenUsage] prompt_tokens_details: {prompt_details}")
                    cached_tokens = prompt_details.get("cached_tokens", 0)
                    if cached_tokens > 0:
                        self.cache_read_tokens += cached_tokens
                        cache_extracted = True

                if self.cache_read_tokens > 0 or self.cache_creation_tokens > 0:
                    cache_extracted = True

            if not tokens_extracted:
                # OpenAI/OpenRouter: prompt_tokens (may include cached)
                # Anthropic: input_tokens (non-cached only)
                # Pass raw value - create_cost_statistics handles both formats
                prompt_tokens = usage.get("prompt_tokens", 0) or usage.get("input_tokens", 0)
                self.total_input_tokens += prompt_tokens
                self.total_output_tokens += usage.get("completion_tokens", 0) or usage.get("output_tokens", 0)
                tokens_extracted = True

        # Method 2: message.usage_metadata (LangChain standard)
        for generation in response.generations:
            for chunk in generation:
                if not hasattr(chunk, "message"):
                    continue

                msg = chunk.message

                # usage_metadata (Gemini/Claude/newer LangChain versions)
                if hasattr(msg, "usage_metadata") and msg.usage_metadata:
                    metadata = msg.usage_metadata
                    logger.debug(f"[TokenUsage] usage_metadata: {metadata}")

                    if not tokens_extracted:
                        self.total_input_tokens += metadata.get("input_tokens", 0)
                        self.total_output_tokens += metadata.get("output_tokens", 0)
                        tokens_extracted = True

                    if not cache_extracted:
                        # Anthropic cache in usage_metadata
                        self.cache_read_tokens += metadata.get("cache_read_input_tokens", 0)
                        self.cache_creation_tokens += metadata.get("cache_creation_input_tokens", 0)

                        # input_token_details format (some LangChain versions)
                        input_details = metadata.get("input_token_details") or {}
                        if input_details:
                            logger.debug(f"[TokenUsage] input_token_details: {input_details}")
                            self.cache_read_tokens += input_details.get("cache_read", 0)
                            self.cache_creation_tokens += input_details.get("cache_creation", 0)

                        if self.cache_read_tokens > 0 or self.cache_creation_tokens > 0:
                            cache_extracted = True

                # response_metadata.usage (Anthropic specific, only for cache)
                if not cache_extracted and hasattr(msg, "response_metadata") and msg.response_metadata:
                    resp_usage = msg.response_metadata.get("usage", {})
                    if resp_usage:
                        logger.debug(f"[TokenUsage] response_metadata.usage: {resp_usage}")
                        self.cache_read_tokens += resp_usage.get("cache_read_input_tokens", 0)
                        self.cache_creation_tokens += resp_usage.get("cache_creation_input_tokens", 0)

        logger.info(
            f"[TokenUsage] input={self.total_input_tokens}, output={self.total_output_tokens}, "
            f"cache_read={self.cache_read_tokens}, cache_creation={self.cache_creation_tokens}"
        )
