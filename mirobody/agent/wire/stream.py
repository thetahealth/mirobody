import ast
import json
import logging
from typing import Any
from collections.abc import AsyncGenerator

from langchain_core.callbacks import AsyncCallbackHandler

from .events_bridge import ReasoningDelta, TextDelta, ToolArgumentsDelta, ToolCallStarted, is_tool_message, result_status, text_events, tool_call_events
from ..models.usage import UsageAccumulator

from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


# LangGraph stream node names that contribute to the final user-visible output.
FINAL_OUTPUT_NODES: set[str] = {"tools", "model"}


# costStatistics deliberately reports TOKENS ONLY. It used to also compute
# dollar amounts from a hardcoded MODEL_PRICING table; provider prices change
# faster than any table gets refreshed, so the amounts drifted into fiction
# while looking authoritative. Tokens are facts from the API; prices are not.



# The artifact-status reader lives with the bridge now; the name stays for the
# contract test and the adapters that import it.
_result_status = result_status


class StreamConverter:
    """This repository's chunk dialect — ``reply`` / ``thinking`` / ``queryTitle``
    / ``queryArguments`` / ``queryDetail`` / ``costStatistics`` — rendered from
    the wire-neutral events `events_bridge` reads out of LangGraph's stream.

    What is this dialect's own: only the ``model`` and ``tools`` nodes are
    user-visible; summarisation-internal model calls are not; a subagent's text
    goes to the ``thinking`` channel so a delegated run narrates instead of
    gluing itself into the answer; every chunk carries the node/step/model it
    came from; and a tool result's content passes through VERBATIM (multimodal
    blocks included) with the status read off its artifact.
    """

    @staticmethod
    def _node_info(metadata: dict[str, Any]) -> dict[str, Any]:
        return {"node": metadata.get("langgraph_node"), "step": metadata.get("langgraph_step"), "model": metadata.get("ls_model_name")}

    @staticmethod
    async def convert_message_chunk(
        chunk: Any,
        metadata: dict[str, Any],
        trace_id: str = None,
        is_subagent: bool = False,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """One ``messages`` chunk → ``reply`` / ``thinking`` chunks."""
        try:
            # SummarizationMiddleware's own model calls are not user-facing output.
            if metadata.get("lc_source") == "summarization":
                return
            node_info = StreamConverter._node_info(metadata)
            if type(chunk).__name__ != "AIMessageChunk" or node_info.get("node") not in FINAL_OUTPUT_NODES:
                return
            for event in text_events(chunk):
                if isinstance(event, ReasoningDelta):
                    yield {"type": "thinking", "content": event.text, **node_info}
                elif isinstance(event, TextDelta):
                    # A subagent's narration streams into the process channel.
                    yield {"type": "thinking" if is_subagent else "reply", "content": event.text, **node_info}
        except Exception as e:
            logger.error("Error converting message chunk: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))

    @staticmethod
    async def process_stream_event(
        stream_type: str,
        stream_event: Any,
        trace_id: str = None,
        namespace: Any = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """One item of ``agent.astream(stream_mode=["messages", "updates"])`` →
        this dialect's chunks. ``namespace`` non-empty means a subagent subgraph.
        Interrupts are the caller's (`hitl.widget_chunk`), not this converter's.
        """
        is_subagent = bool(namespace)
        try:
            if stream_type == "messages":
                try:
                    chunk, chunk_metadata = stream_event
                except (TypeError, ValueError) as e:
                    logger.warning(
                        "Invalid messages event format: event_type=%s error_type=%s trace_id=%s",
                        type(stream_event).__name__, type(e).__name__, trace_id,
                    )
                    return
                async for event in StreamConverter.convert_message_chunk(chunk, chunk_metadata, trace_id=trace_id, is_subagent=is_subagent):
                    if event:
                        yield event
            elif stream_type == "updates":
                seen: set[str] = set()
                for node, update in stream_event.items():
                    if node not in FINAL_OUTPUT_NODES or not isinstance(update, dict):
                        continue
                    for message in update.get("messages") or []:
                        if getattr(message, "tool_calls", None):
                            for event in tool_call_events(message, seen):
                                if isinstance(event, ToolCallStarted):
                                    # Ids and names only: the arguments are the user's question.
                                    tool_name, tool_id = event.name, event.call_id
                                    logger.info("[Tool Call] trace_id=%s tool_name=%s tool_id=%s", trace_id, tool_name, tool_id)
                                    yield {"type": "queryTitle", "content": event.name, "tool_id": event.call_id}
                                elif isinstance(event, ToolArgumentsDelta):
                                    yield {"type": "queryArguments", "content": event.arguments_delta, "tool_id": event.call_id}
                        elif is_tool_message(message) and message.content:
                            # The result is the user's health data: log its size, never its text.
                            logger.info("[Tool Result] trace_id=%s tool_id=%s result_len=%d", trace_id, message.tool_call_id, len(str(message.content)))
                            # Content VERBATIM (a client parses it; multimodal blocks
                            # ride through), status off the artifact — additive keys
                            # existing clients ignore.
                            yield {"type": "queryDetail", "content": message.content, "tool_id": message.tool_call_id, **result_status(message)}
        except Exception as e:
            # A driver exception's text quotes the statement with its parameters;
            # the traceback is kept for everything else.
            logger.error(
                "Error processing stream event: error_type=%s stream_type=%s event_type=%s trace_id=%s",
                type(e).__name__, stream_type, type(stream_event).__name__, trace_id,
                exc_info=not is_driver_exception(e),
            )

    @staticmethod
    def _coerce_tool_result(tool_content: Any) -> dict[str, Any] | None:
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


class TokenUsageCallback(AsyncCallbackHandler):
    """Feeds every model call's normalised ``usage_metadata`` into one
    `usage.UsageAccumulator` for the turn. The provider-specific parsing that
    used to live here (``llm_output.token_usage``, Anthropic's
    ``cache_read_input_tokens``, OpenAI's ``prompt_tokens_details``) is what
    LangChain's ``usage_metadata`` already normalises — see `agent/models/usage.py`."""

    def __init__(self):
        self.usage = UsageAccumulator()

    async def on_llm_end(self, response, **kwargs):
        for generations in response.generations:
            for generation in generations:
                message = getattr(generation, "message", None)
                usage = getattr(message, "usage_metadata", None) if message is not None else None
                if usage:
                    self.usage.add(usage)
        input_tokens, output_tokens = self.usage.input_tokens, self.usage.output_tokens
        cache_read_tokens, cache_creation_tokens = self.usage.cache_read, self.usage.cache_creation
        logger.info(
            "[TokenUsage] input_tokens=%d output_tokens=%d cache_read_tokens=%d cache_creation_tokens=%d",
            input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
        )
