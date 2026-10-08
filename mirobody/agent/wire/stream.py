"""LangGraph's stream → the blocks in `blocks.py`.

Two decisions are this renderer's own, and they are why it reads the stream
rather than taking `events_bridge`'s finished events:

* only the ``model`` and ``tools`` nodes are user-visible, so a
  summarisation-internal model call never reaches a client;
* a tool result's content passes through VERBATIM, multimodal blocks
  included, with the status read off its artifact rather than off its text.

Interrupts are the caller's (`hitl.interrupt_block`), not this module's.
"""

import logging
from typing import Any
from collections.abc import AsyncGenerator, Iterator

from langchain_core.callbacks import AsyncCallbackHandler

from .blocks import REASONING, TEXT, TOOL_CALL, TOOL_RESULT
from .events_bridge import (
    ReasoningDelta,
    TextDelta,
    ToolCallCompleted,
    ToolCallStarted,
    is_tool_message,
    result_status,
    text_events,
    tool_call_events,
)
from mirobody.agent.models.usage import UsageAccumulator

from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


# LangGraph stream node names that contribute to the final user-visible output.
FINAL_OUTPUT_NODES: set[str] = {"tools", "model"}


def _message_blocks(chunk: Any, metadata: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """One ``messages`` chunk → `text` / `reasoning` blocks."""
    # SummarizationMiddleware's own model calls are not user-facing output.
    if metadata.get("lc_source") == "summarization":
        return
    if type(chunk).__name__ != "AIMessageChunk" or metadata.get("langgraph_node") not in FINAL_OUTPUT_NODES:
        return
    for event in text_events(chunk):
        if isinstance(event, ReasoningDelta):
            yield {"type": REASONING, "reasoning": event.text}
        elif isinstance(event, TextDelta):
            yield {"type": TEXT, "text": event.text}


def _update_blocks(update: Any, seen: set[str], trace_id: str | None) -> Iterator[dict[str, Any]]:
    """One ``updates`` item → `tool_call` / `tool_result` blocks."""
    for node, node_update in update.items():
        if node not in FINAL_OUTPUT_NODES or not isinstance(node_update, dict):
            continue
        for message in node_update.get("messages") or []:
            if getattr(message, "tool_calls", None):
                # `tool_call_events` owns the once-per-call-id dedup an
                # `updates` stream needs; a call's name and its arguments
                # arrive as two of its events and become one block here.
                names: dict[str, str] = {}
                for event in tool_call_events(message, seen):
                    if isinstance(event, ToolCallStarted):
                        tool_name = names[event.call_id] = event.name
                        # Ids and names only: the arguments are the user's question.
                        logger.info("[Tool Call] trace_id=%s tool_name=%s tool_id=%s",
                                    trace_id, tool_name, event.call_id)
                    elif isinstance(event, ToolCallCompleted):
                        yield {"type": TOOL_CALL, "id": event.call_id,
                               "name": names.pop(event.call_id, ""), "args": event.arguments}
            elif is_tool_message(message) and message.content:
                # The result is the user's health data: log its size, never its text.
                logger.info("[Tool Result] trace_id=%s tool_id=%s result_len=%d",
                            trace_id, message.tool_call_id, len(str(message.content)))
                yield {"type": TOOL_RESULT, "tool_call_id": message.tool_call_id,
                       "content": message.content, **result_status(message)}


async def stream_blocks(
    stream_type: str,
    stream_event: Any,
    trace_id: str | None = None,
) -> AsyncGenerator[dict[str, Any], None]:
    """One item of ``agent.astream(stream_mode=["messages", "updates"])`` → its
    blocks. Never raises: an item it cannot read is logged and dropped."""
    try:
        if stream_type == "messages":
            try:
                chunk, metadata = stream_event
            except (TypeError, ValueError) as e:
                logger.warning(
                    "Invalid messages event format: event_type=%s error_type=%s trace_id=%s",
                    type(stream_event).__name__, type(e).__name__, trace_id,
                )
                return
            for block in _message_blocks(chunk, metadata):
                yield block
        elif stream_type == "updates":
            for block in _update_blocks(stream_event, set(), trace_id):
                yield block
    except Exception as e:
        # A driver exception's text quotes the statement with its parameters;
        # the traceback is kept for everything else.
        logger.error(
            "Error processing stream event: error_type=%s stream_type=%s event_type=%s trace_id=%s",
            type(e).__name__, stream_type, type(stream_event).__name__, trace_id,
            exc_info=not is_driver_exception(e),
        )


class TokenUsageCallback(AsyncCallbackHandler):
    """Feeds every model call's normalised ``usage_metadata`` into one
    `usage.UsageAccumulator` for the turn. The provider-specific parsing that
    used to live here (``llm_output.token_usage``, Anthropic's
    ``cache_read_input_tokens``, OpenAI's ``prompt_tokens_details``) is what
    LangChain's ``usage_metadata`` already normalises: see `agent/models/usage.py`."""

    def __init__(self):
        self.usage = UsageAccumulator()

    async def on_llm_end(self, response, **kwargs):
        for generations in response.generations:
            for generation in generations:
                message = getattr(generation, "message", None)
                usage = getattr(message, "usage_metadata", None) if message is not None else None
                if usage:
                    self.usage.add(usage)
        cache_read_tokens, cache_creation_tokens = self.usage.cache_read, self.usage.cache_creation
        logger.info(
            "[TokenUsage] input_tokens=%d output_tokens=%d cache_read_tokens=%d cache_creation_tokens=%d",
            self.usage.input_tokens, self.usage.output_tokens,
            cache_read_tokens, cache_creation_tokens,
        )
