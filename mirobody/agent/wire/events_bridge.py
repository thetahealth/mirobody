"""LangGraph's stream → `kernel.events` — the conversion every wire adapter
needs and none should write again.

An agent turn streams two things from LangGraph: ``stream_mode="messages"``
chunks (tokens, as they arrive) and ``stream_mode="updates"`` items (a node's
completed messages: the model's tool calls, the tools' results, an interrupt).
Every surface that renders a turn — this repository's chat chunks, a
consumer's SSE, an OpenAI-compatible relay — used to read those raw objects
itself and each got a different subset right. This module reads them once,
into the wire-neutral `AgentEvent` vocabulary; a renderer maps events to its
wire.

The pieces are composable on purpose. `messages_chunk_events` and
`updates_item_events` are the whole conversion; `text_events`,
`tool_call_events`, `tool_result_event`, `interrupt_event` and `result_status`
are its parts, for a renderer that needs the raw message beside the event
(this repository's ``queryDetail`` passes a tool result's content through
verbatim, multimodal blocks included).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ...kernel.events import (
    ActionRequest,
    AgentEvent,
    Interrupt,
    Interrupted,
    ReasoningDelta,
    TextContent,
    TextDelta,
    ToolArgumentsDelta,
    ToolCallCompleted,
    ToolCallStarted,
    ToolResult,
    UsageDelta,
)
from ..models.messages import message_reasoning

#: ``tool_kind(name) -> str``: how a renderer classifies a tool call
#: (`ToolCallStarted.kind`), so it never carries a hardcoded tool-name list.
ToolKind = Callable[[str], str]


def content_text(content: Any) -> str:
    """A message's ``.content`` (a string or a block list) as plain text; the
    text blocks of a list, nothing else."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(part.get("text", "") for part in content if isinstance(part, dict))
    return ""


def result_status(message: Any) -> dict:
    """``{"status": …}`` (plus ``error_kind``, ``truncated`` when they apply) from
    a tool message's ARTIFACT — a `kernel.tools.Envelope` when the tool answers
    with one. Never from its text: the envelope exists so a reader does not have
    to recover the machine-readable half from a rendering. A tool with no
    envelope makes no claim; one that RAISED (LangChain sets ``status="error"``
    on the message) still does."""
    artifact = getattr(message, "artifact", None)
    status = getattr(artifact, "status", None)
    if not isinstance(status, str):
        raw = getattr(message, "status", None)
        return {"status": raw} if isinstance(raw, str) and raw != "success" else {}
    out: dict = {"status": status}
    kind = getattr(artifact, "error_kind", None)
    if isinstance(kind, str):
        out["error_kind"] = kind
    if getattr(getattr(artifact, "meta", None), "truncated", False):
        out["truncated"] = True
    return out


def usage_event(chunk: Any, *, model_label: str | None = None) -> UsageDelta | None:
    """The chunk's ``usage_metadata`` as one `UsageDelta`, or ``None``."""
    usage = getattr(chunk, "usage_metadata", None)
    if not isinstance(usage, Mapping):
        return None
    input_details = usage.get("input_token_details") or {}
    output_details = usage.get("output_token_details") or {}
    return UsageDelta(
        input_tokens=int(usage.get("input_tokens") or 0),
        output_tokens=int(usage.get("output_tokens") or 0),
        cache_read_tokens=int(input_details.get("cache_read") or 0),
        cache_creation_tokens=int(input_details.get("cache_creation") or 0),
        reasoning_tokens=int(output_details.get("reasoning") or 0),
        model=model_label,
    )


def text_events(chunk: Any) -> list[AgentEvent]:
    """A streamed ``AIMessageChunk``'s reasoning and visible text, in that
    order. Text that arrives beside tool-call chunks is not reply text."""
    events: list[AgentEvent] = []
    reasoning = message_reasoning(chunk)
    if reasoning:
        events.append(ReasoningDelta(text=reasoning))
    text = content_text(getattr(chunk, "content", ""))
    if text and not getattr(chunk, "tool_call_chunks", None) and getattr(chunk, "type", "") in ("AIMessageChunk", "ai"):
        events.append(TextDelta(text=text))
    return events


def messages_chunk_events(chunk: Any, *, model_label: str | None = None) -> list[AgentEvent]:
    """One ``stream_mode="messages"`` chunk → its events: usage first, then
    reasoning, then reply text."""
    events: list[AgentEvent] = []
    usage = usage_event(chunk, model_label=model_label)
    if usage is not None:
        events.append(usage)
    events.extend(text_events(chunk))
    return events


def tool_call_events(message: Any, seen_tool_calls: set[str], *, tool_kind: ToolKind | None = None) -> list[AgentEvent]:
    """The tool calls on a completed ``AIMessage`` → started / arguments /
    completed, once per call id (an ``updates`` stream can re-emit a node)."""
    events: list[AgentEvent] = []
    for call in getattr(message, "tool_calls", None) or []:
        call_id = call.get("id") or ""
        if call_id in seen_tool_calls:
            continue
        seen_tool_calls.add(call_id)
        name = call.get("name", "")
        arguments = call.get("args") or {}
        events.append(ToolCallStarted(call_id=call_id, name=name, execution="server", kind=tool_kind(name) if tool_kind else "tool"))
        events.append(ToolArgumentsDelta(call_id=call_id, arguments_delta=json.dumps(arguments, ensure_ascii=False)))
        events.append(ToolCallCompleted(call_id=call_id, arguments=arguments))
    return events


def is_tool_message(message: Any) -> bool:
    return getattr(message, "type", "") == "tool" or type(message).__name__ == "ToolMessage"


def tool_result_event(message: Any) -> ToolResult:
    """A ``ToolMessage`` → `ToolResult`; ``failed`` when the tool raised or its
    envelope says error, ``completed`` otherwise."""
    content = getattr(message, "content", "")
    text = content_text(content) or str(content)
    status = result_status(message).get("status")
    return ToolResult(
        call_id=getattr(message, "tool_call_id", ""),
        status="failed" if status in ("error", "failed") else "completed",
        content=(TextContent(text=text),),
    )


def interrupt_event(interrupts: Iterable[Any]) -> Interrupted:
    """The ``__interrupt__`` payload → `Interrupted`, with EVERY pending action
    under its real name. Taking only the first and calling it ``ask_user``
    corrupts any surface with several client tools paused at once."""
    interrupts = list(interrupts or ())
    first = interrupts[0] if interrupts else None
    value = getattr(first, "value", None) or {}
    raw_requests = (value.get("action_requests") or []) if isinstance(value, dict) else []
    interrupt_id = getattr(first, "id", "") or ""

    def field(request: Any, key: str) -> Any:
        return request.get(key) if isinstance(request, Mapping) else getattr(request, key, None)

    actions = tuple(
        ActionRequest(call_id="call_" + uuid.uuid4().hex, name=str(field(r, "name") or ""), arguments=field(r, "args") or {})
        for r in raw_requests
    )
    return Interrupted(interrupt=Interrupt(interrupt_id=interrupt_id, continuation_token=interrupt_id, action_requests=actions))


def updates_item_events(
    data: Mapping,
    seen_tool_calls: set[str],
    *,
    tool_kind: ToolKind | None = None,
    nodes: Iterable[str] | None = None,
) -> tuple[list[AgentEvent], bool]:
    """One ``stream_mode="updates"`` item → ``(events, interrupted)``.

    ``interrupted`` is true exactly when the item carries ``__interrupt__``; the
    single `Interrupted` returned then is terminal and the caller stops.
    ``nodes`` limits which graph nodes are read (a renderer that only wants the
    ``model`` and ``tools`` nodes passes them); every message of a node update
    is read — a tools node that ran several calls in parallel reports all of
    its results, not the last.
    """
    if "__interrupt__" in data:
        return [interrupt_event(data["__interrupt__"])], True
    wanted = set(nodes) if nodes is not None else None
    events: list[AgentEvent] = []
    for node, update in data.items():
        if wanted is not None and node not in wanted:
            continue
        if not isinstance(update, Mapping):
            continue
        for message in update.get("messages") or []:
            if getattr(message, "tool_calls", None):
                events.extend(tool_call_events(message, seen_tool_calls, tool_kind=tool_kind))
            elif is_tool_message(message):
                events.append(tool_result_event(message))
    return events, False
