"""History replay for BaseAgent — and it lives here because it is BaseAgent's.

This module used to sit in ``agent/chat/`` as if replay were a property of the
chat transport. It is not: it is the memory strategy of ONE agent. BaseAgent
has no graph, so nothing persists its conversation for it, and its stateless
providers (DeepSeek's ``/responses`` is stateless by spec) return no history
from the provider side either — so its caller must replay the transcript as
flat text. That is a BaseAgent fact, so it belongs next to BaseAgent.

**DeepAgent does not use any of this.** Its graph is compiled with a LangGraph
Postgres checkpointer keyed on ``thread_id = session_id`` (see
``agent/deep/checkpointer.py``), so LangGraph persists the real
``AIMessage``/``ToolMessage`` objects and supplies the conversation itself; the
adapter hands it only the new turn.

This module used to also hold ``rebuild_canonical_messages`` — 200 lines that
rebuilt those same LangChain messages by parsing the persisted ``element_list``
(the UI chunk dicts the SSE stream emits) and re-pairing tool_use/tool_result by
``tool_id``, with an offload scheme for large results. That was a hand-rolled
checkpointer, and a lossy one: the element_list is shaped for a UI, so
``thinking`` never survived the round trip and tool arguments came back as
re-parsed JSON strings. It is deleted, not ported.

What remains:

* ``fold_trace_into_text`` — folds a one-line
  "(previous turn — called read_file /uploads/foo.pdf: HbA1c 6.1% …)" summary
  into the replayed assistant text, so the next turn knows it already read a
  file instead of re-reading the same PDF.
* ``relative_time_hint`` — a resume hint for the CURRENT turn when a user comes
  back days later.

Both are pure functions of the persisted rows — no DB, no network.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

#-----------------------------------------------------------------------------
# Tunables
#-----------------------------------------------------------------------------

# Length of the queryDetail excerpt kept in the one-line trace.
TRACE_RESULT_EXCERPT = 160

# Argument keys that carry a file path / location, in priority order.
_PATH_KEYS = ("path", "file_path", "filepath", "file", "file_key", "name", "filename")

#-----------------------------------------------------------------------------
# Element-list parsing
#-----------------------------------------------------------------------------


def _coerce_element_list(content: Any) -> list[dict[str, Any]]:
    """Normalise a persisted message ``content`` into an element_list.

    The DB stores assistant content as a JSON array of chunk dicts. Be tolerant
    of already-parsed lists, JSON strings, and plain text (legacy rows)."""
    if isinstance(content, list):
        return [e for e in content if isinstance(e, dict)]
    if isinstance(content, str):
        try:
            parsed = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return []
        if isinstance(parsed, list):
            return [e for e in parsed if isinstance(e, dict)]
    return []


def parse_tool_calls(element_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reconstruct ordered tool calls from an element_list.

    Pairs ``queryTitle`` (tool name) / ``queryArguments`` (json args) /
    ``queryDetail`` (result) by their shared ``tool_id``, preserving first-seen
    order. Returns a list of ``{id, name, args(raw str|None), result(str|None)}``.
    """
    by_id: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    def _slot(tid: str) -> dict[str, Any]:
        if tid not in by_id:
            by_id[tid] = {"id": tid, "name": "", "args": None, "result": None}
            order.append(tid)
        return by_id[tid]

    for e in element_list:
        if not isinstance(e, dict):
            continue
        t = e.get("type")
        tid = e.get("tool_id")
        if not tid or t not in ("queryTitle", "queryArguments", "queryDetail"):
            continue
        slot = _slot(tid)
        content = e.get("content", "")
        if t == "queryTitle":
            slot["name"] = content or slot["name"]
        elif t == "queryArguments":
            slot["args"] = content
        elif t == "queryDetail":
            slot["result"] = content

    return [by_id[i] for i in order]


def _extract_path(args: Any) -> str:
    """Pull a file path / location out of raw tool args for the trace line."""
    d: Any = args
    if isinstance(d, str) and d.strip():
        try:
            d = json.loads(d)
        except (json.JSONDecodeError, TypeError):
            return ""
    if not isinstance(d, dict):
        return ""
    for k in _PATH_KEYS:
        v = d.get(k)
        if isinstance(v, str) and v:
            return v
    return ""


def _one_line(text: str, limit: int) -> str:
    """Collapse whitespace and truncate to ``limit`` chars with an ellipsis."""
    if not isinstance(text, str):
        text = str(text)
    flat = " ".join(text.split())
    if len(flat) > limit:
        return flat[:limit].rstrip() + "…"
    return flat


#-----------------------------------------------------------------------------
# One-line tool/file trace folded into replayed assistant text
#-----------------------------------------------------------------------------


def summarize_tool_trace(element_list: list[dict[str, Any]] | str | None) -> str:
    """Return a single provider-agnostic line summarising an assistant turn's
    tool calls, e.g.::

        (previous turn — called read_file /uploads/foo.pdf: HbA1c 6.1% …;
         called query_health_indicators: 12 results)

    Only an *excerpt* of each result is kept — never the full payload. File-tool
    calls carry their path so the next turn re-references /uploads or /library
    instead of rediscovering the file. Returns "" when there are no tool calls.
    """
    calls = parse_tool_calls(_coerce_element_list(element_list))
    if not calls:
        return ""

    segments: list[str] = []
    for c in calls:
        name = c.get("name") or "tool"
        path = _extract_path(c.get("args"))
        seg = f"{name} {path}".strip()
        result = c.get("result")
        if result:
            excerpt = _one_line(result, TRACE_RESULT_EXCERPT)
            if excerpt:
                seg = f"{seg}: {excerpt}"
        segments.append(seg)

    return "(previous turn — called " + "; ".join(segments) + ")"


def fold_trace_into_text(reply: str, element_list: list[dict[str, Any]] | str | None) -> str:
    """Append the trace line to an assistant ``reply`` for the text replay path.
    No-op when there is no tool trace."""
    trace = summarize_tool_trace(element_list)
    if not trace:
        return reply
    return f"{reply}\n{trace}" if reply else trace


#-----------------------------------------------------------------------------
# Temporal context — "now" lives in the system prompt; this adds a resume hint
#-----------------------------------------------------------------------------

# Below this gap we say nothing (normal back-and-forth needs no annotation).
_RESUME_HINT_MIN_SECONDS = 6 * 3600


def relative_time_hint(messages: list[dict[str, Any]], now: datetime | None = None) -> str:
    """Return a short note about how long it's been since the previous message,
    intended to be appended to the CURRENT turn's user message.

    Why here and not per history message: the absolute "current time" is already
    injected (hour-rounded, for cache stability) into the system prompt every
    turn, so the model always knows *now*. What flat replay loses is the *gap*
    when a user resumes a chat days later. Putting a per-message ``[timestamp]``
    back would re-break the history prompt cache (it's a classic cache-buster).
    Instead we surface a single relative hint on the *current* user turn, which
    sits AFTER the cache breakpoint — so temporal continuity is restored at zero
    cache cost.

    Returns "" for short/unknown gaps.
    """
    last: datetime | None = None
    for m in messages:
        ts = m.get("created_at") if isinstance(m, dict) else None
        if isinstance(ts, datetime):
            if last is None or ts > last:
                last = ts
    if last is None:
        return ""
    if now is None:
        now = datetime.now(timezone.utc)
    # Align tz-awareness (PG timestamptz is aware; be defensive about naive).
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    secs = (now - last).total_seconds()
    if secs < _RESUME_HINT_MIN_SECONDS:
        return ""
    days = int(secs // 86400)
    if days >= 1:
        span = f"{days} day{'s' if days != 1 else ''}"
    else:
        hours = int(secs // 3600)
        span = f"{hours} hour{'s' if hours != 1 else ''}"
    return f"(Context: about {span} have passed since the previous message in this conversation.)"
