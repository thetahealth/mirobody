"""
Chat history replay for cross-turn tool/file memory.

Background: the legacy replay path
(`get_last_message` + `compress_messages`) flattened history down to the
assistant `reply` text and dropped the whole tool trace, so on the next turn the
agent had no memory of having read a file and would re-`read_file` the same PDF.
It also prefixed every message with a per-turn ``[timestamp]`` which broke the
prompt cache for the history segment.

This module implements the two replay strategies that fix that, both operating on
the persisted ``element_list`` (the JSON chunk list saved to ``th_messages``):

* R1 ``summarize_tool_trace`` — provider-agnostic. Folds a one-line
  "(previous turn — called read_file /uploads/foo.pdf: HbA1c 6.1% ...)" summary
  into the replayed assistant *text*. Cheap, no structured messages, gives the
  agent enough to know it already read the file.

* R3 ``rebuild_canonical_messages`` — rebuilds real LangChain
  ``AIMessage``/``ToolMessage`` pairs from the element_list, so the agent sees the
  true tool_use/tool_result causal chain. Large/older tool results are offloaded
  to a "reference + summary" stub, keeping tool_use/tool_result pairs intact
  (never an orphan tool_use).

Both paths are pure functions of the element_list — no DB, no network — so they
are unit-testable in isolation.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage

#-----------------------------------------------------------------------------
# Tunables
#-----------------------------------------------------------------------------

# Only read_file's RESULT is literally the bytes of a file that still live at
# args.path, so only its offload stub may point back at that path for re-reading.
# The path can be /uploads (a chat-uploaded file, scoped to this message), /library
# (the user's user_id-bound file history), OR a model-written path like /memories
# /tool_outputs — we echo whatever path was read, never assume /uploads|/library.
#
# Other tools are deliberately NOT here: ls/glob/grep return listings (not a
# readable file at a path); write_file/edit_file return a status (the written
# bytes live at the WRITE path, e.g. /memories, not in the result, and definitely
# not in /uploads|/library); bash/queries are transient. They get a generic stub.
PATH_BACKED_RESULT_TOOLS = {"read_file"}

# A single tool result longer than this (chars) is a candidate for offload once
# it is no longer one of the most-recent results. ~3-4 chars/token, so ~2k tok.
TOOL_RESULT_CHAR_CAP = 8000

# The N most-recent tool results are always kept in full (current question may
# need them); everything older is offloaded to a stub.
KEEP_RECENT_TOOL_RESULTS = 2

# Length of the queryDetail excerpt kept in the R1 one-line trace / offload stub.
TRACE_RESULT_EXCERPT = 160

# Argument keys that carry a file path / location, in priority order.
_PATH_KEYS = ("path", "file_path", "filepath", "file", "file_key", "name", "filename")

#-----------------------------------------------------------------------------
# Element-list parsing (shared by R1 and R3)
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


def extract_reply(element_list: list[dict[str, Any]]) -> str:
    """Concatenate the assistant ``reply`` text from an element_list."""
    return "".join(
        e.get("content", "")
        for e in element_list
        if isinstance(e, dict) and e.get("type") == "reply" and isinstance(e.get("content"), str)
    )


def parse_tool_calls(element_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reconstruct ordered tool calls from an element_list.

    Pairs ``queryTitle`` (tool name) / ``queryArguments`` (json args) /
    ``queryDetail`` (result) by their shared ``tool_id``, preserving first-seen
    order. Returns a list of ``{id, name, args(raw str|None), result(str|None)}``.
    ``tool_id`` is always emitted by the stream layer, so pairing is reliable.
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


def _parse_args(raw: Any) -> dict[str, Any]:
    """Best-effort parse of a queryArguments payload into a dict (LangChain
    ``tool_calls[*].args`` must be a mapping)."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


def _extract_path(args: Any) -> str:
    """Pull a file path / location out of tool args for trace/offload stubs."""
    d = _parse_args(args)
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
# R1 — one-line tool/file trace folded into replayed assistant text
#-----------------------------------------------------------------------------


def summarize_tool_trace(element_list: list[dict[str, Any]] | str | None) -> str:
    """Return a single provider-agnostic line summarising the tool calls in an
    assistant turn, e.g.::

        (previous turn — called read_file /uploads/foo.pdf: HbA1c 6.1% …;
         called search_health_indicators: 12 results)

    Only an *excerpt* of each result is kept — never the full payload. File-tool
    calls carry their path so the next turn re-references /uploads or /library
    instead of rediscovering the file. Returns "" when there are no tool calls.
    """
    element_list = _coerce_element_list(element_list)
    calls = parse_tool_calls(element_list)
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
    """Append the R1 trace line to an assistant ``reply`` for the legacy/text
    replay path. No-op when there is no tool trace."""
    trace = summarize_tool_trace(element_list)
    if not trace:
        return reply
    return f"{reply}\n{trace}" if reply else trace


#-----------------------------------------------------------------------------
# R3 — canonical AIMessage/ToolMessage reconstruction with offload
#-----------------------------------------------------------------------------


def _offload_stub(name: str, args: Any, result: str) -> str:
    """Build the "reference + summary" replacement for an old/large tool result.

    Only read_file's result is the bytes of a file still on the virtual FS, so its
    stub points back at the exact path that was read (re-read with read_file for
    detail) — that path is wherever the file is (/uploads, /library, /memories…),
    echoed from the args, never assumed. Every other tool's result is not a
    re-readable file at a path, so it gets a generic excerpt+size stub.
    """
    excerpt = _one_line(result, TRACE_RESULT_EXCERPT)
    size = len(result)
    path = _extract_path(args)
    if name in PATH_BACKED_RESULT_TOOLS and path:
        return (
            f"[offloaded {size} chars — file content is at {path}; "
            f"summary: {excerpt}. Re-read it with read_file for full detail.]"
        )
    return f"[offloaded {size} chars; summary: {excerpt}. Re-run the tool to retrieve the full output.]"


def _select_assistant_by_agent(group: list[dict[str, Any]], agent: str | None) -> dict[str, Any]:
    """Mirror legacy compress_messages: from a run of consecutive assistant
    messages prefer the one whose ``agent`` matches, else take the last."""
    if agent:
        for m in group:
            if m.get("agent") == agent:
                return m
    return group[-1]


def _user_text(msg: dict[str, Any]) -> str:
    """Extract a textual representation of a user message for replay. Handles the
    file-upload bubble shape ({files:[...]}) by listing the attached paths."""
    content = msg.get("content", "")
    if isinstance(content, str) and content:
        # Might be a JSON file-bubble; try to surface filenames.
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return content
        if isinstance(data, dict) and data.get("files"):
            names = [
                f.get("filename") or f.get("file_name") or f.get("file_key", "")
                for f in data["files"] if isinstance(f, dict)
            ]
            names = [n for n in names if n]
            label = "; ".join(names)
            return f"(attached files: {label})" if label else content
        return content
    if isinstance(msg.get("files"), list):  # already-extracted file message
        names = [f.get("file_name") or f.get("s3_key", "") for f in msg["files"]]
        names = [n for n in names if n]
        return "(attached files: " + "; ".join(names) + ")" if names else ""
    return content if isinstance(content, str) else ""


def rebuild_canonical_messages(
    messages: list[dict[str, Any]],
    agent: str | None = None,
    keep_recent: int = KEEP_RECENT_TOOL_RESULTS,
    result_char_cap: int = TOOL_RESULT_CHAR_CAP,
) -> list[BaseMessage]:
    """Rebuild a canonical LangChain message list from replayed history rows.

    ``messages`` is a chronological list of dicts (oldest→newest) as produced by
    ``get_last_message``: each has ``role``, ``agent`` and an ``element_list``
    (the raw persisted chunk list; falls back to parsing ``content``).

    Produces ``HumanMessage`` / ``AIMessage`` / ``ToolMessage`` objects where:

    * every ``AIMessage.tool_calls[i].id`` has a matching ``ToolMessage`` with the
      same ``tool_call_id`` (no orphan tool_use, even if the result is missing);
    * the ``keep_recent`` most-recent tool results are kept in full (capped at
      ``result_char_cap``); older or oversized ones are replaced by an offload
      stub — content only, the pair is never broken;
    * tool results are stored as plain text (multimodal blocks are already
      text/JSON in queryDetail), so replay is provider-agnostic.
    """
    # 1) Collapse consecutive assistant runs to a single message (agent-aware),
    #    matching the legacy selection semantics.
    selected: list[dict[str, Any]] = []
    i = 0
    n = len(messages)
    while i < n:
        msg = messages[i]
        if msg.get("role") == "assistant":
            group = []
            while i < n and messages[i].get("role") == "assistant":
                group.append(messages[i])
                i += 1
            selected.append(_select_assistant_by_agent(group, agent))
        else:
            selected.append(msg)
            i += 1

    # 2) First pass: find the ids of the most-recent tool results to keep full.
    all_call_ids: list[str] = []
    for msg in selected:
        if msg.get("role") == "assistant":
            for c in parse_tool_calls(_coerce_element_list(msg.get("element_list") or msg.get("content"))):
                all_call_ids.append(c["id"])
    keep_full_ids = set(all_call_ids[-keep_recent:]) if keep_recent > 0 else set()

    # 3) Second pass: emit canonical messages.
    out: list[BaseMessage] = []
    for msg in selected:
        role = msg.get("role")
        if role == "user":
            text = _user_text(msg)
            out.append(HumanMessage(content=text))
            continue

        if role != "assistant":
            continue

        element_list = _coerce_element_list(msg.get("element_list") or msg.get("content"))
        reply = extract_reply(element_list)
        calls = parse_tool_calls(element_list)

        if not calls:
            # Plain assistant turn. Fall back to a legacy plain-text `content`
            # row (no element_list / unparseable JSON) so old history isn't lost.
            if not reply:
                raw = msg.get("content")
                if isinstance(raw, str) and raw.strip() and not element_list:
                    reply = raw
            if reply:
                out.append(AIMessage(content=reply))
            continue

        tool_calls = [
            {"id": c["id"], "name": c.get("name") or "tool", "args": _parse_args(c.get("args"))}
            for c in calls
        ]
        out.append(AIMessage(content=reply, tool_calls=tool_calls))

        # Each tool_call MUST be followed by a paired ToolMessage.
        for c in calls:
            result = c.get("result")
            if result is None:
                content = "[no result recorded for this tool call]"
            else:
                result = result if isinstance(result, str) else json.dumps(result, ensure_ascii=False)
                if c["id"] in keep_full_ids and len(result) <= result_char_cap:
                    content = result
                elif c["id"] in keep_full_ids:
                    # Recent but oversized: hard cap rather than full offload.
                    content = result[:result_char_cap] + "\n…[truncated]"
                else:
                    content = _offload_stub(c.get("name") or "tool", c.get("args"), result)
            out.append(ToolMessage(content=content, tool_call_id=c["id"]))

    return out


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
    sits AFTER the cache breakpoint (history + system are cached; the new turn is
    never cached anyway) — so temporal continuity is restored at zero cache cost.

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


def assert_pairs_intact(messages: list[BaseMessage]) -> None:
    """Invariant check used by tests: every AIMessage tool_call id has a matching
    ToolMessage and vice-versa. Raises AssertionError on violation."""
    ai_ids: list[str] = []
    tool_ids: list[str] = []
    for m in messages:
        if isinstance(m, AIMessage):
            ai_ids.extend(tc["id"] for tc in (m.tool_calls or []))
        elif isinstance(m, ToolMessage):
            tool_ids.append(m.tool_call_id)
    orphans_ai = set(ai_ids) - set(tool_ids)
    orphans_tool = set(tool_ids) - set(ai_ids)
    assert not orphans_ai, f"orphan tool_use (no result): {orphans_ai}"
    assert not orphans_tool, f"orphan tool_result (no call): {orphans_tool}"
    logging.debug("assert_pairs_intact: %d tool pairs OK", len(ai_ids))
