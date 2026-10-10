"""The eval-citation contract: which rids an `eval` call surfaced, and the
`{result, refs}` shape its result then carries.

The readings tool can mint a rid (`kernel.citations`) and still have nothing
to cite: a model computing derived values inside `eval` (a window mean, a
diff, a slope) returns whatever the JS returns — usually `null` — and the
numbers it computed refer to rows that only existed inside the REPL. The
judge's citation check then has no defined input. This module is the tube
between the two ends of the harness that fix it, both stdlib-only:

* :data:`_RID_SINK`, a context variable the eval middleware brackets every
  `eval` call with, and the readings tool reports the rids of the rows it
 just surfaced to, contextually safe across threads (one `eval` = one fresh
 list; async host calls hop loops via `run_coroutine_threadsafe`, which
 carries the context through);
* :func:`refs_result_text`, the pure rewrite that turns a returned JS value's
 rendered text into the documented `{result, refs}` contract — with the
 console tail (`console_tail`) beside it when the model logged its numbers
 and returned nothing — and the explicit-refs scan that lets the model
 override.

Underscore-prefixed so the tool loader never publishes anything in here,
and langchain-free for the same reason the services are: the MCP surface
shares them.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from contextvars import ContextVar
from typing import Any

#: The sink for one in-flight `eval` call: the ordered rids of the rows its
#: PTC tool calls have surfaced so far. None (the default) means "no eval is
#: bracketing this code path" — a direct tool call reports nothing. A plain
#: list is deliberate: context copies share the OBJECT, and appends from a
#: few threads (worker loop, outer loop) stay visible to whichever end
#: drains it; list.append is definitionally atomic under the GIL.
_RID_SINK: ContextVar[list[str] | None] = ContextVar("eval_rid_sink", default=None)


def bracket_eval_rids() -> tuple[list[str], object]:
    """Start one eval's rid collection: (the fresh sink, the reset token).

    Middleware calls this around an eval and resets with the token after;
    whatever rids the readings tool surfaced meanwhile are in the list, in
    first-surfaced order (duplicates included by design: order is evidence
    of *what happened*, and dedup is a drain-time concern).
    """
    sink: list[str] = []
    return sink, _RID_SINK.set(sink)


def unbracket_eval_rids(token: object) -> None:
    """End the collection started by :func:`bracket_eval_rids`."""
    _RID_SINK.reset(token)  # type: ignore[arg-type]


def record_eval_rids(rids: Iterable[str]) -> None:
    """Report that the calling context (an `eval` in flight) just surfaced
    rows with these rids. No-op outside an eval's bracket."""
    sink = _RID_SINK.get()
    if sink is not None:
        sink.extend(rids)


def _unique(rids: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(rids))


#: How much of an eval's console goes into the envelope: the TAIL, because
#: teachers log their computed values last. 2000 is half the library's
#: default result budget (langchain_quickjs truncates the result body at
#: 4000): a logged table tail and the refs and the answer must all fit one
#: message a 32k-window model keeps around. The REPL's own console buffer
#: may have dropped EARLIER output before this trim even applies
#: (`max_stdout_chars`), which is why "tail" is the honest unit.
CONSOLE_TAIL_CHARS = 2000


def console_tail(stdout: str | None) -> str:
    """The console output an envelope carries: the last CONSOLE_TAIL_CHARS of
    the eval's captured stdout, "" when there was none (the key is then
    omitted). When cut, a leading `…` marks it as a tail, so nobody reads it
    as the whole log."""
    if not stdout:
        return ""
    if len(stdout) <= CONSOLE_TAIL_CHARS:
        return stdout
    return "…" + stdout[-CONSOLE_TAIL_CHARS:]


def is_object_shaped(text: str | None) -> bool:
    """Whether a returned JS value's rendered text is object-shaped: begins
    with `{` and ends with `}` (QuickJS's serializer writes dicts that way,
    keys unquoted). A bare string value that happens to look like that is
    the known false positive; injecting `refs` into one is a cosmetic
    corruption of a display string, weighed against the alternative (an
    eval returning a computed OBJECT and silently losing the contract),
    and loses.
    """
    stripped = (text or "").strip()
    return len(stripped) >= 2 and stripped.startswith("{") and stripped.endswith("}")


def _has_toplevel_refs_key(text: str) -> bool:
    """Whether a `{...}`-shaped text names `refs` as a top-level key: a string-aware
    depth scan, so `{note: "refs: none"}` and `{nested: {refs: []}}` do not
    count. JS-literal strings are double- or single-quoted with backslash
    escapes.
    """
    depth = 0
    quote = ""
    escaped = False
    for index, char in enumerate(text):
        if quote:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char in ('"', "'"):
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        elif char == ":" and depth == 1:
            # The key is the identifier straight before the colon, starting
            # after a `{` or `,` at the same depth.
            back = index - 1
            while back >= 0 and (text[back].isalnum() or text[back] in "_$ "):
                back -= 1
            if text[back] in "{," and text[back + 1 : index].strip() == "refs":
                return True
    return False


def refs_result_text(text: str | None, refs: Iterable[str], *, console: str = "") -> str:
    """The eval result with its citation refs applied, in the notation the
    REPL already speaks (QuickJS's serializer writes JS-object text, not
    JSON; matching it keeps every eval answer in one grammar).

    * a returned OBJECT gets `refs: [...]` injected inside its braces —
      unless it already names one: explicit refs win, narrowed or widened,
      and the judge reads them. No `console`: the object already carries the
      numbers, and its `<stdout>` block carries the log;
    * anything else (`null`, `undefined`, a number, a bare string, an array)
      becomes `{result: <value>, refs: [...]}`, strict JSON — the value it
      wraps parses as JSON in every scalar case a model realistically
      returns, and falls back to a quoted string when it does not — with
      `console` (the tail from `console_tail`) inserted when the model
      logged its numbers and returned nothing: teachers log, then `return null`.
    """
    refs_syms = _unique(refs)
    refs_json = json.dumps(refs_syms)
    if text and is_object_shaped(text):
        if _has_toplevel_refs_key(text):
            return text
        body = text.strip()
        inner = body[1:-1].strip()
        return f"{{{inner}, refs: {refs_json}}}" if inner else f"{{refs: {refs_json}}}"
    value_text = (text or "").strip() or "null"
    if value_text == "undefined":
        value_text = "null"
    try:
        value = json.loads(value_text)
    except ValueError:
        value = value_text
    body: dict[str, Any] = {"result": value}
    if console:
        body["console"] = console
    body["refs"] = refs_syms
    return json.dumps(body, ensure_ascii=False)


__all__ = [
    "CONSOLE_TAIL_CHARS",
    "bracket_eval_rids",
    "console_tail",
    "is_object_shaped",
    "record_eval_rids",
    "refs_result_text",
    "unbracket_eval_rids",
]
