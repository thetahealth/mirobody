"""The chat channel's "which date?" — one agent-only tool (issue #53).

`ask_user` is a deepagents human-in-the-loop interrupt: the tool body never
runs. When the model calls it, `HumanInTheLoopMiddleware` pauses the graph
after the model step, `DeepAgent._stream_agent_response` turns the pending
call into a `widget` chunk (question + options) and the turn ends; the user's
next message resumes the SAME thread as the tool's result
(`Command(resume={"decisions": [{"type": "respond", ...}]})`). The thread is
the LangGraph checkpointer (`deep/checkpointer.py`), so nothing else has to
remember that a question is open.

When the question is the examination date of attachments, the model names
them in `report_date_for` and the ANSWER IS APPLIED HERE, before the model
sees it: the date is parsed out of the reply (an option tapped, or free text
like "2026年1月6日" / "就按今天"), filed through the same rule the Data page
bar and `POST /health-indicators/file-date` use (`services/report_date.py`),
and the tool result tells the model what happened. One tool, one round trip.
The tool is handed to DeepAgent directly — it is NOT in the MCP tool
directory: an external MCP client has no widget to answer with.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Optional

from langchain_core.tools import tool

#: The `interrupt_on` entry that makes `ask_user` pause the run. "respond" is
#: the only decision that makes sense for a question: the answer IS the result.
ASK_USER_INTERRUPT = {"ask_user": {"allowed_decisions": ["respond"]}}


@tool
def ask_user(question: str, options: Optional[list[str]] = None,
             report_date_for: Optional[list[str]] = None) -> str:
    """Ask the user ONE short question and WAIT for the answer — never guess.

    Use it when only the user knows something you need before acting
    correctly. `options` (2-4 short strings) renders as one-tap buttons; the
    user may still type. The reply is returned as this tool's result.

    Asking which date a report is from: pass the attachments' file_keys (from
    the attachment note) in `report_date_for`. The answer — a date such as
    "2026-01-06", or "就按今天" / "keep" for the upload day — is then applied
    to those files for you, and the result says what was filed; you do not
    need another call. Offer dates found on the message's other attachments
    as options, plus "就按今天".
    """
    return ""  # never executed: the interrupt turns the call into a widget


_KEEP_WORDS = ("今天", "today", "keep", "上传日", "upload")
_YMD = re.compile(r"(20\d{2})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})")
_MD = re.compile(r"(?<!\d)(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]?")


def parse_date_answer(answer: str, today: Optional[datetime] = None) -> tuple[str, Optional[datetime]]:
    """What a reply to "which date?" means: ("date", dt), ("keep", None) or
    ("unclear", None).

    A full date wins over the keep-words, so "不是今天，是2026-01-06" files
    under the date; "就按今天（2026-09-03）" — the option the model offers —
    reads as keep because the only date in it IS today. A month-day without a
    year is this year's."""
    text = (answer or "").strip()
    today = today or datetime.now()
    m = _YMD.search(text)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return "unclear", None
        if dt.date() == today.date() and any(w in text.lower() for w in _KEEP_WORDS):
            return "keep", None
        return "date", dt
    if any(w in text.lower() for w in _KEEP_WORDS):
        return "keep", None
    m = _MD.search(text)
    if m:
        try:
            return "date", datetime(today.year, int(m.group(1)), int(m.group(2)))
        except ValueError:
            return "unclear", None
    return "unclear", None


async def apply_report_date_answer(user_id: str, file_keys: list[str], answer: str) -> str:
    """File the attachments under the user's answer; return the tool result
    the resumed model reads. Authorization per file, the endpoint's rule."""
    from mirobody.pulse.file_parser.services.file_db_service import FileDbService
    from mirobody.pulse.file_parser.services.report_date import set_file_report_date
    from mirobody.user.care_circle import CareCircleDenied, resolve_subject

    kind, when = parse_date_answer(answer)
    if kind == "unclear":
        return (f"The user replied: {answer!r} — not a date. Nothing was filed; "
                "ask again with a clearer question if the date still matters.")

    lines = []
    for key in file_keys or []:
        row = await FileDbService.get_file_by_key(key)
        if not row:
            lines.append(f"- {key}: no such file in this user's record")
            continue
        owner = str(row.get("query_user_id") or row.get("user_id"))
        if owner != str(user_id):
            try:
                await resolve_subject(user_id, owner, require_write=True)
            except CareCircleDenied:
                lines.append(f"- {key}: no such file in this user's record")
                continue
        try:
            result = await set_file_report_date(owner, key, when)
        except Exception as e:
            logging.error(f"[ask_user] set_file_report_date failed for {key}: {e}", exc_info=True)
            lines.append(f"- {key}: could not be updated")
            continue
        if when is None:
            lines.append(f"- {key}: kept on the upload day")
        else:
            lines.append(
                f"- {key}: {result['moved']} reading(s) filed under {result['report_date'][:10]}"
                + (f", {result['skipped']} already had a reading that day and stayed" if result["skipped"] else "")
            )
    head = (f"The user replied: {answer!r}. "
            + ("Kept the upload day; the user will not be asked again." if when is None
               else f"Applied {when:%Y-%m-%d} (readings still being extracted will be filed under it too)."))
    return head + ("\n" + "\n".join(lines) if lines else "")


def pending_report_date_files(interrupts: Any) -> list[str]:
    """The `report_date_for` of a paused `ask_user`, if it asked about dates."""
    try:
        first = list(interrupts or [])[0]
        value = getattr(first, "value", None) or {}
        requests = value.get("action_requests") or []
        args = (requests[0].get("args") or {}) if requests else {}
        keys = args.get("report_date_for") or []
        return [str(k) for k in keys if str(k).strip()]
    except (IndexError, AttributeError, TypeError):
        return []
