"""The chat channel's "which date?": one agent-only tool (issue #53).

`ask_user` is a deepagents human-in-the-loop interrupt: the tool body never
runs. When the model calls it, `HumanInTheLoopMiddleware` pauses the graph
after the model step, `MirobodyAgent._stream_agent_response` turns the pending
call into an `interrupt` block (question + options) and the turn ends; the user's
next message resumes the SAME thread as the tool's result
(`Command(resume={"decisions": [{"type": "respond", ...}]})`). The thread is
the LangGraph checkpointer (`checkpointer.py`), so nothing else has to
remember that a question is open.

When the question is the examination date of attachments, the model names
them in `report_date_for` and the ANSWER IS APPLIED HERE, before the model
sees it: the date is parsed out of the reply (an option tapped, or free text
like "2026年1月6日" / "就按今天"), filed through the same rule the Data page
bar and `POST /health-indicators/file-date` use (`services/report_date.py`),
and the tool result tells the model what happened. One tool, one round trip.
The tool is handed to the agent directly, it is NOT in the MCP tool
directory: an external MCP client has no widget to answer with.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from langchain_core.messages import BaseMessage
from langchain_core.tools import tool

from mirobody.kernel.ops import is_driver_exception

from .wire.blocks import INTERRUPT

logger = logging.getLogger(__name__)

#: The `interrupt_on` entry that makes `ask_user` pause the run. "respond" is
#: the only decision that makes sense for a question: the answer IS the result.
ASK_USER_INTERRUPT = {"ask_user": {"allowed_decisions": ["respond"]}}


@tool
def ask_user(question: str, options: list[str] | None = None,
             report_date_for: list[str] | None = None) -> str:
    """Ask the user ONE short question and WAIT for the answer, never guess.

    Use it when only the user knows something you need before acting
    correctly. `options` (2-4 short strings) renders as one-tap buttons; the
    user may still type. The reply is returned as this tool's result.

    Asking which date a report is from: pass the attachments' file_keys (from
    the attachment note) in `report_date_for`. The answer: a date such as
    "2026-01-06", or "就按今天" / "keep" for the upload day: is then applied
    to those files for you, and the result says what was filed; you do not
    need another call. Offer dates found on the message's other attachments
    as options, plus "就按今天".
    """
    return ""  # never executed: the interrupt turns the call into an `interrupt` block


_KEEP_WORDS = ("今天", "today", "keep", "上传日", "upload")
#: A keep-word said to be NOT the answer: "不是今天，是上周三", "not today".
_NEGATED_KEEP = re.compile(r"(不是|不按|不要|别按|并非|\bnot|n't)\s*(?:the\s+)?(今天|today|keep|上传日|upload)")
_YMD = re.compile(r"(20\d{2})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})")
_MD = re.compile(r"(?<!\d)(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]?")


def parse_date_answer(answer: str, today: datetime | None = None) -> tuple[str, datetime | None]:
    """What a reply to "which date?" means: ("date", dt), ("keep", None) or
    ("unclear", None).

    A full date wins over the keep-words, so "不是今天，是2026-01-06" files
    under the date; "就按今天（2026-09-03）" (the option the model offers)
    reads as keep because the only date in it IS today. A keep-word that is
    negated ("不是今天，是上周三") is not a keep: with no date the answer is
    unclear, where it used to file under the upload day. A month-day without
    a year is this year's."""
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
    lowered = text.lower()
    if not _NEGATED_KEEP.search(lowered) and any(w in lowered for w in _KEEP_WORDS):
        return "keep", None
    m = _MD.search(text)
    if m:
        try:
            return "date", datetime(today.year, int(m.group(1)), int(m.group(2)))
        except ValueError:
            return "unclear", None
    return "unclear", None


async def apply_report_date_answer(subject_id: str, file_keys: list[str], answer: str, *,
                                   may_write: bool) -> str:
    """File the attachments under the user's answer; return the tool result
    the resumed model reads.

    Only files of `subject_id`'s record, the one this turn is about, and only
    when the asker may change it (`may_write`, resolved per turn by the chat
    layer): the grant `POST /health-indicators/file-date` asks for. Checking
    the subject's own grant instead let a read-only care-circle member
    redate someone else's readings."""
    from mirobody.collect import FileDbService
    from mirobody.collect import set_file_report_date

    kind, when = parse_date_answer(answer)
    if kind == "unclear":
        return (f"The user replied: {answer!r} — not a date. Nothing was filed; "
                "ask again with a clearer question if the date still matters.")
    if not may_write:
        return (f"The user replied: {answer!r}. Nothing was filed: this account may read "
                "this record but not change it.")

    lines = []
    for key in file_keys or []:
        row = await FileDbService.get_file_by_key(key)
        owner = str(row.get("query_user_id") or row.get("user_id")) if row else ""
        if owner != str(subject_id):
            lines.append(f"- {key}: no such file in this user's record")
            continue
        try:
            result = await set_file_report_date(owner, key, when)
        except Exception as e:
            logger.error("[ask_user] filing a report date failed: error_type=%s", type(e).__name__,
                         exc_info=not is_driver_exception(e))
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


def interrupt_block(interrupts: Any) -> dict[str, Any] | None:
    """The `interrupt` block for a pending `ask_user` call, or None.

    LangGraph's own shape: the paused call under its own name and arguments,
    rather than a `widget_type` / `config` frame invented here. A client
    renders `args.question` with `args.options` as one-tap buttons. Only
    `ask_user` can pause this graph (this module), so the first pending action
    is the one.
    """
    try:
        first = list(interrupts or [])[0]
        value = getattr(first, "value", None) or {}
        requests = value.get("action_requests") or []
        args = (requests[0].get("args") or {}) if requests else {}
    except (IndexError, AttributeError, TypeError):
        return None
    question = str(args.get("question") or "").strip()
    if not question:
        return None
    return {
        "type": INTERRUPT,
        "interrupt_id": getattr(first, "id", "") or "",
        "name": "ask_user",
        "args": {
            "question": question,
            "options": [str(o) for o in (args.get("options") or []) if str(o).strip()],
        },
    }


async def pending_answer(agent: Any, config: dict, messages: Any, user_id: str = "", *,
                         may_write: bool = False) -> str | None:
    """The user's message as the answer to an open `ask_user`, if one is open.

    A thread paused on an interrupt has a next node to run and an interrupt on
    its pending task. The client sends the answer as an ordinary chat message
    (typed, or an option tapped in the widget), so nothing in the request says
    "this is a resume": the checkpointer's state does.

    When the open question asked which date attachments are from
    (`report_date_for`), the answer is applied here, to `user_id`'s record
    and only when `may_write`, and what comes back is the tool result the
    model reads: the filing already done, no second tool call (this module).
    """
    if not config.get("configurable", {}).get("thread_id"):
        return None
    try:
        state = await agent.aget_state(config)
    except Exception as e:
        logger.debug("no thread state to resume: error_type=%s", type(e).__name__)
        return None
    if not getattr(state, "next", None):
        return None
    interrupts = None
    for t in (getattr(state, "tasks", None) or ()):
        if getattr(t, "interrupts", None):
            interrupts = t.interrupts
            break
    if not interrupts:
        return None
    last = messages[-1] if messages else None
    content = getattr(last, "content", None) if isinstance(last, BaseMessage) else (last or {}).get("content")
    if isinstance(content, list):
        content = " ".join(str(part.get("text", "")) if isinstance(part, dict) else str(part) for part in content)
    answer = str(content or "").strip()
    if not answer:
        return None
    file_keys = pending_report_date_files(interrupts)
    if file_keys and user_id:
        return await apply_report_date_answer(str(user_id), file_keys, answer, may_write=may_write)
    return answer
