"""Who a read is about: the authenticated caller, and no one else.

There is no parameter naming another person. The chat layer decides whose
record a turn reads and authorises it before the model runs
(`chat/turn.py::_may_chat`); an MCP call reads the account its token or URL
belongs to. A `member` parameter used to let the model name someone else: it
could only guess the id, and 6 of 6 "妈妈的胆固醇" turns measured 2026-09-28
guessed `member="妈妈"` and were refused. Underscore-prefixed so the tool
loader never publishes anything in here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from mirobody.kernel import tools


def caller_of(user_info: Mapping[str, Any] | None) -> str:
    """The authenticated caller's id, or "" when the call carries none."""
    caller_id = user_info.get("user_id") if isinstance(user_info, Mapping) else None
    return caller_id if isinstance(caller_id, str) else ""


def denied(reason: str) -> tools.Envelope:
    return tools.Envelope(
        tools.STATUS_ERROR,
        error_class=tools.ERROR_UNRECOVERABLE,
        error_kind="denied",
        assumptions=(reason,),
    )


def refused(problems) -> tools.Envelope:
    """The structured refusal for arguments the schema does not accept."""
    return tools.invalid_arguments("; ".join(f"{p.parameter}: {p.reason}" for p in problems))
