"""Who a read is about — shared by every tool that reads one person's record.

A care-circle member is resolved through the authorisation check, never
through a trusted parameter: the model supplies `member`, and a model can be
told to supply anything. Underscore-prefixed so the tool loader never
publishes anything in here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ...kernel import query, tools


def caller_of(user_info: Mapping[str, Any] | None) -> str:
    """The authenticated caller's id, or "" when the call carries none."""
    caller_id = user_info.get("user_id") if isinstance(user_info, Mapping) else None
    return caller_id if isinstance(caller_id, str) else ""


async def subject_for(caller_id: str, member: str) -> str:
    """`member` empty (or the caller) means the caller; anyone else goes
    through the care-circle check and raises `query.Denied` when it fails.

    `.subject_id` is not decoration. `resolve_subject` answers with a
    `Subject`, and this used to `str()` the whole dataclass — which has no
    `__str__`, so the "user id" was the repr
    `Subject(operator_id=7, subject_id=42, access=1)`. `th_series_data.user_id`
    is `varchar(200)`, so that bound without error and matched nothing: an
    authorised care-circle read answered "no data", on both the agent and the
    MCP surface, indistinguishably from a member who really has none.
    """
    if not member or member == caller_id:
        return caller_id
    from ...user.care_circle import CareCircleDenied, resolve_subject

    try:
        return str((await resolve_subject(caller_id, member)).subject_id)
    except CareCircleDenied as e:
        raise query.Denied(str(member)) from e


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
