"""One uploaded file, one report date: the write side of issue #53.

Three callers, one rule: the Data page's "which date?" bar, the
`POST /api/v1/health-indicators/file-date` endpoint behind it, and the agent's
`set_report_date` tool (the chat channel's answer to the same question). They
must agree on what "set the date" means, so the meaning lives here and nowhere
else:

* `when` given: every reading of the file moves to that date, as an
  amendment of the row it replaces (`observations.redate`), and the file row
  records `date_source: manual`. Readings that do not exist YET (extraction
  still running) are not lost: the extractor re-reads the file row before it
  saves and honours a manual date it finds there.
* `when` None, "keep the upload day": nothing about the readings changes, the
  file row gets `date_confirmed` so the question is not asked again.

Authorization is the caller's job (`resolve_subject(..., require_write=True)`
for a proxy upload); this module trusts `owner`.

The read side sits below: `resolve_report_date` decides which date a freshly
extracted file's readings get, and `manual_report_date` is how it learns the
person already answered. They were in `database_services.py`, three static
methods away from the rule they have to agree with.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from mirobody.collect import observations
from mirobody.utils.coerce import get_utc_now, parse_date

from .file_db_service import FileDbService

logger = logging.getLogger(__name__)


def file_source_ref(file_key: str) -> str:
    """The `source_ref` every observation extracted from one file carries."""
    return f"th_files:{file_key}"


async def set_file_report_date(owner: str, file_key: str, when: datetime | None) -> dict:
    """Apply the user's answer. Returns the wire payload the callers relay."""
    if when is None:
        await FileDbService.update_file_content(file_key, {"date_confirmed": True})
        return {"file_key": file_key, "date_confirmed": True}

    tz = await observations.user_tz(str(owner))
    moved, skipped = await observations.redate(str(owner), file_source_ref(file_key), when, user_tz=tz)

    report_date = when.strftime("%Y-%m-%d %H:%M:%S")
    await FileDbService.update_file_content(
        file_key, {"report_date": report_date, "date_source": "manual"}
    )
    # The profile summarises "latest" readings; a re-dated file can change
    # which those are. Coalescing and self-guarded, like the ingest path.
    try:
        from mirobody.task import ProfileRefreshTask
        await ProfileRefreshTask.enqueue(str(owner))
    except Exception as e:
        logger.warning(f"[set_file_report_date] profile refresh not enqueued: {e}")

    return {"file_key": file_key, "report_date": report_date, "moved": moved, "skipped": skipped}


async def get_user_current_time_with_timezone(user_id: str) -> datetime:
    """Get current time in user's timezone, falls back to UTC"""
    try:
        from mirobody.user.user import get_user

        first_record = await get_user(user_id=user_id)
        if not first_record:
            return get_utc_now()

        user_tz = (first_record.get("tz") or "").strip()
        if not user_tz:
            return get_utc_now()

        try:
            return datetime.now(ZoneInfo(user_tz)).replace(tzinfo=None)
        except Exception:
            return get_utc_now()

    except Exception:
        return get_utc_now()

async def resolve_report_date(user_id: str, exam_date: str) -> tuple[datetime, str]:
    """The date a file's readings are filed under, and where it came from.

    Returns `(start_time, date_source)`; `date_source` is "extracted" when
    the document carried a usable date and "upload_time" when the user's
    current time stood in for it. The label is written on every reading
    (comment JSON) and on the file row (th_files.file_content), because
    without it a guessed date is indistinguishable from a real one: a
    report photographed as several screenshots shows its date on the first
    page only, so pages 2..n were filed under "today" and nothing recorded
    that "today" was a fallback (issue #53). The Data page asks about
    "upload_time" files, and `POST /health-indicators/file-date` answers.

    A date the model wrote in a shape `parse_date` does not know counts as
    no date. It used to raise, and the raise threw away every reading on
    the file: an unknown date is a reason to ask, not to drop the data.
    """
    if exam_date and exam_date.strip():
        start_time = parse_date(exam_date)
        if start_time is not None:
            return start_time, "extracted"
        logger.warning(f"Unparseable report date {exam_date!r} for user_id {user_id}; filing under the upload time")
    return await get_user_current_time_with_timezone(user_id), "upload_time"

async def manual_report_date(file_key: str) -> datetime | None:
    """The date the user set on this file, if they set one (`date_source:
    manual` on the th_files row), else None. Read right before readings are
    saved, because the answer can arrive while extraction is still running
    and must not be overwritten by the document's own date."""
    try:
        from .file_db_service import FileDbService

        row = await FileDbService.get_file_by_key(file_key)
        content = (row or {}).get("file_content") or {}
        if content.get("date_source") != "manual":
            return None
        return parse_date(str(content.get("report_date") or ""))
    except Exception as e:
        logger.warning(f"manual_report_date lookup failed for {file_key}: {e}")
        return None
