"""One uploaded file, one report date — the write side of issue #53.

Three callers, one rule: the Data page's "which date?" bar, the
`POST /api/v1/health-indicators/file-date` endpoint behind it, and the agent's
`set_report_date` tool (the chat channel's answer to the same question). They
must agree on what "set the date" means, so the meaning lives here and nowhere
else:

* `when` given — every reading of the file moves to that date and is labelled
  `date_source: manual`; the file row records the same. Readings that do not
  exist YET (extraction still running) are not lost: the extractor re-reads the
  file row before it saves and honours a manual date it finds there.
* `when` None — "keep the upload day": nothing about the readings changes, the
  file row gets `date_confirmed` so the question is not asked again.

Authorization is the caller's job (`resolve_subject(..., require_write=True)`
for a proxy upload); this module trusts `owner`.
"""

from __future__ import annotations

import logging
from datetime import datetime

from mirobody.utils import execute_query

from .file_db_service import FileDbService

logger = logging.getLogger(__name__)

# The comment column is encrypted JSON; only rows that are JSON get the
# provenance merged in, anything else (an empty comment from a failed build)
# starts fresh. `||` on jsonb replaces the key.
_MOVE_SQL = """
UPDATE th_series_data t
   SET start_time = :when, end_time = :when, update_time = CURRENT_TIMESTAMP,
       comment = encrypt_content((
           CASE WHEN left(decrypt_content(t.comment), 1) = '{'
                THEN decrypt_content(t.comment)::jsonb
                ELSE '{}'::jsonb END
           || '{"date_source": "manual"}'::jsonb)::text)
 WHERE t.user_id = :uid AND t.source_table = 'th_files'
   AND t.source_table_id = :file_key AND t.deleted = 0
   AND NOT EXISTS (
       SELECT 1 FROM th_series_data o
        WHERE o.user_id = t.user_id AND o.indicator = t.indicator
          AND o.start_time = :when AND o.end_time = :when AND o.id <> t.id)
RETURNING t.id
"""

# The unique (user, indicator, start, end) key counts soft-deleted rows too, so
# a reading the user deleted (a re-uploaded report, a removed file) would block
# the live one from taking its date — found in the clean-slate walk: 18 of 21
# readings "already had a reading that day" that was the deleted copy of
# themselves. Trash that stands in the way is removed for good; live rows are
# never touched here.
_CLEAR_TRASH_SQL = """
DELETE FROM th_series_data o
 WHERE o.user_id = :uid AND o.deleted = 1
   AND o.start_time = :when AND o.end_time = :when
   AND o.indicator IN (
       SELECT t.indicator FROM th_series_data t
        WHERE t.user_id = :uid AND t.source_table = 'th_files'
          AND t.source_table_id = :file_key AND t.deleted = 0)
"""

# Readings of the file still sitting on another date after the move: the
# ones the unique (user, indicator, start, end) key kept where they were.
_LEFT_SQL = """
SELECT COUNT(*) AS n FROM th_series_data
 WHERE user_id = :uid AND source_table = 'th_files'
   AND source_table_id = :file_key AND deleted = 0
   AND (start_time <> :when OR end_time <> :when)
"""


async def set_file_report_date(owner: str, file_key: str, when: datetime | None) -> dict:
    """Apply the user's answer. Returns the wire payload the callers relay."""
    if when is None:
        await FileDbService.update_file_content(file_key, {"date_confirmed": True})
        return {"file_key": file_key, "date_confirmed": True}

    params = {"uid": str(owner), "file_key": file_key, "when": when}
    await execute_query(_CLEAR_TRASH_SQL, params)
    moved = await execute_query(_MOVE_SQL, params) or []
    left = await execute_query(_LEFT_SQL, params) or []
    skipped = int(left[0]["n"]) if left else 0

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

    return {"file_key": file_key, "report_date": report_date, "moved": len(moved), "skipped": skipped}
