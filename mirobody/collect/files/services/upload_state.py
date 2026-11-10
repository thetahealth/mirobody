"""Whether an upload the Data page sent is still wanted (`th_upload`).

The WebSocket upload files its rows only after the whole batch is processed,
which with a slow model is minutes. A delete in that window matched no row,
answered success with nothing deleted, and the upload was filed anyway as a
second copy of the report (#240). The delete now cancels the upload here and
the filing reads the mark: before inserting, so a cancelled upload files
nothing, and once more after inserting, so a cancel that landed while the
rows went in removes them.

The mark is a row, not a set in this process, because the delete is an HTTP
request that a second replica, or this one after a reload, can answer.
"""

from __future__ import annotations

import logging

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)

UPLOADING = "uploading"
CANCELLED = "cancelled"

#: An upload whose process died never settles its row. A day is far past the
#: longest upload (`FileDbService.ROW_WAIT_SECONDS` is ten minutes), and such
#: a row is harmless meanwhile: cancelling it cancels an upload that will
#: never file.
_STALE = "1 day"


async def register(user_id: str, message_id: str) -> None:
    """Record that `user_id` is sending the upload `message_id`. A database
    error is logged and the upload goes on: without the row it cannot be
    cancelled, which is how every upload behaved before."""
    try:
        await execute_query(f"DELETE FROM th_upload WHERE created_at < now() - interval '{_STALE}'",
                            log_sql=False)
        await execute_query(
            """INSERT INTO th_upload (user_id, message_id, state) VALUES (:user_id, :message_id, :state)
               ON CONFLICT (user_id, message_id) DO UPDATE SET state = EXCLUDED.state, created_at = now()""",
            {"user_id": str(user_id), "message_id": str(message_id), "state": UPLOADING}, log_sql=False)
    except Exception as e:
        logger.warning("upload not registered, it cannot be cancelled: message_id=%s error_type=%s", message_id,
                       type(e).__name__, exc_info=not is_driver_exception(e))


async def cancel(user_id: str, message_id: str) -> bool:
    """Cancel `user_id`'s upload `message_id` if it has not been filed yet;
    whether there was one to cancel."""
    rows = await execute_query(
        """UPDATE th_upload SET state = :cancelled
            WHERE user_id = :user_id AND message_id = :message_id AND state = :uploading
        RETURNING message_id""",
        {"user_id": str(user_id), "message_id": str(message_id), "cancelled": CANCELLED, "uploading": UPLOADING},
        log_sql=False)
    return bool(rows)


async def cancelled(user_id: str, message_id: str) -> bool:
    """Whether the upload was cancelled; False on a database error, so an
    unreadable mark files the upload as it always was."""
    try:
        rows = await execute_query(
            "SELECT 1 FROM th_upload WHERE user_id = :user_id AND message_id = :message_id AND state = :cancelled",
            {"user_id": str(user_id), "message_id": str(message_id), "cancelled": CANCELLED}, log_sql=False)
    except Exception as e:
        logger.warning("upload state not read, filed as usual: message_id=%s error_type=%s", message_id,
                       type(e).__name__, exc_info=not is_driver_exception(e))
        return False
    return bool(rows)


async def settle(user_id: str, message_id: str) -> bool:
    """Remove the upload's row now that its files are filed; True when it was
    cancelled in the meantime, so the caller deletes what it just filed. The
    row goes in the same statement that reads it: a cancel either lands
    before (and is returned here) or finds no upload and deletes the filed
    rows the way any delete does."""
    try:
        rows = await execute_query(
            "DELETE FROM th_upload WHERE user_id = :user_id AND message_id = :message_id RETURNING state",
            {"user_id": str(user_id), "message_id": str(message_id)}, log_sql=False)
    except Exception as e:
        logger.warning("upload state not settled: message_id=%s error_type=%s", message_id, type(e).__name__,
                       exc_info=not is_driver_exception(e))
        return False
    return bool(rows) and rows[0].get("state") == CANCELLED
