"""Profile-refresh task.

Producer (pulse): `await ProfileRefreshTask.enqueue(user_id)` on ingest.
Consumer (holywell backend_job): drains a burst, dedupes user_ids, and calls
`UserProfileService.create_user_profile` for each unique user.

Weak consistency with IndicatorSyncTask: profile may occasionally refresh
before the latest dim sync completes and read slightly stale dim data — the
next signal for that user cleans it up.
"""

from __future__ import annotations

import logging

from .base import BaseRedisTask

#-----------------------------------------------------------------------------

class ProfileRefreshTask(BaseRedisTask):
    """Queue payload: ``user_id`` (plain string)."""

    queue_key = "profile_refresh_queue"

    async def consume(self, messages: list[str]) -> None:
        # Lazy import breaks mirobody.task ↔ mirobody.chat/pulse cycle.
        from ..chat.user_profile import UserProfileService

        user_ids = {m for m in messages if m}
        if not user_ids:
            return

        logging.info(f"profile_refresh batch start: {len(user_ids)} user(s)")
        for uid in user_ids:
            try:
                result = await UserProfileService.create_user_profile(uid)
            except Exception as e:
                logging.error(f"profile_refresh crashed: user_id={uid}: {e}", exc_info=True)
                continue

            status = result.get("status")
            if status == "success":
                logging.info(
                    f"profile_refresh done: user_id={uid}, status=success, "
                    f"profile_id={result.get('profile_id')}, "
                    f"version={result.get('version')}, "
                    f"last_execute_doc_id={result.get('last_execute_doc_id')}"
                )
            elif status == "no_incremental_data":
                logging.info(
                    f"profile_refresh skipped: user_id={uid}, status=no_incremental_data, "
                    f"current_version={result.get('current_version')}, "
                    f"last_execute_doc_id={result.get('last_execute_doc_id')}"
                )
            elif status == "error":
                logging.error(
                    f"profile_refresh error: user_id={uid}, "
                    f"message={result.get('message')}"
                )
            else:
                logging.warning(
                    f"profile_refresh unexpected status: user_id={uid}, result={result}"
                )
        logging.info(f"profile_refresh batch done: {len(user_ids)} user(s)")

#-----------------------------------------------------------------------------
