"""Profile-refresh task.

Producer (pulse): `await profile_refresh.add_task(user_id)` on ingest.
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

    async def process_task(self, messages: list[str]) -> None:
        # Lazy import breaks mirobody.task ↔ mirobody.chat/pulse cycle.
        from ..chat.user_profile import UserProfileService

        user_ids = {m for m in messages if m}
        if not user_ids:
            return

        logging.info(f"profile_refresh batch start: {len(user_ids)} user(s)")
        for uid in user_ids:
            try:
                result = await UserProfileService.create_user_profile(uid)
                logging.info(
                    f"profile_refresh done: user_id={uid}, "
                    f"status={result.get('status')}"
                )
            except Exception as e:
                logging.warning(f"profile_refresh failed for user_id={uid}: {e}")
        logging.info(f"profile_refresh batch done: {len(user_ids)} user(s)")

#-----------------------------------------------------------------------------
