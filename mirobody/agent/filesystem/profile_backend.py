"""`/memories/` as a read-only PROJECTION of the health profile, not a copy.

This replaces a `PgFilesystemBackend(scope='memory')` mount that
`user_profile.mirror_profile_to_memories` wrote a second copy into. The copy is
what made a deleted health document keep answering: the profile quotes readings
verbatim ("GLU 7.5 mmol/L, FBG 7.45, PBG 9.5, HbA1c 7.2% ..."), it carried no
`file_key`, and so neither the file delete nor the reading cascade reached it.
Two fixes were needed to chase one stale copy; a projection needs none, because

    ... WHERE user_id = :user_id AND is_deleted = false

is the whole deletion contract. When the source row is invalidated the file
stops existing, in the same query, with nothing to remember to propagate.

One file, read-only. `get_health_profile_core` still injects the bounded core
section into the system prompt; this serves the FULL document for the agent to
read on demand. The eight protocol methods are `DocumentBackend`'s; this class
is the query.
"""

from __future__ import annotations

import logging

from ...utils.db import execute_query
from .document_backend import DocumentBackend

logger = logging.getLogger(__name__)

PROFILE_FILENAME = "health_profile.md"

_READONLY = (
    "/memories/ is a read-only view of the health profile, which is written out of "
    "band by the profile-refresh pass. Write scratch notes to the workspace root (/)."
)


class ProfileBackend(DocumentBackend):
    """The user's health profile, served as one file under `/memories/`."""

    filename = PROFILE_FILENAME
    readonly_message = _READONLY

    def __init__(self, *, user_id: str):
        if not user_id:
            raise ValueError("ProfileBackend requires a non-empty user_id")
        super().__init__()
        self.user_id = str(user_id)

    # ── the projection ───────────────────────────────────────────────────────

    async def document(self) -> str:
        """The latest live profile, decrypted. Empty string when there is none.

        `is_deleted = false` is load-bearing: it is what makes invalidating the
        profile row remove the agent's view of it, with no cascade to write.
        """
        try:
            rows = await execute_query(
                query="""
                SELECT decrypt_content(common_part_encrypted) AS common_part
                  FROM health_user_profile_by_system
                 WHERE user_id = :user_id AND is_deleted = false
                 ORDER BY version DESC
                 LIMIT 1
                """,
                params={"user_id": self.user_id},
            )
        except Exception as e:
            # Loud: every turn mounts this, and a silent failure looks to the
            # model like a user about whom nothing is known.
            logger.warning(f"[profile-filesystem] profile unavailable for {self.user_id}: {e}", exc_info=True)
            return ""
        if not rows:
            return ""
        return str(rows[0].get("common_part") or "").strip()
