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
read on demand.
"""

from __future__ import annotations

import fnmatch
import logging

from deepagents.backends.protocol import (
    BackendProtocol,
    EditResult,
    FileData,
    FileInfo,
    FileDownloadResponse,
    GlobResult,
    GrepMatch,
    GrepResult,
    LsResult,
    ReadResult,
    WriteResult,
)
from deepagents.backends.utils import create_file_data, slice_read_response

from ...utils.db import execute_query

logger = logging.getLogger(__name__)

PROFILE_FILENAME = "health_profile.md"

_READONLY = (
    "/memories/ is a read-only view of the health profile, which is written out of "
    "band by the profile-refresh pass. Write scratch notes to the workspace root (/)."
)


class ProfileBackend(BackendProtocol):
    """The user's health profile, served as one file under `/memories/`."""

    def __init__(self, *, user_id: str):
        if not user_id:
            raise ValueError("ProfileBackend requires a non-empty user_id")
        self.user_id = str(user_id)

    # ── the projection ───────────────────────────────────────────────────────

    async def _document(self) -> str:
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
            logger.warning(f"[profile-fs] profile unavailable for {self.user_id}: {e}", exc_info=True)
            return ""
        if not rows:
            return ""
        return str(rows[0].get("common_part") or "").strip()

    @staticmethod
    def _info(size: int = 0) -> FileInfo:
        return {"path": f"/{PROFILE_FILENAME}", "is_dir": False, "size": size}  # type: ignore[return-value]

    # ── reads ────────────────────────────────────────────────────────────────

    async def als(self, path: str) -> LsResult:
        doc = await self._document()
        # An empty profile lists NOTHING rather than an empty file: otherwise the
        # model sees a memory file and reads it expecting content.
        return LsResult(entries=[self._info(len(doc))] if doc else [])

    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        name = (file_path or "").lstrip("/")
        if name and name != PROFILE_FILENAME:
            return ReadResult(error=f"Entry '{file_path}' not found under /memories.")
        doc = await self._document()
        if not doc:
            return ReadResult(error="No health profile has been built for this user yet.")
        fd = create_file_data(doc)
        n_lines = len(doc.splitlines())
        if offset and offset >= n_lines:
            return ReadResult(file_data=FileData(
                content=f"(end of profile — {n_lines} line(s) total.)",
                encoding=fd.get("encoding", "utf-8")))
        sliced = slice_read_response(fd, offset, limit)
        if isinstance(sliced, ReadResult):
            return sliced
        return ReadResult(file_data=FileData(content=sliced, encoding=fd.get("encoding", "utf-8")))

    async def aglob(self, pattern: str, path: str | None = None) -> GlobResult:
        doc = await self._document()
        if not doc:
            return GlobResult(matches=[])
        pat = (pattern or "").lstrip("/")
        hit = fnmatch.fnmatch(PROFILE_FILENAME, pat) or fnmatch.fnmatch(f"/{PROFILE_FILENAME}", pattern or "")
        return GlobResult(matches=[self._info(len(doc))] if hit else [])

    async def agrep(self, pattern: str, path: str | None = None, glob: str | None = None) -> GrepResult:
        doc = await self._document()
        if not doc:
            return GrepResult(matches=[])
        matches = [
            GrepMatch(path=f"/{PROFILE_FILENAME}", line_number=i, line=line[:500])
            for i, line in enumerate(doc.splitlines(), start=1)
            if pattern in line
        ]
        return GrepResult(matches=matches)

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        doc = await self._document()
        out: list[FileDownloadResponse] = []
        for p in paths or []:
            name = (p or "").lstrip("/")
            if name and name != PROFILE_FILENAME:
                out.append(FileDownloadResponse(path=p, error=f"Entry '{p}' not found."))
            elif not doc:
                out.append(FileDownloadResponse(path=p, error="No health profile yet."))
            else:
                out.append(FileDownloadResponse(path=p, content=doc.encode("utf-8"),
                                               mime_type="text/markdown"))
        return out

    # ── writes are refused ───────────────────────────────────────────────────

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        return WriteResult(error=_READONLY)

    async def aedit(self, file_path: str, old_string: str, new_string: str,
                    replace_all: bool = False) -> EditResult:
        return EditResult(error=_READONLY)
