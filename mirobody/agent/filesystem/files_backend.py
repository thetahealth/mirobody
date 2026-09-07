"""`/uploads/` and `/library/` as read-only projections of `th_files`.

These two mounts used to be COPIES. `_sync_session_uploads` and
`_sync_user_library` ran on every chat turn, selected the authoritative answer
out of `th_files`, and wrote pointer rows into `deep_agent_workspace` — path,
mime, hash, and the extracted text copied column-for-column. The copy bought
nothing on the read path, because the sync queried `th_files` every turn anyway;
what it bought was a second place for the truth to live, and that is what let a
deleted health document keep answering.

So the rows come from `th_files` now, and `is_del = false` is in the query. No
sync, no pointer rows, no deletion to propagate, and no reconciliation pass to
remember to write.

**Only the two fetch methods change.** Everything about how a file is READ —
extracted text for ppt/xlsx, a native file block for PDF on a capable model,
base64 from object storage for images and audio, the lazy first-read extraction,
the multimodal size cap — lives in `PgFilesystemBackend.aread` and is inherited
untouched. That is deliberate: that logic encodes provider quirks (Anthropic
rejecting a `{'type':'file'}` block from a text payload, qwen/deepseek 400ing on
PDF blocks) which are expensive to relearn. The row shape those methods consume
is reproduced exactly, including `encoding` via the same `_is_text_mime` rule
`register_blob` used.

"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Any

from deepagents.backends.protocol import EditResult, FileUploadResponse, WriteResult

from ...utils.db import execute_query
from .backend import PgFilesystemBackend, _is_text_mime
from .naming import guess_mime, safe_basename

logger = logging.getLogger(__name__)

# Kept from the sync functions this replaces, for the same reasons: `ls` returns
# paths and nothing else, so an unbounded history is hundreds of names in one
# tool result.
_MAX_LIBRARY_FILES = 200
_MAX_SESSION_FILES = 50

_READONLY = (
    "This path is a read-only view of your stored files. Write scratch notes to "
    "the workspace root (/) instead."
)


class ThFilesBackend(PgFilesystemBackend):
    """A stored-file scope, sourced from `th_files` instead of a mirror table."""

    def __init__(
        self,
        *,
        user_id: str,
        scope: str,                     # "uploads" | "library"
        file_keys: list[str] | None = None,
        supports_file_block: bool = False,
        turn_names: dict[str, str] | None = None,
    ):
        if scope not in ("uploads", "library"):
            raise ValueError(f"ThFilesBackend scope must be uploads|library, got {scope!r}")
        # session_id is irrelevant to a projection: the row key is the file, and
        # `/uploads/` narrows by the keys this request named rather than by session.
        super().__init__(user_id=user_id, session_id="", scope=scope,
                         supports_file_block=supports_file_block)
        self._keys = [str(k) for k in (file_keys or [])][:_MAX_SESSION_FILES]
        # file_key -> the name THIS request attached the file under. `/uploads/`
        # names a file by this rather than by `th_files.file_name`, which is not
        # stable: the upload pass asks an LLM for a descriptive name and
        # overwrites the column with it (`handlers/base.py::_extract_abstract`),
        # and that write lands DURING the turn, concurrently with the agent. The
        # column is the right name for `/library/`, where it is discovered by
        # `ls`; it is the wrong one here, because `_attachment_reminder` has
        # already told the model the request's name and a rename mid-turn turned
        # that path into `file_not_found` (`ls` had listed it seconds earlier).
        self._turn_names = {str(k): str(v) for k, v in (turn_names or {}).items() if v}

    # ── the projection ───────────────────────────────────────────────────────

    def _row_from_file(self, r: dict[str, Any], path: str) -> dict[str, Any]:
        """Shape one `th_files` row into what the inherited read path expects."""
        name = PurePosixPath(path).name
        mime = r.get("file_type") or guess_mime(name)
        text = str(r.get("original_text") or "")
        return {
            "path": path,
            # `content` is the extracted text: served directly for ppt/xlsx, used
            # for grep on binaries, and the fallback for PDF on text-only models.
            "content": text,
            # Same rule register_blob used, so aread's branches behave identically.
            "encoding": "utf-8" if _is_text_mime(mime, name) else "base64",
            "content_size": int(r.get("text_length") or len(text.encode("utf-8"))),
            "mime_type": mime,
            "scope": self._scope,
            # The bytes still live in object storage; this is the key aread fetches.
            "object_storage_key": r.get("file_key"),
            "content_hash": r.get("content_hash") or "",
            "file_key": r.get("file_key"),
            "source": "user_upload",
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at") or r.get("created_at"),
        }

    async def _files(self) -> list[dict[str, Any]]:
        """Live `th_files` rows for this scope, newest first, de-duped by name."""
        params: dict[str, Any] = {"uid": self._user_id}
        if self._scope == "uploads":
            if not self._keys:
                return []
            placeholders = ", ".join(f":k{i}" for i in range(len(self._keys)))
            for i, k in enumerate(self._keys):
                params[f"k{i}"] = k
            # No text requirement: a file attached to THIS turn is routinely read
            # before the background extraction lands, and aread extracts on
            # demand. The library below does require text, because a row with no
            # text is not yet worth listing in a long history.
            where = f"AND file_key IN ({placeholders})"
            limit = _MAX_SESSION_FILES
        else:
            where = "AND original_text IS NOT NULL AND original_text <> ''"
            if self._keys:
                placeholders = ", ".join(f":x{i}" for i in range(len(self._keys)))
                for i, k in enumerate(self._keys):
                    params[f"x{i}"] = k
                # This request's own attachments belong to /uploads/, not history.
                where += f" AND file_key NOT IN ({placeholders})"
            limit = _MAX_LIBRARY_FILES
        params["limit"] = limit

        try:
            rows = await execute_query(
                query=f"""
                SELECT file_key, decrypt_content(file_name) AS file_name, file_type,
                       content_hash, decrypt_content(original_text) AS original_text,
                       text_length, created_at, updated_at
                  FROM th_files
                 WHERE user_id = :uid AND is_del = false {where}
                 ORDER BY created_at DESC
                 LIMIT :limit
                """,
                params=params,
            )
        except Exception as e:
            logger.warning(f"[{self._scope}-filesystem] th_files query failed for "
                           f"{self._user_id}: {e}", exc_info=True)
            return []

        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for r in rows or []:
            # `/uploads/` names this turn's attachments by the name the request
            # carried; only `/library/` reads the (rewritable) stored name. See
            # `_turn_names` in __init__ for why the column cannot be trusted here.
            pinned = self._turn_names.get(str(r.get("file_key") or ""))
            base = safe_basename(pinned or r.get("file_name") or r.get("file_key") or "")
            if not base:
                continue
            # Newest wins; a repeat name gets its key appended rather than
            # shadowing the older file, matching what the sync did.
            if base in seen:
                key = str(r.get("file_key") or "")
                suffix = safe_basename(key)[:8]
                base = f"{base}__thf_{suffix}" if suffix else base
                if base in seen:
                    continue
            seen.add(base)
            out.append(self._row_from_file(dict(r), f"/{base}"))
        return out

    # ── the two seams the read path goes through ─────────────────────────────

    async def _fetch_row(self, file_path: str) -> dict[str, Any] | None:
        want = "/" + PurePosixPath(str(file_path or "")).name
        for row in await self._files():
            if row["path"] == want:
                return row
        return None

    async def _fetch_rows_under(self, prefix: str) -> list[dict[str, Any]]:
        p = str(prefix or "/")
        rows = await self._files()
        if p in ("", "/"):
            return rows
        return [r for r in rows if r["path"].startswith(p)]

    # ── writes are refused ───────────────────────────────────────────────────

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        return WriteResult(error=_READONLY)

    async def aedit(self, file_path: str, old_string: str, new_string: str,
                    replace_all: bool = False) -> EditResult:
        return EditResult(error=_READONLY)

    async def aupload_files(self, files) -> list[FileUploadResponse]:
        return [FileUploadResponse(path=getattr(f, "path", ""), error=_READONLY)
                for f in (files or [])]
