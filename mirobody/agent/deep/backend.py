"""PostgreSQL-backed deepagents BackendProtocol with object-storage offload.

Scope-based virtual filesystem over a single ``deep_agent_workspace`` table.
The agent's ``CompositeBackend`` (see ``deep_agent._build_backend``) mounts one
``PgFilesystemBackend`` per scope; ``scope`` is part of the row key so the mounts
stay isolated even though they collapse onto only two ``session_id`` values:

    default      -> PgFilesystemBackend(user_id, session_id, scope='workspace')
    /memories/   -> PgFilesystemBackend(user_id, '',         scope='memory')
    /uploads/    -> PgFilesystemBackend(user_id, session_id, scope='uploads')   (read-only)
    /library/    -> PgFilesystemBackend(user_id, '',         scope='library')   (read-only)

Storage tier is decided per write in ``_classify_and_store``:

  * **Inline**: utf-8 payload <= ``INLINE_LIMIT_BYTES`` (256 KB). ``content``
    carries the text; ``object_storage_key`` is NULL.
  * **Offload**: large utf-8 text or any binary payload. Raw bytes go to object
    storage via the project's ``AbstractStorage`` (S3 / Aliyun OSS / local);
    ``object_storage_key`` points at them. ``content`` may carry an extracted
    text representation so ``read``/``grep`` stay useful without round-trips.

This targets the deepagents 0.7 ``BackendProtocol``: the async methods return the
``LsResult`` / ``ReadResult`` / ``WriteResult`` / ``EditResult`` / ``GlobResult``
/ ``GrepResult`` dataclasses (never the deprecated ``*_info`` / ``grep_raw`` /
``aread -> str`` shims, which 0.7.0 deleted outright).

``delete``/``adelete`` is deliberately NOT implemented. It is optional in the
protocol, and that is the wanted posture here: ``/uploads/`` and ``/library/``
are read-only projections of ``th_files``, ``/memories/`` is cross-session state
that belongs to out-of-band consolidation rather than to a single turn, and the
one genuinely disposable scope (``workspace``) is already session-scoped, so
rows do not accumulate within a conversation. Reclaiming old rows is a
background-GC concern (nothing sets ``deleted = 1`` today), not something to
hand the model.

Leaving the method unimplemented is NOT by itself enough to keep the ``delete``
tool away from the model, and the reason is easy to get wrong: deepagents probes
capability with ``_supports_delete(backend)`` against the MOUNTED backend, and
``CompositeBackend`` — which is what DeepAgent mounts — *does* implement
``delete``, routing it per path. So the probe passes, the tool is offered, and
each call comes back as the composite's "delete unsupported" error only after the
model has spent tokens choosing it. The tool is therefore hidden by name via the
harness profile's ``excluded_tools`` (see ``deep_agent._EXCLUDED_NATIVE_TOOLS``);
this module simply never grows a delete path.

Modeled on ``openvital/server/openvital/agents/deep/backend/pg_filesystem.py``.
"""

from __future__ import annotations

import base64
import fnmatch
import hashlib
import logging
import mimetypes
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Literal, Optional

from deepagents.backends.protocol import (
    BackendProtocol,
    EditResult,
    FILE_NOT_FOUND,
    FileDownloadResponse,
    FileInfo,
    FileUploadResponse,
    GlobResult,
    GrepMatch,
    GrepResult,
    INVALID_PATH,
    LsResult,
    ReadResult,
    WriteResult,
)

from ...utils.db import execute_query
from ..utils.coercion import coerce_to_bool, coerce_to_int
from .filetype import MULTIMODAL_EXTS

logger = logging.getLogger(__name__)

# 256 KB cap — larger payloads (even utf-8 text) go to object storage to avoid
# bloating PG rows / index pages.
INLINE_LIMIT_BYTES = 256 * 1024
_DEFAULT_READ_LIMIT = 2000
_OSS_KEY_PREFIX = "agent-workspace"
_TABLE = "deep_agent_workspace"
# Cap for serving raw bytes back as base64 for a multimodal read. Beyond this we
# return an error instead of base64-bombing the model context.
_MAX_MULTIMODAL_BYTES = 24 * 1024 * 1024

Scope = Literal["workspace", "memory", "uploads", "library"]
Source = Literal["agent_write", "agent_upload", "user_upload", "tool_generated"]
_VALID_SCOPES = ("workspace", "memory", "uploads", "library")


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value or "")


class PgFilesystemBackend(BackendProtocol):
    """Postgres-backed ``BackendProtocol`` with object-storage offload.

    Constructed per request (deepagents rebuilds the graph each turn). All DB
    work goes through the project's async ``execute_query``; large/binary bytes
    are offloaded through the shared ``AbstractStorage`` client.
    """

    def __init__(
        self,
        *,
        user_id: str,
        session_id: str = "",
        scope: Scope = "workspace",
        inline_limit: int = INLINE_LIMIT_BYTES,
        supports_file_block: bool = False,
    ) -> None:
        if not user_id:
            raise ValueError("PgFilesystemBackend requires a non-empty user_id")
        if scope not in _VALID_SCOPES:
            raise ValueError(f"unknown scope: {scope!r}")
        self._user_id = str(user_id)
        self._session_id = str(session_id or "")
        self._scope: Scope = scope
        self._inline_limit = int(inline_limit)
        # When True, the bound model accepts a native `{'type': 'file'}` content
        # block (Claude / Gemini …), so pdf/ppt are served as raw base64 bytes
        # (preserving tables/figures/layout) rather than flattened to extracted
        # text. When False (qwen/deepseek/… reject file blocks), they read as the
        # upload-time extracted text. See aread's _TEXT_DOC_EXTS branch.
        self._supports_file_block = bool(supports_file_block)

    @property
    def user_id(self) -> str:
        return self._user_id

    @property
    def scope(self) -> str:
        return self._scope

    # ─── helpers ────────────────────────────────────────────────────────

    def _scope_params(self) -> dict[str, Any]:
        return {
            "user_id": self._user_id,
            "session_id": self._session_id,
            "scope": self._scope,
        }

    @staticmethod
    def _validate_path(file_path: Any) -> str | None:
        """Mirror deepagents ``FilesystemBackend(virtual_mode=True)`` semantics.

        Rejects non-string / empty / non-absolute paths plus ``..`` / ``~`` /
        ``//`` so a routed backend can't be traversed out of its mount. Returns
        the ``INVALID_PATH`` warning constant on rejection (never raises).
        """
        if not isinstance(file_path, str) or not file_path:
            return INVALID_PATH
        if not file_path.startswith("/"):
            return INVALID_PATH
        try:
            parts = PurePosixPath(file_path.replace("\\", "/")).parts
        except Exception:
            return INVALID_PATH
        if ".." in parts or "~" in parts:
            return INVALID_PATH
        if "//" in file_path:
            return INVALID_PATH
        return None

    @staticmethod
    def _sha256(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def _oss_key_for(self, file_path: str, content_hash: str) -> str:
        clean = file_path.lstrip("/")
        slug = self._session_id or "shared"
        return f"{_OSS_KEY_PREFIX}/{self._user_id}/{self._scope}/{slug}/{content_hash[:8]}/{clean}"

    @staticmethod
    def _row_to_file_info(row: dict[str, Any]) -> FileInfo:
        info: FileInfo = {"path": str(row.get("path", ""))}
        if row.get("content_size") is not None:
            info["size"] = int(row["content_size"])
        if row.get("updated_at") is not None:
            info["modified_at"] = _iso(row["updated_at"])
        return info

    def _content_read_expr(self) -> str:
        # Memory scope is encrypted at rest; decrypt on read. Other scopes plaintext.
        return "decrypt_content(content) AS content" if self._scope == "memory" else "content"

    def _content_write_value(self) -> str:
        return "encrypt_content(:content)" if self._scope == "memory" else ":content"

    async def _fetch_row(self, file_path: str) -> dict[str, Any] | None:
        rows = await execute_query(
            query=f"""
            SELECT path, {self._content_read_expr()}, encoding, content_size, mime_type,
                   scope, object_storage_key, content_hash, file_key, source,
                   created_at, updated_at
            FROM {_TABLE}
            WHERE user_id = :user_id
              AND session_id = :session_id
              AND scope = :scope
              AND path = :path
              AND deleted = 0
            LIMIT 1
            """,
            params={**self._scope_params(), "path": file_path},
        )
        if isinstance(rows, list) and rows:
            return dict(rows[0])
        return None

    async def _fetch_rows_under(self, prefix: str) -> list[dict[str, Any]]:
        escaped = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        rows = await execute_query(
            query=rf"""
            SELECT path, {self._content_read_expr()}, encoding, content_size, mime_type,
                   scope, object_storage_key, content_hash, file_key, source,
                   created_at, updated_at
            FROM {_TABLE}
            WHERE user_id = :user_id
              AND session_id = :session_id
              AND scope = :scope
              AND path LIKE :prefix || '%' ESCAPE '\'
              AND deleted = 0
            ORDER BY path ASC
            """,
            params={**self._scope_params(), "prefix": escaped},
        )
        return [dict(r) for r in rows] if isinstance(rows, list) else []

    async def _upsert(
        self,
        *,
        path: str,
        content: str,
        encoding: str,
        content_size: int,
        mime_type: str | None,
        object_storage_key: str | None,
        content_hash: str | None,
        source: Source,
        file_key: str | None = None,
    ) -> None:
        await execute_query(
            query=f"""
            INSERT INTO {_TABLE}
                (user_id, session_id, scope, path, content, encoding, content_size,
                 mime_type, object_storage_key, content_hash, file_key, source,
                 created_at, updated_at, deleted)
            VALUES
                (:user_id, :session_id, :scope, :path, {self._content_write_value()}, :encoding, :size,
                 :mime, :oss_key, :hash, :file_key, :source,
                 NOW(), NOW(), 0)
            ON CONFLICT (user_id, session_id, scope, path) DO UPDATE
                SET content = EXCLUDED.content,
                    encoding = EXCLUDED.encoding,
                    content_size = EXCLUDED.content_size,
                    mime_type = EXCLUDED.mime_type,
                    object_storage_key = EXCLUDED.object_storage_key,
                    content_hash = EXCLUDED.content_hash,
                    file_key = COALESCE(EXCLUDED.file_key, {_TABLE}.file_key),
                    source = EXCLUDED.source,
                    updated_at = NOW(),
                    deleted = 0
            """,
            params={
                **self._scope_params(),
                "path": path,
                "content": content,
                "encoding": encoding,
                "size": int(content_size),
                "mime": mime_type,
                "oss_key": object_storage_key,
                "hash": content_hash,
                "file_key": file_key,
                "source": source,
            },
        )

    # ─── object storage (AbstractStorage: S3 / Aliyun OSS / local) ───────

    async def _put_to_storage(self, key: str, payload: bytes, mime_type: str) -> bool:
        try:
            from ...utils.config.storage.factory import get_storage_client
            storage = get_storage_client()
            _url, err = await storage.put(key, payload, content_type=mime_type)
            if err:
                logger.error("storage put failed for %s: %s", key, err)
                return False
            return True
        except Exception:
            logger.exception("storage put failed for %s", key)
            return False

    async def _get_from_storage(self, key: str) -> bytes | None:
        try:
            from ...utils.config.storage.factory import get_storage_client
            content, err = await get_storage_client().get(key)
            if err:
                logger.error("storage get failed for %s: %s", key, err)
                return None
            return content
        except Exception:
            logger.exception("storage get failed for %s", key)
            return None

    async def _lazy_extract_doc_text(
        self, row: dict[str, Any], file_path: str
    ) -> str | None:
        """On-demand text extraction for a text-document (pdf/ppt) whose inline
        ``content`` is still empty (upload-time parse not finished, or the file
        was registered by reference).

        Two sources, cheapest first:
          1. the ``th_files`` parse cache by ``file_key`` — the asynchronous
             upload parse may have completed since this row was registered;
          2. the raw bytes in object storage — extract synchronously.

        On success the text is written back to the workspace row so subsequent
        reads are instant. Returns the extracted text, or ``None`` if nothing
        could be produced yet.
        """
        file_key = row.get("file_key")
        oss_key = row.get("object_storage_key")
        name = PurePosixPath(file_path).name
        try:
            from .parser import FileParser
            parser = FileParser()

            if file_key:
                text = await parser.get_cached_file_by_key(str(file_key))
                if text:
                    await self._persist_inline_text(file_path, text)
                    return text

            if oss_key:
                raw = await self._get_from_storage(str(oss_key))
                if raw:
                    prepared = await parser.prepare(raw, name)
                    if prepared.parsed_text and prepared.parsed_text.strip():
                        await self._persist_inline_text(file_path, prepared.parsed_text)
                        return prepared.parsed_text
        except Exception as e:
            logger.warning(f"lazy doc extract failed for {file_path}: {e}")
        return None

    async def _persist_inline_text(self, path: str, text: str) -> None:
        """Cache extracted text back into the workspace row's ``content`` column
        so later reads skip re-extraction. Best-effort — never raises."""
        try:
            await execute_query(
                query=f"""
                UPDATE {_TABLE}
                SET content = :content, content_size = :size, updated_at = NOW()
                WHERE user_id = :user_id AND session_id = :session_id
                  AND scope = :scope AND path = :path
                """,
                params={**self._scope_params(), "path": path,
                        "content": text, "size": len(text.encode("utf-8"))},
            )
        except Exception as e:
            logger.warning(f"persist inline text failed for {path}: {e}")

    async def _classify_and_store(
        self,
        *,
        path: str,
        payload: bytes,
        mime_type: str | None,
        source: Source,
        parsed_text_override: str | None = None,
        file_key: str | None = None,
    ) -> str | None:
        """Pick inline vs offload, write the row, return an error string or None."""
        content_hash = self._sha256(payload)
        size = len(payload)

        try:
            decoded_text: str | None = payload.decode("utf-8")
        except UnicodeDecodeError:
            decoded_text = None

        is_inline_text = decoded_text is not None and size <= self._inline_limit

        if is_inline_text:
            await self._upsert(
                path=path,
                content=decoded_text or "",
                encoding="utf-8",
                content_size=size,
                mime_type=mime_type or "text/plain",
                object_storage_key=None,
                content_hash=content_hash,
                source=source,
                file_key=file_key,
            )
            return None

        oss_key = self._oss_key_for(path, content_hash)
        ok = await self._put_to_storage(oss_key, payload, mime_type or "application/octet-stream")
        if not ok:
            return "object_storage_write_failed"

        await self._upsert(
            path=path,
            content=parsed_text_override or "",
            encoding="base64" if decoded_text is None else "utf-8",
            content_size=size,
            mime_type=mime_type or "application/octet-stream",
            object_storage_key=oss_key,
            content_hash=content_hash,
            source=source,
            file_key=file_key,
        )
        return None

    # ─── ls ──────────────────────────────────────────────────────────────

    async def als(self, path: str) -> LsResult:
        path = path or "/"  # qwen may send None instead of the default
        err = self._validate_path(path)
        if err:
            return LsResult(error=err)
        directory = path if path.endswith("/") or path == "/" else path + "/"
        rows = await self._fetch_rows_under(directory)

        seen: dict[str, FileInfo] = {}
        for row in rows:
            full = str(row.get("path", ""))
            tail = full[len(directory):] if directory != "/" else full[1:]
            if not tail:
                continue
            first, sep, _rest = tail.partition("/")
            if not first:
                continue
            child_path = directory + first if directory != "/" else "/" + first
            if sep:
                if child_path not in seen:
                    seen[child_path] = {"path": child_path, "is_dir": True}
            else:
                seen[child_path] = self._row_to_file_info(row)
        return LsResult(entries=sorted(seen.values(), key=lambda e: e["path"]))

    # ─── read ──────────────────────────────────────────────────────────

    async def aread(
        self, file_path: str, offset: int = 0, limit: int = _DEFAULT_READ_LIMIT
    ) -> ReadResult:
        err = self._validate_path(file_path)
        if err:
            return ReadResult(error=err)
        # LLMs frequently send offset/limit as strings or None — coerce so the
        # slice below never raises a TypeError.
        offset = coerce_to_int(offset, 0)
        limit = coerce_to_int(limit, _DEFAULT_READ_LIMIT)

        row = await self._fetch_row(file_path)
        if not row:
            return ReadResult(error=FILE_NOT_FOUND)

        encoding = str(row.get("encoding") or "utf-8")
        oss_key = row.get("object_storage_key")
        inline_text = str(row.get("content") or "")
        created = _iso(row.get("created_at"))
        modified = _iso(row.get("updated_at"))

        # Text-extractable documents (pdf/ppt/pptx/excel): rendering is capability-aware.
        #  * PDF on a file-block-capable model (Claude/Gemini/GPT/…): fall through
        #    to the base64 branch below so the model gets the NATIVE file block —
        #    preserving tables, figures, layout, scanned pages (what vision models
        #    are best at, and what matters for lab reports / scanned medical docs).
        #  * everything else here (ppt/pptx and Excel — no provider accepts these
        #    as file blocks — and PDF on text-only models like qwen/deepseek that
        #    reject `{'type':'file'}` with HTTP 400): serve the upload-time
        #    extracted text. We drop these extensions from deepagents' multimodal
        #    map (module-level patch below) so a text payload renders as a plain
        #    text block while a base64 payload still falls back to a "file" block.
        ext = PurePosixPath(file_path).suffix.lower()
        serve_native_pdf = ext == ".pdf" and self._supports_file_block
        if ext in _TEXT_DOC_EXTS and not serve_native_pdf:
            content = inline_text
            if not content.strip():
                # Inline text not present (the file was registered by reference
                # before its text was extracted). Extract on-demand NOW, fetching
                # the bytes from object storage and running the parser
                # synchronously — so the read returns the text in this call rather
                # than asking the model to poll.
                content = await self._lazy_extract_doc_text(row, file_path) or ""
            if not content.strip():
                # Synchronous extraction produced nothing — retrying would not
                # help (it is not a timing issue). Report a clear, terminal
                # outcome so the model stops re-reading and tells the user.
                name = PurePosixPath(file_path).name
                return ReadResult(
                    file_data={"content": (
                        f"[\"{name}\" could not be read as text — text extraction "
                        f"returned nothing (it may be an empty, corrupt, or "
                        f"unsupported document). Do not retry read_file on it; "
                        f"tell the user the file could not be processed.]"),
                        "encoding": "utf-8",
                        "created_at": created, "modified_at": modified}
                )
            if offset or limit != _DEFAULT_READ_LIMIT:
                lines = content.splitlines()
                sliced = lines[offset: offset + limit] if limit else lines[offset:]
                content = "\n".join(sliced)
            return ReadResult(
                file_data={"content": content, "encoding": "utf-8",
                           "created_at": created, "modified_at": modified}
            )

        # Binary / multimodal file: serve the raw bytes as base64 so the
        # deepagents read_file tool emits a multimodal content block (image /
        # audio / video). The `content` column holds extracted text for
        # grep only — never returned here, or the middleware would ship text as
        # base64. Offset/limit are ignored for binary (pagination is text-only).
        if encoding == "base64":
            if oss_key:
                raw = await self._get_from_storage(str(oss_key))
                if raw is None:
                    return ReadResult(error="object_storage_read_failed")
                if len(raw) > _MAX_MULTIMODAL_BYTES:
                    return ReadResult(
                        error=(
                            f"file too large to read inline "
                            f"({len(raw)} bytes > {_MAX_MULTIMODAL_BYTES}); "
                            f"download or process it with a dedicated tool"
                        )
                    )
                b64 = base64.b64encode(raw).decode("ascii")
            else:
                # Binary is NEVER stored inline in Postgres — it always lives in
                # object storage. A base64 row without an object_storage_key is a
                # corrupt/legacy record.
                return ReadResult(error="binary content unavailable (missing object_storage_key)")
            return ReadResult(
                file_data={"content": b64, "encoding": "base64",
                           "created_at": created, "modified_at": modified}
            )

        # utf-8 text: large text may be offloaded with empty inline content.
        if oss_key and not inline_text:
            raw = await self._get_from_storage(str(oss_key))
            if raw is None:
                return ReadResult(error="object_storage_read_failed")
            content = raw.decode("utf-8", errors="replace")
        else:
            content = inline_text

        if offset or limit != _DEFAULT_READ_LIMIT:
            lines = content.splitlines()
            sliced = lines[offset: offset + limit] if limit else lines[offset:]
            content = "\n".join(sliced)

        return ReadResult(
            file_data={"content": content, "encoding": "utf-8",
                       "created_at": created, "modified_at": modified}
        )

    # ─── write (create-only, text) ──────────────────────────────────────

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        err = self._validate_path(file_path)
        if err:
            return WriteResult(error=err)
        existing = await self._fetch_row(file_path)
        if existing is not None:
            return WriteResult(
                error=(
                    f"File already exists at {file_path!r}; use edit to modify, "
                    f"or delete first."
                )
            )
        payload = str(content if content is not None else "").encode("utf-8")
        err_str = await self._classify_and_store(
            path=file_path,
            payload=payload,
            mime_type="text/plain",
            source="agent_write",
        )
        if err_str:
            return WriteResult(error=err_str)
        return WriteResult(path=file_path)

    # ─── edit ────────────────────────────────────────────────────────────

    async def aedit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        err = self._validate_path(file_path)
        if err:
            return EditResult(error=err)
        replace_all = coerce_to_bool(replace_all, False)
        old_string = "" if old_string is None else str(old_string)
        new_string = "" if new_string is None else str(new_string)
        if old_string == new_string:
            return EditResult(error="new_string must differ from old_string")
        if old_string == "":
            return EditResult(error="old_string must be non-empty")

        row = await self._fetch_row(file_path)
        if not row:
            return EditResult(error=FILE_NOT_FOUND)

        encoding = str(row.get("encoding") or "utf-8")
        if encoding != "utf-8":
            return EditResult(error="edit is only supported on utf-8 files")
        if row.get("object_storage_key") and not str(row.get("content") or ""):
            return EditResult(
                error="edit is not supported on object-storage-offloaded files; rewrite via write_file instead"
            )

        original = str(row.get("content") or "")
        occurrences = original.count(old_string)
        if occurrences == 0:
            return EditResult(error=f"old_string not found in {file_path!r}")
        if occurrences > 1 and not replace_all:
            return EditResult(
                error=(
                    f"old_string is not unique in {file_path!r} ({occurrences} occurrences). "
                    f"Re-call with replace_all=True or pass a longer, unique old_string."
                )
            )

        new_content = (
            original.replace(old_string, new_string)
            if replace_all
            else original.replace(old_string, new_string, 1)
        )
        err_str = await self._classify_and_store(
            path=file_path,
            payload=new_content.encode("utf-8"),
            mime_type=str(row.get("mime_type") or "text/plain"),
            source=str(row.get("source") or "agent_write"),  # preserve provenance
            file_key=row.get("file_key"),
        )
        if err_str:
            return EditResult(error=err_str)
        return EditResult(path=file_path, occurrences=occurrences if replace_all else 1)

    # ─── glob ────────────────────────────────────────────────────────────

    async def aglob(self, pattern: str, path: str | None = None) -> GlobResult:
        # `None` is the protocol default as of deepagents 0.7 (it used to be
        # "/"), and qwen has always been capable of sending it explicitly —
        # both collapse to the scope root here.
        path = path or "/"
        err = self._validate_path(path)
        if err:
            return GlobResult(error=err)
        # qwen may send pattern as None/empty — nothing to match, return empty.
        if not isinstance(pattern, str) or not pattern:
            return GlobResult(matches=[])
        base = path if path.endswith("/") or path == "/" else path + "/"
        rows = await self._fetch_rows_under(base)

        compiled = _compile_glob(pattern, base=base)
        matched: list[FileInfo] = [
            self._row_to_file_info(row)
            for row in rows
            if compiled.match(str(row.get("path", "")))
        ]
        return GlobResult(matches=matched)

    # ─── grep ────────────────────────────────────────────────────────────

    async def agrep(
        self,
        pattern: str,
        path: str | None = None,
        glob: str | None = None,
        *,
        max_count: int | None = None,
    ) -> GrepResult:
        """Search inlined utf-8 ``content`` under ``path``.

        ``max_count`` (deepagents 0.7) is a TOTAL cap on returned matches, not a
        per-file one. Honouring it matters more here than on a single-mount
        backend: ``CompositeBackend`` splits the caller's budget across routes
        and stops asking once it is spent, so a mount that ignored the cap would
        swallow the whole allowance and starve the mounts queried after it.
        ``truncated`` reports that matches were dropped — exactly ``max_count``
        matches with none dropped is complete, not truncated.
        """
        scope_path = path or "/"
        err = self._validate_path(scope_path)
        if err:
            return GrepResult(error=err)
        base = scope_path if scope_path.endswith("/") or scope_path == "/" else scope_path + "/"
        rows = await self._fetch_rows_under(base)

        needle = str(pattern or "")
        if not needle:
            return GrepResult(matches=[])
        try:
            regex = re.compile(needle, re.IGNORECASE)
        except re.error:
            regex = None  # fall back to substring search

        compiled_glob = _compile_glob(glob, base=base) if glob else None
        # A cap of 0 asks for nothing; anything negative is meaningless. Treat
        # both as "no matches wanted" rather than as "unlimited".
        capped = max_count is not None
        if capped and max_count <= 0:
            return GrepResult(matches=[], truncated=True)
        matches: list[GrepMatch] = []
        truncated = False
        for row in rows:
            if truncated:
                break
            full = str(row.get("path", ""))
            if compiled_glob is not None and not compiled_glob.match(full):
                continue
            if str(row.get("encoding") or "utf-8") != "utf-8":
                continue
            content = str(row.get("content") or "")
            if not content:
                continue
            for line_no, line in enumerate(content.splitlines(), start=1):
                hit = regex.search(line) if regex is not None else (needle in line)
                if not hit:
                    continue
                if capped and len(matches) >= max_count:
                    # Stop at the cap and say so: there was at least one more
                    # match we are not returning.
                    truncated = True
                    break
                matches.append({"path": full, "line": line_no, "text": line})
        return GrepResult(matches=matches, truncated=truncated)

    # ─── upload / download ──────────────────────────────────────────────

    async def aupload_files(
        self, files: list[tuple[str, bytes]]
    ) -> list[FileUploadResponse]:
        responses: list[FileUploadResponse] = []
        for file_path, payload in files or []:
            err = self._validate_path(file_path)
            if err:
                responses.append(FileUploadResponse(path=file_path, error=err))
                continue
            try:
                if not isinstance(payload, (bytes, bytearray)):
                    payload = str(payload or "").encode("utf-8")
                err_str = await self._classify_and_store(
                    path=file_path,
                    payload=bytes(payload),
                    mime_type=_guess_mime(file_path),
                    source="agent_upload",
                )
                responses.append(FileUploadResponse(path=file_path, error=err_str))
            except Exception as exc:
                logger.exception("aupload_files failed for %s", file_path)
                responses.append(FileUploadResponse(path=file_path, error=str(exc)))
        return responses

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        responses: list[FileDownloadResponse] = []
        for file_path in paths or []:
            err = self._validate_path(file_path)
            if err:
                responses.append(FileDownloadResponse(path=file_path, content=None, error=err))
                continue
            row = await self._fetch_row(file_path)
            if not row:
                responses.append(FileDownloadResponse(path=file_path, content=None, error=FILE_NOT_FOUND))
                continue
            oss_key = row.get("object_storage_key")
            if oss_key:
                raw = await self._get_from_storage(str(oss_key))
                if raw is None:
                    responses.append(FileDownloadResponse(path=file_path, content=None, error="object_storage_read_failed"))
                    continue
                payload = raw
            else:
                payload = str(row.get("content") or "").encode("utf-8")
            responses.append(FileDownloadResponse(path=file_path, content=payload, error=None))
        return responses

    # ─── audit / registration (non-protocol public surface) ─────────────

    async def aupload_parsed(
        self,
        *,
        path: str,
        raw_bytes: bytes | None,
        parsed_text: str | None,
        mime_type: str | None = None,
        file_key: str | None = None,
        source: Source = "user_upload",
    ) -> str | None:
        """Store an uploaded file: raw bytes offloaded, parsed text kept inline.

        Lets ``read_file``/``grep`` see the extracted text immediately while the
        original bytes remain downloadable. If ``raw_bytes`` is None, only the
        parsed text is stored (as an inline utf-8 file).
        """
        err = self._validate_path(path)
        if err:
            return err
        if raw_bytes is None:
            payload = (parsed_text or "").encode("utf-8")
            return await self._classify_and_store(
                path=path, payload=payload, mime_type=mime_type or "text/plain",
                source=source, file_key=file_key,
            )
        return await self._classify_and_store(
            path=path,
            payload=bytes(raw_bytes),
            mime_type=mime_type or _guess_mime(path),
            source=source,
            parsed_text_override=parsed_text,
            file_key=file_key,
        )

    async def register_blob(
        self,
        *,
        path: str,
        object_storage_key: str,
        content_hash: str = "",
        content_size: int = 0,
        mime_type: str | None = None,
        parsed_text: str | None = None,
        file_key: str | None = None,
        source: Source = "user_upload",
    ) -> str | None:
        """Register an existing object-storage object as a workspace row.

        Used to mirror user-uploaded files (already in storage via ``th_files``)
        into ``/uploads/`` and ``/library/`` WITHOUT duplicating bytes. The agent
        then reads the parsed text inline and downloads raw bytes on demand.

        Security: ``object_storage_key`` is stored verbatim and ``adownload_files``
        will fetch it. The CALLER must verify the key belongs to ``self._user_id``
        (join through ``th_files``) before calling.
        """
        err = self._validate_path(path)
        if err:
            return err
        # Text files are read inline (parsed_text). Binary/multimodal files
        # (pdf/image/audio/video, …) must be encoding='base64' so aread fetches
        # the OSS bytes and the middleware emits a multimodal block; parsed_text
        # stays in `content` for grep only.
        encoding = "utf-8" if _is_text_mime(mime_type, path) else "base64"
        try:
            await self._upsert(
                path=path,
                content=parsed_text or "",
                encoding=encoding,
                content_size=coerce_to_int(content_size, 0),
                mime_type=mime_type,
                object_storage_key=str(object_storage_key),
                content_hash=str(content_hash or ""),
                source=source,
                file_key=file_key,
            )
            return None
        except Exception as exc:
            logger.exception("register_blob failed for %s", path)
            return str(exc)


# Backwards-compatible alias: existing imports reference ``PostgresBackend``.
PostgresBackend = PgFilesystemBackend


def create_postgres_backend(
    session_id: str,
    user_id: str,
    scope: Scope = "workspace",
    **_legacy_kwargs: Any,
) -> PgFilesystemBackend:
    """Create a single-scope PgFilesystemBackend.

    Used by the file-upload path (``agents/utils/file.py`` → ``handle_file_upload``)
    and ad-hoc callers. The deep agent itself builds a multi-scope
    ``CompositeBackend`` directly in ``deep_agent._build_backend``.
    ``**_legacy_kwargs`` swallows retired parameters (``file_parser``,
    ``cache_ttl``, ``cache_maxsize``, ``sandbox_backend``) so existing call
    sites keep working.
    """
    return PgFilesystemBackend(user_id=user_id, session_id=session_id, scope=scope)


# ─── glob compiler ──────────────────────────────────────────────────────


def _compile_glob(pattern: str | None, *, base: str) -> re.Pattern[str]:
    base_prefix = "" if base == "/" else base.rstrip("/")
    if not pattern:
        pattern = "**/*"

    regex_parts: list[str] = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if pattern[i: i + 2] == "**":
            regex_parts.append(".*")
            i += 2
            if i < len(pattern) and pattern[i] == "/":
                i += 1
        elif ch == "*":
            regex_parts.append("[^/]*")
            i += 1
        elif ch == "?":
            regex_parts.append("[^/]")
            i += 1
        elif ch == ".":
            regex_parts.append(r"\.")
            i += 1
        elif ch in r"+()|^$":
            regex_parts.append(re.escape(ch))
            i += 1
        elif ch == "[":
            end = pattern.find("]", i + 1)
            if end == -1:
                regex_parts.append(re.escape(ch))
                i += 1
            else:
                regex_parts.append(pattern[i: end + 1])
                i = end + 1
        else:
            regex_parts.append(re.escape(ch))
            i += 1

    body = "".join(regex_parts)
    full = f"^{re.escape(base_prefix)}/?{body}$"
    try:
        return re.compile(full)
    except re.error:
        return re.compile(fnmatch.translate(base_prefix + "/" + (pattern or "")))


def _guess_mime(file_path: str) -> str | None:
    mime, _ = mimetypes.guess_type(file_path)
    return mime


# Document types we reliably extract text from at upload time. They are stored
# as base64 (object-storage offload) but READ as their extracted text: most
# providers (qwen/DashScope, deepseek, ...) reject a ``{'type': 'file'}``
# content block and only images go through the native vision path. Serving the
# extracted text makes PDF/PPT/Excel chat work across every model.
#
# Excel (.xlsx/.xls/...) matters here: no provider accepts a spreadsheet as a
# native file block, and its bytes are a ZIP the model cannot decode. Without
# this entry ``aread`` would serve raw base64 and the model could not parse it.
# The upload-time parser turns the workbook into a markdown table
# (``_extract_excel_original_text``), which is what we serve instead.
_TEXT_DOC_EXTS = {".pdf", ".ppt", ".pptx", ".xlsx", ".xls", ".xlsm", ".xlsb"}


# deepagents' read_file middleware picks the multimodal content-block type purely
# by file extension via ``backends.utils._EXTENSION_TO_FILE_TYPE`` (it maps
# pdf/ppt/pptx -> "file"). A "file" type forces a ``{'type': 'file'}`` block on
# read regardless of the encoding our backend declares, and most providers
# (qwen/DashScope, deepseek, ...) reject file blocks with HTTP 400. Since we
# always extract pdf/ppt text at upload and serve it as text from ``aread``,
# drop these extensions from the map so the tool renders a plain text block.
# Mutating the dict in place (not rebinding) keeps every importer in sync.
def _patch_deepagents_multimodal_exts() -> None:
    try:
        from deepagents.backends import utils as _da_utils
        for _ext in _TEXT_DOC_EXTS:
            _da_utils._EXTENSION_TO_FILE_TYPE.pop(_ext, None)
    except Exception as _e:  # pragma: no cover - defensive
        logger.warning(f"could not patch deepagents extension map: {_e}")


_patch_deepagents_multimodal_exts()


def _is_text_mime(mime_type: str | None, path: str = "") -> bool:
    """True if the file should be read as inline text (not a multimodal blob)."""
    ext = PurePosixPath(path).suffix.lower() if path else ""
    if ext in MULTIMODAL_EXTS:
        return False
    if mime_type:
        m = mime_type.lower()
        if m.startswith("text/"):
            return True
        if m in ("application/json", "application/xml") or m.endswith("+json") or m.endswith("+xml"):
            return True
        if m.startswith(("image/", "audio/", "video/", "application/pdf")):
            return False
    # Default: treat as text (most agent-written files are text).
    return True
