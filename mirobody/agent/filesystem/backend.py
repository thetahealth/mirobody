"""Rendering half of the agent's filesystem: how a stored file READS.

This module no longer owns any storage. It used to be a Postgres-backed
filesystem — a `deep_agent_workspace` table with four scopes, one of which was
the agent's scratch space and three of which were copies of data another table
already owned. The copies are what let a deleted health document keep answering,
twice: once as an uploaded-file row that outlived `th_files`, once as a health
profile that quoted the readings verbatim.

So sourcing moved out and only rendering stayed. Subclasses supply rows through
`_fetch_row` / `_fetch_rows_under`:

    deep/files_backend.ThFilesBackend   /uploads/ and /library/, over `th_files`
    deep/profile_backend.ProfileBackend /memories/, over the health profile
    deepagents StateBackend             /, the scratch space, checkpointed

What stayed here is the part that is expensive to relearn — how a file becomes
something a model can actually read:

  * text-extractable documents (ppt/pptx/xlsx) serve their extracted text;
  * PDF serves a NATIVE file block on a model that accepts one, and extracted
    text on the ones that answer `{'type':'file'}` with HTTP 400 (qwen, deepseek);
  * images/audio/video serve base64 from object storage, capped, so the
    middleware emits a multimodal block;
  * a document whose text has never been extracted is extracted on first read,
    synchronously, and cached into `th_files.original_text` — the column the
    rest of the system reads, rather than a copy only the agent could see.

Writes are refused. The scratch space that needed them is StateBackend's now.

Targets the deepagents 0.7 ``BackendProtocol``: the async methods return the
``LsResult`` / ``ReadResult`` / ``WriteResult`` / ``EditResult`` / ``GlobResult``
/ ``GrepResult`` dataclasses (never the deprecated ``*_info`` / ``grep_raw`` /
``aread -> str`` shims, which 0.7.0 deleted outright).
"""

from __future__ import annotations

import base64
import fnmatch
import logging
import re
from datetime import datetime, UTC
from pathlib import PurePosixPath
from typing import Any, Literal

from deepagents.backends.protocol import (
    BackendProtocol,
    EditResult,
    FILE_NOT_FOUND,
    FileDownloadResponse,
    FileInfo,
    GlobResult,
    GrepMatch,
    GrepResult,
    INVALID_PATH,
    LsResult,
    ReadResult,
    WriteResult,
)

from ...utils.db import execute_query
from .coercion import coerce_to_int
from .naming import MULTIMODAL_EXTS

logger = logging.getLogger(__name__)

_READONLY = (
    "This path is a read-only view of stored data. Write scratch notes to "
    "the workspace root (/) instead."
)

# 256 KB cap — larger payloads (even utf-8 text) go to object storage to avoid
# bloating PG rows / index pages.
_DEFAULT_READ_LIMIT = 2000
# Cap for serving raw bytes back as base64 for a multimodal read. Beyond this we
# return an error instead of base64-bombing the model context.
_MAX_MULTIMODAL_BYTES = 24 * 1024 * 1024

Scope = Literal["uploads", "library"]
Source = Literal["agent_write", "agent_upload", "user_upload", "tool_generated"]
# `workspace` and `memory` were the two scopes this class stored itself.
# They are graph state and a profile projection now, so a stored-file
# subclass is all that is left to validate.
_VALID_SCOPES = ("uploads", "library")


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
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
        scope: Scope = "library",
        supports_file_block: bool = False,
    ) -> None:
        if not user_id:
            raise ValueError("PgFilesystemBackend requires a non-empty user_id")
        if scope not in _VALID_SCOPES:
            raise ValueError(f"unknown scope: {scope!r}")
        self._user_id = str(user_id)
        self._session_id = str(session_id or "")
        self._scope: Scope = scope
        # When True, the bound model accepts a native `{'type': 'file'}` content
        # block (Claude / Gemini …), so pdf/ppt are served as raw base64 bytes
        # (preserving tables/figures/layout) rather than flattened to extracted
        # text. When False (qwen/deepseek/… reject file blocks), they read as
        # extracted text — extracted on that first read. See aread's
        # _TEXT_DOC_EXTS branch.
        self._supports_file_block = bool(supports_file_block)

    @property
    def user_id(self) -> str:
        return self._user_id

    @property
    def scope(self) -> str:
        return self._scope

    # ─── helpers ────────────────────────────────────────────────────────

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
    def _row_to_file_info(row: dict[str, Any]) -> FileInfo:
        info: FileInfo = {"path": str(row.get("path", ""))}
        if row.get("content_size") is not None:
            info["size"] = int(row["content_size"])
        if row.get("updated_at") is not None:
            info["modified_at"] = _iso(row["updated_at"])
        return info

    async def _fetch_row(self, file_path: str) -> dict[str, Any] | None:
        """One row, shaped for the read path below. Supplied by the subclass.

        This used to be a SELECT against an agent-filesystem table. There is no
        such table: every mount is either graph state or a projection of the
        table that owns the data (see deep/files_backend.py). Row SOURCING is the
        subclass's job; everything from `als` down is shared.
        """
        raise NotImplementedError("subclass must supply rows")

    async def _fetch_rows_under(self, prefix: str) -> list[dict[str, Any]]:
        """Rows under a prefix, shaped for the read path. Supplied by the subclass."""
        raise NotImplementedError("subclass must supply rows")

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
        ``content`` is empty.

        This is where OCR actually happens. Registration deliberately does not
        extract (see ``parser.FileParser.prepare``), so for any document the
        model has not opened before, the first ``read_file`` lands here.

        Three sources, cheapest first:
          1. the ``th_files`` parse cache by ``file_key`` — the upload pipeline's
             own parse may have completed since this row was registered;
          2. the raw bytes in object storage, deduplicated by SHA256 inside
             ``extract_text`` — bytes extracted before never pay twice;
          3. failing both, a real extraction (Vision LLM for scanned pages).

        On success the text is written back to `th_files` so subsequent
        reads are instant. Returns the extracted text, or ``None`` if nothing
        could be produced.
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
                    await self._persist_inline_text(str(file_key), text)
                    return text

            if oss_key:
                raw = await self._get_from_storage(str(oss_key))
                if raw:
                    text = await parser.extract_text(raw, name)
                    if text.strip():
                        await self._persist_inline_text(str(file_key), text)
                        return text
        except Exception as e:
            logger.warning(f"lazy doc extract failed for {file_path}: {e}")
        return None

    async def _persist_inline_text(self, file_key: str, text: str) -> None:
        """Cache extracted text where the rest of the system can see it.

        It used to be written back into the agent's own copy of the file, which
        meant the Files tab, the library listing and every other reader stayed
        ignorant of an extraction the agent had already paid for. `th_files`
        owns the file, so `original_text` is the cache — and the read path picks
        it up on the next turn through the ordinary projection.

        Best-effort: an extraction that cannot be cached is still returned to the
        caller, it just costs again next time.
        """
        if not file_key:
            return
        try:
            await execute_query(
                query="""
                UPDATE th_files
                   SET original_text = encrypt_content(:text),
                       text_length = :size,
                       updated_at = NOW()
                 WHERE user_id = :user_id AND file_key = :file_key AND is_del = false
                """,
                params={"user_id": self._user_id, "file_key": file_key,
                        "text": text, "size": len(text.encode("utf-8"))},
            )
        except Exception as e:
            logger.warning(f"persist extracted text failed for {file_key}: {e}")

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
        #    reject `{'type':'file'}` with HTTP 400): serve the extracted text,
        #    extracting it now if this is the file's first read. We drop these
        #    extensions from deepagents' multimodal
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
        """Refused. This class renders read-only projections.

        It used to own a write path — classify the payload, offload large bytes,
        upsert a row. That path existed to back the agent's scratch space, which
        is now deepagents' checkpointed `StateBackend`, so nothing writes here any
        more and a stub that silently did nothing would be worse than a refusal.
        """
        return WriteResult(error=_READONLY)

    async def aedit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
    ) -> EditResult:
        """Refused — see `awrite`."""
        return EditResult(error=_READONLY)

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
            # `encoding` describes the BYTES, not the text beside them. Skipping
            # base64 rows here made grep useless for exactly the files it matters
            # for: a PDF is `base64` (so read can serve a native file block) and
            # its extracted text sits in `content` — which the schema comment
            # called "extracted/greppable text" while this line dropped it.
            # grep searches text wherever there is text.
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


_TEXT_DOC_EXTS = {".pdf", ".ppt", ".pptx", ".xlsx", ".xls", ".xlsm", ".xlsb"}


def _is_text_mime(mime_type: str | None, path: str = "") -> bool:
    """True if the file should be read as inline text (not a multimodal blob)."""
    ext = PurePosixPath(path).suffix.lower() if path else ""
    if ext in MULTIMODAL_EXTS:
        return False
    if mime_type:
        m = mime_type.lower()
        if m.startswith("text/"):
            return True
        if m in ("application/json", "application/xml") or m.endswith(("+json", "+xml")):
            return True
        if m.startswith(("image/", "audio/", "video/", "application/pdf")):
            return False
    # Default: treat as text (most agent-written files are text).
    return True


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


# Document types we reliably extract text from at upload time. They are stored
# as base64 (object-storage offload) but READ as their extracted text: most
# providers (qwen/DashScope, deepseek, ...) reject a ``{'type': 'file'}``
# content block and only images go through the native vision path. Serving the
# extracted text makes PDF/PPT/Excel chat work across every model.
#
# Excel (.xlsx/.xls/...) matters here: no provider accepts a spreadsheet as a
# native file block, and its bytes are a ZIP the model cannot decode. Without
# this entry ``aread`` would serve raw base64 and the model could not parse it.
# The parser turns the workbook into a markdown table
# (``_extract_excel_original_text``) on first read, which is what we serve.


def _patch_deepagents_multimodal_exts() -> None:
    try:
        from deepagents.backends import utils as _da_utils
        for _ext in _TEXT_DOC_EXTS:
            _da_utils._EXTENSION_TO_FILE_TYPE.pop(_ext, None)
    except Exception as _e:  # pragma: no cover - defensive
        logger.warning(f"could not patch deepagents extension map: {_e}")


_patch_deepagents_multimodal_exts()


_patch_deepagents_multimodal_exts()
