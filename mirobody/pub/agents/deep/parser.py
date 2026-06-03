"""File preparation for the DeepAgent virtual filesystem.

Two responsibilities, both at *upload time* (the agent reads files through the
deepagents-native ``read_file`` tool, not through this module):

1. **Classification** — given a filename/mime, decide whether the file is a
   *multimodal* type (image / audio / video / pdf / ...). Multimodal files are
   stored as raw bytes (object-storage offload) and surfaced to the model as
   multimodal content blocks by the deepagents filesystem middleware. See
   https://docs.langchain.com/oss/python/deepagents/harness#virtual-filesystem-access
   and https://docs.langchain.com/oss/python/langchain/messages#multimodal

2. **Text extraction** — best-effort text for the ``content`` column so that
   ``grep`` works and non-multimodal models still get *some* signal. Reuses the
   shared ``FileAbstractExtractor`` and the ``th_files`` parse cache.

The resulting (raw_bytes, parsed_text, mime_type) triple is handed to
``PgFilesystemBackend.aupload_parsed`` which offloads the bytes and keeps the
text inline.
"""

import hashlib
import logging
import mimetypes
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, BinaryIO, Dict, Optional, Union

from ....pulse.file_parser.services.file_abstract_extractor import FileAbstractExtractor
from ....utils.config import safe_read_cfg
from ....utils.db import execute_query

logger = logging.getLogger(__name__)


# Extensions deepagents surfaces as multimodal content blocks. Kept in sync with
# PgFilesystemBackend._MULTIMODAL_EXTS and the harness docs.
MULTIMODAL_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif",
    ".mp4", ".mpeg", ".mov", ".avi", ".flv", ".mpg", ".webm", ".wmv", ".3gpp",
    ".wav", ".mp3", ".aiff", ".aac", ".ogg", ".flac",
    ".pdf", ".ppt", ".pptx",
}

# Multimodal extensions whose text we still try to extract (so grep + non-pdf
# models get content). Pure media (image/audio/video) have no extractable text.
_TEXT_EXTRACTABLE_MULTIMODAL = {".pdf", ".ppt", ".pptx"}

_MIME_BY_EXT = {
    ".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg", ".gif": "image/gif", ".webp": "image/webp",
    ".txt": "text/plain", ".md": "text/markdown", ".csv": "text/csv",
    ".json": "application/json", ".xml": "application/xml", ".html": "text/html",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
}


def calculate_content_hash(file_bytes: bytes) -> str:
    """SHA256 hex digest of file content — used as cache lookup key."""
    return hashlib.sha256(file_bytes).hexdigest()


def guess_mime(filename: str) -> str:
    ext = PurePosixPath(filename or "").suffix.lower()
    if ext in _MIME_BY_EXT:
        return _MIME_BY_EXT[ext]
    return mimetypes.guess_type(filename or "")[0] or "application/octet-stream"


def is_multimodal(filename: str) -> bool:
    return PurePosixPath(filename or "").suffix.lower() in MULTIMODAL_EXTS


@dataclass
class PreparedFile:
    """Result of preparing an uploaded file for the workspace."""
    raw_bytes: Optional[bytes]
    parsed_text: str
    mime_type: str
    is_multimodal: bool
    content_hash: str


class FileParser:
    """Extract text from uploaded files, with a ``th_files`` parse cache."""

    def __init__(self):
        self.file_abstract_extractor = FileAbstractExtractor()
        self.cache_enabled = safe_read_cfg("GLOBAL_FILE_CACHE_ENABLED", True)
        if isinstance(self.cache_enabled, str):
            self.cache_enabled = self.cache_enabled.lower() in ("true", "1", "yes")
        logger.info(
            f"FileParser initialized: cache read {'enabled' if self.cache_enabled else 'disabled'}"
        )

    async def prepare(
        self,
        file_input: Union[bytes, BinaryIO],
        filename: str,
        *,
        file_key: Optional[str] = None,
    ) -> PreparedFile:
        """Prepare an uploaded file for storage.

        Returns raw bytes (for multimodal/offload), best-effort extracted text
        (for grep / non-multimodal models), the mime type, and the multimodal
        flag. Text extraction never raises — on failure ``parsed_text`` is "".
        """
        if hasattr(file_input, "read"):
            if hasattr(file_input, "seek"):
                file_input.seek(0)
            file_bytes = file_input.read()
        else:
            file_bytes = file_input or b""

        mime = guess_mime(filename)
        multimodal = is_multimodal(filename)
        content_hash = calculate_content_hash(file_bytes) if file_bytes else ""

        # Pure media (image/audio/video) has no text to extract — read happens
        # multimodally. Only extract for text-y and pdf/ppt types.
        ext = PurePosixPath(filename or "").suffix.lower()
        should_extract = (not multimodal) or (ext in _TEXT_EXTRACTABLE_MULTIMODAL)

        parsed_text = ""
        if should_extract and file_bytes:
            parsed_text, _method, _model = await self.parse_file(
                file_input=file_bytes, filename=filename, file_type=ext.lstrip(".")
            )

        return PreparedFile(
            raw_bytes=file_bytes or None,
            parsed_text=parsed_text or "",
            mime_type=mime,
            is_multimodal=multimodal,
            content_hash=content_hash,
        )

    async def parse_file(
        self,
        file_input: Union[bytes, BinaryIO],
        filename: str,
        file_type: str,
    ) -> tuple[str, str, str]:
        """Extract original text, with a ``th_files`` cache. Returns
        (text, method, model). Never raises."""
        try:
            file_type = file_type.lower().lstrip(".")
            if "/" in file_type:
                file_type = file_type.split("/")[-1]

            if hasattr(file_input, "read"):
                if hasattr(file_input, "seek"):
                    file_input.seek(0)
                file_bytes = file_input.read()
            else:
                file_bytes = file_input

            content_hash = calculate_content_hash(file_bytes)

            if self.cache_enabled:
                cached = await self._get_cached_file_by_hash(content_hash)
                if cached:
                    info = cached["parse_info"]
                    logger.info(f"🎯 Cache hit: {filename} (method: {info['method']})")
                    return cached["content"], info["method"], info.get("model", "")

            content_type = guess_mime(filename) if "." in (filename or "") else self._content_type(file_type)
            content = await self.file_abstract_extractor.extract_file_original_text(
                file_content=file_bytes,
                file_type=file_type,
                filename=filename,
                content_type=content_type,
            )
            if content and len(content.strip()) > 10:
                return content, "file_abstract_extractor", "unified_extract"
            return "", "file_abstract_extractor", ""
        except Exception as e:
            logger.error(f"File parsing failed for {filename}: {e}", exc_info=True)
            return "", "error", ""

    @staticmethod
    def _content_type(file_type: str) -> str:
        return _MIME_BY_EXT.get("." + file_type, "application/octet-stream")

    async def _query_th_files_cache(
        self, lookup_key: str, lookup_type: str = "content_hash"
    ) -> Optional[Dict[str, Any]]:
        """Query th_files parse cache by content_hash or file_key."""
        if not lookup_key:
            return None
        query = f"""
            SELECT decrypt_content(original_text) as original_text, file_type,
                   decrypt_content(file_name) as file_name, text_length, content_hash, updated_at
            FROM th_files
            WHERE {lookup_type} = :lookup_key AND is_del = false
              AND original_text IS NOT NULL AND original_text != ''
            ORDER BY updated_at DESC LIMIT 1
        """
        try:
            rows = await execute_query(query, params={"lookup_key": lookup_key})
            if not rows:
                return None
            row = rows[0]
            original_text = row.get("original_text", "")
            if not original_text or len(original_text.strip()) < 10:
                return None
            updated_at = row.get("updated_at")
            age_hours = 0.0
            if updated_at:
                now = datetime.now(timezone.utc) if updated_at.tzinfo else datetime.now()
                age_hours = (now - updated_at).total_seconds() / 3600
            file_name = row.get("file_name", "")
            ext = ("." + file_name.rsplit(".", 1)[-1].lower()) if file_name and "." in file_name else ""
            return {
                "content": original_text,
                "file_type": row.get("file_type", ""),
                "file_extension": ext,
                "original_size": row.get("text_length", 0),
                "content_hash": row.get("content_hash", ""),
                "parse_info": {
                    "method": f"th_files_cache{'_by_key' if lookup_type == 'file_key' else ''}",
                    "model": "",
                    "age_hours": age_hours,
                },
            }
        except Exception as e:
            logger.error(f"Cache query failed ({lookup_type}={lookup_key[:16]}...): {e}")
            return None

    async def _get_cached_file_by_hash(self, content_hash: str) -> Optional[Dict[str, Any]]:
        return await self._query_th_files_cache(content_hash, "content_hash")

    async def get_cached_file_by_key(self, file_key: str) -> Optional[Dict[str, Any]]:
        return await self._query_th_files_cache(file_key, "file_key")
