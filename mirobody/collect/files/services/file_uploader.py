"""
File upload service

Responsible for handling file uploads using unified storage client
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.collect.files.errors import UploadError
from mirobody.utils.config.storage import get_storage_client
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import request_language

logger = logging.getLogger(__name__)


#: What an upload may be, checked by `POST /files/upload` and by the
#: WebSocket's `upload_start`: files `documents.detect` names a kind for, and
#: the containers `GeneticHandler` opens by their bytes. Accepting a file nothing
#: then reads is the defect this set exists to prevent, so the legacy binary
#: `.doc`/`.ppt`/`.xls` stay out: python-docx, python-pptx and openpyxl read
#: only the zip formats.
SUPPORTED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tif", ".tiff", ".svg",
    ".heic", ".heif",
    ".pdf", ".xlsx", ".xlsm", ".docx", ".pptx",
    ".txt", ".md", ".markdown", ".csv", ".json", ".xml", ".log", ".htm", ".html",
    # A bgzipped VCF: the Genomics page offers .vcf.bgz and .bgzf, and
    # GeneticHandler reads them by their bytes.
    ".vcf", ".gz", ".bgz", ".bgzf", ".zip",
}


class FileUploader:
    """Puts an upload in object storage."""

    @classmethod
    async def upload_file_and_get_url(
        cls,
        file: UploadFile,
        filename: str,
        content_type: str,
        expires: int = 7200 * 15,
    ) -> str:
        """`upload_content_and_get_url` for an upload's whole content, its
        position put back at the start for whoever reads it next."""
        try:
            await file.seek(0)
            content = await file.read()
            return await cls.upload_content_and_get_url(
                file_content=content,
                filename=filename,
                content_type=content_type,
                expires=expires,
            )
        finally:
            await file.seek(0)

    @classmethod
    async def upload_content_and_get_url(
        cls,
        file_content: bytes,
        filename: str,
        content_type: str,
        expires: int = 7200 * 15,
    ) -> str:
        """Store `file_content` under the key `filename` and return its URL,
        good for `expires` seconds. Raises `UploadError` with a sentence for
        the person who uploaded the file when it was not stored."""
        language = request_language()
        if not file_content:
            raise UploadError(localize("file_empty", language, "file_uploader"))
        storage = get_storage_client()
        # 30 s up to 10 MB, 60 s above.
        upload_timeout = 30 if len(file_content) <= 10 * 1024 * 1024 else 60
        try:
            full_url, error = await asyncio.wait_for(
                storage.put(key=filename, content=file_content, content_type=content_type, expires=expires),
                timeout=upload_timeout,
            )
        except TimeoutError:
            logger.error("upload timed out: file_key=%s size_bytes=%d", filename, len(file_content))
            raise UploadError(localize("file_upload_timeout", language, "file_uploader")) from None
        if error or not full_url:
            logger.error("upload not stored: file_key=%s", filename)
            raise UploadError(localize("file_upload_failed", language, "file_uploader"))
        # The key, never the URL: it is a presigned link, good for 30 hours.
        logger.info("upload stored: storage=%s file_key=%s size_bytes=%d", storage.get_storage_type(), filename,
                    len(file_content))
        return full_url


# Utility functions for file upload operations

def validate_file_extension(filename: str | None) -> tuple[bool, str]:
    """Whether an upload named `filename` is one `SUPPORTED_EXTENSIONS` takes:
    `(True, "")`, or `(False, the sentence the person is told)`."""
    extension = Path(filename or "").suffix.lower()
    if extension in SUPPORTED_EXTENSIONS:
        return True, ""
    return False, f"File type {extension or '(none)'} not supported. Supported types: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"


#: A folder prefix is one or more `[A-Za-z0-9._-]` segments. Everything else (
#: absolute paths, backslashes, NUL, unicode separators) is rejected rather
#: than sanitized, because sanitizing invites the next bypass.
#:
#: The segment check is NOT redundant with the pattern. `.` is a legitimate
#: character inside a folder name, so it has to be in the class, and that alone
#: makes `..` a matching segment: the first version of this guard accepted
#: `../secrets` and was caught by the test below, not by review.
_SAFE_FOLDER_RE = re.compile(r"^[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*$")


def _is_safe_folder(folder_prefix: str) -> bool:
    if not folder_prefix or not _SAFE_FOLDER_RE.match(folder_prefix):
        return False
    return all(seg not in (".", "..") for seg in folder_prefix.split("/"))


def generate_file_key(filename: str, folder_prefix: str = "uploads") -> str:
    """
    Generate a unique file key for cloud storage

    `folder_prefix` reaches this function straight from the `?folder=` query
    parameter on `POST /files/upload`, so it is attacker-controlled. It used to
    be interpolated as-is, and `AbstractStorage._build_object_key` only does
    `lstrip("/")`, so `?folder=../secrets` produced the key
    `../secrets/<ts>_<id>.pdf` and `LocalStorage` wrote it there: arbitrary
    file write outside `base_path`, reproduced in
    `test_upload_paths.py::test_a_traversing_folder_is_rejected`.

    Rejecting is deliberate: silently rewriting `..` away would let a caller
    aim at a directory they did not name, which is its own surprise.

    Args:
        filename: Original filename
        folder_prefix: Folder prefix for the file path (default: "uploads")

    Returns:
        str: Unique file key with timestamp and UUID

    Raises:
        ValueError: if `folder_prefix` is not a plain relative folder path
    """
    if not _is_safe_folder(folder_prefix or ""):
        raise ValueError(
            f"invalid folder prefix {folder_prefix!r}: expected one or more "
            "path segments of [A-Za-z0-9._-]"
        )
    file_extension = Path(filename).suffix
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = uuid.uuid4().hex[:8]
    return f"{folder_prefix}/{timestamp}_{unique_id}{file_extension}"
