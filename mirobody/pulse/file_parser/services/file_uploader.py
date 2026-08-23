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
from typing import Tuple

# `fastapi` lives in the [server] extra, but file parsing is advertised engine
# functionality — a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.utils.config.storage import get_storage_client
from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx


# Supported file extensions. This gate must match what the handler factory can
# actually route, in BOTH directions, and it has been wrong both ways:
#
#   too narrow — it rejected every audio extension while AudioHandler sat
#                unreachable behind it, and rejected .md while TextHandler
#                happily parses it; the web client advertised audio anyway.
#   too wide   — .doc/.docx/.ppt/.pptx were accepted here with no handler in
#                existence, so the picker let you choose one, the upload ran,
#                and `file_processor` then answered "file not supported". They
#                are removed until there is something that parses them, so the
#                refusal happens at the gate with a list of what does work.
#                (docs/roadmap.md carries this as a capability gap.)
#
# `handlers/test_factory_routing.py` fails if this set and the factory disagree.
SUPPORTED_EXTENSIONS = {
    # Images (ImageHandler takes any image/*; heic/heif come from iPhones)
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".svg",
    ".heic", ".heif",
    # Documents. `.docx`/`.pptx` are back now that `handlers/document.py`
    # parses them; legacy binary `.doc`/`.ppt` stay out, because python-docx
    # and python-pptx read only the zip-based formats and accepting a file we
    # then refuse is the defect this set exists to prevent.
    ".pdf", ".xls", ".xlsx", ".docx", ".pptx",
    # Plain text: lab exports, genetic raw data, notes. `.csv` belongs here —
    # TextHandler owns it now that the never-injected CSVHandler is gone.
    ".txt", ".md", ".markdown", ".csv", ".json", ".xml",
    # Audio (AudioHandler)
    ".wav", ".mp3", ".aiff", ".aac", ".ogg", ".flac", ".m4a",
    # Archives: accepted for their contents, not parsed as themselves
    ".zip", ".rar",
}


class FileUploader:
    """File upload service class"""

    @classmethod
    async def upload_file_and_get_url(
        cls,
        file: UploadFile,
        filename: str,
        content_type: str,
        expires: int = 7200 * 15,
    ) -> str:
        """
        Asynchronously upload file and get URL using unified storage client

        Args:
            file: Upload file object
            filename: Target filename
            content_type: Content type
            expires: Expiration time (seconds)

        Returns:
            str: File URL
        """
        try:
            # Reset file pointer and read content
            await file.seek(0)
            content = await file.read()

            # Delegate to upload_content_and_get_url
            return await cls.upload_content_and_get_url(
                file_content=content,
                filename=filename,
                content_type=content_type,
                expires=expires,
            )
        finally:
            # Reset file pointer for subsequent processing
            await file.seek(0)

    @classmethod
    async def upload_content_and_get_url(
        cls,
        file_content: bytes,
        filename: str,
        content_type: str,
        expires: int = 7200 * 15,
    ) -> str:
        """
        Directly upload file content and get URL using unified storage client

        Args:
            file_content: File content
            filename: Target filename
            content_type: Content type
            expires: Expiration time (seconds)

        Returns:
            str: File URL
        """
        try:
            language = get_req_ctx("language", "en")

            # Check if content is empty
            if not file_content or len(file_content) == 0:
                logging.error(f"File content is empty: {filename}")
                raise ValueError(t("file_empty", language))

            file_size = len(file_content)
            
            # Get storage client at runtime (lazy initialization)
            storage = get_storage_client()
            
            logging.info(f"Starting to upload file content using {storage.get_storage_type()} storage: {filename}, size: {file_size} bytes")

            # Set upload timeout based on file size
            upload_timeout = 30 if file_size <= 10 * 1024 * 1024 else 60  # 30s for <=10MB, 60s for >10MB

            # Use unified storage client with timeout control
            try:
                upload_task = asyncio.create_task(
                    storage.put(
                        key=filename,
                        content=file_content,
                        content_type=content_type,
                        expires=expires
                    )
                )
                full_url, error = await asyncio.wait_for(upload_task, timeout=upload_timeout)
                
                if error:
                    raise ValueError(f"Upload failed: {error}")
                    
            except asyncio.TimeoutError:
                logging.error(f"File upload timeout: {filename}, size: {file_size} bytes")
                raise ValueError(t("file_upload_timeout", language))

            if not full_url:
                raise ValueError(t("file_upload_failed", language))

            logging.info(f"File uploaded successfully to {storage.get_storage_type()} storage: {full_url}")

            return full_url

        except Exception as e:
            logging.error(f"File content upload failed: {str(e)}", stack_info=True)
            raise


# Utility functions for file upload operations

def validate_file_extension(file: UploadFile) -> Tuple[bool, str]:
    """
    Validate uploaded file extension
    
    Args:
        file: The uploaded file
        
    Returns:
        tuple[bool, str]: (is_valid, error_message)
    """
    # Check file extension
    file_extension = Path(file.filename).suffix.lower()
    if file_extension not in SUPPORTED_EXTENSIONS:
        error_msg = f"File type {file_extension} not supported. Supported types: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        return False, error_msg
    
    return True, ""


#: A folder prefix is one or more `[A-Za-z0-9._-]` segments. Everything else —
#: absolute paths, backslashes, NUL, unicode separators — is rejected rather
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
    `../secrets/<ts>_<id>.pdf` and `LocalStorage` wrote it there — arbitrary
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


def get_file_type_category(content_type: str) -> str:
    """
    Determine file type category from content type
    
    Args:
        content_type: MIME content type
        
    Returns:
        str: File category (image, pdf, excel, document)
    """
    if not content_type:
        return "file"
        
    if content_type.startswith("image/"):
        return "image"
    elif content_type == "application/pdf":
        return "pdf"
    elif content_type in ["application/vnd.ms-excel", 
                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
        return "excel"
    else:
        return "document"
