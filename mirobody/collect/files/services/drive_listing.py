"""What the Files page shows, and the URLs it shows them with.

`FileDbService.get_files_paginated` answers which rows; this is the
presentation on top of that answer: a presigned URL re-signed because the
stored one has expired, and a MIME type reduced to the word the frontend picks
an icon from.
"""

import asyncio
import logging
from typing import Any

from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


async def regenerate_file_url(file_key: str, content_type: str = "application/octet-stream") -> str:
    """A signed URL for `file_key`, good for 24 hours, served as
    `content_type`; "" when none can be made."""
    if not file_key:
        return ""
    from mirobody.utils.config.storage import get_storage_client

    try:
        url, err = await get_storage_client().generate_signed_url(
            key=file_key, expires=24 * 3600, content_type=content_type)
    except Exception as e:
        logger.warning("signing a file url failed: file_key=%s error_type=%s", file_key, type(e).__name__,
                       exc_info=not is_driver_exception(e))
        return ""
    if err or not url:
        logger.warning("signing a file url failed: file_key=%s", file_key)
        return ""
    return url


async def _regenerate_urls(file_info: dict) -> None:
    """Sign `file_info`'s URL afresh, in place: the stored one has expired."""
    new_url = await regenerate_file_url(file_info["file_key"], file_info.get("contentType", "application/octet-stream"))
    if new_url:
        file_info["url_full"] = new_url


async def get_uploaded_files_paginated(
    uploader_user_id: str,
    target_user_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """One page of the files attached to a person's record (`th_files`),
    with fresh URLs and the word the frontend picks an icon from.

    Args:
        uploader_user_id: The caller (authorization is the router's)
        target_user_id: Whose record to list (None = the caller's own)
        limit: Maximum number of files to return
        offset: Pagination offset

    Returns:
        Dict containing the files list and the total count
    """
    from .file_db_service import SOURCE_ASK, SOURCE_DATA, FileDbService

    result = await FileDbService.get_files_paginated(
        user_id=uploader_user_id,
        query_user_id=target_user_id,
        created_source=[SOURCE_DATA, SOURCE_ASK],
        limit=limit,
        offset=offset,
    )
    files = result.get("files", [])
    await asyncio.gather(*[_regenerate_urls(f) for f in files if f.get("file_key")])
    for f in files:
        f["file_type"] = _convert_mime_to_file_type(f.get("file_type", ""), f.get("scene", ""))
    logger.info("files listed: user_id=%s total=%s", target_user_id or uploader_user_id, result.get("total", 0))
    return result


def _convert_mime_to_file_type(mime_type: str, scene: str = "") -> str:
    """
    Convert MIME type to friendly file type name.

    Returns one of: excel, csv, image, pdf, genetic
    """
    if not mime_type:
        return "unknown"

    mime_lower = mime_type.lower()

    # Genetic files (scene-based detection)
    if scene == "genetic":
        return "genetic"

    # PDF
    if "pdf" in mime_lower:
        return "pdf"

    # Image types
    if mime_lower.startswith("image/"):
        return "image"

    # Excel types
    excel_mimes = [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # xlsx
        "application/vnd.ms-excel",  # xls
        "application/vnd.ms-excel.sheet.macroenabled.12",  # xlsm
        "application/vnd.ms-excel.sheet.binary.macroenabled.12",  # xlsb
    ]
    if mime_lower in excel_mimes or "spreadsheet" in mime_lower or "excel" in mime_lower:
        return "excel"

    # CSV
    if "csv" in mime_lower:
        return "csv"

    # Markdown / rich text: the upload dropzone advertises "text/Markdown"
    # as accepted, so a .md upload must not render as "unknown".
    if "markdown" in mime_lower:
        return "text"

    # Default fallback based on common patterns
    if "text/plain" in mime_lower:
        # text/plain could be genetic or csv - check file extension if available
        return "genetic"  # Default to genetic for text files in this context

    return "unknown"
