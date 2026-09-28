"""What the Files page shows, and the URLs it shows them with.

`FileDbService.get_files_paginated` answers which rows; this is the
presentation on top of that answer: a presigned URL re-signed because the
stored one has expired, and a MIME type reduced to the word the frontend picks
an icon from.
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def regenerate_file_url(file_key: str, original_filename: str = "", content_type: str = "application/octet-stream") -> str:
    """
    Regenerate file URL using unified storage client

    Args:
        file_key: The file key/path
        original_filename: Original filename (unused, kept for backward compatibility)
        content_type: MIME type of the file

    Returns:
        str: Regenerated signed URL, or empty string if regeneration fails
    """
    if not file_key:
        return ""

    try:
        from mirobody.utils.config.storage import get_storage_client

        storage = get_storage_client()
        storage_type = storage.get_storage_type()

        logger.debug(f"Using {storage_type} storage for URL regeneration, key: {file_key}")

        # Generate signed URL with 24 hours expiration
        url, err = await storage.generate_signed_url(
            key=file_key,
            expires=24 * 3600,
            content_type=content_type
        )
        if err:
            logger.warning(f"URL generation returned empty for key '{file_key}': {err}")
            return ""

        if url:
            return url
        logger.warning(f"URL generation returned empty for key: {file_key}")
        return ""

    except Exception as e:
        logger.error(f"URL regeneration failed for key {file_key}: {str(e)}", stack_info=True)
        return ""

async def _regenerate_urls(file_info: dict) -> None:
    """Regenerate URLs for file_info in place"""
    file_key = file_info.get("file_key", "")

    if not file_key:
        return

    try:
        new_url = await regenerate_file_url(
            file_key, "", file_info.get("contentType", "application/octet-stream")
        )
        if new_url:
            file_info["url_full"] = new_url
    except Exception as e:
        logger.warning(f"Failed to regenerate URL for {file_key}: {str(e)}", "_regenerate_urls")

async def get_uploaded_files_paginated(
    uploader_user_id: str,
    target_user_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Get user's uploaded file history with pagination.

    Now reads from th_files table instead of th_messages.

    Args:
        uploader_user_id: Current user ID (for permission checking)
        target_user_id: Target user ID to query files (None = query uploader_user_id)
        limit: Maximum number of files to return
        offset: Pagination offset

    Returns:
        Dict containing files list, total count, and total size
    """
    from .file_db_service import SOURCE_ASK, SOURCE_DATA, FileDbService

    try:
        # Use FileDbService to query from th_files table
        # Include both report and genetic scenes
        result = await FileDbService.get_files_paginated(
            user_id=uploader_user_id,
            query_user_id=target_user_id,
            # scene=["report", "genetic", "excel", "csv"],  # Both report and genetic files
            created_source=[SOURCE_DATA, SOURCE_ASK],
            limit=limit,
            offset=offset,
        )

        # Regenerate URLs for files with valid file_key
        files = result.get("files", [])
        files_with_keys = [f for f in files if f.get("file_key")]
        if files_with_keys:
            await asyncio.gather(
                *[_regenerate_urls(f) for f in files_with_keys],
                return_exceptions=True
            )

        # Convert MIME type to friendly file type
        for f in files:
            f["file_type"] = _convert_mime_to_file_type(
                f.get("file_type", ""),
                f.get("scene", "")
            )

        logger.info(f"Success: query_user_id={target_user_id or uploader_user_id}, total={result.get('total', 0)}")

        return result

    except Exception as e:
        logger.error(f"Get uploaded files failed: {str(e)}", stack_info=True)
        raise Exception(f"Failed to get uploaded files: {str(e)}")

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
