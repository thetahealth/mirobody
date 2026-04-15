"""
Global Files Utilities

Core data access functions for global file operations:
- Listing files from th_files table
- Getting file info and signed URLs
- Creating file references in workspace

Used by file_read_service.py (MCP tools).
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions
# =============================================================================

def get_file_type(extension: str) -> str:
    """Get file type from extension."""
    FILE_TYPE_MAP = {
        '.pdf': 'PDF',
        '.docx': 'DOCX',
        '.doc': 'DOC',
        '.png': 'IMAGE',
        '.jpg': 'IMAGE',
        '.jpeg': 'IMAGE',
        '.gif': 'IMAGE',
        '.webp': 'IMAGE',
        '.bmp': 'IMAGE',
        '.txt': 'TEXT',
        '.md': 'TEXT',
        '.csv': 'CSV',
        '.xlsx': 'EXCEL',
        '.xls': 'EXCEL',
    }

    ext_lower = extension.lower()
    if not ext_lower.startswith('.'):
        ext_lower = '.' + ext_lower
    return FILE_TYPE_MAP.get(ext_lower, 'UNKNOWN')


def sanitize_filename(filename: str) -> str:
    """Clean filename, remove illegal characters."""
    # Remove or replace illegal characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove control characters
    filename = re.sub(r'[\x00-\x1f\x7f]', '', filename)
    # Limit filename length (preserve extension)
    name, ext = os.path.splitext(filename)
    if len(name) > 200:
        name = name[:200]
    return name + ext


# =============================================================================
# Database Query Functions
# =============================================================================

async def get_file_info_from_file_key(file_key: str) -> Optional[Dict[str, Any]]:
    """
    Generate signed URL and metadata from file_key (with database lookup).

    Args:
        file_key: The file key (path in storage).

    Returns:
        Dict with file_key, filename, file_type, url, etc. or None on error.
    """
    from mirobody.utils import execute_query
    from mirobody.utils.config.storage import get_storage_client

    try:
        file_info = {
            "file_key": file_key,
            "filename": sanitize_filename(Path(file_key).name),
        }

        # Query th_files for metadata
        try:
            query = """
                SELECT file_name, file_type, file_content
                FROM th_files
                WHERE file_key = :file_key AND is_del = false
                LIMIT 1
            """
            result = await execute_query(query, params={"file_key": file_key})

            if result and len(result) > 0:
                row = result[0]
                if row.get("file_name"):
                    file_info["filename"] = sanitize_filename(row["file_name"])
                if row.get("file_type"):
                    file_info["file_type"] = row["file_type"]
                if row.get("file_content"):
                    file_info["file_content"] = row["file_content"]
        except Exception as db_err:
            logger.warning(f"Failed to get metadata from database for '{file_key}': {db_err}")

        # Generate signed URL
        storage = get_storage_client()
        url, err = await storage.generate_signed_url(file_key)
        if err:
            logger.warning(err)

        url = url or ""
        file_info["url"] = url

        return file_info
    except Exception as e:
        logger.error(f"Failed to get file info from file_key '{file_key}': {e}")
        return None


async def list_global_files_from_db(
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    offset: int = 0,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    List all files uploaded by the user from th_files table.

    Args:
        user_id: User identifier.
        start_date: Filter start date (format: 'YYYY-MM-DD').
        end_date: Filter end date (format: 'YYYY-MM-DD').
        offset: Pagination offset.
        limit: Number of results (max 200).

    Returns:
        Dict with files, total, offset, limit, has_more.
    """
    from mirobody.utils import execute_query

    # Handle None values
    if offset is None:
        offset = 0
    if limit is None:
        limit = 50

    try:
        # Constraints: offset >= 0, limit between 1 and 200
        offset = max(0, offset)
        limit = max(1, min(limit, 200))

        # Build WHERE clause for filters
        where_conditions = "WHERE query_user_id = :user_id AND is_del = false"
        params: Dict[str, Any] = {"user_id": user_id}

        # Add date filters
        if start_date:
            try:
                params["start_date"] = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
                where_conditions += " AND created_at >= :start_date"
            except ValueError as e:
                logger.warning(f"Invalid start_date '{start_date}': {e}")

        if end_date:
            try:
                params["end_date"] = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
                where_conditions += " AND created_at <= :end_date"
            except ValueError as e:
                logger.warning(f"Invalid end_date '{end_date}': {e}")

        # Get total count
        count_sql = f"""
            SELECT COUNT(DISTINCT file_key) as total
            FROM th_files
            {where_conditions}
        """
        count_result = await execute_query(count_sql, params=params)
        total_files = count_result[0]["total"] if count_result else 0

        # Get paginated results
        params["limit"] = limit
        params["offset"] = offset

        list_sql = f"""
            SELECT DISTINCT ON (file_key)
                file_key, file_name, file_type, file_content, created_at
            FROM th_files
            {where_conditions}
            ORDER BY file_key, created_at DESC
        """
        list_sql = f"""
            SELECT * FROM ({list_sql}) AS deduplicated
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """
        list_results = await execute_query(list_sql, params=params)

        if not isinstance(list_results, list):
            logger.warning(f"Unexpected query result type: {type(list_results)}")
            return {"files": [], "total": 0, "offset": offset, "limit": limit, "has_more": False}

        logger.info(f"Found {len(list_results)} files from th_files table")

        # Extract file metadata
        results = []
        for row in list_results:
            try:
                file_key = row.get("file_key", "")
                if not file_key:
                    continue

                # Parse file_content JSON
                file_content = row.get("file_content", {})
                if isinstance(file_content, str):
                    try:
                        file_content = json.loads(file_content)
                    except json.JSONDecodeError:
                        file_content = {}

                # Get abstract
                abstract = file_content.get("file_abstract", "")
                if not abstract:
                    raw = file_content.get("raw", "")
                    if raw:
                        abstract = raw[:80] + "..." if len(raw) > 80 else raw
                elif len(abstract) > 80:
                    abstract = abstract[:80] + "..."

                file_info = {
                    "file_key": file_key,
                    "date": row["created_at"].strftime("%Y-%m-%d") if row.get("created_at") else "Unknown",
                }

                if row.get("file_type"):
                    file_info["file_type"] = row["file_type"]
                if abstract:
                    file_info["abstract"] = abstract
                if row.get("file_name"):
                    file_info["filename"] = row["file_name"]

                results.append(file_info)
            except Exception as e:
                logger.warning(f"Failed to process file record: {e}")
                continue

        has_more = (offset + limit) < total_files
        return {
            "files": results,
            "total": total_files,
            "offset": offset,
            "limit": limit,
            "has_more": has_more,
        }

    except Exception as e:
        logger.error(f"List files error: {e}", exc_info=True)
        return {"files": [], "total": 0, "offset": offset, "limit": limit, "has_more": False, "error": str(e)}


# =============================================================================
# Workspace File Reference Functions
# =============================================================================

async def create_file_reference(
    backend: Any,
    url: str,
    file_key: Optional[str],
    workspace_dir: str,
    original_filename: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a file reference in the workspace.

    Files are lazily loaded - only downloaded and parsed when first read.

    Args:
        backend: Backend instance (PostgresBackend).
        url: File URL (for downloading).
        file_key: Optional file_key (for metadata).
        workspace_dir: Workspace directory path.
        original_filename: Optional original filename from database.

    Returns:
        Dict with workspace_path and filename.

    Raises:
        Exception on failure.
    """
    # Determine filename
    if original_filename:
        filename = original_filename
    elif file_key:
        filename = Path(file_key).name
    else:
        filename = Path(url.split('?')[0]).name

    # If no extension, treat as HTML
    if not Path(filename).suffix:
        filename = f"{filename}.html"

    filename = sanitize_filename(filename)

    # Detect file type
    file_ext = Path(filename).suffix
    file_type = get_file_type(file_ext)

    # Generate workspace path
    workspace_path = f"{workspace_dir}/{filename}"

    # Handle filename collision
    try:
        existing_files_info = backend.ls_info(workspace_dir)
        existing_filenames = {f.get("path", "").split("/")[-1] for f in existing_files_info}

        if filename in existing_filenames:
            source = file_key or url
            unique_suffix = source[:8] if len(source) >= 8 else "file"
            name, ext = os.path.splitext(filename)
            filename = f"{name}_{unique_suffix}{ext}"
            workspace_path = f"{workspace_dir}/{filename}"
    except Exception as e:
        logger.warning(f"Failed to check existing files: {e}")

    # Create reference FileData
    file_data = {
        "is_reference": True,
        "file_key": file_key,
        "url": url,
        "filename": filename,
        "file_type": file_type,
        "file_extension": file_ext,
        "parsed": False,
        "content": [],
        "raw_content": "",
        "created_at": datetime.now().isoformat(),
        "modified_at": datetime.now().isoformat(),
        "metadata": {
            "source": "file_key" if file_key else "url",
            "fetch_timestamp": datetime.now().isoformat(),
        }
    }

    # Save reference directly to store (sync method)
    backend.store.put((backend.session_id, backend.user_id), workspace_path, file_data)

    # Update cache
    cache_key = (backend.session_id, backend.user_id, workspace_path)
    backend._file_cache[cache_key] = {
        "data": file_data,
        "timestamp": time.time()
    }

    logger.info(f"Created file reference: {workspace_path}")

    return {
        "workspace_path": workspace_path,
        "filename": filename
    }


__all__ = [
    # Helpers
    "get_file_type",
    "sanitize_filename",
    # Database
    "get_file_info_from_file_key",
    "list_global_files_from_db",
    # Workspace
    "create_file_reference",
]
