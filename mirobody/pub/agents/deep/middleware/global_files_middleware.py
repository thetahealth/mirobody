"""Middleware for global files operations.

This middleware provides tools for fetching global files from th_messages table
and materializing them as references in the session workspace.
"""

import json
import logging
import os
import re
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain.tools import ToolRuntime
from langchain_core.tools import BaseTool, StructuredTool

from mirobody.utils import execute_query
from mirobody.utils.config.storage import get_storage_client

if TYPE_CHECKING:
    from mirobody.pub.agents.deep.backends.postgres_backend import PostgresBackend

logger = logging.getLogger(__name__)

# Import timedelta for date filtering
from datetime import timedelta

# System prompt for global files tools
GLOBAL_FILES_SYSTEM_PROMPT = """## Global Files Tools `list_global_files`, `fetch_files_by_key`, `fetch_files_by_url`

You have access to global files stored in the user's file library. These files persist across all conversations and are stored in the cloud.

- list_global_files: Query available files (sorted by date, newest first). Supports pagination via offset/limit.
- fetch_files_by_key: Download files by file_key to `/workspace/global_files/` (lazy loading)
- fetch_files_by_url: Download external files by URL to `/workspace/global_files/` (lazy loading)

Files are referenced until first access, then downloaded and parsed on-demand. Once fetched, use standard file tools (`read_file`, `edit_file`, `grep`) to work with them."""

LIST_GLOBAL_FILES_TOOL_DESCRIPTION = """List all files uploaded by the user globally.

Returns metadata for files in the user's library (images, PDFs, Excel, documents, etc.).
Results are sorted by upload time (newest first) and automatically deduplicated.

Usage:
- Use start_date/end_date to filter by date range (format: 'YYYY-MM-DD')
- Use offset/limit for pagination (default: 0/50, max limit: 200)
- Returns dict with: files (list), total (count), offset, limit, has_more (bool)
- Each file contains: file_key (identifier), date, file_type (MIME type), filename, abstract (optional)
- If you can't find the file, increase offset to fetch more results
"""

FETCH_FILES_BY_KEY_TOOL_DESCRIPTION = """Fetch files by file_key and add them to the workspace.

Usage:
- Provide a list of file_keys from list_global_files
- Creates references in `/workspace/global_files/` (lazy loading)
- Content is downloaded when first accessed via read_file
- Returns: dict with success (list) and failed (list) entries
"""

FETCH_FILES_BY_URL_TOOL_DESCRIPTION = """Fetch files or web pages from URLs and add them to the workspace.

Usage:
- Provide a list of HTTP/HTTPS URLs
- Creates references in `/workspace/global_files/` (lazy loading)
- Content is downloaded when first accessed via read_file
- Supports both file URLs and web pages (saved as .html)
- Returns: dict with success (list) and failed (list) entries
"""


def _get_backend(backend: Any, runtime: ToolRuntime) -> "PostgresBackend":
    """Get the resolved backend instance from backend or factory.
    
    Args:
        backend: Backend instance or factory function.
        runtime: The tool runtime context.
        
    Returns:
        Resolved backend instance.
    """
    if callable(backend):
        return backend(runtime)
    return backend


def _validate_user_auth(runtime: ToolRuntime) -> tuple[bool, Optional[str], str]:
    """Validate user authentication from runtime.
    
    Args:
        runtime: The tool runtime context.
        
    Returns:
        Tuple of (is_valid, user_id, error_message)
    """
    user_info = runtime.config.get("configurable", {}).get("user_info", {})
    
    if not user_info or not user_info.get("success"):
        return False, None, "Authentication required"
    
    user_id = str(user_info.get("user_id"))
    if not user_id:
        return False, None, "No user ID found"
    
    return True, user_id, ""


def _get_file_type(extension: str) -> str:
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


def _sanitize_filename(filename: str) -> str:
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


async def _get_file_info_from_file_key(file_key: str) -> Optional[Dict[str, Any]]:
    """Generate signed URL and metadata from file_key (with database lookup)."""
    try:
        # Try to get file info from database first
        from mirobody.utils import execute_query
        
        file_info = {
            "file_key": file_key,
            "filename": _sanitize_filename(Path(file_key).name),
        }
        
        # Query th_files for metadata
        try:
            query = """
                SELECT file_name, file_type, file_content
                FROM theta_ai.th_files
                WHERE file_key = :file_key AND is_del = false
                LIMIT 1
            """
            result = await execute_query(query, params={"file_key": file_key})
            
            if result and len(result) > 0:
                row = result[0]
                # Use database filename if available
                if row.get("file_name"):
                    file_info["filename"] = _sanitize_filename(row["file_name"])
                if row.get("file_type"):
                    file_info["file_type"] = row["file_type"]
                # Store file_content for future use
                if row.get("file_content"):
                    file_info["file_content"] = row["file_content"]
        except Exception as db_err:
            logger.warning(f"Failed to get metadata from database for '{file_key}': {db_err}")
        
        # Generate signed URL
        storage = get_storage_client()
        url = await storage.generate_signed_url(file_key) or ""
        file_info["url"] = url
        
        return file_info
    except Exception as e:
        logger.error(f"Failed to get file info from file_key '{file_key}': {e}")
        return None


async def _list_global_files_from_db(
    user_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    offset: int = 0,
    limit: int = 50,
) -> Dict[str, Any]:
    """List all files uploaded by the user from th_files table (optimized)."""
    try:
        # Constraints: offset >= 0, limit between 1 and 200
        offset = max(0, offset)
        limit = max(1, min(limit, 200))
        
        # Build WHERE clause for filters
        where_conditions = "WHERE query_user_id = :user_id AND is_del = false"
        params = {"user_id": user_id}
        
        # Add date filters
        if start_date:
            try:
                params["start_date"] = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)
                where_conditions += " AND updated_at >= :start_date"
            except ValueError as e:
                logger.warning(f"Invalid start_date '{start_date}': {e}")
        
        if end_date:
            try:
                params["end_date"] = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
                where_conditions += " AND updated_at <= :end_date"
            except ValueError as e:
                logger.warning(f"Invalid end_date '{end_date}': {e}")
        
        # Get total count (fast count from th_files, with automatic deduplication by file_key)
        count_sql = f"""
            SELECT COUNT(DISTINCT file_key) as total
            FROM theta_ai.th_files
            {where_conditions}
        """
        count_result = await execute_query(count_sql, params=params)
        total_files = count_result[0]["total"] if count_result else 0
        
        # Get paginated results (already deduplicated by DISTINCT ON file_key)
        params["limit"] = limit
        params["offset"] = offset
        
        list_sql = f"""
            SELECT DISTINCT ON (file_key)
                file_key,
                file_name,
                file_type,
                file_content,
                updated_at
            FROM theta_ai.th_files
            {where_conditions}
            ORDER BY file_key, updated_at DESC
        """
        # Sort by updated_at DESC for final result (newest first)
        list_sql = f"""
            SELECT * FROM ({list_sql}) AS deduplicated
            ORDER BY updated_at DESC
            LIMIT :limit OFFSET :offset
        """
        list_results = await execute_query(list_sql, params=params)
        
        if not isinstance(list_results, list):
            logger.warning(f"Unexpected query result type: {type(list_results)}")
            return {
                "files": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "has_more": False,
            }
        
        logger.info(f"Found {len(list_results)} files from th_files table")
        
        # Extract file metadata (much simpler than parsing th_messages)
        results = []
        for row in list_results:
            try:
                file_key = row.get("file_key", "")
                if not file_key:
                    logger.warning(f"Skipping row without file_key: {row}")
                    continue
                
                # Parse file_content JSON
                file_content = row.get("file_content", {})
                if isinstance(file_content, str):
                    try:
                        file_content = json.loads(file_content)
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse file_content for {file_key}")
                        file_content = {}
                
                # Get abstract (prefer file_abstract, fallback to truncated raw)
                abstract = file_content.get("file_abstract", "")
                if not abstract:
                    raw = file_content.get("raw", "")
                    if raw:
                        abstract = raw[:80] + "..." if len(raw) > 80 else raw
                elif len(abstract) > 80:
                    abstract = abstract[:80] + "..."
                
                file_info = {
                    "file_key": file_key,
                    "date": row["updated_at"].strftime("%Y-%m-%d") if row.get("updated_at") else "Unknown",
                }
                
                # Add file type for better context
                if row.get("file_type"):
                    file_info["file_type"] = row["file_type"]
                
                # Add abstract if available
                if abstract:
                    file_info["abstract"] = abstract
                
                # Add filename for better context
                if row.get("file_name"):
                    file_info["filename"] = row["file_name"]
                
                results.append(file_info)
            
            except Exception as e:
                logger.warning(f"Failed to process file record: {e}")
                continue
        
        has_more = (offset + limit) < total_files
        
        logger.info(f"Returning {len(results)} files, total: {total_files}, offset: {offset}, limit: {limit}, has_more: {has_more}")
        
        return {
            "files": results,
            "total": total_files,
            "offset": offset,
            "limit": limit,
            "has_more": has_more,
        }
    
    except Exception as e:
        logger.error(f"List files error: {e}", exc_info=True)
        return {
            "files": [],
            "total": 0,
            "offset": offset,
            "limit": limit,
            "has_more": False,
        }


def _list_global_files_tool_generator(
    custom_description: str | None = None,
) -> BaseTool:
    """Generate the list_global_files tool.
    
    Args:
        custom_description: Optional custom description for the tool.
        
    Returns:
        Configured list_global_files tool.
    """
    tool_description = custom_description or LIST_GLOBAL_FILES_TOOL_DESCRIPTION
    
    async def async_list_global_files(
        runtime: ToolRuntime,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: int = 0,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """List global files available to the user."""
        is_valid, user_id, error = _validate_user_auth(runtime)
        if not is_valid:
            logger.warning(f"list_global_files auth failed: {error}")
            return {
                "files": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "has_more": False,
            }
        
        logger.info(f"Listing global files for user: {user_id}, start_date: {start_date}, end_date: {end_date}, offset: {offset}, limit: {limit}")
        return await _list_global_files_from_db(user_id, start_date, end_date, offset, limit)
    
    return StructuredTool.from_function(
        name="list_global_files",
        description=tool_description,
        coroutine=async_list_global_files,
    )


async def _create_file_reference(
    backend: Any,
    url: str,
    file_key: Optional[str],
    workspace_dir: str,
) -> Dict[str, Any]:
    """Create a file reference in the workspace.
    
    Args:
        backend: Backend instance
        url: File URL (for downloading)
        file_key: Optional file_key (for metadata)
        workspace_dir: Workspace directory path
        
    Returns:
        Dict with workspace_path and filename, or raises exception on failure
    """
    # Extract filename from URL or file_key
    if file_key:
        filename = Path(file_key).name
    else:
        filename = Path(url.split('?')[0]).name
    
    # If no extension, treat as HTML
    if not Path(filename).suffix:
        filename = f"{filename}.html"
    
    filename = _sanitize_filename(filename)
    
    # Detect file type
    file_ext = Path(filename).suffix
    file_type = _get_file_type(file_ext)
    
    # Generate workspace path
    workspace_path = f"{workspace_dir}/{filename}"
    
    # Handle filename collision - use als_info to list existing files
    try:
        existing_files_info = await backend.als_info(workspace_dir)
        existing_filenames = {Path(f.path).name for f in existing_files_info}
        
        if filename in existing_filenames:
            # Create unique filename using hash of file_key or url
            unique_suffix = (file_key or url)[:8] if len(file_key or url) >= 8 else "file"
            name, ext = os.path.splitext(filename)
            filename = f"{name}_{unique_suffix}{ext}"
            workspace_path = f"{workspace_dir}/{filename}"
    except Exception as e:
        # If listing fails, proceed with original filename (will fail on write if duplicate)
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
    
    # Save reference directly to store (bypass awrite to avoid text processing)
    await backend.store.put(backend.namespace, workspace_path, file_data)
    
    # Update cache
    backend._cache[workspace_path] = file_data
    
    logger.info(f"Created file reference: {workspace_path}")
    
    return {
        "workspace_path": workspace_path,
        "filename": filename
    }


def _fetch_files_by_key_tool_generator(
    backend: Any,
    custom_description: str | None = None,
) -> BaseTool:
    """Generate the fetch_files_by_key tool.
    
    Args:
        backend: Backend instance or factory function.
        custom_description: Optional custom description for the tool.
        
    Returns:
        Configured fetch_files_by_key tool.
    """
    tool_description = custom_description or FETCH_FILES_BY_KEY_TOOL_DESCRIPTION
    
    async def async_fetch_files_by_key(
        file_keys: List[str],
        runtime: ToolRuntime,
        workspace_dir: str = "/workspace/global_files",
    ) -> Dict[str, Any]:
        """Fetch files by file_key and add them to workspace as references."""
        # Validate authentication
        is_valid, user_id, error = _validate_user_auth(runtime)
        if not is_valid:
            logger.warning(f"fetch_files_by_key auth failed: {error}")
            return {
                "success": [],
                "failed": [{"file_key": str(f), "error": error} for f in file_keys]
            }
        
        # Get backend instance
        resolved_backend = _get_backend(backend, runtime)
        logger.info(f"Fetching {len(file_keys)} file(s) by key for user: {user_id}")
        
        success = []
        failed = []
        
        for file_key in file_keys:
            try:
                if not isinstance(file_key, str) or not file_key:
                    failed.append({"file_key": str(file_key), "error": "Invalid file_key"})
                    continue
                
                # Get file info (generates signed URL)
                file_info = await _get_file_info_from_file_key(file_key)
                if not file_info or not file_info.get("url"):
                    failed.append({"file_key": file_key, "error": "File not found or no URL"})
                    continue
                
                # Create file reference
                result = await _create_file_reference(
                    resolved_backend, file_info["url"], file_key, workspace_dir
                )
                success.append({"file_key": file_key, "path": result["workspace_path"]})
                
            except Exception as e:
                logger.error(f"Failed to fetch {file_key}: {e}")
                failed.append({"file_key": file_key, "error": str(e)})
        
        return {"success": success, "failed": failed}
        
    
    return StructuredTool.from_function(
        name="fetch_files_by_key",
        description=tool_description,
        coroutine=async_fetch_files_by_key,
    )


def _fetch_files_by_url_tool_generator(
    backend: Any,
    custom_description: str | None = None,
) -> BaseTool:
    """Generate the fetch_files_by_url tool.
    
    Args:
        backend: Backend instance or factory function.
        custom_description: Optional custom description for the tool.
        
    Returns:
        Configured fetch_files_by_url tool.
    """
    tool_description = custom_description or FETCH_FILES_BY_URL_TOOL_DESCRIPTION
    
    async def async_fetch_files_by_url(
        urls: List[str],
        runtime: ToolRuntime,
        workspace_dir: str = "/workspace/global_files",
    ) -> Dict[str, Any]:
        """Fetch files by URL and add them to workspace as references."""
        # Validate authentication
        is_valid, user_id, error = _validate_user_auth(runtime)
        if not is_valid:
            logger.warning(f"fetch_files_by_url auth failed: {error}")
            return {
                "success": [],
                "failed": [{"url": str(u), "error": error} for u in urls]
            }
        
        # Get backend instance
        resolved_backend = _get_backend(backend, runtime)
        logger.info(f"Fetching {len(urls)} file(s) by URL for user: {user_id}")
        
        success = []
        failed = []
        
        for url in urls:
            try:
                if not isinstance(url, str) or not url:
                    failed.append({"url": str(url), "error": "Invalid URL"})
                    continue
                
                # Create file reference (URL is used directly)
                result = await _create_file_reference(resolved_backend, url, None, workspace_dir)
                success.append({"url": url, "path": result["workspace_path"]})
                
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                failed.append({"url": url, "error": str(e)})
        
        return {"success": success, "failed": failed}
    
    return StructuredTool.from_function(
        name="fetch_files_by_url",
        description=tool_description,
        coroutine=async_fetch_files_by_url,
    )


class GlobalFilesMiddleware(AgentMiddleware):
    """Middleware for global files operations.
    
    Provides tools for fetching global files from th_messages table
    and materializing them as references in the session workspace.
    
    Files are lazily loaded - only downloaded and parsed when first read.
    """
    
    def __init__(
        self,
        backend: Any,
        system_prompt: str | None = None,
        custom_tool_descriptions: dict[str, str] | None = None,
    ):
        """Initialize the global files middleware.
        
        Args:
            backend: Backend instance or factory function.
            system_prompt: Optional custom system prompt override.
            custom_tool_descriptions: Optional custom tool descriptions.
        """
        self.backend = backend
        self._custom_system_prompt = system_prompt
        
        # Generate tools
        custom_descs = custom_tool_descriptions or {}
        self.tools = [
            _list_global_files_tool_generator(
                custom_description=custom_descs.get("list_global_files"),
            ),
            _fetch_files_by_key_tool_generator(
                backend=self.backend,
                custom_description=custom_descs.get("fetch_files_by_key"),
            ),
            _fetch_files_by_url_tool_generator(
                backend=self.backend,
                custom_description=custom_descs.get("fetch_files_by_url"),
            )
        ]
    
    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        """Update the system prompt with global files instructions.
        
        Args:
            request: The model request being processed.
            handler: The handler function to call with the modified request.
            
        Returns:
            The model response from the handler.
        """
        # Use custom system prompt if provided, otherwise use default
        system_prompt = self._custom_system_prompt or GLOBAL_FILES_SYSTEM_PROMPT
        
        if system_prompt:
            request = request.override(
                system_prompt=request.system_prompt + "\n\n" + system_prompt 
                if request.system_prompt else system_prompt
            )
        
        return handler(request)
    
    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """(async) Update the system prompt with global files instructions.
        
        Args:
            request: The model request being processed.
            handler: The handler function to call with the modified request.
            
        Returns:
            The model response from the handler.
        """
        # Use custom system prompt if provided, otherwise use default
        system_prompt = self._custom_system_prompt or GLOBAL_FILES_SYSTEM_PROMPT
        
        if system_prompt:
            request = request.override(
                system_prompt=request.system_prompt + "\n\n" + system_prompt 
                if request.system_prompt else system_prompt
            )
        
        return await handler(request)

