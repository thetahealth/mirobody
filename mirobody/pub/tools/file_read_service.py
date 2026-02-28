"""
File Read Service - MCP Tools for read-only file operations.

Provides access to:
1. Global file library (list_global_files, fetch_remote_files)
2. Workspace file operations (ls, read_file, glob, grep)

All tools receive user_info parameter with user_id and session_id for authentication
and workspace isolation.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from deepagents.backends.utils import format_grep_matches, truncate_if_too_long

from .files_utils import (
    # Backend
    get_backend_with_session_info,
    validate_user_info,
    # Eviction
    maybe_evict_large_result,
    # Constants
    DEFAULT_READ_LIMIT,
    DEFAULT_READ_OFFSET,
    DEFAULT_EVICTION_TOKEN_LIMIT,
    ENABLE_IMAGE_MULTIMODAL,
    IMAGE_EXTENSIONS,
    NUM_CHARS_PER_TOKEN,
    READ_FILE_TRUNCATION_MSG,
    EMPTY_CONTENT_WARNING,
    # Tool descriptions
    LIST_FILES_TOOL_DESCRIPTION,
    READ_FILE_TOOL_DESCRIPTION,
    GLOB_TOOL_DESCRIPTION,
    GREP_TOOL_DESCRIPTION,
    LIST_GLOBAL_FILES_DESCRIPTION,
    FETCH_REMOTE_FILES_DESCRIPTION,
    # Decorator
    func_description,
)

logger = logging.getLogger(__name__)


class FileReadService:
    """
    File Read Service - MCP Tools

    Provides read-only access to workspace and global file library.
    All methods receive user_info parameter (auto-injected by MCP framework).

    user_info structure:
        {
            "user_id": str,      # User identifier (required)
            "session_id": str,   # Session identifier for workspace isolation (optional)
            "token": str,        # Optional auth token
            ...
        }

    Note:
        If session_id is not provided in user_info, the default "mcp" session is used.
        This provides a shared workspace per user for all MCP tool calls.
        Workspace namespace: {user_id}-mcp (or {user_id}-{session_id} if provided)
    """

    def __init__(self):
        self.name = "File Read Service"
        self.version = "1.0.0"
        logger.info("FileReadService initialized")

    # =========================================================================
    # Global File Library Tools
    # =========================================================================
    @func_description(LIST_GLOBAL_FILES_DESCRIPTION)
    async def list_global_files(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        offset: int = 0,
        limit: int = 50,
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            logger.warning(f"list_global_files auth failed: {error}")
            return f"Error: {error}"

        # Handle None values from caller (fallback to defaults)
        if offset is None:
            offset = 0
        if limit is None:
            limit = 50

        try:
            # Import from centralized utils module
            from .files_utils import list_global_files_from_db

            logger.info(f"Listing global files for user: {user_id}")
            result = await list_global_files_from_db(user_id, start_date, end_date, offset, limit)

            # Check for query error first
            if result.get("error"):
                logger.error(f"list_global_files query error: {result['error']}")
                return f"Error: Failed to list global files"

            files = result.get("files", [])
            if not files:
                return "No files found."

            # Format as list of file info strings
            file_entries = [
                {"file_key": f.get("file_key", ""), "filename": f.get("filename", ""), "date": f.get("date", ""), "file_type": f.get("file_type", "")}
                for f in files
            ]
            formatted = truncate_if_too_long(file_entries)
            total = result.get("total", len(files))
            has_more = result.get("has_more", False)

            output = str(formatted)
            if has_more:
                output += f"\n[Showing {len(files)} of {total} files. Use offset={offset + limit} to see more.]"

            # Evict large results to filesystem
            backend, _, _ = get_backend_with_session_info(user_info)
            return await maybe_evict_large_result(backend, output, "list_global_files")
        except Exception as e:
            logger.error(f"list_global_files failed: {e}")
            return f"Error: {e}"
    
    @func_description(FETCH_REMOTE_FILES_DESCRIPTION)
    async def fetch_remote_files(
        self,
        sources: List[str],
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        """Fetch files to /global_files/ from file_keys or URLs."""
        # Handle None value from caller (fallback to default)
        
        workspace_dir = "/global_files"

        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            logger.warning(f"fetch_remote_files auth failed: {error}")
            return f"Error: {error}"

        try:
            backend, _, _ = get_backend_with_session_info(user_info)

            from .files_utils import (
                get_file_info_from_file_key,
                create_file_reference,
            )

            logger.info(f"Fetching {len(sources)} file(s) for user: {user_id}")

            success_paths = []
            failed_items = []

            for source in sources:
                try:
                    if not isinstance(source, str) or not source:
                        failed_items.append(f"{source}: Invalid source")
                        continue

                    # Detect if source is URL or file_key
                    is_url = source.startswith(("http://", "https://"))

                    if is_url:
                        # Fetch directly from URL
                        fetch_result = await create_file_reference(backend, source, None, workspace_dir)
                        success_paths.append(fetch_result["workspace_path"])
                    else:
                        # Treat as file_key, get URL first
                        file_info = await get_file_info_from_file_key(source)
                        if not file_info or not file_info.get("url"):
                            failed_items.append(f"{source}: File not found")
                            continue

                        fetch_result = await create_file_reference(
                            backend, file_info["url"], file_info["filename"], workspace_dir
                        )
                        success_paths.append(fetch_result["workspace_path"])

                except Exception as e:
                    logger.error(f"Failed to fetch {source}: {e}")
                    failed_items.append(f"{source}: {e}")

            # Format output
            if success_paths and not failed_items:
                return f"Fetched files to: {success_paths}"
            elif success_paths and failed_items:
                return f"Fetched files to: {success_paths}\nFailed: {failed_items}"
            elif failed_items:
                return f"Failed to fetch files: {failed_items}"
            else:
                return "No files to fetch."
        except Exception as e:
            logger.error(f"fetch_remote_files failed: {e}")
            return f"Error: {e}"

    # =========================================================================
    # Workspace File Tools
    # =========================================================================

    @func_description(LIST_FILES_TOOL_DESCRIPTION)
    async def ls(
        self,
        path: str = "/",
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        try:
            backend, _, _ = get_backend_with_session_info(user_info)
            infos = await backend.als_info(path)
            paths = [f.get("path", "") for f in infos]
            result = truncate_if_too_long(paths)
            output = str(result)

            # Evict large results to filesystem
            return await maybe_evict_large_result(backend, output, "ls")
        except Exception as e:
            logger.error(f"ls failed for path {path}: {e}")
            return f"Error: {e}"

    @func_description(READ_FILE_TOOL_DESCRIPTION)
    async def read_file(
        self,
        file_path: str,
        offset: int = DEFAULT_READ_OFFSET,
        limit: int = DEFAULT_READ_LIMIT,
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        # Handle None values from caller (fallback to defaults)
        if offset is None:
            offset = DEFAULT_READ_OFFSET
        if limit is None:
            limit = DEFAULT_READ_LIMIT

        try:
            backend, _, _ = get_backend_with_session_info(user_info)

            # Image multimodal support (prepared but disabled)
            # When enabled, this will return base64 image content blocks
            ext = Path(file_path).suffix.lower()
            if ENABLE_IMAGE_MULTIMODAL and ext in IMAGE_EXTENSIONS:
                # Future: return multimodal content via backend.adownload_files
                # For now, fall through to text-based read
                pass

            result = await backend.aread(file_path, offset=offset, limit=limit)

            # Check for empty content
            if not result or result.strip() == "":
                return EMPTY_CONTENT_WARNING

            # Line-based truncation: ensure we don't exceed limit lines
            # Even though backend.aread accepts limit, it may return more lines
            # (e.g., due to line numbering format), so we enforce the limit here
            lines = result.splitlines(keepends=True)
            if len(lines) > limit:
                lines = lines[:limit]
                result = "".join(lines)

            # Token-based truncation: if result still exceeds token limit, truncate by characters
            token_limit = DEFAULT_EVICTION_TOKEN_LIMIT
            if token_limit and len(result) >= NUM_CHARS_PER_TOKEN * token_limit:
                # Calculate truncation message length to ensure final result stays under threshold
                truncation_msg = READ_FILE_TRUNCATION_MSG.format(file_path=file_path)
                max_content_length = NUM_CHARS_PER_TOKEN * token_limit - len(truncation_msg)
                result = result[:max_content_length]
                result += truncation_msg

            return result
        except FileNotFoundError:
            return f"Error: File not found: {file_path}"
        except Exception as e:
            logger.error(f"read_file failed for {file_path}: {e}")
            return f"Error: {e}"

    @func_description(GLOB_TOOL_DESCRIPTION)
    async def glob(
        self,
        pattern: str,
        path: str = "/",
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        try:
            backend, _, _ = get_backend_with_session_info(user_info)
            infos = await backend.aglob_info(pattern, path=path)
            paths = [f.get("path", "") for f in infos]
            result = truncate_if_too_long(paths)
            output = str(result)

            # Evict large results to filesystem
            return await maybe_evict_large_result(backend, output, "glob")
        except Exception as e:
            logger.error(f"glob failed for pattern {pattern}: {e}")
            return f"Error: {e}"

    @func_description(GREP_TOOL_DESCRIPTION)
    async def grep(
        self,
        pattern: str,
        path: Optional[str] = None,
        glob: Optional[str] = None,
        output_mode: Literal["files_with_matches", "content", "count"] = "files_with_matches",
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        try:
            backend, _, _ = get_backend_with_session_info(user_info)
            raw = await backend.agrep_raw(pattern, path=path, glob=glob)

            if isinstance(raw, str):
                # Error message returned from backend
                return raw

            formatted = format_grep_matches(raw, output_mode)
            output = truncate_if_too_long(formatted)  # type: ignore[arg-type]

            # Evict large results to filesystem
            return await maybe_evict_large_result(backend, str(output), "grep")
        except Exception as e:
            logger.error(f"grep failed for pattern {pattern}: {e}")
            return f"Error: {e}"
