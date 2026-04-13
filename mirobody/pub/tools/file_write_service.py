"""
File Write Service - MCP Tools for file write operations.

Provides:
- write_file: Create new files
- edit_file: Edit existing files

All tools receive user_info parameter with user_id and session_id for authentication
and workspace isolation.

Note:
    These tools can be disabled via config.yaml:
    ```yaml
    DISALLOWED_TOOLS_DEEP:
      - write_file
      - edit_file
    ```

    For code execution (execute tool), see code_service.py.
"""

import logging
from typing import Any, Dict, Optional

from .files_utils import (
    func_description,
    get_backend_with_session_info,
    validate_user_info,
    WRITE_FILE_TOOL_DESCRIPTION,
    EDIT_FILE_TOOL_DESCRIPTION,
)

logger = logging.getLogger(__name__)


class FileWriteService:
    """
    File Write Service - MCP Tools

    Provides write access to workspace files.
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
        self.name = "File Write Service"
        self.version = "1.0.0"
        logger.info("FileWriteService initialized")

    @func_description(WRITE_FILE_TOOL_DESCRIPTION)
    async def write_file(
        self,
        file_path: str,
        content: str,
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        try:
            backend, _, _ = get_backend_with_session_info(user_info)
            await backend.awrite(file_path, content)
            return f"Successfully created file: {file_path}"
        except Exception as e:
            logger.error(f"write_file failed for {file_path}: {e}")
            return f"Error: {e}"

    @func_description(EDIT_FILE_TOOL_DESCRIPTION)
    async def edit_file(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False,
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        # Handle None value from caller (fallback to default)
        if replace_all is None:
            replace_all = False

        try:
            backend, _, _ = get_backend_with_session_info(user_info)
            result = await backend.aedit(file_path, old_string, new_string, replace_all=replace_all)
            # result is an EditResult object, convert to string
            return str(result)
        except FileNotFoundError:
            return f"Error: File not found: {file_path}"
        except Exception as e:
            logger.error(f"edit_file failed for {file_path}: {e}")
            return f"Error: {e}"

