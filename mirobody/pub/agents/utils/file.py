"""
Utility functions for agents.

Shared helper functions used by multiple agent implementations.
"""

import base64
import logging
import os
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Default session_id for MCP tools when not provided
# Must match FileReadService.DEFAULT_MCP_SESSION for consistency
DEFAULT_MCP_SESSION = "mcp"


def _build_reminder_message(uploaded_paths: list[str]) -> str:
    """
    Build reminder message for successfully registered files.
    """
    if not uploaded_paths:
        return ""

    if len(uploaded_paths) == 1:
        return (
            f"File registered: {os.path.basename(uploaded_paths[0])}\n"
            f"Path: {uploaded_paths[0]}\n\n"
            f"Use read_file(\"{uploaded_paths[0]}\") to read the content"
        )
    else:
        file_items = [
            f"{i+1}. {os.path.basename(p)}"
            for i, p in enumerate(uploaded_paths)
        ]
        files_text = "\n".join(file_items)
        return (
            f"Registered {len(uploaded_paths)} files:\n{files_text}\n\n"
            f"Example: read_file(\"{uploaded_paths[0]}\")"
        )


async def register_files_to_workspace(
    file_list: list[dict[str, Any]],
    backend: Any,
    files_data: list[dict[str, Any]] | None = None,
) -> Tuple[list[str], str]:
    """
    Register files to workspace for lazy loading on read.

    If files_data contains pre-downloaded content, store it directly.
    Otherwise, store FILE_URL: reference for lazy loading.

    Args:
        file_list: List of file info dicts with keys:
            - file_name: Name of the file
            - file_url: URL to access the file
            - file_type: Type of file (optional)
            - file_key: S3 key (optional)
        backend: PostgresBackend instance
        files_data: Optional pre-downloaded file content list with keys:
            - content: bytes (file binary content)
            - file_name: str
            - file_key: str

    Returns:
        Tuple of (registered_paths, reminder_message)
    """
    if not file_list:
        return [], ""

    # Build lookup map: file_key/file_name -> content (bytes or b64 string)
    content_map = {}
    content_b64_map = {}  # Store pre-encoded base64 to avoid re-encoding
    if files_data:
        logger.debug(f"📦 files_data has {len(files_data)} items")
        for i, fd in enumerate(files_data):
            logger.debug(f"  [{i}] keys: {list(fd.keys())}")
            content = fd.get("content_bytes")
            content_b64 = fd.get("content_b64")  # Pre-encoded base64 from file.py
            if content or content_b64:
                file_key = fd.get("file_key")
                file_name = fd.get("file_name") or fd.get("filename")
                logger.debug(f"  [{i}] file_key={file_key}, file_name={file_name}")
                if file_key:
                    if content:
                        content_map[file_key] = content
                    if content_b64:
                        content_b64_map[file_key] = content_b64
                if file_name:
                    if content:
                        content_map[file_name] = content
                    if content_b64:
                        content_b64_map[file_name] = content_b64
            else:
                logger.warning(f"  [{i}] NO content found!")
        if content_map or content_b64_map:
            logger.info(f"✅ Built content_map with {len(content_map)} entries, content_b64_map with {len(content_b64_map)} entries")
    else:
        logger.debug("files_data is None or empty")

    registered_paths = []

    for file_info in file_list:
        try:
            file_name = file_info.get("file_name")
            file_url = file_info.get("file_url")
            file_type = file_info.get("file_type", "")
            file_key = file_info.get("file_key", "")

            if not file_name:
                logger.warning(f"Skipping file with missing name: {file_info}")
                continue

            # Create workspace file path
            file_path = f"/uploads/{file_name}"

            # Check if we have pre-downloaded content (prefer pre-encoded b64)
            file_content = None
            file_content_b64 = None
            logger.debug(f"🔍 Looking up: file_key='{file_key}', file_name='{file_name}'")

            # Try to get pre-encoded base64 first (avoids re-encoding)
            if file_key and file_key in content_b64_map:
                file_content_b64 = content_b64_map[file_key]
                logger.debug(f"   ✅ Found b64 by file_key")
            elif file_name in content_b64_map:
                file_content_b64 = content_b64_map[file_name]
                logger.debug(f"   ✅ Found b64 by file_name")
            # Fallback to raw bytes
            elif file_key and file_key in content_map:
                file_content = content_map[file_key]
                logger.debug(f"   ✅ Found bytes by file_key")
            elif file_name in content_map:
                file_content = content_map[file_name]
                logger.debug(f"   ✅ Found bytes by file_name")
            else:
                logger.debug(f"   ❌ NOT found in content maps")

            if file_content_b64 or file_content:
                # Use pre-encoded base64 if available, otherwise encode now
                if file_content_b64:
                    raw_content = file_content_b64
                    content_size = len(base64.b64decode(file_content_b64))
                else:
                    raw_content = base64.b64encode(file_content).decode('utf-8')
                    content_size = len(file_content)

                file_data = {
                    "raw_content": raw_content,
                    "file_type": file_type,
                    "file_key": file_key,
                    "lazy_load": True,
                    "parsed": False,
                    "content": [],  # Empty - will be filled after parsing
                }
                logger.info(f"📝 Registered file with content: {file_path} ({content_size} bytes)")
            elif file_url:
                # Store FILE_URL: reference for lazy downloading AND parsing
                file_data = {
                    "content": [f"FILE_URL:{file_url}"],
                    "file_type": file_type,
                    "file_key": file_key,
                    "lazy_load": True,
                    "parsed": False,
                }
                logger.info(f"📝 Registered file for lazy download: {file_path} -> {file_url}")
            else:
                logger.warning(f"Skipping file with no content or URL: {file_name}")
                continue

            # Write file data to workspace (async call - persist to database)
            await backend._put_file_data_async(file_path, file_data, persist=True)
            registered_paths.append(file_path)

        except Exception as e:
            logger.error(f"Failed to register file {file_info.get('file_name', 'unknown')}: {e}")

    reminder = _build_reminder_message(registered_paths)
    return registered_paths, reminder


async def handle_file_upload(
    file_list: list[dict[str, Any]] | None,
    session_id: str = "",
    user_id: str = "",
    files_data: list[dict[str, Any]] | None = None,
    backend: Optional[Any] = None,
) -> Tuple[list[str], str]:
    """
    Handle file upload to workspace (lazy parsing - no immediate parsing).

    If files_data contains pre-downloaded content, it's stored directly (avoids re-download).
    Otherwise, files are registered with their URLs for lazy downloading.
    Actual parsing happens on first read_file() call.

    Args:
        file_list: List of file info dicts from the request.
        session_id: Session ID for workspace isolation. If empty, uses default "mcp" session.
        user_id: User ID for workspace isolation (required if backend not provided).
        files_data: Pre-downloaded file content (from HTTP layer, avoids re-download).
        backend: Pre-created PostgresBackend instance (optional).
                 If provided, session_id and user_id are not required.

    Returns:
        Tuple of (uploaded_paths, reminder_message):
        - uploaded_paths: List of successfully registered file paths
        - reminder_message: Message to notify LLM about uploaded files (empty if no files)

    Note:
        When session_id is empty, the default "mcp" session is used. This matches
        the behavior of FileReadService MCP tools, ensuring files registered here
        can be read by MCP tools even when session_id is not provided.
    """
    if not file_list:
        return [], ""

    try:
        # Use provided backend or create one
        if backend is None:
            # Use default "mcp" session if not provided (matches FileReadService default)
            effective_session_id = session_id or DEFAULT_MCP_SESSION
            if not session_id:
                logger.info(f"No session_id provided, using default session '{DEFAULT_MCP_SESSION}'")

            from ..deep.backend import create_postgres_backend
            backend = create_postgres_backend(
                session_id=effective_session_id,
                user_id=user_id
            )

        # Register files for lazy loading (no parsing)
        registered_paths, reminder_message = await register_files_to_workspace(
            file_list,
            backend,
            files_data=files_data,
        )

        if registered_paths:
            logger.info(f"📝 Registered {len(registered_paths)} files for lazy loading")
            return registered_paths, reminder_message or ""
        else:
            logger.warning("No files were successfully registered")
            return [], ""

    except Exception as e:
        logger.error(f"Error registering files: {str(e)}", exc_info=True)
        return [], ""
