"""
Files Utils - Shared utilities for MCP file tool services.

This package provides:
- Backend creation and user validation (backend.py)
- deepagents compatibility constants (compat.py)
- Large result eviction (eviction.py)
- Global files database operations (global_files.py)

Usage:
    from .files_utils import (
        # Backend
        get_backend,
        get_backend_with_session_info,
        validate_user_info,
        # Eviction
        maybe_evict_large_result,
        # Global files
        list_global_files_from_db,
        get_file_info_from_file_key,
        create_file_reference,
        # Constants
        READ_FILE_TOOL_DESCRIPTION,
        func_description,
    )
"""

# Backend utilities
from .backend import (
    DEFAULT_MCP_SESSION,
    get_backend,
    get_backend_with_session_info,
    validate_user_info,
)

# Eviction utilities
from .eviction import (
    create_content_preview,
    maybe_evict_large_result,
)

# Global files utilities
from .global_files import (
    get_file_type,
    sanitize_filename,
    get_file_info_from_file_key,
    list_global_files_from_db,
    create_file_reference,
)

# Compatibility layer (constants + decorator)
from .compat import (
    # Tool descriptions (deepagents/langchain)
    LIST_FILES_TOOL_DESCRIPTION,
    READ_FILE_TOOL_DESCRIPTION,
    EDIT_FILE_TOOL_DESCRIPTION,
    WRITE_FILE_TOOL_DESCRIPTION,
    GLOB_TOOL_DESCRIPTION,
    GREP_TOOL_DESCRIPTION,
    WRITE_TODOS_TOOL_DESCRIPTION,
    # Global files descriptions (mirobody)
    LIST_GLOBAL_FILES_DESCRIPTION,
    FETCH_REMOTE_FILES_DESCRIPTION,
    # System prompts
    FILESYSTEM_SYSTEM_PROMPT,
    EXECUTION_SYSTEM_PROMPT,
    # Image support
    IMAGE_EXTENSIONS,
    IMAGE_MEDIA_TYPES,
    # Truncation
    READ_FILE_TRUNCATION_MSG,
    NUM_CHARS_PER_TOKEN,
    EMPTY_CONTENT_WARNING,
    # Defaults
    DEFAULT_READ_OFFSET,
    DEFAULT_READ_LIMIT,
    # Feature flags
    ENABLE_IMAGE_MULTIMODAL,
    DEFAULT_EVICTION_TOKEN_LIMIT,
    # Decorator
    func_description,
)

__all__ = [
    # Backend
    "DEFAULT_MCP_SESSION",
    "get_backend",
    "get_backend_with_session_info",
    "validate_user_info",
    # Eviction
    "create_content_preview",
    "maybe_evict_large_result",
    # Global files
    "get_file_type",
    "sanitize_filename",
    "get_file_info_from_file_key",
    "list_global_files_from_db",
    "create_file_reference",
    # Tool descriptions (deepagents/langchain)
    "LIST_FILES_TOOL_DESCRIPTION",
    "READ_FILE_TOOL_DESCRIPTION",
    "EDIT_FILE_TOOL_DESCRIPTION",
    "WRITE_FILE_TOOL_DESCRIPTION",
    "GLOB_TOOL_DESCRIPTION",
    "GREP_TOOL_DESCRIPTION",
    "WRITE_TODOS_TOOL_DESCRIPTION",
    # Global files descriptions (mirobody)
    "LIST_GLOBAL_FILES_DESCRIPTION",
    "FETCH_REMOTE_FILES_DESCRIPTION",
    # System prompts
    "FILESYSTEM_SYSTEM_PROMPT",
    "EXECUTION_SYSTEM_PROMPT",
    # Image support
    "IMAGE_EXTENSIONS",
    "IMAGE_MEDIA_TYPES",
    # Truncation
    "READ_FILE_TRUNCATION_MSG",
    "NUM_CHARS_PER_TOKEN",
    "EMPTY_CONTENT_WARNING",
    # Defaults
    "DEFAULT_READ_OFFSET",
    "DEFAULT_READ_LIMIT",
    # Feature flags
    "ENABLE_IMAGE_MULTIMODAL",
    "DEFAULT_EVICTION_TOKEN_LIMIT",
    # Decorator
    "func_description",
]
