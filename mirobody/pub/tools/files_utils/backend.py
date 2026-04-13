"""
Backend Utilities

Provides shared backend creation and user_info validation functions
for MCP tool services.

Key design: backends and sandboxes are cached per session to ensure
all tool calls (write_file, read_file, ls, execute, etc.) within the
same session share the same PostgresBackend instance. This guarantees
consistent data visibility — a file written by write_file is immediately
visible to ls, grep, glob, and execute without relying on DB round-trips.
"""

import logging
import os
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Default session_id for MCP tools when not provided
DEFAULT_MCP_SESSION = "mcp"

# Cache sandbox backends per user to avoid creating new E2B sandboxes
# on every tool call (E2B has a concurrent sandbox limit).
# Key: user_id, Value: E2BSandboxBackend instance
_sandbox_cache: Dict[str, Any] = {}

# Cache PostgresBackend instances per (session_id, user_id) so all tool
# calls in the same session share one backend (same store, same cache).
# Without this, each tool call creates a fresh backend + store, causing
# data written by write_file to be invisible to ls/glob/grep until the
# next DB round-trip completes — a race condition under async execution.
# Key: (session_id, user_id), Value: PostgresBackend instance
_backend_cache: Dict[tuple, Any] = {}


def _get_sandbox_backend(user_id: str):
    """Get or create a cached sandbox backend for this user.

    Reuses the same E2B sandbox across tool calls for the same user,
    avoiding the concurrent sandbox limit (default: 20).

    Returns:
        E2BSandboxBackend instance if E2B_API_KEY is set, else None.
    """
    api_key = os.environ.get("E2B_API_KEY", "")
    if not api_key:
        return None

    # Return cached sandbox if available
    if user_id in _sandbox_cache:
        return _sandbox_cache[user_id]

    try:
        from ...agents.deep.e2b_backend import E2BSandboxBackend
        sandbox = E2BSandboxBackend(api_key=api_key)
        _sandbox_cache[user_id] = sandbox
        return sandbox
    except Exception as e:
        logger.warning(f"Failed to create E2B sandbox backend: {e}")
        return None


def get_backend_with_session_info(user_info: Optional[Dict[str, Any]]) -> Tuple[Any, str, bool]:
    """
    Get or create a cached PostgresBackend instance from user_info.

    All tool calls with the same (session_id, user_id) share one backend
    instance, ensuring write_file data is immediately visible to ls/grep/etc.

    If E2B_API_KEY is set, injects E2BSandboxBackend for code execution support.

    Args:
        user_info: Dict containing user_id and optionally session_id

    Returns:
        Tuple of (backend, session_id, used_default_session)
        - backend: PostgresBackend instance (with optional sandbox backend)
        - session_id: The session_id used
        - used_default_session: True if default session was used

    Raises:
        ValueError: If user_id is missing
    """
    if not user_info:
        raise ValueError("user_info is required")

    user_id = user_info.get("user_id")
    if not user_id:
        raise ValueError("user_id is required in user_info")

    # Check if session_id is provided
    provided_session_id = user_info.get("session_id")
    used_default_session = not provided_session_id
    session_id = provided_session_id or DEFAULT_MCP_SESSION

    # Return cached backend if available for this session
    cache_key = (session_id, str(user_id))
    if cache_key in _backend_cache:
        backend = _backend_cache[cache_key]
        # Ensure sandbox is attached (may have been created after backend was cached)
        if backend._sandbox is None:
            sandbox = _get_sandbox_backend(str(user_id))
            if sandbox:
                backend._sandbox = sandbox
        return backend, session_id, used_default_session

    from ...agents.deep.backend import create_postgres_backend

    sandbox = _get_sandbox_backend(str(user_id))
    backend = create_postgres_backend(
        session_id=session_id,
        user_id=str(user_id),
        sandbox_backend=sandbox,
    )
    _backend_cache[cache_key] = backend
    logger.debug(f"Created and cached backend for session={session_id}, user={user_id}")
    return backend, session_id, used_default_session


def get_backend(user_info: Optional[Dict[str, Any]]) -> Any:
    """
    Convenience function that returns only the backend.

    Args:
        user_info: Dict containing user_id

    Returns:
        PostgresBackend instance
    """
    backend, _, _ = get_backend_with_session_info(user_info)
    return backend


def validate_user_info(user_info: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[str], str]:
    """
    Validate user_info for authentication.

    Args:
        user_info: Dict containing user_id

    Returns:
        Tuple of (is_valid, user_id, error_message)
    """
    if not user_info:
        return False, None, "user_info is required"

    user_id = user_info.get("user_id")
    if not user_id:
        return False, None, "user_id is required"

    return True, str(user_id), ""


__all__ = [
    "DEFAULT_MCP_SESSION",
    "get_backend_with_session_info",
    "get_backend",
    "validate_user_info",
]
