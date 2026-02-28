"""
Backend Utilities

Provides shared backend creation and user_info validation functions
for MCP tool services.
"""

import logging
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Default session_id for MCP tools when not provided
DEFAULT_MCP_SESSION = "mcp"


def get_backend_with_session_info(user_info: Optional[Dict[str, Any]]) -> Tuple[Any, str, bool]:
    """
    Create PostgresBackend instance from user_info.

    Args:
        user_info: Dict containing user_id and optionally session_id

    Returns:
        Tuple of (backend, session_id, used_default_session)
        - backend: PostgresBackend instance
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

    from ...agents.deep.backend import create_postgres_backend

    backend = create_postgres_backend(session_id=session_id, user_id=user_id)
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
