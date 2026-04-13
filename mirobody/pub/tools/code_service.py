"""
Code Execution Service - MCP Tool for sandbox code execution.

Provides:
- execute: Run shell commands in sandbox (requires E2B_API_KEY)

Only registered when E2B_API_KEY is configured. When not configured,
the execute tool is completely hidden from tool listings.

References:
- deepagents.backends.protocol.SandboxBackendProtocol — execute interface
- deepagents.middleware.filesystem._create_execute_tool — tool creation pattern
"""

import logging
from typing import Any, Dict, Optional

from .files_utils import (
    func_description,
    get_backend_with_session_info,
    validate_user_info,
    maybe_evict_large_result,
    EXECUTE_TOOL_DESCRIPTION,
)

logger = logging.getLogger(__name__)

DEFAULT_EXECUTE_TIMEOUT = 60  # seconds
MAX_EXECUTE_TIMEOUT = 120  # seconds


class CodeService:
    """
    Code Execution Service - MCP Tool

    Provides sandbox code execution via the execute tool.
    Only registered when E2B_API_KEY is configured.

    user_info structure:
        {
            "user_id": str,
            "session_id": str,
            "token": str,
            ...
        }
    """

    def __init__(self):
        self.name = "Code Execution Service"
        self.version = "1.0.0"
        logger.info("CodeService initialized")

    @staticmethod
    def _enabled() -> bool:
        """Only register when E2B_API_KEY is configured."""
        import os
        from mirobody.utils import global_config
        config = global_config()
        # Check both config and environment variable
        key = (config.get_str("E2B_API_KEY") if config else "") or os.environ.get("E2B_API_KEY", "")
        return bool(key)

    @func_description(EXECUTE_TOOL_DESCRIPTION)
    async def execute(
        self,
        command: str,
        timeout: int = DEFAULT_EXECUTE_TIMEOUT,
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        """Execute a shell command in the sandbox environment.

        Delegates to backend.aexecute(command) which routes to the configured
        sandbox backend (E2B).

        Output format follows the deepagents middleware pattern from
        _create_execute_tool().
        """
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            return f"Error: {error}"

        if not command or not str(command).strip():
            return "Error: command parameter is required and must not be empty."

        # Clamp timeout to valid range
        if timeout is None:
            timeout = DEFAULT_EXECUTE_TIMEOUT
        timeout = min(max(int(timeout), 1), MAX_EXECUTE_TIMEOUT)

        try:
            backend, _, _ = get_backend_with_session_info(user_info)

            # Delegate to backend (SandboxBackendProtocol.aexecute)
            result = await backend.aexecute(command, timeout=timeout)

            # Format output (matching deepagents middleware pattern)
            parts = [result.output]

            if result.exit_code is not None:
                status = "succeeded" if result.exit_code == 0 else "failed"
                parts.append(f"\n[Command {status} with exit code {result.exit_code}]")

            if result.truncated:
                parts.append("\n[Output was truncated due to size limits]")

            output = "\n".join(parts)

            # Evict large results to filesystem
            return await maybe_evict_large_result(backend, output, "execute")

        except Exception as e:
            logger.error(f"execute failed: {e}", exc_info=True)
            return f"Error: {e}"
