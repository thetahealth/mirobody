"""E2B Sandbox Backend: Execute code in E2B cloud sandbox.

Extends BaseSandbox — only need to implement execute(), upload_files(),
download_files(), and id property. All file operations (read/write/edit/
grep/glob/ls) are automatically handled via shell commands through execute().

References:
- deepagents.backends.sandbox.BaseSandbox
- deepagents.backends.local_shell.LocalShellBackend
- e2b_code_interpreter.AsyncSandbox
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING

from deepagents.backends.protocol import (
    ExecuteResponse,
    FileDownloadResponse,
    FileUploadResponse,
)
from deepagents.backends.sandbox import BaseSandbox

if TYPE_CHECKING:
    from e2b_code_interpreter import AsyncSandbox

logger = logging.getLogger(__name__)

DEFAULT_SANDBOX_TIMEOUT = 300  # seconds — sandbox lifetime
DEFAULT_EXECUTE_TIMEOUT = 60  # seconds — per-command timeout
MAX_OUTPUT_BYTES = 100_000  # matching LocalShellBackend.max_output_bytes


class E2BSandboxBackend(BaseSandbox):
    """E2B cloud sandbox backend implementing SandboxBackendProtocol.

    Lazily creates an E2B sandbox on first use. All file operations
    (read/write/edit/grep/glob/ls) inherited from BaseSandbox are
    automatically executed via shell commands through execute().

    Args:
        api_key: E2B API key. Falls back to E2B_API_KEY env var.
        sandbox_timeout: Sandbox lifetime in seconds. Default: 300.
        execute_timeout: Default per-command timeout in seconds. Default: 60.
    """

    def __init__(
        self,
        api_key: str | None = None,
        sandbox_timeout: int = DEFAULT_SANDBOX_TIMEOUT,
        execute_timeout: int = DEFAULT_EXECUTE_TIMEOUT,
    ) -> None:
        self._api_key = api_key or os.environ.get("E2B_API_KEY", "")
        self._sandbox_timeout = sandbox_timeout
        self._default_execute_timeout = execute_timeout
        self._sandbox: AsyncSandbox | None = None
        self._sandbox_id: str | None = None

    @property
    def id(self) -> str:
        """Unique identifier for this sandbox instance."""
        return self._sandbox_id or "e2b-not-started"

    # =========================================================================
    # Sandbox Lifecycle
    # =========================================================================

    async def _ensure_sandbox(self) -> None:
        """Lazy-create E2B sandbox on first use."""
        if self._sandbox is not None:
            return

        if not self._api_key:
            raise RuntimeError(
                "E2B_API_KEY is required. Set it as an environment variable "
                "or pass api_key to E2BSandboxBackend()."
            )

        from e2b_code_interpreter import AsyncSandbox

        self._sandbox = await AsyncSandbox.create(
            api_key=self._api_key,
            timeout=self._sandbox_timeout,
        )
        self._sandbox_id = self._sandbox.sandbox_id
        logger.info(f"E2B sandbox created: {self._sandbox_id}")

    async def cleanup(self) -> None:
        """Kill the sandbox and release resources."""
        if self._sandbox:
            try:
                await self._sandbox.kill()
                logger.info(f"E2B sandbox killed: {self._sandbox_id}")
            except Exception:
                logger.warning("Failed to kill E2B sandbox", exc_info=True)
            finally:
                self._sandbox = None
                self._sandbox_id = None

    # =========================================================================
    # execute() — the only abstract method from BaseSandbox
    # =========================================================================

    def execute(
        self,
        command: str,
        *,
        timeout: int | None = None,
    ) -> ExecuteResponse:
        """Sync execute — wraps async aexecute via asyncio.

        References:
        - deepagents.backends.local_shell.LocalShellBackend.execute()
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Already in an async context — run in a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self.aexecute(command, timeout=timeout))
                return future.result()
        else:
            return asyncio.run(self.aexecute(command, timeout=timeout))

    async def aexecute(
        self,
        command: str,
        *,
        timeout: int | None = None,
    ) -> ExecuteResponse:
        """Execute a shell command in the E2B sandbox.

        Output format matches LocalShellBackend: stderr lines prefixed with
        [stderr], output truncated at MAX_OUTPUT_BYTES.

        Args:
            command: Shell command string to execute.
            timeout: Per-command timeout in seconds. Default: 60.

        Returns:
            ExecuteResponse with combined output, exit code, and truncation flag.
        """
        if not command or not isinstance(command, str):
            return ExecuteResponse(
                output="Error: Command must be a non-empty string.",
                exit_code=1,
                truncated=False,
            )

        await self._ensure_sandbox()
        effective_timeout = timeout or self._default_execute_timeout

        try:
            result = await self._sandbox.commands.run(
                command,
                timeout=effective_timeout,
            )
        except TimeoutError:
            return ExecuteResponse(
                output=f"Error: Command timed out after {effective_timeout} seconds.",
                exit_code=124,
                truncated=False,
            )
        except Exception as e:
            logger.error(f"E2B execute failed: {e}", exc_info=True)
            return ExecuteResponse(
                output=f"Error: E2B execution failed: {e}",
                exit_code=1,
                truncated=False,
            )

        # Format output (aligned with LocalShellBackend pattern)
        output_parts: list[str] = []
        if result.stdout:
            output_parts.append(result.stdout)
        if result.stderr:
            for line in result.stderr.strip().split("\n"):
                if line:
                    output_parts.append(f"[stderr] {line}")

        output = "\n".join(output_parts) if output_parts else "<no output>"

        # Truncate if needed
        truncated = False
        if len(output) > MAX_OUTPUT_BYTES:
            output = output[:MAX_OUTPUT_BYTES]
            output += f"\n\n... Output truncated at {MAX_OUTPUT_BYTES} bytes."
            truncated = True

        return ExecuteResponse(
            output=output,
            exit_code=result.exit_code,
            truncated=truncated,
        )

    # =========================================================================
    # upload_files / download_files — abstract from BaseSandbox
    # =========================================================================

    def upload_files(
        self, files: list[tuple[str, bytes]]
    ) -> list[FileUploadResponse]:
        """Upload files to E2B sandbox filesystem (sync wrapper)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self.aupload_files(files))
                return future.result()
        else:
            return asyncio.run(self.aupload_files(files))

    async def aupload_files(
        self, files: list[tuple[str, bytes]]
    ) -> list[FileUploadResponse]:
        """Upload files to E2B sandbox filesystem."""
        await self._ensure_sandbox()
        responses: list[FileUploadResponse] = []
        for path, content in files:
            try:
                await self._sandbox.files.write(path, content)
                responses.append(FileUploadResponse(path=path, error=None))
            except Exception as e:
                logger.warning(f"Failed to upload {path} to E2B: {e}")
                responses.append(FileUploadResponse(path=path, error="invalid_path"))
        return responses

    def download_files(
        self, paths: list[str]
    ) -> list[FileDownloadResponse]:
        """Download files from E2B sandbox filesystem (sync wrapper)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, self.adownload_files(paths))
                return future.result()
        else:
            return asyncio.run(self.adownload_files(paths))

    async def adownload_files(
        self, paths: list[str]
    ) -> list[FileDownloadResponse]:
        """Download files from E2B sandbox filesystem."""
        await self._ensure_sandbox()
        responses: list[FileDownloadResponse] = []
        for path in paths:
            try:
                content = await self._sandbox.files.read(path, format="bytes")
                responses.append(
                    FileDownloadResponse(path=path, content=bytes(content))
                )
            except Exception:
                logger.warning(f"Failed to download {path} from E2B")
                responses.append(
                    FileDownloadResponse(
                        path=path, content=None, error="file_not_found"
                    )
                )
        return responses


__all__ = ["E2BSandboxBackend"]
