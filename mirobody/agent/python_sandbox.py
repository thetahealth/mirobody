"""The local Docker transport for Deep Agents' sandbox interface.

The SDK owns container transport; BaseSandbox owns filesystem helpers.
Each calculation has a disposable, networkless container with no application
mounts or credentials. Only a Unix-socket Docker daemon is admitted, so a
remote Docker context cannot move computation off this machine.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import socket
import subprocess
import tarfile
import threading
from pathlib import Path, PurePosixPath
from uuid import uuid4

import docker
from docker.utils.socket import frames_iter
from deepagents.backends.protocol import ExecuteResponse, FileDownloadResponse, FileUploadResponse
from deepagents.backends.sandbox import BaseSandbox

logger = logging.getLogger(__name__)

_CHUNK_CHARS = 30000
_WORKSPACE = PurePosixPath("/workspace")
_FILE_LIMIT = 8 * 1024 * 1024
_RUNNER = b"""import json, runpy
with open('/workspace/data.json', encoding='utf-8') as handle:
    inputs = json.load(handle)
runpy.run_path('/workspace/main.py', run_name='__main__', init_globals=inputs)
"""


def _local_endpoint() -> str:
    host = os.environ.get("PYTHON_SANDBOX_DOCKER_HOST") or os.environ.get("DOCKER_HOST")
    if not host:
        if Path("/var/run/docker.sock").is_socket():
            host = "unix:///var/run/docker.sock"
        else:
            host = subprocess.check_output(
                ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
                text=True, timeout=5,
            ).strip()
    if not host.startswith("unix:///"):
        raise ValueError("Python computation requires a local Unix-socket Docker daemon")
    return host


class LocalPythonSandbox(BaseSandbox):
    """One local calculation, with standard Deep Agents execution results."""

    def __init__(self, *, image: str | None = None, timeout: int = 15, max_output: int = 40_000) -> None:
        self.image = image or os.environ.get("PYTHON_SANDBOX_IMAGE") or "python:3.12-slim"
        self.timeout = max(1, min(int(timeout), 30))
        self.max_output = max(1, int(max_output))
        self._name = f"mirobody-python-{uuid4().hex}"
        self._client = docker.DockerClient(base_url=_local_endpoint(), timeout=self.timeout + 5)
        self._container = None
        self._closed = threading.Event()
        self._active_stream = None

    @property
    def id(self) -> str:
        return self._name

    def start(self) -> None:
        self._container = self._client.containers.run(
            self.image, ["sleep", str(self.timeout + 60)], name=self._name, detach=True, auto_remove=True,
            user="65534:65534", network_disabled=True, read_only=True,
            use_config_proxy=False,
            tmpfs={"/workspace": "rw,noexec,nosuid,size=64m,uid=65534,gid=65534",
                   "/tmp": "rw,noexec,nosuid,size=64m,uid=65534,gid=65534"},
            working_dir=str(_WORKSPACE), cap_drop=["ALL"],
            security_opt=["no-new-privileges"], pids_limit=64,
            mem_limit="512m", nano_cpus=1_000_000_000,
            labels={"mirobody.python_sandbox": "true"},
        )
        # Cancellation can arrive while the SDK is creating the container.
        if self._closed.is_set():
            self.close()
            raise RuntimeError("Python execution was cancelled")

    def close(self) -> None:
        self._closed.set()
        if self._active_stream is not None:
            try:
                self._active_stream.close()
            except OSError:
                pass
        try:
            self._client.containers.get(self._name).remove(force=True)
        except docker.errors.NotFound:
            pass
        except Exception as exc:
            logger.warning("python container cleanup failed: error_type=%s", type(exc).__name__)

    def _command(self, command: list[str], *, timeout: int | None = None,
                 stdin: bytes | None = None, limit: int | None = None) -> tuple[bytes, int | None, bool]:
        seconds = self.timeout if timeout is None else max(1, min(int(timeout), 30))
        budget = self.max_output if limit is None else limit
        stream = None
        connection = None
        try:
            if self._closed.is_set():
                raise RuntimeError("Python execution was cancelled")
            call = self._client.api.exec_create(
                self._container.id,
                ["timeout", "--kill-after=1", str(seconds), *command],
                user="65534:65534", workdir=str(_WORKSPACE), stdin=stdin is not None,
            )
            if stdin is None:
                stream = self._client.api.exec_start(call["Id"], stream=True)
            else:
                connection = self._client.api.exec_start(call["Id"], socket=True)
                connection._sock.sendall(stdin)
                connection._sock.shutdown(socket.SHUT_WR)
                stream = (chunk for _, chunk in frames_iter(connection, tty=False))
            self._active_stream = connection if connection is not None else stream
            output = bytearray()
            truncated = False
            for chunk in stream:
                remaining = budget - len(output)
                output.extend(chunk[:remaining])
                truncated |= len(chunk) > remaining
            code = self._client.api.exec_inspect(call["Id"])["ExitCode"]
            return bytes(output), code, truncated
        finally:
            self._active_stream = None
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass
            if connection is not None:
                connection.close()

    def execute(self, command: str, *, timeout: int | None = None) -> ExecuteResponse:
        try:
            output, code, truncated = self._command(["/bin/sh", "-c", command], timeout=timeout)
            return ExecuteResponse(output.decode("utf-8", errors="replace"), code, truncated)
        except Exception as exc:
            logger.warning("python container execution failed: error_type=%s", type(exc).__name__)
            return ExecuteResponse(f"error: local Python executor unavailable ({type(exc).__name__})", 1)

    @staticmethod
    def _relative(path: str) -> str:
        target = PurePosixPath(path)
        if ".." in target.parts or not target.is_relative_to(_WORKSPACE) or target == _WORKSPACE:
            raise ValueError("file path is outside the calculation workspace")
        return str(target.relative_to(_WORKSPACE))

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        responses = []
        for path, content in files:
            try:
                relative = self._relative(path)
                if len(content) > _FILE_LIMIT:
                    raise ValueError("input file exceeds the calculation budget")
                archive = io.BytesIO()
                with tarfile.open(fileobj=archive, mode="w") as bundle:
                    info = tarfile.TarInfo(relative)
                    info.size, info.mode, info.uid, info.gid = len(content), 0o600, 65534, 65534
                    bundle.addfile(info, io.BytesIO(content))
                # Docker's archive API cannot see a live tmpfs on a read-only
                # rootfs (Docker 29 returns 400); exec writes into the actual mount.
                # ``exec_run(stdin=...)`` depends on the docker SDK and daemon
                # agreeing on an attach socket; with some pairs it returns ``None``
                # instead of an exit code and the payload never arrives, so every
                # calculation answers "inputs could not be transferred". A base64 body
                # over a plain command needs neither an attach socket nor a writable
                # rootfs (``put_archive`` is refused on this read-only container).
                #
                # It is written in chunks: one argv holds a single argument at most
                # ~128 KiB on Linux, and a whole payload in one ``printf`` failed above
                # 64 KiB of input long before the 8 MiB transfer budget.
                archive.seek(0)
                payload = base64.b64encode(archive.getvalue()).decode("ascii")
                stage = "/tmp/.mirobody-upload.b64"
                _, code, _ = self._command(["/bin/sh", "-c", f": > {stage}"])
                for start in range(0, len(payload), _CHUNK_CHARS):
                    chunk = payload[start:start + _CHUNK_CHARS]
                    _, code, _ = self._command(["/bin/sh", "-c", f"printf %s '{chunk}' >> {stage}"])
                    if code != 0:
                        raise OSError("input transfer failed")
                script = (f"base64 -d {stage} | tar -xf - -C '{_WORKSPACE}' && rm -f {stage}")
                _, code, _ = self._command(["/bin/sh", "-c", script])
                if code != 0:
                    raise OSError("input transfer failed")
                responses.append(FileUploadResponse(path))
            except Exception as exc:
                responses.append(FileUploadResponse(path, error=type(exc).__name__))
        return responses

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        responses = []
        for path in paths:
            try:
                relative = self._relative(path)
                output, code, truncated = self._command(
                    ["tar", "-cf", "-", "-C", str(_WORKSPACE), relative], limit=_FILE_LIMIT + 64 * 1024,
                )
                if code != 0 or truncated:
                    raise OSError("output transfer failed or exceeded its budget")
                archive = io.BytesIO(output)
                with tarfile.open(fileobj=archive) as bundle:
                    member = next(m for m in bundle if m.isfile())
                    with bundle.extractfile(member) as handle:
                        content = handle.read(_FILE_LIMIT + 1)
                if len(content) > _FILE_LIMIT:
                    raise ValueError("output file exceeds the calculation budget")
                responses.append(FileDownloadResponse(path, content))
            except docker.errors.NotFound:
                responses.append(FileDownloadResponse(path, error="file_not_found"))
            except Exception as exc:
                responses.append(FileDownloadResponse(path, error=type(exc).__name__))
        return responses

    def run(self, code: str, inputs: dict) -> ExecuteResponse:
        try:
            self.start()
            uploads = self.upload_files([
                ("/workspace/data.json", json.dumps(inputs, ensure_ascii=False, default=str).encode()),
                ("/workspace/main.py", code.encode()),
                ("/workspace/runner.py", _RUNNER),
            ])
            if any(result.error for result in uploads):
                return ExecuteResponse("error: calculation inputs could not be transferred", 1)
            return self.execute("python -I -B /workspace/runner.py")
        finally:
            self.close()
            self._client.close()
