"""One read-only file, projected from a document produced on demand.

A health profile, a memory document, a rendered summary: several things an
agent should be able to `ls` and `read_file` are one document that some other
process writes and this process only reads. The deepagents `BackendProtocol`
needs eight methods to serve that; the eight are the same every time and only
the document differs. So the protocol lives here once, and a backend is a
subclass with a `document()` — or an instance handed a coroutine function.

Two rules that came from production and are easy to get wrong again:

* An empty document lists NOTHING rather than an empty file. Otherwise the
  model sees a file, reads it expecting content, and reasons from nothing.
* `adownload_files` — the method deepagents' MemoryMiddleware actually calls,
  not `aread` — must answer a missing document with the error string
  ``file_not_found`` exactly. The middleware skips a source that says that and
  raises on anything else, so a user who simply has no document yet would
  otherwise take the conversation down with them.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Awaitable, Callable

from deepagents.backends.protocol import (
    BackendProtocol,
    EditResult,
    FileData,
    FileDownloadResponse,
    FileInfo,
    FileUploadResponse,
    GlobResult,
    GrepMatch,
    GrepResult,
    LsResult,
    ReadResult,
    WriteResult,
)
from deepagents.backends.utils import create_file_data, slice_read_response

_DEFAULT_READONLY = (
    "This path is a read-only view of a document that is written out of band. "
    "Write scratch notes to the workspace root (/) instead."
)


class DocumentBackend(BackendProtocol):
    """Serve one document as ``/<filename>``. Subclass and override
    `document()`, or pass ``document=`` a coroutine function."""

    filename: str = "document.md"
    readonly_message: str = _DEFAULT_READONLY

    def __init__(
        self,
        *,
        filename: str | None = None,
        document: Callable[[], Awaitable[str]] | None = None,
        readonly_message: str | None = None,
    ):
        if filename:
            self.filename = filename
        if readonly_message:
            self.readonly_message = readonly_message
        self._document_fn = document

    async def document(self) -> str:
        """The current document, or ``""`` when there is none."""
        if self._document_fn is None:
            raise NotImplementedError("subclass DocumentBackend with document(), or pass document=")
        return await self._document_fn()

    # ── helpers ─────────────────────────────────────────────────────────────

    def _path(self) -> str:
        return f"/{self.filename}"

    def _info(self, size: int = 0) -> FileInfo:
        return {"path": self._path(), "is_dir": False, "size": size}  # type: ignore[return-value]

    def _is_this_file(self, path: str | None) -> bool:
        name = (path or "").lstrip("/")
        return not name or name == self.filename

    # ── reads ────────────────────────────────────────────────────────────────

    async def als(self, path: str) -> LsResult:
        doc = await self.document()
        return LsResult(entries=[self._info(len(doc))] if doc else [])

    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> ReadResult:
        if not self._is_this_file(file_path):
            return ReadResult(error=f"Entry '{file_path}' not found.")
        doc = await self.document()
        if not doc:
            return ReadResult(error=f"No {self.filename} has been written for this user yet.")
        data = create_file_data(doc)
        n_lines = len(doc.splitlines())
        if offset and offset >= n_lines:
            return ReadResult(file_data=FileData(
                content=f"(end of {self.filename} — {n_lines} line(s) total.)",
                encoding=data.get("encoding", "utf-8"),
            ))
        sliced = slice_read_response(data, offset, limit)
        if isinstance(sliced, ReadResult):
            return sliced
        return ReadResult(file_data=FileData(content=sliced, encoding=data.get("encoding", "utf-8")))

    async def aglob(self, pattern: str, path: str | None = None) -> GlobResult:
        doc = await self.document()
        if not doc:
            return GlobResult(matches=[])
        hit = fnmatch.fnmatch(self.filename, (pattern or "").lstrip("/")) or fnmatch.fnmatch(self._path(), pattern or "")
        return GlobResult(matches=[self._info(len(doc))] if hit else [])

    async def agrep(self, pattern: str, path: str | None = None, glob: str | None = None) -> GrepResult:
        doc = await self.document()
        if not doc:
            return GrepResult(matches=[])
        return GrepResult(matches=[
            GrepMatch(path=self._path(), line_number=i, line=line[:500])
            for i, line in enumerate(doc.splitlines(), start=1)
            if pattern in line
        ])

    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        doc = await self.document()
        out: list[FileDownloadResponse] = []
        for path in paths or []:
            if not self._is_this_file(path) or not doc:
                out.append(FileDownloadResponse(path=path, content=None, error="file_not_found"))
            else:
                out.append(FileDownloadResponse(path=path, content=doc.encode("utf-8"), error=None))
        return out

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """The projection is async-only; an explicit refusal reads better than
        an inherited NotImplementedError that looks like an oversight."""
        raise NotImplementedError(f"{type(self).__name__} is async-only; use adownload_files")

    # ── writes are refused ───────────────────────────────────────────────────

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        return WriteResult(error=self.readonly_message)

    async def aedit(self, file_path: str, old_string: str, new_string: str, replace_all: bool = False) -> EditResult:
        return EditResult(error=self.readonly_message)

    async def aupload_files(self, files) -> list[FileUploadResponse]:
        return [FileUploadResponse(path=getattr(f, "path", ""), error=self.readonly_message) for f in (files or [])]
