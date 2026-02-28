"""PostgreSQL Backend: Store files in PostgreSQL with intelligent parsing.

Persistent storage across sessions using PostgresLangGraphStore, with lazy file
parsing (PDF, DOCX, images) and local caching for performance optimization.
"""

import asyncio
import atexit
import base64
import concurrent.futures
import fnmatch
import io
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional
from cachetools import TTLCache

from deepagents.backends.protocol import (
    BackendProtocol,
    FileInfo,
    FileUploadResponse,
    FileDownloadResponse,
    EditResult,
    WriteResult,
    GrepMatch,
)
from deepagents.backends.utils import (
    format_content_with_line_numbers,
    check_empty_content,
    perform_string_replacement,
)

from .utils import get_file_type
from ..utils import CACHE_TTL_GLOBAL, CACHE_MAX_FILES, CACHE_MAX_WORKERS

MAX_WAIT_TIME = 15

if TYPE_CHECKING:
    from .store import PostgresLangGraphStore
    from .parser import FileParser

logger = logging.getLogger(__name__)

_GLOBAL_FILE_CACHE = TTLCache(maxsize=CACHE_MAX_FILES, ttl=CACHE_TTL_GLOBAL)

# Shared thread pool for running async code in sync context
# Avoids creating a new ThreadPoolExecutor on each _run_async call
_ASYNC_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=CACHE_MAX_WORKERS,
    thread_name_prefix="pg_backend_async_"
)
atexit.register(_ASYNC_EXECUTOR.shutdown, wait=False)

class PostgresBackend(BackendProtocol):
    """PostgreSQL backend with sync-first design (like FilesystemBackend).

    All public methods are synchronous. Async database operations are
    encapsulated in PostgresLangGraphStore. The only async operations
    remaining are file parsing and external file downloads.
    """
    
    def __init__(
        self,
        session_id: str,
        user_id: str,
        store: "PostgresLangGraphStore",
        file_parser: Optional["FileParser"] = None,
        cache_ttl: int = 600,
    ):
        """
        Initialize PostgresBackend.
        
        Args:
            session_id: Session ID for namespace isolation
            user_id: User ID for namespace isolation
            store: PostgresLangGraphStore instance
            file_parser: FileParser instance for intelligent parsing
            cache_ttl: Cache TTL in seconds (default: 300 = 5 minutes)
            cache_maxsize: Maximum cache entries (default: 100)
        """
        self.session_id = session_id
        self.user_id = user_id
        self.namespace = f"{user_id}-{session_id}"
        self.store = store
        self.file_parser = file_parser
        
        # Use global cache for cross-session cache hits
        # Cache key includes (session_id, user_id, file_path) for multi-user isolation
        self._file_cache = _GLOBAL_FILE_CACHE
        self._cache_ttl = cache_ttl
        
        logger.info(f"PostgresBackend initialized: namespace={self.namespace}")
    
    def _match_glob(self, file_path: str, glob_pattern: str) -> bool:
        """Match file path against glob pattern (*, ?, [seq], [!seq]).

        Args:
            file_path: File path to match
            glob_pattern: Glob pattern (e.g., "*.txt", "test_*.py")

        Returns:
            True if path matches pattern
        """
        if not glob_pattern or glob_pattern == "*":
            return True
        
        # Extract filename from path for matching
        filename = Path(file_path).name
        
        # Use fnmatch for glob pattern matching
        return fnmatch.fnmatch(filename, glob_pattern)

    def _format_file_content(self, file_data: dict, offset: int, limit: int) -> str:
        """
        Format file content with line numbers.
        
        Args:
            file_data: FileData dict
            offset: Line offset
            limit: Line limit
            
        Returns:
            Formatted content string
        """
        content_lines = file_data.get("content", [])
        
        # Check for empty content
        if not content_lines:
            empty_msg = check_empty_content("")
            if empty_msg:
                return empty_msg
        
        # Apply offset and limit
        total_lines = len(content_lines)
        start_idx = offset
        end_idx = min(start_idx + limit, total_lines)
        
        if start_idx >= total_lines:
            return f"Error: Line offset {offset} exceeds file length ({total_lines} lines)"
        
        selected_lines = content_lines[start_idx:end_idx]
        
        # Format with line numbers
        return format_content_with_line_numbers(selected_lines, start_line=start_idx + 1)

    
    def _get_file_data(self, file_path: str) -> Optional[dict]:
        """Get file data from cache or database (sync wrapper)."""
        return asyncio.run(self._get_file_data_async(file_path))

    async def _get_file_data_async(self, file_path: str) -> Optional[dict]:
        """Get file data from cache or database (async)."""
        cache_key = (self.session_id, self.user_id, file_path)

        # Check cache first
        if cache_key in self._file_cache:
            cached_entry = self._file_cache[cache_key]
            if time.time() - cached_entry["timestamp"] < self._cache_ttl:
                logger.debug(f"Cache HIT: {file_path}")
                return cached_entry["data"]
            else:
                logger.debug(f"Cache EXPIRED: {file_path}")
                del self._file_cache[cache_key]

        # Query database using store.get() (async)
        try:
            item = await self.store.get((self.session_id, self.user_id), file_path)

            if item and item.value:
                file_data = item.value
                logger.debug(f"Database HIT: {file_path}")

                # Cache for future access
                self._file_cache[cache_key] = {
                    "data": file_data,
                    "timestamp": time.time()
                }
                return file_data

            logger.debug(f"Database MISS: {file_path}")
            return None

        except Exception as e:
            logger.error(f"Failed to get file data for {file_path}: {e}", exc_info=True)
            return None

    def _put_file_data(self, file_path: str, file_data: dict, persist: bool = False) -> None:
        """Save file data to cache, optionally persist to database (sync wrapper).

        Args:
            file_path: File path (used as key)
            file_data: FileData dict
            persist: If True, also write to database (for parsed content).
                     If False, only write to local cache (for raw content).
        """
        asyncio.run(self._put_file_data_async(file_path, file_data, persist))

    async def _put_file_data_async(self, file_path: str, file_data: dict, persist: bool = False) -> None:
        """Save file data to cache, optionally persist to database (async).

        Args:
            file_path: File path (used as key)
            file_data: FileData dict
            persist: If True, also write to database (for parsed content).
                     If False, only write to local cache (for raw content).
        """
        try:
            cache_key = (self.session_id, self.user_id, file_path)

            # Update timestamps
            file_data["modified_at"] = datetime.now().isoformat()
            if "created_at" not in file_data:
                file_data["created_at"] = datetime.now().isoformat()

            # Update global cache (always)
            self._file_cache[cache_key] = {
                "data": file_data,
                "timestamp": time.time()
            }

            # Save to PostgreSQL only if persist=True (async)
            if persist:
                await self.store.put((self.session_id, self.user_id), file_path, file_data)
                logger.debug(f"Persisted file data to DB: {file_path}")
            else:
                logger.debug(f"Cached file data (memory only): {file_path}")

        except Exception as e:
            logger.error(f"Failed to save file data for {file_path}: {e}", exc_info=True)
            raise

    async def _download_reference_file(self, file_data: dict, file_name: str) -> Optional[str]:
        """Download content for a reference file.

        Returns:
            Base64 encoded content, or None if download failed
        """
        import httpx

        url = file_data.get("url")
        file_key = file_data.get("file_key")

        # Attempt 1: Use saved URL
        if url:
            try:
                logger.info(f"📥 Downloading reference file from URL: {file_name}")
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                    file_bytes = response.content
                logger.info(f"✅ Downloaded {len(file_bytes)} bytes: {file_name}")
                return base64.b64encode(file_bytes).decode("utf-8")
            except Exception as e:
                logger.warning(f"Failed to download from URL (may be expired): {e}")

        # Attempt 2: Refresh URL via file_key
        if file_key:
            try:
                from .middleware.global_files_middleware import _get_file_info_from_file_key

                logger.info(f"🔄 URL expired, refreshing from file_key: {file_key}")
                file_info = await _get_file_info_from_file_key(file_key)

                if file_info and file_info.get("url"):
                    new_url = file_info["url"]
                    async with httpx.AsyncClient(timeout=30.0) as client:
                        response = await client.get(new_url)
                        response.raise_for_status()
                        file_bytes = response.content

                    # Update URL in file_data for future use
                    file_data["url"] = new_url
                    logger.info(f"✅ Downloaded {len(file_bytes)} bytes via refreshed URL: {file_name}")
                    return base64.b64encode(file_bytes).decode("utf-8")
            except Exception as e:
                logger.error(f"Failed to download via file_key: {e}")

        logger.error(f"❌ No valid URL or file_key to download: {file_name}")
        return None

    async def _parse_file_lazy(self, file_path: str, file_data: dict) -> str:
        """Parse file with global cache support and intelligent waiting."""

        if not self.file_parser:
            return "[No file parser available]"

        raw_content_b64 = file_data.get("raw_content", "")
        file_name = Path(file_path).name
        if file_name.startswith("/0/") and len(file_name) > 3:
            file_name = file_name[3:]

        file_type = file_data.get("file_type", "UNKNOWN")

        # Handle reference files (created by fetch_remote_files)
        if not raw_content_b64 and file_data.get("is_reference"):
            file_key = file_data.get("file_key")
            if file_key:
                # Query th_files cache by file_key
                cached_data = await self.file_parser._query_th_files_cache(file_key, lookup_type="file_key")
                if cached_data:
                    logger.info(f"🎯 Cache hit via file_key: {file_name}")
                    return cached_data["content"]

            raw_content_b64 = await self._download_reference_file(file_data, file_name)
            if raw_content_b64:
                file_data["raw_content"] = raw_content_b64
                file_data["is_reference"] = False  # No longer a reference after download
            else:
                return "[Failed to download reference file]"

        if not raw_content_b64:
            logger.warning(f" ❌ raw_content is empty! file_data={file_data}")
            return "[No raw content]"

        # OPTIMIZATION: Wait for background processing first (avoid duplicate parsing)
        # If file was just uploaded, file_processing_service may already be processing it
        file_key = file_data.get("file_key")
        if file_key:
            logger.info(f"⏳ Waiting for background processing: {file_name}")
            poll_interval = 1  # Check every 1 second
            for attempt in range(MAX_WAIT_TIME // poll_interval):
                cached_data = await self.file_parser._query_th_files_cache(file_key, lookup_type="file_key")
                if cached_data:
                    wait_time = (attempt + 1) * poll_interval
                    logger.info(f"✅ Background processing completed: {file_name} (waited {wait_time}s)")
                    return cached_data["content"]
                await asyncio.sleep(poll_interval)
            logger.info(f"⏰ Background processing timeout after {MAX_WAIT_TIME}s, parsing locally: {file_name}")
        
        try:
            # Only import FileParser when needed (lazy load)
            from .parser import FileParser
        except ImportError:
            logger.error("FileParser not available - file parsing disabled")
            return f"Error: FileParser not available"
        
        try:
            # local files
            import time
            import hashlib

            file_bytes = base64.b64decode(raw_content_b64)

            # Check cache by content_hash before parsing
            content_hash = hashlib.sha256(file_bytes).hexdigest()
            cached_data = await self.file_parser._query_th_files_cache(content_hash, lookup_type="content_hash")
            if cached_data:
                logger.info(f"🎯 Cache hit via content_hash: {file_name} (hash={content_hash[:16]}...)")
                return cached_data["content"]

            # Cache miss, parse file using FileParser
            logger.info(f"📄 Lazy-parsing: {file_name} (hash={content_hash[:16]}...)")
            parse_start = time.time()

            # FileParser handles cache lookup and saving internally
            parsed_content, parse_method, parse_model = await self.file_parser.parse_file(
                file_input=io.BytesIO(file_bytes),
                filename=file_name,
                file_type=file_type
            )

            parse_duration_ms = int((time.time() - parse_start) * 1000)
            logger.info(f"✅ Parsed {file_name}: {len(parsed_content)} chars, {parse_duration_ms}ms ({parse_method})")

            return parsed_content

        except Exception as e:
            logger.error(f"Parse failed for {file_path}: {e}", exc_info=True)
            return f"[Parse error: {str(e)}]"

    # ==================== Async Method Overrides ====================
    # Override BackendProtocol async methods for full async/await chain

    async def als_info(self, path: str = "/") -> list[FileInfo]:
        """List files and directories in the specified directory (non-recursive).

        Args:
            path: Absolute directory path to list files from.

        Returns:
            List of `FileInfo`-like dicts for files and directories directly in the
                directory. Directories have a trailing `/` in their path and
                `is_dir=True`.
        """
        try:
            items = await self.store.search((self.session_id, self.user_id))
            results = []
            for item in items:
                file_path = item.key
                if not file_path.startswith(path):
                    continue
                file_data = item.value
                results.append(FileInfo(
                    path=file_path,
                    is_dir=False,
                    size=len(file_data.get("content", [])),
                    modified_at=file_data.get("modified_at"),
                ))
            # Keep deterministic order by path (matching FilesystemBackend)
            results.sort(key=lambda x: x.get("path", ""))
            return results
        except Exception as e:
            logger.error(f"Failed to list files in {path}: {e}", exc_info=True)
            return []

    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:
        """Read file content with line numbers.

        Args:
            file_path: Absolute or relative file path.
            offset: Line offset to start reading from (0-indexed).
            limit: Maximum number of lines to read.

        Returns:
            Formatted file content with line numbers, or error message.
        """
        if offset is None:
            offset = 0
        if limit is None:
            limit = 2000

        file_data = await self._get_file_data_async(file_path)
        if not file_data:
            # Provide helpful error with available files
            try:
                items = await self.store.search((self.session_id, self.user_id))
                files = [item.key for item in items] if items else []
                if files:
                    return f"Error: File '{file_path}' not found\n\nAvailable files:\n" + "\n".join(f"  - {f}" for f in files)
            except Exception:
                pass
            return f"Error: File '{file_path}' not found"

        # Lazy parsing on first read (async)
        if not file_data.get("content"):
            logger.info(f"Lazy-parsing: {file_path}")
            parsed_content = await self._parse_file_lazy(file_path, file_data)

            # Update file data with parsed content
            file_data["content"] = parsed_content.split("\n") if parsed_content else []
            await self._put_file_data_async(file_path, file_data, persist=True)

            # Re-fetch to ensure we have the latest data
            file_data = await self._get_file_data_async(file_path)
            if not file_data or not file_data.get("content"):
                return parsed_content or "[Parse failed]"

        # Use standard formatting (matching FilesystemBackend)
        content_lines = file_data.get("content", [])

        # Check empty content
        if not content_lines:
            content = "\n".join(content_lines)
            empty_msg = check_empty_content(content)
            if empty_msg:
                return empty_msg

        # Apply offset and limit
        total_lines = len(content_lines)
        start_idx = offset
        end_idx = min(start_idx + limit, total_lines)

        if start_idx >= total_lines:
            return f"Error: Line offset {offset} exceeds file length ({total_lines} lines)"

        selected_lines = content_lines[start_idx:end_idx]

        # Format with summary header instead of per-line numbers
        start_line = start_idx + 1
        end_line = end_idx
        remaining_lines = total_lines - end_idx
        is_complete = end_idx >= total_lines

        # Build header with range and status
        if is_complete:
            header = f"[Lines {start_line}-{end_line} of {total_lines} total lines, complete]\n\n"
        else:
            header = f"[Lines {start_line}-{end_line} of {total_lines} total lines, {remaining_lines} lines remaining]\n\n"

        # Join content without per-line numbering
        content = "\n".join(selected_lines)

        return header + content

    async def awrite(self, file_path: str, content: str) -> WriteResult:
        """Create a new file with content.

        Args:
            file_path: Path where the new file will be created.
            content: Text content to write to the file.

        Returns:
            `WriteResult` with path on success, or error message if the file
                already exists or write fails. External storage sets `files_update=None`.
        """
        existing = await self._get_file_data_async(file_path)
        if existing is not None:
            return WriteResult(error=f"Cannot write to {file_path} because it already exists. Read and then make an edit, or write to a new path.")

        file_data = {
            "content": content.split("\n"),
            "created_at": datetime.now().isoformat(),
            "modified_at": datetime.now().isoformat(),
        }

        await self._put_file_data_async(file_path, file_data, persist=True)
        return WriteResult(path=file_path, files_update=None)

    async def aedit(self, file_path: str, old_string: str, new_string: str,
                    replace_all: bool = False) -> EditResult:
        """Edit a file by replacing string occurrences.

        Args:
            file_path: Path to the file to edit.
            old_string: The text to search for and replace.
            new_string: The replacement text.
            replace_all: If `True`, replace all occurrences. If `False` (default),
                replace only if exactly one occurrence exists.

        Returns:
            `EditResult` with path and occurrence count on success, or error
                message if file not found or replacement fails. External storage sets
                `files_update=None`.
        """
        file_data = await self._get_file_data_async(file_path)
        if file_data is None:
            return EditResult(error=f"Error: File '{file_path}' not found")

        content_lines = file_data.get("content", [])
        if not content_lines:
            return EditResult(error=f"Error: File '{file_path}' is empty")

        # Join lines to full text
        content = "\n".join(content_lines)

        result = perform_string_replacement(content, old_string, new_string, replace_all)

        if isinstance(result, str):
            # Error message from perform_string_replacement
            return EditResult(error=result)

        new_content, occurrences = result

        # Update file data
        new_file_data = {
            **file_data,
            "content": new_content.split("\n") if new_content else [],
            "modified_at": datetime.now().isoformat(),
        }

        # Clear raw_content after text edit (text and binary no longer match)
        if "raw_content" in new_file_data:
            del new_file_data["raw_content"]
            new_file_data["metadata"] = new_file_data.get("metadata", {})
            new_file_data["metadata"]["raw_content_cleared"] = True

        # Mark as edited
        new_file_data["metadata"] = new_file_data.get("metadata", {})
        new_file_data["metadata"]["edited_in_session"] = True

        # Save to database and cache (persist edited content)
        await self._put_file_data_async(file_path, new_file_data, persist=True)

        return EditResult(
            path=file_path,
            files_update=None,
            occurrences=int(occurrences)
        )


    async def agrep_raw(self, pattern: str, path: Optional[str] = None,
                        glob: Optional[str] = None) -> list[GrepMatch] | str:
        """Search for a literal text pattern in files.

        Uses database search with regex pattern matching.

        Args:
            pattern: Literal string to search for (NOT regex).
            path: Directory or file path to search in. Defaults to current directory.
            glob: Optional glob pattern to filter which files to search.

        Returns:
            List of GrepMatch dicts containing path, line number, and matched text.
        """
        search_path = path or "/"
        glob_pattern = glob or "*"

        try:
            # Note: Unlike FilesystemBackend which uses re.escape for literal search,
            # we use the pattern as-is for regex matching (more flexible for database queries)
            regex = re.compile(pattern, re.IGNORECASE)
        except re.error as e:
            return f"Invalid regex pattern: {str(e)}"

        try:
            items = await self.store.search((self.session_id, self.user_id))
            matches: list[GrepMatch] = []
            for item in items:
                fp = item.key
                if not fp.startswith(search_path) or not self._match_glob(fp, glob_pattern):
                    continue
                file_data = item.value
                # Skip reference files without content
                if file_data.get("is_reference") and not file_data.get("content"):
                    continue
                for line_num, line in enumerate(file_data.get("content", []), 1):
                    if regex.search(line):
                        matches.append(GrepMatch(path=fp, line=int(line_num), text=str(line)))
            return matches
        except Exception as e:
            logger.error(f"Failed to grep: {e}", exc_info=True)
            return f"Error: {str(e)}"

    async def aglob_info(self, pattern: str, path: str = "/") -> list[FileInfo]:
        """Find files matching a glob pattern.

        Args:
            pattern: Glob pattern to match files against (e.g., `'*.py'`, `'**/*.txt'`).
            path: Base directory to search from. Defaults to root (`/`).

        Returns:
            List of `FileInfo` dicts for matching files, sorted by path. Each dict
                contains `path`, `is_dir`, `size`, and `modified_at` fields.
        """
        try:
            items = await self.store.search((self.session_id, self.user_id))
            results = []
            for item in items:
                fp = item.key
                if not fp.startswith(path) or not self._match_glob(fp, pattern):
                    continue
                file_data = item.value
                results.append(FileInfo(
                    path=fp,
                    is_dir=False,
                    size=len(file_data.get("content", [])),
                    modified_at=file_data.get("modified_at"),
                ))
            # Sort results for deterministic order (matching FilesystemBackend)
            results.sort(key=lambda x: x.get("path", ""))
            return results
        except Exception as e:
            logger.error(f"Failed to glob: {e}", exc_info=True)
            return []

    # ==================== Sync Method Wrappers ====================
    # Sync methods for BackendProtocol compatibility (matching FilesystemBackend interface)

    def ls_info(self, path: str = "/") -> list[FileInfo]:
        """List files and directories in the specified directory (non-recursive).
        """
        return asyncio.run(self.als_info(path))

    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:
        """Read file content with line numbers.
        """
        return asyncio.run(self.aread(file_path, offset, limit))

    def write(self, file_path: str, content: str) -> WriteResult:
        """Create a new file with content.
        """
        return asyncio.run(self.awrite(file_path, content))

    def edit(self, file_path: str, old_string: str, new_string: str,
             replace_all: bool = False) -> EditResult:
        """Edit a file by replacing string occurrences.
        """
        return asyncio.run(self.aedit(file_path, old_string, new_string, replace_all))

    def glob_info(self, pattern: str, path: str = "/") -> list[FileInfo]:
        """Find files matching a glob pattern.
        """
        return asyncio.run(self.aglob_info(pattern, path))

    def grep_raw(self, pattern: str, path: Optional[str] = None,
                 glob: Optional[str] = None) -> list[GrepMatch] | str:
        """Search for a literal text pattern in files.
        """
        return asyncio.run(self.agrep_raw(pattern, path, glob))
    # ==================== File Upload/Download ====================

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """Upload multiple files to the filesystem.

        Args:
            files: List of (path, content) tuples where content is bytes.

        Returns:
            List of FileUploadResponse objects, one per input file.
            Response order matches input order.
        """
        responses = []
        for file_path, file_bytes in files:
            try:
                ext = Path(file_path).suffix
                file_data = {
                    "content": [],
                    "raw_content": base64.b64encode(file_bytes).decode('utf-8'),
                    "file_type": get_file_type(ext),
                    "file_extension": ext,
                    "parsed": False,
                    "created_at": datetime.now().isoformat(),
                    "modified_at": datetime.now().isoformat(),
                    "metadata": {"original_size": len(file_bytes), "encoding": "base64"}
                }
                self._put_file_data(file_path, file_data, persist=True)
                responses.append(FileUploadResponse(path=file_path, error=None))
            except FileNotFoundError:
                responses.append(FileUploadResponse(path=file_path, error="file_not_found"))
            except PermissionError:
                responses.append(FileUploadResponse(path=file_path, error="permission_denied"))
            except (ValueError, OSError) as e:
                # ValueError from path validation, OSError for other errors
                if isinstance(e, ValueError) or "invalid" in str(e).lower():
                    responses.append(FileUploadResponse(path=file_path, error="invalid_path"))
                else:
                    responses.append(FileUploadResponse(path=file_path, error="invalid_path"))
            except Exception as e:
                logger.error(f"Failed to upload {file_path}: {e}", exc_info=True)
                responses.append(FileUploadResponse(path=file_path, error="invalid_path"))
        return responses

    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """Download multiple files from the filesystem.

        Args:
            paths: List of file paths to download.

        Returns:
            List of FileDownloadResponse objects, one per input path.
        """
        responses = []
        for file_path in paths:
            try:
                file_data = self._get_file_data(file_path)
                if file_data is None:
                    responses.append(FileDownloadResponse(path=file_path, content=None, error="file_not_found"))
                    continue

                raw_b64 = file_data.get("raw_content", "")
                if raw_b64:
                    try:
                        content = base64.b64decode(raw_b64)
                        responses.append(FileDownloadResponse(path=file_path, content=content, error=None))
                    except Exception:
                        responses.append(FileDownloadResponse(path=file_path, content=None, error="invalid_path"))
                else:
                    # Fallback to text content
                    content_str = "\n".join(file_data.get("content", []))
                    responses.append(FileDownloadResponse(path=file_path, content=content_str.encode('utf-8'), error=None))
            except FileNotFoundError:
                responses.append(FileDownloadResponse(path=file_path, content=None, error="file_not_found"))
            except PermissionError:
                responses.append(FileDownloadResponse(path=file_path, content=None, error="permission_denied"))
            except ValueError:
                responses.append(FileDownloadResponse(path=file_path, content=None, error="invalid_path"))
        return responses

    def upload_parsed_files(self, files: list[tuple[str, str, dict[str, Any]]]) -> list[FileUploadResponse]:
        """Upload already-parsed text files."""
        responses = []
        for file_path, parsed_text, metadata in files:
            try:
                file_data = {
                    "content": parsed_text.split("\n"),
                    "file_key": metadata.get("file_key"),
                    "content_hash": metadata.get("content_hash"),
                    "file_type": metadata.get("file_type"),
                    "file_extension": metadata.get("file_extension"),
                    "parsed": metadata.get("parsed", True),
                    **{k: v for k, v in metadata.items()
                       if k not in ["file_key", "content_hash", "file_type", "file_extension", "parsed"]}
                }
                self._put_file_data(file_path, file_data, persist=True)
                responses.append(FileUploadResponse(path=file_path, error=None))
            except Exception as e:
                logger.error(f"Failed to upload parsed {file_path}: {e}", exc_info=True)
                responses.append(FileUploadResponse(path=file_path, error="invalid_path"))
        return responses
        
        
        

def create_postgres_backend(
    session_id: str,
    user_id: str,
    file_parser=None,
    cache_ttl: int = 300,
    cache_maxsize: int = 100,
):
    """
    Create a PostgresBackend instance with automatic FileParser initialization.

    Args:
        session_id: Session ID for namespace isolation
        user_id: User ID for namespace isolation
        file_parser: FileParser instance (optional, auto-created if None)
        cache_ttl: Cache TTL in seconds (default: 300)
        cache_maxsize: Maximum cache entries (default: 100)

    Returns:
        PostgresBackend instance
    """
    from .store import PostgresLangGraphStore
    from .parser import FileParser

    # Auto-create FileParser if not provided
    if file_parser is None:
        file_parser = FileParser()

    store = PostgresLangGraphStore()

    return PostgresBackend(
        session_id=session_id,
        user_id=user_id,
        store=store,
        file_parser=file_parser,
        cache_ttl=cache_ttl,
    )