"""
PostgreSQL Backend Implementation

Implements BackendProtocol using PostgresLangGraphStore for persistent storage
and FileParser for intelligent file parsing.

Key Features:
- Persistent storage across sessions/threads using PostgreSQL
- Intelligent file parsing (PDF, DOCX, images)
- Lazy parsing strategy (parse only when needed)
- Local caching for performance
- Full BackendProtocol compatibility

Architecture:
    PostgresBackend (BackendProtocol)
        ├── PostgresLangGraphStore (persistence)
        ├── FileParser (intelligent parsing)
        └── TTLCache (local caching)
"""

import asyncio
import base64
import fnmatch
import io
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

# Try to import cachetools, fallback to simple dict if not available
try:
    from cachetools import TTLCache
    HAS_CACHETOOLS = True
except ImportError:
    HAS_CACHETOOLS = False

from deepagents.backends.protocol import (
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

if TYPE_CHECKING:
    from .postgres_store import PostgresLangGraphStore
    from .file_parser import FileParser

logger = logging.getLogger(__name__)


# File type mapping
FILE_TYPE_MAP = {
    '.pdf': 'PDF',
    '.docx': 'DOCX',
    '.doc': 'DOC',
    '.png': 'IMAGE',
    '.jpg': 'IMAGE',
    '.jpeg': 'IMAGE',
    '.gif': 'IMAGE',
    '.webp': 'IMAGE',
    '.bmp': 'IMAGE',
    '.txt': 'TEXT',
    '.md': 'TEXT',
    '.csv': 'CSV',
    '.xlsx': 'EXCEL',
    '.xls': 'EXCEL',
}


class PostgresBackend:
    """
    PostgreSQL-backed implementation of BackendProtocol.
    
    Provides persistent file storage using PostgresLangGraphStore with
    intelligent file parsing capabilities via FileParser.
    
    FileData format (stored in PostgreSQL as JSONB):
    {
        "content": ["line1", "line2", ...],      # Parsed text lines
        "raw_content": "base64_encoded_bytes",   # Original binary (optional)
        "file_type": "PDF" | "IMAGE" | "TEXT",   # File type
        "file_extension": ".pdf",                # File extension
        "parsed": True | False,                  # Parse status
        "created_at": "ISO_timestamp",
        "modified_at": "ISO_timestamp",
        "metadata": {
            "parse_mode": "llm",
            "original_size": 1024,
            ...
        }
    }
    """
    
    def __init__(
        self,
        session_id: str,
        user_id: str,
        store: "PostgresLangGraphStore",
        file_parser: Optional["FileParser"] = None,
        cache_ttl: int = 300,
        cache_maxsize: int = 100,
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
        self.store = store
        self.file_parser = file_parser
        
        # Local cache for performance (key is file_path only)
        if HAS_CACHETOOLS:
            self._cache = TTLCache(maxsize=cache_maxsize, ttl=cache_ttl)
        else:
            # Fallback to simple dict cache (no TTL)
            self._cache = {}
        
        logger.info(
            f"PostgresBackend initialized: session={session_id}, user={user_id}, "
            f"file_parser={'enabled' if file_parser else 'disabled'}, "
            f"cache={'TTL' if HAS_CACHETOOLS else 'dict'}"
        )
    
    def _get_file_type(self, extension: str) -> str:
        """Get file type from extension."""
        ext_lower = extension.lower()
        if not ext_lower.startswith('.'):
            ext_lower = '.' + ext_lower
        return FILE_TYPE_MAP.get(ext_lower, 'UNKNOWN')
    
    def _match_glob(self, file_path: str, glob_pattern: str) -> bool:
        """
        Match file path against glob pattern.
        
        Supports standard glob patterns: *, ?, [seq], [!seq]
        
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
    
    def _run_async(self, coro):
        """
        Run async coroutine in sync context.
        
        Handles nested event loop issues by checking for running loops
        and using thread pool executor when necessary.
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Already in event loop, use thread pool
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(lambda: asyncio.run(coro))
                    return future.result()
            else:
                return loop.run_until_complete(coro)
        except RuntimeError:
            # No event loop, create new one
            return asyncio.run(coro)
    
    async def _get_file_data(self, file_path: str) -> Optional[dict]:
        """
        Get file data from workspace with caching.
        
        Args:
            file_path: File path (used as key)
            
        Returns:
            FileData dict or None if not found
        """
        # Check local cache first
        if file_path in self._cache:
            logger.debug(f"Cache hit: {file_path}")
            return self._cache[file_path]
        
        # Query PostgreSQL
        try:
            item = await self.store.get((self.session_id, self.user_id), file_path)
            
            if item is None:
                return None
            
            file_data = item.value
            
            # Update local cache
            self._cache[file_path] = file_data
            
            return file_data
            
        except Exception as e:
            logger.error(f"Failed to get file data for {file_path}: {e}", exc_info=True)
            return None
    
    async def _put_file_data(self, file_path: str, file_data: dict) -> None:
        """
        Save file data to workspace and update cache.
        
        Args:
            file_path: File path (used as key)
            file_data: FileData dict
        """
        try:
            # Update timestamps
            file_data["modified_at"] = datetime.now().isoformat()
            if "created_at" not in file_data:
                file_data["created_at"] = datetime.now().isoformat()
            
            # Save to PostgreSQL
            await self.store.put((self.session_id, self.user_id), file_path, file_data)
            
            # Update local cache
            self._cache[file_path] = file_data
            
            logger.debug(f"Saved file data: {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save file data for {file_path}: {e}", exc_info=True)
            raise
    
    async def _download_reference_file(self, file_path: str, file_data: dict) -> bool:
        """
        Download reference file from external storage or URL.
        
        Supports two download methods:
        1. file_key: Download from S3/OSS storage using storage client
        2. url: Download from HTTP/HTTPS URL
        
        Downloads binary, stores as base64 in raw_content,
        marks is_reference = False, and saves to database + cache.
        
        Args:
            file_path: File path in workspace (e.g., /workspace/global_files/report.pdf)
            file_data: FileData dict with file_key or url for download
            
        Returns:
            True if download successful, False otherwise
        """
        try:
            file_key = file_data.get("file_key")
            url = file_data.get("url")
            
            # Attempt to download from file_key first (more reliable)
            if file_key:
                logger.info(f"Downloading from storage: {file_key}")
                try:
                    from mirobody.utils.config.storage import get_storage_client
                    storage = get_storage_client()
                    file_content, _ = await storage.get(file_key)
                    
                    if file_content:
                        # Ensure content is bytes
                        if isinstance(file_content, str):
                            file_bytes = file_content.encode('utf-8')
                        else:
                            file_bytes = file_content
                        
                        logger.info(f"✅ Downloaded from storage: {len(file_bytes)} bytes")
                    else:
                        raise Exception("Empty content from storage")
                        
                except Exception as e:
                    logger.warning(f"Storage download failed: {e}, trying URL...")
                    file_bytes = None
            else:
                file_bytes = None
            
            # Fallback to URL download if file_key failed or not available
            if file_bytes is None and url:
                logger.info(f"Downloading from URL: {url[:80]}...")
                try:
                    import httpx
                    
                    # Use browser-like headers to avoid 403/503 errors
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "none",
                        "Cache-Control": "max-age=0",
                    }
                    
                    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                        response = await client.get(url, headers=headers)
                        response.raise_for_status()
                        file_bytes = response.content
                    
                    logger.info(f"✅ Downloaded from URL: {len(file_bytes)} bytes")
                    
                except Exception as e:
                    logger.error(f"URL download failed: {e}")
                    file_bytes = None
            
            # Check if download succeeded
            if file_bytes is None:
                logger.error(f"Failed to download {file_path}: no file_key or url available")
                return False
            
            # Update file_data with downloaded content
            file_data["raw_content"] = base64.b64encode(file_bytes).decode('utf-8')
            file_data["is_reference"] = False  # Mark as materialized
            file_data["content"] = []  # Will be parsed if needed
            file_data["parsed"] = False
            file_data["metadata"] = file_data.get("metadata", {})
            file_data["metadata"]["download_timestamp"] = datetime.now().isoformat()
            file_data["metadata"]["download_size"] = len(file_bytes)
            file_data["metadata"]["download_source"] = "file_key" if file_key else "url"
            
            # Save updated data back to store
            await self._put_file_data(file_path, file_data)
            
            logger.info(f"✅ Cached: {file_path} ({len(file_bytes)} bytes)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download reference file {file_path}: {e}", exc_info=True)
            return False
    
    async def _parse_file_lazy(self, file_path: str, file_data: dict) -> str:
        """
        Parse file content using FileParser (lazy loading).
        
        Args:
            file_path: File path
            file_data: FileData dict with raw_content
            
        Returns:
            Parsed text content
        """
        if not self.file_parser:
            return "[No file parser available]"
        
        raw_content_b64 = file_data.get("raw_content", "")
        if not raw_content_b64:
            return "[No raw content to parse]"
        
        try:
            # Decode base64 to bytes
            file_bytes = base64.b64decode(raw_content_b64)
            
            # Get file info
            file_ext = file_data.get("file_extension", "")
            file_name = Path(file_path).name
            
            logger.debug(f"Parsing file: {file_name} ({len(file_bytes)} bytes)")
            
            # Call FileParser (async)
            parsed_content = await self.file_parser.parse_file(
                io.BytesIO(file_bytes),
                file_ext,
                file_name
            )
            
            logger.info(f"Successfully parsed {file_name}: {len(parsed_content)} chars")
            return parsed_content
            
        except Exception as e:
            logger.error(f"Failed to parse file {file_path}: {e}", exc_info=True)
            return f"[Parse Error: {str(e)}]"
    
    # ==================== BackendProtocol Methods ====================
    
    def ls_info(self, path: str = "/") -> list[FileInfo]:
        """
        List files in a directory.
        
        Args:
            path: Directory path (default: "/")
            
        Returns:
            List of FileInfo objects
        """
        return self._run_async(self._ls_info_async(path))
    
    async def als_info(self, path: str = "/") -> list[FileInfo]:
        """
        Async version of ls_info.
        
        Args:
            path: Directory path (default: "/")
            
        Returns:
            List of FileInfo objects
        """
        return await self._ls_info_async(path)
    
    async def aread(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:
        """
        Async version of read.
        
        Args:
            file_path: File path
            offset: Line offset (0-indexed)
            limit: Maximum lines to return
            
        Returns:
            Formatted file content with line numbers
        """
        return await self._read_async(file_path, offset, limit)
    
    async def awrite(self, file_path: str, content: str) -> WriteResult:
        """
        Async version of write.
        
        Args:
            file_path: File path
            content: Text content
            
        Returns:
            WriteResult
        """
        return await self._write_async(file_path, content)
    
    async def aedit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False
    ) -> EditResult:
        """
        Async version of edit.
        
        Args:
            file_path: File path
            old_string: String to replace
            new_string: Replacement string
            replace_all: Replace all occurrences
            
        Returns:
            EditResult
        """
        return await self._edit_async(file_path, old_string, new_string, replace_all)
    
    async def aglob_info(self, pattern: str, path: str = "/") -> list[FileInfo]:
        """
        Async version of glob_info.
        
        Args:
            pattern: Glob pattern (e.g., "*.txt", "test_*.py")
            path: Directory path to search in (default: "/")
            
        Returns:
            List of FileInfo objects sorted by path
        """
        return await self._glob_info_async(pattern, path)
    
    async def _ls_info_async(self, path: str) -> list[FileInfo]:
        """
        List files in workspace.
        
        Includes all files (local + global references).
        """
        try:
            # Search workspace
            items = await self.store.search((self.session_id, self.user_id))
            
            results = []
            for item in items:
                file_path = item.key
                
                # Filter by path prefix
                if not file_path.startswith(path):
                    continue
                
                file_data = item.value
                
                # Create FileInfo
                file_info = FileInfo(
                    path=file_path,
                    is_dir=False,  # We don't support directories yet
                    size=len(file_data.get("content", [])),
                    modified_at=file_data.get("modified_at"),
                )
                results.append(file_info)
            
            logger.debug(f"Listed {len(results)} files in {path}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to list files in {path}: {e}", exc_info=True)
            return []
    
    def read(self, file_path: str, offset: int = 0, limit: int = 2000) -> str:
        """
        Read file with intelligent lazy parsing.
        
        Workflow:
        1. Get file data from store
        2. Check if already parsed
        3. If not parsed and needs parsing → call _parse_file_lazy()
        4. Cache parsed content
        5. Return formatted content with line numbers
        
        Args:
            file_path: File path
            offset: Line offset (0-indexed)
            limit: Maximum lines to return
            
        Returns:
            Formatted file content with line numbers
        """
        return self._run_async(self._read_async(file_path, offset, limit))
    
    async def _read_async(self, file_path: str, offset: int, limit: int) -> str:
        """
        Read file with lazy loading and caching guarantee.
        
        Performance:
        - First read: 2-10 seconds (download + parse)
        - Subsequent reads: < 1ms (cache hit)
        - Edit/grep after read: < 1ms (use cached content)
        
        Caching guarantee: Once parsed, content stays in cache for entire session.
        No re-download, no re-parsing on subsequent reads/edits/greps.
        """
        # Get file_data from cache or database
        file_data = await self._get_file_data(file_path)
        
        if file_data is None:
            return f"Error: File '{file_path}' not found"
        
        # ⚡ Fast path: Already parsed and cached
        if file_data.get("parsed") and file_data.get("content"):
            logger.debug(f"Cache hit (parsed): {file_path}")
            return self._format_file_content(file_data, offset, limit)
        
        # Slow path: Need to download/parse
        
        # Step 1: Handle reference (lazy download)
        if file_data.get("is_reference"):
            logger.info(f"Lazy downloading: {file_path}")
            downloaded = await self._download_reference_file(file_path, file_data)
            if not downloaded:
                return f"Error: Failed to download '{file_path}'"
            
            # Reload after download
            file_data = await self._get_file_data(file_path)
        
        # Step 2: Parse if needed (PDF/DOCX/IMAGE/EXCEL)
        file_type = file_data.get("file_type", "UNKNOWN")
        if not file_data.get("parsed") and file_type in ["PDF", "DOCX", "IMAGE", "DOC", "EXCEL"]:
            if self.file_parser:
                logger.info(f"Lazy parsing: {file_path} ({file_type})")
                parsed_content = await self._parse_file_lazy(file_path, file_data)
                
                # Cache parsed content
                file_data["content"] = parsed_content.split("\n") if parsed_content else []
                file_data["parsed"] = True
                file_data["metadata"] = file_data.get("metadata", {})
                file_data["metadata"]["parse_timestamp"] = datetime.now().isoformat()
                
                # Save to database and local cache
                await self._put_file_data(file_path, file_data)
                
                logger.info(f"✅ Cached: {file_path} ({len(file_data['content'])} lines)")
            else:
                logger.warning(f"No file_parser available for {file_type}: {file_path}")
        
        # For text files without raw_content, mark as already parsed
        if not file_data.get("parsed") and not file_data.get("raw_content"):
            file_data["parsed"] = True
            await self._put_file_data(file_path, file_data)
        
        return self._format_file_content(file_data, offset, limit)
    
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
    
    def write(self, file_path: str, content: str) -> WriteResult:
        """
        Create a new text file.
        
        Args:
            file_path: File path
            content: Text content
            
        Returns:
            WriteResult
        """
        return self._run_async(self._write_async(file_path, content))
    
    async def _write_async(self, file_path: str, content: str) -> WriteResult:
        """Async implementation of write."""
        # Check if file already exists
        existing = await self._get_file_data(file_path)
        if existing is not None:
            return WriteResult(
                error=f"Cannot write to {file_path} because it already exists. "
                      "Read and then make an edit, or write to a new path."
            )
        
        # Create FileData for text file
        file_data = {
            "content": content.split("\n") if content else [],
            "file_type": "TEXT",
            "file_extension": Path(file_path).suffix,
            "parsed": True,  # Text files are always "parsed"
            "created_at": datetime.now().isoformat(),
            "modified_at": datetime.now().isoformat(),
            "metadata": {}
        }
        
        # Save to store
        await self._put_file_data(file_path, file_data)
        
        return WriteResult(path=file_path, files_update={file_path: file_data})
    
    def edit(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool = False
    ) -> EditResult:
        """
        Edit file by replacing strings.
        
        Args:
            file_path: File path
            old_string: String to replace
            new_string: Replacement string
            replace_all: Replace all occurrences
            
        Returns:
            EditResult
        """
        return self._run_async(self._edit_async(file_path, old_string, new_string, replace_all))
    
    async def _edit_async(
        self,
        file_path: str,
        old_string: str,
        new_string: str,
        replace_all: bool
    ) -> EditResult:
        """
        Edit file in-place.
        
        All edits affect only this session's workspace (session isolation).
        Global files are already in workspace after fetch.
        
        Note: File content must be available (cached).
        For global files, read them first to cache content.
        """
        # Get file_data (from cache or database)
        file_data = await self._get_file_data(file_path)
        
        if file_data is None:
            return EditResult(error=f"Error: File '{file_path}' not found")
        
        # Ensure file content is available
        content_lines = file_data.get("content", [])
        if not content_lines:
            # Check if this is an unread reference file
            if file_data.get("is_reference"):
                return EditResult(
                    error=f"Error: File '{file_path}' not loaded. Read it first to cache content."
                )
            return EditResult(error=f"Error: File '{file_path}' has no content to edit")
        
        # Get content as string
        content = "\n".join(content_lines)
        
        # Perform string replacement
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
        new_file_data["metadata"]["edited_in_session"] = True
        
        # Save to database and cache
        await self._put_file_data(file_path, new_file_data)
        
        return EditResult(
            path=file_path,
            files_update={file_path: new_file_data},
            occurrences=int(occurrences)
        )
    
    def grep_raw(
        self,
        pattern: str,
        path: Optional[str] = None,
        glob: Optional[str] = None
    ) -> list[GrepMatch] | str:
        """
        Search for pattern in files.
        
        Args:
            pattern: Search pattern (regex)
            path: Directory path to search in (default: "/")
            glob: File glob pattern (default: "*")
            
        Returns:
            List of GrepMatch objects, or error string for invalid regex
        """
        return self._run_async(self._grep_raw_async(pattern, path, glob))
    
    async def agrep_raw(
        self,
        pattern: str,
        path: Optional[str] = None,
        glob: Optional[str] = None
    ) -> list[GrepMatch] | str:
        """
        Async version of grep_raw.
        
        Args:
            pattern: Search pattern (regex)
            path: Directory path to search in (default: "/")
            glob: File glob pattern (default: "*")
            
        Returns:
            List of GrepMatch objects, or error string for invalid regex
        """
        return await self._grep_raw_async(pattern, path, glob)
    
    async def _grep_raw_async(
        self,
        pattern: str,
        path: Optional[str],
        glob: Optional[str]
    ) -> list[GrepMatch] | str:
        """
        Search all workspace files (local + cached global).
        
        Performance:
        - Searches cached content only (no download/parse triggered)
        - If global file not yet read, it's skipped (invisible to grep)
        - If global file was read before, searches its cached content (fast)
        
        Skips: Reference files that haven't been downloaded yet
               (is_reference=true and content is empty)
        """
        # Set defaults
        if path is None:
            path = "/"
        if glob is None:
            glob = "*"
        
        try:
            # Compile regex pattern - validate first
            try:
                regex = re.compile(pattern, re.IGNORECASE)
            except re.error as e:
                # Return error string for invalid regex (as per protocol)
                return f"Invalid regex pattern: {str(e)}"
            
            # Search workspace
            items = await self.store.search((self.session_id, self.user_id))
            
            matches = []
            for item in items:
                file_path = item.key
                
                # Filter by path prefix
                if not file_path.startswith(path):
                    continue
                
                # Filter by glob pattern
                if not self._match_glob(file_path, glob):
                    continue
                
                file_data = item.value
                
                # ⚠️ Skip reference files that haven't been downloaded/cached yet
                # (These are global files that were fetched but never read)
                if file_data.get("is_reference") and not file_data.get("content"):
                    logger.debug(f"Skipping uncached reference: {file_path}")
                    continue
                
                # Get content (available for local files and cached global files)
                content_lines = file_data.get("content", [])
                
                if not content_lines:
                    continue
                
                # Search in content
                for line_num, line in enumerate(content_lines, 1):
                    if regex.search(line):
                        matches.append(
                            GrepMatch(
                                path=file_path,
                                line=int(line_num),
                                text=str(line),
                            )
                        )
            
            logger.debug(f"Grep found {len(matches)} matches for pattern '{pattern}'")
            return matches
            
        except Exception as e:
            logger.error(f"Failed to grep pattern '{pattern}': {e}", exc_info=True)
            return f"Error during search: {str(e)}"
    
    def glob_info(self, pattern: str, path: str = "/") -> list[FileInfo]:
        """
        Find files matching glob pattern.
        
        Args:
            pattern: Glob pattern (e.g., "*.txt", "test_*.py")
            path: Directory path to search in (default: "/")
            
        Returns:
            List of FileInfo objects sorted by path
        """
        return self._run_async(self._glob_info_async(pattern, path))
    
    async def _glob_info_async(self, pattern: str, path: str) -> list[FileInfo]:
        """
        Find files matching pattern in workspace.
        
        Includes all files (local + global references).
        Unlike grep, glob shows ALL files including un-downloaded references.
        """
        try:
            # Search workspace
            items = await self.store.search((self.session_id, self.user_id))
            
            results = []
            for item in items:
                file_path = item.key
                
                # Filter by path prefix
                if not file_path.startswith(path):
                    continue
                
                # Filter by glob pattern
                if not self._match_glob(file_path, pattern):
                    continue
                
                file_data = item.value
                
                # Create FileInfo
                file_info = FileInfo(
                    path=file_path,
                    is_dir=False,  # We don't support directories
                    size=len(file_data.get("content", [])),
                    modified_at=file_data.get("modified_at"),
                )
                results.append(file_info)
            
            # Sort by path for deterministic output (as per protocol)
            results.sort(key=lambda x: x.path)
            
            logger.debug(f"Found {len(results)} files matching pattern '{pattern}' in {path}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to glob pattern '{pattern}': {e}", exc_info=True)
            return []
    
    def upload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """
        Upload binary files to store.
        
        Creates FileData entries with:
        - raw_content: base64 encoded binary
        - parsed: False (lazy parsing)
        - file_type: Detected from extension
        
        Args:
            files: List of (file_path, file_bytes) tuples
            
        Returns:
            List of FileUploadResponse objects
        """
        return self._run_async(self._upload_files_async(files))
    
    async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """
        Async version of upload_files.
        
        Args:
            files: List of (file_path, file_bytes) tuples
            
        Returns:
            List of FileUploadResponse objects
        """
        return await self._upload_files_async(files)
    
    async def _upload_files_async(self, files: list[tuple[str, bytes]]) -> list[FileUploadResponse]:
        """Async implementation of upload_files."""
        responses = []
        
        for file_path, file_bytes in files:
            try:
                # Parse file info
                path_obj = Path(file_path)
                ext = path_obj.suffix
                file_type = self._get_file_type(ext)
                
                # Create FileData
                file_data = {
                    "content": [],  # Will be filled on first read
                    "raw_content": base64.b64encode(file_bytes).decode('utf-8'),
                    "file_type": file_type,
                    "file_extension": ext,
                    "parsed": False,  # Mark as unparsed
                    "created_at": datetime.now().isoformat(),
                    "modified_at": datetime.now().isoformat(),
                    "metadata": {
                        "original_size": len(file_bytes),
                        "encoding": "base64",
                    }
                }
                
                # Save to store
                await self._put_file_data(file_path, file_data)
                
                responses.append(FileUploadResponse(path=file_path, error=None))
                
                logger.info(f"Uploaded file: {file_path} ({file_type}, {len(file_bytes)} bytes)")
                
            except Exception as e:
                logger.error(f"Failed to upload file {file_path}: {e}", exc_info=True)
                responses.append(FileUploadResponse(path=file_path, error="invalid_path"))
        
        return responses
    
    def download_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """
        Download files from store.
        
        Returns raw binary content (decoded from base64).
        
        Args:
            paths: List of file paths
            
        Returns:
            List of FileDownloadResponse objects
        """
        return self._run_async(self._download_files_async(paths))
    
    async def adownload_files(self, paths: list[str]) -> list[FileDownloadResponse]:
        """
        Async version of download_files.
        
        Args:
            paths: List of file paths
            
        Returns:
            List of FileDownloadResponse objects
        """
        return await self._download_files_async(paths)
    
    async def _download_files_async(self, paths: list[str]) -> list[FileDownloadResponse]:
        """Async implementation of download_files."""
        responses = []
        
        for file_path in paths:
            file_data = await self._get_file_data(file_path)
            
            if file_data is None:
                responses.append(FileDownloadResponse(
                    path=file_path,
                    content=None,
                    error="file_not_found"
                ))
                continue
            
            # Decode base64 to bytes
            raw_content_b64 = file_data.get("raw_content", "")
            if raw_content_b64:
                try:
                    content_bytes = base64.b64decode(raw_content_b64)
                    responses.append(FileDownloadResponse(
                        path=file_path,
                        content=content_bytes,
                        error=None
                    ))
                except Exception as e:
                    logger.error(f"Failed to decode file {file_path}: {e}")
                    responses.append(FileDownloadResponse(
                        path=file_path,
                        content=None,
                        error="invalid_path"
                    ))
            else:
                # Text file: encode content to bytes
                content_lines = file_data.get("content", [])
                content_str = "\n".join(content_lines)
                responses.append(FileDownloadResponse(
                    path=file_path,
                    content=content_str.encode('utf-8'),
                    error=None
                ))
        
        return responses
    
    # ========================================
    # New Methods: Parsed File Upload & Parse
    # ========================================
    
    def upload_parsed_files(
        self, 
        files: list[tuple[str, str, dict[str, Any]]]
    ) -> list[FileUploadResponse]:
        """
        Upload already-parsed files to workspace.
        
        This method stores parsed text content (not binary) directly to workspace.
        Used by file_handler after parsing or cache lookup.
        
        Args:
            files: List of (file_path, parsed_text, metadata) tuples
                - file_path: Path in workspace (e.g., /uploads/report.pdf)
                - parsed_text: Parsed content as plain text (newline-separated)
                - metadata: Dict with:
                    - content_hash: SHA256 hash
                    - file_key: Reference to th_files.file_key
                    - file_type: PDF/DOCX/IMAGE/TEXT/UNKNOWN
                    - file_extension: .pdf, .docx, etc.
                    - parsed: Boolean flag
                    - cache_hit: Whether content was from cache
                    - parse_method: Parse method used
                    - parse_model: Model used for parsing
                    - etc.
        
        Returns:
            List of FileUploadResponse objects
        """
        return self._run_async(self._upload_parsed_files_async(files))
    
    async def aupload_parsed_files(
        self, 
        files: list[tuple[str, str, dict[str, Any]]]
    ) -> list[FileUploadResponse]:
        """Async version of upload_parsed_files."""
        return await self._upload_parsed_files_async(files)
    
    async def _upload_parsed_files_async(
        self, 
        files: list[tuple[str, str, dict[str, Any]]]
    ) -> list[FileUploadResponse]:
        """Async implementation of upload_parsed_files."""
        responses = []
        
        for file_path, parsed_text, metadata in files:
            try:
                # Build file_data structure for new workspace table
                # content is stored as TEXT, not JSONB array
                file_data = {
                    "content": parsed_text.split("\n"),  # Will be joined in postgres_store.py
                    "file_key": metadata.get("file_key"),
                    "content_hash": metadata.get("content_hash"),
                    "file_type": metadata.get("file_type"),
                    "file_extension": metadata.get("file_extension"),
                    "parsed": metadata.get("parsed", True),
                    # All other metadata fields
                    **{k: v for k, v in metadata.items() 
                       if k not in ["file_key", "content_hash", "file_type", 
                                    "file_extension", "parsed"]}
                }
                
                # Save to workspace
                await self._put_file_data(file_path, file_data)
                
                responses.append(FileUploadResponse(path=file_path, error=None))
                
                logger.info(
                    f"Uploaded parsed file: {file_path} "
                    f"({metadata.get('file_type')}, {len(parsed_text)} chars, "
                    f"cache_hit={metadata.get('cache_hit', False)})"
                )
                
            except Exception as e:
                logger.error(f"Failed to upload parsed file {file_path}: {e}", exc_info=True)
                responses.append(FileUploadResponse(path=file_path, error="invalid_path"))
        
        return responses
    
    async def parse_file(
        self,
        file_bytes: bytes,
        file_name: str,
        file_type: str
    ) -> tuple[str, str, str]:
        """
        Parse file content to plain text.
        
        Uses FileParser to extract text from various file types.
        
        Args:
            file_bytes: Raw file binary content
            file_name: File name (for context)
            file_type: File type (PDF/DOCX/IMAGE/TEXT/UNKNOWN)
        
        Returns:
            Tuple of (parsed_text, parse_method, parse_model)
        """
        if not self.file_parser:
            logger.warning("FileParser not available, returning empty content")
            return ("", "none", "")
        
        try:
            # Convert bytes to BytesIO (FileParser expects FileInput = URL | IO)
            file_io = io.BytesIO(file_bytes)
            
            # Get file extension for file_type parameter
            file_ext = Path(file_name).suffix.lstrip('.')
            if not file_ext:
                file_ext = file_type.lower()
            
            # Use FileParser to parse content
            # FileParser.parse_file(file_input: FileInput, file_type: str, filename: Optional[str])
            parsed_text = await self.file_parser.parse_file(
                file_input=file_io,
                file_type=file_ext,
                filename=file_name
            )
            
            # FileParser returns string directly
            if isinstance(parsed_text, str):
                # Infer parse method from file type
                parse_method = f"fileparser-{file_ext}"
                parse_model = ""
                
                return (parsed_text, parse_method, parse_model)
            
            else:
                logger.warning(f"Unexpected parse result type: {type(parsed_text)}")
                return ("", "unknown", "")
        
        except Exception as e:
            logger.error(f"Parse failed for {file_name}: {e}", exc_info=True)
            return ("", "error", "")