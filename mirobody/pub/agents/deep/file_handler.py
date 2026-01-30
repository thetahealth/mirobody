"""
File Handler Module for DeepAgent

Handles file upload and processing logic for PostgreSQL backend.
Integrates with global file cache for efficient parsing and storage.
"""

import asyncio
import logging
import os
import re
import time
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import httpx

from .backends.cache_manager import (
    get_cache_manager,
    calculate_content_hash
)
from .backends.file_parser import get_file_type_from_extension

logger = logging.getLogger(__name__)


def _sanitize_text(text: str) -> str:
    """
    Sanitize text for PostgreSQL by replacing unsafe characters with spaces.
    
    Removes/replaces:
    - NULL bytes (0x00) - PostgreSQL incompatible
    - C0/C1 control chars (except tab/newline/CR) - potential issues
    - Zero-width chars (ZWSP, ZWNJ, etc) - security risk
    - Bidirectional marks (LTR/RTL) - Trojan Source attack vector
    - Invalid UTF-8 sequences
    
    Args:
        text: Raw text from file parsing
        
    Returns:
        Database-safe text with dangerous chars replaced by spaces
        
    Example:
        >>> _sanitize_text("hello\x00world\u200Btest")
        'hello world test'
    """
    if not text:
        return text
    
    # Pattern: NULL + dangerous C0/C1 controls + zero-width + bidi marks
    # Keep: \t(09) \n(0A) \r(0D)
    pattern = (
        r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F'  # NULL + C0/C1 controls
        r'\u200B-\u200D\uFEFF'  # Zero-width spaces
        r'\u202A-\u202E'  # Bidirectional marks
        r'\uFFFE\uFFFF]'  # Non-characters
    )
    
    cleaned = re.sub(pattern, ' ', text)
    
    # Handle invalid UTF-8 by encoding/decoding with error replacement
    try:
        cleaned = cleaned.encode('utf-8', errors='replace').decode('utf-8')
    except Exception:
        pass
    
    if cleaned != text:
        diff = len(text) - len(cleaned.replace(' ', ''))
        logger.warning(f"Sanitized {diff} unsafe char(s) from text")
    
    return cleaned


def _build_file_metadata(
    content_hash: str,
    file_key: str,
    file_type: str,
    file_extension: str,
    original_size: int,
    cache_hit: bool,
    parse_method: str,
    parse_model: str,
    parse_info: dict = None,
    parse_duration_ms: int = None
) -> dict:
    """
    Build unified file metadata dictionary.
    
    Args:
        content_hash: SHA256 hash of file content
        file_key: S3 key or storage identifier
        file_type: File type (PDF, IMAGE, TEXT, etc.)
        file_extension: File extension (.pdf, .jpg, etc.)
        original_size: Original file size in bytes
        cache_hit: Whether cache was hit
        parse_method: Parse method used
        parse_model: Model used for parsing
        parse_info: Parse info from cache (for cache hit)
        parse_duration_ms: Parse duration (for cache miss)
        
    Returns:
        Metadata dictionary
    """
    metadata = {
        "content_hash": content_hash,
        "file_key": file_key,
        "file_type": file_type,
        "file_extension": file_extension,
        "parsed": True,
        "cache_hit": cache_hit,
        "parse_method": parse_method,
        "parse_model": parse_model,
        "original_size": original_size,
    }
    
    if cache_hit and parse_info:
        metadata["parse_timestamp"] = parse_info["timestamp"].isoformat()
        metadata["parse_age_hours"] = parse_info["age_hours"]
    elif not cache_hit and parse_duration_ms is not None:
        metadata["parse_duration_ms"] = parse_duration_ms
    
    return metadata


def _build_reminder_message(uploaded_paths: list[str]) -> str:
    """
    Build reminder message for successfully uploaded files.
    
    Args:
        uploaded_paths: List of uploaded file paths
        
    Returns:
        Formatted reminder message
    """
    if not uploaded_paths:
        return ""
    
    if len(uploaded_paths) == 1:
        return (
            f"Uploaded and parsed: {os.path.basename(uploaded_paths[0])}\n"
            f"Path: {uploaded_paths[0]}\n\n"
            f"Use read_file(\"{uploaded_paths[0]}\") to read the parsed content"
        )
    else:
        file_items = [
            f"{i+1}. {os.path.basename(p)}" 
            for i, p in enumerate(uploaded_paths)
        ]
        files_text = "\n".join(file_items)
        return (
            f"Uploaded and parsed {len(uploaded_paths)} files:\n{files_text}\n\n"
            f"Example: read_file(\"{uploaded_paths[0]}\")"
        )


async def _process_single_file(
    file_info: dict[str, Any],
    files_content_map: dict[str, bytes],
    backend: Any,
    cache_manager: Any
) -> Optional[tuple[str, str, dict]]:
    """
    Process single file with caching and parsing.

    Args:
        file_info: File information dict from file_list
        files_content_map: Pre-loaded file content mapping (file_key/filename -> bytes)
        backend: Backend instance for parsing
        cache_manager: Cache manager instance

    Returns:
        Tuple of (file_path, parsed_text, metadata) or None if failed
    """
    try:
        file_name = file_info.get("file_name")
        file_url = file_info.get("file_url")
        file_key = file_info.get("file_key")
        file_type = file_info.get("file_type", "").upper()

        # Skip files without required fields
        if not file_name or not file_url:
            logger.warning(f"Skipping file with missing name or URL: {file_info}")
            return None

        # Create workspace file path
        file_path = f"/uploads/{file_name}"

        # Step 1: Get file binary content
        file_bytes = None

        # Priority 1: Use pre-loaded content from files_content_map (fastest)
        if file_key and file_key in files_content_map:
            file_bytes = files_content_map[file_key]
            logger.info(f"✅ Content cache hit (by file_key): {file_name} ({len(file_bytes)} bytes)")
        elif file_name in files_content_map:
            file_bytes = files_content_map[file_name]
            logger.info(f"✅ Content cache hit (by filename): {file_name} ({len(file_bytes)} bytes)")

        # Priority 2: Download from URL as fallback
        if not file_bytes and file_url:
            parsed_url = urlparse(file_url)
            if parsed_url.scheme in ("http", "https"):
                logger.info(f"📥 Downloading from URL: {file_name}")
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(file_url)
                    response.raise_for_status()
                    file_bytes = response.content
                logger.info(f"✅ Downloaded {len(file_bytes)} bytes: {file_name}")
            elif parsed_url.scheme != "file":
                logger.warning(f"⚠️ Unsupported URL scheme for {file_name}: {parsed_url.scheme}")
                return None

        if not file_bytes:
            logger.warning(f"⚠️ No content for {file_name}")
            return None
        
        # Step 2: Calculate content hash
        content_hash = calculate_content_hash(file_bytes)
        logger.debug(f"Content hash for {file_name}: {content_hash[:12]}...")
        
        # Step 3: Check global cache
        cached_data = await cache_manager.get_cached_file(content_hash)
        
        if cached_data:
            # Cache hit! Use cached parsed content
            parsed_text = cached_data["content"]
            parse_info = cached_data["parse_info"]
            file_extension = cached_data.get("file_extension", os.path.splitext(file_name)[1])
            
            # Sanitize text to remove NULL bytes (source protection)
            parsed_text = _sanitize_text(parsed_text)
            
            logger.info(
                f"🎯 Global cache hit: {file_name} "
                f"(method: {parse_info['method']}, "
                f"age: {parse_info['age_hours']:.1f}h, "
                f"saved parsing time!)"
            )
            
            # Build metadata
            metadata = _build_file_metadata(
                content_hash=content_hash,
                file_key=file_key,
                file_type=file_type or cached_data.get("file_type", "UNKNOWN"),
                file_extension=file_extension,
                original_size=len(file_bytes),
                cache_hit=True,
                parse_method=parse_info["method"],
                parse_model=parse_info.get("model", ""),
                parse_info=parse_info
            )
            
            return (file_path, parsed_text, metadata)
        
        else:
            # Cache miss - need to parse file
            logger.info(f"📄 Global cache miss: {file_name} - parsing now...")
            
            # Infer file type from extension if not provided
            file_extension = os.path.splitext(file_name)[1]
            if not file_type:
                file_type = get_file_type_from_extension(file_extension)
            
            # Parse file directly using backend's parser
            parse_start = time.time()
            try:
                result = await backend.parse_file(
                    file_bytes=file_bytes,
                    file_name=file_name,
                    file_type=file_type
                )
                
                if isinstance(result, tuple) and len(result) == 3:
                    parsed_text, parse_method, parse_model = result
                elif isinstance(result, str):
                    parsed_text = result
                    parse_method = f"default-{file_type.lower()}"
                    parse_model = ""
                else:
                    logger.warning(f"Unexpected parse result type: {type(result)}")
                    parsed_text = ""
                    parse_method = "unknown"
                    parse_model = ""
            except Exception as e:
                logger.error(f"Parse failed for {file_name}: {e}", exc_info=True)
                parsed_text = ""
                parse_method = "error"
                parse_model = ""
            
            parse_duration_ms = int((time.time() - parse_start) * 1000)
            
            if not parsed_text:
                logger.warning(f"⚠️ Failed to parse {file_name}, skipping")
                return None
            
            # Sanitize text to remove NULL bytes (source protection)
            parsed_text = _sanitize_text(parsed_text)
            
            logger.info(
                f"✅ Parsed {file_name}: {len(parsed_text)} chars, "
                f"{len(parsed_text.split(chr(10)))} lines, "
                f"{parse_duration_ms}ms ({parse_method})"
            )
            
            # Save to global cache
            await cache_manager.save_cached_file(
                content_hash=content_hash,
                content=parsed_text,
                file_type=file_type,
                file_extension=file_extension,
                original_size=len(file_bytes),
                parse_method=parse_method,
                parse_model=parse_model,
                parse_duration_ms=parse_duration_ms,
                file_key=file_key
            )
            
            # Build metadata
            metadata = _build_file_metadata(
                content_hash=content_hash,
                file_key=file_key,
                file_type=file_type,
                file_extension=file_extension,
                original_size=len(file_bytes),
                cache_hit=False,
                parse_method=parse_method,
                parse_model=parse_model,
                parse_duration_ms=parse_duration_ms
            )
            
            return (file_path, parsed_text, metadata)
    
    except Exception as e:
        logger.error(f"❌ Failed to process file {file_info.get('file_name', 'unknown')}: {e}", exc_info=True)
        return None


async def upload_files_to_backend(
    file_list: list[dict[str, Any]], 
    backend: Any,
    files_data: list[dict[str, Any]] | None = None
) -> tuple[list[str], str]:
    """
    Upload files to PostgreSQL backend with concurrent processing and global cache support.
    
    Workflow (concurrent for each file):
    1. Read file from local path
    2. Calculate content hash for each file
    3. Check global cache for parsed content
    4. If cache miss, parse file and save to cache
    5. Store parsed text in workspace (not binary)
    
    Performance optimization: 
    - Files are processed concurrently for better performance
    - If files_data is provided (from HTTP layer), use local paths to avoid re-downloading
    - Memory-efficient: only file paths are passed, not content
    
    Args:
        file_list: List of file info dicts with keys:
            - file_name: Name of the file
            - file_url: URL to access the file (file://, http://, https://)
            - file_type: Type of file (pdf, image, etc.)
            - file_key: S3 key or storage identifier (optional)
            - file_size: File size in bytes (optional)
        backend: PostgresBackend instance with upload_files() method
        files_data: Optional list of already-cached file data dicts with:
            - content: File content (bytes)
            - file_name: File name
            - content_type: MIME type
            - file_key: S3 key

    Returns:
        Tuple of (uploaded_file_paths, reminder_message)
    """
    if not file_list:
        return ([], "")

    cache_manager = get_cache_manager()

    # Build file_key/filename -> content mapping for fast lookup (if files_data provided)
    files_content_map = {}
    if files_data:
        for file_data in files_data:
            filename = file_data.get("file_name")
            file_key = file_data.get("file_key")
            content = file_data.get("content")

            if content:
                # Index by both file_key (most reliable) and filename
                if file_key:
                    files_content_map[file_key] = content
                if filename:
                    files_content_map[filename] = content

        logger.info(f"✅ Using {len(files_data)} pre-cached file contents (avoiding re-download)")

    # 🚀 Concurrent processing: Create tasks for all files
    process_tasks = [
        _process_single_file(file_info, files_content_map, backend, cache_manager)
        for file_info in file_list
    ]
    
    # Execute all file processing concurrently
    results = await asyncio.gather(*process_tasks, return_exceptions=True)
    
    # Collect successful results
    files_to_upload = []
    uploaded_paths = []
    
    for result in results:
        if result and isinstance(result, tuple):
            file_path, parsed_text, metadata = result
            files_to_upload.append((file_path, parsed_text, metadata))
            uploaded_paths.append(file_path)
        elif isinstance(result, Exception):
            logger.error(f"File processing task failed with exception: {result}")
    
    logger.info(
        f"✅ Concurrent processing completed: {len(files_to_upload)}/{len(file_list)} files successful"
    )
    
    # Upload all parsed files to PostgreSQL backend (workspace)
    if files_to_upload:
        try:
            # Upload to workspace (parsed text only, not binary)
            upload_results = backend.upload_parsed_files(files_to_upload)
            
            # Check for upload errors
            successful_uploads = []
            for result in upload_results:
                if result.error:
                    logger.error(f"❌ Upload failed for {result.path}: {result.error}")
                else:
                    successful_uploads.append(result.path)
                    logger.info(f"✅ Saved to workspace: {result.path}")
            
            # Create reminder message
            if successful_uploads:
                reminder = _build_reminder_message(successful_uploads)
                return (successful_uploads, reminder)
            
        except Exception as e:
            logger.error(f"❌ Backend upload failed: {e}", exc_info=True)
    
    return ([], "")
