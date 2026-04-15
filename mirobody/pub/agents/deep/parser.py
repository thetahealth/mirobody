"""
Unified File Parser for DeepAgent with th_files Cache

Parses files with caching from th_files table:
- Reads cache from th_files.original_text by content_hash
- Uses FileAbstractExtractor.extract_file_original_text for extraction
"""

import logging
from datetime import datetime, timezone
from typing import Union, BinaryIO, Optional, Dict, Any

from ....pulse.file_parser.services.file_abstract_extractor import FileAbstractExtractor
from ....utils.config import safe_read_cfg
from ....utils.db import execute_query
from .utils import calculate_content_hash

logger = logging.getLogger(__name__)


class FileParser:
    """
    File parser with th_files table cache.

    Cache flow:
    1. Check th_files.original_text by content_hash
    2. If miss, use FileAbstractExtractor.extract_file_original_text
    """

    def __init__(self):
        """Initialize parser with FileAbstractExtractor and cache config."""
        self.file_abstract_extractor = FileAbstractExtractor()
        self.cache_enabled = safe_read_cfg("GLOBAL_FILE_CACHE_ENABLED", True)

        if isinstance(self.cache_enabled, str):
            self.cache_enabled = self.cache_enabled.lower() in ("true", "1", "yes")

        logger.info(
            f"FileParser initialized: cache read {'enabled' if self.cache_enabled else 'disabled'}"
        )

    async def parse_file(
        self,
        file_input: Union[bytes, BinaryIO],
        filename: str,
        file_type: str
    ) -> tuple[str, str, str]:
        """
        Parse file with th_files cache lookup.

        Args:
            file_input: File content as bytes or BinaryIO
            filename: Original filename
            file_type: File type (PDF, IMAGE, TEXT, etc.)

        Returns:
            Tuple of (parsed_text, parse_method, parse_model)
        """
        try:
            # Normalize file type
            file_type = file_type.lower().lstrip(".")
            if "/" in file_type:
                file_type = file_type.split("/")[-1]

            # Read bytes if BinaryIO
            if hasattr(file_input, 'read'):
                if hasattr(file_input, 'seek'):
                    file_input.seek(0)
                file_bytes = file_input.read()
            else:
                file_bytes = file_input

            # Step 1: Calculate content hash for cache lookup
            content_hash = calculate_content_hash(file_bytes)

            # Step 2: Check cache (if enabled)
            if self.cache_enabled:
                cached_data = await self._get_cached_file_by_hash(content_hash)
                if cached_data:
                    parse_info = cached_data["parse_info"]
                    logger.info(
                        f"🎯 Cache hit: {filename} "
                        f"(method: {parse_info['method']}, age: {parse_info['age_hours']:.1f}h)"
                    )
                    return (
                        cached_data["content"],
                        parse_info["method"],
                        parse_info.get("model", "")
                    )

            # Step 3: Cache miss - extract original text
            logger.info(f"📄 Cache miss: {filename} - extracting via FileAbstractExtractor...")

            parse_start_time = datetime.now()
            content_type = self._get_content_type(file_type, filename)

            content = await self.file_abstract_extractor.extract_file_original_text(
                file_content=file_bytes,
                file_type=file_type,
                filename=filename,
                content_type=content_type
            )

            parse_duration_ms = int((datetime.now() - parse_start_time).total_seconds() * 1000)

            if content and len(content.strip()) > 10:
                logger.info(
                    f"✅ Extracted: {filename} ({len(content)} chars, {parse_duration_ms}ms)"
                )
                return (content, "file_abstract_extractor", "unified_extract")
            else:
                logger.warning(f"⚠️ Extraction returned empty for: {filename}")
                return ("", "file_abstract_extractor", "")

        except Exception as e:
            logger.error(f"File parsing failed for {filename}: {e}", exc_info=True)
            return (f"File parsing failed: {str(e)}", "error", "")

    def _get_content_type(self, file_type: str, filename: str) -> str:
        """Get MIME content type from file type and filename."""
        type_map = {
            "pdf": "application/pdf",
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "gif": "image/gif",
            "webp": "image/webp",
            "bmp": "image/bmp",
            "txt": "text/plain",
            "md": "text/markdown",
            "csv": "text/csv",
            "json": "application/json",
            "xml": "application/xml",
            "html": "text/html",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls": "application/vnd.ms-excel",
        }

        if file_type in type_map:
            return type_map[file_type]

        if filename and "." in filename:
            ext = filename.rsplit(".", 1)[-1].lower()
            if ext in type_map:
                return type_map[ext]

        return "application/octet-stream"

    async def _query_th_files_cache(
        self,
        lookup_key: str,
        lookup_type: str = "content_hash"
    ) -> Optional[Dict[str, Any]]:
        """
        Query th_files cache by content_hash or file_key.

        Args:
            lookup_key: The key value to search for
            lookup_type: Either "content_hash" or "file_key"

        Returns:
            Cached file data dict or None
        """
        if not lookup_key:
            return None

        where_clause = f"{lookup_type} = :lookup_key"
        query = f"""
            SELECT decrypt_content(original_text) as original_text, file_type,
                   decrypt_content(file_name) as file_name, text_length, content_hash, updated_at
            FROM th_files
            WHERE {where_clause} AND is_del = false
              AND original_text IS NOT NULL AND original_text != ''
            ORDER BY updated_at DESC LIMIT 1
        """

        try:
            rows = await execute_query(query, params={"lookup_key": lookup_key})
            if not rows:
                return None

            row = rows[0]
            original_text = row.get("original_text", "")
            if not original_text or len(original_text.strip()) < 10:
                return None

            # Calculate cache age
            updated_at = row.get("updated_at")
            parse_age_hours = 0
            if updated_at:
                # Handle timezone-aware datetime from database
                now = datetime.now(timezone.utc) if updated_at.tzinfo else datetime.now()
                parse_age_hours = (now - updated_at).total_seconds() / 3600

            # Extract file extension
            file_name = row.get("file_name", "")
            file_extension = ""
            if file_name and "." in file_name:
                file_extension = "." + file_name.rsplit(".", 1)[-1].lower()

            if lookup_type == "file_key":
                logger.info(f"🎯 Cache hit by file_key: {lookup_key} (age: {parse_age_hours:.1f}h)")

            return {
                "content": original_text,
                "file_type": row.get("file_type", ""),
                "file_extension": file_extension,
                "original_size": row.get("text_length", 0),
                "content_hash": row.get("content_hash", ""),
                "parse_info": {
                    "method": f"th_files_cache{'_by_key' if lookup_type == 'file_key' else ''}",
                    "model": "",
                    "duration_ms": 0,
                    "timestamp": updated_at,
                    "age_hours": parse_age_hours,
                }
            }
        except Exception as e:
            logger.error(f"Cache query failed ({lookup_type}={lookup_key[:16]}...): {e}")
            return None

    async def _get_cached_file_by_hash(self, content_hash: str) -> Optional[Dict[str, Any]]:
        """Get cached file by content hash."""
        return await self._query_th_files_cache(content_hash, "content_hash")

    async def get_cached_file_by_key(self, file_key: str) -> Optional[Dict[str, Any]]:
        """Get cached file by file_key."""
        return await self._query_th_files_cache(file_key, "file_key")
