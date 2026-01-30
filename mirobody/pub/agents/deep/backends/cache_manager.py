"""
Global File Cache Manager for DeepAgent

Provides cross-session file caching based on file content hash (SHA256).
Avoids redundant file parsing by sharing parsed content across users and sessions.

Cache Strategy:
    - WRITE: Always enabled - builds up global cache library automatically
    - READ: Controlled by GLOBAL_FILE_CACHE_ENABLED config
        * true (default): Use cache when available (speed optimization)
        * false: Always parse fresh (use latest parsing models)

Configuration:
    GLOBAL_FILE_CACHE_ENABLED: true/false (default: true)
    - Controls whether to READ from cache
    - Cache writes always happen regardless of this setting

Usage:
    cache_manager = get_cache_manager()
    
    # Check cache before parsing (respects GLOBAL_FILE_CACHE_ENABLED)
    cached = await cache_manager.get_cached_file(content_hash)
    if cached:
        # Use cached content
        content = cached["content"]
    else:
        # Parse and save to cache
        content = await parse_file(...)
        # Always saves, regardless of GLOBAL_FILE_CACHE_ENABLED
        await cache_manager.save_cached_file(content_hash, content, ...)
"""

import hashlib
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from .....utils.config import safe_read_cfg
from .....utils.db import execute_query

logger = logging.getLogger(__name__)


class FileCacheManager:
    """
    Global file cache manager.
    
    Manages a shared cache of parsed file content across all users and sessions.
    Files are identified by SHA256 hash of their binary content.
    
    Cache Behavior:
    - WRITE: Always enabled - parsed files are always saved to build up the cache library
    - READ: Controlled by GLOBAL_FILE_CACHE_ENABLED config
        * True (default): Use cache when available (fast, may use older parsing)
        * False: Always parse fresh (slower, but uses latest parsing models)
    
    This design allows continuous cache accumulation while giving users control
    over whether to use cached results or force fresh parsing.
    """
    
    def __init__(self):
        """
        Initialize cache manager with configuration.
        
        Note: 
        - Cache WRITE is always enabled (builds up global cache library)
        - GLOBAL_FILE_CACHE_ENABLED only controls cache READ behavior
        - When disabled, files are still parsed and saved to cache, but cache lookup is skipped
        """
        # Read configuration (controls whether to READ from cache)
        self.enabled = safe_read_cfg("GLOBAL_FILE_CACHE_ENABLED", True)  # 默认启用缓存读取
        
        if isinstance(self.enabled, str):
            self.enabled = self.enabled.lower() in ("true", "1", "yes")
        
        logger.info(
            f"FileCacheManager initialized: "
            f"cache read {'enabled' if self.enabled else 'disabled'} "
            f"(cache write always enabled)"
        )
    
    async def get_cached_file(
        self,
        content_hash: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached file by content hash.
        
        Args:
            content_hash: SHA256 hash of file binary content
            
        Returns:
            Dictionary with cached data:
            {
                "content": "parsed text content",
                "file_type": "PDF",
                "file_extension": ".pdf",
                "original_size": 1024000,
                "parse_info": {
                    "method": "gemini-vision-2.0",
                    "model": "gemini-2.0-flash-exp",
                    "duration_ms": 3521,
                    "timestamp": datetime(...),
                    "age_hours": 3.5
                }
            }
            
            Returns None if cache miss or cache read disabled.
        """
        if not self.enabled:
            logger.debug("Cache read disabled, skipping cache lookup")
            return None
        
        query = """
            SELECT 
                content,
                file_type,
                file_extension,
                original_size,
                parse_method,
                parse_model,
                parse_duration_ms,
                parse_timestamp,
                line_count,
                char_count,
                reference_count
            FROM deep_agent_file_cache
            WHERE content_hash = :content_hash
        """
        
        try:
            rows = await execute_query(
                query=query,
                params={"content_hash": content_hash}
            )
            
            if not rows or len(rows) == 0:
                logger.debug(f"Cache miss: {content_hash[:12]}...")
                return None
            
            row = rows[0]
            parse_timestamp = row["parse_timestamp"]
            parse_age = datetime.now() - parse_timestamp
            parse_age_hours = parse_age.total_seconds() / 3600
            
            # Update access statistics
            await self._update_cache_access(content_hash)
            
            # Build cache data
            cache_data = {
                "content": row["content"],
                "file_type": row["file_type"],
                "file_extension": row.get("file_extension"),
                "original_size": row.get("original_size"),
                "parse_info": {
                    "method": row.get("parse_method", "unknown"),
                    "model": row.get("parse_model", ""),
                    "duration_ms": row.get("parse_duration_ms", 0),
                    "timestamp": parse_timestamp,
                    "age_hours": parse_age_hours,
                },
                "stats": {
                    "line_count": row.get("line_count", 0),
                    "char_count": row.get("char_count", 0),
                    "reference_count": row.get("reference_count", 0),
                }
            }
            
            logger.info(
                f"✅ Cache hit: {content_hash[:12]}... "
                f"(method: {cache_data['parse_info']['method']}, "
                f"age: {parse_age_hours:.1f}h, "
                f"refs: {cache_data['stats']['reference_count']})"
            )
            
            return cache_data
            
        except Exception as e:
            logger.error(f"Failed to get cache for {content_hash[:12]}...: {e}", exc_info=True)
            return None
    
    async def save_cached_file(
        self,
        content_hash: str,
        content: str,
        file_type: str,
        file_extension: str,
        original_size: int,
        parse_method: str,
        parse_model: str = "",
        parse_duration_ms: int = 0,
        file_key: str = None
    ) -> bool:
        """
        Save parsed file content to global cache.
        
        Note: Always attempts to save to cache regardless of GLOBAL_FILE_CACHE_ENABLED.
        This builds up the global cache library. The GLOBAL_FILE_CACHE_ENABLED config
        only controls whether to READ from cache (get_cached_file).
        
        Args:
            content_hash: SHA256 hash of file binary
            content: Parsed text content
            file_type: File type (PDF/DOCX/IMAGE/TEXT)
            file_extension: File extension (.pdf, .docx, etc.)
            original_size: Original file size in bytes
            parse_method: Parse method used (e.g., "gemini-vision-2.0")
            parse_model: Specific model used (e.g., "gemini-2.0-flash-exp")
            parse_duration_ms: Parse duration in milliseconds
            file_key: Optional file_key from th_files
            
        Returns:
            True if saved successfully, False otherwise
        """
        
        try:
            lines = content.split("\n")
            
            query = """
                INSERT INTO deep_agent_file_cache (
                    content_hash,
                    content,
                    file_type,
                    file_extension,
                    original_size,
                    parse_method,
                    parse_model,
                    parse_duration_ms,
                    parse_timestamp,
                    line_count,
                    char_count,
                    first_file_key,
                    reference_count,
                    last_accessed_at,
                    created_at,
                    updated_at
                ) VALUES (
                    :content_hash,
                    :content,
                    :file_type,
                    :file_extension,
                    :original_size,
                    :parse_method,
                    :parse_model,
                    :parse_duration_ms,
                    NOW(),
                    :line_count,
                    :char_count,
                    :first_file_key,
                    1,
                    NOW(),
                    NOW(),
                    NOW()
                )
                ON CONFLICT (content_hash) DO UPDATE SET
                    reference_count = deep_agent_file_cache.reference_count + 1,
                    last_accessed_at = NOW(),
                    updated_at = NOW()
            """
            
            await execute_query(
                query=query,
                params={
                    "content_hash": content_hash,
                    "content": content,
                    "file_type": file_type,
                    "file_extension": file_extension,
                    "original_size": original_size,
                    "parse_method": parse_method,
                    "parse_model": parse_model,
                    "parse_duration_ms": parse_duration_ms,
                    "line_count": len(lines),
                    "char_count": len(content),
                    "first_file_key": file_key
                }
            )
            
            logger.info(
                f"✅ Saved to cache: {content_hash[:12]}... "
                f"({parse_method}, {len(content)} chars, {len(lines)} lines)"
            )
            
            return True
            
        except Exception as e:
            logger.error(
                f"Failed to save cache for {content_hash[:12]}...: {e}",
                exc_info=True
            )
            return False
    
    async def _update_cache_access(self, content_hash: str) -> None:
        """
        Update cache access statistics (internal use).
        
        Increments reference_count and updates last_accessed_at.
        """
        try:
            query = """
                UPDATE deep_agent_file_cache
                SET 
                    reference_count = reference_count + 1,
                    last_accessed_at = NOW(),
                    updated_at = NOW()
                WHERE content_hash = :content_hash
            """
            
            await execute_query(
                query=query,
                params={"content_hash": content_hash}
            )
            
        except Exception as e:
            # Non-critical error, just log it
            logger.debug(f"Failed to update cache access: {e}")
    
    async def cleanup_old_cache(
        self,
        days: int = 90,
        min_references: int = 2
    ) -> int:
        """
        Clean up old unused cache entries (optional maintenance).
        
        Args:
            days: Remove entries not accessed in this many days
            min_references: Keep entries with at least this many references
            
        Returns:
            Number of entries deleted
        """
        if not self.enabled:
            return 0
        
        try:
            query = """
                DELETE FROM deep_agent_file_cache
                WHERE last_accessed_at < NOW() - INTERVAL ':days days'
                  AND reference_count < :min_references
                RETURNING content_hash
            """
            
            result = await execute_query(
                query=query,
                params={
                    "days": days,
                    "min_references": min_references
                }
            )
            
            deleted = len(result) if result else 0
            
            if deleted > 0:
                logger.info(
                    f"🗑️ Cleaned up {deleted} old cache entries "
                    f"(> {days}d, < {min_references} refs)"
                )
            
            return deleted
            
        except Exception as e:
            logger.error(f"Failed to cleanup cache: {e}", exc_info=True)
            return 0


def calculate_content_hash(file_bytes: bytes) -> str:
    """
    Calculate SHA256 hash of file content.
    
    Args:
        file_bytes: File binary content
        
    Returns:
        64-character hexadecimal hash string
    """
    return hashlib.sha256(file_bytes).hexdigest()


# Global singleton
_cache_manager: Optional[FileCacheManager] = None


def get_cache_manager() -> FileCacheManager:
    """
    Get global cache manager instance (singleton).
    
    Returns:
        FileCacheManager instance
    """
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = FileCacheManager()
    return _cache_manager
