"""
Unified cache configuration for file storage.

This module provides consistent cache TTL settings across all file processing layers.
Cache strategy: Outer layers have longer TTL, inner layers have shorter TTL.
"""

# Cache TTL in seconds (layered strategy)
CACHE_TTL_REDIS = 3600       # 1 hour - persistent layer (Redis)
CACHE_TTL_GLOBAL = 1200      # 20 min - shared across instances (module-level cache)
CACHE_TTL_LOCAL = 600        # 10 min - instance local (backend cache)

# Cache size limits
CACHE_MAX_FILES = 100        # Max files in global cache
CACHE_MAX_WORKERS = 4        # Max workers for async executor
