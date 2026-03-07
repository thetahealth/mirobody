from __future__ import annotations

import logging

from typing import TYPE_CHECKING, Any

try:
    import redis as redis_module
    import redis.asyncio as redis_asyncio_module
except ModuleNotFoundError as exc:
    redis_module = None
    redis_asyncio_module = None
    _redis_import_error = exc
else:
    _redis_import_error = None


if TYPE_CHECKING:
    import redis as redis_typing
    import redis.asyncio as redis_asyncio_typing

    SyncRedisClient = redis_typing.Redis
    AsyncRedisClient = redis_asyncio_typing.Redis
    SyncConnectionPool = redis_typing.ConnectionPool
    AsyncConnectionPool = redis_asyncio_typing.ConnectionPool
else:
    SyncRedisClient = Any
    AsyncRedisClient = Any
    SyncConnectionPool = Any
    AsyncConnectionPool = Any


_missing_dependency_logged = False


def redis_available() -> bool:
    return redis_module is not None and redis_asyncio_module is not None


def log_missing_redis_dependency() -> None:
    global _missing_dependency_logged

    if _missing_dependency_logged or redis_available():
        return

    _missing_dependency_logged = True
    logging.warning("redis package is not installed; Redis-backed features are disabled.")
    if _redis_import_error:
        logging.debug("redis import error: %s", _redis_import_error)
