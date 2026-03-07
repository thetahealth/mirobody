from __future__ import annotations

import asyncio
import logging
import threading

from datetime import datetime, timezone
from typing import Any

from .config.postgresql import PostgreSQLConfig


logger = logging.getLogger(__name__)


class _BaseDatabaseRedisClient:
    _KV_TABLE = "mirobody_runtime_kv"
    _HASH_TABLE = "mirobody_runtime_hash"
    _LIST_TABLE = "mirobody_runtime_list"

    def __init__(self, pg_config: PostgreSQLConfig):
        self._pg_config = pg_config

    @staticmethod
    def _stringify(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    @classmethod
    def _schema_statements(cls) -> list[str]:
        return [
            f"""
            CREATE TABLE IF NOT EXISTS {cls._KV_TABLE} (
                cache_key   TEXT PRIMARY KEY,
                cache_value TEXT NOT NULL,
                expires_at  TIMESTAMPTZ,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {cls._HASH_TABLE} (
                cache_key   TEXT NOT NULL,
                field_key   TEXT NOT NULL,
                field_value TEXT NOT NULL,
                expires_at  TIMESTAMPTZ,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (cache_key, field_key)
            );
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_{cls._HASH_TABLE}_expires_at
                ON {cls._HASH_TABLE} (expires_at);
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {cls._LIST_TABLE} (
                id          BIGSERIAL PRIMARY KEY,
                cache_key   TEXT NOT NULL,
                item_value  TEXT NOT NULL,
                expires_at  TIMESTAMPTZ,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_{cls._LIST_TABLE}_cache_key
                ON {cls._LIST_TABLE} (cache_key);
            """,
            f"""
            CREATE INDEX IF NOT EXISTS idx_{cls._LIST_TABLE}_expires_at
                ON {cls._LIST_TABLE} (expires_at);
            """,
        ]


class DatabaseAsyncRedisClient(_BaseDatabaseRedisClient):
    _schema_ready = False
    _schema_lock: asyncio.Lock | None = None

    async def _ensure_schema(self) -> None:
        if self.__class__._schema_ready:
            return

        if self.__class__._schema_lock is None:
            self.__class__._schema_lock = asyncio.Lock()

        async with self.__class__._schema_lock:
            if self.__class__._schema_ready:
                return

            async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
                async with conn.cursor() as cur:
                    for statement in self._schema_statements():
                        await cur.execute(statement)
                    await conn.commit()

            self.__class__._schema_ready = True

    async def _cleanup_key(self, key: str) -> None:
        await self._ensure_schema()

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                for table in (self._KV_TABLE, self._HASH_TABLE, self._LIST_TABLE):
                    await cur.execute(
                        f"DELETE FROM {table} WHERE cache_key=%s AND expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP;",
                        (key,),
                    )
                await conn.commit()

    async def ping(self) -> bool:
        await self._ensure_schema()
        return True

    async def aclose(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def get(self, name: str) -> str | None:
        await self._cleanup_key(name)

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT cache_value FROM {self._KV_TABLE} WHERE cache_key=%s;",
                    (name,),
                )
                row = await cur.fetchone()
                return row[0] if row else None

    async def set(
        self,
        name: str,
        value: Any,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        await self._ensure_schema()

        expires_clause = "CURRENT_TIMESTAMP + (%s * INTERVAL '1 second')" if ex else "NULL"
        params: list[Any] = [name, self._stringify(value)]
        if ex:
            params.append(ex)

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                if nx:
                    sql = f"""
                        INSERT INTO {self._KV_TABLE} (cache_key, cache_value, expires_at, updated_at)
                        VALUES (%s, %s, {expires_clause}, CURRENT_TIMESTAMP)
                        ON CONFLICT (cache_key) DO UPDATE
                        SET cache_value = EXCLUDED.cache_value,
                            expires_at = EXCLUDED.expires_at,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE {self._KV_TABLE}.expires_at IS NOT NULL
                          AND {self._KV_TABLE}.expires_at <= CURRENT_TIMESTAMP
                        RETURNING 1;
                    """
                    await cur.execute(sql, tuple(params))
                    row = await cur.fetchone()
                    await conn.commit()
                    return bool(row)

                sql = f"""
                    INSERT INTO {self._KV_TABLE} (cache_key, cache_value, expires_at, updated_at)
                    VALUES (%s, %s, {expires_clause}, CURRENT_TIMESTAMP)
                    ON CONFLICT (cache_key) DO UPDATE
                    SET cache_value = EXCLUDED.cache_value,
                        expires_at = EXCLUDED.expires_at,
                        updated_at = CURRENT_TIMESTAMP;
                """
                await cur.execute(sql, tuple(params))
                await conn.commit()
                return True

    async def setex(self, name: str, time: int, value: Any) -> bool:
        return await self.set(name=name, value=value, ex=time)

    async def delete(self, *names: str) -> int:
        if not names:
            return 0

        await self._ensure_schema()
        deleted = 0

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                for table in (self._KV_TABLE, self._HASH_TABLE, self._LIST_TABLE):
                    await cur.execute(
                        f"DELETE FROM {table} WHERE cache_key = ANY(%s);",
                        (list(names),),
                    )
                    deleted += cur.rowcount or 0
                await conn.commit()

        return deleted

    async def expire(self, name: str, time: int) -> bool:
        await self._cleanup_key(name)
        updated = 0

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                for table in (self._KV_TABLE, self._HASH_TABLE, self._LIST_TABLE):
                    await cur.execute(
                        f"""
                        UPDATE {table}
                        SET expires_at = CURRENT_TIMESTAMP + (%s * INTERVAL '1 second')
                        WHERE cache_key = %s;
                        """,
                        (time, name),
                    )
                    updated += cur.rowcount or 0
                await conn.commit()

        return updated > 0

    async def ttl(self, name: str) -> int:
        await self._cleanup_key(name)

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                for table in (self._KV_TABLE, self._HASH_TABLE, self._LIST_TABLE):
                    await cur.execute(
                        f"""
                        SELECT expires_at, EXTRACT(EPOCH FROM (expires_at - CURRENT_TIMESTAMP))::BIGINT
                        FROM {table}
                        WHERE cache_key = %s
                        LIMIT 1;
                        """,
                        (name,),
                    )
                    row = await cur.fetchone()
                    if row:
                        expires_at, ttl_seconds = row
                        if expires_at is None:
                            return -1
                        return max(int(ttl_seconds or 0), 0)

        return -2

    async def incr(self, name: str) -> int:
        await self._ensure_schema()

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT cache_value, expires_at
                    FROM {self._KV_TABLE}
                    WHERE cache_key = %s
                    FOR UPDATE;
                    """,
                    (name,),
                )
                row = await cur.fetchone()
                now = datetime.now(timezone.utc)
                if not row or (row[1] is not None and row[1] <= now):
                    value = 1
                    await cur.execute(
                        f"""
                        INSERT INTO {self._KV_TABLE} (cache_key, cache_value, expires_at, updated_at)
                        VALUES (%s, %s, NULL, CURRENT_TIMESTAMP)
                        ON CONFLICT (cache_key) DO UPDATE
                        SET cache_value = EXCLUDED.cache_value,
                            expires_at = NULL,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        (name, str(value)),
                    )
                else:
                    value = int(str(row[0]).strip()) + 1
                    await cur.execute(
                        f"""
                        UPDATE {self._KV_TABLE}
                        SET cache_value = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cache_key = %s;
                        """,
                        (str(value), name),
                    )
                await conn.commit()
                return value

    async def hset(self, name: str, mapping: dict[str, Any]) -> int:
        if not mapping:
            return 0

        await self._cleanup_key(name)

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                for field, value in mapping.items():
                    await cur.execute(
                        f"""
                        INSERT INTO {self._HASH_TABLE} (cache_key, field_key, field_value, updated_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (cache_key, field_key) DO UPDATE
                        SET field_value = EXCLUDED.field_value,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        (name, field, self._stringify(value)),
                    )
                await conn.commit()

        return len(mapping)

    async def hgetall(self, name: str) -> dict[str, str]:
        await self._cleanup_key(name)

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT field_key, field_value FROM {self._HASH_TABLE} WHERE cache_key=%s;",
                    (name,),
                )
                rows = await cur.fetchall()
                return {row[0]: row[1] for row in rows}

    async def rpush(self, name: str, *values: Any) -> int:
        if not values:
            return 0

        await self._cleanup_key(name)

        async with await self._pg_config.get_async_client(cursor_factory=None) as conn:
            async with conn.cursor() as cur:
                for value in values:
                    await cur.execute(
                        f"INSERT INTO {self._LIST_TABLE} (cache_key, item_value) VALUES (%s, %s);",
                        (name, self._stringify(value)),
                    )
                await cur.execute(
                    f"SELECT COUNT(*) FROM {self._LIST_TABLE} WHERE cache_key=%s;",
                    (name,),
                )
                row = await cur.fetchone()
                await conn.commit()
                return int(row[0]) if row else 0

    async def eval(self, script: str, numkeys: int, *args: Any) -> int:
        if numkeys != 1 or len(args) < 2:
            raise NotImplementedError("Only single-key compare-and-delete eval is supported.")

        if 'redis.call("get"' not in script or 'redis.call("del"' not in script:
            raise NotImplementedError("Unsupported eval script for database-backed Redis compatibility.")

        key = str(args[0])
        expected_value = self._stringify(args[1])
        current_value = await self.get(key)
        if current_value == expected_value:
            return await self.delete(key)
        return 0


class DatabaseSyncRedisClient(_BaseDatabaseRedisClient):
    _schema_ready = False
    _schema_lock = threading.Lock()

    def _ensure_schema(self) -> None:
        if self.__class__._schema_ready:
            return

        with self.__class__._schema_lock:
            if self.__class__._schema_ready:
                return

            with self._pg_config.get_client(cursor_factory=None) as conn:
                with conn.cursor() as cur:
                    for statement in self._schema_statements():
                        cur.execute(statement)
                    conn.commit()

            self.__class__._schema_ready = True

    def ping(self) -> bool:
        self._ensure_schema()
        return True

    def close(self) -> None:
        return None


def apply_db_redis_overrides() -> None:
    from .config import global_config
    from .config.redis import RedisConfig

    async_clients: dict[str, DatabaseAsyncRedisClient] = {}
    sync_clients: dict[str, DatabaseSyncRedisClient] = {}

    async def _get_async_client(self: RedisConfig) -> DatabaseAsyncRedisClient | None:
        cfg = global_config()
        if cfg is None:
            logger.error("Global config is not initialized; database-backed Redis client is unavailable.")
            return None

        pg_config = cfg.get_postgresql()
        key = f"{pg_config.host}:{pg_config.port}/{pg_config.database}:{pg_config.schema}"
        client = async_clients.get(key)
        if client is None:
            client = DatabaseAsyncRedisClient(pg_config)
            async_clients[key] = client
        await client.ping()
        return client

    def _get_client(self: RedisConfig) -> DatabaseSyncRedisClient | None:
        cfg = global_config()
        if cfg is None:
            logger.error("Global config is not initialized; database-backed Redis client is unavailable.")
            return None

        pg_config = cfg.get_postgresql()
        key = f"{pg_config.host}:{pg_config.port}/{pg_config.database}:{pg_config.schema}"
        client = sync_clients.get(key)
        if client is None:
            client = DatabaseSyncRedisClient(pg_config)
            sync_clients[key] = client
        client.ping()
        return client

    def _get_async_pool(self: RedisConfig) -> None:
        return None

    def _get_pool(self: RedisConfig) -> None:
        return None

    def _print(self: RedisConfig) -> None:
        cfg = global_config()
        if cfg is None:
            print("cache           : database-backed (config unavailable)")
            return

        pg_config = cfg.get_postgresql()
        print(f"cache           : postgresql://{pg_config.host}:{pg_config.port}/{pg_config.database}")

    RedisConfig.get_async_client = _get_async_client
    RedisConfig.get_client = _get_client
    RedisConfig.get_async_pool = _get_async_pool
    RedisConfig.get_pool = _get_pool
    RedisConfig.print = _print
