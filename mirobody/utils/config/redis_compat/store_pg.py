"""PostgreSQL-backed store backend."""

from __future__ import annotations

import asyncio


class PgStore:
    """Drop-in replacement for MemoryStore, backed by PostgreSQL.

    Uses mirobody_runtime_kv/hash/list/set tables.
    """

    _KV = "mirobody_runtime_kv"
    _HASH = "mirobody_runtime_hash"
    _SET = "mirobody_runtime_set"
    _LIST = "mirobody_runtime_list"
    _schema_ready = False

    def __init__(self, pg_config):
        self._pg = pg_config
        self._pool = None
        self._pool_lock = asyncio.Lock()

    async def _get_pool(self):
        """Lazily create a connection pool and ensure the schema exists."""
        if self._pool is not None:
            return self._pool
        async with self._pool_lock:
            if self._pool is not None:
                return self._pool
            import psycopg_pool
            self._pool = psycopg_pool.AsyncConnectionPool(
                f"host={self._pg.host} port={self._pg.port} "
                f"dbname={self._pg.database}",
                open=False,
                min_size=self._pg.minconn,
                max_size=self._pg.maxconn,
                kwargs=dict(
                    user=self._pg.user,
                    password=self._pg.password,
                    options=f"-c search_path={self._pg.schema}"
                            f" -c app.encryption_key={self._pg.encrypt_key}",
                    cursor_factory=None,
                ),
            )
            await self._pool.open()
            await self._ensure_schema()
            return self._pool

    async def _ensure_schema(self):
        if PgStore._schema_ready:
            return
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                for sql in [
                    f"""CREATE TABLE IF NOT EXISTS {self._KV} (
                        cache_key   TEXT PRIMARY KEY,
                        cache_value TEXT NOT NULL,
                        expires_at  TIMESTAMPTZ,
                        updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )""",
                    f"""CREATE TABLE IF NOT EXISTS {self._HASH} (
                        cache_key   TEXT NOT NULL,
                        field_key   TEXT NOT NULL,
                        field_value TEXT NOT NULL,
                        expires_at  TIMESTAMPTZ,
                        updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (cache_key, field_key)
                    )""",
                    f"""CREATE TABLE IF NOT EXISTS {self._SET} (
                        cache_key  TEXT NOT NULL,
                        member     TEXT NOT NULL,
                        expires_at TIMESTAMPTZ,
                        PRIMARY KEY (cache_key, member)
                    )""",
                    f"""CREATE TABLE IF NOT EXISTS {self._LIST} (
                        id          BIGSERIAL PRIMARY KEY,
                        cache_key   TEXT NOT NULL,
                        item_value  TEXT NOT NULL,
                        expires_at  TIMESTAMPTZ,
                        created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )""",
                    f"CREATE INDEX IF NOT EXISTS idx_{self._LIST}_key ON {self._LIST} (cache_key)",
                ]:
                    await cur.execute(sql)
                await conn.commit()
        PgStore._schema_ready = True

    async def _cleanup(self, cur, *keys):
        if keys:
            await cur.execute(
                f"DELETE FROM {self._KV} WHERE cache_key = ANY(%s) "
                "AND expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP",
                (list(keys),),
            )

    # -- String -----------------------------------------------------------

    async def get(self, key: str) -> str | None:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await self._cleanup(cur, key)
                await cur.execute(f"SELECT cache_value FROM {self._KV} WHERE cache_key=%s", (key,))
                row = await cur.fetchone()
                await conn.commit()
                return row[0] if row else None

    async def set(self, key: str, value: str, ex: int | None = None):
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO {self._KV} (cache_key, cache_value, expires_at, updated_at) "
                    f"VALUES (%s, %s, "
                    f"CASE WHEN %s IS NULL THEN NULL "
                    f"ELSE CURRENT_TIMESTAMP + (%s * INTERVAL '1 second') END, "
                    f"CURRENT_TIMESTAMP) "
                    f"ON CONFLICT (cache_key) DO UPDATE "
                    f"SET cache_value=EXCLUDED.cache_value, expires_at=EXCLUDED.expires_at, "
                    f"updated_at=CURRENT_TIMESTAMP",
                    (key, value, ex, ex),
                )
                await conn.commit()

    async def setnx(self, key: str, value: str) -> bool:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await self._cleanup(cur, key)
                await cur.execute(
                    f"INSERT INTO {self._KV} (cache_key, cache_value, updated_at) "
                    f"VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT (cache_key) DO NOTHING RETURNING 1",
                    (key, value),
                )
                ok = await cur.fetchone() is not None
                await conn.commit()
                return ok

    async def incr(self, key: str, by: int = 1) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await self._cleanup(cur, key)
                try:
                    await cur.execute(
                        f"INSERT INTO {self._KV} (cache_key, cache_value, updated_at) "
                        f"VALUES (%s, %s, CURRENT_TIMESTAMP) "
                        f"ON CONFLICT (cache_key) DO UPDATE "
                        f"SET cache_value=(CAST({self._KV}.cache_value AS BIGINT)+%s)::TEXT, "
                        f"updated_at=CURRENT_TIMESTAMP RETURNING cache_value",
                        (key, str(by), by),
                    )
                    row = await cur.fetchone()
                    await conn.commit()
                    return int(row[0])
                except Exception:
                    await conn.rollback()
                    raise ValueError("value is not an integer or out of range")

    async def append(self, key: str, value: str) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO {self._KV} (cache_key, cache_value, updated_at) "
                    f"VALUES (%s, %s, CURRENT_TIMESTAMP) "
                    f"ON CONFLICT (cache_key) DO UPDATE "
                    f"SET cache_value={self._KV}.cache_value || EXCLUDED.cache_value, "
                    f"updated_at=CURRENT_TIMESTAMP RETURNING LENGTH(cache_value)",
                    (key, value),
                )
                row = await cur.fetchone()
                await conn.commit()
                return row[0]

    # -- Generic ----------------------------------------------------------

    async def exists(self, *keys: str) -> int:
        if not keys:
            return 0
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await self._cleanup(cur, *keys)
                await cur.execute(
                    f"SELECT COUNT(DISTINCT k) FROM ("
                    f"  SELECT cache_key AS k FROM {self._KV} WHERE cache_key=ANY(%s)"
                    f"  UNION SELECT cache_key FROM {self._HASH} WHERE cache_key=ANY(%s)"
                    f"  UNION SELECT cache_key FROM {self._SET} WHERE cache_key=ANY(%s)"
                    f"  UNION SELECT cache_key FROM {self._LIST} WHERE cache_key=ANY(%s)"
                    f") t",
                    (list(keys), list(keys), list(keys), list(keys)),
                )
                row = await cur.fetchone()
                await conn.commit()
                return row[0] if row else 0

    async def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                deleted: set[str] = set()
                for table in (self._KV, self._HASH, self._SET, self._LIST):
                    await cur.execute(
                        f"DELETE FROM {table} WHERE cache_key=ANY(%s) RETURNING cache_key",
                        (list(keys),),
                    )
                    deleted.update(r[0] for r in await cur.fetchall())
                await conn.commit()
                return len(deleted)

    async def keys(self, pattern: str = "*") -> list[str]:
        like = pattern.replace("*", "%").replace("?", "_")
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {self._KV} WHERE expires_at IS NOT NULL AND expires_at<=CURRENT_TIMESTAMP"
                )
                await cur.execute(
                    f"SELECT DISTINCT k FROM ("
                    f"  SELECT cache_key AS k FROM {self._KV} WHERE cache_key LIKE %s"
                    f"  UNION SELECT cache_key FROM {self._HASH} WHERE cache_key LIKE %s"
                    f"  UNION SELECT cache_key FROM {self._SET} WHERE cache_key LIKE %s"
                    f"  UNION SELECT cache_key FROM {self._LIST} WHERE cache_key LIKE %s"
                    f") t",
                    (like, like, like, like),
                )
                rows = await cur.fetchall()
                await conn.commit()
                return [r[0] for r in rows]

    async def expire(self, key: str, seconds: int) -> bool:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                updated = 0
                for table in (self._KV, self._HASH, self._SET, self._LIST):
                    await cur.execute(
                        f"UPDATE {table} SET expires_at = CURRENT_TIMESTAMP + (%s * INTERVAL '1 second') "
                        f"WHERE cache_key = %s",
                        (seconds, key),
                    )
                    updated += cur.rowcount or 0
                await conn.commit()
                return updated > 0

    async def ttl(self, key: str) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                for table in (self._KV, self._HASH, self._SET, self._LIST):
                    await cur.execute(
                        f"SELECT expires_at, "
                        f"EXTRACT(EPOCH FROM (expires_at - CURRENT_TIMESTAMP))::BIGINT "
                        f"FROM {table} WHERE cache_key = %s LIMIT 1",
                        (key,),
                    )
                    row = await cur.fetchone()
                    if row:
                        if row[0] is None:
                            return -1
                        return max(int(row[1] or 0), 0)
                return -2

    # -- Hash -------------------------------------------------------------

    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        if not mapping:
            return 0
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                added = 0
                for field, value in mapping.items():
                    await cur.execute(
                        f"INSERT INTO {self._HASH} (cache_key, field_key, field_value, updated_at) "
                        f"VALUES (%s, %s, %s, CURRENT_TIMESTAMP) "
                        f"ON CONFLICT (cache_key, field_key) DO UPDATE "
                        f"SET field_value=EXCLUDED.field_value, updated_at=CURRENT_TIMESTAMP "
                        f"RETURNING (xmax = 0) AS inserted",
                        (key, field, value),
                    )
                    row = await cur.fetchone()
                    if row and row[0]:
                        added += 1
                await conn.commit()
                return added

    async def hget(self, key: str, field: str) -> str | None:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT field_value FROM {self._HASH} WHERE cache_key=%s AND field_key=%s",
                    (key, field),
                )
                row = await cur.fetchone()
                return row[0] if row else None

    async def hgetall(self, key: str) -> dict[str, str]:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT field_key, field_value FROM {self._HASH} WHERE cache_key=%s",
                    (key,),
                )
                return {r[0]: r[1] for r in await cur.fetchall()}

    async def hdel(self, key: str, *fields: str) -> int:
        if not fields:
            return 0
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {self._HASH} WHERE cache_key=%s AND field_key=ANY(%s)",
                    (key, list(fields)),
                )
                count = cur.rowcount or 0
                await conn.commit()
                return count

    async def hexists(self, key: str, field: str) -> bool:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT 1 FROM {self._HASH} WHERE cache_key=%s AND field_key=%s",
                    (key, field),
                )
                return await cur.fetchone() is not None

    async def hkeys(self, key: str) -> list[str]:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT field_key FROM {self._HASH} WHERE cache_key=%s", (key,))
                return [r[0] for r in await cur.fetchall()]

    async def hvals(self, key: str) -> list[str]:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT field_value FROM {self._HASH} WHERE cache_key=%s", (key,))
                return [r[0] for r in await cur.fetchall()]

    async def hlen(self, key: str) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT COUNT(*) FROM {self._HASH} WHERE cache_key=%s", (key,))
                return (await cur.fetchone())[0]

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO {self._HASH} (cache_key, field_key, field_value, updated_at) "
                    f"VALUES (%s, %s, %s, CURRENT_TIMESTAMP) "
                    f"ON CONFLICT (cache_key, field_key) DO UPDATE "
                    f"SET field_value=(CAST({self._HASH}.field_value AS BIGINT)+%s)::TEXT, "
                    f"updated_at=CURRENT_TIMESTAMP RETURNING field_value",
                    (key, field, str(increment), increment),
                )
                row = await cur.fetchone()
                await conn.commit()
                return int(row[0])

    # -- Set --------------------------------------------------------------

    async def sadd(self, key: str, *members: str) -> int:
        if not members:
            return 0
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                added = 0
                for m in members:
                    await cur.execute(
                        f"INSERT INTO {self._SET} (cache_key, member) VALUES (%s, %s) "
                        f"ON CONFLICT DO NOTHING",
                        (key, m),
                    )
                    added += cur.rowcount or 0
                await conn.commit()
                return added

    async def srem(self, key: str, *members: str) -> int:
        if not members:
            return 0
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {self._SET} WHERE cache_key=%s AND member=ANY(%s)",
                    (key, list(members)),
                )
                count = cur.rowcount or 0
                await conn.commit()
                return count

    async def smembers(self, key: str) -> set[str]:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT member FROM {self._SET} WHERE cache_key=%s", (key,))
                return {r[0] for r in await cur.fetchall()}

    async def sismember(self, key: str, member: str) -> bool:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT 1 FROM {self._SET} WHERE cache_key=%s AND member=%s",
                    (key, member),
                )
                return await cur.fetchone() is not None

    async def scard(self, key: str) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT COUNT(*) FROM {self._SET} WHERE cache_key=%s", (key,))
                return (await cur.fetchone())[0]

    async def sinter(self, *keys: str) -> set[str]:
        if not keys:
            return set()
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT member FROM {self._SET} WHERE cache_key=ANY(%s) "
                    f"GROUP BY member HAVING COUNT(DISTINCT cache_key)=%s",
                    (list(keys), len(keys)),
                )
                return {r[0] for r in await cur.fetchall()}

    async def sunion(self, *keys: str) -> set[str]:
        if not keys:
            return set()
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT DISTINCT member FROM {self._SET} WHERE cache_key=ANY(%s)",
                    (list(keys),),
                )
                return {r[0] for r in await cur.fetchall()}

    async def sdiff(self, *keys: str) -> set[str]:
        if not keys:
            return set()
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                if len(keys) == 1:
                    return await self.smembers(keys[0])
                await cur.execute(
                    f"SELECT member FROM {self._SET} WHERE cache_key=%s "
                    f"EXCEPT "
                    f"SELECT member FROM {self._SET} WHERE cache_key=ANY(%s)",
                    (keys[0], list(keys[1:])),
                )
                return {r[0] for r in await cur.fetchall()}

    # -- List -------------------------------------------------------------

    async def lpush(self, key: str, *values: str) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                for v in values:
                    await cur.execute(
                        f"INSERT INTO {self._LIST} (id, cache_key, item_value) VALUES ("
                        f"  COALESCE((SELECT MIN(id) FROM {self._LIST} WHERE cache_key=%s), 0) - 1,"
                        f"  %s, %s)",
                        (key, key, v),
                    )
                await cur.execute(f"SELECT COUNT(*) FROM {self._LIST} WHERE cache_key=%s", (key,))
                count = (await cur.fetchone())[0]
                await conn.commit()
                return count

    async def rpush(self, key: str, *values: str) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                for v in values:
                    await cur.execute(
                        f"INSERT INTO {self._LIST} (cache_key, item_value) VALUES (%s, %s)",
                        (key, v),
                    )
                await cur.execute(f"SELECT COUNT(*) FROM {self._LIST} WHERE cache_key=%s", (key,))
                count = (await cur.fetchone())[0]
                await conn.commit()
                return count

    async def lpop(self, key: str) -> str | None:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {self._LIST} WHERE id = ("
                    f"  SELECT id FROM {self._LIST} WHERE cache_key=%s ORDER BY id LIMIT 1"
                    f") RETURNING item_value",
                    (key,),
                )
                row = await cur.fetchone()
                await conn.commit()
                return row[0] if row else None

    async def rpop(self, key: str) -> str | None:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"DELETE FROM {self._LIST} WHERE id = ("
                    f"  SELECT id FROM {self._LIST} WHERE cache_key=%s ORDER BY id DESC LIMIT 1"
                    f") RETURNING item_value",
                    (key,),
                )
                row = await cur.fetchone()
                await conn.commit()
                return row[0] if row else None

    async def llen(self, key: str) -> int:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT COUNT(*) FROM {self._LIST} WHERE cache_key=%s", (key,))
                return (await cur.fetchone())[0]

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                # Resolve negative indices via COUNT
                if start < 0 or stop < 0:
                    await cur.execute(
                        f"SELECT COUNT(*) FROM {self._LIST} WHERE cache_key=%s", (key,),
                    )
                    length = (await cur.fetchone())[0]
                    if length == 0:
                        return []
                    if start < 0:
                        start = max(length + start, 0)
                    if stop < 0:
                        stop = length + stop
                if start > stop:
                    return []
                await cur.execute(
                    f"SELECT item_value FROM {self._LIST} WHERE cache_key=%s "
                    f"ORDER BY id OFFSET %s LIMIT %s",
                    (key, start, stop - start + 1),
                )
                return [r[0] for r in await cur.fetchall()]

    async def lindex(self, key: str, index: int) -> str | None:
        async with (await self._get_pool()).connection() as conn:
            async with conn.cursor() as cur:
                if index < 0:
                    await cur.execute(
                        f"SELECT COUNT(*) FROM {self._LIST} WHERE cache_key=%s", (key,),
                    )
                    length = (await cur.fetchone())[0]
                    index = length + index
                    if index < 0:
                        return None
                await cur.execute(
                    f"SELECT item_value FROM {self._LIST} WHERE cache_key=%s "
                    f"ORDER BY id OFFSET %s LIMIT 1",
                    (key, index),
                )
                row = await cur.fetchone()
                return row[0] if row else None
