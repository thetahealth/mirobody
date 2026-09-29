"""Encrypted, expiring process-shared state in Postgres."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import secrets
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

from cryptography.fernet import Fernet

#: Connections this store may hold at once. Every rate-limited request makes
#: one or two calls here, most of them before sign-in; with a connection opened
#: per call, 150 concurrent anonymous logins exhausted Postgres's 100 and
#: answered 500. The burst now queues for these instead, and each call holds
#: its connection for about a millisecond.
POOL_MAX = 8
#: How long a call waits for one of them before failing (`TimeoutError`).
ACQUIRE_TIMEOUT = 10.0


class _Connections:
    """At most `size` connections open, idle ones reused, per event loop.

    Deliberately not `psycopg_pool`: its background tasks outlive an
    `asyncio.run` that never closed the pool, and the loop's shutdown then
    waited on them forever, which is every CLI command and every server exit
    that does not remember to close this store. Here nothing runs between
    calls. A reused connection is pinged first, so one killed by a Postgres
    restart is replaced instead of failing the request that drew it."""

    def __init__(self, connect, size: int) -> None:
        self._connect = connect
        self._idle: list[Any] = []
        self._slots = asyncio.Semaphore(size)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        async with asyncio.timeout(ACQUIRE_TIMEOUT):
            await self._slots.acquire()
        conn = None
        try:
            conn = await self._checkout()
            try:
                yield conn
            except BaseException:
                await conn.rollback()
                raise
            await conn.commit()
        except BaseException:
            if conn is not None and not conn.closed:
                await conn.close()
            conn = None
            raise
        finally:
            if conn is not None and not conn.closed:
                self._idle.append(conn)
            self._slots.release()

    def abandon(self) -> None:
        """Close the idle connections of a loop that has ended. Its loop cannot
        run `await conn.close()` any more, so libpq's own close does it. Left
        to the garbage collector they were closed too, each with a
        ResourceWarning, and only because nothing else held them."""
        while self._idle:
            conn = self._idle.pop()
            try:
                conn.pgconn.finish()
            except Exception:
                pass

    async def _checkout(self) -> Any:
        while self._idle:
            conn = self._idle.pop()
            try:
                # Opens the transaction the call then runs in: one round trip.
                await conn.execute("SELECT 1")
                return conn
            except Exception:
                await conn.close()
        return await self._connect()


class EphemeralStore:
    """Small state shared by server and worker without another service."""

    def __init__(self, pg_config: Any, encryption_key: str):
        self._pg_config = pg_config
        self._cipher = Fernet(encryption_key)
        self._pool: _Connections | None = None
        self._pool_loop: asyncio.AbstractEventLoop | None = None

    def _connection(self):
        """One transaction: committed when the block ends, rolled back if it
        raises. A later event loop (the CLI runs several `asyncio.run` in one
        process) gets connections of its own: one opened in a loop that has
        closed cannot be used in the next."""
        loop = asyncio.get_running_loop()
        if self._pool is None or self._pool_loop is not loop:
            if self._pool is not None:
                self._pool.abandon()
            self._pool = _Connections(lambda: self._pg_config.get_async_client(cursor_factory=None), POOL_MAX)
            self._pool_loop = loop
        return self._pool.connection()

    @staticmethod
    def _hash(key: str) -> bytes:
        return hashlib.sha256(key.encode("utf-8")).digest()

    def _seal(self, value: str) -> str:
        return self._cipher.encrypt(value.encode("utf-8")).decode("ascii")

    def _open(self, value: str) -> str:
        return self._cipher.decrypt(value.encode("ascii")).decode("utf-8")

    async def get(self, key: str) -> str | None:
        async with self._connection() as conn:
            row = await (await conn.execute(
                "SELECT value_ciphertext, counter FROM th_ephemeral "
                "WHERE key_hash = %s AND (expires_at IS NULL OR expires_at > now())",
                (self._hash(key),),
            )).fetchone()
        if row is None:
            return None
        return str(row[1]) if row[1] is not None else self._open(row[0])

    async def take(self, key: str) -> str | None:
        """Consume one-time state in one statement, so concurrent readers cannot replay it."""
        async with self._connection() as conn:
            row = await (await conn.execute(
                "DELETE FROM th_ephemeral WHERE key_hash = %s "
                "AND (expires_at IS NULL OR expires_at > now()) "
                "RETURNING value_ciphertext, counter",
                (self._hash(key),),
            )).fetchone()
        if row is None:
            return None
        return str(row[1]) if row[1] is not None else self._open(row[0])

    async def set(self, key: str, value: str | int, *, ex: int | None = None,
                  nx: bool = False) -> bool:
        sealed = self._seal(str(value))
        async with self._connection() as conn:
            cursor = await conn.execute(
                "INSERT INTO th_ephemeral (key_hash, value_ciphertext, expires_at) "
                "VALUES (%s, %s, CASE WHEN %s::integer IS NULL THEN NULL "
                "ELSE now() + %s * interval '1 second' END) "
                "ON CONFLICT (key_hash) DO UPDATE SET "
                "value_ciphertext = EXCLUDED.value_ciphertext, counter = NULL, "
                "expires_at = EXCLUDED.expires_at "
                + ("WHERE th_ephemeral.expires_at <= now() " if nx else "")
                + "RETURNING key_hash",
                (self._hash(key), sealed, ex, ex),
            )
            return await cursor.fetchone() is not None

    async def setex(self, key: str, ttl: int, value: str | int) -> bool:
        return await self.set(key, value, ex=ttl)

    async def delete(self, key: str) -> int:
        async with self._connection() as conn:
            cursor = await conn.execute(
                "DELETE FROM th_ephemeral WHERE key_hash = %s", (self._hash(key),),
            )
            return cursor.rowcount

    async def delete_if_value(self, key: str, expected: str) -> bool:
        """Release a lock only while its owner still matches."""
        async with self._connection() as conn:
            row = await (await conn.execute(
                "SELECT value_ciphertext FROM th_ephemeral WHERE key_hash = %s "
                "AND (expires_at IS NULL OR expires_at > now()) FOR UPDATE",
                (self._hash(key),),
            )).fetchone()
            if row is None:
                return True
            if self._open(row[0]) != expected:
                return False
            await conn.execute("DELETE FROM th_ephemeral WHERE key_hash = %s", (self._hash(key),))
            return True

    async def take_if_value(self, key: str, expected: str) -> bool:
        """Consume a one-time code only if it matches, with the row locked."""
        async with self._connection() as conn:
            row = await (await conn.execute(
                "SELECT value_ciphertext FROM th_ephemeral WHERE key_hash = %s "
                "AND (expires_at IS NULL OR expires_at > now()) FOR UPDATE",
                (self._hash(key),),
            )).fetchone()
            if row is None or not secrets.compare_digest(self._open(row[0]), expected):
                return False
            await conn.execute("DELETE FROM th_ephemeral WHERE key_hash = %s", (self._hash(key),))
            return True

    async def exists(self, key: str) -> bool:
        async with self._connection() as conn:
            row = await (await conn.execute(
                "SELECT 1 FROM th_ephemeral WHERE key_hash = %s "
                "AND (expires_at IS NULL OR expires_at > now())",
                (self._hash(key),),
            )).fetchone()
        return row is not None

    async def expire(self, key: str, seconds: int) -> bool:
        async with self._connection() as conn:
            cursor = await conn.execute(
                "UPDATE th_ephemeral SET expires_at = now() + %s * interval '1 second' "
                "WHERE key_hash = %s AND (expires_at IS NULL OR expires_at > now())",
                (seconds, self._hash(key)),
            )
            return cursor.rowcount > 0

    async def ttl(self, key: str) -> int:
        async with self._connection() as conn:
            row = await (await conn.execute(
                "SELECT EXTRACT(EPOCH FROM expires_at - now()) FROM th_ephemeral "
                "WHERE key_hash = %s AND (expires_at IS NULL OR expires_at > now())",
                (self._hash(key),),
            )).fetchone()
        if row is None:
            return -2
        return math.ceil(row[0]) if row[0] is not None else -1

    async def incr(self, key: str, *, ttl: int | None = None) -> int:
        """Increment and install the initial window in one atomic statement."""
        async with self._connection() as conn:
            row = await (await conn.execute(
                "INSERT INTO th_ephemeral (key_hash, counter, expires_at) "
                "VALUES (%s, 1, CASE WHEN %s::integer IS NULL THEN NULL "
                "ELSE now() + %s * interval '1 second' END) "
                "ON CONFLICT (key_hash) DO UPDATE SET "
                "counter = CASE WHEN th_ephemeral.expires_at <= now() THEN 1 "
                "ELSE coalesce(th_ephemeral.counter, 0) + 1 END, "
                "expires_at = CASE WHEN th_ephemeral.expires_at <= now() "
                "THEN EXCLUDED.expires_at ELSE th_ephemeral.expires_at END "
                "RETURNING counter",
                (self._hash(key), ttl, ttl),
            )).fetchone()
        return row[0]

    async def hset(self, key: str, *, mapping: Mapping[str, Any]) -> int:
        """Merge a small hash under a row lock, preserving its existing TTL."""
        key_hash = self._hash(key)
        async with self._connection() as conn:
            await conn.execute(
                "INSERT INTO th_ephemeral (key_hash, value_ciphertext) VALUES (%s, %s) "
                "ON CONFLICT (key_hash) DO NOTHING",
                (key_hash, self._seal("{}")),
            )
            row = await (await conn.execute(
                "SELECT value_ciphertext, expires_at <= now() FROM th_ephemeral "
                "WHERE key_hash = %s FOR UPDATE", (key_hash,),
            )).fetchone()
            # Expired, or a counter's row (no value to open): start empty,
            # where Redis would have answered WRONGTYPE.
            data = {} if row[1] or row[0] is None else json.loads(self._open(row[0]))
            data.update({str(k): str(v) for k, v in mapping.items()})
            await conn.execute(
                "UPDATE th_ephemeral SET value_ciphertext = %s, counter = NULL, "
                "expires_at = CASE WHEN expires_at <= now() THEN NULL ELSE expires_at END "
                "WHERE key_hash = %s",
                (self._seal(json.dumps(data)), key_hash),
            )
        return len(mapping)

    async def hgetall(self, key: str) -> dict[str, str]:
        raw = await self.get(key)
        return json.loads(raw) if raw else {}

    async def hget(self, key: str, field: str) -> str | None:
        return (await self.hgetall(key)).get(field)

    async def take_hash(self, key: str) -> dict[str, str]:
        raw = await self.take(key)
        return json.loads(raw) if raw else {}

    async def cleanup(self) -> int:
        async with self._connection() as conn:
            cursor = await conn.execute(
                "DELETE FROM th_ephemeral WHERE expires_at <= now()"
            )
            return cursor.rowcount
