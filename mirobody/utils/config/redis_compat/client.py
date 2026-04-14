"""RedisCompat -- redis.asyncio.Redis-compatible in-process client."""

from __future__ import annotations

import asyncio

from .store_memory import MemoryStore
from .store_pg import PgStore
from .pubsub import PubSub, CompatPubSub


class RedisCompat:
    """Redis-like in-process store. No network, no serialization overhead.

    Usage:
        r = RedisCompat()                    # in-memory
        r = RedisCompat(pg_config=pg_cfg)    # PostgreSQL-backed
        await r.set("key", "value")
        await r.get("key")  # "value"
    """

    def __init__(self, pg_config=None):
        self._store = PgStore(pg_config) if pg_config is not None else MemoryStore()
        self._pubsub = PubSub()

    # -- Connection (no-op, for redis-py compatibility) -------------------
    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        pass

    async def aclose(self) -> None:
        pass

    # -- String -----------------------------------------------------------
    async def get(self, key: str) -> str | None:
        return await self._store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        if nx:
            return await self._store.setnx(key, str(value))
        await self._store.set(key, str(value), ex=ex)
        return True

    async def setex(self, key: str, seconds: int, value: str):
        await self._store.set(key, str(value), ex=seconds)

    async def setnx(self, key: str, value: str) -> bool:
        return await self._store.setnx(key, str(value))

    async def incr(self, key: str) -> int:
        return await self._store.incr(key)

    async def decr(self, key: str) -> int:
        return await self._store.incr(key, by=-1)

    async def incrby(self, key: str, amount: int) -> int:
        return await self._store.incr(key, by=amount)

    async def decrby(self, key: str, amount: int) -> int:
        return await self._store.incr(key, by=-amount)

    async def append(self, key: str, value: str) -> int:
        return await self._store.append(key, value)

    # -- Generic ----------------------------------------------------------
    async def exists(self, *keys: str) -> int:
        return await self._store.exists(*keys)

    async def delete(self, *keys: str) -> int:
        return await self._store.delete(*keys)

    async def keys(self, pattern: str = "*") -> list[str]:
        return await self._store.keys(pattern)

    async def expire(self, key: str, seconds: int) -> bool:
        return await self._store.expire(key, seconds)

    async def ttl(self, key: str) -> int:
        return await self._store.ttl(key)

    # -- Hash -------------------------------------------------------------
    async def hset(self, key: str, field: str | None = None, value: str | None = None, mapping: dict[str, str] | None = None) -> int:
        m = {}
        if field is not None and value is not None:
            m[field] = str(value)
        if mapping:
            m.update({k: str(v) for k, v in mapping.items()})
        return await self._store.hset(key, m)

    async def hget(self, key: str, field: str) -> str | None:
        return await self._store.hget(key, field)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(await self._store.hgetall(key))

    async def hdel(self, key: str, *fields: str) -> int:
        return await self._store.hdel(key, *fields)

    async def hexists(self, key: str, field: str) -> bool:
        return await self._store.hexists(key, field)

    async def hkeys(self, key: str) -> list[str]:
        return await self._store.hkeys(key)

    async def hvals(self, key: str) -> list[str]:
        return await self._store.hvals(key)

    async def hlen(self, key: str) -> int:
        return await self._store.hlen(key)

    async def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        return await self._store.hincrby(key, field, amount)

    async def hmset(self, key: str, mapping: dict[str, str]) -> bool:
        await self._store.hset(key, {k: str(v) for k, v in mapping.items()})
        return True

    async def hmget(self, key: str, keys: list[str], *args: str) -> list[str | None]:
        fields = list(keys) + list(args) if args else list(keys)
        return [await self._store.hget(key, f) for f in fields]

    # -- Set --------------------------------------------------------------
    async def sadd(self, key: str, *members: str) -> int:
        return await self._store.sadd(key, *members)

    async def srem(self, key: str, *members: str) -> int:
        return await self._store.srem(key, *members)

    async def smembers(self, key: str) -> set[str]:
        return await self._store.smembers(key)

    async def sismember(self, key: str, member: str) -> bool:
        return await self._store.sismember(key, member)

    async def scard(self, key: str) -> int:
        return await self._store.scard(key)

    async def sinter(self, *keys: str) -> set[str]:
        return await self._store.sinter(*keys)

    async def sunion(self, *keys: str) -> set[str]:
        return await self._store.sunion(*keys)

    async def sdiff(self, *keys: str) -> set[str]:
        return await self._store.sdiff(*keys)

    # -- List -------------------------------------------------------------
    async def lpush(self, key: str, *values: str) -> int:
        return await self._store.lpush(key, *values)

    async def rpush(self, key: str, *values: str) -> int:
        return await self._store.rpush(key, *values)

    async def lpop(self, key: str) -> str | None:
        return await self._store.lpop(key)

    async def rpop(self, key: str) -> str | None:
        return await self._store.rpop(key)

    async def llen(self, key: str) -> int:
        return await self._store.llen(key)

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        return await self._store.lrange(key, start, stop)

    async def lindex(self, key: str, index: int) -> str | None:
        return await self._store.lindex(key, index)

    async def blpop(self, *args, timeout: float = 0) -> tuple[str, str] | None:
        keys = list(args)
        for key in keys:
            val = await self._store.lpop(key)
            if val is not None:
                return (key, val)
        if not isinstance(self._store, MemoryStore):
            return None
        fut = self._store.add_list_waiter(keys, "left")
        try:
            return await asyncio.wait_for(fut, timeout=timeout or None)
        except asyncio.TimeoutError:
            return None

    async def brpop(self, *args, timeout: float = 0) -> tuple[str, str] | None:
        keys = list(args)
        for key in keys:
            val = await self._store.rpop(key)
            if val is not None:
                return (key, val)
        if not isinstance(self._store, MemoryStore):
            return None
        fut = self._store.add_list_waiter(keys, "right")
        try:
            return await asyncio.wait_for(fut, timeout=timeout or None)
        except asyncio.TimeoutError:
            return None

    # -- Pub/Sub ----------------------------------------------------------
    async def publish(self, channel: str, message: str) -> int:
        return self._pubsub.publish(channel, message)

    def pubsub(self) -> "CompatPubSub":
        return CompatPubSub(self._pubsub)

    # -- Scripting --------------------------------------------------------
    async def eval(self, script: str, numkeys: int, *args) -> int:
        """Minimal EVAL: only supports compare-and-delete (distributed lock release pattern).

        Usage: eval('...redis.call("get",...)...redis.call("del",...)...', 1, key, expected)
        """
        if numkeys != 1 or len(args) < 2:
            raise NotImplementedError("Only single-key compare-and-delete eval is supported.")
        if 'redis.call("get"' not in script or 'redis.call("del"' not in script:
            raise NotImplementedError("Unsupported eval script.")
        key, expected = str(args[0]), str(args[1])
        current = await self.get(key)
        if current == expected:
            return await self.delete(key)
        return 0
