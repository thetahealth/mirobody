"""In-memory store backend."""

from __future__ import annotations

import asyncio
import collections
import fnmatch
import time

from dataclasses import dataclass


@dataclass
class Entry:
    value: str
    expires_at: float | None = None

    @property
    def expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() > self.expires_at


class MemoryStore:
    def __init__(self):
        self._data: dict[str, Entry] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._sets: dict[str, set[str]] = {}
        self._lists: dict[str, collections.deque[str]] = {}
        # BLPOP/BRPOP waiters: key -> list of (side, future)
        self._list_waiters: dict[str, list[tuple[str, asyncio.Future]]] = {}
        # Guard compound read-modify-write sequences against future
        # concurrency (e.g. TaskGroup); currently all methods are
        # synchronous between awaits so the lock is defensive.
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> str | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if entry.expired:
            del self._data[key]
            return None
        return entry.value

    async def set(self, key: str, value: str, ex: int | None = None):
        expires_at = time.monotonic() + ex if ex is not None else None
        self._data[key] = Entry(value=value, expires_at=expires_at)

    async def setnx(self, key: str, value: str) -> bool:
        async with self._lock:
            entry = self._data.get(key)
            if entry is not None and not entry.expired:
                return False
            self._data[key] = Entry(value=value)
            return True

    async def incr(self, key: str, by: int = 1) -> int:
        async with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.expired:
                val = 0
            else:
                try:
                    val = int(entry.value)
                except ValueError:
                    raise ValueError("value is not an integer or out of range")
            val += by
            self._data[key] = Entry(value=str(val))
            return val

    async def append(self, key: str, value: str) -> int:
        async with self._lock:
            entry = self._data.get(key)
            if entry is None or entry.expired:
                new_val = value
            else:
                new_val = entry.value + value
            self._data[key] = Entry(value=new_val)
            return len(new_val)

    async def exists(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._hashes or key in self._sets or key in self._lists:
                count += 1
            else:
                entry = self._data.get(key)
                if entry and not entry.expired:
                    count += 1
        return count

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            deleted = False
            if key in self._data:
                del self._data[key]
                deleted = True
            if key in self._hashes:
                del self._hashes[key]
                deleted = True
            if key in self._sets:
                del self._sets[key]
                deleted = True
            if key in self._lists:
                del self._lists[key]
                deleted = True
            if deleted:
                count += 1
        return count

    async def keys(self, pattern: str = "*") -> list[str]:
        now = time.monotonic()
        all_keys = set(self._hashes.keys()) | set(self._sets.keys()) | set(self._lists.keys())
        all_keys.update(
            k for k, v in self._data.items()
            if not (v.expires_at and now > v.expires_at)
        )
        return [k for k in all_keys if fnmatch.fnmatch(k, pattern)]

    async def expire(self, key: str, seconds: int) -> bool:
        entry = self._data.get(key)
        if entry is not None and not entry.expired:
            entry.expires_at = time.monotonic() + seconds
            return True
        return False

    async def ttl(self, key: str) -> int:
        entry = self._data.get(key)
        if entry is None or entry.expired:
            return -2  # key does not exist
        if entry.expires_at is None:
            return -1  # no expiry
        remaining = entry.expires_at - time.monotonic()
        return max(int(remaining), 0)

    # -- Hash operations --------------------------------------------------

    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        h = self._hashes.setdefault(key, {})
        added = sum(1 for f in mapping if f not in h)
        h.update(mapping)
        return added

    async def hget(self, key: str, field: str) -> str | None:
        h = self._hashes.get(key)
        if h is None:
            return None
        return h.get(field)

    async def hgetall(self, key: str) -> dict[str, str]:
        return self._hashes.get(key, {})

    async def hdel(self, key: str, *fields: str) -> int:
        h = self._hashes.get(key)
        if h is None:
            return 0
        count = 0
        for f in fields:
            if f in h:
                del h[f]
                count += 1
        if not h:
            del self._hashes[key]
        return count

    async def hexists(self, key: str, field: str) -> bool:
        h = self._hashes.get(key)
        return h is not None and field in h

    async def hkeys(self, key: str) -> list[str]:
        return list(self._hashes.get(key, {}).keys())

    async def hvals(self, key: str) -> list[str]:
        return list(self._hashes.get(key, {}).values())

    async def hlen(self, key: str) -> int:
        return len(self._hashes.get(key, {}))

    async def hincrby(self, key: str, field: str, increment: int) -> int:
        h = self._hashes.setdefault(key, {})
        current = int(h.get(field, "0"))
        new_val = current + increment
        h[field] = str(new_val)
        return new_val

    # -- Set operations ---------------------------------------------------

    async def sadd(self, key: str, *members: str) -> int:
        s = self._sets.setdefault(key, set())
        before = len(s)
        s.update(members)
        return len(s) - before

    async def srem(self, key: str, *members: str) -> int:
        s = self._sets.get(key)
        if s is None:
            return 0
        count = 0
        for m in members:
            if m in s:
                s.discard(m)
                count += 1
        if not s:
            del self._sets[key]
        return count

    async def smembers(self, key: str) -> set[str]:
        return self._sets.get(key, set())

    async def sismember(self, key: str, member: str) -> bool:
        s = self._sets.get(key)
        return s is not None and member in s

    async def scard(self, key: str) -> int:
        return len(self._sets.get(key, set()))

    async def sinter(self, *keys: str) -> set[str]:
        sets = [self._sets.get(k, set()) for k in keys]
        if not sets:
            return set()
        return sets[0].intersection(*sets[1:])

    async def sunion(self, *keys: str) -> set[str]:
        result: set[str] = set()
        for k in keys:
            result |= self._sets.get(k, set())
        return result

    async def sdiff(self, *keys: str) -> set[str]:
        sets = [self._sets.get(k, set()) for k in keys]
        if not sets:
            return set()
        return sets[0].difference(*sets[1:])

    # -- List operations --------------------------------------------------

    async def lpush(self, key: str, *values: str) -> int:
        lst = self._lists.setdefault(key, collections.deque())
        for v in values:
            lst.appendleft(v)
        self._wake_waiters(key)
        return len(lst)

    async def rpush(self, key: str, *values: str) -> int:
        lst = self._lists.setdefault(key, collections.deque())
        lst.extend(values)
        self._wake_waiters(key)
        return len(lst)

    async def lpop(self, key: str) -> str | None:
        lst = self._lists.get(key)
        if not lst:
            return None
        val = lst.popleft()
        if not lst:
            del self._lists[key]
        return val

    async def rpop(self, key: str) -> str | None:
        lst = self._lists.get(key)
        if not lst:
            return None
        val = lst.pop()
        if not lst:
            del self._lists[key]
        return val

    async def llen(self, key: str) -> int:
        return len(self._lists.get(key, ()))

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        lst = self._lists.get(key)
        if not lst:
            return []
        length = len(lst)
        if start < 0:
            start = max(length + start, 0)
        if stop < 0:
            stop = length + stop
        return list(lst)[start:stop + 1]

    async def lindex(self, key: str, index: int) -> str | None:
        lst = self._lists.get(key)
        if not lst:
            return None
        if index < 0:
            index = len(lst) + index
        if 0 <= index < len(lst):
            return lst[index]
        return None

    def _wake_waiters(self, key: str):
        waiters = self._list_waiters.get(key)
        if not waiters:
            return
        lst = self._lists.get(key)
        while waiters and lst:
            side, fut = waiters.pop(0)
            if fut.done():
                continue
            val = lst.popleft() if side == "left" else lst.pop()
            fut.set_result((key, val))
            if not lst:
                del self._lists[key]
            break
        if not waiters:
            self._list_waiters.pop(key, None)

    def add_list_waiter(self, keys: list[str], side: str) -> asyncio.Future:
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        for key in keys:
            self._list_waiters.setdefault(key, []).append((side, fut))
        return fut
