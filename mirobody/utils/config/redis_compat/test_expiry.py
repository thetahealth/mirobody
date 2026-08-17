"""EXPIRE has to work on every key type, not just strings.

`MemoryStore` kept expiry on the `Entry` wrapper used for string keys. Hashes,
sets and lists live in separate dicts with nowhere to record one, so `expire()`
against them returned False and set nothing — and False is indistinguishable
from "no such key", so a caller could not tell the difference between "expiry
set" and "this store cannot do that".

That is not theoretical: `user/oauth_service.py` stores authorization codes as
hashes and calls `expire()` on them to bound their lifetime. Against this
backend the call did nothing.
"""

from __future__ import annotations

import time

from .store_memory import MemoryStore


async def test_expire_applies_to_a_hash_key():
    """The oauth authorization-code shape: hset + expire."""
    s = MemoryStore()
    await s.hset("code:abc", {"user_id": "42"})

    assert await s.expire("code:abc", 60) is True
    assert await s.ttl("code:abc") > 0


async def test_an_expired_hash_disappears_from_every_reader():
    """Setting an expiry nothing honours would be worse than not setting one."""
    s = MemoryStore()
    await s.hset("code:abc", {"user_id": "42"})
    await s.expire("code:abc", 60)

    s._other_expires["code:abc"] = time.monotonic() - 1   # force the deadline past

    assert await s.hgetall("code:abc") == {}
    assert await s.exists("code:abc") == 0
    assert await s.ttl("code:abc") == -2
    assert "code:abc" not in await s.keys("*")


async def test_expire_applies_to_sets_and_lists():
    s = MemoryStore()
    await s.sadd("s1", "a")
    await s.rpush("l1", "x")

    assert await s.expire("s1", 60) is True
    assert await s.expire("l1", 60) is True
    assert await s.ttl("s1") > 0
    assert await s.ttl("l1") > 0


async def test_expire_on_a_missing_key_is_still_false():
    s = MemoryStore()
    assert await s.expire("nothing-here", 60) is False
    assert await s.ttl("nothing-here") == -2


async def test_string_keys_are_unchanged():
    """The half that already worked must keep working."""
    s = MemoryStore()
    await s.set("k", "v", ex=60)
    assert await s.get("k") == "v"
    assert await s.ttl("k") > 0

    await s.set("plain", "v")
    assert await s.ttl("plain") == -1        # exists, no expiry
    assert await s.expire("plain", 60) is True
    assert await s.ttl("plain") > 0
