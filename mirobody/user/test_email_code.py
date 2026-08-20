"""A login code is single-use and guessable only a few times.

Two holes, both in code that looked finished:

* The **Redis** branch never deleted the code after a successful verify, while
  the in-memory branch did. So a code was single-use in development and
  replayable for its full 600-second TTL in production — the deployment shape
  where it matters.
* **Neither branch counted failures.** The request rate limiter cannot help:
  its guard is `request.state.user_id > 0`, and `/email/verify` is by
  definition anonymous, so it never fires. 10**6 combinations, 600 seconds,
  unlimited attempts.

Both validators carried byte-identical verify bodies, so both are exercised
here through the same mixin.
"""

from __future__ import annotations

import time

import pytest

from mirobody.user.email import (
    _MAX_VERIFY_ATTEMPTS,
    MandrillEmailValidator,
    SMTPEmailValidator,
)


class FakeRedis:
    """Just enough Redis: get/incr/expire/delete over a dict."""

    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, k):
        return self.store.get(k)

    async def incr(self, k):
        self.store[k] = str(int(self.store.get(k, "0")) + 1)
        return int(self.store[k])

    async def expire(self, k, ttl):
        return True

    async def delete(self, k):
        self.store.pop(k, None)
        return 1


def _validator(cls, redis=None):
    v = cls.__new__(cls)
    v._redis = redis
    v._codes = {}
    v._code_keyprefix = "mirobody:email:code:"
    v._attempt_keyprefix = "mirobody:email:attempt:"
    v._expires_in = 600
    v._predefined_codes = None
    return v


BOTH = pytest.mark.parametrize("cls", [MandrillEmailValidator, SMTPEmailValidator])


@BOTH
@pytest.mark.asyncio
async def test_a_correct_code_verifies_once_and_only_once(cls):
    """The Redis-branch finding: this used to pass twice."""
    r = FakeRedis()
    v = _validator(cls, r)
    r.store["mirobody:email:code:a@b.com"] = "123456"

    assert await v.verify("a@b.com", "123456") is None
    assert await v.verify("a@b.com", "123456") is not None


@BOTH
@pytest.mark.asyncio
async def test_guessing_is_capped_and_burns_the_code(cls):
    r = FakeRedis()
    v = _validator(cls, r)
    r.store["mirobody:email:code:a@b.com"] = "123456"

    for _ in range(_MAX_VERIFY_ATTEMPTS):
        assert await v.verify("a@b.com", "000000") == "Invalid code."

    assert await v.verify("a@b.com", "000000") == "Too many attempts."
    # And the real code no longer works — otherwise an attacker just waits for
    # the counter's TTL and resumes where they left off.
    assert await v.verify("a@b.com", "123456") is not None


@BOTH
@pytest.mark.asyncio
async def test_the_in_memory_branch_has_the_same_guarantees(cls):
    """Dev and prod must not disagree about whether a code is single-use."""
    v = _validator(cls, None)
    v._codes["a@b.com"] = {"value": "123456", "expires_at": time.time() + 600}

    assert await v.verify("a@b.com", "123456") is None
    assert await v.verify("a@b.com", "123456") is not None


@BOTH
@pytest.mark.asyncio
async def test_in_memory_guessing_is_capped_too(cls):
    v = _validator(cls, None)
    v._codes["a@b.com"] = {"value": "123456", "expires_at": time.time() + 600}

    for _ in range(_MAX_VERIFY_ATTEMPTS):
        assert await v.verify("a@b.com", "000000") == "Invalid code."
    assert await v.verify("a@b.com", "000000") == "Too many attempts."


@BOTH
@pytest.mark.asyncio
async def test_an_expired_code_is_refused(cls):
    v = _validator(cls, None)
    v._codes["a@b.com"] = {"value": "123456", "expires_at": time.time() - 1}
    assert await v.verify("a@b.com", "123456") == "Code expired."


@BOTH
@pytest.mark.asyncio
async def test_codes_are_scoped_per_service(cls):
    """One service's code must not authorize another's."""
    r = FakeRedis()
    v = _validator(cls, r)
    r.store["mirobody:email:code:a@b.com:login"] = "123456"
    assert await v.verify("a@b.com", "123456", service="login") is None

    r.store["mirobody:email:code:a@b.com:login"] = "123456"
    assert await v.verify("a@b.com", "123456", service="sharing") is not None


@BOTH
@pytest.mark.asyncio
async def test_a_nonexistent_code_does_not_verify(cls):
    v = _validator(cls, FakeRedis())
    assert await v.verify("nobody@b.com", "123456") is not None
