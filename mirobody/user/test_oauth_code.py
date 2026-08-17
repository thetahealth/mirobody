"""The authorization code must be short-lived, single-use and client-bound.

All three checks existed in `token_handler` as a commented-out block, so the
`authorization_code` grant required only that the stored record carry a
`user_id`. Combined with a Redis branch that read the code and left it in place
behind a `# TODO: pass`, and a TTL taken from the *access token* lifetime
(30 days by default), a leaked authorization code was a month-long credential
that minted unlimited token pairs.

RFC 6749 §4.1.2 (short-lived, single-use) and §4.1.3 (issued to the client
presenting it) are what these tests pin.
"""

from __future__ import annotations

import json
import time

import pytest

from .oauth_service import _AUTH_CODE_TTL_SECONDS, OAuthService


class _FakeRedis:
    """Just enough Redis for the code path under test."""

    def __init__(self):
        self.h: dict[str, dict] = {}

    async def hgetall(self, key):
        return dict(self.h.get(key, {}))

    async def hset(self, key, mapping=None, **kw):
        self.h.setdefault(key, {}).update({k: str(v) for k, v in (mapping or {}).items()})

    async def expire(self, key, ttl):
        return True

    async def delete(self, key):
        return 1 if self.h.pop(key, None) is not None else 0

    async def get(self, key):
        return None

    async def set(self, *a, **kw):
        return True


class _FakeValidator:
    def get_expires_in(self):
        return 60 * 60 * 24 * 30          # the 30 days that used to be the code TTL

    async def generate_tokens(self, user_id, email, auth_method, client_id="", scope=""):
        return "access-token", "refresh-token", None


class _FakeRequest:
    method = "POST"

    def __init__(self, form):
        self._form = form
        self.headers = {}
        self.url = type("U", (), {"path": "/token", "hostname": "localhost"})()
        self.client = None
        self.state = type("S", (), {})()

    async def form(self):
        return self._form

    async def json(self):
        return self._form

    async def body(self):
        return b""


def _service(redis):
    svc = OAuthService.__new__(OAuthService)
    svc._redis = redis
    svc._auth_code_keyprefix = "code:"
    svc._client_keyprefix = "client:"
    svc._state_token_keyprefix = "state:"
    svc._token_validator = _FakeValidator()
    return svc


def _seed(redis, code="C1", client_id="app-1", expires_in=_AUTH_CODE_TTL_SECONDS):
    redis.h["code:" + code] = {
        "client_id": client_id,
        "user_id": "42",
        "scope": "mcp:read",
        "expires_at": str(int(time.time()) + expires_in),
    }


async def _redeem(svc, code="C1", client_id="app-1"):
    req = _FakeRequest({
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "client_secret": "s",
    })
    resp = await svc.token_handler(req)
    return resp.status_code, json.loads(bytes(resp.body).decode())


def test_the_code_ttl_is_ten_minutes_not_the_token_lifetime():
    """The constant is the fix; the token lifetime is what it must not be."""
    assert _AUTH_CODE_TTL_SECONDS == 600
    assert _AUTH_CODE_TTL_SECONDS < _FakeValidator().get_expires_in()


async def test_a_valid_code_still_works():
    redis = _FakeRedis()
    svc = _service(redis)
    _seed(redis)
    status, body = await _redeem(svc)
    assert status == 200, body
    assert body["access_token"] == "access-token"


async def test_a_code_cannot_be_redeemed_twice():
    """The replay this fixes: the Redis branch never deleted the code."""
    redis = _FakeRedis()
    svc = _service(redis)
    _seed(redis)

    first_status, _ = await _redeem(svc)
    assert first_status == 200

    second_status, body = await _redeem(svc)
    assert second_status == 400
    assert body["error"] == "invalid_grant"
    assert "code:C1" not in redis.h, "the code must be gone after one use"


async def test_an_expired_code_is_refused():
    redis = _FakeRedis()
    svc = _service(redis)
    _seed(redis, expires_in=-1)
    status, body = await _redeem(svc)
    assert status == 400
    assert body["error"] == "invalid_grant"


async def test_a_code_issued_to_another_client_is_refused():
    """RFC 6749 §4.1.3 — otherwise one client redeems another's code."""
    redis = _FakeRedis()
    svc = _service(redis)
    _seed(redis, client_id="app-1")
    status, body = await _redeem(svc, client_id="attacker-app")
    assert status == 400
    assert body["error"] == "invalid_grant"


async def test_an_unknown_code_is_refused():
    redis = _FakeRedis()
    svc = _service(redis)
    status, body = await _redeem(svc, code="never-issued")
    assert status == 400
    assert body["error"] == "invalid_grant"


async def test_the_in_memory_path_is_also_single_use_and_expiring():
    """The no-Redis branch deleted the code but never checked expiry."""
    svc = _service(None)
    svc._auth_codes = {
        "C1": {"client_id": "app-1", "user_id": "42", "scope": "mcp:read",
               "expires_at": int(time.time()) + 600},
        "OLD": {"client_id": "app-1", "user_id": "42", "scope": "mcp:read",
                "expires_at": int(time.time()) - 1},
    }
    assert (await _redeem(svc, code="C1"))[0] == 200
    assert (await _redeem(svc, code="C1"))[0] == 400      # single use
    assert (await _redeem(svc, code="OLD"))[0] == 400     # expiry enforced
