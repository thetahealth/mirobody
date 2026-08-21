"""An authorization code may only be sent where the client said to send it.

`redirect_uris` was accepted at registration, echoed back in the response, and
never stored — so the authorize handler had nothing to compare against and used
whatever `redirect_uri` the request carried. Send a logged-in victim to
/authorize with `redirect_uri=https://evil.example/cb` and the code lands on
the attacker's server, exchangeable for that victim's tokens (RFC 6749 §10.6).
"""

from __future__ import annotations

import json

import pytest

from mirobody.user.oauth_service import OAuthService


@pytest.fixture
def svc():
    s = OAuthService.__new__(OAuthService)
    s._redis = None
    s._clients = {
        "mcp_client_good": {
            "secret": "x",
            "redirect_uris": json.dumps([
                "https://app.example/callback",
                "http://127.0.0.1:8765/cb",
            ]),
        },
        "mcp_client_legacy": {"secret": "x"},          # registered without any
    }
    s._client_keyprefix = "oauth:client:"
    return s


@pytest.mark.asyncio
async def test_a_registered_uri_is_accepted(svc):
    assert await svc._is_registered_redirect_uri(
        "mcp_client_good", "https://app.example/callback")


@pytest.mark.asyncio
async def test_an_attacker_controlled_uri_is_rejected(svc):
    """The finding, in one line."""
    assert not await svc._is_registered_redirect_uri(
        "mcp_client_good", "https://evil.example/cb")


@pytest.mark.asyncio
@pytest.mark.parametrize("uri", [
    "https://app.example.evil.com/callback",     # suffix trick on a prefix match
    "https://app.example/callback/../../x",      # path traversal on a prefix match
    "https://app.example/callback?next=evil",    # extra query
    "https://app.example/Callback",              # case
    "https://app.example/callback/",             # trailing slash
])
async def test_near_misses_are_rejected(svc, uri):
    """Exact match, per RFC 6749 §3.1.2.3. Every entry here defeats one of the
    matching schemes people reach for instead."""
    assert not await svc._is_registered_redirect_uri("mcp_client_good", uri)


@pytest.mark.asyncio
async def test_out_of_band_needs_no_registration(svc):
    """Not a URL, nothing is redirected — the code is displayed to be pasted."""
    assert await svc._is_registered_redirect_uri(
        "mcp_client_good", "urn:ietf:wg:oauth:2.0:oob")


@pytest.mark.asyncio
async def test_a_client_that_registered_none_cannot_redirect_at_all(svc):
    """"None registered" must not mean "anything allowed" — that would leave
    the hole open for every client that omits the field."""
    assert not await svc._is_registered_redirect_uri(
        "mcp_client_legacy", "https://anywhere.example/cb")


@pytest.mark.asyncio
async def test_an_unknown_client_is_rejected(svc):
    assert not await svc._is_registered_redirect_uri(
        "mcp_client_nope", "https://app.example/callback")
