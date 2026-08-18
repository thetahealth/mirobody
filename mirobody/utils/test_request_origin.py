"""The origin we hand to clients must be reachable.

`request_origin` produces the `scheme://host[:port]` that goes into OAuth
redirect URIs and the MCP endpoint URLs we give Claude Desktop and Cursor. Five
call sites built it by hand as

    f"{'http' if request.url.hostname == 'localhost' else 'https'}://{request.url.hostname}"

which is wrong three ways at once, and the local-login failure it caused was
found by someone trying to sign in to a dev server:

* `hostname` drops the port, so a server on :18080 advertised `http://localhost`
  — port 80, nothing listening;
* anything not literally "localhost" was forced to https, so `http://127.0.0.1:8000`
  advertised `https://127.0.0.1`;
* the request's own scheme was ignored.
"""

from __future__ import annotations

import pytest
from starlette.datastructures import URL

from .http import request_origin


class _Req:
    def __init__(self, raw: str):
        self.url = URL(raw)


@pytest.mark.parametrize("raw,expected", [
    # the reported bug: a local dev server on a non-default port
    ("http://localhost:18080/auth/session", "http://localhost:18080"),
    # loopback by IP — the old code forced https AND dropped the port
    ("http://127.0.0.1:8000/mcp", "http://127.0.0.1:8000"),
    # production: no port, https preserved
    ("https://mirobody.ai/mcp", "https://mirobody.ai"),
    # an explicit non-default https port must survive
    ("https://example.com:8443/mcp", "https://example.com:8443"),
    # default ports are omitted, because netloc omits them
    ("http://example.com/x", "http://example.com"),
])
def test_origin_is_reachable(raw: str, expected: str):
    assert request_origin(_Req(raw)) == expected


def test_the_port_is_never_dropped():
    """The specific regression: a link a user clicks has to hit the server."""
    origin = request_origin(_Req("http://localhost:18080/oauth/authorize"))
    assert ":18080" in origin, "an OAuth redirect without the port lands on port 80"


def test_scheme_comes_from_the_request_not_the_hostname():
    """Guessing https from the hostname produced an unreachable URL."""
    assert request_origin(_Req("http://myhost:9000/x")).startswith("http://")
    assert request_origin(_Req("https://myhost/x")).startswith("https://")


def test_no_hand_rolled_origin_survives():
    """One helper, or this drifts back into five copies."""
    import pathlib
    root = pathlib.Path(__file__).parent.parent
    offenders = []
    for p in root.rglob("*.py"):
        if "__pycache__" in str(p) or p.name.startswith("test_"):
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        if 'else "https"}://{request.url.hostname}' in text:
            offenders.append(str(p.relative_to(root.parent)))
    assert not offenders, f"hand-built origin (drops the port) in: {offenders}"
