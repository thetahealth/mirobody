"""Protocol-level tests for the MCP server.

These pin behaviours that were each a real defect found in review, and that a
unit test on a helper would not have caught — they only show up when a whole
JSON-RPC request goes through ``McpService.mcp_handler``:

* version negotiation (the handler used to echo its own revision back at every
  client, whatever the client asked for),
* the 2026-07-28 additions (``resultType``, per-request ``_meta``,
  ``server/discover``) landing without breaking handshake-based clients,
* a request with no ``id`` not raising KeyError out of the handler,
* deterministic list ordering, so a client's prompt cache survives a reconnect,
* ``resources/read`` never serving one caller's JWT to the next.

No database, no network: the handler is exercised with a stub request.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from mirobody.mcp.resource import load_resources_from_directory, read_resource
from mirobody.mcp.service import (
    _LATEST_PROTOCOL_VERSION,
    _SUPPORTED_PROTOCOL_VERSIONS,
    McpService,
    _by_name,
)


def _request(body: dict) -> MagicMock:
    r = MagicMock()
    r.method = "POST"
    r.url.path = "/mcp"
    r.url.query = ""
    r.url.hostname = "localhost"
    r.headers = {}

    async def _body():
        return json.dumps(body).encode()

    r.body = _body
    return r


@pytest.fixture(scope="module")
def service():
    return McpService(name="Test MCP", version="9.9.9")


async def _call(service, body: dict) -> dict:
    response = await service.mcp_handler(_request(body))
    return json.loads(response.body.decode())


# ── version negotiation ──────────────────────────────────────────────────────

@pytest.mark.parametrize("requested", _SUPPORTED_PROTOCOL_VERSIONS)
async def test_initialize_honours_the_clients_revision(service, requested):
    """A client pinned to an older revision must be answered in THAT revision."""
    out = await _call(service, {
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": requested},
    })
    assert out["result"]["protocolVersion"] == requested


async def test_initialize_falls_back_for_unknown_revision(service):
    out = await _call(service, {
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "1999-01-01"},
    })
    assert out["result"]["protocolVersion"] == _LATEST_PROTOCOL_VERSION


# ── 2026-07-28 additions ─────────────────────────────────────────────────────

async def test_results_carry_result_type(service):
    """`resultType` is REQUIRED in 2026-07-28 and ignored by older clients."""
    out = await _call(service, {"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert out["result"]["resultType"] == "complete"


async def test_stateless_call_without_initialize(service):
    """2026-07-28 drops the handshake: a first request may be tools/list."""
    out = await _call(service, {
        "jsonrpc": "2.0", "id": 1, "method": "tools/list",
        "params": {"_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {},
        }},
    })
    assert "tools" in out["result"]


@pytest.mark.parametrize("requested,expected", [
    ("2026-07-28", "2026-07-28"),
    ("2024-11-05", "2024-11-05"),   # a revision we still speak: honour it
    ("1999-01-01", _LATEST_PROTOCOL_VERSION),        # one we do not: answer with our newest
    (None,         _LATEST_PROTOCOL_VERSION),        # handshake-era client, no _meta at all
])
async def test_per_request_version_is_negotiated_and_echoed(service, requested, expected):
    """The negotiated revision must come back in `_meta`, per request.

    This is the half that was missing: `_request_protocol_version` existed,
    documented and tested-looking, but had ZERO call sites — the server always
    answered as its own newest revision no matter what the client asked for,
    and the stateless test above passed identically with the method deleted.

    Echoing is not redundant with `initialize`. Under 2026-07-28 the client
    never sends `initialize`, so a response `_meta` is the only channel left
    through which it can learn what the server actually settled on.
    """
    params = {}
    if requested is not None:
        params["_meta"] = {"io.modelcontextprotocol/protocolVersion": requested}

    out = await _call(service, {
        "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": params,
    })
    assert out["result"]["_meta"]["io.modelcontextprotocol/protocolVersion"] == expected


@pytest.mark.parametrize("method", ["tools/list", "prompts/list", "resources/list"])
async def test_cacheable_lists_carry_cache_hints(service, method):
    """2026-07-28 caching metadata, on exactly the methods the spec allows.

    Field names are `ttlMs` / `cacheScope`, taken from the SDK's own
    `ListToolsResult` rather than guessed. `private` is deliberate: the tool
    list is filtered per agent and resources are templated per request, so a
    shared cache must never hand one caller's copy to another.
    """
    out = await _call(service, {"jsonrpc": "2.0", "id": 1, "method": method})
    assert out["result"]["ttlMs"] > 0
    assert out["result"]["cacheScope"] == "private"


async def test_non_cacheable_methods_have_no_hints(service):
    """`ping` is not in CACHEABLE_METHODS; emitting a hint there is a spec violation."""
    out = await _call(service, {"jsonrpc": "2.0", "id": 1, "method": "ping"})
    assert "ttlMs" not in out["result"] and "cacheScope" not in out["result"]


async def test_server_discover(service):
    out = await _call(service, {"jsonrpc": "2.0", "id": 1, "method": "server/discover"})
    result = out["result"]
    assert result["supportedProtocolVersions"] == list(_SUPPORTED_PROTOCOL_VERSIONS)
    assert result["serverInfo"]["name"] == "Test MCP"
    # SHOULD-level per-response identity, for display/debug only.
    assert result["_meta"]["io.modelcontextprotocol/serverInfo"]["version"] == "9.9.9"


# ── robustness ───────────────────────────────────────────────────────────────

async def test_request_without_id_does_not_raise(service):
    """`id` is optional on the wire; the handler used to re-index it and 500."""
    out = await _call(service, {"jsonrpc": "2.0", "method": "tools/list"})
    assert "result" in out or "error" in out


async def test_malformed_body_is_rejected_cleanly(service):
    response = await service.mcp_handler(_request({}))
    assert response.status_code == 200


# ── deterministic ordering ───────────────────────────────────────────────────

def test_lists_are_sorted_not_filesystem_ordered():
    """Tool discovery uses os.scandir, whose order is filesystem-dependent."""
    unsorted = [{"name": "zeta"}, {"name": "alpha"}, {"name": "mid"}]
    assert [t["name"] for t in _by_name(unsorted)] == ["alpha", "mid", "zeta"]
    assert _by_name(None) == []
    assert _by_name([]) == []


def test_resources_sort_by_uri():
    items = [{"uri": "ui://widget/upload.html"}, {"uri": "ui://widget/chart.html"}]
    assert [r["uri"] for r in _by_name(items, key="uri")][0].endswith("chart.html")


# ── resources: the cross-user leak ───────────────────────────────────────────

def test_read_resource_returns_a_copy_not_the_cache():
    """Templating per-request values must not poison the shared cache.

    `resources/read` substitutes {{JWT_TOKEN}} into the widget HTML. While it
    handed back the cached dict, the FIRST caller's token was baked in and every
    later caller was served that token instead of their own.
    """
    resources, _ = load_resources_from_directory("mirobody/agent/resources")
    if not resources:
        pytest.skip("widget resources not present")

    uri = next(iter(resources))
    first = read_resource(resources, uri)
    assert first is not None
    first["text"] = "REPLACED BY CALLER ONE"

    second = read_resource(resources, uri)
    assert second["text"] != "REPLACED BY CALLER ONE"


def test_read_resource_guards_bad_input():
    # `isinstance(resources)` — one argument — used to raise TypeError here.
    assert read_resource({}, "x") is None
    assert read_resource({"a": {}}, "missing") is None
