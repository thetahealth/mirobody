"""`/api/citations` hands `resolve_citations` the ids the answer cited (#192).

A cite may hold a comma: a `ref:doi:` id, or a document's `<path>#L12-L14`
whose file is `lab results, March.pdf`. The comma-separated GET cut those into
pieces, and no piece came back under the id the client asked about.
"""

import asyncio
import json
from urllib.parse import urlencode

import pytest

# The chat service arrives with the `[app]` extra, so CI's minimal job, a
# `pip install -e '.[test]'`, would abort COLLECTION here rather than skip.
try:
    from starlette.requests import Request

    from mirobody.agent.chat import service
except ImportError as missing:  # pragma: no cover - the minimal install
    pytest.skip(f"needs the [app] extra: {missing}", allow_module_level=True)

#: Two of the three cite kinds may hold a comma; the leading space is part of
#: the id too, and the GET's strip changed it.
IDS = ["r1", "/library/lab results, March.pdf#L12-L14", "ref:doi:10.1000/a,b", " r2"]


def _request(method: str, *, body: bytes = b"", query: str = "") -> Request:
    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({"type": "http", "method": method, "path": "/api/citations", "headers": [],
                    "query_string": query.encode(), "client": ("127.0.0.1", 1)}, receive)


def _call(monkeypatch, request: Request) -> tuple[dict, list]:
    asked = []

    async def resolve_citations(user_id, session_id, rids):
        asked.append((user_id, session_id, rids))
        return {"code": 0, "msg": "ok", "data": [{"rid": r, "status": "unknown"} for r in rids]}

    monkeypatch.setattr(service, "resolve_citations", resolve_citations)
    chat = object.__new__(service.ChatService)
    response = asyncio.run(service.ChatService.citations_handler.__wrapped__(chat, request, "7"))
    return json.loads(response.body), asked


def test_a_posted_list_reaches_the_resolver_as_cited(monkeypatch):
    body = json.dumps({"session_id": "s1", "rids": IDS}).encode()
    reply, asked = _call(monkeypatch, _request("POST", body=body))
    assert asked == [("7", "s1", IDS)]
    assert [row["rid"] for row in reply["data"]] == IDS


def test_the_comma_get_still_answers_the_shipped_client(monkeypatch):
    reply, asked = _call(monkeypatch, _request("GET", query=urlencode({"session_id": "s1", "rids": "r1,r2"})))
    assert asked == [("7", "s1", ["r1", "r2"])] and reply["code"] == 0


def test_a_post_without_a_list_of_strings_is_refused(monkeypatch):
    for body in ({"session_id": "s1", "rids": "r1,r2"}, {"session_id": "s1", "rids": ["r1", 2]},
                 {"rids": ["r1"]}, {"session_id": "s1", "rids": []}):
        reply, asked = _call(monkeypatch, _request("POST", body=json.dumps(body).encode()))
        assert reply["code"] == -1 and not asked, body


def test_the_route_accepts_post(monkeypatch):
    monkeypatch.setattr(service, "load_agent", lambda *args, **kwargs: None)
    routes = service.ChatService(routes=[]).routes
    [route] = [r for r in routes if r.path == "/api/citations"]
    assert {"GET", "POST"} <= set(route.methods)
