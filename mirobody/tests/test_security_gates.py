"""The security gates closed on 2026-10-01, each reproduced on a running
server before it was fixed (a second factor skipped for tokens in the query
string, uploads keyed by a client-chosen id, an open redirect on the vendor
callback, a placeholder JWT_KEY kept on loopback). These pin the decisions;
the server itself needs the `[app]` extra, so on a minimal install they skip.
"""
import asyncio

import pytest

pytest.importorskip("fastapi")

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from mirobody.collect.files.file_upload_manager import WebSocketFileUploadManager
from mirobody.server.middlewares import (
    SECURITY_HEADERS,
    JwtMiddleware,
    lacks_second_factor,
)
from mirobody.utils.http import safe_return_url

OWN = "http://localhost:18060"


# -- the vendor callback's return_url ------------------------------------------


@pytest.mark.parametrize(
    "url",
    ["/data", "/data?tab=connect_data_source", OWN + "/data", "http://LOCALHOST:18060/x"],
)
def test_a_return_url_on_this_origin_is_kept(url):
    assert safe_return_url(url, OWN) == url


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/landing",
        "//evil.example/x",
        "/\\evil.example",
        "https://localhost:18060@evil.example/",
        "javascript:alert(1)",
        " /data",
        "/data\n",
        "",
        None,
    ],
)
def test_a_return_url_leaving_this_deployment_is_refused(url):
    assert safe_return_url(url, OWN) is None


def test_listed_origins_and_app_schemes_are_kept():
    allowed = ["https://app.example.com", "theta:"]
    assert safe_return_url("https://app.example.com/back", OWN, allowed) == "https://app.example.com/back"
    assert safe_return_url("theta://oauth-done", OWN, allowed) == "theta://oauth-done"
    assert safe_return_url("https://app.example.com.evil.io/", OWN, allowed) is None
    assert safe_return_url("other://x", OWN, allowed) is None


# -- the second factor, wherever the token came from ---------------------------


def _requires(answer: bool):
    async def requires_second_factor(user_id: int) -> bool:
        return answer
    return requires_second_factor


def test_an_aal1_token_of_an_mfa_account_lacks_the_second_factor():
    assert asyncio.run(lacks_second_factor(7, {"aal": 1}, _requires(True))) is True
    assert asyncio.run(lacks_second_factor(7, {}, _requires(True))) is True


def test_an_aal2_token_or_an_account_without_mfa_passes():
    assert asyncio.run(lacks_second_factor(7, {"aal": 2}, _requires(True))) is False
    assert asyncio.run(lacks_second_factor(7, {"aal": 1}, _requires(False))) is False
    assert asyncio.run(lacks_second_factor(7, {"aal": 1}, None)) is False
    assert asyncio.run(lacks_second_factor(0, {"aal": 1}, _requires(True))) is False


# -- an upload belongs to the account that started it --------------------------


def _manager_with_upload():
    manager = WebSocketFileUploadManager()
    manager.upload_sessions["m1"] = {"user_id": "1", "connection_id": "1_tab", "status": "uploading"}
    sent = []

    async def send_message(connection_id, message):
        sent.append((connection_id, message))

    manager.send_message = send_message
    return manager, sent


def test_only_the_starting_account_sees_its_upload():
    manager, _ = _manager_with_upload()
    assert manager._session_for("m1", "1") is not None
    assert manager._session_for("m1", "2") is None
    status = asyncio.run(manager.get_upload_status("2_tab", "m1", user_id="2"))
    assert status["status"] == "not_found"


def test_another_accounts_chunk_is_refused():
    manager, sent = _manager_with_upload()
    ok = asyncio.run(manager.handle_file_chunk(
        "2_tab", {"messageId": "m1", "filename": "a.txt", "chunk": "", "_real_user_id": "2"},
    ))
    assert ok is False
    assert sent and sent[-1][1]["message"] == "Invalid upload session"
    assert manager.upload_sessions["m1"]["status"] == "uploading"


# -- a placeholder JWT_KEY, wherever the server listens ------------------------


class _Config:
    def __init__(self, host):
        self.http = type("H", (), {"host": host})()
        self.refreshed = False

    def get_str(self, key):
        import os

        from mirobody.utils.config.config import PLACEHOLDER_SENTINEL
        return os.environ.get(key) or PLACEHOLDER_SENTINEL

    def refresh(self):
        self.refreshed = True


@pytest.mark.parametrize("host", ["127.0.0.1", "0.0.0.0"])
def test_a_placeholder_jwt_key_is_replaced_for_the_run(monkeypatch, host):
    from mirobody.server.bootstrap import _guard_placeholder_jwt_key
    from mirobody.utils.config.config import PLACEHOLDER_SENTINEL

    monkeypatch.delenv("JWT_KEY", raising=False)
    config = _Config(host)
    _guard_placeholder_jwt_key(config)
    import os
    assert os.environ["JWT_KEY"] not in ("", PLACEHOLDER_SENTINEL)
    assert len(os.environ["JWT_KEY"]) == 64 and config.refreshed


# -- headers on every response -------------------------------------------------


def test_every_response_carries_the_security_headers():
    async def hello(request):
        return PlainTextResponse("ok")

    app = Starlette(
        routes=[Route("/", hello)],
        middleware=[Middleware(JwtMiddleware, jwt_key="k")],
    )
    response = TestClient(app).get("/")
    for name, value in SECURITY_HEADERS.items():
        assert response.headers.get(name) == value
