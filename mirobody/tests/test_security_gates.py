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


# -- the first-run page: who may change where health data goes -----------------
#
# Reviewed on 2026-10-06 (1.5.4): the token printed at every boot and stayed
# enough on its own forever; the key check swapped the whole process's
# os.environ for up to 90 s; ten wrong tokens behind one proxy address locked
# the owner's right token out; a non-ASCII token was a 500.


def _setup_module():
    # `mirobody.server.routers.setup_router` is also the name the package
    # exports the APIRouter under, which shadows the module on attribute access.
    import importlib

    return importlib.import_module("mirobody.server.routers.setup_router")


def _setup_app(monkeypatch, *, ready: bool, user_id: int = 0):
    from fastapi import FastAPI

    from mirobody.utils.config import llm

    setup = _setup_module()

    monkeypatch.setattr(setup, "_token", "right-token")
    monkeypatch.setattr(setup, "_failures", {})
    monkeypatch.setattr(llm, "chat_default", lambda lookup=None: "claude-sonnet" if ready else None)
    app = FastAPI()

    @app.middleware("http")
    async def signed_in(request, call_next):
        request.state.user_id = user_id
        return await call_next(request)

    app.include_router(setup.router)
    return TestClient(app), setup


def test_a_wrong_or_odd_setup_token_is_refused_not_a_500(monkeypatch):
    client, _ = _setup_app(monkeypatch, ready=False)
    for token in ("", "wrong", "wrông"):
        # A header arrives as latin-1 bytes; the server sees "wrông".
        headers = {"X-Setup-Token": token.encode("latin-1")} if token else {}
        assert client.post("/api/setup", json={"mode": "local"}, headers=headers).json()["code"] == 403


def test_the_right_setup_token_still_works_after_ten_wrong_ones(monkeypatch):
    client, setup = _setup_app(monkeypatch, ready=True, user_id=7)
    for _ in range(12):
        client.post("/api/setup", json={"mode": "local"}, headers={"X-Setup-Token": "wrong"})
    assert client.post("/api/setup", json={"mode": "local"}, headers={"X-Setup-Token": "wrong"}).json()["code"] == 429

    async def saved(choice):
        return {"code": 0, "msg": "ok", "data": {}}

    monkeypatch.setattr(setup, "_save", saved)
    assert client.post("/api/setup", json={"mode": "local"}, headers={"X-Setup-Token": "right-token"}).json()["code"] == 0


def test_once_a_model_is_set_up_the_token_alone_changes_nothing(monkeypatch):
    client, setup = _setup_app(monkeypatch, ready=True, user_id=0)
    called = []

    async def saved(choice):
        called.append(choice)
        return {"code": 0, "msg": "ok", "data": {}}

    monkeypatch.setattr(setup, "_save", saved)
    answer = client.post("/api/setup", json={"mode": "local", "base_url": "https://elsewhere.example/v1"},
                         headers={"X-Setup-Token": "right-token"}).json()
    assert answer["code"] == 401 and not called


def test_the_first_choice_takes_the_token_alone(monkeypatch):
    client, setup = _setup_app(monkeypatch, ready=False, user_id=0)

    async def saved(choice):
        return {"code": 0, "msg": "ok", "data": {}}

    monkeypatch.setattr(setup, "_save", saved)
    assert client.post("/api/setup", json={"mode": "local"}, headers={"X-Setup-Token": "right-token"}).json()["code"] == 0


class _ModelsConfig:
    """Just the MODELS table, for the setup checks below."""

    def __init__(self, providers):
        self._providers = providers

    def get_agent_settings(self):
        return {"providers": self._providers}


def _key_check(monkeypatch, choice_fields):
    import os

    from mirobody.agent import probe, registry
    from mirobody.utils.config import config, settings

    setup = _setup_module()
    providers = {
        "claude-sonnet": {"llm_type": "openai", "api_key": "OPENROUTER_API_KEY", "model": "vendor/model-a",
                          "model_env": "OPENROUTER_CHAT_MODEL"},
        "openrouter-utils": {"llm_type": "openai", "api_key": "OPENROUTER_API_KEY", "model": "vendor/utils-a",
                             "model_env": "OPENROUTER_UTILS_MODEL", "chat": False},
    }
    monkeypatch.setattr(config, "global_config", lambda: _ModelsConfig(providers))
    for name in ("OPENROUTER_API_KEY", "OPENROUTER_CHAT_MODEL", "OPENROUTER_UTILS_MODEL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(settings, "set_in_environment", lambda: frozenset())
    seen = {}

    async def chat(name, resolve=None, entry=None):
        seen["environ"] = os.environ.get("OPENROUTER_API_KEY")
        seen["resolved"] = resolve("OPENROUTER_API_KEY")
        seen["model"] = (entry or {}).get("model")
        return True, "ok"

    async def save(values):
        seen["saved"] = {k: v for k, v in values.items() if v}

    monkeypatch.setattr(probe, "_chat", chat)
    monkeypatch.setattr(settings, "save", save)
    monkeypatch.setattr(registry, "reload_llm_clients", lambda: None)
    answer = asyncio.run(setup._save(setup.SetupChoice(mode="key", name="OPENROUTER_API_KEY",
                                                        value="sk-or-candidate", **choice_fields)))
    return answer, seen


def test_a_key_is_checked_without_touching_the_process_environment(monkeypatch):
    answer, seen = _key_check(monkeypatch, {})
    assert answer.code == 0
    assert seen == {"environ": None, "resolved": "sk-or-candidate", "model": "vendor/model-a",
                    "saved": {"OPENROUTER_API_KEY": "sk-or-candidate"}}


def test_a_model_typed_beside_the_key_is_the_one_checked_and_kept(monkeypatch):
    answer, seen = _key_check(monkeypatch, {"model": "vendor/model-b", "utils_model": "vendor/utils-a"})
    assert answer.code == 0
    # The utility model typed is the configured one: nothing to keep for it.
    assert seen["model"] == "vendor/model-b"
    assert seen["saved"] == {"OPENROUTER_API_KEY": "sk-or-candidate", "OPENROUTER_CHAT_MODEL": "vendor/model-b"}
    assert _key_check(monkeypatch, {"model": "two words"})[0].code == 400


# -- log lines: a database driver's text never reaches them ---------------------


def _driver_error() -> Exception:
    """An exception the way psycopg raises one: its message quotes the statement
    and the bound parameters."""
    error = type("UniqueViolation", (Exception,), {"__module__": "psycopg.errors"})
    return error("duplicate key: INSERT INTO th_observation VALUES ('Jane Doe', 'HbA1c 9.1')")


def _wrapped(driver: Exception) -> RuntimeError:
    """The provider base class's shape: `raise RuntimeError(str(e)) from e`."""
    try:
        try:
            raise driver
        except Exception as e:
            raise RuntimeError(str(e)) from e
    except RuntimeError as wrapper:
        return wrapper


def test_a_wrapped_driver_error_is_still_a_driver_error():
    from mirobody.kernel.ops import is_driver_exception

    assert is_driver_exception(_driver_error())
    assert is_driver_exception(_wrapped(_driver_error()))
    assert not is_driver_exception(RuntimeError("no driver below"))


def test_the_filter_scrubs_a_wrapped_driver_errors_traceback():
    import logging

    from mirobody.kernel.ops import PHIFilter, PHIPolicy

    wrapper = _wrapped(_driver_error())
    record = logging.LogRecord("t", logging.ERROR, __file__, 1, "pull failed", (), (RuntimeError, wrapper, None))
    PHIFilter(PHIPolicy()).filter(record)
    assert record.exc_info is None
    assert record.getMessage() == "pull failed [RuntimeError]"
