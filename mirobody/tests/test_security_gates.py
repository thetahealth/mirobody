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
)
from mirobody.user.auth.bearer import lacks_second_factor
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


# ============================================================================
# Server and user routes (quality pass, 2026-10): whose record a request
# reads, who may export it, and what every answer carries.
# ============================================================================


def _grants(monkeypatch, granted: dict[tuple[int, int], int]):
    """The care circle as a table of `(operator, subject) -> health_access`,
    so the real `resolve_subject` runs without a database."""
    from mirobody.user import care_circle as cc

    async def accepted_membership(operator_id, subject_id):
        access = granted.get((int(operator_id), int(subject_id)))
        return None if access is None else cc.Membership(health_access=access)

    monkeypatch.setattr(cc, "accepted_membership", accepted_membership)


@pytest.mark.parametrize("target", [None, "", "7", "07", " 7 "])
def test_a_request_for_your_own_record_needs_no_grant(monkeypatch, target):
    from mirobody.server.auth import subject_for

    _grants(monkeypatch, {})
    assert asyncio.run(subject_for("7", target)) == "7"
    assert asyncio.run(subject_for("7", target, write=True)) == "7"


def test_a_member_is_named_by_the_id_the_check_decided_on(monkeypatch):
    """`07` passed the check for member 7 and the write was filed under `07`,
    a record nobody reads."""
    from mirobody.server.auth import subject_for
    from mirobody.user.care_circle import ACCESS_EDIT, ACCESS_VIEW

    _grants(monkeypatch, {(7, 8): ACCESS_VIEW, (7, 9): ACCESS_EDIT})
    assert asyncio.run(subject_for("7", "08")) == "8"
    assert asyncio.run(subject_for("7", "009", write=True)) == "9"


def test_a_grant_is_trimmed_to_what_was_asked(monkeypatch):
    from mirobody.server.auth import subject_for
    from mirobody.user.care_circle import ACCESS_NONE, ACCESS_VIEW

    _grants(monkeypatch, {(7, 8): ACCESS_VIEW, (7, 10): ACCESS_NONE})
    assert asyncio.run(subject_for("7", "8", write=True)) is None
    assert asyncio.run(subject_for("7", "10")) is None
    assert asyncio.run(subject_for("7", "11")) is None
    assert asyncio.run(subject_for("7", "not-an-id")) is None


def _router_app(module_name: str, user_id: str = "7"):
    """`module_name`'s router signed in as `user_id`. The package exports each
    APIRouter under its module's name, so the module is reached by import."""
    import importlib

    from fastapi import FastAPI

    from mirobody.server.auth import verify_token

    module = importlib.import_module(f"mirobody.server.routers.{module_name}")
    app = FastAPI()
    app.include_router(module.router)
    app.dependency_overrides[verify_token] = lambda: user_id
    return TestClient(app), module


def test_a_read_grant_does_not_export_a_members_genome(monkeypatch):
    """A care-circle VIEW grant streamed the member's whole genome as VCF;
    the readings export has been owner-only since 1.5.3."""
    from mirobody.user.care_circle import ACCESS_VIEW

    _grants(monkeypatch, {(7, 8): ACCESS_VIEW})
    client, genomics = _router_app("genomics_router")
    read = []

    async def execute_query(query, params=None, **kw):
        read.append(params)
        return []

    monkeypatch.setattr(genomics, "execute_query", execute_query)
    answer = client.get("/api/v1/genomics/export.vcf", params={"build": "GRCh38", "target_user_id": "8"})
    assert answer.json()["code"] == 403 and not read
    own = client.get("/api/v1/genomics/export.vcf", params={"build": "GRCh38"})
    assert own.json()["code"] == 404 and read == [{"user_id": "7"}]


def _oauth_platform(monkeypatch, callback):
    """A registered platform `acme` whose one OAuth2 provider's callback is
    `callback`, as the vendor callback route finds it."""
    from mirobody.collect import LinkType, platform_manager

    class Provider:
        info = type("Info", (), {"auth_type": LinkType.OAUTH2})()

        async def callback(self, code, state):
            return await callback(code, state)

    class Platform:
        def get_provider(self, slug):
            return Provider()

    monkeypatch.setitem(platform_manager._platforms, "acme", Platform())


def _completion_page(html: str) -> dict:
    import json
    import re

    return json.loads(re.search(r"var page = (.*?);\n", html).group(1))


def test_a_failed_vendor_callback_shows_a_code_not_the_exception(monkeypatch):
    async def callback(code, state):
        raise RuntimeError("token exchange failed for you@mirobody.ai")

    _oauth_platform(monkeypatch, callback)
    client, _ = _router_app("public_router")
    html = client.get("/api/v1/pulse/acme/acme_watch/callback", params={"code": "x"}).text
    assert "mirobody.ai" not in html
    page = _completion_page(html)
    assert page["message"]["error"] == "oauth_failed" and page["message"]["success"] is False
    # The opener is told only on this deployment's own origin, never "*".
    assert page["targets"] == ["http://testserver"]


def test_the_completion_page_cannot_be_closed_from_inside(monkeypatch):
    async def callback(code, state):
        return {"note": "</script><script>alert(1)</script>"}

    _oauth_platform(monkeypatch, callback)
    client, _ = _router_app("public_router")
    html = client.get("/api/v1/pulse/acme/acme_watch/callback", params={"code": "x"}).text
    assert html.count("</script>") == 1
    assert _completion_page(html)["message"]["data"] == {"note": "</script><script>alert(1)</script>"}


def _passkeys(monkeypatch, *, enrolled: bool):
    """A WebAuthn service for an account with MFA on, which has registered a
    passkey when `enrolled`; and a token minted with `aal`."""
    from mirobody.user import user as user_module
    from mirobody.user.auth.jwt import JwtTokenValidator
    from mirobody.user.auth.webauthn import WebAuthnService

    async def active(user_id, minted_at=None):
        return True

    async def mfa_enabled(user_id):
        return True

    async def credentials(user_id):
        return [{"credential_id": b"passkey-1", "transports": ["internal"]}] if enrolled else []

    monkeypatch.setattr(user_module, "is_active_account", active)
    validator = JwtTokenValidator("k" * 32)
    service = WebAuthnService(validator, rp_id="localhost", origin="http://localhost:18060")
    monkeypatch.setattr(service, "_is_mfa_enabled", mfa_enabled)
    monkeypatch.setattr(service, "get_credentials_for_user", credentials)

    def token(aal: int) -> str:
        access, _, _ = asyncio.run(validator.generate_tokens(
            "7", "you@mirobody.ai", gen_claims_func=lambda _u, _e: {"aal": aal}))
        return access

    return TestClient(Starlette(routes=service.routes)), token


@pytest.mark.parametrize("route", ["/auth/webauthn/register/options", "/auth/webauthn/register/verify"])
def test_a_second_passkey_takes_the_first(monkeypatch, route):
    """The AAL1 token sign-in hands an MFA account enrolled a passkey of the
    caller's choosing, and registration answered with an AAL2 token."""
    client, token = _passkeys(monkeypatch, enrolled=True)
    answer = client.post(route, json={}, headers={"Authorization": f"Bearer {token(1)}"})
    assert answer.status_code == 403
    assert answer.json()["detail"]["code"] == "ERROR_AAL2_REQUIRED"


def test_the_first_passkey_and_an_aal2_session_still_enrol(monkeypatch):
    client, token = _passkeys(monkeypatch, enrolled=False)
    first = client.post("/auth/webauthn/register/options", headers={"Authorization": f"Bearer {token(1)}"})
    assert first.status_code == 200 and first.json()["data"]["challenge"]

    client, token = _passkeys(monkeypatch, enrolled=True)
    another = client.post("/auth/webauthn/register/options", headers={"Authorization": f"Bearer {token(2)}"})
    assert another.status_code == 200 and another.json()["data"]["excludeCredentials"]


#: What a failing dependency says in these tests: an address and a reading,
#: the two things an exception's text has been seen to carry.
_LEAK = "lookup failed for you@mirobody.ai at 7.3 mmol/L"


def _raises():
    async def fail(*args, **kwargs):
        raise RuntimeError(_LEAK)
    return fail


def test_a_failing_files_route_answers_a_sentence_not_the_exception(monkeypatch):
    client, files = _router_app("file_router")
    monkeypatch.setattr(files, "get_user_data_distribution", _raises())
    answer = client.get("/api/v1/data/data-distribution")
    assert answer.json()["code"] == 500 and "mirobody.ai" not in answer.text

    async def deletion_raised(**kw):
        return {"success": False, "error": f"Internal error: {_LEAK}", "message_id": "m1"}

    monkeypatch.setattr(files, "delete_all_files_from_message", deletion_raised)
    answer = client.post("/api/v1/data/delete-files", json={"message_id": "m1"})
    assert answer.json()["code"] == 500 and "mirobody.ai" not in answer.text


def test_a_public_share_link_answers_a_sentence_not_the_exception(monkeypatch):
    client, share = _router_app("session_share_router")
    monkeypatch.setattr(share.chat_session, "get_shared_session_history", _raises())
    answer = client.get("/api/share/0123456789abcdef0123")
    assert answer.json()["code"] == 500 and "mirobody.ai" not in answer.text


@pytest.mark.parametrize("body", ["7.3 mmol/L, not JSON",
                                  '{"metaInfo": {"timezone": "UTC"}, "healthData": "7.3 mmol/L"}'])
def test_an_unreadable_apple_upload_is_refused_without_quoting_it(body):
    """The decoder's and pydantic's messages quote the input, and the input
    is readings."""
    client, _ = _router_app("apple_router")
    answer = client.post("/apple/health", content=body, headers={"Content-Type": "application/json"})
    assert answer.status_code == 400 and "7.3" not in answer.text
