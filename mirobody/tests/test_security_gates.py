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


# -- the agent and the MCP surface: a secret, a record, a caller ---------------


_LITERAL_KEY = "sk-proj-THIS-IS-THE-LITERAL-SECRET"


def test_a_secret_written_where_a_key_name_belongs_is_never_repeated(caplog):
    from mirobody.agent.models.clients import build_llm_clients, unavailable_reason

    table = {"gpt": {"llm_type": "openai", "model": "gpt-x", "api_key": _LITERAL_KEY},
             "named": {"llm_type": "openai", "model": "gpt-y", "api_key": "OPENAI_API_KEY"}}
    with caplog.at_level("DEBUG"):
        clients = build_llm_clients(table, resolve=lambda name: None)
    assert _LITERAL_KEY not in caplog.text
    assert _LITERAL_KEY not in unavailable_reason(clients["gpt"])
    with pytest.raises(AttributeError) as raised:
        _ = clients["gpt"].invoke
    assert _LITERAL_KEY not in str(raised.value)
    # A variable NAME is what tells the operator what to set: it stays.
    assert "OPENAI_API_KEY" in unavailable_reason(clients["named"])


def test_a_turn_on_a_model_whose_key_is_a_literal_never_shows_it(monkeypatch):
    pytest.importorskip("langchain_core")
    from mirobody.agent import agent as agent_module
    from mirobody.agent.errors import ConfigError
    from mirobody.agent.models.clients import build_llm_clients

    clients = build_llm_clients({"gpt": {"llm_type": "openai", "model": "gpt-x", "api_key": _LITERAL_KEY}},
                                resolve=lambda name: None)
    monkeypatch.setattr(agent_module, "llm_client", clients.get)
    with pytest.raises(ConfigError) as raised:
        agent_module.MirobodyAgent(timezone="UTC")._chat_model("gpt")
    assert _LITERAL_KEY not in str(raised.value)


def _turn_on_someone_elses_record(monkeypatch, *, grant: str, file_list: list[dict]) -> tuple[list[dict], dict]:
    from mirobody.agent.chat import turn
    from mirobody.agent.chat.model import ChatStreamRequest
    from mirobody.user.care_circle import CareCircleDenied

    seen = {"stored": 0}

    async def resolve(operator, subject, *, require_write=False):
        if grant == "none" or (require_write and grant != "write"):
            raise CareCircleDenied("not granted")

    async def store(params):
        seen["stored"] += 1

    async def save(params):
        return None

    async def owner(params):
        return "Mum"

    monkeypatch.setattr(turn, "resolve_subject", resolve)
    monkeypatch.setattr(turn, "_store_files", store)
    monkeypatch.setattr(turn, "_save_question", save)
    monkeypatch.setattr(turn, "_record_owner", owner)
    params = ChatStreamRequest(question="what does this say", user_id="7", query_user_id="9", file_list=file_list)

    async def collect():
        return [block async for block in turn.run(params)]

    return asyncio.run(collect()), seen


def test_an_attachment_on_someone_elses_record_needs_their_write_grant(monkeypatch):
    blocks, seen = _turn_on_someone_elses_record(
        monkeypatch, grant="read", file_list=[{"file_key": "web_uploads/k.pdf", "file_name": "lab.pdf"}])
    assert blocks == [{"type": "error", "message": "No permission to add files to this user's record"}]
    assert seen["stored"] == 0


def test_a_question_on_someone_elses_record_needs_only_their_read_grant(monkeypatch):
    blocks, seen = _turn_on_someone_elses_record(monkeypatch, grant="read", file_list=[])
    assert {"type": "error", "message": "No permission to chat for this user"} not in blocks
    assert _turn_on_someone_elses_record(monkeypatch, grant="none", file_list=[])[0] == [
        {"type": "error", "message": "No permission to chat for this user"}]


def test_the_agent_is_told_whether_the_asker_may_change_the_record(monkeypatch):
    from mirobody.agent.chat import turn
    from mirobody.agent.chat.model import ChatStreamRequest

    async def owner(params):
        return "Mum"

    monkeypatch.setattr(turn, "_record_owner", owner)
    params = ChatStreamRequest(question="q", user_id="7", query_user_id="9", session_id="s")
    assert asyncio.run(turn._agent_kwargs(params, may_write=False))["may_write"] is False


def test_a_date_answer_is_filed_only_with_the_askers_write_grant_on_that_record(monkeypatch):
    pytest.importorskip("langchain_core")
    import mirobody.collect as collect
    from mirobody.agent import hitl

    files = {"k-mum": {"user_id": "7", "query_user_id": "9"}, "k-other": {"user_id": "5", "query_user_id": "5"}}
    filed = []

    async def get_file(file_key, user_id=None):
        return files.get(file_key)

    async def set_date(owner, file_key, when):
        filed.append((owner, file_key))
        return {"report_date": "2026-01-06 00:00:00", "moved": 1, "skipped": 0}

    monkeypatch.setattr(collect.FileDbService, "get_file_by_key", staticmethod(get_file))
    monkeypatch.setattr(collect, "set_file_report_date", set_date)

    out = asyncio.run(hitl.apply_report_date_answer("9", ["k-mum"], "2026-01-06", may_write=False))
    assert filed == [] and "Nothing was filed" in out
    out = asyncio.run(hitl.apply_report_date_answer("9", ["k-mum", "k-other"], "2026-01-06", may_write=True))
    assert filed == [("9", "k-mum")] and "k-other: no such file" in out


def test_a_share_link_that_is_not_a_uuid_never_reaches_the_database(monkeypatch):
    from mirobody.agent.chat import session

    async def execute_query(*args, **kwargs):
        raise AssertionError("an id that cannot be a share link must not be looked up")

    monkeypatch.setattr(session, "execute_query", execute_query)
    answer = asyncio.run(session.get_shared_session_history("x' OR '1'='1"))
    assert answer == {"code": -1, "msg": "Share session not found", "data": {}}


def test_a_failed_read_of_a_shared_conversation_answers_with_a_sentence(monkeypatch):
    from mirobody.agent.chat import session

    async def execute_query(*args, **kwargs):
        raise RuntimeError("SELECT session_id FROM th_session_share WHERE share_session_id = 'leak-7.31415'")

    monkeypatch.setattr(session, "execute_query", execute_query)
    answer = asyncio.run(session.get_shared_session_history("2f1c7a52-3c4e-4c5e-9a40-9d6c2c1d8b11"))
    assert answer["code"] == -4 and "7.31415" not in answer["msg"] and "SELECT" not in answer["msg"]


def _request(body: bytes = b"", query: bytes = b""):
    from starlette.requests import Request

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request({"type": "http", "method": "POST", "path": "/api/x", "headers": [], "query_string": query,
                    "client": ("127.0.0.1", 1)}, receive)


def _reply(response) -> dict:
    import json

    return json.loads(response.body)


def test_the_chat_service_replies_with_sentences_not_exception_text(monkeypatch):
    from mirobody.agent.chat import service

    _, response = asyncio.run(service._json_body(_request(b"{not json")))
    assert _reply(response)["msg"] == "The request body is not valid JSON."

    async def fails(*args, **kwargs):
        raise RuntimeError("relation th_sessions: leak-7.31415")

    monkeypatch.setattr(service, "get_session_summaries", fails)
    monkeypatch.setattr(service, "beneficiary_users", fails)
    chat = object.__new__(service.ChatService)
    history = asyncio.run(service.ChatService.history_handler.__wrapped__(chat, _request(), "7"))
    members = asyncio.run(service.ChatService.beneficiary_user_handler.__wrapped__(chat, _request(), "7"))
    for response in (history, members):
        assert _reply(response)["code"] == -1 and "7.31415" not in _reply(response)["msg"]


def test_the_attachment_note_reads_report_dates_of_the_records_own_files_only(monkeypatch):
    import json

    import mirobody.utils as utils
    from mirobody.agent.prompt import report_date_status

    queries = []

    async def execute_query(sql, params=None, **kwargs):
        queries.append((sql, params))
        if "user_id = :user_id" not in sql or params.get("user_id") != "7":
            return [{"file_key": "k-someone-elses", "file_content": json.dumps({"date_source": "manual",
                                                                                "report_date": "2020-02-02"})}]
        return [{"file_key": "k-own", "file_content": json.dumps({"date_source": "extracted",
                                                                   "report_date": "2026-01-06 00:00:00"})}]

    monkeypatch.setattr(utils, "execute_query", execute_query)
    note = asyncio.run(report_date_status("7", ["k-own", "k-someone-elses"]))
    assert len(queries) == 1
    assert note == "Report dates:\n- file_key=k-own: report date 2026-01-06 (found on the document)"
