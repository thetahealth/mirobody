"""Interactive thinking is explicit; utility calls and unknown APIs stay usable."""

from __future__ import annotations

import asyncio
from copy import deepcopy
import inspect
from pathlib import Path

from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage, ToolMessage, message_chunk_to_message
from openai.resources.responses.responses import AsyncResponses
import pytest
import yaml

import mirobody
from mirobody.agent import probe
from mirobody.agent.models import clients
from mirobody.agent.models.messages import message_reasoning

ROOT = Path(mirobody.__file__).resolve().parent.parent
LOCAL = {"model": "minicpm5-2b", "base_url": "http://127.0.0.1:8080/v1", "thinking_style": "chat_template"}


def _model(entry, **kwargs):
    return clients.build_chat_model(entry, alias="test", resolve=lambda _: "test-key", **kwargs)


def test_doctor_recognizes_shipped_responses_settings_and_still_reports_typos(monkeypatch):
    from mirobody.utils.config import llm

    entries = yaml.safe_load((ROOT / "config.llm.yaml").read_text())["MODELS"]
    monkeypatch.setattr(llm, "model_entries", lambda: entries)
    assert llm.unread_entry_keys() == {}
    entries["openai"]["use_response_api"] = True
    assert llm.unread_entry_keys() == {"openai": ["use_response_api"]}


def test_chat_overrides_disable_without_mutating_a_shared_utility_entry():
    entry = {**LOCAL, "thinking_level": "off", "extra_body": {"chat_template_kwargs": {"enable_thinking": False, "other": 1}}}
    before = deepcopy(entry)
    chat = _model(entry, require_thinking=True)._get_request_payload("hello")
    assert chat["extra_body"]["chat_template_kwargs"] == {"enable_thinking": True, "other": 1}
    utility = _model({**entry, "chat": False}, require_thinking=True)._get_request_payload("extract")
    assert utility["extra_body"]["chat_template_kwargs"]["enable_thinking"] is False
    assert entry == before


def test_table_applies_positive_effort_per_chat_and_preserves_utilities():
    table = {
        "chat": {"model": "m", "base_url": "https://openrouter.ai/api/v1", "thinking_level": "off"},
        "low": {"model": "m", "base_url": "https://openrouter.ai/api/v1", "thinking_level": "low"},
        "utility": {**LOCAL, "chat": False, "thinking_level": "off"},
        "qwen-utils": {"model": "qwen", "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                       "chat": False, "extra_body": {"enable_thinking": False}},
    }
    models = clients.build_llm_clients(table, thinking="high", require_thinking=True, resolve=lambda _: None)
    assert models["chat"].extra_body["reasoning"]["effort"] == "high"
    assert models["low"].extra_body["reasoning"]["effort"] == "low"
    assert models["utility"].extra_body["chat_template_kwargs"]["enable_thinking"] is False
    assert models["qwen-utils"].extra_body["enable_thinking"] is False
    assert _model(table["qwen-utils"], thinking="high", require_thinking=True).extra_body["enable_thinking"] is False


@pytest.mark.parametrize("budget", [None, 2048])
def test_openrouter_requests_visible_thinking_without_changing_host_policy(budget):
    reasoning = {"enabled": False, "exclude": True, "effort": "none"}
    if budget is not None:
        reasoning["max_tokens"] = budget
    host_policy = {"order": ["example-host"], "allow_fallbacks": False}
    model = _model({"model": "vendor/model", "base_url": "https://openrouter.ai/api/v1",
                    "extra_body": {"reasoning": reasoning, "provider": host_policy}}, require_thinking=True)
    body = model._get_request_payload("hello")["extra_body"]
    assert body["provider"] == host_policy
    assert body["reasoning"]["enabled"] is True and body["reasoning"]["exclude"] is False
    if budget is None:
        assert body["reasoning"]["effort"] == "medium"
    else:
        assert body["reasoning"]["max_tokens"] == budget and "effort" not in body["reasoning"]


def test_native_anthropic_keeps_adaptive_shape_and_valid_effort_but_shows_summary():
    entry = {"model": "claude-sonnet-5-5", "llm_type": "google_anthropic_vertex", "project": "project",
             "model_kwargs": {"thinking": {"type": "adaptive", "display": "omitted"}, "output_config": {"effort": "low"}}}
    enabled, effort = clients._chat_thinking_entry(entry, "high", lambda _: None)
    payload = clients._vertex_anthropic_kwargs("claude", enabled, effort, lambda _: None)["model_kwargs"]
    assert payload["thinking"] == {"type": "adaptive", "display": "summarized"}
    assert payload["output_config"] == {"effort": "low"}
    assert entry["model_kwargs"]["thinking"]["display"] == "omitted"


def test_anthropic_disabled_budget_becomes_supported_enabled_shape():
    entry = {"model": "claude-sonnet-4-5", "llm_type": "anthropic", "thinking": {"type": "disabled"}}
    enabled, effort = clients._chat_thinking_entry(entry, "low", lambda _: None)
    payload = clients._anthropic_kwargs("claude", enabled, effort, lambda _: None)
    assert payload["thinking"] == {"type": "enabled", "budget_tokens": 2048}


@pytest.mark.parametrize("entry", [
    {"model": "unknown", "base_url": "http://127.0.0.1:8080/v1"},
    {"model": "gpt-4.1", "base_url": "https://api.openai.com/v1"},
    {"model": "gpt-6-sol", "base_url": "http://127.0.0.1:8080/v1"},
])
def test_unverified_native_reasoning_sends_no_guessed_api_parameter(entry, caplog):
    payload = _model(entry, require_thinking=True)._get_request_payload("hello")
    assert "reasoning_effort" not in payload and "reasoning" not in payload
    assert not payload.get("extra_body")
    assert "native thinking is unverified" in caplog.text


def test_explicit_legacy_tool_transport_is_not_broken_by_a_thinking_hint(caplog):
    entry = {"model": "gpt-6-sol", "base_url": "https://api.openai.com/v1", "use_responses_api": False,
             "reasoning_effort": "none"}
    model = _model(entry, thinking="high", require_thinking=True)
    payload = model._get_request_payload("hello", tools=[probe._TOOL])
    assert payload["reasoning_effort"] == "none" and "messages" in payload
    assert "disabled by its tool-call contract" in caplog.text


def test_shipped_openai_responses_calls_tools_and_replays_encrypted_reasoning_without_storage():
    config = yaml.safe_load((ROOT / "config.llm.yaml").read_text())
    entry = config["MODELS"]["openai"]
    model = clients.build_chat_model(entry, alias="openai", resolve=lambda n: "test-key" if n == "OPENAI_API_KEY" else None,
                                    require_thinking=True)
    question = HumanMessage("Look up my glucose.")
    first = model._get_request_payload([question], tools=[probe._TOOL])
    assert first["reasoning"] == {"summary": "auto", "effort": "medium"}
    assert first["store"] is False and first["tools"][0]["type"] == "function"
    assert first["tools"][0]["name"] == "query_health_indicators"
    assert not {"messages", "reasoning_effort", "temperature", "top_p"} & first.keys()
    inspect.signature(AsyncResponses.create).bind(None, **first)
    reasoning = {"type": "reasoning", "id": "rs_test", "summary": [{"type": "summary_text", "text": "Check the record."}],
                 "encrypted_content": "opaque-test-context"}
    call = {"name": "query_health_indicators", "args": {"keywords": ["glucose"]}, "id": "call_test"}
    assistant = AIMessage(content=[reasoning], tool_calls=[call])
    second = model._get_request_payload([question, assistant, ToolMessage("5.4 mmol/L", tool_call_id="call_test")], tools=[probe._TOOL])
    assert reasoning in second["input"]
    assert any(item.get("type") == "function_call_output" and item["call_id"] == "call_test" for item in second["input"])
    assert "previous_response_id" not in second
    inspect.signature(AsyncResponses.create).bind(None, **second)


@pytest.mark.parametrize("endpoint", ["https://api.deepseek.com/v1", "https://gateway.example/v1"])
def test_deepseek_replays_retained_assistant_reasoning_across_user_turns_with_tools(endpoint):
    entry = {"model": "deepseek-flash", "base_url": endpoint, "thinking_style": "deepseek",
             "extra_body": {"thinking": {"type": "disabled"}}}
    model = _model(entry, thinking="medium", require_thinking=True)
    messages = [HumanMessage("first"), AIMessage("answer", additional_kwargs={"reasoning_content": "Earlier reasoning"}),
                HumanMessage("follow-up")]
    payload = model._get_request_payload(messages, tools=[probe._TOOL])
    assert payload["extra_body"]["thinking"] == {"type": "enabled"}
    assert payload["reasoning_effort"] == "high"
    assert payload["messages"][1]["reasoning_content"] == "Earlier reasoning"
    no_tools = model._get_request_payload(messages)
    assert "reasoning_content" not in no_tools["messages"][1]


def test_harness_repair_does_not_discard_local_reasoning_from_the_current_turn():
    messages = [HumanMessage("question"), AIMessage("", additional_kwargs={"reasoning_content": "Need the result"}),
                HumanMessage("finish the answer", name="__empty_answer__"),
                HumanMessage("last attempt", name="__last_model_call__")]
    model = _model(LOCAL, require_thinking=True)
    payload = model._get_request_payload(messages)
    assert payload["messages"][1]["reasoning_content"] == "Need the result"
    messages.append(HumanMessage("a different question"))
    assert "reasoning_content" not in model._get_request_payload(messages)["messages"][1]


def test_openrouter_fragmented_reasoning_preserves_exact_blocks_and_never_displays_encrypted_data():
    model = _model({"model": "anthropic/claude-sonnet-5.5", "base_url": "https://openrouter.ai/api/v1"}, require_thinking=True)
    fragments = [
        [{"type": "reasoning.summary", "summary": "Read ", "id": "r0", "format": "anthropic-claude-v1", "index": 0}],
        [{"type": "reasoning.summary", "summary": "records.", "id": "r0", "format": "anthropic-claude-v1", "index": 0}],
        [{"type": "reasoning.text", "text": "", "signature": "sig-first", "id": "r1", "format": "anthropic-claude-v1", "index": 1}],
        [{"type": "reasoning.text", "signature": "sig-last", "id": "r1", "format": "anthropic-claude-v1", "index": 1}],
        [{"type": "reasoning.encrypted", "data": "opaque-first", "id": "r2", "format": "anthropic-claude-v1", "index": 2}],
        [{"type": "reasoning.encrypted", "data": "opaque-last", "id": "r2", "format": "anthropic-claude-v1", "index": 2}],
    ]
    original = deepcopy(fragments)
    merged = None
    displayed = []
    for details in fragments:
        chunk = {"choices": [{"index": 0, "delta": {"role": "assistant", "content": "", "reasoning_details": details}}]}
        message = model._convert_chunk_to_generation_chunk(chunk, AIMessageChunk, {}).message
        displayed.append(message_reasoning(message))
        merged = message if merged is None else merged + message
    tool = AIMessageChunk(content="", tool_calls=[{"name": "query_health_indicators", "args": {"keywords": ["glucose"]}, "id": "c1"}])
    assistant = message_chunk_to_message(merged + tool)
    assert "".join(displayed) == "Read records."
    assert message_reasoning(assistant) == "Read records."
    payload = model._get_request_payload([HumanMessage("question"), assistant, ToolMessage("5.4 mmol/L", tool_call_id="c1")], tools=[probe._TOOL])
    sent = payload["messages"][1]
    assert sent["reasoning_details"] == [detail for fragment in original for detail in fragment]
    assert "reasoning_content" not in sent and "_reasoning_detail_chunks" not in sent
    assert fragments == original


def test_openrouter_nonstream_summary_and_encrypted_context_take_the_same_path():
    model = _model({"model": "openai/gpt-6-luna", "base_url": "https://openrouter.ai/api/v1"}, require_thinking=True)
    details = [{"type": "reasoning.summary", "summary": "Check the record.", "index": 0},
               {"type": "reasoning.encrypted", "data": "opaque-context", "index": 1}]
    result = model._create_chat_result({"id": "completion", "model": "openai/gpt-6-luna", "created": 0,
        "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": "Answer.", "reasoning_details": details}}]})
    assistant = result.generations[0].message
    assert message_reasoning(assistant) == "Check the record."
    sent = model._get_request_payload([HumanMessage("question"), assistant])["messages"][1]
    assert sent["reasoning_details"] == details


@pytest.mark.parametrize("entry", [
    {"model": "deepseek-flash", "base_url": "https://api.deepseek.com/v1"},
    {"model": "anthropic/claude-sonnet-5.5", "base_url": "https://openrouter.ai/api/v1"},
    {"model": "gpt-6-sol", "base_url": "https://api.openai.com/v1", "use_responses_api": True, "store": False},
])
def test_genotype_guard_remains_authoritative_over_reasoning_replay(entry):
    from mirobody.agent.middleware.genotype_row_guard import GenotypeRowGuardMiddleware

    private = "SYNTHETIC_GENOTYPE_CONTEXT"
    details = [{"type": "reasoning.encrypted", "data": private, "index": 0}]
    messages = [HumanMessage("Read my genotype."),
                AIMessage("", tool_calls=[{"name": "query_genetic_data", "args": {}, "id": "g1"}]),
                ToolMessage(private, tool_call_id="g1", name="query_genetic_data"),
                AIMessage(content=[{"type": "reasoning", "id": "rs_private", "encrypted_content": private, "summary": []},
                                   {"type": "text", "text": private}],
                          additional_kwargs={"reasoning_content": private, "_reasoning_detail_chunks": [details]}),
                HumanMessage("A new question.")]
    model = _model(entry, require_thinking=True)
    model._get_request_payload(messages, tools=[probe._TOOL])
    guarded, *_ = GenotypeRowGuardMiddleware()._guard_messages(messages)
    payload = model._get_request_payload(guarded, tools=[probe._TOOL])
    assert guarded[3].additional_kwargs == {}
    assert private not in str(payload)
    assert "reasoning_details" not in str(payload) and "reasoning_content" not in str(payload)


class _StreamModel:
    def __init__(self, rounds):
        self.rounds = rounds
        self.inputs = []

    def bind_tools(self, tools):
        assert tools == [probe._TOOL]
        return self

    async def astream(self, messages):
        self.inputs.append(list(messages))
        for chunk in self.rounds[len(self.inputs) - 1]:
            yield chunk


def _tool_chunks():
    return [AIMessageChunk(content="", tool_call_chunks=[{"name": "query_health_indicators", "id": "c1",
             "args": '{"keywords":["glucose"],', "index": 0}]),
            AIMessageChunk(content="", tool_call_chunks=[{"name": None, "id": None, "args": '"days":90}', "index": 0}])]


@pytest.mark.parametrize("reasoning", ["Check the returned result.", ""])
def test_probe_reports_streamed_thinking_after_a_real_tool_round(monkeypatch, reasoning):
    final = [AIMessageChunk(content="", additional_kwargs={"reasoning_content": reasoning}), AIMessageChunk(content="5.4 mmol/L.")]
    fake = _StreamModel([_tool_chunks(), final])
    constructed = {}

    def build(entry, **kwargs):
        constructed.update(kwargs)
        return fake

    monkeypatch.setattr(clients, "build_chat_model", build)
    passed, detail = asyncio.run(probe._chat("local", entry=LOCAL))
    # A tool round without thinking still passes: the setup page saves a key
    # on this result, and adaptive models skip thinking on the short question.
    assert passed is True
    assert constructed["require_thinking"] is True
    assert len(fake.inputs) == 2
    assert fake.inputs[1][-1].type == "tool" and fake.inputs[1][-1].tool_call_id == "c1"
    assert ("first thinking" if reasoning else "no thinking was shown") in detail


def test_probe_rejects_invalid_parsed_tool_arguments_before_a_second_request(monkeypatch):
    invalid = AIMessageChunk(content="", tool_calls=[{"name": "query_health_indicators", "args": {"keywords": "glucose"}, "id": "c1"}])
    fake = _StreamModel([[invalid]])
    monkeypatch.setattr(clients, "build_chat_model", lambda *args, **kwargs: fake)
    passed, detail = asyncio.run(probe._chat("local", entry=LOCAL))
    assert not passed and "parsed arguments" in detail
    assert len(fake.inputs) == 1
