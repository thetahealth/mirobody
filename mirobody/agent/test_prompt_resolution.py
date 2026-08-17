"""How each agent arrives at its system prompt.

Three behaviours pinned here, all three of them regressions waiting to happen
because each was wrong in a way that produced no error:

1. A user's saved instructions are APPENDED to the agent's prompt. They used to
   REPLACE it — `_get_base_prompt` consulted the shipped template only
   `if not base_prompt` — so saving "answer in bullet points" silently threw
   away the lab-report workflow and the no-diagnosis framing with it.
2. `/api/prompts` answers for the agent it was ASKED about. It used to hardcode
   `get_options_for_agent("deep")`, so a Base session was offered a prompt
   describing a virtual filesystem and chart tools BaseAgent does not have.
3. BaseAgent honours `PROMPTS_BASE`. The key shipped in config.yaml doing
   nothing, because the agent read its packaged template directly.
"""

from __future__ import annotations

import pytest

from mirobody.agent import base_agent as ba


#--- 1. DeepAgent composes template + user instructions -----------------------


class _FakeDeep:
    """Just enough DeepAgent to exercise `_get_base_prompt` unbound.

    Instantiating the real one needs an LLM client, a tool registry and a DB;
    the method under test only touches `self.prompt_templates`.
    """

    def __init__(self, templates):
        self.prompt_templates = templates

    _USER_INSTRUCTIONS_HEADER = None  # filled from the real class below


@pytest.fixture
def deep_get_base_prompt(monkeypatch):
    from mirobody.agent.deep_agent import DeepAgent

    _FakeDeep._USER_INSTRUCTIONS_HEADER = DeepAgent._USER_INSTRUCTIONS_HEADER

    def _run(templates, user_prompt, err=None, prompt_name="deep"):
        import asyncio

        async def fake_lookup(user_id, name):
            return user_prompt, err

        monkeypatch.setattr(
            "mirobody.agent.chat.user_config.get_user_prompt_by_name", fake_lookup
        )
        agent = _FakeDeep(templates)
        return asyncio.run(DeepAgent._get_base_prompt(agent, "u1", prompt_name))

    return _run


def test_user_instructions_are_appended_not_substituted(deep_get_base_prompt):
    out = deep_get_base_prompt({"deep": "SHIPPED WORKFLOW"}, "use bullet points")
    assert "SHIPPED WORKFLOW" in out, "the agent's own prompt must survive"
    assert "use bullet points" in out
    assert out.index("SHIPPED WORKFLOW") < out.index("use bullet points")


def test_appended_instructions_are_labelled_as_the_persons_own(deep_get_base_prompt):
    out = deep_get_base_prompt({"deep": "SHIPPED"}, "be terse")
    # The model must be able to tell the two halves apart, and be told which
    # one wins — otherwise "be terse" competes with the no-diagnosis rule.
    assert "person" in out.lower()
    assert "win" in out.lower() or "apply" in out.lower()


def test_no_user_prompt_leaves_the_template_untouched(deep_get_base_prompt):
    out = deep_get_base_prompt({"deep": "SHIPPED"}, "")
    assert out == "SHIPPED"


def test_named_template_wins_over_first(deep_get_base_prompt):
    out = deep_get_base_prompt({"a": "FIRST", "deep": "NAMED"}, "", prompt_name="deep")
    assert out == "NAMED"


def test_unknown_name_falls_back_to_first_template(deep_get_base_prompt):
    out = deep_get_base_prompt({"a": "FIRST", "b": "SECOND"}, "", prompt_name="nope")
    assert out == "FIRST"


def test_a_lookup_error_does_not_lose_the_agent_prompt(deep_get_base_prompt):
    out = deep_get_base_prompt({"deep": "SHIPPED"}, None, err="redis down")
    assert out == "SHIPPED"


def test_no_template_at_all_raises(deep_get_base_prompt):
    from mirobody.agent.deep_agent import DeepAgentError

    with pytest.raises(DeepAgentError):
        deep_get_base_prompt({}, "")


def test_no_template_but_user_prompt_runs_on_it_alone(deep_get_base_prompt):
    """Degraded, not dead — but it means the deployment shipped no template."""
    out = deep_get_base_prompt({}, "only this")
    assert out == "only this"


#--- 2. /api/prompts answers for the agent it was asked about -----------------


def _call_prompt_handler(monkeypatch, query: str, options_by_agent: dict):
    """Drive `prompt_handler` with fakes for its three collaborators."""
    import asyncio
    import json

    from starlette.datastructures import QueryParams

    from mirobody.agent.chat import service as svc

    class _Cfg2:
        def get_options_for_agent(self, name):
            return options_by_agent.get(name, {})

    class _Req:
        query_params = QueryParams(query)
        method = "GET"          # the @public decorator answers OPTIONS first

    class _Validator:
        def verify_http_token(self, request):
            return None, "no token"          # anonymous: system prompts only

    class _Svc:
        _token_validator = _Validator()

    monkeypatch.setattr(svc, "global_config", lambda: _Cfg2())
    monkeypatch.setattr(svc, "json_response_with_code", lambda data, request: data)

    return asyncio.run(svc.ChatService.prompt_handler(_Svc(), _Req()))


def test_prompts_endpoint_answers_for_the_requested_agent(monkeypatch):
    out = _call_prompt_handler(
        monkeypatch,
        "agent=base",
        {"deep": {"prompt_templates": {"deep": "..."}},
         "base": {"prompt_templates": {"base": "..."}}},
    )
    assert out["agent"] == "base"
    assert [p["name"] for p in out["system"]] == ["base"]


def test_prompts_endpoint_defaults_to_deep(monkeypatch):
    out = _call_prompt_handler(
        monkeypatch, "", {"deep": {"prompt_templates": {"deep": "..."}}}
    )
    assert out["agent"] == "deep"
    assert [p["name"] for p in out["system"]] == ["deep"]


def test_an_agent_with_no_templates_gets_an_empty_list_not_deeps(monkeypatch):
    """The old bug: Base was handed Deep's prompt list. Empty is the honest
    answer, and is what tells a client there is nothing to pick."""
    out = _call_prompt_handler(
        monkeypatch,
        "agent=base",
        {"deep": {"prompt_templates": {"deep": "..."}}, "base": {}},
    )
    assert out["agent"] == "base"
    assert out["system"] == []


#--- 3. BaseAgent honours PROMPTS_BASE ----------------------------------------


class _Cfg:
    def __init__(self, options):
        self._options = options

    def get_options_for_agent(self, name):
        return self._options.get(name, {})


def test_base_agent_uses_the_packaged_template_when_config_is_empty(monkeypatch):
    monkeypatch.setattr(ba, "global_config", lambda: _Cfg({"base": {"prompt_templates": {}}}))
    assert ba._resolve_prompt_template("Base") == ba._load_base_prompt_template()


def test_base_agent_uses_a_configured_template_when_one_is_set(monkeypatch):
    monkeypatch.setattr(
        ba, "global_config", lambda: _Cfg({"base": {"prompt_templates": {"x": "CONFIGURED"}}})
    )
    assert ba._resolve_prompt_template("Base") == "CONFIGURED"


def test_base_agent_survives_config_not_being_initialised(monkeypatch):
    """The engine imports without Config.init(); this must not explode."""
    monkeypatch.setattr(ba, "global_config", lambda: None)
    assert ba._resolve_prompt_template("Base") == ba._load_base_prompt_template()


def test_base_agent_skips_an_empty_configured_entry(monkeypatch):
    monkeypatch.setattr(
        ba, "global_config", lambda: _Cfg({"base": {"prompt_templates": {"x": "", "y": "REAL"}}})
    )
    assert ba._resolve_prompt_template("Base") == "REAL"
