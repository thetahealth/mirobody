"""ONE key must run the whole shipped project — either gateway.

Two one-key paths, same promise: `OPENROUTER_API_KEY` (the recommended
default) or `DASHSCOPE_API_KEY` (the fallback for networks where
openrouter.ai is unreachable). Chat, vision and embeddings all follow
whichever key is present, with zero further configuration. Every piece of
that is a default that can silently drift — a chat default naming a provider
the shipped config does not define, an embedding provider with no vector
column behind it — so each is pinned here.

Reads `config.yaml` directly rather than through `Config`: this pins the
SHIPPED file, which is the thing a self-hoster gets, and it needs no env.
"""

from __future__ import annotations

import os
import pathlib

import pytest
from ruamel.yaml import YAML

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_CONFIG = _ROOT / "config.yaml"


def _shipped() -> dict:
    return YAML(typ="safe").load(_CONFIG.read_text(encoding="utf-8")) or {}


def test_both_default_chat_providers_exist_and_use_their_gateway_key():
    pytest.importorskip("langchain_core", reason="chat defaults live in the [app] extra")
    from mirobody.agent.agent import (
        _DEFAULT_PROVIDER,
        _DEFAULT_PROVIDER_FALLBACK,
    )

    deep = _shipped().get("PROVIDERS") or {}
    for default, expected_key in (
        (_DEFAULT_PROVIDER, "OPENROUTER_API_KEY"),
        (_DEFAULT_PROVIDER_FALLBACK, "DASHSCOPE_API_KEY"),
    ):
        assert default in deep, (
            f"the agent's default {default!r} is not a key of the shipped "
            f"PROVIDERS ({sorted(deep)}) — a chat call with no explicit "
            "provider raises ConfigError on an untouched config"
        )
        assert deep[default].get("api_key") == expected_key, (
            f"default {default!r} must route through {expected_key} — that is "
            "the one key its path's quickstart asks for"
        )


@pytest.fixture
def _no_config_keys(monkeypatch):
    """Config-file lookups see only os.environ (the real safe_read_cfg checks
    the environment first), so each test controls exactly which keys exist."""
    import mirobody.utils.config as cfg

    def fake(key, default=""):
        return os.environ.get(key, default)

    monkeypatch.setattr(cfg, "safe_read_cfg", fake)
    for key in ("OPENROUTER_API_KEY", "DASHSCOPE_API_KEY", "GOOGLE_API_KEY",
                "EMBEDDING_PROVIDER"):
        monkeypatch.delenv(key, raising=False)
    return fake


def test_chat_default_follows_the_available_key(_no_config_keys, monkeypatch):
    pytest.importorskip("langchain_core", reason="chat defaults live in the [app] extra")
    from mirobody.agent import agent as agent_module

    monkeypatch.setattr(agent_module, "safe_read_cfg", _no_config_keys)
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-x")
    assert agent_module._default_provider() == agent_module._DEFAULT_PROVIDER_FALLBACK

    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-y")  # openrouter outranks
    assert agent_module._default_provider() == agent_module._DEFAULT_PROVIDER


def test_embedding_provider_follows_the_available_key(_no_config_keys, monkeypatch):
    from mirobody.utils.embedding import resolve_embedding_provider

    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-x")
    assert resolve_embedding_provider() == "qwen"

    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-y")  # openrouter outranks
    assert resolve_embedding_provider() == "openrouter"

    monkeypatch.setenv("EMBEDDING_PROVIDER", "gemini")  # explicit config wins
    assert resolve_embedding_provider() == "gemini"


def test_shipped_config_leaves_embedding_provider_to_the_key():
    """A hardcoded EMBEDDING_PROVIDER would break the OTHER gateway's one-key
    promise: `openrouter` pinned means a DASHSCOPE-only deployment embeds
    against a provider whose key it does not have."""
    assert "EMBEDDING_PROVIDER" not in _shipped(), (
        "config.yaml pins EMBEDDING_PROVIDER — the shipped default must stay "
        "unset so it follows whichever gateway key exists"
    )


def test_both_gateway_paths_are_fully_wired_for_embeddings():
    from mirobody.indicator.fhir.common import (
        DIM_EMBEDDING_COLUMN,
        FHIR_EMBEDDING_COLUMN,
    )
    from mirobody.utils.embedding import EMBEDDING_MODEL_IDS, EMBEDDING_PROVIDERS

    schema = (_ROOT / "mirobody" / "schema" / "01_basedata.sql").read_text(encoding="utf-8")
    for provider in ("openrouter", "qwen"):
        assert provider in EMBEDDING_PROVIDERS, f"{provider}: no API factory"
        assert EMBEDDING_MODEL_IDS.get(provider), f"{provider}: no model id"
        for table_map in (FHIR_EMBEDDING_COLUMN, DIM_EMBEDDING_COLUMN):
            column = table_map.get(provider)
            assert column and column in schema, (
                f"{provider}: column {column!r} missing from the map or from "
                "schema/01_basedata.sql"
            )


def test_every_live_readme_hands_out_the_openai_compatible_path():
    """The README's job is to hand a visitor a working path with the actual
    place to get the key. Since 1.4.1 the path it recommends is any
    OpenAI-compatible endpoint — OpenAI or OpenRouter, each with its key page,
    or another gateway through `<PROVIDER>_BASE_URL` / `<PROVIDER>_MODEL` —
    and it must say the model has to be multimodal, because report photos and
    scanned pages go through the vision path (`<PROVIDER>_VISION_MODEL`).

    DashScope stays a fully wired fallback (`config.yaml` and the gates above
    check it); the README just no longer singles it out by link. The two frozen
    editions under `archived/` are not checked here.
    """
    for name in ("README.md", "README.zh-CN.md"):
        text = (_ROOT / name).read_text(encoding="utf-8")
        assert "openrouter.ai/keys" in text, f"{name}: no OpenRouter key link"
        assert "platform.openai.com/api-keys" in text, f"{name}: no OpenAI key link"
        for var in ("<PROVIDER>_BASE_URL", "<PROVIDER>_MODEL", "<PROVIDER>_VISION_MODEL"):
            assert var in text, f"{name}: does not name {var}"
        assert "OPENAI_API_KEY" in text and "OPENROUTER_API_KEY" in text, (
            f"{name}: does not name both OpenAI-compatible key variables"
        )


def test_a_self_hosted_base_url_override_is_honored(monkeypatch):
    """The README's self-hosting line — serve the same embedding model behind
    any OpenAI-compatible endpoint and point `OPENROUTER_BASE_URL` at it —
    must be a mechanism, not prose."""
    import mirobody.utils.config.config as cfg_mod
    from mirobody.utils.config.config import Config
    from mirobody.utils.config.llm import LLMProvider

    # Constructing a Config registers it as the process-wide singleton;
    # snapshot the current one so this test does not leak a repo-root config
    # into tests that probe "is a database configured?".
    monkeypatch.setattr(cfg_mod, "_global_config", cfg_mod._global_config)

    monkeypatch.setenv("OPENROUTER_BASE_URL", "http://vllm.internal:8000/v1")
    assert Config(yaml_filenames=str(_CONFIG)).get_llm(LLMProvider.OPENROUTER).base_url \
        == "http://vllm.internal:8000/v1"

    monkeypatch.delenv("OPENROUTER_BASE_URL")
    assert Config(yaml_filenames=str(_CONFIG)).get_llm(LLMProvider.OPENROUTER).base_url \
        == "https://openrouter.ai/api/v1", "unset override must fall back to the gateway"


def test_base_url_override_reaches_the_file_extraction_clients(monkeypatch):
    """Issue #52: the override above redirected embeddings, but the vision /
    structured-extraction clients were built from AIConfig's hardcoded table —
    an upload's OCR still called openrouter.ai and the report produced no
    indicators. Pin every OpenAI-compatible construction path to the same
    `<PROVIDER>_BASE_URL` rule, and the fallback when it is unset."""
    import mirobody.utils.config.config as cfg_mod
    from mirobody.utils.config.config import Config
    from mirobody.utils.llm.clients import AIClientManager
    from mirobody.utils.llm.file_processors.backends_openai import (
        _get_openrouter_client,
        _get_qwen_client,
    )

    monkeypatch.setattr(cfg_mod, "_global_config", cfg_mod._global_config)
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-test")
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-ds-test")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-oa-test")
    for env, url in (
        ("OPENROUTER_BASE_URL", "http://gateway.internal:8000/v1"),
        ("DASHSCOPE_BASE_URL",  "http://gateway.internal:8001/v1"),
        ("OPENAI_BASE_URL",     "http://gateway.internal:8002/v1"),
    ):
        monkeypatch.setenv(env, url)
    Config(yaml_filenames=str(_CONFIG))

    def base(client):
        return str(client.base_url).rstrip("/")

    # The exact constructors the vision path calls (fresh manager: the module
    # singleton caches clients from whatever env earlier tests left behind).
    manager = AIClientManager()
    assert base(_get_openrouter_client()) == "http://gateway.internal:8000/v1"
    assert base(_get_qwen_client()) == "http://gateway.internal:8001/v1"
    assert base(manager.get_async_dashscope_client()) == "http://gateway.internal:8001/v1"
    assert base(manager.get_async_openai_client()) == "http://gateway.internal:8002/v1", \
        "OPENAI_BASE_URL must work from config too, not only as the SDK env var"

    for env in ("OPENROUTER_BASE_URL", "DASHSCOPE_BASE_URL", "OPENAI_BASE_URL"):
        monkeypatch.delenv(env)
    manager = AIClientManager()
    assert base(_get_openrouter_client()) == "https://openrouter.ai/api/v1"
    assert base(_get_qwen_client()) == "https://dashscope.aliyuncs.com/compatible-mode/v1"
    assert base(manager.get_async_openai_client()) == "https://api.openai.com/v1", \
        "unset override must fall back to each provider's own gateway"
