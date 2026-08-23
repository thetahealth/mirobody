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
    pytest.importorskip("langchain_core", reason="chat defaults live in the [agents] extra")
    from mirobody.agent.deep_agent import (
        _DEFAULT_PROVIDER_DEEP,
        _DEFAULT_PROVIDER_DEEP_FALLBACK,
    )

    deep = _shipped().get("PROVIDERS_DEEP") or {}
    for default, expected_key in (
        (_DEFAULT_PROVIDER_DEEP, "OPENROUTER_API_KEY"),
        (_DEFAULT_PROVIDER_DEEP_FALLBACK, "DASHSCOPE_API_KEY"),
    ):
        assert default in deep, (
            f"DeepAgent's default {default!r} is not a key of the shipped "
            f"PROVIDERS_DEEP ({sorted(deep)}) — a chat call with no explicit "
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
    fake = lambda key, default="": os.environ.get(key, default)
    monkeypatch.setattr(cfg, "safe_read_cfg", fake)
    for key in ("OPENROUTER_API_KEY", "DASHSCOPE_API_KEY", "GOOGLE_API_KEY",
                "EMBEDDING_PROVIDER"):
        monkeypatch.delenv(key, raising=False)
    return fake


def test_chat_default_follows_the_available_key(_no_config_keys, monkeypatch):
    pytest.importorskip("langchain_core", reason="chat defaults live in the [agents] extra")
    from mirobody.agent import deep_agent

    monkeypatch.setattr(deep_agent, "safe_read_cfg", _no_config_keys)
    monkeypatch.setenv("DASHSCOPE_API_KEY", "sk-x")
    assert deep_agent._default_provider() == deep_agent._DEFAULT_PROVIDER_DEEP_FALLBACK

    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-y")  # openrouter outranks
    assert deep_agent._default_provider() == deep_agent._DEFAULT_PROVIDER_DEEP


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


def test_every_readme_hands_out_both_key_links():
    """The README's job is to hand a visitor a working path — OpenRouter
    first, DashScope when openrouter.ai is unreachable — with the actual
    place to get the key."""
    for name in ("README.md", "README.zh-CN.md", "README.zh-TW.md", "README.ja.md"):
        text = (_ROOT / name).read_text(encoding="utf-8")
        assert "openrouter.ai/keys" in text, f"{name}: no OpenRouter key link"
        assert "dashscope.console.aliyun.com/apiKey" in text, (
            f"{name}: no DashScope key link — the fallback path is a dead end"
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
