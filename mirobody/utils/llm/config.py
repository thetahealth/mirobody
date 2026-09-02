"""Provider catalogue for the `utils/llm/` package.

Two tables, with two distinct jobs and no overlap:

* `_CONFIG` — the OpenAI-compatible providers and where they live. It answers
  "which key, which endpoint" for `clients.py`, and `get_provider_config` is
  the ONE place `<PROVIDER>_BASE_URL` is applied. Gemini is absent on purpose:
  it is reached through the google-genai SDK, not a base_url.
* `_DEFAULT_PROVIDER_PRIORITY` — auto-selection order and the fallback default
  model per provider, for callers that were handed no model name.

A URL or model id may appear in these tables and nowhere else. A call site that
builds an SDK client with its own literal is how issue #52 happened: vision
extraction hardcoded openrouter.ai, so a deployment that redirected OpenRouter
with OPENROUTER_BASE_URL got its text requests proxied and its image requests
401'd against the public gateway.

NOT the same thing as `mirobody/utils/config/llm.py`, despite the near-identical
path — that one is `LLMConfig`, the YAML-driven member of the `Config` family.
See its docstring for the full comparison and why the two have not been merged.
"""

import os
from typing import Any, Dict, List, Optional

from mirobody.utils.config import safe_read_cfg


def _vertex_ai_enabled() -> bool:
    return os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "0").lower() in ("true", "1")


class AIConfig:
    """AI model configuration manager"""

    # Used for auto-selecting available providers
    _DEFAULT_PROVIDER_PRIORITY: List[Dict[str, Any]] = [
        {
            "name": "openai",
            "api_key_env": "OPENAI_API_KEY",
            "default_model": "gpt-5.2",
            "description": "OpenAI GPT Models",
        },
        {
            "name": "openrouter",
            "api_key_env": "OPENROUTER_API_KEY",
            "default_model": "google/gemini-3-flash-preview",
            "description": "OpenRouter (Multi-model Gateway)",
        },
        {
            "name": "gemini",
            "api_key_env": "GOOGLE_API_KEY",
            # default_model is resolved at read time via _resolve_provider();
            # Vertex AI backend doesn't yet serve gemini-3-flash-preview.
            "default_model": "gemini-3-flash-preview",
            "vertex_default_model": "gemini-2.5-flash",
            "description": "Google Gemini",
        },
        {
            "name": "volcengine",
            "api_key_env": "VOLCENGINE_API_KEY",
            "default_model": "doubao-seed-1-8-251228",
            "description": "Volcengine Doubao Seed 1.8",
        },
        {
            "name": "dashscope",
            "api_key_env": "DASHSCOPE_API_KEY",
            "default_model": "qwen-flash",
            "description": "Aliyun DashScope (Qwen Flash)",
        },
    ]
    # `claude` used to sit third in this list, and an ANTHROPIC_API_KEY-only
    # deployment therefore auto-selected it — into a dead end: the claude arm of
    # async_get_text_completion logged "not supported yet" and returned None
    # WITHOUT trying the next available provider, and async_get_structured_output
    # has no claude arm at all. Removing it is what makes such a deployment fall
    # through to a provider that answers. Chat is unaffected: the agent reaches
    # Claude through config.yaml's PROVIDERS_DEEP, not this table.

    # Where each OpenAI-compatible provider lives, when nothing overrides it.
    _CONFIG: Dict[str, Dict[str, str]] = {
        "openai": {
            "api_key_env": "OPENAI_API_KEY",
            "api_base": "https://api.openai.com/v1",
        },
        "openrouter": {
            "api_key_env": "OPENROUTER_API_KEY",
            "api_base": "https://openrouter.ai/api/v1",
        },
        "dashscope": {
            "api_key_env": "DASHSCOPE_API_KEY",
            "api_base": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        },
        # Ark speaks chat/completions; its Coding and Agent plans are served
        # from /api/coding/v3 and /api/plan/v3, which is why the URL below is a
        # default and not a constant.
        "volcengine": {
            "api_key_env": "VOLCENGINE_API_KEY",
            "api_base": "https://ark.cn-beijing.volces.com/api/v3",
        },
    }

    @classmethod
    def get_provider_config(cls, provider: str) -> Dict[str, Any]:
        """Get configuration for specified provider"""
        if provider not in cls._CONFIG:
            raise ValueError(f"Unsupported AI provider: {provider}")

        config = cls._CONFIG[provider].copy()
        # Dynamically get API key
        config["api_key"] = safe_read_cfg(config["api_key_env"])
        # `<PROVIDER>_BASE_URL` redirects the provider to any OpenAI-compatible
        # endpoint — the rule Config.get_llm() already applies and config.yaml
        # documents. Issue #52: embeddings honored OPENROUTER_BASE_URL while
        # file extraction, built from this table, still called openrouter.ai,
        # so a self-hosted gateway got the text requests but never the vision
        # ones. `or` (not a default arg) so an empty env var also falls back.
        config["api_base"] = (
            safe_read_cfg(config["api_key_env"].replace("_API_KEY", "_BASE_URL"))
            or config["api_base"]
        )
        return config

    # ========== Auto-select provider methods ==========

    @staticmethod
    def _resolve_provider(provider: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of the priority entry with default_model resolved against current env."""
        if provider["name"] == "gemini" and _vertex_ai_enabled():
            return {**provider, "default_model": provider["vertex_default_model"]}
        return provider

    @classmethod
    def get_available_provider(cls) -> Optional[Dict[str, Any]]:
        """
        Get first available provider (based on configured API keys)

        Priority: openai > openrouter > gemini > volcengine > dashscope

        Returns:
            Provider config dict with name, api_key_env, default_model, description
            Returns None if no provider is available
        """
        for provider in cls._DEFAULT_PROVIDER_PRIORITY:
            api_key = safe_read_cfg(provider["api_key_env"])
            if api_key:
                return cls._resolve_provider(provider)
        return None

    @classmethod
    def get_provider_by_priority_name(cls, name: str) -> Optional[Dict[str, Any]]:
        """
        Get provider config by name from priority list

        Args:
            name: Provider name (openai/openrouter/gemini/volcengine/dashscope)

        Returns:
            Provider config dict
        """
        for provider in cls._DEFAULT_PROVIDER_PRIORITY:
            if provider["name"] == name:
                return cls._resolve_provider(provider)
        return None

    @classmethod
    def get_provider_status(cls) -> Dict[str, bool]:
        """
        Get configuration status of all providers

        Returns:
            Mapping of provider names to availability status
        """
        return {
            provider["name"]: bool(safe_read_cfg(provider["api_key_env"]))
            for provider in cls._DEFAULT_PROVIDER_PRIORITY
        }
