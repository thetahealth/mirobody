"""LLM provider configuration, as part of the `Config` object family.

Mirrors PostgreSQLConfig / RedisConfig: values come from the loaded YAML config,
and the object hands back a ready client.

    cfg = global_config()
    llm = cfg.get_llm(LLMProvider.OPENAI)
    client = llm.get_async_client()

NOT the same thing as `mirobody/utils/llm/config.py`, despite the near-identical
path. The two coexist and overlap on openai / openrouter / dashscope /
gemini:

  utils/config/llm.py   (this file)  `LLMConfig`  — YAML-driven, 10 providers,
                                     reached through `global_config().get_llm()`.
                                     Used by `utils/embedding.py`.
  utils/llm/config.py                `AIConfig`   — a hardcoded provider table
                                     (base_url, default model, priority order)
                                     paired with `utils/llm/clients.py`'s
                                     `client_manager`. Used by everything under
                                     `utils/llm/` and by the file-processing path.

Both end up constructing an `AsyncOpenAI` with a base_url and a key from
`safe_read_cfg`, so this is genuine duplication — but the provider sets and
construction paths differ, so merging them is a behaviour change rather than a
tidy-up. Tracked in `docs/roadmap.md`; do not merge them casually, there are no
live-model tests to catch a regression.
"""

from __future__ import annotations

import logging
import os
from enum import Enum
from typing import TYPE_CHECKING, Any

import aiohttp

if TYPE_CHECKING:
    from anthropic import Anthropic, AsyncAnthropic
    from google.genai.client import AsyncClient as AsyncGenaiClient, Client as GenaiClient
    from openai import AsyncAzureOpenAI, AsyncOpenAI, AzureOpenAI, OpenAI

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class LLMProvider(str, Enum):
    OPENAI     = "openai"
    OPENROUTER = "openrouter"
    DASHSCOPE  = "dashscope"
    DEEPSEEK   = "deepseek"
    ZHIPU      = "zhipu"
    MOONSHOT   = "moonshot"
    ANTHROPIC  = "anthropic"
    GEMINI     = "gemini"
    VERTEX_AI  = "vertex_ai"
    AZURE      = "azure"
    BEDROCK    = "bedrock"

#-----------------------------------------------------------------------------

# provider → (api_key config key, default base_url)
_OPENAI_COMPAT = {
    LLMProvider.OPENAI:     ("OPENAI_API_KEY",     "https://api.openai.com/v1"),
    LLMProvider.OPENROUTER: ("OPENROUTER_API_KEY", "https://openrouter.ai/api/v1"),
    LLMProvider.DASHSCOPE:  ("DASHSCOPE_API_KEY",  "https://dashscope.aliyuncs.com/compatible-mode/v1"),
    LLMProvider.DEEPSEEK:   ("DEEPSEEK_API_KEY",   "https://api.deepseek.com/v1"),
    LLMProvider.ZHIPU:      ("ZHIPU_API_KEY",      "https://open.bigmodel.cn/api/paas/v4"),
    LLMProvider.MOONSHOT:   ("MOONSHOT_API_KEY",   "https://api.moonshot.cn/v1"),
}

#-----------------------------------------------------------------------------
# The ONE table that names a model literal.
#
# Issue #52's rule (owner, 2026-09-02): anything that names a model or an
# endpoint must be overridable from config, and a literal in code is only ever
# a fallback default living in ONE table. There were three, and the third was a
# LOCAL variable inside `utils/llm/utils.py:async_get_structured_output` —
# rebuilt on every call, impossible to inspect or test.
#
# The two that carried model ids also disagreed on what a provider is CALLED:
# vision said `qwen` where everything else said `dashscope`, and both derive
# their override key from that name — so the uniform `<PROVIDER>_MODEL` /
# `<PROVIDER>_VISION_MODEL` scheme `config.yaml` documents was false for
# Alibaba: structured read `DASHSCOPE_MODEL`, vision read `QWEN_VISION_MODEL`.
# The canonical name is the `LLMProvider` value everywhere now, with `qwen`
# kept as an alias so a deployment that set the old key does not lose it.
#
#: provider → (api key config key, chat/structured default, vision default)
#: A `None` vision default means the provider has no vision path here.
_PROVIDER_DEFAULTS: dict[LLMProvider, tuple[str, str, str | None]] = {
    LLMProvider.GEMINI:     ("GOOGLE_API_KEY",     "gemini-3.8-flash",         "gemini-3.8-flash"),
    # gpt-5.6-terra reads images as well as text, so an OPENAI_API_KEY-only
    # deployment has a vision path instead of a hole where one should be.
    LLMProvider.OPENAI:     ("OPENAI_API_KEY",     "gpt-5.6-terra",            "gpt-5.6-terra"),
    LLMProvider.OPENROUTER: ("OPENROUTER_API_KEY", "google/gemini-3.8-flash",  "google/gemini-3.8-flash"),
    # qwen3.5-flash, not qwen-flash: the latter reaches DashScope's legacy
    # backend, which rejects function results (config.yaml, verified 2026-08-23).
    LLMProvider.DASHSCOPE:  ("DASHSCOPE_API_KEY",  "qwen3.5-flash",            "qwen3-vl-flash"),
}

#: `QWEN_VISION_MODEL` was the key vision used before the tables agreed on the
#: enum value; kept so a deployment that set it does not lose it silently.
_PROVIDER_ALIASES: dict[str, LLMProvider] = {
    "qwen": LLMProvider.DASHSCOPE,
}


#: Env names that mean the same key. Google's own docs and SDK say
#: `GEMINI_API_KEY`; this project's tables say `GOOGLE_API_KEY`, and a key
#: pasted under the name its vendor documents must not read as "no key".
_KEY_ALIASES: dict[str, tuple[str, ...]] = {
    "GOOGLE_API_KEY": ("GOOGLE_API_KEY", "GEMINI_API_KEY"),
}


def read_api_key(env_name: str) -> str:
    """The value of `env_name`, or of any name that means the same thing.

    Every surface that decides "is this provider usable" goes through here, so
    the four one-key paths agree on what counts as a key being present.
    """
    from . import safe_read_cfg

    for name in _KEY_ALIASES.get(env_name, (env_name,)):
        if value := (safe_read_cfg(name, "") or "").strip():
            return value
    return ""


def canonical_provider(name: str) -> LLMProvider | None:
    """The `LLMProvider` a name or alias refers to, or None."""
    key = (name or "").strip().lower()
    if not key:
        return None
    try:
        return LLMProvider(key)
    except ValueError:
        return _PROVIDER_ALIASES.get(key)


def provider_model(provider: LLMProvider | str, *, vision: bool = False) -> str | None:
    """The model id for `provider`, config first and the table's default last.

    Reads `<PROVIDER>_VISION_MODEL` / `<PROVIDER>_MODEL` under the CANONICAL
    name, then under any alias, so both spellings of the key keep working.
    """
    from .config import safe_read_cfg

    canon = provider if isinstance(provider, LLMProvider) else canonical_provider(provider)
    if canon is None or canon not in _PROVIDER_DEFAULTS:
        return None
    suffix = "_VISION_MODEL" if vision else "_MODEL"
    names = [canon.value] + [a for a, c in _PROVIDER_ALIASES.items() if c is canon]
    for name in names:
        if configured := safe_read_cfg(f"{name.upper()}{suffix}"):
            return configured
    _, chat_default, vision_default = _PROVIDER_DEFAULTS[canon]
    return vision_default if vision else chat_default


def provider_api_key_env(provider: LLMProvider | str) -> str | None:
    """The config key holding this provider's credential."""
    canon = provider if isinstance(provider, LLMProvider) else canonical_provider(provider)
    return _PROVIDER_DEFAULTS[canon][0] if canon in _PROVIDER_DEFAULTS else None

#-----------------------------------------------------------------------------

class LLMConfig:
    """
    Single LLM provider's configuration. Created by ``Config.get_llm()``.

    Holds credentials and endpoints read from configuration;
    ``get_client()`` / ``get_async_client()`` lazily create and cache
    the native SDK client.
    """

    _GEMINI_BASE = "https://generativelanguage.googleapis.com"

    def __init__(
        self,
        provider    : LLMProvider,
        *,
        api_key     : str = "",
        base_url    : str = "",
        # Azure
        endpoint    : str = "",
        deployment  : str = "gpt-4o",
        api_version : str = "2024-12-01-preview",
        # Gemini
        gemini_api_version: str = "v1beta",
        # GCP
        gcp_project : str = "",
        gcp_location: str = "",
        # AWS
        aws_region  : str = "",
    ):
        self.provider     = provider
        self.api_key      = api_key
        self.base_url     = base_url
        self.endpoint     = endpoint
        self.deployment   = deployment
        self.api_version  = api_version
        self.gcp_project  = gcp_project
        self.gcp_location = gcp_location
        self.aws_region   = aws_region

        # Gemini / Vertex AI: derive base_url if not explicitly set
        self.gemini_api_version = gemini_api_version
        if provider == LLMProvider.GEMINI and not base_url:
            self.base_url = f"{self._GEMINI_BASE}/{gemini_api_version}"
        elif provider == LLMProvider.VERTEX_AI and not base_url:
            self.base_url = (
                f"https://{gcp_location}-aiplatform.googleapis.com/v1"
                f"/projects/{gcp_project}/locations/{gcp_location}"
            )

        self._client: Any = None
        self._async_client: Any = None

    #-------------------------------------------------

    def print(self):
        if self.provider in _OPENAI_COMPAT:
            print(f"llm             : {self.provider.value}  base_url={self.base_url}")
        elif self.provider == LLMProvider.AZURE:
            print(f"llm             : azure  endpoint={self.endpoint}  deployment={self.deployment}")
        elif self.provider == LLMProvider.VERTEX_AI:
            print(f"llm             : vertex_ai  project={self.gcp_project}  location={self.gcp_location}")
        elif self.provider == LLMProvider.BEDROCK:
            print(f"llm             : bedrock  region={self.aws_region}")
        else:
            print(f"llm             : {self.provider.value}")

    #-------------------------------------------------
    # aiohttp session
    #-------------------------------------------------

    def get_aiohttp_session(self, **kwargs) -> aiohttp.ClientSession:
        """Create an aiohttp.ClientSession with base_url and auth headers pre-configured.

        Caller is responsible for closing the session (use ``async with``).
        Extra *kwargs* are forwarded to ``aiohttp.ClientSession()``.
        """
        headers = kwargs.pop("headers", {})
        headers.setdefault("Content-Type", "application/json")

        p = self.provider
        if p in _OPENAI_COMPAT or p == LLMProvider.AZURE:
            headers.setdefault("Authorization", f"Bearer {self.api_key}")
        elif p == LLMProvider.ANTHROPIC:
            headers.setdefault("x-api-key", self.api_key)
            headers.setdefault("anthropic-version", "2023-06-01")
        elif p == LLMProvider.GEMINI:
            headers.setdefault("x-goog-api-key", self.api_key)
        elif p == LLMProvider.VERTEX_AI:
            import google.auth
            import google.auth.transport.requests

            creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            creds.refresh(google.auth.transport.requests.Request())
            headers.setdefault("Authorization", f"Bearer {creds.token}")

        base_url = self.base_url.rstrip("/") + "/"
        return aiohttp.ClientSession(
            base_url=base_url,
            headers=headers,
            **kwargs,
        )

    #-------------------------------------------------
    # Client builders (lazy, cached per instance)
    #-------------------------------------------------

    def get_client(self) -> OpenAI | Anthropic | GenaiClient | AzureOpenAI:
        if self._client is None:
            self._client = self._build(sync=True)
        return self._client

    def get_async_client(self) -> AsyncOpenAI | AsyncAnthropic | AsyncGenaiClient | AsyncAzureOpenAI:
        if self._async_client is None:
            self._async_client = self._build(sync=False)
        return self._async_client

    #-------------------------------------------------

    def _build(self, *, sync: bool) -> Any:
        p = self.provider

        if p in _OPENAI_COMPAT:
            return self._build_openai_compat(sync=sync)
        if p == LLMProvider.ANTHROPIC:
            return self._build_anthropic(sync=sync)
        if p == LLMProvider.GEMINI:
            return self._build_gemini(sync=sync)
        if p == LLMProvider.VERTEX_AI:
            return self._build_vertex_ai(sync=sync)
        if p == LLMProvider.AZURE:
            return self._build_azure(sync=sync)
        if p == LLMProvider.BEDROCK:
            return self._build_bedrock(sync=sync)

        raise ValueError(f"Unsupported provider: {p!r}")

    #-------------------------------------------------

    def _build_openai_compat(self, *, sync: bool) -> OpenAI | AsyncOpenAI:
        from openai import AsyncOpenAI, OpenAI
        cls = OpenAI if sync else AsyncOpenAI
        return cls(api_key=self.api_key, base_url=self.base_url)

    def _build_anthropic(self, *, sync: bool) -> Anthropic | AsyncAnthropic:
        import anthropic
        cls = anthropic.Anthropic if sync else anthropic.AsyncAnthropic
        return cls(api_key=self.api_key)

    def _build_gemini(self, *, sync: bool) -> GenaiClient | AsyncGenaiClient:
        from google import genai
        client = genai.Client(api_key=self.api_key, vertexai=False)
        return client if sync else client.aio

    def _build_vertex_ai(self, *, sync: bool) -> GenaiClient | AsyncGenaiClient:
        from google import genai
        if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
            raise ValueError(
                "GOOGLE_CLOUD_PROJECT not set. "
                "Configure GCP_PROJECT in YAML and call export_to_env() at startup."
            )
        client = genai.Client()
        return client if sync else client.aio

    def _build_azure(self, *, sync: bool) -> AzureOpenAI | AsyncAzureOpenAI:
        from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        from openai import AsyncAzureOpenAI, AzureOpenAI

        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
        )
        cls = AzureOpenAI if sync else AsyncAzureOpenAI
        return cls(
            azure_ad_token_provider=token_provider,
            azure_endpoint=self.endpoint,
            azure_deployment=self.deployment,
            api_version=self.api_version,
        )

    def _build_bedrock(self, *, sync: bool) -> Any:
        if sync:
            import boto3
            return boto3.client("bedrock-runtime", region_name=self.aws_region)
        import aioboto3
        return aioboto3.Session().client("bedrock-runtime", region_name=self.aws_region)
