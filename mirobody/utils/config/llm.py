"""
LLM provider configuration — mirrors the PostgreSQLConfig / RedisConfig pattern.

Usage::

    cfg = global_config()
    llm = cfg.get_llm(LLMProvider.OPENAI)
    client = llm.get_async_client()
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
    VOLCENGINE = "volcengine"
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
    LLMProvider.VOLCENGINE: ("VOLCENGINE_API_KEY", "https://ark.cn-beijing.volces.com/api/v3"),
    LLMProvider.DEEPSEEK:   ("DEEPSEEK_API_KEY",   "https://api.deepseek.com/v1"),
    LLMProvider.ZHIPU:      ("ZHIPU_API_KEY",      "https://open.bigmodel.cn/api/paas/v4"),
    LLMProvider.MOONSHOT:   ("MOONSHOT_API_KEY",   "https://api.moonshot.cn/v1"),
}

#-----------------------------------------------------------------------------

class LLMConfig:
    """
    Single LLM provider's configuration. Created by ``Config.get_llm()``.

    Holds credentials and endpoints read from the config center;
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
        else:
            import aioboto3
            return aioboto3.Session().client("bedrock-runtime", region_name=self.aws_region)
