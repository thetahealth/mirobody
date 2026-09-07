
import logging
import os
from typing import Any

from google import genai
from openai import AsyncOpenAI, OpenAI

from .config import AIConfig
from ...utils import safe_read_cfg

logger = logging.getLogger(__name__)

class AIClientManager:
    """AI client manager"""

    def __init__(self):
        self._clients: dict[str, Any] = {}
        self._async_clients: dict[str, Any] = {}
        self._initialized = False

    def _initialize_clients(self):
        """Lazy initialize all clients"""
        if self._initialized:
            return
        
        dashscope_config = AIConfig.get_provider_config("dashscope")
        if dashscope_config["api_key"]:
            self._async_clients["dashscope"] = AsyncOpenAI(
                api_key=dashscope_config["api_key"], base_url=dashscope_config["api_base"]
            )

        # OpenAI client, through the same table as everyone else. It used to
        # read the two keys itself, which had a bug of its own — the bare
        # constructor honours only the OPENAI_BASE_URL *environment variable*,
        # so an override set in config.yaml alone was silently ignored — and,
        # more to the point, a second place where a provider's endpoint was
        # decided is exactly how #52 came about.
        openai_config = AIConfig.get_provider_config("openai")
        if openai_config["api_key"]:
            self._async_clients["openai"] = AsyncOpenAI(
                api_key=openai_config["api_key"], base_url=openai_config["api_base"]
            )

        # Google Gemini client — lock to AI Studio backend; without vertexai=False,
        # GOOGLE_GENAI_USE_VERTEXAI=true in env reroutes requests to aiplatform with
        # API-key auth, which Vertex rejects (401 UNAUTHENTICATED).
        google_api_key = safe_read_cfg("GOOGLE_API_KEY")
        if google_api_key:
            try:
                self._async_clients["gemini"] = genai.Client(api_key=google_api_key, vertexai=False).aio
            except Exception as e:
                logger.warning(f"Failed to initialize Gemini client, skipping: {e}")

        self._initialized = True

    def get_async_client(self, provider: str) -> Any:
        """Get asynchronous client"""
        self._initialize_clients()

        client_mapping = {
            "openai": "openai",
            "gemini": "gemini",
            "dashscope": "dashscope",  # similar to openai but a different url
        }

        client_key = client_mapping.get(provider)
        if not client_key or client_key not in self._async_clients:
            raise ValueError(f"Unsupported async client: {provider}")

        return self._async_clients[client_key]

    def get_ai_client(self, provider: str) -> OpenAI:
        """Create AI client for specified provider (OpenAI-compatible).

        No caller today — every LLM path in the repo is async. Kept because it
        is the sync entry point a future call site must use: the alternative it
        replaces is `OpenAI(base_url="https://...")` written inline, which is
        precisely how #52's hardcoded openrouter.ai got there.
        """
        config = AIConfig.get_provider_config(provider)

        return OpenAI(api_key=config["api_key"], base_url=config["api_base"])

    def get_async_ai_client(self, provider: str) -> AsyncOpenAI:
        """Create async AI client for specified provider (OpenAI-compatible)"""
        config = AIConfig.get_provider_config(provider)

        return AsyncOpenAI(api_key=config["api_key"], base_url=config["api_base"])

    def get_async_openai_client(self) -> AsyncOpenAI:
        """Get async OpenAI client"""
        return self.get_async_client("openai")
    
    def get_async_dashscope_client(self) -> AsyncOpenAI:
        """Get async DashScope client"""
        return self.get_async_client("dashscope")

    @staticmethod
    def _use_vertex() -> bool:
        return os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "0").lower() in ("true", "1")

    def get_async_gemini_client(self):
        """Get async Gemini client (auto-routes to Vertex when GOOGLE_GENAI_USE_VERTEXAI=true)."""
        if self._use_vertex():
            return self.get_async_vertex_gemini_client()
        return self.get_async_client("gemini")

    def get_vertex_gemini_client(self) -> genai.Client:
        """Get Vertex AI Gemini client.

        Requires export_to_env() called at startup, which sets:
          GOOGLE_GENAI_USE_VERTEXAI=true  (auto-selects Vertex backend)
          GOOGLE_CLOUD_PROJECT            (GCP project)
          GOOGLE_CLOUD_LOCATION           (GCP region)

        The google-genai SDK reads these env vars natively.
        """
        if "vertex_gemini" not in self._clients:
            if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
                raise ValueError(
                    "GOOGLE_CLOUD_PROJECT not set. "
                    "Configure GCP_PROJECT in YAML and call export_to_env() at startup."
                )
            self._clients["vertex_gemini"] = genai.Client()
        return self._clients["vertex_gemini"]

    def get_async_vertex_gemini_client(self):
        """Get async Vertex AI Gemini client."""
        if "vertex_gemini" not in self._async_clients:
            client = self.get_vertex_gemini_client()
            self._async_clients["vertex_gemini"] = client.aio
        return self._async_clients["vertex_gemini"]


# The one client entry point. Everything below it used to be a second,
# never-wired one: a `_GlobalClients` lazy holder (whose `client_manager`
# property built a SECOND AIClientManager, duplicating this singleton), three
# module globals `openai_client`/`async_openai_client`/`gemini_client`
# initialised to None, an `init_clients()` that filled them — and no caller
# anywhere that ever invoked it. So the globals stayed None for the life of
# every process, and `utils/llm/__init__` re-exported `openai_client` in its
# `__all__` regardless: a documented public name whose value was permanently
# None. A module-level `get_ai_client()` wrapper went the same way; the
# METHOD of the same name on AIClientManager is live and stays.
#
# The class shrank the same way: every SYNC accessor (`get_client`,
# `get_openai_client`, `get_dashscope_client`, `get_gemini_client`) had zero
# callers — the whole repo's LLM traffic is async — as did `is_client_available`
# and `health_check`, and `get_client`'s mapping keyed MODEL names ("gpt-4o",
# "gpt-4.1") onto the openai client, so it carried stale model ids in code for
# no one. Only `get_ai_client` stays sync, as the entry point a future sync
# call site must use instead of constructing its own client.
client_manager = AIClientManager()