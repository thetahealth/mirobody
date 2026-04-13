
import logging
import os
from typing import Any, Dict

from google import genai
from openai import AsyncOpenAI, OpenAI

from .config import AIConfig
from ...utils import safe_read_cfg

logger = logging.getLogger(__name__)

class AIClientManager:
    """AI client manager"""

    def __init__(self):
        self._clients: Dict[str, Any] = {}
        self._async_clients: Dict[str, Any] = {}
        self._initialized = False

    def _initialize_clients(self):
        """Lazy initialize all clients"""
        if self._initialized:
            return
        
        dashscope_api_key = safe_read_cfg("DASHSCOPE_API_KEY")
        if dashscope_api_key:
            self._clients["dashscope"] = OpenAI(api_key=dashscope_api_key,base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")
            self._async_clients["dashscope"] = AsyncOpenAI(api_key=dashscope_api_key,base_url="https://dashscope.aliyuncs.com/compatible-mode/v1")

        # OpenAI client
        openai_api_key = safe_read_cfg("OPENAI_API_KEY")
        if openai_api_key:
            self._clients["openai"] = OpenAI(api_key=openai_api_key)
            self._async_clients["openai"] = AsyncOpenAI(api_key=openai_api_key)

        # Google Gemini client
        google_api_key = safe_read_cfg("GOOGLE_API_KEY")
        if google_api_key:
            try:
                self._clients["gemini"] = genai.Client(api_key=google_api_key)
                self._async_clients["gemini"] = genai.Client(api_key=google_api_key).aio
            except Exception as e:
                import logging
                logging.warning(f"Failed to initialize Gemini client, skipping: {e}")

        self._initialized = True

    def get_client(self, provider: str) -> Any:
        """Get synchronous client"""
        self._initialize_clients()

        # For volcengine/doubao, use dynamically created OpenAI-compatible client
        if provider in ["volcengine", "doubao-lite"]:
            return self.get_ai_client(provider)

        client_mapping = {
            "openai": "openai",
            "gpt-4o": "openai",
            "gpt-4.1": "openai",
            "gpt-o3": "openai",
            "gpt4o-mini": "openai",
            "gemini": "gemini",
            "dashscope": "dashscope", # similar to openai but use different url
        }

        client_key = client_mapping.get(provider)
        if not client_key or client_key not in self._clients:
            raise ValueError(f"Unsupported client: {provider}")

        return self._clients[client_key]

    def get_async_client(self, provider: str) -> Any:
        """Get asynchronous client"""
        self._initialize_clients()

        client_mapping = {
            "openai": "openai",
            "gpt-4o": "openai",
            "gpt-4.1": "openai",
            "gpt-o3": "openai",
            "gpt4o-mini": "openai",
            "gemini": "gemini",
            "dashscope": "dashscope", # similar to openai but use different url
        }

        client_key = client_mapping.get(provider)
        if not client_key or client_key not in self._async_clients:
            raise ValueError(f"Unsupported async client: {provider}")

        return self._async_clients[client_key]

    def get_ai_client(self, provider: str) -> OpenAI:
        """Create AI client for specified provider (OpenAI-compatible)"""
        config = AIConfig.get_provider_config(provider)

        return OpenAI(api_key=config["api_key"], base_url=config["api_base"])

    def get_openai_client(self) -> OpenAI:
        """Get OpenAI client"""
        return self.get_client("openai")

    def get_async_openai_client(self) -> AsyncOpenAI:
        """Get async OpenAI client"""
        return self.get_async_client("openai")
    
    def get_dashscope_client(self) -> OpenAI:
        """Get DashScope client"""
        return self.get_client("dashscope")
    
    def get_async_dashscope_client(self) -> AsyncOpenAI:
        """Get async DashScope client"""
        return self.get_async_client("dashscope")

    def get_gemini_client(self) -> genai.Client:
        """Get Gemini client"""
        return self.get_client("gemini")

    def get_async_gemini_client(self):
        """Get async Gemini client"""
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

    def get_async_openrouter_client(self) -> AsyncOpenAI:
        """Get async OpenRouter client (OpenAI-compatible)"""
        config = AIConfig.get_provider_config("openrouter")
        return AsyncOpenAI(
            api_key=config["api_key"],
            base_url=config["api_base"]
        )

    def is_client_available(self, provider: str) -> bool:
        """检查指定 provider 的 async client 是否可用"""
        self._initialize_clients()
        client_mapping = {
            "openai": "openai",
            "gemini": "gemini",
            "dashscope": "dashscope",
        }
        client_key = client_mapping.get(provider)
        return bool(client_key and client_key in self._async_clients)

    def health_check(self) -> Dict[str, bool]:
        """Check health status of all clients"""
        health_status = {}

        for provider in AIConfig.get_all_providers():
            try:
                config = AIConfig.get_provider_config(provider)
                health_status[provider] = bool(config.get("api_key"))
            except Exception:
                health_status[provider] = False

        return health_status


# Global client manager instance
client_manager = AIClientManager()


# Client getter functions
def get_ai_client(provider: str) -> OpenAI:
    """Get AI client"""
    return client_manager.get_ai_client(provider)


def get_azure_chat_model(deployment: str):
    """Create a LangChain ChatOpenAI model using Azure v1 endpoint + WIF auth.

    Uses /openai/v1/ endpoint — no api_version needed.
    Reads AZURE_OPENAI_ENDPOINT from env (set by export_to_env() at startup).
    """
    from azure.identity import DefaultAzureCredential, get_bearer_token_provider
    from langchain_openai import ChatOpenAI

    credential = DefaultAzureCredential()
    token_provider = get_bearer_token_provider(
        credential, "https://cognitiveservices.azure.com/.default"
    )
    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
    return ChatOpenAI(
        model=deployment,
        base_url=f"{endpoint}/openai/v1/",
        api_key=token_provider,
    )


# Global client objects (lazy initialization)
class _GlobalClients:
    def __init__(self):
        self._client_manager = None

    @property
    def client_manager(self) -> AIClientManager:
        if self._client_manager is None:
            self._client_manager = AIClientManager()
        return self._client_manager

    @property
    def openai_client(self) -> OpenAI:
        return client_manager.get_openai_client()

    @property
    def async_openai_client(self) -> AsyncOpenAI:
        return client_manager.get_async_openai_client()

    @property
    def gemini_client(self):
        return client_manager.get_async_gemini_client()


# Global objects
_global_clients = _GlobalClients()
openai_client = None
async_openai_client = None
gemini_client = None


def init_clients():
    global openai_client, async_openai_client, gemini_client
    try:
        openai_client = _global_clients.openai_client
    except (ValueError, Exception):
        openai_client = None
    try:
        async_openai_client = _global_clients.async_openai_client
    except (ValueError, Exception):
        async_openai_client = None
    try:
        gemini_client = _global_clients.gemini_client
    except (ValueError, Exception):
        gemini_client = None