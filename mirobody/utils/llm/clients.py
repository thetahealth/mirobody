
from typing import Any, Dict

from google import genai
from openai import AsyncOpenAI, OpenAI

from .config import AIConfig
from ...utils import safe_read_cfg

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
            self._clients["gemini"] = genai.Client(api_key=google_api_key)
            self._async_clients["gemini"] = genai.Client(api_key=google_api_key).aio

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

    def get_async_openrouter_client(self) -> AsyncOpenAI:
        """Get async OpenRouter client (OpenAI-compatible)"""
        config = AIConfig.get_provider_config("openrouter")
        return AsyncOpenAI(
            api_key=config["api_key"],
            base_url=config["api_base"]
        )

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
    openai_client = _global_clients.openai_client
    async_openai_client = _global_clients.async_openai_client
    gemini_client = _global_clients.gemini_client
