"""Direct per-provider LLM access (`utils.py`, `file_processors.py`,
`clients.py`) — the code most of the repo actually uses.

The other half that used to live here — `batch_ai_response`/`interface.py`
and the `adapters/` stack — was deleted along with its single caller,
`pulse/file_parser/services/file_llm_analyzer.py` (the orphaned
/ws/upload-with-llm-analysis flow). The streaming half had already been
removed earlier for the same reason (no callers).

Not to be confused with `agent/base/clients.py`, which is the separate
streaming/agentic client stack with MCP tool calling.
"""

# Client management
from .clients import client_manager, openai_client

# Configuration management
from .config import AI_CONFIG, AIConfig

from .file_processors import (
    FileProcessor,
    VisionProviderConfig,
    gemini_file_extract,
    gemini_multi_file_extract,
    doubao_file_extract,
    unified_file_extract,
)

# Utility functions
from .utils import (
    async_get_openai_structured_output,
    async_get_doubao_structured_output,
    async_get_structured_output,
    async_get_text_completion,
    async_get_openai_tts,
    get_openai_chat,
)

# LLM provider config
from .hipaa_policy import (
    export_to_env,
    get_azure_deployment,
)

__version__ = "2.0.0"
__author__ = "AI Team"

# Main exported interfaces
__all__ = [
    # === Configuration and management ===
    "AIConfig",  # Configuration manager
    "AI_CONFIG",  # Global config object
    "client_manager",  # Client manager
    "openai_client",  # OpenAI client
    # === File processing ===
    "FileProcessor",  # File processor
    # "VisionProcessor",  # Vision processor
    "VisionProviderConfig",  # Vision provider config (query available providers)
    "gemini_file_extract",  # Gemini file extraction
    "gemini_multi_file_extract",  # Gemini multi-file extraction
    "doubao_file_extract",  # Doubao file extraction
    "unified_file_extract",  # 🔥 Unified file extraction entry (auto-select model)
    # === Utility functions ===
    "get_openai_chat",  # Get OpenAI chat
    "async_get_openai_tts",  # Get OpenAI TTS
    "async_get_openai_structured_output",  # Get OpenAI structured output
    "async_get_doubao_structured_output",  # Get Doubao structured output
    "async_get_structured_output",  # 🔥 Unified structured output (auto-select provider)
    "async_get_text_completion",  # 🔥 Unified text generation (auto-select provider)
    # === LLM provider config ===
    "export_to_env",  # Bridge config center → SDK env vars (call at startup)
    "get_azure_deployment",  # Azure deployment name resolution
]
