"""
LLM module unified entry point

Provides all AI model related functional interfaces.
"""

# Core interfaces
# Adapters
# from .adapters import BaseModelAdapter, GeminiAdapter, OpenAIAdapter, VolcengineAdapter
from .adapters.factory import adapter_factory

# Client management
from .clients import client_manager, openai_client

# Configuration management
from .config import AI_CONFIG, AIConfig

# File processing
from .file_processors import (
    FileProcessor,
    # VisionProcessor,
    VisionProviderConfig,
    gemini_file_extract,
    gemini_multi_file_extract,
    doubao_file_extract,
    unified_file_extract,
)
from .interface import (
    batch_ai_response,
    get_all_providers_info,
    get_provider_capabilities,
    health_check,
    stream_ai_response,
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

# Version info
__version__ = "2.0.0"
__author__ = "AI Team"

# Main exported interfaces
__all__ = [
    # === Core interfaces ===
    "batch_ai_response",  # 🔥 Main interface: Batch AI response
    "stream_ai_response",  # 🔥 Main interface: Streaming AI response
    # === Configuration and management ===
    "AIConfig",  # Configuration manager
    "AI_CONFIG",  # Global config object
    "client_manager",  # Client manager
    "openai_client",  # OpenAI client
    "adapter_factory",  # Adapter factory
    # === Information queries ===
    "get_provider_capabilities",  # Get provider capabilities
    "get_all_providers_info",  # Get all provider info
    "health_check",  # Health check
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


def get_module_info():
    """Get module information"""
    return {
        "version": __version__,
        "author": __author__,
        "supported_providers": AIConfig.get_all_providers(),
        "adapters": adapter_factory.get_adapter_info(),
        "core_functions": ["batch_ai_response", "stream_ai_response"],
    }


def quick_start_guide():
    """Quick start guide"""
    return """
    🚀 LLM Module Quick Start Guide
    
    === Basic Usage ===
    from utils.llm import batch_ai_response
    
    # Simple call
    result = await batch_ai_response(
        messages=[{"role": "user", "content": "Hello"}],
        provider="volcengine"
    )
    
    === Function Calling ===
    result = await batch_ai_response(
        messages=messages,
        provider="gpt-4.1",
        functions=functions,
        function_call="auto"
    )
    
    === Streaming ===
    from utils.llm import stream_ai_response
    
    async for chunk in stream_ai_response(messages, "gemini"):
        print(chunk.get("content", ""))
    
    === View Available Models ===
    from utils.llm import get_all_providers_info
    print(get_all_providers_info())
    """


# Module-level docstring
__doc__ = f"""
Unified LLM Interface Module v{__version__}

This module provides a unified multi-model AI interface, supporting:
- OpenAI (GPT-4, GPT-4o, etc.)
- Volcengine/Doubao
- Google Gemini
- Other OpenAI API-compatible models

Key Features:
✅ Unified API interface
✅ Complete parameter passthrough
✅ Function Calling support
✅ Streaming and batch processing
✅ Automatic adapter selection

Quick Start:
{quick_start_guide()}
"""