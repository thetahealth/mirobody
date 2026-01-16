"""
Core AI interface module

Provides unified AI call interfaces, supports batch and streaming processing for multiple models.
"""

import logging
from typing import Any, AsyncGenerator, Dict, List

from .adapters.factory import adapter_factory
from .config import AIConfig


async def batch_ai_response(messages: List[dict], provider: str, **kwargs) -> Dict[str, Any]:
    """
    Unified batch AI response interface

    Args:
        messages: Message list
        provider: Model provider
        **kwargs: All other parameters passed directly to underlying adapter, including:
            - max_tokens: Max tokens (auto-set based on model, e.g., GPT-4 is 32768)
            - functions: Function definition list (OpenAI format)
            - function_call: Function call mode ("auto", "none", or specific function name)
            - temperature: Temperature parameter
            - thinking: Thinking mode (volcengine specific)
            - stream: Whether to stream output
            - Other model-specific parameters

    Returns:
        Unified format AI response: {"content": str, "function_calls": List or None}
    """
    # Validate provider
    if not AIConfig.validate_provider(provider):
        return {"content": "", "function_calls": None, "error": f"Unsupported AI provider: {provider}"}

    # Create adapter
    adapter = adapter_factory.create_adapter(provider)

    # Call adapter
    return await adapter.batch_completion(messages, **kwargs)


async def stream_ai_response(messages: List[dict], provider: str, **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Unified streaming AI response interface

    Args:
        messages: Message list
        provider: Model provider
        **kwargs: All other parameters passed directly to underlying adapter

    Yields:
        Streaming response data
    """
    # Validate provider
    if not AIConfig.validate_provider(provider):
        yield {"error": f"Unsupported AI provider: {provider}"}
        return

    # Create adapter
    adapter = adapter_factory.create_adapter(provider)

    # Stream call adapter
    first_token = True
    async for chunk in adapter.stream_completion(messages, **kwargs):
        # Log on first token
        if first_token:
            logging.info(f"Starting stream response, provider: {provider}, model: {AIConfig.get_provider_config(provider).get('model', 'unknown')}")
            first_token = False

        yield chunk


def get_provider_capabilities(provider: str) -> Dict[str, Any]:
    """
    Get provider capability information

    Args:
        provider: Provider name

    Returns:
        Capability info dict
    """
    try:
        config = AIConfig.get_provider_config(provider)
        adapter = adapter_factory.create_adapter(provider)

        return {
            "provider": provider,
            "model": config.get("model"),
            "type": config.get("type"),
            "supports_function_calling": adapter.supports_function_calling(),
            "supports_streaming": True,  # All adapters support streaming
            "api_base": config.get("api_base"),
            "available": bool(config.get("api_key")),
        }
    except Exception as e:
        return {"provider": provider, "available": False, "error": str(e)}


def get_all_providers_info() -> Dict[str, Dict[str, Any]]:
    """Get information for all providers"""
    return {provider: get_provider_capabilities(provider) for provider in AIConfig.get_all_providers()}


def health_check() -> Dict[str, bool]:
    """Health check: Check availability of all providers"""
    return {
        provider: get_provider_capabilities(provider).get("available", False)
        for provider in AIConfig.get_all_providers()
    }
