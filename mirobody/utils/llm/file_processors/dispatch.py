"""Which provider handles this file, and with what settings.

The provider-selection POLICY (priority order, "is its key configured", the
default model per provider) and the dispatch that acts on it live together
here, because they are one decision: `VisionProviderConfig` answers *who* and
`unified_file_extract` acts on the answer. Keeping them in one module is also
what lets a test control both through a single patch point.

Each handler adapts the uniform dispatch signature to one backend's own
parameters — that adaptation is all they do, and it is why adding a provider
touches this file and one backend, never the callers.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from google.genai import types

from mirobody.utils.config import safe_read_cfg

from .backends_openai import doubao_file_extract, qwen_file_extract, vision_file_extract
from .gemini import gemini_file_extract
from .results import _build_prompt_with_schema


class VisionProviderConfig:
    """Vision provider configuration with auto-selection based on API keys."""

    VISION_PROVIDERS: List[Dict[str, Any]] = [
        {
            "name": "gemini",
            "api_key_env": "GOOGLE_API_KEY",
            "default_model": "gemini-3-flash-preview",
            "description": "Google Gemini (Direct API)",
        },
        {
            "name": "openrouter",
            "api_key_env": "OPENROUTER_API_KEY",
            "default_model": "google/gemini-3-flash-preview",
            "description": "OpenRouter (OpenAI Compatible)",
        },
        {
            "name": "qwen",
            "api_key_env": "DASHSCOPE_API_KEY",
            "default_model": "qwen3-vl-flash",
            "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "description": "Qwen Vision (Alibaba Dashscope)",
        },
        {
            "name": "doubao",
            "api_key_env": "VOLCENGINE_API_KEY",
            "default_model": "doubao-seed-1-6-vision-250815",
            "description": "Doubao/Volcengine Vision",
        },
    ]

    @classmethod
    def get_available_provider(cls) -> Optional[Dict[str, Any]]:
        """Get first available provider with configured API key."""
        for provider in cls.VISION_PROVIDERS:
            if safe_read_cfg(provider["api_key_env"]):
                logging.info(f"🔍 Vision provider selected: {provider['name']} ({provider['description']})")
                return provider
        return None

    @classmethod
    def get_provider_by_name(cls, name: str) -> Optional[Dict[str, Any]]:
        """Get provider configuration by name."""
        for provider in cls.VISION_PROVIDERS:
            if provider["name"] == name:
                return provider
        return None

    @classmethod
    def list_available_providers(cls) -> List[str]:
        """List all providers with configured API keys."""
        return [p["name"] for p in cls.VISION_PROVIDERS if safe_read_cfg(p["api_key_env"])]

    @classmethod
    def get_provider_status(cls) -> Dict[str, bool]:
        """Get availability status of all providers."""
        return {p["name"]: bool(safe_read_cfg(p["api_key_env"])) for p in cls.VISION_PROVIDERS}



# =============================================================================

async def _handle_gemini(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for Gemini provider."""
    return await gemini_file_extract(
        file_path=file_path,
        content_type=content_type,
        prompt=prompt,
        config=config,
        model=model
    )


async def _handle_openrouter(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for OpenRouter provider."""
    return await vision_file_extract(
        local_file_path=file_path,
        prompt=prompt,
        model=model,
        response_schema=response_schema,
        json_mode=json_mode
    )


async def _handle_qwen(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for Qwen provider."""
    return await qwen_file_extract(
        local_file_path=file_path,
        prompt=prompt,
        model=model,
        response_schema=response_schema,
        json_mode=json_mode
    )


async def _handle_doubao(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for Doubao provider."""
    final_prompt = _build_prompt_with_schema(prompt, response_schema) if response_schema else prompt
    return await doubao_file_extract(
        local_file_path=file_path,
        prompt=final_prompt,
        model=model,
        json_mode=json_mode
    )


PROVIDER_HANDLERS: Dict[str, Callable] = {
    "gemini": _handle_gemini,
    "openrouter": _handle_openrouter,
    "qwen": _handle_qwen,
    "doubao": _handle_doubao,
}


async def unified_file_extract(
    file_path: str,
    prompt: str,
    content_type: str = "image/jpeg",
    model: Optional[str] = None,
    config: Optional[types.GenerateContentConfig] = None,
    provider: Optional[str] = None,
    json_mode: Optional[bool] = None
) -> str:
    """
    Unified file extraction that auto-selects provider based on API keys.

    Provider priority: gemini > openrouter > qwen > doubao

    Args:
        file_path: Path to the file
        prompt: Extraction prompt
        content_type: MIME type (only used for Gemini)
        model: Model name (auto-selects default if not provided)
        config: Gemini GenerateContentConfig (only works with Gemini)
        provider: Force specific provider (overrides auto-selection)
        json_mode: Force JSON output (None=auto-detect from config)

    Returns:
        Extracted content (JSON string or plain text)

    Raises:
        ValueError: If no provider is available or specified provider is invalid
    """
    if provider:
        provider_config = VisionProviderConfig.get_provider_by_name(provider)
        if not provider_config:
            raise ValueError(f"Unknown provider: {provider}. Available: {list(PROVIDER_HANDLERS.keys())}")
        if not safe_read_cfg(provider_config["api_key_env"]):
            raise ValueError(f"API key not configured for '{provider}' (env: {provider_config['api_key_env']})")
    else:
        provider_config = VisionProviderConfig.get_available_provider()
        if not provider_config:
            status = VisionProviderConfig.get_provider_status()
            raise ValueError(
                f"No vision provider available. Configure one of: "
                f"GOOGLE_API_KEY, OPENROUTER_API_KEY, DASHSCOPE_API_KEY, VOLCENGINE_API_KEY. "
                f"Current status: {status}"
            )

    provider_name = provider_config["name"]
    actual_model = model or safe_read_cfg(f"{provider_name.upper()}_VISION_MODEL", provider_config["default_model"])

    # Extract response_schema from config
    response_schema = getattr(config, 'response_schema', None) if config else None
    if response_schema and provider_name != "gemini":
        logging.info(f"📋 Embedding response_schema into prompt for {provider_name}")

    # Auto-detect json_mode from config
    if json_mode is None:
        json_mode = bool(
            (config and getattr(config, 'response_schema', None)) or
            (config and getattr(config, 'response_mime_type', None) == "application/json")
        )

    logging.info(f"unified_file_extract: {provider_name}, model={actual_model}, json_mode={json_mode}")

    # Dispatch to handler
    handler = PROVIDER_HANDLERS.get(provider_name)
    if not handler:
        raise ValueError(f"Unsupported provider: {provider_name}")

    return await handler(
        file_path=file_path,
        prompt=prompt,
        content_type=content_type,
        model=actual_model,
        config=config,
        response_schema=response_schema,
        json_mode=json_mode
    )
