"""
AI utility functions module

Provides format conversion, helper functions and other common utilities.
"""

import json
import logging

from .config import AIConfig
from ..config.llm import LLMProvider, provider_api_key_env, provider_model, read_api_key

logger = logging.getLogger(__name__)

# `PROJECT_DIR`, `os` and `uuid` used to be here to give `async_get_openai_tts`
# somewhere to write its .mp3 — the only thing in this module that ever touched
# the filesystem, and a function no caller ever invoked. All four went together.
#
# `get_openai_chat` went the same way, and was worse: it rejected every model
# name outside a hardcoded `["gpt-4o", "gpt-4.1"]` allowlist, so the one thing
# a caller would want it for — naming a current model — raised ValueError. Zero
# callers, and the allowlist is the exact anti-pattern issue #52 was about: a
# model id decided in code where no config can reach it.

#-----------------------------------------------------------------------------


#: Priority order for structured extraction. Gemini first: its JSON-schema
#: support is the strictest of the five. The api key and the model default come
#: from `config.llm._PROVIDER_DEFAULTS`, not from here — this list used to carry
#: its own copy of both, INSIDE the function body, so it was rebuilt per call
#: and no test could see it.
STRUCTURED_OUTPUT_PRIORITY: tuple[LLMProvider, ...] = (
    LLMProvider.GEMINI,
    LLMProvider.OPENAI,
    LLMProvider.OPENROUTER,
    LLMProvider.DASHSCOPE,
)


async def async_get_structured_output(
    messages: list[dict],
    response_format: dict,
    model_name: str | None = None,
    provider: str | None = None,
    **kwargs
) -> dict | None:
    """
    Unified structured output function, auto-selects provider based on available API keys
    
    Priority: openai > openrouter > gemini > dashscope
    
    Args:
        messages: Message list
        response_format: Response format config
        model_name: Model name (optional, only effective when provider is specified, otherwise uses auto-selected provider's default model)
        provider: Specify provider (optional, auto-selects if not provided)
        **kwargs: Other parameters like temperature, max_tokens, etc.
        
    Returns:
        Structured JSON response dict, or None on failure
        
    Usage:
        # Auto-select provider (use default model, recommended)
        result = await async_get_structured_output(
            messages=[{"role": "user", "content": "..."}],
            response_format={"type": "json_object"}
        )
        
        # Specify provider and model
        result = await async_get_structured_output(
            messages=messages,
            response_format=response_format,
            provider="openai",
            model_name="gpt-4.1"
        )
    """
    import time
    from .clients import client_manager
    from mirobody.utils.config import safe_read_cfg
    
    start_time = time.time()
    
    # Normalize max_tokens across providers
    max_tokens_value = kwargs.pop("max_tokens", None) or kwargs.pop("max_completion_tokens", None)

    async def _call_provider(prov_name: str, prov_model: str) -> dict | None:
        """Call a specific provider. Returns result dict or None on failure."""
        try:
            if prov_name in ["openai", "openrouter"]:
                if prov_name == "openai":
                    client = client_manager.get_async_openai_client()
                else:
                    client = client_manager.get_async_ai_client("openrouter")

                # OpenAI newer models (GPT-4o+) use max_completion_tokens instead of max_tokens
                provider_kwargs = {**kwargs}
                if max_tokens_value:
                    provider_kwargs["max_completion_tokens"] = max_tokens_value

                response = await client.chat.completions.create(
                    model=prov_model,
                    messages=messages,
                    response_format=response_format,
                    **provider_kwargs
                )
                result = response.choices[0].message.to_dict()
                if result.get("refusal") is None:
                    final_result = json.loads(result["content"])
                    duration = time.time() - start_time
                    logger.info(f"{prov_name} structured output completed, duration: {duration:.3f}s")
                    return final_result
                return None

            if prov_name == "dashscope":
                dashscope_kwargs = {**kwargs}
                if max_tokens_value:
                    dashscope_kwargs["max_tokens"] = max_tokens_value
                client = client_manager.get_async_dashscope_client()
                response = await client.chat.completions.create(
                    model=prov_model,
                    messages=messages,
                    response_format=response_format,
                    **dashscope_kwargs
                )
                result = response.choices[0].message.to_dict()
                if result.get("refusal") is None:
                    final_result = json.loads(result["content"])
                    duration = time.time() - start_time
                    logger.info(f"DashScope structured output completed, duration: {duration:.3f}s")
                    return final_result
                return None

            if prov_name == "gemini":
                client = client_manager.get_async_gemini_client()
                from google.genai import types
                gemini_config_params = {
                    "response_mime_type": "application/json",
                    "temperature": kwargs.get("temperature", 0.1),
                }
                if max_tokens_value:
                    gemini_config_params["max_output_tokens"] = max_tokens_value
                # Extract schema from OpenAI-style response_format and pass as response_json_schema
                # Gemini does not support additionalProperties in protobuf — strip it recursively
                if response_format and response_format.get("type") == "json_schema":
                    schema = response_format.get("json_schema", {}).get("schema")
                    if schema:
                        def _strip_additional_props(obj):
                            if isinstance(obj, dict):
                                return {k: _strip_additional_props(v) for k, v in obj.items() if k != "additionalProperties"}
                            if isinstance(obj, list):
                                return [_strip_additional_props(i) for i in obj]
                            return obj
                        gemini_config_params["response_json_schema"] = _strip_additional_props(schema)
                config = types.GenerateContentConfig(**gemini_config_params)
                prompt = "\n".join([
                    f"{msg['role']}: {msg['content']}"
                    for msg in messages
                ])
                response = await client.models.generate_content(
                    model=prov_model,
                    contents=prompt,
                    config=config,
                )
                if response and response.text:
                    final_result = json.loads(response.text)
                    duration = time.time() - start_time
                    logger.info(f"Gemini structured output completed, duration: {duration:.3f}s")
                    return final_result
                return None

            logger.error(f"Unsupported provider: {prov_name}")
            return None

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Structured output API error ({prov_name}): {type(e).__name__}: {str(e)}, duration: {duration:.3f}s")
            return None

    # Determine provider to use
    if provider:
        # User specified a provider — use it directly, no fallback
        provider_info = AIConfig.get_provider_by_priority_name(provider)
        if not provider_info:
            logger.error(f"Unknown provider: {provider}")
            return None

        if not read_api_key(provider_info["api_key_env"]):
            logger.error(f"Provider {provider} API Key not configured")
            return None
        actual_model = model_name or safe_read_cfg(f"{provider.upper()}_MODEL") or provider_info["default_model"]
        logger.info(f"async_get_structured_output: Using {provider} provider, model: {actual_model}")
        return await _call_provider(provider, actual_model)
    # Auto-select: try each available provider in priority order, fallback on failure
    tried_providers = []
    for canon in STRUCTURED_OUTPUT_PRIORITY:
        if not read_api_key(provider_api_key_env(canon)):
            continue
        prov_name = canon.value
        # `<PROVIDER>_MODEL` overrides the default, mirroring
        # `<PROVIDER>_VISION_MODEL` on the vision path. Needed whenever
        # `<PROVIDER>_BASE_URL` points at a gateway that does not serve
        # the default model id (e.g. OpenRouter redirected to DashScope:
        # `google/gemini-3.8-flash` 404s there).
        prov_model = provider_model(canon)
        logger.info(f"async_get_structured_output: Trying {prov_name} provider, model: {prov_model}")
        result = await _call_provider(prov_name, prov_model)
        if result is not None:
            return result
        tried_providers.append(prov_name)
        logger.warning(f"Provider {prov_name} failed, trying next available provider...")

    status = AIConfig.get_provider_status()
    logger.error(f"All providers failed (tried: {tried_providers}), status: {status}")
    return None


async def async_get_text_completion(
    messages: list[dict],
    model_name: str | None = None,
    provider: str | None = None,
    **kwargs
) -> str | None:
    """
    Unified text generation function, auto-selects provider based on available API keys
    
    For generating plain text (non-JSON), such as Markdown, plain text, etc.
    
    Priority: openai > openrouter > gemini > dashscope
    
    Args:
        messages: Message list, format: [{"role": "system/user/assistant", "content": "..."}]
        model_name: Model name (optional, only effective when provider is specified)
        provider: Specify provider (optional, auto-selects if not provided)
        **kwargs: Other parameters like temperature, max_tokens, etc.
        
    Returns:
        Generated text content, or None on failure
        
    Usage:
        # Auto-select provider
        result = await async_get_text_completion(
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Write a poem about AI."}
            ]
        )
        
        # Specify provider and model
        result = await async_get_text_completion(
            messages=messages,
            provider="openai",
            model_name="gpt-4.1"
        )
    """
    import time
    from .clients import client_manager
    from mirobody.utils.config import safe_read_cfg

    start_time = time.time()

    # Determine provider to use
    if provider:
        provider_info = AIConfig.get_provider_by_priority_name(provider)
        if not provider_info:
            logger.error(f"Unknown provider: {provider}")
            return None
        if not safe_read_cfg(provider_info["api_key_env"]):
            logger.error(f"Provider {provider} API Key not configured")
            return None
        actual_model = model_name or safe_read_cfg(f"{provider.upper()}_MODEL") or provider_info["default_model"]
    else:
        provider_info = AIConfig.get_available_provider()
        if not provider_info:
            status = AIConfig.get_provider_status()
            logger.error(f"No available AI provider, please configure API Key: {status}")
            return None
        provider = provider_info["name"]
        # Same `<PROVIDER>_MODEL` override as async_get_structured_output.
        actual_model = safe_read_cfg(f"{provider.upper()}_MODEL") or provider_info["default_model"]
        if model_name:
            logger.warning(f"model_name='{model_name}' ignored, using provider default model: {actual_model}")
    
    logger.info(f"async_get_text_completion: Using {provider} provider, model: {actual_model}")
    
    try:
        if provider in ["openai", "openrouter", "dashscope"]:
            # OpenAI-compatible clients
            # speaks chat/completions, so it needs no vendor SDK.
            if provider == "openai":
                client = client_manager.get_async_openai_client()
            elif provider == "dashscope":
                client = client_manager.get_async_dashscope_client()
            else:
                client = client_manager.get_async_ai_client(provider)
            
            # Handle max_tokens vs max_completion_tokens for newer OpenAI models
            # Models that require max_completion_tokens: o1, o3, gpt-5.x, etc.
            api_kwargs = kwargs.copy()
            if provider == "openai" and actual_model and "max_tokens" in api_kwargs:
                # Check if model requires max_completion_tokens instead of max_tokens
                requires_new_param = (
                    "o1" in actual_model or 
                    "o3" in actual_model or 
                    actual_model.startswith("gpt-5")
                )
                if requires_new_param:
                    api_kwargs["max_completion_tokens"] = api_kwargs.pop("max_tokens")
            
            response = await client.chat.completions.create(
                model=actual_model,
                messages=messages,
                **api_kwargs
            )
            content = response.choices[0].message.content
            duration = time.time() - start_time
            logger.info(f"{provider} text generation completed, duration: {duration:.3f}s")
            return content
            
        if provider == "gemini":
            # Gemini uses native client
            client = client_manager.get_async_gemini_client()
            from google.genai import types
            
            # Build Gemini-format prompt
            prompt_parts = []
            for msg in messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role == "system":
                    prompt_parts.append(f"System: {content}")
                elif role == "assistant":
                    prompt_parts.append(f"Assistant: {content}")
                else:
                    prompt_parts.append(f"User: {content}")
            
            combined_prompt = "\n\n".join(prompt_parts)
            
            config = types.GenerateContentConfig(
                temperature=kwargs.get("temperature", 0.7),
                max_output_tokens=kwargs.get("max_tokens", 8192),
            )
            
            response = await client.models.generate_content(
                model=actual_model,
                contents=combined_prompt,
                config=config,
            )
            
            if response and response.text:
                duration = time.time() - start_time
                logger.info(f"Gemini text generation completed, duration: {duration:.3f}s")
                return response.text
            return None
            
        logger.error(f"Unsupported provider: {provider}")
        return None
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Text generation API error ({provider}): {type(e).__name__}: {str(e)}, duration: {duration:.3f}s", stack_info=True)
        return None