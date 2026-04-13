"""
AI utility functions module

Provides format conversion, helper functions and other common utilities.
"""

import json
import logging
import os
import uuid
from typing import Dict, List, Optional

from .config import AIConfig

#-----------------------------------------------------------------------------

PROJECT_DIR = os.getenv("PROJECT_PATH") or os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
)

#-----------------------------------------------------------------------------

async def get_openai_chat(model_name: str, messages: List[Dict], **kwargs) -> Optional[str]:
    """
    Get OpenAI chat response (compatible interface)

    Args:
        model_name: Model name
        messages: Message list
        **kwargs: Other parameters

    Returns:
        Response text or None
    """
    try:
        if model_name not in ["gpt-4o", "gpt-4.1"]:
            raise ValueError(f"Invalid model name: {model_name}")

        from .clients import client_manager

        client = client_manager.get_async_openai_client()

        response = await client.chat.completions.create(model=model_name, messages=messages, **kwargs)

        return response.choices[0].message.content

    except Exception as e:
        logging.error(f"OpenAI chat API error: {type(e).__name__}", stack_info=True)
        return None


async def async_get_openai_tts(text: str, voice: str = "alloy", model: str = "tts-1", **kwargs) -> Optional[str]:
    """
    Get OpenAI TTS response
    """
    try:
        from .clients import client_manager

        client = client_manager.get_async_openai_client()

        response = await client.audio.speech.create(model=model, voice=voice, input=text, **kwargs)
        # Save to local
        file_dir = os.path.join(PROJECT_DIR, "tts")
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        file_path = os.path.join(file_dir, f"{uuid.uuid4()}.mp3")
        response.write_to_file(file_path)
        return file_path

    except Exception as e:
        logging.error(f"OpenAI TTS API error: {type(e).__name__}", stack_info=True)
        return None


async def async_get_openai_structured_output(
    model_name: str, messages: List[Dict], response_format: Dict, **kwargs
) -> Optional[Dict]:
    """
    Get OpenAI structured output response
    """
    try:
        from .clients import client_manager

        client = client_manager.get_async_openai_client()

        response = await client.chat.completions.create(
            model=model_name, messages=messages, response_format=response_format, **kwargs
        )
        result = response.choices[0].message.to_dict()
        final_result = {}
        if result["refusal"] is None:
            final_result = json.loads(result["content"])
        return final_result
    except Exception as e:  # noqa
        logging.error(f"OpenAI structured output API error: {type(e).__name__}", stack_info=True)
        return None


async def async_get_doubao_structured_output(
    model_name: str, messages: List[Dict], response_format: Dict = None, **kwargs
) -> Optional[Dict]:
    """
    Get Doubao structured output response
    
    Args:
        model_name: Doubao model name (e.g., doubao-1.5-vision-pro-250328, doubao-1-5-ui-tars-250428)
        messages: Message list, OpenAI-compatible format. Note: if "json" keyword not in messages, it will be auto-added
        response_format: Response format config (Doubao auto-supports JSON output, this param for compatibility)
        **kwargs: Other parameters:
            - temperature: Randomness control (0-1)
            - max_tokens: Max output tokens
            - top_p: Nucleus sampling
            - thinking: Deep thinking config, e.g., {"type": "disabled/enabled/auto"}
            - thinking_type: Simplified thinking param, pass "disabled"/"enabled"/"auto"
        
    Returns:
        Structured JSON response dict, or None on failure
        

    """
    import time
    
    # Record start time
    start_time = time.time()
    
    try:
        from volcenginesdkarkruntime import AsyncArk
        
        # Get Doubao config
        volcengine_config = AIConfig.get_provider_config("volcengine")
        
        # Create AsyncArk client
        client = AsyncArk(
            api_key=volcengine_config["api_key"],
            base_url=volcengine_config["api_base"],
        )
        
        # Prepare request params
        request_params = {
            "model": model_name,
            "messages": messages,
            "response_format": response_format,  # Force JSON output
        }
        
        # Add optional params
        if "temperature" in kwargs:
            request_params["temperature"] = kwargs["temperature"]
        if "max_tokens" in kwargs:
            request_params["max_tokens"] = kwargs["max_tokens"]
        if "top_p" in kwargs:
            request_params["top_p"] = kwargs["top_p"]
        
        # Handle thinking param (via extra_body)
        extra_body = {}
        if "thinking" in kwargs:
            extra_body["thinking"] = kwargs["thinking"]
        elif "thinking_type" in kwargs:
            # Support simplified thinking_type param
            extra_body["thinking"] = {"type": kwargs["thinking_type"]}
        else:
            # Default disable thinking (avoid unnecessary computation)
            extra_body["thinking"] = {"type": "disabled"}
        
        if extra_body:
            request_params["extra_body"] = extra_body
            
        logging.info(f"Calling Doubao API - Model: {model_name}, Messages: {len(messages)}")
        
        # Record API call start time
        api_start_time = time.time()
        
        # Call Doubao API
        response = await client.chat.completions.create(**request_params)
        
        # Calculate API call duration
        api_duration = time.time() - api_start_time
        logging.info(f"Doubao API call completed, duration: {api_duration:.3f}s")
        
        # Extract response content
        if response.choices and len(response.choices) > 0:
            finish_reason = getattr(response.choices[0], "finish_reason", None)
            if finish_reason == "length":
                total_duration = time.time() - start_time
                logging.error(f"Doubao response truncated (finish_reason=length), max_tokens too low. Total: {total_duration:.3f}s")

            content = response.choices[0].message.content
            if content:
                try:
                    # Record JSON parse start time
                    parse_start_time = time.time()

                    # Try to parse JSON
                    final_result = json.loads(content)

                    # Calculate JSON parse duration
                    parse_duration = time.time() - parse_start_time

                    # Calculate total duration
                    total_duration = time.time() - start_time

                    logging.info(f"Doubao structured output success - Parse: {parse_duration:.3f}s, Total: {total_duration:.3f}s")
                    return final_result

                except json.JSONDecodeError as json_error:
                    total_duration = time.time() - start_time
                    content_len = len(content) if content else 0
                    logging.error(
                        f"Doubao response JSON parse failed: {json_error}, "
                        f"finish_reason={finish_reason}, content_length={content_len}, "
                        f"Total: {total_duration:.3f}s, "
                        f"Original content (first 500): {content[:500]}... "
                        f"Original content (last 200): ...{content[-200:] if content_len > 200 else content}",
                        stack_info=True
                    )
                    return None
            else:
                total_duration = time.time() - start_time
                logging.warning(f"Doubao API response content empty, Total: {total_duration:.3f}s")
                return None
        else:
            total_duration = time.time() - start_time
            logging.warning(f"Doubao API response choices empty, Total: {total_duration:.3f}s")
            return None
            
    except Exception as e:
        total_duration = time.time() - start_time
        logging.error(f"Doubao structured output API error: {type(e).__name__}: {str(e)}, Total: {total_duration:.3f}s", stack_info=True)
        return None


async def async_get_structured_output(
    messages: List[Dict],
    response_format: Dict,
    model_name: Optional[str] = None,
    provider: Optional[str] = None,
    **kwargs
) -> Optional[Dict]:
    """
    Unified structured output function, auto-selects provider based on available API keys
    
    Priority: openai > openrouter > claude > gemini > volcengine > dashscope
    
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
    
    # Structured output priority list (Gemini first for best JSON schema support)
    STRUCTURED_OUTPUT_PRIORITY = [
        {
            "name": "gemini",
            "api_key_env": "GOOGLE_API_KEY",
            "default_model": "gemini-3-flash-preview",
            "description": "Google Gemini (Best for structured output)",
        },
        {
            "name": "openai",
            "api_key_env": "OPENAI_API_KEY",
            "default_model": "gpt-5.2",
            "description": "OpenAI GPT Models",
        },
        {
            "name": "openrouter",
            "api_key_env": "OPENROUTER_API_KEY",
            "default_model": "google/gemini-3-flash-preview",
            "description": "OpenRouter (Multi-model Gateway)",
        },
        {
            "name": "volcengine",
            "api_key_env": "VOLCENGINE_API_KEY",
            "default_model": "doubao-seed-1-8-251228",
            "description": "Volcengine Doubao Seed 1.8",
        },
        {
            "name": "dashscope",
            "api_key_env": "DASHSCOPE_API_KEY",
            "default_model": "qwen-flash",
            "description": "Aliyun DashScope (Qwen Flash)",
        },
    ]
    
    # Normalize max_tokens across providers
    max_tokens_value = kwargs.pop("max_tokens", None) or kwargs.pop("max_completion_tokens", None)

    async def _call_provider(prov_name: str, prov_model: str) -> Optional[Dict]:
        """Call a specific provider. Returns result dict or None on failure."""
        try:
            if prov_name in ["openai", "openrouter"]:
                if prov_name == "openai":
                    client = client_manager.get_async_openai_client()
                else:
                    client = client_manager.get_async_openrouter_client()

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
                    logging.info(f"✅ {prov_name} structured output completed, duration: {duration:.3f}s")
                    return final_result
                return None

            elif prov_name == "volcengine":
                volcengine_kwargs = {**kwargs}
                if max_tokens_value:
                    volcengine_kwargs["max_tokens"] = max_tokens_value
                return await async_get_doubao_structured_output(
                    model_name=prov_model,
                    messages=messages,
                    response_format=response_format,
                    **volcengine_kwargs
                )

            elif prov_name == "dashscope":
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
                    logging.info(f"✅ DashScope structured output completed, duration: {duration:.3f}s")
                    return final_result
                return None

            elif prov_name == "gemini":
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
                    logging.info(f"✅ Gemini structured output completed, duration: {duration:.3f}s")
                    return final_result
                return None

            else:
                logging.error(f"Unsupported provider: {prov_name}")
                return None

        except Exception as e:
            duration = time.time() - start_time
            logging.error(f"Structured output API error ({prov_name}): {type(e).__name__}: {str(e)}, duration: {duration:.3f}s")
            return None

    # Determine provider to use
    if provider:
        # User specified a provider — use it directly, no fallback
        provider_info = AIConfig.get_provider_by_priority_name(provider)
        if not provider_info:
            logging.error(f"Unknown provider: {provider}")
            return None

        if not safe_read_cfg(provider_info["api_key_env"]):
            logging.error(f"Provider {provider} API Key not configured")
            return None
        actual_model = model_name or provider_info["default_model"]
        logging.info(f"🔄 async_get_structured_output: Using {provider} provider, model: {actual_model}")
        return await _call_provider(provider, actual_model)
    else:
        # Auto-select: try each available provider in priority order, fallback on failure
        tried_providers = []
        for p in STRUCTURED_OUTPUT_PRIORITY:
            if not safe_read_cfg(p["api_key_env"]):
                continue
            prov_name = p["name"]
            prov_model = p["default_model"]
            logging.info(f"🔄 async_get_structured_output: Trying {prov_name} provider, model: {prov_model}")
            result = await _call_provider(prov_name, prov_model)
            if result is not None:
                return result
            tried_providers.append(prov_name)
            logging.warning(f"⚠️ Provider {prov_name} failed, trying next available provider...")

        status = AIConfig.get_provider_status()
        logging.error(f"All providers failed (tried: {tried_providers}), status: {status}")
        return None


async def async_get_text_completion(
    messages: List[Dict],
    model_name: Optional[str] = None,
    provider: Optional[str] = None,
    **kwargs
) -> Optional[str]:
    """
    Unified text generation function, auto-selects provider based on available API keys
    
    For generating plain text (non-JSON), such as Markdown, plain text, etc.
    
    Priority: openai > openrouter > claude > gemini > volcengine > dashscope
    
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
    
    start_time = time.time()
    
    # Determine provider to use
    if provider:
        provider_info = AIConfig.get_provider_by_priority_name(provider)
        if not provider_info:
            logging.error(f"Unknown provider: {provider}")
            return None
        from mirobody.utils.config import safe_read_cfg
        if not safe_read_cfg(provider_info["api_key_env"]):
            logging.error(f"Provider {provider} API Key not configured")
            return None
        actual_model = model_name or provider_info["default_model"]
    else:
        provider_info = AIConfig.get_available_provider()
        if not provider_info:
            status = AIConfig.get_provider_status()
            logging.error(f"No available AI provider, please configure API Key: {status}")
            return None
        provider = provider_info["name"]
        actual_model = provider_info["default_model"]
        if model_name:
            logging.warning(f"⚠️ model_name='{model_name}' ignored, using provider default model: {actual_model}")
    
    logging.info(f"🔄 async_get_text_completion: Using {provider} provider, model: {actual_model}")
    
    try:
        if provider in ["openai", "openrouter", "dashscope"]:
            # OpenAI-compatible clients
            if provider == "openai":
                client = client_manager.get_async_openai_client()
            elif provider == "openrouter":
                client = client_manager.get_async_openrouter_client()
            else:
                client = client_manager.get_async_dashscope_client()
            
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
            logging.info(f"✅ {provider} text generation completed, duration: {duration:.3f}s")
            return content
            
        elif provider == "volcengine":
            # Use Doubao
            from volcenginesdkarkruntime import AsyncArk
            volcengine_config = AIConfig.get_provider_config("volcengine")
            client = AsyncArk(
                api_key=volcengine_config["api_key"],
                base_url=volcengine_config["api_base"],
            )
            response = await client.chat.completions.create(
                model=actual_model,
                messages=messages,
                **kwargs
            )
            content = response.choices[0].message.content
            duration = time.time() - start_time
            logging.info(f"✅ Volcengine text generation completed, duration: {duration:.3f}s")
            return content
            
        elif provider == "gemini":
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
                logging.info(f"✅ Gemini text generation completed, duration: {duration:.3f}s")
                return response.text
            return None
            
        elif provider == "claude":
            logging.warning("Claude text generation not supported yet, please use other providers")
            return None
            
        else:
            logging.error(f"Unsupported provider: {provider}")
            return None
            
    except Exception as e:
        duration = time.time() - start_time
        logging.error(f"Text generation API error ({provider}): {type(e).__name__}: {str(e)}, duration: {duration:.3f}s", stack_info=True)
        return None