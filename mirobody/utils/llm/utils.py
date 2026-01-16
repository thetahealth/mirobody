"""
AI utility functions module

Provides format conversion, helper functions and other common utilities.
"""

import aiohttp
import json
import logging
import os
import uuid
from typing import Dict, List, Optional

# from mirobody.utils.config import PROJECT_DIR

from .config import AIConfig

#-----------------------------------------------------------------------------

PROJECT_DIR = os.getenv("PROJECT_PATH") or os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
)

#-----------------------------------------------------------------------------

def format_openai_messages(messages: List[Dict], agent_type: str = "agent1") -> List[Dict]:
    """
    Format messages for OpenAI

    Args:
        messages: Original message list
        agent_type: Agent type

    Returns:
        Formatted message list
    """
    formatted_messages = []
    for msg in messages:
        formatted_msg = {"role": msg["role"], "content": msg["content"]}
        # TODO: Can add special handling based on agent_type
        formatted_messages.append(formatted_msg)
    return formatted_messages


async def get_volcengine_chat(model_name: str, messages: List[Dict], **kwargs) -> Optional[Dict]:
    """
    Get Volcengine chat response (raw HTTP interface)

    Args:
        model_name: Model name
        messages: Message list
        **kwargs: Other parameters

    Returns:
        Response data or None
    """
    try:
        if model_name not in ["doubao-lite", "volcengine"]:
            raise ValueError(f"Invalid model name: {model_name}")

        config = AIConfig.get_provider_config(model_name)

        url = f"{config['api_base']}{config['api_path']}"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {config['api_key']}",
        }

        data = {
            "model": config["model"],
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", 2000),
            "stream": kwargs.get("stream", False),
            "temperature": kwargs.get("temperature", 0.5),
            "thinking": {"type": kwargs.get("thinking", "disabled")},
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=headers, timeout=60) as response:
                if response.status != 200:
                    logging.error(f"Volcengine API error: {response.status}, {response.text}")
                    return None

                return response.json()

    except Exception as e:
        logging.error(f"Volcengine chat API error: {type(e).__name__}", stack_info=True)
        return None


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


def validate_messages(messages: List[Dict]) -> bool:
    """
    Validate message format

    Args:
        messages: Message list

    Returns:
        Whether valid
    """
    if not isinstance(messages, list) or not messages:
        return False

    for msg in messages:
        if not isinstance(msg, dict):
            return False
        if "role" not in msg or "content" not in msg:
            return False
        if msg["role"] not in ["system", "user", "assistant"]:
            return False
        if not isinstance(msg["content"], str):
            return False

    return True


def extract_function_calls_from_response(response: Dict, provider: str) -> Optional[List[Dict]]:
    """
    Extract function calls from response

    Args:
        response: API response
        provider: Provider name

    Returns:
        Function call list or None
    """
    try:
        if provider in ["openai", "gpt-4o", "gpt-4.1", "volcengine", "doubao-lite"]:
            # OpenAI format
            if "choices" in response and response["choices"]:
                choice = response["choices"][0]
                if "message" in choice and "function_call" in choice["message"]:
                    func_call = choice["message"]["function_call"]
                    return [{"name": func_call["name"], "arguments": json.loads(func_call["arguments"])}]

        elif provider == "gemini":
            # Gemini format needs special handling
            # Simplified here, should be handled in adapter
            pass

    except Exception as e:
        logging.error(f"Failed to extract function calls: {type(e).__name__}", stack_info=True)

    return None


def merge_streaming_content(chunks: List[str]) -> str:
    """
    Merge streaming content chunks

    Args:
        chunks: Content chunk list

    Returns:
        Merged content
    """
    return "".join(chunks)


def calculate_token_estimate(text: str) -> int:
    """
    Estimate token count for text (simple estimation)

    Args:
        text: Input text

    Returns:
        Estimated token count
    """
    # Simple estimation: ~4 chars/token for English, ~1.5 chars/token for Chinese
    chinese_chars = len([c for c in text if "\u4e00" <= c <= "\u9fff"])
    english_chars = len(text) - chinese_chars

    return int(chinese_chars / 1.5 + english_chars / 4)


def build_function_call_message(name: str, arguments: Dict, call_id: str = None) -> Dict:
    """
    Build function call message

    Args:
        name: Function name
        arguments: Function arguments
        call_id: Call ID (optional)

    Returns:
        Function call message
    """
    message = {
        "role": "assistant",
        "content": None,
        "function_call": {"name": name, "arguments": json.dumps(arguments, ensure_ascii=False)},
    }

    if call_id:
        message["function_call"]["id"] = call_id

    return message


def build_function_result_message(name: str, content: str, call_id: str = None) -> Dict:
    """
    Build function result message

    Args:
        name: Function name
        content: Function execution result
        call_id: Call ID (optional)

    Returns:
        Function result message
    """
    message = {"role": "function", "name": name, "content": content}

    if call_id:
        message["tool_call_id"] = call_id

    return message


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
                    logging.error(f"Doubao response JSON parse failed: {json_error}, Total: {total_duration:.3f}s, Original content: {content[:500]}...", stack_info=True)
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
    
    start_time = time.time()
    
    # Determine provider to use
    if provider:
        # Use specified provider
        provider_info = AIConfig.get_provider_by_priority_name(provider)
        if not provider_info:
            logging.error(f"Unknown provider: {provider}")
            return None
        # Check if API Key is configured
        from mirobody.utils.config import safe_read_cfg
        if not safe_read_cfg(provider_info["api_key_env"]):
            logging.error(f"Provider {provider} API Key not configured")
            return None
        # When provider is specified, can use custom model_name
        actual_model = model_name or provider_info["default_model"]
    else:
        # Auto-select available provider
        provider_info = AIConfig.get_available_provider()
        if not provider_info:
            status = AIConfig.get_provider_status()
            logging.error(f"No available AI provider, please configure API Key: {status}")
            return None
        provider = provider_info["name"]
        # When auto-selecting, ignore model_name, always use provider's default model (avoid model name mismatch)
        actual_model = provider_info["default_model"]
        if model_name:
            logging.warning(f"⚠️ model_name='{model_name}' ignored, using provider default model: {actual_model}")
    
    logging.info(f"🔄 async_get_structured_output: Using {provider} provider, model: {actual_model}")
    
    try:
        # Call different implementations based on provider type
        if provider in ["openai", "openrouter"]:
            # Both OpenAI and OpenRouter use OpenAI-compatible clients
            if provider == "openai":
                client = client_manager.get_async_openai_client()
            else:
                client = client_manager.get_async_openrouter_client()
            
            response = await client.chat.completions.create(
                model=actual_model,
                messages=messages,
                response_format=response_format,
                **kwargs
            )
            result = response.choices[0].message.to_dict()
            if result.get("refusal") is None:
                final_result = json.loads(result["content"])
                duration = time.time() - start_time
                logging.info(f"✅ {provider} structured output completed, duration: {duration:.3f}s")
                return final_result
            return None
            
        elif provider == "volcengine":
            # Use Doubao
            return await async_get_doubao_structured_output(
                model_name=actual_model,
                messages=messages,
                response_format=response_format,
                **kwargs
            )
            
        elif provider == "dashscope":
            # Use DashScope (Qwen)
            client = client_manager.get_async_dashscope_client()
            response = await client.chat.completions.create(
                model=actual_model,
                messages=messages,
                response_format=response_format,
                **kwargs
            )
            result = response.choices[0].message.to_dict()
            if result.get("refusal") is None:
                final_result = json.loads(result["content"])
                duration = time.time() - start_time
                logging.info(f"✅ DashScope structured output completed, duration: {duration:.3f}s")
                return final_result
            return None
            
        elif provider == "gemini":
            # Gemini uses native client
            client = client_manager.get_async_gemini_client()
            # Gemini structured output needs special handling
            from google.genai import types
            config = types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=kwargs.get("temperature", 0.1),
            )
            # Build Gemini-format messages
            prompt = "\n".join([
                f"{msg['role']}: {msg['content']}" 
                for msg in messages
            ])
            response = await client.models.generate_content(
                model=actual_model,
                contents=prompt,
                config=config,
            )
            if response and response.text:
                final_result = json.loads(response.text)
                duration = time.time() - start_time
                logging.info(f"✅ Gemini structured output completed, duration: {duration:.3f}s")
                return final_result
            return None
            
        elif provider == "claude":
            # Claude needs special handling (Anthropic API)
            logging.warning("Claude structured output not supported yet, please use other providers")
            return None
            
        else:
            logging.error(f"Unsupported provider: {provider}")
            return None
            
    except Exception as e:
        duration = time.time() - start_time
        logging.error(f"Structured output API error ({provider}): {type(e).__name__}: {str(e)}, duration: {duration:.3f}s", stack_info=True)
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
            
            response = await client.chat.completions.create(
                model=actual_model,
                messages=messages,
                **kwargs
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