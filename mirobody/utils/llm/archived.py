"""
Archived LLM utility functions
"""

import json
import logging
from typing import Dict, List, Optional

import aiohttp

from .config import AIConfig


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
