"""
OpenAI Model Adapter

Handles special logic for OpenAI and compatible models
"""

import asyncio
import json
import re
import time
from typing import Any, AsyncGenerator, Dict, List

from mirobody.utils.llm.clients import client_manager
from .base import BaseModelAdapter


REASONING_MODELS_REGEX = r"^(gpt5|o3|o1)"


class OpenAIAdapter(BaseModelAdapter):
    """OpenAI model adapter"""

    async def batch_completion(self, messages: List[Dict], **kwargs) -> Dict[str, Any]:
        """OpenAI batch completion"""
        t_start = time.time()

        try:
            # Set default parameters
            kwargs = self.set_default_parameters(kwargs)

            # Get client
            client = client_manager.get_openai_client()

            # Build request parameters
            request_data = {"model": self.model, "messages": self.format_messages(messages), **kwargs}

            # Call API
            response = client.chat.completions.create(**request_data)

            # Log usage
            self.log_usage({"total_tokens": response.usage.total_tokens}, time.time() - t_start)

            # Process response
            choice = response.choices[0]
            result = {"content": choice.message.content or "", "function_calls": None}

            # Check function calls
            if kwargs.get("functions") and choice.finish_reason == "function_call":
                result["function_calls"] = [
                    {
                        "name": choice.message.function_call.name,
                        "arguments": json.loads(choice.message.function_call.arguments),
                    }
                ]

            return result

        except Exception as e:
            return self.handle_error(e, "batch completion")
        
    def set_default_parameters(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        kwargs = super().set_default_parameters(kwargs)

        if re.search(REASONING_MODELS_REGEX, self.model.lower()):
            for key in ["temperature", "top_p", "frequency_penalty", "presence_penalty", "max_tokens"]:
                if key in kwargs:
                    kwargs.pop(key)

        return kwargs

    async def stream_completion(self, messages: List[Dict], **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """OpenAI streaming completion with function calling support"""
        try:
            # Set default parameters
            kwargs = self.set_default_parameters(kwargs)
            kwargs["stream"] = True

            # Get client
            client = client_manager.get_openai_client()

            # Build request parameters
            request_data = {"model": self.model, "messages": self.format_messages(messages), **kwargs}

            # Stream call
            stream = client.chat.completions.create(**request_data)

            buffer = ""
            BUFFER_SIZE = 8
            function_call_buffer = {"name": "", "arguments": ""}
            collecting_function_call = False

            for chunk in stream:
                if chunk.choices and chunk.choices[0].delta:
                    delta = chunk.choices[0].delta
                    
                    # Handle function calls
                    if hasattr(delta, 'function_call') and delta.function_call:
                        collecting_function_call = True
                        if delta.function_call.name:
                            function_call_buffer["name"] += delta.function_call.name
                        if delta.function_call.arguments:
                            function_call_buffer["arguments"] += delta.function_call.arguments
                    
                    # Handle tool calls (newer OpenAI API format)
                    elif hasattr(delta, 'tool_calls') and delta.tool_calls:
                        collecting_function_call = True
                        for tool_call in delta.tool_calls:
                            if tool_call.function:
                                if tool_call.function.name:
                                    function_call_buffer["name"] += tool_call.function.name
                                if tool_call.function.arguments:
                                    function_call_buffer["arguments"] += tool_call.function.arguments
                    
                    # Handle regular content
                    else:
                        content = delta.content or ""
                        if content and content.strip():  # Filter out empty or whitespace-only content
                            buffer += content
                            if len(buffer) >= BUFFER_SIZE or any(
                                p in buffer for p in ["。", "！", "？", ".", "!", "?", "\n"]
                            ):
                                yield {"content": buffer}
                                buffer = ""
                            await asyncio.sleep(0.01)

                # Check if stream is finished
                if chunk.choices and chunk.choices[0].finish_reason:
                    finish_reason = chunk.choices[0].finish_reason
                    
                    # If we were collecting a function call, yield it now
                    if collecting_function_call and function_call_buffer["name"]:
                        try:
                            arguments = json.loads(function_call_buffer["arguments"]) if function_call_buffer["arguments"] else {}
                            yield {
                                "function_calls": [{
                                    "name": function_call_buffer["name"],
                                    "arguments": arguments
                                }]
                            }
                        except json.JSONDecodeError:
                            yield {
                                "function_calls": [{
                                    "name": function_call_buffer["name"],
                                    "arguments": function_call_buffer["arguments"]
                                }]
                            }
                    break

            # Output remaining buffer (only if not empty)
            if buffer and buffer.strip():
                yield {"content": buffer}

        except Exception as e:
            yield {"error": str(e)}

    def supports_function_calling(self) -> bool:
        """OpenAI supports function calling"""
        return True
