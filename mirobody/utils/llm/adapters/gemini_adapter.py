"""
Google Gemini Model Adapter

Handles special logic and format conversion for Gemini models
"""

import asyncio
import time
from typing import Any, AsyncGenerator, Dict, List

from google.genai import types

from mirobody.utils.llm.clients import client_manager
from .base import BaseModelAdapter


class GeminiAdapter(BaseModelAdapter):
    """Gemini model adapter"""

    def format_messages(self, messages: List[Dict]) -> List[Dict]:
        """Convert message format to Gemini format"""
        gemini_messages = []
        for msg in messages:
            role = "user" if msg["role"] == "user" else "model"
            gemini_messages.append({"role": role, "parts": [{"text": msg["content"]}]})
        return gemini_messages

    def convert_function_format(self, functions: List[Dict]) -> Dict[str, Any]:
        """Convert OpenAI functions format to Gemini tools format"""
        if not functions:
            return {}

        tools = [
            {
                "function_declarations": [
                    {"name": func["name"], "description": func["description"], "parameters": func["parameters"]}
                    for func in functions
                ]
            }
        ]
        return {"tools": tools}

    def set_default_parameters(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Set Gemini-specific default parameters"""
        defaults = {
            "temperature": 0.3,  # Gemini prefers slightly higher temperature
        }

        # Merge user parameters, user parameters take priority
        for key, value in defaults.items():
            kwargs.setdefault(key, value)

        return kwargs

    async def batch_completion(self, messages: List[Dict], **kwargs) -> Dict[str, Any]:
        """Gemini batch completion"""
        t_start = time.time()

        try:
            # Set default parameters
            kwargs = self.set_default_parameters(kwargs)

            # Get client
            client = client_manager.get_async_gemini_client()

            # Handle function format conversion
            config_kwargs = {}
            if "functions" in kwargs:
                tool_config = self.convert_function_format(kwargs["functions"])
                config_kwargs.update(tool_config)
                # Remove original parameters
                kwargs.pop("functions", None)
                kwargs.pop("function_call", None)

            # Extract temperature
            if "temperature" in kwargs:
                config_kwargs["temperature"] = kwargs.pop("temperature")

            # Format messages
            gemini_messages = self.format_messages(messages)

            # Create config
            config = types.GenerateContentConfig(**config_kwargs)

            # Call API
            response = await client.models.generate_content(model=self.model, contents=gemini_messages, config=config)

            # Log usage
            self.log_usage({"total_tokens": response.usage_metadata.total_token_count}, time.time() - t_start)

            # Process response
            result = {"content": "", "function_calls": None}

            if response.candidates and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if hasattr(part, "text") and part.text:
                        result["content"] += part.text
                    elif hasattr(part, "function_call") and part.function_call:
                        if not result["function_calls"]:
                            result["function_calls"] = []
                        result["function_calls"].append(
                            {
                                "name": part.function_call.name,
                                "arguments": dict(part.function_call.args) if part.function_call.args else {},
                            }
                        )

            return result

        except Exception as e:
            return self.handle_error(e, "batch completion")

    async def stream_completion(self, messages: List[Dict], **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """Gemini streaming completion"""
        try:
            # Set default parameters
            kwargs = self.set_default_parameters(kwargs)

            # Get client
            client = client_manager.get_async_gemini_client()

            # Handle function format conversion
            config_kwargs = {}
            if "functions" in kwargs:
                tool_config = self.convert_function_format(kwargs["functions"])
                config_kwargs.update(tool_config)
                kwargs.pop("functions", None)
                kwargs.pop("function_call", None)

            # Extract temperature
            if "temperature" in kwargs:
                config_kwargs["temperature"] = kwargs.pop("temperature")

            # Format messages
            gemini_messages = self.format_messages(messages)

            # Create config
            config = types.GenerateContentConfig(**config_kwargs)

            # Stream call
            stream = await client.models.generate_content_stream(
                model=self.model,
                contents=gemini_messages,
                config=config,
            )

            buffer = ""
            BUFFER_SIZE = 8

            async for chunk in stream:
                if hasattr(chunk, "candidates") and chunk.candidates:
                    for candidate in chunk.candidates:
                        if hasattr(candidate, "content") and hasattr(candidate.content, "parts"):
                            for part in candidate.content.parts:
                                if hasattr(part, "text") and part.text:
                                    content = part.text
                                    if content:
                                        buffer += content
                                        if len(buffer) >= BUFFER_SIZE or any(
                                            p in buffer for p in ["。", "！", "？", ".", "!", "?", "\n"]
                                        ):
                                            yield {"content": buffer}
                                            buffer = ""
                                        await asyncio.sleep(0.01)

            # Output remaining buffer
            if buffer:
                yield {"content": buffer}

        except Exception as e:
            yield {"error": str(e)}

    def supports_function_calling(self) -> bool:
        """Gemini supports tool calling"""
        return True
