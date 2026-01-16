"""
Volcengine/Doubao Model Adapter

Handles special logic and parameters for Volcengine models
"""

import aiohttp, asyncio, json, logging, time

from typing import Any, AsyncGenerator, Dict, List
from openai import AsyncOpenAI

from .base import BaseModelAdapter


class VolcengineAdapter(BaseModelAdapter):
    """Volcengine model adapter"""

    def set_default_parameters(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Set Volcengine-specific default parameters"""
        defaults = {
            "max_tokens": 12000,  # Maximum token limit for Doubao model
            "temperature": 0.5,
            "stream": False,
            # "thinking": {"type": "disabled"},  # Volcengine-specific parameter
        }

        # Merge user parameters, user parameters take priority
        for key, value in defaults.items():
            kwargs.setdefault(key, value)

        return kwargs

    async def batch_completion(self, messages: List[Dict], **kwargs) -> Dict[str, Any]:
        """Volcengine batch completion"""
        t_start = time.time()

        try:
            kwargs = self.set_default_parameters(kwargs)

            url = f"{self.api_base}{self.config['api_path']}"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            }

            data = {"model": self.model, "messages": self.format_messages(messages), **kwargs}

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers, timeout=60) as response:

                    if response.status != 200:
                        response_text = await response.text()
                        logging.error(f"Volcengine API error: {response.status}, {response_text}")
                        return {"content": "", "function_calls": None}

                    response_data = await response.json()

                    if "usage" in response_data:
                        self.log_usage(
                            {"total_tokens": response_data["usage"].get("total_tokens", "N/A")}, time.time() - t_start
                        )

                    choice = response_data["choices"][0]
                    result = {"content": choice["message"]["content"] or "", "function_calls": None}

                    if kwargs.get("functions") and choice["message"].get("function_call"):
                        result["function_calls"] = [
                            {
                                "name": choice["message"]["function_call"]["name"],
                                "arguments": json.loads(choice["message"]["function_call"]["arguments"]),
                            }
                        ]

                    return result

        except Exception as e:
            return self.handle_error(e, "batch completion")

    async def stream_completion(self, messages: List[Dict], **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """Volcengine streaming completion"""
        try:
            # Set default parameters
            kwargs = self.set_default_parameters(kwargs)
            kwargs["stream"] = True
            
            client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=self.api_base
            )
            
            resp = await client.chat.completions.create(
                model=self.model,
                messages=self.format_messages(messages),
                **kwargs
            )
            
            buffer = ""
            thinking_buffer = ""
            BUFFER_SIZE = 15  # Further increase buffer size
            async for chunk in resp:
                delta = chunk.choices[0].delta
                if hasattr(delta, "reasoning_content"):
                    thinking_buffer += delta.reasoning_content
                    if len(thinking_buffer) >= BUFFER_SIZE or (
                        len(thinking_buffer) > 10
                        and delta.reasoning_content.endswith(("。", "！", "？", ".", "!", "?"))
                    ):
                        yield {"reasoning": thinking_buffer}
                        thinking_buffer = ""
                elif delta.content:
                    if thinking_buffer:
                        yield {"reasoning": thinking_buffer}
                        thinking_buffer = ""
                    buffer += delta.content
                    should_send = (
                        len(buffer) >= BUFFER_SIZE
                        or (
                            len(buffer) > 10
                            and delta.content.endswith(("。", "！", "？", ".", "!", "?"))
                        )
                        or delta.content.endswith("\n\n")  # Paragraph end
                    )
                    if should_send:
                        yield {"content": buffer}
                        buffer = ""
            if buffer:
                yield {"content": buffer}
        except Exception as e:
            yield {"error": str(e)}


    def supports_function_calling(self) -> bool:
        """Volcengine partially supports function calling"""
        return True  # Need to confirm based on specific model
