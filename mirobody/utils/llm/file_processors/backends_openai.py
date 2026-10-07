"""The OpenAI-compatible vision request: every entry the vision route can name.

OpenRouter, DashScope, OpenAI, DeepSeek and Google's compatibility endpoint all
speak chat/completions with an `image_url` part, so there is one
implementation; what differs per entry (endpoint, key, model, the `extra_body`
that turns thinking off) arrives in the `RouteSpec`.
"""

from __future__ import annotations

from typing import Any

from mirobody.utils.config.llm import RouteSpec
from mirobody.utils.llm import clients
from mirobody.utils.llm.utils import _max_tokens_param

from .media import VisionImage


def _api_params(spec: RouteSpec, messages: list[dict], json_mode: bool,
                max_tokens: int | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {"model": spec.model, "messages": messages}
    if max_tokens:
        params[_max_tokens_param(spec)] = max_tokens
    if spec.extra_body:
        params["extra_body"] = dict(spec.extra_body)
    if spec.reasoning_effort:
        # Declared on the entry, so it reaches the vision surface too, this is
        # the parameter `openai-utils` needs, and it was being dropped here as
        # well as on the text path.
        params["reasoning_effort"] = spec.reasoning_effort
    if json_mode and spec.takes_json_object:
        # An entry that says `response_format: none` gets the JSON instruction
        # from the prompt only: Anthropic's compatibility endpoint answers
        # `json_object` with a 400 rather than ignoring it.
        params["response_format"] = {"type": "json_object"}
    return params


async def image_extract(spec: RouteSpec, image: VisionImage, prompt: str, *, json_mode: bool,
                        max_tokens: int | None = None) -> str:
    """One request: the image, then the prompt. The answer as the model wrote
    it, "" for none; a failed request raises."""
    messages = [{
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": f"data:{image.mime};base64,{image.data}"}},
            {"type": "text", "text": prompt},
        ],
    }]
    response = await clients.client_manager.for_spec(spec).chat.completions.create(
        **_api_params(spec, messages, json_mode, max_tokens))
    return response.choices[0].message.content or ""
