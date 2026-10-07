"""The text surface: structured extraction and plain text, on the model
`UTILS_TEXT_MODEL` routes to.

Both functions resolve the route once, build the request for that one entry
and return `None` when no model answered, never trying another entry (see
`config.llm`: two reports of one person must not be read by two models).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.config.llm import RouteSpec, no_provider_message, resolve_named, resolve_route

logger = logging.getLogger(__name__)


def _max_tokens_param(spec: RouteSpec) -> str:
    """OpenAI's current models reject `max_tokens` in favour of
    `max_completion_tokens`; every other OpenAI-compatible endpoint still
    takes `max_tokens` (DashScope and DeepSeek reject the new name)."""
    return "max_completion_tokens" if spec.api_key_env == "OPENAI_API_KEY" else "max_tokens"


def _route(provider: str | None, model_name: str | None, surface: str) -> RouteSpec | None:
    if provider:
        spec = resolve_named(provider, model=model_name)
        if spec is None:
            logger.error("named provider is not a MODELS entry or provider/model")
            return None
        if not spec.routable:
            logger.error("named provider has no key: model=%s key_name=%s",  # phi: ok configuration names
                         spec.model, spec.api_key_env)
            return None
        return spec
    spec = resolve_route(surface)
    if spec is None:
        # The sentence names the configuration key and the env vars that fix
        # it (`no_provider_message`), and nothing of a request.
        logger.error(no_provider_message(surface))  # phi: ok configuration names only
    return spec


def _for_endpoint(spec: RouteSpec, messages: list[dict], response_format: dict) -> tuple[list[dict], dict | None]:
    """The caller asked for a `json_schema`; return what this endpoint takes.

    `json_schema` passes through. Otherwise the schema goes into the system
    prompt and the parameter is downgraded to `json_object` (DeepSeek, whose
    JSON mode also REQUIRES the word "json" and an example in the prompt,
    which the schema text provides) or dropped entirely (`none`: Anthropic's
    compatibility endpoint rejects `json_object`, so the prompt is the only
    channel left).
    """
    if spec.response_format == "json_schema":
        return messages, response_format

    from .file_processors.results import _build_prompt_with_schema

    schema = (response_format.get("json_schema") or {}).get("schema")
    instruction = _build_prompt_with_schema("", schema).strip()
    out = [dict(m) for m in messages]
    if out and out[0].get("role") == "system":
        out[0]["content"] = f"{out[0].get('content', '')}\n\n{instruction}"
    else:
        out.insert(0, {"role": "system", "content": instruction})
    return out, ({"type": "json_object"} if spec.response_format == "json_object" else None)


def _request_kwargs(spec: RouteSpec, kwargs: dict[str, Any]) -> dict[str, Any]:
    """The per-call kwargs on top of the entry's own: the entry's
    `extra_body` (thinking switches), its temperature when the caller gave
    none, and the right spelling of max_tokens."""
    out = dict(kwargs)
    max_tokens_value = out.pop("max_tokens", None) or out.pop("max_completion_tokens", None)
    if max_tokens_value:
        out[_max_tokens_param(spec)] = max_tokens_value
    if spec.extra_body:
        out["extra_body"] = {**spec.extra_body, **(out.get("extra_body") or {})}
    if "temperature" not in out and spec.temperature is not None:
        out["temperature"] = spec.temperature
    if spec.reasoning_effort and "reasoning_effort" not in out:
        # Only when the entry declares it, so an endpoint that has never heard
        # of the parameter never sees it. `openai-utils` declares `none`
        # because GPT-6 Luna, like gpt-5.6-terra before it, otherwise keeps
        # reasoning on and then rejects the `temperature: 0` every extraction
        # caller sends.
        out["reasoning_effort"] = spec.reasoning_effort
    return out


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)


async def async_get_structured_output(
    messages: list[dict],
    response_format: dict,
    model_name: str | None = None,
    provider: str | None = None,
    **kwargs: Any,
) -> dict | None:
    """One JSON answer from the `UTILS_TEXT_MODEL` route (or the named
    `provider`, an entry name or `provider/model`, with `model_name`
    overriding its model).

    Returns the parsed dict, or None: for "no route is configured" (logged at
    ERROR with the sentence that fixes it), for a failed call, and for a
    refusal. Callers that must tell "no route" from "the call failed" ask
    `resolve_route("text")` first.
    """
    from .clients import client_manager
    from .file_processors.results import parse_json_answer

    start = time.monotonic()
    spec = _route(provider, model_name, "text")
    if spec is None:
        return None

    if spec.llm_type == "anthropic":
        from . import backends_anthropic

        return await backends_anthropic.structured_output(
            spec, messages, (response_format or {}).get("json_schema", {}).get("schema"), **kwargs
        )

    if (response_format or {}).get("type") == "json_schema":
        messages, response_format = _for_endpoint(spec, messages, response_format)

    logger.info("structured output: model=%s", spec.model)
    try:
        client = client_manager.for_spec(spec)
        params = _request_kwargs(spec, kwargs)
        if response_format:
            # Omitted, never `None`: the SDK sends an explicit null, and an
            # endpoint that validates the field 400s on it.
            params["response_format"] = response_format
        response = await client.chat.completions.create(
            model=spec.model,
            messages=messages,
            **params,
        )
        result = response.choices[0].message.to_dict()
        if result.get("refusal") is not None:
            logger.warning("structured output refused: model=%s", spec.model)
            return None
        content = result.get("content") or ""
        if not content.strip():
            # DeepSeek's JSON mode documents "empty content with some
            # probability"; an empty answer is a failed call, not an empty
            # document.
            logger.error("structured output was empty: model=%s", spec.model)
            return None
        # Cut off at max_tokens, the part that closed is kept; any other
        # malformed answer is still a failed call.
        cut = response.choices[0].finish_reason == "length"
        final_result = parse_json_answer(content, cut=cut)
        if cut:
            logger.warning("structured output hit max_tokens, its complete part kept: model=%s char_count=%d",
                           spec.model, len(content))
        logger.info("structured output: model=%s duration_ms=%d", spec.model, _ms(start))
        return final_result
    except Exception as e:
        logger.error("structured output failed: model=%s error_type=%s duration_ms=%d", spec.model,
                     type(e).__name__, _ms(start), exc_info=not is_driver_exception(e))
        return None


async def async_get_text_completion(
    messages: list[dict],
    model_name: str | None = None,
    provider: str | None = None,
    **kwargs: Any,
) -> str | None:
    """Plain text (titles, summaries, profile prose) from the `UTILS_TEXT_MODEL`
    route, or the named `provider`. None when no model answered."""
    from .clients import client_manager

    start = time.monotonic()
    spec = _route(provider, model_name, "text")
    if spec is None:
        return None
    if spec.llm_type == "anthropic":
        from . import backends_anthropic

        return await backends_anthropic.text_completion(spec, messages, **kwargs)
    logger.info("text completion: model=%s", spec.model)
    try:
        client = client_manager.for_spec(spec)
        response = await client.chat.completions.create(
            model=spec.model,
            messages=messages,
            **_request_kwargs(spec, kwargs),
        )
        logger.info("text completion: model=%s duration_ms=%d", spec.model, _ms(start))
        return response.choices[0].message.content
    except Exception as e:
        logger.error("text completion failed: model=%s error_type=%s duration_ms=%d", spec.model,
                     type(e).__name__, _ms(start), exc_info=not is_driver_exception(e))
        return None
