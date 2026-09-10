"""The text surface: structured extraction and plain text, on the model
`UTILS_TEXT_MODEL` routes to.

Both functions resolve the route once, build the request for that one entry
and return `None` when no model answered — never trying another entry (see
`config.llm`: two reports of one person must not be read by two models).
"""

import json
import logging
from typing import Any

from ..config.llm import (
    RouteSpec,
    no_provider_message,
    resolve_named,
    resolve_route,
)

logger = logging.getLogger(__name__)

# `PROJECT_DIR`, `os` and `uuid` used to be here to give `async_get_openai_tts`
# somewhere to write its .mp3 — the only thing in this module that ever touched
# the filesystem, and a function no caller ever invoked. All four went together.
# `get_openai_chat` (a hardcoded `["gpt-4o", "gpt-4.1"]` allowlist, zero
# callers) and `async_get_doubao_structured_output` (a vendor SDK declared and
# never installed) went the same way. The Gemini SDK branch went last: Google's
# OpenAI-compatible endpoint serves the same models, and one request shape is
# one place for a bug to be.


def _max_tokens_param(spec: RouteSpec) -> str:
    """OpenAI's current models reject `max_tokens` in favour of
    `max_completion_tokens`; every other OpenAI-compatible endpoint still
    takes `max_tokens` (DashScope and DeepSeek reject the new name)."""
    return "max_completion_tokens" if spec.api_key_env == "OPENAI_API_KEY" else "max_tokens"


def _route(provider: str | None, model_name: str | None, surface: str) -> RouteSpec | None:
    if provider:
        spec = resolve_named(provider, model=model_name)
        if spec is None:
            logger.error(f"Unknown provider: {provider} (not a MODELS entry or provider/model)")
            return None
        if not spec.routable:
            key_id = spec.api_key_env
            logger.error(f"Provider {provider}: {key_id} is not set")
            return None
        return spec
    spec = resolve_route(surface)
    if spec is None:
        reason = no_provider_message(surface)
        logger.error(reason)
    return spec


def _for_endpoint(spec: RouteSpec, messages: list[dict], response_format: dict) -> tuple[list[dict], dict | None]:
    """The caller asked for a `json_schema`; return what this endpoint takes.

    `json_schema` passes through. Otherwise the schema goes into the system
    prompt and the parameter is downgraded to `json_object` (DeepSeek, whose
    JSON mode also REQUIRES the word "json" and an example in the prompt —
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
    return out


async def async_get_structured_output(
    messages: list[dict],
    response_format: dict,
    model_name: str | None = None,
    provider: str | None = None,
    **kwargs
) -> dict | None:
    """One JSON answer from the `UTILS_TEXT_MODEL` route (or the named
    `provider`, an entry name or `provider/model`, with `model_name`
    overriding its model).

    Returns the parsed dict, or None — for "no route is configured" (logged at
    ERROR with the sentence that fixes it), for a failed call, and for a
    refusal. Callers that must tell "no route" from "the call failed" ask
    `resolve_route("text")` first.
    """
    import time
    from .clients import client_manager
    from .file_processors.results import clean_json_response

    start_time = time.time()
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

    provider_name, model_name = spec.alias, spec.model
    logger.info(f"async_get_structured_output: {provider_name}, model: {model_name}")
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
            logger.warning(f"structured output refused by {provider_name}")
            return None
        content = result.get("content") or ""
        if not content.strip():
            # DeepSeek's JSON mode documents "empty content with some
            # probability"; an empty answer is a failed call, not an empty
            # document.
            logger.error(f"structured output from {provider_name} ({model_name}) was empty")
            return None
        # A model told to answer in JSON by the PROMPT (every entry below
        # `response_format: json_schema`) wraps it in a ```json fence — measured
        # on Anthropic's compatibility endpoint, 2026-09-10. The vision path has
        # always stripped it; this one used to hand the fence to `json.loads`.
        # A no-op on a real json_schema answer, which never starts with a fence.
        final_result = json.loads(clean_json_response(content))
        duration = time.time() - start_time
        logger.info(f"{provider_name} structured output completed, duration: {duration:.3f}s")
        return final_result
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Structured output API error ({provider_name}, {model_name}): {type(e).__name__}: {str(e)}, duration: {duration:.3f}s")
        return None


async def async_get_text_completion(
    messages: list[dict],
    model_name: str | None = None,
    provider: str | None = None,
    **kwargs
) -> str | None:
    """Plain text (titles, summaries, profile prose) from the `UTILS_TEXT_MODEL`
    route, or the named `provider`. None when no model answered."""
    import time
    from .clients import client_manager

    start_time = time.time()
    spec = _route(provider, model_name, "text")
    if spec is None:
        return None
    if spec.llm_type == "anthropic":
        from . import backends_anthropic

        return await backends_anthropic.text_completion(spec, messages, **kwargs)
    provider_name, model_name = spec.alias, spec.model
    logger.info(f"async_get_text_completion: {provider_name}, model: {model_name}")
    try:
        client = client_manager.for_spec(spec)
        response = await client.chat.completions.create(
            model=spec.model,
            messages=messages,
            **_request_kwargs(spec, kwargs),
        )
        content = response.choices[0].message.content
        duration = time.time() - start_time
        logger.info(f"{provider_name} text generation completed, duration: {duration:.3f}s")
        return content
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Text generation API error ({provider_name}, {model_name}): {type(e).__name__}: {str(e)}, duration: {duration:.3f}s", stack_info=True)
        return None
