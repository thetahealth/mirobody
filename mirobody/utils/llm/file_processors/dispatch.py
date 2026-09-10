"""The vision surface: one file → its text, on the model `UTILS_VISION_MODEL` routes to.

The route is data (`config.llm.yaml`); this module resolves it once and hands
the file to the one OpenAI-compatible backend. It used to carry its own table
of providers and their models, and "can this provider read an image" was
expressed by membership in that table — which is how a `DEEPSEEK_API_KEY`-only
deployment came to upload report photos into silence (#68). Now an entry says
`supports_image`, the route lists entries, and the answer to "why no OCR" is
a sentence naming the key and the file to edit.
"""

from __future__ import annotations

import logging
from typing import Any

from mirobody.utils.config.llm import (
    NoProviderError,
    RouteSpec,
    no_provider_message,
    resolve_named,
    resolve_route,
)

from .backends_openai import openai_compatible_file_extract

logger = logging.getLogger(__name__)


def vision_route(provider: str | None = None, model: str | None = None) -> RouteSpec:
    """The spec the vision surface uses: the named entry / `provider/model`
    when the caller gives one, else `UTILS_VISION_MODEL`'s first present key.

    Raises `NoProviderError` (a ValueError) with the fix when there is none,
    `ValueError` when the caller named an entry that does not exist or whose
    key is missing.
    """
    if provider:
        spec = resolve_named(provider, model=model)
        if spec is None:
            raise ValueError(f"Unknown provider: {provider} (not a MODELS entry or provider/model)")
        if not spec.routable:
            raise ValueError(f"API key not configured for '{provider}' (env: {spec.api_key_env})")
        return spec
    spec = resolve_route("vision")
    if spec is None:
        raise NoProviderError(no_provider_message("vision"))
    if model:
        spec = RouteSpec(**{**spec.__dict__, "model": model})
    return spec


async def unified_file_extract(
    file_path: str,
    prompt: str,
    content_type: str = "image/jpeg",
    model: str | None = None,
    config: Any | None = None,
    provider: str | None = None,
    json_mode: bool | None = None
) -> str:
    """
    Read an image or PDF with the vision route's model.

    Args:
        file_path: Path to the file
        prompt: Extraction prompt
        content_type: MIME type (kept for callers; the backend sniffs the file)
        model: Model id override for the resolved entry
        config: An object with `response_schema` / `response_mime_type`
            attributes (the historical Gemini config shape); the schema is
            written into the prompt, the mime type turns json_mode on
        provider: A MODELS entry name or `provider/model` (overrides the route)
        json_mode: Force JSON output (None = infer from `config`)

    Returns:
        Extracted content (JSON string or plain text)

    Raises:
        NoProviderError (a ValueError): no routable entry — the message names
            the fix. ValueError: the named provider is unknown or keyless.
        Whatever the endpoint raises for a failed request: a 400/404 for a
            model that cannot read images used to come back as "" and look
            exactly like a blank page (#68); it is an error now.
    """
    spec = vision_route(provider, model)

    response_schema = getattr(config, 'response_schema', None) if config else None
    if json_mode is None:
        json_mode = bool(
            response_schema or (config and getattr(config, 'response_mime_type', None) == "application/json")
        )

    provider_name, model_name = spec.alias, spec.model
    logger.info(f"unified_file_extract: {provider_name}, model={model_name}, json_mode={json_mode}")
    if spec.llm_type == "anthropic":
        # The native API, for the structured outputs its OpenAI-compatible
        # endpoint does not serve — see `utils/llm/backends_anthropic`.
        from ..backends_anthropic import file_extract

        return await file_extract(spec, file_path, prompt, response_schema=response_schema, json_mode=json_mode)
    return await openai_compatible_file_extract(
        local_file_path=file_path,
        prompt=prompt,
        spec=spec,
        response_schema=response_schema,
        json_mode=json_mode,
    )
