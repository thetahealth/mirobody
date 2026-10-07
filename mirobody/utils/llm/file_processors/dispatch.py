"""The vision surface: one image → the model's answer, on the model `UTILS_VISION_MODEL` routes to.

The route is data (`config.llm.yaml`); this module resolves it once and hands
the image to the entry's backend. It used to carry its own table of providers
and their models, and "can this provider read an image" was expressed by
membership in that table, which is how a `DEEPSEEK_API_KEY`-only deployment
came to upload report photos into silence (#68). Now an entry says
`supports_image`, the route lists entries, and the answer to "why no OCR" is
a sentence naming the key and the file to edit.

Failures are ERRORS here, never empty strings: a provider's `400 This model
does not support image` used to come back as "" and read exactly like a blank
page, and the upload above it reported success over zero indicators (#68).
"""

from __future__ import annotations

import asyncio
import logging

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.config.llm import (
    NoProviderError,
    RouteSpec,
    no_provider_message,
    resolve_named,
    resolve_route,
)
from mirobody.utils.llm import backends_anthropic
from mirobody.utils.llm_output import strip_code_fence

from .backends_openai import image_extract
from .media import model_ready
from .results import json_prompt

logger = logging.getLogger(__name__)

class ImageNotRead(RuntimeError):
    """The vision model answered an image with nothing at all. A text-only
    model does that through some gateways (OpenRouter, measured 2026-09-09)
    where the vendor's own endpoint answers 400; a model that reads the image
    and finds no text says so (`documents.ocr.NO_TEXT`). The message is the
    fix, for the person who uploaded the image."""

    def __init__(self) -> None:
        super().__init__("The vision model returned no text for the image, which is how a model that cannot "
                         "read images answers: point UTILS_VISION_MODEL at an entry with supports_image: true.")


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


async def vision_extract(
    image: bytes,
    mime: str,
    prompt: str,
    *,
    provider: str | None = None,
    model: str | None = None,
    json_mode: bool = False,
    max_tokens: int | None = None,
) -> str:
    """The vision route's model's answer to `prompt` about one image.

    `provider` names a MODELS entry or `provider/model` over the route, and
    `model` the model of the resolved entry. With `json_mode` the prompt asks
    for JSON and the answer comes back without its code fence. `max_tokens`
    caps the answer: a document-OCR pass sets it, so a model that loops stops
    there (`documents.ocr.OCR_MAX_TOKENS`).

    Raises `NoProviderError` (a ValueError) naming the fix when no entry is
    routable, `ValueError` for a named provider that is unknown or keyless,
    `ImageNotRead` for an empty answer, and whatever the endpoint raises for
    a failed request.
    """
    spec = vision_route(provider, model)
    ready = await asyncio.to_thread(model_ready, image, mime)
    final_prompt = json_prompt(prompt) if json_mode else prompt
    logger.info("vision extraction: model=%s json_mode=%s", spec.model, json_mode)
    try:
        if spec.llm_type == "anthropic":
            # An `llm_type: anthropic` entry speaks the native API; its
            # module docstring says why not the compatibility endpoint.
            answer = await backends_anthropic.image_extract(spec, ready, final_prompt, max_tokens=max_tokens)
        else:
            answer = await image_extract(spec, ready, final_prompt, json_mode=json_mode, max_tokens=max_tokens)
    except Exception as e:
        logger.error("vision extraction failed: model=%s error_type=%s", spec.model, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        raise
    if not answer.strip():
        logger.error("vision extraction answered nothing: model=%s", spec.model)
        raise ImageNotRead()
    return strip_code_fence(answer) if json_mode else answer
