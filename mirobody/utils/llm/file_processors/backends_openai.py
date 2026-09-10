"""The OpenAI-compatible vision path — every entry the vision route can name.

OpenRouter, DashScope, OpenAI, DeepSeek and Google's compatibility endpoint all
speak chat/completions with an `image_url` part, so there is one
implementation; what differs per entry (endpoint, key, model, the `extra_body`
that turns thinking off) arrives in the `RouteSpec`. PDFs are rendered to
page images here for everyone — no compatibility endpoint takes a PDF part
(Google's rejects `type: file`; DeepSeek documents images only).

Failures are ERRORS here, not empty strings. The image path used to catch
every exception and return "", so a provider's `400 This model does not
support image` — the exact answer a text-only model gives — was
indistinguishable from a blank page, and the upload above it reported success
over zero indicators (#68). A page that fails inside a multi-page PDF is still
skipped with a warning, because one bad page must not lose the other twenty;
a PDF whose EVERY page fails raises, for the same reason a single image does.
"""

from __future__ import annotations

import asyncio
import logging
import pathlib
import time
from typing import Any

from openai import AsyncOpenAI

from mirobody.utils.config.llm import RouteSpec

from .media import (
    _build_vision_message,
    _convert_pdf_to_base64_images,
    _read_and_optimize_image,
)
from .results import _build_prompt_with_schema, _merge_page_results, clean_json_response
from ...file_types import IMAGE_EXTENSIONS

logger = logging.getLogger(__name__)


def _api_params(spec: RouteSpec, messages: list[dict], json_mode: bool) -> dict[str, Any]:
    params: dict[str, Any] = {"model": spec.model, "messages": messages}
    if spec.extra_body:
        params["extra_body"] = dict(spec.extra_body)
    if json_mode and spec.takes_json_object:
        # An entry that says `response_format: none` gets the JSON instruction
        # from the prompt only — Anthropic's compatibility endpoint answers
        # `json_object` with a 400 rather than ignoring it.
        params["response_format"] = {"type": "json_object"}
    return params


async def _process_pdf(
    pdf_path: str, prompt: str, client: AsyncOpenAI, spec: RouteSpec,
    max_concurrency: int = 5, json_mode: bool = True,
) -> str:
    """One request per rendered page, concurrently."""
    provider_name, model_name = spec.alias, spec.model
    logger.info(f"Processing PDF with {provider_name} ({model_name}): {pdf_path}, json_mode={json_mode}")
    total_start = time.time()

    conversion_start = time.time()
    page_images = _convert_pdf_to_base64_images(pdf_path)
    logger.info(f"All {len(page_images)} pages converted in {time.time() - conversion_start:.2f}s")

    semaphore = asyncio.Semaphore(max_concurrency)

    async def process_page(page_info: dict[str, Any]) -> dict[str, Any]:
        page_num = page_info['page_num']
        async with semaphore:
            try:
                api_start = time.time()
                messages = _build_vision_message(page_info['base64_image'], prompt, json_mode)
                response = await client.chat.completions.create(**_api_params(spec, messages, json_mode))
                logger.info(f"Page {page_num} completed in {time.time() - api_start:.2f}s")
                return {'page': page_num, 'content': response.choices[0].message.content, 'api_duration': time.time() - api_start}
            except Exception as e:
                logger.error(f"Page {page_num} API call failed: {e}")
                return {'page': page_num, 'error': str(e)}

    all_results = sorted(await asyncio.gather(*(process_page(p) for p in page_images)), key=lambda x: x['page'])

    if all_results and all('error' in r for r in all_results):
        raise RuntimeError(
            f"{provider_name} ({model_name}): every page of {pathlib.Path(pdf_path).name} failed — "
            f"first error: {all_results[0]['error']}"
        )

    logger.info(f"All {provider_name} API calls completed in {time.time() - total_start:.2f}s")
    return _merge_page_results(all_results, json_mode)


async def _process_image(
    image_path: str, prompt: str, client: AsyncOpenAI, spec: RouteSpec, json_mode: bool = True,
) -> str:
    """One image, one request. Raises on a failed request."""
    provider_name, model_name = spec.alias, spec.model
    logger.info(f"Processing image with {provider_name} ({model_name}): {image_path}, json_mode={json_mode}")
    start_time = time.time()

    base64_image, stats = _read_and_optimize_image(image_path)
    logger.info(f"Image optimization took: {time.time() - start_time:.2f}s, {stats}")

    api_start = time.time()
    messages = _build_vision_message(base64_image, prompt, json_mode)
    response = await client.chat.completions.create(**_api_params(spec, messages, json_mode))
    logger.info(f"{provider_name} API completed in {time.time() - api_start:.2f}s")

    result = response.choices[0].message.content
    if not (result or "").strip():
        # A text-only model handed an image answers with an EMPTY STRING
        # through some gateways (OpenRouter, measured 2026-09-09) where the
        # vendor's own endpoint answers 400. One photo of a report is never
        # blank, so "no text" for a single image is the model's inability, not
        # the document's emptiness — and it has to say so, or the upload above
        # reports success over nothing (#68).
        raise RuntimeError(
            f"{provider_name} ({model_name}) returned no text for the image — a model that cannot "
            f"read images answers this way; point UTILS_VISION_MODEL at an entry with supports_image: true"
        )
    return clean_json_response(result) if json_mode else result


async def openai_compatible_file_extract(
    local_file_path: str,
    prompt: str,
    spec: RouteSpec,
    client: AsyncOpenAI | None = None,
    response_schema: Any | None = None,
    json_mode: bool = True
) -> str:
    """File extraction through the entry `spec` resolves to.

    The client comes from `client_manager` (so `<PREFIX>_BASE_URL` applies —
    #52) unless the caller passes one. Errors propagate with the entry and
    model named.
    """
    if client is None:
        # Function-local: clients.py builds SDK clients, and a module-scope
        # import here would close the loop back through llm/__init__.
        from ..clients import client_manager

        client = client_manager.for_spec(spec)
    file_path = pathlib.Path(local_file_path)
    try:
        if not file_path.exists():
            raise FileNotFoundError(local_file_path)
        final_prompt = _build_prompt_with_schema(prompt, response_schema) if json_mode and response_schema else prompt
        file_ext = file_path.suffix.lower()
        if file_ext == '.pdf':
            return await _process_pdf(str(file_path), final_prompt, client, spec, json_mode=json_mode)
        if file_ext in IMAGE_EXTENSIONS:
            return await _process_image(str(file_path), final_prompt, client, spec, json_mode=json_mode)
        raise ValueError(f"unsupported file type for vision extraction: {file_ext}")
    except Exception as e:
        provider_name, model_name = spec.alias, spec.model
        logger.error(f"{provider_name} extraction failed ({model_name}): {e}", stack_info=True)
        raise ValueError(f"{provider_name} ({model_name}) failed: {e}") from e
