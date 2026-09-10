"""The utility surfaces on Anthropic's own API — `llm_type: anthropic`.

Why not the OpenAI-compatible endpoint, which this project speaks everywhere
else. Anthropic documents that layer as a way to *test and compare model
capabilities*, not as a production API, and the difference shows on exactly
the thing extraction depends on. Measured against the live endpoint,
2026-09-10:

* `response_format: {"type": "json_object"}` is REFUSED — 400, "Input should
  be 'json_schema'". Not ignored, as the compatibility page says: refused.
* `response_format: {"type": "json_schema", ...}` is accepted only in OpenAI
  strict mode — `strict: true` plus `additionalProperties: false` on every
  object — which the extraction schemas do not carry.
* So the only channel left there is asking for JSON in the prompt, and the
  answer comes back inside a ```json fence, parsed or not depending on the
  model's mood. An indicator extractor that depends on a mood is issue #68
  with extra steps.

The native API has the real thing: `output_config.format` constrains decoding
to the schema, so the text block IS valid JSON (`structured_output` below).
`anthropic.transform_schema` adapts our schemas to what the grammar compiler
accepts — it adds `additionalProperties: false`, drops the constraints the
compiler rejects (`minimum`, `maxLength`, …) and folds them into descriptions.

Everything else is deliberately the same as `file_processors/backends_openai`:
PDFs are rendered to page images and merged page by page, a failed page inside
a multi-page PDF is a warning, a failed single image is an ERROR — never an
empty string that reads like a blank page.
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any

from ..config.llm import RouteSpec

logger = logging.getLogger(__name__)

#: The native API REQUIRES `max_tokens`; the OpenAI-compatible one defaults it.
#: The vision path names none, and one page of a dense panel is a few thousand
#: tokens of JSON — with a constrained grammar, hitting the cap truncates into
#: invalid JSON rather than into a short answer, so the default is generous.
DEFAULT_MAX_TOKENS = 16384

_clients: dict[tuple[str, str], Any] = {}


def client_for(spec: RouteSpec):
    """The cached `AsyncAnthropic` for a resolved route, keyed like
    `AIClientManager`: one per (endpoint, key name)."""
    key = spec.key
    if spec.api_key_env and not key:
        raise ValueError(f"{spec.alias}: {spec.api_key_env} is not set")
    cache_key = (spec.base_url, spec.api_key_env)
    if cache_key not in _clients:
        from anthropic import AsyncAnthropic

        _clients[cache_key] = AsyncAnthropic(api_key=key or "-", base_url=spec.base_url or None)
    return _clients[cache_key]


#-----------------------------------------------------------------------------
# OpenAI-shaped input → Anthropic-shaped request.

def _split_system(messages: list[dict]) -> tuple[str, list[dict]]:
    """Anthropic takes ONE system prompt, out of the message list. Every
    system/developer message is concatenated into it in order — the same rule
    the vendor's own compatibility layer applies."""
    system: list[str] = []
    rest: list[dict] = []
    for message in messages:
        role = message.get("role")
        if role in ("system", "developer"):
            content = message.get("content")
            system.append(content if isinstance(content, str) else json.dumps(content, ensure_ascii=False))
        else:
            rest.append({"role": "assistant" if role == "assistant" else "user", "content": message.get("content")})
    return "\n\n".join(s for s in system if s), rest


def _request_params(spec: RouteSpec, kwargs: dict[str, Any]) -> dict[str, Any]:
    """The per-call parameters: the entry's own, then the caller's.

    An `extra_body` on an `llm_type: anthropic` entry holds NATIVE top-level
    parameters (`thinking`, `output_config`, …) — which is what `extra_body`
    means on the OpenAI SDK too: fields merged into the request body.
    """
    out: dict[str, Any] = dict(spec.extra_body or {})
    if spec.temperature is not None:
        out["temperature"] = spec.temperature
    out.update(kwargs)
    max_tokens = out.pop("max_completion_tokens", None) or out.pop("max_tokens", None)
    out["max_tokens"] = int(max_tokens or DEFAULT_MAX_TOKENS)
    return out


def _text_of(response) -> str:
    """The answer: every text block, joined. A thinking block is not text and
    never reaches the caller."""
    return "".join(block.text for block in response.content if getattr(block, "type", "") == "text")


#-----------------------------------------------------------------------------
# The three surfaces.

async def structured_output(spec: RouteSpec, messages: list[dict], schema: dict | None, **kwargs) -> dict | None:
    """One JSON answer, constrained by `schema`. None when the call failed.

    With `output_config.format` the model cannot emit anything but a document
    matching the schema, so the text block parses without cleaning.
    """
    from anthropic import transform_schema

    provider_name, model_name = spec.alias, spec.model
    start = time.time()
    system, converted = _split_system(messages)
    params = _request_params(spec, kwargs)
    if schema:
        params.setdefault("output_config", {"format": {"type": "json_schema", "schema": transform_schema(schema)}})
    try:
        response = await client_for(spec).messages.create(
            model=spec.model, messages=converted, **({"system": system} if system else {}), **params,
        )
        content = _text_of(response)
        if not content.strip():
            # `max_tokens` reached before any text, or a refusal: either way
            # the caller must not read it as an empty document (#68).
            logger.error(f"structured output from {provider_name} ({model_name}) was empty (stop_reason={response.stop_reason})")
            return None
        result = json.loads(content)
        duration = time.time() - start
        logger.info(f"{provider_name} structured output completed, duration: {duration:.3f}s")
        return result
    except Exception as e:
        duration = time.time() - start
        # The vendor's own error text is the sentence that says WHY extraction
        # produced nothing, which is the whole point of #68.
        logger.error(f"Structured output API error ({provider_name}, {model_name}): {type(e).__name__}: {e}, duration: {duration:.3f}s")  # phi: ok vendor error, never document contents
        return None


async def text_completion(spec: RouteSpec, messages: list[dict], **kwargs) -> str | None:
    """Plain text (titles, summaries, profile prose). None when the call failed."""
    provider_name, model_name = spec.alias, spec.model
    start = time.time()
    system, converted = _split_system(messages)
    try:
        response = await client_for(spec).messages.create(
            model=spec.model, messages=converted,
            **({"system": system} if system else {}), **_request_params(spec, kwargs),
        )
        duration = time.time() - start
        logger.info(f"{provider_name} text generation completed, duration: {duration:.3f}s")
        return _text_of(response)
    except Exception as e:
        duration = time.time() - start
        logger.error(f"Text generation API error ({provider_name}, {model_name}): {type(e).__name__}: {e}, duration: {duration:.3f}s")  # phi: ok vendor error, never document contents
        return None


def _image_message(base64_jpeg: str, prompt: str) -> list[dict]:
    return [{"role": "user", "content": [
        {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": base64_jpeg}},
        {"type": "text", "text": prompt},
    ]}]


async def _one_image(spec: RouteSpec, base64_jpeg: str, prompt: str, schema: dict | None, json_mode: bool) -> str:
    params = _request_params(spec, {})
    if json_mode and schema:
        from anthropic import transform_schema

        params.setdefault("output_config", {"format": {"type": "json_schema", "schema": transform_schema(schema)}})
    response = await client_for(spec).messages.create(
        model=spec.model, messages=_image_message(base64_jpeg, prompt), **params,
    )
    return _text_of(response)


async def file_extract(
    spec: RouteSpec,
    local_file_path: str,
    prompt: str,
    response_schema: Any | None = None,
    json_mode: bool = True,
) -> str:
    """An image or PDF read by a Claude model. Raises with the entry named on
    failure — the contract `backends_openai` established for #68."""
    import asyncio

    from .file_processors.media import _convert_pdf_to_base64_images, _read_and_optimize_image
    from .file_processors.results import _build_prompt_with_schema, _merge_page_results, clean_json_response

    provider_name, model_name = spec.alias, spec.model
    file_path = pathlib.Path(local_file_path)
    schema = response_schema if isinstance(response_schema, dict) else None
    # A schema the grammar can hold is enforced by `output_config`; anything
    # else (a Gemini-shaped schema object) still travels in the prompt.
    final_prompt = prompt if schema else (_build_prompt_with_schema(prompt, response_schema) if json_mode else prompt)
    try:
        if not file_path.exists():
            raise FileNotFoundError(local_file_path)
        suffix = file_path.suffix.lower()
        if suffix == ".pdf":
            pages = _convert_pdf_to_base64_images(str(file_path))
            logger.info(f"Processing PDF with {provider_name} ({model_name}): {len(pages)} pages, json_mode={json_mode}")
            semaphore = asyncio.Semaphore(5)

            async def one_page(page: dict) -> dict:
                async with semaphore:
                    try:
                        text = await _one_image(spec, page["base64_image"], final_prompt, schema, json_mode)
                        return {"page": page["page_num"], "content": text}
                    except Exception as e:
                        page_number = page["page_num"]
                        logger.error(f"Page {page_number} API call failed: {e}")  # phi: ok vendor error; the page content is never logged
                        return {"page": page["page_num"], "error": str(e)}

            results = sorted(await asyncio.gather(*(one_page(p) for p in pages)), key=lambda r: r["page"])
            if results and all("error" in r for r in results):
                raise RuntimeError(
                    f"{provider_name} ({model_name}): every page of {file_path.name} failed — "
                    f"first error: {results[0]['error']}"
                )
            return _merge_page_results(results, json_mode)

        from ..file_types import IMAGE_EXTENSIONS

        if suffix not in IMAGE_EXTENSIONS:
            raise ValueError(f"unsupported file type for vision extraction: {suffix}")
        logger.info(f"Processing image with {provider_name} ({model_name}): {file_path}, json_mode={json_mode}")  # phi: ok a server-side temp path, not content
        base64_jpeg, stats = _read_and_optimize_image(str(file_path))
        logger.info(f"Image optimization: {stats}")  # phi: ok byte sizes and pixel dimensions only
        result = await _one_image(spec, base64_jpeg, final_prompt, schema, json_mode)
        if not result.strip():
            raise RuntimeError(
                f"{provider_name} ({model_name}) returned no text for the image — a model that cannot "
                f"read images answers this way; point UTILS_VISION_MODEL at an entry with supports_image: true"
            )
        return clean_json_response(result) if json_mode else result
    except Exception as e:
        logger.error(f"{provider_name} extraction failed ({model_name}): {e}", stack_info=True)  # phi: ok vendor error, never document contents
        raise ValueError(f"{provider_name} ({model_name}) failed: {e}") from e
