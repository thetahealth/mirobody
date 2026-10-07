"""The utility surfaces on Anthropic's own API: `llm_type: anthropic`.

Why not the OpenAI-compatible endpoint, which this project speaks everywhere
else. Anthropic documents that layer as a way to *test and compare model
capabilities*, not as a production API, and the difference shows on exactly
the thing extraction depends on. Measured against the live endpoint,
2026-09-10:

* `response_format: {"type": "json_object"}` is REFUSED: 400, "Input should
  be 'json_schema'". Not ignored, as the compatibility page says: refused.
* `response_format: {"type": "json_schema", ...}` is accepted only in OpenAI
  strict mode: `strict: true` plus `additionalProperties: false` on every
  object, which the extraction schemas do not carry.
* So the only channel left there is asking for JSON in the prompt, and the
  answer comes back inside a ```json fence, parsed or not depending on the
  model's mood. An indicator extractor that depends on a mood is issue #68
  with extra steps.

The native API has the real thing: `output_config.format` constrains decoding
to the schema, so the text block IS valid JSON (`structured_output` below).
`anthropic.transform_schema` adapts our schemas to what the grammar compiler
accepts, it adds `additionalProperties: false`, drops the constraints the
compiler rejects (`minimum`, `maxLength`, …) and folds them into descriptions.

Everything else is deliberately the same as `file_processors/backends_openai`:
one image per vision request, and a failed call is an ERROR, never an empty
string that reads like a blank page.
"""

from __future__ import annotations

import json
import logging
import time
from functools import lru_cache
from typing import Any

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.config.llm import RouteSpec
from mirobody.utils.llm import clients
from mirobody.utils.llm.file_processors.media import VisionImage
from mirobody.utils.llm.file_processors.results import parse_json_answer

logger = logging.getLogger(__name__)

#: The native API REQUIRES `max_tokens`; the OpenAI-compatible one defaults it.
#: The vision path names none, and one page of a dense panel is a few thousand
#: tokens of JSON: with a constrained grammar, hitting the cap truncates into
#: invalid JSON rather than into a short answer, so the default is generous.
DEFAULT_MAX_TOKENS = 16384

#-----------------------------------------------------------------------------
# OpenAI-shaped input → Anthropic-shaped request.

def _split_system(messages: list[dict]) -> tuple[str, list[dict]]:
    """Anthropic takes ONE system prompt, out of the message list. Every
    system/developer message is concatenated into it in order: the same rule
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


@lru_cache(maxsize=1)
def _accepted_params() -> frozenset[str]:
    """What THIS installed SDK's `messages.stream` takes: the call `_create`
    actually makes.

    Not a hardcoded allow-list, because the parameter set moves: 1.5.0 has no
    `temperature`, `top_p` or `top_k` at all, they were removed, and passing
    one raises `TypeError` before a request is built. Every caller in this
    repository still hands us `temperature=0` (`indicator_extractor`,
    `handlers/base`, `summary`, `profile`), which is how an ANTHROPIC_API_KEY
    deployment extracted zero indicators out of a report it had just accepted.

    Reading the signature means the next removal costs a DEBUG line rather than
    an outage, and it lets the project follow the newest SDK, which is where
    the newest models are supported.
    """
    import inspect

    from anthropic.resources.messages import AsyncMessages

    return frozenset(inspect.signature(AsyncMessages.stream).parameters) - {"self"}


def _request_params(spec: RouteSpec, kwargs: dict[str, Any]) -> dict[str, Any]:
    """The per-call parameters: the entry's own, then the caller's, minus
    anything this SDK will not take.

    An `extra_body` on an `llm_type: anthropic` entry holds NATIVE top-level
    parameters (`thinking`, `output_config`, …), which is what `extra_body`
    means on the OpenAI SDK too: fields merged into the request body.
    """
    out: dict[str, Any] = dict(spec.extra_body or {})
    if spec.temperature is not None:
        out["temperature"] = spec.temperature
    out.update(kwargs)
    max_tokens = out.pop("max_completion_tokens", None) or out.pop("max_tokens", None)
    out["max_tokens"] = int(max_tokens or DEFAULT_MAX_TOKENS)

    accepted = _accepted_params()
    dropped = sorted(k for k in out if k not in accepted)
    for key in dropped:
        del out[key]
    if dropped:
        # DEBUG, not WARNING: a caller asking for `temperature=0` on a model
        # that has no temperature is expressing an intent the endpoint already
        # honours, not making a mistake worth a line per request.
        logger.debug("anthropic: dropped %s (not in this SDK's messages.stream)", ", ".join(dropped))  # phi: ok parameter names
    return out


async def _create(spec: RouteSpec, **params):
    """One request, always through the streaming helper.

    Not `messages.create(...)`: the non-streaming call refuses a `max_tokens`
    large enough that the request "may take longer than 10 minutes" (measured
    on claude-haiku-4-5, 20000 passes and 32000 raises) and the extraction
    callers ask for 32000 (`indicator_extractor`, `handlers/base`). Clamping
    would silently truncate a long report into invalid JSON under a
    constrained grammar; streaming removes the ceiling instead, and
    `get_final_message()` hands back the same Message object either way.
    """
    async with clients.client_manager.anthropic_for_spec(spec).messages.stream(**params) as stream:
        return await stream.get_final_message()


def _text_of(response) -> str:
    """The answer: every text block, joined. A thinking block is not text and
    never reaches the caller."""
    return "".join(block.text for block in response.content if getattr(block, "type", "") == "text")


#-----------------------------------------------------------------------------
# The three surfaces.

async def structured_output(spec: RouteSpec, messages: list[dict], schema: dict | None, **kwargs) -> dict | None:
    """One JSON answer, constrained by `schema`. None when the call failed.

    With `output_config.format` the model cannot emit anything but a document
    matching the schema, so the text block parses as it is, unless it was cut
    at max_tokens: then its complete part is kept, as on the OpenAI path.
    """
    from anthropic import transform_schema

    start = time.monotonic()
    system, converted = _split_system(messages)
    params = _request_params(spec, kwargs)
    if schema:
        params.setdefault("output_config", {"format": {"type": "json_schema", "schema": transform_schema(schema)}})
    try:
        response = await _create(spec, model=spec.model, messages=converted,
                                 **({"system": system} if system else {}), **params)
        content = _text_of(response)
        if not content.strip():
            # `max_tokens` reached before any text, or a refusal: either way
            # the caller must not read it as an empty document (#68).
            logger.error("structured output was empty: model=%s stop_reason=%s", spec.model, response.stop_reason)
            return None
        cut = response.stop_reason == "max_tokens"
        result = parse_json_answer(content, cut=cut)
        if cut:
            logger.warning("structured output hit max_tokens, its complete part kept: model=%s char_count=%d",
                           spec.model, len(content))
        logger.info("structured output: model=%s duration_ms=%d", spec.model, _ms(start))
        return result
    except Exception as e:
        logger.error("structured output failed: model=%s error_type=%s duration_ms=%d", spec.model,
                     type(e).__name__, _ms(start), exc_info=not is_driver_exception(e))
        return None


async def text_completion(spec: RouteSpec, messages: list[dict], **kwargs) -> str | None:
    """Plain text (titles, summaries, profile prose). None when the call failed."""
    start = time.monotonic()
    system, converted = _split_system(messages)
    try:
        response = await _create(spec, model=spec.model, messages=converted,
                                 **({"system": system} if system else {}),
                                 **_request_params(spec, kwargs))
        logger.info("text completion: model=%s duration_ms=%d", spec.model, _ms(start))
        return _text_of(response)
    except Exception as e:
        logger.error("text completion failed: model=%s error_type=%s duration_ms=%d", spec.model,
                     type(e).__name__, _ms(start), exc_info=not is_driver_exception(e))
        return None


async def image_extract(spec: RouteSpec, image: VisionImage, prompt: str, *, max_tokens: int | None = None) -> str:
    """One image read by a Claude model: the answer as it wrote it, "" for
    none. A failed request raises, the contract `backends_openai` keeps for
    #68."""
    params = _request_params(spec, {"max_tokens": max_tokens} if max_tokens else {})
    messages = [{"role": "user", "content": [
        {"type": "image", "source": {"type": "base64", "media_type": image.mime, "data": image.data}},
        {"type": "text", "text": prompt},
    ]}]
    return _text_of(await _create(spec, model=spec.model, messages=messages, **params))


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)
