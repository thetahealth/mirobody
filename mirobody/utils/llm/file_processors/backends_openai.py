"""The OpenAI-compatible vision path — OpenRouter, Qwen and Doubao.

All three speak chat/completions with an image_url part, so they share one
implementation and differ only in client construction and a per-provider
`extra_body` that turns "thinking" off (it costs latency and buys nothing for
extraction). Gemini is NOT here: it has its own SDK and its own PDF handling,
in `gemini.py`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from openai import AsyncOpenAI

if TYPE_CHECKING:
    from volcenginesdkarkruntime import AsyncArk

from mirobody.utils.config import safe_read_cfg

from ..config import AIConfig
from .media import (
    _build_vision_message,
    _convert_pdf_to_base64_images,
    _read_and_optimize_image,
)
from .results import _build_prompt_with_schema, _merge_page_results

# Provider-specific extra parameters for API calls (no thinking, for latency).
PROVIDER_EXTRA_PARAMS: Dict[str, Dict[str, Any]] = {
    "openrouter": {"extra_body": {"reasoning": {"enabled": False}}},
    "qwen": {"extra_body": {"enable_thinking": False}},
    "doubao": {"thinking": {"type": "disabled"}},
}

# =============================================================================

async def _openai_compatible_process_pdf(
    pdf_path: str,
    prompt: str,
    client: Union[AsyncOpenAI, "AsyncArk"],
    model: str,
    provider: str,
    max_concurrency: int = 5,
    json_mode: bool = True
) -> str:
    """Process PDF with OpenAI-compatible API (OpenRouter/Qwen/Doubao)."""
    logging.info(f"Processing PDF with {provider}: {pdf_path}, json_mode={json_mode}")
    total_start = time.time()

    # Phase 1: Convert PDF to images
    conversion_start = time.time()
    page_images = _convert_pdf_to_base64_images(pdf_path)
    logging.info(f"All {len(page_images)} pages converted in {time.time() - conversion_start:.2f}s")

    # Phase 2: Concurrent API calls
    semaphore = asyncio.Semaphore(max_concurrency)
    extra_params = PROVIDER_EXTRA_PARAMS.get(provider, {})

    async def process_page(page_info: Dict[str, Any]) -> Dict[str, Any]:
        page_num = page_info['page_num']
        async with semaphore:
            try:
                logging.info(f"Calling {provider} API for page {page_num}...")
                api_start = time.time()

                messages = _build_vision_message(page_info['base64_image'], prompt, json_mode)
                api_params = {"model": model, "messages": messages, **extra_params}
                if json_mode:
                    api_params["response_format"] = {"type": "json_object"}

                response = await client.chat.completions.create(**api_params)

                logging.info(f"Page {page_num} completed in {time.time() - api_start:.2f}s")
                return {
                    'page': page_num,
                    'content': response.choices[0].message.content,
                    'api_duration': time.time() - api_start
                }
            except Exception as e:
                logging.error(f"Page {page_num} API call failed: {e}")
                return {'page': page_num, 'error': str(e)}

    tasks = [process_page(page_info) for page_info in page_images]
    all_results = sorted(await asyncio.gather(*tasks), key=lambda x: x['page'])

    logging.info(f"All {provider} API calls completed in {time.time() - total_start:.2f}s")
    return _merge_page_results(all_results, json_mode)


async def _openai_compatible_process_image(
    image_path: str,
    prompt: str,
    client: Union[AsyncOpenAI, "AsyncArk"],
    model: str,
    provider: str,
    json_mode: bool = True
) -> str:
    """Process image with OpenAI-compatible API (OpenRouter/Qwen/Doubao)."""
    logging.info(f"Processing image with {provider}: {image_path}, json_mode={json_mode}")
    start_time = time.time()

    base64_image, stats = _read_and_optimize_image(image_path)
    logging.info(f"Image optimization took: {time.time() - start_time:.2f}s, {stats}")

    try:
        api_start = time.time()
        messages = _build_vision_message(base64_image, prompt, json_mode)
        extra_params = PROVIDER_EXTRA_PARAMS.get(provider, {})
        api_params = {"model": model, "messages": messages, **extra_params}
        if json_mode:
            api_params["response_format"] = {"type": "json_object"}

        response = await client.chat.completions.create(**api_params)

        logging.info(f"{provider} API completed in {time.time() - api_start:.2f}s")
        result = response.choices[0].message.content
        return clean_json_response(result) if json_mode and result else (result or "")

    except Exception as e:
        logging.error(f"{provider} API call failed: {e}")
        return ""


async def _openai_compatible_file_extract(
    local_file_path: str,
    prompt: str,
    model: str,
    client: Union[AsyncOpenAI, "AsyncArk"],
    provider: str,
    response_schema: Optional[Any] = None,
    json_mode: bool = True
) -> str:
    """Unified file extraction for OpenAI-compatible providers."""
    file_path = pathlib.Path(local_file_path)
    if not file_path.exists():
        logging.error(f"File not found: {local_file_path}")
        return ""

    # Embed schema in prompt if provided
    final_prompt = _build_prompt_with_schema(prompt, response_schema) if json_mode and response_schema else prompt
    file_ext = file_path.suffix.lower()

    if file_ext == '.pdf':
        return await _openai_compatible_process_pdf(
            str(file_path), final_prompt, client, model, provider, json_mode=json_mode
        )
    elif file_ext in IMAGE_EXTENSIONS:
        return await _openai_compatible_process_image(
            str(file_path), final_prompt, client, model, provider, json_mode=json_mode
        )
    else:
        logging.warning(f"Unsupported file type: {file_ext}")
        return ""



# =============================================================================

def _get_openrouter_client() -> AsyncOpenAI:
    """Get OpenRouter client."""
    return client_manager.get_async_openrouter_client()


def _get_qwen_client() -> AsyncOpenAI:
    """Get Qwen client (OpenAI compatible)."""
    api_key = safe_read_cfg("DASHSCOPE_API_KEY")
    if not api_key:
        raise ValueError("DASHSCOPE_API_KEY not configured")
    return AsyncOpenAI(
        api_key=api_key,
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
    )


def _get_doubao_client() -> "AsyncArk":
    """Get Doubao client."""
    from volcenginesdkarkruntime import AsyncArk
    config = AIConfig.get_provider_config("volcengine")
    return AsyncArk(api_key=config["api_key"], base_url=config["api_base"])


# =============================================================================
# Public API - Provider-Specific Extractors
# =============================================================================

async def doubao_file_extract(
    local_file_path: str,
    prompt: str = "Please extract all test indicators from this report and return the result in JSON format",
    model: str = "doubao-1-5-ui-tars-250428",
    client: Optional["AsyncArk"] = None,
    json_mode: bool = True
) -> str:
    """Doubao file extraction, supports PDF and image files."""
    try:
        client = client or _get_doubao_client()
        return await _openai_compatible_file_extract(
            local_file_path, prompt, model, client, "doubao", json_mode=json_mode
        )
    except Exception as e:
        logging.error(f"Doubao extraction failed: {e}", stack_info=True)
        raise ValueError(f"Doubao API failed: {e}") from e


async def qwen_file_extract(
    local_file_path: str,
    prompt: str = "Please extract all test indicators from this report and return the result in JSON format",
    model: str = "qwen3-vl-plus",
    client: Optional[AsyncOpenAI] = None,
    response_schema: Optional[Any] = None,
    json_mode: bool = True
) -> str:
    """Qwen file extraction using OpenAI-compatible API."""
    try:
        client = client or _get_qwen_client()
        return await _openai_compatible_file_extract(
            local_file_path, prompt, model, client, "qwen",
            response_schema=response_schema, json_mode=json_mode
        )
    except Exception as e:
        logging.error(f"Qwen extraction failed: {e}", stack_info=True)
        raise ValueError(f"Qwen API failed: {e}") from e


async def vision_file_extract(
    local_file_path: str,
    prompt: str = "Please extract all test indicators from this report and return the result in JSON format",
    model: str = "google/gemini-3-flash-preview",
    client: Optional[AsyncOpenAI] = None,
    response_schema: Optional[Any] = None,
    json_mode: bool = True
) -> str:
    """OpenRouter file extraction using vision model."""
    try:
        client = client or _get_openrouter_client()
        return await _openai_compatible_file_extract(
            local_file_path, prompt, model, client, "openrouter",
            response_schema=response_schema, json_mode=json_mode
        )
    except Exception as e:
        logging.error(f"OpenRouter extraction failed: {e}", stack_info=True)
        raise ValueError(f"OpenRouter API failed: {e}") from e


