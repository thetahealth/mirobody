"""
File Processing Module

Provides unified file extraction using various vision model providers.
Automatically selects provider based on configured API keys.
"""

import asyncio
import base64
import io
import json
import logging
import pathlib
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pypdfium2 as pdfium
from google.genai import types
from openai import AsyncOpenAI
from PIL import Image
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from volcenginesdkarkruntime import AsyncArk

from mirobody.utils.config import safe_read_cfg

from .clients import client_manager
from .config import AIConfig


# =============================================================================
# Constants
# =============================================================================

IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.bmp', '.gif', '.webp'}

# Provider-specific extra parameters for API calls (no thinking to improve latency )
PROVIDER_EXTRA_PARAMS: Dict[str, Dict[str, Any]] = {
    "openrouter": {"extra_body": {"reasoning": {"enabled": False}}},
    "qwen": {"extra_body": {"enable_thinking": False}},
    "doubao": {"thinking": {"type": "disabled"}},
}


# =============================================================================
# Provider Configuration
# =============================================================================

class VisionProviderConfig:
    """Vision provider configuration with auto-selection based on API keys."""

    VISION_PROVIDERS: List[Dict[str, Any]] = [
        {
            "name": "gemini",
            "api_key_env": "GOOGLE_API_KEY",
            "default_model": "gemini-3-flash-preview",
            "description": "Google Gemini (Direct API)",
        },
        {
            "name": "openrouter",
            "api_key_env": "OPENROUTER_API_KEY",
            "default_model": "google/gemini-3-flash-preview",
            "description": "OpenRouter (OpenAI Compatible)",
        },
        {
            "name": "qwen",
            "api_key_env": "DASHSCOPE_API_KEY",
            "default_model": "qwen3-vl-flash",
            "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
            "description": "Qwen Vision (Alibaba Dashscope)",
        },
        {
            "name": "doubao",
            "api_key_env": "VOLCENGINE_API_KEY",
            "default_model": "doubao-seed-1-6-vision-250815",
            "description": "Doubao/Volcengine Vision",
        },
    ]

    @classmethod
    def get_available_provider(cls) -> Optional[Dict[str, Any]]:
        """Get first available provider with configured API key."""
        for provider in cls.VISION_PROVIDERS:
            if safe_read_cfg(provider["api_key_env"]):
                logging.info(f"🔍 Vision provider selected: {provider['name']} ({provider['description']})")
                return provider
        return None

    @classmethod
    def get_provider_by_name(cls, name: str) -> Optional[Dict[str, Any]]:
        """Get provider configuration by name."""
        for provider in cls.VISION_PROVIDERS:
            if provider["name"] == name:
                return provider
        return None

    @classmethod
    def list_available_providers(cls) -> List[str]:
        """List all providers with configured API keys."""
        return [p["name"] for p in cls.VISION_PROVIDERS if safe_read_cfg(p["api_key_env"])]

    @classmethod
    def get_provider_status(cls) -> Dict[str, bool]:
        """Get availability status of all providers."""
        return {p["name"]: bool(safe_read_cfg(p["api_key_env"])) for p in cls.VISION_PROVIDERS}


# =============================================================================
# Utility Functions
# =============================================================================

def clean_json_response(response: str) -> str:
    """Remove markdown code block markers from LLM response."""
    if not response:
        return response
    response = response.strip()
    if response.startswith('```json'):
        response = response[7:]
    elif response.startswith('```'):
        response = response[3:]
    if response.endswith('```'):
        response = response[:-3]
    return response.strip()


def _build_prompt_with_schema(prompt: str, response_schema: Optional[Any] = None) -> str:
    """Embed response_schema into prompt for providers without native schema support."""
    if not response_schema:
        return prompt + "\n\nPlease return the result in JSON format."

    try:
        if hasattr(response_schema, 'to_dict'):
            schema_dict = response_schema.to_dict()
        elif hasattr(response_schema, '__dict__'):
            schema_dict = response_schema.__dict__
        elif isinstance(response_schema, dict):
            schema_dict = response_schema
        else:
            schema_dict = str(response_schema)

        schema_str = json.dumps(schema_dict, indent=2, ensure_ascii=False)
        return f"""{prompt}

Please return the result in JSON format that strictly follows this schema:
```json
{schema_str}
```"""
    except Exception as e:
        logging.warning(f"Failed to serialize response_schema: {e}")
        return prompt + "\n\nPlease return the result in JSON format."


def _merge_json_results(json_strings: List[str]) -> str:
    """Merge multiple JSON results into one combined result."""
    merged = {}
    for json_str in json_strings:
        try:
            data = json.loads(json_str)
            if not isinstance(data, dict):
                continue
            for key, value in data.items():
                if key not in merged:
                    merged[key] = value
                elif isinstance(value, list) and isinstance(merged[key], list):
                    merged[key].extend(value)
                elif isinstance(value, dict) and isinstance(merged[key], dict):
                    for k, v in value.items():
                        if k not in merged[key] or not merged[key][k]:
                            merged[key][k] = v
                elif not merged[key] and value:
                    merged[key] = value
        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse JSON: {e}, content: {json_str[:100]}...")
    return json.dumps(merged, ensure_ascii=False)


def _merge_page_results(all_results: List[Dict[str, Any]], json_mode: bool) -> str:
    """Merge page-by-page results into final output."""
    if not all_results:
        logging.warning("No valid analysis results obtained")
        return ""

    valid_contents = []
    for result in all_results:
        if 'content' in result and result['content']:
            valid_contents.append({'page': result.get('page', 0), 'content': result['content']})
        elif 'error' in result:
            logging.warning(f"Page {result.get('page', 0)} extraction failed: {result['error']}")

    if not valid_contents:
        logging.warning("No valid content extracted from any page")
        return ""

    # Single page: return directly
    if len(valid_contents) == 1:
        content = valid_contents[0]['content']
        return clean_json_response(content) if json_mode else content

    # Multiple pages: merge based on mode
    if json_mode:
        cleaned_contents = [clean_json_response(vc['content']) for vc in valid_contents]
        combined = _merge_json_results(cleaned_contents)
        logging.info(f"Merged {len(valid_contents)} pages JSON results")
        return combined
    else:
        text_parts = [f"[Page {vc['page']}]\n{vc['content']}" for vc in valid_contents]
        combined = "\n\n".join(text_parts)
        logging.info(f"Combined {len(valid_contents)} pages, total {len(combined)} characters")
        return combined


# =============================================================================
# Image Processing
# =============================================================================

class FileProcessor:
    """Base file processor with image optimization."""

    @staticmethod
    def optimize_image_for_llm(
        image_data: bytes,
        max_dimension: int = 2048,
        quality: int = 85,
        format: str = "JPEG"
    ) -> Tuple[bytes, dict]:
        """Optimize image by reducing resolution and applying compression."""
        try:
            start_time = time.time()
            original_size = len(image_data)

            img = Image.open(io.BytesIO(image_data))
            original_width, original_height = img.size

            # Convert to RGB if necessary
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = background
            elif img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')

            # Resize if too large
            width, height = img.size
            if width > max_dimension or height > max_dimension:
                scale = min(max_dimension / width, max_dimension / height)
                new_width, new_height = int(width * scale), int(height * scale)
                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                logging.info(f"📐 Image resized: {original_width}x{original_height} → {new_width}x{new_height}")

            # Save optimized image
            output = io.BytesIO()
            save_kwargs = {"format": format, "quality": quality, "optimize": True}
            if format == "JPEG":
                save_kwargs["progressive"] = True
            img.save(output, **save_kwargs)

            optimized_data = output.getvalue()
            compression_ratio = (1 - len(optimized_data) / original_size) * 100

            stats = {
                "original_size": original_size,
                "optimized_size": len(optimized_data),
                "compression_ratio": compression_ratio,
                "original_dimensions": (original_width, original_height),
                "optimized_dimensions": img.size,
                "processing_time": time.time() - start_time
            }
            logging.info(f"✅ Image optimized: {original_size/1024:.1f}KB → {len(optimized_data)/1024:.1f}KB "
                        f"({compression_ratio:.1f}% reduction)")
            return optimized_data, stats

        except Exception as e:
            logging.warning(f"Image optimization failed: {e}")
            return image_data, {"error": str(e), "original_size": len(image_data)}


# =============================================================================
# Common Processing Utilities
# =============================================================================

def _convert_pdf_to_base64_images(pdf_path: str, scale: float = 1.5) -> List[Dict[str, Any]]:
    """Convert PDF pages to optimized base64 images."""
    pdf = pdfium.PdfDocument(pdf_path)
    page_images = []

    for page_num in range(len(pdf)):
        page_start = time.time()
        page = pdf[page_num]
        bitmap = page.render(scale=scale)
        pil_image = bitmap.to_pil()

        img_buffer = io.BytesIO()
        pil_image.save(img_buffer, format="JPEG", quality=90)
        img_data = img_buffer.getvalue()

        optimized_data, stats = FileProcessor.optimize_image_for_llm(
            img_data, max_dimension=1536, quality=85
        )
        base64_image = base64.b64encode(optimized_data).decode('utf-8')

        conversion_time = time.time() - page_start
        page_images.append({
            'page_num': page_num + 1,
            'base64_image': base64_image,
            'conversion_time': conversion_time,
            'stats': stats
        })
        logging.info(f"Page {page_num + 1} converted in {conversion_time:.2f}s")

    pdf.close()
    return page_images


def _build_vision_message(base64_image: str, prompt: str, json_mode: bool) -> List[Dict]:
    """Build OpenAI-compatible vision message."""
    text_content = f"{prompt}. Please return the result in JSON format." if json_mode else prompt
    return [{
        "role": "user",
        "content": [
            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}},
            {"type": "text", "text": text_content}
        ]
    }]


def _read_and_optimize_image(image_path: str) -> Tuple[str, dict]:
    """Read image file and return optimized base64 string."""
    with open(image_path, "rb") as f:
        img_data = f.read()
    optimized_data, stats = FileProcessor.optimize_image_for_llm(
        img_data, max_dimension=1536, quality=85
    )
    return base64.b64encode(optimized_data).decode('utf-8'), stats


# =============================================================================
# OpenAI-Compatible Provider Processing (OpenRouter/Qwen/Doubao)
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
# Gemini-Specific Processing
# =============================================================================

async def _gemini_process_pdf_by_pages(
    pdf_path: str,
    prompt: str,
    client,
    config,
    model: str,
    max_concurrency: int = 8
) -> str:
    """Process PDF page-by-page with Gemini API."""
    logging.info(f"Processing PDF with Gemini: {pdf_path}")
    total_start = time.time()

    # Extract each page as separate PDF
    pdf = pdfium.PdfDocument(pdf_path)
    page_pdfs = []

    for page_num in range(len(pdf)):
        new_pdf = pdfium.PdfDocument.new()
        new_pdf.import_pages(pdf, [page_num])
        pdf_buffer = io.BytesIO()
        new_pdf.save(pdf_buffer)
        page_pdfs.append({'page_num': page_num + 1, 'pdf_data': pdf_buffer.getvalue()})
        new_pdf.close()

    pdf.close()
    logging.info(f"Extracted {len(page_pdfs)} pages")

    # Concurrent API calls
    semaphore = asyncio.Semaphore(max_concurrency)

    async def process_page(page_info: Dict[str, Any]) -> Dict[str, Any]:
        page_num = page_info['page_num']
        async with semaphore:
            try:
                api_start = time.time()
                response = await client.models.generate_content(
                    model=model,
                    contents=[
                        types.Part.from_bytes(data=page_info['pdf_data'], mime_type="application/pdf"),
                        prompt,
                    ],
                    config=config,
                )

                if not response or not hasattr(response, "text") or response.text is None:
                    return {'page': page_num, 'error': 'Empty response'}

                if hasattr(response, "candidates") and response.candidates:
                    finish_reason = getattr(response.candidates[0], "finish_reason", None)
                    if finish_reason in ["SAFETY", "BLOCKED"]:
                        return {'page': page_num, 'error': 'Safety blocked'}

                logging.info(f"Page {page_num} completed in {time.time() - api_start:.2f}s")
                return {'page': page_num, 'content': response.text}

            except Exception as e:
                logging.error(f"Page {page_num} failed: {e}")
                return {'page': page_num, 'error': str(e)}

    tasks = [process_page(page_info) for page_info in page_pdfs]
    all_results = sorted(await asyncio.gather(*tasks), key=lambda x: x['page'])

    logging.info(f"Gemini processing completed in {time.time() - total_start:.2f}s")

    # Determine JSON mode from config
    is_json_mode = config and hasattr(config, 'response_mime_type') and config.response_mime_type == "application/json"
    return _merge_page_results(all_results, is_json_mode)


def _handle_gemini_error(error_msg: str) -> ValueError:
    """Map Gemini API errors to appropriate ValueError messages."""
    error_mappings = [
        ("User location is not supported", "User location is not supported for the API use"),
        ("400", "Configuration error"),
        ("FAILED_PRECONDITION", "Configuration error"),
        ("401", "Authentication error"),
        ("403", "Authentication error"),
        ("429", "Rate limit exceeded"),
    ]
    for key, msg in error_mappings:
        if key in error_msg:
            return ValueError(f"Gemini API {msg}: {error_msg}")
    return ValueError(f"Gemini API failed: {error_msg}")


async def gemini_file_extract(
    file_path: str,
    content_type: str,
    prompt: str,
    config: Optional[types.GenerateContentConfig] = None,
    model: str = "gemini-3-flash-preview"
) -> str:
    """Extract file content using Gemini model (supports PDF natively)."""
    try:
        filepath = pathlib.Path(file_path)
        if not filepath.exists():
            logging.error(f"File not found: {file_path}")
            return ""

        client = client_manager.get_async_gemini_client()

        if config is None:
            config = types.GenerateContentConfig(
                temperature=0.1,
                thinking_config=types.ThinkingConfig(thinking_level="minimal")
            )

        # PDF: use page-by-page processing
        if content_type == "application/pdf" or filepath.suffix.lower() == ".pdf":
            return await _gemini_process_pdf_by_pages(str(filepath), prompt, client, config, model)

        # Other files: process directly
        file_data = filepath.read_bytes()

        # Optimize images
        if content_type and content_type.startswith('image/'):
            file_data, stats = FileProcessor.optimize_image_for_llm(
                file_data, max_dimension=1536, quality=85
            )
            logging.info(f"Gemini: Image optimized, {stats}")

        response = await client.models.generate_content(
            model=model,
            contents=[types.Part.from_bytes(data=file_data, mime_type=content_type), prompt],
            config=config,
        )

        # Validate response
        if not response or not hasattr(response, "text") or response.text is None:
            logging.error("Gemini API returned empty response")
            return ""

        if hasattr(response, "candidates") and response.candidates:
            finish_reason = getattr(response.candidates[0], "finish_reason", None)
            if finish_reason in ["SAFETY", "BLOCKED", "OTHER"]:
                logging.warning(f"Content blocked: {finish_reason}")
                return ""

        return response.text or ""

    except Exception as e:
        error_msg = str(e)
        logging.error(f"Gemini processing failed: {type(e).__name__}: {error_msg}", stack_info=True)
        raise _handle_gemini_error(error_msg) from e


async def gemini_multi_file_extract(
    files: List[Dict[str, str]],
    prompt: str,
    config: Optional[types.GenerateContentConfig] = None,
    model: str = "gemini-3-flash-preview",
) -> str:
    """Process multiple files at once with Gemini."""
    if not files:
        raise ValueError("Files list cannot be empty")

    # Validate files
    for idx, file in enumerate(files):
        if not isinstance(file, dict) or "path" not in file or "mime_type" not in file:
            raise ValueError(f"File at index {idx} must have 'path' and 'mime_type' keys")
        if not pathlib.Path(file["path"]).exists():
            raise ValueError(f"File does not exist: {file['path']}")

    client = client_manager.get_async_gemini_client()
    config = config or types.GenerateContentConfig(temperature=0.1)

    # Build contents
    contents = []
    for file in files:
        file_data = pathlib.Path(file["path"]).read_bytes()
        if file["mime_type"] and file["mime_type"].startswith('image/'):
            file_data, stats = FileProcessor.optimize_image_for_llm(
                file_data, max_dimension=1536, quality=85
            )
            logging.info(f"Gemini: Image {file['path']} optimized, {stats}")
        contents.append(types.Part.from_bytes(data=file_data, mime_type=file["mime_type"]))
    contents.append(prompt)

    try:
        response = await client.models.generate_content(model=model, contents=contents, config=config)

        if not response or not hasattr(response, "text") or response.text is None:
            logging.error("Gemini API returned empty response")
            return ""

        if hasattr(response, "candidates") and response.candidates:
            finish_reason = getattr(response.candidates[0], "finish_reason", None)
            if finish_reason in ["SAFETY", "BLOCKED", "OTHER"]:
                logging.warning(f"Content blocked: {finish_reason}")
                return ""

        return response.text or ""

    except Exception as e:
        error_msg = str(e)
        logging.error(f"Gemini multi-file processing failed: {error_msg}", stack_info=True)
        raise _handle_gemini_error(error_msg) from e


# =============================================================================
# Provider-Specific Client Factories
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


# =============================================================================
# Unified Entry Point with Handler Map
# =============================================================================

async def _handle_gemini(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for Gemini provider."""
    return await gemini_file_extract(
        file_path=file_path,
        content_type=content_type,
        prompt=prompt,
        config=config,
        model=model
    )


async def _handle_openrouter(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for OpenRouter provider."""
    return await vision_file_extract(
        local_file_path=file_path,
        prompt=prompt,
        model=model,
        response_schema=response_schema,
        json_mode=json_mode
    )


async def _handle_qwen(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for Qwen provider."""
    return await qwen_file_extract(
        local_file_path=file_path,
        prompt=prompt,
        model=model,
        response_schema=response_schema,
        json_mode=json_mode
    )


async def _handle_doubao(
    file_path: str, prompt: str, content_type: str, model: str,
    config: Any, response_schema: Any, json_mode: bool
) -> str:
    """Handler for Doubao provider."""
    final_prompt = _build_prompt_with_schema(prompt, response_schema) if response_schema else prompt
    return await doubao_file_extract(
        local_file_path=file_path,
        prompt=final_prompt,
        model=model,
        json_mode=json_mode
    )


# Provider handler registry
PROVIDER_HANDLERS: Dict[str, Callable] = {
    "gemini": _handle_gemini,
    "openrouter": _handle_openrouter,
    "qwen": _handle_qwen,
    "doubao": _handle_doubao,
}


async def unified_file_extract(
    file_path: str,
    prompt: str,
    content_type: str = "image/jpeg",
    model: Optional[str] = None,
    config: Optional[types.GenerateContentConfig] = None,
    provider: Optional[str] = None,
    json_mode: Optional[bool] = None
) -> str:
    """
    Unified file extraction that auto-selects provider based on API keys.

    Provider priority: gemini > openrouter > qwen > doubao

    Args:
        file_path: Path to the file
        prompt: Extraction prompt
        content_type: MIME type (only used for Gemini)
        model: Model name (auto-selects default if not provided)
        config: Gemini GenerateContentConfig (only works with Gemini)
        provider: Force specific provider (overrides auto-selection)
        json_mode: Force JSON output (None=auto-detect from config)

    Returns:
        Extracted content (JSON string or plain text)

    Raises:
        ValueError: If no provider is available or specified provider is invalid
    """
    # Resolve provider
    if provider:
        provider_config = VisionProviderConfig.get_provider_by_name(provider)
        if not provider_config:
            raise ValueError(f"Unknown provider: {provider}. Available: {list(PROVIDER_HANDLERS.keys())}")
        if not safe_read_cfg(provider_config["api_key_env"]):
            raise ValueError(f"API key not configured for '{provider}' (env: {provider_config['api_key_env']})")
    else:
        provider_config = VisionProviderConfig.get_available_provider()
        if not provider_config:
            status = VisionProviderConfig.get_provider_status()
            raise ValueError(
                f"No vision provider available. Configure one of: "
                f"GOOGLE_API_KEY, OPENROUTER_API_KEY, DASHSCOPE_API_KEY, VOLCENGINE_API_KEY. "
                f"Current status: {status}"
            )

    provider_name = provider_config["name"]
    actual_model = model or safe_read_cfg(f"{provider_name.upper()}_VISION_MODEL", provider_config["default_model"])

    # Extract response_schema from config
    response_schema = getattr(config, 'response_schema', None) if config else None
    if response_schema and provider_name != "gemini":
        logging.info(f"📋 Embedding response_schema into prompt for {provider_name}")

    # Auto-detect json_mode from config
    if json_mode is None:
        json_mode = bool(
            (config and getattr(config, 'response_schema', None)) or
            (config and getattr(config, 'response_mime_type', None) == "application/json")
        )

    logging.info(f"unified_file_extract: {provider_name}, model={actual_model}, json_mode={json_mode}")

    # Dispatch to handler
    handler = PROVIDER_HANDLERS.get(provider_name)
    if not handler:
        raise ValueError(f"Unsupported provider: {provider_name}")

    return await handler(
        file_path=file_path,
        prompt=prompt,
        content_type=content_type,
        model=actual_model,
        config=config,
        response_schema=response_schema,
        json_mode=json_mode
    )
