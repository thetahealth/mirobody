"""The Gemini vision path.

Gemini takes a PDF natively, but page-by-page extraction beats handing over the
whole document for long reports, so this module owns that loop and the error
translation that turns the SDK's quota/safety failures into something the
caller can act on.
"""

from __future__ import annotations

import asyncio
import logging
import pathlib
import time
from typing import Any, Dict, List, Optional

from google.genai import types

from ..clients import client_manager
from .media import FileProcessor
from .results import _merge_page_results

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


