"""The reference `Ocr`: one image → its text, through the engine's vision client.

`extract.pdf_text` hands this only the pages whose text layer is empty, and
`extract.image_text` one downscaled photo at a time, never a whole document.
The provider is whichever key is configured (`utils.llm.unified_file_extract`
picks it); a consumer with its own vision model passes its own callable.

With `UTILS_OCR_MODEL` routed (a document-OCR model such as GLM-OCR, which
answers only its own task prompts) the image goes there instead, once per
prompt the entry declares: its text, then its tables as HTML, whose columns
`collect/files/services/table_indicators` reads without a model.
"""

from __future__ import annotations

import os
import tempfile

OCR_PROMPT = """Extract and return ALL text content from this document/image.

Requirements:
1. Extract all text completely, do not omit anything
2. For tables, use the following format:
   - Each row on a separate line
   - Columns separated by |
   - Keep table headers
3. Preserve the original paragraph structure
4. Keep all values, units, dates, measurements exactly as shown
5. Anonymize personal identifiable information (PII):
   - ID number (身份证号): replace with "***"
   - Phone number: replace with "***"
   - Address: keep only city/district level, replace detailed address with "***"
   - Patient ID / Medical record number: replace with "***"
   - Keep name, age, gender, and medical-related dates (examination date, report date) as is

Return ONLY the extracted text content. Do not add any explanations, summaries, or commentary.
If there is no text, return an empty response."""

_SUFFIX = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp", "image/gif": ".gif"}


def _ocr_route():
    from mirobody.utils.config.llm import resolve_route

    return resolve_route("ocr")


async def _extract(image: bytes, mime: str, prompt: str, provider: str | None = None) -> str:
    from mirobody.utils.llm import unified_file_extract

    with tempfile.NamedTemporaryFile(suffix=_SUFFIX.get(mime, ".png"), delete=False) as handle:
        handle.write(image)
        path = handle.name
    try:
        return (await unified_file_extract(file_path=path, prompt=prompt, content_type=mime,
                                           provider=provider, json_mode=False)) or ""
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


async def vision_ocr(image: bytes, mime: str, *, prompt: str = OCR_PROMPT) -> str:
    """Text of one image: the OCR entry's passes when one is routed, else the vision provider."""
    spec = _ocr_route()
    if spec is None:
        return await _extract(image, mime, prompt)
    parts = [await _extract(image, mime, p, spec.alias) for p in spec.ocr_prompts.values()]
    return "\n\n".join(part.strip() for part in parts if part.strip())


def table_ocr():
    """The tables pass alone, for PDF pages whose text layer is already exact;
    None when no OCR entry is routed or it declares no `tables` prompt."""
    spec = _ocr_route()
    if spec is None or "tables" not in spec.ocr_prompts:
        return None

    async def tables(image: bytes, mime: str) -> str:
        return (await _extract(image, mime, spec.ocr_prompts["tables"], spec.alias)).strip()

    return tables
