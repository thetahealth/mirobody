"""An upload's text, its abstract and the name a model gives it.

The text is `mirobody.documents.extract`'s, cached by content hash through
`th_files`. The abstract is read off that text by the text model
(`abstract_from_text`); only a photo with no text in it goes to the vision
model as a picture (`describe_image`), so a photo of a meal still gets one.
"""

from __future__ import annotations

import hashlib
import logging
import os

from mirobody.collect.files.services.prompts.file_abstract_prompt import (
    FALLBACK_ABSTRACT_TEMPLATES,
    FILE_ABSTRACT_PROMPT,
)
from mirobody.documents import detect, extract as documents, render
from mirobody.documents.ocr import table_ocr, vision_ocr
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.file_types import with_extension
from mirobody.utils.llm_output import parse_json_object

logger = logging.getLogger(__name__)

#: What the text-side abstract asks for. Closed (`additionalProperties: false`)
#: like every json_schema the product sends: OpenAI answers HTTP 400 to an open
#: nested object (measured through OpenRouter, 2026-10-06). This flat one was
#: accepted open; closed, a nested field added later cannot bring the 400 back.
ABSTRACT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "file_name": {
            "type": "string",
            "description": "Generated filename with extension"
        },
        "file_abstract": {
            "type": "string",
            "description": "Brief summary of file content (max 150 chars)"
        }
    },
    "required": ["file_name", "file_abstract"]
}

#: Characters of an abstract kept.
MAX_ABSTRACT_CHARS = 200


async def lookup_extracted_text(file_content: bytes) -> str | None:
    """Text a previous extraction of these exact bytes produced, or None.

    The cheap half of extraction: one indexed lookup, never a model call. The
    agent's virtual filesystem uses it at registration time to decide whether a file
    still needs OCR at all (see ``agent/filesystem/parser.FileParser.prepare``).
    """
    if not file_content:
        return None
    return await _read_original_text_cache(hashlib.sha256(file_content).hexdigest())


async def _read_original_text_cache(content_hash: str) -> str | None:
    """Dedup read: SHA256 of the raw bytes -> text some earlier upload extracted.

    The cache IS ``th_files``. Every persistence path
    (``FileDbService.insert_file`` / ``update_file_processed`` /
    ``BaseFileHandler._save_original_text_to_db``) already writes
    ``content_hash`` and ``original_text`` onto the same row, so a dedicated
    hash->text table stored a second copy of the same health text and bought
    nothing: it was never normalised away, nothing ever deleted from it, and no
    foreign key tied it to the file it came from, so a user's extracted report
    text outlived the file they deleted, in a table with no ``user_id``. Reading
    through ``th_files`` (``is_del = false``) makes "delete the file, lose the
    text" true.

    The trade-off, stated plainly: an extraction only lands in the cache if it
    reaches a ``th_files`` row. Registration (``FileParser.prepare``) reads this
    cache but never OCRs to populate it; the agent's own OCR runs lazily, on
    the first ``read_file`` (``PgFilesystemBackend._lazy_extract_doc_text``),
    and is written back into this same ``th_files`` row rather than kept apart.
    That write happens only once the extraction finishes, so a file the agent
    reads before the upload pipeline's own extraction lands can still be OCR'd
    by both. That is a cost, not a correctness, difference.

    ``GLOBAL_FILE_CACHE_ENABLED: false`` forces fresh extraction. Deferred
    imports + broad except: extraction must keep working without a database.
    """
    try:
        from mirobody.utils.config import safe_read_cfg
        if str(safe_read_cfg("GLOBAL_FILE_CACHE_ENABLED", "true")).strip().lower() in ("false", "0", "no"):
            return None
        from mirobody.utils.db import execute_query
        rows = await execute_query(
            """
            SELECT decrypt_content(original_text) AS original_text
            FROM th_files
            WHERE content_hash = :hash AND is_del = false
              AND original_text IS NOT NULL AND original_text != ''
            ORDER BY updated_at DESC LIMIT 1
            """,
            params={"hash": content_hash},
        )
        text = rows[0].get("original_text") if rows else None
        return text if text and text.strip() else None
    except Exception as e:
        logger.warning("original-text cache read failed: error_type=%s", type(e).__name__,
                       exc_info=not is_driver_exception(e))
        return None


class ThFilesTextCache:
    """`documents.extract.TextCache` over `th_files`: reads by content hash;
    writes are the persistence path's (every row carries `content_hash` and
    `original_text`), so `put` is a no-op here."""

    async def get(self, digest: str) -> str | None:
        return await _read_original_text_cache(digest)

    async def put(self, digest: str, text: str) -> None:
        return None


def _truncated(abstract: str) -> str:
    """`abstract` cut to `MAX_ABSTRACT_CHARS`, at a word boundary when one is
    near the end."""
    if len(abstract) <= MAX_ABSTRACT_CHARS:
        return abstract
    truncated = abstract[:MAX_ABSTRACT_CHARS - 3] + "..."
    last_space = truncated.rfind(" ")
    if last_space > MAX_ABSTRACT_CHARS * 0.8:
        truncated = abstract[:last_space] + "..."
    return truncated


class FileAbstractExtractor:
    """An upload's text, its abstract and the name a model gives it."""

    async def extract_file_original_text(
        self,
        file_content: bytes,
        file_type: str,
        filename: str,
        content_type: str | None = None,
    ) -> str:
        """The document's text (`mirobody.documents.extract`): the PDF text layer
        page by page with only scanned pages OCR'd, images downscaled then
        OCR'd, spreadsheets and Word/PowerPoint as markdown, text decoded: cached
        by content hash through `th_files`, so the same bytes are never OCR'd
        twice. ``""`` for a kind nothing reads.

        Failures RAISE. This used to catch everything and return "", so a
        photo uploaded to a deployment with no vision provider (or with a
        text-only model as the vision default) was indistinguishable from a
        blank photo, and the upload above it reported success (#68). The
        callers decide what a failure means for them: the upload handler fails
        the file with the reason, the agent's file reader answers ""."""
        hint = content_type or ({"pdf": "application/pdf", "image": "image/jpeg"}.get((file_type or "").lower()))
        text = await documents.extract_text(filename, hint, file_content, ocr=vision_ocr, tables=table_ocr(), cache=ThFilesTextCache())
        logger.info("original text extracted: file_type=%s char_count=%d", file_type, len(text))
        return text

    async def abstract_from_text(self, original_text: str, filename: str, language: str) -> tuple[str, str]:
        """`(abstract, file_name)` the text model reads off a document's text;
        `("", filename)` when it answered nothing usable. The name keeps the
        upload's own extension."""
        from mirobody.utils.llm import async_get_structured_output

        # `language` reaches this prompt because it used to be an unused
        # parameter, and "use the same language as the content" was one
        # bullet in a list the model ignored. An English lab report came
        # back named `2025-10-15_Laborbericht_Lipide_Glukose.pdf`, and on a
        # second run `..._Rapport_labo_lipides_glycemie.pdf`: German, then
        # French, for the same English document.
        prompt = f"""Based on the document content below, generate:
1. file_name: A descriptive filename in format: Date_Content_Description, with no extension
   - Include date if found (YYYY-MM-DD format)
   - Keep it concise (15-40 chars excluding extension)
   - LANGUAGE: write it in the language the DOCUMENT ITSELF uses. An English
     report gets an English name; a 体检报告 gets a Chinese one. Never translate
     into a third language. If the document's own language is genuinely
     ambiguous, fall back to: {language}

2. file_abstract: A brief summary (max 150 characters)
   - Identify document type
   - Extract key information
   - Highlight main findings
   - Same language rule as above

Return JSON format: {{"file_name": "...", "file_abstract": "..."}}"""

        messages = [
            {"role": "system", "content": prompt},
            {"role": "user", "content": f"Original filename: {filename}\n\nDocument content:\n{original_text[:8000]}"},
        ]
        result = await async_get_structured_output(
            messages=messages,
            response_format={"type": "json_schema", "json_schema": {"name": "abstract_response", "schema": ABSTRACT_SCHEMA}},
            temperature=0.1,
            max_tokens=32000,
        )
        if not isinstance(result, dict):
            return "", filename
        abstract = str(result.get("file_abstract") or "")[:MAX_ABSTRACT_CHARS]
        file_name = with_extension(str(result.get("file_name") or ""), os.path.splitext(filename or "")[1]) or filename
        logger.info("abstract from text: char_count=%d", len(abstract))
        return abstract, file_name

    async def describe_image(self, file_content: bytes, filename: str) -> tuple[str, str]:
        """`(abstract, file_name)` the vision model writes for an image whose
        OCR found no text: a photo of a meal, a scan of an X-ray. Raises what
        the vision surface raises."""
        from mirobody.utils.llm import vision_extract

        width, height, fmt = render.image_info(file_content)
        extension = os.path.splitext(filename or "")[1].lower()
        prompt = (f"{FILE_ABSTRACT_PROMPT}\n\nFile context: Image file: {filename} "
                  f"({width}x{height}, {fmt or 'Unknown'} format)")
        answer = await vision_extract(file_content, detect.image_mime(filename, None, file_content), prompt,
                                      json_mode=True)
        parsed = parse_json_object(answer)
        if parsed is None:
            # Not JSON: the model described the image in prose, which is an abstract.
            return _truncated(answer), filename
        file_name = with_extension(str(parsed.get("file_name") or ""), extension) or filename
        return _truncated(str(parsed.get("file_abstract") or "")), file_name

    @staticmethod
    def fallback_abstract(filename: str, file_type: str) -> str:
        """What the file list says when no abstract could be made. The file IS
        stored; it is the summary that is missing, and the sentence says so."""
        template = FALLBACK_ABSTRACT_TEMPLATES.get(file_type, FALLBACK_ABSTRACT_TEMPLATES["default"])
        # Each placeholder is a complete noun phrase, because the template
        # no longer supplies the unit word after it.
        fields = {
            "pdf": {"page_count": "page count unknown"},
            "image": {"resolution": "resolution unknown"},
            "excel": {"sheet_count": "sheet count unknown"},
            "genetic": {"file_size": "size unknown"},
            "text": {"word_count": "length unknown"},
        }.get(file_type, {"file_type": file_type.upper()})
        return _truncated(template.format(filename=filename, **fields))
