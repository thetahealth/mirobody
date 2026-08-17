"""File preparation for the DeepAgent virtual filesystem.

Two responsibilities, both at *upload time* (the agent reads files through the
deepagents-native ``read_file`` tool, not through this module):

1. **Classification** — given a filename/mime, decide whether the file is a
   *multimodal* type (image / audio / video / pdf / ...). Multimodal files are
   stored as raw bytes (object-storage offload) and surfaced to the model as
   multimodal content blocks by the deepagents filesystem middleware. See
   https://docs.langchain.com/oss/python/deepagents/harness#virtual-filesystem-access
   and https://docs.langchain.com/oss/python/langchain/messages#multimodal

2. **Text extraction, cold** — registration never OCRs. It only asks whether
   these exact bytes were extracted before (one indexed SHA256 lookup against
   ``th_files``); if not, ``content`` stays empty and the real extraction runs
   the first time the model calls ``read_file`` on the file, via
   ``PgFilesystemBackend._lazy_extract_doc_text`` -> ``extract_text``. Files
   nobody opens therefore cost nothing beyond storage, which is the common case
   for chat attachments.

   The trade-off this buys is deliberate: an unopened document is not greppable
   until something reads it, because its text does not exist yet.

The resulting (raw_bytes, parsed_text, mime_type) triple is handed to
``PgFilesystemBackend.aupload_parsed`` which offloads the bytes and keeps the
text inline.
"""

import logging
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import BinaryIO, Optional, Union

from ...pulse.file_parser.services.file_abstract_extractor import (
    FileAbstractExtractor,
    lookup_extracted_text,
)
from ...utils.db import execute_query
from .filetype import guess_mime, is_multimodal

logger = logging.getLogger(__name__)

# Multimodal extensions whose text we still try to extract (so grep + non-pdf
# models get content). Pure media (image/audio/video) have no extractable text.
_TEXT_EXTRACTABLE_MULTIMODAL = {".pdf", ".ppt", ".pptx"}


@dataclass
class PreparedFile:
    """Result of preparing an uploaded file for the workspace."""
    raw_bytes: Optional[bytes]
    parsed_text: str
    mime_type: str
    is_multimodal: bool


class FileParser:
    """Extract text from uploaded files for the workspace ``content`` column."""

    def __init__(self):
        self.file_abstract_extractor = FileAbstractExtractor()

    async def prepare(
        self,
        file_input: Union[bytes, BinaryIO],
        filename: str,
    ) -> PreparedFile:
        """Classify an uploaded file for storage — deliberately does NOT extract.

        Cold loading: OCR is the expensive step and most registered files are
        never opened, so it is deferred to the moment the model actually calls
        ``read_file`` (``PgFilesystemBackend._lazy_extract_doc_text`` ->
        ``extract_text``). Registration stays a byte copy.

        The one thing done here is the cheap half — a single indexed SHA256
        lookup. Bytes somebody already extracted come back inline immediately,
        so a re-uploaded document is free, greppable at once, and needs no
        read-time round-trip.
        """
        if hasattr(file_input, "read"):
            if hasattr(file_input, "seek"):
                file_input.seek(0)
            file_bytes = file_input.read()
        else:
            file_bytes = file_input or b""

        mime = guess_mime(filename)
        multimodal = is_multimodal(filename)

        # Pure media (image/audio/video) is read multimodally and has no text
        # to look up. Only text-y and pdf/ppt types can have a cached extraction.
        ext = PurePosixPath(filename or "").suffix.lower()
        extractable = (not multimodal) or (ext in _TEXT_EXTRACTABLE_MULTIMODAL)

        parsed_text = ""
        if extractable and file_bytes:
            try:
                cached = await lookup_extracted_text(file_bytes)
                if cached and len(cached.strip()) > 10:
                    parsed_text = cached
                    logger.info(f"🎯 {filename}: reused an earlier extraction, no OCR needed")
            except Exception as e:
                logger.warning(f"extraction lookup failed for {filename}: {e}")

        return PreparedFile(
            raw_bytes=file_bytes or None,
            parsed_text=parsed_text,
            mime_type=mime,
            is_multimodal=multimodal,
        )

    async def extract_text(self, file_bytes: bytes, filename: str) -> str:
        """Run the real extraction — the only path here that can call a Vision LLM.

        Reached from ``read_file`` on a document whose text is not inline yet.
        Deduplication still applies inside ``extract_file_original_text``: two
        models opening the same bytes pay for one OCR. Never raises; returns ""
        when nothing could be extracted.
        """
        if not file_bytes:
            return ""
        ext = PurePosixPath(filename or "").suffix.lower()
        try:
            text = await self.file_abstract_extractor.extract_file_original_text(
                file_content=file_bytes,
                file_type=ext.lstrip("."),
                filename=filename,
                content_type=guess_mime(filename),
            )
            return text if text and len(text.strip()) > 10 else ""
        except Exception as e:
            logger.error(f"File parsing failed for {filename}: {e}", exc_info=True)
            return ""

    async def get_cached_file_by_key(self, file_key: str) -> Optional[str]:
        """Text the pulse upload pipeline already parsed for this ``file_key``
        (``th_files.original_text``), or None.

        The upload-time parse runs asynchronously, so a workspace row registered
        by reference may predate its result; this lookup is how the workspace
        picks it up later without re-extracting (see
        ``PgFilesystemBackend._lazy_extract_doc_text``). Distinct from the
        byte-level SHA256 dedup, which lives inside ``FileAbstractExtractor``.
        """
        if not file_key:
            return None
        try:
            rows = await execute_query(
                """
                SELECT decrypt_content(original_text) AS original_text
                FROM th_files
                WHERE file_key = :file_key AND is_del = false
                  AND original_text IS NOT NULL AND original_text != ''
                ORDER BY updated_at DESC LIMIT 1
                """,
                params={"file_key": file_key},
            )
            text = rows[0].get("original_text", "") if rows else ""
            if text and len(text.strip()) >= 10:
                logger.info(f"🎯 th_files parse reused for file_key={file_key[:16]}...")
                return text
            return None
        except Exception as e:
            logger.error(f"th_files lookup failed (file_key={file_key[:16]}...): {e}")
            return None
