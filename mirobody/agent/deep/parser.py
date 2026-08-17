"""File preparation for the DeepAgent virtual filesystem.

Two responsibilities, both at *upload time* (the agent reads files through the
deepagents-native ``read_file`` tool, not through this module):

1. **Classification** — given a filename/mime, decide whether the file is a
   *multimodal* type (image / audio / video / pdf / ...). Multimodal files are
   stored as raw bytes (object-storage offload) and surfaced to the model as
   multimodal content blocks by the deepagents filesystem middleware. See
   https://docs.langchain.com/oss/python/deepagents/harness#virtual-filesystem-access
   and https://docs.langchain.com/oss/python/langchain/messages#multimodal

2. **Text extraction** — best-effort text for the ``content`` column so that
   ``grep`` works and non-multimodal models still get *some* signal. Delegates
   to the shared ``FileAbstractExtractor``, which dedups repeated bytes against
   ``th_file_contents`` (SHA256) for every extraction consumer — this module
   used to keep its own hash cache over ``th_files`` instead, which missed
   pulse-pipeline hits and never wrote back.

The resulting (raw_bytes, parsed_text, mime_type) triple is handed to
``PgFilesystemBackend.aupload_parsed`` which offloads the bytes and keeps the
text inline.
"""

import logging
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import BinaryIO, Optional, Union

from ...pulse.file_parser.services.file_abstract_extractor import FileAbstractExtractor
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
        """Prepare an uploaded file for storage.

        Returns raw bytes (for multimodal/offload), best-effort extracted text
        (for grep / non-multimodal models), the mime type, and the multimodal
        flag. Text extraction never raises — on failure ``parsed_text`` is "".
        """
        if hasattr(file_input, "read"):
            if hasattr(file_input, "seek"):
                file_input.seek(0)
            file_bytes = file_input.read()
        else:
            file_bytes = file_input or b""

        mime = guess_mime(filename)
        multimodal = is_multimodal(filename)

        # Pure media (image/audio/video) has no text to extract — read happens
        # multimodally. Only extract for text-y and pdf/ppt types.
        ext = PurePosixPath(filename or "").suffix.lower()
        should_extract = (not multimodal) or (ext in _TEXT_EXTRACTABLE_MULTIMODAL)

        parsed_text = ""
        if should_extract and file_bytes:
            try:
                text = await self.file_abstract_extractor.extract_file_original_text(
                    file_content=file_bytes,
                    file_type=ext.lstrip("."),
                    filename=filename,
                    content_type=mime,
                )
                if text and len(text.strip()) > 10:
                    parsed_text = text
            except Exception as e:
                logger.error(f"File parsing failed for {filename}: {e}", exc_info=True)

        return PreparedFile(
            raw_bytes=file_bytes or None,
            parsed_text=parsed_text,
            mime_type=mime,
            is_multimodal=multimodal,
        )

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
