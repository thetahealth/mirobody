"""Word and PowerPoint uploads.

`.docx` and `.pptx` sat in `file_uploader.SUPPORTED_EXTENSIONS` with no handler
in existence: the picker accepted the file, the upload ran, and `file_processor`
answered "file not supported" at the end. This is the handler that closes that,
and it is the same shape as `ExcelHandler` on purpose — extract to markdown,
persist it as `original_text`, generate an abstract, and let
`BaseFileHandler.process` trigger indicator extraction. A lab report saved as a
Word document gets exactly the treatment a lab report saved as a PDF gets.

Legacy binary `.doc`/`.ppt` are NOT handled: python-docx and python-pptx read
the zip-based formats only. They stay out of the accepted set rather than
failing after an upload, which is the defect this replaces.
"""

import logging
from typing import Any, Dict

from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.utils.file_types import is_document_file
from mirobody.utils.i18n import t


class DocumentHandler(BaseFileHandler):
    """Word/PowerPoint handler, built-in extraction only."""

    def get_type_name(self) -> str:
        return "document"

    # Routing table lives in utils.file_types so the handler that accepts a
    # document and the extractor that parses it can never disagree.
    is_document_file = staticmethod(is_document_file)

    async def _process_content(
        self,
        ctx: FileProcessingContext,
        temp_file_path: str,
        unique_filename: str,
        full_url: str,
        language: str,
    ) -> Dict[str, Any]:
        if ctx.progress_callback:
            await ctx.progress_callback(55, t("extracting_text_content", language, "file_processor"))

        original_text, content_hash = await self._extract_original_text(
            ctx=ctx,
            file_type="document",
        )

        if original_text:
            await self._save_original_text_to_db(
                file_key=unique_filename,
                original_text=original_text,
                text_length=len(original_text),
                content_hash=content_hash or "",
            )
            logging.info(
                f"💾 Document original text saved to th_files: {unique_filename}, "
                f"length: {len(original_text)}"
            )

        if ctx.progress_callback:
            await ctx.progress_callback(75, t("extracting_abstract", language, "file_processor"))

        file_abstract = ""
        file_name = ctx.filename
        if original_text and original_text.strip():
            try:
                file_abstract, file_name = await self._extract_abstract_from_text(
                    original_text=original_text,
                    filename=ctx.filename,
                    language=language,
                )
            except Exception as e:
                logging.warning(f"⚠️ Document abstract extraction failed: {unique_filename}, error: {e}")

        if ctx.progress_callback:
            await ctx.progress_callback(90, t("text_processing_success", language, "file_processor"))

        return {
            "raw": original_text or "",
            "file_abstract": file_abstract,
            "file_name": file_name,
            "original_text": original_text or "",
            "text_length": len(original_text) if original_text else 0,
            "content_hash": content_hash or "",
        }
