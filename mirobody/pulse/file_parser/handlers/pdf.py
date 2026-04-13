import logging
from typing import Any, Dict

from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.utils.i18n import t


class PDFHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "pdf"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> Dict[str, Any]:
        if ctx.progress_callback:
             await ctx.progress_callback(55, t("extracting_pdf_content", language, "file_processor"))

        # Step 1: Extract original text first (with SHA256 deduplication)
        original_text, content_hash = await self._extract_and_save_original_text(
            ctx=ctx,
            temp_file_path=temp_file_path,
            file_type="pdf",
        )

        # Step 2: Immediately save original_text to th_files
        if original_text:
            await self._save_original_text_to_db(
                file_key=unique_filename,
                original_text=original_text,
                text_length=len(original_text),
                content_hash=content_hash or "",
            )
            logging.info(f"💾 PDF original text saved to th_files: {unique_filename}, length: {len(original_text)}")

        if ctx.progress_callback:
            await ctx.progress_callback(70, t("extracting_abstract", language, "file_processor"))

        # Step 3: Sync extract abstract (must complete before returning success)
        file_abstract = ""
        file_name = ctx.filename
        if original_text and original_text.strip():
            try:
                file_abstract, file_name = await self._extract_abstract_from_text(
                    original_text=original_text,
                    filename=ctx.filename,
                    language=language,
                )
                logging.info(f"✅ PDF abstract extraction completed: {unique_filename}")
            except Exception as e:
                logging.warning(f"⚠️ PDF abstract extraction failed: {unique_filename}, error: {e}")

        if ctx.progress_callback:
            await ctx.progress_callback(85, t("pdf_upload_success", language, "file_processor"))

        # Step 4: indicator extraction is auto-triggered by base process() via original_text

        if ctx.progress_callback:
             await ctx.progress_callback(90, t("pdf_processing_success", language, "file_processor"))

        return {
            "raw": original_text or "",
            "file_abstract": file_abstract,
            "file_name": file_name,
            "original_text": original_text or "",
            "text_length": len(original_text) if original_text else 0,
            "content_hash": content_hash or "",
        }
