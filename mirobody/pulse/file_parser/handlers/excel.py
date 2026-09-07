import logging
from typing import Any

from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.utils.i18n import t
from mirobody.utils.file_types import is_excel_file

logger = logging.getLogger(__name__)


class ExcelHandler(BaseFileHandler):
    """Excel handler: built-in openpyxl extraction, nothing pluggable.

    An ``excel_processor`` injection point used to sit here — documented as
    "injected from mcp_server" so a downstream service could override
    extraction — but nothing in this project ever injected one, so the branch
    was unreachable and has been removed along with the seam in the factory.

    What remains is the shared ``AbstractExtractor`` path (the same one
    PDF/text use), which works for *any* workbook with no extra dependency:
    the workbook is converted to text (``original_text``), an abstract is
    generated, and indicator extraction is auto-triggered by
    ``BaseFileHandler.process`` — identical treatment to a report PDF, on
    both the drive and chat upload paths.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_type_name(self) -> str:
        return "excel"

    # Routing table lives in utils.file_types so the handler that accepts a
    # spreadsheet and the extractor that parses it can never disagree.
    is_excel_file = staticmethod(is_excel_file)

    async def _process_content(
        self,
        ctx: FileProcessingContext,
        temp_file_path: str,
        unique_filename: str,
        full_url: str,
        language: str,
    ) -> dict[str, Any]:
        if ctx.progress_callback:
            await ctx.progress_callback(55, t("extracting_text_content", language, "file_processor"))

        # Built-in extraction: workbook -> text (SHA256 dedup inside the extractor).
        original_text, content_hash = await self._extract_original_text(
            ctx=ctx,
            file_type="excel",
        )

        if original_text:
            await self._save_original_text_to_db(
                file_key=unique_filename,
                original_text=original_text,
                text_length=len(original_text),
                content_hash=content_hash or "",
            )
            logger.info(
                f"Excel original text saved to th_files: {unique_filename}, "
                f"length: {len(original_text)}"
            )

        if ctx.progress_callback:
            await ctx.progress_callback(75, t("extracting_abstract", language, "file_processor"))

        # Sync abstract so the upload result carries it (matches PDF behaviour).
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
                logger.warning(f"Excel abstract extraction failed: {unique_filename}, error: {e}")

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
