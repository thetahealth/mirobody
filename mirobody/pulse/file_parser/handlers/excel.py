import logging
from typing import Any, Dict

from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.utils.i18n import t
from mirobody.utils.file_types import is_excel_file


class ExcelHandler(BaseFileHandler):
    """Excel handler.

    Two modes, with graceful fallback:
      * If an external ``excel_processor`` is injected by a downstream service,
        it is tried first — it may extract structured indicators from report
        layouts it recognises. Such a processor is typically keyed by known
        formats and reports ``success=False`` (or raises) for any workbook it
        does not recognise. When that happens the upload is NOT failed: it falls
        back to the built-in extraction below.
      * The built-in pandas/openpyxl extraction via the shared
        ``AbstractExtractor`` (the same path PDF/text use) works for *any*
        workbook with no extra dependency. The workbook is converted to text
        (``original_text``), an abstract is generated, and indicator extraction
        is auto-triggered by ``BaseFileHandler.process`` — identical treatment
        to a report PDF, on both the drive and chat upload paths.
    """

    def __init__(self, excel_processor=None, **kwargs):
        super().__init__(**kwargs)
        self.excel_processor = excel_processor

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
    ) -> Dict[str, Any]:
        # An injected external processor is tried first: it may extract
        # structured indicators from report layouts it recognises. Such a
        # processor is typically keyed by known formats and returns
        # ``success=False`` (or raises) for any workbook it does not recognise
        # (e.g. a generic spreadsheet or a report index). In that case the upload
        # must NOT be failed: fall through to the built-in pandas/openpyxl
        # extraction below, which turns any workbook into a markdown table
        # (original_text) + abstract + generic indicator extraction. Only a
        # genuine success short-circuits here.
        if self.excel_processor is not None:
            ext_result = None
            try:
                ext_result = await self.excel_processor.process_excel_file(
                    file_content=await ctx.file.read(),
                    filename=ctx.original_filename or ctx.file.filename,
                    content_type=ctx.content_type,
                    user_id=ctx.user_id,
                    query_user_id=ctx.target_user_id,
                    message_id=ctx.message_id,
                    progress_callback=ctx.progress_callback,
                )
            except Exception as e:
                logging.warning(
                    f"⚠️ external excel_processor raised for {ctx.filename}; "
                    f"falling back to built-in extraction: {e}"
                )
            if ext_result and ext_result.get("success"):
                return ext_result
            logging.info(
                f"ℹ️ external excel_processor did not handle {ctx.filename} "
                f"(success={(ext_result or {}).get('success')}); falling back "
                f"to built-in pandas/openpyxl extraction"
            )
            # fall through to the built-in extraction path below

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
            logging.info(
                f"💾 Excel original text saved to th_files: {unique_filename}, "
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
                logging.warning(f"⚠️ Excel abstract extraction failed: {unique_filename}, error: {e}")

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
