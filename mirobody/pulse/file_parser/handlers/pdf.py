import logging
from typing import Any, Dict
from mirobody.utils.i18n import t
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.pulse.file_parser.services.content_formatter import ContentFormatter
from datetime import datetime

class PDFHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "pdf"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> Dict[str, Any]:
        if ctx.progress_callback:
             await ctx.progress_callback(55, t("extracting_pdf_content", language, "file_processor"))

        # raw_text = await self.content_extractor.extract_from_pdf(temp_file_path, ctx.content_type)
        raw_text = "" 

        if ctx.progress_callback:
            await ctx.progress_callback(65, t("processing_pdf_indicators", language, "file_processor"))

        formatted_raw = ""
        
        (indicators, llm_ret) = await self.indicator_extractor.extract_indicators_from_file(
            ocr_db_id=ctx.message_id,
            temp_file_path=temp_file_path,
            content_type=ctx.content_type,
            file_name=ctx.filename,
            user_id=int(ctx.target_user_id),
            source_table="theta_ai.th_messages",
            progress_callback=ctx.progress_callback,
            file_key=unique_filename,
        )
        
        logging.info(f"✅ PDF indicator extraction completed: {unique_filename}, indicator count: {len(indicators) if indicators else 0}")

        if isinstance(llm_ret, dict) and "formatted_content" in llm_ret:
            formatted_raw = llm_ret["formatted_content"]
            logging.info(f"✅ Using formatted content for PDF: {unique_filename}")
        elif indicators and llm_ret:
            try:
                formatted_raw = ContentFormatter.format_parsed_content(
                    file_results=[{"type": "pdf", "raw": raw_text}],
                    file_names=[unique_filename],
                    llm_responses=[llm_ret],
                    indicators_list=[indicators],
                )
                logging.info(f"✅ Using ContentFormatter for PDF: {unique_filename}")
            except Exception as format_error:
                logging.warning(f"⚠️ ContentFormatter failed for PDF: {unique_filename}, error: {format_error}")
                formatted_raw = raw_text
        else:
            formatted_raw = raw_text
            logging.info(f"📄 No indicators found, using raw text for PDF: {unique_filename}")

        if not formatted_raw or formatted_raw.strip() == "":
            formatted_raw = f"# PDF file processing completed\n\n**File name:** {unique_filename}\n**Processing time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\nPDF file has been successfully uploaded and processed."
            logging.warning(f"Formatted content is empty, using final fallback for PDF: {unique_filename}")

        if ctx.progress_callback:
             await ctx.progress_callback(90, t("pdf_processing_success", language, "file_processor"))

        return {
            "raw": formatted_raw,
            # PDF handler in original code DOES call abstract extractor explicitly at the end.
            # So BaseHandler's default behavior is correct for PDF.
        }

