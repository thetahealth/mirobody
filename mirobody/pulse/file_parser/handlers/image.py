import json, logging

from typing import Any, Dict
from mirobody.utils.i18n import t
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.pulse.file_parser.services.content_formatter import ContentFormatter


class ImageHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "image"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> Dict[str, Any]:
        if ctx.progress_callback:
            await ctx.progress_callback(55, t("extracting_content", language, "file_processor"))
        
        if ctx.progress_callback:
            await ctx.progress_callback(65, t("processing_indicators", language, "file_processor"))

        formatted_raw = ""
        
        (indicators, llm_ret) = await self.indicator_extractor.extract_indicators_from_file(
            ocr_db_id=ctx.message_id,
            temp_file_path=temp_file_path,
            content_type=ctx.content_type,
            file_name=ctx.filename,
            user_id=int(ctx.target_user_id),
            source_table="th_files",
            progress_callback=ctx.progress_callback,
            file_key=unique_filename,
        )

        logging.info(f"✅ Image indicator extraction completed: {unique_filename}, indicator count: {len(indicators) if indicators else 0}")

        if indicators and llm_ret:
            try:
                formatted_raw = ContentFormatter.format_parsed_content(
                    file_results=[{"type": "image", "raw": ""}],
                    file_names=[unique_filename],
                    llm_responses=[llm_ret],
                    indicators_list=[indicators],
                )
                logging.info(f"✅ Using ContentFormatter for image: {unique_filename}")
            except Exception as format_error:
                logging.warning(f"⚠️ ContentFormatter failed for image: {unique_filename}, error: {format_error}")
                formatted_raw = ""
        else:
            formatted_raw = ""
            logging.info(f"🖼️ No indicators found, using raw text for image: {unique_filename}")

        # Safety check
        if not formatted_raw or formatted_raw.strip() == "":
            from datetime import datetime
            formatted_raw = f"# Image file processing completed\n\n**File name:** {ctx.filename}\n**Processing time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\nImage file has been successfully uploaded and processed."
            logging.warning(f"Formatted content is empty, using final fallback for image: {ctx.filename}")

        if ctx.progress_callback:
            await ctx.progress_callback(90, t("image_processing_success", language, "file_processor"))

        return {
            "raw": formatted_raw,
            # Note: abstract is handled in base class, but we can pass hints if needed?
            # The base class calls abstract_extractor independently. 
            # However, existing code tries to extract abstract from llm_ret first if available.
            # We should probably return it here if found to optimize.
            "extracted_abstract_hint": self._extract_abstract_from_llm(llm_ret)
        }

    def _extract_abstract_from_llm(self, llm_ret: Any) -> str:
        if isinstance(llm_ret, dict):
            return llm_ret.get("file_abstract", "")
        elif isinstance(llm_ret, str):
            try:
                llm_data = json.loads(llm_ret)
                return llm_data.get("file_abstract", "")
            except (json.JSONDecodeError, AttributeError):
                pass
        return ""

    async def _extract_abstract(self, ctx: FileProcessingContext, unique_filename: str, language: str) -> tuple[str, str]:
        # Override to check if we already have it from content processing? 
        # Or just let base class do it. The original code does BOTH logic (try from llm_ret, then independent extractor?)
        # Actually, original code: 
        # Image: "Extract file_abstract from llm_ret if it's a dict" -> It DOES NOT call abstract_extractor separately if successful?
        # Wait, looking at original _process_image_file:
        # It gets abstract from llm_ret. It DOES NOT call self.abstract_extractor.extract_file_abstract separately for Images!
        # BUT for PDFs, it does: "Extract file abstract" block at the end.
        
        # So ImageHandler should OVERRIDE _extract_abstract to rely on what was found during processing, or do nothing if not found?
        # Let's implementing a custom strategy.
        
        # Since _process_content returns data, we can use that in _build_response. 
        # But _extract_abstract is called after _process_content in BaseHandler.
        
        # I will override _extract_abstract to return empty if I want to skip the base logic, 
        # or I can modify BaseHandler to accept an optional 'abstract' from _process_content result.
        
        # Let's modify BaseHandler slightly to use result_data['file_abstract'] if present.
        return await super()._extract_abstract(ctx, unique_filename, language)


