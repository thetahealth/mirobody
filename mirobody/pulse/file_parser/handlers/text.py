from typing import Any, Dict
from mirobody.utils.i18n import t
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
import uuid

class TextHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "text"

    # Override _get_unique_filename because text handler logic was slightly different
    def _get_unique_filename(self, ctx: FileProcessingContext) -> str:
        if ctx.file_key:
            return ctx.file_key
        file_extension = ctx.file.filename.split(".")[-1] if "." in ctx.file.filename else "txt"
        return f"{str(uuid.uuid4())}.{file_extension}"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> Dict[str, Any]:
        if ctx.progress_callback:
             await ctx.progress_callback(70, t("extracting_text_content", language, "file_processor"))

        # Extract text content
        raw_text = await self.content_extractor.extract_from_text_file(temp_file_path)

        if ctx.progress_callback:
             await ctx.progress_callback(85, t("saving_text_results", language, "file_processor"))

        # Save to database
        record_id = await self.db_service.save_raw_text_to_db(ctx.target_user_id, "text", raw_text)

        if ctx.progress_callback:
            await ctx.progress_callback(90, t("text_processing_success", language, "file_processor"))
            
        return {
            "raw": raw_text,
            "record_id": record_id,
            # Text handler DOES call abstract extractor in original code.
        }

