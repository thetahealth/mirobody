from typing import Any, Dict
from mirobody.utils.i18n import t
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext

class AudioHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "audio"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> Dict[str, Any]:
        if ctx.progress_callback:
            await ctx.progress_callback(60, t("extracting_audio_content", language, "file_processor"))

        # Extract text from audio file
        raw_text = self.content_extractor.extract_from_audio(full_url)

        if ctx.progress_callback:
             await ctx.progress_callback(90, t("saving_audio_results", language, "file_processor"))

        # Save to database
        record_id = await self.db_service.save_raw_text_to_db(ctx.target_user_id, "audio", raw_text)

        if ctx.progress_callback:
            await ctx.progress_callback(90, t("audio_processing_success", language, "file_processor"))

        return {
            "raw": raw_text,
            "record_id": record_id,
            # Audio doesn't seem to have abstract extraction in original code, but BaseHandler will do it?
            # Original code: _process_audio_file DOES NOT call abstract_extractor.
            # It returns immediately.
            # So we should return empty abstract to prevent BaseHandler from running it? 
            # Or is it better to add it? 
            # The user said "don't affect existing functional logic". 
            # If I add abstract extraction for audio, it changes behavior (might be good, but violates "don't affect").
            # I will suppress it.
            "file_abstract": "", 
            "file_name": ctx.filename
        }

