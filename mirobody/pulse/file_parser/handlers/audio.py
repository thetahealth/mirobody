from typing import Any
from mirobody.utils.i18n import t
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext

class AudioHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "audio"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> dict[str, Any]:
        if ctx.progress_callback:
            await ctx.progress_callback(60, t("extracting_audio_content", language, "file_processor"))

        # Extract text from audio file
        raw_text = self.content_extractor.extract_from_audio(full_url)

        if ctx.progress_callback:
            await ctx.progress_callback(90, t("audio_processing_success", language, "file_processor"))

        return {
            "raw": raw_text,
            # An empty (but present) file_abstract deliberately suppresses
            # BaseFileHandler's abstract step: there is nothing to summarise
            # while ContentExtractor.extract_from_audio is a stub that returns
            # the "recognition failed" string rather than a transcript.
            "file_abstract": "",
            "file_name": ctx.filename,
        }

