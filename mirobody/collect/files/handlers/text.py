from typing import Any
from mirobody.utils.i18n import localize
from mirobody.collect.files.handlers.base import BaseFileHandler, FileProcessingContext
import uuid
import hashlib

class TextHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "text"

    # Override _get_unique_filename because text handler logic was slightly different
    def _get_unique_filename(self, ctx: FileProcessingContext) -> str:
        if ctx.file_key:
            return ctx.file_key
        file_extension = ctx.file.filename.split(".")[-1] if "." in ctx.file.filename else "txt"
        return f"{str(uuid.uuid4())}.{file_extension}"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> dict[str, Any]:
        if ctx.progress_callback:
             await ctx.progress_callback(70, localize("extracting_text_content", language, "file_processor"))

        # Calculate content hash for deduplication
        await ctx.file.seek(0)
        file_content = await ctx.file.read()
        content_hash = hashlib.sha256(file_content).hexdigest() if file_content else ""

        # Extract text content
        raw_text = await self.content_extractor.extract_from_text_file(temp_file_path)

        # For text files the raw decode IS the original text. The dedup
        # read/write that used to sit here cached a free decode and was the
        # third hand-copied version of the cache SQL: dedup for expensive
        # extraction lives in FileAbstractExtractor now.
        original_text = raw_text

        file_abstract, file_name = await self._abstract(ctx, original_text, language)

        if ctx.progress_callback:
            await ctx.progress_callback(90, localize("text_processing_success", language, "file_processor"))

        return {
            "raw": raw_text,
            "original_text": original_text,
            "text_length": len(original_text) if original_text else 0,
            "content_hash": content_hash,
            "file_abstract": file_abstract,
            "file_name": file_name,
        }

