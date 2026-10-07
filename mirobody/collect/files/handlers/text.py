from typing import Any
from mirobody.utils.i18n import localize
from mirobody.collect.files.handlers.base import BaseFileHandler, FileProcessingContext
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

    async def _process_content(self, ctx: FileProcessingContext, unique_filename: str, full_url: str, language: str) -> dict[str, Any]:
        if ctx.progress_callback:
             await ctx.progress_callback(70, localize("extracting_text_content", language, "file_processor"))

        # Through `documents.decode_text`, like every other kind: a GBK CSV
        # read as strict UTF-8 came back "" with no error, a BOM stayed in
        # the first header cell, and nothing capped a huge file.
        original_text, content_hash = await self._extract_original_text(ctx=ctx, file_type="text")
        file_abstract, file_name = await self._abstract(ctx, original_text, language)

        if ctx.progress_callback:
            await ctx.progress_callback(90, localize("text_processing_success", language, "file_processor"))

        return {
            "raw": original_text or "",
            "original_text": original_text or "",
            "text_length": len(original_text) if original_text else 0,
            "content_hash": content_hash or "",
            "file_abstract": file_abstract,
            "file_name": file_name,
        }
