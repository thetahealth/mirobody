from typing import Any, Dict
from mirobody.utils.i18n import t
from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
import uuid
import hashlib
import logging

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

        # Calculate content hash for deduplication
        await ctx.file.seek(0)
        file_content = await ctx.file.read()
        content_hash = hashlib.sha256(file_content).hexdigest() if file_content else ""

        # Extract text content
        raw_text = await self.content_extractor.extract_from_text_file(temp_file_path)

        if ctx.progress_callback:
             await ctx.progress_callback(85, t("saving_text_results", language, "file_processor"))

        # Save to database
        record_id = await self.db_service.save_raw_text_to_db(ctx.target_user_id, "text", raw_text)

        # Extract original text using unified method (with th_file_contents cache)
        original_text = raw_text  # For text files, raw content is the original text
        try:
            from mirobody.utils.db import execute_query
            
            # Check th_file_contents cache first
            rows = await execute_query(
                "SELECT decrypt_content(original_text) as original_text FROM th_file_contents WHERE content_hash = :hash LIMIT 1",
                params={"hash": content_hash},
                query_type="select",
                mode="async",
            )

            if rows and len(rows) > 0 and rows[0].get("original_text"):
                original_text = rows[0]["original_text"]
                logging.info(f"✅ Text file reused cached original text: hash={content_hash[:16]}..., length={len(original_text)}")
            elif original_text:
                # Save to th_file_contents for future deduplication
                await execute_query(
                    """
                    INSERT INTO th_file_contents (content_hash, original_text, text_length, file_type)
                    VALUES (:hash, encrypt_content(:text), :length, :file_type)
                    ON CONFLICT (content_hash) DO NOTHING
                    """,
                    params={
                        "hash": content_hash,
                        "text": original_text,
                        "length": len(original_text),
                        "file_type": "text",
                    },
                    query_type="insert",
                    mode="async",
                )
                logging.info(f"✅ Text file original text saved to cache: hash={content_hash[:16]}..., length={len(original_text)}")
        except Exception as e:
            logging.warning(f"⚠️ Failed to cache text file original text: {e}")

        if ctx.progress_callback:
            await ctx.progress_callback(90, t("text_processing_success", language, "file_processor"))
            
        return {
            "raw": raw_text,
            "record_id": record_id,
            "original_text": original_text,
            "text_length": len(original_text) if original_text else 0,
            "content_hash": content_hash,
        }

