import asyncio
import logging
from typing import Any, Dict

from mirobody.pulse.file_parser.handlers.base import BaseFileHandler, FileProcessingContext
from mirobody.pulse.file_parser.services.content_formatter import ContentFormatter
from mirobody.utils.i18n import t


class ImageHandler(BaseFileHandler):
    def get_type_name(self) -> str:
        return "image"

    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> Dict[str, Any]:
        if ctx.progress_callback:
            await ctx.progress_callback(55, t("extracting_content", language, "file_processor"))
        
        # Step 1: Extract original text first (with SHA256 deduplication)
        original_text, content_hash = await self._extract_and_save_original_text(
            ctx=ctx,
            temp_file_path=temp_file_path,
            file_type="image",
        )

        # Step 2: Immediately save original_text to th_files
        if original_text:
            await self._save_original_text_to_db(
                file_key=unique_filename,
                original_text=original_text,
                text_length=len(original_text),
                content_hash=content_hash or "",
            )
            logging.info(f"💾 Image original text saved to th_files: {unique_filename}, length: {len(original_text)}")

        if ctx.progress_callback:
            await ctx.progress_callback(70, t("extracting_abstract", language, "file_processor"))

        # Step 3: Sync extract abstract (must complete before returning success)
        file_abstract = ""
        file_name = ctx.filename
        if original_text and original_text.strip():
            try:
                file_abstract, file_name = await self._extract_abstract_from_text(
                    original_text=original_text,
                    filename=ctx.filename,
                    language=language,
                )
                logging.info(f"✅ Image abstract extraction completed: {unique_filename}")
            except Exception as e:
                logging.warning(f"⚠️ Image abstract extraction failed: {unique_filename}, error: {e}")

        if ctx.progress_callback:
            await ctx.progress_callback(85, t("image_upload_success", language, "file_processor"))

        # Step 4: Start async background task for indicators extraction only
        if original_text and original_text.strip():
            asyncio.create_task(
                self._async_extract_indicators(
                    original_text=original_text,
                    user_id=int(ctx.target_user_id),
                    file_name=ctx.filename,
                    file_key=unique_filename,
                )
            )
            logging.info(f"📤 Image upload completed, async indicator extraction started: {unique_filename}")

        if ctx.progress_callback:
            await ctx.progress_callback(90, t("image_processing_success", language, "file_processor"))

        return {
            "raw": original_text or "",
            "file_abstract": file_abstract,
            "file_name": file_name,
            "original_text": original_text or "",
            "text_length": len(original_text) if original_text else 0,
            "content_hash": content_hash or "",
        }

    async def _save_original_text_to_db(
        self,
        file_key: str,
        original_text: str,
        text_length: int,
        content_hash: str,
    ):
        """Immediately save original_text to th_files table by file_key."""
        try:
            from mirobody.utils.db import execute_query
            
            sql = """
                UPDATE th_files 
                SET original_text = :original_text,
                    text_length = :text_length,
                    content_hash = :content_hash,
                    updated_at = NOW()
                WHERE file_key = :file_key
            """
            
            await execute_query(
                sql,
                params={
                    "file_key": file_key,
                    "original_text": original_text,
                    "text_length": text_length,
                    "content_hash": content_hash,
                },
                query_type="update",
                mode="async"
            )
            
        except Exception as e:
            logging.warning(f"⚠️ Failed to save image original text to th_files: {file_key}, error: {e}")

    async def _async_extract_indicators(
        self,
        original_text: str,
        user_id: int,
        file_name: str,
        file_key: str,
    ):
        """
        Async background task to extract indicators from original text.
        Updates th_files table when complete.
        """
        try:
            logging.info(f"🔄 Starting async indicator extraction for image: {file_key}")
            
            indicators = []
            llm_ret = {}
            formatted_raw = original_text
            
            try:
                (indicators, llm_ret) = await self.indicator_extractor.extract_indicators_from_text(
                    original_text=original_text,
                    user_id=user_id,
                    ocr_db_id=0,
                    source_table="th_files",
                    file_name=file_name,
                    file_key=file_key,
                    save_to_db=True,
                )
                logging.info(f"✅ Async indicator extraction completed for image: {file_key}, count: {len(indicators) if indicators else 0}")
                
                # Format content
                if indicators and llm_ret:
                    try:
                        formatted_raw = ContentFormatter.format_parsed_content(
                            file_results=[{"type": "image", "raw": ""}],
                            file_names=[file_key],
                            llm_responses=[llm_ret],
                            indicators_list=[indicators],
                        )
                    except Exception:
                        formatted_raw = original_text
            except Exception as e:
                logging.warning(f"⚠️ Async indicator extraction failed for image: {file_key}, error: {e}")
            
            # Update th_files with indicator results
            await self._update_file_indicators(
                file_key=file_key,
                formatted_raw=formatted_raw,
                indicators_count=len(indicators) if indicators else 0,
            )
            
            logging.info(f"✅ Async indicator extraction completed for image: {file_key}")
            
        except Exception as e:
            logging.error(f"❌ Async indicator extraction failed for image {file_key}: {e}", exc_info=True)

    async def _update_file_indicators(
        self,
        file_key: str,
        formatted_raw: str,
        indicators_count: int,
    ):
        """Update th_files with indicator extraction results."""
        try:
            from mirobody.utils.db import execute_query
            
            sql = """
                UPDATE th_files 
                SET file_content = jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            COALESCE(file_content, CAST('{}' AS jsonb)),
                            '{raw}',
                            to_jsonb(CAST(:raw AS text))
                        ),
                        '{indicators_count}',
                        to_jsonb(CAST(:indicators_count AS integer))
                    ),
                    '{processed}',
                    to_jsonb(true)
                ),
                updated_at = NOW()
                WHERE file_key = :file_key
            """
            
            await execute_query(
                sql,
                params={
                    "file_key": file_key,
                    "raw": formatted_raw,
                    "indicators_count": indicators_count,
                },
                query_type="update",
                mode="async"
            )
            
            logging.info(f"✅ Updated th_files indicators for image: {file_key}")
            
        except Exception as e:
            logging.warning(f"⚠️ Failed to update image indicators: {e}")



