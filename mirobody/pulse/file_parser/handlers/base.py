import abc
import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Set

from fastapi import UploadFile

from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx

# Import services type hints (avoid circular imports if possible, or use Any)
# In a real scenario, we might use Protocol or specific imports if avoiding circular deps.
# For now we assume services are passed in and duck-typed or we use Any.

@dataclass
class FileProcessingContext:
    file: UploadFile
    user_id: str
    message_id: Optional[str]
    query: str = ""
    query_user_id: str = ""
    progress_callback: Optional[Callable[[int, str], None]] = None
    file_key: Optional[str] = None
    skip_upload_oss: bool = False
    original_filename: Optional[str] = None
    
    @property
    def target_user_id(self) -> str:
        return self.query_user_id if self.query_user_id else self.user_id
    
    @property
    def filename(self) -> str:
        return self.original_filename or self.file.filename
        
    @property
    def content_type(self) -> str:
        return self.file.content_type

class BaseFileHandler(abc.ABC):
    def __init__(
        self, 
        uploader=None, 
        temp_manager=None, 
        content_extractor=None, 
        db_service=None, 
        indicator_extractor=None,
        abstract_extractor=None
    ):
        self.uploader = uploader
        self.temp_manager = temp_manager
        self.content_extractor = content_extractor
        self.db_service = db_service
        self.indicator_extractor = indicator_extractor
        self.abstract_extractor = abstract_extractor
        # Strong references to background tasks to prevent GC before completion
        self._background_tasks: Set[asyncio.Task] = set()

    async def process(self, ctx: FileProcessingContext) -> Dict[str, Any]:
        """Template method for file processing"""
        unique_filename = None  # Track file_key even if processing fails
        try:
            language = get_req_ctx("language", "en")
            
            # 1. Generate unique filename if needed
            unique_filename = self._get_unique_filename(ctx)
            
            # 2. Upload or Get URL (Common step, but can be overridden or skipped by subclasses)
            full_url = await self._handle_upload(ctx, unique_filename, language)
            
            # 3. Save to temp (Common step)
            temp_file_path = await self._save_to_temp(ctx, language)
            
            # 4. Core processing (Specific to file type)
            result_data = await self._process_content(ctx, temp_file_path, unique_filename, full_url, language)

            # 4.5. Auto-start background indicator extraction for any handler that returns original_text
            original_text = result_data.get("original_text")
            if original_text and original_text.strip() and self.indicator_extractor:
                self._start_background_indicator_extraction(
                    original_text=original_text,
                    user_id=int(ctx.target_user_id),
                    file_name=ctx.filename,
                    file_key=unique_filename,
                )

            # 5. Abstract extraction (Common step, but check if already extracted)
            if "file_abstract" in result_data and result_data["file_abstract"]:
                file_abstract = result_data["file_abstract"]
                # Use file_name from result if available
                file_name = result_data.get("file_name", ctx.filename)
            elif "extracted_abstract_hint" in result_data and result_data["extracted_abstract_hint"]:
                 file_abstract = result_data["extracted_abstract_hint"]
                 file_name = result_data.get("file_name", ctx.filename)
            else:
                file_abstract, file_name = await self._extract_abstract(ctx, unique_filename, language)
            
            # 6. Construct final response
            return self._build_response(ctx, result_data, unique_filename, full_url, file_abstract, file_name, language)
            
        except Exception as e:
            return await self._handle_error(ctx, e, unique_filename)

    def _get_unique_filename(self, ctx: FileProcessingContext) -> str:
        if ctx.file_key:
            return ctx.file_key
        
        # Default unique filename generation with web_uploads prefix
        extension = ctx.file.filename.split('.')[-1].lower() if '.' in ctx.file.filename else "bin"
        return f"web_uploads/{str(uuid.uuid4())}.{extension}"

    async def _handle_upload(self, ctx: FileProcessingContext, unique_filename: str, language: str) -> str:
        if ctx.progress_callback:
            await ctx.progress_callback(35, t("uploading_file", language, "file_processor"))

        if ctx.skip_upload_oss:
            # File already uploaded, generate signed URL
            from mirobody.utils.config.storage import get_storage_client
            try:
                storage = get_storage_client()
                full_url, err = await storage.generate_signed_url(unique_filename, content_type=ctx.content_type)
                if err:
                    logging.warning(err)

                full_url = full_url or ""
                logging.info(f"Skipping OSS upload, using existing file: {unique_filename}")
                return full_url
            except Exception as url_error:
                logging.warning(f"Failed to get URL for existing file: {url_error}")
                return ""
        else:
            # Upload
            try:
                full_url = await self.uploader.upload_file_and_get_url(
                    ctx.file, unique_filename, ctx.content_type
                )
                logging.info(f"File upload completed: {unique_filename}, URL: {full_url}")
                return full_url
            except Exception as e:
                # Some handlers might want to proceed even if upload fails (like text), 
                # others might fail. For now, log and return empty string.
                logging.warning(f"File upload failed: {e}")
                return ""

    async def _save_to_temp(self, ctx: FileProcessingContext, language: str) -> Optional[str]:
        if ctx.progress_callback:
            await ctx.progress_callback(45, t("saving_temp_file", language, "file_processor"))
            
        temp_file_path, _ = await self.temp_manager.save_upload_file_to_temp(ctx.file)
        return str(temp_file_path) if temp_file_path else None

    async def _extract_and_save_original_text(
        self,
        ctx: FileProcessingContext,
        temp_file_path: str,
        file_type: str,
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Extract original text from file with SHA256-based deduplication.
        
        Flow:
        1. Read file content and calculate SHA256 hash
        2. Check if hash exists in th_file_contents table
        3. If exists: return cached original_text (skip LLM extraction)
        4. If not: extract using LLM, save to th_file_contents, return text
        
        Args:
            ctx: File processing context
            temp_file_path: Path to temporary file
            file_type: File type ('pdf' or 'image')
            
        Returns:
            Tuple of (original_text, content_hash), or (None, None) if extraction failed
        """
        import hashlib
        
        try:
            # Read file content
            await ctx.file.seek(0)
            file_content = await ctx.file.read()
            
            if not file_content:
                logging.warning(f"[BaseFileHandler] Empty file content: {ctx.filename}")
                return None, None
            
            # Calculate SHA256 hash
            content_hash = hashlib.sha256(file_content).hexdigest()
            
            # Check th_file_contents for existing entry (direct SQL to avoid holywell dependency)
            try:
                from mirobody.utils.db import execute_query
                
                rows = await execute_query(
                    "SELECT decrypt_content(original_text) as original_text FROM th_file_contents WHERE content_hash = :hash LIMIT 1",
                    params={"hash": content_hash},
                )
                
                if rows and len(rows) > 0 and rows[0].get("original_text"):
                    cached_text = rows[0]["original_text"]
                    logging.info(
                        f"[BaseFileHandler] Reused cached original text: "
                        f"hash={content_hash[:16]}..., length={len(cached_text)}"
                    )
                    return cached_text, content_hash
            except Exception as e:
                logging.warning(f"[BaseFileHandler] Failed to check file_contents cache: {e}")
                # Continue with extraction even if cache check fails
            
            # Extract original text using LLM
            original_text = await self.abstract_extractor.extract_file_original_text(
                file_content=file_content,
                file_type=file_type,
                filename=ctx.filename,
                content_type=ctx.content_type,
            )
            
            # Save to th_file_contents for future deduplication (direct SQL)
            if original_text:
                try:
                    from mirobody.utils.db import execute_query
                    
                    # Use INSERT ... ON CONFLICT to handle race conditions
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
                            "file_type": file_type,
                        },
                    )
                    logging.info(
                        f"[BaseFileHandler] Extracted and saved original text: "
                        f"hash={content_hash[:16]}..., length={len(original_text)}"
                    )
                except Exception as e:
                    logging.warning(f"[BaseFileHandler] Failed to save to file_contents: {e}")
                    # Continue even if save fails - we still have the text
            
            return original_text, content_hash
            
        except Exception as e:
            logging.error(
                f"[BaseFileHandler] Failed to extract original text for {ctx.filename}: {e}",
                exc_info=True
            )
            return None, None

    async def _extract_abstract_from_text(
        self,
        original_text: str,
        filename: str,
        language: str,
    ) -> tuple[str, str]:
        """
        Generate file abstract and filename from pre-extracted original text.
        
        Args:
            original_text: Pre-extracted text content
            filename: Original file name
            language: User language
            
        Returns:
            Tuple of (file_abstract, file_name)
        """
        try:
            from mirobody.utils.llm import async_get_structured_output
            
            # Define response schema
            response_schema = {
                "type": "object",
                "properties": {
                    "file_name": {
                        "type": "string",
                        "description": "Generated filename with extension"
                    },
                    "file_abstract": {
                        "type": "string",
                        "description": "Brief summary of file content (max 150 chars)"
                    }
                },
                "required": ["file_name", "file_abstract"]
            }
            
            prompt = """Based on the document content below, generate:
1. file_name: A descriptive filename in format: Date_Content_Description.extension
   - Include date if found (YYYY-MM-DD format)
   - Keep it concise (15-40 chars excluding extension)
   - Use the same language as the content
   
2. file_abstract: A brief summary (max 150 characters)
   - Identify document type
   - Extract key information
   - Highlight main findings

Return JSON format: {"file_name": "...", "file_abstract": "..."}"""

            messages = [
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Original filename: {filename}\n\nDocument content:\n{original_text[:8000]}"}
            ]
            
            result = await async_get_structured_output(
                messages=messages,
                response_format={"type": "json_schema", "json_schema": {"name": "abstract_response", "schema": response_schema}},
                temperature=0.1,
                max_tokens=32000
            )
            
            if result and isinstance(result, dict):
                file_abstract = result.get("file_abstract", "")[:200]
                file_name = result.get("file_name", "") or filename
                logging.info(f"✅ Abstract from text: {filename}, abstract_len={len(file_abstract)}")
                return file_abstract, file_name
            
            return "", filename
            
        except Exception as e:
            logging.warning(f"[BaseFileHandler] Failed to extract abstract from text: {e}")
            return "", filename

    async def _extract_abstract(self, ctx: FileProcessingContext, unique_filename: str, language: str) -> tuple[str, str]:
        file_abstract = ""
        file_name = ctx.filename
        
        try:
            if ctx.progress_callback:
                await ctx.progress_callback(95, "Extracting file summary...")
            
            await ctx.file.seek(0)
            file_content = await ctx.file.read()
            
            # Get simple file type string (e.g., 'pdf', 'image')
            simple_type = self.get_type_name()
            
            result_data = await self.abstract_extractor.extract_file_abstract(
                file_content=file_content,
                file_type=simple_type,
                filename=ctx.filename,
                content_type=ctx.content_type
            )
            
            file_abstract = result_data.get("file_abstract", "")
            # Use generated file name if available, otherwise keep original
            extracted_name = result_data.get("file_name")
            if extracted_name:
                file_name = extracted_name
                
            logging.info(f"✅ {simple_type} abstract extracted: {ctx.filename}, abstract length: {len(file_abstract)}")
        except Exception as e:
            logging.warning(f"⚠️ Abstract extraction failed: {ctx.filename}, error: {e}")
            # Fallback
            simple_type = self.get_type_name()
            fallback = self.abstract_extractor._create_fallback_abstract(ctx.filename, simple_type)
            file_abstract = fallback.get("file_abstract", "")
            
        return file_abstract, file_name

    @abc.abstractmethod
    async def _process_content(self, ctx: FileProcessingContext, temp_file_path: str, unique_filename: str, full_url: str, language: str) -> Dict[str, Any]:
        """
        Core logic to extract content/indicators.
        Should return a dict with keys like 'raw', 'indicators', 'llm_ret', etc.
        """
        pass

    @abc.abstractmethod
    def get_type_name(self) -> str:
        pass

    def _build_response(
        self, 
        ctx: FileProcessingContext, 
        result_data: Dict[str, Any], 
        unique_filename: str, 
        full_url: str, 
        file_abstract: str, 
        file_name: str,
        language: str
    ) -> Dict[str, Any]:
        
        response = {
            "success": True,
            "message": t(f"{self.get_type_name()}_processing_success", language, "file_processor"),
            "type": self.get_type_name(),
            "filename": ctx.filename,
            "full_url": full_url,
            "file_abstract": file_abstract,
            "file_name": file_name,
            "message_id": ctx.message_id,
            "file_key": unique_filename,
        }
        
        # Merge specific result data
        response.update(result_data)
        
        # Optional: add url_thumb if same as full_url
        if "url_thumb" not in response and full_url:
            response["url_thumb"] = full_url
            
        return response

    async def _handle_error(self, ctx: FileProcessingContext, e: Exception, file_key: Optional[str] = None) -> Dict[str, Any]:
        language = get_req_ctx("language", "en")
        error_msg = str(e)
        logging.error(f"File processing failed: {ctx.filename}, file_key: {file_key}, error: {error_msg}", exc_info=True)

        if ctx.message_id:
            try:
                from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
                await FileParserDatabaseService.update_message_content(
                    message_id=ctx.message_id,
                    content=f"❌ {t('file_upload_failed', language, 'file_processor')}\n\n{t('error', language, 'file_processor')}: {error_msg}",
                    reasoning=f"Error occurred during file processing: {error_msg}",
                )
            except Exception as update_error:
                logging.error(f"Failed to update message status: {str(update_error)}", stack_info=True)

        user_message = t(f"{self.get_type_name()}_processing_failed", language, "file_processor")
        if not user_message:
             user_message = f"{t('file_upload_failed', language, 'file_processor')}: {error_msg}"

        # Some specialized error handling for JSON parsing if needed
        if "JSON parsing failed" in error_msg:
             user_message = t("json_parsing_failed", language, "file_processor") or "File processing failed: Invalid response format"

        return {
            "success": False,
            "message": user_message,
            "status": "error",
            "message_id": ctx.message_id,
            "filename": ctx.filename,
            "error": error_msg,
            "type": self.get_type_name(),
            "raw": f"Processing failed: {ctx.filename}",
            "file_key": file_key or "",  # Include file_key even on failure
        }

    # ── Shared indicator extraction methods (used by pdf, image, text, etc.) ──

    def _start_background_indicator_extraction(
        self,
        original_text: str,
        user_id: int,
        file_name: str,
        file_key: str,
    ):
        """Start background indicator extraction with GC-safe task reference."""
        task = asyncio.create_task(
            self._async_extract_indicators(
                original_text=original_text,
                user_id=user_id,
                file_name=file_name,
                file_key=file_key,
            )
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        logging.info(
            f"📤 {self.get_type_name()} upload completed, "
            f"background indicator extraction started: {file_key}"
        )

    async def _async_extract_indicators(
        self,
        original_text: str,
        user_id: int,
        file_name: str,
        file_key: str,
    ):
        """Background task: extract indicators from text and update th_files."""
        file_type = self.get_type_name()
        try:
            logging.info(f"🔄 Starting async indicator extraction for {file_type}: {file_key}")

            indicators = []
            llm_ret = {}
            formatted_raw = original_text

            try:
                from mirobody.pulse.file_parser.services.content_formatter import ContentFormatter

                (indicators, llm_ret) = await self.indicator_extractor.extract_indicators_from_text(
                    original_text=original_text,
                    user_id=user_id,
                    ocr_db_id=0,
                    source_table="th_files",
                    file_name=file_name,
                    file_key=file_key,
                    save_to_db=True,
                )
                logging.info(
                    f"✅ Async indicator extraction completed for {file_type}: {file_key}, "
                    f"count: {len(indicators) if indicators else 0}"
                )

                # Format content
                if isinstance(llm_ret, dict) and "formatted_content" in llm_ret:
                    formatted_raw = llm_ret["formatted_content"]
                elif indicators and llm_ret:
                    try:
                        formatted_raw = ContentFormatter.format_parsed_content(
                            file_results=[{"type": file_type, "raw": original_text}],
                            file_names=[file_key],
                            llm_responses=[llm_ret],
                            indicators_list=[indicators],
                        )
                    except Exception:
                        formatted_raw = original_text
            except Exception as e:
                logging.warning(f"⚠️ Async indicator extraction failed for {file_type}: {file_key}, error: {e}")

            # Update th_files with indicator results
            await self._update_file_indicators(
                file_key=file_key,
                formatted_raw=formatted_raw,
                indicators_count=len(indicators) if indicators else 0,
            )

            logging.info(f"✅ Async indicator extraction completed for {file_type}: {file_key}")

        except Exception as e:
            logging.error(f"❌ Async indicator extraction failed for {file_type} {file_key}: {e}", exc_info=True)

    async def _save_original_text_to_db(
        self,
        file_key: str,
        original_text: str,
        text_length: int,
        content_hash: str,
    ):
        """Save original_text to th_files table by file_key."""
        try:
            from mirobody.utils.db import execute_query

            sql = """
                UPDATE th_files
                SET original_text = encrypt_content(:original_text),
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
            )

        except Exception as e:
            logging.warning(f"⚠️ Failed to save original text to th_files: {file_key}, error: {e}")

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
            )

            logging.info(f"✅ Updated th_files indicators: {file_key}")

        except Exception as e:
            logging.warning(f"⚠️ Failed to update file indicators: {e}")

