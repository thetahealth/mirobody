import abc
import uuid
import logging

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Union
from datetime import datetime

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
                full_url = await storage.generate_signed_url(unique_filename, content_type=ctx.content_type) or ""
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

