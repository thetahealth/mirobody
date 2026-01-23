"""
File processor service

Integrates various atomic services to provide complete file processing functionality
"""

import logging
from typing import Any, Callable, Dict, Optional

from fastapi import UploadFile

from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx

from mirobody.pulse.file_parser.services.compressed_file_processor import CompressedFileProcessor
from mirobody.pulse.file_parser.services.content_extractor import ContentExtractor
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.file_uploader import FileUploader
from mirobody.pulse.file_parser.services.indicator_extractor import IndicatorExtractor
from mirobody.pulse.file_parser.services.temp_file_manager import TempFileManager
from mirobody.pulse.file_parser.services.file_abstract_extractor import FileAbstractExtractor

from mirobody.pulse.file_parser.handlers.factory import FileHandlerFactory
from mirobody.pulse.file_parser.handlers.base import FileProcessingContext
from mirobody.pulse.file_parser.config import get_excel_processor, get_csv_processor


class FileProcessor:
    """Main file processor service class"""

    def __init__(self, excel_processor=None, csv_processor=None):
        """
        Initialize FileProcessor with optional excel_processor and csv_processor.
        
        Args:
            excel_processor: Optional ExcelProcessor instance. If None, will try to 
                           get from global config. If still None, Excel files will 
                           not be supported. This allows mcp_server to inject its 
                           own ExcelProcessor while mirobody can run without it.
            csv_processor: Optional CSVProcessor instance. If None, will try to 
                         get from global config. If still None, CSV files will 
                         not be supported.
        """
        # Initialize services
        self.uploader = FileUploader()
        self.temp_manager = TempFileManager()
        self.content_extractor = ContentExtractor()
        self.db_service = FileParserDatabaseService()
        self.indicator_extractor = IndicatorExtractor()
        self.compressed_processor = CompressedFileProcessor()
        self.abstract_extractor = FileAbstractExtractor()
        
        # Excel processor is optional - use provided or get from global config
        # This allows mcp_server to set the processor globally once during startup
        self.excel_processor = excel_processor or get_excel_processor()
        
        # CSV processor is optional - use provided or get from global config
        self.csv_processor = csv_processor or get_csv_processor()
        
        # Initialize Factory with services
        self.factory = FileHandlerFactory(
            uploader=self.uploader,
            temp_manager=self.temp_manager,
            content_extractor=self.content_extractor,
            db_service=self.db_service,
            indicator_extractor=self.indicator_extractor,
            abstract_extractor=self.abstract_extractor,
            excel_processor=self.excel_processor,
            csv_processor=self.csv_processor,
        )

    async def process_single_file(
        self,
        file: UploadFile,
        query: str,
        user_id: str,
        message_id: Optional[str] = None,
        query_user_id: str = "",
        progress_callback: Optional[Callable[[int, str], None]] = None,
        file_key: Optional[str] = None,  # S3 key if already uploaded
        skip_upload_oss: bool = False,  # Skip upload to OSS if already uploaded
    ) -> Dict[str, Any]:
        """
        Process single uploaded file

        Args:
            file: Uploaded file
            query: Query text
            user_id: User ID
            message_id: Message ID for updating processing status
            query_user_id: User ID for upload assistance, uses user_id if empty
            progress_callback: Progress callback function
            file_key: S3 key if file is already uploaded
            skip_upload_oss: Skip upload to OSS if file is already uploaded

        Returns:
            Dict[str, Any]: Processing result
        """
        try:
            # Determine target user ID, use query_user_id if available, otherwise use user_id
            target_user_id = query_user_id if query_user_id else user_id
            language = get_req_ctx("language", "en")

            logging.info(f"Starting file processing: {file.filename}, operator_user_id: {user_id}, target_user_id: {target_user_id}, message_id: {message_id}")

            # Initial progress: file upload completed
            if progress_callback:
                await progress_callback(30, t("file_upload_completed", language, "file_processor"))

            # Get Handler from Factory
            handler = await self.factory.get_handler(file)
            
            if not handler:
                return {
                    "success": False,
                    "message": t("file_not_supported", language, "file_processor"),
                }

            # Create Context
            ctx = FileProcessingContext(
                file=file,
                user_id=user_id,
                message_id=message_id,
                query=query,
                query_user_id=query_user_id,
                progress_callback=progress_callback,
                file_key=file_key,
                    skip_upload_oss=skip_upload_oss,
                original_filename=file.filename
            )

            # Execute Handler
            return await handler.process(ctx)

        except Exception as e:
            language = get_req_ctx("language", "en")
            logging.error(f"File processing failed: {file.filename}, error: {e}", exc_info=True)

            # If there's a message ID, update message status to failed
            if message_id:
                try:
                    await FileParserDatabaseService.update_message_content(
                        message_id=message_id,
                        content=f"❌ {t('file_upload_failed', language, 'file_processor')}\n\n{t('error', language, 'file_processor')}: {str(e)}",
                        reasoning=f"Error occurred during file processing: {str(e)}",
                    )
                except Exception as update_error:
                    logging.error(f"Failed to update message status: {str(update_error)}", exc_info=True)

            return {
                "success": False,
                "message": f"{t('file_upload_failed', language, 'file_processor')}: {str(e)}",
                "status": "error",
                "message_id": message_id,
            }

    async def delete_health_report(self, user_id: int, report_id: int) -> bool:
        """Delete health report"""
        return await self.db_service.delete_health_report(user_id, report_id)
            
