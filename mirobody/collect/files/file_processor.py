"""
File processor service

Integrates various atomic services to provide complete file processing functionality
"""

from __future__ import annotations

from mirobody.collect.files.services.conversation_summary import update_message_content
import logging
from typing import Any
from collections.abc import Callable

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import request_language

from mirobody.collect.files.services.file_uploader import FileUploader
from mirobody.collect.files.services.indicator_extractor import IndicatorExtractor
from mirobody.collect.files.services.temp_file_manager import TempFileManager
from mirobody.collect.files.services.file_abstract_extractor import FileAbstractExtractor

from mirobody.collect.files.handlers.factory import FileHandlerFactory
from mirobody.collect.files.handlers.base import FileProcessingContext

logger = logging.getLogger(__name__)


class FileProcessor:
    """Main file processor service class"""

    def __init__(self):
        """Wire the extraction services and the handler factory.

        The two optional parameters that stood here (`excel_processor` and
        `csv_processor`) plus the `files/config.py` module that stored
        them globally, were an injection seam with no injector: both callers
        construct `FileProcessor()` with no arguments and nothing ever called
        the setters, so both attributes were always None. For Excel that made a
        branch unreachable; for CSV it made the format unsupported.
        """
        # Initialize services
        self.uploader = FileUploader()
        self.temp_manager = TempFileManager()
        self.indicator_extractor = IndicatorExtractor()
        self.abstract_extractor = FileAbstractExtractor()

        # Initialize Factory with services
        self.factory = FileHandlerFactory(
            uploader=self.uploader,
            temp_manager=self.temp_manager,
            indicator_extractor=self.indicator_extractor,
            abstract_extractor=self.abstract_extractor,
        )

    async def process_single_file(
        self,
        file: UploadFile,
        query: str,
        user_id: str,
        message_id: str | None = None,
        query_user_id: str = "",
        progress_callback: Callable[[int, str], None] | None = None,
        file_key: str | None = None,  # S3 key if already uploaded
        skip_upload_oss: bool = False,  # Skip upload to OSS if already uploaded
    ) -> dict[str, Any]:
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
            language = request_language()

            # The message id identifies the upload; the file name identifies the
            # PATIENT, because that is how a check-up report is named.
            logger.info(f"Starting file processing: message_id: {message_id}, operator_user_id: {user_id}, target_user_id: {target_user_id}")

            # Initial progress: file upload completed
            if progress_callback:
                await progress_callback(30, localize("file_upload_completed", language, "file_processor"))

            # Get Handler from Factory
            handler = await self.factory.get_handler(file)
            
            if not handler:
                return {
                    "success": False,
                    "message": localize("file_not_supported", language, "file_processor"),
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
            language = request_language()
            logger.error(f"File processing failed: {file.filename}, error: {e}", exc_info=True)

            # If there's a message ID, update message status to failed
            if message_id:
                try:
                    await update_message_content(
                        message_id=message_id,
                        content=f"❌ {localize('file_upload_failed', language, 'file_processor')}\n\n{localize('error', language, 'file_processor')}: {str(e)}",
                        reasoning=f"Error occurred during file processing: {str(e)}",
                    )
                except Exception as update_error:
                    logger.error(f"Failed to update message status: {str(update_error)}", exc_info=True)

            return {
                "success": False,
                "message": f"{localize('file_upload_failed', language, 'file_processor')}: {str(e)}",
                "status": "error",
                "message_id": message_id,
            }

