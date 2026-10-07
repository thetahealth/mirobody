"""
Temporary file management service

Responsible for creating, deleting and other operations on temporary files
"""

from __future__ import annotations

import logging
import os
import tempfile
import time
import uuid
from pathlib import Path

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import request_language

logger = logging.getLogger(__name__)


class TempFileManager:
    """Temporary file management service class"""

    @staticmethod
    async def save_upload_file_to_temp(upload_file: UploadFile) -> tuple[Path, str]:
        """
        Save FastAPI's UploadFile object as a temporary file

        Args:
            upload_file: FastAPI's UploadFile object

        Returns:
            tuple[Path, str]: Path object and path string of the temporary file
        """
        temp_file_path = None
        try:
            language = request_language()
            # Read uploaded file content
            content = await upload_file.read()

            # Check if content is empty
            if not content or len(content) == 0:
                logger.error("upload is empty, no temporary copy made")
                raise ValueError(localize("file_empty", language, "temp_file_manager"))

            # Get original filename and extension
            filename = upload_file.filename
            suffix = os.path.splitext(filename)[1] if filename else ""

            # Generate unique temporary filename with UUID and timestamp
            unique_id = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"

            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{unique_id}{suffix}") as temp_file:
                # Write content
                temp_file.write(content)
                temp_file_path = temp_file.name

            # Reset file pointer
            await upload_file.seek(0)

            return Path(temp_file_path), temp_file_path
        except Exception as e:
            logger.error("temporary copy failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
            # If error occurs, ensure to delete potentially created temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except OSError as ex:
                    logger.error("temporary copy not deleted: error_type=%s", type(ex).__name__)
            raise

    @staticmethod
    def cleanup_temp_file(temp_file_path: str) -> bool:
        """
        Clean up temporary file

        Args:
            temp_file_path: Temporary file path

        Returns:
            bool: Whether successfully deleted
        """
        if not temp_file_path or not os.path.exists(temp_file_path):
            return True

        try:
            os.unlink(temp_file_path)
            return True
        except OSError as e:
            logger.error("temporary upload copy not deleted: error_type=%s", type(e).__name__)
            return False

