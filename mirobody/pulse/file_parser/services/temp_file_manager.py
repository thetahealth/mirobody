"""
Temporary file management service

Responsible for creating, deleting and other operations on temporary files
"""

import logging
import os
import tempfile
import time
import uuid
from pathlib import Path

from fastapi import UploadFile

from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx


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
            language = get_req_ctx("language", "en")
            # Read uploaded file content
            content = await upload_file.read()

            # Check if content is empty
            if not content or len(content) == 0:
                logging.error(f"File content is empty: {upload_file.filename}")
                raise ValueError(t("file_empty", language))

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
            logging.error("Error creating temporary file", stack_info=True)
            # If error occurs, ensure to delete potentially created temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception as ex:
                    logging.error(f"Error deleting temporary file: {str(ex)}")
            raise e

    @staticmethod
    def create_temp_file_from_content(content: bytes, filename: str) -> tuple[Path, str]:
        """
        Create temporary file from content

        Args:
            content: File content
            filename: Original filename

        Returns:
            tuple[Path, str]: Path object and path string of the temporary file
        """
        temp_file_path = None
        try:
            language = get_req_ctx("language", "en")
            # Check if content is empty
            if not content or len(content) == 0:
                logging.error(f"File content is empty: {filename}")
                raise ValueError(t("file_empty", language))

            # Get original filename and extension
            suffix = os.path.splitext(filename)[1] if filename else ""

            # Generate unique temporary filename
            unique_id = f"{int(time.time())}_{uuid.uuid4().hex[:8]}"

            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{unique_id}{suffix}") as temp_file:
                temp_file.write(content)
                temp_file_path = temp_file.name

            logging.info(f"Created temporary file: {temp_file_path}")
            return Path(temp_file_path), temp_file_path

        except Exception as e:
            logging.error(f"Failed to create temporary file: {filename}", stack_info=True)
            # If error occurs, ensure to delete potentially created temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception as ex:
                    logging.error(f"Error deleting temporary file: {str(ex)}")
            raise e

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
            logging.info(f"Temporary file deleted: {temp_file_path}")
            return True
        except Exception as e:
            logging.error(f"Failed to delete temporary file: {temp_file_path}, error: {str(e)}", stack_info=True)
            return False

    @staticmethod
    def cleanup_temp_files(temp_file_paths: list[str]) -> dict[str, bool]:
        """
        Batch cleanup temporary files

        Args:
            temp_file_paths: List of temporary file paths

        Returns:
            dict[str, bool]: Deletion result for each file
        """
        results = {}
        for temp_file_path in temp_file_paths:
            results[temp_file_path] = TempFileManager.cleanup_temp_file(temp_file_path)
        return results
