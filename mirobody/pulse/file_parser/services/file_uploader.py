"""
File upload service

Responsible for handling file uploads using unified storage client
"""

import asyncio
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Tuple

from fastapi import UploadFile

from mirobody.utils.config.storage import get_storage_client
from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx


# Supported file extensions
SUPPORTED_EXTENSIONS = {
    # Images
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".svg",
    # Documents
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt",
    # Other common formats
    ".json", ".csv", ".xml", ".zip", ".rar"
}


class FileUploader:
    """File upload service class"""

    @classmethod
    async def upload_file_and_get_url(
        cls,
        file: UploadFile,
        filename: str,
        content_type: str,
        expires: int = 7200 * 15,
    ) -> str:
        """
        Asynchronously upload file and get URL using unified storage client

        Args:
            file: Upload file object
            filename: Target filename
            content_type: Content type
            expires: Expiration time (seconds)

        Returns:
            str: File URL
        """
        try:
            # Reset file pointer and read content
            await file.seek(0)
            content = await file.read()

            # Delegate to upload_content_and_get_url
            return await cls.upload_content_and_get_url(
                file_content=content,
                filename=filename,
                content_type=content_type,
                expires=expires,
            )
        finally:
            # Reset file pointer for subsequent processing
            await file.seek(0)

    @classmethod
    async def upload_content_and_get_url(
        cls,
        file_content: bytes,
        filename: str,
        content_type: str,
        expires: int = 7200 * 15,
    ) -> str:
        """
        Directly upload file content and get URL using unified storage client

        Args:
            file_content: File content
            filename: Target filename
            content_type: Content type
            expires: Expiration time (seconds)

        Returns:
            str: File URL
        """
        try:
            language = get_req_ctx("language", "en")

            # Check if content is empty
            if not file_content or len(file_content) == 0:
                logging.error(f"File content is empty: {filename}")
                raise ValueError(t("file_empty", language))

            file_size = len(file_content)
            
            # Get storage client at runtime (lazy initialization)
            storage = get_storage_client()
            
            logging.info(f"Starting to upload file content using {storage.get_storage_type()} storage: {filename}, size: {file_size} bytes")

            # Set upload timeout based on file size
            upload_timeout = 30 if file_size <= 10 * 1024 * 1024 else 60  # 30s for <=10MB, 60s for >10MB

            # Use unified storage client with timeout control
            try:
                upload_task = asyncio.create_task(
                    storage.put(
                        key=filename,
                        content=file_content,
                        content_type=content_type,
                        expires=expires
                    )
                )
                full_url, error = await asyncio.wait_for(upload_task, timeout=upload_timeout)
                
                if error:
                    raise ValueError(f"Upload failed: {error}")
                    
            except asyncio.TimeoutError:
                logging.error(f"File upload timeout: {filename}, size: {file_size} bytes")
                raise ValueError(t("file_upload_timeout", language))

            if not full_url:
                raise ValueError(t("file_upload_failed", language))

            logging.info(f"File uploaded successfully to {storage.get_storage_type()} storage: {full_url}")

            return full_url

        except Exception as e:
            logging.error(f"File content upload failed: {str(e)}", stack_info=True)
            raise


# Utility functions for file upload operations

def validate_file_extension(file: UploadFile) -> Tuple[bool, str]:
    """
    Validate uploaded file extension
    
    Args:
        file: The uploaded file
        
    Returns:
        tuple[bool, str]: (is_valid, error_message)
    """
    # Check file extension
    file_extension = Path(file.filename).suffix.lower()
    if file_extension not in SUPPORTED_EXTENSIONS:
        error_msg = f"File type {file_extension} not supported. Supported types: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        return False, error_msg
    
    return True, ""


def generate_file_key(filename: str, folder_prefix: str = "uploads") -> str:
    """
    Generate a unique file key for cloud storage
    
    Args:
        filename: Original filename
        folder_prefix: Folder prefix for the file path (default: "uploads")
        
    Returns:
        str: Unique file key with timestamp and UUID
    """
    file_extension = Path(filename).suffix
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = uuid.uuid4().hex[:8]
    return f"{folder_prefix}/{timestamp}_{unique_id}{file_extension}"


def generate_s3_key(filename: str) -> str:
    """
    Generate a unique S3 key for the file (deprecated, use generate_file_key instead)
    
    Args:
        filename: Original filename
        
    Returns:
        str: Unique S3 key with timestamp and UUID
    """
    return generate_file_key(filename)


def get_file_type_category(content_type: str) -> str:
    """
    Determine file type category from content type
    
    Args:
        content_type: MIME content type
        
    Returns:
        str: File category (image, pdf, excel, document)
    """
    if not content_type:
        return "file"
        
    if content_type.startswith("image/"):
        return "image"
    elif content_type == "application/pdf":
        return "pdf"
    elif content_type in ["application/vnd.ms-excel", 
                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
        return "excel"
    else:
        return "document"
