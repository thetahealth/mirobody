"""
File Handler Module for DeepAgent

Handles file upload and processing logic for PostgreSQL backend.
"""

import logging
import os
from typing import Any
from urllib.parse import urlparse, unquote

import httpx

logger = logging.getLogger(__name__)


def upload_files_to_backend(
    file_list: list[dict[str, Any]], 
    backend: Any
) -> tuple[list[str], str]:
    """
    Upload files directly to PostgreSQL backend.
    
    Downloads remote files and uploads them as binary data to PostgreSQL.
    Files are stored with intelligent parsing support (lazy parsing on read).
    
    Args:
        file_list: List of file info dicts with keys:
            - file_name: Name of the file
            - file_url: URL to access the file (file://, http://, https://)
            - file_type: Type of file (pdf, image, etc.)
            - file_key: S3 key or storage identifier (optional)
            - file_size: File size in bytes (optional)
        backend: PostgresBackend instance with upload_files() method
        
    Returns:
        Tuple of (uploaded_file_paths, reminder_message)
    """
    if not file_list:
        return ([], "")
    
    files_to_upload = []  # List of (file_path, file_bytes) tuples
    uploaded_paths = []
    
    for file_info in file_list:
        file_name = file_info.get("file_name")
        file_url = file_info.get("file_url")
        
        # Skip files without required fields
        if not file_name or not file_url:
            logger.warning(f"Skipping file with missing name or URL: {file_info}")
            continue
        
        # Create database file path (no temp directories)
        file_path = f"/uploads/{file_name}"
        
        # Parse URL to determine source
        parsed_url = urlparse(file_url)
        is_local_file = parsed_url.scheme == "file"
        is_remote_file = parsed_url.scheme in ("http", "https")
        
        try:
            file_bytes = None
            
            if is_local_file:
                # Read local file as binary
                local_path = unquote(parsed_url.path)
                with open(local_path, 'rb') as f:
                    file_bytes = f.read()
                logger.info(f"📁 Read {len(file_bytes)} bytes from local file: {file_name}")
            
            elif is_remote_file:
                # Download remote file
                logger.info(f"📥 Downloading remote file: {file_name} from {file_url}")
                with httpx.Client(timeout=30.0) as client:
                    response = client.get(file_url)
                    response.raise_for_status()
                    file_bytes = response.content
                logger.info(f"✅ Downloaded {len(file_bytes)} bytes: {file_name}")
            
            else:
                logger.warning(f"⚠️ Unsupported URL scheme for {file_name}: {parsed_url.scheme}")
                continue
            
            if file_bytes:
                files_to_upload.append((file_path, file_bytes))
                uploaded_paths.append(file_path)
                
        except Exception as e:
            logger.error(f"❌ Failed to process file {file_name}: {e}", exc_info=True)
            continue
    
    # Upload all files to PostgreSQL backend
    if files_to_upload:
        try:
            upload_results = backend.upload_files(files_to_upload)
            
            # Check for upload errors
            successful_uploads = []
            for result in upload_results:
                if result.error:
                    logger.error(f"❌ Upload failed for {result.path}: {result.error}")
                else:
                    successful_uploads.append(result.path)
                    logger.info(f"✅ Uploaded to PostgreSQL: {result.path}")
            
            # Create reminder message
            if successful_uploads:
                if len(successful_uploads) == 1:
                    reminder = f"📎 Uploaded: {os.path.basename(successful_uploads[0])} → {successful_uploads[0]}\n\nUse read_file(\"{successful_uploads[0]}\") to read the file"
                else:
                    file_items = [f"{i+1}. {os.path.basename(p)} → {p}" for i, p in enumerate(successful_uploads)]
                    files_text = "\n".join(file_items)
                    reminder = f"📎 Uploaded {len(successful_uploads)} files:\n{files_text}\n\nExample: read_file(\"{successful_uploads[0]}\")"
                
                return (successful_uploads, reminder)
            
        except Exception as e:
            logger.error(f"❌ Backend upload failed: {e}", exc_info=True)
    
    return ([], "")
