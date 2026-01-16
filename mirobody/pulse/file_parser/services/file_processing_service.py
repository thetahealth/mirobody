"""
File processing service for async file operations
"""

import asyncio
import json
import mimetypes
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import UploadFile
from pydantic import BaseModel

from mirobody.pulse.file_parser.file_processor import FileProcessor
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.db_utils import safe_json_loads
from mirobody.pulse.file_parser.services.file_uploader import (
    generate_file_key,
    get_file_type_category,
    validate_file_extension,
)
from mirobody.utils.config.storage import get_storage_client
from mirobody.utils.utils_audio import get_audio_duration_from_bytes
from mirobody.utils import execute_query
from mirobody.utils.utils_files.utils_s3 import get_content_type
from mirobody.utils.utils_user import get_query_user_id


class FileUploadData(BaseModel):
    """File upload data model - matches router FileUploadData structure"""
    file_url: str               # File access URL
    file_name: str              # Original filename
    file_key: str               # File storage key (S3/OSS)  
    file_size: int              # File size in bytes
    file_type: str              # File MIME type
    upload_time: datetime       # Upload timestamp
    duration: Optional[int] = None  # Audio duration in milliseconds (only for audio files)


async def process_file_uploads(
    files: List[UploadFile],
    user_id: str,
    upload_time: datetime,
    process_single_file_func,
) -> Dict[str, Any]:
    """
    Process multiple file uploads with common logic
    
    Args:
        files: List of files to upload
        user_id: User ID
        upload_time: Upload timestamp
        process_single_file_func: Function to process single file upload
        
    Returns:
        Dict containing:
        - successful_uploads: List of successful upload results
        - failed_uploads: List of failed upload results
        - files_info: List of file info for database storage
        - files_data_for_processing: List of file data for background processing
        - overall_type: Overall file type
        - url_thumbs: List of thumbnail URLs
        - url_fulls: List of full URLs
        - original_filenames: List of original filenames
        - file_sizes: List of file sizes
    """
    # Track results
    successful_uploads = []
    failed_uploads = []
    files_info = []
    url_thumbs = []
    url_fulls = []
    original_filenames = []
    file_sizes = []
    files_data_for_processing = []
    
    # Determine overall file type (based on first file or mixed)
    overall_type = "file"
    
    # Process all files concurrently
    logging.info(f"Starting concurrent upload of {len(files)} files for user {user_id}")
    
    # Create upload tasks for all files
    upload_tasks = [
        process_single_file_func(file, file_index, user_id, upload_time)
        for file_index, file in enumerate(files)
    ]
    
    # Execute all uploads concurrently
    upload_results = await asyncio.gather(*upload_tasks, return_exceptions=False)
    
    # Process results
    for result in upload_results:
        if result["success"]:
            # Successful upload
            successful_uploads.append({
                "file_name": result["file_name"],
                "url": result["url"],
                "file_key": result["file_key"],
                "file_size": result["file_size"],
                "file_type": result["file_type"],
                "upload_time": result["upload_time"]
            })
            
            # Add file info for database storage
            files_info.append({
                "filename": result["file_name"],
                "type": result["file_category"],
                "url_thumb": result["url"],
                "url_full": result["url"],
                "file_size": result["file_size"],
                "file_key": result["file_key"],
                "s3_key": result["file_key"],  # Keep for backward compatibility
                "processed": False,
                "raw": "",
                "file_abstract": "",
                "file_name": result["file_name"],  # Initialize with original filename
                "indicators": [],
                "indicators_count": 0
            })
            
            # Add to arrays for message content
            url_thumbs.append(result["url"])
            url_fulls.append(result["url"])
            original_filenames.append(result["file_name"])
            file_sizes.append(result["file_size"])
            
            # Store file data for background processing
            files_data_for_processing.append({
                "content": result["file_content"],
                "filename": result["file_name"],
                "content_type": result["file_type"],
                "s3_key": result["file_key"]  # Keep s3_key for compatibility but use file_key value
            })
            
            # Update overall type based on first successful file
            if overall_type == "file" and result["file_category"] != "file":
                overall_type = result["file_category"]
        else:
            # Failed upload
            failed_uploads.append({
                "file_name": result["file_name"],
                "error": result["error"]
            })
    
    logging.info(f"Concurrent upload completed: {len(successful_uploads)} successful, {len(failed_uploads)} failed")
    
    return {
        "successful_uploads": successful_uploads,
        "failed_uploads": failed_uploads,
        "files_info": files_info,
        "files_data_for_processing": files_data_for_processing,
        "overall_type": overall_type,
        "url_thumbs": url_thumbs,
        "url_fulls": url_fulls,
        "original_filenames": original_filenames,
        "file_sizes": file_sizes
    }


async def save_upload_message(
    msg_id: str,
    user_id: str,
    session_id: str,
    upload_result: Dict[str, Any],
    successful_count: int,
    failed_count: int,
    total_files: int
) -> Optional[Dict[str, Any]]:
    """
    Save file upload message to database
    
    Args:
        msg_id: Message ID
        user_id: User ID
        session_id: Session ID
        upload_result: Upload result from process_file_uploads
        successful_count: Number of successful uploads
        failed_count: Number of failed uploads
        total_files: Total number of files
        
    Returns:
        Database save result or None
    """
    upload_time = datetime.now()
    
    # Prepare content for th_messages (single record for all files)
    message_content = {
        "success": True,
        "message": f"Processing completed: {successful_count} files successful",
        "type": upload_result["overall_type"] if len(upload_result["files_info"]) == 1 else "file",
        "url_thumb": upload_result["url_thumbs"],
        "url_full": upload_result["url_fulls"],
        "message_id": msg_id,
        "files": upload_result["files_info"],
        "original_filenames": upload_result["original_filenames"],
        "file_sizes": upload_result["file_sizes"],
        "upload_time": upload_time.isoformat(),
        "total_files": total_files,
        "successful_files": successful_count,
        "failed_files": failed_count,
        "query_user_id": user_id,
        "status": "uploaded",
        "progress": 0,
        "timestamp": datetime.now().isoformat(),
        "session_id": session_id
    }
    
    # Save file upload message to database using database service
    db_result = await FileParserDatabaseService.save_file_upload_message(
        msg_id=msg_id,
        user_id=user_id,
        session_id=session_id,
        content=message_content,
        message_type=upload_result["overall_type"] if len(upload_result["files_info"]) == 1 else "file"
    )
    
    if db_result:
        logging.info(f"Files saved to database with msg_id: {msg_id}, total: {successful_count} files")
    
    return db_result


async def process_single_file_upload(
    file: UploadFile, 
    file_index: int, 
    user_id: str, 
    upload_time: datetime
) -> Dict[str, Any]:
    """
    Process single file upload concurrently
    
    Args:
        file: UploadFile object
        file_index: Index of the file in the batch
        user_id: User ID
        upload_time: Upload timestamp
        
    Returns:
        Dict containing upload result
    """
    try:
        # Get storage client at runtime (lazy initialization)
        storage = get_storage_client()
        
        logging.info(f"Processing file {file_index + 1}: {file.filename} for user {user_id}")
        
        # Validate file extension
        is_valid, error_msg = validate_file_extension(file)
        if not is_valid:
            return {
                "success": False,
                "file_name": file.filename,
                "error": error_msg,
                "file_index": file_index
            }
        
        # Read file content
        await file.seek(0)
        file_content = await file.read()
        file_size = len(file_content)
        
        # Check file size
        if file_size == 0:
            return {
                "success": False,
                "file_name": file.filename,
                "error": "File is empty",
                "file_index": file_index
            }
        
        # Determine content type
        content_type = file.content_type
        if not content_type:
            content_type = mimetypes.guess_type(file.filename)[0]
            if not content_type:
                file_extension = Path(file.filename).suffix.lower().lstrip('.')
                content_type = get_content_type(file_extension)
        
        # Determine file type category
        file_category = get_file_type_category(content_type)
        
        # Generate unique file key
        file_key = generate_file_key(file.filename)
        
        # Upload file using unified storage client
        logging.info(f"Uploading file using {storage.get_storage_type()} storage: {file.filename} -> {file_key}")
        
        file_url, error = await storage.put(
            key=file_key,
            content=file_content,
            content_type=content_type,
            expires=7200
        )
        
        if not file_url or error:
            return {
                "success": False,
                "file_name": file.filename,
                "error": error or "Failed to upload file to storage",
                "file_index": file_index
            }
        
        logging.info(f"File uploaded successfully to {storage.get_storage_type()} storage: {file_url}")
        
        return {
            "success": True,
            "file_name": file.filename,
            "url": file_url,
            "file_key": file_key,
            "file_size": file_size,
            "file_type": content_type,
            "file_category": file_category,
            "upload_time": upload_time.isoformat(),
            "file_content": file_content,
            "file_index": file_index
        }
        
    except Exception as e:
        logging.error(f"File upload failed for {file.filename}", stack_info=True)
        return {
            "success": False,
            "file_name": file.filename,
            "error": str(e),
            "file_index": file_index
        }


async def process_files_async(
    files_data: List[Dict[str, Any]],
    user_id: str,
    msg_id: str,
):
    """
    Asynchronously process all uploaded files to extract indicators
    
    Args:
        files_data: List of file data dictionaries containing content, filename, content_type, s3_key
        user_id: User ID
        msg_id: Message ID
    """
    
    async def process_single_file_wrapper(file_data: Dict[str, Any], file_processor: FileProcessor) -> dict:
        """
        Wrapper function to process a single file
        
        Args:
            file_data: Dictionary with file data (content, filename, content_type, s3_key)
            file_processor: The FileProcessor instance
            
        Returns:
            dict: Processing result
        """
        try:
            filename = file_data["filename"]
            logging.info(f"Processing file: {filename}, msg_id: {msg_id}")
            
            # Create a mock UploadFile object for the processor
            class MockUploadFile:
                def __init__(self, filename: str, file_content: bytes, content_type: str):
                    self.filename = filename
                    self.content_type = content_type
                    self.file = BytesIO(file_content)
                    self._content = file_content
                    
                async def read(self):
                    self.file.seek(0)
                    return self.file.read()
                    
                async def seek(self, offset):
                    return self.file.seek(offset)
                    
                def close(self):
                    self.file.close()
            
            mock_file = MockUploadFile(
                filename=filename,
                file_content=file_data["content"],
                content_type=file_data["content_type"]
            )
            
            # Process single file (skip upload since already uploaded)
            result = await file_processor.process_single_file(
                file=mock_file,
                user_id=user_id,
                message_id=msg_id,
                query="",
                file_key=file_data.get("s3_key"),  # Pass the S3 key
                skip_upload_oss=True,  # Skip upload since already uploaded
            )
            
            if result.get("success"):
                # Extract indicators from result if available
                indicators = result.get("indicators", [])
                
                processed_file_info = {
                    "filename": filename,
                    "processed": True,
                    "raw": result.get("raw", result.get("content", "")),  # Use 'raw' field name
                    "file_abstract": result.get("file_abstract", ""),  # Add file abstract
                    "file_name": result.get("file_name", filename),  # Add generated file name, fallback to original
                    "indicators": indicators,
                    "indicators_count": len(indicators)  # Add indicators count
                }
            else:
                processed_file_info = {
                    "filename": filename,
                    "processed": False,
                    "file_name": result.get("file_name", filename),  # Add generated file name even on failure
                    "error": result.get("error", "Processing failed")
                }
            
            logging.info(f"Completed processing file: {filename}, success: {result.get('success')}")
            
            return processed_file_info
            
        except Exception as file_error:
            logging.error(f"Error processing file {file_data.get('filename', 'unknown')}: {str(file_error)}", stack_info=True)
            return {
                "filename": file_data.get("filename", "unknown"),
                "processed": False,
                "error": str(file_error)
            }
    
    try:
        logging.info(f"Starting concurrent async processing for {len(files_data)} files, msg_id: {msg_id}")
        
        # Create file processor instance
        file_processor = FileProcessor()
        
        # Use asyncio to process files concurrently
        import asyncio
        
        # Process all files concurrently using asyncio.gather
        processing_tasks = [
            process_single_file_wrapper(file_data, file_processor) 
            for file_data in files_data
        ]
        
        processed_files = await asyncio.gather(*processing_tasks, return_exceptions=False)
        
        # Filter out any None results
        processed_files = [pf for pf in processed_files if pf is not None]
        
        # Update th_messages with all processed results at once
        if processed_files:
            logging.info(f"Updating database with {len(processed_files)} processed files")
            
            # Update database service method
            await FileParserDatabaseService.update_message_processed_files(
                msg_id=msg_id,
                processed_files=processed_files
            )
            
            logging.info(f"Concurrent async processing completed for all files, msg_id: {msg_id}")
            
    except Exception as e:
        logging.error(f"Concurrent async processing failed for files batch, msg_id: {msg_id}", stack_info=True)


async def delete_files_from_message(
    message_id: str,
    file_keys: list[str],
    user_id: str
) -> dict[str, Any]:
    """
    Delete specific files from a message
    
    Args:
        message_id: The message ID containing the files (may include #idx suffix)
        file_keys: List of file keys to delete
        user_id: User ID for authorization
    
    Returns:
        Dict containing deletion results
    """
    try:
        # Handle message_id with sub-index suffix (e.g., "msg_id#0" -> "msg_id")
        # This suffix is added by get_uploaded_files_paginated when a message contains multiple files
        if "#" in message_id:
            original_id = message_id
            message_id = message_id.split("#")[0]
            logging.info(f"Parsed message_id: original={original_id}, actual_msg_id={message_id}")
        
        logging.info(f"Starting file deletion: message_id={message_id}, file_keys={file_keys}")
        
        # Get message content from database
        query = """
            SELECT theta_ai.decrypt_content(content) AS content, user_id, query_user_id
            FROM theta_ai.th_messages 
            WHERE id = :message_id
            LIMIT 1
        """
        result = await execute_query(
            query=query,
            params={"message_id": message_id},
        )
        
        if not result:
            return {
                "success": False,
                "error": "Message not found",
                "message_id": message_id
            }
        
        # Handle different result formats from execute_query
        if isinstance(result, list) and len(result) > 0:
            message_data = result[0]
        elif isinstance(result, dict):
            message_data = result
        else:
            return {
                "success": False,
                "error": "Invalid message data format",
                "message_id": message_id
            }
        
        # Validate user permission to delete files
        msg_user_id = str(message_data.get("user_id")) if message_data.get("user_id") else None
        msg_query_user_id = str(message_data.get("query_user_id")) if message_data.get("query_user_id") else None
        owner_user_id = msg_query_user_id or msg_user_id
        
        permission_result = await validate_file_operation_permission(
            user_id=user_id, 
            owner_user_id=owner_user_id, 
            required_permissions={'upload': 2}
        )
        
        if not permission_result["success"]:
            logging.warning(f"Permission denied for file deletion: {permission_result['message']}")
            return {
                "success": False,
                "error": f"Permission denied: {permission_result['message']}",
                "message_id": message_id
            }
        
        # Parse content field - it may be a JSON string
        message_content = safe_json_loads(message_data.get("content", "{}"))
        
        
        # Get files from content
        files_list = message_content.get("files", [])
        if not files_list:
            return {
                "success": False,
                "error": "No files found in this message",
                "message_id": message_id
            }
        
        # Track deletion results
        deleted_files = []
        failed_deletions = []
        remaining_files = []
        
        # Process each file
        for file_info in files_list:
            current_file_key = file_info.get("file_key") or file_info.get("s3_key")
            
            if current_file_key in file_keys:
                # Delete from storage using unified storage client
                deletion_success = await delete_file_from_storage(
                    file_key=current_file_key
                )
                
                if deletion_success:
                    deleted_files.append({
                        "file_key": current_file_key,
                        "filename": file_info.get("filename", ""),
                        "type": file_info.get("type", "other"),  # Get file type from files array
                        "status": "deleted"
                    })
                    logging.info(f"Successfully deleted file: {current_file_key}")
                else:
                    failed_deletions.append({
                        "file_key": current_file_key,
                        "filename": file_info.get("filename", ""),
                        "type": file_info.get("type", "other"),  # Get file type from files array
                        "status": "failed",
                        "error": "Storage deletion failed"
                    })
                    # Still remove from database even if storage deletion fails
                    logging.warning(f"Storage deletion failed but removing from database: {current_file_key}")
            else:
                # Keep files not marked for deletion
                remaining_files.append(file_info)
        
        query_user_id = message_data.get("query_user_id", user_id)
        # Update message in database
        update_success = await update_message_after_deletion(
            message_id=message_id,
            remaining_files=remaining_files,
            message_content=message_content
        )
        
        if not update_success:
            return {
                "success": False,
                "error": "Failed to update message in database",
                "message_id": message_id,
                "deleted_files": deleted_files,
                "failed_deletions": failed_deletions
            }
        
        # Start background cascade delete task for successfully deleted files
        owner_user_id = query_user_id if query_user_id else user_id
        if deleted_files:
            _start_background_cascade_delete(
                message_id=message_id,
                user_id=owner_user_id,
                deleted_files=deleted_files
            )

        return {
            "success": True,
            "message_id": message_id,
            "deleted_files": deleted_files,
            "failed_deletions": failed_deletions,
            "remaining_files_count": len(remaining_files),
            "message_deleted": len(remaining_files) == 0
        }
        
    except Exception as e:
        logging.error(f"Error in delete_files_from_message: {str(e)}", stack_info=True)
        return {
            "success": False,
            "error": f"Internal error: {str(e)}",
            "message_id": message_id
        }


async def delete_file_from_storage(file_key: str, is_aliyun: bool = False) -> bool:
    """
    Delete a file from storage using unified storage client
    
    Args:
        file_key: The storage key of the file
        is_aliyun: Deprecated parameter, kept for compatibility
    
    Returns:
        bool: True if deletion successful, False otherwise
    """
    try:
        # Get storage client at runtime
        storage = get_storage_client()
        
        # Use unified storage client
        success, error = await storage.delete(file_key)
        
        if not success:
            logging.warning(f"Failed to delete file {file_key}: {error}")
        
        return success
        
    except Exception as e:
        logging.error(f"Error deleting file from storage: {str(e)}", stack_info=True)
        return False


async def update_message_after_deletion(
    message_id: str,
    remaining_files: list[dict[str, Any]],
    message_content: dict[str, Any]
) -> bool:
    """
    Update message in database after file deletion
    
    Args:
        message_id: The message ID
        remaining_files: List of files that remain after deletion
        message_content: Original message content
    
    Returns:
        bool: True if update successful
    """
    try:
        if len(remaining_files) == 0:
            # No files left, mark message as deleted
            update_query = """
                UPDATE theta_ai.th_messages
                SET is_del = true,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :message_id
                RETURNING id
            """
            params = {"message_id": message_id}
            
            logging.info(f"Marking message as deleted: {message_id}")
        else:
            # Update content with remaining files
            updated_content = message_content.copy()
            updated_content["files"] = remaining_files
            
            # Update file counts and URLs
            updated_content["url_thumb"] = [f["url_thumb"] for f in remaining_files if "url_thumb" in f]
            updated_content["url_full"] = [f["url_full"] for f in remaining_files if "url_full" in f]
            updated_content["original_filenames"] = [f["filename"] for f in remaining_files if "filename" in f]
            updated_content["file_sizes"] = [f["file_size"] for f in remaining_files if "file_size" in f]
            updated_content["total_files"] = len(remaining_files)
            updated_content["successful_files"] = len(remaining_files)
            
            update_query = """
                UPDATE theta_ai.th_messages
                SET content = theta_ai.encrypt_content(:content),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :message_id
                RETURNING id
            """
            params = {
                "message_id": message_id,
                "content": json.dumps(updated_content)
            }
            
            logging.info(f"Updating message content with {len(remaining_files)} remaining files")
        
        result = await execute_query(
            query=update_query,
            params=params,
        )
        
        return result is not None
        
    except Exception as e:
        logging.error(f"Error updating message after deletion: {str(e)}", stack_info=True)
        return False


async def _mark_message_as_deleted(message_id: str) -> dict[str, Any]:
    """
    Mark a message as deleted in the database
    
    Args:
        message_id: The message ID to mark as deleted
    
    Returns:
        Dict containing operation result
    """
    update_query = """
        UPDATE theta_ai.th_messages
        SET is_del = true,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = :message_id
        RETURNING id
    """
    
    try:
        result = await execute_query(
            query=update_query,
            params={"message_id": message_id},
        )
        
        if result:
            logging.info(f"Successfully marked message as deleted: message_id={message_id}")
            return {
                "success": True,
                "message_id": message_id,
                "deleted_files": [],
                "failed_deletions": [],
                "remaining_files_count": 0,
                "message_deleted": True
            }
        else:
            return {
                "success": False,
                "error": "Failed to mark message as deleted",
                "message_id": message_id
            }
    except Exception as e:
        logging.error(f"Error marking message as deleted: {str(e)}", stack_info=True)
        return {
            "success": False,
            "error": f"Failed to delete message: {str(e)}",
            "message_id": message_id
        }


async def delete_all_files_from_message(
    message_id: str,
    user_id: str
) -> dict[str, Any]:
    """
    Delete all files from a message
    
    Handles two scenarios:
    1. Normal files with valid file_keys: Delete from storage and update database
    2. Failed uploads without file_keys: Directly mark message as deleted
    
    Args:
        message_id: The message ID (may include #idx suffix)
        user_id: User ID for authorization
    
    Returns:
        Dict containing deletion results
    """
    # Handle message_id with sub-index suffix (e.g., "msg_id#0" -> "msg_id")
    # This suffix is added by get_uploaded_files_paginated when a message contains multiple files
    if "#" in message_id:
        original_id = message_id
        message_id = message_id.split("#")[0]
        logging.info(f"Parsed message_id: original={original_id}, actual_msg_id={message_id}")
    
    # Fetch message data
    query = """
        SELECT theta_ai.decrypt_content(content) AS content, user_id, query_user_id
        FROM theta_ai.th_messages 
        WHERE id = :message_id
        LIMIT 1
    """
    result = await execute_query(
        query=query,
        params={"message_id": message_id},
    )
    
    if not result:
        return {"success": False, "error": "Message not found", "message_id": message_id}
    
    # Normalize result format
    message_data = result[0] if isinstance(result, list) else result
    if not message_data:
        return {"success": False, "error": "Invalid message data format", "message_id": message_id}
    
    # Validate permissions
    owner_user_id = str(message_data.get("query_user_id") or message_data.get("user_id") or "")
    permission_result = await validate_file_operation_permission(
        user_id=user_id, 
        owner_user_id=owner_user_id, 
        required_permissions={'upload': 2}
    )
    
    if not permission_result["success"]:
        logging.warning(f"Permission denied for deleting files: message_id={message_id}, reason={permission_result['message']}")
        return {
            "success": False,
            "error": f"Permission denied: {permission_result['message']}",
            "message_id": message_id
        }
    
    # Parse message content
    message_content = safe_json_loads(message_data.get("content", "{}"))
    files_list = message_content.get("files", [])
    
    if not files_list:
        return {"success": False, "error": "No files found in this message", "message_id": message_id}
    
    # Extract valid file keys
    file_keys = [
        file_info.get("file_key") or file_info.get("s3_key")
        for file_info in files_list
        if file_info.get("file_key") or file_info.get("s3_key")
    ]
    
    # Handle failed uploads (no file_keys means files never reached storage)
    if not file_keys:
        logging.info(f"No valid file_keys found (failed upload), directly marking message as deleted: message_id={message_id}, files_count={len(files_list)}")
        result = await _mark_message_as_deleted(message_id)
        if result["success"]:
            result["note"] = "Message marked as deleted (all files had failed processing)"
        return result
    
    # Normal deletion flow with valid file_keys
    return await delete_files_from_message(
        message_id=message_id,
        file_keys=file_keys,
        user_id=user_id
    )


async def _background_cascade_delete_by_file_info(
    message_id: str,
    user_id: str,
    deleted_files: List[Dict[str, Any]]
) -> None:
    """
    Background task to cascade delete related health data (th_series_data and genetic data) for deleted files.
    
    Strategy:
    - For genetic files (type='genetic'): Delete genetic data from th_genetic_data table
    - For non-genetic files: Delete health indicators from th_series_data table
    
    Args:
        message_id: Message ID containing the deleted files
        user_id: User ID
        deleted_files: List of deleted file info containing file_key and filename
    """
    try:
        logging.info(f"Starting background cascade delete task: message_id={message_id}, user_id={user_id}, files_count={len(deleted_files)}")
        
        # Process cascade delete for each deleted file
        for file_info in deleted_files:
            filename = file_info.get("filename", "")
            file_key = file_info.get("file_key", "")
            file_type = file_info.get("type", "other")
            
            # Different deletion strategy based on file type
            if file_type == "genetic":
                # For genetic files, only delete genetic data
                genetic_delete_success = await _delete_genetic_data_background(user_id, message_id)
                if genetic_delete_success:
                    logging.info(f"Genetic data deletion successful for genetic file: user_id={user_id}, message_id={message_id}, filename={filename}, type={file_type}")
                else:
                    logging.warning(f"Genetic data deletion failed or no data found: user_id={user_id}, message_id={message_id}, filename={filename}, type={file_type}")
            else:
                # For non-genetic files, delete th_series_data
                await _delete_th_series_data_background(user_id, "theta_ai.th_messages", message_id, file_key)
        
        logging.info(f"Background cascade delete task completed successfully: message_id={message_id}, user_id={user_id}")
        
    except Exception as e:
        logging.error(f"Background cascade delete task failed: message_id={message_id}, user_id={user_id}, error={str(e)}", stack_info=True)


async def _delete_th_series_data_background(
    user_id: str, 
    source_table: str, 
    message_id: str, 
    file_key: Optional[str] = None
) -> None:
    """
    Physically delete th_series_data in background task (DELETE statement)
    
    Args:
        user_id: User ID
        source_table: Source table name (not used in WHERE clause)
        message_id: Message ID
        file_key: File key for precise deletion (if None, will try both new and old formats)
    """
    try:
        delete_count = 0
        
        if file_key:
            # Use efficient OR condition to handle both new and old formats in one query
            new_source_table_id = FileParserDatabaseService.generate_source_table_id(message_id, file_key)
            
            delete_sql = """
            DELETE FROM theta_ai.th_series_data 
            WHERE user_id = :user_id 
              AND (source_table_id = :new_source_table_id 
                   OR (source_table_id = :old_source_table_id 
                       AND NOT EXISTS (
                           SELECT 1 FROM theta_ai.th_series_data t2 
                           WHERE t2.user_id = :user_id 
                             AND t2.source_table_id = :new_source_table_id
                       )))
            """

            result = await execute_query(
                delete_sql,
                {
                    "user_id": user_id,
                    "new_source_table_id": new_source_table_id,
                    "old_source_table_id": message_id,
                },
            )
            
            delete_count = len(result) if result else 0
            
            logging.info(f"th_series_data physical deletion successful: user_id={user_id}, message_id={message_id}, file_key={file_key}, deleted_count={delete_count}")
        else:
            # No file_key provided - use old format (backward compatibility)
            delete_sql = """
            DELETE FROM theta_ai.th_series_data 
            WHERE user_id = :user_id 
              AND source_table_id = :source_table_id
            """

            result = await execute_query(
                delete_sql,
                {
                    "user_id": user_id,
                    "source_table_id": message_id,
                },
            )
            
            delete_count = len(result) if result else 0
            
            logging.info(f"th_series_data physical deletion successful (old format): user_id={user_id}, message_id={message_id}, deleted_count={delete_count}")

    except Exception as e:
        logging.warning(f"th_series_data physical deletion failed: user_id={user_id}, message_id={message_id}, file_key={file_key}, error={str(e)}", stack_info=True)
        raise


async def _delete_genetic_data_background(user_id: str, message_id: str) -> bool:
    """
    Delete genetic data in background task
    
    Args:
        user_id: User ID
        message_id: Message ID
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        delete_success = await FileParserDatabaseService.delete_genetic_data_by_message_id(user_id, message_id)
        
        if delete_success:
            logging.info(f"Genetic data deletion successful: user_id={user_id}, message_id={message_id}")
        else:
            logging.info(f"No genetic data found for deletion: user_id={user_id}, message_id={message_id}")
            
        return delete_success

    except Exception as e:
        logging.warning(f"Genetic data deletion failed: user_id={user_id}, message_id={message_id}, error={str(e)}", stack_info=True)
        return False


def _start_background_cascade_delete(
    message_id: str,
    user_id: str,
    deleted_files: List[Dict[str, Any]]
) -> None:
    """
    Start background cascade delete task
    
    Args:
        message_id: Message ID
        user_id: User ID
        deleted_files: List of deleted file information
    """
    # Record task creation time
    task_create_time = datetime.now()
    logging.info(f"Creating background cascade delete task - message_id: {message_id}, user_id: {user_id}, files_count: {len(deleted_files)}, created_at: {task_create_time}")

    task = asyncio.create_task(
        _background_cascade_delete_by_file_info(
            message_id, user_id, deleted_files
        )
    )

    # Add task completion callback
    def task_done_callback(task):
        completion_time = datetime.now()
        duration = (completion_time - task_create_time).total_seconds()
        if task.exception():
            logging.error(f"Background cascade delete task failed - message_id: {message_id}, user_id: {user_id}, duration: {duration:.2f}s", stack_info=True)
        else:
            logging.info(f"Background cascade delete task completed - message_id: {message_id}, user_id: {user_id}, duration: {duration:.2f}s")

    task.add_done_callback(task_done_callback)

async def upload_files_to_storage(
    files: List[UploadFile], 
    user_id: str,
    folder_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Universal file upload service that can be reused across projects
    
    Uploads multiple files directly to S3/Aliyun OSS without storing metadata in database.
    This function is project-agnostic and can be used in holywell or other projects.
    
    Args:
        files: List of files to upload (UploadFile objects)
        user_id: User identifier for logging and authentication context
        folder_prefix: Custom folder prefix for uploaded files (optional, defaults to 'uploads')
        max_file_size: Maximum allowed file size in bytes (default 50MB)
        
    Returns:
        Dict containing (same format as original FileUploadResponse):
        - code: 0 for complete success, 1 for partial/complete failure
        - msg: Result message
        - data: List of successful upload results (or None if all failed)
        
    Each successful upload result contains (same format as FileUploadData):
        - url: File access URL
        - file_name: Original filename
        - file_key: Storage key (S3/OSS)
        - file_size: File size in bytes  
        - file_type: MIME type
        - upload_time: Upload timestamp (datetime object)
    """
    
    # Input validation
    if not files:
        return {
            "code": 1,
            "msg": "No files provided",
            "data": None
        }
    
    # Get storage client at runtime (lazy initialization)
    storage = get_storage_client()
    
    # Track upload results
    successful_uploads: List[FileUploadData] = []
    failed_uploads = []
    
    logging.info(f"Starting batch upload of {len(files)} files for user {user_id} using {storage.get_storage_type()} storage")
    
    # Process each file
    for file_index, file in enumerate(files):
        try:
            logging.info(f"Processing file {file_index + 1}/{len(files)}: {file.filename}")
            
            
            # Read file content
            await file.seek(0)  # Reset file pointer to beginning
            file_content = await file.read()
            file_size = len(file_content)
            
            # Validate file size
            if file_size == 0:
                failed_uploads.append({
                    "file_name": file.filename,
                    "error": "File is empty"
                })
                continue
            
            # Determine content type
            content_type = file.content_type or "application/octet-stream"
            
            # Generate unique file key for storage
            if folder_prefix is not None:
                file_key = generate_file_key(file.filename, folder_prefix=folder_prefix)
            else:
                file_key = generate_file_key(file.filename)
            
            # Record upload start time
            upload_time = datetime.now()
            
            logging.info(f"Uploading file: {file.filename} ({file_size} bytes) -> {file_key}")
            
            # Upload file using unified storage client
            file_url, error = await storage.put(
                key=file_key,
                content=file_content,
                content_type=content_type,
                expires=7200
            )
            if not file_url or error:
                failed_uploads.append({
                    "file_name": file.filename,
                    "error": error or "Failed to upload file to storage backend"
                })
                continue           
            
            logging.info(f"File uploaded successfully: {file.filename} -> {file_url}")
            
            # Calculate audio duration if file is audio type
            audio_duration = 0
            if content_type and content_type.startswith("audio/"):
                try:
                    audio_duration = get_audio_duration_from_bytes(file_content, content_type)
                    if audio_duration:
                        logging.info(f"Calculated audio duration for {file.filename}: {audio_duration}ms")
                except Exception as duration_error:
                    logging.warning(f"Failed to calculate duration for {file.filename}: {str(duration_error)}")
            
            # Create upload result data using FileUploadData structure
            upload_data = FileUploadData(
                file_url=file_url,
                file_name=file.filename,
                file_key=file_key,
                file_size=file_size,
                file_type=content_type,
                upload_time=upload_time,
                duration=audio_duration
            )
            successful_uploads.append(upload_data.model_dump())
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"File upload failed for {file.filename}: {error_msg}", stack_info=True)
            failed_uploads.append({
                "file_name": file.filename,
                "error": f"Upload failed: {error_msg}"
            })
    
    # Calculate statistics
    total_files = len(files)
    successful_count = len(successful_uploads)
    failed_count = len(failed_uploads)
    
    # Determine overall result code and message
    if successful_count == total_files:
        # All files uploaded successfully
        code = 0
        msg = f"All {total_files} files uploaded successfully"
        data = successful_uploads
    elif successful_count == 0:
        # All files failed to upload
        code = 1
        msg = f"All {total_files} files failed to upload"
        data = None
    else:
        # Partial success
        code = 1
        msg = f"Partial upload: {successful_count} succeeded, {failed_count} failed"
        data = successful_uploads  # Return successful ones for partial success
    
    logging.info(f"Batch upload completed - Total: {total_files}, Success: {successful_count}, Failed: {failed_count}")
    

    return {
        "code": code,
        "msg": msg,
        "data": data
    }


async def validate_file_operation_permission(
    user_id: str, 
    owner_user_id: Optional[str], 
    required_permissions: Dict[str, int] = None
) -> Dict[str, Any]:
    """
    Validate user permission for file operations
    
    Args:
        user_id: Current user ID performing the operation
        owner_user_id: Owner user ID (resource owner, if None defaults to user_id)
        required_permissions: Dict of required permissions with levels (e.g., {'upload': 2})
                             2 = write permission, 1 = read permission, 0 = no permission
        
    Returns:
        Dict containing validation result:
        - success: bool - Whether validation passed
        - owner_user_id: str - Final owner user ID
        - message: str - Error message if validation failed
    """
    # Set default permissions if not provided
    if required_permissions is None:
        required_permissions = {'upload': 2}  # Default to upload write permission
    
    # If no query user specified, default to current user
    if not owner_user_id:
        return {
            "success": True,
            "owner_user_id": user_id,
            "message": "Success"
        }
    
    # If query user is same as current user, allow
    if owner_user_id == user_id:
        return {
            "success": True,
            "owner_user_id": owner_user_id,
            "message": "Success"
        }
    
    try:
        # Check permissions for different user
        # Convert dict keys to list for permission parameter
        permission_list = list(required_permissions.keys())
        user_validation = await get_query_user_id(
            user_id=owner_user_id, 
            query_user_id=user_id,
            permission=permission_list
        )
        if not user_validation.get("success", False):
            logging.warning(f"No permission for query user: {owner_user_id}, user_id: {user_id}")
            return {
                "success": False,
                "owner_user_id": owner_user_id,
                "message": "No permission to access query user"
            }
        
        # Check individual permissions and their levels
        permissions_check = user_validation.get("permissions", {})
        for permission_name, required_level in required_permissions.items():
            actual_level = permissions_check.get(permission_name, 0)
            if actual_level < required_level:
                logging.warning(f"Insufficient permission level: {permission_name}, required: {required_level}, actual: {actual_level}, user_id: {user_id}, owner_user_id: {owner_user_id}")
                permission_level_names = {0: "no permission", 1: "read", 2: "write"}
                required_level_name = permission_level_names.get(required_level, str(required_level))
                actual_level_name = permission_level_names.get(actual_level, str(actual_level))
                return {
                    "success": False,
                    "owner_user_id": owner_user_id,
                    "message": f"Insufficient permission: {permission_name} requires {required_level_name} but only has {actual_level_name}"
                }
        
        return {
            "success": True,
            "owner_user_id": owner_user_id,
            "message": "Success"
        }
        
    except Exception as e:
        logging.error("Permission validation failed", stack_info=True)
        return {
            "success": False,
            "owner_user_id": owner_user_id,
            "message": f"Permission validation error: {str(e)}"
        }


async def get_user_ids_by_msg_id(msg_id: str) -> Dict[str, Any]:
    """
    Query user_id and query_user_id by message_id
    
    Args:
        msg_id: Message ID to query
        
    Returns:
        Dict containing:
        - success: bool - Whether query was successful
        - user_id: str - User ID who sent the message
        - query_user_id: str - Query user ID (target user)
        - message: str - Error message if query failed
    """
    try:
        logging.info(f"Querying user IDs for msg_id: {msg_id}")
        
        # Query message table to get user_id and query_user_id
        query = """
            SELECT 
                user_id, 
                query_user_id
            FROM theta_ai.th_messages 
            WHERE id = :msg_id 
            AND is_del = false
        """
        
        params = {"msg_id": msg_id}
        result = await execute_query(
            query=query,
            params=params,
        )
        
        if not result or len(result) == 0:
            logging.warning(f"No message found or deleted for msg_id: {msg_id}")
            return {
                "success": False,
                "user_id": None,
                "query_user_id": None,
                "message": f"Message not found or deleted for msg_id: {msg_id}"
            }
        
        if isinstance(result, list) and len(result) > 0:
            message_data = result[0]
        elif isinstance(result, dict):
            message_data = result
        
        user_id = str(message_data["user_id"]) if message_data["user_id"] else None
        query_user_id = str(message_data["query_user_id"]) if message_data["query_user_id"] else None
        
        # If query_user_id is None, use user_id as fallback
        if not query_user_id:
            query_user_id = user_id
        
        logging.info(f"Successfully retrieved user IDs - user_id: {user_id}, query_user_id: {query_user_id}")
        return {
            "success": True,
            "user_id": user_id,
            "query_user_id": query_user_id,
            "message": "Success"
        }
        
    except Exception as e:
        error_msg = f"Error querying user IDs for msg_id {msg_id}: {str(e)}"
        logging.warning(error_msg, stack_info=True)
        return {
            "success": False,
            "user_id": None,
            "query_user_id": None,
            "message": error_msg
        }
