"""
File processing service for async file operations
"""

import asyncio
import json
import mimetypes
import logging
import os
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
    Save file upload records to th_files table.
    
    Now writes to th_files instead of th_messages.
    
    Args:
        msg_id: Message ID (used as created_source_id)
        user_id: User ID
        session_id: Session ID (stored in file_content)
        upload_result: Upload result from process_file_uploads
        successful_count: Number of successful uploads
        failed_count: Number of failed uploads
        total_files: Total number of files
        
    Returns:
        Dict with inserted file IDs or None on failure
    """
    from .file_db_service import FileDbService
    
    try:
        # Prepare files_info for th_files table
        files_info = upload_result.get("files_info", [])
        
        # Enrich files_info with additional metadata
        for file_info in files_info:
            file_info["session_id"] = session_id
            file_info["upload_time"] = datetime.now().isoformat()
            file_info["status"] = "uploaded"
        
        # Insert files into th_files table
        inserted_ids = await FileDbService.insert_files_batch(
            user_id=user_id,
            files_info=files_info,
            scene="web",
            created_source="file_upload",
            created_source_id=msg_id,
            query_user_id=user_id,
        )
        
        if inserted_ids:
            logging.info(f"Files saved to th_files: msg_id={msg_id}, inserted={len(inserted_ids)}/{len(files_info)}")
            return {
                "success": True,
                "msg_id": msg_id,
                "inserted_ids": inserted_ids,
                "total_inserted": len(inserted_ids),
            }
        else:
            logging.warning(f"No files inserted for msg_id: {msg_id}")
            return None
            
    except Exception as e:
        logging.error(f"Error saving files to th_files: {str(e)}", stack_info=True)
        return None


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
        
        # Update th_files with all processed results
        if processed_files:
            logging.info(f"Updating th_files with {len(processed_files)} processed files")
            
            from .file_db_service import FileDbService
            
            # Update each file's content in th_files table
            for processed_file in processed_files:
                file_key = None
                # Find file_key from files_data
                for file_data in files_data:
                    if file_data.get("filename") == processed_file.get("filename"):
                        file_key = file_data.get("s3_key") or file_data.get("file_key")
                        break
                
                if file_key and processed_file.get("processed"):
                    await FileDbService.update_file_processed(
                        file_key=file_key,
                        raw=processed_file.get("raw", ""),
                        file_abstract=processed_file.get("file_abstract", ""),
                        indicators=processed_file.get("indicators", []),
                        file_name=processed_file.get("file_name"),
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
    Delete specific files from th_files table.
    
    Now operates on th_files table instead of th_messages.
    
    Args:
        message_id: The source ID (created_source_id in th_files)
        file_keys: List of file keys to delete
        user_id: User ID for authorization
    
    Returns:
        Dict containing deletion results
    """
    from .file_db_service import FileDbService
    
    try:
        logging.info(f"Starting file deletion from th_files: source_id={message_id}, file_keys={file_keys}")
        
        # Track deletion results
        deleted_files = []
        failed_deletions = []
        
        # Process each file key
        # Track query_user_id for cascade delete (used for th_series_data which stores target user's data)
        cascade_delete_user_id = None
        
        for file_key in file_keys:
            # Get file info first
            file_record = await FileDbService.get_file_by_key(file_key, user_id)
            
            if not file_record:
                failed_deletions.append({
                    "file_key": file_key,
                    "filename": "",
                    "type": "other",
                    "status": "failed",
                    "error": "File not found"
                })
                continue
            
            filename = file_record.get("file_name", "")
            file_type = file_record.get("file_type", "other")
            scene = file_record.get("scene", "")  # Get scene for determining file category
            
            # Get query_user_id for cascade delete (th_series_data uses query_user_id as user_id)
            if not cascade_delete_user_id:
                cascade_delete_user_id = file_record.get("query_user_id") or user_id
            
            # Delete from storage
            storage_deleted = await delete_file_from_storage(file_key=file_key)
            
            # Soft delete from database (even if storage deletion fails)
            db_deleted = await FileDbService.soft_delete_file(file_key, user_id)
            
            if db_deleted:
                deleted_files.append({
                    "file_key": file_key,
                    "filename": filename,
                    "type": file_type,
                    "scene": scene,  # Pass scene for cascade delete logic
                    "status": "deleted",
                    "storage_deleted": storage_deleted
                })
                logging.info(f"Successfully deleted file: {file_key}")
            else:
                failed_deletions.append({
                    "file_key": file_key,
                    "filename": filename,
                    "type": file_type,
                    "status": "failed",
                    "error": "Database deletion failed"
                })
        
        # Start background cascade delete task for successfully deleted files
        # Use query_user_id (target user) for th_series_data deletion
        if deleted_files:
            _start_background_cascade_delete(
                message_id=message_id,
                user_id=cascade_delete_user_id or user_id,
                deleted_files=deleted_files
            )
        
        return {
            "success": len(deleted_files) > 0,
            "message_id": message_id,
            "deleted_files": deleted_files,
            "failed_deletions": failed_deletions,
            "remaining_files_count": 0,  # Not applicable for th_files
            "message_deleted": False
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
    [DEPRECATED] Update message in database after file deletion.
    
    This function operates on th_messages table and is kept for backward compatibility.
    New code should use FileDbService for th_files operations.
    
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
                UPDATE th_messages
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
                UPDATE th_messages
                SET content = encrypt_content(:content),
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
        UPDATE th_messages
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
    Delete all files associated with a source ID from th_files table.
    
    Now operates on th_files table instead of th_messages.
    
    Args:
        message_id: The source ID (created_source_id in th_files)
        user_id: User ID for authorization
    
    Returns:
        Dict containing deletion results
    """
    from .file_db_service import FileDbService
    
    try:
        logging.info(f"Starting deletion of all files for source_id={message_id}")
        
        # Get all files for this source_id
        files = await FileDbService.get_files_by_source(
            user_id=user_id,
            created_source="file_upload",
            created_source_id=message_id,
        )
        
        if not files:
            logging.info(f"No files found for source_id={message_id}")
            return {
                "success": True,
                "message_id": message_id,
                "deleted_files": [],
                "failed_deletions": [],
                "note": "No files found"
            }
        
        # Extract file keys
        file_keys = [f.get("file_key") for f in files if f.get("file_key")]
        
        if not file_keys:
            logging.info(f"No valid file_keys found for source_id={message_id}")
            return {
                "success": True,
                "message_id": message_id,
                "deleted_files": [],
                "failed_deletions": [],
                "note": "No valid file keys found"
            }
        
        # Delete all files
        return await delete_files_from_message(
            message_id=message_id,
            file_keys=file_keys,
            user_id=user_id
        )
        
    except Exception as e:
        logging.error(f"Error in delete_all_files_from_message: {str(e)}", stack_info=True)
        return {
            "success": False,
            "error": f"Internal error: {str(e)}",
            "message_id": message_id
        }


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
            scene = file_info.get("scene", "")  # Use scene to determine file category
            
            # Different deletion strategy based on scene
            if scene == "genetic":
                # For genetic files, delete from th_series_data_genetic using file_key
                genetic_delete_success = await _delete_genetic_data_background(user_id, file_key)
                if genetic_delete_success:
                    logging.info(f"Genetic data deletion successful for genetic file: user_id={user_id}, file_key={file_key}, filename={filename}, scene={scene}")
                else:
                    logging.warning(f"Genetic data deletion failed or no data found: user_id={user_id}, file_key={file_key}, filename={filename}, scene={scene}")
            else:
                # For non-genetic files (report, etc.), delete th_series_data
                await _delete_th_series_data_background(user_id, "th_files", message_id, file_key)
        
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
    
    Now uses source_table = 'th_files' for new data.
    Supports source_table_id formats:
    - New format: file_key directly
    - Old format: msg_id_#_file_key_hash
    - Legacy format: msg_id only
    
    Args:
        user_id: User ID
        source_table: Source table name (th_files for new data)
        message_id: Source ID (created_source_id in th_files)
        file_key: File key for precise deletion
    """
    try:
        delete_count = 0
        
        if file_key:
            # Build old format source_table_id for backward compatibility
            from hashlib import md5
            file_key_hash = md5(file_key.encode()).hexdigest()[:10]
            
            # Old format: msg_id_#_file_key_hash
            old_format = f"{message_id}_#_{file_key_hash}"
            
            # Delete matching new format (file_key) and old format
            delete_sql = """
            DELETE FROM th_series_data 
            WHERE user_id = :user_id 
              AND (source_table_id = :file_key 
                   OR source_table_id = :old_format)
            """

            result = await execute_query(
                delete_sql,
                {
                    "user_id": user_id,
                    "file_key": file_key,
                    "old_format": old_format,
                },
            )
            
            delete_count = len(result) if result else 0
            
            logging.info(f"th_series_data deletion successful: user_id={user_id}, file_key={file_key}, deleted_count={delete_count}")
        else:
            # No file_key provided - delete by source_table_id (backward compatibility)
            delete_sql = """
            DELETE FROM th_series_data 
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
            
            logging.info(f"th_series_data deletion successful (legacy format): user_id={user_id}, source_id={message_id}, deleted_count={delete_count}")

    except Exception as e:
        logging.warning(f"th_series_data deletion failed: user_id={user_id}, source_id={message_id}, file_key={file_key}, error={str(e)}", stack_info=True)
        raise


async def _delete_genetic_data_background(user_id: str, file_key: str) -> bool:
    """
    Delete genetic data in background task
    
    Args:
        user_id: User ID
        file_key: File key (used as source_table_id)
        
    Returns:
        bool: True if deletion was successful, False otherwise
    """
    try:
        # Use file_key as source_table_id with source_table = "th_files"
        delete_success = await FileParserDatabaseService.delete_genetic_data_by_source(
            user_id, 
            "th_files", 
            file_key
        )
        
        if delete_success:
            logging.info(f"Genetic data deletion successful: user_id={user_id}, file_key={file_key}")
        else:
            logging.info(f"No genetic data found for deletion: user_id={user_id}, file_key={file_key}")
            
        return delete_success

    except Exception as e:
        logging.warning(f"Genetic data deletion failed: user_id={user_id}, file_key={file_key}, error={str(e)}", stack_info=True)
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
    redis_client: Optional[Any] = None,
    cache_ttl: int = 3600
) -> Dict[str, Any]:
    """
    Universal file upload service that can be reused across projects
    
    Uploads multiple files directly to S3/Aliyun OSS without storing metadata in database.
    This function is project-agnostic and can be used in holywell or other projects.
    
    File Caching Strategy:
        - Files are uploaded to S3/OSS for persistent storage
        - Files are also cached locally in /tmp/holywell_cache/ for faster access
        - Redis stores the local file path (string) with TTL, not binary content
        - This avoids UTF-8 decode errors and provides fast local file access
    
    Args:
        files: List of files to upload (UploadFile objects)
        user_id: User identifier for logging and authentication context
        folder_prefix: Custom folder prefix for uploaded files (optional, defaults to 'uploads')
        redis_client: Optional Redis client for storing local cache paths (default None)
        cache_ttl: Cache expiration time in seconds (default 3600 = 1 hour)
        
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
            
            # Cache file content as base64 in Redis
            if redis_client and file_content:
                try:
                    import base64
                    b64_content = base64.b64encode(file_content).decode('utf-8')
                    await redis_client.setex(
                        f"file_cache:{file_key}",
                        cache_ttl,
                        b64_content
                    )
                    logging.info(f"💾 Cached file to Redis: {file.filename} (TTL: {cache_ttl}s)")
                except Exception as cache_error:
                    logging.warning(f"⚠️ Failed to cache file for {file.filename}: {cache_error}")
            
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
            FROM th_messages 
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
