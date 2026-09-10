"""
File processing service for async file operations
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality — a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from pydantic import BaseModel

from mirobody.pulse.file_parser.file_processor import FileProcessor
from mirobody.pulse.file_parser.memory_upload_file import MemoryUploadFile
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.file_uploader import (
    generate_file_key,
    validate_file_extension,
)
from mirobody.utils.config.storage import get_storage_client
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


class FileUploadData(BaseModel):
    """File upload data model - matches router FileUploadData structure"""
    file_url: str               # File access URL
    file_name: str              # Original filename
    file_key: str               # File storage key (S3/OSS)  
    file_size: int              # File size in bytes
    file_type: str              # File MIME type
    upload_time: datetime       # Upload timestamp


async def process_files_async(
    files_data: list[dict[str, Any]],
    user_id: str,
    msg_id: str,
):
    """
    Asynchronously process all uploaded files to extract indicators

    Args:
        files_data: List of file data dictionaries containing content_bytes, file_name, content_type, file_key
        user_id: User ID
        msg_id: Message ID
    """
    
    async def process_single_file_wrapper(file_data: dict[str, Any], file_processor: FileProcessor) -> dict:
        """
        Wrapper function to process a single file

        Args:
            file_data: Dictionary with file data (content_bytes, file_name, content_type, file_key)
            file_processor: The FileProcessor instance

        Returns:
            dict: Processing result
        """
        try:
            filename = file_data["file_name"]
            logger.info(f"Processing file: {filename}, msg_id: {msg_id}")
            
            mock_file = MemoryUploadFile(
                content=file_data["content_bytes"],
                filename=filename,
                content_type=file_data["content_type"],
            )
            
            # Process single file (skip upload since already uploaded)
            result = await file_processor.process_single_file(
                file=mock_file,
                user_id=user_id,
                message_id=msg_id,
                query="",
                file_key=file_data.get("file_key"),
                skip_upload_oss=True,  # Skip upload since already uploaded
            )
            
            if result.get("success"):
                # Extract indicators from result if available
                indicators = result.get("indicators", [])

                processed_file_info = {
                    "file_name": filename,
                    "processed": True,
                    "raw": result.get("raw", result.get("content", "")),  # Use 'raw' field name
                    "file_abstract": result.get("file_abstract", ""),  # Add file abstract
                    "generated_file_name": result.get("file_name", filename),  # AI generated file name
                    "indicators": indicators,
                    "indicators_count": len(indicators),  # Add indicators count
                    # Add original text fields for th_files columns
                    "original_text": result.get("original_text", ""),
                    "text_length": result.get("text_length", 0),
                    "content_hash": result.get("content_hash", ""),
                }
            else:
                processed_file_info = {
                    "file_name": filename,
                    "processed": False,
                    "generated_file_name": result.get("file_name", filename),  # AI generated file name even on failure
                    "error": result.get("error", "Processing failed"),
                    # Still extract original text fields if available
                    "original_text": result.get("original_text", ""),
                    "text_length": result.get("text_length", 0),
                    "content_hash": result.get("content_hash", ""),
                }
            
            logger.info(f"Completed processing file: {filename}, success: {result.get('success')}")
            
            return processed_file_info
            
        except Exception as file_error:
            logger.error(f"Error processing file {file_data.get('file_name', 'unknown')}: {str(file_error)}", stack_info=True)
            return {
                "file_name": file_data.get("file_name", "unknown"),
                "processed": False,
                "error": str(file_error)
            }
    
    try:
        logger.info(f"Starting concurrent async processing for {len(files_data)} files, msg_id: {msg_id}")
        
        # Create file processor instance
        file_processor = FileProcessor()
        
        # Use asyncio to process files concurrently
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
            logger.info(f"Updating th_files with {len(processed_files)} processed files")
            
            from .file_db_service import FileDbService
            
            # Update each file's content in th_files table
            for processed_file in processed_files:
                file_key = None
                # Find file_key from files_data
                for file_data in files_data:
                    if file_data.get("file_name") == processed_file.get("file_name"):
                        file_key = file_data.get("file_key")
                        break
                
                if file_key:
                    # Update th_files with processing results (including original_text even if processing failed)
                    await FileDbService.update_file_processed(
                        file_key=file_key,
                        raw=processed_file.get("raw", ""),
                        file_abstract=processed_file.get("file_abstract", ""),
                        indicators=processed_file.get("indicators", []),
                        file_name=processed_file.get("generated_file_name"),
                        original_text=processed_file.get("original_text", ""),
                        text_length=processed_file.get("text_length", 0),
                        content_hash=processed_file.get("content_hash", ""),
                    )
            
            logger.info(f"Concurrent async processing completed for all files, msg_id: {msg_id}")
            
    except Exception:
        logger.error(f"Concurrent async processing failed for files batch, msg_id: {msg_id}", stack_info=True)


async def _invalidate_derived_profile(owner_id: str) -> None:
    """Drop the health profile DERIVED from data that no longer exists.

    Revoking the file copy was not enough. The profile generator writes a
    summary into ``health_user_profile_by_system.common_part`` and mirrors the
    detailed version to ``/memories/health_profile.md`` — and that mirror quotes
    the readings verbatim ("GLU 7.5 mmol/L, FBG 7.45, PBG 9.5, HbA1c 7.2% ...").
    It carries no ``file_key``, so it survived both the file delete and the
    reading cascade.

    Measured, after deleting the report through the UI: the file was gone, its
    readings were gone, the agent's copy was revoked — and the next question
    still came back with all twelve values, sourced from that profile.

    Both derived copies are therefore invalidated here rather than repaired.
    They are projections of the record, so once the record changes they are
    wrong by definition; the refresh pass rebuilds them from whatever remains,
    and until it does an ABSENT profile is the only correct state. Keyed by the
    OWNER of the readings, which for a care-circle upload is the member, not
    whoever uploaded the file.
    """
    if not owner_id:
        return
    try:
        await execute_query(
            query="""
            UPDATE health_user_profile_by_system
               SET is_deleted = true, last_update_time = NOW()
             WHERE user_id = :user_id AND is_deleted = false
            """,
            params={"user_id": str(owner_id)},
        )
        # No second statement any more. `/memories/health_profile.md` used to be a
        # COPY in deep_agent_workspace and had to be chased separately; it is now
        # a projection that selects `is_deleted = false`, so the UPDATE above
        # removes the agent's view of the profile as a side effect of
        # invalidating the profile. That is the whole point of the projection.
        logger.info(f"Invalidated derived health profile for user {owner_id}")
    except Exception as e:
        logger.error(f"FAILED to invalidate derived profile for {owner_id}: {e}", exc_info=True)


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
        logger.info(f"Starting file deletion from th_files: source_id={message_id}, file_keys={file_keys}")
        
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
                # Nothing to revoke any more. The agent's view of a file is a
                # projection of THIS row (deep/files_backend.py selects
                # `is_del = false`), so soft-deleting it here is the whole of it.
                deleted_files.append({
                    "file_key": file_key,
                    "filename": filename,
                    "type": file_type,
                    "scene": scene,  # Pass scene for cascade delete logic
                    "status": "deleted",
                    "storage_deleted": storage_deleted,
                })
                logger.info(f"Successfully deleted file: {file_key}")
            else:
                failed_deletions.append({
                    "file_key": file_key,
                    "filename": filename,
                    "type": file_type,
                    "status": "failed",
                    "error": "Database deletion failed"
                })
        
        # The profile is derived from the readings the cascade is about to remove,
        # and it quotes them. Invalidate it inline — a background failure here
        # leaves deleted values in the model's system prompt.
        if deleted_files:
            await _invalidate_derived_profile(cascade_delete_user_id or user_id)

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
        logger.error(f"Error in delete_files_from_message: {str(e)}", stack_info=True)
        return {
            "success": False,
            "error": f"Internal error: {str(e)}",
            "message_id": message_id
        }


async def delete_file_from_storage(file_key: str) -> bool:
    """
    Delete a file from storage using unified storage client

    Args:
        file_key: The storage key of the file

    Returns:
        bool: True if deletion successful, False otherwise
    """
    try:
        # Get storage client at runtime
        storage = get_storage_client()
        
        # Use unified storage client
        err = await storage.delete(file_key)

        if err:
            logger.warning(f"Failed to delete file {file_key}: {err}")
            return False

        return True
        
    except Exception as e:
        logger.error(f"Error deleting file from storage: {str(e)}", stack_info=True)
        return False


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
        logger.info(f"Starting deletion of all files for source_id={message_id}")
        
        # Get all files for this source_id
        files = await FileDbService.get_files_by_source(
            user_id=user_id,
            created_source="file_upload",
            created_source_id=message_id,
        )
        
        if not files:
            logger.info(f"No files found for source_id={message_id}")
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
            logger.info(f"No valid file_keys found for source_id={message_id}")
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
        logger.error(f"Error in delete_all_files_from_message: {str(e)}", stack_info=True)
        return {
            "success": False,
            "error": f"Internal error: {str(e)}",
            "message_id": message_id
        }


async def _background_cascade_delete_by_file_info(
    message_id: str,
    user_id: str,
    deleted_files: list[dict[str, Any]]
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
        logger.info(f"Starting background cascade delete task: message_id={message_id}, user_id={user_id}, files_count={len(deleted_files)}")
        
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
                    logger.info(f"Genetic data deletion successful for genetic file: user_id={user_id}, file_key={file_key}, filename={filename}, scene={scene}")
                else:
                    logger.warning(f"Genetic data deletion failed or no data found: user_id={user_id}, file_key={file_key}, filename={filename}, scene={scene}")
            else:
                # For non-genetic files (report, etc.), delete th_series_data
                await _delete_th_series_data_background(user_id, "th_files", message_id, file_key)
        
        logger.info(f"Background cascade delete task completed successfully: message_id={message_id}, user_id={user_id}")
        
    except Exception as e:
        logger.error(f"Background cascade delete task failed: message_id={message_id}, user_id={user_id}, error={str(e)}", stack_info=True)


async def _delete_th_series_data_background(
    user_id: str, 
    source_table: str, 
    message_id: str, 
    file_key: str | None = None
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
            
            logger.info(f"th_series_data deletion successful: user_id={user_id}, file_key={file_key}, deleted_count={delete_count}")
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
            
            logger.info(f"th_series_data deletion successful (legacy format): user_id={user_id}, source_id={message_id}, deleted_count={delete_count}")

    except Exception as e:
        logger.warning(f"th_series_data deletion failed: user_id={user_id}, source_id={message_id}, file_key={file_key}, error={str(e)}", stack_info=True)
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
            logger.info(f"Genetic data deletion successful: user_id={user_id}, file_key={file_key}")
        else:
            logger.info(f"No genetic data found for deletion: user_id={user_id}, file_key={file_key}")
            
        return delete_success

    except Exception as e:
        logger.warning(f"Genetic data deletion failed: user_id={user_id}, file_key={file_key}, error={str(e)}", stack_info=True)
        return False


def _start_background_cascade_delete(
    message_id: str,
    user_id: str,
    deleted_files: list[dict[str, Any]]
) -> None:
    """
    Start background cascade delete task
    
    Args:
        message_id: Message ID
        user_id: User ID
        deleted_files: List of deleted file information
    """
    # spawn() keeps a strong reference until completion — a bare
    # asyncio.create_task here left the task GC-collectable mid-delete
    # (the exact failure mode utils/tasks.py documents).
    logger.info(f"Creating background cascade delete task - message_id: {message_id}, user_id: {user_id}, files_count: {len(deleted_files)}")
    from mirobody.utils.tasks import spawn
    spawn(
        _background_cascade_delete_by_file_info(message_id, user_id, deleted_files),
        name=f"cascade-delete-{message_id}",
    )

async def upload_files_to_storage(
    files: list[UploadFile], 
    user_id: str,
    folder_prefix: str | None = None,
    redis_client: Any | None = None,
    cache_ttl: int = 3600
) -> dict[str, Any]:
    """
    Universal file upload service that can be reused across projects
    
    Uploads multiple files directly to S3/Aliyun OSS without storing metadata in database.
    This function is project-agnostic: it takes only its arguments and touches
    no module-level state, so it can be lifted into another codebase as-is.
    
    File Caching Strategy:
        - Files are uploaded to S3/OSS for persistent storage
        - Extracted text is cached in Redis under `file_cache:{file_key}`;
          there is no local disk cache (an earlier docstring claimed one).
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
    successful_uploads: list[FileUploadData] = []
    failed_uploads = []
    
    logger.info(f"Starting batch upload of {len(files)} files for user {user_id} using {storage.get_storage_type()} storage")
    
    # Process each file
    for file_index, file in enumerate(files):
        try:
            logger.info(f"Processing file {file_index + 1}/{len(files)}: {file.filename}")
            
            
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

            # `validate_file_extension` has been imported by this module since
            # it was written and was never once called, so SUPPORTED_EXTENSIONS
            # documented a restriction that did not exist: any extension landed
            # in the store. Combined with the file route's old `inline`
            # disposition that made an uploaded .html a stored-XSS payload on
            # this origin.
            ext_ok, ext_err = validate_file_extension(file)
            if not ext_ok:
                failed_uploads.append({
                    "file_name": file.filename,
                    "error": ext_err
                })
                continue
            
            # Determine content type
            content_type = file.content_type or "application/octet-stream"
            
            # Generate unique file key for storage. `folder_prefix` comes off
            # the request's `?folder=` parameter, so an invalid one is a client
            # error for THIS file, not a 500 for the whole batch.
            try:
                if folder_prefix is not None:
                    file_key = generate_file_key(file.filename, folder_prefix=folder_prefix)
                else:
                    file_key = generate_file_key(file.filename)
            except ValueError as e:
                failed_uploads.append({
                    "file_name": file.filename,
                    "error": str(e)
                })
                continue
            
            # Record upload start time
            upload_time = datetime.now()
            
            logger.info(f"Uploading file: {file.filename} ({file_size} bytes) -> {file_key}")
            
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
            
            logger.info(f"File uploaded successfully: {file.filename} -> {file_url}")
            
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
                    logger.info(f"Cached file to Redis: {file.filename} (TTL: {cache_ttl}s)")
                except Exception as cache_error:
                    logger.warning(f"Failed to cache file for {file.filename}: {cache_error}")
            
            # Create upload result data using FileUploadData structure
            upload_data = FileUploadData(
                file_url=file_url,
                file_name=file.filename,
                file_key=file_key,
                file_size=file_size,
                file_type=content_type,
                upload_time=upload_time,
            )
            successful_uploads.append(upload_data.model_dump())
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"File upload failed for {file.filename}: {error_msg}", stack_info=True)
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
    
    logger.info(f"Batch upload completed - Total: {total_files}, Success: {successful_count}, Failed: {failed_count}")
    

    return {
        "code": code,
        "msg": msg,
        "data": data
    }


