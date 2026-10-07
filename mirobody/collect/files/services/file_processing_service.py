"""
File processing service for async file operations
"""

from __future__ import annotations

from mirobody.collect.files.services.genetic_store import delete_genetic_data_by_source
import asyncio
import logging
from datetime import datetime
from typing import Any

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import UploadFile
from pydantic import BaseModel

from mirobody.collect.files.errors import failure_reason
from mirobody.collect.files.file_processor import FileProcessor
from mirobody.collect.files.memory_upload_file import MemoryUploadFile
from mirobody.collect.files.services.report_date import file_source_ref
from mirobody.collect.files.services.file_uploader import (
    generate_file_key,
    validate_file_extension,
)
from mirobody.utils.config.storage import get_storage_client
from mirobody.utils import execute_query
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


class FileUploadData(BaseModel):
    """One stored upload, as `POST /files/upload` answers it."""
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
) -> None:
    """Process a chat turn's attachments, each already stored and filed under
    its `file_key` (`agent/chat/file.process_files_from_storage`), and write
    each one's outcome to its own row.

    Results are matched to rows by position, never by name: two attachments
    named `image.png` had both results written to the first one's row. A
    file that failed is `status: failed` with its reason; it was written
    `completed` with no indicators. The rows are announced written
    (`FileDbService.rows_written`) only after these updates, so an indicator
    extraction that finishes first does not have its count and status
    overwritten by them.
    """
    from .file_db_service import FileDbService

    file_processor = FileProcessor()

    async def process(file_data: dict[str, Any]) -> dict[str, Any]:
        upload = MemoryUploadFile(
            content=file_data["content_bytes"],
            filename=file_data["file_name"],
            content_type=file_data["content_type"],
        )
        try:
            return await file_processor.process_single_file(
                file=upload,
                user_id=user_id,
                message_id=msg_id,
                query="",
                file_key=file_data.get("file_key"),
                skip_upload_oss=True,
            )
        except Exception as e:
            logger.error("attachment processing failed: msg_id=%s error_type=%s", msg_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return {"success": False, "error": failure_reason(e)}

    FileDbService.expect_rows(msg_id)
    try:
        results = await asyncio.gather(*(process(f) for f in files_data))
        for file_data, result in zip(files_data, results, strict=True):
            file_key = file_data.get("file_key")
            if not file_key:
                continue
            if result.get("success"):
                await FileDbService.update_file_processed(
                    file_key=file_key,
                    raw=result.get("raw", ""),
                    file_abstract=result.get("file_abstract", ""),
                    file_name=result.get("file_name") or file_data["file_name"],
                    original_text=result.get("original_text", ""),
                    text_length=result.get("text_length", 0),
                    content_hash=result.get("content_hash", ""),
                )
            else:
                await FileDbService.update_file_content(file_key, {
                    "status": "failed",
                    "processed": False,
                    "progress": 0,
                    "error": result.get("error") or result.get("message") or "Processing failed",
                })
        logger.info("attachments processed: msg_id=%s count=%d failed=%d", msg_id, len(results),
                    sum(1 for r in results if not r.get("success")))
    finally:
        FileDbService.rows_written(msg_id)


async def _invalidate_derived_profile(owner_id: str) -> None:
    """Drop the health profile DERIVED from data that no longer exists.

    Revoking the file copy was not enough. The profile generator writes a
    summary into ``health_user_profile_by_system.common_part`` and mirrors the
    detailed version to ``/memories/health_profile.md``, and that mirror quotes
    the readings verbatim ("GLU 7.5 mmol/L, FBG 7.45, PBG 9.5, HbA1c 7.2% ...").
    It carries no ``file_key``, so it survived both the file delete and the
    reading cascade.

    Measured, after deleting the report through the UI: the file was gone, its
    readings were gone, the agent's copy was revoked, and the next question
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
        logger.info("derived health profile invalidated: user_id=%s", owner_id)
    except Exception as e:
        logger.error("invalidating a derived health profile failed: user_id=%s error_type=%s", owner_id,
                     type(e).__name__, exc_info=not is_driver_exception(e))


async def delete_files_from_message(
    message_id: str,
    file_keys: list[str],
    user_id: str
) -> dict[str, Any]:
    """Delete the caller's files `file_keys`: the `th_files` row (soft) and
    the object in storage, then, in the background, what was extracted from
    each, under the owner of the record it was filed in.

    Args:
        message_id: The source ID (created_source_id in th_files)
        file_keys: List of file keys to delete
        user_id: User ID for authorization

    Returns:
        Dict containing deletion results
    """
    from .file_db_service import FileDbService

    try:
        deleted_files = []
        failed_deletions = []
        #: Each deleted file's record owner: a care-circle upload's readings
        #: are the member's, whoever uploaded the file.
        owners: dict[str, str] = {}
        for file_key in file_keys:
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
            storage_deleted = await delete_file_from_storage(file_key=file_key)
            # Soft delete from database (even if storage deletion fails)
            if await FileDbService.soft_delete_file(file_key, user_id):
                # Nothing to revoke any more. The agent's view of a file is a
                # projection of THIS row (deep/files_backend.py selects
                # `is_del = false`), so soft-deleting it here is the whole of it.
                deleted_files.append({
                    "file_key": file_key,
                    "filename": filename,
                    "type": file_type,
                    "scene": file_record.get("scene", ""),
                    "status": "deleted",
                    "storage_deleted": storage_deleted,
                })
                owners[file_key] = str(file_record.get("query_user_id") or user_id)
            else:
                failed_deletions.append({
                    "file_key": file_key,
                    "filename": filename,
                    "type": file_type,
                    "status": "failed",
                    "error": "Database deletion failed"
                })

        # The profile is derived from the readings the cascade is about to remove,
        # and it quotes them. Invalidate it inline: a background failure here
        # leaves deleted values in the model's system prompt.
        for owner in sorted(set(owners.values())):
            await _invalidate_derived_profile(owner)
        if deleted_files:
            _start_background_cascade_delete(message_id, [(f, owners[f["file_key"]]) for f in deleted_files])

        logger.info("files deleted: message_id=%s deleted=%d failed=%d", message_id, len(deleted_files),
                    len(failed_deletions))
        return {
            "success": len(deleted_files) > 0,
            "message_id": message_id,
            "deleted_files": deleted_files,
            "failed_deletions": failed_deletions,
            "remaining_files_count": 0,  # Not applicable for th_files
            "message_deleted": False
        }

    except Exception as e:
        logger.error("deleting files failed: message_id=%s error_type=%s", message_id, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return {
            "success": False,
            "error": "Internal error",
            "message_id": message_id
        }


async def delete_file_from_storage(file_key: str) -> bool:
    """Delete a file's object from storage; whether it went."""
    try:
        err = await get_storage_client().delete(file_key)
    except Exception as e:
        logger.error("deleting a stored file failed: file_key=%s error_type=%s", file_key, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return False
    if err:
        logger.warning("deleting a stored file failed: file_key=%s", file_key)
        return False
    return True


async def delete_all_files_from_message(
    message_id: str,
    user_id: str
) -> dict[str, Any]:
    """`delete_files_from_message` for every file of `message_id` the caller
    owns.

    Args:
        message_id: The source ID (created_source_id in th_files)
        user_id: User ID for authorization

    Returns:
        Dict containing deletion results
    """
    from .file_db_service import FileDbService

    try:
        files = await FileDbService.get_files_by_source(user_id=user_id, created_source_id=message_id)
        file_keys = [f.get("file_key") for f in files if f.get("file_key")]
        if not file_keys:
            return {
                "success": True,
                "message_id": message_id,
                "deleted_files": [],
                "failed_deletions": [],
                "note": "No files found"
            }
        return await delete_files_from_message(
            message_id=message_id,
            file_keys=file_keys,
            user_id=user_id
        )

    except Exception as e:
        logger.error("deleting a message's files failed: message_id=%s error_type=%s", message_id,
                     type(e).__name__, exc_info=not is_driver_exception(e))
        return {
            "success": False,
            "error": "Internal error",
            "message_id": message_id
        }


async def _background_cascade_delete(message_id: str, deleted: list[tuple[dict[str, Any], str]]) -> None:
    """Erase what was extracted from each deleted file, under the owner of
    the record it was filed in: a genotype file's sets, any other file's
    observations (their coding and frozen extraction go with them). This is
    the privacy path: one file whose erase fails does not stop the others,
    and the failure is logged as one."""
    from mirobody.collect import observations

    failed = 0
    for file_info, owner in deleted:
        file_key = file_info["file_key"]
        try:
            if file_info.get("scene") == "genetic":
                # `delete_genetic_data_by_source` logs its own failure.
                if not await delete_genetic_data_by_source(owner, "th_files", file_key):
                    failed += 1
            else:
                await observations.erase(owner, source_ref=file_source_ref(file_key))
        except Exception as e:
            failed += 1
            logger.error("erasing a deleted file's data failed: file_key=%s error_type=%s", file_key,
                         type(e).__name__, exc_info=not is_driver_exception(e))
    logger.info("cascade delete finished: message_id=%s file_count=%d failed=%d", message_id, len(deleted), failed)


def _start_background_cascade_delete(message_id: str, deleted: list[tuple[dict[str, Any], str]]) -> None:
    """Start `_background_cascade_delete` under `spawn`: a bare
    `asyncio.create_task` left the task GC-collectable mid-delete (the exact
    failure mode utils/tasks.py documents)."""
    from mirobody.utils.tasks import spawn

    spawn(_background_cascade_delete(message_id, deleted), name=f"cascade-delete-{message_id}")


async def upload_files_to_storage(
    files: list[UploadFile], 
    user_id: str,
    folder_prefix: str | None = None,
) -> dict[str, Any]:
    """
    Store uploads in object storage, without filing them in the database.
    It takes only its arguments and touches no module-level state, and
    nothing else holds the bytes.

    Args:
        files: List of files to upload (UploadFile objects)
        user_id: User identifier for logging and authentication context
        folder_prefix: Custom folder prefix for uploaded files (optional, defaults to 'uploads')
        
    Returns:
        Dict containing (same format as original FileUploadResponse):
        - code: 0 for complete success, 1 for partial/complete failure
        - msg: Result message
        - data: List of successful upload results (or None if all failed)
        
    Each successful upload result contains (`FileUploadData`):
        - file_url: File access URL
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
    successful_uploads: list[dict[str, Any]] = []
    failed_uploads = []
    
    logger.info("upload batch: user_id=%s file_count=%d storage=%s", user_id, len(files), storage.get_storage_type())
    
    # Process each file
    for file_index, file in enumerate(files):
        try:
            logger.info("processing upload: index=%d total=%d", file_index + 1, len(files))
            
            
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

            # Without it any extension landed in the store, and with the file
            # route's old `inline` disposition an uploaded .html was a
            # stored-XSS payload on this origin.
            ext_ok, ext_err = validate_file_extension(file.filename)
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
            
            upload_time = datetime.now()
            
            logger.info("uploading file: bytes=%d", file_size)
            
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
                    "error": "Failed to upload file to storage backend"
                })
                continue           
            
            logger.info("file uploaded: bytes=%d", file_size)
            
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
            logger.error("file upload failed: error_type=%s", type(e).__name__,
                         exc_info=not is_driver_exception(e))
            failed_uploads.append({
                "file_name": file.filename,
                "error": "Upload failed"
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
    
    logger.info("upload batch done: total=%d stored=%d failed=%d", total_files, successful_count, failed_count)
    

    return {
        "code": code,
        "msg": msg,
        "data": data
    }
