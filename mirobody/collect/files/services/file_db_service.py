"""
File Database Service for th_files table operations

Provides CRUD operations for the th_files table.
This is a self-contained service: it owns its own DB access and pulls in
nothing from outside the project.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query
from mirobody.utils.req_ctx import request_timezone

from mirobody.utils.coerce import safe_json_dumps, safe_json_loads
from mirobody.utils.db import extract_first_record
from mirobody.utils.file_types import guess_mime, simple_file_type

logger = logging.getLogger(__name__)

#: Where a file was uploaded, named as the web client names its tabs: the Data
#: page, or a message on the Ask page. `th_files.created_source` holds one of
#: these; they were `web_drive`/`web_chat`, and a default "file_upload" that
#: nothing wrote was what made delete-all-files-of-a-message match nothing.
SOURCE_DATA = "data"
SOURCE_ASK = "ask"

#: Upload sessions whose th_files rows are not inserted yet. The WebSocket path
#: inserts every row after the whole batch, while the extraction and genotype
#: tasks it starts per file write to their row as soon as they finish; one that
#: finished first found no row, and its report date, counts and status were lost.
_ROWS_PENDING: dict[str, asyncio.Event] = {}
ROW_WAIT_SECONDS = 600


class FileDbService:
    """
    Database service for th_files table operations.
    
    Handles all CRUD operations for file records stored in th_files table.
    """
    
    @staticmethod
    def expect_rows(message_id: str | None) -> None:
        """This upload's rows will be inserted later; writers must wait."""
        if message_id:
            _ROWS_PENDING.setdefault(message_id, asyncio.Event())

    @staticmethod
    def rows_written(message_id: str | None) -> None:
        event = _ROWS_PENDING.pop(message_id, None) if message_id else None
        if event:
            event.set()

    @staticmethod
    async def rows_ready(message_id: str | None) -> None:
        """Return once this upload's rows exist, or at once when none are pending."""
        event = _ROWS_PENDING.get(message_id) if message_id else None
        if event is None:
            return
        try:
            await asyncio.wait_for(event.wait(), ROW_WAIT_SECONDS)
        except TimeoutError:
            logger.warning("th_files rows still pending after %ds: message_id=%s", ROW_WAIT_SECONDS, message_id)  # phi: ok a constant

    # ============== INSERT Operations ==============
    
    @staticmethod
    async def insert_file(
        user_id: str,
        file_key: str,
        file_name: str = "",
        file_type: str = "",
        file_content: dict[str, Any] | None = None,
        scene: str = "web",
        *,
        created_source: str,
        created_source_id: str | None = None,
        query_user_id: str | None = None,
        original_text: str = "",
        text_length: int = 0,
        content_hash: str = "",
    ) -> int | None:
        """
        Insert a new file record into th_files table.
        
        Args:
            user_id: User ID who owns the file
            file_key: Unique file key (S3/OSS key)
            file_name: Original filename
            file_type: MIME type or file type
            file_content: JSON content with file metadata
            scene: Usage scene (food/report/medicine/journal/web/others)
            created_source: Source that created this file
            created_source_id: Source record ID (e.g., message_id, journal_id)
            query_user_id: Query user ID (defaults to user_id)
            original_text: Original text content extracted from file (for rerank)
            text_length: Length of original text (for rerank strategy)
            content_hash: SHA256 of the raw bytes, the dedup key that lets a
                later upload of the same file reuse this row's original_text
            
        Returns:
            Inserted file ID or None on failure
        """
        try:
            sql = """
                INSERT INTO th_files (
                    user_id, query_user_id, file_name, file_type, file_key,
                    file_content, scene, created_source, created_source_id,
                    original_text, text_length, content_hash,
                    is_del, created_at, updated_at
                ) VALUES (
                    :user_id, :query_user_id, encrypt_content(:file_name), :file_type, :file_key,
                    encrypt_content(:file_content), :scene, :created_source, :created_source_id,
                    encrypt_content(:original_text), :text_length, :content_hash,
                    false, now(), now()
                )
                ON CONFLICT (file_key) DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    file_type = EXCLUDED.file_type,
                    file_content = EXCLUDED.file_content,
                    scene = EXCLUDED.scene,
                    created_source = EXCLUDED.created_source,
                    created_source_id = EXCLUDED.created_source_id,
                    original_text = EXCLUDED.original_text,
                    text_length = EXCLUDED.text_length,
                    content_hash = EXCLUDED.content_hash,
                    updated_at = now()
                -- Only the uploader's own row: a key someone else's file holds
                -- is not the caller's to rename, re-describe or re-scene.
                WHERE th_files.user_id = EXCLUDED.user_id
                RETURNING id
            """
            
            params = {
                "user_id": str(user_id),
                "query_user_id": str(query_user_id) if query_user_id else str(user_id),
                "file_name": file_name or "",
                "file_type": file_type or "",
                "file_key": file_key,
                "file_content": safe_json_dumps(file_content or {}),
                "scene": scene or "web",
                "created_source": created_source,
                "created_source_id": created_source_id,
                "original_text": original_text or "",
                "text_length": text_length or 0,
                "content_hash": content_hash or "",
            }
            
            result = await execute_query(query=sql, params=params)

            if result:
                file_id = result[0].get("id")
                if file_id:
                    logger.info("file filed: id=%s file_key=%s", file_id, file_key)
                    return file_id

            logger.warning("file not filed, no id returned: file_key=%s", file_key)
            return None
            
        except Exception as e:
            logger.error("filing a file failed: file_key=%s error_type=%s", file_key, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return None
    
    @staticmethod
    async def keys_held_by_others(file_keys: list[str], user_id: str) -> set[str]:
        """The keys among `file_keys` whose file belongs to another account.

        A chat request names its attachments by key, and nothing checked whose
        they were: a key read off someone else's shared conversation was
        downloaded, extracted into the caller's record, and its row rewritten.
        """
        if not file_keys:
            return set()
        rows = await execute_query(
            "SELECT file_key FROM th_files WHERE file_key = ANY(:keys) AND user_id <> :user_id",
            params={"keys": list(file_keys), "user_id": str(user_id)},
        )
        return {str(r["file_key"]) for r in rows or []}

    @staticmethod
    async def insert_files_batch(
        user_id: str,
        files_info: list[dict[str, Any]],
        scene: str = "web",
        scenes_by_key: dict[str, str] | None = None,
        *,
        created_source: str,
        created_source_id: str | None = None,
        query_user_id: str | None = None,
    ) -> list[int]:
        """
        Insert multiple file records in batch.
        
        Args:
            user_id: User ID
            files_info: List of file info dicts with keys:
                - file_key: Required
                - file_name: Optional (display name)
                - original_filename: Optional (original upload filename)
                - file_type: Optional (simple type like 'pdf', 'image')
                - content_type: Optional (MIME type)
                - url_thumb, url_full, file_size, raw, file_abstract, etc.
            scene: Usage scene
            created_source: Source identifier
            created_source_id: Source record ID (message_id for tracking)
            query_user_id: Query user ID
            
        Returns:
            List of inserted file IDs
        """
        inserted_ids = []
        
        for file_info in files_info:
            file_key = file_info.get("file_key")
            if not file_key:
                logger.warning("a file with no storage key is not filed: created_source_id=%s", created_source_id)
                continue
            
            # Build file_content with all necessary metadata
            file_content = {
                "url_thumb": file_info.get("url_thumb", ""),
                "url_full": file_info.get("url_full", ""),
                "file_size": file_info.get("file_size", 0),
                "raw": file_info.get("raw", ""),
                "file_abstract": file_info.get("file_abstract", ""),
                "indicators": file_info.get("indicators", []),
                "indicators_count": file_info.get("indicators_count", 0),
                "processed": file_info.get("processed", False),
                "duration": file_info.get("duration"),
                "original_filename": file_info.get("original_filename") or file_info.get("file_name", ""),
                "content_type": file_info.get("content_type", "application/octet-stream"),
                "session_id": file_info.get("session_id", ""),
                "upload_time": file_info.get("upload_time", ""),
                "query": file_info.get("query", ""),
                # Status fields - track upload/processing status
                "status": file_info.get("status", "completed"),  # uploading, processing, completed, failed
                "error": file_info.get("error", ""),  # Error message if failed
                "progress": file_info.get("progress", 100),  # Progress percentage (0-100)
            }
            
            file_id = await FileDbService.insert_file(
                user_id=user_id,
                file_key=file_key,
                file_name=file_info.get("file_name", "") or file_info.get("filename", ""),
                file_type=file_info.get("file_type") or file_info.get("content_type", ""),
                file_content=file_content,
                scene=(scenes_by_key or {}).get(str(file_key), scene),
                created_source=created_source,
                created_source_id=created_source_id,
                query_user_id=query_user_id,
                original_text=file_info.get("original_text", ""),
                text_length=file_info.get("text_length", 0),
                content_hash=file_info.get("content_hash", ""),
            )
            
            if file_id:
                inserted_ids.append(file_id)
        
        logger.info("files filed: inserted=%d total=%d", len(inserted_ids), len(files_info))
        return inserted_ids
    
    # ============== SELECT Operations ==============
    
    @staticmethod
    async def get_file_by_key(
        file_key: str,
        user_id: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Get file record by file_key.
        
        Args:
            file_key: File key to search
            user_id: Optional user ID filter
            
        Returns:
            File record dict or None
        """
        try:
            sql = """
                SELECT id, user_id, query_user_id,
                       decrypt_content(file_name) as file_name, file_type, file_key,
                       decrypt_content(file_content) as file_content,
                       scene, created_source, created_source_id,
                       is_del, created_at, updated_at
                FROM th_files
                WHERE file_key = :file_key AND is_del = false
            """
            params: dict[str, Any] = {"file_key": file_key}
            
            if user_id:
                sql += " AND user_id = :user_id"
                params["user_id"] = str(user_id)
            
            sql += " LIMIT 1"
            
            result = await execute_query(query=sql, params=params)
            record = extract_first_record(result)
            
            if record:
                # Parse file_content from JSON
                record["file_content"] = safe_json_loads(record.get("file_content", "{}"))
            
            return record
            
        except Exception as e:
            logger.error("reading a file row failed: file_key=%s error_type=%s", file_key, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return None
    
    @staticmethod
    async def get_files_paginated(
        user_id: str,
        query_user_id: str | None = None,
        scene: str | list[str] | None = None,
        created_source: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        """
        List the files attached to a user's record, with pagination.

        Args:
            user_id: The caller (used as the target when query_user_id is None;
                authorization itself happens at the router via resolve_subject)
            query_user_id: Whose record to list (None = the caller's own)
            scene: Optional scene filter - can be a single string or list of strings
            created_source: Optional created_source filter
            limit: Page size
            offset: Page offset

        Returns:
            Dict with files list and total count
        """
        try:
            from mirobody.utils.config import get_default_timezone
            timezone = request_timezone(get_default_timezone())

            # Build WHERE clause. The listing answers "which files are attached
            # to the TARGET's record" (query_user_id = whose record), matching
            # the endpoint's docstring and what the agent's VFS serves. It used
            # to also require user_id = viewer ("files the VIEWER uploaded for
            # the target") which made a shared member's file tab permanently
            # empty while the agent quoted her documents. Authorization is the
            # router's job (resolve_subject), not this query's.
            where_conditions = [
                "is_del = false",
                "(file_type IS NULL OR file_type NOT LIKE 'audio/%')"  # Exclude audio files
            ]
            params: dict[str, Any] = {
                "limit": limit,
                "offset": offset,
            }

            # If query_user_id is specified, filter by it; otherwise query the
            # caller's own record.
            target_user_id = query_user_id or user_id
            where_conditions.append("query_user_id = :query_user_id")
            params["query_user_id"] = str(target_user_id)
            
            if scene:
                if isinstance(scene, list):
                    # Multiple scenes - use IN clause
                    scene_placeholders = ", ".join([f":scene_{i}" for i in range(len(scene))])
                    where_conditions.append(f"scene IN ({scene_placeholders})")
                    for i, s in enumerate(scene):
                        params[f"scene_{i}"] = s
                else:
                    # Single scene
                    where_conditions.append("scene = :scene")
                    params["scene"] = scene
            
            if created_source:
                if isinstance(created_source, list):
                    # Multiple sources - use IN clause
                    source_placeholders = ", ".join([f":source_{i}" for i in range(len(created_source))])
                    where_conditions.append(f"created_source IN ({source_placeholders})")
                    for i, s in enumerate(created_source):
                        params[f"source_{i}"] = s
                else:
                    # Single source
                    where_conditions.append("created_source = :created_source")
                    params["created_source"] = created_source
            
            where_clause = " AND ".join(where_conditions)
            
            # Query files
            list_sql = f"""
                SELECT id, user_id, query_user_id,
                       decrypt_content(file_name) as file_name, file_type, file_key,
                       decrypt_content(file_content) as file_content,
                       scene, created_source, created_source_id,
                       created_at, updated_at
                FROM th_files
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """
            
            # Count total
            count_sql = f"""
                SELECT COUNT(1) as total
                FROM th_files
                WHERE {where_clause}
            """
            
            # Execute queries
            list_result = await execute_query(query=list_sql, params=params)
            # Build count params (same as list params but without limit/offset)
            count_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
            count_result = await execute_query(query=count_sql, params=count_params)
            
            # Process results
            files = []
            for row in list_result or []:
                file_content = safe_json_loads(row.get("file_content", "{}"))
                created_at = row.get("created_at")
                
                # Convert to user timezone
                if created_at:
                    try:
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=ZoneInfo("UTC"))
                        local_time = created_at.astimezone(ZoneInfo(timezone))
                        create_time = local_time.isoformat()
                    except Exception:
                        create_time = created_at.isoformat() if created_at else None
                else:
                    create_time = None
                
                # Get original filename from file_content or fallback to file_name
                original_filename = file_content.get("original_filename", row.get("file_name", ""))
                
                # Get content_type from file_content or derive from filename
                stored_content_type = file_content.get("content_type", "")
                content_type = stored_content_type if stored_content_type else guess_mime(row.get("file_name", ""))
                
                # Determine upload_status from status field
                status = file_content.get("status", "completed")
                if status == "completed":
                    upload_status = "complete"
                elif status == "failed":
                    upload_status = "failed"
                elif status == "processing":
                    upload_status = "processing"
                else:
                    upload_status = "complete" if file_content.get("processed", True) else "processing"
                
                file_info = {
                    "id": row.get("id"),
                    "user_id": row.get("user_id"),
                    "query_user_id": row.get("query_user_id"),
                    "file_name": row.get("file_name", ""),
                    "original_name": original_filename,
                    "file_type": row.get("file_type", ""),
                    "type": simple_file_type(row.get("file_type", "")),
                    "file_key": row.get("file_key", ""),
                    "file_size": file_content.get("file_size", 0),
                    "url_full": file_content.get("url_full", ""),
                    "url_thumb": file_content.get("url_thumb", ""),
                    "scene": row.get("scene", ""),
                    "created_source": row.get("created_source", ""),
                    "created_source_id": row.get("created_source_id", ""),
                    "create_time": create_time,
                    "upload_time": create_time,
                    "upload_status": upload_status,
                    "contentType": content_type,
                    "is_uploaded_for_others": query_user_id and query_user_id != user_id,
                    "indicators_count": file_content.get("indicators_count", 0),
                    "processed": file_content.get("processed", False),
                    "file_abstract": file_content.get("file_abstract", ""),
                    "session_id": file_content.get("session_id", ""),
                    # Where the readings' date came from (#53): "extracted" |
                    # "upload_time" | "manual"; empty for files extracted before
                    # the label existed. `date_confirmed` is the user saying the
                    # upload time is right, so the page stops asking.
                    "report_date": file_content.get("report_date", ""),
                    "date_source": file_content.get("date_source", ""),
                    "date_confirmed": bool(file_content.get("date_confirmed", False)),
                    # Why a file is `upload_status: "failed"`, when it is. The row
                    # has carried this since the status did; it was withheld from
                    # the API "for backward compatibility", so a client could show
                    # the failure and never its cause: a report photo uploaded
                    # to a zero-key deployment read "processing failed" with the
                    # one-sentence fix sitting in the database (#68). Empty
                    # otherwise. `status`/`progress` stay internal.
                    "error": file_content.get("error", "") if upload_status == "failed" else "",
                }
                files.append(file_info)
            
            # Get total count
            total = 0
            if count_result:
                first_row = extract_first_record(count_result)
                if first_row:
                    total = first_row.get("total", 0)
            
            logger.info("files listed: user_id=%s total=%s returned=%d", target_user_id, total, len(files))
            
            return {
                "files": files,
                "total": total,
                "limit": limit,
                "offset": offset,
            }
            
        except Exception as e:
            # A fixed sentence up: the caller may show it, and a driver's
            # message quotes the statement.
            owner_id = query_user_id or user_id
            logger.error("listing files failed: user_id=%s error_type=%s", owner_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            raise RuntimeError("Failed to get uploaded files") from e
    
    @staticmethod
    async def get_files_by_source(user_id: str, created_source_id: str) -> list[dict[str, Any]]:
        """The caller's live files attached to one message.

        Not filtered by `created_source`: the one caller passed "file_upload",
        which nothing wrote, so "delete every file of this message" matched
        nothing and reported success while the files stayed. The message id
        names the files and `user_id` scopes them to their owner.
        """
        try:
            sql = """
                SELECT id, user_id, query_user_id,
                       decrypt_content(file_name) as file_name, file_type, file_key,
                       decrypt_content(file_content) as file_content,
                       scene, created_source, created_source_id,
                       created_at, updated_at
                FROM th_files
                WHERE user_id = :user_id
                  AND created_source_id = :created_source_id
                  AND is_del = false
            """
            params: dict[str, Any] = {"user_id": str(user_id), "created_source_id": created_source_id}
            
            result = await execute_query(query=sql, params=params)
            
            files = []
            for row in result or []:
                row["file_content"] = safe_json_loads(row.get("file_content", "{}"))
                files.append(row)
            
            return files
            
        except Exception as e:
            logger.error("listing a message's files failed: created_source_id=%s error_type=%s", created_source_id,
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return []
    
    # ============== UPDATE Operations ==============
    
    @staticmethod
    async def update_file_content(
        file_key: str,
        updates: dict[str, Any],
        user_id: str | None = None,
    ) -> bool:
        """
        Update file_content JSON field.
        
        If updates contains 'file_name' key, also updates the standalone file_name column.
        
        Args:
            file_key: File key to update
            updates: Dict of fields to update in file_content
            user_id: Optional user ID for authorization
            
        Returns:
            True if update successful
        """
        try:
            # First get current file_content
            current = await FileDbService.get_file_by_key(file_key, user_id)
            if not current:
                logger.warning("file not found for update: file_key=%s", file_key)
                return False
            
            # Merge updates into current content
            current_content = current.get("file_content", {})
            current_content.update(updates)
            
            # Check if file_name needs to be updated in standalone column
            update_file_name = "file_name" in updates and updates["file_name"]
            
            if update_file_name:
                sql = """
                    UPDATE th_files
                    SET file_content = encrypt_content(:file_content),
                        file_name = encrypt_content(:file_name),
                        updated_at = now()
                    WHERE file_key = :file_key AND is_del = false
                """
            else:
                sql = """
                    UPDATE th_files
                    SET file_content = encrypt_content(:file_content),
                        updated_at = now()
                    WHERE file_key = :file_key AND is_del = false
                """
            
            params: dict[str, Any] = {
                "file_key": file_key,
                "file_content": safe_json_dumps(current_content),
            }
            
            if update_file_name:
                params["file_name"] = updates["file_name"]
            
            if user_id:
                sql = sql.replace("WHERE file_key", "WHERE user_id = :user_id AND file_key")
                params["user_id"] = str(user_id)
            
            await execute_query(query=sql, params=params)
            logger.info("file row updated: file_key=%s field_count=%d", file_key, len(updates))
            return True
            
        except Exception as e:
            logger.error("updating a file row failed: file_key=%s error_type=%s", file_key, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return False
    
    @staticmethod
    async def update_file_processed(
        file_key: str,
        raw: str = "",
        file_abstract: str = "",
        file_name: str | None = None,
        original_text: str = "",
        text_length: int = 0,
        content_hash: str = "",
    ) -> bool:
        """Record a processed file: what it was read as, its abstract and the
        name a model gave it. The readings and their count are not this
        update's: the indicator extraction writes them when it finishes.

        Args:
            file_key: File key
            raw: Raw extracted content
            file_abstract: File abstract/summary
            file_name: Optional generated file name
            original_text: Original text content (for rerank)
            text_length: Length of original text
            content_hash: SHA256 hash of file content

        Returns:
            True if successful
        """
        try:
            # Fetch current file to get decrypted file_content for merging
            current = await FileDbService.get_file_by_key(file_key)
            if not current:
                logger.warning("file not found for update: file_key=%s", file_key)
                return False

            current_content = current.get("file_content", {})
            current_content.update({
                "raw": raw,
                "file_abstract": file_abstract,
                "processed": True,
                "processed_at": datetime.now(UTC).isoformat(),
                "status": "completed",
                "error": "",
                "progress": 100,
            })
            if file_name:
                current_content["generated_file_name"] = file_name

            # Build UPDATE with encrypted file_content (and file_name if provided)
            sql_parts = [
                "UPDATE th_files SET",
                "file_content = encrypt_content(:file_content)",
            ]
            params: dict[str, Any] = {
                "file_key": file_key,
                "file_content": safe_json_dumps(current_content),
            }

            if file_name:
                sql_parts.append(", file_name = encrypt_content(:file_name)")
                params["file_name"] = file_name

            if original_text:
                sql_parts.append(", original_text = encrypt_content(:original_text)")
                params["original_text"] = original_text

            if text_length > 0:
                sql_parts.append(", text_length = :text_length")
                params["text_length"] = text_length

            if content_hash:
                sql_parts.append(", content_hash = :content_hash")
                params["content_hash"] = content_hash

            sql_parts.append(", updated_at = now()")
            sql_parts.append("WHERE file_key = :file_key AND is_del = false")

            sql = " ".join(sql_parts)

            await execute_query(
                query=sql,
                params=params,
            )

            return True

        except Exception as e:
            logger.error("recording a processed file failed: file_key=%s error_type=%s", file_key,
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return False
    
    # ============== DELETE Operations ==============
    
    @staticmethod
    async def soft_delete_file(
        file_key: str,
        user_id: str,
    ) -> bool:
        """
        Soft delete a file by setting is_del = true.
        
        Args:
            file_key: File key to delete
            user_id: User ID for authorization
            
        Returns:
            True if successful
        """
        try:
            sql = """
                UPDATE th_files
                SET is_del = true,
                    updated_at = now()
                WHERE file_key = :file_key 
                  AND user_id = :user_id
                  AND is_del = false
                RETURNING id
            """
            
            result = await execute_query(
                query=sql,
                params={"file_key": file_key, "user_id": str(user_id)},
            )
            
            if result:
                logger.info("file deleted: file_key=%s", file_key)
                return True
            
            logger.warning("file not found for deletion: file_key=%s", file_key)
            return False
            
        except Exception as e:
            logger.error("deleting a file row failed: file_key=%s error_type=%s", file_key, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return False
