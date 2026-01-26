"""
File Database Service for th_files table operations

Provides CRUD operations for the th_files table.
This is an independent service for mirobody, not dependent on holywell.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union
from zoneinfo import ZoneInfo

from mirobody.utils import execute_query
from mirobody.utils.req_ctx import get_req_ctx

from .db_utils import (
    extract_first_record,
    get_mime_type,
    get_simple_file_type,
    safe_json_dumps,
    safe_json_loads,
)


class FileDbService:
    """
    Database service for th_files table operations.
    
    Handles all CRUD operations for file records stored in th_files table.
    """
    
    TABLE_NAME = "th_files"
    
    # ============== INSERT Operations ==============
    
    @staticmethod
    async def insert_file(
        user_id: str,
        file_key: str,
        file_name: str = "",
        file_type: str = "",
        file_content: Optional[Dict[str, Any]] = None,
        scene: str = "web",
        created_source: str = "file_upload",
        created_source_id: Optional[str] = None,
        query_user_id: Optional[str] = None,
    ) -> Optional[int]:
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
            
        Returns:
            Inserted file ID or None on failure
        """
        try:
            sql = """
                INSERT INTO th_files (
                    user_id, query_user_id, file_name, file_type, file_key,
                    file_content, scene, created_source, created_source_id,
                    is_del, created_at, updated_at
                ) VALUES (
                    :user_id, :query_user_id, :file_name, :file_type, :file_key,
                    CAST(:file_content AS jsonb), :scene, :created_source, :created_source_id,
                    false, now(), now()
                )
                ON CONFLICT (file_key) DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    file_type = EXCLUDED.file_type,
                    file_content = EXCLUDED.file_content,
                    scene = EXCLUDED.scene,
                    created_source = EXCLUDED.created_source,
                    created_source_id = EXCLUDED.created_source_id,
                    updated_at = now()
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
                "created_source": created_source or "file_upload",
                "created_source_id": created_source_id,
            }
            
            result = await execute_query(query=sql, params=params)
            
            if result:
                # execute_query returns a dict directly for INSERT RETURNING
                if isinstance(result, dict):
                    file_id = result.get("id")
                    if file_id:
                        logging.info(f"File inserted: id={file_id}, file_key={file_key}")
                        return file_id
                # Handle list result (for compatibility)
                elif isinstance(result, list) and len(result) > 0:
                    file_id = result[0].get("id") if isinstance(result[0], dict) else None
                    if file_id:
                        logging.info(f"File inserted: id={file_id}, file_key={file_key}")
                        return file_id
            
            logging.warning(f"No id returned for file: file_key={file_key}")
            return None
            
        except Exception as e:
            logging.error(f"Failed to insert file: {str(e)}", stack_info=True)
            return None
    
    @staticmethod
    async def insert_files_batch(
        user_id: str,
        files_info: List[Dict[str, Any]],
        scene: str = "web",
        created_source: str = "file_upload",
        created_source_id: Optional[str] = None,
        query_user_id: Optional[str] = None,
    ) -> List[int]:
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
            file_key = file_info.get("file_key") or file_info.get("s3_key")
            if not file_key:
                logging.warning(f"Skipping file without file_key: {file_info}")
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
                "original_filename": file_info.get("original_filename", file_info.get("filename", "")),
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
                file_name=file_info.get("file_name") or file_info.get("filename", ""),
                file_type=file_info.get("file_type") or file_info.get("content_type", ""),
                file_content=file_content,
                scene=scene,
                created_source=created_source,
                created_source_id=created_source_id,
                query_user_id=query_user_id,
            )
            
            if file_id:
                inserted_ids.append(file_id)
        
        logging.info(f"Batch insert completed: {len(inserted_ids)}/{len(files_info)} files")
        return inserted_ids
    
    # ============== SELECT Operations ==============
    
    @staticmethod
    async def get_file_by_key(
        file_key: str,
        user_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
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
                SELECT id, user_id, query_user_id, file_name, file_type, file_key,
                       file_content, scene, created_source, created_source_id,
                       is_del, created_at, updated_at
                FROM th_files
                WHERE file_key = :file_key AND is_del = false
            """
            params: Dict[str, Any] = {"file_key": file_key}
            
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
            logging.error(f"Failed to get file by key: {str(e)}", stack_info=True)
            return None
    
    @staticmethod
    async def get_file_by_id(file_id: int) -> Optional[Dict[str, Any]]:
        """
        Get file record by ID.
        
        Args:
            file_id: File ID
            
        Returns:
            File record dict or None
        """
        try:
            sql = """
                SELECT id, user_id, query_user_id, file_name, file_type, file_key,
                       file_content, scene, created_source, created_source_id,
                       is_del, created_at, updated_at
                FROM th_files
                WHERE id = :file_id AND is_del = false
                LIMIT 1
            """
            
            result = await execute_query(query=sql, params={"file_id": file_id})
            record = extract_first_record(result)
            
            if record:
                record["file_content"] = safe_json_loads(record.get("file_content", "{}"))
            
            return record
            
        except Exception as e:
            logging.error(f"Failed to get file by id: {str(e)}", stack_info=True)
            return None
    
    @staticmethod
    async def get_files_paginated(
        user_id: str,
        query_user_id: Optional[str] = None,
        scene: Optional[Union[str, List[str]]] = None,
        created_source: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get user's files with pagination.
        
        Args:
            user_id: Current user ID (for permission checking)
            query_user_id: Target user ID to query (None = query user_id)
            scene: Optional scene filter - can be a single string or list of strings
            created_source: Optional created_source filter
            limit: Page size
            offset: Page offset
            
        Returns:
            Dict with files list and total count
        """
        try:
            timezone = get_req_ctx("timezone", "America/Los_Angeles")
            
            # Build WHERE clause
            # user_id = current user (who uploaded)
            # query_user_id = target user (for whom the file was uploaded)
            where_conditions = [
                "user_id = :user_id",
                "is_del = false",
                "(file_type IS NULL OR file_type NOT LIKE 'audio/%')"  # Exclude audio files
            ]
            params: Dict[str, Any] = {
                "user_id": str(user_id),
                "limit": limit,
                "offset": offset,
            }
            
            # If query_user_id is specified, filter by it; otherwise query files uploaded for self
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
                SELECT id, user_id, query_user_id, file_name, file_type, file_key,
                       file_content, scene, created_source, created_source_id,
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
                content_type = stored_content_type if stored_content_type else get_mime_type(row.get("file_name", ""))
                
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
                    "type": get_simple_file_type(row.get("file_type", "")),
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
                    # NOTE: status/error/progress fields are stored in file_content but not exposed in API
                    # to maintain backward compatibility with frontend
                }
                files.append(file_info)
            
            # Get total count
            total = 0
            if count_result:
                first_row = extract_first_record(count_result)
                if first_row:
                    total = first_row.get("total", 0)
            
            logging.info(f"Get files paginated: user_id={target_user_id}, total={total}, returned={len(files)}")
            
            return {
                "files": files,
                "total": total,
                "limit": limit,
                "offset": offset,
            }
            
        except Exception as e:
            logging.error(f"Failed to get files paginated: {str(e)}", stack_info=True)
            raise Exception(f"Failed to get uploaded files: {str(e)}")
    
    @staticmethod
    async def get_files_by_source(
        user_id: str,
        created_source: str,
        created_source_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get files by created source.
        
        Args:
            user_id: User ID
            created_source: Source identifier
            created_source_id: Optional source record ID
            
        Returns:
            List of file records
        """
        try:
            sql = """
                SELECT id, user_id, query_user_id, file_name, file_type, file_key,
                       file_content, scene, created_source, created_source_id,
                       created_at, updated_at
                FROM th_files
                WHERE user_id = :user_id 
                  AND created_source = :created_source
                  AND is_del = false
            """
            params: Dict[str, Any] = {
                "user_id": str(user_id),
                "created_source": created_source,
            }
            
            if created_source_id:
                sql += " AND created_source_id = :created_source_id"
                params["created_source_id"] = created_source_id
            
            result = await execute_query(query=sql, params=params)
            
            files = []
            for row in result or []:
                row["file_content"] = safe_json_loads(row.get("file_content", "{}"))
                files.append(row)
            
            return files
            
        except Exception as e:
            logging.error(f"Failed to get files by source: {str(e)}", stack_info=True)
            return []
    
    # ============== UPDATE Operations ==============
    
    @staticmethod
    async def update_file_content(
        file_key: str,
        updates: Dict[str, Any],
        user_id: Optional[str] = None,
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
                logging.warning(f"File not found for update: file_key={file_key}")
                return False
            
            # Merge updates into current content
            current_content = current.get("file_content", {})
            current_content.update(updates)
            
            # Check if file_name needs to be updated in standalone column
            update_file_name = "file_name" in updates and updates["file_name"]
            
            if update_file_name:
                sql = """
                    UPDATE th_files
                    SET file_content = CAST(:file_content AS jsonb),
                        file_name = :file_name,
                        updated_at = now()
                    WHERE file_key = :file_key AND is_del = false
                """
            else:
                sql = """
                    UPDATE th_files
                    SET file_content = CAST(:file_content AS jsonb),
                        updated_at = now()
                    WHERE file_key = :file_key AND is_del = false
                """
            
            params: Dict[str, Any] = {
                "file_key": file_key,
                "file_content": safe_json_dumps(current_content),
            }
            
            if update_file_name:
                params["file_name"] = updates["file_name"]
            
            if user_id:
                sql = sql.replace("WHERE file_key", "WHERE user_id = :user_id AND file_key")
                params["user_id"] = str(user_id)
            
            await execute_query(query=sql, params=params)
            logging.info(f"File content updated: file_key={file_key}, updates={list(updates.keys())}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to update file content: {str(e)}", stack_info=True)
            return False
    
    @staticmethod
    async def update_file_processed(
        file_key: str,
        raw: str = "",
        file_abstract: str = "",
        indicators: Optional[List[Dict]] = None,
        file_name: Optional[str] = None,
    ) -> bool:
        """
        Update file with processing results.
        
        Args:
            file_key: File key
            raw: Raw extracted content
            file_abstract: File abstract/summary
            indicators: List of extracted indicators
            file_name: Optional generated file name
            
        Returns:
            True if successful
        """
        updates = {
            "raw": raw,
            "file_abstract": file_abstract,
            "indicators": indicators or [],
            "indicators_count": len(indicators) if indicators else 0,
            "processed": True,
            "processed_at": datetime.now().isoformat(),
            # Update status to completed
            "status": "completed",
            "error": "",
            "progress": 100,
        }
        
        if file_name:
            updates["generated_file_name"] = file_name
        
        return await FileDbService.update_file_content(file_key, updates)
    
    @staticmethod
    async def update_file_abstract(
        file_key: str,
        file_abstract: str = "",
        file_name: Optional[str] = None,
    ) -> bool:
        """
        Update file abstract and file_name only (lightweight update for abstract generation).
        
        This method only updates file_abstract and file_name fields without touching
        other processing fields like raw, indicators, processed status, etc.
        
        Args:
            file_key: File key
            file_abstract: File abstract/summary text
            file_name: Optional generated file name (e.g., extracted from PDF/image)
            
        Returns:
            True if successful
        """
        updates = {
            "file_abstract": file_abstract,
            "abstract_updated_at": datetime.now().isoformat(),
        }
        
        if file_name:
            updates["file_name"] = file_name
        
        return await FileDbService.update_file_content(file_key, updates)
    
    @staticmethod
    async def update_file_status(
        file_key: str,
        status: str,
        error: str = "",
        progress: int = 0,
        user_id: Optional[str] = None,
    ) -> bool:
        """
        Update file status.
        
        Args:
            file_key: File key
            status: Status value - one of: uploading, processing, completed, failed
            error: Error message (for failed status)
            progress: Progress percentage (0-100)
            user_id: Optional user ID for authorization
            
        Returns:
            True if successful
        """
        updates = {
            "status": status,
            "error": error,
            "progress": progress,
            "status_updated_at": datetime.now().isoformat(),
        }
        
        # Update processed flag based on status
        if status == "completed":
            updates["processed"] = True
        elif status == "failed":
            updates["processed"] = False
        
        return await FileDbService.update_file_content(file_key, updates, user_id)
    
    @staticmethod
    async def update_source_id(
        file_key: str,
        created_source_id: str,
        user_id: Optional[str] = None,
    ) -> bool:
        """
        Update the created_source_id for a file.
        
        Args:
            file_key: File key
            created_source_id: New source ID
            user_id: Optional user ID
            
        Returns:
            True if successful
        """
        try:
            sql = """
                UPDATE th_files
                SET created_source_id = :created_source_id,
                    updated_at = now()
                WHERE file_key = :file_key AND is_del = false
            """
            params: Dict[str, Any] = {
                "file_key": file_key,
                "created_source_id": created_source_id,
            }
            
            if user_id:
                sql = sql.replace("WHERE file_key", "WHERE user_id = :user_id AND file_key")
                params["user_id"] = str(user_id)
            
            await execute_query(query=sql, params=params)
            logging.info(f"File source_id updated: file_key={file_key}, source_id={created_source_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to update source_id: {str(e)}", stack_info=True)
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
                logging.info(f"File soft deleted: file_key={file_key}")
                return True
            
            logging.warning(f"File not found for deletion: file_key={file_key}")
            return False
            
        except Exception as e:
            logging.error(f"Failed to soft delete file: {str(e)}", stack_info=True)
            return False
    
    @staticmethod
    async def soft_delete_by_source_id(
        user_id: str,
        created_source_id: str,
    ) -> int:
        """
        Soft delete all files by created_source_id.
        
        Args:
            user_id: User ID
            created_source_id: Source ID to delete files for
            
        Returns:
            Number of files deleted
        """
        try:
            sql = """
                UPDATE th_files
                SET is_del = true,
                    updated_at = now()
                WHERE user_id = :user_id 
                  AND created_source_id = :created_source_id
                  AND is_del = false
            """
            
            result = await execute_query(
                query=sql,
                params={
                    "user_id": str(user_id),
                    "created_source_id": created_source_id,
                },
            )
            
            # Get count of affected rows
            count = result.get("record_count", 0) if isinstance(result, dict) else 0
            logging.info(f"Files soft deleted by source_id: source_id={created_source_id}, count={count}")
            return count
            
        except Exception as e:
            logging.error(f"Failed to soft delete by source_id: {str(e)}", stack_info=True)
            return 0
    
    @staticmethod
    async def soft_delete_files_batch(
        user_id: str,
        file_keys: List[str],
    ) -> Dict[str, Any]:
        """
        Soft delete multiple files by file_keys.
        
        Args:
            user_id: User ID
            file_keys: List of file keys to delete
            
        Returns:
            Dict with deleted_count and failed_keys
        """
        deleted_count = 0
        failed_keys = []
        
        for file_key in file_keys:
            success = await FileDbService.soft_delete_file(file_key, user_id)
            if success:
                deleted_count += 1
            else:
                failed_keys.append(file_key)
        
        logging.info(f"Batch delete completed: deleted={deleted_count}, failed={len(failed_keys)}")
        
        return {
            "deleted_count": deleted_count,
            "failed_keys": failed_keys,
        }
    
    # ============== URL Regeneration ==============
    
    @staticmethod
    async def regenerate_file_url(
        file_key: str,
        content_type: str = "application/octet-stream",
    ) -> str:
        """
        Regenerate signed URL for a file.
        
        Args:
            file_key: File key
            content_type: MIME type
            
        Returns:
            Signed URL or empty string
        """
        if not file_key:
            return ""
        
        try:
            from mirobody.utils.config.storage import get_storage_client
            
            storage = get_storage_client()
            url = await storage.generate_signed_url(
                key=file_key,
                expires=24 * 3600,  # 24 hours
                content_type=content_type,
            )
            
            return url or ""
            
        except Exception as e:
            logging.error(f"Failed to regenerate URL: {str(e)}", stack_info=True)
            return ""


# Singleton instance
file_db_service = FileDbService()
