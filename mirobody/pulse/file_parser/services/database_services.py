import asyncio
import json
import logging
from datetime import datetime
from hashlib import md5
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, unquote
from zoneinfo import ZoneInfo
from mirobody.utils import execute_query
from mirobody.utils.req_ctx import get_req_ctx

from .db_utils import (
    safe_json_dumps,
    safe_json_loads,
    parse_date,
    get_utc_now,
    extract_first_record,
    get_mime_type,
    get_simple_file_type,
)


class FileParserDatabaseService:
    @staticmethod
    async def update_message_content(
        message_id: str,
        content: str = None,
        reasoning: str = None,
        message_type: str = None,
    ) -> bool:
        """Update message content, reasoning or type.

        There was a fourth field, `comment`, that no caller ever passed — and
        `th_messages.comment` is no longer part of the schema (01_basedata), so
        writing it would now fail on a fresh database.
        """
        try:
            update_fields = []
            params = {"message_id": message_id}

            if content is not None:
                update_fields.append("content = encrypt_content(:content)")
                params["content"] = safe_json_dumps(content) if isinstance(content, (dict, list)) else content

            if reasoning is not None:
                update_fields.append("reasoning = :reasoning")
                params["reasoning"] = safe_json_dumps(reasoning) if isinstance(reasoning, (dict, list)) else reasoning

            if message_type is not None:
                update_fields.append("message_type = :message_type")
                params["message_type"] = message_type

            if not update_fields:
                return False

            update_sql = f"""
                UPDATE th_messages SET {", ".join(update_fields)} WHERE id = :message_id
            """

            update_result = await execute_query(
                query=update_sql,
                params=params,
            )

            logging.info(f"💾 [DB] Updated message {message_id} with fields: {', '.join(update_fields)}, result: {update_result}")
            return True
        except Exception as e:
            logging.error(f"Error updating message: {e}", stack_info=True)
            return False

    @staticmethod
    async def generate_summary(user_message: str, provider: str) -> str:
        """Generate session summary"""
        # Simplified implementation, can actually call AI model to generate better summary
        return user_message[:10] + "..." if len(user_message) > 10 else user_message

    @staticmethod
    async def generate_and_save_summary(
        user_id: str, session_id: str, user_message: str, provider: str
    ) -> Dict[str, Any]:
        """Asynchronously generate and save summary"""
        try:
            # Generate summary
            summary = await FileParserDatabaseService.generate_summary(user_message, provider)

            # Save summary
            await FileParserDatabaseService.save_conversation_summary(user_id, session_id, summary)

            logging.info(f"session:{session_id}\tSuccessfully saved conversation summary!")
            return {"event": "summary_generated", "session_id": session_id}

        except Exception as e:
            logging.error(f"Error in generate_and_save_summary: {str(e)}", stack_info=True)

            return None

    @staticmethod
    async def save_conversation_summary(user_id: str, session_id: str, summary: str) -> bool:
        """Save conversation summary to database"""
        try:
            logging.info(f"save_conversation_summary: {user_id}, {session_id}, {summary}")
            # Insert summary, do nothing on conflict
            summary_sql = """
                INSERT INTO th_sessions (
                    user_id, session_id, summary, created_at
                )
                VALUES (:user_id, :session_id, :summary, :created_at) 
                ON CONFLICT (session_id) DO NOTHING RETURNING session_id
            """

            await execute_query(
                query=summary_sql,
                params={
                    "user_id": user_id,
                    "session_id": session_id,
                    "summary": summary,
                    "created_at": datetime.now(),
                },
            )

            return True

        except Exception as e:
            logging.error(f"Error saving conversation summary: {str(e)}", stack_info=True)
            return False

    @staticmethod
    async def _save_to_series_data(db_params: List[Dict[str, Any]]) -> int:
        """Parallel task: save to th_series_data table.

        The unique (user, indicator, start, end) key counts soft-deleted rows,
        and this used to be a bare ON CONFLICT DO NOTHING — so a report
        re-uploaded after its file was deleted wrote NOTHING (every reading
        collided with its own deleted copy) while the log said "Write
        complete: 9 records" and the file row said 9 indicators. A collision
        with a DELETED row now revives that row as the new reading; a
        collision with a live row is still left alone.
        """
        if not db_params:
            return 0

        await execute_query(
            query="""INSERT INTO th_series_data (user_id, indicator, value, start_time, end_time, source_table, source_table_id, comment)
               VALUES (:user_id, :indicator, :value, :start_time, :end_time, :source_table, :source_table_id, encrypt_content(:comment))
               ON CONFLICT (user_id, indicator, start_time, end_time) DO UPDATE
                  SET value = EXCLUDED.value, source_table = EXCLUDED.source_table,
                      source_table_id = EXCLUDED.source_table_id, comment = EXCLUDED.comment,
                      deleted = 0, update_time = CURRENT_TIMESTAMP
                WHERE th_series_data.deleted = 1""",
            params=db_params,
        )
        logging.info(f"✅ {len(db_params)} indicator data saved to th_series_data")
        return len(db_params)

    @staticmethod
    def generate_source_table_id(msg_id: str, file_key: str) -> str:
        """
        Generate source_table_id for th_series_data based on file_key.
        
        Uses file_key directly as source_table_id since source_table is th_files.
        file_key is the unique identifier in th_files table.
        
        Args:
            msg_id: Message ID (legacy parameter, kept for backward compatibility)
            file_key: File key from th_files table (primary identifier)
            
        Returns:
            str: file_key as source_table_id, or msg_id as fallback
        """
        # Use file_key directly as source_table_id
        if file_key:
            return file_key
        
        # Fallback to msg_id if no file_key (legacy support)
        return msg_id or ""

    @staticmethod
    async def get_user_current_time_with_timezone(user_id: str) -> datetime:
        """Get current time in user's timezone, falls back to UTC"""
        try:
            from ....user.user import get_user

            first_record = await get_user(user_id=user_id)
            if not first_record:
                return get_utc_now()

            user_tz = (first_record.get("tz") or "").strip()
            if not user_tz:
                return get_utc_now()
            
            try:
                return datetime.now(ZoneInfo(user_tz)).replace(tzinfo=None)
            except Exception:
                return get_utc_now()
                
        except Exception:
            return get_utc_now()

    @staticmethod
    async def resolve_report_date(user_id: str, exam_date: str) -> tuple[datetime, str]:
        """The date a file's readings are filed under, and where it came from.

        Returns `(start_time, date_source)`; `date_source` is "extracted" when
        the document carried a usable date and "upload_time" when the user's
        current time stood in for it. The label is written on every reading
        (comment JSON) and on the file row (th_files.file_content), because
        without it a guessed date is indistinguishable from a real one: a
        report photographed as several screenshots shows its date on the first
        page only, so pages 2..n were filed under "today" and nothing recorded
        that "today" was a fallback (issue #53). The Data page asks about
        "upload_time" files, and `POST /health-indicators/file-date` answers.

        A date the model wrote in a shape `parse_date` does not know counts as
        no date. It used to raise, and the raise threw away every reading on
        the file — an unknown date is a reason to ask, not to drop the data.
        """
        if exam_date and exam_date.strip():
            start_time = parse_date(exam_date)
            if start_time is not None:
                return start_time, "extracted"
            logging.warning(f"Unparseable report date {exam_date!r} for user_id {user_id}; filing under the upload time")
        return await FileParserDatabaseService.get_user_current_time_with_timezone(user_id), "upload_time"

    @staticmethod
    async def manual_report_date(file_key: str) -> Optional[datetime]:
        """The date the user set on this file, if they set one (`date_source:
        manual` on the th_files row), else None. Read right before readings are
        saved, because the answer can arrive while extraction is still running
        and must not be overwritten by the document's own date."""
        try:
            from .file_db_service import FileDbService

            row = await FileDbService.get_file_by_key(file_key)
            content = (row or {}).get("file_content") or {}
            if content.get("date_source") != "manual":
                return None
            return parse_date(str(content.get("report_date") or ""))
        except Exception as e:
            logging.warning(f"manual_report_date lookup failed for {file_key}: {e}")
            return None

    @staticmethod
    async def save_indicators_to_db(
        user_id: str,
        indicators: List[Dict[str, Any]],
        start_time: datetime,
        date_source: str,
        msg_id: str,
        comment: str = "",
        source_table: str = "th_files",
        file_key: str = None,
    ) -> int:
        """Batch save health indicators to th_series_data table.

        `start_time` and `date_source` come from `resolve_report_date`; the
        caller resolves them so the same answer can be recorded on the file row.
        """
        try:
            end_time = start_time
            db_params = []

            for indicator in indicators:
                # Check required fields
                original_indicator = indicator.get("original_indicator")

                if not original_indicator:
                    continue

                # Generate source_table_id with file-level precision
                source_table_id = FileParserDatabaseService.generate_source_table_id(msg_id, file_key)
                
                # Build comment JSON with unit, reference_range, detection_method
                # and the date's provenance (see resolve_report_date).
                try:
                    comment_data = {
                        "unit": indicator.get("unit", ""),
                        "reference_range": indicator.get("reference_range", ""),
                        "detection_method": indicator.get("detection_method", ""),
                        "date_source": date_source,
                    }
                    comment_json = json.dumps(comment_data, ensure_ascii=False)
                except Exception as e:
                    logging.warning(f"Failed to build comment JSON for indicator {original_indicator}: {str(e)}")
                    comment_json = ""
                
                # Build th_series_data parameters
                db_params.append(
                    {
                        "user_id": str(user_id),
                        "indicator": original_indicator,
                        "value": indicator.get("value", ""),
                        "start_time": start_time,
                        "end_time": end_time,
                        "source_table": source_table,
                        "source_table_id": source_table_id,
                        "comment": comment_json,
                    }
                )

            # Execute database write tasks
            if db_params:
                await FileParserDatabaseService._save_to_series_data(db_params)

            logging.info(f"🚀 Write complete: {len(db_params)} records, user_id: {user_id}")

            if db_params:
                # Signal the worker to materialize th_series_dim + backfill
                # embeddings (embedding_<EMBEDDING_PROVIDER>), then refresh the
                # user profile. Both enqueues are coalescing + self-guarded, so a
                # Redis hiccup never fails the ingest write above.
                try:
                    from mirobody.task import IndicatorSyncTask, ProfileRefreshTask
                    await IndicatorSyncTask.enqueue("")
                    await ProfileRefreshTask.enqueue(str(user_id))
                except Exception as e:
                    logging.warning(f"Failed to enqueue indicator-sync/profile-refresh signals: {e}")

            return len(db_params)

        except Exception:
            logging.error(f"Failed to save indicators to database, user_id: {user_id}", stack_info=True)
            return 0

    @staticmethod
    async def delete_genetic_data_by_source(user_id: str, source_table: str, source_table_id: str) -> bool:
        """
        Delete genetic data by source table and ID

        Args:
            user_id: User ID
            source_table: Source table name
            source_table_id: Source table record ID

        Returns:
            bool: Whether deletion was successful
        """
        try:
            # Delete genetic data
            sql = """
                DELETE FROM th_series_data_genetic 
                WHERE user_id = :user_id 
                AND source_table = :source_table 
                AND source_table_id = :source_table_id
            """

            params = {
                "user_id": user_id,
                "source_table": source_table,
                "source_table_id": source_table_id,
            }

            await execute_query(query=sql, params=params,)

            logging.info(f"Genetic data deleted successfully, user_id: {user_id}, source_table: {source_table}, source_table_id: {source_table_id}")
            return True

        except Exception:
            logging.error(f"Failed to delete genetic data, user_id: {user_id}, source_table: {source_table}, source_table_id: {source_table_id}", stack_info=True)
            return False

    @staticmethod
    async def regenerate_file_url(file_key: str, original_filename: str = "", content_type: str = "application/octet-stream") -> str:
        """
        Regenerate file URL using unified storage client
        
        Args:
            file_key: The file key/path
            original_filename: Original filename (unused, kept for backward compatibility)
            content_type: MIME type of the file
            
        Returns:
            str: Regenerated signed URL, or empty string if regeneration fails
        """
        if not file_key:
            return ""
            
        try:
            from mirobody.utils.config.storage import get_storage_client
            
            storage = get_storage_client()
            storage_type = storage.get_storage_type()
            
            logging.debug(f"Using {storage_type} storage for URL regeneration, key: {file_key}")
            
            # Generate signed URL with 24 hours expiration
            url, err = await storage.generate_signed_url(
                key=file_key,
                expires=24 * 3600,
                content_type=content_type
            )
            if err:
                logging.warning(f"URL generation returned empty for key '{file_key}': {err}")
                return ""

            if url:
                return url
            else:
                logging.warning(f"URL generation returned empty for key: {file_key}")
                return ""
                    
        except Exception as e:
            logging.error(f"URL regeneration failed for key {file_key}: {str(e)}", stack_info=True)
            return ""



    @staticmethod
    async def _regenerate_urls(file_info: dict) -> None:
        """Regenerate URLs for file_info in place"""
        file_key = file_info.get("file_key", "")
        
        if not file_key:
            return
            
        try:
            new_url = await FileParserDatabaseService.regenerate_file_url(
                file_key, "", file_info.get("contentType", "application/octet-stream")
            )
            if new_url:
                file_info["url_full"] = new_url
        except Exception as e:
            logging.warning(f"Failed to regenerate URL for {file_key}: {str(e)}", "_regenerate_urls")

    @staticmethod
    async def get_uploaded_files_paginated(
        uploader_user_id: str,
        target_user_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get user's uploaded file history with pagination.
        
        Now reads from th_files table instead of th_messages.
        
        Args:
            uploader_user_id: Current user ID (for permission checking)
            target_user_id: Target user ID to query files (None = query uploader_user_id)
            limit: Maximum number of files to return
            offset: Pagination offset
            
        Returns:
            Dict containing files list, total count, and total size
        """
        from .file_db_service import FileDbService
        
        try:
            # Use FileDbService to query from th_files table
            # Include both report and genetic scenes
            result = await FileDbService.get_files_paginated(
                user_id=uploader_user_id,
                query_user_id=target_user_id,
                # scene=["report", "genetic", "excel", "csv"],  # Both report and genetic files
                created_source=["web_drive", "web_chat"],  # Web drive and chat uploads
                limit=limit,
                offset=offset,
            )
            
            # Regenerate URLs for files with valid file_key
            files = result.get("files", [])
            files_with_keys = [f for f in files if f.get("file_key")]
            if files_with_keys:
                await asyncio.gather(
                    *[FileParserDatabaseService._regenerate_urls(f) for f in files_with_keys],
                    return_exceptions=True
                )
            
            # Convert MIME type to friendly file type
            for f in files:
                f["file_type"] = FileParserDatabaseService._convert_mime_to_file_type(
                    f.get("file_type", ""),
                    f.get("scene", "")
                )
            
            logging.info(f"Success: query_user_id={target_user_id or uploader_user_id}, total={result.get('total', 0)}")
            
            return result
            
        except Exception as e:
            logging.error(f"Get uploaded files failed: {str(e)}", stack_info=True)
            raise Exception(f"Failed to get uploaded files: {str(e)}")

    @staticmethod
    def _convert_mime_to_file_type(mime_type: str, scene: str = "") -> str:
        """
        Convert MIME type to friendly file type name.
        
        Returns one of: excel, csv, image, pdf, genetic
        """
        if not mime_type:
            return "unknown"
        
        mime_lower = mime_type.lower()
        
        # Genetic files (scene-based detection)
        if scene == "genetic":
            return "genetic"
        
        # PDF
        if "pdf" in mime_lower:
            return "pdf"
        
        # Image types
        if mime_lower.startswith("image/"):
            return "image"
        
        # Excel types
        excel_mimes = [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # xlsx
            "application/vnd.ms-excel",  # xls
            "application/vnd.ms-excel.sheet.macroenabled.12",  # xlsm
            "application/vnd.ms-excel.sheet.binary.macroenabled.12",  # xlsb
        ]
        if mime_lower in excel_mimes or "spreadsheet" in mime_lower or "excel" in mime_lower:
            return "excel"
        
        # CSV
        if "csv" in mime_lower:
            return "csv"

        # Markdown / rich text — the upload dropzone advertises "text/Markdown"
        # as accepted, so a .md upload must not render as "unknown".
        if "markdown" in mime_lower:
            return "text"

        # Default fallback based on common patterns
        if "text/plain" in mime_lower:
            # text/plain could be genetic or csv - check file extension if available
            return "genetic"  # Default to genetic for text files in this context

        return "unknown"

    @staticmethod
    async def get_user_data_distribution(user_id: str) -> Dict[str, Any]:
        """Return aggregate counts of the user's processed health data.

        Frontend (web Home DataBar / Drive "Clean data" panel) consumes only
        ``total_records`` and ``total_categories``. The per-bucket
        ``distribution`` list has no consumer and is returned empty.
        """
        try:
            user_id = str(user_id)

            logging.info(f"Getting user data distribution: user_id={user_id}")

            # Aggregate counts from th_series_data + th_series_data_genetic.
            # Categories are derived from th_series_dim.department, with 'Other'
            # for rows whose indicator has no department mapping and 'genetic'
            # if the user has any genetic records.
            query = """
            SELECT
                (
                    SELECT COUNT(1) FROM th_series_data
                    WHERE user_id = :user_id AND deleted = 0
                ) + (
                    SELECT COUNT(1) FROM th_series_data_genetic
                    WHERE user_id = :user_id AND is_deleted = false
                ) AS total_records,
                (
                    SELECT COUNT(DISTINCT cat) FROM (
                        SELECT TRIM(d.dept) AS cat
                        FROM th_series_data t1
                        JOIN th_series_dim t2 ON t1.indicator = t2.original_indicator
                        CROSS JOIN LATERAL unnest(string_to_array(t2.department, ',')) AS d(dept)
                        WHERE t1.user_id = :user_id
                          AND t2.department IS NOT NULL
                          AND TRIM(t2.department) <> ''
                          AND TRIM(d.dept) <> ''
                        UNION
                        SELECT 'Other' WHERE EXISTS (
                            SELECT 1 FROM th_series_data t1
                            LEFT JOIN th_series_dim t2 ON t1.indicator = t2.original_indicator
                            WHERE t1.user_id = :user_id
                              AND (t2.department IS NULL OR TRIM(t2.department) = '')
                        )
                        UNION
                        SELECT 'genetic' WHERE EXISTS (
                            SELECT 1 FROM th_series_data_genetic WHERE user_id = :user_id
                        )
                    ) cats
                ) AS total_categories
            """

            results = await execute_query(query=query, params={"user_id": user_id})

            if results:
                row = results[0] if isinstance(results[0], dict) else dict(results[0])
                total_records = row.get("total_records") or 0
                total_categories = row.get("total_categories") or 0
            else:
                total_records = 0
                total_categories = 0

            logging.info(
                f"Query completed: user={user_id}, total_categories={total_categories}, total_records={total_records}"
            )

            return {
                "user_id": user_id,
                "total_categories": total_categories,
                "total_records": total_records,
                "distribution": [],
            }

        except Exception as e:
            logging.error(f"Failed to query data distribution: {str(e)}", stack_info=True)
            raise Exception(f"Failed to query data distribution: {str(e)}")

