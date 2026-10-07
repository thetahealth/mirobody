"""The WebSocket upload the web client uses: a session per upload
(`upload_start`), its files received in base64 chunks, then processed one by
one through `FileProcessor`, with progress pushed to the socket that started
it and every file filed in `th_files` when the batch is done.
"""

from __future__ import annotations

from mirobody.collect.files.services.conversation_summary import generate_and_save_summary
import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

# `fastapi` lives in the [app] extra, but file parsing is advertised engine
# functionality: a bare `pip install mirobody` must import this module. Every
# use below is an annotation, so PEP 563 (the __future__ import) keeps them as
# strings and the real symbol is only needed by type checkers.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import WebSocket
from mirobody.collect.files.errors import failure_reason
from mirobody.collect.files.file_processor import FileProcessor
from mirobody.collect.files.services.file_db_service import SOURCE_DATA, FileDbService
from mirobody.collect.files.services.file_uploader import validate_file_extension
from mirobody.utils.file_types import guess_mime
from mirobody.collect.files.handlers.genetic import GeneticHandler
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.tasks import spawn
from .memory_upload_file import MemoryUploadFile

logger = logging.getLogger(__name__)


class WebSocketFileUploadManager:
    """WebSocket file upload manager"""

    def __init__(self) -> None:
        self.active_connections: dict[str, WebSocket] = {}  # connection_id -> websocket
        self.upload_sessions: dict[str, dict] = {}  # message_id -> session_info
        self._file_processor: FileProcessor | None = None

    def _session_for(self, message_id: str | None, user_id: str | None) -> dict | None:
        """The upload `message_id` names, if `user_id` started it, else None.

        `messageId` comes from the client and the table is keyed by it alone, so
        any signed-in socket could read another account's upload status or push
        chunks into its upload. Reproduced 2026-10-01: a second account's chunk
        was processed and stored as the first account's file.
        """
        session = self.upload_sessions.get(message_id) if message_id else None
        if session is None or str(session.get("user_id")) != str(user_id):
            return None
        return session

    @property
    def file_processor(self) -> FileProcessor:
        """Built on first use: the manager is constructed at import time."""
        if self._file_processor is None:
            self._file_processor = FileProcessor()
        return self._file_processor

    async def connect(self, websocket: WebSocket, connection_id: str):
        """Establish WebSocket connection
        
        Args:
            websocket: WebSocket connection
            connection_id: Unique connection identifier (format: user_id_trace_id)
        """
        await websocket.accept()
        self.active_connections[connection_id] = websocket
        logger.info("upload socket connected: connection_id=%s", connection_id)

        # Send connection success message (include connection_id so frontend knows it)
        await self.send_message(
            connection_id,
            {
                "type": "connection_established",
                "status": "connected",
                "message": "WebSocket connection established, file upload can begin",
                "timestamp": datetime.now().isoformat(),
                "connectionId": connection_id,  # Let frontend know its connection_id
            },
        )

    def has_active_uploads(self, connection_id: str) -> bool:
        """Check if connection has active uploads"""
        for message_id, session in self.upload_sessions.items():
            if session.get("connection_id") == connection_id and session.get("status") in ["uploading", "processing"]:
                return True
        return False

    def get_active_uploads_count(self, connection_id: str) -> int:
        """Get count of connection's active uploads"""
        count = 0
        for message_id, session in self.upload_sessions.items():
            if session.get("connection_id") == connection_id and session.get("status") in ["uploading", "processing"]:
                count += 1
        return count

    async def disconnect(self, connection_id: str):
        """Disconnect WebSocket connection"""
        if connection_id in self.active_connections:
            del self.active_connections[connection_id]
            logger.info("upload socket disconnected: connection_id=%s", connection_id)

            # Clean up ALL of this connection's sessions, completed included:
            # get_upload_status is only reachable over this (now closed) socket,
            # so a completed session kept here is pure leak, it held the
            # session dict and its results payload forever.
            sessions_to_remove = []
            for message_id, session in self.upload_sessions.items():
                if session.get("connection_id") == connection_id:
                    sessions_to_remove.append(message_id)

            for message_id in sessions_to_remove:
                del self.upload_sessions[message_id]

    async def send_message(self, connection_id: str, message: dict):
        """Send message to specified connection"""
        if connection_id not in self.active_connections:
            logger.info("upload socket gone, message not sent: connection_id=%s type=%s",  # phi: ok a message type this module names
                        connection_id, message.get("type", "unknown"))
            return False

        try:
            websocket = self.active_connections[connection_id]

            # Check WebSocket connection status, only send messages when in OPEN state
            if websocket.client_state.value != 1:  # Not in OPEN state
                logger.info("upload socket closed, message not sent: connection_id=%s type=%s",  # phi: ok a message type this module names
                            connection_id, message.get("type", "unknown"))
                await self.disconnect(connection_id)
                return False

            message_json = json.dumps(message, ensure_ascii=False)
            await websocket.send_text(message_json)
            return True
        except Exception as e:
            logger.debug("upload socket send failed: connection_id=%s error_type=%s", connection_id, type(e).__name__)
            # Automatically clean up connection on send failure
            await self.disconnect(connection_id)
            return False

    async def send_message_by_message_id(self, message_id: str, message: dict) -> bool:
        """Send message to connection associated with a message_id.
        
        This method looks up the connection_id from upload_sessions using message_id,
        which is useful for background tasks that only know the message_id.
        
        Args:
            message_id: The message ID to find the connection for
            message: The message dict to send
            
        Returns:
            True if message sent successfully, False otherwise
        """
        if message_id not in self.upload_sessions:
            logger.debug("no upload session, message not sent: message_id=%s", message_id)
            return False
        
        session = self.upload_sessions[message_id]
        connection_id = session.get("connection_id")
        
        if not connection_id:
            logger.debug("upload session has no socket: message_id=%s", message_id)
            return False
        
        return await self.send_message(connection_id, message)

    async def handle_upload_start(self, connection_id: str, message_data: dict):
        """Open an upload session for the files `message_data` declares.

        Args:
            connection_id: Unique connection identifier (format: user_id_trace_id)
            message_data: Upload message data, may contain _real_user_id for file storage
        """
        try:
            # Generate message ID and session ID
            message_id = message_data.get("messageId") or str(uuid.uuid4())
            session_id = message_data.get("sessionId") or str(uuid.uuid4())
            query = message_data.get("query", "")
            is_first_message = message_data.get("isFirstMessage", False)
            files_info = message_data.get("files", [])
            query_user_id = message_data.get("query_user_id", "")  # User ID for proxy upload
            
            # Get real user_id from message_data (set by router) or extract from connection_id
            real_user_id = message_data.get("_real_user_id") or connection_id.split("_")[0]

            logger.info("upload starting: connection_id=%s user_id=%s message_id=%s file_count=%d target_user_id=%s",
                        connection_id, real_user_id, message_id, len(files_info), query_user_id)

            # AUTHORIZE a proxy upload before anything is written.
            # `query_user_id` is client-supplied: without this gate any
            # authenticated user could write a file into ANY user's record by
            # naming it here, where the victim's agent VFS reads it and the
            # attacker's own file list never shows it. That is the
            # prompt-injection delivery surface SECURITY.md warns about. Same
            # check the proxy paths in public_router use; write access is
            # required because this WRITES to the subject's record.
            if query_user_id and str(query_user_id) != str(real_user_id):
                from mirobody.user.care_circle import CareCircleDenied, resolve_subject
                try:
                    await resolve_subject(real_user_id, query_user_id, require_write=True)
                except (CareCircleDenied, ValueError, TypeError) as e:
                    logger.warning("proxy upload refused: user_id=%s target_user_id=%s error_type=%s", real_user_id,
                                   query_user_id, type(e).__name__)
                    await self.send_message(
                        connection_id,
                        {
                            "type": "error",
                            "messageId": message_id,
                            "message": "You do not have write access to that user's record.",
                        },
                    )
                    return False

            # The gate `POST /files/upload` applies, before a byte is taken:
            # this path had none, so a legacy .xls was stored, read as
            # nothing, and reported complete with 0 readings.
            for declared in files_info:
                supported, reason = validate_file_extension(declared.get("filename"))
                if not supported:
                    await self.send_message(
                        connection_id,
                        {"type": "upload_error", "messageId": message_id, "status": "failed", "message": reason},
                    )
                    return False

            # Determine target user ID for file storage (use real user_id, not connection_id)
            target_user_id = query_user_id if query_user_id else real_user_id
            
            # Create upload session
            session_info = {
                "connection_id": connection_id,  # WebSocket connection identifier (for sending messages)
                "user_id": real_user_id,  # Real user ID (for business logic)
                "target_user_id": target_user_id,  # Target user for file storage
                "message_id": message_id,
                "session_id": session_id,
                "query": query,
                "files": files_info,
                "status": "uploading",
                "progress": 0,
                "created_at": datetime.now(),
                "uploaded_files": [],
                "is_first_message": is_first_message,
                "query_user_id": query_user_id,
            }

            existing = self.upload_sessions.get(message_id)
            if existing is not None and str(existing.get("user_id")) != str(real_user_id):
                # Same answer as an unknown id: whether someone else holds this
                # messageId is not this caller's to learn.
                await self.send_message(
                    connection_id,
                    {"type": "upload_error", "messageId": message_id, "status": "failed",
                     "message": "Invalid upload session"},
                )
                return False

            # Prevent duplicate message creation, check if message ID already exists
            if message_id in self.upload_sessions:
                # If message ID exists and status is incomplete, it might be a duplicate request
                existing_session = self.upload_sessions[message_id]
                if existing_session.get("status") in ["uploading", "processing"]:
                    logger.warning("upload session already running, duplicate start ignored: message_id=%s", message_id)
                    await self.send_message(
                        connection_id,
                        {
                            "type": "upload_start_confirmed",
                            "messageId": message_id,
                            "sessionId": session_id,
                            "status": existing_session.get("status", "uploading"),
                            "progress": existing_session.get("progress", 0),
                            "message": "File upload already in progress...",
                            "files": files_info,
                        },
                    )
                    return True
                logger.info("upload session finished, starting a new one: message_id=%s", message_id)

            self.upload_sessions[message_id] = session_info

            # A new chat session gets its sidebar title from the files.
            if is_first_message:
                try:
                    file_names = [f.get("filename", "unknown") for f in files_info]
                    file_names_str = ", ".join(file_names) if file_names else "unknown files"
                    summary_message = f"file: {file_names_str}"

                    await generate_and_save_summary(
                        user_id=real_user_id,
                        session_id=session_id,
                        user_message=summary_message,
                        provider="system",
                    )
                except Exception as e:
                    logger.error("session summary failed: session_id=%s error_type=%s", session_id, type(e).__name__,
                                 exc_info=not is_driver_exception(e))

            # Send upload start confirmation
            await self.send_message(
                connection_id,
                {
                    "type": "upload_start_confirmed",
                    "messageId": message_id,
                    "sessionId": session_id,
                    "status": "uploading",
                    "progress": 0,
                    "message": f"Ready to receive {len(files_info)} files",
                    "files": files_info,
                },
            )

            return True

        except Exception as e:
            logger.error("upload start failed: connection_id=%s error_type=%s", connection_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            await self.send_message(
                connection_id,
                {
                    "type": "upload_error",
                    "messageId": message_data.get("messageId"),
                    "status": "failed",
                    "message": "Upload start failed",
                },
            )
            return False

    async def handle_file_chunk(self, connection_id: str, message_data: dict):
        """Handle file data chunk"""


        try:
            message_id = message_data.get("messageId")
            filename = message_data.get("filename")
            chunk_data = message_data.get("chunk")  # base64 encoded data
            chunk_index = message_data.get("chunkIndex", 0)
            chunk_count = message_data.get("totalChunks", 1)

            real_user_id = message_data.get("_real_user_id") or connection_id.split("_")[0]
            if self._session_for(message_id, real_user_id) is None:
                await self.send_message(
                    connection_id,
                    {
                        "type": "upload_error",
                        "messageId": message_id,
                        "status": "failed",
                        "message": "Invalid upload session",
                    },
                )
                return False

            session = self.upload_sessions[message_id]

            # Decode file data
            import base64

            try:
                file_content = base64.b64decode(chunk_data)
            except (TypeError, ValueError):
                await self.send_message(
                    connection_id,
                    {
                        "type": "upload_error",
                        "messageId": message_id,
                        "filename": filename,
                        "status": "failed",
                        "message": "Failed to decode file data",
                    },
                )
                return False

            logger.debug("upload chunk: message_id=%s chunk_index=%s chunk_count=%s", message_id, chunk_index,
                         chunk_count)
            # Check if data for this file already exists
            existing_file = None
            for uploaded_file in session["uploaded_files"]:
                if uploaded_file["filename"] == filename:
                    existing_file = uploaded_file
                    break

            if existing_file is None:
                # New file, create record
                content_type = message_data.get("contentType", "application/octet-stream")
                file_size = message_data.get("fileSize", 0)

                file_record = {
                    "filename": filename,
                    "content_type": content_type,
                    "size": file_size,
                    "chunks": {},
                    "total_chunks": chunk_count,
                    "received_chunks": 0,
                    # The file's bytes once every chunk is in, else None.
                    "content": None,
                }
                session["uploaded_files"].append(file_record)
                existing_file = file_record

            # A chunk outside the declared count would let the count complete
            # with a real chunk missing, and the file be assembled without it.
            if not 0 <= chunk_index < existing_file["total_chunks"]:
                await self.send_message(
                    connection_id,
                    {"type": "upload_error", "messageId": message_id, "filename": filename, "status": "failed",
                     "message": "Invalid chunk index"},
                )
                return False
            if existing_file["content"] is None and chunk_index not in existing_file["chunks"]:
                existing_file["chunks"][chunk_index] = file_content
                existing_file["received_chunks"] += 1

            # Calculate file upload progress
            file_progress = (existing_file["received_chunks"] / existing_file["total_chunks"]) * 100

            # Send progress update
            await self.send_message(
                connection_id,
                {
                    "type": "file_progress",
                    "messageId": message_id,
                    "filename": filename,
                    "progress": file_progress,
                    "status": "uploading",
                    "message": f"Uploading {filename}: {file_progress:.1f}%",
                },
            )

            # Check if file is complete
            if existing_file["content"] is None and existing_file["received_chunks"] == existing_file["total_chunks"]:
                # One copy of the file, not the chunks beside it as well.
                chunks = existing_file["chunks"]
                existing_file["content"] = b"".join(chunks[i] for i in range(existing_file["total_chunks"]))
                chunks.clear()

                # Update actual file size in record
                actual_file_size = len(existing_file["content"])
                existing_file["size"] = actual_file_size

                # Complete file received
                await self.send_message(
                    connection_id,
                    {
                        "type": "file_received",
                        "messageId": message_id,
                        "filename": filename,
                        "status": "received",
                        "message": f"File {filename} received successfully",
                        "size": actual_file_size,
                    },
                )

                logger.info("upload file received: message_id=%s size_bytes=%d", message_id, actual_file_size)

            # Every file upload_start declared, not only those that have begun
            # to arrive: checking the arrived ones started processing after the
            # first file of a batch and again after the last, so each file was
            # filed and extracted twice. Once per session.
            declared = {f.get("filename") for f in session.get("files") or [] if f.get("filename")}
            received = {f["filename"] for f in session["uploaded_files"] if f["content"] is not None}
            if len(received) >= len(declared) and not session.get("processing_started"):
                session["processing_started"] = True
                await self.start_file_processing(connection_id, message_id)

            return True

        except Exception as e:
            logger.error("upload chunk failed: connection_id=%s error_type=%s", connection_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            await self.send_message(
                connection_id,
                {
                    "type": "upload_error",
                    "messageId": message_data.get("messageId"),
                    "filename": message_data.get("filename"),
                    "status": "failed",
                    "message": "Failed to process file data",
                },
            )
            return False

    async def start_file_processing(self, connection_id: str, message_id: str):
        """Start processing uploaded files"""
        try:
            session = self.upload_sessions[message_id]
            uploaded_files = list(session["uploaded_files"])
            query = session["query"]
            query_user_id = session["query_user_id"]  # Get proxy upload user ID
            real_user_id = session["user_id"]  # Real user ID for business logic

            logger.info("upload processing: message_id=%s user_id=%s file_count=%d target_user_id=%s", message_id,
                        real_user_id, len(uploaded_files), query_user_id)

            # Check for genetic files using MemoryUploadFile adapter
            has_genetic_files = False
            for f in uploaded_files:
                # Create adapter for checking
                temp_file = MemoryUploadFile(f["content"], f["filename"], f["content_type"])
                if await GeneticHandler.is_genetic_file(temp_file):
                    has_genetic_files = True
                    break


            # Process files asynchronously - pass connection_id for WebSocket, real_user_id for business logic
            spawn(self.process_files_async(connection_id, message_id, uploaded_files, query, query_user_id, has_genetic_files, real_user_id))

        except Exception as e:
            logger.error("upload processing not started: message_id=%s error_type=%s", message_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            await self.update_progress(connection_id, message_id, "failed", 0, "Processing failed")

    async def process_files_async(
        self,
        connection_id: str,
        message_id: str,
        uploaded_files: list[dict],
        query: str,
        query_user_id: str,
        has_genetic_files: bool = False,
        real_user_id: str = None,
    ):
        """Process files asynchronously using unified FileProcessor
        
        Args:
            connection_id: WebSocket connection identifier (for sending progress updates)
            message_id: Upload message ID
            uploaded_files: List of uploaded file data
            query: User query
            query_user_id: Proxy upload user ID
            has_genetic_files: Whether files contain genetic data
            real_user_id: Real user ID for business logic (file storage, database operations)
        """
        FileDbService.expect_rows(message_id)
        try:
            session = self.upload_sessions[message_id]
            # Use real_user_id from parameter or session
            user_id_for_business = real_user_id or session.get("user_id")
            total_files = len(uploaded_files)
            # One per uploaded file, in order: the handler's result, or a
            # failure with its reason.
            outcomes: list[dict] = []

            # Calculate progress allocation
            progress_config = self._calculate_progress_allocation(total_files, has_genetic_files)
            base_progress = progress_config["base_progress"]
            max_progress = progress_config["max_progress"]
            progress_per_file = progress_config["progress_per_file"]

            for i, file_data in enumerate(uploaded_files):
                file_start_progress = base_progress + (i * progress_per_file)
                file_end_progress = min(base_progress + ((i + 1) * progress_per_file), max_progress)
                current_file_callback = self._create_progress_callback(
                    file_start_progress, file_end_progress, i + 1, file_data["filename"], connection_id, message_id
                )
                upload = MemoryUploadFile(file_data["content"], file_data["filename"], file_data["content_type"])
                try:
                    result = await self.file_processor.process_single_file(
                        file=upload,
                        query=query,
                        user_id=user_id_for_business,
                        message_id=message_id,
                        query_user_id=query_user_id,
                        progress_callback=current_file_callback,
                    )
                except Exception as e:
                    logger.error("file processing failed: message_id=%s file_index=%d error_type=%s", message_id, i,
                                 type(e).__name__, exc_info=not is_driver_exception(e))
                    result = {"success": False, "message": failure_reason(e)}
                outcomes.append(result or {"success": False})

            successful_files = sum(1 for o in outcomes if o.get("success"))
            failed_files = total_files - successful_files
            logger.info("upload processed: message_id=%s successful=%d total=%d", message_id, successful_files,
                        total_files)

            return_info = self._return_info(uploaded_files, outcomes, message_id, user_id_for_business, query_user_id,
                                            session)
            # Every file is filed, a failed one too: the row says why it failed.
            await self._save_files_to_database(
                return_info=return_info,
                message_id=message_id,
                user_id=user_id_for_business,
                query_user_id=query_user_id,
                session_id=session.get("session_id", ""),
            )

            if successful_files == 0:
                await self.send_message(
                    connection_id,
                    {
                        "type": "upload_completed",
                        "messageId": message_id,
                        "status": "failed",
                        "progress": 0,
                        "message": f"All {total_files} files failed to process",
                        "results": return_info,
                        "successful_files": 0,
                        "failed_files": total_files,
                        "total_files": total_files,
                    },
                )
                session["status"] = "failed"
                session["progress"] = 0
                session["results"] = return_info
                return

            await self._send_final_completion_status(
                connection_id, message_id, session, return_info, has_genetic_files, successful_files, failed_files, total_files
            )
            await self._start_profile_refresh(user_id_for_business, message_id, query_user_id)

        except Exception as e:
            logger.error("upload processing failed: message_id=%s error_type=%s", message_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            await self.update_progress(connection_id, message_id, "failed", 0, "Processing failed")
        finally:
            FileDbService.rows_written(message_id)
            # Nothing reads a file's bytes after this: kept, every upload's
            # content stayed resident in `upload_sessions` until its socket
            # closed, and on every path that failed.
            for f in uploaded_files:
                f["content"] = None

    async def update_genetic_processing_complete(self, user_id: str, message_id: str):
        """Update genetic processing completion status"""
        try:
            if message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                session["status"] = "completed"
                session["progress"] = 100
            else:
                logger.warning("no upload session for a finished genotype load: message_id=%s", message_id)
        except Exception as e:
            logger.error("recording a finished genotype load failed: message_id=%s error_type=%s", message_id,
                         type(e).__name__, exc_info=not is_driver_exception(e))

    # ==================== Helper Methods for process_files_async ====================

    async def _save_files_to_database(
        self,
        return_info: dict,
        message_id: str,
        user_id: str,
        query_user_id: str,
        session_id: str,
    ):
        """
        Save uploaded files to th_files table.
        
        This is the primary storage for file records. All file metadata is stored here
        including file_key, urls, raw content, indicators, status, etc.
        
        Both successful and failed files are saved (if they have file_key).
        Status field in file_content tracks: uploading, processing, completed, failed
        
        Args:
            return_info: The return information containing files array
            message_id: Message ID (used as created_source_id for tracking)
            user_id: User ID (uploader)
            query_user_id: Query user ID (file owner, if different from uploader)
            session_id: Session ID
        """
        try:
            files_array = return_info.get("files", [])
            if not files_array:
                logger.warning("no files to file: message_id=%s", message_id)
                return
            
            # Prepare files_info for batch insert
            files_info = []
            for file_entry in files_array:
                file_key = file_entry.get("file_key", "")
                
                # Skip files without file_key (never uploaded to storage)
                if not file_key:
                    logger.warning("a file with no storage key is not filed: message_id=%s", message_id)
                    continue
                
                # Determine status based on success flag
                is_success = file_entry.get("success", True)
                if is_success:
                    status = "completed"
                    error_message = ""
                else:
                    status = "failed"
                    error_message = file_entry.get("error", "Processing failed")
                
                # Extract original filename for display
                original_filename = file_entry.get("filename", "")
                # Use generated file_name if available, otherwise fallback to original
                display_file_name = file_entry.get("file_name", original_filename)
                
                # Get MIME type from filename extension
                actual_mime_type = guess_mime(original_filename or file_key)
                
                file_info = {
                    "file_key": file_key,
                    "file_name": display_file_name,
                    "original_filename": original_filename,  # Keep original filename
                    "file_type": actual_mime_type,  # Use derived MIME type
                    "content_type": actual_mime_type,
                    "url_thumb": file_entry.get("url_thumb", ""),
                    "url_full": file_entry.get("url_full", ""),
                    "file_size": file_entry.get("file_size", file_entry.get("size", 0)),
                    "raw": file_entry.get("raw", ""),
                    "file_abstract": file_entry.get("file_abstract", ""),
                    "original_text": file_entry.get("original_text", ""),  # Original text for rerank
                    "text_length": file_entry.get("text_length", 0),  # Text length for rerank strategy
                    "content_hash": file_entry.get("content_hash", ""),  # SHA256 hash for deduplication
                    "indicators": file_entry.get("indicators", []),
                    "indicators_count": file_entry.get("indicators_count", 0),
                    "processed": is_success,
                    "session_id": session_id,
                    "upload_time": datetime.now(UTC).isoformat(),
                    "query": return_info.get("query", ""),  # Store user query if provided
                    "status": status,  # uploading, processing, completed, failed
                    "error": error_message,  # Error message if failed
                    "progress": 100 if is_success else 0,  # Progress percentage
                }
                files_info.append(file_info)
            
            if not files_info:
                logger.warning("no files to file: message_id=%s", message_id)
                return
            
            # Determine target user for file ownership
            # user_id = current logged-in user (who uploaded)
            # query_user_id = target user (for whom the file is uploaded)
            target_user_id = query_user_id if query_user_id else user_id
            
            # A mixed upload must not give every file the first genetic file's
            # scene: the Agent file projection makes its decision per row.
            scenes_by_key = {
                str(entry["file_key"]): (
                    "genetic" if entry.get("type") == "genetic" else
                    "excel" if entry.get("type") == "excel" else
                    "csv" if entry.get("type") == "csv" else "report"
                )
                for entry in files_array if entry.get("file_key")
            }
            
            # Insert files into th_files table
            # user_id: current user who performed the upload
            # query_user_id: target user who owns the data (for "upload for others" feature)
            inserted_ids = await FileDbService.insert_files_batch(
                user_id=user_id,
                files_info=files_info,
                scene="report",
                scenes_by_key=scenes_by_key,
                created_source=SOURCE_DATA,
                created_source_id=message_id,
                query_user_id=target_user_id,
            )
            
            if inserted_ids:
                success_count = sum(1 for f in files_info if f.get("status") == "completed")
                failed_count = sum(1 for f in files_info if f.get("status") == "failed")
                logger.info("files filed: message_id=%s inserted=%d succeeded=%d failed=%d", message_id,
                            len(inserted_ids), success_count, failed_count)
            else:
                logger.warning("no file was filed: message_id=%s", message_id)
                
        except Exception as e:
            logger.error("filing files failed: message_id=%s error_type=%s", message_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            # Don't raise - this is a secondary operation, the main upload was successful

    def _calculate_progress_allocation(self, total_files: int, has_genetic_files: bool) -> dict:
        """
        Calculate progress allocation for file processing.
        Returns dict with base_progress, max_progress, progress_per_file.
        """
        if has_genetic_files:
            # Genetic files: each file uses 30-50% range, remaining for genetic processing system
            progress_per_file = 20 // total_files if total_files > 0 else 20
            base_progress = 30
            max_progress = 50
        else:
            # Non-genetic files: each file uses 30-90% range, last 10% for completion processing
            available_progress = 60  # 30% to 90%
            progress_per_file = available_progress // total_files if total_files > 0 else available_progress
            base_progress = 30
            max_progress = 90

        return {
            "base_progress": base_progress,
            "max_progress": max_progress,
            "progress_per_file": progress_per_file,
        }

    def _create_progress_callback(
        self,
        start_prog: int,
        end_prog: int,
        file_index: int,
        file_name: str,
        connection_id: str,
        message_id: str,
    ) -> Callable[[int, str], Awaitable[None]]:
        """
        Create a file-specific progress callback function.
        Maps internal file progress (30-100) to the allocated progress range.
        """
        async def file_progress_callback(progress: int, message: str):
            # Map file internal progress (30-100) to allocated range
            if progress <= 30:
                mapped_progress = start_prog
            elif progress >= 100:
                mapped_progress = end_prog
            else:
                # Linear mapping: progress 30-100 -> start_prog to end_prog
                progress_ratio = (progress - 30) / 70
                mapped_progress = start_prog + int(progress_ratio * (end_prog - start_prog))

            # The upload SESSION, not the file name: a check-up report is named
            # for the person it is about, and this callback fires roughly seven
            # times per file. 1.4.2 moved two statements off the name and left
            # this one, which was the "seven times" the entry was written about.
            logger.info("upload progress: message_id=%s file_index=%d progress=%d mapped=%d", message_id, file_index,
                        progress, mapped_progress)
            await self.update_progress(
                connection_id,
                message_id,
                "processing",
                mapped_progress,
                message,
                filename=file_name,
            )

        return file_progress_callback

    async def _send_final_completion_status(
        self,
        connection_id: str,
        message_id: str,
        session: dict,
        return_info: dict,
        has_genetic_files: bool,
        successful_files: int,
        failed_files: int,
        total_files: int,
    ):
        """
        Send final completion status via WebSocket.
        Handles both genetic and non-genetic file completion flows.
        """
        if has_genetic_files:
            # For genetic files, keep processing status at 50% progress
            session["status"] = "processing"
            session["progress"] = 50
            session["results"] = return_info

            await self.send_message(
                connection_id,
                {
                    "type": "upload_progress",
                    "messageId": message_id,
                    "status": "processing",
                    "progress": 50,
                    "message": "Genetic files uploaded successfully, processing in background...",
                    "results": return_info,
                    "successful_files": successful_files,
                    "failed_files": failed_files,
                    "total_files": total_files,
                },
            )
        else:
            # For non-genetic files, send final completion message
            session["status"] = "completed"
            session["progress"] = 100
            session["results"] = return_info

            # Determine final status message
            if failed_files > 0:
                final_message = f"Partially completed: {successful_files}/{total_files} files processed successfully"
                final_status = "partial_success"
            else:
                final_message = f"Processing completed: {successful_files} files successful"
                final_status = "completed"

            # Smooth 90-100% progress transition
            logger.info("upload finishing: message_id=%s", message_id)

            await self.update_progress(connection_id, message_id, "processing", 92, "Completing final processing...")
            await asyncio.sleep(0.1)

            await self.update_progress(connection_id, message_id, "processing", 96, "Almost complete...")
            await asyncio.sleep(0.1)

            await self.update_progress(connection_id, message_id, final_status, 100, final_message)

            # Send final completion message
            await self.send_message(
                connection_id,
                {
                    "type": "upload_completed",
                    "messageId": message_id,
                    "status": final_status,
                    "progress": 100,
                    "message": final_message,
                    "results": return_info,
                    "successful_files": successful_files,
                    "failed_files": failed_files,
                    "total_files": total_files,
                },
            )

    async def _start_profile_refresh(
        self,
        user_id: str,
        message_id: str,
        query_user_id: str,
    ):
        """Rebuild the record owner's health profile after an upload, in the
        background. It was named an "embedding update": the embedding half went
        with the semantic tier, and this is what remained."""
        try:
            async def refresh_profile():
                try:
                    # Start user profile creation. Lazy import, this is
                    # documented seam #4 (see pyproject ignore_imports): the
                    # profile GENERATOR lives agent-side because it calls the
                    # LLM. Importing it at module scope would make
                    # `import mirobody.collect.files` require langchain on
                    # a bare engine install; function scope defers that cost
                    # to the moment a profile is actually (re)built, exactly
                    # like task/profile_refresh does.
                    from mirobody.user.profile import UserProfileService

                    owner_user_id = query_user_id if query_user_id else user_id
                    await UserProfileService.create_user_profile(owner_user_id)

                except Exception as e:
                    logger.error("profile refresh after an upload failed: error_type=%s", type(e).__name__)

            spawn(refresh_profile())
        except Exception as e:
            logger.error("profile refresh after an upload not started: error_type=%s", type(e).__name__)

    def _return_info(
        self,
        uploaded_files: list[dict],
        outcomes: list[dict],
        message_id: str,
        user_id: str,
        query_user_id: str,
        session: dict,
    ) -> dict:
        """What an upload reports and files: one entry per uploaded file,
        `outcomes` aligned with `uploaded_files`. A failed batch and a
        succeeded one were built by two functions, and the second counted the
        file sizes by the index of the SUCCESSFUL files, so a genotype file's
        size went to whichever file sat at its index in the upload."""
        upload_time = datetime.now(UTC).isoformat()
        files_array = []
        for file_data, outcome in zip(uploaded_files, outcomes, strict=True):
            size = file_data.get("size", 0)
            file_type = outcome.get("type", "file")
            if not outcome.get("success"):
                files_array.append({
                    "filename": file_data["filename"],
                    "contentType": file_data["content_type"],
                    "type": file_type,
                    "url_thumb": "",
                    "url_full": "",
                    "raw": "",
                    "file_abstract": "",
                    "file_name": file_data["filename"],
                    "size": size,
                    "file_size": size,
                    "file_key": outcome.get("file_key", ""),
                    "error": outcome.get("message") or "File processing failed",
                    "success": False,
                })
                continue
            if file_type == "genetic" and outcome.get("file_size"):
                size = outcome["file_size"]
            generated_name = outcome.get("file_name", "")
            files_array.append({
                "filename": file_data["filename"],
                "type": file_type,
                "url_thumb": outcome.get("url_thumb", outcome.get("full_url", "")),
                "url_full": outcome.get("full_url", ""),
                "raw": str(outcome.get("raw") or ""),
                "file_abstract": outcome.get("file_abstract", ""),
                "file_name": generated_name if generated_name and file_type in ("pdf", "image") else file_data["filename"],
                "file_size": size,
                "file_key": outcome.get("file_key", ""),
                "original_text": outcome.get("original_text", ""),
                "text_length": outcome.get("text_length", 0),
                "content_hash": outcome.get("content_hash", ""),
                # Handlers that extract indicators synchronously (csv/genetic
                # overrides) return them here; without these keys
                # _save_files_to_database always inserted indicators: [].
                "indicators": outcome.get("indicators", []),
                "indicators_count": outcome.get("indicators_count", len(outcome.get("indicators", []) or [])),
                "success": True,
            })

        succeeded = [o for o in outcomes if o.get("success")]
        total_files = len(uploaded_files)
        if not succeeded:
            status, message_text = "failed", f"All {total_files} files failed to process"
        elif len(succeeded) < total_files:
            status = "partial_success"
            message_text = f"Partially successful: {len(succeeded)}/{total_files} files processed successfully"
        else:
            status, message_text = "completed", "File processing completed"
        is_uploaded_for_others = bool(query_user_id) and query_user_id != user_id
        return {
            "success": bool(succeeded),
            "status": status,
            "message": message_text,
            "type": succeeded[0].get("type", "file") if succeeded else "file",
            "url_thumb": [o.get("url_thumb", o.get("full_url", "")) for o in succeeded],
            "url_full": [o.get("full_url", "") for o in succeeded],
            "message_id": message_id,
            "files": files_array,
            "original_filenames": [f["filename"] for f in uploaded_files],
            "file_sizes": [entry.get("file_size", 0) for entry in files_array],
            "upload_time": upload_time,
            "total_files": total_files,
            "successful_files": len(succeeded),
            "failed_files": total_files - len(succeeded),
            "CODE_VERSION": "v2.0_WEBSOCKET_EXCEL_SUPPORTED",
            "query": session.get("query", ""),
            "session_id": session.get("session_id", ""),
            "query_user_id": query_user_id,
            "target_user_name": f"User{query_user_id[:8]}" if is_uploaded_for_others else "",
            "is_uploaded_for_others": is_uploaded_for_others,
            "timestamp": upload_time,
        }

    async def update_progress(self, connection_id: str, message_id: str, status: str, progress: int, message: str,
                              filename: str | None = None) -> None:
        """Record an upload's progress on its session and send it to the socket."""
        try:
            # Update session status
            if message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                session["status"] = status
                session["progress"] = progress
                session["last_message"] = message
                session["updated_at"] = datetime.now()

            # Send WebSocket message
            websocket_data = {
                "type": "upload_progress",
                "messageId": message_id,
                "status": status,
                "progress": progress,
                "message": message,
                "timestamp": datetime.now().isoformat(),
            }
            
            # Include filename if provided, or try to extract from session data
            if filename:
                websocket_data["filename"] = filename
            elif message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                files = session.get("files", [])
                if files and len(files) > 0:
                    # Use the first file's original filename as display name
                    websocket_data["filename"] = files[0].get("filename", "")
            
            await self.send_message(connection_id, websocket_data)

        except Exception as e:
            logger.error("upload progress not sent: message_id=%s error_type=%s", message_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))

    async def handle_upload_end(self, connection_id: str, message_data: dict):
        """Handle file upload end"""
        try:
            message_id = message_data.get("messageId")
            logger.info("upload ending: connection_id=%s message_id=%s", connection_id, message_id)

            real_user_id = message_data.get("_real_user_id") or connection_id.split("_")[0]
            session = self._session_for(message_id, real_user_id)
            if session is not None:

                # Update session status
                session["status"] = "completed"
                session["end_time"] = datetime.now()

                # Send confirmation message
                await self.send_message(
                    connection_id,
                    {
                        "type": "upload_end_response",
                        "messageId": message_id,
                        "status": "completed",
                        "message": "File upload completed",
                        "timestamp": datetime.now().isoformat(),
                    },
                )

                return True
            logger.warning("upload end for no session: message_id=%s", message_id)
            await self.send_message(
                connection_id,
                {
                    "type": "upload_end_response",
                    "messageId": message_id,
                    "status": "error",
                    "message": "Upload session not found",
                },
            )
            return False

        except Exception as e:
            logger.error("upload end failed: connection_id=%s error_type=%s", connection_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            await self.send_message(
                connection_id,
                {
                    "type": "upload_end_response",
                    "messageId": message_data.get("messageId"),
                    "status": "error",
                    "message": "Failed to handle upload end",
                },
            )
            return False

    async def get_upload_status(self, connection_id: str, message_id: str, user_id: str | None = None) -> dict:
        """Get upload status, for the account that started the upload only."""
        try:
            session = self._session_for(message_id, user_id or connection_id.split("_")[0])
            if session is not None:
                return {
                    "type": "upload_status",
                    "messageId": message_id,
                    "status": session.get("status", "unknown"),
                    "progress": session.get("progress", 0),
                    "files": session.get("files", []),
                    "timestamp": datetime.now().isoformat(),
                }
            return {
                "type": "upload_status",
                "messageId": message_id,
                "status": "not_found",
                "message": "Upload session not found",
            }
        except Exception as e:
            logger.error("upload status failed: message_id=%s error_type=%s", message_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return {
                "type": "upload_status",
                "messageId": message_id,
                "status": "error",
                "message": "Failed to get status",
            }


_websocket_file_upload_manager_instance: WebSocketFileUploadManager | None = None


def get_websocket_file_upload_manager() -> WebSocketFileUploadManager:
    """The process's one upload manager: its sessions are what a background
    task finds a client's socket by."""
    global _websocket_file_upload_manager_instance
    if _websocket_file_upload_manager_instance is None:
        _websocket_file_upload_manager_instance = WebSocketFileUploadManager()
    return _websocket_file_upload_manager_instance
