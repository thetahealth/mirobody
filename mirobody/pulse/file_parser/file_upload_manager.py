"""
WebSocket file upload manager
Supports file upload through WebSocket with real-time progress synchronization
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, List

from fastapi import WebSocket
from mirobody.utils import execute_query, get_req_ctx, safe_read_cfg
from mirobody.pulse.file_parser.file_processor import FileProcessor

from mirobody.pulse.file_parser.services.async_file_processor import AsyncFileProcessor
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.tools.utils_sync_dim_table import sync_all_missing_indicators
from mirobody.pulse.file_parser.handlers.genetic import GeneticHandler

from ...chat.user_profile import UserProfileService


class MemoryUploadFile:
    """Mimics FastAPI UploadFile for in-memory content"""
    def __init__(self, content: bytes, filename: str, content_type: str):
        self.content = content
        self.filename = filename
        self.content_type = content_type
        self._position = 0
        self.size = len(content)
        # Dummy file attribute if accessed directly
        self.file = self

    async def read(self, size: int = -1):
        if size == -1:
            result = self.content[self._position :]
            self._position = len(self.content)
        else:
            end_pos = min(self._position + size, len(self.content))
            result = self.content[self._position : end_pos]
            self._position = end_pos
        return result

    async def seek(self, position: int, whence: int = 0):
        if whence == 0:
            self._position = max(0, min(position, len(self.content)))
        elif whence == 1:
            self._position = max(0, min(self._position + position, len(self.content)))
        elif whence == 2:
            self._position = max(0, min(len(self.content) + position, len(self.content)))
        return self._position
    
    def tell(self):
        return self._position

    async def close(self):
        pass


class WebSocketFileUploadManager:
    """WebSocket file upload manager"""

    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}  # user_id -> websocket
        self.upload_sessions: Dict[str, Dict] = {}  # message_id -> session_info
        self._file_processor = None  # Lazy initialization
        self.instance_id = f"ws_upload_{datetime.now().timestamp()}"
        # Database service will be initialized when needed
        self.db_service = None

    @property
    def file_processor(self):
        """Lazy initialization of FileProcessor to allow ExcelProcessor injection"""
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
        logging.info(f"🔗 WebSocket file upload connection established - connection_id: {connection_id}")

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
            logging.info(f"🔌 WebSocket file upload connection disconnected - connection_id: {connection_id}")

            # Clean up incomplete upload sessions for this connection
            sessions_to_remove = []
            for message_id, session in self.upload_sessions.items():
                if session.get("connection_id") == connection_id and session.get("status") != "completed":
                    sessions_to_remove.append(message_id)

            for message_id in sessions_to_remove:
                del self.upload_sessions[message_id]

    async def send_message(self, connection_id: str, message: Dict):
        """Send message to specified connection"""
        # Add detailed debug information
        logging.debug(f"🔍 [WebSocket] Preparing to send message to connection {connection_id}, message type: {message.get('type', 'unknown')}")
        logging.debug(f"🔍 [WebSocket] Current active connections: {list(self.active_connections.keys())}, total count: {len(self.active_connections)}")

        if connection_id not in self.active_connections:
            logging.info(f"Connection {connection_id} not found, cannot send message: {message.get('type', 'unknown')}")
            logging.debug(f"[WebSocket] Connection {connection_id} not in active connections list: {list(self.active_connections.keys())}")
            return False

        try:
            websocket = self.active_connections[connection_id]

            # Check WebSocket connection status, only send messages when in OPEN state
            if websocket.client_state.value != 1:  # Not in OPEN state
                logging.info(f"[WebSocket] Connection closed, cannot send message to connection {connection_id}: {message.get('type', 'unknown')}")
                await self.disconnect(connection_id)
                return False

            message_json = json.dumps(message, ensure_ascii=False)
            await websocket.send_text(message_json)
            logging.debug(f"✅ Successfully sent WebSocket message to connection {connection_id}: {message.get('type', 'unknown')} - {message.get('message', '')}")
            return True
        except Exception as e:
            logging.debug(f"⚠️ Failed to send WebSocket message to connection {connection_id}: {e}")
            # Automatically clean up connection on send failure
            await self.disconnect(connection_id)
            return False

    async def handle_upload_start(self, connection_id: str, message_data: Dict):
        """Handle file upload start
        
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

            logging.info(f"Starting file upload: connection_id={connection_id}, real_user_id={real_user_id}, message_id={message_id}, file_count={len(files_info)}, proxy_user_id={query_user_id}")

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

            # Prevent duplicate message creation, check if message ID already exists
            if message_id in self.upload_sessions:
                # If message ID exists and status is incomplete, it might be a duplicate request
                existing_session = self.upload_sessions[message_id]
                if existing_session.get("status") in ["uploading", "processing"]:
                    logging.warning(f"Message ID {message_id} already exists and is being processed, ignoring duplicate request")
                    await self.send_message(
                        connection_id,
                        {
                            "type": "upload_start",
                            "messageId": message_id,
                            "sessionId": session_id,
                            "status": existing_session.get("status", "uploading"),
                            "progress": existing_session.get("progress", 0),
                            "message": "File upload already in progress...",
                            "files": files_info,
                        },
                    )
                    return True
                else:
                    logging.info(f"Message ID {message_id} exists but is completed, creating new upload session")

            self.upload_sessions[message_id] = session_info

            # Generate summary for first message
            if is_first_message:
                try:
                    file_names = [f.get("filename", "unknown") for f in files_info]
                    file_names_str = ", ".join(file_names) if file_names else "unknown files"
                    summary_message = f"file: {file_names_str}"

                    await FileParserDatabaseService.generate_and_save_summary(
                        user_id=real_user_id,
                        session_id=session_id,
                        user_message=summary_message,
                        provider="system",
                    )
                    logging.info(f"Generated first message summary for session {session_id}")
                except Exception as e:
                    logging.error(f"Failed to generate first message summary: {e}")

            # Check if message record already exists in database
            try:
                existing_message = await FileParserDatabaseService.get_message_details(message_id)
                if existing_message:
                    logging.warning(f"Message record {message_id} already exists in database, skipping creation")
                else:
                    # Create initial message record using real_user_id as uploader
                    await FileParserDatabaseService.log_chat_message(
                        id=message_id,
                        question_id=message_id,
                        user_id=real_user_id,  # Store uploader user ID
                        session_id=session_id,
                        role="user",
                        content=json.dumps(
                            {
                                "status": "uploading",
                                "progress": 0,
                                "files": files_info,
                                "query": query,
                                "message": "File uploading...",
                                "timestamp": datetime.now().isoformat(),
                                "query_user_id": query_user_id,
                                "uploader_user_id": real_user_id,  # Track who actually uploaded it
                            }
                        ),
                        reasoning="WebSocket file upload start",
                        agent="agent0",
                        provider="system",
                        message_type="file",
                        user_name="",
                        query_user_id=target_user_id,
                    )
                    logging.info(f"Created new message record: {message_id}")
            except Exception as e:
                logging.error(f"Failed to check or create message record: {e}")
                # If check fails, still try to create message record
                await FileParserDatabaseService.log_chat_message(
                    id=message_id,
                    question_id=message_id,
                    user_id=real_user_id,
                    session_id=session_id,
                    role="user",
                    content=json.dumps(
                        {
                            "status": "uploading",
                            "progress": 0,
                            "files": files_info,
                            "query": query,
                            "message": "File uploading...",
                            "timestamp": datetime.now().isoformat(),
                            "query_user_id": query_user_id,
                        }
                    ),
                    reasoning="WebSocket file upload start",
                    agent="agent0",
                    provider="system",
                    message_type="file",
                    user_name="",
                    query_user_id=target_user_id,
                )

            # Send upload start confirmation
            await self.send_message(
                connection_id,
                {
                    "type": "upload_start",
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
            logging.error(f"Failed to handle file upload start: {e}", stack_info=True)
            await self.send_message(
                connection_id,
                {
                    "type": "upload_error",
                    "messageId": message_data.get("messageId"),
                    "status": "failed",
                    "message": f"Upload start failed: {str(e)}",
                },
            )
            return False

    async def handle_file_chunk(self, connection_id: str, message_data: Dict):
        """Handle file data chunk"""


        try:
            message_id = message_data.get("messageId")
            filename = message_data.get("filename")
            chunk_data = message_data.get("chunk")  # base64 encoded data
            chunk_index = message_data.get("chunkIndex", 0)
            total_chunks = message_data.get("totalChunks", 1)

            if not message_id or message_id not in self.upload_sessions:
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
            except Exception as e:
                await self.send_message(
                    connection_id,
                    {
                        "type": "upload_error",
                        "messageId": message_id,
                        "filename": filename,
                        "status": "failed",
                        "message": f"Failed to decode file data: {str(e)}",
                    },
                )
                return False

            logging.info(f"filename: {filename}, chunk_index: {chunk_index}, total_chunks: {total_chunks}")
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
                    "total_chunks": total_chunks,
                    "received_chunks": 0,
                    "content": bytearray(),
                }
                session["uploaded_files"].append(file_record)
                existing_file = file_record

            # Add data chunk
            if chunk_index not in existing_file["chunks"]:
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
            if existing_file["received_chunks"] == existing_file["total_chunks"]:
                # Reassemble file content
                existing_file["content"] = bytearray()
                for i in range(existing_file["total_chunks"]):
                    if i in existing_file["chunks"]:
                        existing_file["content"].extend(existing_file["chunks"][i])

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

                logging.info(f"File received successfully: {filename}, actual size: {actual_file_size} bytes")

            # Check if all files have been received
            all_files_received = all(f["received_chunks"] == f["total_chunks"] for f in session["uploaded_files"])
            if all_files_received:
                # Start processing files
                await self.start_file_processing(connection_id, message_id)

            return True

        except Exception as e:
            logging.error(f"Failed to handle file data chunk: {e}", stack_info=True)
            await self.send_message(
                connection_id,
                {
                    "type": "upload_error",
                    "messageId": message_data.get("messageId"),
                    "filename": message_data.get("filename"),
                    "status": "failed",
                    "message": f"Failed to process file data: {str(e)}",
                },
            )
            return False

    async def start_file_processing(self, connection_id: str, message_id: str):
        """Start processing uploaded files"""
        try:
            session = self.upload_sessions[message_id]
            uploaded_files = session["uploaded_files"]
            query = session["query"]
            query_user_id = session["query_user_id"]  # Get proxy upload user ID
            real_user_id = session["user_id"]  # Real user ID for business logic

            logging.info(f"Starting file processing: connection_id={connection_id}, real_user_id={real_user_id}, message_id={message_id}, file_count={len(uploaded_files)}, proxy_user_id={query_user_id}")

            # Check for genetic files using MemoryUploadFile adapter
            has_genetic_files = False
            for f in uploaded_files:
                # Create adapter for checking
                temp_file = MemoryUploadFile(f["content"], f["filename"], f["content_type"])
                if await GeneticHandler.is_genetic_file(temp_file):
                    has_genetic_files = True
                    break

            # Let progress callback handle progress updates
            logging.info(f"Starting file processing flow, genetic files: {has_genetic_files}")

            # Process files asynchronously - pass connection_id for WebSocket, real_user_id for business logic
            asyncio.create_task(self.process_files_async(connection_id, message_id, uploaded_files, query, query_user_id, has_genetic_files, real_user_id))

        except Exception as e:
            logging.error(f"Failed to start file processing: {e}", stack_info=True)
            await self.update_progress(connection_id, message_id, "failed", 0, f"Processing failed: {str(e)}")

    async def process_files_async(
        self,
        connection_id: str,
        message_id: str,
        uploaded_files: List[Dict],
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
        try:
            session = self.upload_sessions[message_id]
            # Use real_user_id from parameter or session
            user_id_for_business = real_user_id or session.get("user_id")
            results = []
            failed_results = []  # Collect failed file results for complete info storage
            url_thumb = []
            url_full = []
            type_list = []
            raws = []

            total_files = len(uploaded_files)

            # Calculate progress allocation
            progress_config = self._calculate_progress_allocation(total_files, has_genetic_files)
            base_progress = progress_config["base_progress"]
            max_progress = progress_config["max_progress"]
            progress_per_file = progress_config["progress_per_file"]

            logging.info(f"Progress allocation: base={base_progress}%, max={max_progress}%, per_file={progress_per_file}%, total_files={total_files}")

            # Process each file
            for i, file_data in enumerate(uploaded_files):
                try:
                    # Calculate current file progress range
                    file_start_progress = base_progress + (i * progress_per_file)
                    file_end_progress = min(base_progress + ((i + 1) * progress_per_file), max_progress)

                    logging.info(f"File {i + 1}/{total_files} ({file_data['filename']}): progress range {file_start_progress}%-{file_end_progress}%")

                    # Create progress callback for current file (use connection_id for WebSocket communication)
                    current_file_callback = self._create_progress_callback(
                        file_start_progress, file_end_progress, i + 1, file_data["filename"], connection_id, message_id
                    )

                    # Wrap in MemoryUploadFile adapter
                    temp_file = MemoryUploadFile(
                        file_data["content"], 
                        file_data["filename"], 
                        file_data["content_type"]
                    )

                    # UNIFIED PROCESSING ENTRY POINT (use real user_id for business logic)
                    result = await self.file_processor.process_single_file(
                        file=temp_file,
                        query=query,
                        user_id=user_id_for_business,
                        message_id=message_id,
                        query_user_id=query_user_id,
                        progress_callback=current_file_callback,
                        )

                    # Collect successful results
                    if result and result.get("success", False):
                        results.append(result)
                        url_thumb.append(result.get("url_thumb", result.get("full_url", "")))
                        url_full.append(result.get("full_url", ""))
                        type_list.append(result.get("type", "file"))
                        raws.append(self._normalize_raw_data(result.get("raw", "")))

                        logging.info(f"File {i + 1} ({file_data['filename']}) processed successfully")
                    else:
                        # Collect failed file info including file_key if available
                        error_message = result.get("message", "File processing failed") if result else "File processing failed"
                        failed_info = {
                            "index": i,
                            "filename": file_data["filename"],
                            "content_type": file_data["content_type"],
                            "size": file_data.get("size", len(file_data.get("content", b""))),
                            "error": error_message,
                            "file_key": result.get("file_key", "") if result else "",
                            "type": result.get("type", "file") if result else "file",
                        }
                        failed_results.append(failed_info)
                        logging.error(f"File {i + 1} ({file_data['filename']}) processing failed: {error_message}")

                except Exception as e:
                    # Collect exception info as failed result
                    failed_info = {
                        "index": i,
                        "filename": file_data["filename"],
                        "content_type": file_data["content_type"],
                        "size": file_data.get("size", len(file_data.get("content", b""))),
                        "error": str(e),
                        "file_key": "",
                        "type": "file",
                    }
                    failed_results.append(failed_info)
                    logging.error(f"Failed to process file {file_data['filename']}: {e}")

            # Statistics of processing results
            successful_files = len(results)
            failed_files = total_files - successful_files

            logging.info(f"Processing result statistics: {successful_files}/{total_files} files successful")

            # Start background tasks for file abstract generation
            files_data_for_background = self._prepare_background_files_data(uploaded_files, results)
            await self._start_async_background_tasks(files_data_for_background, message_id)

            if successful_files == 0:
                # Build complete return info even for failed files
                return_info = self._build_return_info_for_failed(
                    uploaded_files, failed_results, message_id, user_id_for_business, query_user_id, session
                )
                
                # Update message content in database with complete file info
                await FileParserDatabaseService.update_message_content(
                    message_id=message_id,
                    content=return_info,
                    message_type="file",
                )
                
                # Send failure status with complete file info (use connection_id for WebSocket)
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
                
                # Update session status
                session["status"] = "failed"
                session["progress"] = 0
                session["results"] = return_info
                
                logging.info(f"All files failed, saved complete file info: message_id={message_id}")
                return

            # Build return information (include failed_results for partial success scenarios)
            return_info = await self._build_return_info(
                uploaded_files, results, raws, url_thumb, url_full, type_list, message_id, user_id_for_business, query_user_id, session, failed_results
            )

            # Update message content in database
            message_type = return_info.get("type", "file")
            updated_return_info = return_info.copy()
            updated_return_info["type"] = message_type

            await FileParserDatabaseService.update_message_content(
                message_id=message_id,
                content=updated_return_info,
                message_type=message_type,
            )

            # Send final completion status (use connection_id for WebSocket)
            await self._send_final_completion_status(
                connection_id, message_id, session, return_info, has_genetic_files, successful_files, failed_files, total_files
            )

            logging.info(f"File processing completed and final message sent: connection_id={connection_id}, message_id={message_id}, status={session['status']}")

            # Start embedding update background task (use real user_id for business logic)
            await self._start_embedding_update_task(user_id_for_business, message_id, query_user_id)

        except Exception as e:
            logging.error(f"Asynchronous file processing failed: {e}", stack_info=True)
            await self.update_progress(connection_id, message_id, "failed", 0, f"Processing failed: {str(e)}")

    async def update_genetic_processing_complete(self, user_id: str, message_id: str):
        """Update genetic processing completion status"""
        try:
            if message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                session["status"] = "completed"
                session["progress"] = 100
                logging.info(f"Genetic processing session status updated: user_id={user_id}, message_id={message_id}")
            else:
                logging.warning(f"Session information not found: user_id={user_id}, message_id={message_id}")
        except Exception as e:
            logging.error(f"Failed to update genetic processing completion status: {e}")

    # ==================== Helper Methods for process_files_async ====================

    def _normalize_raw_data(self, raw_data, filename: str = "") -> str:
        """
        Normalize raw data to string type.
        Handles bytes, bytearray, and other types conversion.
        """
        if isinstance(raw_data, (bytes, bytearray)):
            try:
                return raw_data.decode("utf-8", errors="replace")
            except Exception as e:
                logging.warning(f"Failed to decode raw data{f' for {filename}' if filename else ''}: {e}")
                return f"<File content decode failed: {str(e)}>"
        elif not isinstance(raw_data, str):
            return str(raw_data)
        return raw_data

    def _calculate_progress_allocation(self, total_files: int, has_genetic_files: bool) -> Dict:
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
        user_id: str,
        message_id: str,
    ):
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

            logging.info(f"File{file_index} ({file_name}): internal progress {progress}% -> mapped progress {mapped_progress}% | {message}")
            await self.update_progress(
                user_id,
                message_id,
                "processing",
                mapped_progress,
                message,
                filename=file_name,
            )

        return file_progress_callback

    def _prepare_background_files_data(
        self,
        uploaded_files: List[Dict],
        results: List[Dict],
    ) -> List[Dict]:
        """
        Prepare files data for background processing tasks.
        Only includes successfully processed files.
        """
        files_data = []
        for i, file_data in enumerate(uploaded_files):
            if i < len(results) and results[i].get("success", False):
                files_data.append({
                    "content": file_data.get("content", b""),
                    "filename": file_data.get("filename", ""),
                    "content_type": file_data.get("content_type", ""),
                    "s3_key": results[i].get("s3_key", results[i].get("file_key", "")),
                })
        return files_data

    async def _start_async_background_tasks(
        self,
        files_data: List[Dict],
        message_id: str,
    ):
        """
        Start background tasks for file abstract generation.
        These tasks run independently and won't affect the main processing flow.
        """
        if not files_data:
            logging.warning(f"⚠️ [Background Tasks] No valid files data for background processing: message_id={message_id}")
            return

        try:
            language = get_req_ctx("language", "en")

            logging.info(f"🚀 [Background Tasks] Starting abstract generation task: message_id={message_id}, file_count={len(files_data)}")
            asyncio.create_task(
                AsyncFileProcessor.generate_file_abstracts_async(
                    files_data=files_data,
                    message_id=message_id,
                    language=language,
                )
            )
        except Exception as bg_error:
            logging.error(f"❌ [Background Tasks] Failed to start background tasks: message_id={message_id}, error={str(bg_error)}", stack_info=True)

    async def _send_final_completion_status(
        self,
        user_id: str,
        message_id: str,
        session: Dict,
        return_info: Dict,
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
                user_id,
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
            logging.info(f"Starting final completion transition: user_id={user_id}, message_id={message_id}")

            await self.update_progress(user_id, message_id, "processing", 92, "Completing final processing...")
            await asyncio.sleep(0.1)

            await self.update_progress(user_id, message_id, "processing", 96, "Almost complete...")
            await asyncio.sleep(0.1)

            await self.update_progress(user_id, message_id, final_status, 100, final_message)

            # Send final completion message
            await self.send_message(
                user_id,
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

    async def _start_embedding_update_task(
        self,
        user_id: str,
        message_id: str,
        query_user_id: str,
    ):
        """
        Start embedding update background task after file processing completion.
        Includes indicator sync and user profile creation.
        """
        try:
            logging.info(f"Starting embedding update background task: user_id={user_id}, message_id={message_id}")

            async def update_embedding_task():
                try:
                    # Only sync indicators if indicator extraction is enabled
                    enable_indicator_extraction = safe_read_cfg("ENABLE_INDICATOR_EXTRACTION") or "0"
                    is_aliyun = safe_read_cfg("CLUSTER") == "ALIYUN"

                    if int(enable_indicator_extraction) or is_aliyun:
                        await sync_all_missing_indicators(generate_embeddings=True)
                    else:
                        logging.info("Skipping sync_all_missing_indicators - indicator extraction is disabled")

                    # Start user profile creation
                    owner_user_id = query_user_id if query_user_id else user_id
                    await UserProfileService.create_user_profile(owner_user_id)

                    logging.info(f"Embedding update background task completed: user_id={user_id}, message_id={message_id}")
                except Exception as e:
                    logging.error(f"Embedding update background task failed: {e}", stack_info=True)

            asyncio.create_task(update_embedding_task())

            logging.info(f"Embedding update background task started: user_id={user_id}, message_id={message_id}")
        except Exception as e:
            logging.error(f"Failed to start embedding update background task: {e}", stack_info=True)

    def _build_return_info_for_failed(
        self,
        uploaded_files: List[Dict],
        failed_results: List[Dict],
        message_id: str,
        user_id: str,
        query_user_id: str,
        session: Dict,
    ) -> Dict:
        """
        Build return information for failed file processing.
        Ensures complete file info (including file_key) is saved even when all files fail.
        """
        import datetime as dt
        upload_time = dt.datetime.now().isoformat()
        total_files = len(uploaded_files)
        
        # Build files array with complete info for failed files
        files_array = []
        for i, file_data in enumerate(uploaded_files):
            # Find corresponding failed result
            failed_info = None
            for fr in failed_results:
                if fr.get("index") == i:
                    failed_info = fr
                    break
            
            file_entry = {
                "filename": file_data["filename"],
                "contentType": file_data["content_type"],
                "size": file_data.get("size", len(file_data.get("content", b""))),
                "type": failed_info.get("type", "file") if failed_info else "file",
                "url_thumb": "",
                "url_full": "",
                "raw": "",
                "file_abstract": "",
                "file_name": file_data["filename"],
                "file_key": failed_info.get("file_key", "") if failed_info else "",
                "error": failed_info.get("error", "Processing failed") if failed_info else "Processing failed",
                "success": False,
            }
            files_array.append(file_entry)
        
        # Build file_sizes array
        file_sizes_array = [
            f.get("size", len(f.get("content", b""))) for f in uploaded_files
        ]
        
        # Handle proxy upload user information
        target_user_name = ""
        is_uploaded_for_others = False
        if query_user_id and query_user_id != user_id:
            is_uploaded_for_others = True
            target_user_name = f"User{query_user_id[:8]}"
        
        return {
            "success": False,
            "status": "failed",
            "message": f"All {total_files} files failed to process",
            "type": "file",
            "url_thumb": [],
            "url_full": [],
            "message_id": message_id,
            "files": files_array,
            "original_filenames": [f["filename"] for f in uploaded_files],
            "file_sizes": file_sizes_array,
            "upload_time": upload_time,
            "total_files": total_files,
            "successful_files": 0,
            "failed_files": total_files,
            "CODE_VERSION": "v2.0_WEBSOCKET_EXCEL_SUPPORTED",
            "query": session.get("query", ""),
            "session_id": session.get("session_id", ""),
            "query_user_id": query_user_id,
            "target_user_name": target_user_name,
            "is_uploaded_for_others": is_uploaded_for_others,
            "timestamp": upload_time,
        }

    # ==================== End Helper Methods ====================

    async def _build_return_info(
        self,
        uploaded_files,
        results,
        raws,
        url_thumb,
        url_full,
        type_list,
        message_id,
        user_id,
        query_user_id,
        session,
        failed_results: List[Dict] = None,  # Optional: include failed file info for partial success
    ):
        """Build return information"""
        import datetime

        upload_time = datetime.datetime.now().isoformat()
        successful_files = len(results)
        total_files = len(uploaded_files)
        failed_files = total_files - successful_files
        
        # Create a mapping from original file index to failed result info
        failed_results_map = {}
        if failed_results:
            for fr in failed_results:
                failed_results_map[fr.get("index")] = fr

        # Build files array, ensure all necessary fields are included
        files_array = []
        result_index = 0  # Track position in results array (only successful files)
        
        for i in range(len(uploaded_files)):
            file_size = uploaded_files[i].get("size", 0)
            
            # Check if this file was successfully processed
            # Since results only contains successful files, we need to check by filename
            is_failed_file = i in failed_results_map
            
            if is_failed_file:
                # This file failed - get info from failed_results
                failed_info = failed_results_map[i]
                file_entry = {
                    "filename": uploaded_files[i]["filename"],
                    "contentType": uploaded_files[i]["content_type"],
                    "type": failed_info.get("type", "file"),
                    "url_thumb": "",
                    "url_full": "",
                    "raw": "",
                    "file_abstract": "",
                    "file_name": uploaded_files[i]["filename"],
                    "file_size": failed_info.get("size", file_size) or len(uploaded_files[i].get("content", b"")),
                    "file_key": failed_info.get("file_key", ""),
                    "error": failed_info.get("error", "Processing failed"),
                    "success": False,
                }
                files_array.append(file_entry)
            else:
                # This file was successful - use results array
                if result_index < len(results):
                    result = results[result_index]
                    result_index += 1
                    
                    # Normalize raw field to string type
                    raw_data = raws[result_index - 1] if result_index - 1 < len(raws) else ""
                    raw_data = self._normalize_raw_data(raw_data, uploaded_files[i]["filename"])

                    # For genetic files, get file size from processing results
                    if result.get("type") == "genetic":
                        result_file_size = result.get("file_size", file_size)
                        if result_file_size and result_file_size > 0:
                            file_size = result_file_size

                    # Extract file abstract from results or generate it
                    file_abstract = ""
                    file_name = uploaded_files[i]["filename"]
                    file_type = result.get("type", "file")
                    
                    if result and isinstance(result, dict):
                        file_abstract = result.get("file_abstract", "")
                        generated_name = result.get("file_name", "")
                        if generated_name and file_type in ["pdf", "image"]:
                            file_name = generated_name
                    
                    # Create proper abstract if not available in results
                    if not file_abstract:
                        try:
                            from mirobody.pulse.file_parser.services.file_abstract_extractor import FileAbstractExtractor
                            extractor = FileAbstractExtractor()
                            
                            logging.info(f"🔍 [WebSocket] Generating file abstract for {uploaded_files[i]['filename']}, type: {file_type}")
                            
                            result_data = await extractor.extract_file_abstract(
                                file_content=uploaded_files[i]["content"],
                                file_type=file_type,
                                filename=uploaded_files[i]["filename"],
                                content_type=uploaded_files[i]["content_type"]
                            )
                            file_abstract = result_data.get("file_abstract", "")
                            generated_name = result_data.get("file_name", "")
                            if generated_name and file_type in ["pdf", "image"]:
                                file_name = generated_name
                            
                            logging.info(f"✅ [WebSocket] Generated file abstract for {uploaded_files[i]['filename']}: '{file_abstract[:50]}...', file_name: '{file_name}'")
                            
                        except Exception as abstract_error:
                            logging.warning(f"⚠️ [WebSocket] File abstract generation failed for {uploaded_files[i]['filename']}: {str(abstract_error)}")
                            file_abstract = f"File: {uploaded_files[i]['filename']} - File uploaded successfully"

                    file_entry = {
                        "filename": uploaded_files[i]["filename"],
                        "type": file_type,
                        "url_thumb": result.get("url_thumb", result.get("full_url", "")),
                        "url_full": result.get("full_url", ""),
                        "raw": raw_data,
                        "file_abstract": file_abstract,
                        "file_name": file_name,
                        "file_size": file_size,
                        "file_key": result.get("file_key", ""),
                        "success": True,
                    }
                    files_array.append(file_entry)
                else:
                    # Fallback: no result available, treat as failed without specific info
                    file_entry = {
                        "filename": uploaded_files[i]["filename"],
                        "contentType": uploaded_files[i]["content_type"],
                        "type": "file",
                        "url_thumb": "",
                        "url_full": "",
                        "raw": "",
                        "file_abstract": "",
                        "file_name": uploaded_files[i]["filename"],
                        "file_size": file_size or len(uploaded_files[i].get("content", b"")),
                        "file_key": "",
                        "error": "Processing failed",
                        "success": False,
                    }
                    files_array.append(file_entry)

        # Build file_sizes array using actual file sizes
        file_sizes_array = []
        for i, f in enumerate(uploaded_files):
            actual_size = f.get("size", 0)

            # For genetic files, get file size from processing results
            if i < len(results) and results[i].get("type") == "genetic":
                result_file_size = results[i].get("file_size", actual_size)
                if result_file_size and result_file_size > 0:
                    actual_size = result_file_size

            file_sizes_array.append(actual_size)

        # Build different response information based on processing results
        if failed_files > 0:
            # Partial or complete failure scenarios
            success_status = successful_files > 0  # Count as partial success if any succeeded
            message_text = f"Partially successful: {successful_files}/{total_files} files processed successfully"
        else:
            # Complete success
            success_status = True
            message_text = "File processing completed"


        # Handle proxy upload user information
        target_user_name = ""
        is_uploaded_for_others = False

        if query_user_id and query_user_id != user_id:
            is_uploaded_for_others = True
            # Use default target username format
            target_user_name = f"User{query_user_id[:8]}"

        return {
            "success": success_status,
            "message": message_text,
            "type": type_list[0] if type_list else "file",
            "url_thumb": url_thumb,
            "url_full": url_full,
            "message_id": message_id,
            # Remove outer "raw" field - keep only the "raw" fields inside files array
            "files": files_array,
            "original_filenames": [f["filename"] for f in uploaded_files],
            "file_sizes": file_sizes_array,
            "upload_time": upload_time,
            "total_files": len(uploaded_files),
            "successful_files": successful_files,
            "failed_files": failed_files,
            "CODE_VERSION": "v2.0_WEBSOCKET_EXCEL_SUPPORTED",
            "query_user_id": query_user_id,
            "target_user_name": target_user_name,
            "is_uploaded_for_others": is_uploaded_for_others,
        }

    async def update_progress(self, user_id: str, message_id: str, status: str, progress: int, message: str, filename: str = None):
        """Update progress and sync to database and WebSocket"""
        try:
            # Update session status
            if message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                session["status"] = status
                session["progress"] = progress
                session["last_message"] = message
                session["updated_at"] = datetime.now()

            # Get existing message content to avoid overwriting file result information
            try:
                existing_message = await FileParserDatabaseService.get_message_details(message_id)

                if existing_message and existing_message.get("content"):
                    import json

                    try:
                        existing_content = (
                            json.loads(existing_message["content"])
                            if isinstance(existing_message["content"], str)
                            else existing_message["content"]
                        )
                    except (json.JSONDecodeError, TypeError):
                        existing_content = {}
                else:
                    existing_content = {}
            except Exception as e:
                logging.warning(f"Failed to get existing message content: {e}")
                existing_content = {}

            # Update progress information but retain existing file result information
            content_data = existing_content.copy() if existing_content else {}

            # Update progress and status information
            content_data.update(
                {
                    "status": status,
                    "progress": progress,
                    "message": message,
                    "timestamp": datetime.now().isoformat(),
                }
            )

            # If session information exists, add more details (but don't overwrite existing file information)
            if message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                session_info = {
                    "files": session.get("files", []),
                    "query": session.get("query", ""),
                    "session_id": session.get("session_id", ""),
                    "query_user_id": session.get("query_user_id", ""),  # Add proxy upload user ID to message content
                }

                # Only add if this information doesn't exist, avoid overwriting existing complete file results
                for key, value in session_info.items():
                    if key not in content_data or not content_data[key]:
                        content_data[key] = value

            # Ensure important file result information is not overwritten
            # If session has complete results information, use it with priority
            if message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                if session.get("results"):
                    session_results = session["results"]
                    # Retain important information like file URLs
                    important_keys = [
                        "url_thumb",
                        "url_full",
                        "files",
                        "original_filenames",
                        "file_sizes",
                        "successful_files",
                        "failed_files",
                        "total_files",
                        "type",
                    ]
                    for key in important_keys:
                        if key in session_results:
                            content_data[key] = session_results[key]

            # Update chat message
            await FileParserDatabaseService.update_message_content(message_id=message_id, content=content_data)

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
            
            await self.send_message(user_id, websocket_data)

        except Exception as e:
            logging.error(f"Failed to update progress: {e}", stack_info=True)

    async def delete_failed_message_record(self, message_id: str):
        """Delete failed message records"""
        try:
            # Delete message record directly from database
            delete_sql = """
                DELETE FROM  theta_ai.th_messages 
                WHERE id = :message_id
            """

            await execute_query(
                query=delete_sql,
                params={"message_id": message_id}
            )

            logging.info(f"Successfully deleted failed message record: {message_id}")

        except Exception as e:
            logging.error(f"Failed to delete failed message record: {message_id}, error: {e}", stack_info=True)

    async def handle_upload_end(self, connection_id: str, message_data: Dict):
        """Handle file upload end"""
        try:
            message_id = message_data.get("messageId")
            logging.info(f"Handling upload end: connection_id={connection_id}, message_id={message_id}")

            if message_id and message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]

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

                logging.info(f"Upload end processing completed: message_id={message_id}")
                return True
            else:
                logging.warning(f"Upload session does not exist: message_id={message_id}")
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
            logging.error(f"Failed to handle upload end: {e}", stack_info=True)
            await self.send_message(
                connection_id,
                {
                    "type": "upload_end_response",
                    "messageId": message_data.get("messageId"),
                    "status": "error",
                    "message": f"Failed to handle upload end: {str(e)}",
                },
            )
            return False

    async def handle_ping(self, connection_id: str, message_data: Dict):
        """Handle ping messages (Note: ping is now handled directly in router, this is for backward compatibility)"""
        try:
            await self.send_message(
                connection_id,
                {
                    "type": "pong",
                    "timestamp": datetime.now().isoformat(),
                },
            )
            return True
        except Exception as e:
            logging.error(f"Failed to handle ping message: {e}")
            return False

    async def get_upload_status(self, connection_id: str, message_id: str) -> Dict:
        """Get upload status"""
        try:
            if message_id in self.upload_sessions:
                session = self.upload_sessions[message_id]
                return {
                    "type": "upload_status",
                    "messageId": message_id,
                    "status": session.get("status", "unknown"),
                    "progress": session.get("progress", 0),
                    "files": session.get("files", []),
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                return {
                    "type": "upload_status",
                    "messageId": message_id,
                    "status": "not_found",
                    "message": "Upload session not found",
                }
        except Exception as e:
            logging.error(f"Failed to get upload status: {e}")
            return {
                "type": "upload_status",
                "messageId": message_id,
                "status": "error",
                "message": f"Failed to get status: {str(e)}",
            }


# Singleton pattern to ensure only one WebSocket manager instance
_websocket_file_upload_manager_instance = None


def get_websocket_file_upload_manager():
    """Get the singleton WebSocket file upload manager instance"""
    global _websocket_file_upload_manager_instance
    if _websocket_file_upload_manager_instance is None:
        _websocket_file_upload_manager_instance = WebSocketFileUploadManager()
    return _websocket_file_upload_manager_instance


# Create global instance for backward compatibility
websocket_file_upload_manager = get_websocket_file_upload_manager()
