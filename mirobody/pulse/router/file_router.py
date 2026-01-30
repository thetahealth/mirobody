"""
WebSocket routes for data_server with file upload progress and real-time communication
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from fastapi import Request

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
from mirobody.utils.req_ctx import set_req_ctx
from mirobody.utils.utils_auth import verify_token, verify_token_string
from mirobody.utils.utils_user import get_query_user_id

from mirobody.pulse.file_parser.file_upload_manager import get_websocket_file_upload_manager
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.list_my_data import MyDataService

# Additional imports for async file processing
from mirobody.pulse.file_parser.services.file_processing_service import (
    delete_files_from_message,
    delete_all_files_from_message,
    upload_files_to_storage
)
from mirobody.pulse.file_parser.services.file_processing_service import FileUploadData

# Import for direct S3 upload endpoint (now using universal service)
# Note: FileUploader, validate_file_extension, generate_file_key are used via upload_files_to_storage

# Get global file upload manager instance (singleton)
websocket_file_upload_manager = get_websocket_file_upload_manager()

router = APIRouter()

# Server-side timeout configuration
WEBSOCKET_IDLE_TIMEOUT = 5 * 60  # 5 minutes in seconds for idle connections
WEBSOCKET_UPLOAD_TIMEOUT = 30 * 60  # 30 minutes in seconds for active uploads

class FileUploadResponse(BaseModel):
    """File upload response model"""
    
    code: int
    msg: str
    data: Optional[List[FileUploadData]]


class FileDeleteRequest(BaseModel):
    """File deletion request model"""
    
    message_id: Union[str, int]
    file_keys: Optional[List[str]] = None  # If None, delete all files
    
    @field_validator('message_id', mode='before')
    @classmethod
    def coerce_message_id_to_str(cls, v):
        """Convert message_id to string (accepts both str and int from frontend)"""
        return str(v) if v is not None else v


class FileDeleteResponse(BaseModel):
    """File deletion response model"""
    
    code: int
    msg: str
    data: Optional[Dict[str, Any]]


my_data_service = MyDataService()

@router.get("/files/{file_path:path}", tags=["files"])
async def serve_storage_file(file_path: str):
    """
    Proxy files from storage (MinIO/S3/OSS) through backend.
    
    This endpoint provides a unified URL for file access that works both
    in browser and inside Docker containers.
    
    URL format: /files/uploads/20231125_123456_abc123.pdf
    
    Benefits:
    - Single URL works for both browser and container access
    - Hides storage implementation details
    - Can add access control if needed
    """
    from fastapi.responses import StreamingResponse
    from mirobody.utils.config.storage import get_storage_client
    import io
    
    try:
        # Security check: prevent path traversal
        if ".." in file_path or file_path.startswith("/"):
            logging.warning(f"Attempted path traversal: {file_path}")
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Get storage client
        storage = get_storage_client()
        
        # Get file from storage
        content, url = await storage.get(file_path)
        
        if content is None:
            logging.warning(f"File not found in storage: {file_path}")
            raise HTTPException(status_code=404, detail="File not found")
        
        # Determine content type from filename
        content_type = storage.get_content_type_from_filename(file_path)
        
        # Extract filename for Content-Disposition header
        filename = file_path.split("/")[-1] if "/" in file_path else file_path
        
        logging.debug(f"Serving file from storage: {file_path}, size: {len(content)} bytes")
        
        # Return file as streaming response
        return StreamingResponse(
            io.BytesIO(content),
            media_type=content_type,
            headers={
                "Content-Disposition": f'inline; filename="{filename}"',
                "Content-Length": str(len(content)),
                "Cache-Control": "public, max-age=86400",  # Cache for 1 day
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error serving file {file_path}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# === WebSocket Routes ===


@router.websocket("/ws/upload-health-report")
async def websocket_upload_health_report(
    websocket: WebSocket, 
    token: str = Query(..., description="Authentication token"),
    connectionId: str = Query(None, description="Optional client-provided connection ID for reconnection support")
):
    """
    WebSocket file upload endpoint
    Supports real-time file upload and progress synchronization
    
    Args:
        token: Authentication token (required)
        connectionId: Optional client-provided connection ID. If provided, the server will use it;
                     otherwise, a new one will be generated. This enables reconnection to previous sessions.
    """
    user_id = None
    # Generate unique trace_id for this WebSocket connection
    trace_id = str(uuid.uuid4())

    # Set request context with trace_id for the entire WebSocket session
    ctx = {
        "trace_id": trace_id,
        "connection_type": "websocket",
        "endpoint": "/ws/upload-health-report",
    }

    logging.info("🔗 WebSocket file upload connection initiated")


    with set_req_ctx(ctx):
        try:
            # Verify token
            try:
                user_id = await verify_token_string(token)
                if not user_id:
                    await websocket.close(code=1008, reason="Invalid token")
                    return
            except Exception as e:
                logging.error(f"Token verification failed: {e}, token: {token}")
                await websocket.close(code=1008, reason="Token verification failed")
                return

            # Use client-provided connectionId or default to user_id (backward compatible)
            if connectionId:
                # Validate that connectionId starts with user_id (security check)
                if not connectionId.startswith(f"{user_id}_") and connectionId != str(user_id):
                    logging.warning(f"Invalid connectionId format: {connectionId}, expected prefix: {user_id}_ or exact match: {user_id}")
                    # Fall back to user_id for backward compatibility
                    connection_id = str(user_id)
                else:
                    connection_id = connectionId
                    logging.info(f"🔗 Using client-provided connectionId: {connection_id}")
            else:
                # Default to user_id for backward compatibility (single tab per user)
                # If frontend wants multi-tab support, it should provide a unique connectionId
                connection_id = str(user_id)
            
            logging.info(f"🔗 WebSocket file upload connection established: user_id={user_id}, connection_id={connection_id}")

            # Establish connection using connection_id (not just user_id) to support multiple tabs
            await websocket_file_upload_manager.connect(websocket, connection_id)

            # Add server-side timeout mechanism
            last_activity_time = datetime.now()

            # Message processing loop with timeout check
            while True:
                try:
                    # Add receive message timeout with asyncio.wait_for
                    message = await asyncio.wait_for(
                        websocket.receive_text(),
                        timeout=30.0,  # 30 second timeout for periodic idle time check
                    )

                    # Update last activity time
                    last_activity_time = datetime.now()

                    try:
                        message_data = json.loads(message)
                        message_type = message_data.get("type")

                        logging.info(f"Received message type: {message_type}")

                        # Handle different types of messages
                        # Note: use connection_id for WebSocket operations, but pass user_id for business logic
                        if message_type == "upload_start":
                            # Add real user_id to message_data for file storage
                            message_data["_real_user_id"] = str(user_id)
                            await websocket_file_upload_manager.handle_upload_start(connection_id, message_data)
                        elif message_type == "upload_chunk":
                            await websocket_file_upload_manager.handle_file_chunk(connection_id, message_data)
                        elif message_type == "upload_end":
                            message_data["_real_user_id"] = str(user_id)
                            await websocket_file_upload_manager.handle_upload_end(connection_id, message_data)
                        elif message_type == "ping":
                            # Send pong directly via current websocket, not through manager
                            # This avoids issues when user is not in active_connections
                            await websocket.send_text(json.dumps({
                                "type": "pong",
                                "timestamp": datetime.now().isoformat(),
                            }))
                            logging.info(f"Sent pong to user {user_id}")
                        elif message_type == "get_status":
                            message_id = message_data.get("messageId")
                            if message_id:
                                status = await websocket_file_upload_manager.get_upload_status(connection_id, message_id)
                                await websocket_file_upload_manager.send_message(connection_id, status)
                        else:
                            logging.warning(f"Unknown message type: {message_type}")

                    except json.JSONDecodeError:
                        logging.error(f"Invalid JSON message: {message}")
                        await websocket_file_upload_manager.send_message(
                            connection_id,
                            {"type": "error", "message": "Invalid JSON message format"},
                        )

                except asyncio.TimeoutError:
                    # Receive timeout, check if idle time limit exceeded
                    current_time = datetime.now()
                    idle_seconds = (current_time - last_activity_time).total_seconds()

                    # Dynamically select timeout: use longer timeout when there are active uploads
                    # Use connection_id to check uploads for this specific connection
                    has_active_uploads = websocket_file_upload_manager.has_active_uploads(connection_id)
                    timeout_threshold = WEBSOCKET_UPLOAD_TIMEOUT if has_active_uploads else WEBSOCKET_IDLE_TIMEOUT
                    timeout_type = "upload" if has_active_uploads else "idle"
                    timeout_minutes = timeout_threshold // 60
                    active_uploads_count = websocket_file_upload_manager.get_active_uploads_count(connection_id)

                    if idle_seconds >= timeout_threshold:
                        logging.info(f"⏰ [DataService] File upload WebSocket {timeout_type} timeout ({idle_seconds:.1f}s/{timeout_threshold}s) for user {user_id} connection {connection_id} (active uploads: {active_uploads_count}), closing connection")

                        # Check if WebSocket connection is still active before sending notification
                        try:
                            if websocket.client_state.value == 1:  # OPEN state
                                await websocket_file_upload_manager.send_message(
                                    connection_id,
                                    {
                                        "type": "connection_timeout",
                                        "message": f"Connection closed due to {timeout_minutes} minutes of inactivity (timeout type: {timeout_type})",
                                        "idle_seconds": idle_seconds,
                                        "timeout_type": timeout_type,
                                        "active_uploads_count": active_uploads_count,
                                    },
                                )
                            else:
                                logging.debug(f"⚠️ [DataService] WebSocket already closed for user {user_id}, skipping timeout notification")
                        except Exception as send_error:
                            logging.debug(f"⚠️ [DataService] Failed to send timeout notification to user {user_id}: {send_error}")

                        # Ensure connection is closed
                        try:
                            if websocket.client_state.value == 1:  # OPEN state
                                await websocket.close(code=1000, reason="Idle timeout")
                        except Exception as close_error:
                            logging.debug(f"⚠️ [DataService] Error closing WebSocket for user {user_id}: {close_error}")
                        break
                    else:
                        # Not timeout yet, continue listening
                        logging.debug(f"🕐 [DataService] File upload WebSocket {timeout_type} check for user {user_id}: {idle_seconds:.1f}s/{timeout_threshold}s (active uploads: {active_uploads_count})")

                except WebSocketDisconnect:
                    logging.info(f"WebSocket connection normally disconnected: user_id={user_id}")
                    break
                except Exception as e:
                    logging.error(f"WebSocket message processing exception: {e}", stack_info=True)
                    break

        except Exception as e:
            logging.error(f"WebSocket connection exception: {e}", stack_info=True)
        finally:
            # Clean up connection using connection_id
            try:
                if connection_id:
                    await websocket_file_upload_manager.disconnect(connection_id)
                    logging.info(f"🔌 WebSocket file upload connection disconnected: user_id={user_id}, connection_id={connection_id}")
            except NameError:
                # connection_id not defined (token verification failed before connection_id was set)
                pass


@router.get("/api/v1/data/data-distribution")
async def get_data_distribution(
    user_id: Optional[str] = Query(None, description="User ID"),
    current_user: str = Depends(verify_token),
) -> JSONResponse:
    """
    Get user data distribution

    Args:
        user_id: Optional user ID, if not provided, use current logged-in user
        current_user: Current logged-in user info

    Returns:
        User data distribution info
    """
    try:
        # If user_id not provided, use current logged-in user's ID
        target_user_id = user_id or current_user

        if not target_user_id:
            return JSONResponse(
                content={"code": -1, "msg": "Empty user ID"},
            )

        logging.info(f"Get data distribution: user_id={target_user_id}")

        # Call service to get data distribution
        result = await my_data_service.get_user_data_distribution(target_user_id)

        return JSONResponse(
            content={"code": 0, "msg": "ok", "data": result},
        )

    except Exception as e:
        logging.error(f"Failed to get data distribution: {str(e)}", stack_info=True)
        return JSONResponse(
            content={"code": -2, "msg": str(e)},
        )


@router.get("/api/v1/data/uploaded-files")
async def get_uploaded_files(
    target_user_id: Optional[str] = Query(None, description="Target user ID - view files uploaded for which user"),
    limit: Optional[int] = Query(100, description="Maximum number of files to return"),
    offset: Optional[int] = Query(0, description="Pagination offset"),
    current_user: str = Depends(verify_token),
) -> JSONResponse:
    """
    Get user's uploaded file history from th_files table.
    
    Data source: th_files table (migrated from th_messages)
    
    Logic:
    - Files are stored in th_files table with user_id field
    - target_user_id is empty: Return current user's own files
    - target_user_id has value: Return specified user's files (if authorized)

    Args:
        target_user_id: Target user ID, if not provided, return own files
        limit: Maximum number of files to return
        offset: Pagination offset
        current_user: Current logged-in user info

    Returns:
        User uploaded file list
    """

    try:
        logging.info(f"Query uploaded files: current_user={current_user}, target_user_id={target_user_id}")

        if target_user_id and target_user_id != str(current_user):
            permission_check = await get_query_user_id(
                user_id=target_user_id,
                query_user_id=str(current_user),
                permission=["uploadfile"]
            )
            if not permission_check.get("success", False):
                return JSONResponse(
                    content={"code": -2, "msg": "No permission to query this file"}
                )

        # Use database service to get uploaded files
        # Pass current_user for permission checking and target_user_id to determine which user's files to query
        result = await FileParserDatabaseService.get_uploaded_files_paginated(
            uploader_user_id=str(current_user),  # For permission checking
            target_user_id=target_user_id,       # Determines which user's files to query
            limit=limit,
            offset=offset,
        )

        return JSONResponse(
            content={"code": 0, "msg": "ok", "data": result},
        )

    except Exception as e:
        logging.error(f"Failed to get uploaded file history: {str(e)}", stack_info=True)
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to get uploaded files: {str(e)}"},
        )


@router.post("/files/upload", response_model=FileUploadResponse)
async def upload_files(
    request: Request,
    files: List[UploadFile] = File(..., description="Files to upload (PDF, images, documents, etc.)"),
    user_id: str = Depends(verify_token),
    folder: Optional[str] = Query(None, description="Custom folder prefix for uploaded files, defaults to 'uploads'")
) -> FileUploadResponse:
    """
    Upload multiple files directly to S3
    
    This endpoint uploads multiple files directly to S3 without storing metadata in database.
    Supports various file formats including PDF, images, and documents.
    Uses the universal upload_files_to_storage service for cross-project compatibility.
    
    Args:
        files: List of files to upload
        user_id: User authentication data
        folder_prefix: Custom folder prefix for uploaded files (optional, defaults to 'uploads')
        
    Returns:
        FileUploadResponse: Standard response with code, msg, and data fields.
        - code: 0 for success, 1 for partial/complete failure
        - msg: Response message
        - data: List of upload results, each containing file URL, file key, size, type, and timestamp
    """
    # Get Redis client from global app state (shared across all requests)
    redis_client = getattr(request.app.state, 'redis', None)
    
    # Use the universal upload service
    result = await upload_files_to_storage(
        files=files,
        user_id=user_id,
        folder_prefix=folder,
        redis_client=redis_client
    )
    
    # Convert result to FastAPI response format
    return FileUploadResponse(
        code=result["code"],
        msg=result["msg"],
        data=result["data"]
    )


@router.post("/api/v1/data/delete-files", response_model=FileDeleteResponse)
async def delete_uploaded_files(
    request: FileDeleteRequest,
    user_id: str = Depends(verify_token)
) -> FileDeleteResponse:
    """
    Delete uploaded files from th_files table.
    
    Data source: th_files table (migrated from th_messages)
    
    This endpoint allows deletion of specific files or all files by source_id.
    - If file_keys is provided, only those specific files will be soft deleted
    - If file_keys is None/empty, all files with the source_id will be deleted
    - Files are soft deleted (is_del = true) in th_files table
    
    Args:
        request: FileDeleteRequest containing message_id (source_id) and optional file_keys
        user_id: User authentication data
        
    Returns:
        FileDeleteResponse: Response containing deletion results
    """
    try:
        logging.info(f"File deletion request: message_id={request.message_id}, file_keys={request.file_keys}, user_id={user_id}")
        
        # Validate input
        if not request.message_id:
            return FileDeleteResponse(
                code=1,
                msg="Message ID is required",
                data=None
            )

                
        # Filter out empty strings from file_keys
        valid_file_keys = [key for key in (request.file_keys or []) if key and key.strip()]
        
        # Determine if deleting all files or specific ones
        if not valid_file_keys:
            # Delete all files from the message
            result = await delete_all_files_from_message(
                message_id=request.message_id,
                user_id=user_id
            )
        else:
            # Delete specific files
            result = await delete_files_from_message(
                message_id=request.message_id,
                file_keys=valid_file_keys,
                user_id=user_id
            )
        
        # Check result and return appropriate response
        if not result.get("success"):
            return FileDeleteResponse(
                code=1,
                msg=result.get("error", "Failed to delete files"),
                data=result
            )
        
        # Successful deletion
        deleted_count = len(result.get("deleted_files", []))
        failed_count = len(result.get("failed_deletions", []))
        message_deleted = result.get("message_deleted", False)
        
        if message_deleted:
            msg = f"Successfully deleted all {deleted_count} files. Message marked as deleted."
        elif failed_count > 0:
            msg = f"Partial deletion: {deleted_count} files deleted, {failed_count} failed"
        else:
            msg = f"Successfully deleted {deleted_count} file(s)"
        
        return FileDeleteResponse(
            code=0,
            msg=msg,
            data=result
        )
        
    except Exception as e:
        logging.error(f"Error in delete_uploaded_files endpoint: {str(e)}", stack_info=True)
        return FileDeleteResponse(
            code=1,
            msg=f"Internal server error: {str(e)}",
            data=None
        )


@router.websocket("/ws/upload-with-llm-analysis")
async def websocket_upload_with_llm_analysis(
    websocket: WebSocket, 
    token: str = Query(..., description="Authentication token")
):
    """
    Dedicated WebSocket endpoint for file upload with LLM analysis
    
    Supports complete file upload and LLM analysis workflow with real-time progress updates.
    """
    user_id = None
    trace_id = str(uuid.uuid4())
    
    # Set request context
    ctx = {
        "trace_id": trace_id,
        "connection_type": "websocket", 
        "endpoint": "/ws/upload-with-llm-analysis",
    }
    
    with set_req_ctx(ctx):
        try:
            # Verify token
            try:
                user_id = await verify_token_string(token)
                if not user_id:
                    await websocket.close(code=1008, reason="Invalid token")
                    return
            except Exception as e:
                logging.error(f"Token verification failed: {e}")
                await websocket.close(code=1008, reason="Token verification failed")
                return

            logging.info(f"🔗 WebSocket LLM analysis connection established: user_id={user_id}")

            # Accept connection
            await websocket.accept()
            
            # Send connection established message
            await websocket.send_text(json.dumps({
                "type": "connection_established",
                "message": "WebSocket connection established for file upload and LLM analysis",
                "timestamp": datetime.now().isoformat()
            }))

            # Message processing loop
            last_activity_time = datetime.now()
            
            while True:
                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(
                        websocket.receive_text(),
                        timeout=30.0
                    )
                    
                    last_activity_time = datetime.now()
                    
                    try:
                        message_data = json.loads(message)
                        message_type = message_data.get("type")
                        
                        logging.info(f"Received LLM WebSocket message type: {message_type}")
                        
                        if message_type == "upload_files":
                            # Handle file upload with LLM analysis
                            await handle_websocket_upload_files(websocket, str(user_id), message_data)
                            
                        elif message_type == "ping":
                            # Handle ping
                            await websocket.send_text(json.dumps({
                                "type": "pong",
                                "timestamp": datetime.now().isoformat()
                            }))
                        else:
                            await websocket.send_text(json.dumps({
                                "type": "error",
                                "message": f"Unknown message type: {message_type}",
                                "timestamp": datetime.now().isoformat()
                            }))
                    
                    except json.JSONDecodeError:
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "message": "Invalid JSON message format",
                            "timestamp": datetime.now().isoformat()
                        }))
                
                except asyncio.TimeoutError:
                    # Check for idle timeout
                    current_time = datetime.now()
                    idle_seconds = (current_time - last_activity_time).total_seconds()
                    
                    # 10 minute idle timeout
                    if idle_seconds >= 600:
                        await websocket.send_text(json.dumps({
                            "type": "timeout",
                            "message": "Connection closed due to inactivity",
                            "timestamp": datetime.now().isoformat()
                        }))
                        break
                        
                except WebSocketDisconnect:
                    logging.info(f"WebSocket LLM connection disconnected: user_id={user_id}")
                    break
                except Exception as e:
                    logging.error(f"WebSocket LLM error: {e}")
                    break
                    
        except Exception as e:
            logging.error(f"WebSocket LLM connection error: {e}")
        finally:
            if user_id:
                logging.info(f"🔌 WebSocket LLM connection closed: user_id={user_id}")


async def handle_websocket_upload_files(websocket: WebSocket, user_id: str, message_data: Dict):
    """Handle file upload and LLM analysis through dedicated WebSocket"""
    try:
        # Extract message data
        files_data = message_data.get("files", [])
        context = message_data.get("context")
        language = message_data.get("language", "zh")
        
        if not files_data:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "No files provided for upload and analysis",
                "timestamp": datetime.now().isoformat()
            }))
            return
        
        logging.info(f"Starting dedicated WebSocket file upload with LLM analysis for user {user_id}, files: {len(files_data)}")
        
        # Send acknowledgment
        await websocket.send_text(json.dumps({
            "type": "upload_started",
            "message": f"Starting upload and analysis of {len(files_data)} files",
            "total_files": len(files_data),
            "timestamp": datetime.now().isoformat()
        }))
        
        # Generate session info
        msg_id = f"ws_upload_{uuid.uuid4().hex}"
        upload_time = datetime.now()
        
        # Convert base64 files data to bytes
        processed_files_data = []
        for i, file_data in enumerate(files_data):
            try:
                filename = file_data.get("filename", f"file_{i}")
                content_b64 = file_data.get("content", "")
                content_type = file_data.get("content_type", "application/octet-stream")
                
                # Decode base64 content
                import base64
                file_content = base64.b64decode(content_b64)
                
                processed_files_data.append({
                    "filename": filename,
                    "content": file_content,
                    "content_type": content_type
                })
                
                # Send progress update
                await websocket.send_text(json.dumps({
                    "type": "file_processed",
                    "file_index": i,
                    "filename": filename,
                    "message": f"File {filename} processed",
                    "timestamp": datetime.now().isoformat()
                }))
                
            except Exception as file_error:
                await websocket.send_text(json.dumps({
                    "type": "file_error",
                    "file_index": i,
                    "filename": file_data.get("filename", f"file_{i}"),
                    "error": str(file_error),
                    "timestamp": datetime.now().isoformat()
                }))
                continue
        
        if not processed_files_data:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "No files could be processed",
                "timestamp": datetime.now().isoformat()
            }))
            return
        
        # Import services and create mock files
        from mirobody.pulse.file_parser.services.file_processing_service import (
            process_file_uploads, save_upload_message, process_single_file_upload
        )
        from io import BytesIO
        
        class MockUploadFile:
            def __init__(self, filename: str, content: bytes, content_type: str):
                self.filename = filename
                self.content_type = content_type
                self.file = BytesIO(content)
                
            async def read(self):
                self.file.seek(0)
                return self.file.read()
                
            async def seek(self, offset):
                return self.file.seek(offset)
        
        mock_files = [
            MockUploadFile(file_data["filename"], file_data["content"], file_data["content_type"])
            for file_data in processed_files_data
        ]
        
        # Process file uploads
        await websocket.send_text(json.dumps({
            "type": "upload_progress",
            "message": "Uploading files to cloud storage",
            "timestamp": datetime.now().isoformat()
        }))
        
        upload_result = await process_file_uploads(
            files=mock_files,
            user_id=user_id,
            upload_time=upload_time,
            process_single_file_func=process_single_file_upload
        )
        
        successful_count = len(upload_result["successful_uploads"])
        failed_count = len(upload_result["failed_uploads"])
        
        # Send upload completed
        await websocket.send_text(json.dumps({
            "type": "upload_completed",
            "message": f"Upload completed: {successful_count} successful, {failed_count} failed",
            "successful_files": successful_count,
            "failed_files": failed_count,
            "timestamp": datetime.now().isoformat()
        }))
        
        if successful_count == 0:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "All files failed to upload",
                "timestamp": datetime.now().isoformat()
            }))
            return
        
        # Save to database
        session_id = f"ws_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        db_result = await save_upload_message(
            msg_id=msg_id,
            user_id=user_id,
            session_id=session_id,
            upload_result=upload_result,
            successful_count=successful_count,
            failed_count=failed_count,
            total_files=len(processed_files_data)
        )
        
        if not db_result:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "Files uploaded but failed to save to database",
                "timestamp": datetime.now().isoformat()
            }))
            return
        
        # Start LLM analysis
        await websocket.send_text(json.dumps({
            "type": "analysis_started",
            "message": f"Starting LLM analysis of {successful_count} files",
            "msg_id": msg_id,
            "timestamp": datetime.now().isoformat()
        }))
        
        # Perform LLM analysis
        try:
            from mirobody.pulse.file_parser.services.file_llm_analyzer import analyze_files_with_extraction
            from mirobody.utils.config import safe_read_cfg
            
            is_aliyun = safe_read_cfg("CLUSTER") == "ALIYUN"
            analysis_provider = "doubao-lite" if is_aliyun else "openai"
            
            # Send analysis progress
            for i, file_data in enumerate(upload_result["files_data_for_processing"]):
                await websocket.send_text(json.dumps({
                    "type": "analysis_progress",
                    "file_index": i,
                    "filename": file_data.get("filename", "Unknown"),
                    "message": f"Analyzing: {file_data.get('filename', 'Unknown')}",
                    "timestamp": datetime.now().isoformat()
                }))
            
            # Perform analysis
            analysis_response = await analyze_files_with_extraction(
                files_data=upload_result["files_data_for_processing"],
                user_id=user_id,
                msg_id=msg_id,
                provider=analysis_provider,
                context=context,
                language=language
            )
            
            if analysis_response.get("success"):
                await websocket.send_text(json.dumps({
                    "type": "analysis_completed",
                    "message": "LLM analysis completed successfully",
                    "msg_id": msg_id,
                    "analysis_result": {
                        "summary": analysis_response.get("summary", ""),
                        "analysis": analysis_response.get("analysis", ""),
                        "recommendations": analysis_response.get("recommendations", []),
                        "key_points": analysis_response.get("key_points", []),
                        "concerns": analysis_response.get("concerns", []),
                        "file_relationships": analysis_response.get("file_relationships", [])
                    },
                    "timestamp": datetime.now().isoformat()
                }))
            else:
                await websocket.send_text(json.dumps({
                    "type": "analysis_failed",
                    "message": "LLM analysis failed",
                    "error": analysis_response.get("error", "Unknown error"),
                    "timestamp": datetime.now().isoformat()
                }))
                
        except Exception as analysis_error:
            await websocket.send_text(json.dumps({
                "type": "analysis_error",
                "message": "LLM analysis encountered an error",
                "error": str(analysis_error),
                "timestamp": datetime.now().isoformat()
            }))
        
    except Exception as e:
        logging.error(f"Error in dedicated WebSocket upload handler: {str(e)}")
        await websocket.send_text(json.dumps({
            "type": "error",
            "message": f"Upload and analysis failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }))

