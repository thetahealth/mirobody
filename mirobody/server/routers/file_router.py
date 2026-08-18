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
from mirobody.server.auth import verify_token, verify_token_string
from mirobody.utils.permissions import get_query_user_id

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
from mirobody.utils.log import secret_fingerprint

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
    Proxy files from storage (S3/OSS) through backend.
    
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
        content, err = await storage.get(file_path)
        if err:
            logging.warning(err)
        
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
                logging.error(
                    "Token verification failed: %s", e,
                    extra={"token": secret_fingerprint(token)},
                )
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
        folder: Custom folder prefix for uploaded files (optional, defaults to 'uploads')
        
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


