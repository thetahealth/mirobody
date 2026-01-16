"""
WebSocket routes for data_server with file upload progress and real-time communication
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import Request

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from mirobody.utils.req_ctx import set_req_ctx
from mirobody.utils.utils_auth import verify_token, verify_token_from_websocket, verify_token_string
from mirobody.utils.utils_user import get_query_user_id
from mirobody.utils.distributed_websocket import get_distributed_ws_manager as get_file_progress_manager

from mirobody.pulse.file_parser.file_upload_manager import get_websocket_file_upload_manager
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.list_my_data import MyDataService

# Additional imports for async file processing
from mirobody.pulse.file_parser.services.async_file_processor import AsyncFileProcessor
from mirobody.pulse.file_parser.services.file_processing_service import (
    process_file_uploads, 
    save_upload_message,
    process_single_file_upload,
    process_files_async,
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

# === Pydantic Models for Health Data Management ===


class HealthDataRequest(BaseModel):
    message_type: str
    content: dict
    session_id: Optional[str] = None
    notes: Optional[str] = None


class HealthRecordData(BaseModel):
    indicator: str
    value: float
    unit: str
    source: str = "manual_input"
    timestamp: Optional[str] = None


class HealthRecordRequest(BaseModel):
    records: List[HealthRecordData]
    session_id: Optional[str] = None
    template_name: Optional[str] = None

class FileUploadResponse(BaseModel):
    """File upload response model"""
    
    code: int
    msg: str
    data: Optional[List[FileUploadData]]


class FileUploadWithProcessingResponse(BaseModel):
    """File upload with processing response model"""
    
    code: int
    msg: str
    data: Optional[Dict[str, Any]]


class FileUploadWithLLMAnalysisResponse(BaseModel):
    """File upload with LLM analysis response model"""
    
    code: int
    msg: str
    data: Optional[Dict[str, Any]]
    llm_analysis: Optional[Dict[str, Any]] = None


class FileDeleteRequest(BaseModel):
    """File deletion request model"""
    
    message_id: str
    file_keys: Optional[List[str]] = None  # If None, delete all files


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


# === Health Data Management Routes ===


@router.post("/data/health/data")
async def save_health_data(
    request: HealthDataRequest,
    current_user: str = Depends(verify_token),
) -> JSONResponse:
    """
    Save health management data to theta_ai.th_messages table

    Supported message_types:
    - exercise_record: Exercise tracking data
    - medication_plan: Medication management data
    - health_assessment: Health test results
    """
    try:
        logging.info(f"Received request: message_type={request.message_type}, user={current_user}")

        # Validate message_type
        valid_types = ["exercise_record", "medication_plan", "health_assessment"]
        if request.message_type not in valid_types:
            return JSONResponse(
                content={
                    "code"  : -1,
                    "msg"   : f"Invalid message_type. Must be one of: {', '.join(valid_types)}"
                },
            )

        # Generate unique IDs for the message and session
        message_id = str(uuid.uuid4())
        session_id = (
            request.session_id or f"health_{request.message_type}_{current_user}_{datetime.now().strftime('%Y%m%d')}"
        )

        # Validate content is not empty
        if not request.content:
            return JSONResponse(
                content={
                    "code"  : -2,
                    "msg"   : "Content cannot be empty"
                },
            )

        logging.info(f"Calling log_chat_message with message_id={message_id}")
        # Save to theta_ai.th_messages table
        await FileParserDatabaseService.log_chat_message(
            id=message_id,
            user_id=current_user,
            session_id=session_id,
            role="user",
            content=json.dumps(request.content, ensure_ascii=False),
            reasoning=request.notes or "",
            agent="health_management",
            provider="frontend",
            user_name=current_user,
            message_type=request.message_type,
        )

        logging.info(f"Successfully saved health data for user {current_user}, type: {request.message_type}")

        return JSONResponse(
            content={
                "code"  : 0,
                "msg"   : "Health data saved successfully",
                "data"  : {
                    "message_id"    : message_id,
                    "session_id"    : session_id,
                    "message_type"  : request.message_type,
                    "saved"         : True,
                }
            },
        )

    except Exception as e:
        error_msg = str(e)
        logging.error(f"❌ Failed to save health data: {error_msg}", stack_info=True)

        return JSONResponse(
            content={
                "code"  : -3,
                "msg"   : error_msg,
                "data"  : {
                    "message_type"  : request.message_type,
                    "user_id"       : current_user,
                    "has_content"   : bool(request.content),
                },
            },
        )


@router.get("/data/health/data/{message_type}")
async def get_health_data(
    message_type: str,
    current_user: str = Depends(verify_token),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 100,
) -> JSONResponse:
    """
    Retrieve health management data from theta_ai.th_messages table
    """
    try:
        # Use database service function
        health_data = await FileParserDatabaseService.get_health_data_by_type(
            user_id=current_user,
            message_type=message_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        return JSONResponse(
            content={
                "code"  : 0,
                "msg"   : "Health data retrieved successfully",
                "data"  : {
                    "message_type"  : message_type,
                    "records"       : health_data,
                    "count"         : len(health_data),
                }
            }
        )

    except Exception as e:
        logging.error(f"Error retrieving health data: {str(e)}", stack_info=True)

        return JSONResponse(
            content={
                "code"  : -1,
                "msg"   : str(e),
            }
        )


@router.get("/data/health/series-data")
async def get_health_series_data(
    start_time: Optional[str] = Query(None, description="Start time in ISO format"),
    end_time: Optional[str] = Query(None, description="End time in ISO format"),
    indicators: Optional[str] = Query(None, description="Comma-separated list of indicators"),
    limit: int = Query(1000, description="Maximum number of records to return"),
    group_by: str = Query("day", description="Group by: day, hour, week"),
    user_id: str = Depends(verify_token),
):
    """
    Get health series data from th_series_data table

    Args:
        start_time: Start time filter
        end_time: End time filter
        indicators: Comma-separated indicator names to filter
        limit: Maximum records to return
        group_by: How to group the data (day, hour, week)
        user_id: Authenticated user ID

    Returns:
        List of health data points
    """
    try:
        # Use database service function
        data = await FileParserDatabaseService.get_health_series_data_by_filters(
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            indicators=indicators,
            limit=limit,
        )

        return {
            "code"  : 0,
            "msg"   : f"Successfully retrieved {len(data)} health data points",
            "data"  : data
        }

    except ValueError as e:
        # Handle specific validation errors (like invalid datetime format)
        logging.error(f"Validation error in health series data: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logging.error(f"Error fetching health series data for user {user_id}: {str(e)}", stack_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch health series data: {str(e)}")


@router.post("/data/health/record")
async def save_health_record(
    request: HealthRecordRequest,
    current_user: str = Depends(verify_token),
):
    """Save health record to theta_ai.th_messages table"""
    try:
        # Generate message ID and session ID if not provided
        message_id = str(uuid.uuid4())
        session_id = request.session_id or str(uuid.uuid4())

        # Prepare content
        record_data = {
            "type": "health_record",
            "template_name": request.template_name,
            "records": [record.dict() for record in request.records],
            "timestamp": datetime.now().isoformat(),
        }

        content = json.dumps(record_data, ensure_ascii=False)

        # Save to theta_ai.th_messages
        await FileParserDatabaseService.log_chat_message(
            id=message_id,
            user_id=current_user,
            session_id=session_id,
            role="user",
            content=content,
            agent="health_record_system",
            message_type="health_record",
        )

        return {
            "code"  : 0,
            "msg"   : "Health record saved successfully",
            "data"  : {
                "id"            : message_id,
                "session_id"    : session_id,
                "records_count" : len(request.records),
            },
        }

    except Exception as e:
        logging.error(f"Error saving health record: {str(e)}", stack_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to save health record: {str(e)}")


@router.get("/data/health/records")
async def get_health_records(
    limit: int = Query(50, description="Maximum number of records to return"),
    offset: int = Query(0, description="Offset for pagination"),
    current_user: str = Depends(verify_token),
):
    """Get health records from theta_ai.th_messages table"""
    try:
        # Use database service function
        records = await FileParserDatabaseService.get_health_records_paginated(
            user_id=current_user,
            limit=limit,
            offset=offset,
        )

        return {
            "code"  : 0,
            "msg"   : "Health records retrieved successfully",
            "data"  : {
                "records"   : records,
                "total"     : len(records)
            }
        }

    except Exception as e:
        logging.error(f"Error getting health records: {str(e)}", stack_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get health records: {str(e)}")


# === WebSocket Routes ===


@router.websocket("/api/ws/file-progress")
async def websocket_file_progress(websocket: WebSocket):
    """WebSocket endpoint for file processing progress"""
    user_id = None
    # Generate unique trace_id for this WebSocket connection
    trace_id = str(uuid.uuid4())

    # Set request context with trace_id for the entire WebSocket session
    ctx = {
        "trace_id": trace_id,
        "connection_type": "websocket",
        "endpoint": "/api/ws/file-progress",
    }

    with set_req_ctx(ctx):
        try:
            # Accept connection first
            await websocket.accept()
            logging.info("🔗 [DataService] WebSocket file-progress connection accepted")

            # Send connection confirmation message
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "connection_status",
                        "status": "accepted",
                        "message": "WebSocket connection accepted, verifying token...",
                    },
                    ensure_ascii=False,
                )
            )

            # Then verify token
            try:
                user_id = await verify_token_from_websocket(websocket)
            except Exception as e:
                logging.error(f"WebSocket token verification failed: {e}, token: {websocket.headers.get('Authorization')}")
                await websocket.close(code=1008, reason="Invalid token")
                return

            if not user_id:
                logging.error("❌ [DataService] WebSocket token verification failed")
                # Send token verification failed message
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "connection_status",
                            "status": "error",
                            "message": "Token verification failed",
                            "error": "authentication_failed",
                        },
                        ensure_ascii=False,
                    )
                )
                await websocket.close(code=1008, reason="Invalid token")
                return

            logging.info(f"✅ [DataService] File progress WebSocket connected for user {user_id}")

            # Register connection to global manager for progress updates
            file_progress_manager = get_file_progress_manager()
            await file_progress_manager.connect(websocket, user_id)

            # Send successful connection confirmation message
            try:
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "connection_status",
                            "status": "connected",
                            "user_id": user_id,
                            "message": "WebSocket connection established successfully",
                            "server": "data_service",  # Identify as from data_service
                        },
                        ensure_ascii=False,
                    )
                )
                logging.info(f"📤 [DataService] Sent connection confirmation to user {user_id}")
            except Exception as send_error:
                logging.warning(f"⚠️ [DataService] Failed to send connection confirmation: {send_error}")

            # Add server-side timeout mechanism
            last_activity_time = datetime.now()

            try:
                # Keep connection alive with timeout check
                while True:
                    try:
                        # Add receive message timeout with asyncio.wait_for
                        message = await asyncio.wait_for(
                            websocket.receive_text(),
                            timeout=30.0,  # 30 second timeout for periodic idle time check
                        )

                        # Update last activity time
                        last_activity_time = datetime.now()

                        logging.debug(f"📨 [DataService] Received WebSocket message from user {user_id}: {message}")

                        # Handle client control messages like cancel file processing
                        if message == "ping":
                            await websocket.send_text("pong")
                            logging.debug(f"🏓 [DataService] Sent pong to user {user_id}")
                        elif message.startswith("{"):
                            # Try to parse JSON message
                            try:
                                msg_data = json.loads(message)
                                if msg_data.get("type") == "heartbeat":
                                    await websocket.send_text(
                                        json.dumps(
                                            {
                                                "type": "heartbeat_response",
                                                "timestamp": msg_data.get("timestamp"),
                                                "server_time": datetime.now().isoformat(),
                                                "server": "data_service",
                                            }
                                        )
                                    )
                                    logging.debug(f"💓 [DataService] Sent heartbeat response to user {user_id}")
                            except json.JSONDecodeError:
                                logging.debug(f"⚠️ [DataService] Received non-JSON message from user {user_id}: {message}")

                    except asyncio.TimeoutError:
                        # Receive timeout, check if idle time limit exceeded
                        current_time = datetime.now()
                        idle_seconds = (current_time - last_activity_time).total_seconds()

                        if idle_seconds >= WEBSOCKET_IDLE_TIMEOUT:
                            logging.info(f"⏰ [DataService] WebSocket idle timeout ({idle_seconds:.1f}s) for user {user_id}, closing connection")

                            # Check if WebSocket connection is still active before sending notification
                            try:
                                if websocket.client_state.value == 1:  # OPEN state
                                    await websocket.send_text(
                                        json.dumps(
                                            {
                                                "type": "connection_status",
                                                "status": "timeout",
                                                "message": f"Connection closed due to {WEBSOCKET_IDLE_TIMEOUT // 60} minutes of inactivity",
                                                "idle_seconds": idle_seconds,
                                            }
                                        )
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
                            logging.debug(f"🕐 [DataService] WebSocket idle check for user {user_id}: {idle_seconds:.1f}s/{WEBSOCKET_IDLE_TIMEOUT}s")

                    except Exception as e:
                        logging.debug(f"[DataService] WebSocket receive error (normal on disconnect): {e}")
                        break

            except Exception as e:
                logging.error(f"[DataService] WebSocket error for user {user_id}: {str(e)}", stack_info=True)

        except Exception as e:
            logging.error(f"[DataService] WebSocket connection error: {str(e)}", stack_info=True)
        finally:
            # Cleanup on disconnect
            if user_id:
                file_progress_manager = get_file_progress_manager()
                await file_progress_manager.disconnect(user_id)
                logging.info(f"🔌 [DataService] File progress WebSocket disconnected for user {user_id}")


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


@router.websocket("/api/ws/chat")
async def websocket_chat(websocket: WebSocket):
    """
    Chat WebSocket endpoint - proxy to main service
    """
    user_id = None
    # Generate unique trace_id for this WebSocket connection
    trace_id = str(uuid.uuid4())

    # Set request context with trace_id for the entire WebSocket session
    ctx = {
        "trace_id": trace_id,
        "connection_type": "websocket",
        "endpoint": "/api/ws/chat",
    }

    with set_req_ctx(ctx):
        try:
            # Accept connection first
            await websocket.accept()
            logging.info("🔗 [DataService] Chat WebSocket connection accepted")

            # Verify token
            user_id = await verify_token_from_websocket(websocket)
            if not user_id:
                await websocket.close(code=1008, reason="Invalid token")
                return

            logging.info(f"✅ [DataService] Chat WebSocket connected for user {user_id}")

            # Current simple implementation: keep connection and handle heartbeat
            while True:
                try:
                    message = await websocket.receive_text()
                    logging.debug(f"📨 [DataService] Chat message from user {user_id}: {message}")

                    # Handle heartbeat
                    if message == "ping":
                        await websocket.send_text("pong")
                    elif message.startswith("{"):
                        try:
                            msg_data = json.loads(message)
                            if msg_data.get("type") == "heartbeat":
                                await websocket.send_text(
                                    json.dumps(
                                        {
                                            "type": "heartbeat_response",
                                            "timestamp": msg_data.get("timestamp"),
                                            "server_time": datetime.now().isoformat(),
                                            "server": "data_service",
                                        }
                                    )
                                )
                        except json.JSONDecodeError:
                            pass

                except Exception as e:
                    logging.debug(f"[DataService] Chat WebSocket error: {e}")
                    break

        except Exception as e:
            logging.error(f"[DataService] Chat WebSocket error: {str(e)}", stack_info=True)
        finally:
            if user_id:
                logging.info(f"🔌 [DataService] Chat WebSocket disconnected for user {user_id}")


@router.get("/api/v1/file-progress/{message_id}")
async def get_file_progress(message_id: str, token: str = Query(..., description="Authentication token")):
    """
    REST API for querying file processing progress
    Can get latest progress through this API even if WebSocket connection is disconnected
    """
    # Generate unique trace_id for this REST API request
    trace_id = str(uuid.uuid4())

    # Set request context with trace_id for this API request
    ctx = {
        "trace_id": trace_id,
        "connection_type": "http",
        "endpoint": "/api/v1/file-progress",
    }

    with set_req_ctx(ctx):
        try:
            # Verify token
            user_id = await verify_token_string(token)
            if not user_id:
                raise HTTPException(status_code=401, detail="Invalid token")

            logging.info(f"📊 [REST API] Query file progress: user_id={user_id}, message_id={message_id}")

            try:
                message_details = await FileParserDatabaseService.get_message_details(message_id)

                if not message_details:
                    return JSONResponse(
                        status_code=404,
                        content={
                            "code"  : -1,
                            "msg"   : "Message not found",
                            "data"  : {
                                "message_id": message_id
                            }
                        },
                    )

                content = message_details.get("content", "")

                try:
                    if isinstance(content, str):
                        content_data = json.loads(content)
                    else:
                        content_data = content

                    # Build progress response
                    response_data = {
                        "code"  : 0,
                        "msg"   : content_data.get("message", ""),
                        "data"  : {
                            "message_id"    : message_id,
                            "status"        : content_data.get("status", "unknown"),
                            "progress"      : content_data.get("progress", 0),
                            "files"         : content_data.get("files", []),
                            "processing_complete" : content_data.get("processing_complete", False),
                            "processing_failed" : content_data.get("processing_failed", False),
                            "loaded_records"    : content_data.get("loaded_records", 0),
                            "progress_details"  : content_data.get("progress_details", {}),
                            "timestamp"     : message_details.get("created_at", ""),
                            "last_update"   : content_data.get("timestamp", "")
                        }
                    }

                    logging.info(f"✅ [REST API] Successfully returned file progress: message_id={message_id}, status={response_data['data']['status']}, progress={response_data['data']['progress']}%")

                    return JSONResponse(content=response_data)

                except json.JSONDecodeError:
                    # If content is not JSON format, return basic info
                    response_data = {
                        "code"  : 0,
                        "msg"   : content if isinstance(content, str) else str(content),
                        "data"  : {
                            "message_id": message_id,
                            "status"    : "unknown",
                            "progress"  : 0,
                            "files"     : [],
                            "processing_complete"   : False,
                            "processing_failed"     : False,
                            "loaded_records"        : 0,
                            "progress_details"      : {},
                            "timestamp"     : message_details.get("created_at", ""),
                            "last_update"   : "",
                        }
                    }

                    logging.info(f"⚠️ [REST API] Message content is not JSON format, returning basic info: message_id={message_id}")

                    return JSONResponse(content=response_data)

            except Exception as db_error:
                logging.error(f"❌ [REST API] Database query failed: {db_error}")
                return JSONResponse(
                    content={
                        "code"  : -1,
                        "msg"   : "Database query failed",
                        "data"  : {
                            "message_id": message_id
                        }
                    },
                )

        except Exception as e:
            logging.error(f"❌ [REST API] Query file progress failed: {e}")
            return JSONResponse(
                content={
                    "code"  : -2,
                    "msg"   : str(e),
                    "data"  : {
                        "message_id": message_id
                    }
                }
            )

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
    Get user's uploaded file history
    
    New logic explanation:
    - Files are directly stored under target user ID (th_messages.user_id = target_user_id)
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


@router.delete("/api/v1/data/uploaded-files/{file_id}")
async def delete_uploaded_file(
    file_id: str,
    target_user_id: Optional[str] = Query(None, description="Target user ID - delete file uploaded for which user"),
    current_user: str = Depends(verify_token),
) -> JSONResponse:
    """
    Soft delete uploaded file

    Args:
        file_id: File ID (message ID)
        target_user_id: Target user ID (optional) - if specified, delete file uploaded for this user
        current_user: Current logged-in user info

    Returns:
        Deletion result
    """
    try:
        logging.info(f"Delete file request: file_id={file_id}, user_id={current_user}, target_user_id={target_user_id}")

        # Check permission if deleting file for another user
        if target_user_id and target_user_id != str(current_user):
            # Use get_query_user_id to check if current user has uploadfile write permission (level 2)
            from mirobody.utils.utils_user import get_query_user_id

            permission_check = await get_query_user_id(
                user_id=target_user_id,
                query_user_id=str(current_user),
                permission=["uploadfile"]  # Check uploadfile permission specifically
            )

            if not permission_check.get("success", False):
                logging.warning(f"Permission denied: current_user={current_user}, target_user_id={target_user_id}, error={permission_check.get('error')}")
                return JSONResponse(
                    content={"code": -2, "msg": "No permission to delete this file"}
                )

            # Check if uploadfile permission is 2 (write access required for deletion)
            uploadfile_permission = permission_check.get("permissions", {}).get("uploadfile", 0)
            if uploadfile_permission < 2:
                logging.warning(f"Insufficient permission: current_user={current_user}, target_user_id={target_user_id}, uploadfile_permission={uploadfile_permission} (requires 2)")
                return JSONResponse(
                    content={"code": -2, "msg": "Insufficient permission to delete this file. Write access required."}
                )

            logging.info(f"Permission check passed: current_user={current_user}, target_user_id={target_user_id}, uploadfile_permission={uploadfile_permission}")

        # Use database service to delete uploaded file
        result = await FileParserDatabaseService.delete_uploaded_file(
            file_id=file_id,
            user_id=str(current_user),
            target_user_id=target_user_id,
        )

        if result["success"]:
            return JSONResponse(
                content={"code": 0, "msg": "ok", "data": result.get("data", {})},
            )
        else:
            # Map service error codes to response codes
            if result["code"] == "NOT_FOUND":
                return JSONResponse(content={"code": -1, "msg": "Not found"})
            elif result["code"] == "PERMISSION_DENIED":
                return JSONResponse(content={"code": -2, "msg": "Failed to remove the file"})
            elif result["code"] == "UPDATE_FAILED":
                return JSONResponse(content={"code": -3, "msg": "Not found"})
            else:
                return JSONResponse(content={"code": -4, "msg": result["message"]})

    except Exception as e:
        logging.error(f"Failed to delete file: {str(e)}", stack_info=True)
        return JSONResponse(
            content={"code": -4, "msg": str(e)}
        )


@router.post("/files/upload", response_model=FileUploadResponse)
async def upload_files(
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
    # Use the universal upload service
    result = await upload_files_to_storage(
        files=files,
        user_id=user_id,
        folder_prefix=folder
    )
    
    # Convert result to FastAPI response format
    return FileUploadResponse(
        code=result["code"],
        msg=result["msg"],
        data=result["data"]
    )
    
    
@router.post("/files/upload/synergy")
async def upload_file(
    request: Request,
    localId: Optional[str] = Form(None),
    userIds: str = Form(...),
    orgId: Optional[str] = Form(None),
    traceId: Optional[str] = Header(None),

):
    """
    Upload file to S3 and return URL.
    
    This endpoint is used by synergy's CustomS3Uploader.
    Response format must be: {"files": [{"url": "...", "size": 123}]}
    """
    from fastapi.responses import JSONResponse
    try:
        # Get all form data
        form = await request.form()
        
        # Find file field (exclude known text fields)
        file = None
        
        for field_name, field_value in form.items():
            # Duck typing: UploadFile has 'filename' attribute, str doesn't
            if hasattr(field_value, 'filename'):
                file = field_value
                break
        
        if not file:
            return JSONResponse(
                content={"files": [], "error": "No file provided"},
                status_code=400
            )
        
        result = await upload_files_to_storage(
            files=[file],
            user_id=userIds,
            folder_prefix="workspace"
        )
        
        data = result.get("data", [])
        if not data:
            return JSONResponse(
                content={"files": [], "error": "Upload failed, no data returned"},
                status_code=500
            )
        
        file_res = data[0]
        return {
            "files": [{
                "url": file_res["file_url"],
                "size": file_res["file_size"]
            }]
        }
        
    except Exception as e:
        return JSONResponse(
            content={"files": [], "error": str(e)},
            status_code=500
        )




@router.get("/api/v1/data/health/weekly-stats")
async def get_weekly_health_stats(
    end_date: str = Query(None, description="End date in YYYY-MM-DD format"),
    period: int = Query(7, description="Number of days to look back"),
    current_user: str = Depends(verify_token),
) -> JSONResponse:
    """
    Get weekly health statistics for dashboard display
    Based on the SQL logic from a_get_devide_data function

    Args:
        end_date: End date for the analysis (defaults to today)
        period: Number of days to analyze (defaults to 7)
        current_user: Current authenticated user

    Returns:
        JSONResponse with weekly health statistics
    """
    try:
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Use database service to get weekly health stats
        response_data = await FileParserDatabaseService.get_weekly_health_stats(
            user_id=current_user,
            end_date=end_date,
            period=period,
        )

        return JSONResponse(
            content={"code": 0, "msg": "ok", "data": response_data},
        )

    except Exception as e:
        logging.error(f"Service error occurred: {str(e)}", stack_info=True)
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to get weekly health stats: {str(e)}"},
        )



@router.post("/api/v1/data/upload-with-processing", response_model=FileUploadWithProcessingResponse)
async def upload_files_with_processing(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(..., description="Files to upload and process"),
    user_id: str = Depends(verify_token),
    session_id: str = Form(None)
) -> FileUploadWithProcessingResponse:
    """
    Upload multiple files to S3 and save metadata to database with async processing
    
    This endpoint:
    1. Uploads files to S3
    2. Saves file metadata to th_messages table (one record for all files)
    3. Triggers asynchronous processing to extract health indicators
    
    Args:
        background_tasks: FastAPI background tasks
        files: List of files to upload
        user_id: User authentication data
        session_id: Session ID for grouping related uploads
        
    Returns:
        FileUploadWithProcessingResponse: Response containing upload results and message IDs
    """
    # Check if files list is empty
    if not files:
        return FileUploadWithProcessingResponse(
            code=1,
            msg="No files provided",
            data=None
        )
    
    # Generate session ID if not provided
    if not session_id:
        session_id = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    # Generate single message ID for all files
    msg_id = f"upload_{uuid.uuid4().hex}"
    upload_time = datetime.now()
    
    # Process files using shared service
    logging.info(f"Processing {len(files)} files for user {user_id}")
    
    # Use shared upload processing logic
    upload_result = await process_file_uploads(
        files=files,
        user_id=user_id,
        upload_time=upload_time,
        process_single_file_func=process_single_file_upload
    )
    
    successful_count = len(upload_result["successful_uploads"])
    failed_count = len(upload_result["failed_uploads"])
    total_files = len(files)
    
    if successful_count == 0:
        return FileUploadWithProcessingResponse(
            code=1,
            msg=f"All {total_files} files failed to upload",
            data={
                "session_id": session_id,
                "total_files": total_files,
                "successful_files": 0,
                "failed_files": failed_count,
                "failed_uploads": upload_result["failed_uploads"]
            }
        )
    
    # Save to database using shared service
    db_result = await save_upload_message(
        msg_id=msg_id,
        user_id=user_id,
        session_id=session_id,
        upload_result=upload_result,
        successful_count=successful_count,
        failed_count=failed_count,
        total_files=total_files
    )
    
    if db_result:
        logging.info(f"Files saved to database with msg_id: {msg_id}, total: {successful_count} files")
        
        # Add background task for async processing of all files
        if upload_result["files_data_for_processing"]:
            background_tasks.add_task(
                process_files_async,
                files_data=upload_result["files_data_for_processing"],
                user_id=user_id,
                msg_id=msg_id,
            )
            
            # Add background task for async file abstract generation
            background_tasks.add_task(
                AsyncFileProcessor.generate_file_abstracts_async,
                files_data=upload_result["files_data_for_processing"],
                message_id=msg_id,
                language="en"
            )
            
            # Add background task for async original text extraction
            background_tasks.add_task(
                AsyncFileProcessor.extract_file_original_texts_async,
                files_data=upload_result["files_data_for_processing"],
                message_id=msg_id,
                language="en"
            )
        
        # Prepare response
        response_data = {
            "session_id": session_id,
            "total_files": total_files,
            "successful_files": successful_count,
            "failed_files": failed_count,
            "successful_uploads": upload_result["successful_uploads"],
            "failed_uploads": upload_result["failed_uploads"] if upload_result["failed_uploads"] else None,
            "message_id": msg_id  # Single message ID for all files
        }
        
        if successful_count == total_files:
            return FileUploadWithProcessingResponse(
                code=0,
                msg=f"All {total_files} files uploaded and queued for processing",
                data=response_data
            )
        else:
            return FileUploadWithProcessingResponse(
                code=0,
                msg=f"Partial success: {successful_count} uploaded, {failed_count} failed",
                data=response_data
            )
    else:
        # Database save failed
        return FileUploadWithProcessingResponse(
            code=1,
            msg="Files uploaded to S3 but failed to save to database",
            data={
                "session_id": session_id,
                "total_files": total_files,
                "successful_files": successful_count,
                "failed_files": failed_count,
                "successful_uploads": upload_result["successful_uploads"],
                "failed_uploads": upload_result["failed_uploads"] if upload_result["failed_uploads"] else None
            }
        )


# HTTP interface removed - now handled via WebSocket at /ws/upload-health-report
# Use message type "upload_with_llm_analysis" for file upload + LLM analysis


@router.post("/api/v1/data/delete-files", response_model=FileDeleteResponse)
async def delete_uploaded_files(
    request: FileDeleteRequest,
    user_id: str = Depends(verify_token)
) -> FileDeleteResponse:
    """
    Delete uploaded files from a message
    
    This endpoint allows deletion of specific files or all files from a message.
    - If file_keys is provided, only those specific files will be deleted
    - If file_keys is None/empty, all files in the message will be deleted
    - If all files are deleted, the message will be marked as deleted (is_del=true)
    
    Args:
        request: FileDeleteRequest containing message_id and optional file_keys
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

