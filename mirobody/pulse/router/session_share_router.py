"""
Session Sharing Router
Session sharing routes - Provides API endpoints for session sharing
"""

import logging

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from mirobody.chat.session_share import session_share_service
from mirobody.utils.utils_auth import verify_token

# Create router
router = APIRouter(prefix="/api/share", tags=["session-share"])


# ============================================================================
# Request/Response Models
# ============================================================================

class CreateShareSessionRequest(BaseModel):
    """Create share session request"""
    session_id: str = Field(..., description="Session ID to share")


class ShareSessionResponse(BaseModel):
    """Share session response"""
    code: int = Field(default=0, description="Response code, 0 indicates success")
    msg: str = Field(default="ok", description="Response message")
    data: dict = Field(default_factory=dict, description="Response data")


# ============================================================================
# API Endpoints
# ============================================================================

@router.post("/create", response_model=ShareSessionResponse)
async def create_share_session(
    request: CreateShareSessionRequest,
    current_user: str = Depends(verify_token)
):
    """
    Create or get share session ID for a session
    Create or get sharing ID for a session
    
    This endpoint requires authentication. It returns a share_session_id that can be used
    to access the chat history without authentication.
    
    Args:
        request: Request containing session_id
        current_user: Current user ID from token
        
    Returns:
        Response containing share_session_id
    """
    try:
        logging.info(f"User {current_user} requesting share session for session {request.session_id}")
        
        result = await session_share_service.create_or_get_share_session(
            user_id=current_user,
            session_id=request.session_id
        )
        
        return ShareSessionResponse(**result)
        
    except Exception as e:
        logging.error(f"Error in create_share_session endpoint: {str(e)}", exc_info=True)
        return ShareSessionResponse(
            code=-1,
            msg=f"Internal error: {str(e)}",
            data={}
        )


@router.get("/{share_session_id}", response_model=ShareSessionResponse)
async def get_shared_session(
    share_session_id: str
):
    """
    Get chat history by share session ID (NO AUTHENTICATION REQUIRED)
    Get chat history via share session ID (no authentication required)
    
    This endpoint is public and does not require authentication. Anyone with the
    share_session_id can view the chat history.
    
    Returns the same format as /api/history endpoint for frontend consistency:
    Returns the same format as /api/history endpoint to maintain frontend consistency:
    
    {
        "code": 0,
        "msg": "ok",
        "data": {
            "history": [
                {
                    "role": "user",
                    "content": "...",
                    "timestamp": "...",
                    "id": "...",
                    ...
                }
            ]
        }
    }
    
    Args:
        share_session_id: Share session ID (UUID format)
        
    Returns:
        Response containing chat history in the same format as /api/history
    """
    try:
        logging.info(f"Public access to share session {share_session_id}")
        
        result = await session_share_service.get_shared_session_history(
            share_session_id=share_session_id
        )
        
        return ShareSessionResponse(**result)
        
    except Exception as e:
        logging.error(f"Error in get_shared_session endpoint: {str(e)}", exc_info=True)
        return ShareSessionResponse(
            code=-1,
            msg=f"Internal error: {str(e)}",
            data={}
        )


@router.post("/share/deactivate", response_model=ShareSessionResponse)
async def deactivate_share_session(
    request: CreateShareSessionRequest,
    current_user: str = Depends(verify_token)
):
    """
    Deactivate a share session (stop sharing)
    Deactivate share session (stop sharing)
    
    This endpoint requires authentication. Only the owner of the session can deactivate
    the share session.
    
    Args:
        request: Request containing session_id
        current_user: Current user ID from token
        
    Returns:
        Response indicating success or failure
    """
    try:
        logging.info(f"User {current_user} deactivating share session for session {request.session_id}")
        
        result = await session_share_service.deactivate_share_session(
            user_id=current_user,
            session_id=request.session_id
        )
        
        return ShareSessionResponse(**result)
        
    except Exception as e:
        logging.error(f"Error in deactivate_share_session endpoint: {str(e)}", exc_info=True)
        return ShareSessionResponse(
            code=-1,
            msg=f"Internal error: {str(e)}",
            data={}
        )

