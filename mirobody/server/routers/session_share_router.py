"""Sharing a chat: a link anyone holding it can open, until its owner stops it."""

import logging

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

# Module-style import: the endpoint below is also named
# `deactivate_share_session`, so a bare `from ... import` of the service
# function would be shadowed by the route handler and recurse into itself.
from mirobody.agent.chat import session as chat_session
from mirobody.server.auth import verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse, err, failed
from mirobody.utils.log import secret_fingerprint

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/share", tags=["session-share"])


class CreateShareSessionRequest(BaseModel):
    """Create share session request"""
    session_id: str = Field(..., description="Session ID to share")


def _answered(result: dict) -> StandardResponse | ErrorResponse:
    """The share service's `{code, msg, data}` as the envelope it is."""
    if result.get("code"):
        return err(result["code"], result.get("msg") or "The share link could not be used.")
    return StandardResponse(**result)


@router.post("/create", response_model=StandardResponse | ErrorResponse)
async def create_share_session(
    request: CreateShareSessionRequest,
    current_user: str = Depends(verify_token)
):
    """
    Create or get the share link for a session the caller owns.

    Returns a share_session_id that opens the chat history without
    authentication.

    Args:
        request: Request containing session_id
        current_user: Current user ID from token

    Returns:
        Response containing share_session_id
    """
    try:
        logger.info("share link requested: user_id=%s session_id=%s",  # phi: ok account and session ids
                    current_user, request.session_id)

        result = await chat_session.create_or_get_share_session(
            user_id=current_user,
            session_id=request.session_id
        )

        return _answered(result)

    except Exception as e:
        return failed("share link creation", e, "The share link could not be created.")


@router.get("/{share_session_id}", response_model=StandardResponse | ErrorResponse)
async def get_shared_session(
    share_session_id: str
):
    """
    Get chat history by share session ID (NO AUTHENTICATION REQUIRED)

    This endpoint is public and does not require authentication. Anyone with the
    share_session_id can view the chat history.

    Returns the same format as /api/history endpoint for frontend consistency:

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
        # The id is the credential that opens the chat: the log gets its
        # fingerprint, which still ties together the requests one link made.
        logger.info("share link opened: share=%s", secret_fingerprint(share_session_id))

        result = await chat_session.get_shared_session_history(
            share_session_id=share_session_id
        )

        return _answered(result)

    except Exception as e:
        return failed("shared chat read", e, "The shared chat could not be loaded.")


# The router prefix is already /api/share, so "/share/deactivate" published
# this as /api/share/share/deactivate: clients call
# /api/share/deactivate and got a 404.
# The double path stays as an alias until known deployments confirm nothing
# adapted to it.
@router.post("/deactivate", response_model=StandardResponse | ErrorResponse)
@router.post("/share/deactivate", response_model=StandardResponse | ErrorResponse, include_in_schema=False)
async def deactivate_share_session(
    request: CreateShareSessionRequest,
    current_user: str = Depends(verify_token)
):
    """
    Deactivate a share session (stop sharing)

    This endpoint requires authentication. Only the owner of the session can deactivate
    the share session.

    Args:
        request: Request containing session_id
        current_user: Current user ID from token

    Returns:
        Response indicating success or failure
    """
    try:
        logger.info("share link stopped: user_id=%s session_id=%s",  # phi: ok account and session ids
                    current_user, request.session_id)

        result = await chat_session.deactivate_share_session(
            user_id=current_user,
            session_id=request.session_id
        )

        return _answered(result)

    except Exception as e:
        return failed("share link deactivation", e, "The share link could not be stopped.")
