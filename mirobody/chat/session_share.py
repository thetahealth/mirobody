"""
Session Sharing Service

Service for publicly sharing chat sessions. Allows users to create shareable links
for their chat conversations without requiring authentication.
"""

import logging
import uuid
from typing import Dict, Any
from datetime import datetime

from ..utils import execute_query


class SessionShareService:
    """
    Session sharing service for creating and managing shareable chat sessions.
    """

    @staticmethod
    async def create_or_get_share_session(user_id: str, session_id: str) -> Dict[str, Any]:
        """
        Create or get share session ID for a given session.
        
        If a share session already exists for this session, returns the existing one.
        Otherwise, creates a new share session.
        
        Args:
            user_id: User ID of the session owner
            session_id: Original session ID to share
            
        Returns:
            Dict containing:
                - code: Status code (0 for success, negative for errors)
                - msg: Status message
                - data: Dict with share_session_id, session_id, created_at, is_new
        """
        try:
            # Check if user owns this session
            session_check_sql = """
                SELECT user_id FROM theta_ai.th_sessions 
                WHERE session_id = :session_id
            """
            session_result = await execute_query(
                session_check_sql,
                params={"session_id": session_id}
            )
            
            if not session_result or len(session_result) == 0:
                return {
                    "code": -1,
                    "msg": "Session not found",
                    "data": {}
                }
            
            session_owner = str(session_result[0].get("user_id"))
            if session_owner != user_id:
                return {
                    "code": -2,
                    "msg": "Unauthorized: You don't own this session",
                    "data": {}
                }
            
            # Check if share session already exists
            check_sql = """
                SELECT share_session_id, created_at, is_active 
                FROM theta_ai.th_session_share 
                WHERE session_id = :session_id AND is_active = TRUE
            """
            
            result = await execute_query(
                check_sql,
                params={"session_id": session_id}
            )
            
            if result and len(result) > 0:
                # Share session already exists, return existing one
                share_session_id = str(result[0].get("share_session_id"))
                created_at = result[0].get("created_at")
                
                logging.info(f"Returning existing share session {share_session_id} for session {session_id}")
                
                return {
                    "code": 0,
                    "msg": "ok",
                    "data": {
                        "share_session_id": share_session_id,
                        "session_id": session_id,
                        "created_at": created_at.isoformat() if created_at else None,
                        "is_new": False
                    }
                }
            
            # Create new share session
            share_session_id = str(uuid.uuid4())
            insert_sql = """
                INSERT INTO theta_ai.th_session_share (
                    share_session_id, session_id, user_id, created_at, updated_at, is_active
                )
                VALUES (:share_session_id, :session_id, :user_id, :created_at, :updated_at, TRUE)
                RETURNING created_at
            """
            
            created_at = datetime.now()
            await execute_query(
                insert_sql,
                params={
                    "share_session_id": share_session_id,
                    "session_id": session_id,
                    "user_id": user_id,
                    "created_at": created_at,
                    "updated_at": created_at
                }
            )
            
            logging.info(f"Created new share session {share_session_id} for session {session_id}")
            
            return {
                "code": 0,
                "msg": "ok",
                "data": {
                    "share_session_id": share_session_id,
                    "session_id": session_id,
                    "created_at": created_at.isoformat(),
                    "is_new": True
                }
            }
            
        except Exception as e:
            logging.error(f"Error creating/getting share session: {str(e)}", exc_info=True)
            return {
                "code": -3,
                "msg": f"Internal error: {str(e)}",
                "data": {}
            }

    @staticmethod
    async def get_shared_session_history(share_session_id: str) -> Dict[str, Any]:
        """
        Get chat history by share session ID (no authentication required).
        
        Returns the same format as get_chat_history for consistency with frontend.
        This allows the frontend to use the same rendering logic for both regular
        and shared sessions.
        
        Args:
            share_session_id: Share session ID (public identifier)
            
        Returns:
            Dict containing:
                - code: Status code (0 for success, negative for errors)
                - msg: Status message
                - data: Dict with history list in the same format as /api/history
        """
        try:
            # Get session_id from share_session_id
            share_query_sql = """
                SELECT session_id, user_id, created_at, is_active
                FROM theta_ai.th_session_share
                WHERE share_session_id = :share_session_id
            """
            
            share_result = await execute_query(
                share_query_sql,
                params={"share_session_id": share_session_id}
            )
            
            if not share_result or len(share_result) == 0:
                return {
                    "code": -1,
                    "msg": "Share session not found",
                    "data": {}
                }
            
            session_id = share_result[0].get("session_id")
            user_id = share_result[0].get("user_id")
            is_active = share_result[0].get("is_active")
            
            if not is_active:
                return {
                    "code": -2,
                    "msg": "This share session has been deactivated",
                    "data": {}
                }
            
            # Use the same get_chat_history function to get messages
            # This ensures 100% consistency with the existing /api/history endpoint
            from .message import get_chat_history
            
            history = await get_chat_history(user_id, session_id)
            
            logging.info(f"Retrieved {len(history)} messages for share session {share_session_id}")
            
            # Return the same format as /api/history endpoint
            return {
                "code": 0,
                "msg": "ok",
                "data": {
                    "history": history
                }
            }
            
        except Exception as e:
            logging.error(f"Error getting shared session history: {str(e)}", exc_info=True)
            return {
                "code": -4,
                "msg": f"Internal error: {str(e)}",
                "data": {}
            }

    @staticmethod
    async def deactivate_share_session(user_id: str, session_id: str) -> Dict[str, Any]:
        """
        Deactivate a share session (optional feature).
        
        Marks the share session as inactive, making it inaccessible via the
        share link. The original session remains intact.
        
        Args:
            user_id: User ID of the session owner
            session_id: Original session ID
            
        Returns:
            Dict containing:
                - code: Status code (0 for success, negative for errors)
                - msg: Status message
                - data: Empty dict
        """
        try:
            # Check ownership
            check_sql = """
                SELECT user_id FROM theta_ai.th_session_share
                WHERE session_id = :session_id
            """
            
            result = await execute_query(
                check_sql,
                params={"session_id": session_id}
            )
            
            if not result or len(result) == 0:
                return {
                    "code": -1,
                    "msg": "Share session not found",
                    "data": {}
                }
            
            owner_id = str(result[0].get("user_id"))
            if owner_id != user_id:
                return {
                    "code": -2,
                    "msg": "Unauthorized: You don't own this share session",
                    "data": {}
                }
            
            # Deactivate
            update_sql = """
                UPDATE theta_ai.th_session_share
                SET is_active = FALSE, updated_at = :updated_at
                WHERE session_id = :session_id
            """
            
            await execute_query(
                update_sql,
                params={
                    "session_id": session_id,
                    "updated_at": datetime.now()
                }
            )
            
            logging.info(f"Deactivated share session for session {session_id}")
            
            return {
                "code": 0,
                "msg": "Share session deactivated successfully",
                "data": {}
            }
            
        except Exception as e:
            logging.error(f"Error deactivating share session: {str(e)}", exc_info=True)
            return {
                "code": -3,
                "msg": f"Internal error: {str(e)}",
                "data": {}
            }


# Create service instance
session_share_service = SessionShareService()

