"""HTTP surface for data sharing (`/invitation/*`).

Split out of `mirobody/user/sharing.py`, which was 2,235 lines holding the
router, the service and the raw-SQL DAO at once — and living under `user/`
rather than alongside every other route file. The service and its request
models stay in `user/sharing.py`; only the HTTP layer moved.

The route paths, methods, handler names and response shapes are unchanged; the
route table was diffed before and after.
"""

import json
import logging
import os

from typing import Any, Dict, Optional

import aiohttp

from fastapi import APIRouter, Depends, HTTPException

from mirobody.user.sharing import (
    AcceptSharedByMeRequest,
    AcceptSharedWithMeRequest,
    RemoveSharedByMeRequest,
    RemoveSharedWithMeRequest,
    RequestSharedWithMeRequest,
    SendSharedByMeRequest,
    SendVerificationCodeRequest,
    UpdateSharedByMeNicknameRequest,
    UpdateSharedNicknameRequest,
    UpdateSharedWithMeNicknameRequest,
    UpdateUserAvatarRequest,
    get_sharing_service,
)
from mirobody.utils import execute_query
from mirobody.server.auth import verify_token
from mirobody.utils.s3 import aget_s3_url

router = APIRouter(prefix="/invitation", tags=["invitation"])


# ============================================================================
# API Endpoints - MUST match invitation.py exactly
# ============================================================================

@router.post("/shared-by-me/send")
async def send_shared_by_me_invitation(
        request: SendSharedByMeRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Send invitation for 'Shared by Me' - invite someone to view my data
    
    Supports relationship_type: "family" (default) or "healthcare"
    """
    try:
        logging.debug(f"send_invitation called with user_id={current_user_id}, email={request.email}, type={request.type}")
        service = await get_sharing_service()

        nickname = request.nickname
        if not nickname:
            nickname = request.email.split("@")[0]

        permission = request.permission if request.permission else {"all": 1}
        relationship_type = request.type if request.type else "family"

        result = await service.send_invitation(
            user_id=current_user_id,
            email=request.email,
            nickname=nickname,
            permission=permission,
            relationship_type=relationship_type
        )

        return result

    except Exception as e:
        logging.error(f"Exception in send_invitation: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-by-me/remove")
async def remove_shared_by_me(
        request: RemoveSharedByMeRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Remove 'Shared by Me' - remove someone I shared my data with

    Priority: share_id > query_user_id (for backward compatibility)
    """
    try:
        service = await get_sharing_service()

        # Prioritize share_id, fallback to query_user_id
        if request.share_id:
            # Get member_user_id for cleanup check
            check_result = await execute_query(
                """SELECT member_user_id
                   FROM th_share_relationship
                   WHERE share_id = :share_id
                     AND owner_user_id = :current_user_id;""",
                params={"share_id": request.share_id, "current_user_id": current_user_id},
            )
            if not check_result or not check_result[0]:
                return {"code": -1, "msg": "Share not found or unauthorized"}
            member_user_id = str(check_result[0]["member_user_id"])

            # Delete using share_id
            result = await execute_query(
                """DELETE
                   FROM th_share_relationship
                   WHERE share_id = :share_id
                     AND owner_user_id = :current_user_id;""",
                params={"share_id": request.share_id, "current_user_id": current_user_id},
            )
        elif request.query_user_id:
            # Fallback to old method for backward compatibility
            member_user_id = request.query_user_id
            result = await execute_query(
                """DELETE
                   FROM th_share_relationship
                   WHERE owner_user_id = :current_user_id
                     AND member_user_id = :query_user_id;""",
                params={"current_user_id": current_user_id, "query_user_id": request.query_user_id},
            )
        else:
            return {"code": -1, "msg": "Either query_user_id or share_id is required"}

        if not result:
            return {"code": -1, "msg": "Shared record not found"}

        # Check if there are any other relationships between these two users
        remaining_relations = await execute_query(
            """SELECT COUNT(*) as cnt
               FROM th_share_relationship
               WHERE (owner_user_id = :user1 AND member_user_id = :user2)
                  OR (owner_user_id = :user2 AND member_user_id = :user1);""",
            params={"user1": current_user_id, "user2": member_user_id},
        )

        # If no relationships exist, delete config records for both directions
        if remaining_relations and remaining_relations[0].get("cnt", 0) == 0:
            await execute_query(
                """DELETE
                   FROM th_share_user_config
                   WHERE (setter_user_id = :user1 AND target_user_id = :user2)
                      OR (setter_user_id = :user2 AND target_user_id = :user1);""",
                params={"user1": current_user_id, "user2": member_user_id},
            )

        return {"code": 0, "msg": "Successfully removed"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-with-me/remove")
async def remove_shared_with_me(
        request: RemoveSharedWithMeRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Remove 'Shared with Me' - remove someone who shared their data with me
    Priority: share_id > owner_user_id (for backward compatibility)
    """
    try:
        service = await get_sharing_service()

        # Prioritize share_id, fallback to owner_user_id
        if request.share_id:
            # Get owner_user_id for cleanup check
            check_result = await execute_query(
                """SELECT owner_user_id
                   FROM th_share_relationship
                   WHERE share_id = :share_id
                     AND member_user_id = :current_user_id;""",
                params={"share_id": request.share_id, "current_user_id": current_user_id},
            )
            if not check_result or not check_result[0]:
                return {"code": -1, "msg": "Share not found or unauthorized"}
            owner_user_id = str(check_result[0]["owner_user_id"])

            # Delete using share_id
            result = await execute_query(
                """DELETE
                   FROM th_share_relationship
                   WHERE share_id = :share_id
                     AND member_user_id = :current_user_id;""",
                params={"share_id": request.share_id, "current_user_id": current_user_id},
            )
        elif request.owner_user_id:
            # Fallback to old method for backward compatibility
            owner_user_id = request.owner_user_id
            result = await execute_query(
                """DELETE
                   FROM th_share_relationship
                   WHERE owner_user_id = :owner_user_id
                     AND member_user_id = :current_user_id;""",
                params={"owner_user_id": request.owner_user_id, "current_user_id": current_user_id},
            )
        else:
            return {"code": -1, "msg": "Either owner_user_id or share_id is required"}

        if not result:
            return {"code": -1, "msg": "Shared record not found"}

        # Check if there are any other relationships between these two users
        remaining_relations = await execute_query(
            """SELECT COUNT(*) as cnt
               FROM th_share_relationship
               WHERE (owner_user_id = :user1 AND member_user_id = :user2)
                  OR (owner_user_id = :user2 AND member_user_id = :user1);""",
            params={"user1": current_user_id, "user2": owner_user_id},
        )

        # If no relationships exist, delete config records for both directions
        if remaining_relations and remaining_relations[0].get("cnt", 0) == 0:
            await execute_query(
                """DELETE
                   FROM th_share_user_config
                   WHERE (setter_user_id = :user1 AND target_user_id = :user2)
                      OR (setter_user_id = :user2 AND target_user_id = :user1);""",
                params={"user1": current_user_id, "user2": owner_user_id},
            )

        return {"code": 0, "msg": "Successfully removed"}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error in remove_shared_with_me endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-by-me/list")
async def list_shared_by_me(
        current_user_id: str = Depends(verify_token)
):
    """
    List all invitations sent by current user
    """
    try:
        service = await get_sharing_service()
        result = await service.list_sent_invitations(current_user_id)
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-with-me/list")
async def list_shared_with_me(
        current_user_id: str = Depends(verify_token)
):
    """
    List all invitations received by current user
    """
    try:
        service = await get_sharing_service()
        result = await service.list_received_invitations_by_user_id(current_user_id)
        return result

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shared/detail")
async def get_shared_detail(
        shared_user_id: str,
        current_user_id: str = Depends(verify_token)
):
    """
    Get sharing relationship detail with a specific user
    
    Returns:
    - My nickname and avatar for the shared user (consistent across both directions)
    - shared_by_me relationship details (if exists)
    - shared_with_me relationship details (if exists)
    """
    try:
        # Get shared user info
        user_result = await execute_query(
            """SELECT id, email
               FROM health_app_user
               WHERE id = :user_id
                 AND is_del = false""",
            params={"user_id": shared_user_id},
        )

        if not user_result or not user_result[0]:
            return {
                "code": -1,
                "msg": "User not found",
                "data": None
            }

        shared_user_email = user_result[0].get("email")

        # Get my nickname and avatar for the shared user
        # Storage: setter_user_id=me, target_user_id=shared_user_id
        config_result = await execute_query(
            """SELECT nickname, avatar_key
               FROM th_share_user_config
               WHERE setter_user_id = :setter
                 AND target_user_id = :target
                 AND context = 'default'""",
            params={"setter": current_user_id, "target": shared_user_id},
        )

        nickname = None
        avatar_url = None
        if config_result and config_result[0]:
            nickname = config_result[0].get("nickname")
            avatar_key = config_result[0].get("avatar_key")
            if avatar_key:
                try:
                    avatar_url = await aget_s3_url(avatar_key, "avatar.jpg", content_type="image/jpeg")
                except Exception as avatar_error:
                    logging.warning(f"Failed to generate avatar URL: {avatar_error}")

        # Get shared_by_me relationship (owner=me, member=them)
        share_by_me_result = await execute_query(
            """SELECT r.share_id,
                      r.status,
                      r.relationship_type,
                      r.permissions,
                      r.created_at,
                      CASE
                          WHEN r.status = 'pending' THEN r.created_at + INTERVAL '72 hours'
                   ELSE NULL
            END
            as expired_at
            FROM th_share_relationship r
            WHERE r.owner_user_id = :owner AND r.member_user_id = :member""",
            params={"owner": current_user_id, "member": shared_user_id},
        )

        shared_by_me = {"exists": False}
        if share_by_me_result and share_by_me_result[0]:
            row = share_by_me_result[0]
            permission_data = row.get("permissions", {})
            if isinstance(permission_data, str):
                permission_data = json.loads(permission_data)

            rel_type = row.get("relationship_type", "data_sharing")
            if rel_type == "data_sharing":
                rel_type = "family"

            shared_by_me = {
                "exists": True,
                "share_id": str(row.get("share_id")),
                "status": row.get("status"),
                "type": rel_type,
                "permission": permission_data,
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                "expired_at": int(row.get("expired_at").timestamp() * 1000) if row.get("expired_at") else None,
                "created_timestamp": int(row.get("created_at").timestamp() * 1000) if row.get("created_at") else None
            }

        # Get shared_with_me relationship (owner=them, member=me)
        share_with_me_result = await execute_query(
            """SELECT r.share_id,
                      r.status,
                      r.relationship_type,
                      r.permissions,
                      r.created_at,
                      CASE
                          WHEN r.status = 'pending' THEN r.created_at + INTERVAL '72 hours'
                   ELSE NULL
            END
            as expired_at
            FROM th_share_relationship r
            WHERE r.owner_user_id = :owner AND r.member_user_id = :member""",
            params={"owner": shared_user_id, "member": current_user_id},
        )

        shared_with_me = {"exists": False}
        if share_with_me_result and share_with_me_result[0]:
            row = share_with_me_result[0]
            permission_data = row.get("permissions", {})
            if isinstance(permission_data, str):
                permission_data = json.loads(permission_data)

            rel_type = row.get("relationship_type", "data_sharing")
            if rel_type == "data_sharing":
                rel_type = "family"

            shared_with_me = {
                "exists": True,
                "share_id": str(row.get("share_id")),
                "status": row.get("status"),
                "type": rel_type,
                "permission": permission_data,
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                "expired_at": int(row.get("expired_at").timestamp() * 1000) if row.get("expired_at") else None,
                "created_timestamp": int(row.get("created_at").timestamp() * 1000) if row.get("created_at") else None
            }

        return {
            "code": 0,
            "msg": "Share relationship detail retrieved successfully",
            "data": {
                "shared_user_id": shared_user_id,
                "shared_user_email": shared_user_email,
                "nickname": nickname,
                "avatar_url": avatar_url,
                "shared_by_me": shared_by_me,
                "shared_with_me": shared_with_me
            }
        }

    except Exception as e:
        logging.error(f"Error getting shared detail: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared/list")
async def list_all_shared_relationships(
        current_user_id: str = Depends(verify_token)
):
    """
    Get combined list of all authorized sharing relationships
    
    Returns a single list with direction field indicating:
    - "shared_by_me": I share with them
    - "shared_with_me": They share with me
    - "both": Bidirectional sharing
    
    For bidirectional shares, permissions are returned as a map with separate configs for each direction
    """
    try:
        # Query all authorized relationships where I'm involved
        rows = await execute_query(
            """
            -- Get relationships where I'm the owner (shared_by_me)
            SELECT 'by_me'          as direction,
                   r.share_id,
                   r.member_user_id as other_user_id,
                   r.member_email   as other_email,
                   r.status,
                   r.relationship_type,
                   r.created_at,
                   c.nickname,
                   c.avatar_key,
                   NULL             as expired_at
            FROM th_share_relationship r
                     INNER JOIN health_app_user u
                                ON u.id::text = r.member_user_id
                AND u.is_del = false
            LEFT JOIN th_share_user_config c
            ON c.setter_user_id = :current_user_id
                AND c.target_user_id = r.member_user_id
                AND c.context = 'default'
            WHERE r.owner_user_id = :current_user_id
              AND r.status = 'authorized'

            UNION ALL

            -- Get relationships where I'm the member (shared_with_me)
            SELECT 'with_me'       as direction,
                   r.share_id,
                   r.owner_user_id as other_user_id,
                   r.owner_email   as other_email,
                   r.status,
                   r.relationship_type,
                   r.created_at,
                   c.nickname,
                   c.avatar_key,
                   NULL            as expired_at
            FROM th_share_relationship r
                     INNER JOIN health_app_user u
                                ON u.id::text = r.owner_user_id
                AND u.is_del = false
            LEFT JOIN th_share_user_config c
            ON c.setter_user_id = :current_user_id
                AND c.target_user_id = r.owner_user_id
                AND c.context = 'default'
            WHERE r.member_user_id = :current_user_id
              AND r.status = 'authorized'

            ORDER BY created_at DESC
            """,
            params={"current_user_id": current_user_id},
        )

        # Group relationships by other_user_id
        user_relations = {}

        for row in rows or []:
            other_user_id = row.get("other_user_id")
            direction = row.get("direction")

            if other_user_id not in user_relations:
                user_relations[other_user_id] = {
                    "other_user_id": other_user_id,
                    "other_email": row.get("other_email"),
                    "nickname": row.get("nickname"),
                    "avatar_key": row.get("avatar_key"),
                    "type": row.get("relationship_type", "data_sharing"),
                    "by_me": None,
                    "with_me": None
                }

            # Normalize type
            rel_type = row.get("relationship_type", "data_sharing")
            if rel_type == "data_sharing":
                rel_type = "family"

            # Store direction-specific data
            direction_data = {
                "share_id": str(row.get("share_id")),
                "status": row.get("status"),
                "type": rel_type,
                "expired_at": int(row.get("expired_at").timestamp() * 1000) if row.get("expired_at") else None,
                "created_timestamp": int(row.get("created_at").timestamp() * 1000) if row.get("created_at") else None,
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None
            }

            if direction == "by_me":
                user_relations[other_user_id]["by_me"] = direction_data
            else:
                user_relations[other_user_id]["with_me"] = direction_data

        # Build final list with direction and permissions map
        result_list = []
        by_me_count = 0
        with_me_count = 0
        both_count = 0

        for user_id, rel_data in user_relations.items():
            # Get avatar URL
            avatar_url = None
            if rel_data["avatar_key"]:
                try:
                    avatar_url = await aget_s3_url(rel_data["avatar_key"], "avatar.jpg", content_type="image/jpeg")
                except Exception as avatar_error:
                    logging.warning(f"Failed to generate avatar URL: {avatar_error}")

            # Determine direction and build item
            if rel_data["by_me"] and rel_data["with_me"]:
                # Bidirectional
                direction = "both"
                both_count += 1
                share_id = rel_data["by_me"]["share_id"]  # Use by_me share_id
                # Use by_me type for main type
                item_type = rel_data["by_me"]["type"]
                expired_at = rel_data["by_me"]["expired_at"]
                created_at = rel_data["by_me"]["created_at"]

            elif rel_data["by_me"]:
                # Only shared by me
                direction = "shared_by_me"
                by_me_count += 1
                share_id = rel_data["by_me"]["share_id"]
                item_type = rel_data["by_me"]["type"]
                expired_at = rel_data["by_me"]["expired_at"]
                created_at = rel_data["by_me"]["created_at"]

            else:
                # Only shared with me
                direction = "shared_with_me"
                with_me_count += 1
                share_id = rel_data["with_me"]["share_id"]
                item_type = rel_data["with_me"]["type"]
                expired_at = rel_data["with_me"]["expired_at"]
                created_at = rel_data["with_me"]["created_at"]

            result_list.append({
                "share_id": share_id,
                "email": rel_data["other_email"],
                "nickname": rel_data["nickname"],
                "status": "authorized",
                "type": item_type,
                "direction": direction,
                "shared_user_id": user_id,
                "avatar_url": avatar_url,
                "expired_at": expired_at,
                "created_at": created_at
            })

        return {
            "code": 0,
            "msg": "Share relationships retrieved successfully",
            "data": {
                "list": result_list,
                "summary": {
                    "total": len(result_list),
                    "by_me_count": by_me_count,
                    "with_me_count": with_me_count,
                    "both_count": both_count
                }
            }
        }

    except Exception as e:
        logging.error(f"Error getting shared list: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/user/check")
async def check_user_exists(
        request: SendSharedByMeRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Check if user exists by email and sharing status
    
    Returns is_shared=true if any direction (shared_by_me OR shared_with_me) is authorized
    """
    try:
        lower_email = request.email.strip().lower()

        user_result = await execute_query(
            """SELECT id, name, email
               FROM health_app_user
               WHERE email = :email
                 AND is_del = false""",
            params={"email": lower_email},
        )

        if user_result and user_result[0]:
            target_user_id = str(user_result[0].get("id"))

            # Check sharing relationships
            is_shared = False

            # Check if I share to them (owner=me, member=them)
            share_by_me = await execute_query(
                """SELECT status
                   FROM th_share_relationship
                   WHERE owner_user_id = :owner
                     AND member_user_id = :member
                     AND status = 'authorized'""",
                params={"owner": current_user_id, "member": target_user_id},
            )

            # Check if they share to me (owner=them, member=me)
            share_with_me = await execute_query(
                """SELECT status
                   FROM th_share_relationship
                   WHERE owner_user_id = :owner
                     AND member_user_id = :member
                     AND status = 'authorized'""",
                params={"owner": target_user_id, "member": current_user_id},
            )

            # is_shared = true if ANY direction is authorized
            is_shared = bool(share_by_me or share_with_me)

            return {
                "code": 0,
                "msg": "User exists",
                "data": {
                    "exists": True,
                    "user": {
                        # "id": user_result[0].get("id"),
                        "name": user_result[0].get("name"),
                        "email": user_result[0].get("email")
                    },
                    "is_shared": is_shared
                }
            }
        else:
            return {
                "code": 0,
                "msg": "User does not exist",
                "data": {
                    "exists": False,
                    "is_shared": False
                }
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/code/send")
async def send_verification_code(
        request: SendVerificationCodeRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Send verification code to email
    """
    try:
        service = await get_sharing_service()

        if not service._email_validator:
            raise HTTPException(status_code=500, detail="Email service not configured")

        result = await service._email_validator.send(request.email, 60 * 60 * 72)

        if result is None or result == "success":
            return {"code": 0, "msg": "Verification code sent"}
        else:
            return {"code": -1, "msg": result}

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-by-me/authorize")
async def authorize_shared_by_me(
        request: AcceptSharedByMeRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Authorize my member (for "My Members" tab)

    Priority: share_id > query_user_id (for backward compatibility)
    """
    try:
        service = await get_sharing_service()

        # Prioritize share_id for better performance
        result = await service.authorize_invitation(
            owner_user_id=current_user_id if not request.share_id else None,
            query_user_id=request.query_user_id if not request.share_id else None,
            share_id=request.share_id,
            permission=request.permission,
            email=None,
            verification_code=None
        )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-with-me/authorize")
async def authorize_shared_with_me(
        request: AcceptSharedWithMeRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Authorize 'Shared with Me' - ALWAYS requires email verification
    If server_url is provided, calls remote MCP server for email verification
    Priority: share_id > owner_user_id (for backward compatibility)
    """
    try:

        service = await get_sharing_service()

        # Determine verification strategy based on code/email presence
        verification_email = None
        verification_code = None

        if request.code and request.email:
            # Need email verification
            server_url = request.server_url
            if not server_url:
                # Use MCP_PUBLIC_URL from environment variable as default
                import os
                server_url = os.environ.get("MCP_PUBLIC_URL")
                if server_url:
                    logging.debug(f"Using MCP_PUBLIC_URL from environment: {server_url}")

            # If server_url is provided, verify through remote MCP server; otherwise use local
            if server_url:
                import aiohttp

                async with aiohttp.ClientSession() as session:
                    url = f"{server_url}/email/verify"
                    payload = {"email": request.email, "code": request.code}

                    logging.info(f"Calling remote verification at {url} with payload: {payload}")
                    try:
                        async with session.post(url, json=payload) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                logging.error(f"Remote verification failed with status {response.status}: {error_text}")
                                return {"code": -1, "msg": f"Remote verification failed (status {response.status}): {error_text}"}

                            result_json = await response.json()
                            logging.info(f"Remote verification response: {result_json}")

                            # Check if remote verification was successful
                            if not result_json.get("success"):
                                error_msg = result_json.get("msg", "Unknown error")
                                logging.error(f"Remote verification failed: success=False, msg={error_msg}, full response={result_json}")
                                return {"code": -1, "msg": f"Remote verification failed: {error_msg}"}

                            # Remote verification successful, verification will be skipped in service
                            logging.info("Remote verification successful")
                    except Exception as e:
                        logging.error(f"Exception during remote verification: {e}")
                        return {"code": -1, "msg": f"Remote verification error: {str(e)}"}
            else:
                # No server_url, use local verification
                verification_email = request.email
                verification_code = request.code

        ##some case APP do not know owner_user_id
        if not request.share_id and not request.owner_user_id:
            lower_email = request.email.strip().lower()
            user_result = await execute_query(
                """SELECT id, name, email
                   FROM health_app_user
                   WHERE email = :email
                     AND is_del = false""",
                params={"email": lower_email},
            )

            if user_result and user_result[0]:
                request.owner_user_id = str(user_result[0].get("id"))

        # Prioritize share_id for better performance
        result = await service.authorize_invitation(
            owner_user_id=request.owner_user_id if not request.share_id else None,
            query_user_id=current_user_id if not request.share_id else None,
            share_id=request.share_id,
            permission=request.permission,
            email=verification_email,
            verification_code=verification_code,
            nickname=request.nickname
        )

        return result

    except Exception as e:
        logging.error(f"Exception in shared-with-me authorize: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-with-me/request")
async def request_shared_with_me(
        request: RequestSharedWithMeRequest,
        query_user_id: str = Depends(verify_token)
):
    """
    Request for 'Shared with Me' - request to share my data with someone    
    Supports relationship_type: "family" (default) or "healthcare"
    """
    try:
        service = await get_sharing_service()
        relationship_type = request.type if request.type else "family"

        result = await service.request_share_access(
            query_user_id=query_user_id,
            email=request.email,
            nickname=request.nickname,
            permission=request.permission,
            relationship_type=relationship_type
        )

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _resolve_user_id_from_share(
        service,
        share_id: Optional[str],
        direct_user_id: Optional[str],
        current_user_id: str,
        role: str  # "owner" or "member"
) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Helper function to resolve user_id from share_id or direct user_id

    Returns: (resolved_user_id, error_response)
    If error_response is not None, should return it immediately
    """
    if share_id:
        # Determine which user_id to fetch based on role
        if role == "owner":
            # Current user is owner, get member
            query = """SELECT member_user_id
                       FROM th_share_relationship
                       WHERE share_id = :share_id
                         AND owner_user_id = :current_user_id;"""
            result_key = "member_user_id"
        else:
            # Current user is member, get owner
            query = """SELECT owner_user_id
                       FROM th_share_relationship
                       WHERE share_id = :share_id
                         AND member_user_id = :current_user_id;"""
            result_key = "owner_user_id"

        check_result = await execute_query(
            query,
            params={"share_id": share_id, "current_user_id": current_user_id},
        )
        if not check_result or not check_result[0]:
            return None, {"code": -1, "msg": "Share not found or unauthorized"}
        return str(check_result[0][result_key]), None
    elif direct_user_id:
        return direct_user_id, None
    else:
        return None, {"code": -1, "msg": "Either user_id or share_id is required"}


async def _resolve_bidirectional_share(
        service,
        share_id: Optional[str],
        direct_user_id: Optional[str],
        current_user_id: str
) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
    """
    Helper function to resolve user_id for bidirectional relationships (like avatar update)
    Either party can update the other's avatar

    Returns: (target_user_id, error_response)
    """
    if share_id:
        # Get both owner and member from share_id
        check_result = await execute_query(
            """SELECT owner_user_id, member_user_id
               FROM th_share_relationship
               WHERE share_id = :share_id;""",
            params={"share_id": share_id},
        )
        if not check_result or not check_result[0]:
            return None, {"code": -1, "msg": "Share not found"}

        owner_from_db = str(check_result[0]["owner_user_id"])
        member_from_db = str(check_result[0]["member_user_id"])

        # If current user is owner, target is member; if member, target is owner
        if owner_from_db == current_user_id:
            return member_from_db, None
        elif member_from_db == current_user_id:
            return owner_from_db, None
        else:
            return None, {"code": -1, "msg": "Unauthorized: you are not part of this share"}
    elif direct_user_id:
        return direct_user_id, None
    else:
        return None, {"code": -1, "msg": "Either user_id or share_id is required"}


@router.post("/shared-by-me/update-nickname")
async def update_shared_by_me_nickname(
        request: UpdateSharedByMeNicknameRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Update nickname for 'Shared by Me' - update how I call someone I shared with
    Priority: share_id > query_user_id (for backward compatibility)
    """
    try:
        service = await get_sharing_service()

        # Resolve target user_id
        target_user_id, error = await _resolve_user_id_from_share(
            service, request.share_id, request.query_user_id, current_user_id, "owner"
        )
        if error:
            return error

        # Update config: I (setter) am updating how I call the target
        result = await service.update_user_config(
            setter_user_id=current_user_id,
            target_user_id=target_user_id,
            nickname=request.nickname
        )

        return result

    except Exception as e:
        logging.error(f"Exception in update_shared_by_me_nickname: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared-with-me/update-nickname")
async def update_shared_with_me_nickname(
        request: UpdateSharedWithMeNicknameRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Update nickname for 'Shared with Me' - update how I call someone who shared with me
    Priority: share_id > owner_user_id (for backward compatibility)
    """
    try:
        service = await get_sharing_service()

        # Resolve target user_id
        target_user_id, error = await _resolve_user_id_from_share(
            service, request.share_id, request.owner_user_id, current_user_id, "member"
        )
        if error:
            return error

        # Update config: I (setter) am updating how I call the target
        result = await service.update_user_config(
            setter_user_id=current_user_id,
            target_user_id=target_user_id,
            nickname=request.nickname
        )

        return result

    except Exception as e:
        logging.error(f"Exception in update_shared_with_me_nickname: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared/update-nickname")
async def update_shared_nickname(
        request: UpdateSharedNicknameRequest,
        current_user_id: str = Depends(verify_token)
):
    """
    Update nickname for any user - update how I call another user
    
    This endpoint unifies nickname updates regardless of sharing direction.
    Nickname is a personal preference and does not require sharing relationship validation.
    """
    try:
        service = await get_sharing_service()

        # Update nickname configuration
        result = await service.update_user_config(
            setter_user_id=current_user_id,
            target_user_id=request.user_id,
            nickname=request.nickname
        )

        return result

    except Exception as e:
        logging.error(f"Exception in update_shared_nickname: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/permissions/list")
async def get_permission_list(
        current_user_id: str = Depends(verify_token)
):
    """
    Get list of available permission types
    """
    try:
        # Use th_share_permission_type table
        permissions = await execute_query(
            """SELECT permission_key, permission_name, permission_description, category, display_order
               FROM th_share_permission_type
               WHERE is_active = true
               ORDER BY display_order, permission_key;""",
            params={},
        )

        result = []
        if permissions:
            for perm in permissions:
                result.append({
                    "key": perm.get("permission_key"),
                    "name": perm.get("permission_name"),
                    "description": perm.get("permission_description"),
                    "category": perm.get("category"),
                    "display_order": perm.get("display_order", 0)
                })

        return {
            "code": 0,
            "msg": "Permission list retrieved successfully",
            "data": result
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shared/update-avatar")
async def update_user_avatar(
        request: UpdateUserAvatarRequest,
        user_id: str = Depends(verify_token)
) -> Dict[str, Any]:
    """
    Update user avatar (and optionally nickname) - allows either party to update the other's avatar

    Priority: share_id > owner_user_id (for backward compatibility)
    Note: Can update both avatar and nickname in a single request
    """
    try:
        service = await get_sharing_service()

        # Resolve target user_id (bidirectional - either party can update)
        target_user_id, error = await _resolve_bidirectional_share(
            service, request.share_id, request.owner_user_id, user_id
        )
        if error:
            return {**error, "data": None}

        logging.info(f"User {user_id} (setter) updating avatar for user {target_user_id} (target) with key: {request.avatar_key}")

        # Update config using unified service method
        result = await service.update_user_config(
            setter_user_id=user_id,
            target_user_id=target_user_id,
            avatar_key=request.avatar_key,
            nickname=request.nickname
        )

        if result.get("code") != 0:
            return {**result, "data": None}

        return {
            "code": 0,
            "msg": result.get("msg", "Avatar updated successfully"),
            "data": {
                "setter_user_id": user_id,
                "target_user_id": target_user_id,
                "avatar_key": request.avatar_key,
                "nickname": request.nickname
            }
        }

    except Exception as e:
        logging.error(f"Error updating user avatar: {e}")
        import traceback
        traceback.print_exc()
        return {
            "code": 1,
            "msg": f"Error updating avatar: {str(e)}",
            "data": None
        }
