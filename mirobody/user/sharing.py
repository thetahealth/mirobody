"""
Data Sharing Service - Refactored with th_share_* tables
External API interface is 100% compatible with invitation.py
Internal implementation uses new th_share_* table structure
"""

import json
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator

from mirobody.user.email import AbstractEmailCodeValidator
from mirobody.utils.utils_auth import verify_token
from mirobody.utils import execute_query
from mirobody.utils.utils_files.utils_s3 import aget_s3_url

router = APIRouter(prefix="/invitation", tags=["invitation"])


# ============================================================================
# Pydantic Models - MUST match invitation.py exactly
# ============================================================================

class SendSharedByMeRequest(BaseModel):
    """Send 'Shared by Me' invitation request"""
    email: str
    nickname: Optional[str] = None
    permission: Optional[Dict[str, int]] = None
    type: Optional[str] = "family"  # Relationship type: "family" or "healthcare"

    @field_validator('type')
    def validate_type(cls, v):
        if v and v not in ["family", "healthcare"]:
            raise ValueError('type must be "family" or "healthcare"')
        return v if v else "family"


class SendVerificationCodeRequest(BaseModel):
    """Send verification code request"""
    email: str
    server_url: Optional[str] = None


class AcceptSharedByMeRequest(BaseModel):
    """Accept 'Shared by Me' invitation request"""
    code: Optional[str] = None
    email: Optional[str] = None
    permission: Dict[str, int] = {"all": 1}
    user_info: Optional[Dict] = None
    query_user_id: Optional[str] = None
    share_id: Optional[str] = None

    @field_validator('query_user_id')
    def validate_query_user_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('query_user_id cannot be empty')
        return v.strip() if v else None

    @field_validator('share_id')
    def validate_share_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('share_id cannot be empty')
        return v.strip() if v else None


class AcceptSharedWithMeRequest(BaseModel):
    """Accept 'Shared with Me' invitation request - ALWAYS requires email verification"""
    code: str
    email: str
    permission: Dict[str, int] = {"all": 1}
    owner_user_id: Optional[str] = None
    share_id: Optional[str] = None
    nickname: Optional[str] = None
    server_url: Optional[str] = None

    @field_validator('owner_user_id')
    def validate_owner_user_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('owner_user_id cannot be empty')
        return v.strip() if v else None

    @field_validator('share_id')
    def validate_share_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('share_id cannot be empty')
        return v.strip() if v else None


class RemoveSharedByMeRequest(BaseModel):
    """Remove 'Shared by Me' - works for both pending and authorized"""
    query_user_id: Optional[str] = None
    share_id: Optional[str] = None

    @field_validator('query_user_id')
    def validate_query_user_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('query_user_id cannot be empty')
        return v.strip() if v else None

    @field_validator('share_id')
    def validate_share_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('share_id cannot be empty')
        return v.strip() if v else None


class RemoveSharedWithMeRequest(BaseModel):
    """Remove 'Shared with Me' - works for both pending and authorized"""
    owner_user_id: Optional[str] = None
    share_id: Optional[str] = None

    @field_validator('owner_user_id')
    def validate_owner_user_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('owner_user_id cannot be empty')
        return v.strip() if v else None

    @field_validator('share_id')
    def validate_share_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('share_id cannot be empty')
        return v.strip() if v else None


class UpdateUserAvatarRequest(BaseModel):
    """Update user avatar request - for authorized users to update another user's avatar"""
    owner_user_id: Optional[str] = None  # Target user receiving the avatar
    share_id: Optional[str] = None  # Alternative to owner_user_id
    avatar_key: str  # S3 key for the avatar image
    nickname: Optional[str] = None  # Optional nickname update

    @field_validator('avatar_key')
    def validate_avatar_key(cls, v):
        if not v or v.strip() == "":
            raise ValueError('avatar_key cannot be empty')
        return v.strip()

    @field_validator('owner_user_id')
    def validate_owner_user_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('owner_user_id cannot be empty')
        return v.strip() if v else None

    @field_validator('share_id')
    def validate_share_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('share_id cannot be empty')
        return v.strip() if v else None


class RequestSharedWithMeRequest(BaseModel):
    """Request 'Shared with Me' - request someone to share with me"""
    email: str
    nickname: Optional[str] = None
    permission: Dict[str, int] = {"all": 1}
    type: Optional[str] = "family"  # Relationship type: "family" or "healthcare"

    @field_validator('type')
    def validate_type(cls, v):
        if v and v not in ["family", "healthcare"]:
            raise ValueError('type must be "family" or "healthcare"')
        return v if v else "family"


class UpdateSharedByMeNicknameRequest(BaseModel):
    """Update nickname for 'Shared by Me'"""
    query_user_id: Optional[str] = None
    share_id: Optional[str] = None
    nickname: str

    @field_validator('query_user_id')
    def validate_query_user_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('query_user_id cannot be empty')
        return v.strip() if v else None

    @field_validator('share_id')
    def validate_share_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('share_id cannot be empty')
        return v.strip() if v else None


class UpdateSharedWithMeNicknameRequest(BaseModel):
    """Update nickname for 'Shared with Me'"""
    owner_user_id: Optional[str] = None
    share_id: Optional[str] = None
    nickname: str

    @field_validator('owner_user_id')
    def validate_owner_user_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('owner_user_id cannot be empty')
        return v.strip() if v else None

    @field_validator('share_id')
    def validate_share_id(cls, v):
        if v and v.strip() == "":
            raise ValueError('share_id cannot be empty')
        return v.strip() if v else None


class UpdateSharedNicknameRequest(BaseModel):
    user_id: str
    nickname: str


# ============================================================================
# Service Layer - Uses th_share_* tables internally
# ============================================================================

class SharingService:
    """
    Data sharing service using th_share_* tables
    External interface compatible with invitation.py
    """

    def __init__(self, email_code_validator: AbstractEmailCodeValidator = None):
        self._email_validator = email_code_validator

    # ------------------------------------------------------------------------
    # Core Sharing Functions
    # ------------------------------------------------------------------------

    async def send_invitation(
            self,
            user_id: str,
            email: str,
            nickname: str,
            permission: Dict[str, int] = None,
            relationship_type: str = "family"
    ) -> Dict[str, Any]:
        """
        Send member invitation - create user if not exists and create authorized share record
        Uses th_share_relationship and th_share_user_config tables
        
        Args:
            relationship_type: "family" or "healthcare" (default: "family")
        """
        if not email:
            return {"code": -1, "msg": "Email is required"}

        lower_email = email.strip().lower()

        try:
            # Get current user info
            owner_info = await execute_query(
                "SELECT id, name, email FROM theta_ai.health_app_user WHERE id=:owner_user_id AND is_del=FALSE;",
                params={"owner_user_id": int(user_id)},
            )
            if not owner_info:
                return {"code": -6, "msg": "Current user not found"}

            owner_user_email = owner_info[0]["email"]

            # Check if user exists
            existing_user = await execute_query(
                "SELECT id, name FROM theta_ai.health_app_user WHERE email=:email AND is_del=FALSE;",
                params={"email": lower_email},
            )

            if existing_user:
                member_user_id = str(existing_user[0]["id"])
            else:
                # Create new user
                default_nickname = nickname if nickname else lower_email.split("@")[0]
                new_user = await execute_query(
                    """INSERT INTO theta_ai.health_app_user (email, name, is_del)
                       VALUES (:email, :name, false) RETURNING id;""",
                    params={"email": lower_email, "name": default_nickname},
                )
                if not new_user:
                    return {"code": -2, "msg": "Failed to create user"}
                member_user_id = str(new_user["id"])

            # Check if relationship already exists
            existing_rel = await execute_query(
                """SELECT share_id
                   FROM theta_ai.th_share_relationship
                   WHERE owner_user_id = :owner_user_id
                     AND member_user_id = :member_user_id
                     and status in ('authorized', 'pending')
                ;""",
                params={"owner_user_id": user_id, "member_user_id": member_user_id},
            )

            if existing_rel:
                return {"code": -3, "msg": "Member relationship already exists"}

            # Create share relationship
            if not permission:
                permission = {"all": 0}

            await execute_query(
                """INSERT INTO theta_ai.th_share_relationship
                       (owner_user_id, member_user_id, owner_email, member_email, status, permissions, relationship_type)
                   VALUES (:owner_user_id, :member_user_id, :owner_email, :member_email, 'authorized', :permissions, :relationship_type);""",
                params={
                    "owner_user_id": user_id,
                    "member_user_id": member_user_id,
                    "owner_email": owner_user_email,
                    "member_email": lower_email,
                    "permissions": json.dumps(permission),
                    "relationship_type": relationship_type
                },
            )

            # Insert nickname record for owner (how I call the member)
            default_nickname = nickname if nickname else lower_email.split("@")[0]
            await execute_query(
                """INSERT INTO theta_ai.th_share_user_config
                       (setter_user_id, target_user_id, nickname, created_at, updated_at)
                   VALUES (:setter_user_id, :target_user_id, :nickname, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (setter_user_id, target_user_id, context)
                DO
                UPDATE SET nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP;""",
                params={
                    "setter_user_id": user_id,
                    "target_user_id": member_user_id,
                    "nickname": default_nickname
                },
            )

            # Also create reverse nickname record (how the member will see me)
            # Use owner's name, or email prefix as default
            reverse_nickname = owner_user_email.split("@")[0]  # Default to email prefix
            owner_user_name = owner_info[0].get("name")
            if owner_user_name and owner_user_name.strip():
                reverse_nickname = owner_user_name

            await execute_query(
                """INSERT INTO theta_ai.th_share_user_config
                       (setter_user_id, target_user_id, nickname, created_at, updated_at)
                   VALUES (:setter_user_id, :target_user_id, :nickname, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (setter_user_id, target_user_id, context)
                DO
                UPDATE SET nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP;""",
                params={
                    "setter_user_id": member_user_id,
                    "target_user_id": user_id,
                    "nickname": reverse_nickname
                },
                query_type="insert",
                mode="async"
            )

            return {"code": 0, "msg": "Invitation sent successfully"}

        except Exception as e:
            logging.error(f"Error sending invitation: {e}")
            return {"code": -5, "msg": str(e)}

    async def remove_shared_by_me(self, query_user_id: str, current_user_id: str) -> Dict[str, Any]:
        """Remove 'Shared by Me' - delete where I am sharing with someone"""
        try:
            result = await execute_query(
                """DELETE
                   FROM theta_ai.th_share_relationship
                   WHERE owner_user_id = :current_user_id
                     AND member_user_id = :query_user_id;""",
                params={"current_user_id": current_user_id, "query_user_id": query_user_id},
                query_type="delete",
                mode="async"
            )

            if not result:
                return {"code": -1, "msg": "Shared record not found"}

            # Check if there are any other relationships between these two users
            remaining_relations = await execute_query(
                """SELECT COUNT(*) as cnt
                   FROM theta_ai.th_share_relationship
                   WHERE (owner_user_id = :user1 AND member_user_id = :user2)
                      OR (owner_user_id = :user2 AND member_user_id = :user1);""",
                params={"user1": current_user_id, "user2": query_user_id},
            )

            # If no relationships exist, delete config records for both directions
            if remaining_relations and remaining_relations[0].get("cnt", 0) == 0:
                await execute_query(
                    """DELETE
                       FROM theta_ai.th_share_user_config
                       WHERE (setter_user_id = :user1 AND target_user_id = :user2)
                          OR (setter_user_id = :user2 AND target_user_id = :user1);""",
                    params={"user1": current_user_id, "user2": query_user_id},
                )

            return {"code": 0, "msg": "Successfully removed"}

        except Exception as e:
            logging.error(f"Error removing shared by me: {e}")
            return {"code": -2, "msg": str(e)}

    async def remove_shared_with_me(self, owner_user_id: str, current_user_id: str) -> Dict[str, Any]:
        """Remove 'Shared with Me' - delete where someone shared with me"""
        try:
            result = await execute_query(
                """DELETE
                   FROM theta_ai.th_share_relationship
                   WHERE owner_user_id = :owner_user_id
                     AND member_user_id = :current_user_id;""",
                params={"owner_user_id": owner_user_id, "current_user_id": current_user_id},
            )

            if not result:
                return {"code": -1, "msg": "Shared record not found"}

            # Check if there are any other relationships between these two users
            remaining_relations = await execute_query(
                """SELECT COUNT(*) as cnt
                   FROM theta_ai.th_share_relationship
                   WHERE (owner_user_id = :user1 AND member_user_id = :user2)
                      OR (owner_user_id = :user2 AND member_user_id = :user1);""",
                params={"user1": current_user_id, "user2": owner_user_id},
            )

            # If no relationships exist, delete config records for both directions
            if remaining_relations and remaining_relations[0].get("cnt", 0) == 0:
                await execute_query(
                    """DELETE
                       FROM theta_ai.th_share_user_config
                       WHERE (setter_user_id = :user1 AND target_user_id = :user2)
                          OR (setter_user_id = :user2 AND target_user_id = :user1);""",
                    params={"user1": current_user_id, "user2": owner_user_id},
                )

            return {"code": 0, "msg": "Successfully removed"}

        except Exception as e:
            logging.error(f"Error removing shared with me: {e}")
            return {"code": -2, "msg": str(e)}

    async def list_sent_invitations(self, user_id: str) -> Dict[str, Any]:
        """
        List all shares sent by user
        Uses v_my_shares view for simplified query
        
        Returns type (relationship_type) and expired_at (invitation expiration)
        """
        logging.info(f"List sent invitations for user shared-by-me {user_id}")
        try:
            rows = await execute_query(
                """SELECT r.share_id,
                          r.member_user_id    as query_user_id,
                          r.member_email      as query_user_email,
                          r.permissions       as permission,
                          r.created_at        as create_at,
                          r.status,
                          r.relationship_type as type,
                          c.avatar_key,
                          c.nickname,
                          CASE
                              WHEN r.status = 'pending' THEN r.created_at + INTERVAL '72 hours'
                       ELSE NULL
                END
                as expired_at
                FROM theta_ai.th_share_relationship r
                INNER JOIN theta_ai.health_app_user u
                    ON u.id::text = r.member_user_id
                    AND u.is_del = false
                LEFT JOIN theta_ai.th_share_user_config c
                    ON c.setter_user_id = r.owner_user_id
                    AND c.target_user_id = r.member_user_id
                    AND c.context = 'default'
                WHERE r.owner_user_id = :owner_user_id
                  AND r.status IN ('pending', 'authorized')
                ORDER BY r.created_at DESC;""",
                params={"owner_user_id": user_id},
            )

            invitations = []
            if rows:
                for row in rows:
                    permission_data = {}
                    if row.get("permission"):
                        try:
                            if isinstance(row["permission"], str):
                                permission_data = json.loads(row["permission"])
                            else:
                                permission_data = row["permission"]
                        except:
                            permission_data = {"all": 1}

                    status = row.get("status", "pending")
                    is_pending = status == "pending"

                    # Get avatar URL
                    avatar_url = None
                    avatar_key = row.get("avatar_key")
                    if avatar_key:
                        try:
                            avatar_url = await aget_s3_url(avatar_key, "avatar.jpg", content_type="image/jpeg")
                        except Exception as avatar_error:
                            logging.warning(f"Failed to generate avatar URL: {avatar_error}")

                    # Get relationship type (normalize for backward compatibility)
                    rel_type = row.get("type", "data_sharing")
                    if rel_type == "data_sharing":
                        rel_type = "family"  # Default to family for backward compatibility

                    invitation_item = {
                        "share_id": str(row.get("share_id")) if row.get("share_id") else None,
                        "email": row.get("query_user_email"),
                        "nickname": row.get("nickname"),
                        "accepted": 0 if is_pending else 1,
                        "status": status,
                        "permission": permission_data if permission_data else {"all": 1},
                        "type": rel_type,
                        "expired_at": int(row.get("expired_at").timestamp() * 1000) if row.get("expired_at") else None,
                        "created_timestamp": int(row.get("create_at").timestamp() * 1000) if row.get("create_at") else None,
                        "created_at": row.get("create_at").isoformat() if row.get("create_at") else None,
                        "query_user_id": row.get("query_user_id"),
                        "avatar_url": avatar_url
                    }
                    invitations.append(invitation_item)
            logging.info(f"List sent invitations for user shared-by-me {user_id}")
            return {"code": 0, "msg": "Sent invitations retrieved successfully", "data": invitations}

        except Exception as e:
            logging.error(f"Error listing sent invitations: {e}")
            return {"code": -1, "msg": str(e)}

    async def list_received_invitations_by_user_id(self, user_id: str) -> Dict[str, Any]:
        """
        List all shares received by user
        Uses v_shared_with_me view for simplified query
        
        Returns type (relationship_type) and expired_at (invitation expiration)
        """
        logging.info(f"Listing invitations for share-with-me user {user_id}")
        try:
            rows = await execute_query(
                """SELECT r.share_id,
                          r.owner_user_id,
                          r.owner_email       as owner_user_email,
                          r.permissions       as permission,
                          r.created_at        as create_at,
                          r.status,
                          r.relationship_type as type,
                          c.avatar_key,
                          c.nickname,
                          CASE
                              WHEN r.status = 'pending' THEN r.created_at + INTERVAL '72 hours'
                       ELSE NULL
                END
                as expired_at
                FROM theta_ai.th_share_relationship r
                INNER JOIN theta_ai.health_app_user u
                    ON u.id::text = r.owner_user_id
                    AND u.is_del = false
                LEFT JOIN theta_ai.th_share_user_config c
                    ON c.setter_user_id = r.member_user_id
                    AND c.target_user_id = r.owner_user_id
                    AND c.context = 'default'
                WHERE r.member_user_id = :query_user_id
                  AND r.status IN ('pending', 'authorized')
                ORDER BY r.created_at DESC;""",
                params={"query_user_id": user_id},
            )

            invitations = []
            if rows:
                for row in rows:
                    permission_data = {}
                    if row.get("permission"):
                        try:
                            if isinstance(row["permission"], str):
                                permission_data = json.loads(row["permission"])
                            else:
                                permission_data = row["permission"]
                        except:
                            permission_data = {"all": 1}

                    status = row.get("status", "pending")
                    is_pending = status == "pending"

                    # Get avatar URL
                    avatar_url = None
                    avatar_key = row.get("avatar_key")
                    if avatar_key:
                        try:
                            avatar_url = await aget_s3_url(avatar_key, "avatar.jpg", content_type="image/jpeg")
                        except Exception as avatar_error:
                            logging.warning(f"Failed to generate avatar URL: {avatar_error}")

                    # Get relationship type (normalize for backward compatibility)
                    rel_type = row.get("type", "data_sharing")
                    if rel_type == "data_sharing":
                        rel_type = "family"  # Default to family for backward compatibility

                    invitations.append({
                        "share_id": str(row.get("share_id")) if row.get("share_id") else None,
                        "email": row.get("owner_user_email"),
                        "nickname": row.get("nickname"),
                        "accepted": 0 if is_pending else 1,
                        "status": status,
                        "permission": permission_data if permission_data else {"all": 1},
                        "type": rel_type,
                        "expired_at": int(row.get("expired_at").timestamp() * 1000) if row.get("expired_at") else None,
                        "created_timestamp": int(row.get("create_at").timestamp() * 1000) if row.get("create_at") else None,
                        "created_at": row.get("create_at").isoformat() if row.get("create_at") else None,
                        "owner_user_id": row.get("owner_user_id"),
                        "avatar_url": avatar_url
                    })
            logging.info(f"Listing invitations success for user {user_id} share-with-me invitations.len {len(invitations)}")
            return {"code": 0, "msg": "Received invitations retrieved successfully", "data": invitations}

        except Exception as e:
            logging.error(f"Error listing received invitations by user ID: {e}")
            return {"code": -1, "msg": str(e)}

    async def authorize_invitation(
            self,
            owner_user_id: Optional[str] = None,
            query_user_id: Optional[str] = None,
            share_id: Optional[str] = None,
            permission: Dict[str, int] = None,
            email: str = None,
            verification_code: str = None,
            nickname: str = None
    ) -> Dict[str, Any]:
        """
        Authorize an invitation - unified function for both tabs
        Can use either (owner_user_id, query_user_id) or share_id
        """
        try:
            # Email verification if email and code are provided
            if email and verification_code:
                if self._email_validator:
                    try:
                        verification_result = await self._email_validator.verify(email, verification_code)
                        if verification_result is not None:
                            return {"code": -1, "msg": f"Email verification failed: {verification_result}"}
                    except Exception as e:
                        return {"code": -2, "msg": f"Email verification error: {str(e)}"}
                else:
                    return {"code": -3, "msg": "Email verification service not available"}

            # Build UPDATE query based on parameters
            if share_id:
                # Use share_id directly - more efficient
                update_query = """UPDATE theta_ai.th_share_relationship
                                  SET status='authorized',
                                      permissions=:permissions,
                                      updated_at=NOW()
                                  WHERE share_id = :share_id RETURNING owner_user_id, member_user_id;"""
                params = {
                    "permissions": json.dumps(permission),
                    "share_id": int(share_id)
                }
            elif owner_user_id and query_user_id:
                # Fallback to old method for backward compatibility
                update_query = """UPDATE theta_ai.th_share_relationship
                                  SET status='authorized',
                                      permissions=:permissions,
                                      updated_at=NOW()
                                  WHERE owner_user_id = :owner_user_id
                                    AND member_user_id = :query_user_id RETURNING owner_user_id, member_user_id;"""
                params = {
                    "permissions": json.dumps(permission),
                    "owner_user_id": owner_user_id,
                    "query_user_id": query_user_id
                }
            else:
                return {"code": -6, "msg": "Either share_id or (owner_user_id, query_user_id) is required"}

            result = await execute_query(
                update_query,
                params=params,
            )

            if not result:
                return {"code": -4, "msg": "Invitation record not found"}

            # Extract owner_user_id and member_user_id from result
            actual_owner_id = str(result.get("owner_user_id") or owner_user_id)
            actual_member_id = str(result.get("member_user_id") or query_user_id)

            logging.debug(f"authorize_invitation: owner={actual_owner_id}, member={actual_member_id}")

            # Always create/update nickname record
            # If nickname not provided, get it from user table name, or use email prefix
            if not nickname:
                target_user = await execute_query(
                    "SELECT name, email FROM theta_ai.health_app_user WHERE id=:user_id AND is_del=FALSE;",
                    params={"user_id": int(actual_owner_id)},
                )
                if target_user and target_user[0]:
                    nickname = target_user[0].get("name")
                    if not nickname or nickname.strip() == "":
                        # If name is empty, use email prefix
                        user_email = target_user[0].get("email", "")
                        nickname = user_email.split("@")[0] if user_email else "Unknown"
                else:
                    nickname = "Unknown"

            await execute_query(
                """INSERT INTO theta_ai.th_share_user_config
                       (setter_user_id, target_user_id, nickname, created_at, updated_at)
                   VALUES (:setter_user_id, :target_user_id, :nickname, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (setter_user_id, target_user_id, context)
                DO
                UPDATE SET nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP;""",
                params={
                    "setter_user_id": actual_member_id,
                    "target_user_id": actual_owner_id,
                    "nickname": nickname
                },
            )

            return {"code": 0, "msg": "Invitation authorized successfully"}

        except Exception as e:
            logging.error(f"Error authorizing invitation: {e}")
            return {"code": -5, "msg": str(e)}

    async def update_user_config(
            self,
            setter_user_id: str,
            target_user_id: str,
            nickname: Optional[str] = None,
            avatar_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update user config (nickname and/or avatar)
        Unified method for updating th_share_user_config table
        Can update nickname, avatar_key, or both in a single call
        """
        try:
            # Build dynamic SQL based on what's being updated
            update_fields = []
            params = {
                "setter_user_id": setter_user_id,
                "target_user_id": target_user_id
            }

            if nickname is not None:
                update_fields.append("nickname = EXCLUDED.nickname")
                params["nickname"] = nickname

            if avatar_key is not None:
                update_fields.append("avatar_key = EXCLUDED.avatar_key")
                params["avatar_key"] = avatar_key

            if not update_fields:
                return {"code": -1, "msg": "No fields to update"}

            # Build column list and values list
            columns = ["setter_user_id", "target_user_id", "created_at", "updated_at"]
            values = [":setter_user_id", ":target_user_id", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"]

            if nickname is not None:
                columns.append("nickname")
                values.append(":nickname")

            if avatar_key is not None:
                columns.append("avatar_key")
                values.append(":avatar_key")

            update_clause = ", ".join(update_fields) + ", updated_at = CURRENT_TIMESTAMP"

            query = f"""INSERT INTO theta_ai.th_share_user_config
                ({", ".join(columns)})
                VALUES ({", ".join(values)})
                ON CONFLICT (setter_user_id, target_user_id, context)
                DO UPDATE SET {update_clause};"""

            await execute_query(
                query,
                params=params,
            )

            msg_parts = []
            if nickname is not None:
                msg_parts.append("nickname")
            if avatar_key is not None:
                msg_parts.append("avatar")

            return {"code": 0, "msg": f"{' and '.join(msg_parts).capitalize()} updated successfully"}

        except Exception as e:
            logging.error(f"Error updating user config: {e}")
            return {"code": -2, "msg": str(e)}

    async def request_share_access(
            self,
            query_user_id: str,
            email: str,
            nickname: str = None,
            permission: Dict[str, int] = None,
            relationship_type: str = "family"
    ) -> Dict[str, Any]:
        """
        Request someone to view my data - reverse invitation
        
        Args:
            relationship_type: "family" or "healthcare" (default: "family")
        """
        if not email:
            return {"code": -1, "msg": "Email is required"}

        lower_email = email.strip().lower()
        default_nickname = nickname if nickname else lower_email.split("@")[0]

        try:
            # Get current user info
            user_info = await execute_query(
                "SELECT id, name, email FROM theta_ai.health_app_user WHERE id=:query_user_id AND is_del=FALSE;",
                params={"query_user_id": int(query_user_id)},
            )
            if not user_info:
                return {"code": -6, "msg": "Current user not found"}

            query_user_email = user_info[0]["email"]

            # Check if invitee exists
            existing_user = await execute_query(
                "SELECT id, name FROM theta_ai.health_app_user WHERE email=:email AND is_del=FALSE;",
                params={"email": lower_email},
            )

            if existing_user:
                owner_user_id = str(existing_user[0]["id"])
            else:
                # Create new user
                new_user = await execute_query(
                    """INSERT INTO theta_ai.health_app_user (email, name, is_del)
                       VALUES (:email, :name, false) RETURNING id;""",
                    params={"email": lower_email, "name": default_nickname},
                )
                if not new_user:
                    return {"code": -2, "msg": "Failed to create user"}
                owner_user_id = str(new_user["id"])

            # Check if relationship already exists
            existing_rel = await execute_query(
                """SELECT share_id
                   FROM theta_ai.th_share_relationship
                   WHERE owner_user_id = :owner_user_id
                     AND member_user_id = :query_user_id;""",
                params={"owner_user_id": owner_user_id, "query_user_id": query_user_id},
            )

            if existing_rel:
                return {"code": -3, "msg": "Share relationship already exists"}

            # Create relationship
            if not permission:
                permission = {"all": 1}

            await execute_query(
                """INSERT INTO theta_ai.th_share_relationship
                       (owner_user_id, member_user_id, owner_email, member_email, status, permissions, relationship_type)
                   VALUES (:owner_user_id, :member_user_id, :owner_email, :member_email, 'pending', :permissions, :relationship_type);""",
                params={
                    "owner_user_id": owner_user_id,
                    "member_user_id": query_user_id,
                    "owner_email": lower_email,
                    "member_email": query_user_email,
                    "permissions": json.dumps(permission),
                    "relationship_type": relationship_type
                },
            )

            # Insert nickname record for current user (how I call the invitee)
            await execute_query(
                """INSERT INTO theta_ai.th_share_user_config
                       (setter_user_id, target_user_id, nickname, created_at, updated_at)
                   VALUES (:setter_user_id, :target_user_id, :nickname, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (setter_user_id, target_user_id, context)
                DO
                UPDATE SET nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP;""",
                params={
                    "setter_user_id": query_user_id,
                    "target_user_id": owner_user_id,
                    "nickname": default_nickname
                },
            )

            # Also create reverse nickname record (how the invitee will see me)
            # Use current user's name, or email prefix as default
            reverse_nickname = query_user_email.split("@")[0]  # Default to email prefix
            current_user_name = user_info[0].get("name")
            if current_user_name and current_user_name.strip():
                reverse_nickname = current_user_name

            await execute_query(
                """INSERT INTO theta_ai.th_share_user_config
                       (setter_user_id, target_user_id, nickname, created_at, updated_at)
                   VALUES (:setter_user_id, :target_user_id, :nickname, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (setter_user_id, target_user_id, context)
                DO
                UPDATE SET nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP;""",
                params={
                    "setter_user_id": owner_user_id,
                    "target_user_id": query_user_id,
                    "nickname": reverse_nickname
                },
            )

            return {"code": 0, "msg": "Share request sent successfully"}

        except Exception as e:
            logging.error(f"Error requesting share access: {e}")
            return {"code": -5, "msg": str(e)}

    async def get_query_users_simple(self, user_id: str, name: str = "") -> List[Dict[str, str]]:
        """
        Get a lightweight list of users with minimal information.
        This includes the current user and all users who have shared their data with the current user.
        This is optimized for UI dropdowns and user selection lists.

        Args:
            user_id: The ID of the current user
            name: The name of the current user (optional, used as fallback)

        Returns:
            A list of users, each with:
            - id: User ID
            - name: Real name from database (fallback to nickname if empty)
            - nickname: Display nickname from share config (fallback to default if empty)
            - gender: User gender (None if not set)
            - blood_type: Blood type (None if not set)
            - age: Calculated age (None if birth date not set)
            - is_current_user: Boolean indicating if this is the current user
        """

        def _calculate_age(birth_str: str) -> Optional[int]:
            """
            Calculate age from birth date string.
            
            Args:
                birth_str: Birth date string (supports 'YYYY-MM-DD', 'YYYY/MM/DD' formats)
                
            Returns:
                Age as integer, or None if birth_str is empty or invalid
            """
            if not birth_str:
                return None

            try:
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y%m%d']:
                    try:
                        birth_date = datetime.strptime(birth_str, fmt).date()
                        today = date.today()
                        age = today.year - birth_date.year
                        # Adjust if birthday hasn't occurred this year
                        if (today.month, today.day) < (birth_date.month, birth_date.day):
                            age -= 1
                        return age
                    except ValueError:
                        continue
                return None
            except Exception:
                return None

        def _convert_gender(gender_value) -> Optional[str]:
            """
            Convert numeric gender value to string.
            
            Args:
                gender_value: Gender value from database (1, 2, or None)
                
            Returns:
                "male" for 1, "female" for 2, None for others
            """
            if gender_value == 1:
                return "male"
            elif gender_value == 2:
                return "female"
            else:
                return None

        try:
            logging.info(f"Getting simple query users for user_id: {user_id}, name: {name}")

            # Query current user's information
            current_user_query = """
                                 SELECT blood, birth, name, gender
                                 FROM theta_ai.health_app_user
                                 WHERE id = :user_id
                                   AND is_del = false \
                                 """

            current_user_data = await execute_query(
                current_user_query,
                params={"user_id": int(user_id)},
            )

            # Extract current user's information
            current_blood = None
            current_birth = None
            current_db_name = None
            current_gender = None
            if current_user_data and len(current_user_data) > 0:
                current_blood = current_user_data[0].get("blood")
                current_birth = current_user_data[0].get("birth")
                current_db_name = current_user_data[0].get("name")
                current_gender = _convert_gender(current_user_data[0].get("gender"))

            # Prepare result list starting with current user
            # name fallback: db name -> parameter name -> "Current User"
            # nickname: parameter name or None
            current_user_nickname = name if name else None
            current_user_real_name = current_db_name if current_db_name else (name if name else "Current User")

            result = [{
                "id": user_id,
                "name": current_user_real_name,
                "nickname": current_user_nickname,
                "gender": current_gender,
                "blood_type": current_blood,
                "age": _calculate_age(current_birth),
                "is_current_user": True
            }]

            # Query users who shared with me (I am the member, they are the owner)
            query = """
                    SELECT r.owner_user_id,
                           c.nickname as user_nickname,
                           h.blood,
                           h.birth,
                           h.name     as user_name,
                           h.gender   as user_gender
                    FROM theta_ai.th_share_relationship r
                             LEFT JOIN theta_ai.th_share_user_config c
                                       ON c.setter_user_id = r.member_user_id
                                           AND c.target_user_id = r.owner_user_id
                                           AND c.context = 'default'
                             LEFT JOIN theta_ai.health_app_user h
                                       ON h.id = CAST(r.owner_user_id AS INTEGER)
                                           AND h.is_del = false
                    WHERE r.member_user_id = :user_id
                      AND r.status = 'authorized' \
                    """

            share_result = await execute_query(
                query,
                params={"user_id": user_id},
            )

            if not share_result:
                logging.info(f"No shared users found for user_id {user_id}")
            else:
                logging.info(f"Found {len(share_result)} shared users for user_id {user_id}")

                # Add shared users to result
                for row in share_result:
                    # Extract fields
                    user_db_name = row.get("user_name")
                    user_nickname = row.get("user_nickname")
                    user_gender = _convert_gender(row.get("user_gender"))

                    # name fallback: db name -> nickname -> f"User {id}"
                    # nickname: use as-is (may be None)
                    display_nickname = user_nickname
                    display_name = user_db_name if user_db_name else (user_nickname if user_nickname else f"User {row['owner_user_id']}")

                    result.append({
                        "id": row["owner_user_id"],
                        "name": display_name,
                        "nickname": display_nickname,
                        "gender": user_gender,
                        "blood_type": row.get("blood"),
                        "age": _calculate_age(row.get("birth")),
                        "is_current_user": False
                    })

            return result

        except Exception as e:
            logging.error(f"Error getting simple query users: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            # Return just the current user if there's an error
            current_user_nickname = name if name else None
            current_user_real_name = name if name else "Current User"
            return [{
                "id": user_id,
                "name": current_user_real_name,
                "nickname": current_user_nickname,
                "gender": None,
                "blood_type": None,
                "age": None,
                "is_current_user": True
            }]


# ============================================================================
# Service Instance
# ============================================================================

_sharing_service = None


async def get_sharing_service():
    """Get sharing service instance"""
    global _sharing_service

    if _sharing_service:
        return _sharing_service

    # Create email validator
    email_validator = None
    try:
        from mirobody.user.email import MandrillEmailValidator
        from mirobody.utils.config import global_config

        cfg = global_config()
        if cfg:
            email_validator = MandrillEmailValidator(
                apiKey=cfg.get_str("EMAIL_SMTP_PASS"),
                template=cfg.get_str("EMAIL_TEMPLATE"),
                from_email=cfg.get_str("EMAIL_FROM"),
                from_name=cfg.get_str("EMAIL_FROM_NAME"),
                predefined_codes=cfg.get_dict("EMAIL_PREDEFINE_CODES", {}),
                redis=await cfg.get_redis().get_async_client()
            )
        else:
            logging.warning("global_config() returns None.")

    except Exception as e:
        logging.warning(f"Failed to create email validator: {e}")

    _sharing_service = SharingService(
        email_code_validator=email_validator
    )

    return _sharing_service


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
                   FROM theta_ai.th_share_relationship
                   WHERE share_id = :share_id
                     AND owner_user_id = :current_user_id;""",
                params={"share_id": int(request.share_id), "current_user_id": current_user_id},
            )
            if not check_result or not check_result[0]:
                return {"code": -1, "msg": "Share not found or unauthorized"}
            member_user_id = str(check_result[0]["member_user_id"])

            # Delete using share_id
            result = await execute_query(
                """DELETE
                   FROM theta_ai.th_share_relationship
                   WHERE share_id = :share_id
                     AND owner_user_id = :current_user_id;""",
                params={"share_id": int(request.share_id), "current_user_id": current_user_id},
            )
        elif request.query_user_id:
            # Fallback to old method for backward compatibility
            member_user_id = request.query_user_id
            result = await execute_query(
                """DELETE
                   FROM theta_ai.th_share_relationship
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
               FROM theta_ai.th_share_relationship
               WHERE (owner_user_id = :user1 AND member_user_id = :user2)
                  OR (owner_user_id = :user2 AND member_user_id = :user1);""",
            params={"user1": current_user_id, "user2": member_user_id},
        )

        # If no relationships exist, delete config records for both directions
        if remaining_relations and remaining_relations[0].get("cnt", 0) == 0:
            await execute_query(
                """DELETE
                   FROM theta_ai.th_share_user_config
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
                   FROM theta_ai.th_share_relationship
                   WHERE share_id = :share_id
                     AND member_user_id = :current_user_id;""",
                params={"share_id": int(request.share_id), "current_user_id": current_user_id},
            )
            if not check_result or not check_result[0]:
                return {"code": -1, "msg": "Share not found or unauthorized"}
            owner_user_id = str(check_result[0]["owner_user_id"])

            # Delete using share_id
            result = await execute_query(
                """DELETE
                   FROM theta_ai.th_share_relationship
                   WHERE share_id = :share_id
                     AND member_user_id = :current_user_id;""",
                params={"share_id": int(request.share_id), "current_user_id": current_user_id},
            )
        elif request.owner_user_id:
            # Fallback to old method for backward compatibility
            owner_user_id = request.owner_user_id
            result = await execute_query(
                """DELETE
                   FROM theta_ai.th_share_relationship
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
               FROM theta_ai.th_share_relationship
               WHERE (owner_user_id = :user1 AND member_user_id = :user2)
                  OR (owner_user_id = :user2 AND member_user_id = :user1);""",
            params={"user1": current_user_id, "user2": owner_user_id},
        )

        # If no relationships exist, delete config records for both directions
        if remaining_relations and remaining_relations[0].get("cnt", 0) == 0:
            await execute_query(
                """DELETE
                   FROM theta_ai.th_share_user_config
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
               FROM theta_ai.health_app_user
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
               FROM theta_ai.th_share_user_config
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
            FROM theta_ai.th_share_relationship r
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
            FROM theta_ai.th_share_relationship r
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
            FROM theta_ai.th_share_relationship r
                     INNER JOIN theta_ai.health_app_user u
                                ON u.id::text = r.member_user_id
                AND u.is_del = false
            LEFT JOIN theta_ai.th_share_user_config c
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
            FROM theta_ai.th_share_relationship r
                     INNER JOIN theta_ai.health_app_user u
                                ON u.id::text = r.owner_user_id
                AND u.is_del = false
            LEFT JOIN theta_ai.th_share_user_config c
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
               FROM theta_ai.health_app_user
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
                   FROM theta_ai.th_share_relationship
                   WHERE owner_user_id = :owner
                     AND member_user_id = :member
                     AND status = 'authorized'""",
                params={"owner": current_user_id, "member": target_user_id},
            )

            # Check if they share to me (owner=them, member=me)
            share_with_me = await execute_query(
                """SELECT status
                   FROM theta_ai.th_share_relationship
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
                   FROM theta_ai.health_app_user
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
                       FROM theta_ai.th_share_relationship
                       WHERE share_id = :share_id
                         AND owner_user_id = :current_user_id;"""
            result_key = "member_user_id"
        else:
            # Current user is member, get owner
            query = """SELECT owner_user_id
                       FROM theta_ai.th_share_relationship
                       WHERE share_id = :share_id
                         AND member_user_id = :current_user_id;"""
            result_key = "owner_user_id"

        check_result = await execute_query(
            query,
            params={"share_id": int(share_id), "current_user_id": current_user_id},
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
               FROM theta_ai.th_share_relationship
               WHERE share_id = :share_id;""",
            params={"share_id": int(share_id)},
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
               FROM theta_ai.th_share_permission_type
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
