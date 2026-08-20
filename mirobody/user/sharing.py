"""Data sharing: request models, service and storage access.

Uses the `th_share_*` tables. The HTTP layer that used to live here — the
`/invitation/*` router and its 18 handlers — is now in
`mirobody/server/routers/sharing_router.py`, alongside every other route
file. This module was 2,235 lines doing router, service and raw-SQL DAO all
at once.
"""

import json
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List

from pydantic import BaseModel, field_validator

from mirobody.user.email import AbstractEmailCodeValidator
from mirobody.utils import execute_query
from mirobody.utils.s3 import aget_s3_url


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

def _parse_permission(row: dict) -> dict:
    """The `permission` column, whether the driver handed back JSON or a str.

    Was copy-pasted verbatim into two list methods. The `{"all": 1}` fallback
    is read-only access — the safe direction to fail for a row whose permission
    JSON does not parse.
    """
    raw = row.get("permission")
    if not raw:
        return {}
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return {"all": 1}


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
                "SELECT id, name, email FROM health_app_user WHERE id=:owner_user_id AND is_del=FALSE;",
                params={"owner_user_id": int(user_id)},
            )
            if not owner_info:
                return {"code": -6, "msg": "Current user not found"}

            owner_user_email = owner_info[0]["email"]

            # Atomic find-or-create: avoids check-then-act race with concurrent
            # share invitations against the same new email.
            default_nickname = nickname if nickname else lower_email.split("@")[0]
            user_row = await execute_query(
                """WITH ins AS (
                       INSERT INTO health_app_user (email, name, is_del)
                       VALUES (:email, :name, false)
                       ON CONFLICT (email) WHERE (is_del = false) DO NOTHING
                       RETURNING id
                   )
                   SELECT id FROM ins
                   UNION ALL
                   SELECT id FROM health_app_user
                       WHERE email = :email AND is_del = false
                   LIMIT 1;""",
                params={"email": lower_email, "name": default_nickname},
            )
            if not user_row:
                return {"code": -2, "msg": "Failed to create user"}
            member_user_id = str(user_row[0]["id"])

            # Check if relationship already exists
            existing_rel = await execute_query(
                """SELECT share_id
                   FROM th_share_relationship
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
                """INSERT INTO th_share_relationship
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
                """INSERT INTO th_share_user_config
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
                """INSERT INTO th_share_user_config
                       (setter_user_id, target_user_id, nickname, created_at, updated_at)
                   VALUES (:setter_user_id, :target_user_id, :nickname, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (setter_user_id, target_user_id, context)
                DO
                UPDATE SET nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP;""",
                params={
                    "setter_user_id": member_user_id,
                    "target_user_id": user_id,
                    "nickname": reverse_nickname
                },
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
                   FROM th_share_relationship
                   WHERE owner_user_id = :current_user_id
                     AND member_user_id = :query_user_id;""",
                params={"current_user_id": current_user_id, "query_user_id": query_user_id},
            )

            if not result:
                return {"code": -1, "msg": "Shared record not found"}

            # Check if there are any other relationships between these two users
            remaining_relations = await execute_query(
                """SELECT COUNT(*) as cnt
                   FROM th_share_relationship
                   WHERE (owner_user_id = :user1 AND member_user_id = :user2)
                      OR (owner_user_id = :user2 AND member_user_id = :user1);""",
                params={"user1": current_user_id, "user2": query_user_id},
            )

            # If no relationships exist, delete config records for both directions
            if remaining_relations and remaining_relations[0].get("cnt", 0) == 0:
                await execute_query(
                    """DELETE
                       FROM th_share_user_config
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
                   FROM th_share_relationship
                   WHERE owner_user_id = :owner_user_id
                     AND member_user_id = :current_user_id;""",
                params={"owner_user_id": owner_user_id, "current_user_id": current_user_id},
            )

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
                FROM th_share_relationship r
                INNER JOIN health_app_user u
                    ON u.id::text = r.member_user_id
                    AND u.is_del = false
                LEFT JOIN th_share_user_config c
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
                    permission_data = _parse_permission(row)

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
                FROM th_share_relationship r
                INNER JOIN health_app_user u
                    ON u.id::text = r.owner_user_id
                    AND u.is_del = false
                LEFT JOIN th_share_user_config c
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
                    permission_data = _parse_permission(row)

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
            nickname: str = None,
            acting_user_id: Optional[str] = None,
            require_email_verification: bool = False,
    ) -> Dict[str, Any]:
        """
        Authorize an invitation - unified function for both tabs
        Can use either (owner_user_id, query_user_id) or share_id

        `acting_user_id` is the authenticated caller and is REQUIRED. The
        share_id branch used to run

            UPDATE th_share_relationship SET status='authorized',
                   permissions=:permissions WHERE share_id = :share_id

        with no predicate naming the caller at all. Any authenticated user
        could flip any share row to `authorized` with permissions of their
        choosing: insert a pending share against a victim's email, authorize
        it, and read that victim's entire health record, with no notification
        on their side. The (owner_user_id, query_user_id) branch was safe only
        because the router happened to pass the caller's own id into one of
        them — safety by call-site convention, which the other branch did not
        follow.
        """
        try:
            if not acting_user_id:
                return {"code": -7, "msg": "Authenticated caller is required"}

            # Email verification. `if email and verification_code:` meant a
            # caller who simply omitted BOTH skipped verification entirely,
            # while `authorize_shared_with_me`'s own docstring said it "ALWAYS
            # requires email verification". `require_email_verification` makes
            # the requirement the caller's explicit choice instead of an
            # accident of which arguments were filled in.
            if require_email_verification and not (email and verification_code):
                return {"code": -8, "msg": "Email verification is required"}

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
                # Use share_id directly - more efficient.
                #
                # The caller must be a PARTY to the row. Either side may
                # authorize — the owner approves someone they are sharing with,
                # the member accepts an invitation addressed to them — but a
                # third party is not a party and matches neither column.
                update_query = """UPDATE th_share_relationship
                                  SET status='authorized',
                                      permissions=:permissions,
                                      updated_at=NOW()
                                  WHERE share_id = :share_id
                                    AND (owner_user_id = :acting_user_id
                                         OR member_user_id = :acting_user_id)
                                  RETURNING owner_user_id, member_user_id;"""
                params = {
                    "permissions": json.dumps(permission),
                    "share_id": share_id,
                    "acting_user_id": str(acting_user_id),
                }
            elif owner_user_id and query_user_id:
                # The (owner, member) pair form. The caller must still be one of
                # the two: relying on the router to pass its own id into the
                # right slot is the convention the share_id branch broke.
                if str(acting_user_id) not in (str(owner_user_id), str(query_user_id)):
                    return {"code": -9, "msg": "Not a party to this share"}
                update_query = """UPDATE th_share_relationship
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
                # Also the answer when the caller is not a party to the row:
                # the predicate matched nothing. Same message either way, so
                # this does not become a share_id oracle.
                return {"code": -4, "msg": "Invitation record not found"}

            row = result[0]
            actual_owner_id = str(row.get("owner_user_id") or owner_user_id)
            actual_member_id = str(row.get("member_user_id") or query_user_id)

            logging.debug(f"authorize_invitation: owner={actual_owner_id}, member={actual_member_id}")

            # Always create/update nickname record
            # If nickname not provided, get it from user table name, or use email prefix
            if not nickname:
                target_user = await execute_query(
                    "SELECT name, email FROM health_app_user WHERE id=:user_id AND is_del=FALSE;",
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
                """INSERT INTO th_share_user_config
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

            query = f"""INSERT INTO th_share_user_config
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
                "SELECT id, name, email FROM health_app_user WHERE id=:query_user_id AND is_del=FALSE;",
                params={"query_user_id": int(query_user_id)},
            )
            if not user_info:
                return {"code": -6, "msg": "Current user not found"}

            query_user_email = user_info[0]["email"]

            # Check if invitee exists
            existing_user = await execute_query(
                "SELECT id, name FROM health_app_user WHERE email=:email AND is_del=FALSE;",
                params={"email": lower_email},
            )

            if existing_user:
                owner_user_id = str(existing_user[0]["id"])
            else:
                # Create new user
                new_user = await execute_query(
                    """INSERT INTO health_app_user (email, name, is_del)
                       VALUES (:email, :name, false) RETURNING id;""",
                    params={"email": lower_email, "name": default_nickname},
                )
                if not new_user:
                    return {"code": -2, "msg": "Failed to create user"}
                owner_user_id = str(new_user[0]["id"])

            # Check if relationship already exists
            existing_rel = await execute_query(
                """SELECT share_id
                   FROM th_share_relationship
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
                """INSERT INTO th_share_relationship
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
                """INSERT INTO th_share_user_config
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
                """INSERT INTO th_share_user_config
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
                                 FROM health_app_user
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
                    FROM th_share_relationship r
                             LEFT JOIN th_share_user_config c
                                       ON c.setter_user_id = r.member_user_id
                                           AND c.target_user_id = r.owner_user_id
                                           AND c.context = 'default'
                             LEFT JOIN health_app_user h
                                       ON h.id = CAST(r.owner_user_id AS INTEGER)
                    WHERE r.member_user_id = :user_id
                      AND r.status = 'authorized'
                      AND h.is_del = false \
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
