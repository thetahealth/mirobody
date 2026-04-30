"""
User Settings Module
User settings management module
"""

import logging
import secrets
import time
import traceback

from typing import Optional

import jwt as pyjwt
from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mirobody.utils.utils_auth import verify_token
from mirobody.utils import execute_query
from mirobody.utils.config import get_default_timezone, global_config

# Create router
router = APIRouter(prefix="/api")


class ProfileSettings(BaseModel):
    gender: Optional[str] = "other"
    birth: Optional[str] = None
    blood: Optional[str] = None


class PreferenceSettings(BaseModel):
    language: Optional[str] = "en"  # "zh", "en", "ja", "fr", "es"
    timezone: Optional[str] = None
    dateFormat: Optional[str] = "YYYY-MM-DD"


class PrivacySettings(BaseModel):
    dataSharing: Optional[bool] = True
    aiAnalysis: Optional[bool] = True
    analyticsTracking: Optional[bool] = True


class NotificationSettings(BaseModel):
    email: Optional[bool] = True
    push: Optional[bool] = True
    healthAlerts: Optional[bool] = True
    deviceSync: Optional[bool] = True
    weeklyReport: Optional[bool] = True


class SecuritySettings(BaseModel):
    mfa_enabled: Optional[bool] = None


class UserSettings(BaseModel):
    profile: Optional[ProfileSettings] = None
    preferences: Optional[PreferenceSettings] = None
    privacy: Optional[PrivacySettings] = None
    notifications: Optional[NotificationSettings] = None
    security: Optional[SecuritySettings] = None


class UserSettingsRequest(BaseModel):
    settings: UserSettings

class PostUserSettingsRequest(BaseModel):
    timezone: Optional[str] = None
    mfa_enabled: Optional[bool] = None

class CreateVirtualUserRequest(BaseModel):
    name: str
    email: str
    gender: Optional[str] = "other"  # "male", "female", "other"
    birth: Optional[str] = None
    blood: Optional[str] = None


def gender_str_to_int(gender_str: str) -> int:
    """Convert gender string to database integer"""
    gender_map = {"male": 1, "female": 2, "other": 0}
    return gender_map.get(gender_str, 0)


def gender_int_to_str(gender_int: Optional[int]) -> str:
    """Convert database integer to gender string"""
    gender_map = {1: "male", 2: "female", 0: "other"}
    return gender_map.get(gender_int, "other")

@router.post("/user/settings")
async def set_user_settings(
    request: PostUserSettingsRequest,
    user_id: str = Depends(verify_token)
):
    update_fields = []
    params = {}

    if request.timezone and isinstance(request.timezone, str):
        update_fields.append("tz = :tz")
        params["tz"] = request.timezone

    if request.mfa_enabled is not None:
        # Validate: cannot disable MFA while CW connected.
        if not request.mfa_enabled:
            cw_result = await execute_query(
                "SELECT registration_status FROM commonwell_patient WHERE user_id = :uid AND is_del = FALSE LIMIT 1",
                params={"uid": int(user_id)},
            )
            cw_connected = bool(cw_result and len(cw_result) > 0 and cw_result[0].get("registration_status") == "registered")
            if cw_connected:
                return JSONResponse(
                    content={"code": -2, "msg": "Cannot disable MFA while connected to Health Records Network. Please disconnect first."},
                    status_code=400,
                )
        update_fields.append("mfa_enabled = :mfa_enabled")
        params["mfa_enabled"] = request.mfa_enabled

    if not update_fields:
        return JSONResponse(content={"code": -1, "msg": "Empty input."})

    params["user_id"] = user_id
    update_sql = f"UPDATE health_app_user SET {', '.join(update_fields)} WHERE id = :user_id"

    try:
        await execute_query(update_sql, params=params)
    except Exception as e:
        return JSONResponse(content={"code": -2, "msg": str(e)})

    response_data = None

    # When MFA is toggled off, issue a new AAL1 token to downgrade the session.
    if request.mfa_enabled is False:
        try:
            jwt_key = global_config().get("JWT_KEY") if global_config() else None
            if jwt_key:
                email_result = await execute_query(
                    "SELECT email FROM health_app_user WHERE id = :uid AND is_del = FALSE",
                    params={"uid": int(user_id)},
                )
                email = email_result[0].get("email", "") if email_result else ""
                now = int(time.time()) - 60
                payload = {
                    "sub": user_id,
                    "iss": "", "aud": "",
                    "iat": now, "orig_iat": now, "nbf": now,
                    "exp": now + 60 * 60 * 24 * 30,
                    "client_id": "", "scope": "",
                    "jti": secrets.token_urlsafe(16),
                    "aal": 1,
                    "email": email,
                    "token_type": "oauth_access_token",
                }
                response_data = {"access_token": pyjwt.encode(payload, jwt_key, algorithm="HS256")}
        except Exception as e:
            logging.warning(f"Failed to generate AAL1 token on MFA disable: {e}")

    return JSONResponse(content={"code": 0, "msg": "Okay.", "data": response_data})


@router.get("/user/settings")
async def get_user_settings(
    user_id: str = Depends(verify_token),
    accept_language: Optional[str] = Header(None),
    timezone: Optional[str] = Header(None),
):
    """Get user settings from database"""
    try:
        logging.info(f"Getting settings for user: {user_id}, lang: {accept_language}, tz: {timezone}")

        # Get user profile info from health_app_user table
        user_sql = """
            SELECT email, gender, birth, blood, tz, lang, mfa_enabled
            FROM health_app_user
            WHERE id = :user_id AND is_del = false
        """

        user_result = await execute_query(
            user_sql,
            params={"user_id": int(user_id)}
        )

        # Check if user exists and extract data
        if user_result and len(user_result) > 0:
            user_data = user_result[0]
            logging.info(f"Using user data: {user_data}"
                         )
        else:
            # User not found or deleted
            logging.warning(f"No user data found for user_id: {user_id}")

            return JSONResponse(
                content={"code": -1, "msg": "User not found"},
                status_code=404,
            )

        # Build security info if WebAuthn is configured
        security = None
        webauthn_rp_id = global_config().get("WEBAUTHN_RP_ID") if global_config() else ""
        if webauthn_rp_id:
            uid = int(user_id)

            # Check if user has WebAuthn credentials
            cred_result = await execute_query(
                "SELECT COUNT(1) AS cnt FROM webauthn_credentials WHERE user_id = :uid AND is_del = FALSE",
                params={"uid": uid},
            )
            webauthn_registered = bool(cred_result and cred_result[0].get("cnt", 0) > 0)

            # Check if user is CW connected
            cw_result = await execute_query(
                "SELECT registration_status FROM commonwell_patient WHERE user_id = :uid AND is_del = FALSE LIMIT 1",
                params={"uid": uid},
            )
            cw_connected = bool(cw_result and len(cw_result) > 0 and cw_result[0].get("registration_status") == "registered")

            mfa_enabled = bool(user_data.get("mfa_enabled", False))

            # Auto-fix: if CW connected but MFA not enabled, force enable it.
            if cw_connected and not mfa_enabled:
                await execute_query(
                    "UPDATE health_app_user SET mfa_enabled = TRUE, update_at = CURRENT_TIMESTAMP WHERE id = :uid AND is_del = FALSE",
                    params={"uid": uid},
                )
                mfa_enabled = True

            security = {
                "mfa_enabled": mfa_enabled,
                "webauthn_supported": True,
                "webauthn_registered": webauthn_registered,
                "cw_connected": cw_connected,
            }

        # Build settings response
        settings = {
            "profile": {
                "gender": gender_int_to_str(user_data.get("gender")),
                "birth": user_data.get("birth", ""),
                "blood": user_data.get("blood", ""),
            },
            "preferences": {
                "language": accept_language or user_data.get("lang", "en"),
                "timezone": user_data.get("tz") or timezone or get_default_timezone(),
                "dateFormat": "YYYY-MM-DD",
            },
            "privacy": {
                "dataSharing": True,
                "aiAnalysis": True,
                "analyticsTracking": True,
            },
            "notifications": {
                "email": True,
                "push": True,
                "healthAlerts": True,
                "deviceSync": True,
                "weeklyReport": True,
            },
        }

        if security:
            settings["security"] = security

        return JSONResponse(
            content={"code": 0, "msg": "ok", "data": settings},
        )


    except Exception as e:
        logging.error(f"Error getting user settings: {str(e)}\n{traceback.format_exc()}")

        # raise HTTPException(status_code=500, detail="Failed to get user settings")
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to get user settings: {str(e)}"},
        )
    


@router.put("/user/settings")
async def update_user_settings(
    request: UserSettingsRequest,
    user_id: str = Depends(verify_token),
    accept_language: Optional[str] = Header(None),
    timezone: Optional[str] = Header(None),
):
    """Update user settings in database"""
    try:
        logging.info(f"Updating settings for user: {user_id}, request: {request.dict()}")

        settings = request.settings

        # Prepare update fields for health_app_user table
        update_fields = []
        update_params = {"user_id": int(user_id)}

        # Update profile information if provided
        if settings.profile:
            profile = settings.profile

            if profile.gender is not None:
                update_fields.append("gender = :gender")
                update_params["gender"] = gender_str_to_int(profile.gender)

            if profile.birth is not None:
                update_fields.append("birth = :birth")
                update_params["birth"] = profile.birth

            if profile.blood is not None:
                update_fields.append("blood = :blood")
                update_params["blood"] = profile.blood

        # Update preferences if provided
        if settings.preferences:
            if settings.preferences.timezone is not None:
                update_fields.append("tz = :tz")
                update_params["tz"] = settings.preferences.timezone
            elif timezone:
                update_fields.append("tz = :tz") 
                update_params["tz"] = timezone

            if settings.preferences.language is not None:
                update_fields.append("lang = :lang")
                update_params["lang"] = settings.preferences.language

        # Update security settings if provided
        if settings.security and settings.security.mfa_enabled is not None:
            # Validate: cannot disable MFA while CW connected
            if not settings.security.mfa_enabled:
                cw_result = await execute_query(
                    "SELECT registration_status FROM commonwell_patient WHERE user_id = :uid AND is_del = FALSE LIMIT 1",
                    params={"uid": int(user_id)},
                )
                cw_connected = bool(cw_result and len(cw_result) > 0 and cw_result[0].get("registration_status") == "registered")
                if cw_connected:
                    return JSONResponse(
                        content={"code": -2, "msg": "Cannot disable MFA while connected to Health Records Network. Please disconnect first."},
                        status_code=400,
                    )

            update_fields.append("mfa_enabled = :mfa_enabled")
            update_params["mfa_enabled"] = settings.security.mfa_enabled

        # Execute update if there are fields to update
        if update_fields:
            update_fields.append("update_at = CURRENT_TIMESTAMP")
            update_sql = f"""
                UPDATE health_app_user
                SET {", ".join(update_fields)}
                WHERE id = :user_id AND is_del = false
            """

            await execute_query(
                update_sql,
                params=update_params,
            )

        return JSONResponse(
            content={"code": 0, "msg": "ok"},
        )

    except Exception as e:
        logging.error(f"Error updating user settings: {str(e)}\n{traceback.format_exc()}")
        # raise HTTPException(status_code=500, detail="Failed to update user settings")

        return JSONResponse(
            content={"code": -1, "msg": f"Failed to update user settings: {str(e)}"},
        )


@router.post("/user/virtual")
async def create_virtual_user(
    request: CreateVirtualUserRequest,
    current_user_id: str = Depends(verify_token),
):
    """Create a virtual user and establish beneficiary relationship"""
    try:
        logging.info(f"Creating virtual user for user: {current_user_id}, request: {request.dict()}")

        # Check if username already exists
        check_email_query = """
            SELECT id FROM health_app_user 
            WHERE email = :email AND is_del = false
        """
        
        existing_user = await execute_query(
            check_email_query,
            params={"email": request.email.lower()},
        )

        if existing_user:
            return JSONResponse(
                content={"code": -1, "msg": "Username already exists"},
                status_code=400
            )

        # Create virtual user in health_app_user table
        create_user_query = """
            INSERT INTO health_app_user 
            (is_del, email, name, gender, birth, blood, tz, create_at, update_at)
            VALUES (false, :email, :name, :gender, :birth, :blood, 'UTC', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, name
        """

        user_params = {
            "email": request.email.lower(),
            "name": request.name,
            "gender": gender_str_to_int(request.gender or "other"),
            "birth": request.birth or "",
            "blood": request.blood or "",
        }

        user_result = await execute_query(
            create_user_query,
            params=user_params,
        )

        if not user_result:
            return JSONResponse(
                content={"code": -1, "msg": "Failed to create virtual user - no result returned"},
                status_code=500
            )

        row = user_result[0]
        virtual_user_id = str(row["id"])
        virtual_user_name = row["name"]

        # Get current user's email for the relationship
        current_user_query = """
            SELECT email FROM health_app_user
            WHERE id = :user_id AND is_del = false
        """

        current_user_result = await execute_query(
            current_user_query,
            params={"user_id": int(current_user_id)},
        )

        current_user_email = current_user_result[0]["email"] if current_user_result else ""

        # Create share relationship using new th_share_relationship table
        create_relationship_query = """
            INSERT INTO th_share_relationship
            (owner_user_id, member_user_id, owner_email, member_email, status, permissions, relationship_type)
            VALUES (:owner_user_id, :member_user_id, :owner_email, :member_email, 'authorized', :permissions, 'data_sharing')
            ON CONFLICT (owner_user_id, member_user_id) DO NOTHING
        """

        # Default permission: full access (all: 2 means write access)
        import json
        default_permissions = {"all": 2}

        await execute_query(
            create_relationship_query,
            params={
                "owner_user_id": current_user_id,
                "member_user_id": virtual_user_id,
                "owner_email": current_user_email,
                "member_email": request.email.lower(),
                "permissions": json.dumps(default_permissions),
            },
        )

        # Create nickname record in th_share_user_config
        create_config_query = """
            INSERT INTO th_share_user_config
            (setter_user_id, target_user_id, nickname, created_at, updated_at)
            VALUES (:setter_user_id, :target_user_id, :nickname, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (setter_user_id, target_user_id, context)
            DO UPDATE SET nickname = EXCLUDED.nickname, updated_at = CURRENT_TIMESTAMP
        """

        await execute_query(
            create_config_query,
            params={
                "setter_user_id": current_user_id,
                "target_user_id": virtual_user_id,
                "nickname": virtual_user_name,
            },
        )

        logging.info(f"Successfully created virtual user {virtual_user_id} for user {current_user_id}")

        return JSONResponse(
            content={
                "code": 0, 
                "msg": "ok",
                "data": {
                    "id": virtual_user_id,
                    "name": virtual_user_name,
                    "email": request.email.lower()
                }
            },
        )

    except Exception as e:
        logging.error(f"Error creating virtual user: {str(e)}\n{traceback.format_exc()}")
        
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to create virtual user: {str(e)}"},
            status_code=500
        )
