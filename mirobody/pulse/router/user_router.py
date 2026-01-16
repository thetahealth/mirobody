"""
User Settings Module
User settings management module
"""

import logging
import traceback

from typing import Optional

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mirobody.utils.utils_auth import verify_token
from mirobody.utils import execute_query

# Create router
router = APIRouter(prefix="/api")


class ProfileSettings(BaseModel):
    gender: Optional[str] = "other"
    birth: Optional[str] = None
    blood: Optional[str] = None


class PreferenceSettings(BaseModel):
    language: Optional[str] = "en"  # "zh", "en", "ja", "fr", "es"
    timezone: Optional[str] = "America/Los_Angeles"
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


class UserSettings(BaseModel):
    profile: Optional[ProfileSettings] = None
    preferences: Optional[PreferenceSettings] = None
    privacy: Optional[PrivacySettings] = None
    notifications: Optional[NotificationSettings] = None


class UserSettingsRequest(BaseModel):
    settings: UserSettings

class PostUserSettingsRequest(BaseModel):
    timezone: Optional[str] = None

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
    update_sql = "UPDATE theta_ai.health_app_user SET"
    params = {}
    is_first_param = True

    if request.timezone and isinstance(request.timezone, str):
        params["tz"] = request.timezone
        if is_first_param:
            is_first_param = False
        else:
            update_sql += ","
        update_sql += " tz = :tz"

    if not params:
        return JSONResponse(content={"code": -1, "msg": "Empty input."})

    params["user_id"] = user_id
    update_sql += " WHERE id = :user_id"

    try:
        await execute_query(update_sql, params=params)
    except Exception as e:
        return JSONResponse(content={"code": -2, "msg": str(e)})

    return JSONResponse(content={"code": 0, "msg": "Okay."})


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
            SELECT email, gender, birth, blood, tz, lang
            FROM theta_ai.health_app_user 
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

        # Build settings response
        settings = {
            "profile": {
                "gender": gender_int_to_str(user_data.get("gender")),
                "birth": user_data.get("birth", ""),
                "blood": user_data.get("blood", ""),
            },
            "preferences": {
                "language": accept_language or user_data.get("lang", "en"),
                "timezone": user_data.get("tz") or timezone or "America/Los_Angeles",
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

        # Execute update if there are fields to update
        if update_fields:
            update_fields.append("update_at = CURRENT_TIMESTAMP")
            update_sql = f"""
                UPDATE theta_ai.health_app_user 
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
            SELECT id FROM theta_ai.health_app_user 
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
            INSERT INTO theta_ai.health_app_user 
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

        # Handle different possible result structures - similar to invitation service
        if isinstance(user_result, dict):
            virtual_user_id = str(user_result["id"])
            virtual_user_name = user_result["name"]
        elif isinstance(user_result, (list, tuple)) and len(user_result) > 0:
            # If result is wrapped in a list/tuple, access first element
            first_result = user_result[0]
            if isinstance(first_result, dict):
                virtual_user_id = str(first_result["id"])
                virtual_user_name = first_result["name"]
            else:
                # If it's a tuple (id, name)
                virtual_user_id = str(first_result[0])
                virtual_user_name = first_result[1]
        else:
            return JSONResponse(
                content={"code": -1, "msg": "Invalid database result format"},
                status_code=500
            )

        # Get current user's email for the relationship
        current_user_query = """
            SELECT email FROM theta_ai.health_app_user
            WHERE id = :user_id AND is_del = false
        """

        current_user_result = await execute_query(
            current_user_query,
            params={"user_id": int(current_user_id)},
        )

        current_user_email = current_user_result[0]["email"] if current_user_result else ""

        # Create share relationship using new th_share_relationship table
        create_relationship_query = """
            INSERT INTO theta_ai.th_share_relationship
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
            INSERT INTO theta_ai.th_share_user_config
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
