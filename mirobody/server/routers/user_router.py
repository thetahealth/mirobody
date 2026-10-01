"""
User Settings Module
User settings management module
"""

import logging


from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mirobody.user.auth.jwt import ACCESS_TOKEN_TYPE, validator_from_config
from mirobody.server.auth import verify_token
from mirobody.utils import execute_query
from mirobody.utils.config import get_default_timezone, global_config
from mirobody.user import care_circle as cc
from mirobody.user.user import get_user
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api")


class ProfileSettings(BaseModel):
    gender: str | None = "other"
    birth: str | None = None
    blood: str | None = None


class PreferenceSettings(BaseModel):
    language: str | None = "en"  # "zh", "en", "ja", "fr", "es"
    timezone: str | None = None
    dateFormat: str | None = "YYYY-MM-DD"


class PrivacySettings(BaseModel):
    dataSharing: bool | None = True
    aiAnalysis: bool | None = True
    analyticsTracking: bool | None = True


class NotificationSettings(BaseModel):
    email: bool | None = True
    push: bool | None = True
    healthAlerts: bool | None = True
    deviceSync: bool | None = True
    weeklyReport: bool | None = True


class SecuritySettings(BaseModel):
    mfa_enabled: bool | None = None


class UserSettings(BaseModel):
    profile: ProfileSettings | None = None
    preferences: PreferenceSettings | None = None
    privacy: PrivacySettings | None = None
    notifications: NotificationSettings | None = None
    security: SecuritySettings | None = None


class UserSettingsRequest(BaseModel):
    settings: UserSettings

class PostUserSettingsRequest(BaseModel):
    timezone: str | None = None
    mfa_enabled: bool | None = None

class CreateVirtualUserRequest(BaseModel):
    name: str
    #: Ignored. The server mints the address; see `create_virtual_user`.
    email: str | None = None
    gender: str | None = "other"  # "male", "female", "other"
    birth: str | None = None
    blood: str | None = None


def gender_str_to_int(gender_str: str) -> int:
    """Convert gender string to database integer"""
    gender_map = {"male": 1, "female": 2, "other": 0}
    return gender_map.get(gender_str, 0)


def gender_int_to_str(gender_int: int | None) -> str:
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
        update_fields.append("mfa_enabled = :mfa_enabled")
        params["mfa_enabled"] = request.mfa_enabled

    if not update_fields:
        return JSONResponse(content={"code": -1, "msg": "Empty input."})

    params["user_id"] = user_id
    update_sql = f"UPDATE health_app_user SET {', '.join(update_fields)} WHERE id = :user_id"

    try:
        await execute_query(update_sql, params=params)
    except Exception as e:
        # A driver's message quotes the SQL with its bound parameters: it goes
        # to neither the caller nor the log.
        logger.warning("profile update failed: %s", type(e).__name__, exc_info=not is_driver_exception(e))
        return JSONResponse(content={"code": -2, "msg": "Failed to update the profile."})

    response_data = None

    # When MFA is toggled off, issue a new AAL1 token to downgrade the session.
    if request.mfa_enabled is False:
        try:
            jwt_key = global_config().get("JWT_KEY") if global_config() else None
            if jwt_key:
                user_row = await get_user(user_id=user_id)
                email = user_row.get("email", "") if user_row else ""

                # This used to build the whole payload inline and call
                # pyjwt.encode directly, hardcoding iss/aud/client_id/scope to
                # "" while every other token in the system carries the
                # configured values. It survived only because verify_token runs
                # with verify_iss and verify_aud both False. Claim shape lives
                # in user/jwt.py; a change there has to reach this token too.
                response_data = {
                    "access_token": validator_from_config().generate_token(
                        user_id,
                        {
                            "aal": 1,
                            "email": email,
                            # generate_token does not add this; only
                            # generate_tokens does. The OAuth introspection
                            # endpoint reports it (RFC 7662), so omitting it
                            # would make introspect return null for exactly
                            # the tokens minted here.
                            "token_type": ACCESS_TOKEN_TYPE,
                        },
                    )
                }
        except Exception as e:
            logger.warning("Failed to generate AAL1 token on MFA disable: %s", type(e).__name__)

    return JSONResponse(content={"code": 0, "msg": "Okay.", "data": response_data})


@router.get("/user/settings")
async def get_user_settings(
    user_id: str = Depends(verify_token),
    accept_language: str | None = Header(None),
    timezone: str | None = Header(None),
):
    """Get user settings from database"""
    try:
        logger.info(f"Getting settings for user: {user_id}, lang: {accept_language}, tz: {timezone}")

        # Get user profile info from health_app_user table
        user_data = await get_user(user_id=user_id)

        # Check if user exists and extract data
        if not user_data:
            # User not found or deleted
            logger.warning(f"No user data found for user_id: {user_id}")

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

            security = {
                "mfa_enabled": bool(user_data.get("mfa_enabled", False)),
                "webauthn_supported": True,
                "webauthn_registered": webauthn_registered,
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
        logger.error("getting user settings failed: %s", type(e).__name__, exc_info=not is_driver_exception(e))

        # raise HTTPException(status_code=500, detail="Failed to get user settings")
        return JSONResponse(
            content={"code": -1, "msg": "Failed to get user settings."},
        )
    


@router.put("/user/settings")
async def update_user_settings(
    request: UserSettingsRequest,
    user_id: str = Depends(verify_token),
    accept_language: str | None = Header(None),
    timezone: str | None = Header(None),
):
    """Update user settings in database"""
    try:
        logger.info(f"Updating settings for user: {user_id}, fields: {sorted(request.settings.model_fields_set)}")  # phi: ok field names, not values

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
        logger.error("updating user settings failed: %s", type(e).__name__, exc_info=not is_driver_exception(e))
        # raise HTTPException(status_code=500, detail="Failed to update user settings")

        return JSONResponse(
            content={"code": -1, "msg": "Failed to update user settings."},
        )


@router.post("/user/virtual")
async def create_virtual_user(
    request: CreateVirtualUserRequest,
    current_user_id: str = Depends(verify_token),
):
    """Create a virtual user and establish beneficiary relationship"""
    try:
        logger.info(f"Creating virtual user for user: {current_user_id}")

        # The address is minted here, never taken from the client. A real one
        # sent by a client put that person's future account inside the
        # caller's circle, read-write, the day they first signed in with a
        # code. The person gets their own sign-in through `activation`.
        from mirobody.user.activation import placeholder_email

        create_user_query = """
            INSERT INTO health_app_user
            (is_del, email, name, gender, birth, blood, tz, managed_by, create_at, update_at)
            VALUES (false, :email, :name, :gender, :birth, :blood, 'UTC', :managed_by, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, name
        """

        user_params = {
            "managed_by": int(current_user_id),
            "email": placeholder_email(),
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

        # A managed member is a real circle row, accepted and read-write, with
        # no invitation handshake, because this person will never sign in. The
        # shortcut is scoped to the CALLER'S OWN circle, which is the whole
        # safety story: `force_accept_managed_member` writes only into a circle
        # the caller owns. `health_access = 2` is right here and wrong anywhere
        # else: for anyone who can sign in that switch is theirs and starts at
        # 0, but a managed member cannot set it, so it is held by the person who
        # created them, who is the same person.
        circle_id = await cc.ensure_own_circle(current_user_id)
        await cc.force_accept_managed_member(
            circle_id, int(virtual_user_id), nickname=virtual_user_name
        )

        logger.info(f"Successfully created virtual user {virtual_user_id} for user {current_user_id}")

        return JSONResponse(
            content={
                "code": 0, 
                "msg": "ok",
                "data": {
                    "id": virtual_user_id,
                    "name": virtual_user_name,
                    "email": "",
                    "managed": True,
                }
            },
        )

    except Exception as e:
        logger.error("creating a virtual user failed: %s", type(e).__name__, exc_info=not is_driver_exception(e))
        
        return JSONResponse(
            content={"code": -1, "msg": "Failed to create the virtual user."},
            status_code=500
        )
