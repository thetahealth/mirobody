import json
import logging

from typing import Any, Dict, Optional, List
from urllib.parse import unquote

from .utils_auth import verify_token_string
from .db import execute_query


def check_permissions(requested_permissions: List[str], db_permissions: Dict) -> Dict[str, int]:
    """
    Check if requested permissions exist in database permissions
    Simplified logic:
    1. Check exact match (case insensitive)
    2. If no match, use 'all' value
    3. If 'all' missing, default to 0

    Args:
        requested_permissions: List of requested permissions, e.g. ["device", "clinic_doc"]
        db_permissions: Permission dictionary from database, e.g. {"all": 1, "device": 2}

    Returns:
        dict: Status dictionary for each requested permission with permission levels (0/1/2)
    """
    result = {}

    if not requested_permissions:
        return result

    if not db_permissions:
        # Return 0 for all requested permissions if database permissions are empty
        return {perm: 0 for perm in requested_permissions}

    # Convert db_permissions keys to lowercase for case-insensitive matching
    perm_dict = {}
    for key, value in db_permissions.items():
        perm_dict[key.lower()] = int(value) if isinstance(value, (int, str)) and str(value).isdigit() else 0

    # Get 'all' permission value, default to 0 if missing
    all_permission_value = perm_dict.get('all', 0)

    # Check each requested permission
    for req_perm in requested_permissions:
        req_perm_lower = req_perm.lower().strip()

        # First: Check for exact match (case insensitive)
        if req_perm_lower in perm_dict:
            result[req_perm] = perm_dict[req_perm_lower]
        else:
            # Second: Use 'all' value if no exact match
            result[req_perm] = all_permission_value

    return result


async def authenticate_user(token: str) -> Optional[str]:
    """Authenticate user token and return user information"""
    try:
        # Log original token
        logging.debug(f"Original token: {token[:50]}..." if len(token) > 50 else f"Original token: {token}")

        # Ensure token format is correct
        token = unquote(token)
        logging.debug(f"After URL decode: {token[:50]}..." if len(token) > 50 else f"After URL decode: {token}")

        # Fix: Don't add Bearer prefix here, let verify_token_string handle it
        # verify_token_string will handle Bearer prefix correctly
        logging.debug(f"Passing to verify_token_string: {token[:50]}..."
            if len(token) > 50
            else f"Passing to verify_token_string: {token}")

        # Verify token and get user ID
        user_id = await verify_token_string(token)
        if not user_id:
            logging.error("Token verification failed: unable to get user ID")
            return None

        logging.info(f"Token verified successfully, user: {user_id}")

        return str(user_id)

    except Exception as e:
        logging.error(f"Token verification exception: {str(e)}, token: {token[:50]}..."
            if len(token) > 50
            else f"Token verification exception: {str(e)}, token: {token}", stack_info=True)
        return None


async def get_query_user_id(
    user_id: str,                           # owner_user_id, namely the data owner
    query_user_id: Optional[str] = None,    # member_user_id
    permission: Optional[List[str]] = None,
) -> Dict[str, Any]:
    # Handle case where user_id is None
    if not user_id:
        permission_dict = {perm: 0 for perm in permission} if permission else {}
        return {
            "success": False,
            "error": "User ID not provided",
            "query_user_id": None,
            "permissions": permission_dict,
        }

    # Use current user ID if query_user_id is not provided
    if not query_user_id:
        # Querying self, return 2 (write access) for all requested permissions
        permission_dict = {perm: 2 for perm in permission} if permission else {}
        return {"query_user_id": user_id, "success": True, "permissions": permission_dict}

    # Check proxy query permissions if query_user_id is provided and different from current user_id
    if user_id != query_user_id:
        query = """
        select member_user_id as query_user_id, permissions as permission 
        from theta_ai.th_share_relationship
        where member_user_id = :query_user_id
        and owner_user_id = :user_id
        and status = 'authorized'
        """
        params = {"query_user_id": query_user_id, "user_id": user_id}
        result = await execute_query(query, params, query_type="select")
        if result:
            db_permissions = result[0].get("permission", {})

            # Ensure db_permissions is a dictionary
            if isinstance(db_permissions, str):
                try:
                    db_permissions = json.loads(db_permissions)
                except json.JSONDecodeError:
                    logging.warning(f"Failed to parse permissions JSON string: {db_permissions}")
                    db_permissions = {}
            elif not isinstance(db_permissions, dict):
                db_permissions = {}

            # Check specific permissions if permission parameter is provided
            if permission:
                permission_dict = check_permissions(permission, db_permissions)
            else:
                # Return all permissions from database if permission parameter not provided
                permission_dict = db_permissions if isinstance(db_permissions, dict) else {}

            logging.info(f"Proxy query for user: {query_user_id}, permission check: {permission_dict}")
            return {
                "query_user_id": query_user_id,
                "success": True,
                "permissions": permission_dict,
            }
        else:
            # No permission to query, return 0 for all requested permissions
            permission_dict = {perm: 0 for perm in permission} if permission else {}
            return {
                "success": False,
                "error": "No permission to query this user's data",
                "query_user_id": None,
                "permissions": permission_dict,
            }
    else:
        # Return directly if query_user_id equals current user_id, querying self has write permissions (level 2) by default
        permission_dict = {perm: 2 for perm in permission} if permission else {}
        return {"query_user_id": user_id, "success": True, "permissions": permission_dict}


async def get_user_language(user_id: str) -> str:
    """
    Unified function to get user language - for a019_ai_task directory only

    Args:
        user_id: User ID (string type)

    Returns:
        str: Language code, 'zh' for Chinese, 'en' for English
    """
    try:
        # health_app_user.id is integer primary key, need to convert user_id to integer
        try:
            user_id_int = int(user_id)
        except ValueError:
            logging.warning(f"Invalid user_id format: {user_id}. Using default language 'en'.")
            return "en"

        sql = """SELECT lang FROM theta_ai.health_app_user WHERE id = :user_id"""

        result = await execute_query(
            sql, params={"user_id": user_id_int}, query_type="select", mode="async"
        )

        if result and result[0].get("lang"):
            language_code = result[0]["lang"]
            if language_code == "zh_CN":
                return "zh"
            elif language_code == "en":
                return "en"
            else:
                logging.warning(f"Unsupported language_code '{language_code}' for user {user_id_int}. Defaulting to 'en'.")
                return "en"  # Default to English for unsupported codes

        return "en"  # Default to English if lang is not set

    except Exception as e:
        logging.error(f"Error fetching language for user {user_id}: {e}. Defaulting to 'en'.", stack_info=True)
        return "en"  # Default to English if anything goes wrong
