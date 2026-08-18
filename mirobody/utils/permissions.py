"""Care-circle permission resolution: may user A read user B's data?

`get_query_user_id` is the single entry point and the only thing anything
outside this module imports. It answers "the caller is A, the request names B —
which user id should this query actually run against, and is that allowed?",
which is the check standing between one person's health record and another's.

Was `utils_user.py`, which stuttered (`mirobody.utils.utils_user`) and read as
a grab bag of user helpers. It is not: two of its four functions
(`authenticate_user`, `get_user_language`) had no callers at all and were
removed. `check_permissions` stays — it looks unused from outside, but
`get_query_user_id` calls it, and that is the whole check.

This module deliberately imports NOTHING from `.auth`. It used to open with
`from .auth import verify_token_string`, a name it never referenced — and
because `utils/__init__` imports this module eagerly, that one dead line made
`import mirobody.utils` (hence `mirobody.user`, `mirobody.task`, and anything
calling `execute_query`) require FastAPI, which ships only in the `[server]`
extra. A plain `pip install mirobody` could not import its own utils package.
Keep this module framework-free.
"""

import json
import logging

from typing import Any, Dict, Optional, List

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

    all_permission_value = perm_dict.get('all', 0)

    for req_perm in requested_permissions:
        req_perm_lower = req_perm.lower().strip()

        # First: Check for exact match (case insensitive)
        if req_perm_lower in perm_dict:
            result[req_perm] = perm_dict[req_perm_lower]
        else:
            # Second: Use 'all' value if no exact match
            result[req_perm] = all_permission_value

    return result


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
    # Normalise to str: user_id may be int (JWT sub) while query_user_id arrives as a str from
    # URL params, causing a spurious cross-user branch and a varchar=integer SQL type error.
    if str(user_id) != str(query_user_id):
        query = """
        select member_user_id as query_user_id, permissions as permission
        from th_share_relationship
        where member_user_id = :query_user_id
        and owner_user_id = :user_id
        and status = 'authorized'
        """
        params = {"query_user_id": str(query_user_id), "user_id": str(user_id)}
        result = await execute_query(query, params)
        if result:
            db_permissions = result[0].get("permission", {})

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
