#!/usr/bin/env python3
"""
User Service
Responsible for user basic information and health profile management
"""

import logging

from datetime import datetime
from typing import Any, Dict

from mirobody.utils import execute_query

#-----------------------------------------------------------------------------

class UserService:
    """User Information Service"""

    def __init__(self):
        self.name = "User Service"
        self.version = "2.0.0"

    #-------------------------------------------------------------------------

    def _serialize_datetime(self, obj: Any) -> Any:
        """Convert datetime objects to ISO format strings for JSON serialization"""

        if isinstance(obj, datetime):
            return obj.isoformat()
        
        elif isinstance(obj, dict):
            return {k: self._serialize_datetime(v) for k, v in obj.items()}
        
        elif isinstance(obj, list):
            return [self._serialize_datetime(item) for item in obj]
        
        return obj
    
    #-------------------------------------------------------------------------

    async def get_user_health_profile(self, user_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get user health profile information

        Retrieves comprehensive health profile data from health_user_profile_by_system table.

        Args:
            no args needed.

        Returns:
            Dictionary containing user health profile data
        """

        try:
            user_id = user_info.get("user_id")
            user_id = str(user_id)
            
            # Log for debugging
            logging.info(f"Getting health profile for user: {user_id}")

            # Get the latest profile from health_user_profile_by_system table
            sql = """
                SELECT last_update_time, common_part FROM theta_ai.health_user_profile_by_system 
                WHERE user_id = :user_id AND is_deleted = false 
                ORDER BY version DESC 
                LIMIT 1
            """
            
            result = await execute_query(sql, params={"user_id": user_id})

            if result and isinstance(result, list):
                profile_data = result[0]
                
                # Serialize datetime objects in the profile data
                serialized_data = self._serialize_datetime(profile_data)
                
                return {
                    "success": True,
                    "data": serialized_data,
                }
            
            # No data found, use redirect_to_upload to trigger upload flow
            return {
                "success": False,
                "redirect_to_upload": True,
                "error": "No health profile data found",
                "data": None,
            }

        except Exception as e:
            logging.error(f"Failed to retrieve user health profile: {str(e)}")

            return {
                "success": False,
                "error": f"Failed to retrieve user health profile: {str(e)}",
                "data": None,
            }

#-----------------------------------------------------------------------------
