#!/usr/bin/env python3
"""
Health Indicator Service
Responsible for searching and retrieving health indicators from time-series data
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from mirobody.utils.db import execute_query

#-----------------------------------------------------------------------------

class HealthIndicatorService:
    """Health Indicator Service"""

    def __init__(self):
        self.name = "Health Indicator Service"
        self.version = "1.0.0"

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

    async def get_health_indicator(self, user_info: Dict[str, Any], keywords: str) -> Dict[str, Any]:
        """
        Get health indicators based on keywords

        Searches the theta_ai.th_series_data table for indicators matching the keywords.

        Args:
            user_info: User information dictionary (must contain 'user_id')
            keywords: Search keywords for indicator name (uses ILIKE matching)

        Returns:
            Dictionary containing:
                - success: bool
                - data: List of matching records (or empty list)
                - error: Error message (if applicable)
        """

        try:
            user_id = user_info.get("user_id")
            if not user_id:
                return {
                    "success": False,
                    "error": "user_id is required",
                    "data": None
                }

            user_id = str(user_id)

            logging.info(f"Searching health indicators for user: {user_id}, keywords: {keywords}")

            # Select all columns to ensure we get "other fields" like original_data, etc.
            # Sorting by column 5 (timestamp) descending to get latest data first
            sql = """
                SELECT * FROM theta_ai.th_series_data 
                WHERE user_id = :user_id 
                AND indicator ILIKE :keywords
                ORDER BY 5 DESC
                LIMIT 50
            """

            search_pattern = f"%{keywords}%"

            result = await execute_query(sql, params={"user_id": user_id, "keywords": search_pattern})

            if result:
                # Serialize datetime objects
                serialized_data = self._serialize_datetime(result)
                return {
                    "success": True,
                    "data": serialized_data,
                }

            # No matches found
            return {
                "success": True,
                "data": [],
            }

        except Exception as e:
            logging.error(f"Failed to search health indicators: {str(e)}")

            return {
                "success": False,
                "error": f"Failed to search health indicators: {str(e)}",
                "data": None,
            }

#-----------------------------------------------------------------------------
