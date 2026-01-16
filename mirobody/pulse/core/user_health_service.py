"""
User health data query service for management APIs
"""

import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from ...utils import execute_query


class UserHealthDataService:
    """Service for querying user health data (for management/admin use)"""
    
    @staticmethod
    async def get_user_health_data(
        user_id: str,
        start_date: datetime,
        end_date: datetime,
        indicators: Optional[List[str]] = None,
        data_sources: Optional[List[str]] = None,
        include_series: bool = True,
        include_summary: bool = True,
    ) -> Dict[str, Any]:
        """
        Query user health data within a time range
        
        Args:
            user_id: User ID
            start_date: Start date
            end_date: End date (max 7 days from start)
            indicators: List of indicators to filter (optional)
            data_sources: List of data sources to filter (optional)
            include_series: Whether to include fine-grained data from series_data table
            include_summary: Whether to include aggregated data from th_series_data table
            
        Returns:
            Dictionary containing user data and statistics
            
        Raises:
            ValueError: If time range exceeds 7 days
        """
        # Validate time range
        if (end_date - start_date).days > 7:
            raise ValueError("Time range cannot exceed 7 days")
        
        logging.info(f"Querying health data for user {user_id} from {start_date} to {end_date}")
        
        result = {
            "user_id": user_id,
            "time_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": (end_date - start_date).days + 1,
            },
            "filters": {
                "indicators": indicators,
                "data_sources": data_sources,
            },
            "summary": {},
            "series_data": [],
            "aggregated_data": [],
        }
        
        # Query series_data (fine-grained data)
        if include_series:
            try:
                series_data = await UserHealthDataService._query_series_data(
                    user_id, start_date, end_date, indicators, data_sources
                )
                result["series_data"] = series_data
                logging.info(f"Retrieved {len(series_data)} series data records")
            except Exception as e:
                logging.error(f"Failed to query series_data: {str(e)}")
                result["series_data_error"] = str(e)
        
        # Query th_series_data (aggregated data)
        if include_summary:
            try:
                summary_data = await UserHealthDataService._query_summary_data(
                    user_id, start_date, end_date, indicators, data_sources
                )
                result["aggregated_data"] = summary_data
                logging.info(f"Retrieved {len(summary_data)} aggregated data records")
            except Exception as e:
                logging.error(f"Failed to query th_series_data: {str(e)}")
                result["aggregated_data_error"] = str(e)
        
        # Generate statistics
        all_indicators = set()
        all_sources = set()
        
        for record in result["series_data"]:
            if record.get("indicator"):
                all_indicators.add(record["indicator"])
            if record.get("source"):
                all_sources.add(record["source"])
        
        for record in result["aggregated_data"]:
            if record.get("indicator"):
                all_indicators.add(record["indicator"])
            if record.get("source"):
                all_sources.add(record["source"])
        
        result["summary"] = {
            "total_series_records": len(result["series_data"]),
            "total_aggregated_records": len(result["aggregated_data"]),
            "unique_indicators": len(all_indicators),
            "unique_sources": len(all_sources),
            "indicators": sorted(list(all_indicators)),
            "sources": sorted(list(all_sources)),
        }
        
        return result
    
    @staticmethod
    async def _query_series_data(
        user_id: str,
        start_date: datetime,
        end_date: datetime,
        indicators: Optional[List[str]],
        data_sources: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        """
        Query series_data table (fine-grained health data)
        
        Args:
            user_id: User ID
            start_date: Start date
            end_date: End date
            indicators: List of indicators to filter
            data_sources: List of data sources to filter
            
        Returns:
            List of data records
        """
        query = """
        SELECT 
            user_id,
            indicator,
            source,
            time,
            value,
            timezone,
            update_time,
            task_id,
            source_id
        FROM theta_ai.series_data
        WHERE user_id = :user_id
          AND time >= :start_date
          AND time <= :end_date
        """
        
        params = {
            "user_id": user_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        
        # Add indicator filter
        if indicators and len(indicators) > 0:
            placeholders = ", ".join([f":indicator_{i}" for i in range(len(indicators))])
            query += f" AND indicator IN ({placeholders})"
            for i, ind in enumerate(indicators):
                params[f"indicator_{i}"] = ind
        
        # Add data source filter
        if data_sources and len(data_sources) > 0:
            placeholders = ", ".join([f":source_{i}" for i in range(len(data_sources))])
            query += f" AND source IN ({placeholders})"
            for i, src in enumerate(data_sources):
                params[f"source_{i}"] = src
        
        # Order by time (no limit - controlled by 7-day time range restriction)
        query += " ORDER BY time ASC"
        
        logging.debug(f"Executing series_data query with params: {params}")
        
        results = await execute_query(query, params=params)
        
        return results or []
    
    @staticmethod
    async def _query_summary_data(
        user_id: str,
        start_date: datetime,
        end_date: datetime,
        indicators: Optional[List[str]],
        data_sources: Optional[List[str]],
    ) -> List[Dict[str, Any]]:
        """
        Query th_series_data table (aggregated health data)
        
        Args:
            user_id: User ID
            start_date: Start date
            end_date: End date
            indicators: List of indicators to filter
            data_sources: List of data sources to filter
            
        Returns:
            List of data records with standard indicator information
        """
        query = """
        SELECT 
            t1.id,
            t1.user_id,
            t1.indicator,
            t1.value,
            t1.start_time,
            t1.end_time,
            t1.source,
            t1.source_table,
            t1.comment,
            t1.create_time,
            t1.update_time,
            t2.standard_indicator,
            t2.category_group,
            t2.category,
            t2.unit
        FROM theta_ai.th_series_data t1
        LEFT JOIN theta_ai.th_series_dim t2 ON t1.indicator = t2.original_indicator
        WHERE t1.user_id = :user_id
          AND t1.start_time >= :start_date
          AND t1.end_time <= :end_date
          AND t1.deleted = 0
        """
        
        params = {
            "user_id": user_id,
            "start_date": start_date,
            "end_date": end_date,
        }
        
        # Add indicator filter
        if indicators and len(indicators) > 0:
            placeholders = ", ".join([f":indicator_{i}" for i in range(len(indicators))])
            query += f" AND t1.indicator IN ({placeholders})"
            for i, ind in enumerate(indicators):
                params[f"indicator_{i}"] = ind
        
        # Add data source filter
        if data_sources and len(data_sources) > 0:
            placeholders = ", ".join([f":source_{i}" for i in range(len(data_sources))])
            query += f" AND t1.source IN ({placeholders})"
            for i, src in enumerate(data_sources):
                params[f"source_{i}"] = src
        
        # Order by start_time and limit
        query += " ORDER BY t1.start_time ASC LIMIT 5000"
        
        logging.debug(f"Executing th_series_data query with params: {params}")
        
        results = await execute_query(query, params=params)
        
        return results or []
    
    @staticmethod
    async def get_user_data_sources(user_id: str) -> List[Dict[str, Any]]:
        """
        Get all data sources for a specific user
        
        Args:
            user_id: User ID
            
        Returns:
            List of data sources with record counts
        """
        query = """
        SELECT 
            source,
            COUNT(*) as record_count,
            MIN(time) as earliest_data,
            MAX(time) as latest_data
        FROM theta_ai.series_data
        WHERE user_id = :user_id
        GROUP BY source
        ORDER BY record_count DESC
        """
        
        logging.info(f"Querying data sources for user {user_id}")
        
        results = await execute_query(query, params={"user_id": user_id})
        
        return results or []
    
    @staticmethod
    async def get_user_indicators(user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get all indicators for a specific user
        
        Args:
            user_id: User ID
            limit: Maximum number of indicators to return
            
        Returns:
            List of indicators with metadata
        """
        query = """
        SELECT 
            t1.indicator,
            t2.standard_indicator,
            t2.category_group,
            t2.category,
            t2.unit,
            COUNT(*) as record_count,
            MIN(t1.time) as earliest_data,
            MAX(t1.time) as latest_data
        FROM theta_ai.series_data t1
        LEFT JOIN theta_ai.th_series_dim t2 ON t1.indicator = t2.original_indicator
        WHERE t1.user_id = :user_id
        GROUP BY t1.indicator, t2.standard_indicator, t2.category_group, t2.category, t2.unit
        ORDER BY record_count DESC
        LIMIT :limit
        """
        
        logging.info(f"Querying indicators for user {user_id} (limit: {limit})")
        
        results = await execute_query(query, params={"user_id": user_id, "limit": limit})
        
        return results or []

