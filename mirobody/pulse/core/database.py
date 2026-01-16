"""
Core database service module

Provides base classes for database operations shared by all Platforms and Providers
"""

import logging

from abc import ABC
from typing import Any, Dict, List, Optional

from ...utils import execute_query


class BaseDatabaseService(ABC):
    """
    Base database service class

    Provides common database operation methods, subclasses can inherit and extend
    """

    def __init__(self, db_config=None):
        """
        Initialize database service

        Args:
            db_config: Database configuration, defaults to core configuration
        """
        self.db_config = db_config

    async def execute_query(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None,
            db_config: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        try:
            result = await execute_query(
                query=query, params=params or {}, db_config=db_config or self.db_config
            )
            return result or []
        except Exception as e:
            logging.error(f"Database query failed: {str(e)}")
            raise

    async def execute_insert(
            self,
            table: str,
            data: Dict[str, Any],
            schema: str = "theta_ai",
            returning: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Execute insert operation

        Args:
            table: Table name
            data: Insert data
            schema: Database schema
            returning: Returning fields

        Returns:
            Insert result
        """
        try:
            # Build insert SQL
            columns = list(data.keys())
            placeholders = [f":{col}" for col in columns]

            query = f"""
                INSERT INTO {schema}.{table} ({", ".join(columns)})
                VALUES ({", ".join(placeholders)})
            """

            if returning:
                query += f" RETURNING {returning}"

            result = await self.execute_query(query, data)
            return result[0] if result else None

        except Exception as e:
            logging.error(f"Database insert failed: {str(e)}")
            raise

    async def execute_update(
            self,
            table: str,
            data: Dict[str, Any],
            where_clause: str,
            where_params: Dict[str, Any],
            schema: str = "theta_ai",
    ) -> int:
        """
        Execute update operation

        Args:
            table: Table name
            data: Update data
            where_clause: WHERE clause
            where_params: WHERE parameters
            schema: Database schema

        Returns:
            Number of affected rows
        """
        try:
            # Build update SQL
            set_clauses = [f"{col} = :{col}" for col in data.keys()]

            query = f"""
                UPDATE {schema}.{table} 
                SET {", ".join(set_clauses)}
                WHERE {where_clause}
            """

            # Merge parameters
            all_params = {**data, **where_params}

            result = await self.execute_query(query, all_params)
            return len(result) if result else 0

        except Exception as e:
            logging.error(f"Database update failed: {str(e)}")
            raise

    async def execute_delete(
            self,
            table: str,
            where_clause: str,
            where_params: Dict[str, Any],
            schema: str = "theta_ai",
    ) -> int:
        """
        Execute delete operation

        Args:
            table: Table name
            where_clause: WHERE clause
            where_params: WHERE parameters
            schema: Database schema

        Returns:
            Number of affected rows
        """
        try:
            query = f"""
                DELETE FROM {schema}.{table} 
                WHERE {where_clause}
            """

            result = await self.execute_query(query, where_params)
            return len(result) if result else 0

        except Exception as e:
            logging.error(f"Database delete failed: {str(e)}")
            raise

    async def check_table_exists(self, table: str, schema: str = "theta_ai") -> bool:
        """
        Check if table exists

        Args:
            table: Table name
            schema: Database schema

        Returns:
            Whether table exists
        """
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = :schema 
                    AND table_name = :table
                )
            """
            result = await self.execute_query(query, {"schema": schema, "table": table})
            return result[0]["exists"] if result else False
        except Exception as e:
            logging.error(f"Check table existence failed: {str(e)}")
            return False

    async def get_table_info(self, table: str, schema: str = "theta_ai") -> List[Dict[str, Any]]:
        """
        Get table information

        Args:
            table: Table name
            schema: Database schema

        Returns:
            List of table field information
        """
        try:
            query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_schema = :schema 
                AND table_name = :table
                ORDER BY ordinal_position
            """
            result = await self.execute_query(query, {"schema": schema, "table": table})
            return result or []
        except Exception as e:
            logging.error(f"Get table info failed: {str(e)}")
            return []


class CacheableDatabaseService(BaseDatabaseService):
    """
    Cacheable database service base class
    """

    def __init__(self, db_config=None, cache_ttl: int = 300):
        """
        Initialize cacheable database service

        Args:
            db_config: Database configuration
            cache_ttl: Cache time to live (seconds)
        """
        super().__init__(db_config)
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, float] = {}

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache is valid"""
        import time

        if cache_key not in self._cache_timestamps:
            return False
        return (time.time() - self._cache_timestamps[cache_key]) < self.cache_ttl

    def _set_cache(self, cache_key: str, value: Any) -> None:
        """Set cache"""
        import time

        self._cache[cache_key] = value
        self._cache_timestamps[cache_key] = time.time()

    def _get_cache(self, cache_key: str) -> Optional[Any]:
        """Get cache"""
        if self._is_cache_valid(cache_key):
            return self._cache.get(cache_key)
        return None

    def _clear_cache(self, pattern: Optional[str] = None) -> None:
        """Clear cache"""
        if pattern:
            # Clear caches matching pattern
            keys_to_remove = [key for key in self._cache.keys() if pattern in key]
            for key in keys_to_remove:
                self._cache.pop(key, None)
                self._cache_timestamps.pop(key, None)
        else:
            # Clear all caches
            self._cache.clear()
            self._cache_timestamps.clear()

    async def cached_query(
            self,
            cache_key: str,
            query: str,
            params: Optional[Dict[str, Any]] = None,
            use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Cached query support

        Args:
            cache_key: Cache key
            query: SQL query
            params: Query parameters
            use_cache: Whether to use cache

        Returns:
            Query result
        """
        if use_cache:
            cached_result = self._get_cache(cache_key)
            if cached_result is not None:
                logging.info(f"Cache hit for key: {cache_key}")
                return cached_result

        # Cache miss, execute query
        result = await self.execute_query(query, params)

        if use_cache:
            self._set_cache(cache_key, result)
            logging.info(f"Cache set for key: {cache_key}")

        return result


class ManageDatabaseService(CacheableDatabaseService):
    """
    Management Database Service Class
    
    Specialized for handling database operations related to indicator management
    """
    
    async def get_yearly_stats(self, start_time) -> List[Dict[str, Any]]:
        """
        Get yearly indicator statistics data from both series_data and th_series_data tables
        
        Args:
            start_time: Start time
            
        Returns:
            Statistics data list with indicator_type field
        """
        # Query series data (time-series indicators)
        series_query = """
        SELECT 
            COALESCE(source, 'unknown') as source,
            indicator,
            COUNT(DISTINCT user_id) as unique_users,
            COUNT(*) as total_records,
            MAX(time) as latest_time,
            'series' as indicator_type
        FROM theta_ai.series_data 
        WHERE time > :start_time 
        GROUP BY source, indicator 
        """

        # Query summary data (summary indicators)
        # Include both normal summary (source_table = '') and aggregate indicators (task_id = 'aggregate_indicator')
        summary_query = """
        SELECT 
            COALESCE(th.source, 'unknown') as source,
            th.indicator,
            COUNT(DISTINCT th.user_id) as unique_users,
            COUNT(*) as total_records,
            MAX(th.end_time) as latest_time,
            CASE 
                WHEN MAX(th.source_table) = 'series_data'
                THEN 'aggregate'
                ELSE 'summary'
            END as indicator_type
        FROM theta_ai.th_series_data th
        WHERE th.end_time > :start_time 
          AND th.deleted = 0
          AND th.source_table IN ('series_data', '')
          AND th.source IN (
              SELECT DISTINCT source 
              FROM theta_ai.series_data 
              WHERE source IS NOT NULL
          )
        GROUP BY th.source, th.indicator
        """

        # Combine both queries
        combined_query = f"""
        ({series_query})
        UNION ALL
        ({summary_query})
        ORDER BY total_records DESC
        """

        params = {"start_time": start_time}
        return await self.execute_query(combined_query, params)
    
    async def get_indicator_record_info(self, indicator: str, source: str, indicator_type: str = None) -> Optional[Dict[str, Any]]:
        """
        Get indicator record information from appropriate table based on indicator type
        
        Args:
            indicator: Indicator name
            source: Data source
            indicator_type: Indicator type ('series' or 'summary'), if None will use heuristic
            
        Returns:
            Record information dictionary or None
        """
        # Require explicit type when provided, otherwise use heuristic for backward compatibility
        if indicator_type is not None:
            if indicator_type not in ['series', 'summary']:
                raise ValueError(f"Invalid indicator_type '{indicator_type}'. Must be 'series' or 'summary'")
            is_summary = (indicator_type == 'summary')
        else:
            # Fallback to heuristic for backward compatibility
            from .indicators_info import is_summary_indicator
            is_summary = is_summary_indicator(indicator)
        
        if is_summary:
            # Query th_series_data for summary indicators
            query = """
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT user_id) as user_count,
                MIN(start_time) as earliest_time,
                MAX(end_time) as latest_time
            FROM theta_ai.th_series_data 
            WHERE indicator = :indicator AND source = :source AND deleted = 0 AND source_table = ''
            """
        else:
            # Query series_data for series indicators
            query = """
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT user_id) as user_count,
                MIN(time) as earliest_time,
                MAX(time) as latest_time
            FROM theta_ai.series_data 
            WHERE indicator = :indicator AND source = :source
            """

        params = {"indicator": indicator, "source": source}
        result = await self.execute_query(query, params)

        return result[0] if result and result[0]["record_count"] > 0 else None
    
    async def get_existing_indicator_count(self, indicator: str, source: str, indicator_type: str = None) -> int:
        """
        Get existing indicator record count from appropriate table based on indicator type
        
        Args:
            indicator: Indicator name
            source: Data source
            indicator_type: Indicator type ('series' or 'summary'), if None will use heuristic
            
        Returns:
            Record count
        """
        # Require explicit type when provided, otherwise use heuristic for backward compatibility
        if indicator_type is not None:
            if indicator_type not in ['series', 'summary']:
                raise ValueError(f"Invalid indicator_type '{indicator_type}'. Must be 'series' or 'summary'")
            is_summary = (indicator_type == 'summary')
        else:
            # Fallback to heuristic for backward compatibility
            from .indicators_info import is_summary_indicator
            is_summary = is_summary_indicator(indicator)
        
        if is_summary:
            # Query th_series_data for summary indicators
            query = """
            SELECT COUNT(*) as existing_count
            FROM theta_ai.th_series_data 
            WHERE indicator = :indicator AND source = :source AND deleted = 0 AND source_table = ''
            """
        else:
            # Query series_data for series indicators
            query = """
            SELECT COUNT(*) as existing_count
            FROM theta_ai.series_data 
            WHERE indicator = :indicator AND source = :source
            """

        params = {"indicator": indicator, "source": source}
        result = await self.execute_query(query, params)

        return result[0]["existing_count"] if result else 0
    
    async def update_indicator_name(self, old_indicator: str, new_indicator: str, source: str, indicator_type: str = None) -> int:
        """
        Update indicator name in appropriate table based on indicator type
        
        Args:
            old_indicator: Original indicator name
            new_indicator: New indicator name
            source: Data source
            indicator_type: Indicator type ('series' or 'summary'), if None will use heuristic
            
        Returns:
            Number of updated records
        """
        # Require explicit type, no fallback to heuristic
        if indicator_type is None:
            raise ValueError("indicator_type must be explicitly provided ('series' or 'summary')")
        
        if indicator_type not in ['series', 'summary']:
            raise ValueError(f"Invalid indicator_type '{indicator_type}'. Must be 'series' or 'summary'")
            
        is_summary = (indicator_type == 'summary')
        
        if is_summary:
            # Update th_series_data for summary indicators
            query = """
            UPDATE theta_ai.th_series_data 
            SET 
                indicator = :new_indicator,
                update_time = CURRENT_TIMESTAMP
            WHERE indicator = :old_indicator 
              AND source = :source
              AND deleted = 0
              AND source_table = ''
            """
        else:
            # Update series_data for series indicators
            query = """
            UPDATE theta_ai.series_data 
            SET 
                indicator = :new_indicator,
                update_time = CURRENT_TIMESTAMP
            WHERE indicator = :old_indicator 
              AND source = :source
            """

        params = {
            "new_indicator": new_indicator,
            "old_indicator": old_indicator,
            "source": source
        }

        await self.execute_query(query, params)

        # Verify update result
        verify_result = await self.get_existing_indicator_count(new_indicator, source)
        return verify_result

    async def get_user_provider_stats_cached(self, user_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Get user provider statistics with caching - one query for all providers
        
        Args:
            user_id: User ID
            
        Returns:
            Dict mapping source to aggregated statistics {source: {record_count, last_sync_time}}
        """
        cache_key = f"user_provider_stats_{user_id}"

        # Try to get from cache first
        cached_result = self._get_cache(cache_key)
        if cached_result is not None:
            return cached_result

        try:
            query = """
            SELECT 
                source,
                COUNT(*) as record_count,
                MAX(time) as last_sync_time
            FROM theta_ai.series_data 
            where user_id =:user_id
            GROUP BY source
            order by source desc
            """

            params = {"user_id": user_id}
            raw_results = await self.execute_query(query, params)

            # Convert to dict for easy lookup
            stats_dict = {}
            for row in raw_results:
                source = row.get("source")
                parts = source.split(".")
                source = ".".join(parts[:2])
                if source == "resmed" and "theta.theta_resmed" not in stats_dict:
                    source = "theta.theta_resmed"
                if source:
                    stats_dict[source] = {
                        "record_count": row.get("record_count", 0),
                        "last_sync_time": row.get("last_sync_time")
                    }
                if source == "vital.apple_health_kit" and "apple_health" not in stats_dict:
                    stats_dict["apple_health"] = stats_dict[source]

            # Cache the result for 5 minutes
            self._set_cache(cache_key, stats_dict)
            return stats_dict

        except Exception as e:
            logging.error(f"Error getting cached user provider stats for user {user_id}: {str(e)}")
            return {}
