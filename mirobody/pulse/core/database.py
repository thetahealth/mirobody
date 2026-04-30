"""
Core database service module

Provides base classes for database operations shared by all Platforms and Providers
"""

import logging

from abc import ABC
from typing import Any, Dict, List, Optional

from ...utils import execute_query
from ...utils.db import global_engines, global_config

from sqlalchemy import text


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

    async def execute_query_with_session_params(
            self,
            query: str,
            params: Optional[Dict[str, Any]] = None,
            session_params: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute query with SET LOCAL session parameters in the same transaction.

        Args:
            query: SQL query
            params: Query parameters
            session_params: List of SET LOCAL statements, e.g. ["SET LOCAL enable_seqscan = off"]
        """
        db_config = self.db_config or ""
        if not isinstance(db_config, str):
            db_config = ""

        global global_engines
        if db_config in global_engines:
            engine = global_engines[db_config]
        else:
            config = global_config()
            if not config:
                raise ValueError("no configuration found")
            engine = config.get_postgresql(db_config).get_async_engine()
            global_engines[db_config] = engine

        import time as _time
        conn = None
        try:
            start = _time.time()
            conn = await engine.connect()
            if session_params:
                for sp in session_params:
                    await conn.execute(text(sp))
            cur = await conn.execute(text(query), params or {})
            rows = cur.fetchall()
            result = [dict(row._mapping) for row in rows]
            await conn.commit()
            elapsed = round((_time.time() - start) * 1e3, 2)
            logged_query = " ".join(query.split())
            if len(logged_query) > 512:
                logged_query = logged_query[:512] + "..."
            logging.info(logged_query, extra={"records": len(result), "time_cost": elapsed})
            return result
        except Exception as e:
            if conn:
                await conn.rollback()
            logging.error(f"Database query with session params failed: {str(e)}")
            raise
        finally:
            if conn:
                await conn.close()

    async def execute_insert(
            self,
            table: str,
            data: Dict[str, Any],
            schema: str = "",
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
                INSERT INTO {table} ({", ".join(columns)})
                VALUES ({", ".join(placeholders)})
            """

            if returning:
                query += f" RETURNING {returning}"

            result = await self.execute_query(query, data)
            return result[0] if returning and result else None

        except Exception as e:
            logging.error(f"Database insert failed: {str(e)}")
            raise

    async def execute_update(
            self,
            table: str,
            data: Dict[str, Any],
            where_clause: str,
            where_params: Dict[str, Any],
            schema: str = "",
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
                UPDATE {table} 
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
            schema: str = "",
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
                DELETE FROM {table} 
                WHERE {where_clause}
            """

            result = await self.execute_query(query, where_params)
            return len(result) if result else 0

        except Exception as e:
            logging.error(f"Database delete failed: {str(e)}")
            raise

    async def check_table_exists(self, table: str, schema: str = "") -> bool:
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
                    WHERE table_name = :table
                )
            """
            result = await self.execute_query(query, {"table": table})
            return result[0]["exists"] if result else False
        except Exception as e:
            logging.error(f"Check table existence failed: {str(e)}")
            return False

    async def get_table_info(self, table: str, schema: str = "") -> List[Dict[str, Any]]:
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
                WHERE table_name = :table
                ORDER BY ordinal_position
            """
            result = await self.execute_query(query, {"table": table})
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
        # Uses series_data_time_idx (time DESC) for the WHERE clause.
        # COUNT(DISTINCT user_id) removed — it forced per-group hash sets causing OOM.
        series_query = """
        SELECT
            COALESCE(source, 'unknown') as source,
            indicator,
            COUNT(*) as total_records,
            MAX(time) as latest_time,
            'series' as indicator_type
        FROM series_data
        WHERE time > :start_time
        GROUP BY source, indicator
        """

        # Query summary data (summary indicators)
        # COUNT(DISTINCT user_id) removed — same OOM risk as above.
        # Seq Scan is optimal here: data fits in shared buffers (724ms), and no
        # existing index covers (end_time + source_table) well enough to beat it.
        summary_query = """
        SELECT
            COALESCE(th.source, 'unknown') as source,
            th.indicator,
            COUNT(*) as total_records,
            MAX(th.end_time) as latest_time,
            CASE
                WHEN MAX(th.source_table) = 'series_data'
                THEN 'aggregate'
                ELSE 'summary'
            END as indicator_type
        FROM th_series_data th
        WHERE th.end_time > :start_time
          AND th.source_table IN ('series_data', '')
        GROUP BY th.source, th.indicator
        """

        params = {"start_time": start_time}

        # Execute separately to avoid PostgreSQL OOM on simultaneous aggregation.
        # Use cached_query (TTL 300s) — this is a low-frequency management UI endpoint,
        # no need to hit two large tables on every request.

        # series_data: force Bitmap Heap Scan on series_data_time_idx (time DESC).
        # Default optimizer picks Seq Scan (15s full-table) because random_page_cost=4.0
        # overestimates I/O cost. With seqscan off, it uses Bitmap Scan (3s).
        # Note: random_page_cost=1.1 is worse here — it picks Index Scan (per-row
        # random lookup, 43s) instead of Bitmap Scan (batch sequential, 3s).
        series_cache_key = "yearly_stats_series"
        cached = self._get_cache(series_cache_key)
        if cached is not None:
            series_results = cached
        else:
            series_results = await self.execute_query_with_session_params(
                series_query, params,
                session_params=["SET LOCAL enable_seqscan = off"],
            )
            self._set_cache(series_cache_key, series_results)

        summary_results = await self.cached_query("yearly_stats_summary", summary_query, params)

        combined = (series_results or []) + (summary_results or [])
        combined.sort(key=lambda r: r.get("total_records", 0), reverse=True)
        return combined
    
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
            FROM th_series_data 
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
            FROM series_data 
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
            FROM th_series_data 
            WHERE indicator = :indicator AND source = :source AND deleted = 0 AND source_table = ''
            """
        else:
            # Query series_data for series indicators
            query = """
            SELECT COUNT(*) as existing_count
            FROM series_data 
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
            UPDATE th_series_data 
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
            UPDATE series_data 
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
        # TODO: Migrate to daily_summary table once available.
        # The original query scans all series_data rows for the user (~750K+ rows),
        # causing 15-17s response times. Returning empty dict until daily_summary
        # provides pre-aggregated per-user source stats.
        return {}
