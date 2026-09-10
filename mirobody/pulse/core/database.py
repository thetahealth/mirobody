"""
Core database service module

Provides base classes for database operations shared by all Platforms and Providers
"""

import logging

from abc import ABC
from typing import Any

from ...utils import execute_query
from ...utils.db import engine_for

from sqlalchemy import text

logger = logging.getLogger(__name__)


class CacheableDatabaseService(ABC):
    """`execute_query` plus a small TTL cache, for the management queries.

    This was a three-level hierarchy — `BaseDatabaseService` (query + insert/
    update/delete/table-info builders) → `CacheableDatabaseService` (cache) →
    `ManageDatabaseService` — with exactly one leaf and no caller of the
    generic builders. Folded into the one class the leaf actually uses.
    """

    def __init__(self, db_config=None, cache_ttl: int = 300):
        self.db_config = db_config
        self.cache_ttl = cache_ttl
        self._cache: dict[str, Any] = {}
        self._cache_timestamps: dict[str, float] = {}

    async def execute_query(
            self,
            query: str,
            params: dict[str, Any] | None = None,
            db_config: Any | None = None,
    ) -> list[dict[str, Any]]:
        try:
            result = await execute_query(
                query=query, params=params or {}, db_config=db_config or self.db_config or ""
            )
            return result or []
        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
            raise

    async def execute_query_with_session_params(
            self,
            query: str,
            params: dict[str, Any] | None = None,
            session_params: list[str] | None = None,
    ) -> list[dict[str, Any]]:
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

        engine = engine_for(db_config)

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
            logger.info(logged_query, extra={"records": len(result), "time_cost": elapsed})
            return result
        except Exception as e:
            if conn:
                await conn.rollback()
            logger.error(f"Database query with session params failed: {str(e)}")
            raise
        finally:
            if conn:
                await conn.close()


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

    def _get_cache(self, cache_key: str) -> Any | None:
        """Get cache"""
        if self._is_cache_valid(cache_key):
            return self._cache.get(cache_key)
        return None

    async def cached_query(
            self,
            cache_key: str,
            query: str,
            params: dict[str, Any] | None = None,
            use_cache: bool = True,
    ) -> list[dict[str, Any]]:
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
                logger.info(f"Cache hit for key: {cache_key}")
                return cached_result

        # Cache miss, execute query
        result = await self.execute_query(query, params)

        if use_cache:
            self._set_cache(cache_key, result)
            logger.info(f"Cache set for key: {cache_key}")

        return result


class ManageDatabaseService(CacheableDatabaseService):
    """
    Management Database Service Class

    Specialized for handling database operations related to indicator management
    """

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
            from ..standardize.indicators_info import is_summary_indicator
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

        # Verify update result. `indicator_type` is in scope and must be forwarded:
        # without it the count falls back to a heuristic and can read the series
        # table for a summary indicator (and vice versa), reporting 0 for a rename
        # that succeeded.
        verify_result = await self.get_existing_indicator_count(
            new_indicator, source, indicator_type
        )
        return verify_result

    async def get_user_provider_stats_cached(self, user_id: str) -> dict[str, dict[str, Any]]:
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
