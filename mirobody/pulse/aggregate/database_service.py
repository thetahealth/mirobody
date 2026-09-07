"""
Database Service for Aggregate Indicator

Handles database operations for aggregate indicator:
- Batch saving summary data to th_series_data (UPSERT)
"""

import logging

from typing import Any

from ..readings import upsert_readings

logger = logging.getLogger(__name__)


class AggregateDatabaseService:
    """Database service for aggregate indicator operations"""

    def __init__(self):
        """Initialize database service"""

    async def batch_save_summary_data(
            self,
            summary_records: list[dict[str, Any]],
            batch_size: int = 1000
    ) -> bool:
        """
        Batch save summary records to th_series_data through `pulse/readings.py`
        (`on_conflict="update"`: idempotent, a collision replaces the row).
        
        Args:
            summary_records: List of summary records to save
            batch_size: Number of records per batch
            
        Returns:
            True if successful, False otherwise
        """
        if not summary_records:
            logger.info("No summary records to save")
            return True

        try:
            # A re-aggregation re-sends the truth: a collision replaces the row.
            # `anchored`: these rows already ARE days — the aggregator writes each
            # at local 00:00:00–23:59:59 of the day it summarises, including the
            # sleep family, whose 18:00 window was applied when the window was
            # chosen. Pushing them through it again would file every night a day
            # early (see pulse/readings.py::day_key).
            total_processed = await upsert_readings(
                summary_records, on_conflict="update", anchored=True, batch_size=batch_size
            )
            logger.info(
                f"Successfully saved {total_processed} summary records to th_series_data"
            )
            return True

        except Exception as e:
            logger.error(f"Error batch saving summary records: {e}")
            return False
