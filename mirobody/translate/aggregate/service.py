"""
Aggregate Indicator Service

Pure business logic for aggregate indicator calculation.
No longer manages locks, timestamps, or stats caching - these are handled by Task layer.
"""

import logging
import time
from datetime import datetime
from typing import Any

from .aggregators import SQLAggregator, AggregatorProtocol
from .database_service import AggregateDatabaseService

logger = logging.getLogger(__name__)


class AggregateIndicatorService:
    """
    Service for aggregate indicator calculation - Pure business logic
    
    Responsibilities:
    - Aggregation calculation logic
    - Database operations
    
    NOT responsible for:
    - Locks (handled by Scheduler via distributed_lock)
    - Timestamps (handled by Task via PullTask base class)
    - Stats caching (handled by Task via PullTask base class)
    """

    def __init__(
            self,
            aggregator: AggregatorProtocol | None = None,
            db_service: AggregateDatabaseService | None = None,
    ):
        """
        Initialize service with dependency injection
        
        Args:
            aggregator: Aggregator implementation (default: SQLAggregator)
            db_service: Database service (default: AggregateDatabaseService)
        """
        self.db_service = db_service or AggregateDatabaseService()
        self.aggregator = aggregator or SQLAggregator()

        logger.info(
            f"Initialized AggregateIndicatorService with {type(self.aggregator).__name__}"
        )

    async def process_incremental(
        self,
        last_timestamp: float | None = None,
        user_id: str | None = None
    ) -> dict[str, Any]:
        """
        Main incremental processing function - Pure business logic

        Args:
            last_timestamp: Last processing timestamp (float, seconds with
                sub-second precision; provided by Task layer)
            user_id: Optional user ID filter (None = all users)

        Returns:
            Dict with processing results:
            {
                "status": "success" | "no_data" | "error",
                "mode": "normal" | "cold_start",
                "summaries_created": int,
                "users_affected": int,
                "execution_time_ms": float,
                "new_timestamp": float  # For Task to update cache
            }
        """
        start_time = time.time()

        try:
            # Determine mode and fallback timestamp if needed
            if last_timestamp is None:
                last_timestamp = time.time() - 86400
                mode = "cold_start"
            else:
                mode = "normal"

            logger.info(
                f"[AggregateIndicator] Processing mode={mode}, "
                f"last_timestamp={last_timestamp}"
            )

            # Business logic: Get trigger tasks
            tasks = await self.aggregator.get_trigger_tasks(
                since_timestamp=last_timestamp
            )

            if not tasks:
                logger.info("[AggregateIndicator] No trigger tasks found")
                return {"status": "no_data", "mode": mode}

            logger.info(f"[AggregateIndicator] Found {len(tasks)} trigger tasks")

            # Business logic: Calculate aggregations
            all_summaries = await self.aggregator.calculate_batch_aggregations(tasks)

            # Business logic: Save to database
            if all_summaries:
                save_success = await self.db_service.batch_save_summary_data(
                    all_summaries
                )
                if not save_success:
                    logger.error("[AggregateIndicator] Failed to save data")
                    return {"status": "save_failed", "mode": mode}

                # Election runs HERE, once, on the write side: which source a
                # day publishes from is one decision, and answering it at read
                # time in two places is how a chat answer and a dashboard came
                # to show two numbers for the same Tuesday. See election.py.
                await self._elect_written_days(all_summaries)

            # Calculate new timestamp for Task to cache.
            # Keep sub-second precision: series_data.update_time is
            # millisecond-precise, and truncating the cursor to an int
            # second would roll it backwards within the same second,
            # causing the next run to re-process the same batch.
            new_timestamp = max(
                task.update_time.timestamp() for task in tasks
            )

            # Calculate execution time
            execution_time_ms = (time.time() - start_time) * 1000

            logger.info(
                f"[AggregateIndicator] Completed: "
                f"{len(all_summaries)} summaries created, "
                f"{len({task.user_id for task in tasks})} users affected, "
                f"execution_time={execution_time_ms:.1f}ms"
            )

            return {
                "status": "success",
                "mode": mode,
                "summaries_created": len(all_summaries),
                "users_affected": len({task.user_id for task in tasks}),
                "execution_time_ms": execution_time_ms,
                "new_timestamp": new_timestamp  # Task will cache this
            }

        except Exception as e:
            logger.error(f"[AggregateIndicator] Error during processing: {e}")
            return {"status": "error", "error": str(e)}

    async def recalculate_date_range(
            self,
            start_date: datetime,
            end_date: datetime,
            user_id: str | None = None
    ) -> dict[str, Any]:
        """
        Recalculate aggregations for a specific date range
        
        This method handles historical data processing by delegating to the aggregator.
        
        Args:
            start_date: Start date for recalculation
            end_date: End date for recalculation
            user_id: Optional user ID filter
            
        Returns:
            Dict with processing results
        """
        # Guard against unbounded all-users backfills. 30 days is the
        # chunk size used internally by calculate_time_range_aggregations;
        # beyond this the DB load grows linearly with active user count.
        if user_id is None:
            days_span = (end_date - start_date).days + 1
            if days_span > 30:
                return {
                    "status": "error",
                    "error": (
                        f"Range {days_span} days exceeds 30-day limit for "
                        f"all-users backfill. Narrow the range or pass user_id."
                    ),
                }

        logger.info(
            f"Starting historical recalculation: {start_date.isoformat()} to {end_date.isoformat()}, "
            f"user={user_id or 'all'}"
        )

        # Delegate to aggregator for time range processing
        all_summaries = await self.aggregator.calculate_time_range_aggregations(
            start_date=start_date,
            end_date=end_date,
            user_id=user_id
        )

        # Save summaries
        if all_summaries:
            save_success = await self.db_service.batch_save_summary_data(all_summaries)
            if not save_success:
                return {"status": "error", "error": "Failed to save summary data"}
            await self._elect_written_days(all_summaries)

        logger.info(
            f"Historical recalculation completed: {len(all_summaries)} summaries created"
        )

        return {
            "status": "success",
            "summaries_created": len(all_summaries),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }


    async def _elect_written_days(self, summaries: list[dict]) -> None:
        """Re-elect every (person, day) this pass wrote.

        Scoped to what changed rather than to the whole history: a person's
        day is a few hundred cells, and re-electing a year on every four-minute
        tick would be a background job pretending to be an incremental one.

        Failure is logged and swallowed. An unelected day still ANSWERS (the
        readers fall back to the newest row and say so in the provenance) and
        an aggregation pass that refuses to finish because of it would be a
        worse outcome than a day that is merely not yet arbitrated.
        """
        from .election import elect_range

        by_user: dict[str, list] = {}
        for row in summaries:
            # A summary row already IS a day: its start is local 00:00 of the
            # day it summarises, so the date is the day.
            start = row.get("start_time")
            day = start.date() if isinstance(start, datetime) else None
            if day is not None:
                by_user.setdefault(str(row.get("user_id")), []).append(day)
        for user_id, days in by_user.items():
            try:
                await elect_range(user_id, min(days), max(days))
            except Exception as e:
                logger.warning(
                    "[AggregateIndicator] election skipped for one subject: error_type=%s", type(e).__name__
                )

    # ========== Redis Operations ==========

