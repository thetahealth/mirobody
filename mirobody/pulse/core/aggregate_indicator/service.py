"""
Aggregate Indicator Service

Pure business logic for aggregate indicator calculation.
No longer manages locks, timestamps, or stats caching - these are handled by Task layer.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional

from .aggregators import SQLAggregator, AggregatorProtocol
from .database_service import AggregateDatabaseService


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
            aggregator: Optional[AggregatorProtocol] = None,
            db_service: Optional[AggregateDatabaseService] = None,
    ):
        """
        Initialize service with dependency injection
        
        Args:
            aggregator: Aggregator implementation (default: SQLAggregator)
            db_service: Database service (default: AggregateDatabaseService)
        """
        self.db_service = db_service or AggregateDatabaseService()
        self.aggregator = aggregator or SQLAggregator()

        logging.info(
            f"Initialized AggregateIndicatorService with {type(self.aggregator).__name__}"
        )

    async def process_incremental(
        self,
        last_timestamp: Optional[int] = None,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Main incremental processing function - Pure business logic
        
        Args:
            last_timestamp: Last processing timestamp (provided by Task layer)
            user_id: Optional user ID filter (None = all users)
            
        Returns:
            Dict with processing results:
            {
                "status": "success" | "no_data" | "error",
                "mode": "normal" | "cold_start",
                "summaries_created": int,
                "users_affected": int,
                "execution_time_ms": float,
                "new_timestamp": int  # For Task to update cache
            }
        """
        start_time = time.time()

        try:
            # Determine mode and fallback timestamp if needed
            if last_timestamp is None:
                last_timestamp = int(time.time()) - 86400
                mode = "cold_start"
            else:
                mode = "normal"

            logging.info(
                f"[AggregateIndicator] Processing mode={mode}, "
                f"last_timestamp={last_timestamp}"
            )

            # Business logic: Get trigger tasks
            tasks = await self.aggregator.get_trigger_tasks(
                since_timestamp=last_timestamp
            )

            if not tasks:
                logging.info("[AggregateIndicator] No trigger tasks found")
                return {"status": "no_data", "mode": mode}

            logging.info(f"[AggregateIndicator] Found {len(tasks)} trigger tasks")

            # Business logic: Calculate aggregations
            all_summaries = await self.aggregator.calculate_batch_aggregations(tasks)

            # Business logic: Save to database
            if all_summaries:
                save_success = await self.db_service.batch_save_summary_data(
                    all_summaries
                )
                if not save_success:
                    logging.error("[AggregateIndicator] Failed to save data")
                    return {"status": "save_failed", "mode": mode}

            # Calculate new timestamp for Task to cache
            new_timestamp = max(
                int(task.update_time.timestamp()) for task in tasks
            )

            # Calculate execution time
            execution_time_ms = (time.time() - start_time) * 1000

            logging.info(
                f"[AggregateIndicator] Completed: "
                f"{len(all_summaries)} summaries created, "
                f"{len(set(task.user_id for task in tasks))} users affected, "
                f"execution_time={execution_time_ms:.1f}ms"
            )

            return {
                "status": "success",
                "mode": mode,
                "summaries_created": len(all_summaries),
                "users_affected": len(set(task.user_id for task in tasks)),
                "execution_time_ms": execution_time_ms,
                "new_timestamp": new_timestamp  # Task will cache this
            }

        except Exception as e:
            logging.error(f"[AggregateIndicator] Error during processing: {e}")
            return {"status": "error", "error": str(e)}

    async def recalculate_date_range(
            self,
            start_date: datetime,
            end_date: datetime,
            user_id: Optional[str] = None
    ) -> Dict[str, Any]:
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
        logging.info(
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

        logging.info(
            f"Historical recalculation completed: {len(all_summaries)} summaries created"
        )

        return {
            "status": "success",
            "summaries_created": len(all_summaries),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }

    # ========== Redis Operations ==========

