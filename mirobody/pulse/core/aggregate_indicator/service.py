"""
Aggregate Indicator Service

Pure business logic for aggregate indicator calculation.
No longer manages locks, timestamps, or stats caching - these are handled by Task layer.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .aggregators import SQLAggregator, AggregatorProtocol
from .database_service import AggregateDatabaseService
from .rule_generator import get_rules_by_source_indicator
from ..fhir_mapping import get_fhir_id, FhirMapping
from ..indicators_info import StandardIndicator


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

            # Register missing FHIR indicators BEFORE saving, then backfill fhir_id
            if all_summaries:
                await self._register_missing_fhir_indicators()
                self._backfill_fhir_ids(all_summaries)

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

    @staticmethod
    def _backfill_fhir_ids(summaries: List[Dict[str, Any]]):
        """Re-fill fhir_id on summaries that were None (cache was updated by register_missing)."""
        backfilled = 0
        for summary in summaries:
            if summary.get("fhir_id") is None:
                fhir_id = get_fhir_id(summary.get("indicator", ""))
                if fhir_id:
                    summary["fhir_id"] = fhir_id
                    backfilled += 1
        if backfilled:
            logging.info(f"[FhirMapping] Backfilled {backfilled} fhir_ids before save")

    async def _register_missing_fhir_indicators(self):
        """Register any pending FHIR indicators (auto_register mode only)."""
        try:
            instance = FhirMapping.get_instance()
            if instance is None or not instance.get_pending():
                return

            # Build indicator_info_map covering both source and aggregated indicator names
            info_map = {}
            for ind in StandardIndicator:
                info = ind.value
                if not info.name:
                    continue
                source_info = {
                    "short_name": info.name_zh or info.name,
                    "description": info.description or "",
                    "unit": info.standard_unit or "",
                }
                # Map source indicator name (e.g. "heartRates")
                info_map[info.name] = source_info

                # Map all aggregated indicator names (e.g. "dailyAvgHeartRates")
                rules = get_rules_by_source_indicator(info.name)
                for rule in rules:
                    # Get unit from aggregator (uses HHMM in comments)
                    raw_unit = self.aggregator._get_aggregation_unit(info.name, rule.aggregation_type) if hasattr(self.aggregator, '_get_aggregation_unit') else source_info['unit']
                    # For fhir_indicators, use standard HH:MM format (not HHMM)
                    fhir_unit = raw_unit.replace('HHMM', 'HH:MM')
                    agg_info = {
                        "short_name": f"{source_info['short_name']}({rule.aggregation_type})" if info.name_zh else rule.target_indicator,
                        "description": f"{info.description or info.name} - {rule.aggregation_type} aggregation",
                        "unit": fhir_unit,
                    }
                    info_map[rule.target_indicator] = agg_info

            await instance.register_missing(info_map)
        except Exception as e:
            logging.warning(f"[AggregateIndicator] FHIR registration skipped: {e}")

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

        # Register missing FHIR indicators BEFORE saving, then backfill fhir_id
        if all_summaries:
            await self._register_missing_fhir_indicators()
            self._backfill_fhir_ids(all_summaries)

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

