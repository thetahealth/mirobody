"""
Derived Calculation Task (TH-174 W2.2)

PullTask that runs DerivedAggregator on a schedule.
Independent from AggregateIndicatorTask.
"""

import logging
from datetime import datetime
from typing import Dict

from ..scheduler import PullTask, ScheduleType
from .derived_aggregator import DerivedAggregator


class DerivedCalculationTask(PullTask):
    """Computes derived indicators from th_series_data daily summaries."""

    def __init__(self):
        super().__init__(
            provider_slug="derived_indicator",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=360,  # Check every 6 hours
            execution_interval_hours=6.0,
            lock_duration_hours=1.0,
        )
        self.aggregator = DerivedAggregator()

    async def execute(self) -> bool:
        try:
            logging.info("[DerivedCalculationTask] Starting execution...")

            result = await self.aggregator.process(lookback_days=90)

            stats = {
                "executed_at": datetime.now().isoformat(),
                "total_computed": result.get("total_computed", 0),
                "total_skipped": result.get("total_skipped", 0),
                "by_rule": result.get("by_rule", {}),
            }
            await self.save_task_stats(stats)

            logging.info(
                f"[DerivedCalculationTask] Done: {stats['total_computed']} computed, "
                f"{stats['total_skipped']} skipped"
            )
            return True

        except Exception as e:
            logging.error(f"[DerivedCalculationTask] Execution error: {e}")
            return False

    async def get_task_info(self) -> Dict:
        full_status = await self.get_full_status()
        full_status.update({
            "task_name": "Derived Indicator Calculation",
            "description": "Compute derived indicators from daily summaries",
            "execution_frequency": "Every 6 hours",
            "rules_count": len(self.aggregator.rules),
        })
        return full_status
