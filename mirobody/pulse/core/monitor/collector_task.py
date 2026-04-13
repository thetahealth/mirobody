"""
Monitor Collector Tasks (TH-141)

Two PullTask implementations that feed the report tables:
- HourlyCollectorTask: platform_hourly_profile (every hour)
- DailyProfileTask: indicator_daily_profile (every 24 hours)
"""

import logging
from datetime import date, datetime, timedelta
from typing import Dict

from ..scheduler import PullTask, ScheduleType
from .collector_service import MonitorCollectorService


class HourlyCollectorTask(PullTask):
    """Collects hourly ingestion stats into platform_hourly_profile."""

    def __init__(self):
        super().__init__(
            provider_slug="monitor_hourly_collector",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=60,
            execution_interval_hours=1.0,
            lock_duration_hours=0.5,
        )
        self.service = MonitorCollectorService()

    async def execute(self) -> bool:
        try:
            # Recalculate hour slots with recently modified data
            # Looks at update_time in the last 2 hours, only recalculates slots within 7 days
            logging.info("[HourlyCollectorTask] Collecting changed hour slots")
            result = await self.service.collect_changed_hourly_stats(
                lookback_hours=2, max_days=7
            )

            stats = {
                "executed_at": datetime.now().isoformat(),
                "hours_recalculated": result.get("hours_recalculated", 0),
                "rows_upserted": result.get("rows_upserted", 0),
            }
            await self.save_task_stats(stats)

            logging.info(
                f"[HourlyCollectorTask] Done: {stats['hours_recalculated']} hours, "
                f"{stats['rows_upserted']} rows upserted"
            )
            return True

        except Exception as e:
            logging.error(f"[HourlyCollectorTask] Execution error: {e}")
            return False

    async def get_task_info(self) -> Dict:
        full_status = await self.get_full_status()
        full_status.update({
            "task_name": "Monitor Hourly Collector",
            "description": "Collect hourly platform ingestion stats from series_data",
            "execution_frequency": "Every 1 hour",
        })
        return full_status


class DailyProfileTask(PullTask):
    """Collects daily indicator quality profiles into indicator_daily_profile."""

    def __init__(self):
        super().__init__(
            provider_slug="monitor_daily_profile",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=360,
            execution_interval_hours=6.0,
            lock_duration_hours=2.0,
        )
        self.service = MonitorCollectorService()

    async def execute(self) -> bool:
        try:
            # Recalculate day slots with recently modified data
            # Looks at update_time in the last 12 hours, only recalculates slots within 30 days
            logging.info("[DailyProfileTask] Collecting changed day slots")
            result = await self.service.collect_changed_daily_profiles(
                lookback_hours=12, max_days=30
            )

            stats = {
                "executed_at": datetime.now().isoformat(),
                "days_recalculated": result.get("days_recalculated", 0),
                "rows_upserted": result.get("rows_upserted", 0),
            }
            await self.save_task_stats(stats)

            logging.info(
                f"[DailyProfileTask] Done: {stats['days_recalculated']} days, "
                f"{stats['rows_upserted']} rows upserted"
            )
            return True

        except Exception as e:
            logging.error(f"[DailyProfileTask] Execution error: {e}")
            return False

    async def get_task_info(self) -> Dict:
        full_status = await self.get_full_status()
        full_status.update({
            "task_name": "Monitor Daily Profile",
            "description": "Collect daily indicator quality profiles from series_data",
            "execution_frequency": "Every 24 hours",
        })
        return full_status
