"""The scheduled job that runs DerivedAggregator, apart from the aggregation job."""

import logging

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.scheduler import PullTask, ScheduleType
from .rules import DerivedAggregator

logger = logging.getLogger(__name__)


class DerivedCalculationTask(PullTask):
    """Computes derived indicators from the stored daily summaries."""

    def __init__(self):
        super().__init__(
            provider_slug="derived_indicator",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=360,  # Check every 6 hours
            execution_interval_hours=6.0,
        )
        self.aggregator = DerivedAggregator()

    async def execute(self) -> bool:
        try:
            logger.info("[DerivedCalculationTask] Starting execution...")

            result = await self.aggregator.process(lookback_days=90)

            computed_count, skipped_count = result.get("total_computed", 0), result.get("total_skipped", 0)
            logger.info(f"[DerivedCalculationTask] Done: {computed_count} computed, {skipped_count} skipped")
            return True

        except Exception as e:
            logger.error("derived task failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
            return False


_task = None


async def start_derived_scheduler() -> None:
    """Register the derived-quantity job with the shared scheduler.

    Separate from the aggregation job it used to ride along with: aggregation
    produces a day's number for a measured quantity, this produces quantities
    nothing measured, and a caller should be able to run one without the other.
    """
    global _task
    from mirobody.utils.scheduler import scheduler

    if _task is not None:
        logger.warning("Derived indicator task already registered")
        return
    _task = DerivedCalculationTask()
    scheduler.register_task(_task)
    logger.info("Derived indicator task registered successfully")
