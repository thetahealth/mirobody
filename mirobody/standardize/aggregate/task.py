"""
Aggregate Indicator Task

Implements PullTask interface to integrate with the unified scheduler.
Uses the base class's timestamp service for its incremental position.
"""

import logging

from mirobody.kernel.ops import is_driver_exception

from .service import AggregateIndicatorService
from mirobody.utils.scheduler import PullTask, ScheduleType

logger = logging.getLogger(__name__)


class AggregateIndicatorTask(PullTask):
    """
    Aggregate Indicator task implementation
    
    Inherits from PullTask to integrate with the unified scheduler system.
    Executes every 4 minutes to calculate aggregations from series_data.
    """

    def __init__(self):
        """
        Initialize aggregate indicator task
        """
        super().__init__(
            provider_slug="aggregate_indicator",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=4,  # Check every 4 minutes
            execution_interval_hours=4 / 60,  # Execute every 4 minutes
        )

        self.service = AggregateIndicatorService()

    async def execute(self) -> bool:
        """
        Execute aggregation calculation
        
        Uses base class abilities:
        - get_last_execution_timestamp() to get last processing position
        - update_last_execution_timestamp() to update position
        
        Returns:
            True if execution successful, False otherwise
        """
        try:
            logger.info("[AggregateIndicatorTask] Starting execution...")

            # Use base class ability: get last timestamp
            last_timestamp = await self.get_last_execution_timestamp()

            # Call service for pure business logic
            result = await self.service.process_incremental(
                last_timestamp=last_timestamp
            )

            status = result.get('status')

            if status == 'success':
                # Use base class ability: update timestamp
                new_timestamp = result.get('new_timestamp')
                if new_timestamp:
                    await self.update_last_execution_timestamp(new_timestamp)
                
                stats_dict = {
                    "summaries_created": result.get('summaries_created', 0),
                    "users_affected": result.get('users_affected', 0),
                    "execution_time_ms": result.get('execution_time_ms', 0),
                }

                logger.info(
                    f"[AggregateIndicatorTask] Completed successfully: "
                    f"{stats_dict['summaries_created']} summaries, "
                    f"{stats_dict['users_affected']} users, "
                    f"{stats_dict['execution_time_ms']:.1f}ms"
                )
                return True

            if status in ['no_data', 'skipped']:
                logger.info(f"[AggregateIndicatorTask] {status}")
                return True  # Not an error, just no work to do

            logger.error(f"[AggregateIndicatorTask] Failed: {status}")
            return False

        except Exception as e:
            logger.error("aggregate task failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
            return False
