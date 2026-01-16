"""
Aggregate Indicator Task

Implements PullTask interface to integrate with the unified scheduler.
Uses base class cache services for timestamp and stats management.
"""

import logging
from datetime import datetime
from typing import Dict

from .service import AggregateIndicatorService
from ..scheduler import PullTask, ScheduleType


class AggregateIndicatorTask(PullTask):
    """
    Aggregate Indicator task implementation
    
    Inherits from PullTask to integrate with the unified scheduler system.
    Executes every 5 minutes to calculate aggregations from series_data.
    """

    def __init__(self):
        """
        Initialize aggregate indicator task
        """
        super().__init__(
            provider_slug="aggregate_indicator",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=6,  # Check every 6 minutes
            execution_interval_hours=6 / 60,  # Execute every 6 minutes (0.1 hours)
            lock_duration_hours=18 / 60  # Lock for 10 minutes (0.3 hours)
        )

        self.service = AggregateIndicatorService()

    async def execute(self) -> bool:
        """
        Execute aggregation calculation
        
        Uses base class abilities:
        - get_last_execution_timestamp() to get last processing position
        - update_last_execution_timestamp() to update position
        - save_task_stats() to save execution statistics
        
        Returns:
            True if execution successful, False otherwise
        """
        try:
            logging.info("[AggregateIndicatorTask] Starting execution...")

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
                
                # Use base class ability: save stats
                # Define aggregate_indicator specific stats structure
                stats_dict = {
                    "executed_at": datetime.now().isoformat(),
                    "summaries_created": result.get('summaries_created', 0),
                    "users_affected": result.get('users_affected', 0),
                    "execution_time_ms": result.get('execution_time_ms', 0),
                    "mode": result.get('mode', 'normal')
                }
                await self.save_task_stats(stats_dict)

                logging.info(
                    f"[AggregateIndicatorTask] Completed successfully: "
                    f"{stats_dict['summaries_created']} summaries, "
                    f"{stats_dict['users_affected']} users, "
                    f"{stats_dict['execution_time_ms']:.1f}ms"
                )
                return True

            elif status in ['no_data', 'skipped']:
                logging.info(f"[AggregateIndicatorTask] {status}")
                return True  # Not an error, just no work to do

            else:
                logging.error(f"[AggregateIndicatorTask] Failed: {status}")
                return False

        except Exception as e:
            logging.error(f"[AggregateIndicatorTask] Execution error: {e}")
            return False

    async def get_task_info(self) -> Dict:
        """
        Get comprehensive task information
        
        Uses base class ability: get_full_status()
        
        Returns:
            Dictionary with task details including cached data
        """
        # Use base class async full status method
        full_status = await self.get_full_status()

        # Add aggregate-specific metadata
        full_status.update({
            "task_name": "Aggregate Indicator Calculation",
            "description": "Calculate summary indicators from series data",
            "execution_frequency": "Every 6 minutes",
        })

        return full_status
