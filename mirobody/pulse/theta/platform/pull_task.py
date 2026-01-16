"""
Pull task implementation for Theta providers
"""

import logging

from typing import Dict, Optional

from mirobody.pulse.core.scheduler import PullTask, ScheduleType
from .base import BaseThetaProvider

# Provider execution interval configuration (hours)
PROVIDER_EXECUTION_INTERVALS = {
    "theta_renpho": 24.0,  # Renpho: execute once every 24 hours
    "theta_vital": 6.0,  # Vital: execute once every 6 hours
    "theta_cgm": 1.0,  # CGM: execute once every 1 hour
    "theta_whoop": 24.0,  # Whoop: execute once every 1 hour
    "default": 1.0,  # Default: execute once every 1 hour
}

# Provider lock duration configuration (hours)
PROVIDER_LOCK_DURATIONS = {
    "theta_renpho": 23.5,  # Renpho: lock for 23.5 hours
    "theta_vital": 5.5,  # Vital: lock for 5.5 hours
    "theta_cgm": 0.5,  # CGM: lock for 0.5 hours
    "theta_whoop": 23.5,  # Whoop: lock for 0.5 hours
    "default": 0.5,  # Default: lock for 0.5 hours
}


class ThetaProviderPullTask(PullTask):
    """
    Theta Provider Pull Task

    Create corresponding pull task for each Theta Provider, supporting:
    - Configurable execution intervals (different frequencies for different providers)
    - Distributed locks (prevent duplicate execution in multi-docker instances)
    - Force execution mode
    - Detailed status monitoring
    """

    def __init__(
        self,
        provider: BaseThetaProvider,
        schedule_type: ScheduleType = ScheduleType.HOURLY,
        custom_execution_interval: Optional[float] = None,
        custom_lock_duration: Optional[float] = None,
    ):
        """
        Initialize Pull Task

        Args:
            provider: Theta Provider instance
            schedule_type: Schedule type, defaults to hourly scheduling
            custom_execution_interval: Custom execution interval (hours), overrides default configuration
            custom_lock_duration: Custom lock duration (hours), overrides default configuration
        """
        self.provider = provider

        # Get provider execution interval configuration
        execution_interval = custom_execution_interval or self._get_execution_interval()
        lock_duration = custom_lock_duration or self._get_lock_duration()

        super().__init__(
            provider_slug=provider.info.slug,
            schedule_type=schedule_type,
            execution_interval_hours=execution_interval,
            lock_duration_hours=lock_duration,
        )

        logging.info(
            f"Initialized pull task for {self.provider_slug}: "
            f"execution_interval={execution_interval}h, "
            f"lock_duration={lock_duration}h, "
            f"schedule_type={schedule_type.value}"
        )

    def _get_execution_interval(self) -> float:
        """Get provider execution interval configuration"""
        return PROVIDER_EXECUTION_INTERVALS.get(self.provider.info.slug, PROVIDER_EXECUTION_INTERVALS["default"])

    def _get_lock_duration(self) -> float:
        """Get provider lock duration configuration"""
        return PROVIDER_LOCK_DURATIONS.get(self.provider.info.slug, PROVIDER_LOCK_DURATIONS["default"])

    async def execute(self) -> bool:
        """
        Execute pull task

        Returns:
            Whether execution was successful
        """
        try:
            logging.info(
                f"Starting pull task for provider: {self.provider_slug} "
                f"(execution_interval: {self.execution_interval_hours}h)"
            )

            # Call provider's pull_and_push method
            success = await self.provider.pull_and_push()

            if success:
                logging.info(f"Pull task completed successfully for provider: {self.provider_slug}")
            else:
                logging.error(f"Pull task failed for provider: {self.provider_slug}")

            return success

        except Exception as e:
            logging.error(f"Pull task error for provider {self.provider_slug}: {str(e)}")
            return False

    def get_provider_config(self) -> Dict:
        """Get provider configuration information"""
        return {
            "provider_slug": self.provider_slug,
            "provider_name": getattr(self.provider.info, "name", "Unknown"),
            "execution_interval_hours": self.execution_interval_hours,
            "lock_duration_hours": self.lock_duration_hours,
            "schedule_type": self.schedule_type.value,
            "configured_interval": PROVIDER_EXECUTION_INTERVALS.get(self.provider_slug, "default"),
            "configured_lock_duration": PROVIDER_LOCK_DURATIONS.get(self.provider_slug, "default"),
        }


def create_pull_task_for_provider(
    provider: BaseThetaProvider,
    schedule_type: ScheduleType = ScheduleType.HOURLY,
    custom_execution_interval: Optional[float] = None,
    custom_lock_duration: Optional[float] = None,
) -> ThetaProviderPullTask:
    """
    Create Pull Task for Theta Provider

    Args:
        provider: Provider instance
        schedule_type: Schedule type
        custom_execution_interval: Custom execution interval (hours)
        custom_lock_duration: Custom lock duration (hours)

    Returns:
        Configured Pull Task instance
    """
    task = ThetaProviderPullTask(
        provider=provider,
        schedule_type=schedule_type,
        custom_execution_interval=custom_execution_interval,
        custom_lock_duration=custom_lock_duration,
    )

    return task


def get_provider_execution_config() -> Dict:
    """Get execution configuration for all providers"""
    return {
        "execution_intervals": PROVIDER_EXECUTION_INTERVALS,
        "lock_durations": PROVIDER_LOCK_DURATIONS,
        "description": {
            "execution_intervals": "Provider execution interval configuration (hours)",
            "lock_durations": "Provider distributed lock duration configuration (hours)",
        },
    }
