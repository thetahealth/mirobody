"""The scheduled pull of one provider."""

import logging

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.scheduler import PullTask, ScheduleType
from .base import BasePullProvider

logger = logging.getLogger(__name__)


class ProviderPullTask(PullTask):
    """A provider's `pull_and_push`, every `pull_interval_hours`.

    The scheduler checks at the top of each hour, so an interval under an
    hour runs hourly; its lock keeps one run across instances.
    """

    def __init__(self, provider: BasePullProvider):
        self.provider = provider
        super().__init__(
            provider_slug=provider.info.slug,
            schedule_type=ScheduleType.HOURLY,
            execution_interval_hours=provider.pull_interval_hours,
        )

    async def execute(self) -> bool:
        try:
            return await self.provider.pull_and_push()
        except Exception as e:
            logger.error("provider pull failed: provider=%s error_type=%s", self.provider_slug,
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return False
