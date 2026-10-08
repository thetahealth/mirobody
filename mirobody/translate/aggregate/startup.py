"""Registering the aggregation job with the shared scheduler."""

import logging

from mirobody.utils.scheduler import scheduler
from .task import AggregateIndicatorTask

logger = logging.getLogger(__name__)

_aggregate_task = None


async def start_aggregate_indicator_scheduler() -> None:
    """Register the aggregate indicator task."""
    global _aggregate_task

    if _aggregate_task is not None:
        logger.warning("Aggregate indicator task already registered")
        return

    _aggregate_task = AggregateIndicatorTask()
    scheduler.register_task(_aggregate_task)
    logger.info("Aggregate indicator task registered successfully")
