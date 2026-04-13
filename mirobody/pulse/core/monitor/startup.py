"""
Monitor Collector Startup (TH-141)

Register hourly and daily collector tasks with the unified scheduler.
"""

import logging

from .collector_task import HourlyCollectorTask, DailyProfileTask
from ..scheduler import scheduler

_hourly_task = None
_daily_task = None


async def start_monitor_collector():
    """Register monitor collector tasks with the unified scheduler."""
    global _hourly_task, _daily_task

    if _hourly_task is not None:
        logging.warning("Monitor collector tasks already registered")
        return

    logging.info("Registering monitor collector tasks with scheduler...")

    _hourly_task = HourlyCollectorTask()
    _daily_task = DailyProfileTask()

    scheduler.register_task(_hourly_task)
    scheduler.register_task(_daily_task)

    logging.info("Monitor collector tasks registered successfully")


async def stop_monitor_collector():
    """Stop is handled by the unified scheduler."""
    logging.info("Monitor collector tasks will be stopped by unified scheduler")


def get_hourly_task_status() -> dict:
    """Get hourly collector task status (synchronous)."""
    if _hourly_task:
        return _hourly_task.get_status()
    return {"status": "not_initialized"}


def get_daily_task_status() -> dict:
    """Get daily profile task status (synchronous)."""
    if _daily_task:
        return _daily_task.get_status()
    return {"status": "not_initialized"}


async def get_hourly_task_full_status() -> dict:
    """Get hourly collector task full status (async)."""
    if _hourly_task:
        return await _hourly_task.get_task_info()
    return {"status": "not_initialized"}


async def get_daily_task_full_status() -> dict:
    """Get daily profile task full status (async)."""
    if _daily_task:
        return await _daily_task.get_task_info()
    return {"status": "not_initialized"}
