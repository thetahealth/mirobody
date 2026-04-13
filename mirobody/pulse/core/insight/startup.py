"""
Insight Engine Startup

Register insight engine task with the unified scheduler.
"""

import logging

from .engine_task import InsightEnginePullTask
from ..scheduler import scheduler

_insight_task = None


async def start_insight_engine():
    """Register insight engine task with the unified scheduler."""
    global _insight_task

    if _insight_task is not None:
        logging.warning("Insight engine task already registered")
        return

    logging.info("Registering insight engine task with scheduler...")

    _insight_task = InsightEnginePullTask()
    scheduler.register_task(_insight_task)

    logging.info("Insight engine task registered successfully")


async def stop_insight_engine():
    """Stop is handled by the unified scheduler."""
    logging.info("Insight engine task will be stopped by unified scheduler")


async def get_insight_task_full_status() -> dict:
    """Get insight engine task full status (async)."""
    if _insight_task:
        return await _insight_task.get_task_info()
    return {"status": "not_initialized"}
