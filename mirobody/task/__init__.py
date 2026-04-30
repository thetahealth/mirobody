"""Task package.

`load_tasks_from_directories` is the single entry point for task registration.
It always loads the built-in task modules in this package, plus any extra
directories passed in, so callers don't need to list `mirobody/task` alongside
their own `TASK_DIRS`. `iter_redis_tasks` then enumerates whatever registered.
"""

from __future__ import annotations

from .base import BaseRedisTask
from .loader import load_tasks_from_directories as load_tasks_from_directories

from .indicator_sync import IndicatorSyncTask as IndicatorSyncTask
from .profile_refresh import ProfileRefreshTask as ProfileRefreshTask

def iter_redis_tasks() -> list[type[BaseRedisTask]]:
    """Concrete `BaseRedisTask` subclasses currently registered."""
    return [cls for cls in BaseRedisTask.__subclasses__() if cls.queue_key]
