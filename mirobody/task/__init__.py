"""Task package.

`load_tasks_from_directories` is the single entry point for task registration.
It always loads the built-in task modules in this package, plus any extra
directories passed in, so callers don't need to list `mirobody/task` alongside
their own `TASK_DIRS`. `iter_redis_tasks` then enumerates whatever registered.
"""

from __future__ import annotations

from .base import BaseRedisTask, BaseTask
from .loader import load_tasks_from_directories


def iter_redis_tasks() -> list[type[BaseRedisTask]]:
    """Concrete `BaseRedisTask` subclasses currently registered."""
    return [cls for cls in BaseRedisTask.__subclasses__() if cls.queue_key]


__all__ = [
    "BaseRedisTask",
    "BaseTask",
    "iter_redis_tasks",
    "load_tasks_from_directories",
]
