"""Task package.

`load_tasks_from_directories` is the single entry point for task registration.
It always loads the built-in task modules in this package, plus any extra
directories passed in, so callers don't need to list `mirobody/task` alongside
their own `TASK_DIRS`. `iter_tasks` then enumerates whatever registered.
"""

from __future__ import annotations

from .base import BaseTask
from .loader import load_tasks_from_directories as load_tasks_from_directories

from .profile_refresh import ProfileRefreshTask as ProfileRefreshTask

def iter_tasks() -> list[type[BaseTask]]:
    """Concrete task subclasses currently registered."""
    return [cls for cls in BaseTask.__subclasses__() if cls.queue_key]
