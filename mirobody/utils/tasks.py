"""Background tasks that actually survive.

`asyncio.create_task(...)` whose result is discarded is a real bug, not a style
preference. The CPython docs are explicit:

    Save a reference to the result of this function, to avoid a task
    disappearing mid-execution. The event loop only keeps weak references to
    tasks. A task that isn't referenced elsewhere may get garbage collected at
    any time, even before it's done.
    — https://docs.python.org/3/library/asyncio-task.html

This repo had 15 such call sites, and they run the work a user would most
notice losing: file processing after upload, OAuth callback token exchange,
vendor data pulls, embedding updates. A collected task fails silently — no
exception, no log, just a job that never happened.

The same docs note the second half of the problem: nobody awaits these, so a
failure surfaces only as "Task exception was never retrieved" at GC time, if at
all. `spawn` therefore logs failures itself.

    from ..utils.tasks import spawn
    spawn(self.process_files_async(...), name="file-processing")
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from collections.abc import Coroutine

logger = logging.getLogger(__name__)

# Strong references, per the documented pattern. Tasks remove themselves on
# completion so this never grows without bound.
_BACKGROUND_TASKS: set[asyncio.Task] = set()


def _log_result(task: asyncio.Task) -> None:
    _BACKGROUND_TASKS.discard(task)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "background task %r failed: %s", task.get_name(), exc, exc_info=exc,
        )


def spawn(coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> asyncio.Task:
    """Run `coro` in the background, holding a strong reference to it.

    Returns the task so a caller that *does* want to await or cancel it can.
    Fire-and-forget callers can ignore the return value safely — unlike a bare
    `asyncio.create_task`, this one cannot be garbage collected mid-flight, and
    its failure is logged rather than swallowed.
    """
    task = asyncio.create_task(coro, name=name)
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_log_result)
    return task


def pending_count() -> int:
    """How many spawned tasks are still running. For diagnostics and tests."""
    return len(_BACKGROUND_TASKS)
