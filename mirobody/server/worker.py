"""Standalone worker runner — starts the mirobody task consumers without an
HTTP server.

Mirrors `Server.start(yaml_files)` in shape so deployment is symmetric: both
read the same YAML config; this class spins up the Redis-queue consumer loops
(no uvicorn, no routers). The thin launcher lives at the repo-root
`main_worker.py`, next to `main.py`.

Task discovery is automatic: every `BaseRedisTask` subclass registered under
`mirobody.task` is picked up via `iter_redis_tasks()`, so adding a new task
class is enough — no wiring needed here.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from ..task import iter_redis_tasks, load_tasks_from_directories
from ..utils import Config

#-----------------------------------------------------------------------------

class Worker:
    @staticmethod
    async def start(yaml_files: list[str] = []) -> None:
        # Load configuration via YAML (same pattern as Server.start).
        config = await Config.init(yaml_filenames=yaml_files)
        config.print()

        logging.info("Worker runner starting")

        # One redis client shared by all consumers — redis.asyncio.Redis has
        # an internal connection pool (max_connections from config), so each
        # BLPOP borrows its own connection and they don't serialize.
        redis = await config.get_redis().get_async_client()

        # Pull in user-defined task modules before enumerating subclasses.
        load_tasks_from_directories(config.task_dirs)

        task_classes = iter_redis_tasks()
        if not task_classes:
            raise RuntimeError("No BaseRedisTask subclasses discovered in mirobody.task")

        stop_events = [asyncio.Event() for _ in task_classes]

        # Wire SIGTERM/SIGINT → stop events for graceful shutdown.
        loop = asyncio.get_running_loop()
        def _request_stop() -> None:
            logging.info("Shutdown signal received")
            for ev in stop_events:
                ev.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                pass  # Windows: add_signal_handler is unsupported

        logging.info(f"Starting {len(task_classes)} task consumer(s): "
                     f"{[c.__name__ for c in task_classes]}")

        tasks = [
            asyncio.create_task(cls(redis).run(ev))
            for cls, ev in zip(task_classes, stop_events)
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            logging.info("Closing redis client")
            await redis.aclose()
            logging.info("Worker runner stopped")

#-----------------------------------------------------------------------------
