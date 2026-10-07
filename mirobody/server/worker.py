"""Standalone worker runner: starts the mirobody task consumers without an
HTTP server.

Mirrors `Server.start(yaml_files)` in shape so deployment is symmetric: both
read the same YAML config; this class spins up the Postgres task consumers
without an HTTP server.

Task discovery is automatic: every `BaseTask` subclass registered under
`mirobody.task` is picked up via `iter_tasks()`, so adding a new task
class is enough, no wiring needed here.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from mirobody.task import iter_tasks, load_tasks_from_directories
from mirobody.utils import Config
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


async def _clean_ephemeral(config: Config, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            count = await config.get_ephemeral().cleanup()
            if count:
                logger.info("expired temporary state removed: count=%d", count)
        except Exception as exc:
            logger.warning("temporary state cleanup failed: error_type=%s",
                           type(exc).__name__,
                           exc_info=not is_driver_exception(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=3600)
        except TimeoutError:
            pass

#-----------------------------------------------------------------------------

class Worker:
    @staticmethod
    async def start(
        yaml_files: list[str] | None = None,
        config: Config | None = None,
    ) -> None:
        # Load configuration via YAML (same pattern as Server.start), unless
        # the caller already built a Config instance and passed it in.
        if yaml_files is None:
            yaml_files = []
        if config is None:
            config = await Config.init(yaml_filenames=yaml_files)
        config.print()

        # What the first-run page saved; each task loop rereads it (task/base.py).
        from mirobody.utils.config import settings
        await settings.apply()

        # The worker runs the extraction queues, so it has the same question
        # the server asks at boot: which surfaces have a provider.
        from mirobody.utils.config.doctor import log_report, provider_report
        log_report(provider_report(config), logger)

        logger.info("Worker runner starting")

        pg_config = config.get_postgresql()

        # Pull in user-defined task modules before enumerating subclasses.
        load_tasks_from_directories(config.task_dirs)

        task_classes = iter_tasks()
        if not task_classes:
            raise RuntimeError("No BaseTask subclasses discovered in mirobody.task")

        stop_events = [asyncio.Event() for _ in task_classes]
        cleanup_stop = asyncio.Event()

        # Wire SIGTERM/SIGINT → stop events for graceful shutdown.
        loop = asyncio.get_running_loop()
        def _request_stop() -> None:
            logger.info("Shutdown signal received")
            for ev in stop_events:
                ev.set()
            cleanup_stop.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                pass  # Windows: add_signal_handler is unsupported

        logger.info(f"Starting {len(task_classes)} task consumer(s): "
                     f"{[c.__name__ for c in task_classes]}")

        tasks = [
            asyncio.create_task(cls(pg_config).run(ev))
            for cls, ev in zip(task_classes, stop_events, strict=False)
        ]
        tasks.append(asyncio.create_task(_clean_ephemeral(config, cleanup_stop)))

        await asyncio.gather(*tasks)
        logger.info("Worker runner stopped")

#-----------------------------------------------------------------------------
