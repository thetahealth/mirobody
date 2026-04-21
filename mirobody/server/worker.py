"""Standalone worker runner — starts the mirobody task consumers without an
HTTP server.

Mirrors `Server.start(yaml_files)` in shape so deployment is symmetric: both
read the same YAML config; this class spins up the Redis-queue consumer loops
(no uvicorn, no routers). The thin launcher lives at the repo-root
`main_worker.py`, next to `main.py`.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from ..task import IndicatorSyncTask, ProfileRefreshTask
from ..utils import Config

#-----------------------------------------------------------------------------

class Worker:
    @staticmethod
    async def start(yaml_files: list[str] = []) -> None:
        # Load configuration via YAML (same pattern as Server.start).
        config = await Config.init(yaml_filenames=yaml_files)
        config.print()

        logging.info("Worker runner starting")

        # One redis client shared by both consumers — redis.asyncio.Redis has
        # an internal connection pool (max_connections from config), so each
        # BLPOP borrows its own connection and they don't serialize.
        redis = await config.get_redis().get_async_client()

        indicator_sync = IndicatorSyncTask(redis)
        profile_refresh = ProfileRefreshTask(redis)

        indicator_sync_stop = asyncio.Event()
        profile_refresh_stop = asyncio.Event()

        # Wire SIGTERM/SIGINT → stop events for graceful shutdown.
        loop = asyncio.get_running_loop()
        def _request_stop() -> None:
            logging.info("Shutdown signal received")
            indicator_sync_stop.set()
            profile_refresh_stop.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_stop)
            except NotImplementedError:
                pass  # Windows: add_signal_handler is unsupported

        tasks = [
            asyncio.create_task(indicator_sync.start_worker(indicator_sync_stop)),
            asyncio.create_task(profile_refresh.start_worker(profile_refresh_stop)),
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            logging.info("Closing redis client")
            await redis.aclose()
            logging.info("Worker runner stopped")

#-----------------------------------------------------------------------------
