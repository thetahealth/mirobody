"""Postgres-backed background tasks with claim, acknowledgement and retry."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Any, ClassVar

from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


class BaseTask:
    """A worker task whose subclass supplies ``queue_key`` and ``consume``.

    A claim makes rows temporarily invisible, then successful processing
    acknowledges them. An interrupted worker leaves the rows for the next
    worker after the lease expires.
    """

    queue_key: ClassVar[str] = ""
    drain_cap: ClassVar[int] = 20
    poll_interval_sec: ClassVar[int] = 5
    lease_sec: ClassVar[int] = 3600
    retry_sec: ClassVar[int] = 30
    max_attempts: ClassVar[int] = 5
    heartbeat_sec: ClassVar[int] = 600

    def __init__(self, pg_config: Any) -> None:
        if not type(self).queue_key:
            raise RuntimeError(f"{type(self).__name__}.queue_key must be set")
        self._pg_config = pg_config

    @classmethod
    async def enqueue(cls, payload: Any) -> None:
        if not cls.queue_key:
            raise RuntimeError(f"{cls.__name__}.queue_key must be set")
        from mirobody.utils.config import global_config

        config = global_config()
        if config is None:
            raise RuntimeError(f"{cls.__name__}.enqueue called before Config.init")
        message = payload if isinstance(payload, str) else json.dumps(payload)
        async with (await config.get_postgresql().get_async_client(cursor_factory=None)) as conn:
            await conn.execute(
                "INSERT INTO th_task_queue (queue_name, payload) VALUES (%s, %s)",
                (cls.queue_key, message),
            )
        logger.info("task enqueued: type=%s", cls.__name__)

    async def consume(self, messages: list[str]) -> None:
        raise NotImplementedError(type(self).__name__)

    async def _claim_batch(self) -> tuple[str, list[tuple[int, str]]]:
        token = str(uuid.uuid4())
        async with (await self._pg_config.get_async_client(cursor_factory=None)) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    WITH picked AS (
                        SELECT id FROM th_task_queue
                        WHERE queue_name = %s AND failed_at IS NULL
                          AND available_at <= now()
                        ORDER BY id DESC
                        LIMIT %s FOR UPDATE SKIP LOCKED
                    )
                    UPDATE th_task_queue AS task
                    SET available_at = now() + (%s * interval '1 second'),
                        lease_token = %s, attempts = attempts + 1
                    FROM picked WHERE task.id = picked.id
                    RETURNING task.id, task.payload
                    """,
                    (type(self).queue_key, type(self).drain_cap, type(self).lease_sec, token),
                )
                rows = await cur.fetchall()
        return token, [(row_id, payload) for row_id, payload in rows]

    async def _finish_batch(self, token: str, ids: list[int], *, success: bool) -> None:
        if not ids:
            return
        async with (await self._pg_config.get_async_client(cursor_factory=None)) as conn:
            if success:
                await conn.execute(
                    "DELETE FROM th_task_queue WHERE id = ANY(%s) AND lease_token = %s",
                    (ids, token),
                )
            else:
                await conn.execute(
                    """
                    UPDATE th_task_queue
                    SET lease_token = NULL,
                        available_at = now() + (%s * interval '1 second'),
                        failed_at = CASE WHEN attempts >= %s THEN now() ELSE NULL END
                    WHERE id = ANY(%s) AND lease_token = %s
                    """,
                    (type(self).retry_sec, type(self).max_attempts, ids, token),
                )

    async def _keep_lease(self, token: str, ids: list[int], stop: asyncio.Event) -> None:
        # Profile refresh can outlive a fixed lease when a model call stalls.
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(1, type(self).lease_sec // 3))
                return
            except TimeoutError:
                pass
            async with (await self._pg_config.get_async_client(cursor_factory=None)) as conn:
                await conn.execute(
                    "UPDATE th_task_queue SET available_at = now() + (%s * interval '1 second') "
                    "WHERE id = ANY(%s) AND lease_token = %s",
                    (type(self).lease_sec, ids, token),
                )

    async def run(self, stop_event: asyncio.Event) -> None:
        cls = type(self)
        logger.info("task consumer started: type=%s", cls.__name__)
        last_active = time.monotonic()
        while not stop_event.is_set():
            try:
                token, batch = await self._claim_batch()
                if not batch:
                    if time.monotonic() - last_active >= cls.heartbeat_sec:
                        logger.info("task consumer idle: type=%s", cls.__name__)
                        last_active = time.monotonic()
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=cls.poll_interval_sec)
                    except TimeoutError:
                        pass
                    continue
                ids = [row_id for row_id, _ in batch]
                lease_stop = asyncio.Event()
                try:
                    async with asyncio.TaskGroup() as group:
                        group.create_task(self._keep_lease(token, ids, lease_stop))
                        try:
                            await group.create_task(
                                self.consume([payload for _, payload in batch])
                            )
                            await self._finish_batch(token, ids, success=True)
                        finally:
                            lease_stop.set()
                except Exception:
                    await self._finish_batch(token, ids, success=False)
                    raise
                last_active = time.monotonic()
                logger.info("task batch completed: type=%s count=%d", cls.__name__, len(ids))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "task consumer error: type=%s error_type=%s",
                    cls.__name__, type(exc).__name__,
                    exc_info=not is_driver_exception(exc),
                )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=cls.retry_sec)
                except TimeoutError:
                    pass
        logger.info("task consumer stopped: type=%s", cls.__name__)
