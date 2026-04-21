"""Base classes for async background tasks.

`BaseTask` is the transport-agnostic interface: every task has a producer
(`add_task`) and a consumer (`process_task` + `start_worker`). Subclasses
override the methods they're responsible for; unoverridden methods raise
`NotImplementedError`.

`BaseRedisTask` is the concrete Redis-list implementation — provides `add_task`
via LPUSH and `start_worker` via a BLPOP drain-batch loop. Domain subclasses
extend `BaseRedisTask`, declare `queue_key`, and implement `process_task`.

Future backends (SQS, Postgres LISTEN/NOTIFY, in-memory for tests, …) can
derive from `BaseTask` directly as siblings of `BaseRedisTask`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from typing import Any, ClassVar
from redis.asyncio import Redis

#-----------------------------------------------------------------------------

class BaseTask:
    async def add_task(self, payload: dict[str, Any]) -> None:
        """Enqueue a task payload."""
        raise NotImplementedError(f"{type(self).__name__}.add_task({payload!r})")

    async def process_task(self, messages: list[str]) -> None:
        """Process a batch of raw payloads drained from the queue."""
        raise NotImplementedError(f"{type(self).__name__}.process_task({len(messages)} msg)")

    async def start_worker(self, stop_event: asyncio.Event) -> None:
        """Run the consumer loop until stop_event is set."""
        raise NotImplementedError(f"{type(self).__name__}.start_worker(stop_set={stop_event.is_set()})")

#-----------------------------------------------------------------------------

class BaseRedisTask(BaseTask):
    """Redis-list-backed task. Concrete for `add_task` and `start_worker`;
    subclasses only declare `queue_key` and override `process_task`."""

    # Class-level config — per-subclass constants, not per-instance state.
    queue_key: ClassVar[str] = ""
    max_queue_len: ClassVar[int] = 0  # 0 = unlimited; >0 causes add_task to raise when queue is at/over cap
    drain_cap: ClassVar[int] = 500
    blpop_timeout_sec: ClassVar[int] = 30
    retry_sleep_sec: ClassVar[int] = 5
    heartbeat_sec: ClassVar[int] = 600  # log "still alive" every N seconds while idle

    def __init__(self, redis: Redis) -> None:
        cls = type(self)
        if not cls.queue_key:
            raise RuntimeError(f"{cls.__name__}.queue_key must be set")

        self._redis = redis

    # ---------- Producer side ----------

    async def add_task(self, payload: Any) -> None:
        """LPUSH a payload onto this task's queue. Strings go on the wire
        as-is; anything else is JSON-serialized first.

        Raises `RuntimeError` if `max_queue_len > 0` and the queue is already
        at/over capacity — callers should treat this as "consumer is falling
        behind, back off". Other errors (redis unreachable, etc.) are logged
        and swallowed so that transient infra failures don't fail ingest."""
        cls = type(self)
        try:
            if cls.max_queue_len > 0:
                length = await self._redis.llen(cls.queue_key)
                if length >= cls.max_queue_len:
                    raise RuntimeError(
                        f"{cls.__name__}: queue {cls.queue_key} is full "
                        f"({length} >= {cls.max_queue_len})"
                    )

            msg = payload if isinstance(payload, str) else json.dumps(payload)
            await self._redis.lpush(cls.queue_key, msg)
            logging.info(f"{cls.__name__} enqueued: {payload}")
        except RuntimeError:
            raise  # queue-full surfaces to caller
        except Exception as e:
            logging.error(f"{cls.__name__}.add_task failed ({payload}): {e}")

    # ---------- Consumer side ----------

    async def start_worker(self, stop_event: asyncio.Event) -> None:
        cls = type(self)
        logging.info(f"{cls.__name__} starting (queue={cls.queue_key}, heartbeat={cls.heartbeat_sec}s)")

        last_active = time.monotonic()
        while not stop_event.is_set():
            try:
                batch = await self._pop_batch()
                if not batch:
                    now = time.monotonic()
                    if now - last_active >= cls.heartbeat_sec:
                        logging.info(f"{cls.__name__} alive (queue {cls.queue_key} empty, idle {int(now - last_active)}s)")
                        last_active = now
                    continue
                await self.process_task(batch)
                last_active = time.monotonic()
            except asyncio.CancelledError:
                logging.info(f"{cls.__name__} cancelled")
                raise
            except Exception as e:
                logging.error(f"{cls.__name__} loop error: {e}", stack_info=True)
                await asyncio.sleep(cls.retry_sleep_sec)

        logging.info(f"{cls.__name__} stopped")

    async def _pop_batch(self) -> list[str]:
        cls = type(self)
        popped = await self._redis.blpop(cls.queue_key, timeout=cls.blpop_timeout_sec)
        if popped is None:
            return []

        _key, first = popped
        batch = [first]
        for _ in range(cls.drain_cap - 1):
            more = await self._redis.lpop(cls.queue_key)
            if more is None:
                break
            batch.append(more)

        return batch

#-----------------------------------------------------------------------------
