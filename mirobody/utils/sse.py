"""Server-Sent Events keepalive — the one implementation every streaming
endpoint uses.

An agent turn goes quiet in three places: before the first token (auth,
session, agent construction, tool loading, model queueing), inside a tool
call, and between recursion hops. Every middlebox on the path — a mobile
client, an API gateway, a reverse proxy — reads "no bytes for a while" as
"connection dead" and cuts it. The fix is to make liveness *observable
bytes*: write one small frame, and only while the source is silent.

The frame here is an SSE **comment** (a line starting with ``:``), which every
SSE parser ignores — the browser's EventSource, the OpenAI SDKs, and any
hand-rolled reader that checks the ``data:`` prefix. It must stay a comment: a
``data: {"type": "ping"}`` renders as garbage in a client that shipped before
it existed and cannot be updated.

**Silence-triggered, never unconditional.** A fixed-cadence ping keeps firing
while tokens flow, which *hides* a hung turn (pings continue, tokens stop).
Triggered by silence, three states stay distinguishable: pings only = alive
but idle; tokens = healthy; neither = dead.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Callable

log = logging.getLogger(__name__)

#: The keepalive frame. A comment frame by contract — see the module docstring.
SSE_PING = ": ping\n\n"

#: Must sit well under the SHORTEST idle timeout on the path. Known bounds: a
#: cloud load balancer that defaults to 15 s, a bare nginx ingress at 60 s, a
#: mobile runtime that declares a turn dead after 90 s without bytes. 8 s keeps
#: about 2× margin against the tightest and tolerates 10 s-class layers; the
#: 15 s a first design suggested does not.
DEFAULT_HEARTBEAT_SECONDS = 8.0

#: The key every endpoint falls back to; an endpoint's own key wins.
SHARED_KEY = "SSE_HEARTBEAT_SECONDS"


def heartbeat_seconds(*keys: str, read: Callable[[str], str | None] | None = None) -> float:
    """The keepalive interval: the first of ``keys`` that is set, then
    ``SSE_HEARTBEAT_SECONDS``, then the built-in default. ``<= 0`` disables.

    ``read`` is how a key is looked up — ``os.getenv`` by default. An
    application that keeps configuration somewhere other than the environment
    passes its own reader (``safe_read_cfg`` here).

    **Call once per stream, never at import time.** A module-level read
    freezes the value before a lifespan hook has copied configuration into
    the environment, and the knob silently stops working in the cloud while
    the code still looks configurable.
    """
    lookup = read or os.getenv
    for key in (*keys, SHARED_KEY):
        raw = lookup(key)
        if raw is None or not str(raw).strip():
            continue
        try:
            return float(raw)
        except ValueError:
            log.warning("sse: %s is not a number — ignoring", key)
    return DEFAULT_HEARTBEAT_SECONDS


def sse_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """The response headers every SSE stream needs, or the heartbeat never
    reaches the client.

    A buffering middlebox defeats the heartbeat *silently*: it collects the
    pings and flushes them with the first real bytes, so the client does
    eventually receive them and a test sees nothing wrong — while the
    upstream idle timer was never reset. That matters more than the interval.

    - ``X-Accel-Buffering: no`` — the de-facto per-response switch nginx and
      most cloud gateways honour;
    - ``no-transform`` — no compression or transcoding of the body; a gzip
      stream cannot flush frame by frame;
    - ``no-cache, no-store`` — a cached or coalesced event stream is always
      wrong.
    """
    headers = {
        "Cache-Control": "no-cache, no-store, no-transform",
        "X-Accel-Buffering": "no",
    }
    if extra:
        headers.update(extra)
    return headers


async def ping_while_pending(task: asyncio.Future, interval: float | None = None) -> AsyncIterator[str]:
    """Yield heartbeat frames until ``task`` finishes; the caller then awaits
    the task itself for its result or exception::

        task = asyncio.ensure_future(slow_thing())
        async for frame in ping_while_pending(task, interval):
            yield frame
        result = await task

    For the one long ``await`` inside a generator that is already streaming.
    ``asyncio.wait`` neither cancels the task on timeout nor re-raises its
    exception, so the caller's own try/except keeps working.
    """
    interval = heartbeat_seconds() if interval is None else interval
    if interval <= 0:
        return
    while True:
        _done, pending = await asyncio.wait({task}, timeout=interval)
        if not pending:
            return
        yield SSE_PING


async def with_heartbeat(frames: AsyncIterator[str], interval: float | None = None) -> AsyncIterator[str]:
    """Forward SSE frames, inserting ``SSE_PING`` whenever the source is
    silent for more than ``interval`` seconds. The source's own exceptions and
    termination are preserved.

    **Do not wrap a generator whose ``finally`` must run deterministically on
    client disconnect.** This relay consumes the source in a task; when that
    task is cancelled while blocked on ``q.put`` (queue full), the source is
    *suspended*, not closed, and its ``finally`` runs only when the async
    generator is finalised (GC / loop shutdown). A chat endpoint that
    persists an interrupted turn in such a ``finally`` must be driven by the
    server directly and emit its own heartbeat — as ``agent/chat`` does.
    """
    interval = heartbeat_seconds() if interval is None else interval
    if interval <= 0:
        async for frame in frames:
            yield frame
        return
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    done = object()

    async def _pump() -> None:
        try:
            async for frame in frames:
                await queue.put(frame)
        except asyncio.CancelledError:
            # Cancelled by the `finally` below (the consumer left). Never touch
            # the queue here: a `put` on a full queue inside a cancelled task
            # blocks forever.
            raise
        except BaseException as exc:  # forwarded to the consumer below
            await queue.put(exc)
        else:
            await queue.put(done)

    task = asyncio.create_task(_pump())
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=interval)
            except TimeoutError:
                yield SSE_PING
                continue
            if item is done:
                return
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        task.cancel()
