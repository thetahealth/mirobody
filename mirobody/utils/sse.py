"""Server-Sent Events keepalive: the interval and the response headers every
streaming endpoint shares.

An agent turn goes quiet in three places: before the first token (auth,
session, agent construction, tool loading, model queueing), inside a tool
call, and between recursion hops. Every middlebox on the path (a mobile
client, an API gateway, a reverse proxy) reads "no bytes for a while" as
"connection dead" and cuts it. The fix is to make liveness *observable
bytes*: write one small frame, and only while the source is silent (the chat
stream writes a `heartbeat` block, `agent/chat/turn.py`).

**Silence-triggered, never unconditional.** A fixed-cadence ping keeps firing
while tokens flow, which *hides* a hung turn (pings continue, tokens stop).
Triggered by silence, three states stay distinguishable: pings only = alive
but idle; tokens = healthy; neither = dead.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

logger = logging.getLogger(__name__)

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

    ``read`` is how a key is looked up: ``os.getenv`` by default. An
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
            logger.warning("sse: %s is not a number — ignoring", key)
    return DEFAULT_HEARTBEAT_SECONDS


def sse_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """The response headers every SSE stream needs, or the heartbeat never
    reaches the client.

    A buffering middlebox defeats the heartbeat *silently*: it collects the
    pings and flushes them with the first real bytes, so the client does
    eventually receive them and a test sees nothing wrong, while the
    upstream idle timer was never reset. That matters more than the interval.

    - ``X-Accel-Buffering: no``: the de-facto per-response switch nginx and
      most cloud gateways honour;
    - ``no-transform``, no compression or transcoding of the body; a gzip
      stream cannot flush frame by frame;
    - ``no-cache, no-store``: a cached or coalesced event stream is always
      wrong.
    """
    headers = {
        "Cache-Control": "no-cache, no-store, no-transform",
        "X-Accel-Buffering": "no",
    }
    if extra:
        headers.update(extra)
    return headers
