"""Pub/Sub support for both TCP server and in-process client."""

from __future__ import annotations

import asyncio

from dataclasses import dataclass, field

from .resp import encode_array, encode_bulk_string


@dataclass(eq=False)
class Subscriber:
    writer: asyncio.StreamWriter
    channels: set[str] = field(default_factory=set)


class PubSub:
    def __init__(self):
        self._channels: dict[str, set[Subscriber]] = {}

    def subscribe(self, subscriber: Subscriber, *channels: str):
        for ch in channels:
            subscriber.channels.add(ch)
            self._channels.setdefault(ch, set()).add(subscriber)

    def unsubscribe(self, subscriber: Subscriber, *channels: str):
        targets = channels if channels else tuple(subscriber.channels)
        for ch in targets:
            subscriber.channels.discard(ch)
            subs = self._channels.get(ch)
            if subs:
                subs.discard(subscriber)
                if not subs:
                    del self._channels[ch]

    def publish(self, channel: str, message: str) -> int:
        subs = self._channels.get(channel, set())
        msg = encode_array([
            encode_bulk_string("message"),
            encode_bulk_string(channel),
            encode_bulk_string(message),
        ])
        count = 0
        for sub in list(subs):
            try:
                sub.writer.write(msg)
                count += 1
            except Exception:
                subs.discard(sub)
        return count


# -- In-process Pub/Sub adapter (redis.asyncio.PubSub compatible) ---------

class CompatPubSub:
    """Minimal redis.asyncio.client.PubSub-compatible wrapper for in-process use."""

    def __init__(self, pubsub: PubSub):
        self._pubsub = pubsub
        self._channels: set[str] = set()
        self._queue: asyncio.Queue[dict] = asyncio.Queue()
        self._subscriber = _InProcessSubscriber(self._queue)

    async def subscribe(self, *channels: str) -> None:
        for ch in channels:
            self._channels.add(ch)
            self._pubsub._channels.setdefault(ch, set()).add(self._subscriber)

    async def unsubscribe(self, *channels: str) -> None:
        targets = channels if channels else tuple(self._channels)
        for ch in targets:
            self._channels.discard(ch)
            subs = self._pubsub._channels.get(ch)
            if subs:
                subs.discard(self._subscriber)
                if not subs:
                    del self._pubsub._channels[ch]

    async def get_message(self, ignore_subscribe_messages: bool = False, timeout: float = 0) -> dict | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout or 0.01)
        except asyncio.TimeoutError:
            return None

    async def close(self) -> None:
        await self.unsubscribe()


@dataclass(eq=False)
class _InProcessSubscriber:
    """Adapts the queue-based CompatPubSub to the writer-based PubSub.publish()."""
    _queue: asyncio.Queue
    channels: set[str] = field(default_factory=set)
    writer: object = None  # not used; satisfies Subscriber-like duck typing

    def __getattr__(self, name):
        """PubSub.publish() calls subscriber.writer.write(msg) -- intercept it."""
        if name == "writer":
            return self
        raise AttributeError(name)

    def write(self, data: bytes) -> None:
        # Parse channel and message from the RESP array written by PubSub.publish()
        # Format: *3\r\n$7\r\nmessage\r\n$<len>\r\n<channel>\r\n$<len>\r\n<msg>\r\n
        parts = data.split(b"\r\n")
        # Extract bulk strings: indices 2=message, 4=channel, 6=data
        channel = parts[4].decode() if len(parts) > 4 else ""
        message = parts[6].decode() if len(parts) > 6 else ""
        self._queue.put_nowait({"type": "message", "channel": channel, "data": message})
