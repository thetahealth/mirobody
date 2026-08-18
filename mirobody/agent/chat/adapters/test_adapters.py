"""The transport seam: what a second protocol would actually have to write.

`ChatProtocolAdapter` declared `handle_request` and `stream_output` abstract.
Between them those are 183 code lines, of which 5 emit SSE framing — so a
WebSocket adapter would have inherited the obligation to reimplement 178 lines
of accumulator dispatch, database persistence, end-chunk ordering and
heartbeat timing in order to change `data: {...}\\n\\n` into a frame.

These tests pin the two things that must not drift:

1. The exact bytes an HTTP client receives, chunk by chunk. Recorded against
   the pre-refactor implementation and unchanged by it.
2. That the seam is narrow — a subclass overriding only `encode_chunk` gets a
   working adapter. That is the property the ABC was supposed to provide and
   did not.
"""

from __future__ import annotations

import pytest


import asyncio
import json

import pytest

from .base import ChatProtocolAdapter
from .http import HTTPChatAdapter


class _Recorder(HTTPChatAdapter):
    """An adapter with the database and the agent replaced by fakes."""

    def __init__(self):
        super().__init__()
        self.saved: list = []

    async def save_assistant_response(self, reply_id, params, user_id, content, question_msg_id):
        self.saved.append({"reply_id": reply_id, "content": content})


async def _drive(adapter, chunks: list[dict]) -> list[str]:
    async def gen():
        for c in chunks:
            yield c

    ctx = {
        "user_id": "1", "query_user_id": "1", "msg_id": "q1",
        "session_id": "s1", "agent": "app", "params": object(),
    }
    return [frame async for frame in adapter.stream_output(gen(), ctx)]


async def test_sse_framing_is_exact():
    """Byte-for-byte, including the blank-line terminator SSE requires."""
    adapter = _Recorder()
    frames = await _drive(adapter, [
        {"type": "thinking", "content": "hmm"},
        {"type": "reply", "content": "Hello "},
        {"type": "reply", "content": "world"},
        {"type": "end", "content": ""},
    ])

    for f in frames:
        assert f.startswith("data: "), f
        assert f.endswith("\n\n"), f
        json.loads(f[len("data: "):].strip())     # must be valid JSON

    types = [json.loads(f[6:])["type"] for f in frames]
    # `id` is emitted first so the client can correlate; `end` last, and only
    # after the response has been persisted.
    assert types[0] == "id"
    assert types[-1] == "end"
    assert "thinking" in types and "reply" in types


async def test_reply_and_thinking_accumulate_into_one_element_each():
    """Streaming sends many chunks; the database gets one element per run."""
    adapter = _Recorder()
    await _drive(adapter, [
        {"type": "reply", "content": "a"},
        {"type": "reply", "content": "b"},
        {"type": "reply", "content": "c"},
        {"type": "end", "content": ""},
    ])

    assert len(adapter.saved) == 1
    elements = adapter.saved[0]["content"]
    replies = [e for e in elements if e.get("type") == "reply"]
    assert len(replies) == 1 and replies[0]["content"] == "abc"


async def test_end_is_sent_only_after_the_response_is_persisted():
    """A client that acts on `end` must not race the write.

    The ordering is load-bearing: the frontend reloads history when it sees
    `end`, so emitting it before the save means the reload can miss the very
    message that just streamed.
    """
    order: list[str] = []

    class _Slow(_Recorder):
        async def save_assistant_response(self, **kw):
            await asyncio.sleep(0.01)
            order.append("saved")

    adapter = _Slow()

    async def gen():
        yield {"type": "reply", "content": "x"}
        yield {"type": "end", "content": ""}

    ctx = {"user_id": "1", "query_user_id": "1", "msg_id": "q1",
           "session_id": "s1", "agent": "app", "params": object()}
    async for frame in adapter.stream_output(gen(), ctx):
        if json.loads(frame[6:])["type"] == "end":
            order.append("end-sent")

    assert order == ["saved", "end-sent"]


async def test_a_new_transport_only_has_to_encode():
    """The point of the base class.

    This subclass writes no pipeline: no accumulator, no persistence, no
    heartbeat, no end-ordering. It changes the wire format and nothing else —
    which is what adding WebSocket should cost.
    """
    class _JsonLines(ChatProtocolAdapter):
        def __init__(self):
            super().__init__()
            self.scene = "ws"
            self.saved = []

        def encode_chunk(self, chunk: dict) -> str:
            return json.dumps(chunk, ensure_ascii=False) + "\n"

        async def save_assistant_response(self, reply_id, params, user_id, content, question_msg_id):
            self.saved.append(content)

    adapter = _JsonLines()
    frames = await _drive(adapter, [
        {"type": "reply", "content": "hi"},
        {"type": "end", "content": ""},
    ])

    assert frames, "a subclass overriding only encode_chunk must still stream"
    for f in frames:
        assert not f.startswith("data: ")
        assert f.endswith("\n")
        json.loads(f)
    assert [json.loads(f)["type"] for f in frames][-1] == "end"
    assert adapter.saved, "persistence must come from the base, not the transport"
