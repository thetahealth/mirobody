"""HTTP transport: Server-Sent Events.

Everything that is not the wire format — permission checks, the parallel
file/question/history fetch, history replay, chunk accumulation, persisting the
assistant response, end-chunk ordering, heartbeats, client-disconnect handling
— lives in `ChatProtocolAdapter`. This file is what is genuinely HTTP.
"""

import json

from typing import Any

from .base import ChatProtocolAdapter


class HTTPChatAdapter(ChatProtocolAdapter):
    """Streams the chat response as SSE."""

    def encode_chunk(self, chunk: dict[str, Any]) -> str:
        """One SSE event.

        The trailing blank line is what terminates an event; without it a
        client buffers indefinitely.
        """
        return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
