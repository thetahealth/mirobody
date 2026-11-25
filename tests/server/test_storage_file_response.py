"""Stored bytes must leave the file endpoint as one ASGI response body.

A client that joins chunks still reconstructs the file, so asserting on
``.content`` would pass while ``StreamingResponse(io.BytesIO(...))`` emits
one body event per newline. The count below is the regression for #360.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, Request

from mirobody.server.routers.file_router import serve_storage_file

# Opaque bytes with embedded newlines. The handler does not decode them.
payload = b"\xff\xd8" + b"\x00\n\xff" * 1024 + b"\xff\xd9"


def _scope(app: FastAPI) -> dict:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/files/uploads/scan.jpg",
        "raw_path": b"/files/uploads/scan.jpg",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "app": app,
    }


async def test_storage_file_sends_one_binary_body():
    app = FastAPI()
    scope = _scope(app)
    request = Request(scope)
    storage = MagicMock()
    storage.get = AsyncMock(return_value=(payload, None))
    storage.get_content_type_from_filename.return_value = "image/jpeg"

    with (
        patch(
            "mirobody.server.routers.file_router.verify_token_claims",
            new=AsyncMock(return_value=("1", {})),
        ),
        patch(
            "mirobody.server.routers.file_router.lacks_second_factor",
            new=AsyncMock(return_value=False),
        ),
        patch(
            "mirobody.server.routers.file_router._authorize_file_read",
            new=AsyncMock(return_value=True),
        ),
        patch("mirobody.utils.config.storage.get_storage_client", return_value=storage),
    ):
        response = await serve_storage_file(
            request,
            file_path="uploads/scan.jpg",
            authorization="Bearer synthetic",
            access_token=None,
        )

    events: list[dict] = []

    async def send(message: dict) -> None:
        events.append(message)

    async def receive() -> dict:
        # Streaming responses wait here for a disconnect and cancel the wait
        # once the body has been written.
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    await response(scope, receive, send)

    start = next(event for event in events if event["type"] == "http.response.start")
    assert start["status"] == 200
    headers = {
        name.decode("latin-1").lower(): value.decode("latin-1")
        for name, value in start["headers"]
    }
    assert headers["content-type"] == "image/jpeg"
    assert headers["content-disposition"] == 'attachment; filename="scan.jpg"'
    assert headers["cache-control"] == "private, no-store"
    assert headers["content-length"] == str(len(payload))

    bodies = [event for event in events if event["type"] == "http.response.body"]
    assert len(bodies) == 1
    assert bodies[0]["body"] == payload
    assert bodies[0].get("more_body", False) is False
