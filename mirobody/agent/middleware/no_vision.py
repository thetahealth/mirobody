"""A model that cannot see never receives an image block.

The file backend already answers an image read with its OCR text when the
model cannot see (`PgFilesystemBackend._image_as_text`). deepagents' `read_file`
still wraps any `.jpg`/`.png` result as an image block, by extension and
whatever the encoding, so the OCR text went out as a fake base64 image and a
text-only server answered 500 "image input is not supported" (MiniCPM5-2B,
measured). This turns such a block back into the text it carries.

An image already in the thread is the other way in: read while a model that
sees answered, then sent again with every request after a switch to one that
does not, failing each turn. deepagents' `UnsupportedContentMiddleware` strips
such blocks by the model's profile, and the `local` entry's profile says it
sees, because the entry was written for a model that does; whether the model
running now sees is the server's answer (`served.sees`). So for a model the
server calls blind, every request goes out with its image blocks replaced by
a line saying an image was there.
"""

from __future__ import annotations

from typing import Any

from deepagents.middleware.unsupported_content import UnsupportedContentMiddleware
from langchain_core.messages import ToolMessage

#: Where the backend's OCR note starts; any other image payload is real bytes.
_BACKEND_NOTE = "[You cannot see images"
_UNREADABLE = (
    "[You cannot see images, and this image could not be read as text. Do not guess "
    "what it shows: say you cannot see the photo and ask the user to describe it.]"
)
_MEDIA_BLOCKS = frozenset({"image", "file", "video", "audio"})


def as_text(result: Any) -> Any:
    """`result` with every media block of a `read_file` answer replaced by text."""
    if not isinstance(result, ToolMessage) or result.name != "read_file":
        return result
    blocks = result.content_blocks
    if not any(block.get("type") in _MEDIA_BLOCKS for block in blocks):
        return result
    parts = []
    for block in blocks:
        if block.get("type") in _MEDIA_BLOCKS:
            payload = str(block.get("base64") or "")
            parts.append(payload if payload.startswith(_BACKEND_NOTE) else _UNREADABLE)
        elif block.get("type") == "text":
            parts.append(str(block.get("text") or ""))
    return ToolMessage(content="\n\n".join(p for p in parts if p), name=result.name,
                       tool_call_id=result.tool_call_id, status=result.status)


_IMAGE_IN_HISTORY = "[An image was attached here. You cannot see images: do not guess what it showed.]"


class NoVisionReadMiddleware(UnsupportedContentMiddleware):
    """Added to the stack only for a model that cannot see."""

    def _is_supported(self, block: Any, *, model: Any, in_tool_message: bool) -> bool:
        if block.get("type") in ("image", "video"):
            return False
        return super()._is_supported(block, model=model, in_tool_message=in_tool_message)

    def _replace(self, block: Any, message: Any) -> Any:
        if block.get("type") in ("image", "video"):
            return {"type": "text", "text": _IMAGE_IN_HISTORY}
        return super()._replace(block, message)

    async def awrap_tool_call(self, request: Any, handler: Any) -> Any:
        return as_text(await handler(request))

    def wrap_tool_call(self, request: Any, handler: Any) -> Any:
        return as_text(handler(request))


__all__ = ["NoVisionReadMiddleware", "as_text"]
