"""The routers' response envelope: `{"code", "msg", "data"}`.

The FastAPI routers under `server/routers` answer with this shape: `code`
0 on success, an HTTP-like code on failure, `msg` a sentence for a human,
`data` the payload (always an object, empty on failure, so a client can read
`data.x` without a null check). `utils/http.py:json_response_with_code`, which
the Starlette routes (sign-in, passkeys, chat, MCP) answer with, writes the
same three keys, so a client reads either without knowing which answered.
It used to add `success` and omit an empty `data`; the web client's one
response handler reads `code === 0 || success`, so neither was load-bearing.

These answer otherwise, on purpose:

* `apple_router`: `/apple/health` and `/apple/cda` add `success` and
  `message`, which the mobile client reads; this repository cannot rebuild it.
* `records_router`: the hosted platform's shapes (`{"object": "list", ...}`,
  `{"error": {...}}`), for code written against docs.mirobody.ai.
* `POST /files/upload`: the envelope with a LIST in `data`, one entry per
  file, which the web client's upload reads.
* a token without the second factor its account requires: 403
  `{"detail": {"code": "ERROR_AAL2_REQUIRED"}}` (`user/auth/bearer.py`), the
  shape the web client's interceptor upgrades on.
* bytes, not answers: a stored file, the CSV, VCF and NDJSON exports, the
  vendor OAuth completion page.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, Field

from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


class StandardResponse(BaseModel):
    """A successful answer."""

    code: int = Field(default=0, description="0 on success")
    msg: str = Field(default="ok", description="Response message")
    data: dict[str, Any] = Field(default_factory=dict, description="Response data")


class ErrorResponse(BaseModel):
    """A failed answer. `code` is HTTP-like; the HTTP status itself stays 200
    for these routers, which is what the web client was written against."""

    code: int = Field(default=500, description="Error code")
    msg: str = Field(..., description="Error details")
    data: dict[str, Any] = Field(default_factory=dict, description="Always empty on failure")


def ok(data: dict[str, Any] | None = None, msg: str = "ok") -> StandardResponse:
    return StandardResponse(msg=msg, data=data if data is not None else {})


def err(code: int, msg: str) -> ErrorResponse:
    return ErrorResponse(code=code, msg=msg)


def failed(action: str, exc: Exception, msg: str, *, code: int = 500) -> ErrorResponse:
    """`err(code, msg)` for a request that raised, logged by the house rule.

    The log names the action and the exception's type, never its text, and
    keeps the traceback only when the exception is not a database driver's,
    whose message quotes the SQL with its bound parameters. The reply is the
    fixed `msg`: an exception's text is not a sentence for a client.
    """
    logger.error("%s failed: error_type=%s", action, type(exc).__name__,
                 exc_info=not is_driver_exception(exc))
    return err(code, msg)
