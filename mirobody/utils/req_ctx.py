"""Per-request facts that every layer below the router would otherwise thread.

A `ContextVar` holding one dict for the duration of a request or a WebSocket
session: who is asking, in what language, in what timezone, under what trace
id. `server/middlewares.py` fills it for HTTP, `file_router` for the upload
socket.

Read it through the named accessors below (`request_language()`), not
`get_req_ctx("language", "en")`. Eleven call sites spelled that out, which is
eleven places to get the default wrong and no way to grep for who depends on
the key.
"""

from contextlib import contextmanager
from contextvars import ContextVar

REQ_CTX = ContextVar("request_ctx", default=None)


def get_req_ctx(key, default=None):
    ctx = REQ_CTX.get()
    return ctx[key] if ctx and key in ctx else default


def update_req_ctx(**kwargs):
    ctx = REQ_CTX.get()
    if ctx is not None:
        ctx.update(kwargs)


@contextmanager
def set_req_ctx(data):
    token = REQ_CTX.set(data)
    try:
        yield
    finally:
        REQ_CTX.reset(token)


#: What a caller gets when nothing said otherwise. English, because a message
#: nobody can read is worse than one in the wrong language.
DEFAULT_LANGUAGE = "en"


def request_language() -> str:
    """The language this request asked for, or English."""
    return get_req_ctx("language", DEFAULT_LANGUAGE)


def request_timezone(default: str = "UTC") -> str:
    """The timezone this request stated, or `default`."""
    return get_req_ctx("timezone", default)
