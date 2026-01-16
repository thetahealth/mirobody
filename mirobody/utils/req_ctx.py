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
