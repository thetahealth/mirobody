"""Assembling the middleware stack.

Lifted out of `Server.__init__` so the composition root registers a stack rather
than computing one. The two comments below are load-bearing incident records —
both bugs took the whole server down at startup and are easy to reintroduce.
"""

from __future__ import annotations

import logging

from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

from .middlewares import (
    JwtMiddleware,
    RequestRateLimiterMiddleware,
    UserInfoUpdaterMiddleware,
)

logger = logging.getLogger(__name__)


def build_middlewares(
    *,
    http_headers=None,
    jwt_key: str = "",
    jwt_sub_decode_func=None,
    url_paths_for_request_rate_limiter=None,
    url_paths_for_user_info_updater=None,
    redis=None,
    pg_pool=None,
) -> list[Middleware]:
    """Outermost first, in the order Starlette applies them."""
    middlewares: list[Middleware] = [
        Middleware(GZipMiddleware,
                   minimum_size=10_000),
    ]

    # Configure CORS from http_headers config.
    # Extract CORS-related headers if provided; otherwise use secure defaults.
    #
    # `http_headers` arrives as `config.http.headers`, which is a LIST of
    # (name, value) tuples — the shape uvicorn wants — not a dict. Normalize
    # first: calling .get() on it raised AttributeError on EVERY startup, and
    # `or {}` upstream cannot save it because HttpConfig always appends a
    # "Server" header, so the list is never empty (introduced in 06e1ad9).
    cors_headers = dict(http_headers) if http_headers else {}
    if cors_headers:
        allowed_origin = cors_headers.get("Access-Control-Allow-Origin", "")
        allowed_methods = cors_headers.get("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
        allowed_headers = cors_headers.get("Access-Control-Allow-Headers", "Authorization, Content-Type")
        allow_credentials = cors_headers.get("Access-Control-Allow-Credentials", "false").lower() == "true"
        max_age = int(cors_headers.get("Access-Control-Max-Age", "600"))

        # Warn if wildcard origin is used with credentials (invalid per CORS spec).
        if allowed_origin == "*" and allow_credentials:
            # NOTE: do NOT `import logging` here. `logging` is imported at module
            # scope, and a function-local import rebinds the name for the WHOLE of
            # __init__ — which made the `logger.info(...)` at the top of this same
            # method raise UnboundLocalError and took the entire server down at
            # startup (introduced in 06e1ad9, the CORS refactor).
            logging.getLogger(__name__).warning(
                "CORS: Access-Control-Allow-Origin='*' with Allow-Credentials=true "
                "is invalid per the CORS spec and will be rejected by browsers. "
                "Set a specific origin instead."
            )

        middlewares.append(
            Middleware(CORSMiddleware,
                       allow_origins=[allowed_origin] if allowed_origin else [],
                       allow_methods=allowed_methods.split(", ") if "," in allowed_methods else [allowed_methods],
                       allow_headers=allowed_headers.split(", ") if "," in allowed_headers else [allowed_headers],
                       allow_credentials=allow_credentials,
                       max_age=max_age,
                       )
        )
    if jwt_key:
        middlewares.append(
            Middleware(JwtMiddleware, jwt_key=jwt_key, decode_func=jwt_sub_decode_func)
        )

        if url_paths_for_request_rate_limiter and isinstance(url_paths_for_request_rate_limiter, dict):
            middlewares.append(
                Middleware(RequestRateLimiterMiddleware, url_paths=url_paths_for_request_rate_limiter, redis_client=redis)
            )

        if url_paths_for_user_info_updater and isinstance(url_paths_for_user_info_updater, list):
            middlewares.append(
                Middleware(UserInfoUpdaterMiddleware, url_paths=url_paths_for_user_info_updater, pg_pool=pg_pool)
            )

    return middlewares
