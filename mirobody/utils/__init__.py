"""Shared helpers, imported one module at a time.

This package is consumed by two very different installs: the ``[app]`` server,
which uses everything here, and the ``[agent]`` library extra — or a consumer
that wants only ``utils.sse`` / ``utils.net`` / ``utils.llm_output`` /
``utils.prompts``. A package ``__init__`` that eagerly imported the config
loader, the HTTP helpers and the database module would make ``import
mirobody.utils.sse`` require ``ruamel.yaml``, ``starlette`` and a database
driver. So the names this package re-exports are resolved lazily (PEP 562):
``from mirobody.utils import execute_query`` still works, and importing one
leaf module costs only that module.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

#: re-exported name -> the module that defines it.
_EXPORTS = {
    "Config": ".config",
    "global_config": ".config",
    "safe_read_cfg": ".config",
    "JsonFormatter": ".log",
    "secret_fingerprint": ".log",
    "init_log_console": ".log",
    "init_log_file": ".log",
    "init_log_tqdm": ".log",
    "init_log": ".log",
    "request_origin": ".http",
    "get_client_ip": ".http",
    "get_jwt_token": ".http",
    "json_response": ".http",
    "json_response_with_code": ".http",
    "jsonrpc_result": ".http",
    "jsonrpc_error": ".http",
    "redirect": ".http",
    "Request": "starlette.requests",
    "Response": "starlette.responses",
    "StreamingResponse": "starlette.responses",
    "Route": "starlette.routing",
    "execute_query": ".db",
    "get_req_ctx": ".req_ctx",
    "update_req_ctx": ".req_ctx",
}


def __getattr__(name: str):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module, __name__), name)
    globals()[name] = value  # resolve once
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))


if TYPE_CHECKING:  # the same names, visible to type checkers and IDEs
    from starlette.requests import Request
    from starlette.responses import Response, StreamingResponse
    from starlette.routing import Route

    from .config import Config, global_config, safe_read_cfg
    from .db import execute_query
    from .http import (
        get_client_ip,
        get_jwt_token,
        json_response,
        json_response_with_code,
        jsonrpc_error,
        jsonrpc_result,
        redirect,
        request_origin,
    )
    from .log import JsonFormatter, init_log, init_log_console, init_log_file, init_log_tqdm, secret_fingerprint
    from .req_ctx import get_req_ctx, update_req_ctx
