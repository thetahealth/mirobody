"""Serving the built web client.

The client is OPTIONAL: a `pip install` ships no `frontend/`, and the server is
expected to run API + MCP only in that case. `add_htdoc_routes` on a missing
directory is a no-op rather than raising.

This used to walk the tree at startup, read every file into memory, and
register one literal `Route` per file, with a hard-coded whitelist of SPA
paths re-serving index.html. Two failure modes, both observed:

- the whitelist lagged the client's router: `/welcome`, `/chat/:sessionId`,
  `/developer`, `/indicator*` and client-side `/auth/*` routes all 404'd on direct
  navigation or refresh (inventoried against the client's router,
  2026-08-17);
- the literal routes were folded into `FastAPI(routes=...)` at construction,
  before any `include_router` call, so a catch-all fallback could never be
  added there without shadowing the API.

Now it uses `app.frontend()` (FastAPI >= 0.138, verified against the installed
0.141.1 source): files come off disk via `StaticFiles` (ETag/Range for free),
and any path with no matching route falls back to index.html — but only for
requests that accept text/html, so an API client still gets a real 404.
`app.frontend()` routes match only after every path operation regardless of
registration order, which is what removes both failure modes structurally.

One thing stays hand-rolled — API-prefix 404 guards: the html-only fallback already protects API clients,
  but a *browser* navigating to a mistyped backend path (`/api/...`, `/mcp/...`)
  would otherwise receive the SPA shell with a 200. Real backend routes are
  registered before this is called, so they win by order; the guards only
  catch what nothing else matched. `/auth` is deliberately NOT guarded
  wholesale: the client router owns callback routes under `/auth`, and a
  deployment's identity gateway may 302 to one of them with tokens in the URL
  fragment — those must reach the SPA shell, not a backend 404. Only the
  backend-owned `/auth/session` and `/auth/webauthn` subtrees get guards.
"""

import os

from fastapi import FastAPI, HTTPException
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# Backend-owned URL prefixes (from the manually-built service routes and every
# APIRouter prefix in server/routers/). A GET under these that nothing matched
# is a mistake, not a SPA deep link — answer 404, not the shell.
_API_PREFIXES = (
    "/api",
    "/mcp",
    "/oauth",
    "/oauth2",
    "/invitation",
    "/apple",
    "/google",
    "/email",
    "/personal",
    "/auth/session",
    "/auth/webauthn",
    "/.well-known",
)

class _CacheControl:
    """Cache headers the static routes cannot set themselves.

    `app.frontend()` exposes no header hook, so this sits in the middleware
    stack instead: Vite's hashed `assets/*` never change content under the
    same name (immutable), while `index.html` and `/mirobody.json` are the
    two files a deployment swap must propagate immediately (no-cache — the
    ETag from StaticFiles makes revalidation cheap, but without no-cache
    browsers apply heuristic freshness and can keep serving a stale shell).
    Responses that already set Cache-Control (e.g. SSE) are left alone.
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope["path"]

        async def send_with_cache_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                if "cache-control" not in headers:
                    status = message["status"]
                    content_type = headers.get("content-type", "")
                    if path.startswith("/assets/") and status == 200:
                        headers["cache-control"] = "public, max-age=31536000, immutable"
                    elif content_type.startswith("text/html") or path == "/mirobody.json":
                        headers["cache-control"] = "no-cache"
            await send(message)

        await self.app(scope, receive, send_with_cache_headers)


async def _api_not_found(_path: str) -> None:
    raise HTTPException(status_code=404)


def add_htdoc_routes(app: FastAPI, dir: str) -> None:
    """Mount the built web client onto `app`.

    Must be called AFTER every real route is registered (path operations and
    routers alike): the API-prefix guards rely on losing to earlier routes,
    and `app.frontend()` is low-priority by construction either way.
    """
    if not dir or not os.path.isdir(dir):
        return

    for prefix in _API_PREFIXES:
        app.add_api_route(
            f"{prefix}/{{_path:path}}",
            _api_not_found,
            methods=["GET", "HEAD"],
            include_in_schema=False,
        )

    app.frontend("/", directory=dir, fallback="index.html")
    app.add_middleware(_CacheControl)
