"""Serving the built web client.

The client is OPTIONAL: a `pip install` ships no `frontend/`, and the server is
expected to run API + MCP only in that case. `add_htdoc_routes` on a missing
directory is a no-op rather than raising.

This used to walk the tree at startup, read every file into memory, and
register one literal `Route` per file, with a hard-coded whitelist of SPA
paths re-serving index.html. Two failure modes, both observed:

- the whitelist lagged the client's router: `/welcome`, `/chat/:sessionId`,
  `/developer`, `/indicator*` and `/auth/wechat/callback` all 404'd on direct
  navigation or refresh (docs/frontend-shipping.md, route inventory of
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

Two things stay hand-rolled:

- The `__/` tree (Firebase's popup-auth helper pages, snapshotted into
  `frontend/__/` because the client build does not produce them): `handler`
  and `iframe` have no file extension, so `StaticFiles` would serve them as
  text/plain and the browser would not render them. They get explicit routes
  with explicit media types, and `handler` keeps its `{{POST_BODY}}`
  templating for the POST leg of the popup flow.
- API-prefix 404 guards: the html-only fallback already protects API clients,
  but a *browser* navigating to a mistyped backend path (`/api/...`, `/mcp/...`)
  would otherwise receive the SPA shell with a 200. Real backend routes are
  registered before this is called, so they win by order; the guards only
  catch what nothing else matched. `/auth` is deliberately NOT guarded
  wholesale: the client router owns `/auth/wechat/callback`. Unreachable in
  the opensource deployment (no WeChat flag is injected into mirobody.json —
  see Server.__init__), but proprietary deployments serve their
  WeChat-enabled client through this same module and their gateway 302s to
  that route with tokens in the URL fragment. Only the backend-owned
  `/auth/session` and `/auth/webauthn` subtrees get guards.
"""

import os

from fastapi import FastAPI, HTTPException
from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
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
    "/wechat",
    "/email",
    "/personal",
    "/auth/session",
    "/auth/webauthn",
    "/.well-known",
)

_MEDIA_TYPES = {
    "js": "application/javascript",
    "json": "application/json",
    "css": "text/css",
}


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


def _build_firebase_helper_routes(dir: str) -> list[Route]:
    routes: list[Route] = []
    helper_dir = os.path.join(dir, "__")
    if not os.path.isdir(helper_dir):
        return routes

    for root, _dirs, files in os.walk(helper_dir):
        for file in files:
            full_path = os.path.join(root, file)
            route_path = "/" + os.path.relpath(full_path, dir)

            with open(full_path, "rb") as f:
                content = f.read()

            suffix = file.rsplit(".", 1)[-1] if "." in file else ""
            # Extensionless helpers (handler, iframe) are pages the browser
            # must render — hence the text/html default.
            media_type = _MEDIA_TYPES.get(suffix, "text/html")

            if route_path == "/__/auth/handler":

                async def handler_endpoint(request: Request, content=content) -> Response:
                    if request.method == "OPTIONS":
                        return Response(status_code=204)
                    body = content
                    if request.method == "POST":
                        body = body.replace(b"{{POST_BODY}}", await request.body())
                    return Response(content=body, media_type="text/html")

                routes.append(
                    Route(route_path, endpoint=handler_endpoint,
                          methods=["GET", "HEAD", "POST", "OPTIONS"])
                )
            else:

                async def file_endpoint(request: Request, content=content,
                                        media_type=media_type) -> Response:
                    return Response(content=content, media_type=media_type)

                routes.append(
                    Route(route_path, endpoint=file_endpoint, methods=["GET", "HEAD"])
                )

    return routes


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

    for route in _build_firebase_helper_routes(dir):
        app.router.routes.append(route)

    app.frontend("/", directory=dir, fallback="index.html")
    app.add_middleware(_CacheControl)
