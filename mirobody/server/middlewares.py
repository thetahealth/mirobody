import logging
import re
import time
import uuid

from typing import Any
from collections.abc import Awaitable, Callable

from psycopg_pool import AsyncConnectionPool
from mirobody.utils.ephemeral import EphemeralStore

from starlette.datastructures import Headers, MutableHeaders
from starlette.responses import JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.i18n import language_from_headers

from mirobody.user import JwtTokenValidator
from mirobody.user.auth.bearer import aal2_required_response, bearer_subject, lacks_second_factor, mcp_resource
from mirobody.utils.http import loggable_path, request_origin

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

#: Paths an AAL1 session of an MFA account may still reach: the WebAuthn and
#: session routes that raise it to AAL2 (registering a passkey asks
#: `lacks_second_factor` itself), and the settings read the web client's
#: `ensureAAL2` makes to learn whether a passkey is registered. Matched as a
#: substring so an `API_PREFIX` in front does not matter.
_AAL1_REACHABLE = ("/auth/webauthn/", "/auth/session/")
_AAL1_REACHABLE_GETS = ("/api/user/settings",)


#: What a caller may name its own request id with. It lands in every log line
#: of the request, so an unchecked header was a way to write into the log.
_TRACE_ID = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
TRACE_HEADER = "X-Request-Id"


def _trace_id_from(headers) -> str:
    """The caller's request id when it is a sane one, else a fresh one.

    The two names theta-smart's services already propagate (`X-Request-Id`
    from the iOS client, `X-Trace-Id` from older callers), so one id follows a
    request across both backends' logs.
    """
    for key in (TRACE_HEADER, "X-Trace-Id"):
        value = headers.get(key)
        if value and _TRACE_ID.match(value):
            return value
    return str(uuid.uuid4())


def _aal1_reachable(method: str, path: str) -> bool:
    if any(p in path for p in _AAL1_REACHABLE):
        return True
    return method == "GET" and any(path.endswith(p) for p in _AAL1_REACHABLE_GETS)


#: On every response. `nosniff` stops a browser from running an uploaded file
#: served as text; SAMEORIGIN keeps the page out of other sites' frames; and
#: `same-origin` keeps the full URL out of the Referer sent to other sites,
#: which matters because /mcp/<token> and /share/<id> carry credentials.
SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "SAMEORIGIN",
    "Referrer-Policy": "same-origin",
}

#-----------------------------------------------------------------------------

class ResponseHeadersMiddleware:
    """The outermost middleware: every HTTP response carries `SECURITY_HEADERS`
    and the request's `X-Request-Id`.

    Pure ASGI, so it sees what the JWT middleware, which used to add them,
    never did: a CORS preflight, an unhandled 500, every response of a server
    without JWT_KEY. The id goes in the scope's state, where the layers below
    log under it.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        trace_id = _trace_id_from(Headers(scope=scope))
        scope.setdefault("state", {})["trace_id"] = trace_id

        async def stamped(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for name, value in {**SECURITY_HEADERS, TRACE_HEADER: trace_id}.items():
                    headers.setdefault(name, value)
            await send(message)

        await self.app(scope, receive, stamped)


class UnhandledErrorMiddleware:
    """A request that raised is logged by the house rule and answered with the
    envelope's 500.

    It used to reach Starlette's own handler, which answered plain text (the
    traceback in debug mode) and re-raised for the server to log the
    traceback: a driver's quotes the SQL and its parameters. Installed inside
    the CORS middleware, so a cross-origin client can still read the 500.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started = False

        async def tracked(message: Message) -> None:
            nonlocal started
            started = started or message["type"] == "http.response.start"
            await send(message)

        try:
            await self.app(scope, receive, tracked)
        except Exception as e:
            trace_id = scope.get("state", {}).get("trace_id", "")
            logger.error("request failed: trace_id=%s error_type=%s", trace_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
            # Once the response has started there is nothing left to answer
            # with: the server closes the connection.
            if not started:
                from mirobody.server.envelope import err

                answer = JSONResponse(err(500, "Internal server error.").model_dump(), status_code=500)
                await answer(scope, receive, send)

#-----------------------------------------------------------------------------

class JwtMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        dispatch = None,
        jwt_key: str = "",
        decode_func: Callable[[str], int] | None = None,
        requires_second_factor: Callable[[int], Awaitable[bool]] | None = None,
        uri_prefix: str = "",
    ):
        # The one place a token issued to an MCP client is a credential; see
        # `user/auth/bearer.py`.
        self._uri_prefix = uri_prefix
        self._mcp_path = f"{uri_prefix}/mcp"
        # Whether an account's requests need `aal` >= 2. Without this, MFA
        # protected nothing: sign-in hands an MFA account an AAL1 fallback
        # token, and no route ever asked for more.
        self._requires_second_factor = requires_second_factor
        if jwt_key:
            self._token_validator = JwtTokenValidator(jwt_key)
        else:
            self._token_validator = None

        self._decode_func = decode_func if callable(decode_func) else None

        super().__init__(app, dispatch)

    #-----------------------------------------------------

    async def dispatch(self, request, call_next) -> Response:
        if request.method == "OPTIONS":
            return Response()

        #-------------------------------------------------

        # Record current time.
        request.state.start_time = time.time()

        # Every request gets one, signed in or not, and `ResponseHeadersMiddleware`
        # sends it back: the id is only useful for debugging if the person
        # reporting a failure can quote it.
        trace_id = getattr(request.state, "trace_id", "") or _trace_id_from(request.headers)
        request.state.trace_id = trace_id
        ctx: dict[str, Any] = {"trace_id": trace_id}

        #-------------------------------------------------
        # Check JWT token.

        request.state.user_id = 0
        claims = None

        if self._token_validator:
            token = request.headers.get("Authorization")
            if token and isinstance(token, str):
                while token.startswith("Bearer "):
                    token = token[7:]

                if token:
                    payload, err = self._token_validator.verify_token(token)
                    if not err and isinstance(payload, dict):
                        path = request.url.path
                        at_mcp = path == self._mcp_path or path.startswith(self._mcp_path + "/")
                        request.state.user_id = await bearer_subject(
                            payload,
                            mcp_resource=mcp_resource(request_origin(request), self._uri_prefix) if at_mcp else "",
                            decode=self._decode_func,
                        )
                        if request.state.user_id:
                            claims = payload

        if (request.state.user_id > 0
                and not _aal1_reachable(request.method, request.url.path)
                and await lacks_second_factor(request.state.user_id, claims, self._requires_second_factor)):
            return aal2_required_response()

        #-------------------------------------------------

        if request.state.user_id > 0:
            ctx["user_id"] = request.state.user_id
            # What `utils/log.py` reads from the context besides the trace id.
            # A capability path (/mcp/<token>, /api/share/<id>) is digested.
            ctx["path"] = loggable_path(request.url.path)
            ctx["method"] = request.method

            # Get user's language. Parsed by `utils.i18n`, which the WebSocket
            # upload handshake also calls: this used to be the only copy, and
            # the socket had no language at all.
            request.state.language = language_from_headers(request.headers)
            if request.state.language:
                ctx["language"] = request.state.language

            # Get user's timezone.
            request.state.timezone = ""
            for key in ["x-timezone", "X-Timezone", "timezone"]:
                if key in request.headers:
                    request.state.timezone = request.headers.get(key)
                    ctx["timezone"] = request.state.timezone
                    break

        from mirobody.utils.req_ctx import REQ_CTX
        REQ_CTX.set(ctx)

        #-------------------------------------------------

        return await call_next(request)

#-----------------------------------------------------------------------------

class UserInfoUpdaterMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        dispatch = None,
        url_paths   : list[str] | None = None,
        pg_pool     : AsyncConnectionPool[Any] | None = None
    ):
        self._url_paths = url_paths
        self._pg_pool = pg_pool

        super().__init__(app, dispatch)

    #-----------------------------------------------------

    async def dispatch(self, request, call_next) -> Response:
        if self._url_paths and \
            self._pg_pool and \
            request.url.path in self._url_paths and \
            request.state.user_id > 0 and \
            len(request.state.timezone) > 0 and \
            len(request.state.language) > 0:

            async with self._pg_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE health_app_user SET lang=%s,tz=%s,update_at=CURRENT_TIMESTAMP WHERE id=%s;",
                        (request.state.language, request.state.timezone, request.state.user_id)
                    )
                    await conn.commit()
    
        #-------------------------------------------------

        return await call_next(request)

#-----------------------------------------------------------------------------

class RequestRateLimiterMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        dispatch = None,
        url_paths   : dict[str, int] | None = None,
        ephemeral_client: EphemeralStore | None = None
    ):
        self._url_paths = url_paths if isinstance(url_paths, dict) else None
        self._ephemeral_client = ephemeral_client
        self._cache_key_prefix = "limit:"

        super().__init__(app, dispatch)

    #-----------------------------------------------------

    async def dispatch(self, request, call_next) -> Response:
        if self._url_paths and self._ephemeral_client:

            threshold = self._url_paths.get(request.url.path)
            if isinstance(threshold, int) and threshold > 0:
                # Authenticated requests count per user, unauthenticated ones
                # per client IP. Requiring `user_id > 0` meant the pre-auth
                # endpoints (/password/login, /password/register) could
                # structurally never be limited: the exact routes an online
                # password-guessing attack hits. `request.client.host` is the
                # peer address, not X-Forwarded-For, so behind a proxy it is
                # coarse but unspoofable; deployments that trust their proxy can
                # front this with proxy-level limiting instead.
                if request.state.user_id > 0:
                    counter_id = str(request.state.user_id)
                else:
                    counter_id = f"ip:{request.client.host if request.client else 'unknown'}"
                key = f"{self._cache_key_prefix}{counter_id}:{request.url.path}"
                try:
                    resp = await self._ephemeral_client.incr(key, ttl=60)
                    if isinstance(resp, int) and resp > threshold:
                        resp = await self._ephemeral_client.ttl(key)
                        return Response(
                            status_code=429,
                            headers={
                                "Retry-After": str(resp) if isinstance(resp, int) and resp > 0 else "60"
                            },
                            content="Too Many Requests"
                        )
                except Exception as e:
                    # A limiter that cannot count must not let the burst
                    # through (these are the pre-auth routes), and must not
                    # answer 500 with a traceback per request either.
                    logger.warning("rate limiter unavailable: error_type=%s", type(e).__name__)
                    return Response(status_code=503, headers={"Retry-After": "5"}, content="Service Unavailable")

        #-------------------------------------------------

        return await call_next(request)

#-----------------------------------------------------------------------------
