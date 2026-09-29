import re
import time
import uuid

from typing import Any
from collections.abc import Awaitable, Callable

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from starlette.responses import JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

from mirobody.utils.i18n import language_from_headers

from mirobody.user import JwtTokenValidator
from mirobody.user.auth.bearer import bearer_subject, mcp_resource
from mirobody.utils.http import request_origin

#-----------------------------------------------------------------------------

#: Paths an AAL1 session of an MFA account may still reach: the WebAuthn and
#: session routes that raise it to AAL2, and the settings read the web client's
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


def _aal2_required() -> Response:
    # The web client's interceptor keys on `detail.code`, runs the passkey
    # upgrade and retries the request (the same shape as the session routes'
    # ERROR_SESSION_MAX_LIFETIME).
    return JSONResponse(
        {"detail": {"code": "ERROR_AAL2_REQUIRED", "message": "This account requires a passkey for this request."}},
        status_code=403,
    )

#-----------------------------------------------------------------------------

def get_request_info(request):
    try:
        url = str(request.url)
        path = str(request.url.path)
    except Exception:
        host = request.headers.get("Host", "unknown")
        url = f"{request.scheme}://{host}{request.path}"
        path = request.path

    method = request.method
    base_url = str(request.base_url)

    return {"url": url, "base_url": base_url, "path": path, "method": method}


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

        # Every request gets one, signed in or not, and it goes back to the
        # caller: the id is only useful for debugging if the person reporting
        # a failure can quote it. Before, it was minted only for signed-in
        # requests and never left the server.
        trace_id = _trace_id_from(request.headers)
        request.state.trace_id = trace_id
        ctx: dict[str, Any] = {"trace_id": trace_id}

        #-------------------------------------------------
        # Check JWT token.

        request.state.user_id = 0
        aal = 0

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
                            try:
                                aal = int(payload.get("aal") or 0)
                            except (TypeError, ValueError):
                                aal = 0

        if (request.state.user_id > 0 and aal < 2 and self._requires_second_factor
                and not _aal1_reachable(request.method, request.url.path)
                and await self._requires_second_factor(request.state.user_id)):
            refused = _aal2_required()
            refused.headers[TRACE_HEADER] = trace_id
            return refused

        #-------------------------------------------------

        if request.state.user_id > 0:
            ctx["user_id"] = request.state.user_id
            try:
                ctx.update(get_request_info(request))
            except Exception:
                # Best-effort log enrichment. `ctx` already carries the user_id,
                # which is the part anything downstream reads; failing the
                # request because a header could not be parsed would be worse.
                pass

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

        response = await call_next(request)
        response.headers[TRACE_HEADER] = trace_id
        return response

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
        redis_client: Redis | None = None
    ):
        self._url_paths = url_paths if isinstance(url_paths, dict) else None
        self._redis_client = redis_client
        self._cache_key_prefix = "limit:"

        super().__init__(app, dispatch)

    #-----------------------------------------------------

    async def dispatch(self, request, call_next) -> Response:
        if self._url_paths and self._redis_client:

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
                resp = await self._redis_client.incr(key)
                if isinstance(resp, int):
                    if resp == 1:
                        await self._redis_client.expire(key, 60)
                    elif resp > threshold:
                        resp = await self._redis_client.ttl(key)
                        return Response(
                            status_code=429,
                            headers={
                                "Retry-After": str(resp) if isinstance(resp, int) else "60"
                            },
                            content="Too Many Requests"
                        )

        #-------------------------------------------------

        return await call_next(request)

#-----------------------------------------------------------------------------
