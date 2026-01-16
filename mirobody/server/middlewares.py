import time, uuid

from typing import Callable, Any

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from starlette.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware

from ..user import JwtTokenValidator

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

    return dict(url=url, base_url=base_url, path=path, method=method)


class JwtMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        dispatch = None,
        jwt_key: str = "",
        decode_func: Callable[[str], int] | None = None
    ):
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

        #-------------------------------------------------
        # Check JWT token.

        request.state.user_id = 0

        if self._token_validator:
            token = request.headers.get("Authorization")
            if token and isinstance(token, str):
                while token.startswith("Bearer "):
                    token = token[7:]

                if token:
                    payload, err = self._token_validator.verify_token(token)
                    if not err and payload:
                        if isinstance(payload, dict) and "sub" in payload:
                            sub = payload["sub"]
                            if sub:
                                if self._decode_func:
                                    request.state.user_id = self._decode_func(sub)
                                else:
                                    try:
                                        request.state.user_id = int(sub)
                                    except:
                                        request.state.user_id = 0

        #-------------------------------------------------

        if request.state.user_id > 0:
            ctx = {
                "user_id": request.state.user_id
            }
            try:
                ctx.update(get_request_info(request))
            except Exception:
                pass

            # Get user's language.
            request.state.language = ""
            for key in ["x-language", "X-Language", "accept-language", "Accept-Language"]:
                if key in request.headers:
                    value = request.headers.get(key)
                    if value:
                        languages = value.split(",")
                        for language in languages:
                            language_code = language.split(";")[0].strip()
                            if language_code and language_code != "*":
                                request.state.language = language_code
                                ctx["language"] = request.state.language
                                break
                        if request.state.language:
                            break

            # Get user's timezone.
            request.state.timezone = ""
            for key in ["x-timezone", "X-Timezone", "timezone"]:
                if key in request.headers:
                    request.state.timezone = request.headers.get(key)
                    ctx["timezone"] = request.state.timezone
                    break

            # Get request trace ID.
            request.state.trace_id = ""
            for key in ["traceid", "trace_id", "X-Request-Id", "x-request-id"]:
                if key in request.headers:
                    request.state.trace_id = request.headers.get(key)
                    ctx["trace_id"] = request.state.trace_id
                    break

            if not request.state.trace_id:
                request.state.trace_id = str(uuid.uuid4())
                ctx["trace_id"] = request.state.trace_id

            if ctx:
                from ..utils.req_ctx import REQ_CTX
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
        redis_client: Redis | None = None
    ):
        self._url_paths = url_paths if isinstance(url_paths, dict) else None
        self._redis_client = redis_client
        self._cache_key_prefix = "limit:"

        super().__init__(app, dispatch)

    #-----------------------------------------------------

    async def dispatch(self, request, call_next) -> Response:
        if self._url_paths and \
            request.state.user_id > 0 and \
            self._redis_client:

            threshold = self._url_paths.get(request.url.path)
            if isinstance(threshold, int) and threshold > 0:
                key = f"{self._cache_key_prefix}{request.state.user_id}:{request.url.path}"
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
