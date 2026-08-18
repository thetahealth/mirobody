import logging, os

from typing import Any, Callable
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles
from starlette.middleware import Middleware

from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.cors import CORSMiddleware

from .bootstrap import create_schema
from .middleware_stack import build_middlewares
from .htdoc import add_htdoc_routes
from .middlewares import JwtMiddleware, UserInfoUpdaterMiddleware, RequestRateLimiterMiddleware

from .. import __version__
from ..user import (
    AbstractTokenValidator,
    JwtTokenValidator,
    OAuthService,
    UserService
)
from ..mcp import McpService
from ..agent.chat import ChatService

from ..utils.config.storage.constants import DEFAULT_LOCAL_CHARTS_PATH

#-----------------------------------------------------------------------------

class Server:
    def __init__(
        self,

        server_name     : str = "",
        server_version  : str = __version__,

        uri_prefix      : str = "",
        htdoc           : str = "",

        jwt_key         : str = "",
        jwt_private_key : str = "",
        jwt_iss         : str = "",
        jwt_aud         : str = "",
        jwt_client_id   : str = "",
        jwt_scope       : str = "",
        jwt_expires_in  : int = 0,

        jwt_token_validator : AbstractTokenValidator | None = None,
        jwt_sub_decode_func : Callable[[str], int] | None = None,
        gen_jwt_claims_func : Callable[[str, str], dict] | None = None,

        pg_pool         : AsyncConnectionPool[Any] | None = None,
        redis           : Redis | None = None,

        # The following parameters can be generated via
        #   config.get_mcp_options().

        tool_dirs       : list[str] = [],
        resource_dirs   : list[str] = [],

        mcp_server_url  : str = "",

        # The following parameters can be generated via
        #   config.get_agent_options().

        agent_dirs      : list[str] = [],
        private_agent_dirs  : list[str] = [],
        api_keys        : dict[str, str] = {},

        # The following parameters can be generated via
        #   config.get_email_options().

        email_from      : str = "",
        email_from_name : str = "",
        email_template  : str = "",
        email_password  : str = "",
        email_predefined: str | bytes | bytearray | dict[str, str] | None = None,
        email_smtp_host : str = "",
        email_smtp_port : int = 0,
        email_smtp_user : str = "",

        apple_client_id : str = "",
        apple_team_id   : str = "",
        apple_key_id    : str = "",
        apple_private_key       : str = "",
        apple_auth_client_id    : str = "",

        google_client_id        : str = "",


        # The following parameters can be generated via
        #   config.get_webauthn_options().

        webauthn_rp_id          : str = "",
        webauthn_rp_name        : str = "",
        webauthn_origin         : str = "",
        webauthn_mfa_ticket_ttl : int = 300,

        firebase_project_id     : str = "",
        firebase_api_key        : str = "",
        firebase_auth_domain    : str = "",
        firebase_storage_bucket : str = "",
        firebase_messaging_sender_id: str = "",
        firebase_app_id         : str = "",
        firebase_measurement_id : str = "",

        webpage_config          : dict[str, Any] | None = None,

        url_paths_for_user_info_updater     : list[str] | None = None,      # ["url_path"]
        url_paths_for_request_rate_limiter  : dict[str, int] | None = None, # {"url_path": requests_per_minute}

        local_chart_dir         : str = "",

        http_headers            : dict[str, str] | None = None,

        **kwargs
    ):
        self._pg_pool = pg_pool

        self._redis = redis
        logging.info(f"Server is running in {"Redis" if self._redis else "local memory"} mode.")

        self._jwt_token_validator = jwt_token_validator \
            if jwt_token_validator \
            else JwtTokenValidator(
                key         = jwt_key,
                iss         = jwt_iss,
                aud         = jwt_aud,
                client_id   = jwt_client_id,
                scope       = jwt_scope,
                expires_in  = jwt_expires_in
            )

        #-------------------------------------------------

        self._webpage_config = webpage_config
        if not self._webpage_config:
            self._webpage_config = {}

        if not firebase_project_id:
            if "__FIREBASE_PROJECT_ID__" in self._webpage_config:
                firebase_project_id = self._webpage_config["__FIREBASE_PROJECT_ID__"]
                if firebase_project_id and not isinstance(firebase_project_id, str):
                    firebase_project_id = None

        if "__IS_GOOGLE_LOGIN_ON__" not in self._webpage_config:
            self._webpage_config["__IS_GOOGLE_LOGIN_ON__"] = True if google_client_id or firebase_project_id else False

        if "__IS_APPLE_LOGIN_ON__" not in self._webpage_config:
            self._webpage_config["__IS_APPLE_LOGIN_ON__"] = True if apple_client_id else False

        if "__IS_WEBAUTHN_ON__" not in self._webpage_config:
            self._webpage_config["__IS_WEBAUTHN_ON__"] = True if webauthn_rp_id else False

        #-------------------------------------------------

        os.environ.update([
            ("USER_AGENT", f"{server_name if server_name else "Theta MCP Server"} {server_version if server_version else __version__}")
        ])

        #-------------------------------------------------

        self._routes = []

        self._oauth_service = OAuthService(
            token_validator     = self._jwt_token_validator,
            gen_jwt_claims_func = gen_jwt_claims_func,

            uri_prefix      = uri_prefix,
            routes          = self._routes,

            redis           = self._redis
        )

        self._user_service = UserService(
            token_validator = self._jwt_token_validator,

            uri_prefix      = uri_prefix,
            routes          = self._routes,

            db_pool         = self._pg_pool,
            redis           = self._redis,

            # Email login.
            email_from      = email_from,
            email_from_name = email_from_name,
            email_template  = email_template,
            email_password  = email_password,
            email_predefined= email_predefined,
            email_smtp_host = email_smtp_host,
            email_smtp_port = email_smtp_port,
            email_smtp_user = email_smtp_user,

            # Apple login.
            apple_client_id = apple_client_id,
            apple_team_id   = apple_team_id,
            apple_key_id    = apple_key_id,
            apple_private_key   = apple_private_key,
            apple_auth_client_id= apple_auth_client_id,

            # Google login.
            google_client_id    = google_client_id,
            firebase_project_id = firebase_project_id,


            # WebAuthn (AAL2).
            webauthn_rp_id      = webauthn_rp_id,
            webauthn_rp_name    = webauthn_rp_name,
            webauthn_origin     = webauthn_origin,
            webauthn_mfa_ticket_ttl = webauthn_mfa_ticket_ttl,
        )

        self._mcp_service = McpService(
            token_validator = self._jwt_token_validator,

            name            = server_name,
            version         = server_version,

            uri_prefix      = uri_prefix,
            routes          = self._routes,

            tool_dirs       = tool_dirs,
            resource_dirs   = resource_dirs,

            db_pool         = self._pg_pool,
            redis           = self._redis
        )

        self._chat_service = ChatService(
            token_validator = self._jwt_token_validator,

            db_pool         = self._pg_pool,

            uri_prefix      = uri_prefix,
            routes          = self._routes,

            agent_dirs          = agent_dirs,
            private_agent_dirs  = private_agent_dirs,
        )

        self._routes.append(Route(f"{uri_prefix}/api/health", endpoint=self.health_check_handler, methods=["GET"]))

        # Add static file serving for chart images.
        charts_dir = local_chart_dir or DEFAULT_LOCAL_CHARTS_PATH
        if os.path.exists(charts_dir):
            self._routes.append(Mount("/charts", app=StaticFiles(directory=charts_dir)))
            logging.info(f"Chart static files enabled: {charts_dir}")
        else:
            logging.warning(f"Chart directory not found: {charts_dir}. Please ensure it exists or is mounted.")

        if htdoc:
            self._routes.append(
                Route(
                    "/mirobody.json",
                    endpoint=lambda x: JSONResponse(content=self._webpage_config),
                    methods=["GET", "HEAD"]
                )
            )

            async def auth_init_endpoint(request: Request) -> Response:
                return JSONResponse(
                    content={
                        "apiKey": firebase_api_key,
                        "authDomain": firebase_auth_domain if request.url.hostname == "localhost" else request.url.hostname,
                        "projectId": firebase_project_id,
                        "storageBucket": firebase_storage_bucket,
                        "messagingSenderId": firebase_messaging_sender_id,
                        "appId": firebase_app_id,
                        "measurementId": firebase_measurement_id
                    }
                )
            self._routes.append(
                Route("/__/auth/init.json", endpoint=auth_init_endpoint, methods=["GET", "HEAD"])
            )
            self._routes.append(
                Route("/__/firebase/init.json", endpoint=auth_init_endpoint, methods=["GET", "HEAD"])
            )

            # The static client itself is mounted by `add_htdoc_routes` in
            # `start()`, after every router — its SPA fallback must lose to
            # all real routes. Only the config endpoints the client fetches
            # at boot (/mirobody.json, the Firebase init) live here.

        #-------------------------------------------------

        self._middlewares = build_middlewares(
            http_headers=http_headers,
            jwt_key=jwt_key,
            jwt_sub_decode_func=jwt_sub_decode_func,
            url_paths_for_request_rate_limiter=url_paths_for_request_rate_limiter,
            url_paths_for_user_info_updater=url_paths_for_user_info_updater,
            redis=self._redis,
            pg_pool=self._pg_pool,
        )
    #-----------------------------------------------------

    async def health_check_handler(self, request: Request) -> Response:
        # `agents` reads the module-global agent registry rather than a count
        # cached on ChatService: the registry is what `get_global_agent` resolves
        # against, and the cached `_agent_count` attribute no longer exists —
        # this handler raised AttributeError on every /api/health call.
        from ..agent.chat.agent import get_global_agent_count

        return JSONResponse(
            content = {
                "service"               : self._mcp_service._name,
                "version"               : self._mcp_service._version,
                "tools"                 : self._mcp_service._tools_count,
                "public_tools"          : (self._mcp_service._tools_count - self._mcp_service._auth_tools_count),
                "authenticated_tools"   : self._mcp_service._auth_tools_count,
                "resources"             : self._mcp_service._resources_count,
                "agents"                : get_global_agent_count(),
            }
        )

    #-----------------------------------------------------

    def get_routes(self) -> list:
        return self._routes

    def get_middlewares(self) -> list:
        return self._middlewares

    #-----------------------------------------------------

    @staticmethod
    async def start(yaml_files: list[str] = [], fastapi_routers: list = []):
        # Load configuration via file.
        from ..utils import Config
        config = await Config.init(yaml_filenames=yaml_files)
        config.print()

        await create_schema(config)

        #-----------------------------------------------------
        # Init mirobody server.
        
        # Create global resources (PostgreSQL pool and Redis client)
        pg_pool = await config.get_postgresql().get_async_pool()
        redis = await config.get_redis().get_async_client()

        server = Server(
            server_name     = config.http.name,
            server_version  = config.http.version,

            uri_prefix      = config.http.uri_prefix,
            htdoc           = config.http.htdoc,

            # jwt_key         = config.jwt_key,
            # jwt_private_key = config.jwt_private_key,

            pg_pool         = pg_pool,
            redis           = redis,

            webpage_config  = config.get_dict("MIROBODY_WEB_CONFIG", {}),

            url_paths_for_request_rate_limiter  = config.get_dict("REQUEST_RATE_LIMITER"),
            url_paths_for_user_info_updater     = config.get_list("USER_INFO_UPDATER"),

            local_chart_dir = config.get_str("LOCAL_CHARTS_DIR"),

            http_headers    = config.http.headers or {},

            **config.get_mcp_options(),
            **config.get_agent_options(),

            **config.get_jwt_options(),
            **config.get_email_options(),
            **config.get_apple_options(),
            **config.get_google_options(),
            **config.get_firebase_options()
        )

        #-----------------------------------------------------
        # Init fastapi server.

        from fastapi import FastAPI
        app = FastAPI(
            debug       = config.log.level <= logging.DEBUG,
            routes      = server.get_routes(),
            middleware  = server.get_middlewares()
        )

        # Store global resources in app.state for access by all routers
        app.state.redis = redis
        app.state.pg_pool = pg_pool
        
        logging.info(f"Global resources stored in app.state: Redis={'enabled' if redis else 'disabled'}, PostgreSQL={'enabled' if pg_pool else 'disabled'}")

        #-----------------------------------------------------
        # Add other routers.

        from .routers.middleware import init
        await init()

        from mirobody.server.routers import (
            public_router as pulse_public_router,
            apple_router,
            manage_router,
            user_router,
            file_router,
            session_share_router,
            sharing_router,
            indicator_router,
        )
        app.include_router(pulse_public_router)
        # apple_router is ALSO nested inside pulse_public_router (routers/__init__),
        # so Apple Health uploads answer on both /apple/* and /api/v1/pulse/apple/*.
        # docs/apple-health.md documents POST /apple/health, and a self-hosted
        # deployment's uploader may point at either surface; keep both mounts.
        # (The former import alias `old_router` was misleading: the actual legacy
        # /api/v1/health/apple-health router was never registered here — its dead
        # remains were removed from apple_router.py.)
        app.include_router(apple_router)
        app.include_router(manage_router)
        app.include_router(file_router)
        app.include_router(user_router)
        app.include_router(session_share_router)
        app.include_router(sharing_router)
        app.include_router(indicator_router)

        for router in fastapi_routers:
            app.include_router(router)

        # Last on purpose: the SPA fallback and the API-prefix 404 guards
        # only work if every real route above is already registered.
        add_htdoc_routes(app, config.http.htdoc)

        #-----------------------------------------------------
        # Start asgi server.

        config.print_predefined_codes()

        import uvicorn
        asgi_server = uvicorn.Server(
            uvicorn.Config(
                app         = app,
                host        = config.http.host,
                port        = config.http.port,
                headers     = config.http.headers,
                log_level   = config.log.level if config.log.level <= logging.DEBUG else logging.WARNING
            )
        )
        await asgi_server.serve()

#-----------------------------------------------------------------------------
