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

from .middlewares import JwtMiddleware, UserInfoUpdaterMiddleware, RequestRateLimiterMiddleware

from ..user import (
    JwtTokenValidator,
    OAuthService,
    UserService
)
from ..mcp import McpService
from ..chat import ChatService

#-----------------------------------------------------------------------------

class Server:
    def __init__(
        self,

        server_name     : str = "",
        server_version  : str = "",

        uri_prefix      : str = "",
        htdoc           : str = "",

        jwt_key         : str = "",
        jwt_private_key : str = "",

        jwt_sub_decode_func : Callable[[str], int] | None = None,
        gen_jwt_claims_func : Callable[[str, str], dict] | None = None,

        pg_pool         : AsyncConnectionPool[Any] | None = None,
        redis           : Redis | None = None,

        # The following parameters can be generated via
        #   config.get_mcp_options().

        tool_dirs       : list[str] = [],
        resource_dirs   : list[str] = [],

        private_tool_dirs       : list[str] = [],
        private_resource_dirs   : list[str] = [],

        web_server_url  : str = "",
        mcp_server_url  : str = "",
        data_server_url : str = "",

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

        apple_client_id : str = "",
        apple_team_id   : str = "",
        apple_key_id    : str = "",
        apple_private_key       : str = "",
        apple_auth_client_id    : str = "",

        google_client_id        : str = "",

        qr_login_url            : str = "",

        firebase_project_id     : str = "",
        firebase_api_key        : str = "",
        firebase_auth_domain    : str = "",
        firebase_storage_bucket : str = "",
        firebase_messaging_sender_id: str = "",
        firebase_app_id         : str = "",
        firebase_measurement_id : str = "",

        webpage_config          : dict[str, Any] | None = None,

        url_paths_for_user_info_updater  : list[str] | None = None,      # ["url_path"]
        url_paths_for_request_rate_limiter : dict[str, int] | None = None, # {"url_path": requests_per_minute}
    ):
        self._pg_pool = pg_pool

        self._redis = redis
        logging.info(f"Server is running in {"Redis" if self._redis else "local memory"} mode.")

        self._jwt_token_validator = JwtTokenValidator(jwt_key)

        self._webpage_config = webpage_config
        if not self._webpage_config:
            self._webpage_config = {}
        self._webpage_config.update(
            {
                "__IS_QR_LOGIN_ON__"        : True if qr_login_url else False,
                "__IS_GOOGLE_LOGIN_ON__"    : True if google_client_id or firebase_project_id else False,
                "__IS_APPLE_LOGIN_ON__"     : True if apple_client_id else False
            }
        )

        os.environ.update([
            ("USER_AGENT", f"{server_name if server_name else "Theta MCP Server"} {server_version if server_version else "1.0.0"}")
        ])

        #-------------------------------------------------

        self._routes = []

        self._oauth_service = OAuthService(
            token_validator     = self._jwt_token_validator,
            gen_jwt_claims_func = gen_jwt_claims_func,

            uri_prefix      = uri_prefix,
            routes          = self._routes,

            web_server_url  = web_server_url,
            mcp_server_url  = mcp_server_url,

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

            # Apple login.
            apple_client_id = apple_client_id,
            apple_team_id   = apple_team_id,
            apple_key_id    = apple_key_id,
            apple_private_key   = apple_private_key,
            apple_auth_client_id= apple_auth_client_id,

            # Google login.
            google_client_id    = google_client_id,
            firebase_project_id = firebase_project_id,

            # QR code login.
            qr_login_url    = qr_login_url
        )

        self._mcp_service = McpService(
            token_validator = self._jwt_token_validator,

            name            = server_name,
            version         = server_version,

            uri_prefix      = uri_prefix,
            routes          = self._routes,

            tool_dirs       = tool_dirs,
            resource_dirs   = resource_dirs,

            private_tool_dirs       = private_tool_dirs,
            private_resource_dirs   = private_resource_dirs,

            web_server_url  = web_server_url,
            mcp_server_url  = mcp_server_url,
            data_server_url = data_server_url,

            db_pool         = self._pg_pool,
            redis           = self._redis
        )

        self._chat_service = ChatService(
            token_validator = self._jwt_token_validator,

            db_pool         = self._pg_pool,
            redis           = self._redis,

            uri_prefix      = uri_prefix,
            routes          = self._routes,

            mcp_server_url  = mcp_server_url,

            agent_dirs          = agent_dirs,
            private_agent_dirs  = private_agent_dirs,
            api_keys            = api_keys
        )

        self._routes.append(Route(f"{uri_prefix}/api/health", endpoint=self.health_check_handler, methods=["GET"]))

        # Add static file serving for chart images
        # Use standard chart directory: ./.theta/mcp/charts
        charts_dir = "./.theta/mcp/charts"
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

            #---------------------------------------------

            self.add_htdoc_routes(
                htdoc,
                {
                    "__FIREBASE_API_KEY__"              : firebase_api_key,
                    "\"__FIREBASE_AUTH_DOMAIN__\""      : f"window.location.hostname === \"localhost\" ? \"{firebase_auth_domain}\" : window.location.hostname",
                    "__FIREBASE_PROJECT_ID__"           : firebase_project_id,
                    "__FIREBASE_STORAGE_BUCKET__"       : firebase_storage_bucket,
                    "__FIREBASE_MESSAGING_SENDER_ID__"  : firebase_messaging_sender_id,
                    "__FIREBASE_APP_ID__"               : firebase_app_id,
                    "__FIREBASE_MEASUREMENT_ID__"       : firebase_measurement_id
                }
            )

        #-------------------------------------------------

        self._middlewares = [
            Middleware(GZipMiddleware,
                       minimum_size=10_000),
            # Middleware(CORSMiddleware,
            #            allow_origins=['*'],
            #            allow_methods=['*'],
            #            allow_headers=['*'],
            #            allow_credentials=True,
            #            )
        ]
        if jwt_key:
            self._middlewares.append(
                Middleware(JwtMiddleware, jwt_key=jwt_key, decode_func=jwt_sub_decode_func)
            )

            if url_paths_for_request_rate_limiter and isinstance(url_paths_for_request_rate_limiter, dict):
                self._middlewares.append(
                    Middleware(RequestRateLimiterMiddleware, url_paths=url_paths_for_request_rate_limiter, redis_client=self._redis)
                )

            if url_paths_for_user_info_updater and isinstance(url_paths_for_user_info_updater, list):
                self._middlewares.append(
                    Middleware(UserInfoUpdaterMiddleware, url_paths=url_paths_for_user_info_updater, pg_pool=self._pg_pool)
                )

    #-----------------------------------------------------

    async def health_check_handler(self, request: Request) -> Response:
        return JSONResponse(
            content = {
                "service"               : self._mcp_service._name,
                "version"               : self._mcp_service._version,
                "tools"                 : self._mcp_service._tools_count,
                "public_tools"          : (self._mcp_service._tools_count - self._mcp_service._auth_tools_count),
                "authenticated_tools"   : self._mcp_service._auth_tools_count,
                "resources"             : self._mcp_service._resources_count,
                "agents"                : self._chat_service._agent_count,
            }
        )

    #-----------------------------------------------------

    def add_htdoc_routes(self, dir: str, placeholders: dict[str, str] = {}):
        index_bytes = None
        auth_handler_bytes = None

        filename_suffix_to_media_type = {
            "js"    : "application/javascript",
            "json"  : "application/json",
            "css"   : "text/css",
            "png"   : "image/png",
            "gif"   : "image/gif",
            "jpeg"  : "image/jpeg",
            "jpg"   : "image/jpeg",
            "svg"   : "image/svg+xml",
            "html"  : "text/html",
            "htm"   : "text/htm"
        }

        for root, dirs, files in os.walk(dir):
            prefix = root.removeprefix(dir) + "/"

            for file in files:
                route = prefix + file

                with open(os.path.join(root, file), "rb") as f:
                    bytes = f.read()

                    suffix = ""
                    pos = file.rfind(".")
                    if pos >= 0:
                        suffix = file[pos+1:].strip().lower()
                    media_type = filename_suffix_to_media_type.get(suffix, "text/html")

                    if placeholders:
                        if media_type.startswith("application/") or media_type.startswith("text/"):
                            s = bytes.decode()
                            for k, v in placeholders.items():
                                s = s.replace(k, v)
                            bytes = s.encode()

                    if route == "/__/auth/handler":
                        auth_handler_bytes = bytes
                        continue
                    elif route == "/index.html":
                        index_bytes = bytes

                    async def file_endpoint(request: Request, content=bytes, media_type=media_type) -> Response:
                        return Response(content=content, media_type=media_type)

                    self._routes.append(
                        Route(route, endpoint=file_endpoint, methods=["GET", "HEAD"])
                    )

        if auth_handler_bytes:
            async def auth_handler_endpoint(request: Request, content=auth_handler_bytes) -> Response:
                if request.method == "OPTIONS":
                    return Response(status_code=204)

                bytes = auth_handler_bytes
                if request.method == "POST":
                    post_body = await request.body()
                    bytes = bytes.replace(b"{{POST_BODY}}", post_body)

                return Response(content=bytes, media_type="text/html")

            self._routes.append(
                Route("/__/auth/handler", endpoint=auth_handler_endpoint, methods=["GET", "HEAD", "POST", "OPTIONS"])
            )

        if index_bytes:
            async def index_endpoint(request: Request, content=index_bytes) -> Response:
                return Response(content=content, media_type="text/html")

            for index_route in ["/login", "/mcplogin", "/chat", "/drive", "/home", "/share/{share_id}", "/"]:
                self._routes.append(
                    Route(index_route, endpoint=index_endpoint, methods=["GET", "HEAD"])
                )

    #-----------------------------------------------------

    def get_routes(self) -> list:
        return self._routes

    def get_middlewares(self) -> list:
        return self._middlewares

    #-----------------------------------------------------

    @staticmethod
    async def start(yaml_files: list[str] = []):
        # Load configuration via file.
        from ..utils import Config
        config = await Config.init(yaml_filenames=yaml_files)
        config.print()

        if os.environ.get("ENV").strip().upper() not in ["TEST", "GRAY", "PROD"]:
            async with await config.get_postgresql().get_async_client(cursor_factory=None) as conn:
                dirname = os.path.join(os.path.dirname(__file__), "..", "res", "sql")

                async with conn.cursor() as cur:
                    for filename in sorted(os.listdir(dirname)):
                        with open(os.path.join(dirname, filename), "r", encoding="utf-8") as f:
                            statements = f.read()
                            try:
                                await cur.execute(statements)
                                logging.info(f"SQL file {filename} executed successfully.")
                            except Exception as e:
                                logging.error(str(e), exc_info=True, extra={"sql_filename": filename})
                    logging.info("SQL files initialization completed.")

        #-----------------------------------------------------
        # Init mirobody server.

        server = Server(
            server_name     = config.http.name,
            server_version  = config.http.version,

            uri_prefix      = config.http.uri_prefix,
            htdoc           = config.http.htdoc,

            jwt_key         = config.jwt_key,
            jwt_private_key = config.jwt_private_key,

            pg_pool         = await config.get_postgresql().get_async_pool(),
            redis           = await config.get_redis().get_async_client(),

            webpage_config  = config.get_dict("MIROBODY_WEB_CONFIG", {}),

            url_paths_for_request_rate_limiter  = config.get_dict("REQUEST_RATE_LIMITER"),
            url_paths_for_user_info_updater     = config.get_list("USER_INFO_UPDATER"),

            **config.get_mcp_options(),

            **config.get_agent_options(),

            **config.get_email_options(),
            **config.get_apple_options(),
            **config.get_google_options(),
            **config.get_qr_options(),
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

        #-----------------------------------------------------
        # Add other routers.

        from ..pulse.router.middleware import init
        await init()

        from mirobody.pulse.router import (
            public_router as pulse_public_router,
            apple_router as old_router,
            manage_router,
            user_router,
            file_router,
            food_router,
            session_share_router,
            skill_router
        )
        app.include_router(pulse_public_router)
        app.include_router(old_router)
        app.include_router(manage_router)
        app.include_router(file_router)
        app.include_router(food_router)
        app.include_router(user_router)
        app.include_router(session_share_router)
        app.include_router(skill_router)

        from mirobody.user.sharing import router as user_invitation_router
        app.include_router(user_invitation_router)

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
