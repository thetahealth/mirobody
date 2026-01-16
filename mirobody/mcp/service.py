import json, logging, secrets, urllib

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from datetime import datetime

from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from typing import Any

from ..utils import (
    get_jwt_token,

    json_response,
    json_response_with_code,

    jsonrpc_result,
    jsonrpc_error
)

from ..user import (
    check_relationship,

    AbstractTokenValidator
)

from .resource import load_resources_from_directories
from .tool import load_tools_from_directories, call_tool

#-----------------------------------------------------------------------------

CODE_PARSE_ERROR = -32700
CODE_INVALID_REQUEST = -32600
CODE_METHOD_NOT_FOUND = -32601
CODE_INVALID_PARAMS = -32602
CODE_INTERNAL_ERROR = -32603

# Implementation specific errors: -32000 to -32099.

# Custom application errors: -32768 to -32000.
CODE_AUTH_REQUIRED = -32000

#-----------------------------------------------------------------------------

class ResponseEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


#-----------------------------------------------------------------------------

class McpService:

    _global_instance = None

    def __init__(
        self,

        token_validator: AbstractTokenValidator | None = None,

        protocol_version: str = "",

        name: str = "",
        version: str = "",

        uri_prefix: str = "",
        routes: list | None = None,

        tool_dirs: list[str] = [],
        resource_dirs: list[str] = [],

        private_tool_dirs: list[str] = [],
        private_resource_dirs: list[str] = [],

        web_server_url: str = "",
        mcp_server_url: str = "",
        data_server_url: str = "",

        db_pool: AsyncConnectionPool[Any] | None = None,
        redis: Redis | None = None
    ):
        self._token_validator = token_validator

        self._protocol_version = protocol_version if protocol_version else "2025-06-18"
        self._name = name if name else "Theta MCP Server"
        self._version = version if version else "1.0.0"

        self._uri_prefix = uri_prefix

        # self._web_server_url  = web_server_url
        # self._mcp_server_url = mcp_server_url
        # self._data_server_url = data_server_url

        self._db_pool = db_pool

        self._redis = redis
        if self._redis:
            self._mcp_url_keyprefix = "mirobody:mcp:url:"
            self._temporary_mcp_url_keyprefix = "mirobody:mcp:url:temp:"
        else:
            self._mcp_urls = {}

        #----------------------------------------------

        self._resource_map, self._resources = load_resources_from_directories(resource_dirs)
        self._resources_count = len(self._resources)

        # for key in self._resource_map:
        #     if "text" in self._resource_map[key]:
        #         self._resource_map[key]["text"] = self._resource_map[key]["text"] \
        #             .replace("{{WEB_SERVER_URL}}", self._web_server_url) \
        #             .replace("{{MCP_SERVER_URL}}", self._mcp_server_url) \
        #             .replace("{{DATA_SERVER_URL}}", self._data_server_url)

        #----------------------------------------------

        self._callable, self._tool_descriptions = load_tools_from_directories(tool_dirs)

        if not self._callable:
            self._callable = {}

        if not self._tool_descriptions:
            self._tool_descriptions = []

        #----------------------------------------------

        self._tools_count = 0
        self._auth_tools_count = 0
        for tool_name in self._callable:
            self._tools_count += 1

            tool_info = self._callable[tool_name]
            if not tool_info or "auth" not in tool_info:
                continue

            if tool_info["auth"]:
                self._auth_tools_count += 1

        #-------------------------------------------------

        if routes is not None:
            self.routes = routes
        else:
            self.routes = []

        self.routes.append(Route(f"{uri_prefix}/mcp/{{secret:str}}", endpoint=self.mcp_handler, methods=["POST", "GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/mcp", endpoint=self.mcp_handler, methods=["POST", "GET", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/personal/mcp", endpoint=self.generate_personal_mcp, methods=["POST", "OPTIONS"]))

        #-------------------------------------------------

        if not McpService._global_instance:
            McpService._global_instance = self

    #-----------------------------------------------------

    async def mcp_handler(self, request: Request) -> Response:
        if request.method == "POST":
            # Single HTTP request.
            pass

        elif request.method == "GET":
            # TODO: WebSocket.
            pass

        elif request.method == "OPTIONS":
            # Return straightly.
            return Response(
                content="200 ok",
                status_code=200,
                headers={
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "*",
                    "Access-Control-Allow-Headers": "*"
                }
            )

        else:
            return Response(
                content="405 Method Not Allowed",
                status_code=405,
                headers={
                    "Access-Control-Allow-Origin": "*"
                }
            )

        #-------------------------------------------------

        body = await request.body()
        try:
            jsonrpc = json.loads(body)
        except:
            print()
            print(f"uri: {request.url.path}?{request.url.query}")
            for key, value in request.headers.items():
                print(f"header '{key}': {value}")
            print(f"body: {body}")
            print()

            return jsonrpc_error(
                id=None,
                code=CODE_PARSE_ERROR,
                msg="Invalid request body",
                method="",
                request=request
            )

        logging.info(f"mcp_handler received request {jsonrpc} headers: {request.headers}")
        # According to the MCP specification,
        #   the ID field should always be there.
        id = None
        if "id" in jsonrpc:
            id = jsonrpc["id"]

        if not isinstance(jsonrpc, dict):
            return jsonrpc_error(
                id=id,
                code=CODE_INVALID_REQUEST,
                msg="Invalid request body",
                method="",
                request=request
            )

        if "method" not in jsonrpc or \
            not isinstance(jsonrpc["method"], str) or \
            len(jsonrpc["method"]) == 0:
            return jsonrpc_error(
                id=id,
                code=CODE_INVALID_REQUEST,
                msg="Invalid MCP method",
                method="",
                request=request
            )

        method = jsonrpc["method"]

        url_prefix = f"{"http" if request.url.hostname == "localhost" else "https"}://{request.url.hostname}"

        #-------------------------------------------------

        # tools/list                Discover available tools        Array of tool definitions with schemas
        # tools/call                Execute a specific tool         Tool execution result

        # resources/list            List available direct resources Array of resource descriptors
        # resources/templates/list	Discover resource templates     Array of resource template definitions
        # resources/read            Retrieve resource contents      Resource data with metadata
        # resources/subscribe       Monitor resource changes        Subscription confirmation

        # prompts/list              Discover available prompts      Array of prompt descriptors
        # prompts/get               Retrieve prompt details	Full    prompt definition with arguments

        if method == "tools/list":
            return jsonrpc_result(
                id=id,
                result={
                    "tools": self._tool_descriptions
                },
                method=method,
                request=request
            )

        elif method == "prompts/list":
            return jsonrpc_result(
                id=id,
                result={
                    "prompts": []
                },
                method=method,
                request=request
            )

        elif method == "resources/list":
            return jsonrpc_result(
                id=id,
                result={
                    "resources": self._resources
                },
                method=method,
                request=request
            )

        #-------------------------------------------------

        elif method == "tools/call":

            if "params" not in jsonrpc or not isinstance(jsonrpc["params"], dict):
                return jsonrpc_error(
                    id=jsonrpc["id"],
                    code=CODE_INVALID_PARAMS,
                    msg="Empty parameter",
                    method="tools/call",
                    request=request
                )
            params = jsonrpc["params"]
            logging.debug(json.dumps(params, ensure_ascii=False))

            if "name" not in params or not isinstance(params["name"], str) or len(params["name"]) == 0:
                return jsonrpc_error(
                    id=jsonrpc["id"],
                    code=CODE_INVALID_PARAMS,
                    msg="Empty parameter name",
                    method="tools/call",
                    request=request
                )

            if not self._callable or \
                not isinstance(self._callable, dict) or \
                params["name"] not in self._callable or \
                not self._callable[params["name"]]:
                return jsonrpc_error(
                    id=jsonrpc["id"],
                    code=CODE_INVALID_PARAMS,
                    msg="Unsupported parameter name",
                    method="tools/call",
                    request=request
                )

            #---------------------------------------------

            tool = self._callable[params["name"]]
            jwt_token = get_jwt_token(request)
            logging.debug(f"jwt_token: {jwt_token}")
            user_id = ""

            if tool["auth"] and jwt_token and self._token_validator:
                payload, err = self._token_validator.verify_token(jwt_token)
                if err:
                    logging.warning(err)
                elif not isinstance(payload, dict):
                    logging.warning("Invalid token payload")
                elif "sub" not in payload:
                    logging.warning("No sub field found")
                else:
                    user_id = payload["sub"]

            if tool["auth"] and not user_id:
                try:
                    user_secret = request.path_params["secret"]
                    if user_secret:
                        if self._redis:
                            resp = await self._redis.get(self._mcp_url_keyprefix + user_secret)
                            if resp and isinstance(resp, str):
                                user_id = resp

                            if not user_id:
                                # Then check temporary urls.
                                resp = await self._redis.get(self._temporary_mcp_url_keyprefix + user_secret)
                                if resp and isinstance(resp, str):
                                    user_id = resp
                        else:
                            user_id = self._mcp_urls.get(user_secret, "")
                except:
                    user_id = ""

            if tool["auth"] and not user_id:
                state = secrets.token_urlsafe(32)
                check_interval = 10  # Seconds.
                timeout = 300  # Seconds.

                oauth_params = {
                    "response_type": "code",
                    "client_id": "theta_mcp",
                    "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
                    "scope": "read write",
                    "state": state,
                }

                authorization_url = f"""{url_prefix}/mcplogin?oauth_params={
                urllib.parse.quote(
                    urllib.parse.urlencode(oauth_params)
                )
                }"""

                return jsonrpc_result(
                    id=jsonrpc["id"],
                    result={
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(
                                    {
                                        "success": True,
                                        "message": "OAuth authentication URL generated. Client should open browser automatically",
                                        "authorization_url": authorization_url,
                                        "auto_open_browser": True,
                                        "client_instructions": f"""
🔐 Authentication link generated (direct login mode)

🌐 Authentication URL: {authorization_url}

📋 Please follow these steps:
1. The client should automatically open a browser; if not, please manually copy the link above and open it.
2. Select a login method (Google or email verification code) to complete login.
3. After successful login, it will automatically redirect to the device authorization page.
4. After authorization, the system will automatically handle token acquisition.

💡 Tip: After authentication is complete, you can re-call the relevant health data query tools
""",
                                        "auto_polling": {
                                            "enabled": True,
                                            "state": state,
                                            "check_interval": check_interval,
                                            "max_wait_time": timeout,
                                        }
                                    },
                                    ensure_ascii=False,
                                    separators=(',', ':')
                                )
                            }
                        ]
                    },
                    method="tools/call",
                    request=request
                )

            #---------------------------------------------

            # query_user_id = ""
            # try:
            #     user_secret = request.path_params["secret"]
            #     if user_secret:
            #         if self._redis:
            #             resp = await self._redis.get(self._mcp_url_keyprefix + user_secret)
            #             if resp and isinstance(resp, str):
            #                 query_user_id = resp
            #         else:
            #             query_user_id = self._mcp_urls.get(user_secret, "")
            # except:
            #     query_user_id = ""

            # if query_user_id:
            #     if user_id != query_user_id:
            #         err = await check_relationship(self._db_pool, user_id, query_user_id, ["chat"])
            #         if err:
            #             return jsonrpc_error(
            #                 id      = id,
            #                 code    = CODE_INTERNAL_ERROR,
            #                 msg     = err,
            #                 method  = params["name"],
            #                 request = request
            #             )

            #         user_id = query_user_id

            #---------------------------------------------

            result = await call_tool(
                tools=self._callable,
                tool_name=params["name"],
                arguments=params["arguments"] if "arguments" in params else {},
                user_id=user_id
            )

            is_error = False
            data = result
            if isinstance(result, dict):
                if "redirect_to_upload" in result:
                    return jsonrpc_result(
                        id=jsonrpc["id"],
                        result={
                            "content": [
                                {
                                    "type": "text",
                                    "text": json.dumps(
                                        {
                                            "success": True,
                                            "message": "Health Data uploading URL generated. Client should open browser automatically",
                                            "open_url": f"{url_prefix}/drive",
                                            "auto_open_browser": True,
                                            "client_instructions": "No health data found. To access comprehensive health data including medical records, functional examinations, and device-generated data, please upload your health information first",
                                        },
                                        ensure_ascii=False,
                                        separators=(',', ':')
                                    )
                                }
                            ]
                        },
                        method=params["name"],
                        request=request
                    )

                if "success" in result and isinstance(result["success"], bool):
                    is_error = not result["success"]

                if is_error and "error" in result:
                    data = result["error"]

                elif not is_error and "data" in result:
                    data = result["data"]

            result={
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(data, ensure_ascii=False, separators=(',', ':'), cls=ResponseEncoder)
                    }
                ],
                "isError": is_error,
            }
            if not is_error:
                if isinstance(data, dict):
                    result["structuredContent"] = data
                elif isinstance(data, list):
                    result["structuredContent"] = {"data": data}

            return jsonrpc_result(
                id=jsonrpc["id"],
                result=result,
                method=params["name"],
                request=request
            )

        #-------------------------------------------------

        elif method == "resources/read":
            if "params" not in jsonrpc or not isinstance(jsonrpc["params"], dict):
                return jsonrpc_error(
                    id=jsonrpc["id"],
                    code=CODE_INVALID_PARAMS,
                    msg="Empty parameter",
                    method="resources/read",
                    request=request
                )
            params = jsonrpc["params"]

            if "uri" not in params or not isinstance(params["uri"], str) or len(params["uri"]) == 0:
                return jsonrpc_error(
                    id=jsonrpc["id"],
                    code=CODE_INVALID_PARAMS,
                    msg="Empty parameter uri",
                    method="resources/read",
                    request=request
                )

            uri = params["uri"]
            if uri not in self._resource_map:
                return jsonrpc_result(
                    id=id,
                    result={
                        "contents": [],
                        "_meta": {
                            "error": f"Unknown resource: {uri}"
                        }
                    },
                    method=method,
                    request=request
                )

            resource = self._resource_map[uri]
            if "text" in resource:
                current_server = f"{"http" if request.url.hostname == "localhost" else "https"}://{request.url.hostname}"
                resource["text"] = resource["text"] \
                    .replace("{{WEB_SERVER_URL}}", current_server) \
                    .replace("{{MCP_SERVER_URL}}", current_server) \
                    .replace("{{DATA_SERVER_URL}}", current_server)
            if isinstance(resource, dict) and "text" in resource:
                jwt_token = get_jwt_token(request)
                if jwt_token is None:
                    jwt_token = ""

                resource["text"] = resource["text"].replace("{{JWT_TOKEN}}", jwt_token)

            return jsonrpc_result(
                id=id,
                result={
                    "contents": [
                        resource,
                    ]
                },
                method=method,
                request=request
            )

        #-------------------------------------------------

        elif method == "initialize":
            return jsonrpc_result(
                id=id,
                result={
                    "protocolVersion": self._protocol_version,
                    "capabilities": {
                        "prompts": {},
                        "resources": {},
                        "tools": {
                            "listChanged": False
                        }
                    },
                    "serverInfo": {
                        "name": self._name,
                        "version": self._version
                    }
                },
                method=method,
                request=request
            )

        elif method == "notifications/initialized":
            return json_response(
                content="",
                status_code=200,
                request=request
            )

        elif method == "ping":
            return jsonrpc_result(
                id=id,
                result={},
                method=method,
                request=request
            )

        #-------------------------------------------------

        else:
            return jsonrpc_error(
                id=id,
                code=CODE_METHOD_NOT_FOUND,
                msg="MCP method not found",
                request=request
            )

    #-----------------------------------------------------

    async def generate_personal_mcp(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        #-------------------------------------------------

        if not self._token_validator:
            return json_response_with_code(-1, "No JWT token validator.", request=request)

        token = get_jwt_token(request)

        payload, err = self._token_validator.verify_token(token)
        if err:
            return json_response_with_code(-2, err, request=request)
        if not payload:
            return json_response_with_code(-3, "Empty token payload.", request=request)

        #-------------------------------------------------

        try:
            data = await request.json()
            beneficiary_user_id = data.get("user_id", "")
        except:
            beneficiary_user_id = ""

        user_id = payload.get("sub")
        if not user_id or not isinstance(user_id, str):
            return json_response_with_code(-4, "Invalid user ID.", request=request)

        if len(beneficiary_user_id) > 0 and beneficiary_user_id != user_id:
            err = await check_relationship(self._db_pool, user_id, beneficiary_user_id, ["chat"])
            if err:
                return json_response_with_code(-5, err, request=request)

            user_id = beneficiary_user_id

        #-------------------------------------------------

        # Get existing user secret.
        if self._redis:
            try:
                user_secret = await self._redis.get(self._mcp_url_keyprefix+user_id)
            except Exception as e:
                logging.warning(str(e))
                user_secret = ""
        else:
            user_secret = self._mcp_urls.get(user_id, "")

        if not user_secret:
            # Generate a new user secret.
            user_secret = secrets.token_urlsafe(64)

            if self._redis:
                try:
                    await self._redis.set(self._mcp_url_keyprefix+user_secret, user_id)
                    await self._redis.set(self._mcp_url_keyprefix+user_id, user_secret)
                except Exception as e:
                    return json_response_with_code(-6, str(e), request=request)
            else:
                self._mcp_urls[user_secret] = user_id
                self._mcp_urls[user_id] = user_secret

        #-------------------------------------------------

        url_prefix = f"{"http" if request.url.hostname == "localhost" else "https"}://{request.url.hostname}"

        return json_response_with_code(data={"url": f"{url_prefix}/mcp/{user_secret}"}, request=request)

    #-----------------------------------------------------

    @classmethod
    async def generate_temporary_personal_mcp(cls, user_id: str) -> tuple[str | None, str | None]:
        if not user_id:
            return None, "Empty user ID."

        if not cls._global_instance:
            return None, "Invalid MCP service."

        service = cls._global_instance

        if not service._redis:
            return None, "Invalid redis connection."
        
        #-------------------------------------------------

        user_secret = f"{int(datetime.now().timestamp())}-{secrets.token_urlsafe(32)}"

        try:
            await service._redis.set(
                name    = service._temporary_mcp_url_keyprefix + user_secret,
                value   = user_id,
                ex      = 60 * 10
            )
        except Exception as e:
            return None, str(e)

        #-------------------------------------------------

        return f"{service._uri_prefix}/mcp/{user_secret}", None

#-----------------------------------------------------------------------------
