import json, logging, secrets, urllib

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from datetime import datetime

from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from typing import Any

from ..utils.http import META_PROTOCOL_VERSION, request_origin

from ..utils import (
    get_jwt_token,

    json_response,
    json_response_with_code,

    jsonrpc_result,
    jsonrpc_error,

    global_config
)

from ..user import (
    check_relationship,

    AbstractTokenValidator
)

from .resource import load_resources_from_directories
from .tool import load_tools_from_directories, call_tool

#-----------------------------------------------------------------------------

CODE_PARSE_ERROR        = -32700
CODE_INVALID_REQUEST    = -32600
CODE_METHOD_NOT_FOUND   = -32601
CODE_INVALID_PARAMS     = -32602
CODE_INTERNAL_ERROR     = -32603

# Implementation specific errors: -32000 to -32099.
#
# 2026-07-28 partitions this range: -32000..-32019 is LEGACY (new codes must not
# be allocated there, and receivers may assume no meaning), while -32020..-32099
# is reserved for the specification itself. CODE_AUTH_REQUIRED below predates
# that policy and stays for compatibility with clients already keyed to it.
CODE_UNSUPPORTED_PROTOCOL_VERSION = -32022  # spec-defined (2026-07-28)

# Custom application errors: -32768 to -32000.
CODE_AUTH_REQUIRED      = -32000

#-----------------------------------------------------------------------------
# Protocol revisions this server implements, newest first. `initialize`
# negotiates against this list; per-request `_meta` (2026-07-28's stateless
# model) is validated against it too.
_LATEST_PROTOCOL_VERSION = "2026-07-28"
_SUPPORTED_PROTOCOL_VERSIONS = (
    "2026-07-28",   # stateless: per-request _meta, resultType, MRTR, no session id
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
)

# Declared once: `initialize` and `server/discover` MUST advertise the same
# capabilities, and they held separate copies of this dict that could drift.
# An empty value means "supported, with no optional sub-capabilities" — so no
# `subscribe`, no `listChanged` on resources, which is what makes
# resources/subscribe and resources/templates/list correctly method-not-found.
_CAPABILITIES = {
    "prompts": {},
    "resources": {},
    "tools": {
        "listChanged": False,
    },
}

# 2026-07-28 caching metadata, emitted on the methods the spec marks cacheable.
# Our tool/resource sets are built once at startup and never change while the
# process runs (no listChanged notifications), so a client may hold them for a
# few minutes. `private` because the tool list is filtered per agent and the
# resources are templated per request — a shared cache must not serve one
# caller's copy to another.
_LIST_CACHE_HINT = (300_000, "private")

#-----------------------------------------------------------------------------

def _by_name(items: list | None, key: str = "name") -> list:
    """Stable ordering for list responses.

    Tools are discovered with `os.scandir`, whose order is filesystem-dependent,
    so `tools/list` could return the same set in a different order on the next
    reconnect. 2026-07-28 calls out deterministic ordering explicitly: the tool
    list sits near the front of the model's context, so a reshuffle invalidates
    the client's upstream prompt cache for no reason.
    """
    if not items:
        return []
    return sorted(items, key=lambda item: str(item.get(key, "")) if isinstance(item, dict) else "")

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

        token_validator         : AbstractTokenValidator | None = None,

        protocol_version        : str = "",
        name                    : str = "",
        version                 : str = "",

        uri_prefix              : str = "",
        routes                  : list | None = None,

        tool_dirs               : list[str] = [],
        resource_dirs           : list[str] = [],

        db_pool                 : AsyncConnectionPool[Any] | None = None,
        redis                   : Redis | None = None,

        **kwargs
    ):
        self._token_validator   = token_validator

        # Newest revision we implement. `initialize` negotiates DOWN to whatever
        # the client asked for when we also support it (see _negotiate_version);
        # it used to ignore the client's request entirely and echo this back,
        # which is a spec violation in every revision.
        self._protocol_version  = protocol_version if protocol_version else _LATEST_PROTOCOL_VERSION
        self._name              = name if name else "Theta MCP Server"
        self._version           = version if version else "1.0.0"

        self._uri_prefix        = uri_prefix

        self._db_pool           = db_pool

        self._redis             = redis
        if self._redis:
            self._mcp_url_keyprefix             = "mirobody:mcp:url:"
            self._temporary_mcp_url_keyprefix   = "mirobody:mcp:url:temp:"
        else:
            self._mcp_urls = {}

        #----------------------------------------------

        self._resource_map, self._resources = load_resources_from_directories(resource_dirs)
        self._resources_count = len(self._resources)

        #----------------------------------------------

        self._callable, self._tool_descriptions = load_tools_from_directories(tool_dirs)

        if not self._callable:
            self._callable = {}

        if not self._tool_descriptions:
            self._tool_descriptions = []

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

    @property
    def _server_info(self) -> dict:
        return {"name": self._name, "version": self._version}

    def _negotiate_version(self, requested: str | None) -> str:
        """Pick the revision to speak with this client.

        Return the client's request when we implement it, otherwise our newest.
        Every MCP revision requires this handshake; the old code returned
        `self._protocol_version` unconditionally, so a client pinned to
        2024-11-05 was told the server was speaking a revision it had never
        agreed to.
        """
        if isinstance(requested, str) and requested in _SUPPORTED_PROTOCOL_VERSIONS:
            return requested
        return self._protocol_version

    @staticmethod
    def _request_protocol_version(jsonrpc: dict) -> str | None:
        """Per-request protocol version from `params._meta` (2026-07-28).

        That revision drops the initialize/initialized handshake and the
        Mcp-Session-Id header: every request instead carries its own protocol
        version, client identity and capabilities in `_meta`, so any request can
        land on any instance behind a load balancer. Reading it here is what
        lets one server answer both stateless 2026-07-28 clients and older
        handshake-based ones.
        """
        params = jsonrpc.get("params")
        if not isinstance(params, dict):
            return None
        meta = params.get("_meta")
        if not isinstance(meta, dict):
            return None
        value = meta.get(META_PROTOCOL_VERSION)
        return value if isinstance(value, str) else None

    #-----------------------------------------------------

    async def _resolve_secret_user(self, user_secret: str) -> str:
        """User id behind a PERMANENT personal-URL secret, or "".

        tools/call has always resolved this (it must, to authorize); tools/list
        needs it too now that part of the tool surface is data-gated per user.
        """
        if not user_secret:
            return ""
        if self._redis:
            try:
                return await self._redis.get(self._mcp_url_keyprefix + user_secret) or ""
            except Exception as e:
                logging.warning("MCP: permanent URL lookup failed: %s", e)
                return ""
        return self._mcp_urls.get(user_secret, "")

    # Tools whose only possible answer without the corresponding data is
    # "no data": each maps to the EXISTS probe that decides its visibility.
    _DATA_GATED = {
        "get_genetic_data":
            "SELECT 1 FROM th_series_data_genetic"
            " WHERE user_id = :uid AND is_deleted = false LIMIT 1",
        "query_health_indicators":
            "SELECT 1 FROM th_series_data"
            " WHERE user_id = :uid AND deleted = 0 LIMIT 1",
    }

    async def _data_gated_tools(self, user_id: str) -> set[str]:
        """Tool names to HIDE from tools/list for this user.

        A data-reading tool for a user with none of that data can only ever
        answer "no data" — listing it makes every client carry its schema for
        nothing. Fails OPEN per probe (nothing hidden) — a DB hiccup must not
        shrink the tool surface of a user who does have data.
        """
        if not user_id:
            return set()

        from ..utils import execute_query

        hidden: set[str] = set()
        for name, probe in self._DATA_GATED.items():
            if name not in self._callable:
                continue
            try:
                rows = await execute_query(probe, {"uid": str(user_id)}, log_sql=False)
                if not rows:
                    hidden.add(name)
            except Exception as e:
                logging.warning("MCP: data gate check failed for %s: %s", name, e)
        return hidden

    #-----------------------------------------------------

    async def mcp_handler(self, request: Request) -> Response:
        if request.method == "POST":
            # Single HTTP request.
            pass

        elif request.method == "GET":
            # TODO: WebSocket.
            pass

        elif request.method == "OPTIONS":
            # Return straightly. CORS headers are handled by CORSMiddleware.
            return Response(
                content     = "200 ok",
                status_code = 200,
            )

        else:
            return Response(
                content     = "405 Method Not Allowed",
                status_code = 405,
            )

        #-------------------------------------------------

        body = await request.body()
        try:
            jsonrpc = json.loads(body)
        except Exception:
            # This used to `print()` the full URL, EVERY request header — including
            # `Authorization: Bearer …` — and the entire unbounded body to stdout.
            # Any unauthenticated caller could trigger it by posting invalid JSON,
            # making it both a bearer-token/PHI leak into the logs and a log-flood
            # DoS. Log the shape of the failure, never its credentials or content.
            logging.warning(
                "MCP: malformed JSON-RPC body",
                extra={
                    "path": request.url.path,
                    "body_bytes": len(body),
                    "content_type": request.headers.get("content-type", ""),
                },
            )

            return jsonrpc_error(
                id      = None,
                code    = CODE_PARSE_ERROR,
                msg     = "Invalid request body",
                method  = "",
                request = request
            )

        # According to the MCP specification the ID field should always be
        # there — but a request that omits it is still well-formed JSON, and
        # every error branch below used to re-index `jsonrpc["id"]` directly.
        # An unauthenticated POST without "id" therefore raised KeyError from
        # inside the handler; with FastAPI debug enabled (which follows
        # LOG_LEVEL, DEBUG by default) that returns a full stack trace to the
        # caller. Extract once here, use `id` everywhere after.
        id = None
        if "id" in jsonrpc:
            id = jsonrpc["id"]

        if not isinstance(jsonrpc, dict):
            return jsonrpc_error(
                id      = id,
                code    = CODE_INVALID_REQUEST,
                msg     = "Invalid request body",
                method  = "",
                request = request
            )

        if "method" not in jsonrpc or \
            not isinstance(jsonrpc["method"], str) or \
            len(jsonrpc["method"]) == 0:
            return jsonrpc_error(
                id      = id,
                code    = CODE_INVALID_REQUEST,
                msg     = "Invalid MCP method",
                method  = "",
                request = request
            )

        method = jsonrpc["method"]

        # The revision THIS request is speaking. 2026-07-28 removed the
        # initialize/initialized handshake, so there is no session in which to
        # remember a negotiated version — the client restates it in `_meta` on
        # every call, and the server has to settle it per request. Handshake-era
        # clients send no `_meta`; they fall through to our newest supported
        # revision, exactly as before.
        negotiated = self._negotiate_version(self._request_protocol_version(jsonrpc))

        url_prefix = request_origin(request)

        #-------------------------------------------------

        user_id     = ""
        session_id  = ""
        agent_name  = ""

        user_secret = request.path_params.get("secret", "")
        if user_secret and self._redis:
            try:
                user_secret_payload = await self._redis.get(self._temporary_mcp_url_keyprefix + user_secret)
                if user_secret_payload:
                    user_secret_params = json.loads(user_secret_payload)

                    if user_secret_params and isinstance(user_secret_params, dict):
                        user_id     = user_secret_params.get("user_id", "")
                        session_id  = user_secret_params.get("session_id", "")
                        agent_name  = user_secret_params.get("agent_name", "")   
            except Exception as e:
                # An unreadable temp-URL payload means the caller silently ends
                # up unauthenticated, which is a confusing failure to debug from
                # the outside. Log the shape — never the secret itself.
                logging.warning("MCP: temporary URL lookup failed: %s", e)

        #-------------------------------------------------

        # What this server answers, and what it deliberately does not. The
        # previous version of this comment listed the whole MCP method surface
        # without marking which half was wired, so it read as a support matrix
        # when it was a spec crib sheet — three of the methods it named fall
        # through to CODE_METHOD_NOT_FOUND.
        #
        #   IMPLEMENTED
        #     tools/list                 tool definitions with schemas
        #     tools/call                 execute one tool
        #     resources/list             resource descriptors
        #     resources/read             resource contents (templated per user)
        #     prompts/list               always [] — this server exposes none
        #     initialize                 handshake revisions only
        #     server/discover            2026-07-28 stateless discovery
        #     notifications/initialized, ping
        #
        #   NOT IMPLEMENTED — method-not-found is the correct answer, not a gap:
        #     prompts/get                we advertise zero prompts, so there is
        #                                nothing any name could resolve to
        #     resources/subscribe        we do not declare the subscribe
        #     resources/templates/list   or listChanged capability in
        #                                `_CAPABILITIES`, so a spec-conforming
        #                                client never sends these

        if method == "tools/list":
            # Data-dependent exposure: get_genetic_data answers from the user's
            # uploaded genotype file, and most users never upload one. Listing
            # the tool anyway makes every external MCP client carry its schema
            # and lets a model call it just to learn "no data" — so when the
            # caller is identifiable and has no genetic rows, the tool is not
            # listed at all. Unidentifiable callers (bare /mcp before OAuth)
            # keep the full list: capability discovery must not require auth.
            hidden = await self._data_gated_tools(
                user_id or await self._resolve_secret_user(request.path_params.get("secret", ""))
            )
            if hidden:
                base_tools = [t for t in self._tool_descriptions if t.get("name") not in hidden]
            else:
                base_tools = self._tool_descriptions

            if agent_name and base_tools:
                # Filter tools based on agent configuration

                # Get agent options from config
                config = global_config()
                suffix = agent_name.strip().upper()

                allowed_tools   = set(config.get_list(f"ALLOWED_TOOLS_{suffix}", []))
                disallowed_tools= set(config.get_list(f"DISALLOWED_TOOLS_{suffix}", []))

                # Apply filtering
                if allowed_tools:
                    # Whitelist mode: only include allowed tools
                    tools = [tool for tool in base_tools if tool.get("name") in allowed_tools]
                else:
                    tools = []

                # Apply blacklist (higher priority, can override whitelist)
                if disallowed_tools:
                    tools = [tool for tool in (tools if tools else base_tools) if tool.get("name") not in disallowed_tools]

                return jsonrpc_result(
                    id      = id,
                    protocol_version = negotiated,
                    result  = {
                        "tools": _by_name(tools)
                    },
                    cache_hint = _LIST_CACHE_HINT,
                    method  = method,
                    request = request
                )

            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                result  = {
                    "tools": _by_name(base_tools)
                },
                cache_hint = _LIST_CACHE_HINT,
                method  = method,
                request = request
            )

        elif method == "prompts/list":
            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                result  = {
                    "prompts": []
                },
                cache_hint = _LIST_CACHE_HINT,
                method  = method,
                request = request
            )

        elif method == "resources/list":
            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                result  = {
                    "resources": _by_name(self._resources, key="uri")
                },
                cache_hint = _LIST_CACHE_HINT,
                method  = method,
                request = request
            )

        #-------------------------------------------------

        elif method == "tools/call":

            if "params" not in jsonrpc or not isinstance(jsonrpc["params"], dict):
                return jsonrpc_error(
                    id      = id,
                    code    = CODE_INVALID_PARAMS,
                    msg     = "Empty parameter",
                    method  = "tools/call",
                    request = request
                )
            params = jsonrpc["params"]

            if "name" not in params or not isinstance(params["name"], str) or len(params["name"]) == 0:
                return jsonrpc_error(
                    id      = id,
                    code    = CODE_INVALID_PARAMS,
                    msg     = "Empty parameter name",
                    method  = "tools/call",
                    request = request
                )

            if not self._callable or \
                not isinstance(self._callable, dict) or \
                params["name"] not in self._callable or \
                not self._callable[params["name"]]:
                return jsonrpc_error(
                    id      = id,
                    code    = CODE_INVALID_PARAMS,
                    msg     = "Unsupported parameter name",
                    method  = "tools/call",
                    request = request
                )

            #---------------------------------------------

            tool = self._callable[params["name"]]
            jwt_token = get_jwt_token(request)

            if tool["auth"] and not user_id and jwt_token and self._token_validator:
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
                user_secret = request.path_params.get("secret", "")
                if user_secret:
                    if self._redis:
                        # Check permanent urls.
                        try:
                            user_id = await self._redis.get(self._mcp_url_keyprefix + user_secret)
                        except Exception as e:
                            # Same as above: Redis being down degrades to
                            # "unauthenticated" rather than an error, so without
                            # this line an outage looks like a permissions bug.
                            logging.warning("MCP: permanent URL lookup failed: %s", e)
                            user_id = ""
                    else:
                        user_id = self._mcp_urls.get(user_secret, "")

            if tool["auth"] and not user_id:
                state           = secrets.token_urlsafe(32)
                check_interval  = 10    # Seconds.
                timeout         = 300   # Seconds.

                oauth_params = {
                    "response_type" : "code",
                    "client_id"     : "theta_mcp",
                    "redirect_uri"  : "urn:ietf:wg:oauth:2.0:oob",
                    "scope"         : "read write",
                    "state"         : state,
                }

                authorization_url = f"""{url_prefix}/mcplogin?oauth_params={urllib.parse.quote(urllib.parse.urlencode(oauth_params))}"""

                return jsonrpc_result(
                    id      = id,
                    protocol_version = negotiated,
                    result  = {
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
                    method  = "tools/call",
                    request = request
                )

            #---------------------------------------------

            result = await call_tool(
                tools       = self._callable,
                tool_name   = params["name"],
                arguments   = params["arguments"] if "arguments" in params else {},
                user_id     = user_id,
                session_id  = session_id
            )

            is_error = False
            data = result
            if isinstance(result, dict):
                if "redirect_to_upload" in result:
                    return jsonrpc_result(
                        id      = id,
                        protocol_version = negotiated,
                        result  = {
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
                        method  = params["name"],
                        request = request
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
                id      = id,
                protocol_version = negotiated,
                result  = result,
                method  = params["name"],
                request = request
            )

        #-------------------------------------------------

        elif method == "resources/read":
            if "params" not in jsonrpc or not isinstance(jsonrpc["params"], dict):
                return jsonrpc_error(
                    id      = id,
                    code    = CODE_INVALID_PARAMS,
                    msg     = "Empty parameter",
                    method  = "resources/read",
                    request = request
                )
            params = jsonrpc["params"]

            if "uri" not in params or not isinstance(params["uri"], str) or len(params["uri"]) == 0:
                return jsonrpc_error(
                    id      = id,
                    code    = CODE_INVALID_PARAMS,
                    msg     = "Empty parameter uri",
                    method  = "resources/read",
                    request = request
                )

            uri = params["uri"]
            if uri not in self._resource_map:
                return jsonrpc_result(
                    id      = id,
                    protocol_version = negotiated,
                    result  = {
                        "contents": [],
                        "_meta": {
                            "error": f"Unknown resource: {uri}"
                        }
                    },
                    method  = method,
                    request = request
                )

            # Copy before templating. `self._resource_map[uri]` is the SHARED,
            # process-wide cache loaded once at startup; the placeholders below
            # — including {{JWT_TOKEN}} — are per-REQUEST values. Templating the
            # cached dict in place permanently baked the first caller's JWT into
            # the widget: every later request found no {{JWT_TOKEN}} left to
            # substitute and was served the first user's token instead. On an
            # OAuth-protected server carrying personal health data that is a
            # cross-user credential leak, and it survived until restart.
            resource = dict(self._resource_map[uri])
            if "text" in resource:
                current_server = request_origin(request)
                jwt_token = get_jwt_token(request) or ""
                resource["text"] = resource["text"] \
                    .replace("{{WEB_SERVER_URL}}", current_server) \
                    .replace("{{MCP_SERVER_URL}}", current_server) \
                    .replace("{{DATA_SERVER_URL}}", current_server) \
                    .replace("{{JWT_TOKEN}}", jwt_token)

            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                result  = {
                    "contents": [
                        resource,
                    ]
                },
                method  = method,
                request = request
            )

        #-------------------------------------------------

        elif method == "initialize":
            # Negotiate: honour the client's requested revision when we speak it.
            # 2026-07-28 clients never send this at all — they carry the version
            # per request in `_meta` — so this branch exists purely for the
            # handshake-based revisions, which are supported for a year-long
            # offramp.
            requested = None
            if isinstance(jsonrpc.get("params"), dict):
                requested = jsonrpc["params"].get("protocolVersion")

            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                server_info = self._server_info,
                result  = {
                    "protocolVersion": self._negotiate_version(requested),
                    "capabilities": _CAPABILITIES,
                    "serverInfo": {
                        "name": self._name,
                        "version": self._version
                    }
                },
                method  = method,
                request = request
            )

        elif method == "server/discover":
            # 2026-07-28's optional, stateless replacement for `initialize`:
            # a client MAY ask what the server supports, but is not required to
            # handshake before calling anything. Advertising the full supported
            # list (rather than a single version) is what lets a client pick.
            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                server_info = self._server_info,
                result  = {
                    "protocolVersion": self._protocol_version,
                    "supportedProtocolVersions": list(_SUPPORTED_PROTOCOL_VERSIONS),
                    "capabilities": _CAPABILITIES,
                    "serverInfo": self._server_info,
                },
                cache_hint = _LIST_CACHE_HINT,
                method  = method,
                request = request
            )

        elif method == "notifications/initialized":
            return json_response(
                content     = "",
                status_code = 200,
                request     = request
            )

        elif method == "ping":
            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                result  = {},
                method  = method,
                request = request
            )

        #-------------------------------------------------

        else:
            return jsonrpc_error(
                id      = id,
                code    = CODE_METHOD_NOT_FOUND,
                msg     = "MCP method not found",
                request = request
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
        except Exception as e:
            # Optional body: a request without one is legitimate, so this is
            # debug, not a warning.
            logging.debug("MCP: no JSON body on personal-URL request: %s", e)
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
            user_secret = secrets.token_urlsafe(96)

            if self._redis:
                try:
                    # Set 1 year expiration (365 days)
                    await self._redis.set(self._mcp_url_keyprefix+user_secret, user_id, ex=365*24*60*60)
                    await self._redis.set(self._mcp_url_keyprefix+user_id, user_secret, ex=365*24*60*60)
                except Exception as e:
                    return json_response_with_code(-6, str(e), request=request)
            else:
                self._mcp_urls[user_secret] = user_id
                self._mcp_urls[user_id]     = user_secret

        #-------------------------------------------------

        url_prefix = request_origin(request)

        return json_response_with_code(data={"url": f"{url_prefix}/mcp/{user_secret}"}, request=request)

    #-----------------------------------------------------

    @classmethod
    async def generate_temporary_personal_mcp(cls, user_id: str, session_id: str = "", agent_name: str = "", expiration: int = 60*10) -> tuple[str | None, str | None]:
        if not user_id:
            return None, "Empty user ID."

        if not cls._global_instance:
            return None, "Invalid MCP service."

        service = cls._global_instance
        if not service._redis:
            return None, "Invalid redis connection."
        
        #-------------------------------------------------

        user_secret = secrets.token_urlsafe(32)

        payload = {
            "user_id"   : user_id,
            "session_id": session_id,
            "agent_name": agent_name
        }

        try:
            await service._redis.set(
                name    = service._temporary_mcp_url_keyprefix + user_secret,
                value   = json.dumps(payload, ensure_ascii=False, separators=(',', ':')),
                ex      = expiration
            )
        except Exception as e:
            return None, str(e)

        #-------------------------------------------------

        return f"{service._uri_prefix}/mcp/{user_secret}", None

#-----------------------------------------------------------------------------
