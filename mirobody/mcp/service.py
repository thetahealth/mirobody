import json
import logging

from datetime import datetime

from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route


from mirobody.utils.http import META_PROTOCOL_VERSION, request_origin

from mirobody.utils import get_jwt_token, json_response, json_response_with_code, jsonrpc_result, jsonrpc_error

from mirobody.user import AbstractTokenValidator
from mirobody.user import personal_mcp
from mirobody.user.auth.bearer import bearer_subject, mcp_resource

from .stdio import PROTOCOL_VERSIONS
from .tool import load_tools_from_directories, call_tool

logger = logging.getLogger(__name__)

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
# The protocol revisions this server implements are `stdio.PROTOCOL_VERSIONS`,
# newest first, the list the stdio server speaks: `initialize` negotiates
# against it and per-request `_meta` (2026-07-28's stateless model) is
# validated against it too.

# Declared once: `initialize` and `server/discover` MUST advertise the same
# capabilities, and they held separate copies of this dict that could drift.
# An empty value means "supported, with no optional sub-capabilities". No
# `resources` or `prompts` key: this server publishes tools, and a client that
# asks for resources/list or prompts/list gets method-not-found, which is the
# spec's word for "not offered". (`prompts` was declared, and answered with an
# empty list: a capability advertised to say there is nothing there.) (It used to serve two ChatGPT Apps SDK widgets from here;
# no tool ever pointed at them, and they are gone.)
_CAPABILITIES = {
    "tools": {
        "listChanged": False,
    },
}

# 2026-07-28 caching metadata, emitted on the methods the spec marks cacheable.
# The tool set is built once at startup and never changes while the process
# runs (no listChanged notifications), so a client may hold it for a few
# minutes. `private` because the list is filtered per caller: a shared cache
# must not serve one caller's copy to another.
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

    def __init__(
        self,

        token_validator         : AbstractTokenValidator | None = None,

        name                    : str = "",
        version                 : str = "",

        uri_prefix              : str = "",
        routes                  : list | None = None,

        tool_dirs               : list[str] | None = None,
    ):
        self._token_validator   = token_validator

        self._name              = name if name else "mirobody MCP Server"
        self._version           = version if version else "1.0.0"

        self._uri_prefix        = uri_prefix

        # How long a personal MCP link lives, from when it is made, never
        # extended by use. The link is a bearer credential carried IN a URL:
        # pasted into a desktop client's config, landing in screenshots and
        # shell history, and `/mcp/{secret}` needs nothing else to read that
        # person's record. `MCP_URL_TTL_DAYS` to change it.
        from mirobody.utils.config import safe_read_cfg

        default_days = personal_mcp.DEFAULT_TTL_DAYS
        try:
            self._mcp_url_ttl_days = max(1, int(safe_read_cfg("MCP_URL_TTL_DAYS") or default_days))
        except ValueError:
            logger.warning("MCP_URL_TTL_DAYS is not an integer; using %d days", default_days)
            self._mcp_url_ttl_days = default_days

        #----------------------------------------------

        self._callable, self._tool_descriptions = load_tools_from_directories(tool_dirs or [])

        #-------------------------------------------------

        if routes is not None:
            self.routes = routes
        else:
            self.routes = []

        self.routes.append(Route(f"{uri_prefix}/mcp/{{secret:str}}", endpoint=self.mcp_handler, methods=["POST", "GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/mcp", endpoint=self.mcp_handler, methods=["POST", "GET", "OPTIONS"]))
        # RFC 9728 protected-resource metadata, at the path-suffixed location the
        # MCP authorization spec asks clients to try first, and at the root.
        self.routes.append(Route(f"/.well-known/oauth-protected-resource{uri_prefix}/mcp", endpoint=self.resource_metadata_handler, methods=["GET"]))
        self.routes.append(Route("/.well-known/oauth-protected-resource", endpoint=self.resource_metadata_handler, methods=["GET"]))

        # One resource: GET lists the caller's links (made, and reading their
        # record), POST makes one (replacing the same pair's), DELETE revokes
        # the caller's own; `/{id}` revokes one link by its creator or subject.
        self.routes.append(Route(f"{uri_prefix}/personal/mcp", endpoint=self.generate_personal_mcp, methods=["GET", "POST", "DELETE", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/personal/mcp/{{link_id:int}}", endpoint=self.revoke_personal_mcp_link, methods=["DELETE", "OPTIONS"]))

    #-----------------------------------------------------

    async def resource_metadata_handler(self, request: Request) -> Response:
        origin = request_origin(request)
        return json_response({
            "resource": mcp_resource(origin, self._uri_prefix),
            "authorization_servers": [origin],
            "scopes_supported": ["mcp:read", "mcp:write"],
            "bearer_methods_supported": ["header"],
            "resource_name": self._name,
        }, request=request)

    #-----------------------------------------------------

    @property
    def _server_info(self) -> dict:
        return {"name": self._name, "version": self._version}

    def _negotiate_version(self, requested: str | None) -> str:
        """Pick the revision to speak with this client.

        Return the client's request when we implement it, otherwise our newest.
        Every MCP revision requires this handshake; the old code returned the
        newest unconditionally, so a client pinned to 2024-11-05 was told the
        server was speaking a revision it had never agreed to.
        """
        if isinstance(requested, str) and requested in PROTOCOL_VERSIONS:
            return requested
        return PROTOCOL_VERSIONS[0]

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

    async def _caller(self, request: Request, secret_user: str) -> str:
        """Whose record this request reads, decided one way for every method:
        the personal link's subject on `/mcp/{secret}`, where a bearer token
        sent beside it is ignored; otherwise the verified bearer's account;
        otherwise "". `tools/call` used to prefer the bearer and `tools/list`
        to read the link only, so a link and a token answered for two people,
        and an OAuth client on bare `/mcp` was never gated."""
        if secret_user:
            return secret_user
        token = get_jwt_token(request)
        if not token or not self._token_validator:
            return ""
        payload, err = self._token_validator.verify_token(token)
        if err:
            logger.info("MCP bearer token refused")
            return ""
        resource = mcp_resource(request_origin(request), self._uri_prefix)
        return str(await bearer_subject(payload, mcp_resource=resource) or "")

    async def _resolve_secret_user(self, user_secret: str) -> str:
        """The subject a personal link reads, or "" when it may not be used
        now. Checked on every request (`personal_mcp.authorize`)."""
        return await personal_mcp.authorize(user_secret)

    # Tools whose only possible answer without the corresponding data is
    # "no data": each maps to the EXISTS probe that decides its visibility.
    #
    # The observation probe reads `v_observation`, the view the tool reads: a
    # probe on the raw table counted a retracted row (it stays, amended).
    _GENOTYPE_PROBE = "SELECT 1 FROM th_genotype_set WHERE user_id = :uid AND status = 'active' LIMIT 1"
    _DATA_GATED = {
        "query_genetic_data": _GENOTYPE_PROBE,
        "query_pharmacogenomics": _GENOTYPE_PROBE,
        "query_health_indicators":
            "SELECT 1 FROM v_observation WHERE user_id = :uid LIMIT 1",
        "query_medications":
            "SELECT 1 FROM th_medication_plan WHERE user_id = :uid AND deleted = 0 LIMIT 1",
    }

    async def _data_gated_tools(self, user_id: str) -> set[str]:
        """Tool names to HIDE from tools/list for this user.

        A data-reading tool for a user with none of that data can only ever
        answer "no data": listing it makes every client carry its schema for
        nothing. Fails OPEN per probe (nothing hidden): a DB hiccup must not
        shrink the tool surface of a user who does have data.
        """
        if not user_id:
            return set()

        from mirobody.utils import execute_query

        hidden: set[str] = set()
        for name, probe in self._DATA_GATED.items():
            if name not in self._callable:
                continue
            try:
                rows = await execute_query(probe, {"uid": str(user_id)}, log_sql=False)
                if not rows:
                    hidden.add(name)
            except Exception as e:
                logger.warning("MCP: data gate check failed: tool_name=%s error_type=%s",
                               name, type(e).__name__)
        return hidden

    def _unavailable_tools(self) -> set[str]:
        """Tools whose service says this deployment cannot serve them now."""
        return {name for name, tool in self._callable.items()
                if tool and callable(tool.get("available")) and not tool["available"]()}

    #-----------------------------------------------------

    def tool_counts(self) -> tuple[int, int]:
        """(tools, of which need an authenticated caller), for the health check."""
        return len(self._callable), sum(1 for t in self._callable.values() if t and t.get("auth"))

    async def mcp_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            # CORS headers are added by CORSMiddleware.
            return Response(content="200 ok", status_code=200)
        if request.method != "POST":
            # Streamable HTTP: a GET asks for a server-sent stream, which this
            # server does not offer, and 405 is the answer the spec names for
            # that. A GET used to fall through to body parsing and come back as
            # a JSON-RPC parse error.
            return Response(content="405 Method Not Allowed", status_code=405, headers={"Allow": "POST, OPTIONS"})

        #-------------------------------------------------

        body = await request.body()
        try:
            jsonrpc = json.loads(body)
        except Exception:
            # This used to `print()` the full URL, EVERY request header: including
            # `Authorization: Bearer …`, and the entire unbounded body to stdout.
            # Any unauthenticated caller could trigger it by posting invalid JSON,
            # making it both a bearer-token/PHI leak into the logs and a log-flood
            # DoS. Log the shape of the failure, never its credentials or content.
            logger.warning(
                "MCP: malformed JSON-RPC body",
                extra={
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

        # A body that parses but is not an object (null, 5, "text", a batch)
        # is refused before anything reads a key of it: `"id" in 5` raised a
        # TypeError, an unauthenticated 500.
        if not isinstance(jsonrpc, dict):
            return jsonrpc_error(
                id      = None,
                code    = CODE_INVALID_REQUEST,
                msg     = "Invalid request body",
                method  = "",
                request = request
            )

        # According to the MCP specification the ID field should always be
        # there, but a request that omits it is still well-formed JSON, and
        # every error branch below used to re-index `jsonrpc["id"]` directly.
        # An unauthenticated POST without "id" therefore raised KeyError from
        # inside the handler; with FastAPI debug enabled (which follows
        # LOG_LEVEL, DEBUG by default) that returns a full stack trace to the
        # caller. Extract once here, use `id` everywhere after.
        id = jsonrpc.get("id")

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

        # A link that no longer authorizes is refused on every method, not only
        # on tools/call: a client holding a revoked or expired link otherwise
        # initialized and listed tools happily and failed on its first call.
        secret = request.path_params.get("secret", "")
        secret_user = await self._resolve_secret_user(secret) if secret else ""
        if secret and not secret_user:
            refused = jsonrpc_error(
                id      = id,
                code    = CODE_AUTH_REQUIRED,
                msg     = "This personal MCP link is not valid. Make a new one in Settings.",
                method  = method,
                request = request
            )
            refused.status_code = 401
            refused.headers["WWW-Authenticate"] = 'Bearer error="invalid_token"'
            return refused

        # A notification (no "id") is answered 202 Accepted with no body, which
        # Streamable HTTP requires. `notifications/initialized` got 200 and a
        # JSON `""` body: Codex's client read that as a malformed message,
        # initialized three times over and never listed a tool. Any other
        # notification fell through to "method not found", an answer a
        # notification may not receive at all.
        if "id" not in jsonrpc:
            logger.info("mcp notification", extra={"mcp_method": method, "status": 202})
            return Response(status_code=202)

        # The revision THIS request is speaking. 2026-07-28 removed the
        # initialize/initialized handshake, so there is no session in which to
        # remember a negotiated version: the client restates it in `_meta` on
        # every call, and the server has to settle it per request. Handshake-era
        # clients send no `_meta`; they fall through to our newest supported
        # revision, exactly as before.
        # A handshake client restates its version in the `MCP-Protocol-Version`
        # header (Streamable HTTP, since 2025-06-18) instead.
        negotiated = self._negotiate_version(
            self._request_protocol_version(jsonrpc) or request.headers.get("mcp-protocol-version")
        )

        url_prefix = request_origin(request)

        #-------------------------------------------------

        #   IMPLEMENTED: tools/list, tools/call, initialize (handshake
        #     revisions only), server/discover (2026-07-28 stateless
        #     discovery), ping; every notification is accepted above (202)
        #   NOT IMPLEMENTED, where method-not-found is the answer and not a gap:
        #     prompts/*, resources/*   `_CAPABILITIES` declares neither, so a
        #                              conforming client never sends these

        if method == "tools/list":
            # Data-dependent exposure: a data-reading tool for a user with none
            # of that data can only answer "no data" (most users never upload a
            # genotype file). Listing it anyway makes every external MCP client
            # carry its schema and lets a model call it just to learn that, so
            # when the caller is identifiable and has no such rows, the tool is
            # not listed at all (`_DATA_GATED`). Unidentifiable callers (bare
            # /mcp before OAuth) keep the full list: capability discovery must
            # not require auth.
            hidden = await self._data_gated_tools(await self._caller(request, secret_user))
            hidden |= self._unavailable_tools()
            if hidden:
                base_tools = [t for t in self._tool_descriptions if t.get("name") not in hidden]
            else:
                base_tools = self._tool_descriptions

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


        if method == "tools/call":

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

            arguments = params.get("arguments")
            if arguments is not None and not isinstance(arguments, dict):
                return jsonrpc_error(
                    id      = id,
                    code    = CODE_INVALID_PARAMS,
                    msg     = "Tool arguments must be an object",
                    method  = "tools/call",
                    request = request
                )

            tool = self._callable[params["name"]]
            user_id = await self._caller(request, secret_user) if tool["auth"] else ""

            if tool["auth"] and not user_id:
                # MCP authorization: 401, and where to find the authorization
                # server (RFC 9728). The client runs OAuth with PKCE and retries.
                refused = jsonrpc_error(
                    id      = id,
                    code    = CODE_AUTH_REQUIRED,
                    msg     = "Authentication required",
                    method  = "tools/call",
                    request = request
                )
                refused.status_code = 401
                challenge = f'Bearer resource_metadata="{url_prefix}/.well-known/oauth-protected-resource{self._uri_prefix}/mcp"'
                if get_jwt_token(request):
                    challenge += ', error="invalid_token"'
                refused.headers["WWW-Authenticate"] = challenge
                return refused

            #---------------------------------------------

            result = await call_tool(
                tools       = self._callable,
                tool_name   = params["name"],
                arguments   = arguments or {},
                user_id     = user_id,
            )

            # A tool answer used to be able to hijack this reply: any result
            # carrying `redirect_to_upload` was replaced by an "open /drive and
            # upload" message. Its only producer was the genetics tool's no-rows
            # branch, and `_DATA_GATED` already hides that tool from a user with
            # no genetic rows, so the redirect could only fire for a user who
            # HAS uploaded a genotype file and asked about rsIDs it does not
            # carry, where "upload your data first" is the wrong answer. The
            # tool now says "not typed" in its envelope and this branch is gone.
            is_error = False
            data = result
            if isinstance(result, dict):
                if "success" in result and isinstance(result["success"], bool):
                    is_error = not result["success"]
                # The record tools answer with an envelope's `status`, never
                # `success`, so a refused, denied or failed read reached the
                # client as `isError: false` with "error (...)" as its text.
                if result.get("status") == "error":
                    is_error = True

                if is_error and "error" in result:
                    data = result["error"]

                elif not is_error and "data" in result:
                    data = result["data"]

            # A tool that answers in prose (the medical-knowledge passages) is
            # sent as that text, not as a JSON string of it.
            text = data if isinstance(data, str) else json.dumps(
                data, ensure_ascii=False, separators=(',', ':'), cls=ResponseEncoder)
            result={
                "content": [
                    {
                        "type": "text",
                        "text": text
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

        if method == "initialize":
            # Negotiate: honour the client's requested revision when we speak it.
            # 2026-07-28 clients never send this at all (they carry the version
            # per request in `_meta`) so this branch exists purely for the
            # handshake-based revisions, which are supported for a year-long
            # offramp.
            requested = None
            if isinstance(jsonrpc.get("params"), dict):
                requested = jsonrpc["params"].get("protocolVersion")
            # One version in one answer: `_meta` said 2026-07-28 beside a
            # handshake that settled on 2025-06-18.
            handshake = self._negotiate_version(requested)

            return jsonrpc_result(
                id      = id,
                protocol_version = handshake,
                server_info = self._server_info,
                result  = {
                    "protocolVersion": handshake,
                    "capabilities": _CAPABILITIES,
                    "serverInfo": {
                        "name": self._name,
                        "version": self._version
                    }
                },
                method  = method,
                request = request
            )

        if method == "server/discover":
            # 2026-07-28's optional, stateless replacement for `initialize`:
            # a client MAY ask what the server supports, but is not required to
            # handshake before calling anything. Advertising the full supported
            # list (rather than a single version) is what lets a client pick.
            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                server_info = self._server_info,
                # `supportedVersions` is the field the SDK's `DiscoverResult`
                # requires; this answered `supportedProtocolVersions`, which the
                # SDK's own client rejected. The identity rides in `_meta`.
                result  = {
                    "supportedVersions": list(PROTOCOL_VERSIONS),
                    "capabilities": _CAPABILITIES,
                },
                cache_hint = _LIST_CACHE_HINT,
                method  = method,
                request = request
            )

        if method == "ping":
            return jsonrpc_result(
                id      = id,
                protocol_version = negotiated,
                result  = {},
                method  = method,
                request = request
            )

        #-------------------------------------------------

        return jsonrpc_error(
            id      = id,
            code    = CODE_METHOD_NOT_FOUND,
            msg     = "MCP method not found",
            request = request
        )

    #-----------------------------------------------------

    async def _personal_mcp_subject(self, request: Request) -> tuple[str, str, Response | None]:
        """`(caller, subject, refusal)`: who is asking, and whose record the
        personal link they are making or revoking reads.

        Shared by mint and revoke so the two cannot drift on authorization:
        the shape of bug that let a caller act on someone else's record once
        already (`/ws/upload-health-report`, 2026-08-23).
        """
        if not self._token_validator:
            return "", "", json_response_with_code(-1, "No JWT token validator.", request=request)

        # 401, not 200-with-an-error-body: this route is consumed by MCP
        # clients, and a client that reads the status code (most do) took
        # "no token" for success. `/api/chat` has always answered 401 here, so
        # the two halves of the same API disagreed. The envelope is unchanged.
        payload, err = self._token_validator.verify_token(get_jwt_token(request))
        if err:
            return "", "", json_response_with_code(-2, err, request=request, status=401)
        # The account the token speaks for, live and not revoked since it was
        # minted: no second read of `sub`, nor a weaker liveness check after it.
        caller = await bearer_subject(payload)
        if not caller:
            return "", "", json_response_with_code(-3, "Not a valid session.", request=request, status=401)
        user_id = str(caller)

        # An optional body: a request without one is legitimate, and a JSON
        # client may send the member's id as a number or a null.
        try:
            data = await request.json()
        except ValueError:
            data = None
        wanted = data.get("user_id") if isinstance(data, dict) else None
        beneficiary_user_id = "" if wanted in (None, "") else str(wanted)

        if beneficiary_user_id and beneficiary_user_id != user_id:
            # The personal MCP URL can be minted for someone else's record only
            # if the care circle says so. `check_relationship` used to answer
            # this by parsing a permissions bag out of `th_share_relationship`
            # and returning an error STRING: with two bugs in the parse
            # (`isinstance(obj)` one-arg, and an unbound `e` in the handler)
            # that made the success path raise.
            from mirobody.user.care_circle import CareCircleDenied, resolve_subject
            try:
                await resolve_subject(user_id, beneficiary_user_id)
            except CareCircleDenied as denied:
                # 403: authenticated, and not allowed. Deliberately not 404:
                # the care circle already refuses to say whether the subject
                # exists (see user/test_care_circle.py), and the body carries
                # that same non-committal message.
                return "", "", json_response_with_code(-5, str(denied), request=request, status=403)

            return user_id, beneficiary_user_id, None

        return user_id, user_id, None

    async def generate_personal_mcp(self, request: Request) -> Response:
        """`/personal/mcp`, for the caller and (with `user_id` in the body)
        a family member whose record the care circle lets them read.

        GET lists the caller's live links: the ones they made and the ones
        reading their record. POST makes a link and returns its URL, the only
        time the secret is shown; a live link of the same caller and subject is
        revoked by it, so this is also "regenerate". DELETE revokes the link the
        caller made for that subject, and only that one: a family member's
        DELETE used to take away the person's own link.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        caller, subject, refusal = await self._personal_mcp_subject(request)
        if refusal is not None:
            return refusal

        try:
            if request.method == "GET":
                return json_response_with_code(data=await personal_mcp.listed(caller), request=request)
            if request.method == "DELETE":
                count = await personal_mcp.revoke_mine(caller, subject)
                return json_response_with_code(data={"revoked": count > 0}, request=request)
            secret, link = await personal_mcp.mint(caller, subject, ttl_days=self._mcp_url_ttl_days)
        except Exception as e:
            logger.warning("MCP personal link %s failed: error_type=%s", request.method, type(e).__name__)
            return json_response_with_code(-6, "Could not change the personal MCP link.", request=request)

        return json_response_with_code(
            data={**link, "url": f"{request_origin(request)}/mcp/{secret}"}, request=request)

    async def revoke_personal_mcp_link(self, request: Request) -> Response:
        """`DELETE /personal/mcp/{id}`: revoke one link, by the person who made
        it or the person whose record it reads. Anyone else gets the same 404
        as a link that does not exist."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        caller, _, refusal = await self._personal_mcp_subject(request)
        if refusal is not None:
            return refusal
        try:
            revoked = await personal_mcp.revoke(int(request.path_params["link_id"]), caller)
        except Exception as e:
            logger.warning("MCP personal link revoke failed: error_type=%s", type(e).__name__)
            return json_response_with_code(-6, "Could not revoke the personal MCP link.", request=request)
        if not revoked:
            return json_response_with_code(-7, "No such link.", request=request, status=404)
        return json_response_with_code(data={"revoked": True}, request=request)

    #-----------------------------------------------------

#-----------------------------------------------------------------------------
