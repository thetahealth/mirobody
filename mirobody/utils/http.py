import json, logging, time

from starlette.responses import Response
from starlette.requests import Request

# MCP 2026-07-28 `_meta` keys, defined next to the code that writes them.
# `mcp/service.py` used to keep its own copies while this module hardcoded the
# serverInfo literal, so the two could drift. `clientInfo` was declared here
# too and never written or read by anything — removed rather than left as a
# third name to keep in sync.
META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
META_SERVER_INFO      = "io.modelcontextprotocol/serverInfo"

#-----------------------------------------------------------------------------

def get_client_ip(request: Request) -> str:
    ip = request.headers.get("X-Forwarded-For", "")
    if ip:
        ip = ip.split(",")[0].strip()
    if ip:
        return ip
    
    if request.client:
        return request.client.host
    
    return ""

#-----------------------------------------------------------------------------

def request_origin(request: Request) -> str:
    """`scheme://host[:port]` for the request, as the client would type it.

    Five call sites built this by hand as::

        f"{'http' if request.url.hostname == 'localhost' else 'https'}://{request.url.hostname}"

    which is wrong three ways, and the results go into OAuth redirect URIs and
    the MCP endpoint URLs we hand to clients:

    * `hostname` DROPS THE PORT, so a local server on :18080 published
      `http://localhost` — port 80, nothing listening. This is the broken local
      login link.
    * anything not literally "localhost" was forced to https, so a server
      reached at `http://127.0.0.1:8000` advertised `https://127.0.0.1`.
    * the request's own scheme was ignored entirely.

    `netloc` carries host and port together and omits the port when it is the
    scheme default, so this is correct for `https://mirobody.ai` too.

    Behind a reverse proxy this reflects the forwarded scheme/host only if the
    ASGI server is run with proxy headers enabled; otherwise it reports the
    internal address, which is the standard caveat for any such helper.
    """
    return f"{request.url.scheme}://{request.url.netloc}"


def get_jwt_token(request: Request) -> str:
    return request.headers.get("Authorization")

#-----------------------------------------------------------------------------

def _fill_extra_log(request: Request = None, extra: dict[str, any] = None):
    if not request:
        return
    
    if not isinstance(extra, dict):
        return
    
    if request.url and request.url.path:
        extra["url"] = request.url.path

    platform = request.headers.get("X-Platform")
    if platform:
        extra["platform"] = platform

    version = request.headers.get("X-Ver")
    if version:
        extra["version"] = version

    ip = get_client_ip(request)
    if ip:
        extra["ip"] = ip

    if hasattr(request.state, "start_time"):
        extra["time_cost"] = round((time.time()-request.state.start_time)*1e3, 2)

#-----------------------------------------------------------------------------

def json_response(content: any, status_code: int = 200, request: Request = None, disable_log: bool = False) -> Response:
    if not disable_log:
        extra = {
            "status": status_code
        }
        _fill_extra_log(request=request, extra=extra)

        message = ""
        if content and isinstance(content, dict):
            if "message" in content and isinstance(content["message"], str) and len(content["message"]) > 0:
                message = content["message"]
            elif "msg" in content and isinstance(content["msg"], str) and len(content["msg"]) > 0:
                message = content["msg"]

        if status_code >= 400:
            logging.warning(message, stacklevel=2, extra=extra)
        else:
            logging.info(message, stacklevel=2, extra=extra)

    return Response(
        content     = json.dumps(
            content,
            ensure_ascii= False,
            separators  = (',', ':')
        ),
        status_code = status_code,
        media_type  = "application/json; charset=utf-8"
    )

def json_response_with_code(code: int = 0, msg: str = "ok", data: any = None, request: Request = None, disable_log: bool = False) -> Response:
    if not disable_log:
        extra = {
            "status": 200,
            "code"  : code
        }
        _fill_extra_log(request=request, extra=extra)

        if code != 0:
            logging.warning(msg, stacklevel=2, extra=extra)
        else:
            logging.info(msg, stacklevel=2, extra=extra)

    content = {
        "success"   : True if code == 0 else False,
        "code"      : code,
        "msg"       : msg
    }

    if data is not None:
        content["data"] = data
    
    return Response(
        content     = json.dumps(
            content,
            ensure_ascii= False,
            separators  = (',', ':')
        ),
        status_code = 200,
        media_type  = "application/json; charset=utf-8"
    )

#-----------------------------------------------------------------------------

def redirect(url: str, status_code: int = 302, request: Request = None, disable_log: bool = False) -> Response:
    if not disable_log:
        extra = {
            "status"    : status_code,
            "location"  : url
        }
        _fill_extra_log(request=request, extra=extra)

        logging.info("", stacklevel=2, extra=extra)

    return Response(
        content     = "",
        status_code = status_code,
        headers     = {
            "Location": url
        }
    )

#-----------------------------------------------------------------------------

def jsonrpc_result(
    id: any,
    result: any = None,
    method: str = "",
    request: Request = None,
    disable_log: bool = False,
    result_type: str = "complete",
    server_info: dict | None = None,
    cache_hint: tuple[int, str] | None = None,
    protocol_version: str | None = None,
) -> Response:
    """Build a JSON-RPC 2.0 result response.

    ``result_type`` / ``server_info`` carry the MCP 2026-07-28 additions:

    * ``resultType`` is REQUIRED on every result in that revision (it is what
      makes polymorphic results like ``input_required`` possible). Clients on
      earlier revisions ignore the unknown key, and the spec tells new clients
      to read an ABSENT ``resultType`` as ``"complete"`` — so emitting it is
      backward compatible in both directions.
    * ``io.modelcontextprotocol/serverInfo`` in ``_meta`` is a SHOULD, meant for
      display and debugging only; the spec is explicit that neither side may
      make security or behaviour decisions from it.

    ``cache_hint`` is ``(ttl_ms, scope)`` and emits 2026-07-28's ``ttlMs`` /
    ``cacheScope`` — field names taken from the SDK's own `ListToolsResult`, not
    guessed. The spec marks ``tools/list``, ``prompts/list``,
    ``resources/list``, ``resources/templates/list``, ``resources/read`` and
    ``server/discover`` as cacheable; we pass it on all of those we implement
    EXCEPT ``resources/read``, deliberately. Our resource bodies are templated
    per request with the caller's JWT (see the copy-before-templating note in
    `mcp/service.py`), so a cached read is a cached credential — it would
    outlive a logout by up to the TTL. Being spec-permitted is not the same as
    being safe for this server's payloads.

    ``protocol_version`` echoes the revision this response is speaking. Under
    2026-07-28 there is no handshake, so a stateless client has no other way to
    learn what the server settled on — `initialize` is exactly the call it never
    makes. Echoing per response is therefore not redundant with the
    `initialize` result; it is the only channel that survives the handshake's
    removal.

    All four are injected only when ``result`` is a dict that does not already
    carry them, so a caller can always override.
    """
    if not disable_log:
        extra = {
            "mcp_method": method,
            "mcp_id"    : id
        }
        _fill_extra_log(request=request, extra=extra)

        log_message = json.dumps(
            result,
            ensure_ascii= False,
            separators  = (',', ':')
        )
        if len(log_message) > 100:
            log_message = log_message[0:100] + "..."

        if result and isinstance(result, dict) and "isError" in result and \
            isinstance(result["isError"], bool) and result["isError"]:
            
            logging.warning(log_message, stacklevel=2, extra=extra)
        else:
            logging.info(log_message, stacklevel=2, extra=extra)

    #-----------------------------------------------------

    if isinstance(result, dict):
        if result_type and "resultType" not in result:
            result["resultType"] = result_type
        if server_info or protocol_version:
            meta = result.setdefault("_meta", {})
            if isinstance(meta, dict):
                if server_info:
                    meta.setdefault(META_SERVER_INFO, server_info)
                if protocol_version:
                    meta.setdefault(META_PROTOCOL_VERSION, protocol_version)
        if cache_hint:
            ttl_ms, scope = cache_hint
            result.setdefault("ttlMs", ttl_ms)
            result.setdefault("cacheScope", scope)

    content = {
        "jsonrpc"   : "2.0",
        "result"    : result
    }

    if id is not None:
        content["id"] = id

    return Response(
        content     = json.dumps(
            content,
            ensure_ascii= False,
            separators  = (',', ':')
        ),
        status_code = 200,
        media_type  = "application/json; charset=utf-8"
    )

#-----------------------------------------------------------------------------

def jsonrpc_error(id: any, code: int, msg: str = "", data: any = None, method: str = "", request: Request = None, disable_log: bool = False) -> Response:
    if not disable_log:
        extra = {
            "mcp_method": method,
            "mcp_id"    : id,
            "mcp_code"  : code
        }
        _fill_extra_log(request=request, extra=extra)

        logging.warning(msg, stacklevel=2, extra=extra)

    content = {
        "jsonrpc"   : "2.0",
        "id"        : id,
        "error"     : {
            "code"      : code,
            "message"   : msg,
        }
    }

    if id is not None:
        content["id"] = id

    if data is not None:
        content["error"]["data"] = data

    return Response(
        content     = json.dumps(
            content,
            ensure_ascii= False,
            separators  = (',', ':')
        ),
        status_code = 200,
        media_type  = "application/json; charset=utf-8"
    )

#-----------------------------------------------------------------------------
