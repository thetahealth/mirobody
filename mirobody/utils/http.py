import json
import logging
import time

from starlette.responses import Response
from starlette.requests import Request

logger = logging.getLogger(__name__)

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
            logger.warning(message, stacklevel=2, extra=extra)
        else:
            logger.info(message, stacklevel=2, extra=extra)

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
            logger.warning(msg, stacklevel=2, extra=extra)
        else:
            logger.info(msg, stacklevel=2, extra=extra)

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

        logger.info("", stacklevel=2, extra=extra)

    return Response(
        content     = "",
        status_code = status_code,
        headers     = {
            "Location": url
        }
    )

#-----------------------------------------------------------------------------

def _result_shape(result: any) -> dict:
    """What a JSON-RPC result LOOKS like: sizes, kinds and counts.

    Everything here is a number or a type name, which is the whole of what a
    log line may carry about a payload. `result_bytes` answers "did it come
    back empty"; `content_types` answers "was it text or a resource"; neither
    answers "what did it say", and that is deliberate.
    """
    try:
        size = len(json.dumps(result, ensure_ascii=False, separators=(',', ':'), default=str))
    except (TypeError, ValueError):
        size = -1
    shape: dict = {"result_bytes": size, "result_type": type(result).__name__}
    if isinstance(result, dict):
        content = result.get("content")
        if isinstance(content, list):
            shape["content_count"] = len(content)
            shape["content_types"] = ",".join(
                sorted({str(c.get("type")) for c in content if isinstance(c, dict)})
            )
        for key in ("tools", "resources", "prompts", "resourceTemplates"):
            if isinstance(result.get(key), list):
                shape[f"{key}_count"] = len(result[key])
    return shape


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
    guessed. The spec marks ``tools/list``, ``prompts/list`` and
    ``server/discover`` (among others) as cacheable; we pass it on the ones we
    implement. Nothing this server returns is templated per caller, so there is
    no response a cache could leak from one caller to another.

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

        # The SHAPE of the result, never the result. This used to log its first
        # hundred serialised characters, and for `tools/call` those are the
        # person's readings — a health-data leak into the log on the one path
        # the chat-side redaction did not cover, found by grepping a container
        # after a real turn. A hundred characters is not a redaction; it is a
        # smaller leak.
        extra.update(_result_shape(result))
        is_error = bool(isinstance(result, dict) and result.get("isError") is True)
        if is_error:
            logger.warning("mcp result", stacklevel=2, extra=extra)
        else:
            logger.info("mcp result", stacklevel=2, extra=extra)

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

        logger.warning(msg, stacklevel=2, extra=extra)

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
