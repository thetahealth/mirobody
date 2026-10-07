import hashlib
import json
import logging
import re
import time
from typing import Any

from starlette.responses import Response
from starlette.requests import Request

logger = logging.getLogger(__name__)

# MCP 2026-07-28 `_meta` keys, defined next to the code that writes them.
# `mcp/service.py` used to keep its own copies while this module hardcoded the
# serverInfo literal, so the two could drift. `clientInfo` was declared here
# too and never written or read by anything: removed rather than left as a
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
      `http://localhost`: port 80, nothing listening. This is the broken local
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


_SCHEME_ENTRY = re.compile(r"^([a-z][a-z0-9+.-]*):(?://)?$")


def safe_return_url(url: str, own_origin: str, also_allowed=()) -> str | None:
    """`url` if a redirect there stays with this deployment, else None.

    A vendor's OAuth callback is public by nature and took `return_url` from
    its query string, so `state=success&return_url=https://anywhere` answered
    302 to anywhere (reproduced 2026-10-01). Kept: a path on this origin, this
    origin, and what `also_allowed` names, either an origin
    (`https://app.example.com`) or a scheme for an app's own links (`theta:`).
    """
    if not isinstance(url, str) or not url or url != url.strip():
        return None
    # A backslash or a control character is read differently by different
    # parsers: browsers treat `/\evil.example` as `//evil.example`.
    if "\\" in url or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in url):
        return None
    if url.startswith("/"):
        return None if url.startswith("//") else url
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    origins, schemes = set(), set()
    for entry in also_allowed or ():
        entry = str(entry).strip().lower().rstrip("/")
        m = _SCHEME_ENTRY.match(entry + (":" if entry and ":" not in entry else ""))
        if entry.startswith(("http://", "https://")):
            origins.add(entry)
        elif m:
            schemes.add(m.group(1))
    if scheme in ("http", "https"):
        if not parts.netloc or "@" in parts.netloc:
            return None
        origin = f"{scheme}://{parts.netloc.lower()}"
        return url if origin in origins | {own_origin.lower().rstrip("/")} else None
    return url if scheme and scheme in schemes else None

#-----------------------------------------------------------------------------

def get_jwt_token(request: Request) -> str:
    return request.headers.get("Authorization")

#-----------------------------------------------------------------------------

#: Paths whose next segment IS the credential: a personal MCP link opens one
#: person's record, a share id opens a chat. Every request line logged them
#: whole, so anyone reading the log could use them.
_CAPABILITY_SEGMENT = re.compile(r"(/mcp/|/api/share/)([A-Za-z0-9_-]{16,})")


def loggable_path(path: str) -> str:
    """`path` with any capability segment replaced by a short digest of it.

    The digest keeps requests from one link correlatable in the log; it is not
    the link, and 32 bits of its SHA-256 cannot be turned back into it. Short
    literal routes (`/api/share/deactivate`) are left as they are."""
    return _CAPABILITY_SEGMENT.sub(
        lambda m: f"{m.group(1)}~{hashlib.sha256(m.group(2).encode()).hexdigest()[:8]}", path
    )


def _log_extra(request: Request | None, **fields: object) -> dict[str, object]:
    """`fields` and how long the request has run: ids, counts and status codes,
    the keys `PHIFilter` keeps. The path, the client's address and its
    `X-Platform`/`X-Ver` headers are not among them; the filter would delete
    them from every line."""
    extra = dict(fields)
    start = getattr(request.state, "start_time", None) if request is not None else None
    if start is not None:
        extra["duration_ms"] = round((time.time() - start) * 1e3, 2)
    return extra

#-----------------------------------------------------------------------------

def json_response(content: Any, status_code: int = 200, request: Request | None = None,
                  disable_log: bool = False) -> Response:
    if not disable_log:
        extra = _log_extra(request, status=status_code)

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

def json_response_with_code(code: int = 0, msg: str = "ok", data: Any = None, request: Request | None = None,
                           disable_log: bool = False, status: int = 200) -> Response:
    """The `{code, msg, data}` envelope: the same one `server/envelope.py` has.

    There used to be two. This one also carried `success`, which said exactly
    what `code == 0` says, and it OMITTED `data` entirely when there was none,
    so a client reading `data.x` had to null-check this shape and not the
    other. Both are gone: `success` because it was redundant, the omission
    because `data` is now always an object. The web client's one response
    handler already read `code === 0 || success`, so it needs no change.

    `status` exists for the routes where the HTTP status is part of the
    contract: an MCP client keys on 401, not on a body it may not parse.
    It defaults to 200 because every other caller (and the web client)
    reads `code`, and changing that for all of them is a coordinated
    release, not a bug fix. (2026-09-14 regression report, F-7)
    """
    if not disable_log:
        extra = _log_extra(request, status=status)
        if code != 0:
            extra["error_code"] = code
            logger.warning(msg, stacklevel=2, extra=extra)
        else:
            logger.info(msg, stacklevel=2, extra=extra)

    content = {
        "code"      : code,
        "msg"       : msg,
        "data"      : {} if data is None else data
    }
    
    return Response(
        content     = json.dumps(
            content,
            ensure_ascii= False,
            separators  = (',', ':')
        ),
        status_code = status,
        media_type  = "application/json; charset=utf-8"
    )

#-----------------------------------------------------------------------------

def redirect(url: str, status_code: int = 302, request: Request | None = None, disable_log: bool = False) -> Response:
    if not disable_log:
        logger.info("", stacklevel=2, extra=_log_extra(request, status=status_code))

    return Response(
        content     = "",
        status_code = status_code,
        headers     = {
            "Location": url
        }
    )

#-----------------------------------------------------------------------------

def _result_shape(result: Any) -> dict[str, object]:
    """What a JSON-RPC result LOOKS like: sizes, kinds and counts.

    Everything here is a number or a type name, which is the whole of what a
    log line may carry about a payload. `size_bytes` answers "did it come
    back empty"; `content_types` answers "was it text or a resource"; neither
    answers "what did it say", and that is deliberate.
    """
    try:
        size = len(json.dumps(result, ensure_ascii=False, separators=(',', ':'), default=str))
    except (TypeError, ValueError):
        size = -1
    shape: dict[str, object] = {"size_bytes": size, "result_type": type(result).__name__}
    if isinstance(result, dict):
        content = result.get("content")
        if isinstance(content, list):
            shape["count"] = len(content)
            shape["content_types"] = ",".join(
                sorted({str(c.get("type")) for c in content if isinstance(c, dict)})
            )
        for key in ("tools", "resources", "prompts", "resourceTemplates"):
            if isinstance(result.get(key), list):
                shape["count"] = len(result[key])
    return shape


def jsonrpc_result(
    id: Any,
    result: Any = None,
    method: str = "",
    request: Request | None = None,
    disable_log: bool = False,
    result_type: str = "complete",
    server_info: dict | None = None,
    cache_hint: tuple[int, str] | None = None,
    protocol_version: str | None = None,
) -> Response:
    """Build a JSON-RPC 2.0 result response.

    ``result_type`` / ``server_info`` carry the MCP 2026-07-28 additions:

    * ``resultType`` is REQUIRED on every result in that revision (it is what
      makes polymorphic results like ``input_required`` possible), and sent
      only there: the TypeScript SDK 1.30.1 (2025-11-25) parses an empty
      result strictly and rejected `ping` over the unknown key.
    * ``io.modelcontextprotocol/serverInfo`` in ``_meta`` is a SHOULD, meant for
      display and debugging only; the spec is explicit that neither side may
      make security or behaviour decisions from it.

    ``cache_hint`` is ``(ttl_ms, scope)`` and emits 2026-07-28's ``ttlMs`` /
    ``cacheScope``: field names taken from the SDK's own `ListToolsResult`, not
    guessed. The spec marks ``tools/list``, ``prompts/list`` and
    ``server/discover`` (among others) as cacheable; we pass it on the ones we
    implement. Nothing this server returns is templated per caller, so there is
    no response a cache could leak from one caller to another.

    ``protocol_version`` echoes the revision this response is speaking. Under
    2026-07-28 there is no handshake, so a stateless client has no other way to
    learn what the server settled on: `initialize` is exactly the call it never
    makes. Echoing per response is therefore not redundant with the
    `initialize` result; it is the only channel that survives the handshake's
    removal.

    All four are injected only when ``result`` is a dict that does not already
    carry them, so a caller can always override.
    """
    if not disable_log:
        extra = _log_extra(request, mcp_method=method, request_id=id)

        # The SHAPE of the result, never the result. This used to log its first
        # hundred serialised characters, and for `tools/call` those are the
        # person's readings: a health-data leak into the log on the one path
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
        stateless = protocol_version is None or protocol_version >= "2026-07-28"
        if result_type and stateless and "resultType" not in result:
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

def jsonrpc_error(id: Any, code: int, msg: str = "", data: Any = None, method: str = "",
                  request: Request | None = None, disable_log: bool = False) -> Response:
    if not disable_log:
        extra = _log_extra(request, mcp_method=method, request_id=id, error_code=code)
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
