import json, logging, time

from starlette.responses import Response
from starlette.requests import Request

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

def jsonrpc_result(id: any, result: any = None, method: str = "", request: Request = None, disable_log: bool = False) -> Response:
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
