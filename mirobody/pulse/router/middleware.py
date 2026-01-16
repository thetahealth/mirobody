import logging, pytz, time

from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Response

from .. import setup_platform_system_async
from ...pulse.theta.platform.startup import start_theta_pull_scheduler
from ...utils.req_ctx import REQ_CTX
from ...utils import get_client_ip


def register_middleware(app):
    @app.middleware("http")
    async def before_request(request, call_next):
        if request.url.path in [
            "/api/health",
            "/api/v1/pulse/invitations/health"
        ]:
            return Response()

        #-------------------------------------------------

        start_time = time.time()

        ctx = {
            "start_time"    : round(start_time*1e3, 2),
            "trace_id"      : get_trace_id(request.headers) or str(int(start_time*1e3)),
            "ip"            : get_client_ip(request),
        }
        if request.method:
            ctx["method"] = request.method
        if request.url and request.url.path:
            ctx["url"] = request.url.path
        if request.headers:
            ctx["request_time"]   = get_request_time_info(request.headers),
            ctx["language"]       = get_language(request.headers),
            ctx["timezone"]       = get_timezone(request.headers),

        REQ_CTX.set(ctx)

        #-------------------------------------------------

        try:
            logging.info(f"🚀 REQUEST START", extra=ctx)

            response = await call_next(request)

            ctx["time_cost"] = round((time.time() - start_time)*1e3, 2)
            logging.info("✅ REQUEST END", extra=ctx)

            return response

        except Exception as e:
            ctx["time_cost"] = round((time.time() - start_time)*1e3, 2)
            logging.error(f"❌ REQUEST ERROR: {str(e)}", extra=ctx)

            raise


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


def get_trace_id(data):
    trace_id_name_list = ["traceid", "trace_id", "X-Request-Id", "x-request-id"]
    for name in trace_id_name_list:
        if name in data:
            return data[name]
    return False


def get_timezone(data):
    timezone_name_list = ["x-timezone", "X-Timezone", "timezone"]
    for name in timezone_name_list:
        if name in data:
            return data[name]
    return "America/Los_Angeles"


def get_timezone_without_default(data):
    timezone_name_list = ["x-timezone", "X-Timezone", "timezone"]
    for name in timezone_name_list:
        if name in data:
            return data[name]
    return None


def get_timestamp(data):
    timestamp_name_list = ["x-timestamp", "X-Timestamp"]
    for name in timestamp_name_list:
        if name in data:
            return str(data[name])
    return str(int(time.time() * 1000))


def get_request_time_info(data):
    timezone_name = get_timezone(data)
    timezone = pytz.timezone(timezone_name)

    timestamp = get_timestamp(data)
    return datetime.fromtimestamp(int(timestamp) // 1000, tz=timezone)


def get_language(data):
    custom_headers = ["x-language", "X-Language"]
    for name in custom_headers:
        if name in data and data[name]:
            return data[name]

    language_name_list = ["accept-language", "Accept-Language"]
    for name in language_name_list:
        if name in data and data[name]:
            accept_lang = data[name]
            languages = accept_lang.split(",")
            for lang in languages:
                lang_code = lang.split(";")[0].strip()
                if lang_code != "*" and lang_code:
                    return lang_code

    return "en"


def get_language_without_default(data):
    custom_headers = ["x-language", "X-Language"]
    for name in custom_headers:
        if name in data and data[name]:
            return data[name]

    language_name_list = ["accept-language", "Accept-Language"]
    for name in language_name_list:
        if name in data and data[name]:
            accept_lang = data[name]
            languages = accept_lang.split(",")
            for lang in languages:
                lang_code = lang.split(";")[0].strip()
                if lang_code != "*" and lang_code:
                    return lang_code

    return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.getLogger("uvicorn.access").handlers.clear()
    logging.getLogger("uvicorn").handlers.clear()

    await init()
    yield

    try:
        from mirobody.pulse.theta.platform.startup import stop_theta_pull_scheduler
        from ..core.aggregate_indicator.startup import stop_aggregate_indicator_scheduler

        await stop_theta_pull_scheduler()
        await stop_aggregate_indicator_scheduler()
    except Exception as e:
        logging.error(f"Failed to stop schedulers: {str(e)}")


async def init():
    logging.info("start init db...")
    await setup_platform_system_async()
    await start_theta_pull_scheduler()
    
    # Start aggregate indicator scheduler
    try:
        from ..core.aggregate_indicator.startup import start_aggregate_indicator_scheduler
        await start_aggregate_indicator_scheduler(False)
        logging.info("Aggregate indicator scheduler started")
    except Exception as e:
        logging.error(f"Failed to start aggregate indicator scheduler: {str(e)}")
        raise  # Re-raise to prevent service from starting if tests fail
