import aiohttp, datetime, json, logging, secrets, time

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from .agent import (
    load_agents_from_directories,
    get_global_agents,
    get_agents_with_llm_client_names
)
from .session import (
    create_session,
    get_session_summaries,
    get_session_summaries_by_person,
    delete_session
)
from .model import ChatStreamRequest
from .message import (
    get_chat_history
)
from .user_config import (
    get_user_mcps,
    set_user_mcp,
    delete_user_mcp,

    get_user_prompts,
    set_user_prompt,
    delete_user_prompt
)
from .unified_chat_service import UnifiedChatService
from .adapters import HTTPChatAdapter

from ..user import (
    JwtTokenValidator,
)
from ..user.user import get_user_info
from ..user.sharing import get_sharing_service
from ..utils import (
    json_response_with_code,
    json_response,

    execute_query,

    global_config,

    Request,
    Response,
    StreamingResponse,
    Route
)

#-----------------------------------------------------------------------------

class ChatService:
    def __init__(
        self,
        token_validator : JwtTokenValidator | None = None,

        uri_prefix      : str = "",
        routes          : list | None = None,

        db_pool         : AsyncConnectionPool | None = None,
        redis           : Redis | None = None,

        mcp_server_url  : str = "",

        agent_dirs          : list[str] = [],
        private_agent_dirs  : list[str] = [],
        api_keys        : dict[str, str] = {}
    ):
        self._token_validator = token_validator

        self._db_pool = db_pool
        self._redis = redis

        self._mcp_server_url = mcp_server_url

        self._openai_api_key = api_keys.get("OPENAI_API_KEY", "")
        self._gemini_api_key = api_keys.get("GOOGLE_API_KEY", "")

        cfg = global_config()
        self._agents = load_agents_from_directories(agent_dirs, config=cfg)
        self._private_agents = load_agents_from_directories(private_agent_dirs, private=True, config=cfg)

        self._agent_count = len(self._agents) + len(self._private_agents)

        #-------------------------------------------------

        if routes is not None:
            self.routes = routes
        else:
            self.routes = []

        self.routes.append(Route(f"{uri_prefix}/api/agents", endpoint=self.agents_handler, methods=["GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/providers", endpoint=self.provider_handler, methods=["GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/models", endpoint=self.model_handler, methods=["GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/prompts", endpoint=self.prompt_handler, methods=["GET", "OPTIONS"]))
        
        self.routes.append(Route(f"{uri_prefix}/api/session", endpoint=self.session_handler, methods=["POST", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/api/history", endpoint=self.history_handler, methods=["GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/history_by_person", endpoint=self.personal_history_handler, methods=["GET", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/history/delete", endpoint=self.history_delete_handler, methods=["POST", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/api/chat", endpoint=self.chat_handler, methods=["POST", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/api/beneficiary-users", endpoint=self.beneficiary_user_handler, methods=["GET", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/api/user/mcp", endpoint=self.mcp_config_get_handler, methods=["GET", "POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/user/mcp/set", endpoint=self.mcp_config_server_set_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/user/mcp/delete", endpoint=self.mcp_config_server_delete_handler, methods=["POST", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/api/user/prompt", endpoint=self.prompt_config_get_handler, methods=["GET", "POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/user/prompt/set", endpoint=self.prompt_config_set_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/api/user/prompt/delete", endpoint=self.prompt_config_delete_handler, methods=["POST", "OPTIONS"]))


    #-----------------------------------------------------

    async def agents_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        data = []
        for agent_name in get_global_agents(public=True):
            if agent_name:
                data.append({
                    "name": agent_name,
                    "description": "",
                    "code": agent_name
                })
        data = sorted(data, key=lambda x: x["code"])

        return json_response_with_code(
            data=data,
            request=request
        )

    #-------------------------------------------------------------------------

    async def model_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        data = get_agents_with_llm_client_names()

        return json_response_with_code(
            data=data,
            request=request
        )

    #-------------------------------------------------------------------------

    async def provider_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        names = get_agents_with_llm_client_names()
        data = [{"code": k.split("/")[1], "name": k} for k in names]

        return json_response_with_code(
            data=data,
            request=request
        )

    #-------------------------------------------------------------------------

    async def prompt_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        system_prompts = []
        options = global_config().get_options_for_agent("deep")
        if isinstance(options, dict) and "prompt_templates" in options:
            system_prompts = [{"name": name} for name in options["prompt_templates"]]

        user_prompts = []
        user_id, err = self._token_validator.verify_http_token(request)
        if not err and user_id:
            user_prompts_dict, err = await get_user_prompts(user_id)
            if not err and user_prompts_dict:
                user_prompts = [{"name": name, "order": value.get("order", 0)} for name, value in user_prompts_dict.items()]

        return json_response_with_code(data={"system": system_prompts, "user": user_prompts}, request=request)

    #-------------------------------------------------------------------------

    async def session_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)

        #-------------------------------------------------

        try:
            params = await request.json()
            query_user_id = params.get("query_user_id", user_id)
        except:
            query_user_id = user_id

        return json_response(
            await create_session(user_id, query_user_id),
            request=request
        )

    #-------------------------------------------------------------------------

    async def history_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)

        #-------------------------------------------------

        session_id = request.query_params.get("session_id", "")

        try:
            if session_id:
                history = await get_chat_history(user_id, session_id)
                return json_response_with_code(
                    data={
                        "history": history
                    },
                    request=request
                )
            else:
                summaries = await get_session_summaries(user_id)
                return json_response_with_code(
                    data={
                        "summaries": summaries
                    },
                    request=request
                )
        except Exception as e:
            return json_response_with_code(
                code=-1,
                msg=str(e)
            )

    #-------------------------------------------------------------------------

    async def personal_history_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)

        #-------------------------------------------------

        return json_response(
            await get_session_summaries_by_person(user_id),
            request=request
        )
    
    #-------------------------------------------------------------------------

    async def history_delete_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)

        #-------------------------------------------------

        try:
            params = await request.json()
            session_id = params["session_id"]
        
        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)

        #-------------------------------------------------

        err = await delete_session(user_id, session_id)
        if err:
            return json_response_with_code(-2, err, request=request)

        return json_response_with_code(request=request)
    
    #-------------------------------------------------------------------------

    async def chat_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        if not request.state.user_id or \
            not isinstance(request.state.user_id, int) or \
            request.state.user_id <= 0:

            return json_response(status_code=401, request=request)

        user_id = request.state.user_id

        #-------------------------------------------------

        try:
            params = await request.json()
            params["user_id"] = str(user_id)
            params["token"] = request.headers.get("Authorization")

        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)

        #-------------------------------------------------

        if "timezone" not in params:
            if request.state.timezone:
                params["timezone"] = request.state.timezone

        if "language" not in params:
            if request.state.language:
                params["language"] = request.state.language

        if "timezone" not in params or "language" not in params:
            user_info, err = await get_user_info(self._db_pool, user_id)
            if err:
                logging.warning(err, extra={"user": user_id})
            else:
                if "timezone" not in params:
                    params["timezone"] = user_info.timezone if user_info.timezone else "America/Los_Angeles"
                if "language" not in params:
                    params["language"] = user_info.language if user_info.language else "en"

        #-------------------------------------------------

        adapter = HTTPChatAdapter(UnifiedChatService())

        return StreamingResponse(
            adapter.handle_request(
                params=ChatStreamRequest(**params),
            ),
            headers={
                "cache-control": "no-cache, no-transform",
                "x-accel-buffering": "no",
            },
            media_type="text/event-stream"
        )
    
    #-------------------------------------------------------------------------

    async def beneficiary_user_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)

        #-------------------------------------------------

        try:
            service = await get_sharing_service()
            data = await service.get_query_users_simple(user_id)

        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)
        
        return json_response_with_code(data=data, request=request)

    #-------------------------------------------------------------------------

    async def mcp_config_get_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)
        
        #-------------------------------------------------

        config, err = await get_user_mcps(user_id)
        if err:
            return json_response_with_code(-1, err, request=request)
        
        return json_response_with_code(data=config, request=request)
    

    async def mcp_config_server_set_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)
        
        #-------------------------------------------------

        try:
            params = await request.json()

            if not params or not isinstance(params, dict):
                return json_response_with_code(-1, "Invalid request body.", request=request)

        except Exception as e:
            return json_response_with_code(-2, str(e), request=request)
        
        #-------------------------------------------------

        err = await set_user_mcp(
            user_id,
            params.get("name"),
            params.get("url"),
            params.get("token", ""),
            params.get("enabled", True),
            params.get("order", 0)
        )
        if err:
            return json_response_with_code(-3, err, request=request)
        
        return json_response_with_code(request=request)


    async def mcp_config_server_delete_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)
        
        #-------------------------------------------------

        try:
            params = await request.json()

            if not params or not isinstance(params, dict):
                return json_response_with_code(-1, "Invalid request body.", request=request)

        except Exception as e:
            return json_response_with_code(-2, str(e), request=request)
        
        #-------------------------------------------------

        err = await delete_user_mcp(user_id, params.get("name"))
        if err:
            return json_response_with_code(-3, err, request=request)
        
        return json_response_with_code(request=request)

    #-------------------------------------------------------------------------

    async def prompt_config_get_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)
        
        #-------------------------------------------------

        prompts, err = await get_user_prompts(user_id)
        if err:
            return json_response_with_code(-2, err, request=request)
        
        prompts_list = []
        for prompt_name, prompt_value in prompts.items():
            prompt_value["name"] = prompt_name
            prompts_list.append(prompt_value)

        return json_response_with_code(data=prompts_list, request=request)


    async def prompt_config_set_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)
        
        #-------------------------------------------------

        try:
            params = await request.json()

            if not params or not isinstance(params, dict):
                return json_response_with_code(-1, "Invalid request body.", request=request)

        except Exception as e:
            return json_response_with_code(-2, str(e), request=request)
        
        #-------------------------------------------------

        err = await set_user_prompt(user_id, params.get("name"), params.get("prompt"), params.get("order"))
        if err:
            return json_response_with_code(-3, err, request=request)
        
        return json_response_with_code(request=request)


    async def prompt_config_delete_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        #-------------------------------------------------

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)
        
        #-------------------------------------------------

        try:
            params = await request.json()

            if not params or not isinstance(params, dict):
                return json_response_with_code(-1, "Invalid request body.", request=request)

        except Exception as e:
            return json_response_with_code(-2, str(e), request=request)
        
        #-------------------------------------------------

        err = await delete_user_prompt(user_id, params.get("name"))
        if err:
            return json_response_with_code(-3, err, request=request)
        
        return json_response_with_code(request=request)

#-----------------------------------------------------------------------------
