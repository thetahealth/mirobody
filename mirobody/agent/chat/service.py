import functools, logging

from psycopg_pool import AsyncConnectionPool

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
    get_chat_history,
    set_message_rating
)
from .user_config import (
    get_user_mcps,
    set_user_mcp,
    delete_user_mcp,

    get_user_prompts,
    set_user_prompt,
    delete_user_prompt
)
from .adapters import HTTPChatAdapter

from ...user import (
    JwtTokenValidator,
)
from ...user.user import get_user_info
from ...user.care_circle import beneficiary_users
from ...utils import (
    json_response_with_code,
    json_response,

    global_config,

    Request,
    Response,
    StreamingResponse,
    Route
)


#-----------------------------------------------------------------------------

def _preflight(request: Request) -> Response | None:
    """CORS preflight, which every endpoint here has to answer identically."""
    if request.method == "OPTIONS":
        return json_response_with_code(disable_log=True)
    return None


def public_endpoint(fn):
    """Reachable without a token. Handles preflight, nothing else.

    Explicit rather than implicit: an endpoint is public because it says so,
    not because someone forgot the token check. `test_service_handlers.py`
    pins the resulting public set.
    """
    @functools.wraps(fn)
    async def wrapper(self, request: Request, *args, **kwargs) -> Response:
        return _preflight(request) or await fn(self, request, *args, **kwargs)

    wrapper.__mirobody_public__ = True
    return wrapper


def self_authenticating(fn):
    """Preflight only — the handler does its own, non-standard auth.

    Two endpoints need this and neither fits `requires_auth`:

    * `chat_handler` reads `request.state.user_id`, populated by middleware,
      rather than verifying the header itself.
    * `prompt_handler` verifies a token but must NOT 401 without one: it
      returns the system prompt list to anonymous callers and merges in the
      user's own prompts only when a token is present. Wrapping it in
      `requires_auth` would 401 clients that legitimately have no session yet.

    Labelled separately from `public_endpoint` so the "reachable without a
    token" set stays exactly three endpoints and stays checkable.
    """
    @functools.wraps(fn)
    async def wrapper(self, request: Request, *args, **kwargs) -> Response:
        return _preflight(request) or await fn(self, request, *args, **kwargs)

    wrapper.__mirobody_public__ = False
    return wrapper


async def _json_body(request: Request) -> tuple[dict | None, Response | None]:
    """Parse a JSON object body. Five handlers repeated this same try/except
    plus dict check verbatim; the error codes they returned (-1 vs -2) differed
    only by which one the author happened to paste."""
    try:
        params = await request.json()
    except Exception as e:
        return None, json_response_with_code(-1, str(e), request=request)
    if not params or not isinstance(params, dict):
        return None, json_response_with_code(-1, "Invalid request body.", request=request)
    return params, None


def requires_auth(fn):
    """Preflight, then a verified bearer token; passes `user_id` to the handler.

    This preamble was copy-pasted into 13 handlers. Beyond the repetition, it
    made authentication a thing you had to remember to write — a new handler
    was authenticated only if its author happened to paste the right four
    lines. Here it is the decorator, so leaving it off is visible.
    """
    @functools.wraps(fn)
    async def wrapper(self, request: Request, *args, **kwargs) -> Response:
        preflight = _preflight(request)
        if preflight is not None:
            return preflight

        user_id, err = self._token_validator.verify_http_token(request)
        if err:
            return json_response(err, status_code=401, request=request)

        return await fn(self, request, user_id, *args, **kwargs)

    wrapper.__mirobody_public__ = False
    return wrapper

class ChatService:
    def __init__(
        self,
        token_validator : JwtTokenValidator | None = None,

        uri_prefix      : str = "",
        routes          : list | None = None,

        db_pool         : AsyncConnectionPool | None = None,

        agent_dirs          : list[str] = [],
        private_agent_dirs  : list[str] = [],
    ):
        self._token_validator = token_validator
        self._db_pool = db_pool

        # Called for the side effect: both populate the module-global agent
        # registry that `get_global_agent(s)` reads. The returned dicts used to
        # be stored on self and counted into `_agent_count`, which nothing read.
        cfg = global_config()
        load_agents_from_directories(agent_dirs, config=cfg)
        load_agents_from_directories(private_agent_dirs, private=True, config=cfg)

        #-------------------------------------------------

        self.routes = routes if routes is not None else []

        for path, handler, methods in (
            ("/api/agents",             self.agents_handler,                    ["GET"]),
            ("/api/providers",          self.provider_handler,                  ["GET"]),
            ("/api/models",             self.model_handler,                     ["GET"]),
            ("/api/prompts",            self.prompt_handler,                    ["GET"]),

            ("/api/session",            self.session_handler,                   ["POST"]),

            ("/api/history",            self.history_handler,                   ["GET"]),
            ("/api/history_by_person",  self.personal_history_handler,          ["GET"]),
            ("/api/history/delete",     self.history_delete_handler,            ["POST"]),
            ("/api/rating",             self.rating_handler,                    ["POST"]),

            ("/api/chat",               self.chat_handler,                      ["POST"]),

            ("/api/beneficiary-users",  self.beneficiary_user_handler,          ["GET"]),

            ("/api/user/mcp",           self.mcp_config_get_handler,            ["GET", "POST"]),
            ("/api/user/mcp/set",       self.mcp_config_server_set_handler,     ["POST"]),
            ("/api/user/mcp/delete",    self.mcp_config_server_delete_handler,  ["POST"]),

            ("/api/user/prompt",        self.prompt_config_get_handler,         ["GET", "POST"]),
            ("/api/user/prompt/set",    self.prompt_config_set_handler,         ["POST"]),
            ("/api/user/prompt/delete", self.prompt_config_delete_handler,      ["POST"]),
        ):
            # OPTIONS on every route: each handler answers preflight via its
            # auth decorator, so the route must accept the method to reach it.
            self.routes.append(
                Route(f"{uri_prefix}{path}", endpoint=handler, methods=[*methods, "OPTIONS"])
            )

    #-------------------------------------------------------------------------

    @public_endpoint
    async def agents_handler(self, request: Request) -> Response:
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

    @public_endpoint
    async def model_handler(self, request: Request) -> Response:
        data = get_agents_with_llm_client_names()

        return json_response_with_code(
            data=data,
            request=request
        )

    #-------------------------------------------------------------------------

    @public_endpoint
    async def provider_handler(self, request: Request) -> Response:
        names = get_agents_with_llm_client_names()
        data = [{"code": k.split("/")[1], "name": k} for k in names]

        return json_response_with_code(
            data=data,
            request=request
        )

    #-------------------------------------------------------------------------

    @self_authenticating
    async def prompt_handler(self, request: Request) -> Response:
        """System prompts for ONE agent, plus the caller's own saved prompts.

        The agent is a query parameter because a prompt belongs to an agent, not
        to the deployment: `deep.jinja` describes a virtual filesystem, QuickJS
        and chart tools that BaseAgent does not have, so offering it to a Base
        session is offering instructions for tools that are not there. This
        used to hardcode `get_options_for_agent("deep")` and answer every caller
        with Deep's list whatever agent they had selected.

        An agent with no configured templates gets an empty list — which is the
        honest answer, and is what tells a client there is nothing to pick.
        """
        agent = (request.query_params.get("agent") or "deep").strip().lower() or "deep"

        system_prompts = []
        options = global_config().get_options_for_agent(agent)
        if isinstance(options, dict) and "prompt_templates" in options:
            system_prompts = [{"name": name} for name in options["prompt_templates"]]

        user_prompts = []
        user_id, err = self._token_validator.verify_http_token(request)
        if not err and user_id:
            user_prompts_dict, err = await get_user_prompts(user_id)
            if not err and user_prompts_dict:
                user_prompts = [{"name": name, "order": value.get("order", 0)} for name, value in user_prompts_dict.items()]

        # Echo the agent back: the caller asked for one agent's prompts and a
        # client that cannot tell which list it received is the bug above.
        return json_response_with_code(
            data={"agent": agent, "system": system_prompts, "user": user_prompts},
            request=request,
        )

    #-------------------------------------------------------------------------

    @requires_auth
    async def session_handler(self, request: Request, user_id: str) -> Response:
        try:
            params = await request.json()
            query_user_id = params.get("query_user_id", user_id)
            session_id = params.get("session_id")  # Optional — clients may
                                                   # supply a structured id
                                                   # (e.g. a client encoding
                                                   # compare pane info into it).
        except Exception:
            query_user_id = user_id
            session_id = None

        return json_response(
            await create_session(user_id, query_user_id, session_id=session_id),
            request=request
        )

    #-------------------------------------------------------------------------

    @requires_auth
    async def history_handler(self, request: Request, user_id: str) -> Response:
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

    @requires_auth
    async def personal_history_handler(self, request: Request, user_id: str) -> Response:
        return json_response(
            await get_session_summaries_by_person(user_id),
            request=request
        )
    
    #-------------------------------------------------------------------------

    @requires_auth
    async def history_delete_handler(self, request: Request, user_id: str) -> Response:
        params, err_response = await _json_body(request)
        if err_response:
            return err_response
        session_id = params.get("session_id")
        if not session_id:
            return json_response_with_code(-1, "missing session_id", request=request)

        err = await delete_session(user_id, session_id)
        if err:
            return json_response_with_code(-2, err, request=request)

        return json_response_with_code(request=request)

    #-------------------------------------------------------------------------

    @requires_auth
    async def rating_handler(self, request: Request, user_id: str) -> Response:
        try:
            params = await request.json()
            rating = int(params["rating"])
            # The web client rates an assistant response by its message id.
            message_id = (
                params.get("responseId")
                or params.get("response_id")
                or params.get("questionId")
                or params.get("question_id")
            )
        except (KeyError, TypeError, ValueError) as e:
            return json_response_with_code(-1, f"invalid rating payload: {e}", request=request)

        if not message_id:
            return json_response_with_code(-1, "missing responseId", request=request)

        #-------------------------------------------------

        updated = await set_message_rating(user_id, str(message_id), rating)
        if not updated:
            return json_response_with_code(-2, "message not found or not owned by user", request=request)

        return json_response_with_code(request=request)

    #-------------------------------------------------------------------------

    @self_authenticating
    async def chat_handler(self, request: Request) -> Response:
        if not request.state.user_id or \
            not isinstance(request.state.user_id, int) or \
            request.state.user_id <= 0:

            return json_response("", status_code=401, request=request)

        user_id = request.state.user_id

        #-------------------------------------------------

        try:
            params = await request.json()
            params["user_id"] = str(user_id)
            params["token"] = request.headers.get("Authorization")

        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)

        #-------------------------------------------------

        if not isinstance(params.get("question"), str):
            return json_response_with_code(-2, "Invalid question.", request=request)

        params["question"] = params["question"].strip()
        # An attachment-only turn is a real request ("read this"), so a missing
        # question is only empty when nothing else came with it. This guard used
        # to reject on the text alone, which made "attach an image, press send"
        # a hard -3 for every client.
        if not params["question"] and not params.get("file_list"):
            return json_response_with_code(-3, "Empty question.", request=request)

        #-------------------------------------------------

        if "timezone" not in params:
            if request.state.timezone:
                params["timezone"] = request.state.timezone

        if "language" not in params:
            if request.state.language:
                params["language"] = request.state.language

        if "timezone" not in params or "language" not in params:
            user_info, err = await get_user_info(user_id)
            if err:
                logging.warning(err, extra={"user": user_id})
            else:
                if "timezone" not in params:
                    from mirobody.utils.config import get_default_timezone
                    params["timezone"] = user_info.timezone if user_info.timezone else get_default_timezone()
                if "language" not in params:
                    params["language"] = user_info.language if user_info.language else "en"

        #-------------------------------------------------

        # Reject unknown fields the way the MCP surface does, instead of
        # letting `ChatStreamRequest(**params)` turn a caller's typo (or a
        # natural guess like `model`) into a bare 500.
        import inspect
        accepted = set(inspect.signature(ChatStreamRequest.__init__).parameters) - {"self"}
        unknown = sorted(set(params) - accepted)
        if unknown:
            return json_response_with_code(
                -4,
                f"Unknown field(s): {', '.join(unknown)}. "
                f"Accepted: {', '.join(sorted(accepted))}.",
                request=request,
            )

        adapter = HTTPChatAdapter()

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

    @requires_auth
    async def beneficiary_user_handler(self, request: Request, user_id: str) -> Response:
        try:
            data = await beneficiary_users(user_id)

        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)
        
        return json_response_with_code(data=data, request=request)

    #-------------------------------------------------------------------------

    @requires_auth
    async def mcp_config_get_handler(self, request: Request, user_id: str) -> Response:
        config, err = await get_user_mcps(user_id)
        if err:
            return json_response_with_code(-1, err, request=request)
        
        return json_response_with_code(data=config, request=request)
    

    @requires_auth
    async def mcp_config_server_set_handler(self, request: Request, user_id: str) -> Response:
        params, err_response = await _json_body(request)
        if err_response:
            return err_response

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


    @requires_auth
    async def mcp_config_server_delete_handler(self, request: Request, user_id: str) -> Response:
        params, err_response = await _json_body(request)
        if err_response:
            return err_response

        err = await delete_user_mcp(user_id, params.get("name"))
        if err:
            return json_response_with_code(-3, err, request=request)
        
        return json_response_with_code(request=request)

    #-------------------------------------------------------------------------

    @requires_auth
    async def prompt_config_get_handler(self, request: Request, user_id: str) -> Response:
        prompts, err = await get_user_prompts(user_id)
        if err:
            return json_response_with_code(-2, err, request=request)
        
        prompts_list = []
        for prompt_name, prompt_value in prompts.items():
            prompt_value["name"] = prompt_name
            prompts_list.append(prompt_value)

        return json_response_with_code(data=prompts_list, request=request)


    @requires_auth
    async def prompt_config_set_handler(self, request: Request, user_id: str) -> Response:
        params, err_response = await _json_body(request)
        if err_response:
            return err_response

        err = await set_user_prompt(user_id, params.get("name"), params.get("prompt"), params.get("order"))
        if err:
            return json_response_with_code(-3, err, request=request)
        
        return json_response_with_code(request=request)


    @requires_auth
    async def prompt_config_delete_handler(self, request: Request, user_id: str) -> Response:
        params, err_response = await _json_body(request)
        if err_response:
            return err_response

        err = await delete_user_prompt(user_id, params.get("name"))
        if err:
            return json_response_with_code(-3, err, request=request)
        
        return json_response_with_code(request=request)

#-----------------------------------------------------------------------------
