import functools
import logging

from psycopg_pool import AsyncConnectionPool

from ..registry import available_models, load_agent
from .session import (
    create_session,
    get_session_summaries,
    get_session_summaries_by_person,
    delete_session
)
from .model import ChatStreamRequest, has_attachment
from .message import (
    get_chat_history,
    set_message_rating
)
from .adapters import HTTPChatAdapter

from ...user import (
    JwtTokenValidator,
)
from ...user.user import get_user_info
from ...user.care_circle import beneficiary_users
from ...utils.sse import sse_headers
from ...utils import (
    json_response_with_code,
    json_response,

    global_config,

    Request,
    Response,
    StreamingResponse,
    Route
)

logger = logging.getLogger(__name__)


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

    One endpoint needs this and it does not fit `requires_auth`: `chat_handler`
    reads `request.state.user_id`, populated by middleware, rather than
    verifying the header itself.

    Labelled separately from `public_endpoint` so the "reachable without a
    token" set stays small and stays checkable.
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

        agent_dirs      : list[str] | None = None,
    ):
        self._token_validator = token_validator
        self._db_pool = db_pool

        # Called for the side effect: the registry holds the one agent class
        # and its LLM clients for the life of the process.
        load_agent(agent_dirs or [], config=global_config())

        #-------------------------------------------------

        self.routes = routes if routes is not None else []

        for path, handler, methods in (
            ("/api/models",             self.model_handler,                     ["GET"]),
            ("/api/prompts",            self.prompt_handler,                    ["GET"]),

            ("/api/session",            self.session_handler,                   ["POST"]),

            ("/api/history",            self.history_handler,                   ["GET"]),
            ("/api/history_by_person",  self.personal_history_handler,          ["GET"]),
            ("/api/history/delete",     self.history_delete_handler,            ["POST"]),
            ("/api/rating",             self.rating_handler,                    ["POST"]),

            ("/api/chat",               self.chat_handler,                      ["POST"]),

            ("/api/beneficiary-users",  self.beneficiary_user_handler,          ["GET"]),
        ):
            # OPTIONS on every route: each handler answers preflight via its
            # auth decorator, so the route must accept the method to reach it.
            self.routes.append(
                Route(f"{uri_prefix}{path}", endpoint=handler, methods=[*methods, "OPTIONS"])
            )

    #-------------------------------------------------------------------------

    @public_endpoint
    async def model_handler(self, request: Request) -> Response:
        """Provider names whose key resolves — bare names, no `Agent/` prefix.

        The shipped web client splits each entry on `/` into `{agent, provider}`
        and falls back to the whole string as the provider when there is no
        slash, so a bare name works unchanged.
        """
        data = available_models()

        return json_response_with_code(
            data=data,
            request=request
        )

    #-------------------------------------------------------------------------

    @public_endpoint
    async def prompt_handler(self, request: Request) -> Response:
        """The `PROMPTS` templates a chat request may name with `prompt_name`.

        One list, from config, the same for every caller. `{"system": [...]}` is
        the shape the shipped web client reads; it used to carry a `user` list
        too (prompts a person saved through `/api/user/prompt/*`) — nothing in
        the shipped client could create one, and a framework's system prompt is
        not something a user edits from a settings box, so that surface is gone.
        A deployment with no configured templates gets an empty list.
        """
        options = global_config().get_agent_settings()
        templates = options.get("prompt_templates") if isinstance(options, dict) else None
        system_prompts = [{"name": name} for name in (templates or {})]

        return json_response_with_code(
            data={"system": system_prompts},
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
        # a hard -3 for every client. `has_attachment` rather than truthiness:
        # `[{}]` names no file, and letting it through would promise the model
        # an attachment that `/uploads/` cannot serve.
        if not params["question"] and not has_attachment(params.get("file_list")):
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
                logger.warning(err, extra={"user": user_id})
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
            headers=sse_headers(),
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

    #-------------------------------------------------------------------------

#-----------------------------------------------------------------------------
