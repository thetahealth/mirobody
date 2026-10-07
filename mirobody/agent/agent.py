"""The agent: one harness, on LangChain + deepagents.

`MirobodyAgent.generate_response` runs a turn: the chat model of the
requested `MODELS` entry, the four record tools an external MCP client also
sees (plus the harness's own filesystem tools, the `eval` REPL and
`ask_user`), a Postgres-backed virtual filesystem that projects the person's uploads, library and
health profile read-only, a LangGraph checkpointer that holds the conversation
per session, and the middleware that keeps a turn bounded (model-call budget,
tool-call cap, fault containment, retry governance). Every moving part is
inspectable and self-hostable; this is what mirobody.ai runs.

There is deliberately ONE agent. A deployment that wants a different harness
replaces this class (see `registry.py`); it does not add a second one to
switch between. The MCP surface (`mirobody/mcp/`) is the seam for every other
agent runtime.
"""

import asyncio
import logging
import uuid
from typing import Any, TYPE_CHECKING
from collections.abc import AsyncGenerator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool

from .registry import default_model, llm_client, llm_client_names
from mirobody.kernel import query
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import get_req_ctx
from mirobody.utils.config import safe_read_cfg
from mirobody.utils.config.llm import chat_entries

from . import harness
from .errors import AgentError, ConfigError, client_safe_error
from .hitl import ASK_USER_INTERRUPT, ask_user, interrupt_block, pending_answer
from .models.clients import build_llm_clients, unavailable_reason
from .models.usage import usage_block
from .prompt import attachment_reminder, build_system_prompt, question_language
from .wire.blocks import ERROR, NOTICE, TEXT
from .wire.stream import TokenUsageCallback, stream_blocks
from .middleware import (
    GenotypeRowGuardMiddleware,
    GenotypeSafeSummarizationMiddleware,
    UniversalPromptCachingMiddleware,
)

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel

logger = logging.getLogger(__name__)


def _zone(name: str | None) -> str:
    """`name` when it is an IANA zone, else the deployment's default zone,
    else UTC. A turn's zone arrives from a header, the request body or the
    profile, and `GMT+8`, `UTC+8`, `+08:00`, `CST` or `Etc/Unknown` reached
    `ZoneInfo` in the system prompt unchecked and failed every turn."""
    from mirobody.utils.config import get_default_timezone

    for candidate in (name, get_default_timezone()):
        if not candidate:
            continue
        try:
            ZoneInfo(candidate)
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning("a time zone is not an IANA name; falling back")
            continue
        return candidate
    return "UTC"


def _route(model: str) -> Any:
    """The resolved route (`config.llm.RouteSpec`) of the chat entry `model`,
    or None when it is not one."""
    from mirobody.utils.config.llm import resolve_named

    return resolve_named(model) if model in chat_entries() else None


def _latest_question(messages: list) -> str:
    """The text of the last user message, whichever form the list holds."""
    for message in reversed(messages or []):
        role = message.get("role") if isinstance(message, dict) else getattr(message, "type", "")
        if role in ("user", "human"):
            content = message.get("content") if isinstance(message, dict) else message.content
            return content if isinstance(content, str) else " ".join(
                b.get("text", "") for b in content or [] if isinstance(b, dict))
    return ""


class MirobodyAgent:

    def __init__(
        self,
        timezone: str | None = None,
        allowed_tools: list[str] | None = None,
        disallowed_tools: list[str] | None = None,
        prompt_templates: dict[str, str] = None,
        record_owner: str = "",
        may_write: bool = False,
        **kwargs
    ):
        self.record_owner = record_owner
        # Whether the asker may change the record this turn reads (the chat
        # layer resolves it per turn); an `ask_user` date is filed only then.
        self.may_write = may_write
        self.timezone = _zone(timezone)
        self.allowed_tools = allowed_tools
        self.disallowed_tools = disallowed_tools or []
        self.prompt_templates = prompt_templates
        # The persona name the prompt addresses the model by. Configurable so a
        # deployment can brand it; it is not an identifier anywhere else.
        self.agent_name = safe_read_cfg("AGENT_NAME") or "Mirobody"
        # With no entry ready, the first one, so the error a chat then raises
        # names a real entry and its missing key.
        self.default_model = default_model() or next(iter(chat_entries()), "")
        # Two layers, not interchangeable (see `_build_agent`). MODEL_CALL_LIMIT
        # is the real budget, counted in model calls by ModelCallBudgetMiddleware,
        # whose last call is made without tools so the model answers from what
        # it has. RECURSION_LIMIT is a raw LangGraph super-step
        # ceiling, a last-resort net for a runaway: it must sit above the call
        # budget or it fires first and hard-fails with GraphRecursionError.
        # Unset, `harness.recursion_limit_for` derives it from the built graph.
        self.model_call_limit = int(safe_read_cfg("MODEL_CALL_LIMIT") or 50)
        self.recursion_limit = int(safe_read_cfg("RECURSION_LIMIT") or 0) or None

    def _chat_model(self, requested: str | None) -> tuple[Any, str, str | None]:
        """The client of the `MODELS` entry this turn answers with: the one
        the request names when it is configured, else the default. Returns the
        client, that entry's name, and the notice to show when a named entry
        was not configured and the default answers instead."""
        name = requested or self.default_model
        client = llm_client(name)
        notice = None
        if client is None and requested:
            logger.warning("the requested model is not configured; using the default")
            name, client = self.default_model, llm_client(self.default_model)
            notice = f"Model '{requested}' is not configured. Using the default, '{name}'."
        if client is None:
            available = ", ".join(llm_client_names()) or "none"
            raise ConfigError(f"Model '{requested or name}' is not configured. Available models: {available}.")
        # An entry whose key or address is missing is a placeholder
        # (`build_llm_clients`); its reason names the variable to set.
        reason = unavailable_reason(client)
        if reason:
            logger.error("chat model unavailable: its key or address is not set")
            raise ConfigError(f"Model '{name}' cannot be used: {reason}")
        return client, name, notice

    async def _load_tools(self, user_id: str) -> list[BaseTool]:
        """The MCP tools as LangChain tools, bound to `user_id`. A tool that
        cannot be built is logged and left out by `load_global_tools`."""
        from .tool_loader import load_global_tools

        return await load_global_tools(
            user_id=user_id,
            allowed_tools=self.allowed_tools,
            disallowed_tools=self.disallowed_tools,
        )

    def _get_base_prompt(self, prompt_name: str) -> str:
        """The agent's own system prompt: the `PROMPTS` template named by the
        request, else the first configured one.

        This used to also fetch a prompt the user had saved under the same name
        and append it. Nothing in the shipped client could save one, and a
        health agent's system prompt is not a per-user preference: the
        reading workflow, the flag-against-printed-ranges rule and the
        no-diagnosis framing are the product, not a setting. One source now.
        """
        if self.prompt_templates:
            base_prompt = self.prompt_templates.get(prompt_name) or ""
            if base_prompt:
                return base_prompt
            for key, value in self.prompt_templates.items():
                if value:
                    logger.info("using the first configured prompt template")
                    return value
        raise AgentError("No prompt template is configured (check PROMPTS in config)")


    async def _build_system_prompt(
        self,
        base_prompt: str,
        user_id: str,
        tools: list,
        question: str = "",
    ) -> str:
        """Build system prompt with tools, time, user context, and health-profile core."""
        from mirobody.user.profile import get_health_profile_core
        maxlen = int(safe_read_cfg("PROFILE_CORE_MAXLEN") or 2000)
        health_profile = await get_health_profile_core(user_id, maxlen) if user_id else None
        try:
            system_prompt = await build_system_prompt(
                base_prompt=base_prompt,
                langchain_tools=tools,
                agent_name=self.agent_name,
                record_owner=self.record_owner,
                timezone=self.timezone,
                health_profile=health_profile,
                tool_round_limit=self.model_call_limit,
                answer_language=question_language(question),
            )
            logger.info("Built system prompt successfully")
            return system_prompt
        except Exception as e:
            logger.error("system prompt failed to render: error_type=%s", type(e).__name__,
                         exc_info=not is_driver_exception(e))
            raise AgentError(
                f"The system prompt could not be rendered ({type(e).__name__}); check PROMPTS in the configuration."
            ) from e
    
    async def _build_backend(
        self, session_id: str, user_id: str, file_list: list[dict[str, Any]] | None = None,
        supports_file_block: bool = False, supports_image: bool = True,
    ) -> tuple[Any, list | None]:
        """Build the deepagents virtual filesystem.

        With a ``user_id``: a ``CompositeBackend`` of read-only PROJECTIONS over
        the tables that already own the data: there is no agent-filesystem
        table (see the comment below for how that changed):

          default        → StateBackend                    scratch, rw
          /memories/...  → ProfileBackend                   health profile, ro
          /uploads/...   → ThFilesBackend(scope='uploads')  this request's files, ro
          /library/...   → ThFilesBackend(scope='library')  file history, ro

        ``/uploads/`` and ``/library/`` project ``th_files`` directly (no byte
        copy; parsed text inlined for grep, bytes surfaced multimodally on
        read). Anonymous calls fall back to ``StateBackend``. Returns
        ``(backend, permissions)``.
        """
        if not user_id:
            from deepagents.backends import StateBackend
            return StateBackend(), None

        from .filesystem.files_backend import ThFilesBackend
        from .filesystem.profile_backend import ProfileBackend

        # Every mount is graph state or a read-only PROJECTION of the table
        # that owns the data; there is no agent-filesystem table. Mirroring
        # th_files into pointer rows gave truth a second home, which is how a
        # deleted health document kept answering.
        #   /            scratch space: StateBackend, checkpointed by LangGraph
        #   /memories/   health_user_profile_by_system (is_deleted = false)
        #   /uploads/    th_files, narrowed to THIS request's file_keys
        #   /library/    th_files (is_del = false), the rest of the history
        this_turn_keys = [str(f["file_key"]) for f in (file_list or [])
                          if isinstance(f, dict) and f.get("file_key")]

        # The names `attachment_reminder` announces to the model, so the mount
        # answers to exactly the paths the model was handed. Deriving them from
        # `th_files.file_name` instead would reintroduce the mid-turn rename race
        # (see `ThFilesBackend.__init__`). The `file_key` fallback mirrors the
        # reminder's (`attachment_reminder`, below): a request that carries no
        # `file_name` must still name the file the same way at both ends.
        this_turn_names = {str(f["file_key"]): str(f.get("file_name") or f.get("file_key") or "")
                           for f in (file_list or [])
                           if isinstance(f, dict) and f.get("file_key")}

        memory = ProfileBackend(user_id=user_id)
        uploads = ThFilesBackend(user_id=user_id, scope="uploads",
                                 file_keys=this_turn_keys,
                                 turn_names=this_turn_names,
                                 supports_file_block=supports_file_block,
                                 supports_image=supports_image)
        library = ThFilesBackend(user_id=user_id, scope="library",
                                 file_keys=this_turn_keys,
                                 supports_file_block=supports_file_block,
                                 supports_image=supports_image)

        routes = {
            "/memories/": memory,
            "/uploads/": uploads,
            "/library/": library,
        }
        # Every mount is a PROJECTION (a table, a directory the agent must not
        # edit), so `read_only_mounts` denies writes on all of them up front:
        # a refusal the model does not have to spend a tool call to discover.
        # The scratch root is graph state, not a table: it was a
        # PgFilesystemBackend(scope='workspace') row per file; LangGraph's
        # checkpointer already persists state per thread, so the table was
        # storing what the checkpointer stores.
        return harness.read_only_mounts(routes)


    def _create_stream_config(self, token_counter: Any, session_id: str = "") -> dict:
        """`thread_id` is what the checkpointer keys conversation state on. One
        session == one thread, so a resumed session continues its own history
        and a new session starts clean.

        `configurable` carried a `user_info` dict too. Nothing read it: the
        tools are bound to their caller at load time (`tool_loader`), and
        LangGraph does not put `configurable` into checkpoint metadata, so the
        bearer token it held was neither reaching a tool nor reaching Postgres.
        """
        config: dict[str, Any] = {"callbacks": [token_counter]}
        if self.recursion_limit:
            config["recursion_limit"] = self.recursion_limit
        if session_id:
            config["configurable"] = {"thread_id": session_id}
        return config

    #: Which LangChain transport package carries a non-text content block
    #: inside a ``ToolMessage``: the only shape ``read_file`` can deliver a
    #: PDF in. Keyed by module root, so it covers every client a package
    #: builds: ``langchain_openai`` is ChatOpenAI, AzureChatOpenAI, the
    #: OpenRouter client (ChatOpenAI + base_url) and our ReasoningChatOpenAI
    #: subclass alike, because they all speak Chat Completions.
    _TOOL_MESSAGE_CARRIES_FILES = {
        "langchain_anthropic": True,        # document block inside tool_result
        "langchain_google_genai": True,     # inline_data Part
        "langchain_google_vertexai": True,
        "langchain_openai": False,          # Chat Completions, see below
    }

    def _supports_file_block(self, llm_client: Any) -> bool:
        """Whether THIS TRANSPORT can carry a PDF inside a ``ToolMessage``.

        Not "can this model read a PDF". That is what ``model.profile``
        (``pdf_tool_message`` / ``pdf_inputs``) answers, and it is the wrong
        question: the two are independent, and asking the model is what
        produced these, measured 2026-09-10 with a PDF attached to a question:

            claude-sonnet via OpenRouter → 400 tool messages must include a
                                               non-empty string tool_call_id
            gpt-5.6-terra via OpenAI     → 400 Invalid value: 'file'. Supported
                                               values are: 'text', 'refusal',
                                               'image_url', and 'input_audio'

        Both models genuinely read PDFs (one over Anthropic's native API, the
        other over the Responses API) and neither of those is the endpoint
        this client is talking to. Meanwhile qwen and deepseek were fine, for
        the accidental reason that their profile is ``None``: they took the
        extracted-text path, which works. So the profile got the answer wrong
        in both directions, and the price was a 400 shown to a user who had
        simply attached a report.

        The transport is decided by the client's class, which is what
        ``build_chat_model`` picks from ``llm_type``, so this reads the same
        configuration, one step later and without a second table to keep in
        step.

        **A profile may only narrow, never widen.** The first version of this
        let a non-None ``profile["pdf_tool_message"]`` answer outright, as an
        escape hatch for a gateway that does accept the block, and the 400 it
        was written to end came straight back, because that merged dict has
        three origins and two of them are statements about the MODEL:

        * LangChain's own bundled profile (models.dev). `gpt-5.6-terra` carries
          ``pdf_tool_message: True`` there and still answers `400 Invalid
          value: 'file'` on Chat Completions;
        * the entry's ``supports_pdf`` boolean, which used to write this field
          too (`clients._profile_override`: fixed there as well);
        * an explicit ``profile:`` dict, the only one of the three that is
          about the wire.

        Nothing here can tell them apart, so none of them may GRANT. A ``False``
        may still veto, because being wrong in that direction costs a PDF read
        as extracted text (which works) instead of a 400 shown to a user who
        attached a report. A gateway that genuinely carries the block is a fact
        about a TRANSPORT, so it belongs in ``_TOOL_MESSAGE_CARRIES_FILES``.
        """
        vetoed = (getattr(llm_client, "profile", None) or {}).get("pdf_tool_message") is False

        roots = {base.__module__.split(".")[0] for base in type(llm_client).__mro__}
        for package, carries in self._TOOL_MESSAGE_CARRIES_FILES.items():
            if package in roots:
                carries = carries and not vetoed
                logger.info(f"file-block support: pdf={carries} (transport={package})")  # phi: ok a bool and a package name
                return carries
        # An unrecognised transport serves extracted text: a PDF that reads as
        # text is worse than a native file block, and better than a 400.
        logger.info(f"file-block support: pdf=False (unrecognised transport {type(llm_client).__name__})")
        return False

    def _supports_image(self, client: Any, model: str) -> bool:
        """Whether this turn's model, the `MODELS` entry `model` that
        `_chat_model` picked, is sent an image, or its OCR text.

        A profile or entry `false` is final; a local entry asks its server
        (`served.sees`), because the entry names the model it was written for
        and a text-only one may be running instead (MiniCPM5-2B answered an
        image block with "image input is not supported").
        """
        if (getattr(client, "profile", None) or {}).get("image_inputs") is False:
            return False
        from mirobody.utils.config.served import sees

        spec = _route(model)
        return sees(spec) if spec is not None else True

    def _still_loading(self, model: str) -> bool:
        """Whether `model` runs on a local model server whose router is still
        downloading or loading it (`served.served_status`). A question sent
        then waits for the load with no sign of life: 14 minutes, measured on
        a 4-core CPU on the first start (2026-10-07). An `unloaded` model is
        left to the request, which is what starts its load."""
        from mirobody.utils.config.served import served_status

        spec = _route(model)
        if spec is None or not (spec.base_url_env and spec.base_url):
            return False
        return served_status(spec.base_url, spec.model) == "loading"

    #: Read-only tools the `eval` REPL may call as `tools.<name>`; each guards
    #: itself because the PTC bridge bypasses the tool middleware.
    _PTC_TOOLS: tuple[str, ...] = (query.TOOL_NAME,)
    _MAX_PTC_CALLS = 8
    #: How many times ONE call (same tool, same normalised arguments) may run in
    #: a turn before `RetryGovernanceMiddleware` refuses it. Two, because the
    #: honest reason to repeat a call is a transient failure, and a third
    #: attempt after two transients is a loop, not persistence.
    _RETRY_LIMIT = 2
    #: How many times the health-data tool may run in one turn regardless of
    #: arguments. Twelve is generous for "catalogue, then four indicators, then
    #: a trend" and still bounded; `exit_behavior="continue"` lets the model
    #: write its answer from what it already has rather than ending the turn.
    _QUERY_CALL_LIMIT = 12
    #: Native tools hidden from the model via the harness profile's
    #: ``excluded_tools``. PgFilesystemBackend does not implement ``delete``,
    #: but that alone does not hide it: the capability probe runs against
    #: ``CompositeBackend``, which does implement it, so deepagents offers the
    #: tool and every call comes back "unsupported" after the model has already
    #: spent tokens on it. Excluding by name is what keeps it off the list.
    _EXCLUDED_NATIVE_TOOLS = frozenset({"delete"})

    async def _build_agent(
        self,
        session_id: str,
        user_id: str,
        llm_client: "BaseChatModel",
        system_prompt: str,
        tools: list[BaseTool],
        file_list: list[dict[str, Any]] | None = None,
        supports_file_block: bool = False,
        supports_image: bool = True,
    ) -> tuple[Any, Any]:
        """The compiled graph and the backend it reads through."""
        try:
            # Build the deepagents virtual filesystem (CompositeBackend). User
            # uploads + history are auto-mounted at /uploads/ and /library/ as
            # live th_files projections: the agent reads them with the native
            # read_file tool (multimodal for pdf/image/…). No custom file MCP
            # tools, no external sandbox.
            backend, permissions = await self._build_backend(
                session_id, user_id, file_list, supports_file_block=supports_file_block,
                supports_image=supports_image,
            )

            # The stack itself (fault containment → retry governance → invalid-call
            # repair → empty-answer repair → model-call budget → per-tool caps →
            # interpreter) is `harness.standard_middleware`.
            from langchain_quickjs import CodeInterpreterMiddleware

            # What this agent adds at the tail: the genotype guard and, last so
            # its decision wins, cross-provider prompt caching. The genotype-safe
            # summarisation takes the place of deepagents' own, which runs
            # before the stack (a middleware of the same name replaces it).
            genotype_guard = GenotypeRowGuardMiddleware()
            tail: list[Any] = [
                GenotypeSafeSummarizationMiddleware(llm_client, backend, genotype_guard),
                genotype_guard,
                UniversalPromptCachingMiddleware(ttl="5m", unsupported_model_behavior="ignore")
            ]
            if not supports_image:
                from .middleware import NoVisionReadMiddleware
                tail.insert(0, NoVisionReadMiddleware())

            middleware = harness.standard_middleware(
                retry_limit=self._RETRY_LIMIT,
                model_call_limit=self.model_call_limit,
                # A cap on ONE tool rather than on the loop: the health-data tool
                # is the one a confused model can spin on.
                tool_call_limits={query.TOOL_NAME: self._QUERY_CALL_LIMIT},
                # In-process JS/TS REPL (`eval`). The read-only data tool is
                # exposed inside it as `tools.<name>`; PTC calls bypass the tool
                # middleware, so the data tool guards itself.
                # `DISALLOWED_TOOLS: [eval]` turns the REPL off, as the agent
                # README says it does: it only ever reached the MCP tool list,
                # and the interpreter was added regardless.
                interpreter=None if "eval" in self.disallowed_tools else CodeInterpreterMiddleware(
                    ptc=list(self._PTC_TOOLS), max_ptc_calls=self._MAX_PTC_CALLS),
                tail=tail,
            )

            # Conversation memory. With a checkpointer, LangGraph holds the real
            # message objects per `thread_id` (= session_id) and the caller hands
            # in ONLY the new turn. None (dependency or DB unavailable) degrades
            # to a stateless turn rather than a failure; see checkpointer.py.
            from .checkpointer import get_checkpointer

            agent = harness.assemble(
                model=llm_client,
                # Agent-only tool (hitl.py): the chat channel's "which date?"
                # question; the answer is applied on resume, in generate_response.
                # Never in the MCP tool directory: an MCP client has no widget
                # to answer ask_user with.
                tools=[*tools, ask_user],
                system_prompt=system_prompt,
                backend=backend,
                permissions=permissions,
                middleware=middleware,
                checkpointer=await get_checkpointer(),
                interrupt_on=ASK_USER_INTERRUPT,
                recursion_limit=self.recursion_limit,
                model_call_limit=self.model_call_limit,
                excluded_native_tools=self._EXCLUDED_NATIVE_TOOLS,
            )

            logger.info(f"Agent built successfully for session: {session_id}")
            return agent, backend

        except AgentError:
            raise
        except Exception as e:
            logger.error("agent build failed: error_type=%s", type(e).__name__,
                         exc_info=not is_driver_exception(e))
            raise AgentError(f"The agent could not be built ({type(e).__name__}).") from e

    async def _stream_agent_response(
        self,
        agent: Any,
        messages: list[dict[str, Any]] | list[BaseMessage],
        config: dict,
        resume: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Stream one run of the graph as `wire.blocks`.

        `resume` is the user's answer to an open `ask_user` question: the
        thread is paused on that interrupt, so the answer goes in as the
        tool's result (`Command(resume=...)`) instead of as a new message.
        """
        logger.info("Starting agent stream")
        trace_id = get_req_ctx("trace_id") or str(uuid.uuid4())
        if resume is not None:
            from langgraph.types import Command
            graph_input: Any = Command(resume={"decisions": [{"type": "respond", "message": resume}]})
        else:
            graph_input = {"messages": messages}

        try:
            # No `subgraphs=True`: the graph has none (`harness.assemble`
            # disables the general-purpose subagent and passes `subagents=[]`).
            async for stream_type, stream_event in agent.astream(
                graph_input, stream_mode=["messages", "updates"], config=config,
            ):
                # An `ask_user` call: the middleware paused the graph after the
                # model step. Hand the pending question to the client and end
                # the turn; the next user message resumes this thread.
                if stream_type == "updates" and isinstance(stream_event, dict) and "__interrupt__" in stream_event:
                    pending = interrupt_block(stream_event["__interrupt__"])
                    if pending:
                        yield pending
                    return
                # `stream_blocks` logs and drops an item it cannot read.
                async for block in stream_blocks(stream_type, stream_event, trace_id=trace_id):
                    yield block

            logger.info("agent stream completed")

        except AgentError as e:
            # Raised by our own middleware (a model that keeps sending malformed
            # calls): its message is ours, and says more than a type name.
            logger.error("agent stream stopped: error_type=%s", type(e).__name__)
            yield {"type": ERROR, "message": str(e)}

        except Exception as e:
            # Type name only, in the log and to the client: provider error bodies
            # echo prompts, driver errors echo statements, and both were reaching
            # the browser and the chat history through str(e).
            logger.error("agent streaming error: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
            detail = client_safe_error(e)
            # A connect failure names neither the unreachable host nor the way
            # out. Connection-shaped errors get the pointer the docs already
            # carry: openrouter.ai is unreachable from some networks, and the
            # DashScope gateway is the documented drop-in fallback.
            if "connect" in type(e).__name__.lower():
                detail += (
                    " The model provider's endpoint may be unreachable from "
                    "this network; see the DashScope fallback in config.yaml."
                )
            yield {"type": ERROR, "message": detail}

    async def generate_response(
        self,
        user_id: str,
        messages: list[dict[str, Any]] | list[BaseMessage],
        session_id: str = "",
        file_list: list[dict[str, Any]] | None = None,
        provider: str | None = None,
        prompt_name: str = "",
        language: str = "",
        **kwargs
    ) -> AsyncGenerator[dict[str, Any], None]:
        """One turn, as `wire.blocks` (`registry.AbstractAgent`). `provider` is
        the request's field of that name: the `MODELS` entry to answer with,
        the default when empty. `language` is the asker's, for a sentence the
        harness says itself."""
        if not messages:
            yield {"type": ERROR, "message": "Empty message"}
            return

        if not user_id:
            yield {"type": ERROR, "message": "User ID is required"}
            return

        logger.info("agent request: session_id=%s model=%s message_count=%d", session_id, provider, len(messages))

        try:
            llm_client, model, notice = self._chat_model(provider)
            if notice:
                # The SYSTEM speaking, not the model. On the reasoning channel
                # it was indistinguishable from the model's own trace.
                yield {"type": NOTICE, "message": notice}
            if await asyncio.to_thread(self._still_loading, model):
                logger.info("chat model still loading; answered without it: session_id=%s", session_id)
                yield {"type": TEXT, "text": localize("local_model_loading", language or "en", module="chat")}
                return
            model_name = getattr(llm_client, "model_name", None) or getattr(llm_client, "model", "Unknown")

            # Uploads reach the agent as FILES, never as message payload:
            # `_build_backend` projects them into /uploads/ by file_key
            # (ThFilesBackend over th_files, no byte copy) and the prompt tells
            # the model to read_file them on demand.
            loaded_tools = await self._load_tools(user_id)
            system_prompt = await self._build_system_prompt(
                self._get_base_prompt(prompt_name), user_id, loaded_tools, _latest_question(messages))

            supports_file_block = self._supports_file_block(llm_client)
            supports_image = await asyncio.to_thread(self._supports_image, llm_client, model)

            agent, backend = await self._build_agent(
                session_id=session_id,
                user_id=user_id,
                llm_client=llm_client,
                system_prompt=system_prompt,
                tools=loaded_tools,
                file_list=file_list,
                supports_file_block=supports_file_block,
                supports_image=supports_image,
            )

            token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(token_counter, session_id)

            # A thread paused on `ask_user` takes this message as the answer;
            # the attachment note belongs to a NEW turn only.
            final_messages = messages
            resume = await pending_answer(agent, stream_config, messages, user_id, may_write=self.may_write)
            if resume is None:
                # Name this turn's attachments and where to read them, so the
                # model never needs an `ls /uploads/` round trip and never
                # silently misses one. A message of this turn, not part of the
                # cached system prompt (`attachment_reminder`). Matches the list's
                # element type (BaseMessage vs dict) rather than mixing forms.
                reminder = await attachment_reminder(backend, file_list)
                if reminder:
                    if final_messages and isinstance(final_messages[-1], BaseMessage):
                        from langchain_core.messages import HumanMessage
                        final_messages = [*final_messages, HumanMessage(content=reminder)]
                    else:
                        final_messages = [*final_messages, {"role": "user", "content": reminder}]

            async for event in self._stream_agent_response(
                agent=agent,
                messages=final_messages,
                config=stream_config,
                resume=resume,
            ):
                yield event

            usage = usage_block(token_counter.usage, model_name)
            if usage:
                yield usage

        except AgentError as e:
            # An AgentError's message is ours (configuration, no model…) and
            # safe to show; anything else is reported by type only.
            logger.error("agent error: error_type=%s", type(e).__name__)
            yield {"type": ERROR, "message": str(e)}

        except Exception as e:
            logger.error("Unexpected error: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
            yield {"type": ERROR, "message": client_safe_error(e)}

    @classmethod
    def load_llm_clients(cls, llm_client_config: dict[str, Any]) -> dict[str, Any]:
        """The registry's hook: one chat model per `MODELS` entry."""
        return build_llm_clients(llm_client_config, owner=cls.__name__)
