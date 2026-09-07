"""The agent: one harness, on LangChain + deepagents.

`MirobodyAgent.generate_response` runs a turn: the LLM client for the requested
provider, the MCP tools (the same five an external client sees, plus the
harness's own filesystem tools, the `eval` REPL and `ask_user`), a Postgres-
backed virtual filesystem that projects the person's uploads, library and
health profile read-only, a LangGraph checkpointer that holds the conversation
per session, and the middleware that keeps a turn bounded (model-call budget,
tool-call cap, fault containment, retry governance). Every moving part is
inspectable and self-hostable; this is what mirobody.ai runs.

There is deliberately ONE agent. A deployment that wants a different harness
replaces this class (see `registry.py`); it does not add a second one to
switch between. The MCP surface (`mirobody/mcp/`) is the seam for every other
agent runtime.
"""

import logging
import os
import uuid
from typing import Any, TYPE_CHECKING
from collections.abc import AsyncGenerator

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool

from .chat.model import UserInfo
from .registry import llm_client, llm_client_names
from ..kernel import query
from ..kernel.ops import is_driver_exception
from ..utils.log import get_req_ctx
from ..utils.config import safe_read_cfg

from . import harness
from .errors import AgentError, ConfigError, client_safe_error
from .hitl import ASK_USER_INTERRUPT, ask_user, pending_answer, widget_chunk
from .models.clients import build_llm_clients
from .models.usage import cost_statistics_message
from .prompt import attachment_reminder, build_system_prompt
from .wire.stream import StreamConverter, TokenUsageCallback
from .middleware import (
    UniversalPromptCachingMiddleware,
)

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel

logger = logging.getLogger(__name__)


# The default LLM provider when the caller names none.
#
# Both values must be KEYS of the shipped PROVIDERS in config.yaml —
# these strings are looked up in that dict, not resolved as model names. An
# earlier value, "gemini-3.5-flash", was a model name matching no shipped key
# (the entry is called "gemini-flash"), so a call with no provider raised
# ConfigError on an untouched config.
#
# Two defaults because the repo promises TWO one-key paths: "claude-sonnet"
# routes through OPENROUTER_API_KEY (the recommended default), "qwen" through
# DASHSCOPE_API_KEY (the fallback for networks where openrouter.ai is
# unreachable).
# `_default_provider()` picks by which key is actually present — the same
# select-by-available-key idea the vision pipeline already uses — so a bare
# DASHSCOPE_API_KEY deployment chats without touching DEFAULT_PROVIDER.
_DEFAULT_PROVIDER = "claude-sonnet"
_DEFAULT_PROVIDER_FALLBACK = "qwen"


def _default_provider() -> str:
    if os.environ.get("OPENROUTER_API_KEY") or safe_read_cfg("OPENROUTER_API_KEY", ""):
        return _DEFAULT_PROVIDER
    if os.environ.get("DASHSCOPE_API_KEY") or safe_read_cfg("DASHSCOPE_API_KEY", ""):
        return _DEFAULT_PROVIDER_FALLBACK
    return _DEFAULT_PROVIDER

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel

logger = logging.getLogger(__name__)


class MirobodyAgent:

    def __init__(
        self,
        user_id: str | None = None,
        user_name: str | None = None,
        token: str | None = None,
        timezone: str | None = None,
        allowed_tools: list[str] | None = None,
        disallowed_tools: list[str] | None = None,
        prompt_templates: dict[str, str] = None,
        **kwargs
    ):
        self.user_info = UserInfo(user_id=user_id, user_name=user_name or "User")
        self.token = token
        from mirobody.utils.config import get_default_timezone
        self.timezone = timezone or get_default_timezone()
        self.allowed_tools = allowed_tools
        self.disallowed_tools = disallowed_tools or []
        self.prompt_templates = prompt_templates
        # The persona name the prompt addresses the model by. Configurable so a
        # deployment can brand it; it is not an identifier anywhere else.
        self.agent_name = safe_read_cfg("AGENT_NAME") or "Mirobody"
        self.default_provider = safe_read_cfg("DEFAULT_PROVIDER") or _default_provider()
        self.file_parse_cache_ttl = int(safe_read_cfg("FILE_CACHE_TTL") or 300)
        self.file_parse_cache_maxsize = int(safe_read_cfg("FILE_CACHE_MAXSIZE") or 100)
        # Two layers, and they are not interchangeable (see `_build_agent`):
        #
        # MODEL_CALL_LIMIT is the real budget, counted in agent iterations (= model
        # calls, one per tool round) and enforced by ModelCallLimitMiddleware, which
        # ends the run gracefully so the model still writes a final answer. This is
        # what "N rounds" should mean, and it is immune to how many middleware nodes
        # run per cycle.
        #
        # RECURSION_LIMIT is a raw LangGraph super-step ceiling kept only as a
        # last-resort net for a true runaway. It must sit WELL ABOVE the model-call
        # budget or it fires first and hard-fails with GraphRecursionError instead of
        # degrading: every built-in middleware compiles its after_model hook as its
        # own graph node, so one tool round costs several super-steps. Default it to
        # ~6x the round budget so the graceful limit always wins.
        self.model_call_limit = int(safe_read_cfg("MODEL_CALL_LIMIT") or 50)
        self.recursion_limit = int(
            safe_read_cfg("RECURSION_LIMIT") or max(100, self.model_call_limit * 6)
        )

    async def _init_llm_client(self, provider: str | Any | None) -> tuple[Any, str, bool, str]:
        original_provider = provider
        fallback_used = False
        fallback_message = ""

        if provider:
            agent_llm_client = llm_client(provider) if isinstance(provider, str) else provider
        else:
            agent_llm_client = llm_client(self.default_provider)

        # Fallback to default provider if the requested one is not supported
        if not agent_llm_client:
            default_provider = self.default_provider
            logger.warning(f"Provider '{original_provider}' not supported, falling back to '{default_provider}'")
            agent_llm_client = llm_client(default_provider)

            if agent_llm_client:
                fallback_used = True
                fallback_message = f"Provider '{original_provider}' not configured. Using default '{default_provider}'.\n"
            else:
                available = llm_client_names()
                available_str = ", ".join(available) if available else "None"
                raise ConfigError(
                    f"Provider '{original_provider or default_provider}' is not configured. "
                    f"Available providers: {available_str}"
                )

        # Validate client (check for PlaceholderClient)
        try:
            _ = agent_llm_client.invoke
        except AttributeError as attr_error:
            logger.error(f"Provider validation failed: {attr_error}")
            raise ConfigError(f"Provider initialization failed: {attr_error}")
        
        # Extract model name
        model_name = getattr(agent_llm_client, "model_name", None) or getattr(agent_llm_client, "model", "Unknown")

        return agent_llm_client, model_name, fallback_used, fallback_message
    
    async def _load_tools(self, user_id: str, session_id: str = "") -> list:

        tools = []
        from .tool_loader import load_global_tools

        disallowed_tools = list(self.disallowed_tools)

        try:
            global_tools = await load_global_tools(
                user_id=user_id,
                token=self.token,
                session_id=session_id,
                allowed_tools=self.allowed_tools,
                disallowed_tools=disallowed_tools
            )
            tools.extend(global_tools)
            logger.info(f"Loaded {len(global_tools)} global tools")
        except Exception as e:
            logger.warning(f"Failed to load global tools: {e}")
        return tools

    def _get_base_prompt(self, prompt_name: str) -> str:
        """The agent's own system prompt: the `PROMPTS` template named by the
        request, else the first configured one.

        This used to also fetch a prompt the user had saved under the same name
        and append it. Nothing in the shipped client could save one, and a
        health agent's system prompt is not a per-user preference — the
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
        language: str,
        user_id: str,
        tools: list,
    ) -> str:
        """Build system prompt with tools, time, user context, and health-profile core."""
        from ..user.profile import get_health_profile_core
        maxlen = int(safe_read_cfg("PROFILE_CORE_MAXLEN") or 2000)
        health_profile = await get_health_profile_core(user_id, maxlen) if user_id else None
        try:
            system_prompt = await build_system_prompt(
                base_prompt=base_prompt,
                language=language,
                user_id=user_id,
                langchain_tools=tools,
                agent_name=self.agent_name,
                user_name=self.user_info.user_name,
                timezone=self.timezone,
                health_profile=health_profile,
                tool_round_limit=self.model_call_limit,
            )
            logger.info("Built system prompt successfully")
            return system_prompt
        except Exception as e:
            logger.error(f"Failed to build system prompt: {str(e)}")
            raise AgentError(
                f"System prompt construction failed: {str(e)}",
                user_message=f"Failed to build the agent's system prompt. Details: {str(e)}"
            )
    
    @staticmethod
    def _skills_source_dir() -> str:
        """The local directory holding Agent Skills, or "" when none exists.

        First existing entry of ``SKILL_DIRS`` wins; the packaged
        ``mirobody/agent/skills`` (ships in the wheel) is the fallback, so skills
        work on a bare pip install with zero configuration. Deployments list
        their own directory first to override.
        """
        from ..utils.config import global_config

        cfg = global_config()
        dirs = cfg.get_dirs("SKILL_DIRS", []) if cfg else []
        packaged = os.path.join(os.path.dirname(os.path.abspath(__file__)), "skills")
        for d in [*(dirs or []), packaged]:
            if d and os.path.isdir(d):
                return os.path.abspath(d)
        return ""

    async def _build_backend(
        self, session_id: str, user_id: str, file_list: list[dict[str, Any]] | None = None,
        supports_file_block: bool = False,
    ) -> tuple[Any, list | None]:
        """Build the deepagents virtual filesystem.

        With a ``user_id``: a ``CompositeBackend`` of read-only PROJECTIONS over
        the tables that already own the data — there is no agent-filesystem
        table (see the comment below for how that changed):

          default        → StateBackend                    scratch, rw
          /memories/...  → ProfileBackend                   health profile, ro
          /uploads/...   → ThFilesBackend(scope='uploads')  this request's files, ro
          /library/...   → ThFilesBackend(scope='library')  file history, ro
          /skills/...    → Agent Skills from SKILL_DIRS (local, ro)

        ``/uploads/`` and ``/library/`` project ``th_files`` directly (no byte
        copy; parsed text inlined for grep, bytes surfaced multimodally on
        read). ``/skills/`` is the read half of SkillsMiddleware's progressive
        disclosure: the middleware injects each skill's frontmatter at startup,
        and the agent ``read_file``s the full SKILL.md through this mount only
        when a task calls for it. Anonymous calls fall back to ``StateBackend``.
        Returns ``(backend, permissions)``.
        """
        if not user_id:
            from deepagents.backends import StateBackend
            return StateBackend(), None

        from .filesystem.files_backend import ThFilesBackend
        from .filesystem.profile_backend import ProfileBackend

        # Every mount is now either graph state or a read-only PROJECTION of the
        # table that owns the data. There is no agent-filesystem table:
        #
        #   /            the agent's scratch space — StateBackend, checkpointed by
        #                LangGraph (checkpointer.py), so it survives the turn
        #                without a table of its own
        #   /memories/   projects health_user_profile_by_system (is_deleted = false)
        #   /uploads/    projects th_files, narrowed to THIS request's file_keys
        #   /library/    projects th_files (is_del = false), the rest of the history
        #
        # The two mirroring passes that used to run here — one per turn, copying
        # path/mime/hash/text out of th_files into pointer rows — are gone. They
        # bought nothing on the read path (they queried th_files every turn
        # anyway) and cost a second home for the truth, which is how a deleted
        # health document kept answering.
        this_turn_keys = [str(f["file_key"]) for f in (file_list or [])
                          if isinstance(f, dict) and f.get("file_key")]

        # The names `attachment_reminder` announces to the model, so the mount
        # answers to exactly the paths the model was handed. Deriving them from
        # `th_files.file_name` instead would reintroduce the mid-turn rename race
        # (see `ThFilesBackend.__init__`). The `file_key` fallback mirrors the
        # reminder's (`attachment_reminder`, below) — a request that carries no
        # `file_name` must still name the file the same way at both ends.
        this_turn_names = {str(f["file_key"]): str(f.get("file_name") or f.get("file_key") or "")
                           for f in (file_list or [])
                           if isinstance(f, dict) and f.get("file_key")}

        memory = ProfileBackend(user_id=user_id)
        uploads = ThFilesBackend(user_id=user_id, scope="uploads",
                                 file_keys=this_turn_keys,
                                 turn_names=this_turn_names,
                                 supports_file_block=supports_file_block)
        library = ThFilesBackend(user_id=user_id, scope="library",
                                 file_keys=this_turn_keys,
                                 supports_file_block=supports_file_block)

        routes = {
            "/memories/": memory,
            "/uploads/": uploads,
            "/library/": library,
        }
        # Agent Skills ride the same composite: SkillsMiddleware lists them,
        # the native read_file serves their bodies from this mount. Local
        # directory, read-only — the agent must never edit its own skills.
        skills_dir = self._skills_source_dir()
        if skills_dir:
            from deepagents.backends import FilesystemBackend
            routes["/skills/"] = FilesystemBackend(root_dir=skills_dir)

        # Every mount is a PROJECTION (a table, a directory the agent must not
        # edit), so `read_only_mounts` denies writes on all of them up front —
        # a refusal the model does not have to spend a tool call to discover.
        # The scratch root is graph state, not a table: it was a
        # PgFilesystemBackend(scope='workspace') row per file; LangGraph's
        # checkpointer already persists state per thread, so the table was
        # storing what the checkpointer stores.
        return harness.read_only_mounts(routes)


    def _create_stream_config(self, user_id: str, token_counter: Any, session_id: str = "") -> dict:
        user_info = {
            "user_id": user_id,
            "token": self.token,
            "success": True
        }

        config = {
            "recursion_limit": self.recursion_limit,
            "callbacks": [token_counter],
            "configurable": {
                "user_info": user_info
            }
        }
        # `thread_id` is what the checkpointer keys conversation state on. One
        # session == one thread, so a resumed session continues its own history
        # and a new session starts clean.
        if session_id:
            config["configurable"]["thread_id"] = session_id
        return config

    async def _prepare_context(
        self,
        user_id: str,
        session_id: str,
        language: str,
        provider: str | Any | None,
        prompt_name: str,
        tools: list[BaseTool] | None = None,
    ) -> tuple["BaseChatModel", str, str | None, list[BaseTool], str]:
        """
        Prepare LLM client, tools, and system prompt.

        Returns:
            Tuple of (llm_client, model_name, fallback_msg, tools, system_prompt)
        """
        llm_client, model_name, fallback_used, fallback_msg = await self._init_llm_client(provider)

        loaded_tools = tools if tools is not None else await self._load_tools(user_id, session_id)

        base_prompt = self._get_base_prompt(prompt_name)
        system_prompt = await self._build_system_prompt(base_prompt, language, user_id, loaded_tools)

        return llm_client, model_name, (fallback_msg if fallback_used else None), loaded_tools, system_prompt

    def _supports_file_block(self, llm_client: Any) -> bool:
        """Whether the bound model accepts a native PDF content block.

        Single source of truth is LangChain's normalized ``model.profile``
        (a ``ModelProfile``, populated by the partner package from models.dev and
        **overridable per-provider in the PROVIDERS config via a ``profile:``
        merge** — see ``load_llm_clients``). This replaces any hand-maintained
        provider allow-list: capability now travels with the model.

        ``read_file`` delivers the PDF inside a ``ToolMessage``, so the precise
        capability is ``pdf_tool_message``. Fall back to ``pdf_inputs`` when the
        profile omits the tool-message datum (e.g. Gemini reports ``pdf_inputs``
        but not ``pdf_tool_message``), and to ``False`` when there is no profile
        at all (e.g. an openai-compatible qwen/deepseek endpoint whose profile is
        ``None`` — unless the config explicitly overrides it).
        """
        profile = getattr(llm_client, "profile", None) or {}
        supported = profile.get("pdf_tool_message")
        if supported is None:
            supported = profile.get("pdf_inputs")
        supported = bool(supported)
        logger.info(f"file-block support: pdf={supported} (profile_present={bool(profile)})")
        return supported

    # Native tools this agent must not offer the model, hidden via the harness
    # profile's ``excluded_tools`` (deepagents appends a ``_ToolExclusionMiddleware``
    # for it).
    #
    # ``delete``: PgFilesystemBackend deliberately does not implement it (see
    # backend.py). That alone is NOT enough to hide the tool, because the
    # capability probe runs against the mounted backend — and ``CompositeBackend``
    # DOES implement ``delete``, routing per path. So deepagents considers delete
    # supported, offers it, and every call comes back as the composite's
    # "unsupported" error after the model has already spent tokens on it. Excluding
    # it by name is what actually keeps it off the tool list.
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
    _EXCLUDED_NATIVE_TOOLS = frozenset({"delete"})

    async def _build_agent(
        self,
        session_id: str,
        user_id: str,
        llm_client: "BaseChatModel",
        system_prompt: str,
        tools: list[BaseTool],
        messages: list[dict[str, Any]] | list[BaseMessage],
        file_list: list[dict[str, Any]] | None = None,
        supports_file_block: bool = False,
    ) -> tuple[Any, Any, list]:
        """
        Build agent with backend and handle file uploads.

        Returns:
            Tuple of (agent, backend, messages)
        """
        try:
            # Build the deepagents virtual filesystem (CompositeBackend). User
            # uploads + history are auto-mounted at /uploads/ and /library/ as
            # live th_files projections — the agent reads them with the native
            # read_file tool (multimodal for pdf/image/…). No custom file MCP
            # tools, no external sandbox.
            backend, permissions = await self._build_backend(
                session_id, user_id, file_list, supports_file_block=supports_file_block
            )

            # The stack itself (fault containment → retry governance → invalid-call
            # repair → model-call budget → per-tool caps → interpreter) is
            # `harness.standard_middleware`; what this agent adds at the tail is
            # Agent Skills and cross-provider prompt caching, last so its
            # decision wins.
            from langchain_quickjs import CodeInterpreterMiddleware

            tail: list[Any] = []
            # Agent Skills (agentskills.io), deepagents-native: frontmatter is
            # injected into the system prompt at startup; the body is read
            # through the /skills/ mount only when a task needs it. Skipped for
            # anonymous sessions (StateBackend — no /skills/ mount to read from).
            if user_id and self._skills_source_dir():
                from deepagents.middleware.skills import SkillsMiddleware
                tail.append(SkillsMiddleware(backend=backend, sources=[("/skills/", "Mirobody")]))
            tail.append(UniversalPromptCachingMiddleware(ttl="5m", unsupported_model_behavior="ignore"))

            middleware = harness.standard_middleware(
                retry_limit=self._RETRY_LIMIT,
                model_call_limit=self.model_call_limit,
                # A cap on ONE tool rather than on the loop: the health-data tool
                # is the one a confused model can spin on.
                tool_call_limits={query.TOOL_NAME: self._QUERY_CALL_LIMIT},
                # In-process JS/TS REPL (`eval`). The read-only data tool is
                # exposed inside it as `tools.<name>`; PTC calls bypass the tool
                # middleware, so the data tool guards itself.
                interpreter=CodeInterpreterMiddleware(ptc=list(self._PTC_TOOLS), max_ptc_calls=self._MAX_PTC_CALLS),
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
                # Never in the MCP tool directory — an MCP client has no widget
                # to answer ask_user with.
                tools=[*tools, ask_user],
                system_prompt=system_prompt,
                backend=backend,
                permissions=permissions,
                middleware=middleware,
                checkpointer=await get_checkpointer(),
                interrupt_on=ASK_USER_INTERRUPT,
                recursion_limit=self.recursion_limit,
                excluded_native_tools=self._EXCLUDED_NATIVE_TOOLS,
            )

            logger.info(f"Agent built successfully for session: {session_id}")
            return agent, backend, messages

        except AgentError:
            raise
        except Exception as e:
            logger.error(f"Agent building failed: {str(e)}")
            raise AgentError(f"Failed to build agent: {str(e)}")
            
    async def _stream_agent_response(
        self,
        agent: Any,
        messages: list[dict[str, Any]] | list[BaseMessage],
        config: dict,
        chat_context: Any = None,
        skip_tool_names: set[str] | None = None,
        resume: str | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Stream agent response with optional tool filtering.

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

        # Track tool_ids that should be skipped (for filtering queryDetail)
        skipped_tool_ids: set[str] = set()

        try:
            # subgraphs=True surfaces subagent (subgraph) events — without it the
            # parent graph only sees a single `task` ToolMessage when the subagent
            # FINISHES, so nothing streams during a subagent run (the original bug).
            # With it, each item becomes a (namespace, stream_type, payload) triple;
            # the subagent's react subgraph reuses node names "model"/"tools", so its
            # tokens/tool-calls flow through process_stream_event into the existing
            # reply/queryTitle/queryDetail event types — no frontend change needed.
            async for stream_item in agent.astream(
                graph_input,
                context=chat_context,
                stream_mode=["messages", "updates"],
                subgraphs=True,
                config=config
            ):
                # subgraphs=True yields 3-tuples; tolerate 2-tuples defensively.
                if isinstance(stream_item, tuple) and len(stream_item) == 3:
                    namespace, stream_type, stream_event = stream_item
                else:
                    namespace, (stream_type, stream_event) = (), stream_item
                # An `ask_user` call: the middleware paused the graph after the
                # model step. Hand the question to the client as a widget and
                # end the turn; the next user message resumes this thread.
                if stream_type == "updates" and isinstance(stream_event, dict) and "__interrupt__" in stream_event:
                    widget = widget_chunk(stream_event["__interrupt__"])
                    if widget:
                        yield widget
                    return
                try:
                    async for event in StreamConverter.process_stream_event(
                        stream_type, stream_event, trace_id=trace_id, namespace=namespace
                    ):
                        if not event:
                            continue

                        # Filter specified tools and their results
                        if skip_tool_names:
                            event_type = event.get('type')
                            tool_id = event.get('tool_id', '')

                            if event_type == 'queryTitle':
                                tool_name = event.get('content', '')
                                if tool_name in skip_tool_names:
                                    if tool_id:
                                        skipped_tool_ids.add(tool_id)
                                    logger.debug(f"Skipping tool: {tool_name}, tool_id={tool_id}")
                                    continue

                            elif event_type == 'queryDetail':
                                if tool_id in skipped_tool_ids:
                                    logger.debug(f"Skipping tool result, tool_id={tool_id}")
                                    continue

                        yield event
                except Exception as e:
                    logger.error("Error processing stream chunk: error_type=%s trace_id=%s", type(e).__name__, trace_id)
                    continue

            logger.info("agent stream completed")

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
            yield {"type": "error", "content": detail}

    async def generate_response(
        self,
        user_id: str,
        messages: list[dict[str, Any]] | list[BaseMessage],
        language: str = "en",
        session_id: str = "",
        file_list: list[dict[str, Any]] | None = None,
        provider: str | Any | None = None,
        prompt_name: str = "",
        tools: list[BaseTool] | None = None,
        **kwargs
    ) -> AsyncGenerator[dict[str, Any], None]:

        if not messages:
            yield {"type": "error", "content": "Empty message"}
            return

        if not user_id:
            yield {"type": "error", "content": "User ID is required"}
            return

        logger.info(f"agent request: session={session_id}, provider={provider}, messages={len(messages)}")

        try:
            # `files_data` (the HTTP layer's pre-downloaded bytes) is deliberately
            # NOT consumed here — it arrives via **kwargs and is ignored. Uploads
            # reach the agent as FILES, not as message payload: _build_backend
            # projects them into /uploads/ by file_key (ThFilesBackend over
            # th_files, no byte copy) and the prompt tells the model to
            # read_file them on demand. Injecting the bytes into the turn would
            # duplicate that and blow up the context.

            # The attachment reminder is built AFTER the mount exists — see the
            # append below, and `attachment_reminder` for why the paths have to
            # come from the mount rather than from this request.

            llm_client, model_name, fallback_msg, loaded_tools, system_prompt = await self._prepare_context(
                user_id=user_id,
                session_id=session_id,
                language=language,
                provider=provider,
                prompt_name=prompt_name,
                tools=tools,
            )

            if fallback_msg:
                yield {"type": "thinking", "content": fallback_msg}

            supports_file_block = self._supports_file_block(llm_client)

            agent, backend, final_messages = await self._build_agent(
                session_id=session_id,
                user_id=user_id,
                llm_client=llm_client,
                system_prompt=system_prompt,
                tools=loaded_tools,
                messages=messages,
                file_list=file_list,
                supports_file_block=supports_file_block,
            )

            # Tell the model exactly which files this turn attached and where to
            # read them, so it never needs an `ls /uploads/` round-trip and never
            # silently misses one. Transient — appended to the run's messages
            # only, not the cached system prompt. Matches the list's element type
            # (BaseMessage vs dict) to avoid mixing forms.
            token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(user_id, token_counter, session_id)

            # A thread paused on `ask_user` takes this message as the answer;
            # the attachment note belongs to a NEW turn only.
            resume = await pending_answer(agent, stream_config, messages, user_id)
            if resume is None:
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

            cost = cost_statistics_message(token_counter.usage, model_name)
            if cost:
                yield cost

        except AgentError as e:
            # An AgentError's message is ours (configuration, no provider…) and
            # safe to show; anything else is reported by type only.
            logger.error("agent error: error_type=%s", type(e).__name__)
            yield {"type": "error", "content": str(e)}

        except Exception as e:
            logger.error("Unexpected error: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
            yield {"type": "error", "content": client_safe_error(e)}
    
    #-------------------------------------------------------------------------

    #-------------------------------------------------------------------------

    @classmethod
    def load_llm_clients(cls, llm_client_config: dict[str, Any]) -> dict[str, Any]:
        """The registry's hook: one chat model per `PROVIDERS` entry."""
        return build_llm_clients(llm_client_config, owner=cls.__name__)
