"""The agent: one harness, on LangChain + deepagents.

`MirobodyAgent.generate_response` runs a turn: the LLM client for the requested
provider, the four record tools an external MCP client also sees, plus the
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
import uuid
from typing import Any, TYPE_CHECKING
from collections.abc import AsyncGenerator

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool

from .registry import llm_client, llm_client_names
from mirobody.kernel import query
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils.req_ctx import get_req_ctx
from mirobody.utils.config import safe_read_cfg
from mirobody.utils.config.llm import chat_default, chat_entries

from . import harness
from .errors import AgentError, ConfigError, client_safe_error
from .hitl import ASK_USER_INTERRUPT, ask_user, interrupt_block, pending_answer
from .models.clients import build_llm_clients
from .models.usage import usage_block
from .prompt import attachment_reminder, build_system_prompt
from .wire.blocks import ERROR, NOTICE
from .wire.stream import TokenUsageCallback, stream_blocks
from .middleware import (
    GenotypeRowGuardMiddleware,
    GenotypeSafeSummarizationMiddleware,
    UniversalPromptCachingMiddleware,
)

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel

logger = logging.getLogger(__name__)


def _default_provider() -> str:
    """The model to chat with when the caller names none: the first
    `MODELS` entry (config order, utility-only entries excluded) whose key is
    present: the order of that table is the contract. With no key present,
    the first entry, so the error a chat then raises names a real entry and
    its missing key."""
    return chat_default() or next(iter(chat_entries()), "")


class MirobodyAgent:

    def __init__(
        self,
        timezone: str | None = None,
        allowed_tools: list[str] | None = None,
        disallowed_tools: list[str] | None = None,
        prompt_templates: dict[str, str] = None,
        record_owner: str = "",
        **kwargs
    ):
        self.record_owner = record_owner
        from mirobody.utils.config import get_default_timezone
        self.timezone = timezone or get_default_timezone()
        self.allowed_tools = allowed_tools
        self.disallowed_tools = disallowed_tools or []
        self.prompt_templates = prompt_templates
        # The persona name the prompt addresses the model by. Configurable so a
        # deployment can brand it; it is not an identifier anywhere else.
        self.agent_name = safe_read_cfg("AGENT_NAME") or "Mirobody"
        self.default_provider = safe_read_cfg("DEFAULT_MODEL") or _default_provider()
        # Two layers, not interchangeable (see `_build_agent`). MODEL_CALL_LIMIT
        # is the real budget, counted in model calls and enforced by
        # ModelCallLimitMiddleware, which ends the run gracefully so the model
        # still writes an answer. RECURSION_LIMIT is a raw LangGraph super-step
        # ceiling, a last-resort net for a runaway: it must sit WELL above the
        # call budget or it fires first and hard-fails with GraphRecursionError,
        # since every middleware compiles its after_model hook as its own node
        # and one tool round costs several super-steps. Default it to ~6x.
        self.model_call_limit = int(safe_read_cfg("MODEL_CALL_LIMIT") or 50)
        self.recursion_limit = int(
            safe_read_cfg("RECURSION_LIMIT") or max(100, self.model_call_limit * 6)
        )

    async def _init_llm_client(self, provider: str | None) -> tuple[Any, str, bool, str]:
        original_provider = provider
        fallback_used = False
        fallback_message = ""

        if provider:
            agent_llm_client = llm_client(provider)
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
            raise ConfigError(f"Provider initialization failed: {attr_error}") from attr_error
        
        # Extract model name
        model_name = getattr(agent_llm_client, "model_name", None) or getattr(agent_llm_client, "model", "Unknown")

        return agent_llm_client, model_name, fallback_used, fallback_message
    
    async def _load_tools(self, user_id: str) -> list:

        tools = []
        from .tool_loader import load_global_tools

        disallowed_tools = list(self.disallowed_tools)

        try:
            global_tools = await load_global_tools(
                user_id=user_id,
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
            )
            logger.info("Built system prompt successfully")
            return system_prompt
        except Exception as e:
            logger.error(f"Failed to build system prompt: {str(e)}")
            raise AgentError(
                f"System prompt construction failed: {str(e)}",
                user_message=f"Failed to build the agent's system prompt. Details: {str(e)}"
            ) from e
    
    async def _build_backend(
        self, session_id: str, user_id: str, file_list: list[dict[str, Any]] | None = None,
        supports_file_block: bool = False,
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
                                 supports_file_block=supports_file_block)
        library = ThFilesBackend(user_id=user_id, scope="library",
                                 file_keys=this_turn_keys,
                                 supports_file_block=supports_file_block)

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
        config: dict[str, Any] = {
            "recursion_limit": self.recursion_limit,
            "callbacks": [token_counter],
        }
        if session_id:
            config["configurable"] = {"thread_id": session_id}
        return config

    async def _prepare_context(
        self,
        user_id: str,
        provider: str | None,
        prompt_name: str,
    ) -> tuple["BaseChatModel", str, str | None, list[BaseTool], str]:
        """
        Prepare LLM client, tools, and system prompt.

        Returns:
            Tuple of (llm_client, model_name, fallback_msg, tools, system_prompt)
        """
        llm_client, model_name, fallback_used, fallback_msg = await self._init_llm_client(provider)

        loaded_tools = await self._load_tools(user_id)

        base_prompt = self._get_base_prompt(prompt_name)
        system_prompt = await self._build_system_prompt(base_prompt, user_id, loaded_tools)

        return llm_client, model_name, (fallback_msg if fallback_used else None), loaded_tools, system_prompt

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
    ) -> tuple[Any, Any]:
        """The compiled graph and the backend it reads through."""
        try:
            # Build the deepagents virtual filesystem (CompositeBackend). User
            # uploads + history are auto-mounted at /uploads/ and /library/ as
            # live th_files projections: the agent reads them with the native
            # read_file tool (multimodal for pdf/image/…). No custom file MCP
            # tools, no external sandbox.
            backend, permissions = await self._build_backend(
                session_id, user_id, file_list, supports_file_block=supports_file_block
            )

            # The stack itself (fault containment → retry governance → invalid-call
            # repair → model-call budget → per-tool caps → interpreter) is
            # `harness.standard_middleware`.
            from langchain_quickjs import CodeInterpreterMiddleware

            # What this agent adds at the tail: cross-provider prompt caching,
            # last so its decision wins.
            genotype_guard = GenotypeRowGuardMiddleware()
            tail: list[Any] = [
                GenotypeSafeSummarizationMiddleware(llm_client, backend, genotype_guard),
                genotype_guard,
                UniversalPromptCachingMiddleware(ttl="5m", unsupported_model_behavior="ignore")
            ]

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
                excluded_native_tools=self._EXCLUDED_NATIVE_TOOLS,
            )

            logger.info(f"Agent built successfully for session: {session_id}")
            return agent, backend

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
            # subgraphs=True surfaces subagent (subgraph) events: without it the
            # parent graph only sees a single `task` ToolMessage when the subagent
            # FINISHES, so nothing streams during a subagent run (the original bug).
            # With it, each item becomes a (namespace, stream_type, payload) triple;
            # the subagent's react subgraph reuses node names "model"/"tools", so its
            # tokens and tool calls flow through `stream_blocks` into the same
            # text/tool_call/tool_result blocks, no client change needed.
            async for stream_item in agent.astream(
                graph_input,
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
                # model step. Hand the pending question to the client and end
                # the turn; the next user message resumes this thread.
                if stream_type == "updates" and isinstance(stream_event, dict) and "__interrupt__" in stream_event:
                    pending = interrupt_block(stream_event["__interrupt__"])
                    if pending:
                        yield pending
                    return
                try:
                    async for block in stream_blocks(
                        stream_type, stream_event, trace_id=trace_id, namespace=namespace
                    ):
                        if block:
                            yield block
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
            yield {"type": ERROR, "message": detail}

    async def generate_response(
        self,
        user_id: str,
        messages: list[dict[str, Any]] | list[BaseMessage],
        session_id: str = "",
        file_list: list[dict[str, Any]] | None = None,
        provider: str | None = None,
        prompt_name: str = "",
        **kwargs
    ) -> AsyncGenerator[dict[str, Any], None]:

        if not messages:
            yield {"type": ERROR, "message": "Empty message"}
            return

        if not user_id:
            yield {"type": ERROR, "message": "User ID is required"}
            return

        logger.info(f"agent request: session={session_id}, provider={provider}, messages={len(messages)}")

        try:
            # Uploads reach the agent as FILES, never as message payload:
            # `_build_backend` projects them into /uploads/ by file_key
            # (ThFilesBackend over th_files, no byte copy) and the prompt tells
            # the model to read_file them on demand.
            llm_client, model_name, fallback_msg, loaded_tools, system_prompt = await self._prepare_context(
                user_id=user_id,
                provider=provider,
                prompt_name=prompt_name,
            )

            if fallback_msg:
                # The SYSTEM speaking, not the model. On the reasoning channel
                # it was indistinguishable from the model's own trace.
                yield {"type": NOTICE, "message": fallback_msg}

            supports_file_block = self._supports_file_block(llm_client)

            agent, backend = await self._build_agent(
                session_id=session_id,
                user_id=user_id,
                llm_client=llm_client,
                system_prompt=system_prompt,
                tools=loaded_tools,
                file_list=file_list,
                supports_file_block=supports_file_block,
            )

            token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(token_counter, session_id)

            # A thread paused on `ask_user` takes this message as the answer;
            # the attachment note belongs to a NEW turn only.
            final_messages = messages
            resume = await pending_answer(agent, stream_config, messages, user_id)
            if resume is None:
                # Name this turn's attachments and where to read them, so the
                # model never needs an `ls /uploads/` round trip and never
                # silently misses one. Transient: appended to the run's messages,
                # not to the cached system prompt. Matches the list's element
                # type (BaseMessage vs dict) rather than mixing forms.
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
            # An AgentError's message is ours (configuration, no provider…) and
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
