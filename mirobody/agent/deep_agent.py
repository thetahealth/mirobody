import logging
import os
import uuid
from typing import Any, AsyncGenerator, Optional, TYPE_CHECKING

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool
from langchain.chat_models import init_chat_model

from .chat.model import UserInfo
from .chat.agent import get_llm_client_by_name
from ..utils.log import get_req_ctx
from ..utils.config import safe_read_cfg
from ..utils.log import secret_fingerprint

from .utils import (
    StreamConverter,
    TokenUsageCallback,
    build_system_prompt,
    DeepAgentError,
    ConfigError,
)
from .deep.middleware import (
    InvalidToolCallRepairMiddleware,
    ToolFaultMiddleware,
    UniversalPromptCachingMiddleware,
)

# DeepAgent's default LLM provider when none is specified by the caller.
#
# Both values must be KEYS of the shipped PROVIDERS_DEEP in config.yaml —
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
# DASHSCOPE_API_KEY deployment chats without touching DEFAULT_PROVIDER_DEEP.
_DEFAULT_PROVIDER_DEEP = "claude-sonnet"
_DEFAULT_PROVIDER_DEEP_FALLBACK = "qwen"


def _default_provider() -> str:
    if os.environ.get("OPENROUTER_API_KEY") or safe_read_cfg("OPENROUTER_API_KEY", ""):
        return _DEFAULT_PROVIDER_DEEP
    if os.environ.get("DASHSCOPE_API_KEY") or safe_read_cfg("DASHSCOPE_API_KEY", ""):
        return _DEFAULT_PROVIDER_DEEP_FALLBACK
    return _DEFAULT_PROVIDER_DEEP

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel

logger = logging.getLogger(__name__)


# Provider-config keys consumed by us (not forwarded to ``init_chat_model``).
_NON_INIT_CONFIG_KEYS = {
    "model", "llm_type", "response_with_tools",
    "profile", "supports_pdf", "supports_image",
}


def _coerce_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def _build_profile_override(config: dict[str, Any]) -> dict[str, Any]:
    """Translate a provider config's multimodal declaration into a ``ModelProfile``
    fragment merged onto ``model.profile``.

    Friendly, documented surface (booleans on the provider entry):

        supports_pdf:   true   # model accepts a native PDF block (read_file)
        supports_image: true   # model accepts a native image block

    ``supports_pdf`` sets both ``pdf_inputs`` and ``pdf_tool_message`` (read_file
    delivers files inside a ToolMessage); ``supports_image`` likewise. An advanced
    ``profile:`` dict of raw ModelProfile fields is also honoured and takes
    precedence, for any capability the booleans don't cover. Returns ``{}`` when
    nothing is declared, so the model's native profile is used unchanged.
    """
    override: dict[str, Any] = {}
    if "supports_pdf" in config:
        flag = _coerce_flag(config["supports_pdf"])
        override["pdf_inputs"] = flag
        override["pdf_tool_message"] = flag
    if "supports_image" in config:
        flag = _coerce_flag(config["supports_image"])
        override["image_inputs"] = flag
        override["image_tool_message"] = flag
    raw = config.get("profile")
    if isinstance(raw, dict):
        override.update(raw)  # advanced escape hatch wins
    return override


class _PlaceholderClient:
    """Stand-in `init_chat_model` client used when an API key is missing.

    Holds the model name (so `getattr(client, "model_name")` works for
    diagnostics) but raises `AttributeError` with a helpful message on any
    other attribute access — including the `invoke` lookup in
    `DeepAgent._init_llm_client`.
    """

    def __init__(self, model_name: str, missing_key: str, provider_name: str):
        object.__setattr__(self, "_missing_key", missing_key)
        object.__setattr__(self, "_provider_name", provider_name)
        object.__setattr__(self, "model_name", model_name)
        object.__setattr__(self, "model", model_name)

    def __getattribute__(self, name):
        if name in ("model_name", "model", "_missing_key", "_provider_name"):
            return object.__getattribute__(self, name)
        missing_key = object.__getattribute__(self, "_missing_key")
        raise AttributeError(
            f"Missing {missing_key}. Get API key from provider and set in .env or environment"
        )


class DeepAgent():

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
        self.agent_name = "Theta"
        self.agent_identifier = self.__class__.__name__.removesuffix("Agent")
        self.default_provider = safe_read_cfg("DEFAULT_PROVIDER_DEEP") or _default_provider()
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

    async def _init_llm_client(self, provider: str | Any | None, agent_class_name: str) -> tuple[Any, str, bool, str]:
        original_provider = provider
        fallback_used = False
        fallback_message = ""

        if provider:
            agent_llm_client = get_llm_client_by_name(agent_class_name, provider) if isinstance(provider, str) else provider
        else:
            agent_llm_client = get_llm_client_by_name(agent_class_name, self.default_provider)

        # Fallback to default provider if the requested one is not supported
        if not agent_llm_client:
            default_provider = self.default_provider
            logger.warning(f"Provider '{original_provider}' not supported, falling back to '{default_provider}'")
            agent_llm_client = get_llm_client_by_name(agent_class_name, default_provider)

            if agent_llm_client:
                fallback_used = True
                fallback_message = f"Provider '{original_provider}' not configured. Using default '{default_provider}'.\n"
            else:
                from .chat.agent import global_llm_clients_for_agents
                available = list(global_llm_clients_for_agents.get(agent_class_name, {}).keys())
                available_str = ", ".join(available) if available else "None"
                raise ConfigError(
                    f"Provider '{original_provider or default_provider}' not configured for {agent_class_name}. "
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
        from .deep.tool_loader import load_global_tools

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

    # Header the user's own instructions are appended under. It names them as
    # the user's, and states the precedence rule explicitly, because the model
    # otherwise has no way to tell which half of its system prompt is the
    # product's safety framing and which half is a formatting preference
    # someone typed into a settings box.
    _USER_INSTRUCTIONS_HEADER = (
        "\n\n---\n\n"
        "## The person's own instructions\n\n"
        "The person you are helping has added the following instructions. Follow "
        "them for tone, format and emphasis. They do NOT relax anything above: "
        "the reading workflow, the citation requirement and the "
        "no-diagnosis rule still apply in full, and where the two disagree, the "
        "instructions above win.\n\n"
    )

    async def _get_base_prompt(self, user_id: str, prompt_name: str) -> str:
        """The agent's own system prompt, with the user's instructions appended.

        This used to return exactly ONE of the two: a user prompt matching
        `prompt_name` won outright, and the shipped template was consulted only
        `if not base_prompt`. So a user who saved "answer in bullet points"
        silently discarded the whole `deep` prompt — the lab-report reading
        workflow, the flag-against-printed-ranges instruction and the
        no-diagnosis framing with it — through a control that presents as a
        preference. A health agent must not lose its safety framing because
        someone set a formatting preference, so the two are composed.

        `prompt_name` is one name serving two namespaces (a shipped template and
        the user's own saved prompts). That conflation is what made the old
        either/or read as reasonable. Both lookups still run: the template
        decides the base, the user prompt is appended if one exists under that
        name.
        """
        from .chat.user_config import get_user_prompt_by_name

        # 1. The agent's own prompt — by name, else the first configured one.
        base_prompt = ""
        if self.prompt_templates:
            base_prompt = self.prompt_templates.get(prompt_name) or ""
            if base_prompt:
                logger.info(f"Using template prompt: {prompt_name}")
            else:
                for key, value in self.prompt_templates.items():
                    if value:
                        base_prompt = value
                        logger.info(f"Using fallback prompt: {key}")
                        break

        # 2. The user's own instructions, if they saved any under this name.
        # An empty name means "none requested" — asking the store for it only
        # produced a WARNING on every default chat.
        user_prompt = ""
        if prompt_name:
            user_prompt, err = await get_user_prompt_by_name(user_id, prompt_name)
            if err:
                logger.warning(f"Failed to load user prompt '{prompt_name}': {err}")
                user_prompt = ""

        if not base_prompt:
            # No template at all. A user prompt is better than refusing to
            # answer, but it is NOT the composed prompt this method promises —
            # say so, because it means the deployment shipped no template.
            if user_prompt:
                logger.error(
                    f"No prompt template for '{prompt_name}'; running on the user's "
                    "instructions ALONE — the agent's own prompt is missing from "
                    "this deployment (check PROMPTS_<AGENT> in config)."
                )
                return user_prompt
            raise DeepAgentError(f"No prompt template found for '{prompt_name}' and no fallback available")

        if user_prompt:
            logger.info(f"Appending user instructions: {prompt_name}")
            return base_prompt + self._USER_INSTRUCTIONS_HEADER + user_prompt

        return base_prompt


    async def _build_system_prompt(
        self,
        base_prompt: str,
        language: str,
        user_id: str,
        tools: list,
    ) -> str:
        """Build system prompt with tools, time, user context, and health-profile core."""
        from .chat.user_profile import get_health_profile_core
        maxlen = int(safe_read_cfg("DEEP_PROFILE_CORE_MAXLEN") or 2000)
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
            )
            logger.info("Built system prompt successfully")
            return system_prompt
        except Exception as e:
            logger.error(f"Failed to build system prompt: {str(e)}")
            raise DeepAgentError(
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

        from deepagents.backends import CompositeBackend, StateBackend
        from deepagents.middleware.filesystem import FilesystemPermission
        from .deep.files_backend import ThFilesBackend
        from .deep.profile_backend import ProfileBackend

        # Every mount is now either graph state or a read-only PROJECTION of the
        # table that owns the data. There is no agent-filesystem table:
        #
        #   /            the agent's scratch space — StateBackend, checkpointed by
        #                LangGraph (deep/checkpointer.py), so it survives the turn
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

        # The names `_attachment_reminder` announces to the model, so the mount
        # answers to exactly the paths the model was handed. Deriving them from
        # `th_files.file_name` instead would reintroduce the mid-turn rename race
        # (see `ThFilesBackend.__init__`). The `file_key` fallback mirrors the
        # reminder's (`_attachment_reminder`, below) — a request that carries no
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
        permissions = [
            FilesystemPermission(operations=["write"], paths=["/uploads/**"], mode="deny"),
            FilesystemPermission(operations=["write"], paths=["/library/**"], mode="deny"),
        ]

        # Agent Skills ride the same composite: SkillsMiddleware lists them,
        # the native read_file serves their bodies from this mount. Local
        # directory, read-only — the agent must never edit its own skills.
        skills_dir = self._skills_source_dir()
        if skills_dir:
            from deepagents.backends import FilesystemBackend
            routes["/skills/"] = FilesystemBackend(root_dir=skills_dir)
            permissions.append(
                FilesystemPermission(operations=["write"], paths=["/skills/**"], mode="deny")
            )

        # The scratch root is graph state, not a table. It was a
        # PgFilesystemBackend(scope='workspace') row per file; LangGraph's
        # checkpointer already persists state per thread, so the table was
        # storing what the checkpointer stores.
        backend = CompositeBackend(default=StateBackend(), routes=routes)
        return backend, permissions


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
        llm_client, model_name, fallback_used, fallback_msg = await self._init_llm_client(provider, self.agent_identifier)

        loaded_tools = tools if tools is not None else await self._load_tools(user_id, session_id)

        base_prompt = await self._get_base_prompt(user_id, prompt_name)
        system_prompt = await self._build_system_prompt(base_prompt, language, user_id, loaded_tools)

        return llm_client, model_name, (fallback_msg if fallback_used else None), loaded_tools, system_prompt

    @staticmethod
    def _harness_provider_key(client: Any) -> str | None:
        """The provider key deepagents will ACTUALLY look up for this model.

        Ask deepagents itself (``_models.get_model_provider``) rather than reading
        ``client._get_ls_params()["ls_provider"]`` directly. The two disagree, and
        the disagreement is silent: a chat-model class that does not override
        ``_get_ls_params`` falls back to langchain-core's CLASS-NAME derivation, so
        ``ChatAnthropicVertex`` resolves to ``anthropicvertex`` — not the
        ``llm_type`` string our config calls it. Registering the harness profile
        under the wrong key is a no-op that leaves the general-purpose subagent
        ENABLED, and the model then happily calls ``task``.

        This exact silent failure already happened in production once
        (2026-07-28) and looked fine in test only because the default there is an
        OpenAI-family model, and ``ChatOpenAI`` does override ``ls_provider``.

        Best-effort: falls back to ``_get_ls_params`` and then to None.
        """
        try:
            from deepagents._models import get_model_provider
            key = get_model_provider(client)
            if key:
                return key
        except Exception:
            logger.warning("could not derive harness provider key from deepagents", exc_info=True)
        try:
            return (client._get_ls_params() or {}).get("ls_provider")
        except Exception:
            return None

    def _supports_file_block(self, llm_client: Any) -> bool:
        """Whether the bound model accepts a native PDF content block.

        Single source of truth is LangChain's normalized ``model.profile``
        (a ``ModelProfile``, populated by the partner package from models.dev and
        **overridable per-provider in the PROVIDERS_* config via a ``profile:``
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
    # deep/backend.py). That alone is NOT enough to hide the tool, because the
    # capability probe runs against the mounted backend — and ``CompositeBackend``
    # DOES implement ``delete``, routing per path. So deepagents considers delete
    # supported, offers it, and every call comes back as the composite's
    # "unsupported" error after the model has already spent tokens on it. Excluding
    # it by name is what actually keeps it off the tool list.
    _EXCLUDED_NATIVE_TOOLS = frozenset({"delete"})

    def _apply_harness_profile(self, llm_client: "BaseChatModel") -> None:
        """Register the harness profile that shapes this agent's native tool set.

        Two effects, both wanted unconditionally:

        1. **No ``task`` subagent.** ``task`` here is only ever a no-op self-clone
           of the same agent, and streaming a subagent run was the source of the
           "no events until the subagent finishes" stall. There is no
           ``create_deep_agent`` kwarg for this — deepagents' supported
           no-subagent configuration is BOTH halves of: a profile with
           ``general_purpose_subagent(enabled=False)`` registered under the key
           deepagents resolves for THIS model, AND no synchronous ``subagents``
           passed to ``create_deep_agent`` (``_build_agent`` passes ``[]``).
           Either half alone leaves ``task`` reachable.

           This used to be conditional on ``"task" in disallowed_tools``, which
           silently never fired: ``DISALLOWED_TOOLS_DEEP`` is empty in config.yaml,
           so the subagent was in fact ENABLED in production. Now unconditional.

        2. **No ``delete`` tool** — see ``_EXCLUDED_NATIVE_TOOLS``.

        ``register_harness_profile`` is an idempotent field-wise merge, so
        re-registering per build is deterministic last-write-wins. Registration is
        GLOBAL per provider key, which is fine now that every DeepAgent build wants
        the same answer.
        """
        provider = self._harness_provider_key(llm_client)
        if not provider:
            logger.warning(
                "Cannot derive harness provider key from model; the general-purpose "
                "'task' subagent may stay enabled for this build"
            )
            return
        try:
            from deepagents import (
                GeneralPurposeSubagentProfile,
                HarnessProfile,
                register_harness_profile,
            )
            register_harness_profile(
                provider,
                HarnessProfile(
                    general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
                    excluded_tools=self._EXCLUDED_NATIVE_TOOLS,
                ),
            )
            logger.info(
                f"harness profile for '{provider}': 'task' subagent disabled, "
                f"excluded tools={sorted(self._EXCLUDED_NATIVE_TOOLS)}"
            )
        except Exception as exc:
            logger.warning(f"failed to apply harness profile: {exc}")

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

            from deepagents import create_deep_agent

            # Order matters — user middleware is spliced into the middle of
            # deepagents' own stack in the order given, outermost first:
            #   1. ToolFaultMiddleware   — outermost, so it contains faults from
            #      every tool AND from the wrappers below it.
            #   2. InvalidToolCallRepairMiddleware — a call whose JSON never
            #      parsed reaches no tool at all; this feeds the parse error
            #      back and retries instead of ending the turn empty.
            #   3. ModelCallLimitMiddleware — the real per-turn budget.
            #   4. CodeInterpreterMiddleware — the `eval` REPL.
            #   5. SkillsMiddleware — Agent Skills frontmatter into the prompt.
            #   6. UniversalPromptCachingMiddleware — last so its decision wins.
            middleware: list[Any] = [ToolFaultMiddleware(), InvalidToolCallRepairMiddleware()]

            # Bound the loop by REAL tool rounds (model calls) rather than by raw
            # LangGraph super-steps. `exit_behavior="end"` ENDS the run gracefully,
            # so the model still gets to write a final answer: a legitimate
            # multi-step task (read a big PDF in chunks, then analyze) runs to
            # completion while a pathological tool-loop still terminates. The raw
            # `recursion_limit` below stays only as a last-resort runaway net, and
            # must sit well above this budget — one round costs several super-steps
            # because each built-in middleware compiles its own graph node.
            try:
                from langchain.agents.middleware import ModelCallLimitMiddleware
                middleware.append(
                    ModelCallLimitMiddleware(run_limit=self.model_call_limit, exit_behavior="end")
                )
            except Exception as exc:
                logger.warning(f"model call limit middleware unavailable: {exc}")

            # In-process JS/TS interpreter (langchain-quickjs). Adds an `eval`
            # tool — a persistent REPL — for compute, in-process with no
            # external sandbox service or key.
            try:
                from langchain_quickjs import CodeInterpreterMiddleware
                middleware.append(CodeInterpreterMiddleware())
            except Exception as exc:
                logger.warning(f"code interpreter middleware unavailable: {exc}")

            # Agent Skills (agentskills.io), deepagents-native: frontmatter is
            # injected into the system prompt at startup; the body is read
            # through the /skills/ mount only when a task needs it. Skipped for
            # anonymous sessions (StateBackend — no /skills/ mount to read from).
            if user_id and self._skills_source_dir():
                try:
                    from deepagents.middleware.skills import SkillsMiddleware
                    middleware.append(
                        SkillsMiddleware(backend=backend, sources=[("/skills/", "Mirobody")])
                    )
                except Exception as exc:
                    logger.warning(f"skills middleware unavailable: {exc}")

            middleware.append(
                UniversalPromptCachingMiddleware(ttl="5m", unsupported_model_behavior="ignore")
            )

            # Must run BEFORE create_deep_agent: that is where the harness profile
            # (and so the subagent / excluded-tool decisions) is resolved.
            self._apply_harness_profile(llm_client)

            agent_kwargs: dict[str, Any] = dict(
                model=llm_client,
                tools=tools,
                system_prompt=system_prompt,
                backend=backend,
                middleware=middleware,
                # The other half of the no-subagent configuration (see
                # `_disable_task_subagent`): the harness profile suppresses the
                # auto-added general-purpose subagent, and passing an explicit
                # empty list guarantees we never hand it one of our own.
                subagents=[],
            )
            if permissions is not None:
                agent_kwargs["permissions"] = permissions

            # Conversation memory. With a checkpointer, LangGraph holds the real
            # message objects per `thread_id` (= session_id) and the caller hands
            # in ONLY the new turn — which is why there is no longer a module
            # rebuilding history out of persisted UI chunks. None (dependency or
            # DB unavailable) degrades to a stateless turn rather than a failure;
            # see deep/checkpointer.py.
            from .deep.checkpointer import get_checkpointer
            checkpointer = await get_checkpointer()
            if checkpointer is not None:
                agent_kwargs["checkpointer"] = checkpointer

            agent = create_deep_agent(**agent_kwargs).with_config(
                {"recursion_limit": self.recursion_limit}
            )

            logger.info(f"Agent built successfully for session: {session_id}")
            return agent, backend, messages

        except DeepAgentError:
            raise
        except Exception as e:
            logger.error(f"Agent building failed: {str(e)}")
            raise DeepAgentError(f"Failed to build agent: {str(e)}")
            
    async def _stream_agent_response(
        self,
        agent: Any,
        messages: list[dict[str, Any]] | list[BaseMessage],
        config: dict,
        chat_context: Any = None,
        skip_tool_names: set[str] | None = None,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Stream agent response with optional tool filtering.
        """
        logger.info("Starting DeepAgent stream")
        trace_id = get_req_ctx("trace_id") or str(uuid.uuid4())

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
                {"messages": messages},
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
                    logger.error(f"Error processing stream chunk: {str(e)}, trace_id={trace_id}")
                    continue

            logger.info("DeepAgent stream completed")

        except Exception as e:
            logger.error(f"DeepAgent streaming error: {str(e)}", stack_info=True)
            detail = str(e)
            # A connect failure names neither the unreachable host nor the way
            # out. Connection-shaped errors get the pointer the docs already
            # carry: openrouter.ai is unreachable from some networks, and the
            # DashScope gateway is the documented drop-in fallback.
            if "connect" in f"{type(e).__name__} {detail}".lower():
                detail += (
                    " — the model provider's endpoint may be unreachable from "
                    "this network; see the DashScope fallback in config.yaml"
                )
            yield {"type": "error", "content": f"Streaming error: {detail}"}

    async def generate_response(
        self,
        user_id: str,
        messages: list[dict[str, Any]] | list[BaseMessage],
        language: str = "en",
        session_id: str = "",
        file_list: list[dict[str, Any]] | None = None,
        provider: str | Any | None = None,
        prompt_name: str = "",
        tools: Optional[list[BaseTool]] = None,
        **kwargs
    ) -> AsyncGenerator[dict[str, Any], None]:

        if not messages:
            yield {"type": "error", "content": "Empty message"}
            return

        if not user_id:
            yield {"type": "error", "content": "User ID is required"}
            return

        logger.info(f"DeepAgent request: session={session_id}, provider={provider}, messages={len(messages)}")

        try:
            # `files_data` (the HTTP layer's pre-downloaded bytes) is deliberately
            # NOT consumed here — it arrives via **kwargs and is ignored. Uploads
            # reach the agent as FILES, not as message payload: _build_backend
            # projects them into /uploads/ by file_key (ThFilesBackend over
            # th_files, no byte copy) and the prompt tells the model to
            # read_file them on demand. Injecting the bytes into the turn would
            # duplicate that and blow up the context. BaseAgent still uses
            # files_data (it has no virtual FS).

            # Tell the model exactly which files were attached this turn (and
            # their /uploads/ paths) so it reads them without an ls round-trip and
            # never misses one. Transient — appended to the run's messages only,
            # not the cached system prompt. Matches the incoming list's element
            # type (BaseMessage vs dict) to avoid mixing forms.
            reminder = _attachment_reminder(file_list)
            if reminder:
                if messages and isinstance(messages[-1], BaseMessage):
                    from langchain_core.messages import HumanMessage
                    messages = [*messages, HumanMessage(content=reminder)]
                else:
                    messages = [*messages, {"role": "user", "content": reminder}]

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

            token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(user_id, token_counter, session_id)

            async for event in self._stream_agent_response(
                agent=agent,
                messages=final_messages,
                config=stream_config,
            ):
                yield event

            if token_counter.total_input_tokens > 0 or token_counter.total_output_tokens > 0:
                yield StreamConverter.create_cost_statistics(
                    token_counter.total_input_tokens,
                    token_counter.total_output_tokens,
                    model_name,
                    cache_read_tokens=token_counter.cache_read_tokens,
                    cache_creation_tokens=token_counter.cache_creation_tokens,
                )

        except (DeepAgentError, ValueError) as e:
            logger.error(f"DeepAgent error: {e}")
            yield {"type": "error", "content": str(e)}

        except Exception as e:
            logger.error(f"Unexpected error: {e}", stack_info=True)
            yield {"type": "error", "content": f"Unexpected error: {e}\n\nCheck logs for details."}
    
    #-------------------------------------------------------------------------
    
    @classmethod
    def load_llm_clients(cls, llm_client_config: dict[str, Any]) -> dict[str, Any]:
        class_name = cls.__name__

        if not llm_client_config or len(llm_client_config) == 0:
            logger.warning(f"No LLM providers configured for {class_name}")
            return {}

        llm_clients = {}
        failed = []

        for provider_name, provider_kwargs in llm_client_config.items():
            if not provider_kwargs or not isinstance(provider_kwargs, dict):
                logger.warning(f"[{class_name}] Invalid config for '{provider_name}': not a dictionary")
                failed.append((provider_name, "Invalid format"))
                continue

            try:
                config = dict(provider_kwargs)
                model = config.get("model", "unknown")
                llm_type = config.get("llm_type", "openai")

                # Log config (sanitized)
                safe_config = {k: v for k, v in config.items() if k != "api_key"}
                logger.info(f"[{class_name}] Loading '{provider_name}': {safe_config}")

                if not model or model == "unknown":
                    logger.warning(f"[{class_name}] Skipped '{provider_name}': missing 'model' field")
                    failed.append((provider_name, "Missing 'model'"))
                    continue

                # Resolve project field if present
                project_name = config.get("project")
                if project_name and isinstance(project_name, str):
                    actual_project = os.environ.get(project_name) or safe_read_cfg(project_name)
                    if actual_project:
                        config["project"] = actual_project

                # Resolve base_url from environment variable
                base_url_ref = config.get("base_url")
                if base_url_ref and isinstance(base_url_ref, str):
                    actual_base_url = os.environ.get(base_url_ref) or safe_read_cfg(base_url_ref)
                    if actual_base_url:
                        config["base_url"] = actual_base_url

                # Handle api_key resolution
                api_key_name = config.get("api_key")
                if api_key_name and isinstance(api_key_name, str):
                    actual_api_key = os.environ.get(api_key_name) or safe_read_cfg(api_key_name)

                    if actual_api_key:
                        config["api_key"] = actual_api_key
                    else:
                        logger.warning(f"[{class_name}] API key '{api_key_name}' not found - creating placeholder for '{provider_name}'")
                        llm_clients[provider_name] = _PlaceholderClient(model, api_key_name, provider_name)
                        continue
                else:
                    logger.info(f"[{class_name}] No api_key in config for '{provider_name}' - using default auth (ADC/environment)")

                # Handle Azure WIF auth: use token_provider as api_key, build v1 base_url
                auth_type = config.pop("auth_type", None)
                if auth_type == "azure_wif":
                    try:
                        from azure.identity import get_bearer_token_provider
                        if os.environ.get("AZURE_FEDERATED_TOKEN_FILE"):
                            from azure.identity import WorkloadIdentityCredential
                            credential = WorkloadIdentityCredential(
                                tenant_id=os.environ["AZURE_TENANT_ID"],
                                client_id=os.environ["AZURE_CLIENT_ID"],
                                token_file_path=os.environ["AZURE_FEDERATED_TOKEN_FILE"],
                            )
                        token_provider = get_bearer_token_provider(
                            credential, "https://cognitiveservices.azure.com/.default"
                        )
                        config["api_key"] = token_provider
                        # Build v1 base_url: {endpoint}/openai/v1/
                        base_url = config.get("base_url", "")
                        if base_url and "/openai/v1" not in base_url:
                            config["base_url"] = f"{base_url.rstrip('/')}/openai/v1/"
                        logger.info(f"[{class_name}] Azure WIF (v1 endpoint) injected for '{provider_name}'")
                    except Exception as e:
                        logger.error(f"[{class_name}] Azure WIF auth failed for '{provider_name}': {e}")
                        failed.append((provider_name, f"Azure auth: {e}"))
                        continue

                # Optional per-provider multimodal capability declaration. See
                # _build_profile_override: friendly booleans (supports_pdf /
                # supports_image) are the documented surface; they fill the
                # model's normalized LangChain ``profile`` so PDF/image rendering
                # works on providers whose profile is unknown (e.g. any
                # OpenAI-compatible endpoint, where the profile is None).
                profile_override = _build_profile_override(config)

                # Build init_chat_model kwargs
                model_provider = llm_type
                init_kwargs = {k: v for k, v in config.items() if k not in _NON_INIT_CONFIG_KEYS}

                # Log call parameters — with the key FINGERPRINTED, never raw.
                # This line used to print init_kwargs verbatim, which put the
                # live api_key in cleartext at INFO once per provider on every
                # boot — straight into `docker compose logs`, `> server.log`,
                # any log shipper. Found by a first-run reviewer tailing the
                # log to check startup health. Same discipline as
                # secret_fingerprint() everywhere else: identity, not value.
                loggable_kwargs = {
                    k: (secret_fingerprint(v) if k == "api_key" and isinstance(v, str) else v)
                    for k, v in init_kwargs.items()
                }
                logger.info(f"[{class_name}] Calling init_chat_model('{provider_name}'): model={model}, provider={model_provider}, kwargs={loggable_kwargs}")

                try:
                    client = init_chat_model(model=model, model_provider=model_provider, **init_kwargs)
                    if profile_override:
                        try:
                            client.profile = {**(getattr(client, "profile", None) or {}), **profile_override}
                            logger.info(f"[{class_name}] '{provider_name}' capability override: {profile_override}")
                        except Exception as prof_err:
                            logger.warning(f"[{class_name}] could not apply capability override for '{provider_name}': {prof_err}")
                    llm_clients[provider_name] = client
                    logger.info(f"[{class_name}] ✓ Initialized '{provider_name}': {model_provider}/{model}")
                except Exception as e:
                    logger.error(f"[{class_name}] ✗ Failed '{provider_name}': provider={model_provider}, model={model}, error={type(e).__name__}: {e}", exc_info=True)
                    failed.append((provider_name, f"{type(e).__name__}: {str(e)}"))
            except Exception as outer_e:
                logger.error(f"[{class_name}] Unexpected error for '{provider_name}': {outer_e}", exc_info=True)
                failed.append((provider_name, f"Unexpected: {str(outer_e)}"))

        # Summary
        loaded = len(llm_clients)
        total = len(llm_client_config)

        if loaded > 0:
            # "Loaded 5/5" on a zero-key deployment read as five USABLE
            # providers; count the placeholders so the summary cannot.
            keyless = [
                name for name in llm_clients
                if (llm_client_config.get(name) or {}).get("api_key")
                and not safe_read_cfg((llm_client_config.get(name) or {}).get("api_key"))
            ]
            note = (
                f" ({len(keyless)} placeholder{'s' if len(keyless) != 1 else ''}"
                " — no usable key)" if keyless else ""
            )
            logger.info(f"[{class_name}] Loaded {loaded}/{total} providers{note}: {', '.join(llm_clients.keys())}")

        if failed:
            for name, reason in failed:
                logger.warning(f"[{class_name}] Failed '{name}': {reason}")

        if loaded == 0:
            logger.warning(f"[{class_name}] No providers loaded (0/{total}) - agent may be disabled intentionally")

        return llm_clients


def _attachment_reminder(file_list: list[dict[str, Any]] | None) -> str | None:
    """A short note naming the files attached to THIS turn and their /uploads/
    paths, so the model reads them without first having to ``ls /uploads/`` (and
    never silently misses an attachment). Returns None when nothing is attached.

    Injected as a transient message (NOT the system prompt — that is cached and
    must stay stable across turns). Ephemeral: persistence saves the user
    question + assistant reply separately, not this note.
    """
    items = [f for f in (file_list or []) if isinstance(f, dict) and f.get("file_key")]
    paths: list[str] = []
    from .deep.files_backend import _MAX_SESSION_FILES, safe_basename

    for f in items[:_MAX_SESSION_FILES]:
        name = safe_basename(f.get("file_name") or str(f.get("file_key")))
        if name:
            paths.append(f"/uploads/{name}")
    if not paths:
        return None
    listing = "\n".join(f"- {p}" for p in paths)
    return (
        "[System note: the user attached the following file(s) to THIS message. "
        "Read the relevant one(s) with read_file before answering:\n"
        f"{listing}]"
    )


