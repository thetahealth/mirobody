import logging
from typing import Any, AsyncGenerator, Optional

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool

from .utils import StreamConverter, TokenUsageCallback
from .deep_agent import DeepAgent
from .mix import MixMixin

logger = logging.getLogger(__name__)


class MixAgent(DeepAgent, MixMixin):
    """
    Two-phase model fusion Agent.

    Features:
    - Static tool loading (inherits from DeepAgent)
    - Phase 1 (Orchestrator) collects AND processes data via tools
      (gathering + computing/shaping it into clean, chart-ready form)
    - Phase 2 (Responder) presents the result: prose reply + charts
    - Reuses DeepAgent's core methods

    Phase 1 (orchestrator) runs all tools and never writes the reply;
    Phase 2 (responder) is pure presentation/generation — it draws the
    charts and composes the answer, and is never bound to tools.

    Class attributes:
    - _all_llm_clients: Internal storage for all LLM clients (including @orchestrator/@responder)
    """

    _all_llm_clients: dict[str, dict[str, Any]] = {}  # {agent_name: {provider_name: client}}
    _group_responder_map: dict[str, dict[str, list[str]]] = {}  # {agent_name: {group_name: [responder_names]}}

    def __init__(
        self,
        user_id: str | None = None,
        user_name: str | None = None,
        token: str | None = None,
        timezone: str | None = None,
        prompt_templates: dict[str, str] | None = None,
        prompt_dir: str | None = None,
        **kwargs
    ):
        super().__init__(
            user_id=user_id,
            user_name=user_name,
            token=token,
            timezone=timezone,
            prompt_templates=prompt_templates,
            **kwargs
        )
        # `agent_identifier` ("Mix") is set by DeepAgent.__init__ from the
        # class name; no need to re-derive here.
        self.prompt_dir = prompt_dir  # custom prompt directory (subclass override)

        responder_providers, responder_configs = self._extract_responder_providers()
        group_responder_map = MixAgent._group_responder_map.get(self.agent_identifier, {})

        self._init_mix_mixin(
            responder_providers=responder_providers,
            responder_configs=responder_configs,
            prompt_templates=self.prompt_templates,
            group_responder_map=group_responder_map,
        )

        logger.info(f"MixAgent initialized for user: {user_id}")

    def _extract_responder_providers(self) -> tuple[dict[str, Any], dict[str, dict]]:
        """
        Extract @responder providers from internal LLM clients storage.

        Extracts response_with_tools configuration for each responder:
        - If response_with_tools is explicitly set (true/false), use that value
        - If not set, default to None (responder used for all cases)

        Returns:
            Tuple of (providers dict, configs dict)
        """
        from ...utils import global_config

        providers = {}
        configs = {}

        # Get all LLM clients from internal storage (includes @orchestrator/@responder)
        agent_name = self.agent_identifier
        llm_clients = MixAgent._all_llm_clients.get(agent_name, {})

        # Get original config to extract response_with_tools field
        config = global_config()
        agent_config = config.get_options_for_agent(agent_name) if config else {}
        providers_config = agent_config.get("providers", {}) if agent_config else {}

        # Flatten nested config if needed
        flattened_providers_config = self._flatten_provider_config(providers_config)

        for provider_name, client in llm_clients.items():
            if "@responder" in provider_name:
                providers[provider_name] = client
                # Get config from the flattened provider config
                original_config = flattened_providers_config.get(provider_name, {})

                # Extract response_with_tools - None if not specified (flexible mode)
                response_with_tools = original_config.get("response_with_tools")

                configs[provider_name] = {
                    "model": original_config.get("model") or getattr(client, "model_name", None) or getattr(client, "model", provider_name),
                    "response_with_tools": response_with_tools,  # Can be True, False, or None
                }

        logger.info(f"Extracted {len(providers)} @responder providers: {list(providers.keys())}")
        return providers, configs

    def _create_phase1_agent(
        self, llm_client: Any, system_prompt: str,
        backend: Any, tools: list[BaseTool], permissions: list | None = None,
    ) -> Any:
        """Create Phase 1 (orchestrator) Agent.

        Built on upstream `deepagents.create_deep_agent` so Phase 1 inherits
        the full deepagents stack (Todo + Filesystem + SubAgent + Summarization
        + caching + PatchToolCalls) plus our `UniversalPromptCachingMiddleware`.

        We wrap the schema in an explicit `ToolStrategy(OrchestratorManifest)`
        rather than passing the bare schema. This is deliberate and load-bearing
        (see `langchain/agents/factory.py` ~lines 1219-1277):

        - A bare schema is treated as `AutoStrategy`: for models whose profile
          declares native structured-output support (OpenAI GPT-5, Anthropic
          Claude, Gemini 3.x with tools) LangChain picks `ProviderStrategy` and
          binds tools WITHOUT `tool_choice` — so the model is free to emit
          free text or jump straight to the manifest without collecting data.
        - An explicit `ToolStrategy` is preserved as-is for every model, and
          because the manifest registers as a structured-output tool LangChain
          forces `tool_choice="any"` (factory.py:1271). Every Phase 1 turn must
          therefore emit a tool call — either a real data-collection tool or the
          manifest. Uniform across Claude / Gemini / OpenAI; no per-model branch.

        Phase 1 thus never wanders into free-text answers, and the loop ends
        when LangChain parses the manifest tool call into
        `state.structured_response` (which `mixin._stream_agent` watches for).
        A model that needs no data can emit the manifest on turn 1
        (`has_tools=False`) — this implicitly handles intent classification:
        no separate router is needed, and Phase 2 then replies as pure chat.
        """
        from deepagents import create_deep_agent
        from langchain.agents.structured_output import ToolStrategy
        from .deep.middleware import UniversalPromptCachingMiddleware
        from .mix.models import OrchestratorManifest

        middleware: list[Any] = []
        try:
            from langchain_quickjs import CodeInterpreterMiddleware
            middleware.append(CodeInterpreterMiddleware())
        except Exception as exc:
            logger.warning(f"code interpreter middleware unavailable: {exc}")
        middleware.append(
            UniversalPromptCachingMiddleware(ttl="5m", unsupported_model_behavior="ignore")
        )

        kwargs: dict[str, Any] = dict(
            model=llm_client,
            tools=tools,
            system_prompt=system_prompt,
            backend=backend,
            response_format=ToolStrategy(OrchestratorManifest),
            middleware=middleware,
        )
        if permissions is not None:
            kwargs["permissions"] = permissions
        return create_deep_agent(**kwargs).with_config({"recursion_limit": self.recursion_limit})

    # === Main Entry Point ===

    async def generate_response(
        self,
        user_id: str,
        messages: list[dict[str, Any]] | list[BaseMessage],
        language: str = "en",
        session_id: str = "",
        file_list: list[dict[str, Any]] | None = None,
        files_data: list[dict[str, Any]] | None = None,
        provider: str | Any | None = None,
        prompt_name: str = "",
        tools: Optional[list[BaseTool]] = None,
        chat_context: Any = None,
        **kwargs
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Generate response: Phase 1 (Orchestrator) collects → Phase 2 (Responder) composes."""
        if not messages:
            yield {"type": "error", "content": "Empty message"}
            return
        if not user_id:
            yield {"type": "error", "content": "User ID required"}
            return

        logger.info(f"MixAgent request: session={session_id}, messages={len(messages)}")

        try:
            # === Initialize LLM (reuse parent method) ===
            llm_client, model_name, _, fallback_msg = await self._init_llm_client(
                provider, self.agent_identifier
            )
            if fallback_msg:
                yield {"type": "thinking", "content": fallback_msg}

            # === Build Backend (CompositeBackend; auto-mounts /uploads + /library) ===
            backend, permissions = await self._build_backend(session_id, user_id, file_list)

            # === Load tools (static, reuse parent method) ===
            loaded_tools = tools if tools is not None else await self._load_tools(user_id, session_id)

            # === Build Phase 1 prompt (use "orchestrator" key by default) ===
            phase1_prompt_name = prompt_name or "orchestrator"
            base_prompt = await self._get_base_prompt(user_id, phase1_prompt_name)
            system_prompt = await self._build_system_prompt(base_prompt, language, user_id, loaded_tools)

            # === Create Phase 1 Agent ===
            phase1_agent = self._create_phase1_agent(llm_client, system_prompt, backend, loaded_tools, permissions)

            # === Phase 1: Data Collection ===
            logger.info("Phase 1: Data Collection")
            phase1_tokens = TokenUsageCallback()
            phase1_config = self._create_stream_config(user_id, phase1_tokens)

            collected_messages: list[BaseMessage] = []
            has_tools = False
            manifest_note = ""
            manifest_rounds = 0

            async for event in self._stream_agent(
                phase1_agent, messages, phase1_config,
                chat_context=chat_context,
                collect_tool_context=True,
            ):
                if event.get("type") == "_metadata":
                    collected_messages = event.get("collected_messages", [])
                    has_tools = event.get("has_tools", False)
                    manifest_note = event.get("manifest_note", "")
                    manifest_rounds = event.get("manifest_rounds", 0)
                    continue

                yield event

            # Phase 1 cost statistics
            phase1_cost = StreamConverter.create_cost_statistics(
                phase1_tokens.total_input_tokens,
                phase1_tokens.total_output_tokens,
                model_name,
                cache_read_tokens=phase1_tokens.cache_read_tokens,
                cache_creation_tokens=phase1_tokens.cache_creation_tokens,
            )

            # === Phase 2: Response Generation ===
            logger.info(f"Phase 2: Response Generation (messages={len(collected_messages)}, has_tools={has_tools})")

            # Build Phase 2 system prompt (no user_id for privacy)
            phase2_prompt = await self._build_phase2_prompt(
                has_tools, language,
                prompt_dir=self.prompt_dir,
                orchestrator_note=manifest_note,
            )

            # Build Phase 2 messages
            phase2_messages = list(messages)
            phase2_collected: list[BaseMessage] = []

            if has_tools:
                # Has tool calls: filter failed results and intermediate tools
                # (generate_answer no longer exists; OrchestratorManifest is hidden
                # by LangChain in state.structured_response, not as a ToolMessage)
                phase2_collected = self._filter_tool_messages(collected_messages)

            # Phase 2 streaming output
            phase2_input_tokens = 0
            phase2_output_tokens = 0
            phase2_model = ""
            phase2_cache_read = 0
            phase2_cache_creation = 0

            # Determine group name for responder selection
            # provider can be a group name (e.g., "sonnet&gemini") or a plain provider name
            selected_group = provider if isinstance(provider, str) else None

            async for event in self._stream_phase2_response(
                has_tools, phase2_prompt, phase2_messages,
                phase2_collected,
                group=selected_group,
            ):
                if event.get("type") == "_cost_metadata":
                    phase2_input_tokens = event.get("input_tokens", 0)
                    phase2_output_tokens = event.get("output_tokens", 0)
                    phase2_model = event.get("model", "")
                    phase2_cache_read = event.get("cache_read_tokens", 0)
                    phase2_cache_creation = event.get("cache_creation_tokens", 0)
                    continue
                yield event

            # Phase 2 cost statistics
            phase2_cost = StreamConverter.create_cost_statistics(
                phase2_input_tokens, phase2_output_tokens, phase2_model,
                cache_read_tokens=phase2_cache_read,
                cache_creation_tokens=phase2_cache_creation,
            )

            # === Merge cost statistics ===
            yield self._merge_cost_statistics(phase1_cost, phase2_cost)

        except Exception as e:
            logger.error(f"MixAgent error: {e}", exc_info=True)
            yield {"type": "error", "content": str(e)}

    # === LLM Client Loading ===

    @classmethod
    def load_llm_clients(cls, llm_client_config: dict[str, Any]) -> dict[str, Any]:
        """
        Load LLM clients with support for nested provider groups.

        Supports two configuration formats:

        1. Flat format (legacy, for backward compatibility):
           ```yaml
           PROVIDERS_MIX:
             claude-sonnet:
               llm_type: openai
               model: anthropic/claude-sonnet-4.6
             gemini-3-pro@responder:
               llm_type: google-genai
               model: gemini-3.1-pro-preview
               response_with_tools: true
           ```

        2. Nested format (recommended, flexible provider groups):
           ```yaml
           PROVIDERS_MIX:
             claude|gemini:  # Group name for frontend display
               claude-sonnet@orchestrator:  # Phase 1 provider
                 llm_type: openai
                 model: anthropic/claude-sonnet-4.6
               gemini-3-pro@responder:  # Phase 2 provider (with tools)
                 llm_type: google-genai
                 model: gemini-3.1-pro-preview
                 response_with_tools: true
               gemini-3-flash@responder:  # Phase 2 provider (without tools)
                 llm_type: google-genai
                 model: gemini-3.1-flash-lite-preview
                 response_with_tools: false
           ```

        Requirements:
        - At least one @orchestrator provider (Phase 1)
        - At least one @responder provider (Phase 2)
        - response_with_tools field is optional:
          - If provided (true/false), used to select responder based on tool usage
          - If omitted, responder is used for both has_tools=true and has_tools=false

        The @responder and @orchestrator providers are filtered from public API exposure.
        Returns group names (e.g., "claude|gemini") for nested format, or filtered
        provider names for flat format (excluding @orchestrator/@responder).
        """
        class_name = cls.__name__
        agent_name = class_name.replace("Agent", "")

        if not llm_client_config:
            logger.warning(f"[{class_name}] No LLM providers configured")
            return {}

        # Flatten nested configuration
        flattened_config = cls._flatten_provider_config(llm_client_config)

        # Validate required providers
        has_orchestrator = any("@orchestrator" in k for k in flattened_config.keys())
        has_responder = any("@responder" in k for k in flattened_config.keys())

        if not has_orchestrator:
            logger.warning(f"[{class_name}] No @orchestrator provider found - at least one required")
        if not has_responder:
            logger.warning(f"[{class_name}] No @responder provider found - at least one required")

        # Load all providers using parent's method
        all_clients = DeepAgent.load_llm_clients(flattened_config)

        # Store all clients internally (for @responder access in _extract_responder_providers)
        cls._all_llm_clients[agent_name] = all_clients

        # Build group -> responder names mapping (using prefixed names from flattened config)
        group_responder_map: dict[str, list[str]] = {}
        for key, value in llm_client_config.items():
            if not isinstance(value, dict):
                continue
            is_group = any("@orchestrator" in k or "@responder" in k for k in value.keys())
            if is_group:
                # Use prefixed names (group::provider) to match keys in all_clients
                responder_names = [f"{key}::{k}" for k in value.keys() if "@responder" in k]
                if responder_names:
                    group_responder_map[key] = responder_names
        cls._group_responder_map[agent_name] = group_responder_map

        # Build public-facing clients dict (group names only, no @orchestrator/@responder)
        public_clients = {}

        for key, value in llm_client_config.items():
            if not isinstance(value, dict):
                continue

            # Check if this is a nested group (contains @orchestrator/@responder)
            is_group = any("@orchestrator" in k or "@responder" in k for k in value.keys())

            if is_group:
                # Nested format: use group name, map to first orchestrator client (prefixed)
                for provider_name in value.keys():
                    prefixed_name = f"{key}::{provider_name}"
                    if "@orchestrator" in provider_name and prefixed_name in all_clients:
                        public_clients[key] = all_clients[prefixed_name]
                        break
            else:
                # Flat format: add if not @orchestrator/@responder
                if "@orchestrator" not in key and "@responder" not in key:
                    if key in all_clients:
                        public_clients[key] = all_clients[key]

        logger.info(f"[{class_name}] Loaded {len(all_clients)} internal clients, {len(public_clients)} public: {list(public_clients.keys())}")
        return public_clients

    @staticmethod
    def _flatten_provider_config(config: dict[str, Any]) -> dict[str, Any]:
        """
        Flatten nested provider configuration to flat format.

        Nested format (providers are prefixed with group name to avoid collisions):
          group_name:
            provider1@orchestrator: {...}
            provider2@responder: {...}

        Flat result:
          group_name::provider1@orchestrator: {...}
          group_name::provider2@responder: {...}

        This ensures that multiple groups with the same provider names
        (e.g., both having "gemini-3-pro@responder") don't overwrite each other.

        Args:
            config: Provider configuration (nested or flat)

        Returns:
            Flattened configuration dict
        """
        flattened = {}

        for key, value in config.items():
            if not isinstance(value, dict):
                logger.warning(f"Skipping invalid provider config: {key}")
                continue

            # Check if this is a nested group (contains providers with @ suffix)
            is_group = any("@orchestrator" in k or "@responder" in k for k in value.keys())

            if is_group:
                # Nested format - prefix with group name to avoid key collisions
                for provider_name, provider_config in value.items():
                    if isinstance(provider_config, dict):
                        prefixed_name = f"{key}::{provider_name}"
                        flattened[prefixed_name] = provider_config
                    else:
                        logger.warning(f"Skipping invalid provider in group '{key}': {provider_name}")
            else:
                # Flat format - add directly
                flattened[key] = value

        return flattened


__all__ = ["MixAgent"]
