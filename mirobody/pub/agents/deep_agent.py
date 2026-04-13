import logging
import os
import uuid
from typing import Any, AsyncGenerator, Optional, TYPE_CHECKING

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool
from langchain.chat_models import init_chat_model

from ...chat.model import UserInfo
from ...chat.agent import get_llm_client_by_name
from ...utils.log import get_req_ctx
from ...utils.config import safe_read_cfg

from .deep.utils import StreamConverter, TokenUsageCallback
from .deep.backend import create_postgres_backend
from .deep.prompt_builder import build_system_prompt
from .deep.middleware import UniversalPromptCachingMiddleware
from .deep.errors import DeepAgentError, ConfigError
from langchain.agents.middleware import AgentMiddleware
from langchain.agents import create_agent

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel
    from .deep.backend import PostgresBackend

logger = logging.getLogger(__name__)


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
        **kargs
    ):
        self.user_info = UserInfo(user_id=user_id, user_name=user_name or "User")
        self.token = token
        from mirobody.utils.config import get_default_timezone
        self.timezone = timezone or get_default_timezone()
        self.allowed_tools = allowed_tools 
        self.disallowed_tools = disallowed_tools or []
        self.prompt_templates = prompt_templates
        self.agent_name = "Theta"
        self.default_provider = safe_read_cfg("DEFAULT_PROVIDER_DEEP") or "gemini-3-flash"
        self.file_parse_cache_ttl = int(safe_read_cfg("FILE_CACHE_TTL") or 300)
        self.file_parse_cache_maxsize = int(safe_read_cfg("FILE_CACHE_MAXSIZE") or 100)
        self.recursion_limit = int(safe_read_cfg("RECURSION_LIMIT") or 100)

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
                from ...chat.agent import global_llm_clients_for_agents
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

        try:
            global_tools = await load_global_tools(
                user_id=user_id,
                token=self.token,
                session_id=session_id,
                allowed_tools=self.allowed_tools,
                disallowed_tools=self.disallowed_tools
            )
            tools.extend(global_tools)
            logger.info(f"Loaded {len(global_tools)} global tools")
        except Exception as e:
            logger.warning(f"Failed to load global tools: {e}")
        return tools

    async def _get_base_prompt(self, user_id: str, prompt_name: str) -> str:
        
        from ...chat.user_config import get_user_prompt_by_name

        base_prompt = ""
        
        # Get user's prompt
        s, err = await get_user_prompt_by_name(user_id, prompt_name)
        if not err and s:
            base_prompt = s
            logger.info(f"Loaded user prompt: {prompt_name}")
        elif err:
            logger.warning(f"Failed to load user prompt '{prompt_name}': {err}")
        
        # Get system prompt from templates
        if not base_prompt and self.prompt_templates:
            base_prompt = self.prompt_templates.get(prompt_name)
            if base_prompt:
                logger.info(f"Using template prompt: {prompt_name}")
        
        # Fallback to first available template
        if not base_prompt and self.prompt_templates:
            for key, value in self.prompt_templates.items():
                if value:
                    base_prompt = value
                    logger.info(f"Using fallback prompt: {key}")
                    break
        
        # If still no prompt, this is critical
        if not base_prompt:
            raise DeepAgentError(f"No prompt template found for '{prompt_name}' and no fallback available")
        
        return base_prompt
    
    async def _build_system_prompt(
        self,
        base_prompt: str,
        language: str,
        user_id: str,
        tools: list,
    ) -> str:
        """Build system prompt with tools, time, and user context."""
        try:
            system_prompt = await build_system_prompt(
                base_prompt=base_prompt,
                language=language,
                user_id=user_id,
                langchain_tools=tools,
                agent_name=self.agent_name,
                user_name=self.user_info.user_name,
                timezone=self.timezone
            )
            logger.info("Built system prompt successfully")
            return system_prompt
        except Exception as e:
            logger.error(f"Failed to build system prompt: {str(e)}")
            raise DeepAgentError(
                f"System prompt construction failed: {str(e)}",
                user_message=f"Failed to build the agent's system prompt. Details: {str(e)}"
            )
    
    async def _create_backend(self, session_id: str, user_id: str) -> Any:
        """Create PostgreSQL backend with auto-initialized FileParser."""
        try:
            backend = create_postgres_backend(
                session_id=session_id,
                user_id=user_id,
                cache_ttl=self.file_parse_cache_ttl,
                cache_maxsize=self.file_parse_cache_maxsize,
            )
            logger.info(f"Created backend for session: {session_id}")
            return backend
        except Exception as e:
            logger.error(f"Backend creation failed: {str(e)}")
            raise DeepAgentError(f"Backend creation failed: {str(e)}")

    @staticmethod
    def _create_middlewares(
        llm_client: Any,
        backend: Any,
        **kwargs
    ) -> Any:
        """
        Create the agent instance with middleware stack.

        Middleware stack (in order):
        1. SummarizationMiddleware - Long context summarization
        2. PatchToolCallsMiddleware - Tool call fixes
        3. UniversalPromptCachingMiddleware - Prompt caching for supported models
        """
        from deepagents.middleware.summarization import SummarizationMiddleware, compute_summarization_defaults
        from deepagents.middleware.patch_tool_calls import PatchToolCallsMiddleware

        # Compute summarization defaults based on model profile
        summarization_defaults = compute_summarization_defaults(llm_client)

        summarization_middleware = SummarizationMiddleware(
            model=llm_client,
            backend=backend,
            trigger=summarization_defaults["trigger"],
            keep=summarization_defaults["keep"],
            trim_tokens_to_summarize=None,
            truncate_args_settings=summarization_defaults["truncate_args_settings"],
        )

        # Build middleware stack
        middleware_stack: list[AgentMiddleware] = [
            summarization_middleware,
            PatchToolCallsMiddleware(),
            UniversalPromptCachingMiddleware(ttl="5m", unsupported_model_behavior="ignore"),
        ]

        return middleware_stack

    def _create_stream_config(self, user_id: str, token_counter: Any) -> dict:
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
        agent_class_name = self.__class__.__name__.replace("Agent", "")
        llm_client, model_name, fallback_used, fallback_msg = await self._init_llm_client(provider, agent_class_name)

        loaded_tools = tools if tools is not None else await self._load_tools(user_id, session_id)

        base_prompt = await self._get_base_prompt(user_id, prompt_name)
        system_prompt = await self._build_system_prompt(base_prompt, language, user_id, loaded_tools)

        return llm_client, model_name, (fallback_msg if fallback_used else None), loaded_tools, system_prompt

    async def _build_agent(
        self,
        session_id: str,
        user_id: str,
        llm_client: "BaseChatModel",
        system_prompt: str,
        tools: list[BaseTool],
        messages: list[dict[str, Any]] | list[BaseMessage],
        file_list: list[dict[str, Any]] | None = None,
        files_data: list[dict[str, Any]] | None = None,
    ) -> tuple[Any, "PostgresBackend", list]:
        """
        Build agent with backend and handle file uploads.

        Returns:
            Tuple of (agent, backend, messages)
        """
        try:
            backend = await self._create_backend(session_id, user_id)

            # Handle file uploads if present
            if files_data:
                from .utils import handle_file_upload
                _, file_reminder = await handle_file_upload(
                    file_list=file_list,
                    files_data=files_data,
                    backend=backend,
                )
                if file_reminder:
                    if isinstance(messages, list) and len(messages) >= 1:
                        messages = list(messages)  # Make a copy to avoid mutating original
                        messages.insert(-1, {"role": "user", "content": file_reminder})
                    else:
                        messages = [{"role": "user", "content": file_reminder}]

            # non tool-related middles allowed
            middleware_stack = self._create_middlewares(llm_client, backend)

            # using native deepagents middleware instead 
            # from deepagents.middleware import FilesystemMiddleware
            # from langchain.agents.middleware import TodoListMiddleware
            # middleware_stack.extend(TodoListMiddleware())
            # file_middleware = FilesystemMiddleware(backend=backend)
            # middleware_stack.extend(file_middleware)

            agent = create_agent(
                llm_client,
                system_prompt=system_prompt,
                tools=tools,
                middleware=middleware_stack
            ).with_config({"recursion_limit": 1000})

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
            async for stream_type, stream_event in agent.astream(
                {"messages": messages},
                context=chat_context,
                stream_mode=["messages", "updates"],
                config=config
            ):
                try:
                    async for event in StreamConverter.process_stream_event(
                        stream_type, stream_event, trace_id=trace_id
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
            yield {"type": "error", "content": f"Streaming error: {str(e)}"}

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
        **kargs
    ) -> AsyncGenerator[dict[str, Any], None]:

        if not messages:
            logger.warning("Empty messages received")
            yield {"type": "error", "content": "Empty message"}
            return

        if not isinstance(messages, list):
            logger.warning(f"Invalid messages type: {type(messages)}")
            yield {"type": "error", "content": "Invalid messages format"}
            return

        if not user_id or not isinstance(user_id, str):
            logger.warning("Invalid or missing user_id")
            yield {"type": "error", "content": "User ID is required"}
            return

        logger.info(f"DeepAgent request: session={session_id}, provider={provider}, messages={len(messages)}")

        try:
            # Use files_data from kargs if provided (HTTP layer override)
            effective_files_data = kargs.get('files_data', files_data)
            if effective_files_data:
                logger.info(f"Processing {len(effective_files_data)} files")

            # Phase 1: Prepare LLM, tools, and system prompt
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

            # Phase 2: Build agent with backend and file handling
            agent, backend, final_messages = await self._build_agent(
                session_id=session_id,
                user_id=user_id,
                llm_client=llm_client,
                system_prompt=system_prompt,
                tools=loaded_tools,
                messages=messages,
                file_list=file_list,
                files_data=effective_files_data,
            )

            # Phase 3: Stream response
            token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(user_id, token_counter)

            async for event in self._stream_agent_response(
                agent=agent,
                messages=final_messages,
                config=stream_config,
            ):
                yield event

            # Yield token statistics after streaming completes
            if token_counter.total_input_tokens > 0 or token_counter.total_output_tokens > 0:
                yield StreamConverter.create_cost_statistics(
                    token_counter.total_input_tokens,
                    token_counter.total_output_tokens,
                    model_name,
                    cache_read_tokens=token_counter.cache_read_tokens,
                    cache_creation_tokens=token_counter.cache_creation_tokens,
                )

        except DeepAgentError as e:
            logger.error(f"DeepAgent error: {str(e)}")
            yield {"type": "error", "content": str(e)}

        except ValueError as e:
            logger.error(f"DeepAgent value error: {str(e)}")
            yield {"type": "error", "content": str(e)}

        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", stack_info=True)
            yield {"type": "error", "content": f"Unexpected error: {str(e)}\n\nCheck logs for details."}
    
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

                        class PlaceholderClient:
                            def __init__(self, model_name, missing_key, provider_name):
                                object.__setattr__(self, '_model_name', model_name)
                                object.__setattr__(self, '_missing_key', missing_key)
                                object.__setattr__(self, '_provider_name', provider_name)
                                object.__setattr__(self, 'model_name', model_name)
                                object.__setattr__(self, 'model', model_name)

                            def __getattribute__(self, name):
                                if name in ('_model_name', '_missing_key', '_provider_name', 'model_name', 'model'):
                                    return object.__getattribute__(self, name)

                                missing_key = object.__getattribute__(self, '_missing_key')
                                missing_key_msg = f"Missing {missing_key}. Get API key from provider and set in .env or environment"
                                raise AttributeError(missing_key_msg)

                        llm_clients[provider_name] = PlaceholderClient(model, api_key_name, provider_name)
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

                # Build init_chat_model kwargs
                model_provider = llm_type
                init_kwargs = {k: v for k, v in config.items() if k not in ["model", "llm_type", "response_with_tools"]}

                # Log call parameters
                logger.info(f"[{class_name}] Calling init_chat_model('{provider_name}'): model={model}, provider={model_provider}, kwargs={init_kwargs}")

                try:
                    client = init_chat_model(model=model, model_provider=model_provider, **init_kwargs)
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
            logger.info(f"[{class_name}] Loaded {loaded}/{total} providers: {', '.join(llm_clients.keys())}")

        if failed:
            for name, reason in failed:
                logger.warning(f"[{class_name}] Failed '{name}': {reason}")

        if loaded == 0:
            logger.warning(f"[{class_name}] No providers loaded (0/{total}) - agent may be disabled intentionally")

        return llm_clients
