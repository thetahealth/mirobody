import logging, os
from typing import Any, AsyncGenerator, Optional
from dataclasses import dataclass, field
import uuid

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool
from deepagents import create_deep_agent
from langchain.chat_models import init_chat_model

from ...chat.model import UserInfo
from ...chat.agent import get_llm_client_by_name
from ...utils.log import get_req_ctx
from ...utils.config import safe_read_cfg

from .deep.utils import StreamConverter, TokenUsageCallback
from .deep.backends import create_postgres_backend
from .deep.file_handler import upload_files_to_backend
from .deep.prompt_builder import build_system_prompt
from .deep.middleware import GlobalFilesMiddleware
from .deep.exceptions import (
    DeepAgentException,
    LLMInitializationError,
    BackendCreationError,
    PromptLoadError,
    SystemPromptBuildError,
    AgentBuildError,
    ErrorMessageHandler,
)

logger = logging.getLogger(__name__)


@dataclass
class AgentContext:
    """Agent preparation context."""
    user_id: str
    session_id: str
    language: str
    provider: str | Any | None
    messages: list[dict[str, Any]] | list[BaseMessage]
    file_list: list[dict[str, Any]] | None
    files_data: list[dict[str, Any]] | None
    prompt_name: str
    llm_client: Any = None
    model_name: str = "Unknown"
    tools: list = field(default_factory=list)
    system_prompt: Any = None
    backend: Any = None
    agent: Any = None


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
        self.timezone = timezone or "America/Los_Angeles"
        self.allowed_tools = allowed_tools
        self.disallowed_tools = disallowed_tools
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
                error_msg = ErrorMessageHandler.provider_not_found(
                    provider_name=original_provider or default_provider,
                    agent_name=agent_class_name,
                    available_providers=available
                )
                raise LLMInitializationError(
                    f"Provider not available: {original_provider or default_provider}",
                    user_message=error_msg
                )

        # Validate client (check for PlaceholderClient)
        try:
            _ = agent_llm_client.invoke
        except AttributeError as attr_error:
            error_msg = str(attr_error)
            logger.error(f"Provider validation failed: {error_msg}")
            raise LLMInitializationError(
                f"Provider initialization failed: {error_msg}",
                user_message=error_msg
            )
        
        # Extract model name
        model_name = getattr(agent_llm_client, "model_name", None) or getattr(agent_llm_client, "model", "Unknown")

        return agent_llm_client, model_name, fallback_used, fallback_message
    
    async def _load_tools(self, user_id: str) -> list:

        tools = []
        from .deep.tool_loader import load_global_tools

        try:
            global_tools = await load_global_tools(
                user_id=user_id,
                token=self.token,
                allowed_tools=self.allowed_tools,
                disallowed_tools=self.disallowed_tools
            )
            tools.extend(global_tools)
            logger.info(f"Loaded {len(global_tools)} global tools")
        except Exception as e:
            logger.warning(f"Failed to load global tools: {e}")
        
        # from ...chat.tool_loader import load_mcp_tools
        # try:
        #     mcp_tools = await load_mcp_tools(user_id=user_id, token=self.token)
        #     tools.extend(mcp_tools)
        #     logger.info(f"Loaded {len(mcp_tools)} MCP tools")
        # except Exception as e:
        #     logger.warning(f"Failed to load MCP tools: {e}")
        
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
            error = Exception(f"No prompt template found for '{prompt_name}' and no fallback available")
            error_msg = ErrorMessageHandler.prompt_load_failed(prompt_name, error)
            logger.error(error_msg)
            raise PromptLoadError(
                f"No prompt template available for '{prompt_name}'",
                user_message=error_msg
            )
        
        return base_prompt
    
    async def _build_system_prompt(
        self,
        base_prompt: str,
        language: str,
        user_id: str,
        tools: list,
        llm_client: Any
    ) -> Any:
        """Build system prompt with tools, time, and user context."""
        try:
            system_prompt = await build_system_prompt(
                base_prompt=base_prompt,
                language=language,
                user_id=user_id,
                langchain_tools=tools,
                model_client=llm_client,
                agent_name=self.agent_name,
                user_name=self.user_info.user_name,
                timezone=self.timezone
            )
            logger.info(f"Built system prompt successfully")
            return system_prompt
        except Exception as e:
            error_msg = ErrorMessageHandler.system_prompt_build_failed(e)
            logger.error(f"Failed to build system prompt: {str(e)}")
            raise SystemPromptBuildError(
                f"System prompt construction failed: {str(e)}",
                user_message=error_msg
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
            error_msg = ErrorMessageHandler.backend_creation_failed(e)
            logger.error(f"Backend creation failed: {str(e)}")
            raise BackendCreationError(f"Backend creation failed: {str(e)}", user_message=error_msg)
    
    async def _upload_files_to_backend(
        self,
        file_list: list[dict[str, Any]] | None,
        backend: Any,
        messages: list[dict[str, Any]] | list[BaseMessage],
        files_data: list[dict[str, Any]] | None = None
    ) -> None:
        if not file_list:
            return

        try:
            uploaded_paths, reminder_message = await upload_files_to_backend(
                file_list, 
                backend,
                files_data=files_data
            )

            if uploaded_paths:
                logger.info(f"Uploaded {len(uploaded_paths)} files to PostgreSQL")
                if reminder_message and not any(isinstance(msg, BaseMessage) for msg in messages):
                    messages.append({
                        "role": "user",
                        "content": reminder_message
                    })
            else:
                logger.warning("No files were successfully uploaded")
        except Exception as e:
            logger.error(f"Error uploading files to backend: {str(e)}")


    def _create_middleware(self, backend: Any) -> list:
        return [GlobalFilesMiddleware(backend=backend)]

    def _create_agent_instance(
        self,
        llm_client: Any,
        system_prompt: Any,
        tools: list,
        backend: Any,
        middleware: list
    ) -> Any:
        agent = create_deep_agent(
            model=llm_client,
            system_prompt=system_prompt,
            tools=tools if tools else None,
            backend=backend,
            middleware=middleware
        )
        return agent

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
        messages: list[dict[str, Any]],
        file_list: list[dict[str, Any]] | None,
        files_data: list[dict[str, Any]] | None,
        prompt_name: str,
        tools: list[BaseTool] | None = None,
    ) -> AgentContext:
        context = AgentContext(
            user_id=user_id,
            session_id=session_id,
            language=language,
            provider=provider,
            messages=messages,
            file_list=file_list,
            files_data=files_data,
            prompt_name=prompt_name
        )

        agent_class_name = self.__class__.__name__.replace("Agent", "")
        llm_result = await self._init_llm_client(provider, agent_class_name)
        context.llm_client = llm_result[0]
        context.model_name = llm_result[1]
        context._fallback_msg = llm_result[3] if llm_result[2] else None

        context.tools = tools if tools is not None else await self._load_tools(user_id)

        base_prompt = await self._get_base_prompt(user_id, prompt_name)
        context.system_prompt = await self._build_system_prompt(
            base_prompt,
            language,
            user_id,
            context.tools,
            context.llm_client
        )

        return context

    async def _build_agent(self, context: AgentContext) -> AgentContext:
        try:
            context.backend = await self._create_backend(context.session_id, context.user_id)
            
            if context.files_data:
                context.backend.store_pending_files(context.files_data)
                
                if context.file_list and not any(isinstance(msg, BaseMessage) for msg in context.messages):
                    names = [f.get("file_name", "") for f in context.file_list[:3]]
                    suffix = f", +{len(context.file_list) - 3} more" if len(context.file_list) > 3 else ""
                    context.messages.append({
                        "role": "user",
                        "content": f"Files: {', '.join(names)}{suffix}. Use read_file(\"/uploads/filename\")."
                    })
            
            middleware = self._create_middleware(context.backend)
            
            context.agent = self._create_agent_instance(
                context.llm_client,
                context.system_prompt,
                context.tools,
                context.backend,
                middleware
            )
            
            logger.info(f"Agent built successfully for session: {context.session_id}")
            return context
        except BackendCreationError:
            raise
        except Exception as e:
            error_msg = ErrorMessageHandler.agent_build_failed(e)
            logger.error(f"Agent building failed: {str(e)}")
            raise AgentBuildError(
                f"Failed to build agent: {str(e)}",
                user_message=error_msg
            )
    
    async def _stream_agent_response(
        self,
        agent: Any,
        messages: list[dict[str, Any]] | list[BaseMessage],
        token_counter: TokenUsageCallback,
        config: dict,
        model_name: str
    ) -> AsyncGenerator[dict[str, Any], None]:
        logger.info("Starting DeepAgent stream")
        trace_id = get_req_ctx("trace_id") or str(uuid.uuid4())
    

        try:
            async for stream_type, stream_event in agent.astream(
                {"messages": messages}, 
                stream_mode=["messages", "updates"], 
                config=config
            ):
                try:
                    async for event in StreamConverter.process_stream_event(
                        stream_type, stream_event, trace_id=trace_id
                    ):
                        if event:
                            yield event
                except Exception as e:
                    logger.error(f"Error processing stream chunk: {str(e)}, trace_id={trace_id}")
                    continue

            logger.info("DeepAgent stream completed")
            
            if token_counter.total_input_tokens > 0 or token_counter.total_output_tokens > 0:
                yield StreamConverter.create_cost_statistics(
                    token_counter.total_input_tokens,
                    token_counter.total_output_tokens,
                    model_name
                )
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
            context = await self._prepare_context(
                user_id=user_id,
                session_id=session_id,
                language=language,
                provider=provider,
                messages=messages,
                file_list=file_list,
                files_data=files_data,
                prompt_name=prompt_name,
                tools=tools,
            )
            
            if 'files_data' in kargs:
                context.files_data = kargs['files_data']
                logger.info(f"Received {len(context.files_data)} file paths from HTTP layer")

            if hasattr(context, '_fallback_msg') and context._fallback_msg:
                yield {"type": "thinking", "content": context._fallback_msg}

            context = await self._build_agent(context)

            context.token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(user_id, context.token_counter)

            async for event in self._stream_agent_response(
                agent=context.agent,
                messages=context.messages,
                token_counter=context.token_counter,
                config=stream_config,
                model_name=context.model_name
            ):
                yield event
            
        except DeepAgentException as e:
            logger.error(f"DeepAgent error: {str(e)}")
            yield {"type": "error", "content": e.user_message}
            
        except ValueError as e:
            logger.error(f"DeepAgent value error: {str(e)}")
            yield {"type": "error", "content": str(e)}
            
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}", stack_info=True)
            yield {"type": "error", "content": f"Unexpected error: {str(e)}\n\nCheck logs for details."}
    
    #-------------------------------------------------------------------------
    
    @staticmethod
    def load_llm_clients(llm_client_config: dict[str, Any]) -> dict[str, Any]:
        if not llm_client_config or len(llm_client_config) == 0:
            logger.warning("No LLM providers configured in PROVIDERS_DEEP section")
            return {}
        
        common_keys = ["GOOGLE_API_KEY", "OPENAI_API_KEY", "OPENROUTER_API_KEY", "ANTHROPIC_API_KEY"]
        has_any_key = any(
            os.environ.get(key) or safe_read_cfg(key)
            for key in common_keys
        )
        
        if not has_any_key:
            logger.warning(f"No API keys found. Please set at least one: {', '.join(common_keys)}")
        
        llm_clients = {}
        failed = []
        
        for provider_name, provider_kwargs in llm_client_config.items():
            logger.debug(f"Initializing provider '{provider_name}'...")
            
            if not provider_kwargs or not isinstance(provider_kwargs, dict):
                logger.warning(f"Invalid config for '{provider_name}': not a dictionary")
                failed.append((provider_name, "Invalid format"))
                continue

            try:
                config = dict(provider_kwargs)
                model = config.get("model", "unknown")
                llm_type = config.get("llm_type", "openai")
                logger.debug(f"  Model: {model}, Type: {llm_type}")
                
                api_key_name = config.get("api_key")
                actual_api_key = None
                
                if api_key_name and isinstance(api_key_name, str):
                    logger.debug(f"  Looking for API key: {api_key_name}")
                    actual_api_key = os.environ.get(api_key_name) or safe_read_cfg(api_key_name)
                    
                    if actual_api_key:
                        config["api_key"] = actual_api_key
                        logger.debug(f"  API key resolved")
                    else:
                        logger.warning(f"  {api_key_name} not found - creating placeholder for frontend visibility")
                
                if not model or model == "unknown":
                    logger.warning(f"Provider '{provider_name}' skipped: missing 'model' field")
                    failed.append((provider_name, "Missing 'model'"))
                    continue
                
                if not actual_api_key and api_key_name:
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
                            provider_name = object.__getattribute__(self, '_provider_name')
                            
                            error_msg = ErrorMessageHandler.api_key_missing(missing_key, provider_name)
                            raise AttributeError(error_msg)
                    
                    llm_clients[provider_name] = PlaceholderClient(model, api_key_name, provider_name)
                    logger.info(f"Created placeholder for '{provider_name}': {model} (missing {api_key_name})")
                    continue
                
                model_provider = llm_type
                init_kwargs = {k: v for k, v in config.items() if k not in ["model", "llm_type"]}
                
                try:
                    client = init_chat_model(model=model, model_provider=model_provider, **init_kwargs)
                    llm_clients[provider_name] = client
                    logger.info(f"Successfully initialized '{provider_name}': {model_provider}/{model}")
                except Exception as e:
                    api_key_var = config.get("api_key") if "config" in locals() else None
                    error_msg = ErrorMessageHandler.provider_init_failed(provider_name, e, api_key_var)
                    logger.error(f"Failed to load '{provider_name}': {str(e)}")
                    logger.debug(error_msg)
                    failed.append((provider_name, str(e)))
            except Exception as outer_e:
                logger.error(f"Unexpected error loading provider '{provider_name}': {str(outer_e)}")
                failed.append((provider_name, str(outer_e)))
        
        loaded = len(llm_clients)
        total = len(llm_client_config)
        
        logger.info(f"\nDeepAgent Providers: {loaded}/{total} loaded")
        if loaded > 0:
            logger.info(f"Available: {', '.join(llm_clients.keys())}")
        if failed:
            logger.warning(f"Failed: {len(failed)}/{total}")
            for name, reason in failed[:3]:
                logger.warning(f"  - {name}: {reason}")
        
        if loaded == 0:
            logger.warning("WARNING: No LLM providers loaded. Check API keys and config.yaml PROVIDERS_DEEP section.")
        
        return llm_clients
