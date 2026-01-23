# Standard library imports
import logging
import os
from typing import Any, AsyncGenerator, Optional
from dataclasses import dataclass, field

from langchain_core.messages import BaseMessage
from langchain_core.tools import BaseTool

from mirobody.chat.model import UserInfo
from mirobody.chat.agent import get_llm_client_by_name
from mirobody.utils.log import get_req_ctx

from deepagents import create_deep_agent
from langchain.chat_models import init_chat_model

# Local module imports
from .deep.utils import StreamConverter, TokenUsageCallback
from .deep.parser import FileParser
from .deep.backends import create_postgres_backend
from .deep.file_handler import upload_files_to_backend
from .deep.prompt_builder import build_system_prompt
from .deep.tool_loader import load_global_tools, load_mcp_tools
from .deep.middleware import GlobalFilesMiddleware
from mirobody.utils.config import safe_read_cfg

# Exception handling
from .deep.exceptions import (
    DeepAgentException,
    ConfigurationError,
    APIKeyError,
    ProviderConfigError,
    DatabaseConnectionError,
    LLMInitializationError,
    BackendCreationError,
    ToolLoadError,
    PromptLoadError,
    SystemPromptBuildError,
    AgentBuildError,
    ErrorMessageHandler,
)

logger = logging.getLogger(__name__)


@dataclass
class AgentContext:
    """Context object for passing data between agent preparation stages."""
    user_id: str
    session_id: str
    language: str
    provider: str | Any | None
    messages: list[dict[str, Any]] | list[BaseMessage]
    file_list: list[dict[str, Any]] | None
    prompt_name: str

    # Initialized during preparation
    llm_client: Any = None
    model_name: str = "Unknown"
    tools: list = field(default_factory=list)
    system_prompt: Any = None
    backend: Any = None
    agent: Any = None
    token_counter: Any = None
    
    # Downloaded file content (from HTTP layer) - avoids re-downloading
    files_data: list[dict[str, Any]] | None = None

    # Fallback information
    fallback_used: bool = False
    fallback_message: str = ""


class DeepAgent():

    def _get_config(self, key: str, default: Any) -> Any:
        """
        Read integer configuration value with default fallback.
        
        Args:
            key: Configuration key name
            default: Default value if config not found or empty
            
        Returns:
            Integer configuration value
        """
        value = safe_read_cfg(key)
        return value if value else default

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
        """
        Initialize DeepAgent with user configuration.
        
        Args:
            user_id: User ID
            user_name: User name for display
            token: JWT token for authentication
            timezone: User timezone
            allowed_tools: List of allowed tool names (whitelist)
            disallowed_tools: List of disallowed tool names (blacklist)
            prompt_templates: Dict of prompt templates by name
            **kargs: Additional configuration options:
                - file_parse_cache_ttl: File parsing cache TTL in seconds (default: 300)
                - file_parse_cache_maxsize: Max number of cached parsed files (default: 100)
                - recursion_limit: Max recursion depth for agent graph (default: 20)
        
        Note: 
            - File parsing uses unified vision API which automatically selects the best
              available provider (gemini/openrouter/doubao) based on configured API keys
            - PDF parsing: PyPDF (fast) → LLM vision (fallback for complex PDFs)
            - Image parsing: LLM vision with automatic provider selection
        """

        self.user_info = UserInfo(
            user_id=user_id,
            user_name=user_name if user_name else "User"
        )

        self.token = token
        self.timezone = timezone if timezone else "America/Los_Angeles"
        self.allowed_tools = allowed_tools

        self.disallowed_tools = disallowed_tools

        self.prompt_templates = prompt_templates

        self.agent_name = "Theta"  # only used for system prompt init
        self.default_provider = safe_read_cfg("DEFAULT_PROVIDER_DEEP", "gemini-3-flash")

        # Load configuration with defaults
        self.file_parse_cache_ttl = int(self._get_config("FILE_CACHE_TTL", 300))
        self.file_parse_cache_maxsize = int(self._get_config("FILE_CACHE_MAXSIZE", 100))
        self.recursion_limit = int(self._get_config("RECURSION_LIMIT", 50))

    # ------------------------- Protected Methods for Modular Override -------------------------

    async def _init_llm_client(self, provider: str | Any | None, agent_class_name: str) -> tuple[Any, str, bool, str]:
        """
        Initialize LLM client from provider name or instance.

        Args:
            provider: LLM provider name, alias, or ChatModel instance
            agent_class_name: Agent class name for config matching

        Returns:
            Tuple of (llm_client, model_name, fallback_used, fallback_message)
            
        Raises:
            LLMInitializationError: If LLM client cannot be initialized
        """
        try:
            original_provider = provider
            fallback_used = False
            fallback_message = ""

            if provider:
                if isinstance(provider, str):
                    agent_llm_client = get_llm_client_by_name(agent_class_name, provider)
                else:
                    agent_llm_client = provider
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
                    from mirobody.chat.agent import global_llm_clients_for_agents
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

            # ===== Early Detection: Check if this is a PlaceholderClient (missing API key) =====
            # We need to validate it NOW before it enters streaming phase
            try:
                # Try to access a method that real LLM clients have but PlaceholderClient doesn't
                # This will trigger PlaceholderClient's __getattribute__ and expose the API key error
                _ = agent_llm_client.invoke
            except AttributeError as attr_error:
                # This is a PlaceholderClient with missing API key
                error_msg = str(attr_error)
                logger.error(f"Provider validation failed: {error_msg}")
                raise LLMInitializationError(
                    f"Provider initialization failed: {error_msg}",
                    user_message=error_msg
                )
            
            # Extract model name for logging
            if hasattr(agent_llm_client, "model_name"):
                model_name = agent_llm_client.model_name
            elif hasattr(agent_llm_client, "model"):
                model_name = agent_llm_client.model
            else:
                model_name = "Unknown"

            return agent_llm_client, model_name, fallback_used, fallback_message
            
        except LLMInitializationError:
            raise
        except Exception as e:
            error_msg = ErrorMessageHandler.provider_init_failed(
                provider_name=str(provider) if provider else "default",
                error=e
            )
            logger.error(f"LLM init failed: {str(e)}", exc_info=True)
            raise LLMInitializationError(f"LLM init failed: {str(e)}", user_message=error_msg)
    
    async def _load_global_tools(self, user_id: str) -> list:
        """
        Load global/project tools.
        
        Args:
            user_id: User ID
            
        Returns:
            List of global tools
            
        Raises:
            ToolLoadError: If critical tool loading fails
        """
        try:
            global_tools = await load_global_tools(
                user_id=user_id,
                token=self.token,
                allowed_tools=self.allowed_tools,
                disallowed_tools=self.disallowed_tools
            )
            logger.info(f"Loaded {len(global_tools)} global tools")
            return global_tools
        except Exception as e:
            error_msg = ErrorMessageHandler.tool_load_failed("global", e)
            logger.error(f"Failed to load global tools: {str(e)}", exc_info=True)
            logger.warning("Continuing without global tools")
            # Tools are optional - return empty list instead of raising
            return []

    async def _load_mcp_tools(self, user_id: str) -> list:
        """
        Load user MCP tools.
        
        Args:
            user_id: User ID
            
        Returns:
            List of MCP tools
            
        Raises:
            ToolLoadError: If critical MCP tool loading fails
        """
        try:
            user_tools = await load_mcp_tools(user_id=user_id, token=self.token)
            logger.info(f"Loaded {len(user_tools)} MCP tools")
            return user_tools
        except Exception as e:
            error_msg = ErrorMessageHandler.tool_load_failed("MCP", e)
            logger.error(f"Failed to load MCP tools: {str(e)}", exc_info=True)
            logger.warning("Continuing without MCP tools")
            # Tools are optional - return empty list instead of raising
            return []

    async def _load_tools(self, user_id: str) -> list:
        """
        Load all tools (global + MCP).
        
        Args:
            user_id: User ID
            
        Returns:
            Combined list of all tools
        """
        global_tools = await self._load_global_tools(user_id)
        mcp_tools = await self._load_mcp_tools(user_id)
        return global_tools + mcp_tools

    async def _get_base_prompt(self, user_id: str, prompt_name: str) -> str:
        """
        Get base prompt from user config or templates.
        
        Args:
            user_id: User ID
            prompt_name: Name of prompt template
            
        Returns:
            Base prompt string
            
        Raises:
            PromptLoadError: If no prompt template can be loaded
        """
        from ...chat.user_config import get_user_prompt_by_name

        base_prompt = ""
        
        try:
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
            
        except PromptLoadError:
            raise
        except Exception as e:
            error_msg = ErrorMessageHandler.prompt_load_failed(prompt_name, e)
            logger.error(f"Unexpected error loading prompt: {str(e)}", exc_info=True)
            raise PromptLoadError(
                f"Failed to load prompt '{prompt_name}': {str(e)}",
                user_message=error_msg
            )
    
    async def _build_system_prompt(
            self,
            base_prompt: str,
            language: str,
            user_id: str,
            tools: list,
            llm_client: Any
    ) -> Any:
        """
        Build system prompt with dynamic components.
        
        Args:
            base_prompt: Base prompt template
            language: User language
            user_id: User ID
            tools: List of available tools
            llm_client: LLM client instance
            
        Returns:
            Built system prompt
            
        Raises:
            SystemPromptBuildError: If system prompt construction fails
        """
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
            logger.info(f"Built system prompt successfully, type: {type(system_prompt)}")
            return system_prompt
        except Exception as e:
            error_msg = ErrorMessageHandler.system_prompt_build_failed(e)
            logger.error(f"Failed to build system prompt: {str(e)}", exc_info=True)
            raise SystemPromptBuildError(
                f"System prompt construction failed: {str(e)}",
                user_message=error_msg
            )
    
    async def _create_backend(self, session_id: str, user_id: str) -> Any:
        """
        Create backend instance for file storage and persistence.

        Args:
            session_id: Session ID
            user_id: User ID

        Returns:
            Backend instance
            
        Raises:
            BackendCreationError: If backend creation fails
        """
        try:
            backend = create_postgres_backend(
                session_id=session_id,
                user_id=user_id,
                file_parser=FileParser(),
                cache_ttl=self.file_parse_cache_ttl,
                cache_maxsize=self.file_parse_cache_maxsize,
            )
            logger.info(f"Created backend for session: {session_id}")
            return backend
            
        except Exception as e:
            error_msg = ErrorMessageHandler.backend_creation_failed(e)
            logger.error(f"Backend creation failed: {str(e)}", exc_info=True)
            raise BackendCreationError(f"Backend creation failed: {str(e)}", user_message=error_msg)
    
    async def _upload_files_to_backend(
            self,
            file_list: list[dict[str, Any]] | None,
            backend: Any,
            messages: list[dict[str, Any]] | list[BaseMessage],
            files_data: list[dict[str, Any]] | None = None
    ) -> None:
        """
        Upload files to backend and add reminder to messages.
        
        Args:
            file_list: List of files to upload (metadata)
            backend: Backend instance
            messages: Message list (modified in-place)
            files_data: Downloaded file content (from HTTP layer) - avoids re-downloading
        """
        if not file_list:
            return

        try:
            # Pass files_data to avoid re-downloading from S3
            uploaded_paths, reminder_message = upload_files_to_backend(
                file_list, 
                backend,
                files_data=files_data
            )

            if uploaded_paths:
                logger.info(f"✅ Uploaded {len(uploaded_paths)} files to PostgreSQL")

                # Add reminder message to conversation
                if reminder_message and not self._check_base_messages(messages):
                    messages.append({
                        "role": "user",
                        "content": reminder_message
                    })
            else:
                logger.warning("⚠️ No files were successfully uploaded")

        except Exception as e:
            logger.error(f"❌ Error uploading files to backend: {str(e)}", exc_info=True)
            # Continue without files - non-critical error

    def _check_base_messages(self, messages: list[BaseMessage]) -> bool:
        return any(isinstance(msg, BaseMessage) for msg in messages)

    def _create_middleware(self, backend: Any) -> list:
        """
        Create middleware list for agent.
        
        Args:
            backend: Backend instance
            
        Returns:
            List of middleware instances
        """
        return [
            GlobalFilesMiddleware(backend=backend),
        ]

    def _create_agent_instance(
            self,
            llm_client: Any,
            system_prompt: Any,
            tools: list,
            backend: Any,
            middleware: list
    ) -> Any:
        """
        Create agent instance with all components.
        
        Args:
            llm_client: LLM client instance
            system_prompt: System prompt
            tools: List of tools
            backend: Backend instance
            middleware: List of middleware
            
        Returns:
            Agent instance
        """
        agent = create_deep_agent(
            model=llm_client,
            system_prompt=system_prompt,
            tools=tools if tools else None,
            backend=backend,
            middleware=middleware
        )
        return agent

    # def _prepare_messages(self, messages: list[dict[str, Any]] | list[BaseMessage]) -> list[dict[str, Any]]:
    #     """
    #     Prepare messages by adding language following instruction.
        
    #     Args:
    #         messages: Original message list
            
    #     Returns:
    #         Modified message list
    #     """
    #     if self._check_base_messages(messages):
    #         return messages
    #     if isinstance(messages[-1], dict) and messages[-1].get("role") == "user":
    #         messages[-1]["content"] += "(You must reply in same Language I just asked)"
    #     return messages

    def _create_stream_config(self, user_id: str, token_counter: Any) -> dict:
        """
        Create configuration for agent streaming.
        
        Args:
            user_id: User ID
            token_counter: Token counter callback
            
        Returns:
            Stream configuration dict
        """
        user_info = {
            "user_id": user_id,
            "token": self.token,
            "success": True  # Authentication flag
        }

        config = {
            "recursion_limit": self.recursion_limit,
            "callbacks": [token_counter],
            "configurable": {
                "user_info": user_info
            }
        }
        return config

    # ------------------------- Stage Methods -------------------------

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
        """
        Stage 1: Prepare all context components (LLM, tools, prompt).
        
        Args:
            user_id: User ID
            session_id: Session ID
            language: User language
            provider: LLM provider
            messages: Chat messages
            file_list: Uploaded files
            prompt_name: Prompt template name
            
        Returns:
            AgentContext with initialized components
        """
        context = AgentContext(
            user_id=user_id,
            session_id=session_id,
            language=language,
            provider=provider,
            messages=messages,
            file_list=file_list,
            files_data=files_data,  # Pass downloaded file content
            prompt_name=prompt_name
        )

        # Initialize LLM client
        agent_class_name = self.__class__.__name__.replace("Agent", "")
        context.llm_client, context.model_name, context.fallback_used, context.fallback_message = await self._init_llm_client(provider, agent_class_name)

        # Load external tools （deepagents tools are not ） 
        context.tools = tools if tools is not None else await self._load_tools(user_id)

        # Build system prompt
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
        """
        Stage 2: Build agent with backend, files, and middleware.
        
        Args:
            context: Agent context from preparation stage
            
        Returns:
            Updated context with agent instance
            
        Raises:
            AgentBuildError: If agent building fails
            BackendCreationError: If backend creation fails
        """
        try:
            # Create backend
            context.backend = await self._create_backend(context.session_id, context.user_id)
            
            # Upload files (pass files_data to avoid re-downloading)
            await self._upload_files_to_backend(
                context.file_list, 
                context.backend, 
                context.messages,
                files_data=context.files_data
            )
            
            # Create middleware
            middleware = self._create_middleware(context.backend)
            
            # Create agent instance
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
            # Re-raise backend errors as-is (already have user message)
            raise
        except Exception as e:
            error_msg = ErrorMessageHandler.agent_build_failed(e)
            logger.error(f"Agent building failed: {str(e)}", exc_info=True)
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
        """
        Stage 3: Stream agent responses with token conversion.
        
        Args:
            agent: Agent instance
            messages: Prepared messages
            token_counter: Token counter callback
            config: Stream configuration
            model_name: Model name for cost calculation
            
        Yields:
            Converted stream events
        """
        logger.info("Starting DeepAgent stream")
        trace_id = get_req_ctx("trace_id")

        # Generate trace_id if not available from request context
        if not trace_id:
            import uuid
            from datetime import datetime

            # Generate UUID as trace_id
            trace_id = str(uuid.uuid4())

            # Get user's latest message for logging
            user_message = ""
            if messages and len(messages) > 0:
                last_msg = messages[-1]
                if isinstance(last_msg, dict) and "content" in last_msg:
                    user_message = str(last_msg["content"])[:200]  # First 200 chars

            # Log the mapping: trace_id <-> timestamp <-> user message
            logger.info(
                f"📍 [Init Trace Mapping] trace_id={trace_id}, "
                f"timestamp={datetime.now().isoformat()}, "
                f"user_message={user_message}"
            )

        try:
            async for stream_type, stream_event in agent.astream(
                {"messages": messages}, 
                stream_mode=["messages", "updates"], 
                config=config
            ):
                try:
                    # Process stream event using StreamConverter static method
                    async for event in StreamConverter.process_stream_event(
                        stream_type, stream_event, trace_id=trace_id
                    ):
                        if event:
                            yield event

                except Exception as e:
                    logger.error(f"Error processing stream chunk: {str(e)}, trace_id={trace_id}", exc_info=True, extra={"trace_id": trace_id})
                    continue

            logger.info("DeepAgent stream completed")

            # Add cost statistics at the end
            yield StreamConverter.create_cost_statistics(
                token_counter.total_input_tokens,
                token_counter.total_output_tokens,
                model_name
            )

        except Exception as e:
            logger.error(f"DeepAgent streaming error: {str(e)}", stack_info=True)
            yield {"type": "error", "content": f"Streaming error: {str(e)}"}

    # ------------------------- Main Entry Point -------------------------

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
        """
        Generate streaming response using DeepAgent.
        
        This method follows a three-stage pipeline:
        1. Prepare Context: Initialize LLM, tools, and system prompt
        2. Build Agent: Create backend, upload files, configure middleware
        3. Stream Response: Execute agent and stream results
        
        Args:
            user_id: User ID
            messages: Chat history messages [dict[str, Any]]
            provider: LLM provider name, alias, or ChatModel instance
            session_id: Session ID for persistence
            language: User language
            file_list: List of uploaded files
            prompt_name: Name of prompt template to use
            **kargs: Additional keyword arguments
            
        Yields:
            Stream chunks in unified format
        """
        # ===== Early Validation (baseline_agent style) =====
        
        # Validate messages
        if not messages:
            logger.warning("Empty messages received")
            yield {"type": "error", "content": "Empty message"}
            return
        
        if not isinstance(messages, list):
            logger.warning(f"Invalid messages type: {type(messages)}")
            yield {"type": "error", "content": "Invalid messages format"}
            return
        
        # Validate user_id
        if not user_id or not isinstance(user_id, str):
            logger.warning("Invalid or missing user_id")
            yield {"type": "error", "content": "User ID is required"}
            return
        
        logger.info(f"DeepAgent request: session={session_id}, provider={provider}, messages={len(messages)}")

        try:
            # Stage 1: Prepare context (LLM, tools, prompt)
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
            
            # Attach files_data from kargs (set by HTTP adapter)
            if 'files_data' in kargs:
                context.files_data = kargs['files_data']
                logger.info(f"✅ Received {len(context.files_data)} files from HTTP layer (in-memory)")

            # Yield fallback warning if provider was not supported
            if context.fallback_used and context.fallback_message:
                yield {"type": "thinking", "content": context.fallback_message}

            # Stage 2: Build agent (backend, files, middleware)
            context = await self._build_agent(context)

            # Prepare messages for streaming
            # context.messages = self._prepare_messages(context.messages)

            # Initialize streaming components
            context.token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(user_id, context.token_counter)

            # Stage 3: Stream agent response
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
            logger.error(f"Unexpected error: {str(e)}", exc_info=True)
            yield {"type": "error", "content": f"Unexpected error: {str(e)}\n\nCheck logs for details."}
    
    #-------------------------------------------------------------------------
    
    @staticmethod
    def load_llm_clients(llm_client_config: dict[str, Any]) -> dict[str, Any]:
        """
        Load LLM clients from config for DeepAgent.
        
        This method is called by the framework during agent initialization to
        load all configured LLM providers from config.yaml (PROVIDERS_DEEP section).
        
        Uses LangChain's unified init_chat_model interface for consistent client creation.
        
        Args:
            llm_client_config: Dictionary of provider configurations from config.yaml
        
        Returns:
            Dictionary mapping provider names to initialized LangChain ChatModel instances
        """
        from mirobody.utils.config import safe_read_cfg
        
        # ===== Early Configuration Check =====
        
        # Check if any providers are configured
        if not llm_client_config or len(llm_client_config) == 0:
            logger.warning("No LLM providers configured in PROVIDERS_DEEP section")
            logger.warning("Add at least one provider in config.yaml under PROVIDERS_DEEP")
            return {}
        
        # Check if any common API key is available
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
                
                # Log what we're trying to initialize
                model = config.get("model", "unknown")
                llm_type = config.get("llm_type", "openai")
                logger.debug(f"  Model: {model}, Type: {llm_type}")
                
                # Resolve API key
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
                
                # Check model field
                if not model or model == "unknown":
                    logger.warning(f"Provider '{provider_name}' skipped: missing 'model' field")
                    failed.append((provider_name, "Missing 'model'"))
                    continue
                
                # If API key is missing, create placeholder immediately
                if not actual_api_key and api_key_name:
                    # Create placeholder that throws friendly error on ANY attribute access
                    class PlaceholderClient:
                        """Placeholder that throws friendly error immediately when accessed."""
                        def __init__(self, model_name, missing_key, provider_name):
                            # Use object.__setattr__ to bypass __getattribute__
                            object.__setattr__(self, '_model_name', model_name)
                            object.__setattr__(self, '_missing_key', missing_key)
                            object.__setattr__(self, '_provider_name', provider_name)
                            object.__setattr__(self, 'model_name', model_name)
                            object.__setattr__(self, 'model', model_name)
                        
                        def __getattribute__(self, name):
                            # Allow access to internal attributes
                            if name in ('_model_name', '_missing_key', '_provider_name', 'model_name', 'model'):
                                return object.__getattribute__(self, name)
                            
                            # Any other attribute access triggers friendly error via ErrorMessageHandler
                            missing_key = object.__getattribute__(self, '_missing_key')
                            provider_name = object.__getattribute__(self, '_provider_name')
                            
                            error_msg = ErrorMessageHandler.api_key_missing(missing_key, provider_name)
                            raise AttributeError(error_msg)
                    
                    llm_clients[provider_name] = PlaceholderClient(model, api_key_name, provider_name)
                    logger.info(f"Created placeholder for '{provider_name}': {model} (missing {api_key_name})")
                    continue
                
                # API key is present, initialize real client
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
                # Catch any unexpected errors in the outer try block
                logger.error(f"Unexpected error loading provider '{provider_name}': {str(outer_e)}", exc_info=True)
                failed.append((provider_name, str(outer_e)))
        
        # Log summary
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
