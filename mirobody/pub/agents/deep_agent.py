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

from .utils import (
    StreamConverter,
    TokenUsageCallback,
    build_system_prompt,
    DeepAgentError,
    ConfigError,
)
from .deep.middleware import UniversalPromptCachingMiddleware

# DeepAgent's default LLM provider when none is specified by the caller.
_DEFAULT_PROVIDER_DEEP = "gemini-3.5-flash"

if TYPE_CHECKING:
    from langchain_core.language_models import BaseChatModel
    from .deep.backend import PostgresBackend

logger = logging.getLogger(__name__)


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
        self.default_provider = safe_read_cfg("DEFAULT_PROVIDER_DEEP") or _DEFAULT_PROVIDER_DEEP
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
    
    async def _build_backend(
        self, session_id: str, user_id: str, file_list: list[dict[str, Any]] | None = None,
    ) -> tuple[Any, list | None]:
        """Build the deepagents virtual filesystem.

        With a ``user_id``: a 5-mount ``CompositeBackend`` over the scope-based
        ``deep_agent_workspace`` table (deepagents-native fs tools operate on it):

          default        → workspace (scope='workspace', session=cur)  scratch, rw
          /memories/...  → memory    (scope='memory',    session='')   cross-session, rw
          /uploads/...   → uploads   (scope='uploads',   session=cur)  this request's files, ro
          /library/...   → library   (scope='library',   session='')   file history, ro

        ``/uploads/`` and ``/library/`` are auto-populated from ``th_files`` via
        ``register_blob`` (pointers — no byte copy; parsed text inlined for grep,
        bytes surfaced multimodally on read). Anonymous calls fall back to
        ``StateBackend``. Returns ``(backend, permissions)``.
        """
        if not user_id:
            from deepagents.backends import StateBackend
            return StateBackend(), None

        from deepagents.backends import CompositeBackend
        from deepagents.middleware.filesystem import FilesystemPermission
        from .deep.backend import PgFilesystemBackend

        workspace = PgFilesystemBackend(user_id=user_id, session_id=session_id or "", scope="workspace")
        memory = PgFilesystemBackend(user_id=user_id, session_id="", scope="memory")
        uploads = PgFilesystemBackend(user_id=user_id, session_id=session_id or "", scope="uploads")
        library = PgFilesystemBackend(user_id=user_id, session_id="", scope="library")
        charts = PgFilesystemBackend(user_id=user_id, session_id=session_id or "", scope="charts")

        # Mirror this request's uploads first, then history (excluding those keys).
        try:
            session_keys = await _sync_session_uploads(uploads, user_id=user_id, file_list=file_list)
            await _sync_user_library(library, user_id=user_id, exclude_file_keys=session_keys)
        except Exception as exc:
            logger.warning(f"upload/library mirroring failed: {exc}", exc_info=True)

        backend = CompositeBackend(
            default=workspace,
            routes={
                "/memories/": memory,
                "/uploads/": uploads,
                "/library/": library,
                "/charts/": charts,
            },
        )
        permissions = [
            FilesystemPermission(operations=["write"], paths=["/uploads/**"], mode="deny"),
            FilesystemPermission(operations=["write"], paths=["/library/**"], mode="deny"),
        ]
        return backend, permissions


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
        llm_client, model_name, fallback_used, fallback_msg = await self._init_llm_client(provider, self.agent_identifier)

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
            # Build the deepagents virtual filesystem (CompositeBackend). User
            # uploads + history are auto-mounted at /uploads/ and /library/ via
            # register_blob — the agent reads them with the native read_file tool
            # (multimodal for pdf/image/…). No custom file MCP tools, no E2B.
            backend, permissions = await self._build_backend(session_id, user_id, file_list)

            from deepagents import create_deep_agent

            # In-process JS/TS interpreter (langchain-quickjs). Adds an `eval`
            # tool — a persistent REPL — replacing the E2B remote sandbox for
            # compute. Caching middleware runs last so its decision wins.
            middleware: list[Any] = []
            try:
                from langchain_quickjs import CodeInterpreterMiddleware
                middleware.append(CodeInterpreterMiddleware())
            except Exception as exc:
                logger.warning(f"code interpreter middleware unavailable: {exc}")
            middleware.append(
                UniversalPromptCachingMiddleware(ttl="5m", unsupported_model_behavior="ignore")
            )

            agent_kwargs: dict[str, Any] = dict(
                model=llm_client,
                tools=tools,
                system_prompt=system_prompt,
                backend=backend,
                middleware=middleware,
            )
            if permissions is not None:
                agent_kwargs["permissions"] = permissions

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
            if files_data:
                logger.info(f"Processing {len(files_data)} files")

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

            agent, backend, final_messages = await self._build_agent(
                session_id=session_id,
                user_id=user_id,
                llm_client=llm_client,
                system_prompt=system_prompt,
                tools=loaded_tools,
                messages=messages,
                file_list=file_list,
                files_data=files_data,
            )

            token_counter = TokenUsageCallback()
            stream_config = self._create_stream_config(user_id, token_counter)

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


# ── uploads / library mirroring (th_files -> deepagents filesystem) ──────────
# Pointers only: register_blob stores object_storage_key (= th_files.file_key)
# + inlined parsed text; raw bytes stay in object storage and are surfaced
# multimodally on read. No byte duplication into Postgres.

_MAX_SESSION_UPLOAD_POINTERS = 50
_MAX_USER_LIBRARY_POINTERS = 200


def _safe_basename(file_name: str) -> str | None:
    """Flatten a th_files name to a path-safe basename (no separators/leading dots)."""
    safe = str(file_name or "").replace("/", "_").replace("\\", "_").lstrip(".")
    return safe or None


async def _sync_session_uploads(uploads_backend, *, user_id: str, file_list) -> set:
    """Mirror this request's attached files into /uploads/ (read-only).

    Uses the request ``file_list`` (file_key + file_name); enriches each with the
    th_files parse cache (decrypted original_text / content_hash / size). Returns
    the set of file_keys registered, for exclusion from /library/.
    """
    from ...utils.db import execute_query
    from .deep.parser import guess_mime

    items = [f for f in (file_list or []) if isinstance(f, dict) and f.get("file_key")]
    if not items:
        return set()
    keys = [str(f["file_key"]) for f in items][:_MAX_SESSION_UPLOAD_POINTERS]

    rowmap: dict[str, dict] = {}
    try:
        in_clause = ", ".join(f":k{i}" for i in range(len(keys)))
        params = {f"k{i}": k for i, k in enumerate(keys)}
        params["uid"] = str(user_id)
        rows = await execute_query(
            query=f"""
            SELECT file_key, decrypt_content(file_name) as file_name, file_type,
                   content_hash, decrypt_content(original_text) as original_text, text_length
            FROM th_files
            WHERE user_id = :uid AND file_key IN ({in_clause}) AND is_del = false
            """,
            params=params,
        )
        for r in (rows or []):
            rowmap[str(r.get("file_key"))] = dict(r)
    except Exception as exc:
        logger.warning(f"sync_session_uploads query failed: {exc}")

    registered: set = set()
    for f in items:
        key = str(f["file_key"])
        row = rowmap.get(key, {})
        name = _safe_basename(f.get("file_name") or row.get("file_name") or key)
        if not name:
            continue
        try:
            err = await uploads_backend.register_blob(
                path=f"/{name}",
                object_storage_key=key,
                content_hash=str(row.get("content_hash") or ""),
                content_size=int(row.get("text_length") or 0),
                mime_type=guess_mime(name),
                parsed_text=(row.get("original_text") or None),
                file_key=key,
                source="user_upload",
            )
            if err is None:
                registered.add(key)
        except Exception:
            logger.warning(f"sync_session_uploads register failed for {name}", exc_info=True)

    logger.info(f"sync_session_uploads: user={user_id} registered={len(registered)}/{len(items)}")
    return registered


async def _sync_user_library(library_backend, *, user_id: str, exclude_file_keys: set) -> None:
    """Mirror the user's parsed file history (excluding this request's keys) into /library/ (read-only)."""
    from ...utils.db import execute_query
    from .deep.parser import guess_mime

    params: dict[str, Any] = {"uid": str(user_id), "limit": _MAX_USER_LIBRARY_POINTERS}
    exclusion = ""
    if exclude_file_keys:
        ph = ", ".join(f":ex{i}" for i in range(len(exclude_file_keys)))
        exclusion = f"AND file_key NOT IN ({ph})"
        for i, k in enumerate(exclude_file_keys):
            params[f"ex{i}"] = str(k)

    try:
        rows = await execute_query(
            query=f"""
            SELECT file_key, decrypt_content(file_name) as file_name, file_type,
                   content_hash, decrypt_content(original_text) as original_text, text_length
            FROM th_files
            WHERE user_id = :uid AND is_del = false
              AND original_text IS NOT NULL AND original_text <> ''
              {exclusion}
            ORDER BY created_at DESC LIMIT :limit
            """,
            params=params,
        )
    except Exception as exc:
        logger.warning(f"sync_user_library query failed: {exc}")
        return

    seen: set = set()
    count = 0
    for r in (rows or []):
        key = str(r.get("file_key") or "")
        base = _safe_basename(r.get("file_name") or key)
        if not base:
            continue
        name = base if base not in seen else f"{base}__thf_{key[:8]}"
        seen.add(name)
        try:
            err = await library_backend.register_blob(
                path=f"/{name}",
                object_storage_key=key,
                content_hash=str(r.get("content_hash") or ""),
                content_size=int(r.get("text_length") or 0),
                mime_type=guess_mime(name),
                parsed_text=(r.get("original_text") or None),
                file_key=key,
                source="user_upload",
            )
            if err is None:
                count += 1
        except Exception:
            logger.warning(f"sync_user_library register failed for {base}", exc_info=True)

    logger.info(f"sync_user_library: user={user_id} registered={count}")
