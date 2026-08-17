import io, json, logging, os

from types import SimpleNamespace
from typing import Any, AsyncGenerator, Literal

from google import genai
from openai import AsyncOpenAI

from ...utils import safe_read_cfg

from ...mcp import (
    get_global_functions,
    call_global_tool,
    McpService
)

#-----------------------------------------------------------------------------

class AbstractClient():
    """Abstract base class for all LLM client implementations"""

    def __init__(self, **kwargs) -> None:
        """Initialize the client with configuration parameters"""
        # Default configuration constants
        self._default_max_steps = kwargs.get("default_max_steps",   20)
        self._max_steps_limit   = kwargs.get("max_steps_limit",     100)
        self._min_chunk_size    = kwargs.get("min_chunk_size",      30)
        self._mcp_server_name   = kwargs.get("mcp_server_name",     "theta_health")
        self._http_timeout      = kwargs.get("http_timeout",        30_000)
        self._llm_temperature   = kwargs.get("llm_temperature",     0.1)

        self._input_price       = kwargs.get("input_price",         0.0)
        self._output_price      = kwargs.get("output_price",        0.0)

        # Subclasses set `self._api_key_name` before calling super().__init__().

        # Initialize max_steps from kwargs or use default
        self._max_steps = kwargs.get("max_steps")
        if not self._max_steps or \
            not isinstance(self._max_steps, int) or \
            self._max_steps <= 0:
            self._max_steps = self._default_max_steps
        if self._max_steps >= self._max_steps_limit:
            self._max_steps = self._max_steps_limit

        # Initialize API key from kwargs or environment
        potential_api_key = kwargs.get("api_key")
        if potential_api_key:
            real_api_key = os.environ.get(potential_api_key)
            if real_api_key:
                self._api_key = real_api_key
            else:
                self._api_key = potential_api_key
        else:
            # Use _api_key_name if set by subclass, otherwise empty string
            default_api_key_name = getattr(self, "_api_key_name", "")
            self._api_key = os.environ.get(default_api_key_name) if default_api_key_name else ""

        if self._api_key and not isinstance(self._api_key, str):
            self._api_key = ""

    #-----------------------------------------------------

    def _validate_api_key(self) -> str | None:
        """
        Validate API key configuration.

        Returns:
            Error message if API key is invalid, None otherwise
        """
        if not self._api_key or \
            not isinstance(self._api_key, str) or \
            self._api_key == getattr(self, "_api_key_name", ""):

            api_key_name = getattr(self, "_api_key_name", "API key")
            return f"{api_key_name} is required for {self.__class__.__name__} functionality."
        return None

    def _validate_messages(self, **kwargs) -> tuple[list | None, str | None]:
        """
        Validate messages parameter from kwargs.

        Returns:
            tuple: (messages, error_message)
            If validation succeeds: (messages_list, None)
            If validation fails: (None, error_message)
        """
        messages = kwargs.get("messages")
        if not messages:
            return None, "Empty message."
        if not isinstance(messages, list):
            return None, "Invalid messages."
        return messages, None

    async def _generate_mcp_url(self, user_id: str) -> str:
        """
        Generate complete MCP URL for the given user.

        Args:
            user_id: User ID to generate MCP for

        Returns:
            Complete MCP URL (e.g., "https://example.com/mcp/abc123")
            If MCP is not available, returns empty string
        """
        if not user_id:
            return ""

        mcp_public_url = safe_read_cfg("MCP_PUBLIC_URL").rstrip("/")
        if not mcp_public_url:
            return ""

        mcp_uri, err = await McpService.generate_temporary_personal_mcp(user_id, agent_name="Base")
        if err:
            logging.error(err)
            return ""

        return f"{mcp_public_url}{mcp_uri}"

    async def _build_tools(self, user_id: str, tools: list[str], tool_format: Literal["gemini", "openai", ""] = "gemini") -> list:
        """
        Build tools list, preferring MCP when available, falling back to direct function calls.

        Args:
            user_id: User ID for MCP generation
            tools: List of tool names to include (if None, all tools are included)
            tool_format: Format for tools - "gemini", "openai", or ""

        Returns:
            list: Tools configuration for the LLM
        """
        # Try to use MCP if this client supports it
        supports_mcp = getattr(self, "_supports_mcp", False)
        mcp_url = ""
        if supports_mcp:
            mcp_url = await self._generate_mcp_url(user_id)

        if mcp_url:
            # MCP is available - use it!
            if tool_format == "gemini":
                # Gemini Interactions API format
                # Note: MCP server format doesn't support tool-level filtering at the protocol level,
                # but the MCP server itself should only expose the filtered tools
                return [{
                    "type": "mcp_server",
                    "name": self._mcp_server_name,
                    "url": mcp_url
                }]
            else:
                # OpenAI Responses API format
                return [{
                    "type": "mcp",
                    "server_label": self._mcp_server_name,
                    "server_url": mcp_url,
                    "require_approval": {
                        "never": {
                            "tool_names": tools
                        }
                    }
                }]

        # MCP not available - use direct function calls
        functions = get_global_functions(style="" if tool_format == "gemini" else tool_format)
        tools_set = set(tools) if tools else set()

        return [f for f in functions if f.get("function", {}).get("name") in tools_set or f.get("name") in tools_set]

    #-----------------------------------------------------

    async def ainvoke(self, **kwargs) -> AsyncGenerator[dict[str, Any], None]:
        """
        Generate streaming responses from the LLM.

        Args:
            **kwargs: Configuration parameters including messages, prompt, etc.

        Yields:
            dict: Response chunks with type and content
        """
        raise NotImplementedError("Subclasses must implement ainvoke()")

#-----------------------------------------------------------------------------

class OpenAIResponsesClient(AbstractClient):
    """OpenAI-compatible Responses API client — the BaseAgent workhorse.

    BaseAgent's admission rule (and why there is no Chat Completions client
    left in this file): BaseAgent exists to hand our MCP server to the
    PROVIDER's own agent loop over a Responses-style API. A provider that only
    offers Chat Completions is consumed through DeepAgent's LangChain stack
    instead — a local function-call loop here would just duplicate that, worse.

    Subclasses target any /responses-speaking endpoint by overriding the class
    attributes below (see DeepSeekResponsesClient, DashScopeClient).
    """

    _API_KEY_NAME  = "OPENAI_API_KEY"
    _KEY_HINT      = "You can create one from OpenAI https://platform.openai.com/api-keys"
    _BASE_URL      = None                # None → the SDK default (api.openai.com)
    _DEFAULT_MODEL = "gpt-5-nano"
    _SUPPORTS_MCP  = True                # provider-side MCP execution
    _STATEFUL      = True                # previous_response_id + store

    def __init__(self, **kwargs):
        self._api_key_name = self._API_KEY_NAME
        super().__init__(**kwargs)

        self._model    = kwargs.get("model", self._DEFAULT_MODEL)
        self._base_url = kwargs.get("base_url", self._BASE_URL)
        self._supports_mcp = self._SUPPORTS_MCP
        self._supports_previous_response_id = self._STATEFUL
        self._redis = kwargs.get("redis")

    #-----------------------------------------------------

    async def ainvoke(self, **kwargs) -> AsyncGenerator[dict[str, Any], None]:
        # Validate API key using base class method
        error = self._validate_api_key()
        if error:
            yield {"type": "error", "content": f"{error} {self._KEY_HINT}"}
            return

        # Validate messages using base class method
        messages, error = self._validate_messages(**kwargs)
        if error:
            yield {"type": "error", "content": error}
            return

        # Build system instruction from prompt parameter
        prompt = kwargs.get("prompt")
        instructions = prompt if prompt and isinstance(prompt, str) else ""

        user_id = kwargs.get("user_id", "")

        # Build tools using base class method (prioritizes MCP)
        tools = await self._build_tools(user_id, kwargs.get("tools", []), tool_format="")

        #-------------------------------------------------

        if self._supports_previous_response_id and not self._redis:
            self._redis = kwargs.get("redis")

        _redis_key = f"responses:{self._model}:{user_id}" if user_id else ""
        previous_response_id = None
        if self._supports_previous_response_id and _redis_key and self._redis:
            try:
                previous_response_id = await self._redis.get(_redis_key)
            except Exception as e:
                logging.warning(str(e))

        # Initialize token counters
        input_tokens = 0
        output_tokens = 0
        reasoning_tokens = 0
        total_tokens = 0

        # Pass the resolved key explicitly: AsyncOpenAI() alone reads only the
        # OPENAI_API_KEY env var, which silently ignored per-provider `api_key`
        # config (validated above but never used) and broke every subclass.
        client = AsyncOpenAI(api_key=self._api_key, base_url=self._base_url)

        steps = 0
        if previous_response_id:
            for msg in reversed(messages):
                if msg.get("role") == "user":
                    conversation_input = [msg]
                    break
            else:
                conversation_input = messages.copy()
        else:
            conversation_input = messages.copy()

        cached_text = io.StringIO()

        response_id = None

        try:
            while True:
                try:
                    create_kwargs = {
                        "model"        : self._model,
                        "tools"        : tools if tools else None,
                        "input"        : conversation_input,
                        "instructions" : instructions,
                        "stream"       : True,
                    }
                    if self._supports_previous_response_id:
                        # Only stateful providers get `store`: DeepSeek's
                        # stateless /responses always answers `store: false`.
                        create_kwargs["store"] = True
                    if previous_response_id:
                        create_kwargs["previous_response_id"] = previous_response_id
                        previous_response_id = None  # only first request

                    stream = await client.responses.create(**create_kwargs)

                    # Track function calls in this response
                    pending_function_calls = {}  # id -> {name, arguments}
                    response_output = []  # To store output items for next turn

                    async for event in stream:
                        if event.type == "response.output_text.delta":
                            cached_text.write(event.delta)
                            if cached_text.tell() >= self._min_chunk_size:
                                yield {"type": "reply", "content": cached_text.getvalue()}
                                cached_text.seek(0)
                                cached_text.truncate(0)

                        elif event.type == "response.output_text.done":
                            if cached_text:
                                if cached_text.tell() > 0:
                                    yield {"type": "reply", "content": cached_text.getvalue()}
                                    cached_text.seek(0)
                                    cached_text.truncate(0)

                        elif event.type == "response.reasoning_text.delta":
                            # DeepSeek streams chain-of-thought this way;
                            # OpenAI models never emit the event.
                            if event.delta:
                                yield {"type": "thinking", "content": event.delta}

                        elif event.type == "response.output_item.added":
                            if event.item.type == "function_call":
                                pending_function_calls[event.item.id] = {
                                    "id"        : event.item.call_id,
                                    "name"      : event.item.name,
                                    "arguments" : ""
                                }
                                yield {"type": "queryTitle", "content": event.item.name, "tool_id": event.item.id}

                        elif event.type == "response.function_call_arguments.done":
                            if event.item_id in pending_function_calls:
                                pending_function_calls[event.item_id]["arguments"] += event.arguments
                            yield {"type": "queryArguments", "content": event.arguments, "tool_id": event.item_id}

                        elif event.type == "response.mcp_call_arguments.done":
                            yield {"type": "queryArguments", "content": event.arguments, "tool_id": event.item_id}

                        elif event.type == "response.mcp_call.completed":
                            # MCP calls are handled by the server, just yield the result
                            if hasattr(event, "result") and event.result:
                                yield {"type": "queryDetail", "content": json.dumps(event.result, ensure_ascii=False), "tool_id": event.item_id}

                        elif event.type == "response.output_item.done":
                            # Store completed output items for potential next turn
                            response_output.append(event.item)
                            # The done item carries the final arguments; DeepSeek
                            # does not emit response.function_call_arguments.done,
                            # so this is where its tool arguments materialize.
                            if event.item.type == "function_call" and event.item.id in pending_function_calls:
                                fc_info = pending_function_calls[event.item.id]
                                if event.item.arguments and not fc_info["arguments"]:
                                    fc_info["arguments"] = event.item.arguments
                                    yield {"type": "queryArguments", "content": event.item.arguments, "tool_id": event.item.id}

                        elif event.type == "response.completed":
                            response_id = event.response.id
                            input_tokens += event.response.usage.input_tokens
                            output_tokens += event.response.usage.output_tokens
                            if event.response.usage.output_tokens_details:
                                reasoning_tokens += event.response.usage.output_tokens_details.reasoning_tokens or 0
                            total_tokens += event.response.usage.total_tokens

                        elif event.type == "response.failed":
                            err = getattr(event.response, "error", None)
                            yield {"type": "error", "content": str(err) if err else "response failed"}

                    # After stream completes, execute any pending function calls
                    if pending_function_calls:
                        steps += 1
                        if steps > self._max_steps:
                            yield {"type": "error", "content": "Too many steps."}
                            break

                        # Execute function calls and collect results
                        function_results = []
                        for item_id, fc_info in pending_function_calls.items():
                            fc_id   = fc_info["id"]
                            fc_name = fc_info["name"]
                            try:
                                fc_args = json.loads(fc_info["arguments"]) if fc_info["arguments"] else {}
                            except json.JSONDecodeError:
                                fc_args = {}

                            # Execute the function
                            fc_result = await call_global_tool(fc_name, fc_args, user_id)
                            try:
                                fc_result_text = json.dumps(fc_result, ensure_ascii=False)
                                yield {"type": "queryDetail", "content": fc_result_text, "tool_id": item_id}
                            except Exception as e:
                                logging.warning(str(e))
                                fc_result_text = str(fc_result)

                            function_results.append({
                                "type"      : "function_call_output",
                                "call_id"   : fc_id,
                                "output"    : fc_result_text
                            })

                        # Prepare the next turn. This used to send ONLY
                        # response_output + function_results with no
                        # previous_response_id, so the provider never saw the
                        # original user turn again and answered tool results
                        # out of context.
                        if self._supports_previous_response_id and response_id:
                            # Stateful chain: the server holds the history.
                            previous_response_id = response_id
                            conversation_input = function_results
                        else:
                            # Stateless (DeepSeek) or ZDR: accumulate locally.
                            conversation_input = conversation_input + response_output + function_results
                    else:
                        # No function calls, we're done
                        break

                except Exception as e:
                    if "previous_response_id" in str(e) and "unsupported_parameter" in str(e):
                        logging.warning(f"previous_response_id not supported (ZDR), retrying without it")
                        self._supports_previous_response_id = False
                        conversation_input = messages.copy()
                        if _redis_key and self._redis:
                            try:
                                await self._redis.delete(_redis_key)
                            except Exception:
                                pass
                        continue
                    logging.error(str(e))
                    yield {"type": "error", "content": str(e)}
                    break

            # Yield final cost statistics
            content = {
                "model": self._model,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "thought_tokens": reasoning_tokens,
                "total_tokens": total_tokens
            }

            content["total_cost"] = (input_tokens * self._input_price + (reasoning_tokens + output_tokens) * self._output_price) / 1e6

            yield {"type": "costStatistics", "content": content}

            if self._supports_previous_response_id and _redis_key and self._redis and response_id:
                try:
                    await self._redis.set(_redis_key, response_id, ex=12*60*60)
                except Exception as e:
                    logging.warning(str(e))
        finally:
            cached_text.close()

#-----------------------------------------------------------------------------

class GeminiClient(AbstractClient):
    """Google Gemini API client with Interactions API support"""

    def __init__(self, **kwargs):
        self._api_key_name = "GOOGLE_API_KEY"
        super().__init__(**kwargs)

        self._model = kwargs.get("model", "gemini-2.5-flash")
        self._supports_mcp = False  # gemini-3-flash-preview MCP native calling is unreliable
        self._redis = kwargs.get("redis")


    @staticmethod
    def _sanitize_for_gemini(obj):
        """Recursively replace empty lists/dicts with None — Gemini rejects empty collections."""
        if isinstance(obj, dict):
            return {k: GeminiClient._sanitize_for_gemini(v) for k, v in obj.items()} or None
        if isinstance(obj, list):
            return [GeminiClient._sanitize_for_gemini(v) for v in obj] if obj else None
        return obj

    @staticmethod
    def _turns_to_steps(input_turns: list) -> list:
        """Convert the internal turn-list ([{role, content}]) into the step-list
        ([{type, content}]) the Interactions API expects.

        A user/model turn becomes a user_input/model_output step; any
        function_result item nested in a turn's content is promoted to its own
        top-level function_result step (it is a step, not a content item).
        """
        steps: list = []
        for turn in input_turns:
            role    = turn.get("role") or "user"
            content = turn.get("content", "")

            if isinstance(content, str):
                items = [{"type": "text", "text": content}] if content else []
            elif isinstance(content, list):
                items = content
            else:
                items = []

            regular: list = []
            for item in items:
                if isinstance(item, dict) and item.get("type") == "function_result":
                    steps.append({
                        "type":    "function_result",
                        "call_id": item.get("call_id", ""),
                        "name":    item.get("name", ""),
                        "result":  item.get("result"),
                    })
                else:
                    regular.append(item)

            if regular:
                steps.append({
                    "type":    "user_input" if role == "user" else "model_output",
                    "content": regular,
                })

        return steps

    @staticmethod
    async def _adapt_steps_stream(stream):
        """Translate the step.* SSE events into the flat content.* event
        vocabulary the consumer loop below is written against.

        Local function calls are read from interaction.completed (where their
        arguments are guaranteed complete) and re-emitted as function_call deltas
        just before the complete event, so the loop executes the call during the
        stream and continues at completion.
        """
        async for event in stream:
            event_type = getattr(event, "event_type", None)

            if event_type == "interaction.created":
                interaction = getattr(event, "interaction", None)
                yield SimpleNamespace(
                    event_type  = "interaction.start",
                    interaction = SimpleNamespace(id=getattr(interaction, "id", None)),
                )

            elif event_type == "step.start":
                # Server-executed tool steps surface here with complete payloads.
                step  = getattr(event, "step", None)
                stype = getattr(step, "type", None)
                if stype == "mcp_server_tool_call":
                    yield SimpleNamespace(event_type="content.delta", delta=SimpleNamespace(
                        type      = "mcp_server_tool_call",
                        name      = getattr(step, "name", ""),
                        id        = getattr(step, "id", ""),
                        arguments = getattr(step, "arguments", {}) or {},
                    ))
                elif stype == "mcp_server_tool_result":
                    yield SimpleNamespace(event_type="content.delta", delta=SimpleNamespace(
                        type    = "mcp_server_tool_result",
                        result  = getattr(step, "result", ""),
                        call_id = getattr(step, "call_id", ""),
                    ))

            elif event_type == "step.delta":
                delta = getattr(event, "delta", None)
                if delta is not None and getattr(delta, "type", None) == "text":
                    yield SimpleNamespace(event_type="content.delta", delta=SimpleNamespace(
                        type = "text",
                        text = getattr(delta, "text", ""),
                    ))

            elif event_type == "interaction.completed":
                interaction = getattr(event, "interaction", None)
                for step in (getattr(interaction, "steps", None) or []):
                    if getattr(step, "type", None) == "function_call":
                        yield SimpleNamespace(event_type="content.delta", delta=SimpleNamespace(
                            type      = "function_call",
                            name      = getattr(step, "name", ""),
                            id        = getattr(step, "id", ""),
                            arguments = getattr(step, "arguments", {}) or {},
                        ))
                yield SimpleNamespace(event_type="interaction.complete", interaction=interaction)

            elif event_type == "error":
                yield event

    #-----------------------------------------------------

    async def ainvoke(self, **kwargs) -> AsyncGenerator[dict[str, Any], None]:
        # Get GCP Vertex AI config at first
        gcp_project = os.environ.get("GOOGLE_CLOUD_PROJECT")
        gcp_location= os.environ.get("GOOGLE_CLOUD_LOCATION")

        use_vertexai= os.environ.get("GOOGLE_GENAI_USE_VERTEXAI")
        if gcp_project and gcp_location and use_vertexai and use_vertexai.lower() in ("true", "1"):
            use_vertexai = True
        else:
            use_vertexai = False

        if not use_vertexai:
            # And then check user-defined API key
            error = self._validate_api_key()
            if error:
                yield {"type": "error", "content": f"{error} You can create one from Google https://makersuite.google.com/app/apikey"}
                return

        # Validate messages using base class method
        messages, error = self._validate_messages(**kwargs)
        if error:
            yield {"type": "error", "content": error}
            return

        # Convert messages to TurnParam format
        input_turns = []
        for message in messages:
            role = "user" if message["role"] == "user" else "model"
            input_turns.append({"role": role, "content": message["content"]})

        # Get file info for native Gemini file access
        file_infos = kwargs.get("file_infos", [])

        # Inject files into the last user message for Gemini native access
        # Gemini only supports: HTTPS URLs, gs:// (Cloud Storage), YouTube URLs, or File API URIs
        if file_infos and input_turns:
            for i in range(len(input_turns) - 1, -1, -1):
                if input_turns[i].get("role") == "user":
                    original_content = input_turns[i].get("content", "")
                    # Convert to multimodal content: [text, image/document, ...]
                    # Gemini format: {"type": "image"|"document"|"video"|"audio", "uri": url, "mime_type": ...}
                    file_parts = []
                    for f in file_infos:
                        url = f.get("url")
                        mime_type = f.get("mime_type", "")
                        if not url:
                            continue
                        # Only use URLs that Gemini supports (HTTPS, gs://, youtube)
                        if not (url.startswith("https://") or url.startswith("gs://")):
                            logging.warning(f"⚠️ Skipping unsupported URL for Gemini: {url[:50]}...")
                            continue
                        # Determine type from MIME type
                        if mime_type.startswith("image/"):
                            part_type = "image"
                        elif mime_type.startswith("video/"):
                            part_type = "video"
                        elif mime_type.startswith("audio/"):
                            part_type = "audio"
                        else:
                            part_type = "document"  # PDF, docx, etc.
                        file_parts.append({"type": part_type, "uri": url, "mime_type": mime_type})
                    if file_parts:
                        input_turns[i]["content"] = [
                            {"type": "text", "text": original_content},
                            *file_parts
                        ]
                        logging.info(f"📎 Injected {len(file_parts)} file(s) for Gemini native access")
                    break

        # Build system instruction from prompt parameter
        prompt = kwargs.get("prompt")
        system_instruction = prompt if prompt and isinstance(prompt, str) else ""

        user_id = kwargs.get("user_id", "")
        session_id = kwargs.get("session_id", "")
        tool_names = kwargs.get("tools", [])

        #-------------------------------------------------

        if not self._redis:
            self._redis = kwargs.get("redis")

        if use_vertexai:
            async for chunk in self._ainvoke_vertex(
                gcp_project         = gcp_project,
                gcp_location        = gcp_location,
                input_turns         = input_turns,
                system_instruction  = system_instruction,
                tool_names          = tool_names,
                user_id             = user_id,
                session_id          = session_id,
            ):
                yield chunk
        else:
            async for chunk in self._ainvoke_interactions(
                input_turns         = input_turns,
                system_instruction  = system_instruction,
                tool_names          = tool_names,
                user_id             = user_id,
                session_id          = session_id,
            ):
                yield chunk

    #-----------------------------------------------------

    async def _ainvoke_interactions(
        self,
        *,
        input_turns         : list,
        system_instruction  : str,
        tool_names          : list[str],
        user_id             : str,
        session_id          : str,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """AI Studio path via Interactions API (stateful, supports MCP)."""
        logging.info(f"GeminiClient: AI Studio / Interactions path, model={self._model}, tools={len(tool_names) if tool_names else 0}")

        # Key intentionally scopes by user_id only (no session_id): we want the
        # stateful interaction chain to persist across sessions for the same user.
        # Known limitation: concurrent requests from the same user race on this key
        # and may fork/interleave the server-side chain. Acceptable while the UX
        # serializes requests per user (one in-flight reply at a time).
        _redis_key = f"gemini:interaction:{self._model}:{user_id}" if user_id else ""
        previous_interaction_id = None
        if _redis_key and self._redis:
            try:
                previous_interaction_id = await self._redis.get(_redis_key)
            except Exception as e:
                logging.warning(str(e))

        # Keep full history so we can rebuild the request if the cached interaction
        # is stale on the server side and we need to fall back to a stateless call.
        original_input_turns = input_turns
        if previous_interaction_id and input_turns:
            for i in range(len(input_turns) - 1, -1, -1):
                if input_turns[i].get("role") == "user":
                    input_turns = [input_turns[i]]
                    break

        # Interactions API uses OpenAI-like flat format: {"type": "function", "name": ..., ...}
        tools = await self._build_tools(user_id, tool_names, tool_format="gemini")

        # Fallback tools without MCP, for when MCP causes malformed_function_call
        tools_set = set(tool_names) if tool_names else set()
        tools_fallback = [f for f in get_global_functions(style="") if f.get("name") in tools_set]

        # Initialize token counters
        input_tokens = 0
        output_tokens = 0
        thought_tokens = 0
        total_tokens = 0

        steps = 0
        interaction_id = None
        retries = 0
        max_retries = 2
        retry_hint = ""
        stale_interaction_recovered = False

        client = genai.Client(
            api_key     = self._api_key,
            http_options= {"timeout": self._http_timeout}
        )

        while True:
            try:
                # Create interaction with streaming
                create_kwargs = {
                    "model": self._model,
                    "input": GeminiClient._turns_to_steps(input_turns),
                    "stream": True,
                }

                effective_system_instruction = system_instruction + retry_hint if retry_hint else system_instruction
                if effective_system_instruction:
                    create_kwargs["system_instruction"] = effective_system_instruction

                if tools:
                    create_kwargs["tools"] = tools

                if previous_interaction_id:
                    create_kwargs["previous_interaction_id"] = previous_interaction_id

                stream = GeminiClient._adapt_steps_stream(
                    await client.aio.interactions.create(**create_kwargs)
                )

                should_continue = False
                function_call_info = None

                async for event in stream:
                    event_type = getattr(event, "event_type", None)

                    if event_type == "interaction.start":
                        # Get interaction ID
                        interaction = getattr(event, "interaction", None)
                        if interaction:
                            interaction_id = getattr(interaction, "id", None)

                    elif event_type == "content.delta":
                        delta = getattr(event, "delta", None)
                        if not delta:
                            continue

                        delta_type = getattr(delta, "type", None)

                        if delta_type == "text":
                            text = getattr(delta, "text", "")
                            if text:
                                yield {"type": "reply", "content": text}

                        elif delta_type == "mcp_server_tool_call":
                            tool_name = getattr(delta, "name", "")
                            tool_id = getattr(delta, "id", "")
                            yield {"type": "queryTitle", "content": tool_name, "tool_id": tool_id}

                            tool_arguments = getattr(delta, "arguments", {})
                            if tool_arguments:
                                yield {"type": "queryArguments", "content": json.dumps(tool_arguments, ensure_ascii=False), "tool_id": tool_id}

                        elif delta_type == "mcp_server_tool_result":
                            tool_result = getattr(delta, "result", "")
                            if tool_result:
                                if isinstance(tool_result, str):
                                    result_str = tool_result
                                elif hasattr(tool_result, "items"):
                                    result_str = json.dumps(tool_result.items, ensure_ascii=False) if tool_result.items else ""
                                else:
                                    result_str = str(tool_result)

                            tool_id = getattr(delta, "call_id", "")
                            yield {"type": "queryDetail", "content": result_str, "tool_id": tool_id}

                        elif delta_type == "function_call":
                            function_call_name = getattr(delta, "name", "")
                            if function_call_name:
                                function_call_id = getattr(delta, "id", "")
                                function_call_arguments = getattr(delta, "arguments", {})

                                yield {"type": "queryTitle", "content": function_call_name, "tool_id": function_call_id}
                                yield {"type": "queryArguments", "content": json.dumps(function_call_arguments, ensure_ascii=False), "tool_id": function_call_id}

                                # Call the function
                                function_call_result = await call_global_tool(function_call_name, function_call_arguments, user_id, session_id)
                                try:
                                    function_call_result_text = json.dumps(function_call_result, ensure_ascii=False)
                                    yield {"type": "queryDetail", "content": function_call_result_text, "tool_id": function_call_id}
                                except Exception as e:
                                    logging.warning(str(e), exc_info=True)

                                # Store function call info for continuation
                                function_call_info = {
                                    "name": function_call_name,
                                    "call_id": function_call_id,
                                    "result": function_call_result
                                }

                    elif event_type == "interaction.complete":
                        interaction = getattr(event, "interaction", None)
                        if interaction:
                            if hasattr(interaction, "id"):
                                interaction_id = interaction.id

                            if hasattr(interaction, "usage") and interaction.usage:
                                usage = interaction.usage
                                input_tokens += getattr(usage, "total_input_tokens", 0) or 0
                                output_tokens += getattr(usage, "total_output_tokens", 0) or 0
                                thought_tokens += getattr(usage, "total_thought_tokens", 0) or 0
                                total_tokens += getattr(usage, "total_tokens", 0) or 0

                        # Check if we need to continue with function result
                        if function_call_info:
                            steps += 1
                            if steps > self._max_steps:
                                yield {"type": "error", "content": "Too many steps."}
                                break  # Stop the loop when max steps exceeded
                            elif not interaction_id:
                                logging.error("interaction_id is not available for function result")
                                yield {"type": "error", "content": "Failed to get interaction context from Gemini"}
                                break  # Stop the loop on error
                            else:
                                # Prepare for continuation
                                previous_interaction_id = interaction_id
                                input_turns = [{
                                    "role": "user",
                                    "content": [{
                                        "type": "function_result",
                                        "name": function_call_info["name"],
                                        "call_id": function_call_info["call_id"],
                                        "result": GeminiClient._sanitize_for_gemini(function_call_info["result"])
                                    }]
                                }]
                                should_continue = True

                    elif event_type == "error":
                        error = getattr(event, "error", None)
                        if error:
                            message = getattr(error, "message", "Unknown error")
                            yield {"type": "error", "content": message}

                # If we shouldn't continue, break the loop
                if not should_continue:
                    break

            except Exception as e:
                err_str = str(e)
                err_lower = err_str.lower()

                # Recover from stale previous_interaction_id (server-side expired/unknown).
                # Only safe before any function-call continuation (steps == 0), and only once.
                is_stale_interaction = (
                    steps == 0
                    and previous_interaction_id
                    and not stale_interaction_recovered
                    and (
                        "previous_interaction_id" in err_lower
                        or ("interaction" in err_lower and any(
                            k in err_lower for k in ("not found", "not_found", "invalid", "expired", "does not exist")
                        ))
                    )
                )
                if is_stale_interaction:
                    logging.warning(f"stale previous_interaction_id, clearing redis and retrying stateless: {err_str}")
                    if _redis_key and self._redis:
                        try:
                            await self._redis.delete(_redis_key)
                        except Exception:
                            pass
                    previous_interaction_id = None
                    input_turns = original_input_turns
                    stale_interaction_recovered = True
                    continue

                if "malformed_function_call" in err_str and retries < max_retries:
                    retries += 1
                    retry_hint = "\n\nIMPORTANT: Your previous response contained a malformed function call with invalid JSON. You MUST produce strictly valid JSON for all function calls."
                    if retries == 1 and tools:
                        # First retry: ditch MCP, use direct function definitions
                        tools = tools_fallback
                        logging.warning(f"malformed_function_call: switching to function definitions, retry {retries}/{max_retries}")
                    elif retries == 2:
                        # Second retry: no tools at all, answer from knowledge
                        tools = None
                        logging.warning(f"malformed_function_call: dropping all tools, retry {retries}/{max_retries}")
                    continue
                logging.error(err_str, exc_info=True, stack_info=True)
                yield {"type": "error", "content": err_str}
                break

        # Yield final cost statistics
        content = {
            "model": self._model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "thought_tokens": thought_tokens,
            "total_tokens": total_tokens
        }

        content["total_cost"] = (input_tokens * self._input_price + (thought_tokens + output_tokens) * self._output_price) / 1e6

        yield {"type": "costStatistics", "content": content}

        if _redis_key and self._redis and interaction_id:
            try:
                await self._redis.set(_redis_key, interaction_id, ex=12*60*60)
            except Exception as e:
                logging.warning(str(e), exc_info=True)

    #-----------------------------------------------------

    async def _ainvoke_vertex(
        self,
        *,
        gcp_project         : str,
        gcp_location        : str,
        input_turns         : list,
        system_instruction  : str,
        tool_names          : list[str],
        user_id             : str,
        session_id          : str,
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Vertex AI path via models.generate_content_stream — Interactions API is not GA on Vertex."""
        from google.genai import types

        logging.info(f"GeminiClient: Vertex AI path, model={self._model}, project={gcp_project}, location={gcp_location}, tools={len(tool_names) if tool_names else 0}")

        client = genai.Client(
            vertexai    = True,
            project     = gcp_project,
            location    = gcp_location,
            http_options= {"timeout": self._http_timeout},
        )

        # Convert input_turns -> list[types.Content]
        contents: list[types.Content] = []
        for turn in input_turns:
            role = turn.get("role") or "user"
            raw  = turn.get("content", "")
            parts: list[types.Part] = []
            if isinstance(raw, str):
                if raw:
                    parts.append(types.Part.from_text(text=raw))
            elif isinstance(raw, list):
                for item in raw:
                    if not isinstance(item, dict):
                        continue
                    item_type = item.get("type")
                    if item_type == "text":
                        text = item.get("text", "")
                        if text:
                            parts.append(types.Part.from_text(text=text))
                    elif item_type in ("image", "video", "audio", "document") and item.get("uri"):
                        parts.append(types.Part.from_uri(
                            file_uri  = item["uri"],
                            mime_type = item.get("mime_type", ""),
                        ))
            if parts:
                contents.append(types.Content(role=role, parts=parts))

        # Build tools using gemini-flat declarations (no MCP on this path)
        tools_config = None
        if tool_names:
            tools_set = set(tool_names)
            declarations = [
                types.FunctionDeclaration(
                    name                  = f["name"],
                    description           = f.get("description", ""),
                    parameters_json_schema= f.get("parameters"),
                )
                for f in get_global_functions(style="gemini")
                if f.get("name") in tools_set
            ]
            if declarations:
                tools_config = [types.Tool(function_declarations=declarations)]

        config = types.GenerateContentConfig(
            system_instruction        = system_instruction or None,
            tools                     = tools_config,
            temperature               = self._llm_temperature,
            automatic_function_calling= types.AutomaticFunctionCallingConfig(disable=True) if tools_config else None,
        )

        input_tokens   = 0
        output_tokens  = 0
        thought_tokens = 0
        total_tokens   = 0
        steps          = 0

        cached_text = io.StringIO()

        try:
            while True:
                try:
                    stream = await client.aio.models.generate_content_stream(
                        model    = self._model,
                        contents = contents,
                        config   = config,
                    )

                    assistant_parts: list[types.Part] = []
                    pending_calls: list[tuple[str, str, dict]] = []  # (id, name, args)

                    async for chunk in stream:
                        usage = getattr(chunk, "usage_metadata", None)
                        if usage:
                            input_tokens   = getattr(usage, "prompt_token_count", 0) or 0
                            output_tokens  = getattr(usage, "candidates_token_count", 0) or 0
                            thought_tokens = getattr(usage, "thoughts_token_count", 0) or 0
                            total_tokens   = getattr(usage, "total_token_count", 0) or 0

                        if not chunk.candidates:
                            continue

                        for cand in chunk.candidates:
                            if not cand.content or not cand.content.parts:
                                continue
                            for part in cand.content.parts:
                                # Thought parts: keep for context continuity but don't yield
                                if getattr(part, "thought", False):
                                    assistant_parts.append(part)
                                    continue

                                if part.text:
                                    cached_text.write(part.text)
                                    if cached_text.tell() >= self._min_chunk_size:
                                        yield {"type": "reply", "content": cached_text.getvalue()}
                                        cached_text.seek(0)
                                        cached_text.truncate(0)
                                    assistant_parts.append(part)

                                elif part.function_call:
                                    # Flush any buffered text first to preserve order
                                    if cached_text.tell() > 0:
                                        yield {"type": "reply", "content": cached_text.getvalue()}
                                        cached_text.seek(0)
                                        cached_text.truncate(0)

                                    fc = part.function_call
                                    fc_id   = fc.id or fc.name or ""
                                    fc_name = fc.name or ""
                                    fc_args = dict(fc.args) if fc.args else {}
                                    yield {"type": "queryTitle", "content": fc_name, "tool_id": fc_id}
                                    yield {"type": "queryArguments", "content": json.dumps(fc_args, ensure_ascii=False), "tool_id": fc_id}
                                    pending_calls.append((fc_id, fc_name, fc_args))
                                    assistant_parts.append(part)

                    # Flush trailing text
                    if cached_text.tell() > 0:
                        yield {"type": "reply", "content": cached_text.getvalue()}
                        cached_text.seek(0)
                        cached_text.truncate(0)

                    if not pending_calls:
                        break

                    steps += 1
                    if steps > self._max_steps:
                        yield {"type": "error", "content": "Too many steps."}
                        break

                    # Append model turn (text + function calls) and the function responses
                    contents.append(types.Content(role="model", parts=assistant_parts))

                    response_parts: list[types.Part] = []
                    for fc_id, fc_name, fc_args in pending_calls:
                        fc_result = await call_global_tool(fc_name, fc_args, user_id, session_id)
                        try:
                            yield {"type": "queryDetail", "content": json.dumps(fc_result, ensure_ascii=False), "tool_id": fc_id}
                        except Exception as e:
                            logging.warning(str(e), exc_info=True)

                        sanitized = GeminiClient._sanitize_for_gemini(fc_result)
                        response_parts.append(types.Part.from_function_response(
                            name     = fc_name,
                            response = {"result": sanitized},
                        ))

                    contents.append(types.Content(role="user", parts=response_parts))

                except Exception as e:
                    logging.error(str(e), exc_info=True, stack_info=True)
                    yield {"type": "error", "content": str(e)}
                    break

            content_stats = {
                "model"         : self._model,
                "input_tokens"  : input_tokens,
                "output_tokens" : output_tokens,
                "thought_tokens": thought_tokens,
                "total_tokens"  : total_tokens,
            }
            content_stats["total_cost"] = (input_tokens * self._input_price + (thought_tokens + output_tokens) * self._output_price) / 1e6
            yield {"type": "costStatistics", "content": content_stats}
        finally:
            cached_text.close()


#-----------------------------------------------------------------------------

class DeepSeekResponsesClient(OpenAIResponsesClient):
    """DeepSeek Responses API client.

    Verified against https://api-docs.deepseek.com/guides/responses_api/
    (2026-08): the API is STATELESS — `previous_response_id` is unsupported and
    every response carries `store: false` — so the tool loop accumulates the
    full conversation locally. Tool types are `function` plus the server-side
    `web_search` only; there is NO provider-side MCP, so our tools run through
    the local function-call fallback. Chain-of-thought streams as
    `response.reasoning_text.delta` (handled by the base class), and tool
    arguments only materialize on `response.output_item.done` (ditto).
    """

    _API_KEY_NAME  = "DEEPSEEK_API_KEY"
    _KEY_HINT      = "You can create one from DeepSeek https://platform.deepseek.com"
    _BASE_URL      = "https://api.deepseek.com"
    _DEFAULT_MODEL = "deepseek-v4-flash"
    _SUPPORTS_MCP  = False
    _STATEFUL      = False

#-----------------------------------------------------------------------------

class DashScopeClient(OpenAIResponsesClient):
    """Aliyun 百炼 (Model Studio) Responses API client.

    DashScope implements the OpenAI Responses API at
    /compatible-mode/v1/responses with stateful chaining (`previous_response_id`
    valid for 7 days, `store` defaults to true) — verified against
    https://www.alibabacloud.com/help/en/model-studio/qwen-api-via-openai-responses
    (2026-08). This replaced the old Chat Completions implementation, whose
    local function-call loop duplicated DeepAgent.

    _SUPPORTS_MCP is False for a TRANSPORT reason, not a capability one:
    DashScope's `type: "mcp"` tool accepts `server_protocol: "sse"` only
    ("Currently, only sse is supported" — model-studio/mcp doc), while our
    public MCP endpoint speaks streamable HTTP and serves no SSE route. Flip
    it on the day either side gains the other transport; the payload shape is
    {type, server_protocol, server_label, server_url, headers}, NOT OpenAI's
    {type, server_label, server_url, require_approval}.

    The default model must stay on a NEW-backend model: on this endpoint
    `qwen-plus` and `qwen-flash` route to a legacy Agent backend that maps
    `function_call_output` input items to a role-"tool" message and then
    rejects it ("tool must be one of user,assistant,system,function"), which
    kills every tool round-trip. `qwen3.5-flash` routes to the current backend
    and passed the full live loop — chained continuation, reasoning deltas,
    cross-request previous_response_id — on 2026-08-17.
    """

    _API_KEY_NAME  = "DASHSCOPE_API_KEY"
    _KEY_HINT      = "You can create one from Alibaba Cloud Model Studio https://bailian.console.aliyun.com"
    _BASE_URL      = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    _DEFAULT_MODEL = "qwen3.5-flash"
    _SUPPORTS_MCP  = False
    _STATEFUL      = True

#-----------------------------------------------------------------------------
