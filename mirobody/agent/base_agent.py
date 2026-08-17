"""BaseAgent — provider-native MCP integration (base-level agent)."""

import asyncio
import datetime
import functools
import importlib.resources
import logging
from typing import Any, AsyncGenerator
from zoneinfo import ZoneInfo

import redis.asyncio
from jinja2 import Environment

from ..utils import global_config
from .chat import get_llm_client_by_name, detect_language
from ..mcp import get_global_tools


from .base.clients import (
    OpenAIResponsesClient,
    GeminiClient,
    DeepSeekResponsesClient,
    DashScopeClient,
)
from ..utils.tasks import spawn


def _get_default_timezone() -> str:
    # Deferred import: `mirobody.utils.config` pulls heavy chat-layer deps.
    from mirobody.utils.config import get_default_timezone
    return get_default_timezone()


@functools.lru_cache(maxsize=1)
def _load_base_prompt_template() -> str:
    """Read `agent/prompts/base.jinja` once, cache for subsequent calls."""
    return (
        importlib.resources.files("mirobody")
        .joinpath("agent/prompts/base.jinja")
        .read_text(encoding="utf-8")
    )


def _resolve_prompt_template(agent_name: str) -> str:
    """`PROMPTS_<AGENT>` if the deployment configured one, else the packaged file.

    `config.yaml` has shipped an empty `PROMPTS_BASE:` next to a working
    `PROMPTS_DEEP:` — a knob wired to nothing, because this agent read
    `base.jinja` directly and never consulted config. Setting it did nothing,
    silently, which is worse than not offering it. Now it is real, and
    `/api/prompts?agent=base` advertises exactly what this returns.

    BaseAgent has no per-request prompt selection (`generate_response` takes no
    `prompt_name` — the provider runs the tool loop, so there is no place to
    branch), so a deployment listing several templates gets the first. Not
    cached: config is loaded once at startup, and re-reading a dict per turn is
    cheaper than a cache that goes stale on reload.
    """
    cfg = global_config()
    if cfg is not None:
        options = cfg.get_options_for_agent(agent_name.lower())
        templates = options.get("prompt_templates") if isinstance(options, dict) else None
        if templates:
            for name, text in templates.items():
                if text:
                    logging.info(f"BaseAgent using configured prompt template: {name}")
                    return text
    return _load_base_prompt_template()


#-----------------------------------------------------------------------------

class BaseAgent():
    """
    Base-level health assistant agent — provider-native MCP integration.

    This agent speaks Responses-style APIs only — Chat Completions providers
    are DeepAgent's job (its LangChain loop already does local function
    calling; duplicating it here is what the deleted OpenAIChatClient family
    used to do):
    - OpenAI Responses API (provider-side MCP, stateful)
    - Gemini Interactions API (mcp_server delta, stateful)
    - DeepSeek Responses API (stateless, local function-call fallback)
    - Aliyun DashScope Responses API (stateful; function fallback until
      DashScope MCP speaks streamable HTTP)

    Mirobody's most minimal agent: no LangChain agent loop, no middleware
    stack — the provider does the work and we stream-process events.

    Key Features:
    - Multi-turn conversation with context management
    - Tool filtering (allow/disallow lists)
    - Automatic language detection and response
    - File attachment support (images, documents)
    - Streaming responses for better UX

    Example:
        >>> agent = BaseAgent(
        ...     user_id="user123",
        ...     allowed_tools=["query_health_indicators", "resolve_indicator"],
        ...     user_message_threshold=5
        ... )
        >>> async for chunk in agent.generate_response(
        ...     messages=[{"role": "user", "content": "What's my blood pressure trend?"}],
        ...     provider="gemini-2.5-flash"
        ... ):
        ...     print(chunk)
    """

    def __init__(
        self,
        user_id                 : str | None = None,
        allowed_tools           : list[str] | None = None,
        disallowed_tools        : list[str] | None = None,
        user_message_threshold  : int | None = None,
        **kwargs
    ):
        self._agent_name             = "Base"
        self._default_provider       = "gemini-2.5-flash"
        self._user_id                = user_id
        self._user_message_threshold = user_message_threshold or 5

        # Build effective tool list: allowed set (or all global tools), minus disallowed.
        self._tools = list(allowed_tools) if allowed_tools else list(get_global_tools().keys())
        if disallowed_tools:
            self._tools = [name for name in self._tools if name not in disallowed_tools]

        self._redis: redis.asyncio.Redis | None = None

    #-------------------------------------------------------------------------

    @staticmethod
    def _normalize_messages(messages: list[Any]) -> list[dict[str, Any]]:
        """BaseAgent works in plain {role, content} dicts; its provider clients and
        `_trim_to_recent_user_turns` assume that shape. The shared chat adapter only
        hands canonical LangChain BaseMessage objects to DeepAgent-family agents, but
        normalize defensively here so a stray BaseMessage can never crash BaseAgent
        (it degrades to text — BaseAgent never used the structured tool trace)."""
        from langchain_core.messages import BaseMessage
        _ROLE = {"human": "user", "ai": "assistant", "tool": "tool", "system": "system"}
        out: list[dict[str, Any]] = []
        for m in messages:
            if isinstance(m, BaseMessage):
                content = m.content
                if not isinstance(content, str):
                    content = str(content)
                out.append({"role": _ROLE.get(getattr(m, "type", ""), "user"), "content": content})
            elif isinstance(m, dict):
                out.append(m)
            # silently drop anything else (shouldn't happen)
        return out

    def _trim_to_recent_user_turns(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Keep only the last `_user_message_threshold` user turns (and everything after)."""
        seen = 0
        for i in range(len(messages) - 1, -1, -1):
            if messages[i].get("role") == "user":
                seen += 1
                if seen >= self._user_message_threshold:
                    return messages[i:] if i > 0 else messages
        return messages

    async def generate_response(
        self,
        messages        : list[dict[str, Any]],
        question        : str | None = None,
        file_list       : list[dict[str, Any]] | None = None,
        provider        : str | Any | None = None,
        **kwargs
    ) -> AsyncGenerator[dict[str, Any], None]:
        if not messages:
            yield {"type": "error", "content": "Empty message."}
            return

        messages = self._normalize_messages(messages)
        messages = self._trim_to_recent_user_turns(messages)

        timezone = kwargs.get("timezone") or _get_default_timezone()

        # Extract file info for Gemini native access (URL + MIME type)
        file_infos = [
            {
                "url": f.get("file_url"),
                "mime_type": f.get("file_type", ""),
                "file_key": f.get("file_key", ""),
            }
            for f in (file_list or []) if f.get("file_url")
        ]

        # Also upload to workspace for MCP tools (read_file, etc.)
        from .utils import handle_file_upload
        spawn(
            handle_file_upload(
                file_list=file_list,
                session_id=kwargs.get("session_id", ""),
                user_id=self._user_id,
                files_data=kwargs.get("files_data"),
            )
        )

        prompt = Environment().from_string(_resolve_prompt_template(self._agent_name)).render(
            agent_name="Theta",
            language=detect_language(question),
            current_time=datetime.datetime.now(ZoneInfo(timezone)).strftime("%Y-%m-%d %H:%M:%S %z"),
        )

        #-------------------------------------------------

        if not self._redis:
            self._redis = await global_config().get_redis().get_async_client()

        #-------------------------------------------------

        llm_client = get_llm_client_by_name(self._agent_name, provider)
        if not llm_client:
            llm_client = get_llm_client_by_name(self._agent_name, self._default_provider)

        if not llm_client:
            yield {"type": "error", "content": f"provider {provider} not found"}

        else:
            # BaseAgent is the thinnest derivation over the MCP tool surface:
            # a minimal prompt, the provider's own agent loop, no server-side
            # charting or interception. The consuming client (Claude Desktop,
            # ChatGPT, a custom MCP host) brings its own visualization and
            # sandbox; the stream passes through untouched.
            async for chunk in llm_client.ainvoke(
                question        = question,
                messages        = messages,
                prompt          = prompt,
                user_id         = self._user_id,
                session_id      = kwargs.get("session_id", ""),
                file_infos      = file_infos,  # Pass file info for Gemini native access
                tools           = self._tools,  # Pass pre-filtered tool names
                redis           = self._redis,
            ):
                yield chunk

    #-------------------------------------------------------------------------

    @staticmethod
    def load_llm_clients(llm_client_config: dict[str, Any]) -> dict[str, Any]:
        llm_clients = {}

        for provider_name, provider_kwargs in llm_client_config.items():
            if provider_name.startswith("gemini"):
                llm_clients[provider_name] = GeminiClient(**provider_kwargs)

            elif provider_name.startswith("gpt"):
                llm_clients[provider_name] = OpenAIResponsesClient(**provider_kwargs)

            elif provider_name.startswith("deepseek"):
                llm_clients[provider_name] = DeepSeekResponsesClient(**provider_kwargs)

            elif provider_name.startswith("qwen") or provider_name.startswith("dashscope"):
                llm_clients[provider_name] = DashScopeClient(**provider_kwargs)

        return llm_clients

#-----------------------------------------------------------------------------
