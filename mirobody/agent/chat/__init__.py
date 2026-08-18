"""Chat service package — the agent layer's conversation surface.

Exports are resolved lazily (PEP 562, the numpy/scipy pattern) instead of
eagerly at package-import time. This matters for the langchain-core-style
layering: engine modules are allowed to import the ENGINE-side leaf that
historically lives in this package (``chat.user_profile`` — the health
profile, slated to migrate engine-side in the package split), and an
eager ``from .service import ChatService`` here would drag the entire agent
stack (and so LangChain) into every such import. With lazy exports,
``import mirobody.agent.chat.user_profile`` touches nothing else, while every
existing ``from mirobody.agent.chat import ChatService`` keeps working unchanged —
it just pays its own import cost at first use.
"""

from typing import TYPE_CHECKING

_AGENT_EXPORTS = (
    "load_agents_from_module",
    "load_agents_from_directory",
    "load_agents_from_directories",
    "get_global_agent_count",
    "get_global_agents",
    "get_agent",
    "get_global_agent",
    "get_llm_client_by_name",
    "detect_language",
)
_USER_CONFIG_EXPORTS = (
    "get_user_mcps",
    "get_user_prompt_by_name",
)

__all__ = [*_AGENT_EXPORTS, "ChatService", *_USER_CONFIG_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .agent import (
        detect_language,
        get_agent,
        get_global_agent,
        get_global_agent_count,
        get_global_agents,
        get_llm_client_by_name,
        load_agents_from_directories,
        load_agents_from_directory,
        load_agents_from_module,
    )
    from .service import ChatService
    from .user_config import get_user_mcps, get_user_prompt_by_name


def __getattr__(name: str):
    if name in _AGENT_EXPORTS:
        from . import agent
        return getattr(agent, name)
    if name == "ChatService":
        from .service import ChatService
        return ChatService
    if name in _USER_CONFIG_EXPORTS:
        from . import user_config
        return getattr(user_config, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
