"""BaseAgent support sub-package.

Provider client classes plus BaseAgent's history-replay strategy
(`history_replay.py`), consumed by `mirobody.agent.base_agent.BaseAgent`.
Mirrors how `deep_agent.py` consumes `deep/`.

Exports resolve lazily (PEP 562), and that is load-bearing rather than style —
same reason as `agent/__init__.py` and `agent/chat/__init__.py`. Importing a
SUBMODULE executes this `__init__` first, so an eager `from .clients import …`
here made `from ..base.history_replay import fold_trace_into_text` — two
dependency-free functions the chat layer needs on the BaseAgent path — drag in
the google-genai and openai SDKs: measured at 1929 modules for a module that
imports only json/datetime. With lazy exports that import costs what it looks
like it costs, and every `from mirobody.agent.base import GeminiClient` keeps
working unchanged.
"""

from typing import TYPE_CHECKING

_CLIENT_EXPORTS = (
    "AbstractClient",
    "OpenAIResponsesClient",
    "GeminiClient",
    "DeepSeekResponsesClient",
    "DashScopeClient",
)

__all__ = [*_CLIENT_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .clients import (
        AbstractClient,
        DashScopeClient,
        DeepSeekResponsesClient,
        GeminiClient,
        OpenAIResponsesClient,
    )


def __getattr__(name: str):
    if name in _CLIENT_EXPORTS:
        from . import clients

        return getattr(clients, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
