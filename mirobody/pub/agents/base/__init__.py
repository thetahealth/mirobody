"""BaseAgent support sub-package.

Provider client classes consumed by `mirobody.pub.agents.base_agent.BaseAgent`.
Mirrors how `deep_agent.py` consumes `deep/` and `mix_agent.py` consumes `mix/`.
"""

from .clients import (
    AbstractClient,
    OpenAIResponsesClient,
    GeminiClient,
    MiroThinkerClient,
    OpenAIChatClient,
    OpenRouterClient,
    NebulaClient,
    DashScopeClient,
    DoubaoClient,
)

__all__ = [
    "AbstractClient",
    "OpenAIResponsesClient",
    "GeminiClient",
    "MiroThinkerClient",
    "OpenAIChatClient",
    "OpenRouterClient",
    "NebulaClient",
    "DashScopeClient",
    "DoubaoClient",
]
