"""The chat surface: sessions, messages, the SSE adapter and `ChatService`.

Exports resolve lazily (PEP 562): `mirobody.agent.chat.model` and
`mirobody.agent.chat.session` are imported by the server layer on their own,
and an eager `from .service import ChatService` here would pull the whole agent
stack (and so LangChain) into every such import.
"""

from typing import TYPE_CHECKING

__all__ = ["ChatService"]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .service import ChatService


def __getattr__(name: str):
    if name == "ChatService":
        from .service import ChatService
        return ChatService
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
