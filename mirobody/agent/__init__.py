"""③ Answer: the agent, its tools, and the chat surface.

    agent.py        `MirobodyAgent` — one turn, end to end (LangChain +
                    deepagents)
    registry.py     how `AGENT_DIRS` picks the agent, and how a deployment
                    REPLACES it (there is no switching between agents)
    harness.py      assembling a deepagents agent: the no-subagent profile,
                    the read-only mounts, the middleware stack in order
    prompt.py       the system prompt and the attachment note (+ prompts/)
    tool_loader.py  the MCP tools as LangChain tools (+ tools/)
    hitl.py         `ask_user`, the one human-in-the-loop tool
    checkpointer.py conversation memory (LangGraph, Postgres)
    errors.py       the agent's exceptions, and the one safe way to show one

    models/         the chat model: build it, read it, count its tokens
    filesystem/     the virtual filesystem it reads through
    wire/           LangGraph's stream → what a client renders
    middleware/     fault containment, retry governance, prompt caching
    tools/          the tools, served over `/mcp` and handed to the agent
    chat/           the HTTP chat product: sessions, messages, SSE
    skills/         Agent Skills, mounted read-only at `/skills/`

`models/`, `filesystem/document_backend.py`, `filesystem/naming.py`, `wire/events_bridge.py`,
`harness.py` and `middleware/` are the LIBRARY half — what
`pip install 'mirobody[agent]'` exists for, and what a consumer running its
own agent imports. The rest is this reference agent and its product surface.

Exports resolve lazily (PEP 562), and that is load-bearing: the MCP layer
imports `mirobody.agent.tools.*` to serve the tools, which executes THIS
`__init__` — an eager `from .agent import MirobodyAgent` here would make
`/mcp` require langchain. import-linter cannot see that chain (a submodule
import creates no graph edge to the parent package), so the release workflow's
bare-wheel smoke test is the gate that keeps this honest.
"""

from typing import TYPE_CHECKING

_EXPORTS = {
    "MirobodyAgent": "agent",
}

__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .agent import MirobodyAgent


def __getattr__(name: str):
    if name in _EXPORTS:
        import importlib

        module = importlib.import_module(f".{_EXPORTS[name]}", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
