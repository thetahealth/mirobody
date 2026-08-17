"""One tool's failure must never end the conversation.

LangGraph's ``ToolNode`` ships ``handle_tool_errors=_default_handle_tool_errors``,
which RE-RAISES anything that isn't an argument-validation error. So an exception
inside any tool — a bad row, a timed-out HTTP call, a driver hiccup — propagates
out of the graph and takes the whole turn with it:

  1. the SSE stream dies mid-answer, so the user gets a truncated reply;
  2. the raw driver text (a full SQLAlchemy/psycopg dump, SQL and bound
     parameters included) can reach end users.

This middleware turns any such fault into an ordinary error ``ToolMessage``: the
model sees "that tool failed", can apologise or try another route, and the
conversation survives. It wraps EVERY tool the agent has — global MCP tools, the
user's own MCP tools, and deepagents' native filesystem tools — including ones
added later, which is why it lives here rather than as a decorator on individual
tools.

NOT caught, deliberately:

  * ``GraphBubbleUp`` (``GraphInterrupt`` et al.) — how ``interrupt()`` suspends a
    run. DeepAgent passes no ``interrupt_on`` today, but swallowing these would
    silently break any approval flow added later.
  * ``asyncio.CancelledError`` — a ``BaseException``, so ``except Exception``
    misses it by construction; a disconnected client must still cancel the run.

Ported from the a007-mirovital agent (``agent/tool_faults.py``).
"""

import json
import logging

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.errors import GraphBubbleUp

logger = logging.getLogger(__name__)


def _fault_message(request, exc: Exception) -> ToolMessage:
    """The error ``ToolMessage`` the model sees in place of a crashed tool result.

    Carries the tool name and the exception TYPE only. This text goes into the
    model's context and models have historically echoed such strings to users
    verbatim, so it must stay free of SQL, connection strings, and payload
    fragments — the full traceback goes to the log instead.
    """
    call = getattr(request, "tool_call", None) or {}
    name = call.get("name") or "unknown_tool"
    logger.exception("tool %s failed: %s", name, type(exc).__name__, exc_info=exc)
    return ToolMessage(
        content=json.dumps(
            {
                "error": f"{name} failed to run ({type(exc).__name__})",
                "hint": "This tool is temporarily unavailable. Continue with what you "
                        "already have, or try a different tool or a narrower request — "
                        "do not repeat this exact call, and never show this message to "
                        "the user verbatim.",
            },
            ensure_ascii=False,
        ),
        name=name,
        tool_call_id=call.get("id") or "",
        status="error",
    )


class ToolFaultMiddleware(AgentMiddleware):
    """Contain every tool fault as an error result instead of a dead turn."""

    def wrap_tool_call(self, request, handler):
        try:
            return handler(request)
        except GraphBubbleUp:
            raise
        except Exception as exc:  # containment is the point
            return _fault_message(request, exc)

    async def awrap_tool_call(self, request, handler):
        try:
            return await handler(request)
        except GraphBubbleUp:
            raise
        except Exception as exc:  # containment is the point
            return _fault_message(request, exc)
