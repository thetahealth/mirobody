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

from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import AIMessage, ToolMessage
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


#-----------------------------------------------------------------------------

# The retry hint the model sees for each unparseable call. Mentioning
# `none` specifically because that is the observed failure: claude-sonnet-4.6
# (via OpenRouter, temperature 0.1) deterministically emitted
# `{"aggregate": none, ...}` for query_health_indicators — Python's None
# instead of JSON null / the quoted enum string "none".
_INVALID_CALL_HINT = (
    "Your call to this tool was DROPPED: the arguments were not valid JSON. "
    "Common causes: Python literals (none/None/True/False) instead of JSON "
    "(null/true/false), or an unquoted enum value (write \"none\", not none). "
    "Re-issue the call with strictly valid JSON arguments."
)

# One malformed call is worth two retries; a model that keeps emitting broken
# JSON is not going to be argued into correctness, and each retry costs a full
# model call. Counted per turn by scanning this turn's repair ToolMessages.
_MAX_REPAIRS_PER_TURN = 2


class InvalidToolCallRepairMiddleware(AgentMiddleware):
    """A malformed tool call must never end the conversation either.

    ``ToolFaultMiddleware`` above catches tools that CRASH — but a tool call
    whose arguments fail JSON parsing never reaches any tool. LangChain parks
    it in ``AIMessage.invalid_tool_calls``; the react loop routes on
    ``tool_calls`` alone, sees none, and ends the graph. The user gets
    "Answer Completed" over an empty message, and nothing is logged.

    This hook answers each unparseable call with an error ``ToolMessage``
    (the OpenAI wire format requires every emitted tool_call id to be
    answered anyway) and jumps back to the model, which then re-issues the
    call correctly. Scoped to the pure-invalid case — when valid calls exist
    alongside, ToolNode must stay the next node, and it resolves the turn.
    """

    @hook_config(can_jump_to=["model"])
    def after_model(self, state, runtime):  # noqa: ARG002 — runtime is the hook signature
        messages = state.get("messages") or []
        last = messages[-1] if messages else None
        if not isinstance(last, AIMessage):
            return None

        invalid = last.invalid_tool_calls or []
        if not invalid or last.tool_calls:
            return None

        # Count this turn's previous repairs (scan back to the last non-repair
        # boundary: any message that is not one of our error ToolMessages and
        # not an AIMessage that only carried invalid calls).
        repairs = 0
        for msg in reversed(messages[:-1]):
            if isinstance(msg, ToolMessage) and msg.name == "__invalid_tool_call__":
                repairs += 1
            elif isinstance(msg, AIMessage) and not msg.tool_calls and msg.invalid_tool_calls:
                continue
            else:
                break

        if repairs >= _MAX_REPAIRS_PER_TURN:
            logger.error(
                "invalid tool calls persisted after %d repairs; giving up: %s",
                repairs,
                [c.get("name") for c in invalid],
            )
            return None

        logger.warning(
            "repairing invalid tool call(s): %s",
            [{"name": c.get("name"), "error": c.get("error")} for c in invalid],
        )

        return {
            "messages": [
                ToolMessage(
                    content=json.dumps(
                        {
                            "error": f"call to {call.get('name') or 'unknown tool'} "
                                     "had malformed JSON arguments",
                            "hint": _INVALID_CALL_HINT,
                        },
                        ensure_ascii=False,
                    ),
                    # Marker name: how the repair counter above recognises its
                    # own messages. Never a real tool name.
                    name="__invalid_tool_call__",
                    tool_call_id=call.get("id") or "invalid_tool_call",
                    status="error",
                )
                for call in invalid
            ],
            "jump_to": "model",
        }
