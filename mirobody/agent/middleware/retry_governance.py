"""A tool that already failed the same way must not be called again.

Three separate things end a turn badly, and only the first two had answers:

* a tool that CRASHES — `ToolFaultMiddleware` turns it into an error result;
* a tool call whose JSON never parsed — `InvalidToolCallRepairMiddleware`
  feeds the parse error back;
* a tool that answered "you are not allowed to read this", after which the
  model rephrases the same request and spends the rest of its budget getting
  the same refusal. That is what this middleware is for.

The rule is `mirobody.kernel.tools.RetryLedger`, and the ledger is consulted BEFORE
the tool runs: a call whose `(tool, normalised arguments)` already returned an
unrecoverable result is refused outright, and one that has run its limit for
this turn is refused with a different reason. Arguments are normalised
(`tools.canonical_key`), so `["a","b"]` and `["b","a"]` are one call and a
reordering does not buy another attempt.

## It reads the artifact, not the text

Whether a result was unrecoverable is `Envelope.error_class`, carried on the
`ToolMessage.artifact`. It is deliberately NOT parsed out of the message text:
a harness may truncate, summarise or evict that text, and governance that
depended on it would silently stop governing at exactly the point in a long
turn where it matters most.

## Where it sits

Inside `ToolFaultMiddleware` (which contains crashes from everything below it,
this middleware included) and outside the rest. deepagents splices user
middleware after its own `FilesystemMiddleware`, so what arrives here is the
raw `ToolMessage` before eviction — which is exactly why reading the artifact
works.
"""

from __future__ import annotations

import asyncio
import logging

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

from mirobody.kernel import tools as tool_kernel
logger = logging.getLogger(__name__)


class RetryGovernanceMiddleware(AgentMiddleware):
    """Refuse a repeat of a call that already failed unrecoverably, and cap how
    many times one call may run in a turn.

    One ledger per agent build (= one turn), guarded by a lock because
    LangGraph may run tool calls concurrently and a ledger that miscounts under
    concurrency is worse than none.
    """

    def __init__(self, *, limit: int) -> None:
        super().__init__()
        self._ledger = tool_kernel.RetryLedger(limit=limit)
        self._lock = asyncio.Lock()

    # --- the hook ------------------------------------------------------------

    async def awrap_tool_call(self, request, handler):
        call = getattr(request, "tool_call", None) or {}
        tool_name = call.get("name") or ""
        args = call.get("args") if isinstance(call.get("args"), dict) else {}
        if not tool_name:
            return await handler(request)

        async with self._lock:
            allowed, reason = self._ledger.allows(tool_name, args)
        if not allowed:
            logger.info("refused repeat call to %s (%s)", tool_name, reason)
            return _refusal(self._ledger, call, tool_name, reason)

        result = await handler(request)
        async with self._lock:
            self._ledger.record(tool_name, args, error_class=_error_class(result))
        return result

    def wrap_tool_call(self, request, handler):
        """The synchronous path. The ledger is the same object; the lock is
        not taken because a synchronous tool call cannot interleave."""
        call = getattr(request, "tool_call", None) or {}
        tool_name = call.get("name") or ""
        args = call.get("args") if isinstance(call.get("args"), dict) else {}
        if not tool_name:
            return handler(request)

        allowed, reason = self._ledger.allows(tool_name, args)
        if not allowed:
            logger.info("refused repeat call to %s (%s)", tool_name, reason)
            return _refusal(self._ledger, call, tool_name, reason)
        result = handler(request)
        self._ledger.record(tool_name, args, error_class=_error_class(result))
        return result


def _refusal(ledger, call: dict, tool_name: str, reason: str) -> ToolMessage:
    """The refusal the model reads, and the envelope governance reads. Both say
    the same thing, so a harness that truncates one leaves the other intact."""
    return ToolMessage(
        content=ledger.refusal_text(tool_name, reason),
        name=tool_name,
        tool_call_id=call.get("id") or "",
        status="error",
        artifact=tool_kernel.repeated_call(tool_name, reason),
    )


def _error_class(result: object) -> str | None:
    """`recoverable` / `unrecoverable` / `None`, from the result's artifact.

    A tool that returns no envelope (every native filesystem tool) is simply
    counted, never marked dead: absence of an artifact is absence of a claim,
    not a claim of success.
    """
    artifact = getattr(result, "artifact", None)
    if isinstance(artifact, tool_kernel.Envelope):
        return artifact.error_class
    if isinstance(artifact, dict):
        value = artifact.get("error_class")
        return value if value in (tool_kernel.ERROR_RECOVERABLE, tool_kernel.ERROR_UNRECOVERABLE) else None
    return None


__all__ = ["RetryGovernanceMiddleware"]
