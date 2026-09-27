"""Keep previous-turn genotype tool rows out of the next model request.

The checkpointer retains real ToolMessages, including their content. A new
turn may therefore replay an old genotype table without calling the bounded
query tool again. This middleware keeps only results produced by this agent
build (one turn), and accounts for every kept result at the model boundary.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Sequence
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from mirobody.agent.tools.genetic_service import TOOL_NAME
from mirobody.kernel import tools

logger = logging.getLogger(__name__)

_REDACTED = "Genotype result from a previous turn was removed. Call query_genetic_data again for current facts."
_REDACTED_ANSWER = "Previous answer used genotype data. Query the active upload again for current facts."


def _digest(content: Any) -> bytes:
    return hashlib.sha256(repr(content).encode("utf-8")).digest()


class GenotypeRowGuardMiddleware(AgentMiddleware):
    """One guard per agent build, with a row ledger for its current turn."""

    def __init__(self) -> None:
        super().__init__()
        self._current: dict[str, tuple[int, bytes]] = {}
        self._lock = threading.Lock()

    def _record(self, result: Any) -> None:
        if not isinstance(result, ToolMessage) or result.name != TOOL_NAME:
            return
        artifact = result.artifact
        if not isinstance(artifact, tools.Envelope):
            return
        with self._lock:
            self._current[result.tool_call_id] = (artifact.meta.row_count, _digest(result.content))

    def _guard_messages(self, messages: Sequence[Any]) -> tuple[list[Any], int, int, int, int]:
        with self._lock:
            current = dict(self._current)
        returned_rows = sum(rows for rows, _ in current.values())
        visible_rows = redacted = redacted_answers = 0
        seen: set[str] = set()
        guarded = []
        last_user = max((i for i, message in enumerate(messages) if isinstance(message, HumanMessage)),
                        default=len(messages))
        prior_genetic_result = False
        for index, message in enumerate(messages):
            if isinstance(message, AIMessage) and prior_genetic_result and index < last_user:
                guarded.append(message.model_copy(update={"content": _REDACTED_ANSWER}))
                redacted_answers += 1
                continue
            if not isinstance(message, ToolMessage) or message.name != TOOL_NAME:
                guarded.append(message)
                continue
            call_id = message.tool_call_id
            record = current.get(call_id)
            if record is None or call_id in seen or _digest(message.content) != record[1]:
                guarded.append(message.model_copy(update={"content": _REDACTED, "artifact": None}))
                redacted += 1
                if index < last_user:
                    prior_genetic_result = True
                continue
            seen.add(call_id)
            visible_rows += record[0]
            guarded.append(message)
        if visible_rows > returned_rows:
            raise RuntimeError("genotype row budget exceeded")
        return guarded, visible_rows, returned_rows, redacted, redacted_answers

    def _guard_request(self, request: Any) -> Any:
        messages, visible_count, returned_count, redacted_count, prior_answers_count = self._guard_messages(request.messages)
        if returned_count or redacted_count or prior_answers_count:
            logger.info("genotype model row budget: visible=%d returned=%d redacted=%d prior_answers=%d",
                        visible_count, returned_count, redacted_count, prior_answers_count)
        return request.override(messages=messages) if redacted_count or prior_answers_count else request

    async def awrap_tool_call(self, request: Any, handler: Any) -> Any:
        result = await handler(request)
        if (getattr(request, "tool_call", None) or {}).get("name") == TOOL_NAME:
            self._record(result)
        return result

    def wrap_tool_call(self, request: Any, handler: Any) -> Any:
        result = handler(request)
        if (getattr(request, "tool_call", None) or {}).get("name") == TOOL_NAME:
            self._record(result)
        return result

    async def awrap_model_call(self, request: Any, handler: Any) -> Any:
        return await handler(self._guard_request(request))

    def wrap_model_call(self, request: Any, handler: Any) -> Any:
        return handler(self._guard_request(request))


__all__ = ["GenotypeRowGuardMiddleware"]
