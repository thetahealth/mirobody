"""Keep previous-turn genotype tool rows out of the next model request.

The checkpointer retains real ToolMessages, including their content. A new
turn may therefore replay an old genotype table without calling the bounded
query tool again. This middleware keeps only results produced by this agent
build (one turn), and accounts for every kept result at the model boundary.
"""

from __future__ import annotations

import hashlib
import logging
import posixpath
import threading
from collections.abc import Sequence
from typing import Any

from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from mirobody.agent.middleware.empty_answer import NUDGE_NAME
from mirobody.agent.middleware.model_budget import LAST_CALL_NAME
from mirobody.agent.tools.genetic_service import TOOL_NAME
from mirobody.kernel import tools

logger = logging.getLogger(__name__)

_REDACTED = "Genotype result from a previous turn was removed. Call query_genetic_data again for current facts."
_REDACTED_ANSWER = "Previous answer used genotype data. Query the active upload again for current facts."
_SCRATCH_REFUSAL = "Scratch files are unavailable after a genotype query. Use query_genetic_data for current facts."
_READ_FILES = frozenset({"read_file", "grep", "glob", "ls"})
_WRITE_FILES = frozenset({"write_file", "edit_file"})
_READ_MOUNTS = ("/uploads", "/library", "/memories")
#: Human messages the harness writes into a turn; a turn starts at the person's.
_HARNESS_NAMES = frozenset({NUDGE_NAME, LAST_CALL_NAME})


def _digest(content: Any) -> bytes:
    return hashlib.sha256(repr(content).encode("utf-8")).digest()


def _redacted_answer(message: AIMessage) -> AIMessage:
    # A model may write a copied genotype into a later tool-call argument.
    return message.model_copy(update={"content": _REDACTED_ANSWER,
                                      "tool_calls": [], "invalid_tool_calls": [],
                                      "additional_kwargs": {}, "response_metadata": {}})


def _redacted_tool(message: ToolMessage) -> ToolMessage:
    return message.model_copy(update={"content": _REDACTED, "artifact": None,
                                      "response_metadata": {}})


def _genetic_call_ids(messages: Sequence[Any]) -> set[str]:
    return {
        str(call.get("id"))
        for message in messages if isinstance(message, AIMessage)
        for call in message.tool_calls
        if call.get("name") == TOOL_NAME and call.get("id")
    }


def _is_genetic_result(message: Any, call_ids: set[str]) -> bool:
    return isinstance(message, ToolMessage) and (
        message.name == TOOL_NAME or message.tool_call_id in call_ids
    )


def redact_genotype_history(messages: Sequence[Any]) -> list[Any]:
    """Remove genotype results and the answers derived from them before memory writes.

    DeepAgents summarizes with a separate model and offloads old messages to
    ``/conversation_history``. Both paths run before the ordinary model-call
    row guard, so they need the same boundary independently.
    """
    redacted: list[Any] = []
    dependent_answer = False
    genetic_ids = _genetic_call_ids(messages)
    for message in messages:
        # A turn starts at the person's message, not at the harness's own
        # (the empty-answer nudge, the last-call instruction), or the answer
        # after it went unredacted.
        if isinstance(message, HumanMessage) and message.name not in _HARNESS_NAMES:
            dependent_answer = False
        if _is_genetic_result(message, genetic_ids):
            redacted.append(_redacted_tool(message))
            dependent_answer = True
        elif isinstance(message, ToolMessage) and dependent_answer:
            redacted.append(_redacted_tool(message))
        elif isinstance(message, AIMessage) and dependent_answer:
            redacted.append(_redacted_answer(message))
        else:
            redacted.append(message)
    return redacted


class GenotypeRowGuardMiddleware(AgentMiddleware):
    """One guard per agent build, with a row ledger for its current turn."""

    def __init__(self) -> None:
        super().__init__()
        self._current: dict[str, tuple[int, bytes]] = {}
        self._genetic_seen = False
        self._lock = threading.Lock()

    def _record(self, result: Any) -> None:
        if not isinstance(result, ToolMessage):
            return
        with self._lock:
            self._genetic_seen = True
        artifact = result.artifact
        if not isinstance(artifact, tools.Envelope):
            return
        with self._lock:
            self._current[result.tool_call_id] = (artifact.meta.row_count, _digest(result.content))

    def mark_genetic_seen(self) -> None:
        with self._lock:
            self._genetic_seen = True

    def _guard_messages(self, messages: Sequence[Any]) -> tuple[list[Any], int, int, int, int]:
        with self._lock:
            current = dict(self._current)
        returned_rows = sum(rows for rows, _ in current.values())
        visible_rows = redacted = redacted_answers = 0
        seen: set[str] = set()
        guarded = []
        last_user = max((i for i, message in enumerate(messages) if isinstance(message, HumanMessage)),
                        default=len(messages))
        genetic_ids = _genetic_call_ids(messages)
        prior_genetic_result = False
        for index, message in enumerate(messages):
            if isinstance(message, AIMessage) and prior_genetic_result and index < last_user:
                guarded.append(_redacted_answer(message))
                redacted_answers += 1
                continue
            if (isinstance(message, ToolMessage) and prior_genetic_result
                    and index < last_user and not _is_genetic_result(message, genetic_ids)):
                guarded.append(_redacted_tool(message))
                redacted += 1
                continue
            if not _is_genetic_result(message, genetic_ids):
                guarded.append(message)
                continue
            call_id = message.tool_call_id
            record = current.get(call_id)
            if record is None or call_id in seen or _digest(message.content) != record[1]:
                guarded.append(_redacted_tool(message))
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
        genetic_ids = _genetic_call_ids(request.messages)
        if any(_is_genetic_result(message, genetic_ids) for message in request.messages):
            with self._lock:
                self._genetic_seen = True
        messages, visible_count, returned_count, redacted_count, prior_answers_count = self._guard_messages(request.messages)
        if returned_count or redacted_count or prior_answers_count:
            logger.info("genotype model row budget: visible=%d returned=%d redacted=%d prior_answers=%d",
                        visible_count, returned_count, redacted_count, prior_answers_count)
        return request.override(messages=messages) if redacted_count or prior_answers_count else request

    def _scratch_refusal(self, request: Any) -> ToolMessage | None:
        call = getattr(request, "tool_call", None) or {}
        name = call.get("name") or ""
        if name not in _READ_FILES | _WRITE_FILES:
            return None
        with self._lock:
            genetic_seen = self._genetic_seen
        if not genetic_seen:
            return None
        if name in _READ_FILES:
            args = call.get("args") if isinstance(call.get("args"), dict) else {}
            path = args.get("file_path") or args.get("path") or ""
            if isinstance(path, str) and path.startswith("/"):
                normalized = posixpath.normpath(path)
                if any(normalized == mount or normalized.startswith(mount + "/") for mount in _READ_MOUNTS):
                    return None
        return ToolMessage(content=_SCRATCH_REFUSAL, name=name,
                           tool_call_id=call.get("id") or "", status="error")

    async def awrap_tool_call(self, request: Any, handler: Any) -> Any:
        refusal = self._scratch_refusal(request)
        if refusal is not None:
            return refusal
        result = await handler(request)
        if (getattr(request, "tool_call", None) or {}).get("name") == TOOL_NAME:
            self._record(result)
        return result

    def wrap_tool_call(self, request: Any, handler: Any) -> Any:
        refusal = self._scratch_refusal(request)
        if refusal is not None:
            return refusal
        result = handler(request)
        if (getattr(request, "tool_call", None) or {}).get("name") == TOOL_NAME:
            self._record(result)
        return result

    async def awrap_model_call(self, request: Any, handler: Any) -> Any:
        return await handler(self._guard_request(request))

    def wrap_model_call(self, request: Any, handler: Any) -> Any:
        return handler(self._guard_request(request))


__all__ = ["GenotypeRowGuardMiddleware"]
