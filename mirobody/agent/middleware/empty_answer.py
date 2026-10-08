"""A reply with no answer text and no tool call must not end the turn blank.

A reasoning model can spend a whole response in its reasoning channel and
return `content=""` with no tool call: the react loop sees nothing to run and
ends the graph, and the person gets "Answer Completed" over an empty message.
Measured on local models (2026-09-22): Bonsai-27B did it once in four requests,
MiniCPM5-2B on its first multi-round question (`finish_reason=length`, 8,496
characters of reasoning, 0 of answer).

The empty message is dropped and the model is asked once, by name, for the
answer. A second empty reply ends the turn as the empty turn it is: the chat
turn (`chat/turn.py`) then writes its localized "no answer" line and records
`finish_reason=empty`. Raising instead showed "The service hit an internal
error (RuntimeError)" with that line under it, and logged a traceback.

The nudge is the harness talking, not the person: it is removed from the
history once the turn has its answer or has given up, so a later turn does
not read it and the genotype redaction boundary (a person's message) is not
moved by it.
"""

import logging

from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage

from mirobody.agent.middleware.model_budget import LAST_CALL_NAME
from mirobody.agent.models.messages import message_reasoning, message_text

logger = logging.getLogger(__name__)

#: `HumanMessage.name` of the nudge, so the counter below finds its own messages.
NUDGE_NAME = "__empty_answer__"

_NUDGE = (
    "Your last reply had no answer text. Write the answer to the person now, "
    "from the tool results above. Call a tool only if something essential is missing."
)

_MAX_NUDGES_PER_TURN = 1


def _nudges(messages: list) -> list:
    """This turn's nudges, since the person's last message. The budget's
    last-call instruction is the harness speaking too, and kept."""
    found = []
    for msg in reversed(messages):
        if isinstance(msg, HumanMessage):
            if msg.name == NUDGE_NAME:
                found.append(msg)
            elif msg.name != LAST_CALL_NAME:
                break
    return found


class EmptyAnswerRepairMiddleware(AgentMiddleware):
    """Drops an empty final reply and asks for the answer once; a second empty
    reply ends the turn empty, for the chat turn to say so."""

    @hook_config(can_jump_to=["model"])
    def after_model(self, state, runtime):
        messages = state.get("messages") or []
        last = messages[-1] if messages else None
        if not isinstance(last, AIMessage) or last.tool_calls or last.invalid_tool_calls:
            return None
        nudges = _nudges(messages[:-1])
        cleanup = [RemoveMessage(id=m.id) for m in nudges if m.id]
        if message_text(last).strip():
            return {"messages": cleanup} if cleanup else None

        finish_reason = (last.response_metadata or {}).get("finish_reason")
        reasoning_len = len(message_reasoning(last))
        if len(nudges) >= _MAX_NUDGES_PER_TURN:
            logger.warning("empty answer after %d nudge(s), ending the turn empty: finish_reason=%s reasoning_chars=%d",
                           len(nudges), finish_reason, reasoning_len)
            return {"messages": cleanup} if cleanup else None

        logger.warning("empty answer, asking again: finish_reason=%s reasoning_chars=%d", finish_reason, reasoning_len)
        removal = [RemoveMessage(id=last.id)] if last.id else []
        return {"messages": [*removal, HumanMessage(content=_NUDGE, name=NUDGE_NAME)], "jump_to": "model"}
