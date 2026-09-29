"""A reply with no answer text and no tool call must not end the turn blank.

A reasoning model can spend a whole response in its reasoning channel and
return `content=""` with no tool call: the react loop sees nothing to run and
ends the graph, and the person gets "Answer Completed" over an empty message.
Measured on local models (2026-09-22): Bonsai-27B did it once in four requests,
MiniCPM5-2B on its first multi-round question (`finish_reason=length`, 8,496
characters of reasoning, 0 of answer).

The empty message is dropped and the model is asked once, by name, for the
answer. A second empty reply raises, which the streaming loop turns into an
`error` event the client renders, as `InvalidToolCallRepairMiddleware` does
when its budget runs out.
"""

import logging

from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage

from mirobody.agent.models.messages import message_reasoning, message_text

logger = logging.getLogger(__name__)

#: `HumanMessage.name` of the nudge, so the counter below finds its own messages.
NUDGE_NAME = "__empty_answer__"

_NUDGE = (
    "Your last reply had no answer text. Write the answer to the person now, "
    "from the tool results above. Call a tool only if something essential is missing."
)

_MAX_NUDGES_PER_TURN = 1


class EmptyAnswerRepairMiddleware(AgentMiddleware):
    """Drops an empty final reply and asks for the answer once, then gives up loudly."""

    @hook_config(can_jump_to=["model"])
    def after_model(self, state, runtime):
        messages = state.get("messages") or []
        last = messages[-1] if messages else None
        if not isinstance(last, AIMessage) or last.tool_calls or last.invalid_tool_calls:
            return None
        if message_text(last).strip():
            return None

        nudge_count = 0
        for msg in reversed(messages[:-1]):
            if isinstance(msg, HumanMessage):
                if msg.name != NUDGE_NAME:
                    break
                nudge_count += 1

        finish_reason = (last.response_metadata or {}).get("finish_reason")
        reasoning_len = len(message_reasoning(last))
        if nudge_count >= _MAX_NUDGES_PER_TURN:
            logger.error("empty answer after %d nudge(s): finish_reason=%s reasoning_chars=%d",
                         nudge_count, finish_reason, reasoning_len)
            raise RuntimeError("the model returned no answer text twice; please retry, or switch model")

        logger.warning("empty answer, asking again: finish_reason=%s reasoning_chars=%d", finish_reason, reasoning_len)
        removal = [RemoveMessage(id=last.id)] if last.id else []
        return {"messages": [*removal, HumanMessage(content=_NUDGE, name=NUDGE_NAME)], "jump_to": "model"}
