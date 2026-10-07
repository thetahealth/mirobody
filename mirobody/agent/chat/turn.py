"""One chat turn: permissions, the agent's blocks, the transcript, the SSE.

This was `chat/adapters/`: an abstract base class whose single abstract method
was the five lines that write `data: {...}\n\n`, under 700 lines of pipeline
every future transport would have inherited unchanged. There is one transport.
The layer that genuinely has a second reader is `kernel.events`, and it is one
directory up.

The order matters in two places and only two:

* the user's row is written before the answer streams, so the assistant row's
  `question_id` points at a row that exists;
* `end` is held back until the answer is saved, because the client reloads
  history when it sees `end` and would otherwise miss the message that just
  streamed.

The agent runs in a background task feeding a queue, so a client that
disconnects mid-answer does not cancel a turn still worth saving.
"""

import asyncio
import json
import logging
import uuid

from typing import Any
from collections.abc import AsyncGenerator

from mirobody.agent.checkpointer import thread_for
from mirobody.agent.registry import agent_name, new_agent
from mirobody.agent.chat.file import process_files_from_storage
from mirobody.agent.errors import client_safe_error

from mirobody.agent.chat.message import save_message
from mirobody.agent.chat.model import ChatStreamRequest, has_attachment
from mirobody.agent.wire.blocks import (
    END,
    ERROR,
    FINISH_EMPTY,
    FINISH_ERROR,
    FINISH_STOP,
    FINISH_UNAVAILABLE,
    HEARTBEAT,
    START,
    TEXT,
    answer_text,
    merge,
)

from mirobody.kernel.ops import is_driver_exception
from mirobody.user.care_circle import CareCircleDenied, resolve_subject, shared_with_me
from mirobody.utils import execute_query, safe_read_cfg
from mirobody.utils.sse import heartbeat_seconds
from mirobody.utils.config import get_default_timezone
from mirobody.utils.i18n import localize
from mirobody.utils.tasks import spawn

logger = logging.getLogger(__name__)


def sse(block: dict[str, Any]) -> str:
    """One SSE event. The trailing blank line is what terminates it; without it
    a client buffers indefinitely."""
    return f"data: {json.dumps(block, ensure_ascii=False)}\n\n"


async def stream(params: ChatStreamRequest) -> AsyncGenerator[str, None]:
    """One turn as Server-Sent Events: `run`, framed.

    The transport is five lines because that is all a transport is. What the
    ABC this replaced made abstract was exactly this, under 700 lines of
    pipeline no second transport would have wanted to inherit.
    """
    async for block in run(params):
        yield sse(block)


async def run(params: ChatStreamRequest) -> AsyncGenerator[dict[str, Any], None]:
    """Every block of one turn: the agent's, plus the ones the stream itself
    reports (`start`, `heartbeat`, `end`).

    The seam for a transport that is not SSE. `wire/blocks.py` is the
    vocabulary; a WebSocket surface is `async for block in run(params): await
    ws.send_json(block)`.
    """
    try:
        params.query_user_id = params.query_user_id or params.user_id
        params.session_id = params.session_id or str(uuid.uuid4())

        if not await _may_chat(params):
            yield {"type": ERROR, "message": "No permission to chat for this user"}
            return

        # An attachment-only turn asks "read this": say it out loud ONCE,
        # before anything downstream reads `params.question`. Three things key
        # on that field: the user row (else the assistant row's `question_id`
        # points at a row that does not exist), the language the turn is
        # answered in (an empty question reads as English), and the message the
        # model receives. The empty message this replaces is not harmless:
        # Anthropic rejects a zero-length text block (400), and LangGraph
        # checkpoints it, so it replays.
        if not params.question and has_attachment(params.file_list):
            params.question = localize("attachment_only_question",
                                       params.language or "en", module="chat")

        # Minted ONCE, here: the file rows and the user row must agree on it,
        # and they did not when the client sent no `question_id`.
        params.question_id = params.question_id or f"q_{uuid.uuid4()}"

        # Independent I/O, run in parallel to cut time-to-first-byte.
        _, saved = await asyncio.gather(_store_files(params), _save_question(params))

        # The title comes from the user's question alone, so it need not wait
        # for the answer: the sidebar fills in seconds earlier this way.
        if saved:
            spawn(_summarize_once(params.user_id, params.session_id))

        async for block in _pump(_agent_blocks(params), params):
            yield block

    except Exception as e:
        logger.error("chat turn failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        yield {"type": ERROR, "message": client_safe_error(e)}


async def _may_chat(params: ChatStreamRequest) -> bool:
    """Whether the caller may open a chat on `query_user_id`'s record."""
    if not params.query_user_id or params.query_user_id == params.user_id:
        return True
    try:
        await resolve_subject(params.user_id, params.query_user_id)
    except CareCircleDenied as denied:
        logger.error("chat refused: user_id=%s subject_id=%s reason=%s",
                     params.user_id, params.query_user_id, type(denied).__name__)
        return False
    return True


async def _store_files(params: ChatStreamRequest) -> None:
    """File this turn's attachments into `th_files` and start their extraction.

    The downloaded bytes are NOT handed to the agent. Uploads reach it as
    FILES: `_build_backend` projects them into `/uploads/` by file_key and the
    prompt tells the model to `read_file` them, so injecting the bytes into the
    turn would duplicate that and blow up the context.
    """
    await process_files_from_storage(
        file_list=params.file_list,
        user_id=params.user_id,
        msg_id=params.question_id,
        session_id=params.session_id,
        query_user_id=params.query_user_id,
    )


async def _save_question(params: ChatStreamRequest) -> str | None:
    """The user's row in `th_messages`, or None when the turn carries no text."""
    if not params.question:
        return None
    return await save_message(
        user_id=params.user_id,
        query_user_id=params.query_user_id,
        content=params.question,
        role="user",
        session_id=params.session_id,
        scene=params.scene,
        agent=agent_name(),
        msg_id=params.question_id,
        provider=params.provider,
    )


async def _save_answer(params: ChatStreamRequest, reply_id: str, transcript: list[dict]) -> None:
    await save_message(
        user_id=params.user_id,
        query_user_id=params.query_user_id,
        content=transcript,
        role="assistant",
        session_id=params.session_id,
        scene=params.scene,
        agent=agent_name(),
        msg_id=reply_id,
        question_id=params.question_id,
        provider=params.provider,
    )


async def _summarize_once(user_id: str, session_id: str) -> None:
    """Fire-and-forget summary generation; skip if the session already has one."""
    try:
        result = await execute_query(
            "SELECT summary FROM th_sessions "
            "WHERE session_id = :session_id AND user_id = :user_id LIMIT 1",
            params={"session_id": session_id, "user_id": user_id},
        )
        if result and (not result[0].get("summary") or result[0].get("summary") == "New Session"):
            from mirobody.agent.chat.summary import generate_and_save_summary
            await generate_and_save_summary(user_id=user_id, session_id=session_id, provider=None)
            logger.info("Summary generated (session=%s)", session_id)
    except Exception as e:
        logger.error("summary generation failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))


async def _record_owner(params: ChatStreamRequest) -> str:
    """The name of the person whose record this turn reads, when that is not
    the person asking; "" on their own record.

    Without it the agent was told nothing on a turn asked on someone's behalf:
    every tool already read the other record, but the prompt said "their own",
    so answers said "your cholesterol" over the other person's numbers and
    flagged `mom_lab_2025-11.md` as "not yours" (measured 2026-09-28).
    """
    if params.query_user_id == params.user_id:
        return ""
    # The circle's label for them ("妈妈") before their account name: it is the
    # word the asker uses, so it is the word a question about them contains.
    row = next((r for r in await shared_with_me(params.user_id)
                if str(r.get("user_id")) == str(params.query_user_id)), None) or {}
    return row.get("nickname") or row.get("name") or "another person"


async def _agent_kwargs(params: ChatStreamRequest) -> dict[str, Any]:
    """What `AbstractAgent.generate_response` is called with (`registry.py`).

    `messages` carries ONLY this turn. The agent's graph is compiled with a
    LangGraph checkpointer keyed on `thread_for(owner, session_id)`, so LangGraph
    holds the real AIMessage / ToolMessage objects and replays the conversation
    itself. `th_messages` stays authoritative for /api/history and sharing; it
    is not fed back into the agent loop.
    """
    return {
        # The person whose data the agent operates on: the help-ask target when
        # set, the requester otherwise. `_may_chat` already validated it.
        "user_id": params.query_user_id or params.user_id,
        "record_owner": await _record_owner(params),
        # The agent keys its conversation memory on this; see `thread_for`.
        "session_id": thread_for(params.user_id, params.session_id),
        "language": params.language,
        "timezone": params.timezone or get_default_timezone(),
        "messages": [{"role": "user", "content": params.question}],
        "file_list": params.file_list,
        "provider": params.provider,
        "prompt_name": params.prompt_name,
    }


async def _agent_blocks(params: ChatStreamRequest) -> AsyncGenerator[dict[str, Any], None]:
    """The agent's blocks for this turn, always closed with `end`.

    Why a turn ENDED is a fact about the run, and a client had no way to tell
    "the model finished" from "the budget ran out" from "it crashed": all three
    arrived as the same empty `end`, and a reader that cannot distinguish them
    shows "Answer Completed" over a truncated reply.
    """
    try:
        kwargs = await _agent_kwargs(params)
        # None means no agent class was found in AGENT_DIRS at startup. A
        # constructor that RAISES does not land here; it propagates below.
        agent = new_agent(**kwargs)
        if not agent:
            yield {"type": ERROR, "message": "No agent is configured on this server"}
            yield {"type": END, "finish_reason": FINISH_UNAVAILABLE}
            return

        finish = FINISH_STOP
        async for block in agent.generate_response(**kwargs):
            if isinstance(block, dict) and block.get("type") == ERROR:
                finish = FINISH_ERROR
            yield block
        yield {"type": END, "finish_reason": finish}

    except Exception as e:
        logger.error("agent turn failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        yield {"type": ERROR, "message": client_safe_error(e)}
        yield {"type": END, "finish_reason": FINISH_ERROR}


async def _pump(
    blocks: AsyncGenerator[dict[str, Any], None],
    params: ChatStreamRequest,
) -> AsyncGenerator[dict[str, Any], None]:
    """Forward the turn's blocks, keeping the connection alive while the model
    thinks."""
    queue: asyncio.Queue = asyncio.Queue()
    spawn(_accumulate(blocks, params, queue))

    # Silence-triggered keepalive (see `utils/sse.py` for why it must be
    # silence-triggered and how short the interval has to be). A `heartbeat`
    # block rather than an SSE comment, because blocks are the unit here.
    interval = heartbeat_seconds(read=safe_read_cfg)
    try:
        while True:
            try:
                block = await asyncio.wait_for(
                    queue.get(), timeout=interval if interval > 0 else None
                )
            except TimeoutError:
                yield {"type": HEARTBEAT}
                continue
            if block is None:
                return
            if block.get("type") == ERROR:
                # The producer logged the failure with its type; the message
                # is `client_safe_error` or an `AgentError`'s own sentence,
                # and it can name a client-chosen model, so it stays out.
                logger.warning("turn sent an error block")
            yield block
    except (GeneratorExit, asyncio.CancelledError):
        logger.warning("Client disconnected; the background turn continues")
        raise


async def _accumulate(
    blocks: AsyncGenerator[dict[str, Any], None],
    params: ChatStreamRequest,
    queue: asyncio.Queue,
) -> None:
    """Forward every block to `queue` while building the transcript to store.

    The upstream `end` is swallowed and a fresh one emitted after the save, so
    WHY the turn ended has to be carried across: otherwise the client always
    reads `stop`.
    """
    reply_id = f"web_{uuid.uuid4()}"
    transcript: list[dict[str, Any]] = []
    finish_reason, completed = FINISH_STOP, False

    await queue.put({"type": START, "id": reply_id})
    try:
        async for block in blocks:
            if block.get("type") == END:
                completed = True
                reason = block.get("finish_reason")
                finish_reason = reason if isinstance(reason, str) else finish_reason
                continue
            merge(transcript, block)
            await queue.put(block)

        # A turn that ends without one `text` block is not a success the client
        # can render: it draws an empty bubble under "Answer Completed".
        # Measured 2026-09-11 across two vendors: gemini spent 2,337 of 2,539
        # output tokens on reasoning and qwen 8,943 of 9,862, both finishing
        # `stop` with no error event and no answer.
        if completed and not answer_text(transcript).strip():
            filler = localize("empty_turn", params.language or "en", module="chat")
            merge(transcript, {"type": TEXT, "text": filler})
            await queue.put({"type": TEXT, "text": filler})
            if finish_reason == FINISH_STOP:
                finish_reason = FINISH_EMPTY
            logger.warning("turn produced no answer text: finish_reason=%s", finish_reason)

        if completed:
            # WITH the reason. Without it a turn the budget cut off, or one an
            # upstream error ended, reopened as an ordinary truncated answer:
            # the live client was told and the stored conversation was not.
            transcript.append({"type": END, "finish_reason": finish_reason})
            try:
                await _save_answer(params, reply_id, transcript)
                logger.info("Response saved to database (reply_id=%s)", reply_id)
            except Exception as e:
                # Even if the save fails, `end` must go out or the client hangs.
                logger.error("saving the answer failed: error_type=%s", type(e).__name__,
                             exc_info=not is_driver_exception(e))
            await queue.put({"type": END, "finish_reason": finish_reason})

    except Exception as e:
        logger.error("the turn's background task failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        await queue.put({"type": ERROR, "message": client_safe_error(e)})
        await queue.put({"type": END, "finish_reason": FINISH_ERROR})
    finally:
        await queue.put(None)
