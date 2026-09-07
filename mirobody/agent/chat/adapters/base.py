"""
Base adapter class for chat protocols

Defines the interface and common logic that all protocol adapters share.
Subclasses implement protocol-specific details (HTTP SSE, WebSocket, etc.).

All user_id parameters are consistently typed as str throughout.
"""

import asyncio
import json
import logging
import uuid

from abc import ABC, abstractmethod
from typing import Any
from collections.abc import AsyncGenerator

from ...registry import agent_name, new_agent
from ..file import process_files_from_storage
from ...errors import client_safe_error

from ..message import save_message
from ..model import ChatStreamRequest, has_attachment

from ....user.care_circle import CareCircleDenied, resolve_subject
from ....utils import execute_query, safe_read_cfg
from ....utils.sse import heartbeat_seconds
from ....utils.config import get_default_timezone
from ....utils.i18n import t
from ....utils.tasks import spawn

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------
# Constants
#-----------------------------------------------------------------------------

# Chunk types persisted into element_list but never streamed to the client.
_NON_STREAMING_TYPES = {"food_snap", "report"}


#: Why a turn ended. A closed set, because a client branches on it: `stop` is
#: the model finishing, `error` is a fault the user was told about, and
#: `unavailable` is never having started.
FINISH_STOP = "stop"
FINISH_ERROR = "error"
FINISH_UNAVAILABLE = "unavailable"


class ChunkAccumulator:
    """
    Efficient chunk accumulator using list accumulation
    
    Usage:
        acc = ChunkAccumulator()
        acc.reply_chunks.append("Hello ")
        acc.reply_chunks.append("world")
        acc.flush_reply()  # Creates {"type": "reply", "content": "Hello world"}
    """
    __slots__ = ('reply_chunks', 'thinking_chunks', 'element_list', 'stream_completed', 'finish_reason')
    
    def __init__(self):
        self.reply_chunks = []
        self.thinking_chunks = []
        self.element_list = []
        self.stream_completed = False
        #: Why the turn ended, carried from the upstream `end` to the one this
        #: adapter emits after saving. `stop` until something says otherwise.
        self.finish_reason = FINISH_STOP
    
    def flush_reply(self) -> bool:
        """
        Flush accumulated reply chunks to element_list.
        Returns True if content was flushed, False if no content.
        """
        if not self.reply_chunks:
            return False
        content = ''.join(self.reply_chunks)
        if content:  # Only add non-empty content
            self.element_list.append({"type": "reply", "content": content})
        self.reply_chunks.clear()
        return True
    
    def flush_thinking(self) -> bool:
        """
        Flush accumulated thinking chunks to element_list.
        Returns True if content was flushed, False if no content.
        """
        if not self.thinking_chunks:
            return False
        content = ''.join(self.thinking_chunks)
        if content:  # Only add non-empty content
            self.element_list.append({"type": "thinking", "content": content})
        self.thinking_chunks.clear()
        return True
    
    def flush_all(self) -> None:
        """Flush both thinking and reply chunks."""
        self.flush_thinking()
        self.flush_reply()
    
    def finalize(self) -> list:
        """
        Finalize accumulation: flush remaining content and add 'end' chunk if needed.
        Returns the complete element_list.
        """
        self.flush_all()
        if self.stream_completed:
            self.element_list.append({"type": "end"})
        return self.element_list

#-----------------------------------------------------------------------------
# Base Adapter
#-----------------------------------------------------------------------------

class ChatProtocolAdapter(ABC):
    """
    Abstract base class for chat protocol adapters.

    Provides common logic for all protocols:
    - Permission validation
    - Agent resolution and chunk streaming
    - Message saving (user question, assistant response)
    - Message history retrieval
    - Chat extraction logging

    Note: All user_id parameters are typed as str for consistency.
    """

    def __init__(self):
        self.scene: str = "web"  # Default scene, subclasses can override
    
    # =========================================================================
    # The only thing a protocol has to supply: how a chunk goes on the wire.
    #
    # `handle_request` and `stream_output` used to be abstract. Between them
    # they are 183 lines, of which 5 emitted SSE framing — so declaring them
    # abstract obliged a second transport to reimplement the accumulator
    # dispatch, the database write, the end-chunk ordering and the heartbeat
    # timing in order to change `data: {...}\n\n` into a frame. They are
    # concrete below; only the encoding is abstract, which is the seam that
    # actually differs between SSE and WebSocket.
    # =========================================================================

    @abstractmethod
    def encode_chunk(self, chunk: dict[str, Any]) -> str:
        """Serialise one chunk into this protocol's wire format."""

    def get_session_id(self, params: ChatStreamRequest) -> str:
        """Session id used for storage.

        The HTTP behaviour is the sensible default. A transport with no session
        of its own (a socket keyed by trace_id, say) overrides it.
        """
        return params.session_id or str(uuid.uuid4())

    # =========================================================================
    # Common methods - shared by all protocols
    # =========================================================================
    
    def get_scene(self) -> str:
        """
        Get the scene identifier for this adapter.
        
        Returns:
            Scene string (e.g., 'web' for HTTP)
        """
        return self.scene
    
    #-------------------------------------------------------------------------

    async def validate_permissions(
        self,
        params: ChatStreamRequest,
        user_id: str,
        **kwargs
    ) -> bool:
        """
        Validate help-ask permissions.
        
        In help-ask scenarios, user A can query user B's data if they have
        the appropriate chat permissions.
        
        Args:
            params: Chat request parameters
            user_id: Current user ID (str)
            **kwargs: Additional arguments (e.g., token for HTTP)
            
        Returns:
            True if permissions are valid, False otherwise
        """        
        # If not help-ask scenario (querying own data), always allow
        if not params.query_user_id or params.query_user_id == user_id:
            return True
        
        # Validate help-ask permissions
        # The call this replaced also forwarded a `token` kwarg that the
        # function did not accept — a TypeError waiting on whichever caller
        # first passed one.
        try:
            await resolve_subject(user_id, params.query_user_id)
        except CareCircleDenied as denied:
            logger.error(
                f"user {user_id} may not open a chat on {params.query_user_id}'s record: {denied}"
            )
            return False

        return True
    
    #-------------------------------------------------------------------------

    async def save_user_question(
        self,
        params: ChatStreamRequest,
        user_id: str
    ) -> str:
        """
        Save user question to database.
        
        Args:
            params: Chat request parameters
            user_id: User ID (str)
            
        Returns:
            The saved message ID
        """        
        return await save_message(
            user_id=user_id,
            query_user_id=params.query_user_id or user_id,
            content=params.question,
            role='user',
            session_id=self.get_session_id(params),
            scene=self.get_scene(),
            agent=agent_name(),
            message_type="text",
            msg_id=params.msg_id,
            provider=params.provider
        )
    
    #-------------------------------------------------------------------------

    async def save_assistant_response(
        self,
        reply_id: str,
        params: ChatStreamRequest,
        user_id: str,
        content: Any,
        question_msg_id: str
    ) -> str:
        """
        Save assistant response to database.
        
        Args:
            reply_id: Response message ID
            params: Chat request parameters
            user_id: User ID (str)
            content: Response content (element_list)
            question_msg_id: ID of the question this responds to
            
        Returns:
            The saved message ID
        """        
        return await save_message(
            user_id=user_id,
            query_user_id=params.query_user_id or user_id,
            content=content,
            role='assistant',
            session_id=self.get_session_id(params),
            scene=self.get_scene(),
            agent=agent_name(),
            msg_id=reply_id,
            question_id=question_msg_id,
            message_type="text",
            provider=params.provider  
        )
    
    #-------------------------------------------------------------------------

    def _prepare_agent_kwargs(self, params: ChatStreamRequest) -> dict[str, Any]:
        """Pack the request into the kwargs the agent receives — built ONCE.

        This used to be two packagings: this method produced a 16-field dict
        for UnifiedChatService, which repackaged it into agent_kwargs with
        renames along the way (``question`` → ``content`` → ``question``) and
        fields nothing consumed (``enable_mcp``, ``group_id``, the JWT
        ``user_id``). One dict, final names, only consumed fields.

        ``messages`` carries ONLY this turn. The agent's graph is compiled with
        a LangGraph checkpointer keyed on ``thread_id = session_id``
        (deep/checkpointer.py), so LangGraph holds the real AIMessage /
        ToolMessage objects and replays the conversation itself. `th_messages`
        stays authoritative for /api/history and sharing; it is not fed back
        into the agent loop. (A second path used to replay flattened
        `th_messages` rows for an agent without a checkpointer; that agent and
        its replay went together.)
        """
        messages = [{"role": "user", "content": params.question}]

        return {
            # `user_id` is the person whose data the agent operates on: the
            # help-ask target when set, the requester otherwise. Permissions
            # were already validated in handle_request.
            "user_id"       : params.query_user_id or str(params.user_id),
            "session_id"    : self.get_session_id(params) or params.trace_id or "",
            "language"      : params.language or "",
            "timezone"      : params.timezone or get_default_timezone(),
            "token"         : params.token or "",
            # Chat.
            "question"      : params.question,
            "messages"      : messages,
            "file_list"     : params.file_list or [],
            "files_data"    : getattr(params, "files_data", None),  # Downloaded file content (avoids re-download)
            # LLM. (`params.agent` is accepted and ignored: there is one agent.)
            "provider"      : params.provider,
            "prompt_name"   : params.prompt_name,
        }

    #-------------------------------------------------------------------------

    async def _stream_agent_chunks(
        self,
        agent_kwargs: dict[str, Any],
    ) -> AsyncGenerator[dict[str, Any], None]:
        """Resolve the agent and forward its raw chunks; always close with `end`.

        Formerly ``UnifiedChatService.generate_chat_response`` — a stateless
        single-method class between the adapter and the agent whose main
        activity was repackaging the adapter's dict. Merged here; the class
        is gone.

        No accumulation or formatting happens here — chunks stream through
        as-is ({"type": "reply"|"thinking"|"queryTitle"|"queryArguments"|
        "queryDetail"|"costStatistics"|"error", ...}); `stream_output` owns
        accumulation and persistence.
        """
        try:
            # None means no agent class was found in AGENT_DIRS at startup. A
            # constructor that RAISES does not land here; it propagates to the
            # except below.
            agent_instance = new_agent(**agent_kwargs)
            if not agent_instance:
                yield {"type": "error", "content": "No agent is configured on this server"}
                yield {"type": "end", "content": "", "finish_reason": FINISH_UNAVAILABLE}
                return

            # Why a turn ENDED is a fact about the run, and the client had no way
            # to tell "the model finished" from "the budget ran out" from "it
            # crashed" — all three arrived as the same empty `end`. A reader that
            # cannot distinguish them shows "Answer Completed" over a truncated
            # reply, which is what it did. Additive: `content` is unchanged and a
            # client that ignores the key behaves exactly as before.
            finish_reason = FINISH_STOP
            async for chunk in agent_instance.generate_response(**agent_kwargs):
                if isinstance(chunk, dict) and chunk.get("type") == "error":
                    finish_reason = FINISH_ERROR
                yield chunk

            # Signal end of stream
            yield {"type": "end", "content": "", "finish_reason": finish_reason}

        except Exception as e:
            logger.error(f"Error generating chat response: {str(e)}", exc_info=True)

            yield {"type": "error", "content": client_safe_error(e)}
            yield {"type": "end", "content": "", "finish_reason": FINISH_ERROR}

    #-------------------------------------------------------------------------

    async def handle_request(
        self,
        params: ChatStreamRequest,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """
        Handle HTTP chat request - returns SSE stream
        
        Uses base class logic for common operations and implements
        HTTP-specific SSE streaming.
        
        Performance optimization: Independent async operations are executed
        in parallel using asyncio.gather() to reduce TTFB.
        
        All parameters are explicitly passed via ChatContext - no implicit dependencies.
        """
        try:
            if params.scene and isinstance(params.scene, str):
                self.scene = params.scene

            # Ensure query_user_id is set
            if not params.query_user_id:
                params.query_user_id = params.user_id
            
            # Ensure session_id is set
            if not params.session_id:
                params.session_id = str(uuid.uuid4())
            
            # Permission validation must be done first (blocking)
            if not await self.validate_permissions(params, params.user_id):
                yield self.encode_chunk({"type": "error", "content": "No permission to chat for this user"})
                return

            # An attachment-only turn asks "read this" — say it out loud, ONCE,
            # before anything downstream reads `params.question`. Everything
            # that turn touches keys on that field: `_save_question_if_needed`
            # (so the turn leaves a user row and the session gets a title
            # instead of an assistant answer whose `question_id` points at a
            # row that does not exist), the language the turn is answered in
            # (an empty question reads as English, so a Chinese user who typed
            # nothing got an English-instructed prompt), and the message the
            # model receives. Substituting later, at message-build
            # time, fixed only the last of those — and even that only until a
            # `current_turn_note` was folded in ahead of it, which left the user
            # message a bare time hint asking nothing at all.
            #
            # The empty message this replaces is not a harmless no-op: Anthropic
            # rejects a zero-length text block outright (400), and LangGraph
            # checkpoints the turn's input BEFORE the model node runs, so the
            # failed turn stays in the session thread and is replayed on every
            # later turn of that session.
            if not params.question and has_attachment(params.file_list):
                params.question = t("attachment_only_question",
                                    params.language or "en", module="chat")

            question_msg_id = params.question_id or f"q_{uuid.uuid4()}"
            
            # Independent I/O, run in parallel to cut time-to-first-byte.
            files_data, saved_msg_id = await asyncio.gather(
                self._process_files_if_needed(params, question_msg_id),
                self._save_question_if_needed(params),
            )
            
            # Attach files_data to params for Agent to use (avoids re-downloading)
            if files_data:
                params.files_data = files_data
            
            # Use saved message ID if question was saved
            if saved_msg_id:
                question_msg_id = saved_msg_id
            params.question_id = question_msg_id

            # Kick off summary generation in parallel with the assistant
            # stream. Since the title is derived from the user's question
            # only, we don't need to wait for the response to finish — the
            # sidebar title shows up seconds earlier this way.
            if saved_msg_id:
                spawn(
                    self._generate_summary_if_missing(
                        user_id=params.user_id,
                        session_id=params.session_id,
                    )
                )
            
            agent_kwargs = self._prepare_agent_kwargs(params)

            # Stream the response
            async for frame in self.stream_output(
                self._stream_agent_chunks(agent_kwargs),
                {
                    'user_id': params.user_id,
                    'query_user_id': params.query_user_id,
                    'msg_id': params.question_id,
                    'session_id': params.session_id,
                    'params': params
                }
            ):
                yield frame

        except Exception as e:
            logger.error(f"Error in HTTP chat handler: {str(e)}", exc_info=True)

            # Yield error as SSE
            yield self.encode_chunk({"type": "error", "content": client_safe_error(e)})

    async def _process_files_if_needed(
        self,
        params: ChatStreamRequest,
        msg_id: str
    ) -> list[dict] | None:
        """
        Process files if file_list is provided.
        Safe to use in asyncio.gather() - returns files_data with content if files exist.

        All parameters are passed explicitly from ChatStreamRequest.

        Returns:
            List of file data dicts with 'content' (bytes) if files processed, None otherwise
        """
        if not params.file_list:
            return None

        # Get Redis client dynamically
        from ....utils.config import global_config
        redis_client = None
        try:
            redis_client = await global_config().get_redis().get_async_client()
        except Exception as e:
            logger.warning(f"Failed to get Redis client for file caching: {e}")

        files_data = await process_files_from_storage(
            file_list=params.file_list,
            user_id=params.user_id,
            msg_id=msg_id,
            session_id=params.session_id,
            query_user_id=params.query_user_id,
            language=params.language,
            redis_client=redis_client
        )

        return files_data if files_data else None

    async def _save_question_if_needed(
        self,
        params: ChatStreamRequest,
    ) -> str | None:
        """
        Save user question if provided.
        Safe to use in asyncio.gather() - returns msg_id or None.
        """
        if not params.question:
            return None

        return await self.save_user_question(params, params.user_id)

    async def _generate_summary_if_missing(self, user_id: str, session_id: str) -> None:
        """Fire-and-forget summary generation; skip if session already has one."""
        try:
            check_sql = """
                SELECT summary FROM th_sessions
                WHERE session_id = :session_id AND user_id = :user_id
                LIMIT 1
            """
            result = await execute_query(
                check_sql,
                params={"session_id": session_id, "user_id": user_id},
            )
            if result and (not result[0].get("summary") or result[0].get("summary") == "New Session"):
                from ..summary import generate_and_save_summary
                await generate_and_save_summary(
                    user_id=user_id,
                    session_id=session_id,
                    provider=None,
                )
                logger.info("Summary generated (session=%s)", session_id)
        except Exception as summary_error:
            logger.error("Summary generation error: %s", summary_error, exc_info=True)

    async def stream_output(
        self,
        chunks: AsyncGenerator[dict[str, Any], None],
        context: dict[str, Any]
    ) -> AsyncGenerator[str, None]:
        """
        Convert unified chunks to SSE format and handle chunk accumulation.
        
        This method:
        1. Generates reply_id for the response
        2. Accumulates reply/thinking content into element_list (for database)
        3. Streams chunks to frontend immediately (for real-time display)
        4. Saves complete response to database after stream ends
        
        Performance optimization: Uses ChunkAccumulator with list accumulation
        instead of string concatenation (O(n) vs O(n²)).
        """
        output_queue = asyncio.Queue()
        
        async def _background_processor():
            # Generate reply_id in adapter layer (not in service)
            reply_id = f"web_{uuid.uuid4()}"
            accumulator = ChunkAccumulator()
            
            # Define chunk type handlers (dictionary dispatch for O(1) lookup)
            def handle_reply(chunk):
                """Handle reply chunk: flush thinking if present, then accumulate reply"""
                if accumulator.thinking_chunks:
                    accumulator.flush_thinking()
                accumulator.reply_chunks.append(chunk.get("content", ""))
                return True  # Send to frontend
            
            def handle_thinking(chunk):
                """Handle thinking chunk: flush reply if present, then accumulate thinking"""
                if accumulator.reply_chunks:
                    accumulator.flush_reply()
                accumulator.thinking_chunks.append(chunk.get("content", ""))
                return True  # Send to frontend
            
            def handle_end(chunk):
                """Handle end chunk: mark stream completed, don't send to frontend yet.

                The upstream `end` is swallowed here and a fresh one is emitted
                after the response is saved, so WHY the turn ended has to be
                carried across — otherwise the client always reads `stop`, which
                is the bug this field exists to fix.
                """
                accumulator.stream_completed = True
                reason = chunk.get("finish_reason")
                if isinstance(reason, str):
                    accumulator.finish_reason = reason
                return False  # Don't send to frontend (will be sent after save)

            def handle_other(chunk):
                """Handle other chunk types: flush all accumulated content, then append chunk"""
                accumulator.flush_all()
                accumulator.element_list.append(chunk)
                return True  # Send to frontend

            # Heartbeats need no handler here: they are produced by the consumer
            # loop below (queue-read timeout), never by the agent stream.
            chunk_handlers = {
                "reply": handle_reply,
                "thinking": handle_thinking,
                "end": handle_end,
            }
            
            # Send reply_id to frontend first
            await output_queue.put({"type": "id", "content": reply_id})
            
            try:
                async for chunk in chunks:
                    chunk_type = chunk.get("type", "")
                    
                    # Pure dictionary dispatch - O(1) lookup for all chunk types
                    # Get handler for this chunk type, default to handle_other for unknown types
                    handler = chunk_handlers.get(chunk_type, handle_other)
                    
                    # Execute handler and check if should send to frontend
                    should_send = handler(chunk)
                    
                    if should_send:
                        await output_queue.put(chunk)
                
                # Finalize: flush remaining accumulated content and add end chunk if needed
                element_list = accumulator.finalize()
                
                # Save response when stream completed and we have reply_id
                if accumulator.stream_completed and reply_id:
                    params = context['params']
                    try:
                        await self.save_assistant_response(
                            reply_id=reply_id,
                            params=params,
                            user_id=context['user_id'],
                            content=element_list,
                            question_msg_id=context['msg_id']
                        )
                        logger.info("Response saved to database (reply_id=%s)", reply_id)
                    except Exception as save_error:
                        logger.error("Failed to save response: %s", save_error, exc_info=True)
                        # Even if save fails, send 'end' to avoid frontend hanging
                elif not reply_id:
                    logger.warning("Missing reply_id, skip saving")
                
                # Send 'end' chunk only after saving is complete (or if no save needed)
                if accumulator.stream_completed:
                    await output_queue.put({
                        "type": "end",
                        "content": "",
                        "finish_reason": accumulator.finish_reason,
                    })
                    logger.info("Stream ended, 'end' signal sent to frontend")
                    
            except Exception as e:
                logger.error("Background processor error: %s", e, exc_info=True)
                await output_queue.put({"type": "error", "content": client_safe_error(e)})
                await output_queue.put({"type": "end", "content": "", "finish_reason": FINISH_ERROR})
            finally:
                await output_queue.put(None)
                logger.debug("Background task completed")
        
        spawn(_background_processor())
        
        # Silence-triggered keepalive (see `utils/sse.py` for why it must be
        # silence-triggered and how short the interval has to be). The frame
        # is a `heartbeat` chunk rather than an SSE comment because chunks are
        # the adapter-neutral unit here — `encode_chunk` owns the wire — and
        # the shipped web client drops `type: heartbeat` on sight. Two knobs
        # used to multiply into a 40-second first ping (`HEARTBEAT_INTERVAL` ×
        # `HEARTBEAT_COUNTER_THRESHOLD`), past most proxies' idle timeout; one
        # key now, shared with every other streaming endpoint.
        interval = heartbeat_seconds(read=safe_read_cfg)
        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        output_queue.get(),
                        timeout=interval if interval > 0 else None,
                    )
                except TimeoutError:
                    yield self.encode_chunk({"type": "heartbeat", "content": ""})
                    continue

                if chunk is None:
                    break

                chunk_type = chunk.get("type", "")
                if chunk_type == "error":
                    logger.error(json.dumps(chunk, ensure_ascii=False))
                if chunk_type not in _NON_STREAMING_TYPES:
                    yield self.encode_chunk(chunk)

            logger.info("Frontend stream completed")
            
        except (GeneratorExit, asyncio.CancelledError):
            logger.warning("Client disconnected, but background AI task continues")
            raise
            
        except Exception as e:
            logger.error("Frontend stream error: %s", e, exc_info=True)
            yield self.encode_chunk({"type": "error", "content": client_safe_error(e)})
