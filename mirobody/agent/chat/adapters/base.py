"""
Base adapter class for chat protocols

Defines the interface and common logic that all protocol adapters share.
Subclasses implement protocol-specific details (HTTP SSE, WebSocket, etc.).

All user_id parameters are consistently typed as str throughout.
"""

import asyncio, json, logging, uuid

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator

from langchain_core.messages import BaseMessage, HumanMessage

from ..agent import agent_keeps_own_history, get_global_agent
from ..file import process_files_from_storage
from ...base.history_replay import relative_time_hint

from ..message import (
    compress_messages,
    save_message,
    get_last_message,
    chat_list_setter_hollywell
)
from ..model import ChatStreamRequest

from ....utils import execute_query, get_query_user_id, safe_read_cfg
from ....utils.config import get_default_timezone
from ....utils.tasks import spawn

#-----------------------------------------------------------------------------
# Constants
#-----------------------------------------------------------------------------

class PERMISSION_ENUM:
    """Permission type identifiers"""
    chat = "chat"


class PERMISSION_LEVEL_ENUM:
    """Permission level values"""
    no_permission = 0
    read = 1
    write = 2


# Chunk types persisted into element_list but never streamed to the client.
_NON_STREAMING_TYPES = {"food_snap", "report"}

class ChunkAccumulator:
    """
    Efficient chunk accumulator using list accumulation
    
    Usage:
        acc = ChunkAccumulator()
        acc.reply_chunks.append("Hello ")
        acc.reply_chunks.append("world")
        acc.flush_reply()  # Creates {"type": "reply", "content": "Hello world"}
    """
    __slots__ = ('reply_chunks', 'thinking_chunks', 'element_list', 'stream_completed')
    
    def __init__(self):
        self.reply_chunks = []
        self.thinking_chunks = []
        self.element_list = []
        self.stream_completed = False
    
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
        permission_kwargs = {"permission": [PERMISSION_ENUM.chat]}
        if "token" in kwargs:
            permission_kwargs["token"] = kwargs["token"]
            
        permission_result = await get_query_user_id(
            params.query_user_id, 
            user_id,
            **permission_kwargs
        )
        
        if not permission_result.get("success"):
            logging.error(f"Permission validation failed: {permission_result.get('error')}")
            return False
            
        permissions = permission_result.get("permissions", {})
        if permissions.get(PERMISSION_ENUM.chat, 0) < PERMISSION_LEVEL_ENUM.read:
            logging.error(f"Insufficient chat permissions for user {user_id} to query {params.query_user_id}")
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
            agent=params.agent,
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
            agent=params.agent,
            msg_id=reply_id,
            question_id=question_msg_id,
            message_type="text",
            provider=params.provider  
        )
    
    #-------------------------------------------------------------------------

    async def get_message_history(
        self,
        user_id: str,
        query_user_id: str | None = None,
        session_id: str | None = None,
        scene: str | None = None
    ) -> list:
        """
        Get message history for a session.
        
        Processes raw messages to:
        1. Extract file information from JSON content
        2. Filter consecutive assistant messages
        3. Remove trailing assistant message for web scene
        
        Args:
            user_id: User ID (str)
            query_user_id: Query user ID for help-ask scenarios
            session_id: Session ID
            scene: Scene identifier (uses adapter's scene if not provided)
            
        Returns:
            Processed list of messages
        """
        effective_scene = scene or self.get_scene()
        messages = await get_last_message(user_id, query_user_id, session_id, scene=effective_scene)
        
        # Process messages to extract files from JSON content
        new_messages = []
        for m in messages:
            content = m.get("content", "")
            role = m.get("role", "user")
            
            try:
                data = json.loads(content)
                if role == "user" and "files" in data:
                    files = data["files"]
                    new_msg = dict(
                        role="user",
                        files=[dict(
                            s3_key=f["file_key"],
                            file_name=f["filename"],
                            file_type=f["type"]
                        ) for f in files],
                        type="file"
                    )
                    new_messages.append(new_msg)
                else:
                    new_messages.append(m)
            except (json.JSONDecodeError, TypeError):
                new_messages.append(m)
        
        # Filter out consecutive assistant messages, keeping only the last one in each sequence
        filtered_messages = []
        for i, msg in enumerate(new_messages):
            if msg.get("role") != "assistant":
                filtered_messages.append(msg)
            else:
                # Keep this assistant message if it's the last or next is not assistant
                if i == len(new_messages) - 1 or new_messages[i + 1].get("role") != "assistant":
                    filtered_messages.append(msg)
                else:
                    logging.debug(f"Skipping consecutive assistant message at index {i}")
        
        return filtered_messages
    
    #-------------------------------------------------------------------------

    async def log_chat_extraction(
        self,
        params: ChatStreamRequest,
        user_id: str,
        msg_id: str
    ):
        """
        Log chat for extraction (fire-and-forget).
        
        Only logs for self-queries (not help-ask scenarios).
        
        Args:
            params: Chat request parameters
            user_id: User ID (str)
            msg_id: Message ID
        """
        if params.question and (params.query_user_id == user_id):
            spawn(
                chat_list_setter_hollywell(
                    user_id=user_id,
                    msg_id=msg_id,
                    question=params.question
                )
            )
    
    #-------------------------------------------------------------------------

    def _prepare_agent_kwargs(
        self,
        params: ChatStreamRequest,
        messages: list,
        current_turn_note: str = "",
    ) -> dict[str, Any]:
        """Pack the request into the kwargs the agent receives — built ONCE.

        This used to be two packagings: this method produced a 16-field dict
        for UnifiedChatService, which repackaged it into agent_kwargs with
        renames along the way (``question`` → ``content`` → ``question``) and
        fields nothing consumed (``enable_mcp``, ``group_id``, the JWT
        ``user_id``). One dict, final names, only consumed fields.

        current_turn_note: optional text appended to THIS turn's user message
        (e.g. a resume/time-gap hint). It rides on the new, uncached turn so it
        never invalidates the cached history/system prefix.
        """

        # Append the current question (+ optional note). Match the element type of
        # the replayed history: canonical replay yields LangChain BaseMessage
        # objects, the legacy path yields {role, content} dicts. (astream coerces
        # either, but keeping the list homogeneous avoids surprises downstream.)
        question = params.question
        if current_turn_note:
            question = f"{question}\n\n{current_turn_note}" if question else current_turn_note
        if messages and isinstance(messages[-1], BaseMessage):
            messages = messages + [HumanMessage(content=question)]
        else:
            messages = messages + [dict(role="user", content=question)]

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
            # LLM.
            "agent"         : params.agent,
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
        agent_name = agent_kwargs["agent"]

        try:
            agent_instance = get_global_agent(agent_name=agent_name, **agent_kwargs)

            if not agent_instance:
                # None means the NAME is not registered (unknown agent or a
                # dangling alias) — that is the only case this fallback covers.
                # A constructor that RAISES does not land here; it propagates
                # to the except below and errors out without fallback.
                logging.warning(
                    f"⚠️ Agent '{agent_name}' is not registered "
                    f"(user {agent_kwargs['user_id']}). Falling back to DeepAgent."
                )

                # Yield warning chunk so frontend knows about the fallback
                yield {
                    "type": "thinking",
                    "content": f"[System] Agent '{agent_name}' unavailable, using default agent."
                }

                agent_instance = get_global_agent(agent_name="Deep", **agent_kwargs)

                if not agent_instance:
                    yield {"type": "error", "content": "No agent instance available"}
                    yield {"type": "end", "content": ""}
                    return

            async for chunk in agent_instance.generate_response(**agent_kwargs):
                yield chunk

            # Signal end of stream
            yield {"type": "end", "content": ""}

        except Exception as e:
            logging.error(f"Error generating chat response: {str(e)}", exc_info=True)

            yield {"type": "error", "content": f"⚠️ An error occurred: {str(e)}"}
            yield {"type": "end", "content": ""}

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
            
            question_msg_id = params.question_id or f"q_{uuid.uuid4()}"
            
            parallel_tasks = [
                self._process_files_if_needed(params, question_msg_id),
                self._save_question_if_needed(params),
                self.get_message_history(params.user_id, params.query_user_id, params.session_id),
            ]
            
            # Execute all independent operations in parallel
            files_data, saved_msg_id, messages = await asyncio.gather(*parallel_tasks)
            
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
            
            # History. Who supplies it depends on the agent:
            #
            #   DeepAgent family — NOBODY here does. The graph is compiled with a
            #     LangGraph checkpointer keyed on thread_id = session_id, so it
            #     holds the real AIMessage/ToolMessage objects and replays them
            #     itself. We hand in only THIS turn's message. The module that
            #     used to rebuild history by parsing persisted UI chunks
            #     (base/history_replay.py) is gone — the checkpointer's job,
            #     and doing it by hand lost `thinking` and re-parsed tool args.
            #
            #   BaseAgent — still needs it: it has no graph and no checkpointer,
            #     and its stateless providers (DeepSeek) get no history from the
            #     provider side either. It keeps the flat {role, content} replay.
            #
            # `th_messages` is unchanged and still authoritative for /api/history
            # and sharing; it is simply no longer fed back into the DeepAgent loop.
            if agent_keeps_own_history(params.agent):
                compressed_messages = []
                time_note = ""
            else:
                compressed_messages = compress_messages(params.agent, messages, 4000)
                # Temporal continuity on resume: the system prompt carries the
                # (hour-rounded) current time, but flat replay drops the gap since
                # the last turn. Surface it on the CURRENT user turn — after the
                # cache breakpoint, so the cached prefix is untouched.
                time_note = relative_time_hint(messages)

            # Log for chat extraction (fire-and-forget via base class)
            await self.log_chat_extraction(params, params.user_id, params.question_id)

            agent_kwargs = self._prepare_agent_kwargs(
                params, compressed_messages, current_turn_note=time_note,
            )

            # Stream the response
            async for frame in self.stream_output(
                self._stream_agent_chunks(agent_kwargs),
                {
                    'user_id': params.user_id,
                    'query_user_id': params.query_user_id,
                    'msg_id': params.question_id,
                    'session_id': params.session_id,
                    'agent': params.agent,
                    'params': params
                }
            ):
                yield frame

        except Exception as e:
            logging.error(f"Error in HTTP chat handler: {str(e)}", exc_info=True)

            # Yield error as SSE
            yield self.encode_chunk({"type": "error", "content": str(e)})

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
            logging.warning(f"Failed to get Redis client for file caching: {e}")

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
                logging.info("✅ Summary generated (session=%s)", session_id)
        except Exception as summary_error:
            logging.error("❌ Summary generation error: %s", summary_error, exc_info=True)

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
                """Handle end chunk: mark stream completed, don't send to frontend yet"""
                accumulator.stream_completed = True
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
                        logging.info("✅ Response saved to database (reply_id=%s)", reply_id)
                    except Exception as save_error:
                        logging.error("❌ Failed to save response: %s", save_error, exc_info=True)
                        # Even if save fails, send 'end' to avoid frontend hanging
                elif not reply_id:
                    logging.warning("⚠️ Missing reply_id, skip saving")
                
                # Send 'end' chunk only after saving is complete (or if no save needed)
                if accumulator.stream_completed:
                    await output_queue.put({"type": "end", "content": ""})
                    logging.info("✅ Stream ended, 'end' signal sent to frontend")
                    
            except Exception as e:
                logging.error("Background processor error: %s", e, exc_info=True)
                await output_queue.put({"type": "error", "content": str(e)})
            finally:
                await output_queue.put(None)
                logging.debug("Background task completed")
        
        spawn(_background_processor())
        
        HEARTBEAT_INTERVAL = int(safe_read_cfg("HEARTBEAT_INTERVAL", "10"))
        HEARTBEAT_COUNTER_THRESHOLD = int(safe_read_cfg("HEARTBEAT_COUNTER_THRESHOLD", "3"))
        heartbeat_counter = 0
        try:
            while True:
                try:
                    chunk = await asyncio.wait_for(
                        output_queue.get(),
                        timeout=HEARTBEAT_INTERVAL
                    )

                    if chunk is None:
                        break

                    heartbeat_counter = 0

                    chunk_type = chunk.get("type", "")
                    if chunk_type == "error":
                        logging.error(json.dumps(chunk, ensure_ascii=False))
                    if chunk_type not in _NON_STREAMING_TYPES:
                        yield self.encode_chunk(chunk)
                    

                except asyncio.TimeoutError:
                    logging.debug(f"heartbeat_counter: {heartbeat_counter}")
                    heartbeat_counter += 1
                    if heartbeat_counter > HEARTBEAT_COUNTER_THRESHOLD:
                        heartbeat_counter = 0

                        yield self.encode_chunk({"type": "heartbeat", "content": ""})

                    continue

            logging.info("Frontend stream completed")
            
        except (GeneratorExit, asyncio.CancelledError):
            logging.warning("⚠️ Client disconnected, but background AI task continues")
            raise
            
        except Exception as e:
            logging.error("Frontend stream error: %s", e, exc_info=True)
            yield self.encode_chunk({"type": "error", "content": str(e)})
