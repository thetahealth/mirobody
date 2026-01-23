"""
HTTP Chat Adapter (Optimized Version)
Handles HTTP-specific chat logic including SSE streaming

Performance optimizations:
- Uses list accumulation instead of string concatenation (O(n) vs O(n²))
- ChunkAccumulator class encapsulates accumulation logic for clarity
- Reduces memory allocations and GC pressure

All parameters are explicitly passed via ChatContext - no implicit dependencies.
"""

import asyncio, json, logging, uuid

from typing import AsyncGenerator, Dict, Any

from .base import (
    CHUNK_TYPE_ENUMS,

    ChatProtocolAdapter
)
from ..model import ChatStreamRequest
from ..message import compress_messages
from ..unified_chat_service import UnifiedChatService
from ..file import process_files_from_storage

from ...utils import execute_query

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


class HTTPChatAdapter(ChatProtocolAdapter):
    """
    HTTP protocol adapter for chat - Optimized Version
    
    Protocol-specific characteristics:
    1. Uses Server-Sent Events (SSE) for streaming
    2. Uses real session_id for session management
    3. scene='web' for web connections
    """
    
    def __init__(self, chat_service: UnifiedChatService):
        super().__init__(chat_service)
        self.scene = "web"
    
    def get_session_id(self, params: ChatStreamRequest) -> str:
        """HTTP uses real session_id"""
        return params.session_id or str(uuid.uuid4())
    
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
            # Ensure query_user_id is set
            if not params.query_user_id:
                params.query_user_id = params.user_id
            
            # Ensure session_id is set
            if not params.session_id:
                params.session_id = str(uuid.uuid4())
            
            # Permission validation must be done first (blocking)
            if not await self.validate_permissions(params, params.user_id):
                yield f"data: {json.dumps({'type': 'error', 'content': 'No permission to chat for this user'}, ensure_ascii=False)}\n\n"
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
            
            # Compress messages (CPU-bound, cannot be parallelized with I/O)
            compressed_messages = compress_messages(params.agent, messages, 4000)
            
            # Log for chat extraction (fire-and-forget via base class)
            await self.log_chat_extraction(params, params.user_id, params.question_id)

            input_data = await self.prepare_unified_input(
                params, params.user_id, compressed_messages,
            )
            
            # Stream the response
            async for sse_chunk in self.stream_output(
                self.chat_service.generate_chat_response(**input_data),
                {
                    'user_id': params.user_id,
                    'query_user_id': params.query_user_id,
                    'msg_id': params.question_id,
                    'session_id': params.session_id,
                    'agent': params.agent,
                    'params': params
                }
            ):
                yield sse_chunk

        except Exception as e:
            logging.error(f"Error in HTTP chat handler: {str(e)}", exc_info=True)

            # Yield error as SSE
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)}, ensure_ascii=False)}\n\n"
    
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
        
        files_data = await process_files_from_storage(
            file_list=params.file_list,
            user_id=params.user_id,
            msg_id=msg_id,
            session_id=params.session_id,
            query_user_id=params.query_user_id,
            language=params.language  # Explicit parameter from request
        )
        
        # Return files_data (with content) to avoid re-downloading in Agent
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
    
    async def stream_output(
        self,
        chunks: AsyncGenerator[Dict[str, Any], None],
        context: Dict[str, Any]
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
            
            def handle_heartbeat(chunk):
                """Handle heartbeat chunk: only send to frontend, no accumulation"""
                return True  # Send to frontend only
            
            def handle_other(chunk):
                """Handle other chunk types: flush all accumulated content, then append chunk"""
                accumulator.flush_all()
                accumulator.element_list.append(chunk)
                return True  # Send to frontend
            
            # Dictionary dispatch for O(1) type lookup instead of O(n) if-elif chain
            # All chunk types are handled here with consistent logic
            chunk_handlers = {
                "reply": handle_reply,
                "thinking": handle_thinking,
                "end": handle_end,
                "heartbeat": handle_heartbeat,
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
                        
                        # Generate summary in background (fire-and-forget, don't block 'end')
                        async def _generate_summary_background():
                            try:
                                check_sql = """
                                    SELECT summary FROM theta_ai.th_sessions 
                                    WHERE session_id = :session_id AND user_id = :user_id
                                    LIMIT 1
                                """
                                result = await execute_query(
                                    check_sql,
                                    params={
                                        "session_id": context['session_id'],
                                        "user_id": context['user_id']
                                    },
                                )
                                
                                if result and (not result[0].get("summary") or result[0].get("summary") == "New Session"):
                                    from ..summary import generate_and_save_summary
                                    await generate_and_save_summary(
                                        user_id=context['user_id'],
                                        session_id=context['session_id'],
                                        provider=params.provider or "auto"
                                    )
                                    logging.info("✅ Summary generated (session=%s)", context['session_id'])
                            except Exception as summary_error:
                                logging.error("❌ Summary generation error: %s", summary_error, exc_info=True)
                        
                        # Fire-and-forget summary generation
                        asyncio.create_task(_generate_summary_background())
                        
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
        
        asyncio.create_task(_background_processor())
        
        HEARTBEAT_INTERVAL = 3
        HEARTBEAT_COUNTER_THRESHOLD = 10
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

                    chunk_type = chunk.get("type", "")
                    if chunk_type == "error":
                        logging.error(json.dumps(chunk, ensure_ascii=False))
                    if chunk_type not in CHUNK_TYPE_ENUMS.non_streaming_types:
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                except asyncio.TimeoutError:
                    logging.debug(f"heartbeat_counter: {heartbeat_counter}")
                    heartbeat_counter += 1
                    if heartbeat_counter >= HEARTBEAT_COUNTER_THRESHOLD:
                        heartbeat_counter = 0

                        yield f"data: {json.dumps({'type': 'heartbeat', 'content': ''}, ensure_ascii=False)}\n\n"

                    continue

            logging.info("Frontend stream completed")
            
        except (GeneratorExit, asyncio.CancelledError):
            logging.warning("⚠️ Client disconnected, but background AI task continues")
            raise
            
        except Exception as e:
            logging.error("Frontend stream error: %s", e, exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)}, ensure_ascii=False)}\n\n"
