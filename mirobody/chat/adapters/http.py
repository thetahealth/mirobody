"""
HTTP Chat Adapter
Handles HTTP-specific chat logic including SSE streaming

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

class HTTPChatAdapter(ChatProtocolAdapter):
    """
    HTTP protocol adapter for chat
    
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
            _, saved_msg_id, messages = await asyncio.gather(*parallel_tasks)
            
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
    ) -> None:
        """
        Process files if file_list is provided.
        Safe to use in asyncio.gather() - returns None if no files.
        
        All parameters are passed explicitly from ChatStreamRequest.
        """
        if not params.file_list:
            return None
        
        await process_files_from_storage(
            file_list=params.file_list,
            user_id=params.user_id,
            msg_id=msg_id,
            session_id=params.session_id,
            query_user_id=params.query_user_id,
            language=params.language  # Explicit parameter from request
        )
        return None
    
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
        2. Accumulates reply/thinking content into element_list
        3. Saves response to database after stream completes
        4. Converts chunks to SSE format for frontend
        """
        output_queue = asyncio.Queue()
        
        async def _background_processor():
            # Generate reply_id in adapter layer (not in service)
            reply_id = f"web_{uuid.uuid4()}"
            full_content = ""
            thinking_content = ""
            element_list = []
            stream_completed = False
            
            # Send reply_id to frontend first
            await output_queue.put({"type": "id", "content": reply_id})
            
            try:
                async for chunk in chunks:
                    chunk_type = chunk.get("type", "")
                    
                    if chunk_type == "end":
                        stream_completed = True
                        # Don't add end chunk here - will be added after finalize to ensure correct order
                        continue  
                    
                    # Send all other chunks immediately for streaming
                    await output_queue.put(chunk)
                    
                    # Accumulate content for database storage
                    # Accumulation strategy: flush previous type's content when switching to a new type
                    if chunk_type == "reply":
                        content = chunk.get("content", "")
                        full_content += content
                        # When switching from thinking to reply, flush the accumulated thinking
                        if thinking_content:
                            element_list.append({"type": "thinking", "content": thinking_content})
                            thinking_content = ""
                    elif chunk_type == "thinking":
                        content = chunk.get("content", "")
                        thinking_content += content
                        # When switching from reply to thinking, flush the accumulated reply
                        if full_content:
                            element_list.append({"type": "reply", "content": full_content})
                            full_content = ""
                    elif chunk_type != "heartbeat":
                        # Other streaming types (queryTitle, queryDetail, costStatistics, error, etc.)
                        if full_content:
                            element_list.append({"type": "reply", "content": full_content})
                            full_content = ""
                        if thinking_content:
                            element_list.append({"type": "thinking", "content": thinking_content})
                            thinking_content = ""
                        element_list.append(chunk)
                
                # Finalize remaining accumulated content
                if thinking_content:
                    element_list.append({"type": "thinking", "content": thinking_content})
                if full_content:
                    element_list.append({"type": "reply", "content": full_content})
                
                # Add end chunk after all content has been finalized to ensure correct order
                if stream_completed:
                    element_list.append({"type": "end"})
                
                # Save response when stream completed and we have reply_id
                if stream_completed and reply_id:
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
                if stream_completed:
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
