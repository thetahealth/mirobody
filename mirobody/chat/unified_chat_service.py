"""
Unified Chat Service - Protocol Agnostic Core Service
This service handles the core chat logic without caring about the transport protocol
"""

import logging

from typing import Any, AsyncGenerator

from .agent import get_global_agent

from ..utils.config import global_config

#-----------------------------------------------------------------------------

class UnifiedChatService:
    """
    Core chat service that handles business logic.
    
    This service is protocol-agnostic and doesn't know or care about WebSocket vs HTTP.
    It focuses solely on:
    1. Agent initialization and configuration
    2. Streaming raw chunks from agents
    3. Error handling
    
    Chunk accumulation, formatting, and persistence are handled by the adapter layer.
    This separation ensures the service remains simple and reusable across protocols.
    """

    async def generate_chat_response(
        self,
        query_user_id: str,
        agent: str,
        messages: list[dict[str, Any]],
        file_list: list | None = None,
        prompt_name: str = "",
        **kwargs
    ) -> AsyncGenerator[dict[str, Any], None]:
        """
        Generate chat response by streaming raw chunks from the agent.
        
        This method does NOT accumulate or format chunks. It simply forwards chunks
        from the agent as-is. Accumulation and formatting are handled by adapters.
        
        All parameters are passed explicitly - no implicit dependencies.

        Args:
            query_user_id: User being queried (for help-ask feature)
            agent: Agent type to use
            messages: Previous messages in the conversation including current user's message
            file_list: List of files attached to the message
            prompt_name: Name of the prompt template to use
            **kwargs: Additional arguments (trace_id, session_id, provider, timezone, language, token, etc.)

        Yields:
            Raw chunks from the agent in unified format:
            - {"type": "reply", "content": "text token"}
            - {"type": "thinking", "content": "reasoning token"}
            - {"type": "queryTitle", "content": "tool name"}
            - {"type": "queryArguments", "content": "tool arguments", "tool_id": "unique ID"}
            - {"type": "queryDetail", "content": "tool results", "tool_id": "unique ID"}
            - {"type": "costStatistics", "content": {...}}
            - {"type": "end", "content": ""}
            - {"type": "error", "content": "error message"}
        
        Note:
            Chunk accumulation, element_list construction, reply_id generation, and 
            database persistence are all handled by the adapter layer (e.g., HTTPChatAdapter).
        """

        #-------------------------------------------------
        # Load agent configuration

        config = global_config()
        agent_options = config.get_options_for_agent(agent) if config else {}

        agent_kwargs = {
            # User.
            "user_id"           : query_user_id,
            "session_id"        : kwargs.get("session_id", "") or kwargs.get("trace_id", ""),
            "language"          : kwargs.get("language", "en"),
            "timezone"          : kwargs.get("timezone", "America/Los_Angeles"),
            "token"             : kwargs.get("token", ""),
            # Chat.
            "messages"          : messages,
            "file_list"         : file_list or [],
            # LLM.
            "provider"          : kwargs.get("provider", ""),
            "allowed_tools"     : agent_options.get("allowed_tools"),
            "disallowed_tools"  : agent_options.get("disallowed_tools"),
            "prompt_templates"  : agent_options.get("prompt_templates"),
            "prompt_name"       : prompt_name,
        }

        #-------------------------------------------------

        try:
            # Get agent instance from global registry
            agent_instance = get_global_agent(agent_name = agent, **agent_kwargs)

            if agent_instance:
                # Use the requested agent
                logging.info(f"Using agent '{agent}' for user: {query_user_id}")

            else:
                # Agent initialization failed - fallback with warning
                logging.warning(
                    f"⚠️ Agent '{agent}' failed to initialize for user {query_user_id}. "
                    f"Falling back to DeepAgent."
                )
                
                # Yield warning chunk so frontend knows about the fallback
                yield {
                    "type": "thinking",
                    "content": f"[System] Agent '{agent}' unavailable, using default agent."
                }
                
                # Try Deep as fallback
                agent = "Deep"
                agent_instance = get_global_agent(agent_name = agent, **agent_kwargs)
                
                if not agent_instance:
                    yield {"type": "error", "content": f"No agent instance available"}
                    yield {"type": "end", "content": ""}
                    return

            response_gen = agent_instance.generate_response(**agent_kwargs)

            # Stream chunks directly from agent without accumulation or formatting
            async for chunk in response_gen:
                yield chunk
            
            # Signal end of stream
            yield {"type": "end", "content": ""}

        except Exception as e:
            logging.error(f"Error generating chat response: {str(e)}", exc_info=True)

            yield {"type": "error", "content": f"⚠️ An error occurred: {str(e)}"}
            yield {"type": "end", "content": ""}

#-----------------------------------------------------------------------------
