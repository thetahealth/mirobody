import logging
from typing import Dict, Any, AsyncGenerator

logger = logging.getLogger(__name__)


class StreamConverter:
    """
    Pure static utility class for converting LangGraph stream output to unified format.
    
    Uses stream_mode="messages" for real-time LLM token streaming.
    Tool calls and results are handled via stream_mode="updates" in deep_agent.py.
    
    Supported output types: reply, thinking, costStatistics
    
    Core features:
    - Real-time processing of LLM token streams
    - Stateless pure functions
    - Complete data pass-through to upper layers
    """

    FINAL_OUTPUT_NODES = {
        "tools",           # Tools node
        "model",           # Direct model invocation
    }
    
    @staticmethod
    async def convert_message_chunk(
        chunk: Any, 
        metadata: Dict[str, Any],
        trace_id: str = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Convert LangGraph message chunk to unified format.
        
        Only processes AIMessageChunk for text/thinking content.
        Tool calls are handled separately via updates mode.
        
        Args:
            chunk: LangChain AIMessageChunk object
            metadata: Metadata containing langgraph_step, langgraph_node, etc.
            trace_id: Optional trace ID for logging
            
        Yields:
            Unified format event dictionary with type: reply or thinking
        """
        try:
            chunk_type = type(chunk).__name__
            
            # Extract metadata information
            node_info = StreamConverter._extract_metadata_info(metadata)
            node_name = node_info.get("node")
            
            # Only process AIMessageChunk for text content from final output nodes
            if chunk_type == "AIMessageChunk" and node_name in StreamConverter.FINAL_OUTPUT_NODES:
                async for event in StreamConverter._handle_ai_message_chunk(chunk, node_info, trace_id):
                    yield event
            
        except Exception as e:
            logger.error(f"Error converting message chunk: {str(e)}", exc_info=True)
    
    @staticmethod
    async def _handle_ai_message_chunk(
        chunk: Any, 
        node_info: Dict[str, Any],
        trace_id: str = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Handle AIMessageChunk - only process text content for real-time streaming.
        
        Tool calls are handled separately via updates mode in deep_agent.py.
        """
        content = getattr(chunk, 'content', '')
        if content:
            # Handle list format from Gemini (e.g., [{'type': 'text', 'text': '...', 'index': 0}])
            if isinstance(content, list):
                text_parts = []
                thinking_parts = []
                
                for block in content:
                    if isinstance(block, dict):
                        block_type = block.get('type')
                        if block_type == 'text':
                            text_parts.append(block.get('text', ''))
                        elif block_type == 'thinking':
                            # Gemini uses 'thinking' field for thinking content
                            thinking_parts.append(block.get('thinking', ''))
                
                # Output thinking content separately with type="thinking"
                if thinking_parts:
                    thinking_content = ''.join(thinking_parts)
                    if thinking_content:
                        yield {
                            "type": "thinking",
                            "content": thinking_content,
                            **node_info
                        }
                
                content = ''.join(text_parts)
            
            # Output regular text content as reply
            if content: 
                yield {
                    "type": "reply",
                    "content": content,
                    **node_info
                }
    
    @staticmethod
    def _extract_metadata_info(metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key information from metadata.
        
        Args:
            metadata: Metadata provided by LangGraph
            
        Returns:
            Dictionary containing node, step, and other information
        """
        return {
            "node": metadata.get('langgraph_node'),
            "step": metadata.get('langgraph_step'),
            "model": metadata.get('ls_model_name'),
        }
    
    @staticmethod
    async def process_stream_event(
        stream_type: str,
        stream_event: Any,
        trace_id: str = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Process a single stream event from LangGraph agent.astream().
        
        Handles both "messages" and "updates" stream modes:
        - messages: Real-time token-by-token text streaming
        - updates: Tool calls and tool results (complete messages)
        
        Args:
            stream_type: Either "messages" or "updates"
            stream_event: The event data from LangGraph stream
            trace_id: Optional trace ID for logging
            
        Yields:
            Unified format event dictionaries with types:
            - reply, thinking (from messages mode)
            - queryTitle (tool call info)
            - queryDetail, image (tool results)
        """
        from langchain_core.messages import AIMessage, ToolMessage
        
        try:
            # Handle text streaming (real-time token-by-token)
            if stream_type == "messages":
                try:
                    chunk, chunk_metadata = stream_event
                except (TypeError, ValueError) as e:
                    logger.warning(
                        f"Invalid messages event format: {type(stream_event).__name__}, "
                        f"error: {str(e)}, trace_id={trace_id}"
                    )
                    return
                
                async for event in StreamConverter.convert_message_chunk(chunk, chunk_metadata, trace_id=trace_id):
                    if event:
                        yield event
            
            # Handle tool calls and results (complete messages)
            elif stream_type == "updates":
                for step, step_data in stream_event.items():
                    # Only process model and tools steps
                    if step not in StreamConverter.FINAL_OUTPUT_NODES:
                        continue
                    
                    # Validate step_data
                    if step_data is None or not isinstance(step_data, dict):
                        logger.debug(
                            f"Skipping node '{step}' with invalid step_data type: "
                            f"{type(step_data).__name__}, trace_id={trace_id}"
                        )
                        continue
                    
                    # Validate messages field
                    if 'messages' not in step_data:
                        logger.debug(
                            f"Skipping node '{step}' without messages field, trace_id={trace_id}"
                        )
                        continue
                    
                    if not step_data['messages'] or not isinstance(step_data['messages'], list):
                        logger.debug(
                            f"Skipping node '{step}' with invalid messages: "
                            f"{type(step_data.get('messages')).__name__}, trace_id={trace_id}"
                        )
                        continue
                    
                    # Get the last message in this step (latest update)
                    last_message = step_data['messages'][-1]
                    
                    # Step "model": Extract AIMessage content and tool_calls
                    if step == "model":
                        if isinstance(last_message, AIMessage):
                            # Handle tool calls
                            if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
                                for tc in last_message.tool_calls:
                                    tool_name = tc.get('name', '')
                                    tool_id = tc.get('id', '')
                                    tool_args = tc.get('args', {})
                                    
                                    if tool_name and tool_id:
                                        # Log complete tool call
                                        logger.info(
                                            f"[Tool Call] trace_id={trace_id},"
                                            f"tool={tool_name}, args={tool_args}, id={tool_id}"
                                        )
                                        
                                        # Emit queryTitle with display name
                                        yield {
                                            "type": "queryTitle",
                                            "content": tool_name,
                                            "tool_id": tool_id,
                                            "args": tool_args,
                                        }
                    
                    # Step "tools": Extract ToolMessage results
                    elif step == "tools":
                        if isinstance(last_message, ToolMessage):
                            tool_content = last_message.content 
                            tool_call_id = last_message.tool_call_id
                            
                            if tool_content:
                                # Log complete tool result
                                logger.info(
                                    f"[Tool Result] trace_id={trace_id}, "
                                    f"tool_id={tool_call_id}, result={tool_content}"
                                )
                                
                                # Emit queryDetail (no truncation, pass complete data to upper layer)
                                yield {
                                    "type": "queryDetail",
                                    "content": tool_content,
                                    "tool_id": tool_call_id,
                                }
                                
                                # Extract and emit chart data if present
                                chart_event = StreamConverter.extract_chart_data(tool_content, tool_call_id)
                                if chart_event:
                                    yield chart_event
                                    
        except Exception as e:
            logger.error(
                f"Error processing stream event: {str(e)}, "
                f"stream_type={stream_type}, "
                f"event_type={type(stream_event).__name__}, "
                f"trace_id={trace_id}", 
                exc_info=True
            )
    
    @staticmethod
    def extract_chart_data(tool_content: str, tool_call_id: str) -> Dict[str, Any] | None:
        """
        Extract chart data from tool result and return image event.
        
        Args:
            tool_content: Tool result content (JSON string or dict)
            tool_call_id: Tool call ID
            
        Returns:
            Image event dict if chart data found, None otherwise
        """
        import json
        import ast
        
        try:
            # Parse content to dict
            tool_res = None
            if isinstance(tool_content, dict):
                tool_res = tool_content
            elif isinstance(tool_content, str):
                try:
                    tool_res = json.loads(tool_content)
                except (json.JSONDecodeError, TypeError):
                    try:
                        tool_res = ast.literal_eval(tool_content)
                    except (ValueError, SyntaxError):
                        return None
            
            if not isinstance(tool_res, dict):
                return None
            
            # Check if it's chart data
            if tool_res.get("is_chart") and tool_res.get("success"):
                chart_data = {
                    "title": tool_res.get("chart_title", ""),
                    "url": tool_res.get("url", ""),
                    "filename": tool_res.get("filename", "")
                }
                return {
                    "type": "image",
                    "content": json.dumps(chart_data, ensure_ascii=False),
                    "tool_id": tool_call_id
                }
            
            return None
        except Exception as e:
            logger.error(f"Failed to extract chart data: {e}", exc_info=True)
            return None
    
    @staticmethod
    def create_cost_statistics(input_tokens, output_tokens, model_name: str = "unknown") -> Dict[str, Any]:
        """
        Create cost statistics (unified format).
        
        Args:
            usage_data: Token usage data
            model_name: Model name
            
        Returns:
            Cost statistics dictionary
        """
        try:
            # Pricing configuration (USD per million tokens)
            # Keys are substrings to match against model_name
            PRICING = {
                "claude-sonnet-4.5": {"input": 3.00, "output": 15.00},
                "claude-haiku-4.5": {"input": 1.00, "output": 5.00},
                "claude-opus-4.5": {"input": 5.00, "output": 25.00},
                "gemini-3-flash": {"input": 0.50, "output": 3.00},
                "gemini-3-pro": {"input": 2.00, "output": 12.00},
                "gpt-5-mini": {"input": 0.25, "output": 2.00},
                "gpt-5.2": {"input": 1.75, "output": 14.00},
                "deepseek-v3.2": {"input": 0.24, "output": 0.38},
                "kimi-k2-thinking": {"input": 0.40, "output": 1.75},
                "kimi-k2": {"input": 0.39, "output": 1.90},
            }
            
            # Always calculate total tokens
            total_tokens = input_tokens + output_tokens
            
            # Find matching pricing by substring match (prefer longest key for precision)
            model_name_lower = model_name.lower()
            rates = None
            matched_key = ""
            for key, pricing in PRICING.items():
                if key in model_name_lower and len(key) > len(matched_key):
                    matched_key = key
                    rates = pricing
            
            # Calculate cost if model pricing is available
            if rates:
                input_cost = (input_tokens / 1_000_000) * rates["input"]
                output_cost = (output_tokens / 1_000_000) * rates["output"]
                total_cost = round(input_cost + output_cost, 5)
            else:
                total_cost = "unrecognized model"
            
            return {
                "type": "costStatistics",
                "content": {
                    "model": model_name,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "total_tokens": total_tokens,
                    "total_cost": total_cost
                }
            }
        except Exception as e:
            logger.error(f"Failed to create cost statistics: {e}")
            return None
