import logging
from typing import Dict, Any, AsyncGenerator

from langchain_core.callbacks import AsyncCallbackHandler

from .constants import FINAL_OUTPUT_NODES, MODEL_PRICING

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
            if chunk_type == "AIMessageChunk" and node_name in FINAL_OUTPUT_NODES:
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
                    if step not in FINAL_OUTPUT_NODES:
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
    def create_cost_statistics(
        input_tokens: int,
        output_tokens: int,
        model_name: str = "unknown",
        cache_read_tokens: int = 0,
        cache_creation_tokens: int = 0
    ) -> Dict[str, Any] | None:
        """
        Create cost statistics with prompt caching support.

        Token breakdown (per Anthropic/OpenRouter API):
        - input_tokens: Non-cached input tokens (charged at full input rate)
        - cache_read_tokens: Tokens read from cache (charged at ~10% of input rate)
        - cache_creation_tokens: Tokens written to cache (charged at ~125% of input rate)

        Args:
            input_tokens: Non-cached input tokens
            output_tokens: Output tokens
            model_name: Model name for pricing lookup
            cache_read_tokens: Tokens read from cache (prompt caching hit)
            cache_creation_tokens: Tokens written to cache (prompt caching miss)

        Returns:
            Cost statistics dictionary or None on error
        """
        try:
            # Detect format:
            # - OpenAI/OpenRouter: input_tokens includes cache_read_tokens (input >= cache_read)
            # - Anthropic: input_tokens is non-cached only (input < cache_read typically)
            is_openai_format = input_tokens >= cache_read_tokens and cache_read_tokens > 0

            if is_openai_format:
                # OpenAI format: input_tokens already includes cache_read
                total_input = input_tokens + cache_creation_tokens
                non_cached_input = input_tokens - cache_read_tokens
            else:
                # Anthropic format: input_tokens is non-cached only
                total_input = input_tokens + cache_read_tokens + cache_creation_tokens
                non_cached_input = input_tokens

            total_tokens = total_input + output_tokens

            # Find matching pricing by substring match (prefer longest key for precision)
            model_name_lower = model_name.lower()
            rates = None
            matched_key = ""
            for key, pricing in MODEL_PRICING.items():
                if key in model_name_lower and len(key) > len(matched_key):
                    matched_key = key
                    rates = pricing

            # Calculate costs
            total_cost = None
            cost_saved = None

            if rates:
                input_rate = rates["input"]
                output_rate = rates["output"]

                # Cache pricing rules:
                # - cache_read = input * 0.1 (all models, 90% discount)
                # - cache_creation = input * 0.25 (Claude only, add 25% premium)
                cache_read_rate = input_rate * 0.1
                is_claude = "claude" in matched_key
                cache_creation_rate = input_rate * 0.25 if is_claude else 0

                # Cost breakdown:
                # - non_cached_input: Fresh processing → full input rate
                # - cache_read_tokens: Cache hits → discounted rate (all models)
                # - cache_creation_tokens: Cache writes → premium rate (Claude only)
                input_cost = (non_cached_input / 1_000_000) * input_rate
                output_cost = (output_tokens / 1_000_000) * output_rate
                cache_read_cost = (cache_read_tokens / 1_000_000) * cache_read_rate
                cache_creation_cost = (cache_creation_tokens / 1_000_000) * cache_creation_rate

                total_cost = round(input_cost + output_cost + cache_read_cost + cache_creation_cost, 6)

                # Calculate savings: cache_read tokens at discounted rate vs full input rate
                if cache_read_tokens > 0:
                    cost_without_cache = (cache_read_tokens / 1_000_000) * input_rate
                    cost_saved = round(cost_without_cache - cache_read_cost, 6)

            # Build response (all values as strings for stability)
            content = {
                "model": model_name,
                "input_tokens": str(total_input),
                "output_tokens": str(output_tokens),
                "total_tokens": str(total_tokens),
                "total_cost": f"{total_cost:.6f}" if total_cost is not None else "unrecognized model",
            }

            # Add cache info only if cache was used
            if cache_read_tokens > 0:
                content["cache_read_tokens"] = str(cache_read_tokens)
                if cost_saved is not None and cost_saved > 0:
                    content["cost_saved"] = f"{cost_saved:.6f}"

            return {
                "type": "costStatistics",
                "content": content
            }
        except Exception as e:
            logger.error(f"Failed to create cost statistics: {e}")
            return None

class TokenUsageCallback(AsyncCallbackHandler):
    """
    Callback handler to track token usage across LLM calls.

    Supports multiple providers:
    - Anthropic (direct API): cache_read_input_tokens, cache_creation_input_tokens
    - OpenRouter: prompt_tokens_details.cached_tokens
    - OpenAI: prompt_tokens_details.cached_tokens
    - Gemini: usage_metadata.input_tokens
    """

    def __init__(self):
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.cache_read_tokens = 0
        self.cache_creation_tokens = 0

    async def on_llm_end(self, response, **kwargs):
        # Track if we've already extracted tokens to avoid double counting
        tokens_extracted = False
        cache_extracted = False

        # Debug: log full response structure
        logger.debug(f"[TokenUsage] llm_output keys: {response.llm_output.keys() if response.llm_output else 'None'}")

        # Method 1: response.llm_output["token_usage"] (OpenAI/OpenRouter format)
        if response.llm_output and "token_usage" in response.llm_output:
            usage = response.llm_output["token_usage"]
            logger.debug(f"[TokenUsage] token_usage: {usage}")

            # Extract cache tokens first (needed for input calculation)
            cached_tokens = 0
            if not cache_extracted:
                # Anthropic format
                self.cache_read_tokens += usage.get("cache_read_input_tokens", 0)
                self.cache_creation_tokens += usage.get("cache_creation_input_tokens", 0)

                # OpenRouter/OpenAI format: prompt_tokens_details.cached_tokens
                prompt_details = usage.get("prompt_tokens_details") or {}
                if prompt_details:
                    logger.debug(f"[TokenUsage] prompt_tokens_details: {prompt_details}")
                    cached_tokens = prompt_details.get("cached_tokens", 0)
                    if cached_tokens > 0:
                        self.cache_read_tokens += cached_tokens
                        cache_extracted = True

                if self.cache_read_tokens > 0 or self.cache_creation_tokens > 0:
                    cache_extracted = True

            if not tokens_extracted:
                # OpenAI/OpenRouter: prompt_tokens (may include cached)
                # Anthropic: input_tokens (non-cached only)
                # Pass raw value - create_cost_statistics handles both formats
                prompt_tokens = usage.get("prompt_tokens", 0) or usage.get("input_tokens", 0)
                self.total_input_tokens += prompt_tokens
                self.total_output_tokens += usage.get("completion_tokens", 0) or usage.get("output_tokens", 0)
                tokens_extracted = True

        # Method 2: message.usage_metadata (LangChain standard)
        for generation in response.generations:
            for chunk in generation:
                if not hasattr(chunk, "message"):
                    continue

                msg = chunk.message

                # usage_metadata (Gemini/Claude/newer LangChain versions)
                if hasattr(msg, "usage_metadata") and msg.usage_metadata:
                    metadata = msg.usage_metadata
                    logger.debug(f"[TokenUsage] usage_metadata: {metadata}")

                    if not tokens_extracted:
                        self.total_input_tokens += metadata.get("input_tokens", 0)
                        self.total_output_tokens += metadata.get("output_tokens", 0)
                        tokens_extracted = True

                    if not cache_extracted:
                        # Anthropic cache in usage_metadata
                        self.cache_read_tokens += metadata.get("cache_read_input_tokens", 0)
                        self.cache_creation_tokens += metadata.get("cache_creation_input_tokens", 0)

                        # input_token_details format (some LangChain versions)
                        input_details = metadata.get("input_token_details") or {}
                        if input_details:
                            logger.debug(f"[TokenUsage] input_token_details: {input_details}")
                            self.cache_read_tokens += input_details.get("cache_read", 0)
                            self.cache_creation_tokens += input_details.get("cache_creation", 0)

                        if self.cache_read_tokens > 0 or self.cache_creation_tokens > 0:
                            cache_extracted = True

                # response_metadata.usage (Anthropic specific, only for cache)
                if not cache_extracted and hasattr(msg, "response_metadata") and msg.response_metadata:
                    resp_usage = msg.response_metadata.get("usage", {})
                    if resp_usage:
                        logger.debug(f"[TokenUsage] response_metadata.usage: {resp_usage}")
                        self.cache_read_tokens += resp_usage.get("cache_read_input_tokens", 0)
                        self.cache_creation_tokens += resp_usage.get("cache_creation_input_tokens", 0)

        logger.info(
            f"[TokenUsage] input={self.total_input_tokens}, output={self.total_output_tokens}, "
            f"cache_read={self.cache_read_tokens}, cache_creation={self.cache_creation_tokens}"
        )
