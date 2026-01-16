import logging
import json
import ast
from typing import Dict, Any, Optional, Generator
from dataclasses import dataclass, field

from mirobody.pub.agents.deep.stream_converter.smart_truncate import smart_truncate

logger = logging.getLogger(__name__)


MAX_CUTOFF_LEN = 500
@dataclass
class StreamState:
    """
    Maintains streaming state to ensure queryTitle and queryDetail are one-to-one and non-duplicated.
    """
    # Set of tool_call_ids that have emitted queryTitle (prevent duplicates)
    emitted_tool_titles: set = field(default_factory=set)
    
    # Set of tool_call_ids that have emitted queryDetail (prevent duplicates)
    emitted_tool_details: set = field(default_factory=set)
    
    # Accumulate parameter JSON strings by tool_call_id
    tool_args_buffer: Dict[str, str] = field(default_factory=dict)
    
    # Store tool names by tool_call_id
    tool_names: Dict[str, str] = field(default_factory=dict)
    
    # Store completion flags by tool_call_id
    tool_args_complete: set = field(default_factory=set)
    
    # Current message_id being processed (for detecting new stream start)
    current_message_id: Optional[str] = None
    
    # Current tool_call_id being built (for handling chunks with id=None)
    current_tool_id: Optional[str] = None
    
    def reset(self):
        """Reset all state."""
        self.emitted_tool_titles.clear()
        self.emitted_tool_details.clear()
        self.tool_args_buffer.clear()
        self.tool_names.clear()
        self.tool_args_complete.clear()
        self.current_message_id = None
        self.current_tool_id = None


class StreamConverter:
    """
    Converts deepagents (LangGraph) stream output to unified format matching frontend.
    
    Uses stream_mode="messages" to process streaming chunks and metadata.
    
    Supported output types: reply, thinking, queryTitle, queryDetail, costStatistics, error
    
    Core features:
    - Ensures strict one-to-one correspondence between queryTitle and queryDetail for each tool call
    - Prevents duplicate emissions via state machine
    - Real-time processing of LLM token streams
    - Intelligent truncation with field preservation
    """
    
    def __init__(self):
        """Initialize converter instance, maintain streaming state."""
        self.state = StreamState()
    
    def convert_message_chunk(
        self, 
        chunk: Any, 
        metadata: Dict[str, Any],
        trace_id: str = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Convert LangGraph message chunk to unified format.
        
        Args:
            chunk: LangChain AIMessageChunk or ToolMessage object
            metadata: Metadata containing langgraph_step, langgraph_node, etc.
            trace_id: Optional trace ID for logging
            
        Yields:
            Unified format event dictionary
            
        Chunk structure examples:
        - AIMessageChunk: 
          - tool_calls: [{'name': 'tool_name', 'id': 'call_xxx', 'args': {}}]  # First chunk has complete name and id
          - tool_call_chunks: [{'args': '{"', ...}]  # Subsequent chunks accumulate args incrementally
          - content: 'token'  # Text token
          - response_metadata: {'finish_reason': 'tool_calls'/'stop'}
        - ToolMessage:
          - content: 'Tool return result'
        """
        try:
            chunk_type = type(chunk).__name__
            
            # Detect if it's a new message stream (via message_id)
            message_id = getattr(chunk, 'id', None)
            if message_id and message_id != self.state.current_message_id:
                # New message stream started, but don't reset state (multi-turn conversations may be related)
                self.state.current_message_id = message_id
            
            # Extract metadata information
            node_info = self._extract_metadata_info(metadata)
            
            # Process different types of chunks
            if chunk_type == "AIMessageChunk":
                yield from self._handle_ai_message_chunk(chunk, node_info, trace_id)
            elif chunk_type == "ToolMessage":
                yield from self._handle_tool_message_chunk(chunk, node_info, trace_id)

                # Extract and yield chart data if present
                chart_result = self._extract_chart_from_result(chunk)
                if chart_result:
                    yield chart_result
            
        except Exception as e:
            logger.error(f"Error converting message chunk: {str(e)}", exc_info=True)
    
    def _handle_ai_message_chunk(
        self, 
        chunk: Any, 
        node_info: Dict[str, Any],
        trace_id: str = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Handle AIMessageChunk.
        
        Lifecycle:
        1. First chunk: tool_calls has name and id → emit queryTitle
        2. Subsequent chunks: tool_call_chunks accumulate args
        3. finish_reason='tool_calls': args complete → emit queryDetail
        4. content non-empty: emit reply token
        """
        # 1. Check for complete tool_calls (tool call start)
        tool_calls = getattr(chunk, 'tool_calls', [])
        if tool_calls:
            for tool_call in tool_calls:
                tool_id = tool_call.get('id')
                tool_name = tool_call.get('name')
                tool_args = tool_call.get('args', {})
                
                # Log tool call with full args (no truncation)
                if tool_id and tool_name:
                    logger.info(
                        f"[Tool Call] trace_id={trace_id}, "
                        f"tool={tool_name}, "
                        f"args={tool_args}, "
                        f"id={tool_id}"
                    )
                
                # Emit queryTitle (only once)
                if tool_id and tool_name and tool_id not in self.state.emitted_tool_titles:
                    self.state.tool_names[tool_id] = tool_name
                    self.state.current_tool_id = tool_id  # Set current tool being processed
                    
                    yield {
                        "type": "queryTitle",
                        "content": self._get_tool_display_name(tool_name),
                        "tool_id": tool_id,
                    }
                    
                    self.state.emitted_tool_titles.add(tool_id)
                    logger.debug(f"Emitted queryTitle for tool: {tool_name}, id: {tool_id}")
        
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
                            # 🔧 Gemini uses 'thinking' field for thinking content, not 'text'
                            # This is the ONLY reliable way to identify thinking blocks
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
    def _extract_chart_from_result(chunk: any) -> Optional[Dict[str, Any]]:
        """Extract chart data from tool result (called before truncation)"""

        result_content = getattr(chunk, 'content', '')
        try:
            tool_res = None
            if isinstance(result_content, dict):
                tool_res = result_content
                logger.info("Result is already a dict")
            elif isinstance(result_content, str):
                try:
                    tool_res = json.loads(result_content)
                    logger.info("Parsed as JSON")
                except (json.JSONDecodeError, TypeError):
                    try:
                        tool_res = ast.literal_eval(result_content)
                        logger.info("Parsed as Python dict using ast.literal_eval")
                    except (ValueError, SyntaxError) as e:
                        # logger.warning(f"Failed to parse result string: {e}")
                        return None
            
            if not isinstance(tool_res, dict):
                logger.debug(f"tool_res is not a dict: {type(tool_res)}")
                return None
            
            logger.info(f"Checking chart data: is_chart={tool_res.get('is_chart')}, success={tool_res.get('success')}")
            
            # Check if it's chart data
            if tool_res.get("is_chart") and tool_res.get("success"):
                chart_data = {
                    "title": tool_res.get("chart_title", ""),
                    "url": tool_res.get("url", ""),
                    "filename": tool_res.get("filename", "")
                }
                logger.info(f"Extracted chart data: {chart_data}")
                return {"type": "image", "content": json.dumps(chart_data, ensure_ascii=False)}
            
            return None
                    
        except Exception as e:
            logger.error(f"Failed to extract chart data: {e}", exc_info=True)
            return None

    
    def _handle_tool_message_chunk(
        self, 
        chunk: Any, 
        node_info: Dict[str, Any],
        trace_id: str = None
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Handle ToolMessage - tool execution result.
        
        Note: This is the tool's return value, different from queryDetail (tool's parameters).
        """
        content = getattr(chunk, 'content', '')
        tool_call_id = getattr(chunk, 'tool_call_id', '')
        
        if content:
            # Log full tool result (no truncation)
            logger.info(
                f"[Tool Result] trace_id={trace_id}, "
                f"tool_id={tool_call_id}, "
                f"result={content}"
            )
            
            # Send truncated content to frontend
            yield {
                "type": "queryDetail",
                "content": smart_truncate(content, MAX_CUTOFF_LEN),
                "tool_id": tool_call_id,
            }

            
    
    def _extract_metadata_info(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
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
    def _get_tool_display_name(tool_name: str) -> str:
        """
        Get tool display name.
        
        Args:
            tool_name: Original tool name
            
        Returns:
            Display name or original name
        """
        # Try to load from config
        try:
            from mirobody.utils.config import global_config
            config = global_config()
            if config:
                tool_display_names = config.get_dict("TOOL_DISPLAY_NAMES_DEEP", {})
                return tool_display_names.get(tool_name, tool_name)
        except Exception:
            pass
        
        return tool_name
    
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
                "claude-ops-4.5": {"input": 5.00, "output": 25.00},
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
            rates = None
            matched_key = ""
            for key, pricing in PRICING.items():
                if key in model_name and len(key) > len(matched_key):
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
    
    def reset_state(self):
        """Reset converter state (for new conversation session)."""
        self.state.reset()
        logger.debug("Converter state reset")


converter = StreamConverter()
