import logging
from langchain_core.callbacks import AsyncCallbackHandler

logger = logging.getLogger(__name__)

class TokenUsageCallback(AsyncCallbackHandler):
    def __init__(self):
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.cache_read_tokens = 0
        self.cache_creation_tokens = 0

    async def on_llm_end(self, response, **kwargs):
        # Debug: log full response structure
        logger.info(f"[TokenUsage] llm_output keys: {response.llm_output.keys() if response.llm_output else 'None'}")
        
        if response.llm_output and "token_usage" in response.llm_output:
            usage = response.llm_output["token_usage"]
            logger.info(f"[TokenUsage] token_usage: {usage}")
            self.total_input_tokens += usage.get("prompt_tokens", 0)
            self.total_output_tokens += usage.get("completion_tokens", 0)
            # Anthropic cache tokens (non-streaming)
            self.cache_read_tokens += usage.get("cache_read_input_tokens", 0)
            self.cache_creation_tokens += usage.get("cache_creation_input_tokens", 0)
        
        # Standard usage_metadata (Gemini/Claude/OpenAI compatible)
        for generation in response.generations:
            for chunk in generation:
                if hasattr(chunk, "message"):
                    msg = chunk.message
                    
                    # Try usage_metadata first
                    if hasattr(msg, "usage_metadata") and msg.usage_metadata:
                        metadata = msg.usage_metadata
                        logger.info(f"[TokenUsage] usage_metadata: {metadata}")
                        self.total_input_tokens += metadata.get("input_tokens", 0)
                        self.total_output_tokens += metadata.get("output_tokens", 0)
                        
                        # Anthropic cache in usage_metadata (newer versions)
                        self.cache_read_tokens += metadata.get("cache_read_input_tokens", 0)
                        self.cache_creation_tokens += metadata.get("cache_creation_input_tokens", 0)
                        
                        # Also try input_token_details (some Langchain versions)
                        input_details = metadata.get("input_token_details") or {}
                        if input_details:
                            logger.info(f"[TokenUsage] input_token_details: {input_details}")
                        self.cache_read_tokens += input_details.get("cache_read", 0)
                        self.cache_creation_tokens += input_details.get("cache_creation", 0)
                    
                    # Try response_metadata.usage (Anthropic specific)
                    if hasattr(msg, "response_metadata") and msg.response_metadata:
                        resp_usage = msg.response_metadata.get("usage", {})
                        if resp_usage:
                            logger.info(f"[TokenUsage] response_metadata.usage: {resp_usage}")
                            self.cache_read_tokens += resp_usage.get("cache_read_input_tokens", 0)
                            self.cache_creation_tokens += resp_usage.get("cache_creation_input_tokens", 0)
        
        logger.info(f"[TokenUsage] Final: input={self.total_input_tokens}, output={self.total_output_tokens}, cache_read={self.cache_read_tokens}, cache_creation={self.cache_creation_tokens}")
