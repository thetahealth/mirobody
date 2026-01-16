from langchain_core.callbacks import AsyncCallbackHandler

class TokenUsageCallback(AsyncCallbackHandler):
    def __init__(self):
        self.total_input_tokens = 0
        self.total_output_tokens = 0

    async def on_llm_end(self, response, **kwargs):
        if response.llm_output and "token_usage" in response.llm_output:
            usage = response.llm_output["token_usage"]
            self.total_input_tokens += usage.get("prompt_tokens", 0)
            self.total_output_tokens += usage.get("completion_tokens", 0)
        
        # Standard usage_metadata (Gemini/Claude/OpenAI compatible)
        for generation in response.generations:
            for chunk in generation:
                if hasattr(chunk, "message") and hasattr(chunk.message, "usage_metadata"):
                    metadata = chunk.message.usage_metadata
                    if metadata:
                        self.total_input_tokens += metadata.get("input_tokens", 0)
                        self.total_output_tokens += metadata.get("output_tokens", 0)
