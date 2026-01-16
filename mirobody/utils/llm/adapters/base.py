"""
Base model adapter

Defines common interface and base implementation for all model adapters.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Dict, List, Optional


class BaseModelAdapter(ABC):
    """Base model adapter class"""

    def __init__(self, provider: str, config: Dict[str, Any]):
        self.provider = provider
        self.config = config
        self.model = config.get("model")
        self.api_key = config.get("api_key")
        self.api_base = config.get("api_base")

    @abstractmethod
    async def batch_completion(self, messages: List[Dict], **kwargs) -> Dict[str, Any]:
        """
        Batch completion request

        Args:
            messages: Message list
            **kwargs: Model-specific parameters

        Returns:
            Unified format response: {"content": str, "function_calls": List or None}
        """
        pass

    @abstractmethod
    async def stream_completion(self, messages: List[Dict], **kwargs) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Streaming completion request

        Args:
            messages: Message list
            **kwargs: Model-specific parameters

        Yields:
            Streaming response data
        """
        pass

    def format_messages(self, messages: List[Dict]) -> List[Dict]:
        """Format messages (subclasses can override)"""
        return messages

    def set_default_parameters(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Set default parameters"""
        # Set appropriate max_tokens based on model
        model = self.model or ""

        # GPT-4.1 model token limit
        if "gpt-4.1" in model.lower():
            default_max_tokens = 32768
        # GPT-4 token limit
        elif "gpt-4" in model.lower() and "mini" not in model.lower():
            default_max_tokens = 32768
        # GPT-4o-mini token limit
        elif "gpt-4o-mini" in model.lower():
            default_max_tokens = 16384
        # GPT-3.5 token limit
        elif "gpt-3.5" in model.lower():
            default_max_tokens = 16384
        # Other models use smaller default for compatibility
        else:
            default_max_tokens = 16384

        defaults = {"max_tokens": default_max_tokens, "temperature": 0.1, "stream": False}

        # Merge user parameters, user params take priority
        for key, value in defaults.items():
            kwargs.setdefault(key, value)

        return kwargs

    def log_usage(self, usage_info: Dict[str, Any], time_cost: float):
        """Log usage information"""
        logging.info(f"Provider: {self.provider}, Usage: {usage_info}, Time: {time_cost:.3f}s")

    def handle_error(self, error: Exception, context: str = "") -> Dict[str, Any]:
        """Unified error handling"""
        error_msg = f"{self.provider} {context} error: {type(error).__name__}: {str(error)}"
        logging.error(error_msg, stack_info=True)
        return {"content": "", "function_calls": None, "error": error_msg}

    def supports_function_calling(self) -> bool:
        """Check if function calling is supported (subclasses can override)"""
        return True

    def convert_function_format(self, functions: List[Dict]) -> Any:
        """Convert function format (subclasses can override)"""
        return functions

    def extract_function_calls(self, response: Any) -> Optional[List[Dict]]:
        """Extract function calls (subclasses can override)"""
        return None
