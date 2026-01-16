"""
Model adapter factory

Dynamically creates corresponding adapter instances based on provider type.
"""

from typing import Dict, Type

from mirobody.utils.llm.config import AIConfig
from .base import BaseModelAdapter
from .gemini_adapter import GeminiAdapter
from .openai_adapter import OpenAIAdapter
from .volcengine_adapter import VolcengineAdapter


class AdapterFactory:
    """Adapter factory class"""

    # Provider type to adapter class mapping
    _ADAPTER_MAPPING: Dict[str, Type[BaseModelAdapter]] = {
        "openai": OpenAIAdapter,
        "volcengine": VolcengineAdapter,
        "gemini": GeminiAdapter,
        "claude": OpenAIAdapter,  # Claude uses OpenAI-compatible adapter
    }

    @classmethod
    def create_adapter(cls, provider: str) -> BaseModelAdapter:
        """
        Create adapter for specified provider

        Args:
            provider: Provider name

        Returns:
            Corresponding adapter instance

        Raises:
            ValueError: Unsupported provider
        """
        # Get config
        config = AIConfig.get_provider_config(provider)
        provider_type = config.get("type", "openai")

        # Get adapter class
        adapter_class = cls._ADAPTER_MAPPING.get(provider_type)
        if not adapter_class:
            # Default to OpenAI adapter (compatibility)
            adapter_class = OpenAIAdapter

        # Create adapter instance
        return adapter_class(provider, config)

    @classmethod
    def register_adapter(cls, provider_type: str, adapter_class: Type[BaseModelAdapter]):
        """
        Register new adapter type

        Args:
            provider_type: Provider type
            adapter_class: Adapter class
        """
        cls._ADAPTER_MAPPING[provider_type] = adapter_class

    @classmethod
    def get_supported_types(cls) -> list:
        """Get list of supported provider types"""
        return list(cls._ADAPTER_MAPPING.keys())

    @classmethod
    def get_adapter_info(cls) -> Dict[str, str]:
        """Get adapter information summary"""
        return {provider_type: adapter_class.__name__ for provider_type, adapter_class in cls._ADAPTER_MAPPING.items()}


# Global factory instance
adapter_factory = AdapterFactory()
