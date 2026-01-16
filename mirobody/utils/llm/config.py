"""
AI model configuration management module

Manages AI model configurations, provides validation and retrieval interfaces,
supports dynamic configuration loading, and auto-selects providers based on available API keys.
"""

from typing import Any, Dict, List, Optional

from mirobody.utils.config import safe_read_cfg


class AIConfig:
    """AI model configuration manager"""

    _PROVIDER = {
        "openai": {
            "base_url": "https://api.openai.com/v1",
        },
        "volcengine": {
            "base_url": "https://ark.cn-beijing.volces.com/api/v3",
        },
        "dashscope": {
            "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        },
    }

    # Default provider priority list (sorted by priority)
    # Used for auto-selecting available providers
    _DEFAULT_PROVIDER_PRIORITY: List[Dict[str, Any]] = [
        {
            "name": "openai",
            "api_key_env": "OPENAI_API_KEY",
            "default_model": "gpt-4.1",
            "description": "OpenAI GPT Models",
        },
        {
            "name": "openrouter",
            "api_key_env": "OPENROUTER_API_KEY",
            "default_model": "google/gemini-2.5-flash",
            "description": "OpenRouter (Multi-model Gateway)",
        },
        {
            "name": "claude",
            "api_key_env": "ANTHROPIC_API_KEY",
            "default_model": "claude-3-7-sonnet-20250219",
            "description": "Anthropic Claude",
        },
        {
            "name": "gemini",
            "api_key_env": "GOOGLE_API_KEY",
            "default_model": "gemini-2.5-flash",
            "description": "Google Gemini",
        },
        {
            "name": "volcengine",
            "api_key_env": "VOLCENGINE_API_KEY",
            "default_model": "doubao-1.5-thinking-pro-250415",
            "description": "Volcengine Doubao",
        },
        {
            "name": "dashscope",
            "api_key_env": "DASHSCOPE_API_KEY",
            "default_model": "qwen-max",
            "description": "Aliyun DashScope (Qwen)",
        },
    ]

    # AI provider configurations
    _CONFIG = {
        "gpt4o-mini": {
            "model": "gpt-4o-mini",
            "api_key_env": "OPENAI_API_KEY",
            "api_base": "https://api.openai.com",
            "api_path": "/v1/chat/completions",
            "type": "openai",
        },
        "gpt-4o": {
            "model": "gpt-4o",
            "api_key_env": "OPENAI_API_KEY",
            "api_base": "https://api.openai.com",
            "api_path": "/v1/chat/completions",
            "type": "openai",
        },
        "gpt-4.1": {
            "model": "gpt-4.1",
            "api_key_env": "OPENAI_API_KEY",
            "api_base": "https://api.openai.com",
            "api_path": "/v1/chat/completions",
            "type": "openai",
        },
        "gpt-o3": {
            "model": "o3",
            "api_key_env": "OPENAI_API_KEY",
            "api_base": "https://api.openai.com",
            "api_path": "/v1/chat/completions",
            "type": "openai",
        },
        "volcengine": {
            "model": "doubao-1.5-thinking-pro-250415",
            "api_key_env": "VOLCENGINE_API_KEY",
            "api_base": "https://ark.cn-beijing.volces.com/api/v3",
            "api_path": "/chat/completions",
            "type": "volcengine",
        },
        "doubao-lite": {
            "model": "doubao-1-5-lite-32k-250115",
            "api_key_env": "VOLCENGINE_API_KEY",
            "api_base": "https://ark.cn-beijing.volces.com/api/v3",
            "api_path": "/chat/completions",
            "type": "volcengine",
        },
        "claude": {
            "model": "claude-3-7-sonnet-20250219",
            "api_key_env": "ANTHROPIC_API_KEY",
            "api_base": "https://api.anthropic.com",
            "api_path": "/v1/messages",
            "type": "claude",
        },
        "gemini": {
            "model": "gemini-3-flash-preview",
            "api_key_env": "GOOGLE_API_KEY",
            "api_base": "https://generativelanguage.googleapis.com",
            "api_path": "/v1/chat/completions",
            "type": "gemini",
        },
        "openai": {
            "model": "gpt-4o-mini",
            "api_key_env": "OPENAI_API_KEY",
            "api_base": "https://api.openai.com",
            "api_path": "/v1/chat/completions",
            "type": "openai",
        },
        "openrouter": {
            "model": "google/gemini-3-flash-preview",
            "api_key_env": "OPENROUTER_API_KEY",
            "api_base": "https://openrouter.ai/api/v1",
            "api_path": "/chat/completions",
            "type": "openai",
        },
    }

    @classmethod
    def get_provider_config(cls, provider: str) -> Dict[str, Any]:
        """Get configuration for specified provider"""
        if provider not in cls._CONFIG:
            raise ValueError(f"Unsupported AI provider: {provider}")

        config = cls._CONFIG[provider].copy()
        # Dynamically get API key
        config["api_key"] = safe_read_cfg(config["api_key_env"])
        return config

    @classmethod
    def get_all_providers(cls) -> list:
        """Get list of all supported providers"""
        return list(cls._CONFIG.keys())

    @classmethod
    def get_providers_by_type(cls, provider_type: str) -> list:
        """Get provider list by type"""
        return [provider for provider, config in cls._CONFIG.items() if config.get("type") == provider_type]

    @classmethod
    def validate_provider(cls, provider: str) -> bool:
        """Validate if provider is supported"""
        return provider in cls._CONFIG

    @classmethod
    def add_provider(cls, provider: str, config: Dict[str, Any]) -> None:
        """Dynamically add new provider configuration"""
        required_fields = ["model", "api_key_env", "api_base", "api_path", "type"]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Configuration missing required field: {field}")

        cls._CONFIG[provider] = config

    @classmethod
    def get_config_summary(cls) -> Dict[str, str]:
        """Get configuration summary"""
        return {provider: f"{config['type']} - {config['model']}" for provider, config in cls._CONFIG.items()}

    # ========== Auto-select provider methods ==========
    
    @classmethod
    def get_available_provider(cls) -> Optional[Dict[str, Any]]:
        """
        Get first available provider (based on configured API keys)
        
        Priority: openai > openrouter > gemini > volcengine > dashscope
        
        Returns:
            Provider config dict with name, api_key_env, default_model, description
            Returns None if no provider is available
        """
        for provider in cls._DEFAULT_PROVIDER_PRIORITY:
            api_key = safe_read_cfg(provider["api_key_env"])
            if api_key:
                return provider
        return None
    
    @classmethod
    def get_available_provider_name(cls) -> Optional[str]:
        """
        Get first available provider name
        
        Returns:
            Provider name, or None if no provider is available
        """
        provider = cls.get_available_provider()
        return provider["name"] if provider else None
    
    @classmethod
    def get_provider_by_priority_name(cls, name: str) -> Optional[Dict[str, Any]]:
        """
        Get provider config by name from priority list
        
        Args:
            name: Provider name (openai/openrouter/gemini/volcengine/dashscope)
            
        Returns:
            Provider config dict
        """
        for provider in cls._DEFAULT_PROVIDER_PRIORITY:
            if provider["name"] == name:
                return provider
        return None
    
    @classmethod
    def list_available_providers(cls) -> List[str]:
        """
        List all available providers with configured API keys
        
        Returns:
            List of available provider names
        """
        available = []
        for provider in cls._DEFAULT_PROVIDER_PRIORITY:
            api_key = safe_read_cfg(provider["api_key_env"])
            if api_key:
                available.append(provider["name"])
        return available
    
    @classmethod
    def get_provider_status(cls) -> Dict[str, bool]:
        """
        Get configuration status of all providers
        
        Returns:
            Mapping of provider names to availability status
        """
        return {
            provider["name"]: bool(safe_read_cfg(provider["api_key_env"]))
            for provider in cls._DEFAULT_PROVIDER_PRIORITY
        }
    
    @classmethod
    def get_default_model_for_available_provider(cls) -> Optional[str]:
        """
        Get default model for available provider
        
        Returns:
            Default model name, or None if no provider is available
        """
        provider = cls.get_available_provider()
        return provider["default_model"] if provider else None
    
    @classmethod
    def auto_get_provider_config(cls, preferred_provider: Optional[str] = None) -> Dict[str, Any]:
        """
        Auto-get provider config with optional preferred provider
        
        If preferred_provider is specified and its API key is configured, use it;
        otherwise auto-select first available provider.
        
        Args:
            preferred_provider: Preferred provider name (optional)
            
        Returns:
            Provider config dict
            
        Raises:
            ValueError: If no provider is available
        """
        # Check if preferred provider is available
        if preferred_provider:
            if preferred_provider in cls._CONFIG:
                config = cls._CONFIG[preferred_provider]
                api_key = safe_read_cfg(config["api_key_env"])
                if api_key:
                    return cls.get_provider_config(preferred_provider)
                # Preferred provider unavailable, continue auto-selection
        
        # Auto-select available provider
        available_provider = cls.get_available_provider()
        if not available_provider:
            status = cls.get_provider_status()
            raise ValueError(
                f"No AI provider available. Please configure one of the following API keys:\n"
                f"  - OPENAI_API_KEY (for OpenAI)\n"
                f"  - OPENROUTER_API_KEY (for OpenRouter)\n"
                f"  - ANTHROPIC_API_KEY (for Claude)\n"
                f"  - GOOGLE_API_KEY (for Gemini)\n"
                f"  - VOLCENGINE_API_KEY (for Doubao)\n"
                f"  - DASHSCOPE_API_KEY (for DashScope/Qwen)\n"
                f"Current status: {status}"
            )
        
        return cls.get_provider_config(available_provider["name"])


# Global configuration objects
AI_CONFIG = AIConfig._CONFIG
AI_PROVIDER = AIConfig._PROVIDER
