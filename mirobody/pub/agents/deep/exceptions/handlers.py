"""Error message handlers and diagnostics for DeepAgent."""

import logging

logger = logging.getLogger(__name__)


class ErrorMessageHandler:
    """Generate user-friendly error messages for common issues."""
    
    API_PROVIDERS = {
        "GOOGLE_API_KEY": {"name": "Google AI", "url": "https://aistudio.google.com/app/apikey"},
        "OPENAI_API_KEY": {"name": "OpenAI", "url": "https://platform.openai.com/api-keys"},
        "OPENROUTER_API_KEY": {"name": "OpenRouter", "url": "https://openrouter.ai/keys"},
        "ANTHROPIC_API_KEY": {"name": "Anthropic", "url": "https://console.anthropic.com/settings/keys"}
    }
    
    @staticmethod
    def api_key_missing(api_key_var: str, provider_name: str = "") -> str:
        """Generate error message for missing API key."""
        provider_info = ErrorMessageHandler.API_PROVIDERS.get(api_key_var, {})
        provider_display = provider_info.get("name", api_key_var)
        url = provider_info.get("url", "")
        
        msg = f"Failed to build agent: {api_key_var} is required for {provider_display} functionality."
        if url:
            msg += f"\nGet API key: {url}"
        msg += f"\nYou can set {api_key_var} in .env file or environment variable"
        return msg
    
    @staticmethod
    def provider_not_found(provider_name: str, agent_name: str, available_providers: list[str]) -> str:
        """Generate error message when provider not found."""
        available = ", ".join(available_providers) if available_providers else "None"
        return f"Provider '{provider_name}' not configured for {agent_name}.\nAvailable providers: {available}\nHint: Add to PROVIDERS_{agent_name.upper()} in config.yaml"
    
    @staticmethod
    def provider_init_failed(provider_name: str, error: Exception, api_key_var: str = None) -> str:
        """Generate error message for provider initialization failure."""
        return f"Provider '{provider_name}' initialization failed: {error}\nHint: Check config.yaml PROVIDERS_DEEP section"
    
    @staticmethod
    def database_connection_failed(error: Exception) -> str:
        """Generate error message for database connection failure."""
        return f"Database connection failed: {error}\nHint: Check PG_* settings in config.yaml"
    
    @staticmethod
    def redis_connection_failed(error: Exception) -> str:
        """Generate error message for Redis connection failure."""
        return f"Redis connection failed: {error}\nHint: Check REDIS_* settings in config.yaml"
    
    @staticmethod
    def backend_creation_failed(error: Exception) -> str:
        """Generate error message for backend creation failure."""
        error_str = str(error).lower()
        if any(kw in error_str for kw in ['connection', 'database', 'postgres', 'psycopg']):
            return ErrorMessageHandler.database_connection_failed(error)
        return f"Backend creation failed: {error}\nHint: Check database settings in config.yaml"
    
    @staticmethod
    def tool_load_failed(tool_type: str, error: Exception) -> str:
        """Generate error message for tool loading failure."""
        return f"Failed to load {tool_type} tools: {error}\nNote: Tools are optional - agent will continue without them"
    
    @staticmethod
    def prompt_load_failed(prompt_name: str, error: Exception) -> str:
        """Generate error message for prompt loading failure."""
        return f"Failed to load prompt template '{prompt_name}': {error}\nHint: Check database connection or use built-in prompts"
    
    @staticmethod
    def system_prompt_build_failed(error: Exception) -> str:
        """Generate error message for system prompt building failure."""
        return f"Failed to build system prompt: {error}\nHint: Please check config.yaml for corresponding config values"
    
    @staticmethod
    def agent_build_failed(error: Exception) -> str:
        """Generate error message for agent building failure."""
        return f"Failed to build agent: {error}\nHint: Please check config.yaml for corresponding config values"
