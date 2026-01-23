"""Base exception classes for DeepAgent."""


class DeepAgentException(Exception):
    """Base exception for DeepAgent errors."""
    
    def __init__(self, message: str, user_message: str = None):
        """
        Initialize exception with technical and user-friendly messages.
        
        Args:
            message: Technical error message for logs
            user_message: User-friendly message for frontend display
        """
        super().__init__(message)
        self.user_message = user_message or message


class ConfigurationError(DeepAgentException):
    """Configuration error."""
    pass


class APIKeyError(ConfigurationError):
    """API key missing or invalid."""
    pass


class ProviderConfigError(ConfigurationError):
    """Provider configuration error."""
    pass


class DatabaseConnectionError(DeepAgentException):
    """Database connection error."""
    pass


class LLMInitializationError(DeepAgentException):
    """LLM client initialization error."""
    pass


class BackendCreationError(DeepAgentException):
    """Backend creation error."""
    pass


class ToolLoadError(DeepAgentException):
    """Tool loading error."""
    pass


class PromptLoadError(DeepAgentException):
    """Prompt loading error."""
    pass


class SystemPromptBuildError(DeepAgentException):
    """System prompt building error."""
    pass


class AgentBuildError(DeepAgentException):
    """Agent building error."""
    pass
