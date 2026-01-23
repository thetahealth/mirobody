"""Exception handling for DeepAgent.

This module provides:
- Custom exception classes with user-friendly messages
- Error message handlers for common configuration issues
- Unified error handling that logs for engineers and outputs to frontend for users
"""

from .base import (
    DeepAgentException,
    ConfigurationError,
    APIKeyError,
    ProviderConfigError,
    DatabaseConnectionError,
    LLMInitializationError,
    BackendCreationError,
    ToolLoadError,
    PromptLoadError,
    SystemPromptBuildError,
    AgentBuildError,
)
from .handlers import ErrorMessageHandler

__all__ = [
    "DeepAgentException",
    "ConfigurationError",
    "APIKeyError",
    "ProviderConfigError",
    "DatabaseConnectionError",
    "LLMInitializationError",
    "BackendCreationError",
    "ToolLoadError",
    "PromptLoadError",
    "SystemPromptBuildError",
    "AgentBuildError",
    "ErrorMessageHandler",
]
