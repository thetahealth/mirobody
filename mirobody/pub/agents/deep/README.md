# DeepAgent Exception Handling

## Overview

Unified exception handling for DeepAgent that:

- Logs technical details for engineers
- Outputs user-friendly messages to frontend
- Provides actionable solutions for common issues

## Structure

```
exceptions/
├── __init__.py          # Export all exception classes and handlers
├── base.py              # Exception class definitions
└── handlers.py          # Error message generation
```

## Usage

### Exception Handling

```python
from .deep.exceptions import (
    BackendCreationError,
    ErrorMessageHandler
)

try:
    backend = create_postgres_backend(...)
except Exception as e:
    # Generate user-friendly message
    error_msg = ErrorMessageHandler.backend_creation_failed(e)
  
    # Log for engineers
    logger.error(f"Backend creation failed: {str(e)}", exc_info=True)
  
    # Raise with both technical and user-friendly messages
    raise BackendCreationError(
        f"Backend creation failed: {str(e)}",  # For logs
        user_message=error_msg                   # For frontend
    )
```

### In generate_response

```python
async def generate_response(...):
    try:
        # Agent logic
        ...
    except DeepAgentException as e:
        # Log technical error
        logger.error(f"DeepAgent error: {str(e)}")
      
        # Output user-friendly message to frontend
        yield {"type": "error", "content": e.user_message}
```

## Error Message Handlers

### Available Handlers

```python
ErrorMessageHandler.api_key_missing(api_key_var, provider_name)
ErrorMessageHandler.provider_not_found(provider_name, agent_name, available)
ErrorMessageHandler.provider_init_failed(provider_name, error, api_key_var)
ErrorMessageHandler.database_connection_failed(error)
ErrorMessageHandler.redis_connection_failed(error)
ErrorMessageHandler.backend_creation_failed(error)
```

## Exception Classes

- `DeepAgentException` - Base exception (has `user_message` property)
- `ConfigurationError` - Configuration issues
  - `APIKeyError` - API key problems
  - `ProviderConfigError` - Provider config issues
- `DatabaseConnectionError` - Database connection failures
- `LLMInitializationError` - LLM init failures
- `BackendCreationError` - Backend creation failures

## Design Principles

1. **Dual Messages** - Technical for logs, user-friendly for frontend
2. **Concise** - Short, clear error messages
3. **Actionable** - Specific steps to fix issues
4. **English-only** - All messages in English
5. **Centralized** - All error handling in one place
