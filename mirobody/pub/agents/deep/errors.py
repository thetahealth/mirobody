"""Simplified error handling for DeepAgent.

Aligned with baseline_agent.py style - minimal exception classes, clear error messages.
"""


class DeepAgentError(Exception):
    """Base error for DeepAgent."""
    pass


class ConfigError(DeepAgentError):
    """Configuration errors (API keys, database, provider config, etc)."""
    pass
