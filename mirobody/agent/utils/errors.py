"""Simplified error handling for DeepAgent.

Aligned with baseline_agent.py style - minimal exception classes, clear error messages.
"""


class DeepAgentError(Exception):
    """Base error for DeepAgent."""
    pass


class ConfigError(DeepAgentError):
    """Configuration errors (API keys, database, provider config, etc)."""
    pass


def client_safe_error(e: BaseException) -> str:
    """Exception → message safe to stream to the client and into chat history.

    Exception TYPE only — the same threat model `ToolFaultMiddleware` already
    enforces for tool faults. Streaming `str(e)` sent provider error bodies,
    URLs and internal identifiers to the browser AND persisted them in chat
    history via `save_assistant_response`, where they outlive the incident.
    The full text belongs in the server log (callers log it with exc_info
    before calling this), not in the answer.
    """
    return f"⚠️ The service hit an internal error ({type(e).__name__}). Please try again — the details have been logged."
