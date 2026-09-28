"""The agent's own exceptions, and the one safe way to show an exception to a client."""


class AgentError(Exception):
    """Base error for the agent loop: its message is ours and safe to show."""


class ConfigError(AgentError):
    """Configuration errors (API keys, database, provider config, etc)."""


def client_safe_error(e: BaseException) -> str:
    """Exception → message safe to stream to the client and into chat history.

    Exception TYPE only: the same threat model `ToolFaultMiddleware` already
    enforces for tool faults. Streaming `str(e)` sent provider error bodies,
    URLs and internal identifiers to the browser AND persisted them in chat
    history via `chat.turn._save_answer`, where they outlive the incident.
    The full text belongs in the server log (callers log it with exc_info
    before calling this), not in the answer.
    """
    from mirobody.utils.req_ctx import get_req_ctx

    # The request id finds this failure's log lines; without it a report of
    # "it said internal error" matched every error that day.
    trace_id = get_req_ctx("trace_id")
    reference = f" (reference: {trace_id})" if trace_id else ""
    return (f"⚠️ The service hit an internal error ({type(e).__name__}). "
            f"Please try again — the details have been logged{reference}.")
