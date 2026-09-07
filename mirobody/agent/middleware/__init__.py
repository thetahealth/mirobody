"""The agent's own middleware.

Mirobody-specific middleware on top of the deepagents stack:

- `UniversalPromptCachingMiddleware` — prompt caching across providers (upstream
  deepagents only wires caching for Anthropic/Bedrock/Fireworks).
- `ToolFaultMiddleware` — a crashing tool becomes an error ToolMessage instead of
  killing the turn.
- `InvalidToolCallRepairMiddleware` — a tool call with unparseable JSON arguments
  becomes an error ToolMessage + retry instead of silently ending the turn.
- `RetryGovernanceMiddleware` — a call that already failed unrecoverably is
  refused before it runs again (`mirobody.kernel.tools.RetryLedger`).

The rest of the stack (FilesystemMiddleware, SummarizationMiddleware,
PatchToolCallsMiddleware, SubAgentMiddleware) is pulled directly from the
upstream `deepagents` and `langchain` packages and assembled in
`agent.MirobodyAgent._build_agent`. Note `TodoListMiddleware` is NOT among them:
deepagents 0.7 dropped it from the default stack and the agent does not add it
back, so there is no `write_todos` tool.
"""

from .prompt_caching import UniversalPromptCachingMiddleware
from .retry_governance import RetryGovernanceMiddleware
from .tool_faults import InvalidToolCallRepairMiddleware, ToolFaultMiddleware

__all__ = [
    "InvalidToolCallRepairMiddleware",
    "RetryGovernanceMiddleware",
    "ToolFaultMiddleware",
    "UniversalPromptCachingMiddleware",
]
