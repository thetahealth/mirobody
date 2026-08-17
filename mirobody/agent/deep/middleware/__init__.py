"""DeepAgent middleware sub-package.

Mirobody-specific middleware shipped with DeepAgent:

- `UniversalPromptCachingMiddleware` — prompt caching across providers (upstream
  deepagents only wires caching for Anthropic/Bedrock/Fireworks).
- `ToolFaultMiddleware` — a crashing tool becomes an error ToolMessage instead of
  killing the turn.

The rest of the stack (FilesystemMiddleware, SummarizationMiddleware,
PatchToolCallsMiddleware, SubAgentMiddleware) is pulled directly from the
upstream `deepagents` and `langchain` packages and assembled in
`deep_agent.DeepAgent._build_agent`. Note `TodoListMiddleware` is NOT among them:
deepagents 0.7 dropped it from the default stack and DeepAgent does not add it
back, so there is no `write_todos` tool.
"""

from .prompt_caching import UniversalPromptCachingMiddleware
from .tool_faults import ToolFaultMiddleware

__all__ = ["UniversalPromptCachingMiddleware", "ToolFaultMiddleware"]
