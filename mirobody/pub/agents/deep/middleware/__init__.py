"""DeepAgent middleware sub-package.

`UniversalPromptCachingMiddleware` is the only Mirobody-specific middleware
shipped with DeepAgent. The rest of the stack (TodoListMiddleware,
FilesystemMiddleware, SummarizationMiddleware, PatchToolCallsMiddleware) is
pulled directly from the upstream `deepagents` and `langchain` packages
and assembled in `deep_agent.DeepAgent._create_middlewares`.
"""

from .prompt_caching import UniversalPromptCachingMiddleware

__all__ = ["UniversalPromptCachingMiddleware"]
