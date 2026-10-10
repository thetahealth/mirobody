"""The agent's own middleware.

Mirobody-specific middleware on top of the deepagents stack:

- `UniversalPromptCachingMiddleware`: prompt caching across providers (upstream
  deepagents only wires caching for Anthropic/Bedrock/Fireworks).
- `ToolFaultMiddleware`: a crashing tool becomes an error ToolMessage instead of
  killing the turn.
- `InvalidToolCallRepairMiddleware`: a tool call with unparseable JSON arguments
  becomes an error ToolMessage + retry instead of silently ending the turn.
- `EmptyAnswerRepairMiddleware`: a reply with no answer text and no tool call
  is asked for once more instead of ending the turn blank.
- `ModelCallBudgetMiddleware`: the turn's model-call budget, whose last call
  is made without tool calls and asked for the answer.
- `RetryGovernanceMiddleware`: a call that already failed unrecoverably is
  refused before it runs again (`mirobody.kernel.tools.RetryLedger`).
- `NoVisionReadMiddleware`: for a model that cannot see, `read_file` answers an
  image with its OCR text instead of an image block.
- `RefCollectingInterpreter` (eval_refs.py): the QuickJS `eval` tool, whose
  result carries the rids of the readings rows the eval surfaced as
  `{result, refs}` — the citation contract for values computed in the REPL.

The filesystem and tool-call repair pieces come from upstream `deepagents`.
GenotypeSafeSummarizationMiddleware replaces its summarization slot so history
files and the separate summary model do not receive genotype tool rows.
`agent.MirobodyAgent._build_agent` assembles the stack. Note `TodoListMiddleware` is NOT among them:
deepagents 0.7 dropped it from the default stack and the agent does not add it
back, so there is no `write_todos` tool.
"""

from .empty_answer import EmptyAnswerRepairMiddleware
from .model_budget import ModelCallBudgetMiddleware
from .prompt_caching import UniversalPromptCachingMiddleware
from .genotype_row_guard import GenotypeRowGuardMiddleware
from .genotype_summarization import GenotypeSafeSummarizationMiddleware
from .no_vision import NoVisionReadMiddleware
from .retry_governance import RetryGovernanceMiddleware
from .tool_faults import InvalidToolCallRepairMiddleware, ToolFaultMiddleware

__all__ = [
    "EmptyAnswerRepairMiddleware",
    "InvalidToolCallRepairMiddleware",
    "ModelCallBudgetMiddleware",
    "GenotypeRowGuardMiddleware",
    "GenotypeSafeSummarizationMiddleware",
    "NoVisionReadMiddleware",
    "RetryGovernanceMiddleware",
    "ToolFaultMiddleware",
    "UniversalPromptCachingMiddleware",
]
