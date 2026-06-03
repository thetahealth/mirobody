"""Mirobody built-in agents.

| Agent      | Runtime mechanism                                          | Prompts                                |
|------------|------------------------------------------------------------|----------------------------------------|
| `BaseAgent`| Provider's own agent loop drives tools (server-side MCP)   | `pub/prompts/base/theta_health.jinja`  |
| `DeepAgent`| LangChain `create_agent` + `deepagents` middleware stack   | `pub/prompts/deep/*.jinja`             |
| `MixAgent` | Two-phase: orchestrator (`tool_choice='any'`) → responder  | `pub/prompts/mix/{orchestrator,responder}.jinja` |

All prompts live under `mirobody/pub/prompts/<agent>/` as Jinja templates
and are wired into agents via `PROMPTS_<AGENT>` config keys in `config.yaml`
(see `mirobody.utils.config.config` for the loader).

For tool-sourcing details (native middleware tools vs MCP tools, blocklist
behaviour) see each agent's docstring and `deep/tool_loader._NATIVE_TOOL_BLOCKLIST`.
"""

from .base_agent import BaseAgent
from .deep_agent import DeepAgent
from .mix_agent import MixAgent

__all__ = [
    "BaseAgent",
    "DeepAgent",
    "MixAgent",
]
