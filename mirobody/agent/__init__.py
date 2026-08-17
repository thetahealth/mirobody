"""Mirobody built-in agents.

| Agent      | Runtime mechanism                                          | Prompts                                |
|------------|------------------------------------------------------------|----------------------------------------|
| `BaseAgent`| Provider's own agent loop drives tools (server-side MCP)   | `agent/prompts/base.jinja`             |
| `DeepAgent`| LangChain `create_agent` + `deepagents` middleware stack   | `agent/prompts/deep.jinja`             |

**Why there are two, and why that is not a duplicated capability.** They are
not two answers to one question — they are the project's two CONSUMPTION
MODELS, and each is the reference implementation of one of them:

* **DeepAgent — you run the whole engine.** Clone or `pip install
  'mirobody[agents]'`, and the agent loop runs *here*: our LangChain/deepagents
  stack, our Postgres-backed virtual filesystem (`/uploads`, `/library`,
  `/memories`, `/skills`), our in-process QuickJS interpreter, our
  per-turn model-call budget, our Agent Skills. Maximum capability, and every
  moving part is yours to inspect and self-host. This is what mirobody.ai runs.

* **BaseAgent — somebody else's model consumes us over MCP.** It hands the MCP
  server URL to the PROVIDER (OpenAI Responses `mcp_server`, Gemini
  Interactions `mcp_server` delta) and the provider drives the tool loop
  against our server; we only stream its events. There is
  no LangChain here and no middleware — deliberately. That makes it the
  closest thing we have to a live rehearsal of what Claude Desktop, Cursor or a
  ChatGPT App experiences when it points at `/mcp`: if a tool's description is
  too thin for a model to use unaided, this is the path where it shows.

So the rule of thumb: **change DeepAgent to make the self-hosted product
better; change BaseAgent (or really, the tool descriptions it exercises) to
make the MCP surface better for third parties.** A capability that only works
in DeepAgent is a capability external MCP clients do not get.

All prompts live under `mirobody/agent/prompts/` as one `<agent>.jinja` each
and are wired into agents via `PROMPTS_<AGENT>` config keys in `config.yaml`
(see `mirobody.utils.config.config` for the loader).

For tool-sourcing details (native middleware tools vs MCP tools, blocklist
behaviour) see each agent's docstring and `deep/tool_loader._NATIVE_TOOL_BLOCKLIST`.

Exports resolve lazily (PEP 562), and that is load-bearing, not style: this
package also contains ``agent/chat/``, and the ENGINE's two documented seams
import ``mirobody.agent.chat.user_profile`` — which executes THIS ``__init__``
first. An eager ``from .deep_agent import DeepAgent`` here would therefore make
``import mirobody.pulse.file_parser`` require langchain on a bare engine
install. import-linter cannot see that chain (a submodule import does not
create a graph edge to the parent package), so the release workflow's bare-
wheel smoke test is the gate that keeps this honest.
"""

from typing import TYPE_CHECKING

_EXPORTS = {
    "BaseAgent": "base_agent",
    "DeepAgent": "deep_agent",
}

__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .base_agent import BaseAgent
    from .deep_agent import DeepAgent


def __getattr__(name: str):
    if name in _EXPORTS:
        import importlib

        module = importlib.import_module(f".{_EXPORTS[name]}", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
