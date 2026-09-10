# The agent

One agent ships here: **`MirobodyAgent`** ([`agent.py`](./agent.py)), built on
LangChain `create_agent` and the [deepagents](https://github.com/langchain-ai/deepagents)
middleware stack. It is the reference implementation of mirobody's answer
layer — the demonstration that the data model (the catalogue, the day, the
envelope) is what an AI agent needs — and it is the only one, on purpose.

What a turn has:

| Piece | Where | What it does |
| --- | --- | --- |
| tools | [`tools/`](./tools/) via [`tool_loader.py`](./tool_loader.py) | the five MCP tools, with `query.TOOL_SCHEMA` passed through verbatim; the same tools any MCP client sees over `/mcp` |
| virtual filesystem | [`filesystem/`](./filesystem/) — `backend.py`, `files_backend.py`, `profile_backend.py` | `/uploads`, `/library`, `/memories`, `/skills` — read-only projections of the tables that own the data |
| REPL | `langchain-quickjs` | the `eval` tool, with the read-only data tool reachable inside it |
| memory | [`checkpointer.py`](./checkpointer.py) | LangGraph Postgres checkpointer, `thread_id = session_id` |
| governance | [`middleware/`](./middleware/) | fault containment, retry refusal keyed on the envelope, prompt caching; plus the model-call and tool-call budgets |
| one question | [`hitl.py`](./hitl.py) | `ask_user`, the human-in-the-loop interrupt (never an MCP tool) |
| prompt | [`prompts/mirobody.jinja`](./prompts/mirobody.jinja), [`prompt.py`](./prompt.py) | names only tools the harness provides — `tests/test_prompts.py` fails otherwise |
| skills | [`skills/`](./skills/) | Agent Skills, mounted read-only at `/skills/` |
| wire | [`wire/`](./wire/), [`chat/`](./chat/) | LangGraph events → the chunk dicts below; sessions, messages, SSE |

Adding capability means adding a **tool** or a **skill**, not another agent.

## Configuration

One agent, so the keys carry no agent-name suffix (`config.yaml`):

```yaml
MODELS:            # the model picker: one LangChain chat model per entry
  claude-sonnet:
    llm_type: openai
    api_key: OPENROUTER_API_KEY     # the config/env key that holds the secret
    base_url: https://openrouter.ai/api/v1
    model: anthropic/claude-sonnet-5
PROMPTS:
  - agent/prompts/mirobody.jinja   # path, or path@name; the first is the default
ALLOWED_TOOLS:        # whitelist, or
DISALLOWED_TOOLS:     # blacklist (`eval` here turns the REPL off)
DEFAULT_MODEL:     # else: the entry whose key is present
AGENT_NAME:           # the persona name in the prompt; default "Mirobody"
```

`/api/models` lists the `MODELS` entries whose key resolves, as bare names.
A chat request picks one with `provider`.

## Replacing the agent

There is no switching between agents and no second slot. Replacement comes
from two places ([`registry.py`](./registry.py)): an installed package that
declares a `mirobody.agents` entry point is looked at first, then the
`AGENT_DIRS` directories in order; the first class that defines
`generate_response` becomes the agent for the process. A deployment that wants
its own harness `pip install`s it, or lists its own directory in an overlay (an
overlay replaces the list) — and never touches this package.
`examples/mirobody_example_plugin/mirobody_example_plugin/agent.py` is the smallest possible one.

The contract is two methods:

```python
class MyAgent:
    def __init__(self, **kwargs): ...          # receives MODELS / PROMPTS / tool lists + the turn's kwargs

    @classmethod
    def load_llm_clients(cls, providers: dict) -> dict:   # optional; {name: client}
        return {}

    async def generate_response(self, user_id: str, messages: list[dict], **kwargs):
        yield {"type": "reply", "content": "..."}
```

Every other agent runtime — Claude Desktop, Cursor, a Responses-API loop of
your own — is meant to reach the same data through `/mcp`, and needs nothing
from this directory.

## Response chunks

An agent streams dicts with a `type` and `content`
([`wire/stream.py`](./wire/stream.py) builds them for the shipped agent):

| Type | Content | Meaning |
| --- | --- | --- |
| `reply` | `str` | the answer, markdown, streamed |
| `thinking` | `str` | reasoning or progress note; foldable in the client |
| `queryTitle` | `str` (tool name) + `tool_id` | a tool call starts |
| `queryArguments` | `str` (JSON) + `tool_id` | its arguments |
| `queryDetail` | `str` + `tool_id`, `status`, `error_kind?`, `truncated?` | its result; the extra keys come from the tool's envelope, not from the text |
| `widget` | `{question, widget_type, config}` | an `ask_user` question; the next user message answers it |
| `costStatistics` | `{model, input_tokens, output_tokens, total_tokens, ...}` | once, at the end; tokens only |
| `error` | `str` | user-visible; the agent stops |
| `end` | `""` + `finish_reason` (`stop` / `error` / `unavailable`) | emitted by the chat adapter, not the agent |
