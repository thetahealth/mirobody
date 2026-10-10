# The agent

One agent ships here: **`MirobodyAgent`** ([`agent.py`](./agent.py)), built on
[deepagents](https://github.com/langchain-ai/deepagents) (`create_deep_agent`)
and its middleware stack. It is the reference implementation of mirobody's answer
layer — the demonstration that the data model (the catalogue, the day, the
envelope) is what an AI agent needs — and it is the only one, on purpose.

What a turn has:

| Piece | Where | What it does |
| --- | --- | --- |
| tools | [`tools/`](./tools/) via [`tool_loader.py`](./tool_loader.py) | the four record tools, each schema passed through verbatim, the same ones an MCP client sees over `/mcp`; the three terminology tools stay MCP-only (`tool_loader._MCP_ONLY_TOOLS`) |
| virtual filesystem | [`filesystem/`](./filesystem/) — `backend.py`, `files_backend.py`, `profile_backend.py` | `/uploads`, `/library`, `/memories` — read-only projections of the tables that own the data |
| REPL | `langchain-quickjs` + [`middleware/eval_refs.py`](./middleware/eval_refs.py) | the `eval` tool, with the read-only data tool reachable inside it; an eval that surfaced readings rows answers with `{result, refs:[rids]}` so values computed there can be cited (a returned object's own `refs` wins) |
| memory | [`checkpointer.py`](./checkpointer.py) | LangGraph Postgres checkpointer, one thread per asker and session (`thread_for`) |
| governance | [`middleware/`](./middleware/) | fault containment, retry refusal keyed on the envelope, prompt caching; plus the model-call and tool-call budgets |
| one question | [`hitl.py`](./hitl.py) | `ask_user`, the human-in-the-loop interrupt (never an MCP tool) |
| reference search | [`medref.py`](./medref.py) | `search_medical_reference`, offline FTS5 over bundled MedlinePlus summaries + FDA labels ([`../res/medref/`](../res/medref/)); general knowledge with citeable `ref:` ids, agent-only like `ask_user` |
| prompt | [`prompts/mirobody.jinja`](./prompts/mirobody.jinja) (+ [`mirobody_compact.jinja`](./prompts/mirobody_compact.jinja), ≤ half the bytes for small local models), [`prompt.py`](./prompt.py) | names only tools the harness provides — the local suite fails otherwise; also states the deployment facts (answering model, where requests go) |
| wire | [`wire/`](./wire/), [`chat/`](./chat/) | LangGraph events → the blocks below; sessions, messages, SSE |

Adding capability means adding a **tool**, not another agent.

## Configuration

One agent, so the keys carry no agent-name suffix (`config.yaml`):

```yaml
MODELS:            # the model picker: one LangChain chat model per entry
  claude-sonnet:
    llm_type: openai
    api_key: OPENROUTER_API_KEY     # the config/env key that holds the secret
    base_url: https://openrouter.ai/api/v1
    model: anthropic/claude-sonnet-5.5
PROMPTS:
  - agent/prompts/mirobody.jinja   # path, or path@name; the first is the default
  - agent/prompts/mirobody_compact.jinja   # the compact variant; prompt_name="mirobody_compact" picks it
ALLOWED_TOOLS:        # whitelist, or
DISALLOWED_TOOLS:     # blacklist (`eval` here turns the REPL off)
DEFAULT_MODEL:     # used when ready; else the first entry whose key is present
AGENT_NAME:           # the persona name in the prompt; default "Mirobody"
```

Two entry-level keys tune a small local model's context behaviour rather
than a global default: `profile.max_input_tokens` (deepagents compacts at
85% of it; undeclared, the trigger is a fixed 170,000 tokens, unreachable
inside a 32k window) and `tool_result_offload_tokens` (a tool result larger
than that, estimated in the model's own tokens, is paged from the virtual
filesystem instead of sitting inline; unset, a quarter of
`max_input_tokens`, and entries declaring neither keep deepagents' 20,000
4-chars-per-token default — a ratio that misjudges Chinese text ~2.5×).
The shipped `local` entry carries the first; both derive the second.

`/api/models` lists the `MODELS` entries whose key resolves, as bare names,
the default first (`utils.config.llm.default_model`, which `mirobody doctor --probe`
tests too). A chat request picks one with `provider`.

## Replacing the agent

There is no switching between agents and no second slot. Replacement comes
from two places ([`registry.py`](./registry.py)): an installed package that
declares a `mirobody.agents` entry point is looked at first, then the
`AGENT_DIRS` directories in order; the first class that defines
`generate_response` becomes the agent for the process. A deployment that wants
its own harness `pip install`s it, or lists its own directory in an overlay (an
overlay replaces the list) — and never touches this package.
`examples/mirobody_example_plugin/mirobody_example_plugin/agent.py` is the smallest possible one.

The contract is two methods, written out in full on
[`registry.AbstractAgent`](./registry.py):

```python
class MyAgent:
    def __init__(self, **kwargs): ...          # MODELS / PROMPTS / tool lists + the turn's values

    @classmethod
    def load_llm_clients(cls, models: dict) -> dict:      # optional; {name: client}
        return {}

    async def generate_response(self, user_id: str, messages: list[dict], **kwargs):
        yield {"type": "text", "text": "..."}                 # the blocks below
```

`**kwargs` is not optional: `language`, `session_id`, `file_list`, `provider`,
`prompt_name`, `timezone`, `record_owner` and `may_write` arrive that way, and
the chat layer may add one without breaking a plugin that predates it.
`user_id` is the person the turn is ABOUT — the care-circle target when someone
asks on another's behalf — and is already authorised; `record_owner` is that
person's name then, and `""` on the asker's own record; `may_write` says
whether the asker may change that record. `messages` carries ONLY this turn:
conversation state is the agent's own (the shipped one keys a LangGraph
checkpointer on `session_id`).

Every other agent runtime — Claude Code, Codex, Cursor, Claude Desktop, a Responses-API loop of
your own — is meant to reach the same data through `/mcp`, and needs nothing
from this directory.

## Seams

What an application building on mirobody binds, and what it does not:

| To | Bind | Not |
| --- | --- | --- |
| replace the agent | `mirobody.agents` entry point / `AGENT_DIRS`, against `registry.AbstractAgent` + [`wire/blocks.py`](./wire/blocks.py) (stdlib only, so a plugin needs no LangChain) | this module |
| add a tool | `mirobody.tools` entry point / `MCP_TOOL_DIRS` | — |
| render your own wire | [`wire/events_bridge.py`](./wire/events_bridge.py) → `mirobody.kernel.events`, the wire-neutral event vocabulary | `wire/stream.py`, which is this repository's own renderer |
| carry the turn over something other than SSE | `chat.turn.run`, which yields blocks; `chat.turn.stream` is the SSE framing over it and is five lines | — |
| build a deepagents agent | [`harness.py`](./harness.py), [`middleware/`](./middleware/), [`models/`](./models/), `filesystem/{naming,document_backend}.py` | — |

## Response blocks

An agent streams dicts named the way LangChain names them
([`wire/blocks.py`](./wire/blocks.py) is the vocabulary,
[`wire/stream.py`](./wire/stream.py) renders it for the shipped agent):

| Type | Fields | Meaning |
| --- | --- | --- |
| `text` | `text` | the answer, markdown, streamed |
| `reasoning` | `reasoning` | thinking or a progress note; foldable in the client |
| `tool_call` | `id`, `name`, `args` | one tool call, name and arguments together |
| `tool_result` | `tool_call_id`, `content`, `status`, `error_kind?`, `truncated?` | its result, content verbatim; the status keys come from the tool's envelope, not from the text |
| `interrupt` | `interrupt_id`, `name`, `args` | a paused run — `ask_user`; the next user message answers it |
| `usage` | `model`, `input_tokens`, `output_tokens`, `total_tokens`, `*_token_details?` | once, at the end; LangChain's `usage_metadata` shape, tokens only |
| `notice` | `message` | the SYSTEM talking to the user ("that model is not configured, using the default"), never the model's own words |
| `error` | `message` | user-visible; the agent stops |

Three more are about the stream rather than the model, and
[`chat/turn.py`](./chat/turn.py) emits them: `start` (`id`, the id the answer is
saved under), `heartbeat`, and `end` (`finish_reason`: `stop` / `error` /
`unavailable` / `empty`). The stored transcript keeps `end`'s reason, so a
reopened conversation can still say the turn was cut off.

Before 1.4.4 these were `reply` / `thinking` / `queryTitle` / `queryArguments` /
`queryDetail` / `costStatistics` / `widget` over one `content` field, invented
here. Transcripts written then are renamed when they are loaded
(`blocks.upgrade`), so old conversations still render.
