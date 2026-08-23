# 🤖 Developing Agents

Agents are the "brains" of Mirobody. They process user messages, execute logic (like calling LLMs or tools), and stream responses back to the chat interface.

## 🧱 Built-in Agents

Two built-in agents ship under this directory, each backed by a different runtime mechanism. Pick by what your use case needs:

| Agent                   | File                              | Mechanism                                                                                                                     | When to use                                                                                                                                                         |
| ----------------------- | --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`BaseAgent`** | [`base_agent.py`](./base_agent.py) | Provider's own agent loop drives tools (server-side MCP, or local function-call fallback)                                     | Simplest case: one LLM, provider has native MCP / tool support, no planning needed                                                                                  |
| **`DeepAgent`** | [`deep_agent.py`](./deep_agent.py) | LangChain `create_agent` + `deepagents` middleware stack (Filesystem, Summarization, PatchToolCalls, ToolFault, ModelCallLimit, PromptCaching). No subagents (`task` is disabled) and no `write_todos`. | Complex tasks needing workspace files (`ls`/`read_file`/`write_file`/`edit_file`/`glob`/`grep`), long-context summarization |

### Provider client classes (used by `BaseAgent`)

Each LLM provider has a thin client wrapper in [`base/clients.py`](./base/clients.py).

**Admission rule:** BaseAgent clients speak Responses-style APIs only — the
point is handing our MCP server to the *provider's* agent loop. A provider
that only offers Chat Completions is consumed through `DeepAgent`'s LangChain
stack instead; a local function-call loop here would duplicate it (that is why
the former `OpenAIChatClient` / OpenRouter / Nebula / Doubao wrappers were
removed).

| Provider                                  | Class                     | Default model        | MCP  | Stateful |
| ----------------------------------------- | ------------------------- | -------------------- | ---- | -------- |
| OpenAI Responses API                      | `OpenAIResponsesClient` | `gpt-5-nano`       | ✅   | ✅       |
| Gemini Interactions API                   | `GeminiClient`          | `gemini-2.5-flash` | ⏸ *  | ✅       |
| DeepSeek Responses API                    | `DeepSeekResponsesClient` | `deepseek-v4-flash` | ❌ ** | ❌      |
| Aliyun 百炼 DashScope Responses API       | `DashScopeClient`       | `qwen3.5-flash`    | ⏸ ***| ✅       |

\* native MCP calling was unreliable on `gemini-3-flash-preview`; local function fallback for now.
\** DeepSeek's `/responses` supports `function` + server-side `web_search` only — no MCP tool type (checked 2026-08).
\*** DashScope MCP accepts `server_protocol: "sse"` only; our MCP endpoint is streamable-HTTP (checked 2026-08).

## 📂 Discovery (custom agents)

Agents are discovered by scanning the directories listed under `AGENT_DIRS`
in `config.yaml`, which ships as:

```yaml
AGENT_DIRS:
  - mirobody/agent
```

Dropping a `.py` file in `mirobody/agent/` is therefore enough for it to be
picked up. To keep your own agents outside the package, add your directory to
`AGENT_DIRS` in an overlay — a directory that is not listed there is never
scanned, no matter what it is named.

### Discovery Rules

1. **File Location**: Must be a `.py` file inside a directory listed in `AGENT_DIRS`.
2. **Naming Convention**: Class name must end with `Agent` (e.g., `SupportAgent`).
3. **Inheritance**: Technically optional, but recommended to follow the standard signature.

## 🏗️ Implementation Guide

An agent is a simple Python class with an async generator method `generate_response`.

### The `generate_response` Method

This is the core method called by the system.

```python
    async def generate_response(
        self,
        messages: list[dict],
        user_id: str,
        **kwargs
    ):
        """
        Args:
            messages: List of message objects [{"role": "user", "content": "..."}]
            user_id: The ID of the user making the request.
            **kwargs: Additional context (language, timezone, etc.)
      
        Yields:
             dict: A chunk of the response.
        """
```

### Response Chunks

You stream data back to the UI by yielding dictionaries with a `type` and `content`. Chunk types emitted by the built-in agents (see [`utils/stream.py`](utils/stream.py) for the helpers that build them):

| Type               | Content shape                                                                          | Description                                                                                                                                                  |
| ------------------ | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `reply`          | `str`                                                                                | Main text of the response, markdown-supported. Streamed token by token.                                                                                      |
| `thinking`       | `str`                                                                                | Model's reasoning trace or progress note; shown as a foldable log.                                                                                           |
| `queryTitle`     | `str` (tool name); plus `tool_id`                                                  | A tool call is about to start. Pair with later `queryDetail` by `tool_id`.                                                                               |
| `queryArguments` | `str` (JSON-serialized args); plus `tool_id`                                       | Arguments the agent passed to the tool.                                                                                                                      |
| `queryDetail`    | `str` (tool result, often JSON); plus `tool_id`                                    | Result returned by the tool.                                                                                                                                 |
| `costStatistics` | `dict` (`model`, `input_tokens`, `output_tokens`, `total_tokens`, `cache_read_tokens?`, `cache_creation_tokens?`) | Token-usage summary, yielded once at the end of the stream. Tokens only — no dollar amounts: prices change faster than any hardcoded table.                  |
| `error`          | `str`                                                                                | User-visible error message; the agent stops after yielding this.                                                                                             |
| `end`            | `""`                                                                                 | Stream-complete sentinel.**Emitted by the chat adapter** (`HTTPChatAdapter`), not by the agent itself — agents simply finish their async generator. |

> Notes:
>
> - `queryTitle` + `queryArguments` + `queryDetail` are issued in the same conceptual triplet keyed by `tool_id`. UIs that don't want to show tool calls can filter on `type` alone.

### LLM Client Management

If your agent uses an LLM (Large Language Model), you must implement the `load_llm_clients` static method. This allows the system to initialize the LLM client based on your configuration.

```python
    @staticmethod
    def load_llm_clients(llm_client_config: dict[str, Any]) -> dict[str, Any]:
        """
        Args:
            llm_client_config: The dictionary value from 'PROVIDERS_{AGENT_NAME}' in config.yaml.
      
        Returns:
            dict: A dictionary of initialized LLM clients. 
                  Key is the provider name (e.g., 'openai'), Value is the client instance.
        """
```

**Configuration (`config.yaml`):**

Mirobody uses a prefixed configuration naming convention. For an agent named `MyAgent`:

| Config Key              | Description                                                |
| ----------------------- | ---------------------------------------------------------- |
| `PROVIDERS_MY`        | LLM provider definitions (passed to `load_llm_clients`). |
| `ALLOWED_TOOLS_MY`    | List of allowed tools (whitelist).                         |
| `DISALLOWED_TOOLS_MY` | List of disallowed tools (blacklist).                      |
| `PROMPTS_MY`          | Path to prompt templates.                                  |

**Example `config.yaml`:**

```yaml
PROVIDERS_MYAGENT:
  openai-gpt4:
    llm_type: openai
    api_key: OPENAI_API_KEY
    model: gpt-4
    temperature: 0.7

ALLOWED_TOOLS_MY:
- my_tool
```

## 💡 Example: Echo Agent

Save this as `agents/echo.py`.

```python
import asyncio

class EchoAgent:
    """
    A simple agent that echoes back what you say.
    """

    def __init__(self, **kwargs):
        pass

    @staticmethod
    def load_llm_clients(llm_client_config: dict) -> dict:
        # This agent doesn't use an LLM, so return empty
        return {}


    async def generate_response(self, messages: list[dict], **kwargs):
        # 1. Get the last user message
        last_message = messages[-1]["content"]

        # 2. Simulate some "thinking"
        yield {
            "type": "thinking",
            "content": f"Analyzing message length: {len(last_message)} chars..."
        }
        await asyncio.sleep(1)

        # 3. Stream the reply
        response_text = f"You said: {last_message}"
      
        # Simulate streaming token by token
        for word in response_text.split():
            yield {
                "type": "reply",
                "content": word + " "
            }
            await asyncio.sleep(0.1)

        # 4. Finish
        yield {"type": "end", "content": ""}
```

## 🧩 Reference

For the logic behind agent loading, see:
[`mirobody/agent/chat/agent.py`](chat/agent.py)
