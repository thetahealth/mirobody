# 🤖 Developing Agents

Agents are the "brains" of Mirobody. They process user messages, execute logic (like calling LLMs or tools), and stream responses back to the chat interface.

## 📂 Discovery

Mirobody automatically discovers agents in the following locations:

1.  **Custom Agents**: `agents/` (Root directory) - **Place your own agents here.**
2.  **Core Agents**: `mirobody/pub/agents/` - Built-in system agents.

### Discovery Rules
1.  **File Location**: Must be a `.py` file inside `agents/`.
2.  **Naming Convention**: Class name must end with `Agent` (e.g., `SupportAgent`).
3.  **Inheritance**: Technically optional, but recommended to follow the standard signature.

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

You stream data back to the UI by yielding dictionaries with a `type` and `content`.

| Type | Content | Description |
|------|---------|-------------|
| `thinking` | `str` | Displayed as a "thought process" or log in the UI. |
| `reply` | `str` | Main text of the response (Markdown supported). |
| `error` | `str` | Error message to display to the user. |
| `end` | `""` | Signals that the response is complete. |

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

| Config Key | Description |
|------------|-------------|
| `PROVIDERS_MY` | LLM provider definitions (passed to `load_llm_clients`). |
| `ALLOWED_TOOLS_MY` | List of allowed tools (whitelist). |
| `DISALLOWED_TOOLS_MY` | List of disallowed tools (blacklist). |
| `PROMPTS_MY` | Path to prompt templates. |

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
[`mirobody/chat/agent.py`](mirobody/chat/agent.py)
