
## Project Setup and Run Guide

This guide walks you through configuring, building, and running the project in both Docker and local environments. Follow the steps in order, and refer to the notes for your platform when relevant.

## I. Config

1. Set up environment variables.
    
   Create your own `.env` file based on the `.env.example` file:
   ```ini
   ENV=localdb
   ```

2. Accomplish configuration.

   Create your own `config.localdb.yaml` file according to the value of `ENV` variable.
   
   First of all, you have to fill one of the following api keys at least, so as to visit LLMs:
   ```yml
   GOOGLE_API_KEY: ''
   OPENAI_API_KEY: ''
   OPENROUTER_API_KEY: ''
   ```

   Append your local directories to the following lists, once you are ready to develop your own MCP tools/resources or agents:
   ```yml
   MCP_TOOL_DIRS:
   - mirobody/pub/tools
   MCP_RESOURCE_DIRS:
   - mirobody/pub/resources
   AGENT_DIRS:
   - mirobody/pub/agents
   ```

   You can take configuration keys listed in the `config.yaml` into account.
   However, `config.yaml` itself is assumed to be immutable.

## III. Agent Development Guide

This project provides two types of agents, each with different capabilities and use cases:

### 1. DeepAgent (Recommended for Complex Tasks, Especially File Processing & Management)

**Framework**: [LangChain DeepAgents](https://docs.langchain.com/oss/python/deepagents/overview)

**Features**:
- ✅ Multi-step task planning and execution
- ✅ File system operations (read, write, edit, glob, grep)
- ✅ Subagent spawning for context isolation
- ✅ Long-term memory across conversations
- ✅ PostgreSQL backend for file persistence
- ✅ Full MCP tools integration

**When to Use**:
- Complex workflows requiring multiple steps
- Tasks involving file operations
- Projects needing persistent context
- Scenarios requiring task decomposition


**Supported Providers(via langchain)**:
- **Gemini 3.0** (via Google GenAI SDK)
- **GPT-5** (via OpenAI API or Openrouter)
- **Claude Sonnet an other models** (via OpenRouter)

**Configuration** (`config.yaml`):

```yaml
GOOGLE_API_KEY: 'your_google_api_key'        # For Gemini models
OPENAI_API_KEY: 'your_openai_api_key'        # For GPT models
OPENROUTER_API_KEY: 'your_openrouter_key'    # For Claude, DeepSeek, etc.
# For other models, refer to: https://docs.langchain.com/oss/python/integrations/chat


# Provider Configurations
PROVIDERS_DEEP:
  # Gemini 3.0 models (requires langchain-google-genai>=4.1.2)
  gemini-3-pro:
    llm_type: google-genai
    api_key: GOOGLE_API_KEY
    model: gemini-3-pro-preview
    temperature: 1.0
  
  gemini-3-flash:
    llm_type: google-genai
    api_key: GOOGLE_API_KEY
    model: gemini-3-flash-preview
    temperature: 1.0
  
  # OpenAI GPT models
  gpt-5.1:
    llm_type: openai
    api_key: OPENAI_API_KEY
    base_url: https://api.openai.com/v1/
    model: gpt-5.1
    temperature: 0.1
  
  # Claude via OpenRouter
  claude-sonnet:
    llm_type: openai  # Use OpenAI-compatible API
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: anthropic/claude-sonnet-4.5
    temperature: 0.1
  
  # DeepSeek via OpenRouter
  deepseek-v3.2:
    llm_type: openai
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: deepseek/deepseek-v3.2
    temperature: 0.1

# Optional: Custom prompts
PROMPTS_DEEP:
- mirobody/pub/agents/deep/prompts/theta_health.jinja
- mirobody/pub/agents/deep/prompts/theta_health_simple.jinja

# Optional: Tool filtering
ALLOWED_TOOLS_DEEP:    # Leave empty to allow all tools
DISALLOWED_TOOLS_DEEP: # Specify tools to disable
```

**API Key Setup**:

1. **Google API Key**: Get from [Google AI Studio](https://aistudio.google.com/app/apikey)
2. **OpenAI API Key**: Get from [OpenAI Platform](https://platform.openai.com/api-keys)
3. **OpenRouter API Key**: Get from [OpenRouter](https://openrouter.ai/keys)

> **Note**: You only need to configure the API keys for the providers you plan to use.


**Usage**:
```python
from mirobody.pub.agents.deep_agent import DeepAgent

agent = DeepAgent(
    user_id="user123",
    user_name="John Doe",
    token="jwt_token",
    timezone="America/Los_Angeles"
)

async for event in agent.generate_response(
    user_id="user123",
    messages=[{"role": "user", "content": "Analyze this data"}],
    provider="gemini-3-flash",
    session_id="session_001"
):
    print(event)
```

### 2. BaselineAgent (Best for Simple MCP Integration)

**Framework**: Direct MCP integration with Gemini 2.5 / MiroMind

**Features**:
- ✅ Native MCP server support
- ✅ Streaming responses
- ✅ Direct tool configuration to LLM
- ✅ Lightweight and fast
- ❌ No file system operations
- ❌ No subagents

**When to Use**:
- Simple conversational tasks
- Direct MCP tool usage
- Minimal setup required
- Gemini/MiroMind native features

**Supported Providers**:
- **Gemini 2.5/3.0** (native Google GenAI)
- **MiroThinker** (custom model)

**Configuration** (`config.yaml`):

```yaml
# Required API Keys
GOOGLE_API_KEY: 'your_google_api_key'           # For Gemini models
MIROTHINKER_API_KEY: 'your_mirothinker_key'     # For MiroThinker refer to https://research.miromind.ai

# Provider Configurations
PROVIDERS_BASELINE:
  gemini-3-pro:
    api_key: GOOGLE_API_KEY
    model: gemini-3-pro-preview
  
  gemini-3-flash:
    api_key: GOOGLE_API_KEY
    model: gemini-3-flash-preview
  
  gemini-2.5-flash:
    api_key: GOOGLE_API_KEY
    model: gemini-2.5-flash
  
  miro-thinker:
    api_key: MIROTHINKER_API_KEY
    model: miro-thinker
```

**Usage**:
```python
from mirobody.pub.agents.baseline_agent import BaselineAgent

agent = BaselineAgent(
    user_id="user123",
    user_name="John Doe"
)

async for event in agent.generate_response(
    user_id="user123",
    messages=[{"role": "user", "content": "What's my health status?"}],
    provider="gemini-2.5-flash"
):
    print(event)
```

## IV. MCP Tools Development Guide

MCP (Model Context Protocol) tools are modular functions that agents can call to extend their capabilities.

### Tool Structure

```
mirobody/pub/
├── tools/              # General-purpose tools
│   ├── chart_service.py    # Visualization tools (25+ chart types)
│   └── ...
└── tools_health/       # Domain-specific tools
    ├── genetic_service.py
    ├── health_indicator_service.py
    └── user_service.py
```

### Creating a New Tool

**Example**: Create a weather service tool

1. **Create tool file** (`mirobody/pub/tools/weather_service.py`):

```python
from typing import Dict, Any, Optional

async def get_weather(
    city: str,
    user_info: Optional[Dict[str, Any]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Get weather information for a city.
    
    Args:
        city: City name
        user_info: Optional user authentication info
        
    Returns:
        Weather data dictionary
    """
    # Your implementation here
    return {
        "city": city,
        "temperature": "22°C",
        "condition": "Sunny"
    }

# Export for MCP loader
__all__ = ["get_weather"]
```

2. **Add tool directory to config** (`config.yaml`):

```yaml
MCP_TOOL_DIRS:
  - mirobody/pub/tools
  - mirobody/pub/tools_health
  - your/custom/tools/directory  # Add your directory
```

3. **Use in agent**:

All tools are automatically loaded and available to agents that support MCP:
- ✅ DeepAgent (full support)
- ✅ BaselineAgent (native MCP)

### Tool Best Practices

1. **Authentication**: Use `user_info` parameter for user-specific operations
   ```python
   async def my_tool(param: str, user_info: Optional[Dict[str, Any]] = None):
       user_id = user_info.get("user_id") if user_info else None
       # Validate user access
   ```

2. **Error Handling**: Return structured error responses
   ```python
   return {"error": "Invalid input", "code": 400}
   ```

3. **Type Hints**: Use proper type annotations for better validation
   ```python
   from typing import List, Dict, Any, Optional
   ```

4. **Documentation**: Include clear docstrings with Args and Returns

5. **Async Operations**: Prefer async functions for I/O operations
   ```python
   async def fetch_data(...) -> Dict[str, Any]:
       async with aiohttp.ClientSession() as session:
           ...
   ```

### Available Built-in Tools

**General Tools** (`mirobody/pub/tools/`):
- `chart_service.py` - 25+ chart types (line, bar, pie, sankey, etc.)

**Health Tools** (`mirobody/pub/tools_health/`):
- `genetic_service.py` - Genetic data analysis
- `health_indicator_service.py` - Health metrics tracking
- `user_service.py` - User profile management

### Tool Discovery

Tools are automatically discovered by:
1. Scanning directories in `MCP_TOOL_DIRS`
2. Loading all functions with proper signatures
3. Registering them with the MCP service

No manual registration required! Just add your tool file to a configured directory.

## V. Agent Comparison Matrix

| Feature | DeepAgent | BaselineAgent |
|---------|-----------|---------------|
| **Framework** | LangChain DeepAgents | Gemini/MiroThinker MCP |
| **Planning** | ✅ Built-in todos | ❌ |
| **File System** | ✅ Full support | ❌ |
| **Subagents** | ✅ Task isolation | ❌ |
| **MCP Tools** | ✅ Full integration | ✅ Native |
| **MCP Resources** | ❌ | ✅ Native |
| **MCP Prompt Templates** | ❌ | ✅ Native |
| **Memory** | ✅ PostgreSQL | ❌ |
| **Streaming** | ✅ | ✅ |
| **Model Support** | Multi-provider (Gemini, GPT, Claude, DeepSeek) | Gemini 2.5/3.0, MiroThinker |
| **Best For** | Complex workflows, file operations | Simple tasks, native MCP |
| **Setup Complexity** | Medium | Low |

Choose the agent that best fits your use case!

## II. Run

1. Run as Dockers.
   
   ```bash
   ./deploy.sh
   ```

2. Run as Local Server.

   [Optional] Create a Python virtual environment, and then activate it:
   ```bash
   python -v venv .venv
   source .venv/bin/activate
   ```

   Install Python packages:
   ```bash
   pip install mirobody -r requirements.txt
   ```
   And you might need to install the following system packages as well:
   ```bash
   sudo apt install -y g++ gfortran build-essential libfftw3-dev libhdf5-dev libblas-dev liblapack-dev
   ```
   
   Install Nodejs packages:
   ```bash
   npm install @antv/gpt-vis-ssr ws
   ```

   Configure PostgreSQL and Redis servers via filling the following values within your `config.localdb.yaml` file:
   ```yml
   # PostgreSQL.
   PG_HOST: ''
   PG_PORT: 5432
   PG_USER: ''
   PG_PASSWORD: ''
   PG_DBNAME: ''
   PG_SCHEMA: ''

   # Redis.
   REDIS_HOST: ''
   REDIS_PORT: 6379
   REDIS_DB: 0
   REDIS_PASSWORD: ''
   ```

   Start mirobody server:
   ```bash
   python main.py
   ```
   The server will start with default settings. Check the console output for the host and port. If you’re running under WSL, access the server from Windows via `http://localhost:18080`.
