# ⚙️ Configuration Guide

Mirobody uses a flexible configuration system that combines YAML files and environment variables.

## 📄 Configuration Files

1. **`config.yaml`**: The default configuration template. **Do not edit this directly.**
2. **`config.{env}.yaml`**: Environment-specific overrides (e.g., `config.localdb.yaml`). Use this for your local settings.
3. **`.env`**: Secrets and environment variables.

### Priority Order

1. Environment Variables (Highest)
2. `config.{env}.yaml`
3. `config.yaml` (Lowest)

## 🌍 Timezone

| Key                | Description                                                   | Default               |
| ------------------ | ------------------------------------------------------------- | --------------------- |
| `DEFAULT_TIMEZONE` | Default timezone for users who haven't set their own timezone | `America/Los_Angeles` |

Set this in your `config.{env}.yaml` to match your deployment region:

```yaml
# For Japan deployment
DEFAULT_TIMEZONE: Asia/Tokyo
```

Valid values are [IANA timezone names](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) (e.g., `Asia/Shanghai`, `America/New_York`, `Europe/London`, `UTC`).

## 📝 Logging

| Key         | Description                                      | Default   |
| ----------- | ------------------------------------------------ | --------- |
| `LOG_NAME`  | Log file name prefix. Empty = console-only       | *(empty)* |
| `LOG_DIR`   | Directory for log files                          | *(empty)* |
| `LOG_LEVEL` | Log level: `debug`, `info`, `warning`, `error`   | `DEBUG`   |

By default (no `LOG_NAME`), logs go to **console only** (stdout). To enable file logging, set both `LOG_NAME` and `LOG_DIR` in your `config.{env}.yaml`:

```yaml
LOG_NAME: mirobody
LOG_DIR: ./logs
```

Log files are created as `{date}_{name}_{time}.log` in the specified directory. Console logging remains active alongside file logging.

## 🏗️ Infrastructure

Core system settings found in `config.yaml`.

### Database (PostgreSQL)

| Key             | Description                                     |
| --------------- | ----------------------------------------------- |
| `PG_HOST`     | Hostname (e.g.,`localhost` or `10.108.0.2`) |
| `PG_PORT`     | Port (Default:`5432`)                         |
| `PG_USER`     | Username                                        |
| `PG_PASSWORD` | Password                                        |
| `PG_DBNAME`   | Database name                                   |

### Cache (Redis)

| Key                | Description             |
| ------------------ | ----------------------- |
| `REDIS_HOST`     | Hostname                |
| `REDIS_PORT`     | Port (Default:`6379`) |
| `REDIS_PASSWORD` | Password                |

## 🤖 Agent Configuration

Agents are configured using a specific naming convention: `KEY_{AGENT_NAME}`.

### 1. Providers (`PROVIDERS_{NAME}`)

Defines the LLM clients available to the agent. Passed to the agent's `load_llm_clients` method.

```yaml
PROVIDERS_DEEP:
  gemini-3-flash:
    llm_type: google-genai
    api_key: GOOGLE_API_KEY  # References env var
    model: gemini-2.0-flash-exp
  gpt-4o:
    llm_type: openai
    api_key: OPENAI_API_KEY
    model: gpt-4o
```

#### MixAgent Configuration

MixAgent uses a two-phase model fusion architecture. Phase 1 (Orchestrator) uses providers with `@orchestrator` suffix for tool orchestration and data collection, while Phase 2 (Responder) uses providers with `@responder` suffix for response generation.

**Important**:

- Providers with `@responder` and `@orchestrator` suffixes are internal-only and will NOT appear in frontend APIs (`/api/providers`, `/api/models`).
- At least one `@orchestrator` provider is required for Phase 1
- At least one `@responder` provider is required for Phase 2

##### Nested Configuration Format (Recommended)

Group multiple providers under a single frontend-visible name:

```yaml
PROVIDERS_MIX:
  claude|gemini:  # Frontend display name (e.g., "claude|gemini" shown in UI)
    # Phase 1 (Orchestrator) - Tool orchestration and data collection
    claude-sonnet@orchestrator:
      llm_type: openai
      api_key: OPENROUTER_API_KEY
      base_url: https://openrouter.ai/api/v1
      model: anthropic/claude-sonnet-4.6
      temperature: 0.1

    # Phase 2 (Responder) - Response generation with tool context
    gemini-3-pro@responder:
      llm_type: google-genai
      api_key: GOOGLE_API_KEY
      model: gemini-3.1-pro-preview
      temperature: 1.0
      response_with_tools: true  # Used when Phase 1 made tool calls

    # Phase 2 (Responder) - Quick responses without tool context
    gemini-3-flash@responder:
      llm_type: google-genai
      api_key: GOOGLE_API_KEY
      model: gemini-3.1-flash-lite-preview
      temperature: 1.0
      response_with_tools: false  # Used when Phase 1 had no tool calls
```

##### Flat Configuration Format (Legacy)

For backward compatibility, flat format is also supported:

```yaml
PROVIDERS_MIX:
  claude-sonnet@orchestrator:
    llm_type: openai
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: anthropic/claude-sonnet-4.6
    temperature: 0.1

  gemini-pro@responder:
    llm_type: google-genai
    api_key: GOOGLE_API_KEY
    model: gemini-3.1-pro-preview
    temperature: 1.0
    response_with_tools: true
```

##### Response Selection Logic

The `response_with_tools` field determines which responder to use:

| `response_with_tools` | Usage                                                               |
| ----------------------- | ------------------------------------------------------------------- |
| `true`                | Used when Phase 1 made tool calls (complex queries with data)       |
| `false`               | Used when Phase 1 had no tool calls (simple queries, quick answers) |
| *omitted* or `null` | **Flexible mode** - Used for both cases (single responder)    |

##### Example: Single Responder for All Cases

```yaml
PROVIDERS_MIX:
  claude|gemini:
    claude-sonnet@orchestrator:
      llm_type: openai
      model: anthropic/claude-sonnet-4.6

    gemini-3-pro@responder:  # No response_with_tools field
      llm_type: google-genai
      model: gemini-3.1-pro-preview
      temperature: 1.0
      # response_with_tools omitted - used for all cases
```

### 2. Tools (`ALLOWED_TOOLS_{NAME}` / `DISALLOWED_TOOLS_{NAME}`)

Control which tools an agent can access using whitelist or blacklist configurations.

#### Whitelist Configuration

Explicitly specify allowed tools - agent can only use these tools:

```yaml
ALLOWED_TOOLS_DEEP:
  - web_search
  - calculator
  - file_reader
```

#### Blacklist Configuration

Specify disallowed tools - agent can use all tools except these:

```yaml
DISALLOWED_TOOLS_DEEP:
  - dangerous_tool
  - deprecated_tool
```

#### Combined Configuration

When both are specified, whitelist takes precedence:

```yaml
ALLOWED_TOOLS_DEEP:
  - web_search
  - calculator
  - file_reader

DISALLOWED_TOOLS_DEEP:
  - file_reader  # This will be ignored - whitelist has priority
```

**Note**: If neither is specified, the agent has access to all available tools.

### 3. Prompts (`PROMPTS_{NAME}`)

Path to Jinja2 template files used for system prompts.

```yaml
PROMPTS_DEEP:
- mirobody/pub/agents/deep/prompts/default.jinja
```

#### Path with Suffix Format

You can specify a custom key name using `path@suffix` format:

```yaml
PROMPTS_DEEP:
- mirobody/pub/agents/deep/prompts/default.jinja@main
- mirobody/pub/agents/deep/prompts/simple.jinja@simple
```

This will create `prompt_templates` with keys `main` and `simple` instead of deriving from file names.

#### MixAgent Prompts Configuration

MixAgent requires two prompts with specific keys:

- `@orchestrator`: Phase 1 prompt for tool orchestration
- `@responder`: Phase 2 prompt for response generation

```yaml
PROMPTS_MIX:
  - pub/agents/mix/prompts/orchestrator.jinja@orchestrator
  - pub/agents/mix/prompts/responder.jinja@responder
```

**Important**: Both prompts are required for MixAgent to function properly.

## 🧪 Code Execution (Sandbox)

Mirobody supports running code in isolated sandbox environments for data analysis, computation, and file processing. This is powered by [E2B](https://e2b.dev) cloud sandboxes.

### How It Works

When `E2B_API_KEY` is configured, the `execute` tool becomes available to agents. The architecture follows the [deepagents](https://github.com/langchain-ai/deepagents) `SandboxBackendProtocol` pattern:

- **E2BSandboxBackend** implements `BaseSandbox` — all file operations (read/write/edit/grep/glob) and code execution share the same isolated E2B sandbox
- **PostgresBackend** delegates `execute()` calls to the E2B sandbox while handling workspace file operations via PostgreSQL
- Agents use `write_file` to create files in the sandbox, then `execute` to run code, then `read_file` to retrieve results

### Configuration

| Key           | Description                                  | Required |
| ------------- | -------------------------------------------- | -------- |
| `E2B_API_KEY` | API key from [e2b.dev](https://e2b.dev)      | Yes      |

Set in your `config.{env}.yaml` or as an environment variable:

```yaml
E2B_API_KEY: "e2b_..."
```

Or via environment variable:

```bash
export E2B_API_KEY="e2b_..."
```

### Usage Examples

Once configured, agents can execute shell commands in the sandbox:

- `execute(command="python3 -c 'print(2+2)'")` — inline Python
- `write_file("/script.py", "import pandas as pd; ...")` then `execute(command="python3 /script.py")` — multi-step
- `execute(command="pip install scikit-learn && python3 analysis.py")` — install packages + run

### Disabling Code Execution

To disable code execution, simply leave `E2B_API_KEY` empty (default). The `execute` tool will return a configuration error when called. You can also explicitly block it:

```yaml
DISALLOWED_TOOLS_DEEP:
  - execute
```

## 🔒 Security

### Credential Encryption

Sensitive keys in `config.yaml` (ending in `_KEY`, `_PASSWORD`, etc.) can be encrypted.

- Use the `CONFIG_ENCRYPTION_KEY` from your `.env` file to encrypt/decrypt values.
- If a value matches `REPLACE_THIS_VALUE_IN_PRODUCTION`, it must be set via environment variable or override file.

### Environment Variables

For sensitive data like API keys, use environment variables:

```bash
export OPENAI_API_KEY="sk-..."
```

Then reference them in YAML or let the system auto-detect them if they match the config key.

## 🏥 LLM Provider Configuration

All LLM traffic routes through HIPAA-compliant providers:
- **Chat / Embedding** → Azure OpenAI (WIF auth)
- **File processing** → GCP Vertex Gemini

### Azure OpenAI

Configure via a single `AZURE_OPENAI` block in your `config.{env}.yaml`:

```yaml
AZURE_OPENAI:
  endpoint: https://my-resource.openai.azure.com/
  api_version: 2025-03-01-preview   # optional — defaults to 2025-03-01-preview
  deployments:
    gpt-4o: my-gpt4o-deployment     # deployment name; all models share the endpoint above
    gpt-4.1: my-gpt41-deployment
    gpt-4o-mini: my-gpt4o-mini
    text-embedding-3-small: my-embed-small
    text-embedding-3-large: my-embed-large
```

All deployment entries inherit `endpoint` from the parent block. Only override `endpoint` at the model level if the model is deployed on a **different** Azure resource (rare):

```yaml
AZURE_OPENAI:
  endpoint: https://default.openai.azure.com/
  deployments:
    gpt-4o: my-gpt4o-deployment        # uses parent endpoint
    text-embedding-3-small:
      deployment: embed-small-prod
      endpoint: https://embed.openai.azure.com/  # different resource
```

#### Authentication

Azure OpenAI uses **Workload Identity Federation (WIF)** — no API key is needed. Credentials are auto-injected by the Kubernetes Azure Workload Identity webhook on EKS.

EKS pods must have the following injected by the webhook:
- `AZURE_CLIENT_ID` environment variable
- Federated token file at `/var/run/secrets/azure/tokens/azure-identity-token`

#### Configuration Center (JSON)

If your config center delivers settings as JSON, set `AZURE_OPENAI` as a JSON string — the system parses it automatically:

```bash
AZURE_OPENAI='{"endpoint":"https://my-resource.openai.azure.com/","api_version":"2025-03-01-preview","deployments":{"gpt-4o":"my-gpt4o-deployment","gpt-4.1":"my-gpt41-deployment","text-embedding-3-small":"my-embed-small"}}'
```

#### Key Reference

| Key | Where | Description |
| --- | ----- | ----------- |
| `AZURE_OPENAI.endpoint` | YAML / JSON | Azure OpenAI resource endpoint (shared by all models) |
| `AZURE_OPENAI.api_version` | YAML / JSON | API version, default `2025-03-01-preview` |
| `AZURE_OPENAI.deployments` | YAML / JSON | Model → deployment name mapping |

### Vertex AI (GCP)

All file processing (document extraction, image analysis) is routed to **Vertex AI Gemini**.

| Variable | Description | Required | Default |
| -------- | ----------- | -------- | ------- |
| `GCP_PROJECT` | GCP project ID where Vertex AI is enabled | Yes | *(none)* |
| `GCP_LOCATION` | Vertex AI region | No | `us-east5` |

```yaml
GCP_PROJECT: my-gcp-project-id
GCP_LOCATION: us-east5
```

#### Authentication

Vertex AI uses **Application Default Credentials (ADC)** — no API key is needed.

- **EKS / GKE**: Workload Identity Federation auto-injects credentials.
- **Local development**: `gcloud auth application-default login`
- **Service account**: `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"`

## 🧪 Testing

Mirobody includes an integration test suite covering file operations, code execution, MCP protocol, and chat API.

### Prerequisites

- A running Mirobody server (local or Docker)
- Demo account configured (`EMAIL_PREDEFINE_CODES` in config)
- Python test dependencies: `pip install pytest httpx`

### Environment Variables

| Variable       | Description                          | Default                    |
| -------------- | ------------------------------------ | -------------------------- |
| `MIROBODY_URL` | Server URL                           | `http://localhost:18080`   |
| `DEMO_EMAIL`   | Demo account email                   | `demo1@mirobody.ai`       |
| `DEMO_CODE`    | Demo verification code               | `777777`                   |
| `E2B_API_KEY`  | E2B sandbox API key (for execute)    | *(empty — execute tests skipped)* |

### Running Tests

```bash
# All tests
pytest tests/ -v

# Quick tests only (skip slow LLM/E2B tests)
pytest tests/ -v -m "not slow"

# By category
pytest tests/ -v -m mcp       # MCP tool tests (file ops, execute discovery)
pytest tests/ -v -m e2b       # E2B sandbox tests (requires E2B_API_KEY)
pytest tests/ -v -m chat      # Chat API tests (requires LLM provider keys)

# Specific test file
pytest tests/test_execute_tool.py -v   # File operations & execute tool
```

### Test Categories

| Marker   | Description                                                       | E2B Required |
| -------- | ----------------------------------------------------------------- | ------------ |
| `mcp`    | MCP protocol: file ops (write/read/edit/ls/glob/grep), execute    | No*          |
| `e2b`    | E2B sandbox: command execution, cross-filesystem sync             | Yes          |
| `chat`   | Chat API: agents trigger tools, session-scoped file visibility    | No*          |
| `slow`   | Tests that take >10s (LLM calls, sandbox creation)                | Varies       |

*\* Some tests within `mcp` and `chat` markers are additionally marked `e2b` and skipped without `E2B_API_KEY`.*

### What the Tests Cover

**Without E2B** (always run):
- `write_file` → `read_file` round-trip
- `write_file` → `ls` visibility (the reported bug scenario)
- `write_file` → `edit_file` → `read_file` CRUD cycle
- `write_file` → `glob` / `grep` discoverability
- Multiple writes → `ls` shows all files
- Subdirectory file operations
- Error handling (nonexistent files, duplicate writes)
- Execute tool graceful degradation (returns config error)
- Chat API: write → ls via real session_id

**With E2B** (requires `E2B_API_KEY`):
- Shell command execution (echo, Python, pip install)
- Exit code reporting (success/failure)
- stderr capture
- Timeout handling
- Cross-filesystem sync: `write_file` (PostgreSQL) → `execute` (E2B sandbox)
- Sandbox state persistence across calls
- Chat API: agents trigger execute and return results