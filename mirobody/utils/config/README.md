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
| `LOG_LEVEL` | Log level: `debug`, `info`, `warning`, `error`   | `INFO`   |

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

#### Multimodal capability (`supports_pdf` / `supports_image`)

The agent reads uploaded PDFs and images through its `read_file` tool. Whether a
model can ingest them **natively** (preserving tables, figures, layout, scanned
pages) vs. needing pre-extracted text is auto-detected from LangChain's
normalized [model profile](https://docs.langchain.com/oss/python/langchain/models#model-profiles) —
so for first-party models (Gemini, OpenAI, Anthropic, Vertex) you configure
**nothing**.

Declare it only for **OpenAI-compatible endpoints** whose profile is unknown
(DashScope, Volcengine, OpenRouter-proxied models, …):

```yaml
PROVIDERS_DEEP:
  qwen-vl:                     # a vision model on DashScope
    llm_type: openai
    api_key: DASHSCOPE_API_KEY
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    model: qwen-vl-max
    supports_pdf: true         # read PDFs natively
    supports_image: true       # read images natively
  deepseek:                    # a text-only model — omit both
    llm_type: openai
    api_key: DASHSCOPE_API_KEY
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    model: deepseek-v4-flash
```

| Flag | Effect when `true` | When unset / `false` |
| --- | --- | --- |
| `supports_pdf` | PDFs sent as a native file block | PDF served as pre-extracted text (works on any text model) |
| `supports_image` | images sent as a native image block | image served as a native block only if the model's profile already allows it |

Notes:

- `PPT`/`PPTX` always read as extracted text (no provider accepts them as a file block).
- Advanced: a raw `profile: { … }` dict of [`ModelProfile`](https://reference.langchain.com/python/langchain_core/language_models/#langchain_core.language_models.ModelProfile) fields is also honored and overrides the friendly flags.

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
- agent/prompts/deep.jinja
```

Paths are resolved twice: first as `os.path.isfile(path)` relative to the
working directory, then relative to the installed `mirobody` package. So a
**package-relative** path like the one above works both from a source checkout
and from a `pip install` — which is why `config.yaml` uses that form — while
your own templates outside the package should use an absolute path, or one
relative to wherever you launch the server.

(This section documented `mirobody/agent/deep/prompts/default.jinja` and
`simple.jinja`. Neither the directory nor the files exist; the shipped
templates are under `agent/prompts/`.)

#### Path with Suffix Format

You can specify a custom key name using `path@suffix` format:

```yaml
PROMPTS_DEEP:
- agent/prompts/deep.jinja@main
- /path/to/your/own.jinja@simple
```

This will create `prompt_templates` with keys `main` and `simple` instead of deriving from file names.

## 🧪 Code Execution (QuickJS)

DeepAgent computes with an **in-process JS/TS interpreter** —
[langchain-quickjs](https://pypi.org/project/langchain-quickjs/)'
`CodeInterpreterMiddleware`, which adds a persistent `eval` REPL tool. No API
key, no network, no external sandbox service, nothing to provision.

Nothing to configure — it is on whenever the `[agents]` extra is installed. To
turn it off, block the tool:

```yaml
DISALLOWED_TOOLS_DEEP:
  - eval
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

LLM clients are managed via `LLMConfig`, following the same pattern as `PostgreSQLConfig` / `RedisConfig`:

```python
from mirobody.utils.config import global_config, LLMProvider

cfg = global_config()
llm = cfg.get_llm(LLMProvider.OPENAI)
client = llm.get_async_client()   # → AsyncOpenAI
```

### Supported Providers

#### OpenAI-compatible (API key + base URL)

These providers all return `OpenAI` / `AsyncOpenAI` clients:

| Provider | Enum | Config Key | Base URL |
| --- | --- | --- | --- |
| OpenAI | `OPENAI` | `OPENAI_API_KEY` | `https://api.openai.com/v1` |
| OpenRouter | `OPENROUTER` | `OPENROUTER_API_KEY` | `https://openrouter.ai/api/v1` |
| DashScope (Qwen) | `DASHSCOPE` | `DASHSCOPE_API_KEY` | `https://dashscope.aliyuncs.com/compatible-mode/v1` |
| Volcengine (Doubao) | `VOLCENGINE` | `VOLCENGINE_API_KEY` | `https://ark.cn-beijing.volces.com/api/v3` |
| DeepSeek | `DEEPSEEK` | `DEEPSEEK_API_KEY` | `https://api.deepseek.com/v1` |
| Zhipu (GLM) | `ZHIPU` | `ZHIPU_API_KEY` | `https://open.bigmodel.cn/api/paas/v4` |
| Moonshot (Kimi) | `MOONSHOT` | `MOONSHOT_API_KEY` | `https://api.moonshot.cn/v1` |

```yaml
# config.{env}.yaml — just add the API key
OPENAI_API_KEY: "sk-..."
DEEPSEEK_API_KEY: "sk-..."
```

#### Anthropic (Claude)

Returns `Anthropic` / `AsyncAnthropic` clients.

| Config Key |
| --- |
| `ANTHROPIC_API_KEY` |

#### Gemini (Google, API key)

Returns `google.genai.Client` / async client.

| Config Key | Description | Default |
| --- | --- | --- |
| `GOOGLE_API_KEY` | API key | *(required)* |
| `GEMINI_API_VERSION` | REST API version (`v1` or `v1beta`) | `v1beta` |

`v1beta` includes all features (Interactions API, Semantic Retriever, Tuned Models). `v1` is the stable subset — sufficient for `generateContent` and `embedContent`.

#### Cloud Providers (no API key — credential-based auth)

| Provider | Enum | Auth | Returns |
| --- | --- | --- | --- |
| Vertex AI (GCP) | `VERTEX_AI` | Application Default Credentials | `google.genai.Client` |
| Azure OpenAI | `AZURE` | Workload Identity Federation | `AzureOpenAI` / `AsyncAzureOpenAI` |
| AWS Bedrock | `BEDROCK` | IAM role / env credentials | `boto3` / `aioboto3` bedrock-runtime client |

### HTTP Client (`get_aiohttp_session`)

All providers support `get_aiohttp_session()`, which returns an `aiohttp.ClientSession` with `base_url` and auth headers pre-configured. Useful for lightweight REST calls (e.g., embeddings) where the full SDK is unnecessary.

```python
from mirobody.utils.config import global_config, LLMProvider

cfg = global_config()

# Gemini embedding
llm = cfg.get_llm(LLMProvider.GEMINI)
async with llm.get_aiohttp_session() as session:
    async with session.post("/models/gemini-embedding-001:batchEmbedContents", json=payload) as resp:
        data = await resp.json()

# DashScope (Qwen) embedding — OpenAI-compatible
llm = cfg.get_llm(LLMProvider.DASHSCOPE)
async with llm.get_aiohttp_session() as session:
    async with session.post("/embeddings", json={
        "model": "text-embedding-v4",
        "input": texts,
    }) as resp:
        data = await resp.json()

# Vertex AI — ADC token auto-refreshed
llm = cfg.get_llm(LLMProvider.VERTEX_AI)
async with llm.get_aiohttp_session() as session:
    async with session.post("/publishers/google/models/gemini-embedding-001:predict", json=payload) as resp:
        data = await resp.json()
```

Auth headers per provider:

| Provider | Header |
| --- | --- |
| OpenAI-compatible (7 providers) | `Authorization: Bearer {api_key}` |
| Anthropic | `x-api-key: {api_key}` |
| Gemini | `x-goog-api-key: {api_key}` |
| Vertex AI | `Authorization: Bearer {oauth2_token}` (ADC) |

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

If your configuration source delivers settings as JSON, set `AZURE_OPENAI` as a JSON string — the system parses it automatically:

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

### AWS Bedrock

AWS Bedrock provides a unified `Converse` API for multiple model providers (Claude, Llama, Nova, Mistral, etc.).

| Variable | Description | Required | Default |
| -------- | ----------- | -------- | ------- |
| `AWS_REGION` | AWS region for Bedrock | No | `us-east-1` |

```yaml
AWS_REGION: us-east-1
```

#### Authentication

Bedrock uses **IAM credentials** — no API key is needed.

- **EKS**: IAM Roles for Service Accounts (IRSA) auto-injects credentials.
- **Local development**: `aws configure` or `export AWS_PROFILE=...`
- **Service account**: `export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...`

#### Usage

```python
llm = cfg.get_llm(LLMProvider.BEDROCK)
client = llm.get_client()

response = client.converse(
    modelId="anthropic.claude-sonnet-4-20250514-v1:0",
    messages=[{"role": "user", "content": [{"text": "Hello"}]}],
)
```

## 🧪 Testing

Tests sit beside the code they cover; the repository publishes no separate
`tests/` tree.
Bare `pytest` from the repo root is the whole suite — seconds, no
database, no network, no API key:

```bash
pip install -e '.[agents,test]'
pytest
```

Layout, markers, snapshot regeneration and the release gates:
**[docs/testing.md](../../../docs/testing.md)**.

### Test Categories

Two markers exist, both declared in `pyproject.toml`:

| Marker | Meaning | Skip with |
| --- | --- | --- |
| `needs_db` | requires a live PostgreSQL | `-m 'not needs_db'` |
| `needs_llm` | calls a real model provider and costs money | `-m 'not needs_llm'` |

This table used to list `mcp` / `chat` / `slow` and more, none of which were
ever defined, so `pytest -m mcp` reported *0 tests collected*. The
`pyproject.toml` comment above `markers` records the same trap.

### What the Tests Cover

For this module: config parsing, provider wiring and the one-key defaults —
`mirobody/test_one_key_defaults.py`. The
agent's PostgreSQL-backed filesystem tools (`write_file` / `read_file` / `ls` /
`glob` / `grep`) have no dedicated suite yet; the bullet list that used to sit
here described upstream tests that never shipped with this repository.

There is no external code-execution sandbox to test: QuickJS (see the section
above) runs in-process, needs no key, and only reshapes already-fetched
numbers into chart JSON — no sandbox lifecycle, no shell, no remote state.