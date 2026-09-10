# ⚙️ Configuration Guide

Mirobody uses a flexible configuration system that combines YAML files and environment variables.

## 📄 Configuration Files

1. **`config.yaml`**: server, database, login — what the containers wire. **Do not edit this directly.**
   Its `INCLUDE` list names the two files that load right after it:
2. **`config.llm.yaml`**: the one to open — `MODELS` (the model table) and which entry each surface uses. It names the key variable, never the secret.
3. **`config.devices.yaml`**: Garmin / Oura / Whoop (empty credentials, vendor endpoints filled in) and Google / Apple sign-in.
4. **`config.{env}.yaml`**: Environment-specific overrides (e.g., `config.localdb.yaml`). Use this for your local settings.
5. **`.env`**: `ENV`, the encryption keys, and the ONE LLM API key.

### Priority Order

1. Environment Variables (Highest)
2. `config.{env}.yaml`
3. the `INCLUDE`d files, in list order (`config.llm.yaml`, then `config.devices.yaml`)
4. `config.yaml` (Lowest)

Dictionaries do not merge across files: a later file's key replaces the whole
value.

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

One agent, one set of keys — no agent-name suffix (before 1.4.0 these were
`PROVIDERS_DEEP`, `PROMPTS_DEEP`, `ALLOWED_TOOLS_DEEP`, `DISALLOWED_TOOLS_DEEP`,
`DEFAULT_PROVIDER_DEEP`), and no "provider" in the model keys: in this project a
provider is a device or data source (`PROVIDER_DIRS`), so 1.4.1 renamed
`PROVIDERS` → `MODELS`, `DEFAULT_PROVIDER` → `DEFAULT_MODEL` and
`EMBEDDING_PROVIDER` → `UTILS_EMBEDDING_MODEL`.

**Upgrading from 1.3.x or 1.4.0?** The old spellings still work. Each is
renamed onto its current name as the config file merges, and the log says so
once. Rename them anyway — the alias is a migration courtesy, not the contract.

Renaming happens at LOAD time, and that placement is the whole point: the
shipped `config.llm.yaml` declares `MODELS`, `PROMPTS`, `ALLOWED_TOOLS` and
`DISALLOWED_TOOLS` itself, so an alias that only filled in when the new key
was *missing* would never have fired — the shipped default shadowed the
overlay, which is exactly how a 1.3.x deployment came up with zero providers
and an empty `/api/models` and nothing in the log. Because the rename happens
as each file merges, ordinary layering still decides: a later file's old
spelling overrides an earlier file's new one. Environment variables alias the
same way, and the current spelling always wins when both are set.

Four keys are **not** aliased, and a config that still carries one is named in
the log rather than ignored: `PRIVATE_AGENT_DIRS` (use `AGENT_DIRS`),
`MCP_RESOURCE_DIRS` (removed with the MCP `resources` capability), and
`HEARTBEAT_INTERVAL` / `HEARTBEAT_COUNTER_THRESHOLD` — `SSE_HEARTBEAT_SECONDS`
is not those under a new name, since the old pair multiplied to a first ping at
40 s while the new one fires on silence.

### 1. Models (`MODELS`)

One entry per model the deployment may call. The chat entries become LangChain
chat models at startup (`mirobody/agent/models/clients.py`); `api_key` names the
environment variable (in `.env`) that holds the secret, and an entry whose key is
absent is listed as unusable rather than failing the boot.

```yaml
MODELS:
  claude-sonnet:
    llm_type: openai
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: anthropic/claude-sonnet-5
    supports_image: true
  gemini-flash:
    llm_type: openai            # Google's OpenAI-compatible endpoint
    api_key: GOOGLE_API_KEY
    base_url: https://generativelanguage.googleapis.com/v1beta/openai/
    model: gemini-3.8-flash
    supports_image: true
```

`DEFAULT_MODEL` names the entry a chat uses when the client sends none;
unset, the default is the **first entry (in file order) whose key is present** —
so the order of `MODELS` is a contract, as it is in mirovital's
`MODEL_PROVIDERS`. An entry with `chat: false` is for the utility surfaces only
(file parsing, indicator extraction, titles) and never reaches the picker; an
entry with `embedding: <family>` is an embedding model and is used by nothing
but `UTILS_EMBEDDING_MODEL`.

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
MODELS:
  qwen-utils:                  # the utility model on DashScope: MUST read images
    llm_type: openai
    api_key: DASHSCOPE_API_KEY
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    model: qwen3.8-flash
    supports_image: true       # read images natively — required for UTILS_VISION_MODEL
    chat: false
  kimi:                        # a text-only model — say so, and the vision
    llm_type: openai           # route (UTILS_VISION_MODEL) will skip it
    api_key: DASHSCOPE_API_KEY
    base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
    model: kimi-k2.5
    supports_image: false
```

| Flag | Effect when `true` | When unset / `false` |
| --- | --- | --- |
| `supports_pdf` | PDFs sent as a native file block | PDF served as pre-extracted text (works on any text model) |
| `supports_image` | images sent as a native image block | image served as a native block only if the model's profile already allows it |

Notes:

- `PPT`/`PPTX` always read as extracted text (no provider accepts them as a file block).
- Advanced: a raw `profile: { … }` dict of [`ModelProfile`](https://reference.langchain.com/python/langchain_core/language_models/#langchain_core.language_models.ModelProfile) fields is also honored and overrides the friendly flags.

### 2. Tools (`ALLOWED_TOOLS` / `DISALLOWED_TOOLS`)

Which MCP tools the agent may call. A whitelist names the only tools allowed;
a blacklist removes tools from the full set; when both are given the whitelist
wins. Neither set means every tool.

```yaml
ALLOWED_TOOLS:
  - query_health_indicators
  - resolve_indicator

DISALLOWED_TOOLS:
  - get_genetic_data
```

### 3. Prompts (`PROMPTS`)

Jinja2 templates for the system prompt. The first entry is the default; a
client can ask for another by name (`prompt_name`).

```yaml
PROMPTS:
- agent/prompts/mirobody.jinja
- /path/to/your/own.jinja@concise      # path@name overrides the key
```

Paths are resolved twice: first as `os.path.isfile(path)` relative to the
working directory, then relative to the installed `mirobody` package. So a
**package-relative** path like the one above works both from a source checkout
and from a `pip install` — which is why `config.yaml` uses that form — while
your own templates outside the package should use an absolute path, or one
relative to wherever you launch the server.

`AGENT_NAME` is the persona name the template addresses the model by
(default `Mirobody`).

### 4. The agent itself (`AGENT_DIRS`)

The directories scanned for the agent class; an installed `mirobody.agents`
entry point is looked at first, then these, and the first class with
`generate_response` wins. This is how a deployment **replaces** the shipped
agent with its own — see `mirobody/agent/README.md`. There is no second agent
and no switching.

## 🧪 Code Execution (QuickJS)

The agent computes with an **in-process JS/TS interpreter** —
[langchain-quickjs](https://pypi.org/project/langchain-quickjs/)'
`CodeInterpreterMiddleware`, which adds a persistent `eval` REPL tool. No API
key, no network, no external sandbox service, nothing to provision.

Nothing to configure — it is on whenever the `[app]` extra is installed. To
turn it off, block the tool:

```yaml
DISALLOWED_TOOLS:
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

**One key runs every surface, and every decision is in `config.llm.yaml`.**
The key itself goes in `.env` (one of the five below); the YAML only names it.
`MODELS` is one table of entries (alias → `llm_type`, `api_key` name, `base_url`,
`model`, `supports_image` / `supports_pdf` / `json_schema` / `chat` / `embedding`,
`extra_body`); the chat picker lists the entries whose key is present, first one
default. `UTILS_VISION_MODEL` (report photos, scans — entries MUST declare
`supports_image: true`), `UTILS_TEXT_MODEL` (indicator extraction, titles,
summaries) and `UTILS_EMBEDDING_MODEL` each name the entries their surface may
use: a list — the first whose key is present wins, which is how one key runs
everything — or one name, a `provider/model` string, or an inline spec to pin
one. The same-named environment variable overrides the file. Python holds no
model name; `mirobody.utils.config.llm` only reads these. Any one of these keys
is enough:

| Key in `.env` | Chat (picker default) | Vision + text — `UTILS_VISION_MODEL` / `UTILS_TEXT_MODEL` | `UTILS_EMBEDDING_MODEL` |
| --- | --- | --- | --- |
| `OPENROUTER_API_KEY` | `claude-sonnet` (anthropic/claude-sonnet-5) | `openrouter-utils` (google/gemini-3.8-flash) | `openrouter-embed` (qwen/qwen3-embedding-8b) |
| `DASHSCOPE_API_KEY` | `qwen` (qwen3.8-flash) | `qwen-utils` (qwen3.8-flash) | `qwen-embed` (text-embedding-v4) |
| `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) | `gemini-flash` (gemini-3.8-flash) | `gemini-utils` (gemini-3.8-flash) | `gemini-embed` (gemini-embedding-001) |
| `OPENAI_API_KEY` | `openai` (gpt-5.6-terra) | `openai-utils` (gpt-5.6-terra) | `openai-embed` (text-embedding-3-small) |
| `DEEPSEEK_API_KEY` | `deepseek` (deepseek-flash) | `deepseek-utils` (deepseek-flash) | — (lexical search only) |

The names are `MODELS` entries in `config.llm.yaml`; the model ids in
parentheses are what those entries said on 2026-09-10 and live only there. The
`*-utils` entries are multimodal on purpose: the vision surface reads report
photos and scanned pages, and a text-only model there is issue #68.

To change a model, edit the entry (or point the surface's `UTILS_*` key at another
entry, or write `provider/model`); `<PREFIX>_BASE_URL` in `.env` (PREFIX = the api_key
name without `_API_KEY`) redirects every entry reading that key to another
OpenAI-compatible gateway. The per-key model overrides of 1.4.0
(`<PREFIX>_MODEL`, `_VISION_MODEL`, `_EMBEDDING_MODEL`) are retired and warned about
at boot. `mirobody doctor` prints what each surface selects with the current
configuration and names the fix where one has nothing; the server and worker log the
same at boot. Selection happens once per surface; a failed call is reported, never
retried on another entry.

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
| DeepSeek | `DEEPSEEK` | `DEEPSEEK_API_KEY` | `https://api.deepseek.com/v1` |

Any other OpenAI-compatible vendor (Zhipu, Moonshot, a self-hosted vLLM) is a
`MODELS` entry in config.llm.yaml — `llm_type: openai`, its `base_url`, the name of
the `.env` variable holding its secret — rather than an enum member: the enum lists
what the project selects on its own, and it only selects providers whose
defaults it has verified.

```bash
# .env — the key, and nothing else about models
OPENAI_API_KEY=sk-...
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
pip install -e '.[app,test]'
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