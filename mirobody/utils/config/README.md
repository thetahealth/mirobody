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

By default (no `LOG_NAME`), logs go to **console only** (stderr). To enable file logging, set both `LOG_NAME` and `LOG_DIR` in your `config.{env}.yaml`:

```yaml
LOG_NAME: mirobody
LOG_DIR: ./logs
```

Log files are created as `{date}_{name}_{time}.log` in the specified directory. Console logging remains active alongside file logging.

Every handler runs the PHI filter (`mirobody.kernel.ops`): a line carries ids,
counts, durations, status codes and type names, and a message longer than 300
characters is cut. SQL statements are logged at `debug`, as text without their
values.

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
| `PG_TIMEOUT`  | Seconds to wait for a connection (Default:`10`) |

### Temporary state

Short-lived authentication state, counters and provider locks use PostgreSQL.
`CONFIG_ENCRYPTION_KEY` protects values in `th_ephemeral`; keep it stable across
restarts. There is no separate cache service to configure.

## 🤖 Agent Configuration

One agent, one set of keys — no agent-name suffix (before 1.4.0 these were
`PROVIDERS_DEEP`, `PROMPTS_DEEP`, `ALLOWED_TOOLS_DEEP`, `DISALLOWED_TOOLS_DEEP`,
`DEFAULT_PROVIDER_DEEP`), and no "provider" in the model keys: in this project a
provider is a device or data source (`PROVIDER_DIRS`), so 1.4.1 renamed
`PROVIDERS` → `MODELS` and `DEFAULT_PROVIDER` → `DEFAULT_MODEL`
(`EMBEDDING_PROVIDER` was renamed too, and is now removed with the surface it
chose).

**Upgrading from 1.3.x or 1.4.0?** Rename the old spellings: they are no
longer read. Each one a config file or the environment still carries is named
once in the boot log, with its successor, and ignored. The renamed keys were
read under their new names from 1.4.0 to 1.5.4.

The same goes for the keys that were removed rather than renamed:
`PRIVATE_AGENT_DIRS` (use `AGENT_DIRS`), `MCP_RESOURCE_DIRS` (removed with the
MCP `resources` capability), `UTILS_EMBEDDING_MODEL` (nothing embeds since
1.5.0), and `HEARTBEAT_INTERVAL` / `HEARTBEAT_COUNTER_THRESHOLD`:
`SSE_HEARTBEAT_SECONDS` is not those under a new name, since the old pair
multiplied to a first ping at 40 s while the new one fires on silence.

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
    model: anthropic/claude-sonnet-5.5
    supports_image: true
  gemini-flash:
    llm_type: google-genai      # NOT the OpenAI-compatible endpoint — see below
    api_key: GOOGLE_API_KEY
    model: gemini-3.8-flash
    supports_image: true
```

Gemini's chat entry is the one exception to "every vendor is an
OpenAI-compatible endpoint", and the reason is the tool loop: Gemini 3.x
attaches a `thought_signature` to every function call and requires it back
verbatim on the next turn, which the compatibility endpoint has nowhere to
carry. An agent on that path cannot complete a single tool call. Extraction is
unaffected and stays on the compatibility endpoint (`gemini-utils`) — one call,
no second turn, nothing to return.

`DEFAULT_MODEL` names the entry a chat uses when the client sends none;
unset, the default is the **first entry (in file order) whose key is present** —
so the order of `MODELS` is a contract. An entry with `chat: false` is for the
utility surfaces only (file parsing, indicator extraction, titles) and never
reaches the picker.

#### Multimodal capability (`supports_pdf` / `supports_image`)

The agent reads uploaded PDFs and images through its `read_file` tool. Whether a
model can ingest them **natively** (preserving tables, figures, layout, scanned
pages) vs. needing pre-extracted text is auto-detected from LangChain's
normalized [model profile](https://docs.langchain.com/oss/python/langchain/models#model-profiles) —
so for first-party models (Gemini, OpenAI, Anthropic, Vertex) you configure
**nothing**.

**A PDF is a second question, and the transport answers it.** `read_file`
delivers a document inside a tool message, and only the native `anthropic` and
`google-genai` clients accept a non-text block there. Every OpenAI-compatible
endpoint refuses one — `400 tool messages must include a non-empty string
tool_call_id` on OpenRouter, `400 Invalid value: 'file'` on OpenAI — including
for models that genuinely read PDFs over their own vendor API. So on
`llm_type: openai` a PDF is always served as extracted text, whatever the model
profile or `supports_pdf` says. That is a full answer, not a degraded one: the
text path is what qwen and deepseek have always used.

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
| `supports_pdf` | the model reads PDFs — a native file block IF the transport also carries one (above) | PDF served as pre-extracted text (works on any text model) |
| `supports_image` | images sent as a native image block | image served as a native block only if the model's profile already allows it |

Notes:

- `PPT`/`PPTX` always read as extracted text (no provider accepts them as a file block).
- Advanced: a raw `profile: { … }` dict of [`ModelProfile`](https://reference.langchain.com/python/langchain_core/language_models/#langchain_core.language_models.ModelProfile) fields is also honored and overrides the friendly flags. `max_input_tokens` is the one a small local model needs: the agent compacts the conversation at 85% of it and pages a tool result longer than a quarter of it out to its virtual filesystem.

### 2. Tools (`ALLOWED_TOOLS` / `DISALLOWED_TOOLS`)

Which MCP tools the agent may call. A whitelist names the only tools allowed;
a blacklist removes tools from the full set; when both are given the whitelist
wins. Neither set means every tool.

```yaml
ALLOWED_TOOLS:
  - query_health_indicators
  - query_medications

DISALLOWED_TOOLS:
  - query_genetic_data
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

Sensitive keys (ending in `_KEY`, `_PASSWORD`, etc.) can be stored encrypted.

- Use the `CONFIG_ENCRYPTION_KEY` from your `.env` file to encrypt/decrypt values.
- A plaintext secret in a file you name (`-c`, `config.{env}.yaml`, a `.key.yaml`) is encrypted in that file when it loads. The defaults are read, never rewritten: `config.yaml` and the files it INCLUDEs.
- The `config.yaml` in the working directory is the defaults only with its `config.llm.yaml` beside it; otherwise the copy installed with the package is used, so another project's `config.yaml` is neither read nor touched.
- If a value matches `REPLACE_THIS_VALUE_IN_PRODUCTION`, it must be set via environment variable or override file.

### Environment Variables

For sensitive data like API keys, use environment variables:

```bash
export OPENAI_API_KEY="sk-..."
```

Then reference them in YAML or let the system auto-detect them if they match the config key.

## 🏥 LLM Provider Configuration

**One key runs every surface, and every decision is in `config.llm.yaml`.**
The key itself goes in `.env` (one of the six below); the YAML only names it.
`MODELS` is one table of entries (alias → `llm_type`, `api_key` name, `base_url`,
`model`, `supports_image` / `supports_pdf` / `response_format` / `chat`,
`extra_body`); the chat picker lists the entries whose key is present, first one
default. `UTILS_VISION_MODEL` (report photos, scans — entries MUST declare
`supports_image: true`) and `UTILS_TEXT_MODEL` (indicator extraction, titles,
summaries) each name the entries their surface may use: a list — the first whose key is present wins, which is how one key runs
everything — or one name, a `provider/model` string, or an inline spec to pin
one. The same-named environment variable overrides the file. Python holds no
model name; `mirobody.utils.config.llm` only reads these. Any one of these keys
is enough:

| Key in `.env` | Chat (picker default) | Vision + text — `UTILS_VISION_MODEL` / `UTILS_TEXT_MODEL` |
| --- | --- | --- |
| `OPENROUTER_API_KEY` | `claude-sonnet` (anthropic/claude-sonnet-5.5) | `openrouter-utils` (google/gemini-3.8-flash) |
| `DASHSCOPE_API_KEY` | `qwen` (qwen3.8-flash) | `qwen-utils` (qwen3.8-flash) |
| `GOOGLE_API_KEY` (or `GEMINI_API_KEY`) | `gemini-flash` (gemini-3.8-flash) | `gemini-utils` (gemini-3.8-flash) |
| `OPENAI_API_KEY` | `openai` (gpt-6-sol) | `openai-utils` (gpt-6-luna) |
| `ANTHROPIC_API_KEY` | `claude` (claude-sonnet-5-5) | `anthropic-utils` (claude-haiku-4-5) |
| `DEEPSEEK_API_KEY` | `deepseek` (deepseek-flash) | `deepseek-utils` (deepseek-flash) |

The names are `MODELS` entries in `config.llm.yaml`; the model ids in
parentheses are what those entries said on 2026-10-06 and live only there. The
`*-utils` entries are multimodal on purpose: the vision surface reads report
photos and scanned pages, and a text-only model there is issue #68.

Two of the six are not OpenAI-compatible on every surface, and the entries say
so rather than the code guessing. `claude` and `anthropic-utils` are
`llm_type: anthropic` — the vendor's own API, because its OpenAI-compatible
endpoint refuses `response_format: json_object` outright and takes a schema
only in OpenAI strict mode, so extraction there would depend on a model
remembering to answer in JSON. Everything else is `llm_type: openai` against
the vendor's own endpoint. Nothing embeds: `UTILS_EMBEDDING_MODEL` and the
`*-embed` entries went with the semantic tier they served, and a deployment
file still setting the key is told so at boot.

To change a model, edit the entry (or point the surface's `UTILS_*` key at another
entry, or write `provider/model`); `<PREFIX>_BASE_URL` in `.env` (PREFIX = the api_key
name without `_API_KEY`) redirects every entry reading that key to another
OpenAI-compatible gateway. The per-key model overrides of 1.4.0
(`<PREFIX>_MODEL`, `_VISION_MODEL`, `_EMBEDDING_MODEL`) are retired and warned about
at boot. `mirobody doctor` prints what each surface selects with the current
configuration and names the fix where one has nothing; the server and worker log the
same at boot. Selection happens once per surface; a failed call is reported, never
retried on another entry.

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

For this module: config parsing, provider wiring and the one-key defaults. The
agent's PostgreSQL-backed filesystem tools (`write_file` / `read_file` / `ls` /
`glob` / `grep`) have no dedicated suite yet; the bullet list that used to sit
here described upstream tests that never shipped with this repository.

There is no external code-execution sandbox to test: QuickJS (see the section
above) runs in-process, needs no key, and only reshapes already-fetched
numbers into chart JSON — no sandbox lifecycle, no shell, no remote state.