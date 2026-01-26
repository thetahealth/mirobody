# ⚙️ Configuration Guide

Mirobody uses a flexible configuration system that combines YAML files and environment variables.

## 📄 Configuration Files

1.  **`config.yaml`**: The default configuration template. **Do not edit this directly.**
2.  **`config.{env}.yaml`**: Environment-specific overrides (e.g., `config.localdb.yaml`). Use this for your local settings.
3.  **`.env`**: Secrets and environment variables.

### Priority Order
1.  Environment Variables (Highest)
2.  `config.{env}.yaml`
3.  `config.yaml` (Lowest)

## 🏗️ Infrastructure

Core system settings found in `config.yaml`.

### Database (PostgreSQL)
| Key | Description |
|-----|-------------|
| `PG_HOST` | Hostname (e.g., `localhost` or `10.108.0.2`) |
| `PG_PORT` | Port (Default: `5432`) |
| `PG_USER` | Username |
| `PG_PASSWORD` | Password |
| `PG_DBNAME` | Database name |

### Cache (Redis)
| Key | Description |
|-----|-------------|
| `REDIS_HOST` | Hostname |
| `REDIS_PORT` | Port (Default: `6379`) |
| `REDIS_PASSWORD` | Password |

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

### 2. Tools (`ALLOWED_TOOLS_{NAME}`)
Whitelist of tools this agent can use.

```yaml
ALLOWED_TOOLS_DEEP:
- web_search
- calculator
```

### 3. Prompts (`PROMPTS_{NAME}`)
Path to Jinja2 template files used for system prompts.

```yaml
PROMPTS_DEEP:
- mirobody/pub/agents/deep/prompts/default.jinja
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
