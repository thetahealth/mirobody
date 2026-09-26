# Security Policy

Mirobody processes health data. A defect here can expose the most sensitive
record a person has, so we would rather hear about a suspected problem that
turns out to be nothing than not hear about a real one.

## Reporting a vulnerability

**Do not open a public issue for a security report.**

Use one of:

- **GitHub private advisory** — [Report a vulnerability](https://github.com/thetahealth/mirobody/security/advisories/new)
  (preferred: keeps the discussion attached to the repo and lets us credit you
  on the published advisory)
- **Email** — security@thetahealth.ai

Please include enough to reproduce: affected version or commit, configuration,
and the steps. If a proof of concept touches real health data, redact it — a
synthetic reproduction is worth more to us than a real one, and
[mirobody-eval](https://github.com/thetahealth/mirobody-eval) generates
PHI-free health data for exactly this purpose.

### What to expect

| | |
| --- | --- |
| Acknowledgement | within 3 business days |
| Initial assessment | within 10 business days |
| Fix or mitigation plan | communicated with the assessment |
| Public disclosure | coordinated with you, after a fix ships |

We will credit you in the advisory unless you ask us not to.

## Scope

**In scope** — this repository: the engine (`mirobody/`), the HTTP and MCP
server surfaces, the deployment scripts (`deploy.sh`, `compose.yaml`), and the
published `mirobody` package on PyPI.

Of particular interest:

- Authentication and session handling (JWT, OAuth, WebAuthn, email codes)
- Cross-user data access — anything that lets one account read another's
  records, including through care-circle sharing
- The agent tool surface: prompt injection through an uploaded document or a
  vendor payload that reaches a tool call or the virtual filesystem
- Secrets handling: config encryption, key material in logs or error paths
- SSRF or credential leakage in the vendor integrations

**Out of scope** — the hosted products (chat.mirobody.ai,
platform.mirobody.ai): report those to security@thetahealth.ai as well, but
they are not covered by this repository's advisories. Also out of scope:
findings that require an attacker who already has host or database access, and
reports from automated scanners without a demonstrated impact.

## Deploying Mirobody safely

The defaults in this repository are tuned for **local, single-user
evaluation**, not for exposing to a network. Before anything reachable by
others:

- **Set `PRODUCTION: true` in your config — first, before anything else.**
  It is the switch the rest of this list hangs off: the server then refuses
  to start while any demo affordance remains (predefined login codes, any
  `REPLACE_THIS_VALUE_IN_PRODUCTION` placeholder still in place) and ignores
  `SEED_DEMO_DATA`. Environment names carry no behavior — `ENV=prod` alone
  protects nothing, and the server warns if it sees that pattern without
  the switch. Set `BOOTSTRAP_SCHEMA: false` too if you provision the schema
  yourself.
- Replace the demo accounts. `config.yaml` ships two predefined logins
  (`you@mirobody.ai` and `mom@mirobody.ai`, code `111111`) — remove
  `EMAIL_PREDEFINE_CODES` entirely. With `PRODUCTION: true` the server enforces this instead of
  trusting the checklist.
- Generate your own `CONFIG_ENCRYPTION_KEY` and keep `.env` out of version
  control. `deploy.sh` generates one; do not copy a key between environments.
- **The database-content key is `PG_ENCRYPTION_KEY`, a separate value from
  `CONFIG_ENCRYPTION_KEY`.** It backs the Postgres-side `encrypt_content()`
  function (`pgcrypto`'s `encrypt(..., 'aes')`), which covers chat message
  content, uploaded file name/content/extracted text, medication free text and
  the user profile's stored markdown. Indicator values in `th_series_data` are
  not covered. Generate and set `PG_ENCRYPTION_KEY` too; `config.yaml` ships it
  under the same `REPLACE_THIS_VALUE_IN_PRODUCTION` placeholder.
- Restrict CORS to the origins you actually serve.
- Terminate TLS in front of the service. Set `MCP_PUBLIC_URL` to an HTTPS URL —
  the MCP surface carries the same health data as the API.
- Do not expose Postgres or Redis. `compose.yaml` publishes them on the host
  for local development convenience.

Self-hosting means the data stays on your infrastructure, and so does the
responsibility for it. Mirobody is Apache-2.0 licensed and provided without
warranty; it is not a medical device and its output is not medical advice.

## What the server calls off your machine

Everything else stays in your Postgres, your Redis and your upload directory.
There is no telemetry and no usage reporting.

| Destination | When | What is sent |
| --- | --- | --- |
| The model behind your key (`config.llm.yaml`) | chat; reading a report photo or PDF; extracting indicators; splitting a journal sentence; embeddings, when an embedding model is configured | the question and the rows the agent reads; the file; the sentence; the text to embed |
| Garmin, Oura, Whoop | only after a person links one | OAuth tokens, and requests for that person's own data |
| Your SMTP server | when `EMAIL_SMTP_*` is configured, to send a sign-in code | the address and the code |
| S3 or Aliyun OSS | only when configured in place of the local disk | uploaded files |
| Google, Apple | only when their sign-in is enabled | token verification against their public keys |

`② Translate` (a name to a code, a unit to UCUM) calls nothing: the vocabulary
ships inside the package.

## Supported versions

Fixes land on `main` and ship in the next release. We do not backport to older
releases — please track the latest.
