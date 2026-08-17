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

**Out of scope** — the hosted products (Theta Wellness, mirobody.ai,
`mcp.thetahealth.ai`): report those to security@thetahealth.ai as well, but
they are not covered by this repository's advisories. Also out of scope:
findings that require an attacker who already has host or database access, and
reports from automated scanners without a demonstrated impact.

## Deploying Mirobody safely

The defaults in this repository are tuned for **local, single-user
evaluation**, not for exposing to a network. Before anything reachable by
others:

- Replace the demo accounts. `config.localdb.yaml` ships predefined logins
  (`demo1@mirobody.ai` / `777777`) — remove `EMAIL_PREDEFINE_CODES` entirely.
- Generate your own `CONFIG_ENCRYPTION_KEY` and keep `.env` out of version
  control. `deploy.sh` generates one; do not copy a key between environments.
- Restrict CORS to the origins you actually serve.
- Terminate TLS in front of the service. Set `MCP_PUBLIC_URL` to an HTTPS URL —
  the MCP surface carries the same health data as the API.
- Do not expose Postgres or Redis. `compose.yaml` publishes them on the host
  for local development convenience.

Self-hosting means the data stays on your infrastructure, and so does the
responsibility for it. Mirobody is Apache-2.0 licensed and provided without
warranty; it is not a medical device and its output is not medical advice.

## Supported versions

Fixes land on `main` and ship in the next release. We do not backport to older
releases — please track the latest.
