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
- Generate your own `CONFIG_ENCRYPTION_KEY` and `JWT_KEY`, and keep `.env` out
  of version control. `deploy.sh` generates both; do not copy a key between
  environments. Without `PRODUCTION: true`, a server with the placeholder
  `JWT_KEY` makes one up for the run, wherever it listens, so sessions end at
  restart. A loopback server used to keep the placeholder, but a reverse proxy
  on the same machine puts it on the internet.
- **The database-content key is `PG_ENCRYPTION_KEY`, a separate value from
  `CONFIG_ENCRYPTION_KEY`.** It backs the Postgres-side `encrypt_content()`
  function (`pgcrypto`'s `encrypt(..., 'aes')`), which covers chat message
  content, uploaded file name/content/extracted text, medication free text and
  the user profile's stored markdown. Readings in `th_observation` (its
  `note_text` aside) and genotypes in `th_genotype` are not covered. Generate
  and set `PG_ENCRYPTION_KEY` too; `config.yaml` ships it under the same
  `REPLACE_THIS_VALUE_IN_PRODUCTION` placeholder. A stack first deployed with
  1.5.2's `deploy.sh` encrypted with that placeholder, and upgrading keeps it:
  a new key would leave those fields unreadable, and there is no command yet
  that re-encrypts them (`docs/roadmap.md`). Treat such a deployment as a
  local one.
- **`SETUP_TOKEN` guards the setup page, which decides where health data is
  sent** (a vendor key, or the address of a model server). `deploy.sh`
  generates it into `.env`; without it the server makes one per run. While no
  model is set up, the token alone saves a choice, and the server prints the
  link with it at startup. Once a model is set up, a change also needs a
  signed-in session (with its second factor when MFA is on), and the link is
  no longer printed. Keep the token as secret as `.env`, and set the model in
  `.env` instead if you would rather not have the page at all: a name set
  there is never replaced from the page.
- Leave `COLLECT_WEBHOOK_SECRET` unset unless a vendor pushes to you. The
  `/api/v1/pulse/{platform}/.../webhook` routes answer 404 without it. With it,
  give the vendor the URL with `?secret=<value>` or send `X-Webhook-Secret`.
- Configure mail (`EMAIL_SMTP_*`) before anyone else registers. With mail,
  registering takes a code sent to the address. Without it an address is
  only a username, held by whoever registers it first. Once mail is on, the
  owner's first code sign-in clears a password nobody proved and ends the
  sessions minted with it.
- Restrict CORS to the origins you actually serve.
- A device vendor's OAuth callback sends the browser back only to this
  server's origin, the CORS origin above, or what `OAUTH_RETURN_ORIGINS`
  lists (an origin, or an app's own scheme such as `theta:`). Anything else
  gets the completion page instead of a redirect.
- Connecting a device ties the vendor's consent to the account that started
  the link, not to the browser that finishes it. Someone with an account on
  the same server could start a link and send its consent page to another
  user, whose wearable data would then reach the first account. On a server
  shared beyond people you trust, connect devices only from links you started
  yourself; `docs/roadmap.md` has the fix and why it is not in yet.
- Set `WEBAUTHN_RP_ID` to your domain to offer passkeys. An account that
  turns MFA on then needs its passkey for every request, a file link
  (`?access_token=`) and the upload socket included.
- Terminate TLS in front of the service. Set `MCP_PUBLIC_URL` to an HTTPS URL —
  the MCP surface carries the same health data as the API. Set HSTS there: every
  response already carries `X-Content-Type-Options: nosniff`,
  `X-Frame-Options: SAMEORIGIN` and `Referrer-Policy: same-origin`, and with
  `PRODUCTION: true` the API docs (`/docs`, `/redoc`, `/openapi.json`) are off.
- Do not expose Postgres. `compose.yaml` binds its host port to `127.0.0.1`
  for local development; restrict access to that port on shared hosts.

Self-hosting means the data stays on your infrastructure, and so does the
responsibility for it. Mirobody is Apache-2.0 licensed and provided without
warranty; it is not a medical device and its output is not medical advice.

## What the server calls off your machine

Everything else stays in your Postgres and your upload directory.
There is no telemetry and no usage reporting.

| Destination | When | What is sent |
| --- | --- | --- |
| The model behind your key (`config.llm.yaml`) | chat; reading a report photo or PDF; extracting indicators; splitting a journal sentence | the question and the rows the agent reads; the file; the sentence |
| Your own model server (`LOCAL_BASE_URL`, `LOCAL_OCR_BASE_URL`; llama.cpp's `llama-server` as shipped), when set instead of a key | the same | the same, to that server: with it on this machine, none of it leaves |
| Hugging Face, or the mirror `HF_ENDPOINT` names | the first time the model server is asked for a model (the compose `llama` service, or `llama-server` on the host; this request is the model server's, not Mirobody's) | a download request for the model's files; nothing about you |
| Garmin, Oura, Whoop | only after a person links one | OAuth tokens, and requests for that person's own data |
| Your SMTP server | when `EMAIL_SMTP_*` is configured, to send a sign-in code | the address and the code |
| S3 or Aliyun OSS | only when configured in place of the local disk | uploaded files |

`② Translate` (a name to a code, a unit to UCUM) calls nothing: the vocabulary
ships inside the package.

## Supported versions

Fixes land on `main` and ship in the next release. We do not backport to older
releases — please track the latest.
