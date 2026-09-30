---
name: mirobody
description: Self-host Mirobody, the open-source health data engine, and use it from an agent. Use when the user wants to run a personal or family health record on their own machine, bring lab reports and wearables (Apple Health, Garmin, Oura, WHOOP) into one place an AI can read, expose their health data to Claude Desktop, Cursor or another agent over MCP, or operate a stack that is already running (model key, upgrade, backup, ports). Docker is the only requirement; one model API key runs every surface.
license: Apache-2.0
metadata:
  author: thetahealth
  homepage: https://github.com/thetahealth/mirobody
---

# Mirobody

Mirobody reads a person's health data from any source (lab PDFs, report
photos, spreadsheets, device exports, genotype files, a typed sentence),
settles every value onto one standard code, and answers questions over that
record with each number traced to the file it came from. It runs in Docker on
the user's machine, on a model key they choose. Nothing leaves the machine
except calls to that model and to a wearable vendor once one is linked.

This skill gets a stack running and connects an agent to it. The sibling
`translate-health-data` skill covers the library without Docker: it turns raw
files into coded rows and explains a report only from resolved evidence.

## Agent contract

- **The model key comes from the user.** Ask for it, or for which provider they
  use. Never invent one, never print one back, never paste it into a chat
  reply. Write it into `.env` and nowhere else.
- **Ask before anything that deletes data**: `docker compose down -v`,
  `docker volume rm`, removing the checkout. `docker compose down` without
  `-v` keeps the database and uploads, and needs no confirmation.
- **Do not expose port 18060 beyond the machine** without reading
  `SECURITY.md` with the user first. The default deployment is for one
  machine or one home network.
- **Never commit `.env`.** `deploy.sh` creates it with mode 0600 and the
  repository ignores it; leave both as they are.
- **Do not skip a verification step** because the previous one printed nothing
  alarming. Each step below checks a different thing.

## Preflight

```bash
docker --version && docker compose version    # Docker 24+ with Compose v2
git --version
```

The image is about 460 MB to download, and the stack uses ports `18060`
(app) and `18062` (Postgres, bound to localhost only). If either port is
taken, set `MIROBODY_HOST_PORT` or `PG_HOST_PORT` in `.env` before step 2.
No Python, Node.js, GPU or Git LFS is needed on the host.

## Install: three commands

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh
echo 'OPENROUTER_API_KEY=sk-or-...' >> .env && docker compose up -d
```

What `deploy.sh` does, so you can explain it: writes the five secrets the
stack needs into `.env` (generated with `openssl rand`), pulls the published
image `thetahealth4mirobody/mirobody`, starts Postgres, the server and the
worker with `docker compose up -d --wait`, and prints the demo sign-in. If
Docker Hub is unreachable from the daemon it switches to a mirror on its own;
if the image cannot be pulled at all it builds one from the checkout, which
takes several minutes and needs `git lfs pull` first.

The third command is the only one that needs the user. Any ONE of these keys
runs every surface (extraction, chat, embeddings):

| Provider | Variable |
| --- | --- |
| OpenRouter | `OPENROUTER_API_KEY` |
| OpenAI | `OPENAI_API_KEY` |
| Google Gemini | `GOOGLE_API_KEY` |
| Anthropic | `ANTHROPIC_API_KEY` |
| DeepSeek | `DEEPSEEK_API_KEY` |
| DashScope | `DASHSCOPE_API_KEY` |

Any OpenAI-compatible gateway works through `<PROVIDER>_BASE_URL` and
`<PROVIDER>_MODEL`; the model must be multimodal, because report photos go
to it. `config.llm.yaml` names which model each key selects.

**After editing `.env`, run `docker compose up -d`, not `restart`.** Compose
reads the env file when it creates a container; `restart` keeps the old
environment and the key is silently ignored.

## Verify, in this order

```bash
docker compose ps
```

Expected: `pg` healthy, `mirobody` healthy, `mirobody_worker` running,
`mirobody_init` exited with code 0 (it runs once to hand the upload volume to
the app's user).

```bash
curl -fsS http://localhost:18060/api/health
```

Expected: JSON with `"service":"mirobody"` and the version. Anything else
means the server is not up; read `docker compose logs mirobody`.

```bash
docker compose exec mirobody mirobody doctor
```

Prints one row per surface (chat, extraction, embeddings) with the provider
each one selected. **Exit status 1 with no key set is expected before step 3**
and means "no provider anywhere"; after the key is in and `up -d` has run, it
exits 0 and every row names a provider. A row left blank with a key present
means that key's provider cannot serve that surface; the table says which.

## First use

Open `http://localhost:18060` and sign in on the Email code tab as
`you@mirobody.ai` with code `111111`. `SEED_DEMO_DATA` is on by default, so
two accounts already hold data: this one, and `mom@mirobody.ai`, who shares
her record view-only. Set `SEED_DEMO_DATA=false` in `.env` before the first
start for an empty deployment.

- **Data page**: drop a file. `demo/upload/` in the checkout holds four the
  seed leaves out (a lab PDF, a report photo, a spreadsheet, a CSV). Each
  analyte comes out with a value, a unit and a code, linked to the page it was
  read from.
- **Ask page**: a question such as "How has my cholesterol moved?" finds every
  file that carries it, whatever the lab called it, and names the file behind
  each number.
- **Journal**: a typed sentence (`headache since last night, BP 150/95`) is
  split by the model and coded from the vocabulary.

## Connect an agent over MCP

Every tool the built-in agent has is also served at `/mcp`, gated per user.
In the web app, **Settings** issues a personal MCP link; paste it into Claude
Desktop, Cursor, or any MCP client. The tool list is data-gated: an account
with no lab data does not see the lab-query tool, so an empty list is a
signal to upload something, not a fault.

The library's offline tools also run without the server, over stdio and
with no key, as six MCP tools (`resolve_indicator`, `normalize_unit`,
`convert_unit`, `standardize_reading`, `standardize_complaint`,
`standardize_report`):

```bash
uvx mirobody mcp
```

## Operate

| Task | Command |
| --- | --- |
| Follow the server log | `docker compose logs -f mirobody` |
| Follow the extraction worker | `docker compose logs -f mirobody_worker` |
| Stop, keep all data | `docker compose down` |
| Start again | `docker compose up -d` |
| Upgrade to a new release | `git pull && ./deploy.sh` |
| Change the app port | `MIROBODY_HOST_PORT=8080` in `.env`, then `docker compose up -d` |
| Back up and restore | `docs/backup-restore.md` in the checkout |

Wearables connect from inside the app. Garmin, Oura and WHOOP are OAuth
clients of their vendor, so each needs credentials from that vendor's
developer programme; `docs/provider-setup.md` walks through it. Apple has no
API to pull from: an `export.zip` from the Health app is read on the host by
`mirobody import apple`, and a phone app pushes into a running stack through
`POST /apple/health` (`docs/apple-health.md`). The Data page does not take a
Health export.

## When something is wrong

| Symptom | Cause and fix |
| --- | --- |
| `deploy.sh` says a port is in use | Set `MIROBODY_HOST_PORT` or `PG_HOST_PORT` in `.env`, run it again |
| `doctor` exits 1 after the key was added | `.env` was edited but `docker compose up -d` was not run, or the line has a typo; `grep -o '^[A-Z_]*_API_KEY=' .env` shows the name without printing the value |
| The server log says `config.localdb.yaml` is a directory | Docker Desktop mounted the overlay from a path it does not share as a file (a checkout under `/tmp` on macOS does this); the stack runs on defaults. Move the checkout under the home directory and run `./deploy.sh` again |
| `deploy.sh` says `compose.override.yaml` is from 1.5.2 | The message names the two commands; it is an upgrade from an older layout |
| Pull fails and the build says the bundle is an LFS pointer | `git lfs install && git lfs pull`, then `./deploy.sh` again |
| A report photo is not extracted | The selected model is not multimodal; `doctor` shows which model extraction chose |

## Privacy, stated once

The record lives in the stack's own Postgres volume. The only outbound calls
are to the model behind the key in `.env` and to a wearable vendor the user
linked. Name to code and unit normalization run offline inside the image.
Encryption at rest does not yet cover every field; `SECURITY.md` says what
that means before the stack reaches a network the user does not control.
