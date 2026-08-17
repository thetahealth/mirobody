<div align="center">

# 🚀 Mirobody

**The AI-native health data engine — collect, standardize, and reason over labs, wearables & genomics.**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Benchmarks](https://img.shields.io/badge/%F0%9F%A4%97_Benchmarks-4k%2B_downloads_each-FFD21E.svg)](https://huggingface.co/healthmemoryarena)
[![arXiv](https://img.shields.io/badge/arXiv-2604.02834-b31b1b.svg)](https://arxiv.org/abs/2604.02834)
[![Live](https://img.shields.io/badge/Live-mirobody.ai-black)](https://mirobody.ai)
[![Theta](https://img.shields.io/badge/Powers-Theta%20Wellness-green)](https://www.thetahealth.ai/)

*Blood tests, wearables, genomics, imaging — all fragmented, all incompatible.
Before AI can understand your health, someone has to unify these signals into a
single standard AI can actually read. That is what this engine does.*

<img src="docs/images/where-your-data-comes-from.svg" alt="From wearables to food photos — one standard format, ready for AI." width="920">

</div>

The engine does three things, and the codebase (and [Contributing](#-contributing)) is organized around exactly these three verbs:

| Verb                 | What it means                                                                                                     | Where                                                   |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| **① Collect** | Pull signals in: 4 production providers · 8 file formats · Apple Health                                             | [`pulse/`](mirobody/pulse/) |
| **② Sort**    | Standardize: resolve any reading to canonical codes (LOINC · SNOMED CT · RxNorm), normalize units, land as FHIR | [`indicator/`](mirobody/indicator/)                    |
| **③ Answer**  | Reason: agents read the*original documents* through a virtual filesystem and answer with charts & citations     | [`agent/`](mirobody/agent/)                  |

---

## ⚡ Try it in 60 seconds

**No install** — point any MCP client (Claude Desktop, Cursor, Cherry Studio) at the hosted server and talk to a live health-data engine:

```json
{ "mcpServers": { "mirobody": { "url": "https://mcp.thetahealth.ai/mcp" } } }
```

#### Protocol: MCP **2026-07-28** — the current revision

We implement the [2026-07-28 spec](https://modelcontextprotocol.io/specification/2026-07-28/), the *stateless* revision that removed the `initialize`/`initialized` handshake and `Mcp-Session-Id` outright:

- **Per-request `_meta`** — each request carries its own protocol version and client capabilities, so any request can land on any instance behind a load balancer with no shared session state.
- **`resultType` on every result** — the field that makes polymorphic results (`complete` / `input_required`) possible.
- **`server/discover`** — optional stateless capability discovery; no handshake needed before the first real call.
- **Deterministic `tools/list` ordering** — a reconnect doesn't reshuffle the list and invalidate your client's prompt cache.

And it negotiates **down**: a client pinned to an older revision is answered in *its* revision, all the way back to `2024-11-05`, for the length of the deprecation offramp.

Two of its tools need no account at all, because ② Sort is pure terminology:

| Tool | What it does | Needs |
| --- | --- | --- |
| `resolve_indicator` | any-language indicator name → canonical LOINC | nothing — offline, no user data |
| `normalize_unit` | free-text unit → canonical UCUM + comparability family | nothing — offline, no user data |
| `query_health_indicators` | your own records — search, read and aggregate in **one** call; every result carries its LOINC identity | your account |
| `get_genetic_data` | your variants by rsid | your account |

**As a library** — the engine is a pip install, and ② Sort needs nothing but the package:

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" "血红蛋白" "ヘモグロビン"
```

```python
from mirobody.engine import resolve
resolve("血红蛋白").loinc   # -> '718-7'   offline: no key, no config, no network
```

Runnable walkthroughs: [`examples/`](examples/README.md) — five scripts from offline resolution to a full-server preflight, each verified to run.

**Self-hosted** — your data never leaves your machine: see [Quick Start](#-quick-start). Your own deployment serves the same MCP surface at `/mcp`.

---

## ① Collect — every signal, one intake

- **Production device providers behind one plugin contract** — battle-tested providers for **Garmin, Oura, Whoop** plus [300+ devices](mirobody/pulse/providers/README.md) via the pulse platform, and [Apple Health](mirobody/pulse/apple/README.md) import with CDA processing. A provider is a directory: drop it in, and discovery, OAuth and pull scheduling are wired for you.
- **8 file formats parsed with AI** — PDF lab reports, Excel, CSV, images, audio, archives, plain text, and **genetic exports (WeGene)**; LLM-powered indicator extraction ([`pulse/file_parser/`](mirobody/pulse/file_parser/), 13k lines).
- Ingest pipeline: staged intake → validate → normalize → daily rollups → [AI insights](mirobody/pulse/insight/) that feed back into the record — closing the loop.

## ② Sort — one standard AI can actually read

The part none of the adjacent open-source projects have — a **semantic standardization layer**, not a lookup table:

- **Concept graph**: 440,961 nodes · 22,044,110 cross-vocabulary edges · **595,746 source ids** distilled into canonical concepts (LOINC · SNOMED CT · RxNorm bridges), shipped via Git LFS ([`indicator/`](mirobody/indicator/README.md)).
- **Embedding-based resolution**: free-text indicator names → canonical codes, with **50,240 multilingual aliases** (中文 22,578 · 日本語 16,809 · +6 languages) — `血红蛋白`, `ヘモグロビン` and `hemoglobin` all land on LOINC 718-7.
- **We measure that claim instead of asserting it.** [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) scores the offline resolver against the panels a physical actually orders — lipid, CBC, metabolic, liver, thyroid, hormones, tumour markers, urinalysis, vitals — written the way a report prints them, in English, 中文 and 日本語. **116/116 today; it scored 32/94 the day it was written.** It grades *clinical* correctness, not resolution rate: answering `血红蛋白` with the code for HbA1c is scored as a failure, and `血圧` (a panel, not an observation) is required to resolve to *nothing*, because a confident wrong code is worse than an honest miss.
- **Unit normalization** to UCUM families (~310), 316 standard pulse indicators, FHIR R4 output.
- Taxonomy of 25 clinical categories (Vital signs, Lab & Clinical, Body measures, …).

## ③ Answer — agents that read the originals

There are **two ways to consume this layer**, and an agent for each — the difference is *who runs the tool loop*:

| | **DeepAgent** — you run the engine | **BaseAgent** — your model consumes ours |
| --- | --- | --- |
| Tool loop runs | here, in your deployment | in the **LLM provider**, against `/mcp` over HTTP |
| For | self-hosting the whole thing | Claude Desktop · Cursor · ChatGPT Apps · any MCP client |
| Extras | virtual filesystem, QuickJS, Agent Skills, charts | whatever the MCP tool surface exposes — nothing hidden |

- **DeepAgent** — the primary agent, on [deepagents](https://github.com/langchain-ai/deepagents) 0.7 / LangChain 1.3. Multi-provider (OpenAI, Gemini, Anthropic, OpenRouter, any OpenAI-compatible endpoint); a **PostgreSQL-backed virtual filesystem** (`/uploads`, `/library`, `/memories`, `/charts`, `/skills`) lets the model `read_file` your *original* PDF — multimodally — instead of a lossy extraction; in-process JS interpreter (QuickJS) for real computation; per-turn model-call budget with graceful stop.
- **BaseAgent** — no LangChain, on purpose. It hands the MCP server to the provider (OpenAI Responses `mcp_server`, Gemini Interactions) and streams the result. That makes it our own rehearsal of the third-party experience: **anything BaseAgent can't do unaided is something an outside MCP client can't do either.** It does not chart — the consuming client brings its own visualization.
- **MCP server built in** ([`mcp/`](mirobody/mcp/)) — every tool doubles as an MCP tool over HTTP; works as MCP client *and* OAuth-enabled MCP server. **Agent Skills** (SKILL.md) served via deepagents' native SkillsMiddleware from [`mirobody/agent/skills/`](mirobody/agent/skills/).
- Care-circle sharing with per-person consent:

<div align="center"><img src="docs/images/your-care-circle.svg" alt="Your care circle — invite the people you trust by email; you stay in control: remove a member or unshare anytime, and health sharing stays off until you allow it." width="920"></div>

---

## 🏗️ Architecture

The engine is three verbs — **① Collect → ② Sort → ③ Answer** — and the package
layout says the same thing.

```
mirobody/
│
│  ── the ENGINE (pip install mirobody · no agent framework, machine-enforced) ──
│
├── engine.py            ②  The front door: resolve() offline, parse_file() one-LLM-call
├── cli.py                   mirobody parse | resolve | serve | worker
├── pulse/               ①  COLLECT — every signal, one intake
│   ├── providers/           production device providers (Garmin/Oura/Whoop, 300+ devices)
│   ├── apple/               Apple Health import (zip + CDA)
│   ├── file_parser/         8 file formats → indicators via LLM extraction (needs DB)
│   ├── ingest/              StandardPulseData: the universal exchange format that
│   │                        every source above converges on (was `data_upload/`)
│   └── core/                domain models, daily rollups, insights (needs DB)
├── indicator/           ②  SORT — one standard AI can actually read
│   └── fhir/                concept graph · embedding resolution · units → UCUM · taxonomy
├── res/                     the shipped data: LOINC/SNOMED bundles (Git LFS, see
│                            LICENSE-3RD-PARTY + *.NOTICE) · resolver_overrides.tsv · sql/
│
│  ── shared infrastructure: not a fourth verb, used BY the three ──────────
│
├── mcp/                     MCP server: every tool doubles as an MCP tool over HTTP
├── task/                    background workers (indicator sync, profile refresh)
│                              ← used by pulse, server
├── user/                    accounts, auth, care-circle consent
│                              ← used by agent, mcp, pulse, server
├── utils/                   config (encrypted YAML), direct LLM SDK access, db,
│                            locales ← used by EVERY other package. Keep it a
│                            leaf: it must import nothing above itself
│
│  ── the AGENT LAYER (pip install 'mirobody[agents]' · LangChain lives ONLY here) ──
│
├── agent/               ③  ANSWER — one roof for everything conversational
│   ├── deep_agent.py        DeepAgent — model 1: YOU run the engine. deepagents/
│   │                        LangChain, PG virtual fs, QuickJS, Agent Skills
│   ├── base_agent.py        BaseAgent — model 2: someone else's model consumes us
│   │                        over MCP. Hands /mcp to the provider, which drives
│   │                        the tool loop. No LangChain, on purpose.
│   ├── base/ · deep/        the two agents' internals (backends, middleware)
│   ├── chat/                sessions · messages · history replay · sharing · profile
│   ├── tools/               the MCP tool surface (MCP_TOOL_DIRS): terminology
│   │                        (② Sort, offline), health records, genetics
│   ├── skills/              Agent Skills (SKILL.md) — deepagents SkillsMiddleware
│   ├── prompts/             Jinja system prompts
│   └── resources/           MCP UI widgets for ChatGPT Apps (see its README)
└── server/                  HTTP lifecycle + the FastAPI routers (server/routers/)

frontend/                    the built-in web client — OUTSIDE the package on
                             purpose: wheels ship the engine, not 8MB of JS.
                             Served when `frontend/` exists next to the process
                             (Docker/source); pip installs pair with mirobody.ai.
```

### What you get at each install size

| Install | What works | Footprint |
| --- | --- | --- |
| *the wheel + numpy only* | `from mirobody.engine import resolve` — the offline resolver | **77 MB**, 2 packages |
| `pip install mirobody` | + `mirobody parse` (one LLM key) · file parsing (PDF/Excel/audio) · FHIR output | 233 MB, 90 packages |
| `pip install 'mirobody[server]'` | + the HTTP API and MCP endpoint | needs Postgres + Redis |
| `pip install 'mirobody[agents]'` | + DeepAgent/BaseAgent and `mirobody serve` (includes `[server]`) | + the LangChain stack |
| `pip install 'mirobody[indicator-build]'` | rebuilding the terminology bundles themselves | needs LOINC/UMLS sources |

Sizes measured on a clean venv, not estimated. **51 MB of the 77 MB floor is the
shipped LOINC/SNOMED data** — that is the resolver, not overhead, and it is what
makes standardization work with the network unplugged.

The database driver, HTTP server, S3 and email clients used to be in the default
install; they moved to `[server]`, which is what `[agents]` pulls in. If you only
want the engine as a library, you no longer pay for a Postgres driver.

### The one rule, machine-enforced

**The engine must import with no agent framework installed.** `langchain*`,
`deepagents` and `langgraph` are allowed only under `agent/` and `server/` —
the same layering langchain itself uses for `langchain-core`. Two
`[tool.importlinter.contracts]` in `pyproject.toml` fail the build on
violation, function-local imports included:

```bash
pip install -e '.[test]' && lint-imports
```

That is what lets `mirobody.engine` resolve an indicator with numpy as the only
third-party package present. `utils/` is deliberately a leaf — a top-level
`from sqlalchemy import text` in `utils/db.py` once made the whole database
stack a hard requirement of a function that never opens a connection.
`utils/`, `user/` and `task/` are not verbs; they are the infrastructure the
three stand on. One deliberate seam crosses the boundary today, recorded with
its exit plan in `pyproject.toml`'s `ignore_imports` and in
[docs/roadmap.md](docs/roadmap.md).

### Two ways to consume ③ Answer

The distinction most often misread as duplication. There are two agents because
there are two ways to use this project, and each is the reference implementation
of one:

| | **DeepAgent** | **BaseAgent** |
| --- | --- | --- |
| Who runs the tool loop | us — LangChain / deepagents, in this process | the LLM **provider**, against our `/mcp` over HTTP |
| Typical user | you self-host the whole engine (`pip install 'mirobody[agents]'`, Docker, mirobody.ai) | a third party points Claude Desktop / Cursor / a ChatGPT App at our MCP endpoint |
| Gets the virtual filesystem, QuickJS, Skills | yes | no — only what the MCP tool surface exposes |
| Why we keep it | maximum capability, fully inspectable, self-hosted | it is the live rehearsal of the third-party experience: if a tool description is too thin for a model to use unaided, it fails here first |

**A capability that exists only inside DeepAgent is one external MCP clients do
not have.** So "make the MCP offering better" is work in
[`agent/tools/`](mirobody/agent/tools/) and the tool descriptions — not in the
DeepAgent middleware stack.

### Data flow, end to end

```
vendor APIs / files / Apple Health          ① pulse
        └─> StandardPulseData ─> validate ─> normalize ─> daily rollups
                 └─> indicator names ─> ② indicator: canonical codes (LOINC·SNOMED·RxNorm)
                          └─> FHIR R4 rows in Postgres
                                   └─> ③ agent: read ORIGINAL documents through the
                                       virtual fs, compute, chart, answer — and insights
                                       feed back into the record, closing the loop
```

---

## 📊 Benchmarks — we don't say "trust us", we ship the eval

Our health-AI benchmarks are the **most-downloaded in their category on Hugging Face** (4,000+ each):

| Benchmark                                                                       | What it measures                                                                                                                                                | Downloads |
| ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| [ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench)         | Event-driven longitudinal health agents — 100 synthetic users, 10,000 queries, programmatic ground truth ([arXiv:2604.02834](https://arxiv.org/abs/2604.02834)) | 4,800+    |
| [MedHall-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHall-Bench) | Medical hallucination                                                                                                                                           | 4,500+    |
| [MedHarm-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHarm-Bench) | Harmful medical advice                                                                                                                                          | 4,300+    |

Reproduce any of them with one command via **[mirobody-eval](https://github.com/thetahealth/mirobody-eval)** — our open evaluation framework. Its generator also produces the synthetic (PHI-free) health data used in demos and tests.

We hold the engine itself to the same standard. **Resolver coverage** — can ② Sort name the everyday tests on a real lab report? — runs in this repo, offline, in under a second:

```bash
pytest mirobody/test_engine_coverage.py -s
#   offline resolver coverage: 116/116 = 100%
```

It started at **32/94** — the benchmark has since grown to 116 cases. The gap was not the concept graph; it was that the index is built from LOINC long names, so it knew `LDL-C` but not `LDL cholesterol`, knew 葡萄糖 but not `血糖`, and answered `血红蛋白` with the code for HbA1c. Both classes of failure are one TSV row each to fix — [see Contributing](#-contributing).

---

## 🏥 Powers Theta

> *"Theta collects it; Mirobody sorts every signal precisely into its own bucket, turning raw noise into a structured, AI-readable standard."*

[**Theta Wellness**](https://www.thetahealth.ai/) ([App Store](https://apps.apple.com/us/app/theta-wellness/id6739960903) · [Google Play](https://play.google.com/store/apps/details?id=com.thetaai.theta)) is the HIPAA-compliant consumer app built on this engine — the same architecture that powers a production medical-grade health agent can power your product. We focus on health; swap the files in [`mirobody/agent/tools/`](mirobody/agent/tools/) to build your own vertical.

---

## ⚡ Quick Start

### 📋 Prerequisites

- **Docker & Docker Compose**: Ensure these are installed and running.
- **Git**: To clone the repository.
- **Git LFS**: Required to pull binary data files (e.g. `fhir_concept_graph.bin`). Install via `apt install git-lfs` (Linux) or `brew install git-lfs` (macOS). Git for Windows includes it by default. Run `git lfs install` once after installing.

### 1. Deploy via Docker

```bash
git clone https://github.com/thetahealth/mirobody.git
cd mirobody
./deploy.sh
```

This script will:

- Generate a secure `.env` file.
- Create a default configuration file (`config.localdb.yaml`).
- Build the Docker image.
- Start the services (Postgres, Redis, Mirobody).

Then open `http://localhost:18080` in your web browser.

> **📝 Configuration Notes:**
>
> - A `.env` file will be created automatically with two variables:
>   - `ENV`: The name of the current config.
>   - `CONFIG_ENCRYPTION_KEY`: A 32-byte string used for encrypting sensitive variables.
> - The default configuration template is [`config.yaml`](config.yaml).
>   - **👉 See [CONFIG](mirobody/utils/config/README.md) for a detailed configuration guide.**
>   - **👉 See [DATABASE](mirobody/schema/README.md) for the schema, what it contains, and how it is applied.**
> - **Tip**: Check `EMAIL_PREDEFINE_CODES` for predefined email accounts and verification codes used for user login.
> - **🌍 Timezone**: Set `DEFAULT_TIMEZONE` in `config.{env}.yaml` to match your region (e.g., `America/New_York`, `Europe/London`, `Asia/Tokyo`). Defaults to `America/Los_Angeles`. See [CONFIG](mirobody/utils/config/README.md#-timezone) for details.
> - **LLM Setup**: `OPENROUTER_API_KEY` is required for the Deep agent.
> - **Auth Setup**: To enable **Google/Apple OAuth** or **Email Verification**, set the respective variables in `config.{env}.yaml`.
> - All API keys will be encrypted automatically once Mirobody loads them using the `CONFIG_ENCRYPTION_KEY` value.

### 🐍 Local Python Development

If you prefer to run the Mirobody agent code locally (for debugging or development) while keeping the database and cache in Docker:

**1. Start Backing Services**

```bash
docker compose up -d pg redis
```

**2. Environment Setup**

Prerequisites:

- **Python**: 3.12 or higher (hard floor — the codebase uses PEP 701 f-strings, so 3.11 cannot even import it)

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Install Python dependencies.
# The bare package is the data ENGINE as a library; running the chat server
# needs the agent layer, which is the [agents] extra:
pip install -e '.[agents]'
# Optional extras:
# pip install -e .[cn]    # China region (Aliyun OSS, Volcengine, Dashscope)
```

**3. Configuration**

```bash
# Create .env
echo "ENV=localdb" > .env
# Generate a random encryption key (optional but recommended)
echo "CONFIG_ENCRYPTION_KEY=$(openssl rand -hex 32)" >> .env
```

Then create `config.localdb.yaml` next to `config.yaml` (this file is **not** in
the repo — `deploy.sh` generates one for Docker runs, but on the local path you
create it yourself). The defaults in `config.yaml` point at Docker-internal IPs,
so a local process **must** override the database hosts:

```yaml
# Point at the pg/redis containers' published ports (see compose.yaml)
PG_HOST: localhost
PG_PORT: 18082
REDIS_HOST: localhost
REDIS_PORT: 18089
# If 18080 is taken on your machine, pick another port:
# HTTP_PORT: 28080

# OpenRouter API key (Required for Deep Agent)
OPENROUTER_API_KEY: 'sk-or-...'

# Optional: OpenAI or Google keys
OPENAI_API_KEY: 'sk-...'
GOOGLE_API_KEY: '...'
```

> **Note:** Sensitive keys are automatically encrypted by the system using the `CONFIG_ENCRYPTION_KEY` found in your `.env` file.

**4. Run the Application**

```bash
mirobody serve   # or: python -m mirobody serve
```

The server will start at `http://localhost:18080`.

> **First start takes about a minute**: the server creates the schema and runs
> every SQL file under `mirobody/schema/` before it begins listening. Wait for
> the `SQL files initialization completed` log line — it is not hung.

**The CLI at a glance**

| Command | What it does |
| --- | --- |
| `mirobody parse <file>` | Lab report in, standardized LOINC table out — one LLM key, zero infrastructure |
| `mirobody resolve <terms…>` | Offline indicator-name resolution — no key, no config, no network |
| `mirobody serve` | Run the HTTP server (chat, MCP, API) — needs `[agents]` |
| `mirobody worker` | Run the background task worker (indicator sync, profile refresh) |

### 👤 First Login

Use the pre-configured demo accounts:

- **Email**: `demo1@mirobody.ai`
- **Password**: `777777`

### 2. Extend It — Tools and Skills

Mirobody adopts a **"Tools-First"** philosophy: a tool is a plain Python function, a skill is a plain Markdown file. No registration, no binding logic.

#### 🐍 Python Tools

Tool modules are auto-discovered from the directories in `MCP_TOOL_DIRS` (default: [`mirobody/agent/tools/`](mirobody/agent/tools/) — add your own directory in `config.{env}.yaml`). Every function doubles as a REST tool **and** an MCP tool, local or remote HTTP. **👉 See [TOOLS](mirobody/agent/tools/README.md) for the developer guide.**

```python
# your_tools_dir/my_tools.py
def analyze_data(input_data: str) -> dict:
    """
    Description of this tool.

    Args:
        input_data: Description of this argument.

    Returns:
        Description of the return value.
    """
    return {"result": "analysis"}
```

> **🔐 JWT authentication**: if your tool needs the calling user, take a
> **`user_info: Dict[str, Any]`** parameter and leave it out of the docstring's
> `Args:`. The server fills it from the verified JWT and hides it from the tool
> schema, so the model never sees or supplies it:
>
> ```python
> async def my_tool(self, query: str, user_info: Dict[str, Any]) -> dict:
>     user_id = user_info.get("user_id")     # verified, not model-supplied
> ```
>
> **Do not take a `user_id` parameter.** This note used to say exactly that, and
> it produces a tool that is both broken and unsafe: `user_id` is not the
> injection hook, so the server does not fill it, and it stays visible in the
> tool's JSON Schema — meaning the model supplies it, and any MCP client can ask
> for another person's data by passing a different value. Tool loading now warns
> loudly when it sees this shape.

#### 📖 Agent Skills

Mirobody supports **[Agent Skills](https://agentskills.io/)** through [deepagents](https://docs.langchain.com/oss/python/deepagents/overview)' native `SkillsMiddleware` — the same machinery LangChain's own deep agents use, not a bespoke loader:

- A skill is a directory with a `SKILL.md` (YAML frontmatter: `name` + `description`; body: the instructions). Nothing else is required.
- Skill directories come from `SKILL_DIRS` in config; the packaged default [`mirobody/agent/skills/`](mirobody/agent/skills/) ships with the wheel and is mounted read-only at `/skills/` in the agent's virtual filesystem.
- **Progressive disclosure**: the agent sees every skill's frontmatter at startup, and reads the full body through the `/skills/` mount only when the task calls for it — rich capability, minimal standing context.

The shipped [`lab-report-walkthrough`](mirobody/agent/skills/lab-report-walkthrough/SKILL.md) skill is the reference: it encodes the read-the-original / flag-against-printed-ranges / no-diagnosis workflow and is exactly the shape to copy for your own.

```
mirobody/agent/skills/
└── lab-report-walkthrough/
    └── SKILL.md          # frontmatter (name, description) + instructions
```

#### 🌐 HTTP Remote MCP Server

Mirobody's MCP server supports **HTTP/HTTPS remote access**, enabling:

- **Cloud Deployments**: Deploy your MCP server on any cloud platform
- **ChatGPT Apps**: Integrate with OpenAI's ChatGPT Apps via HTTPS
- **Cross-Network Access**: Access tools from anywhere, not just localhost
- **OAuth Security**: Secure remote access with OAuth authentication

To enable remote HTTP access, set `MCP_PUBLIC_URL` in your `config.{env}.yaml`:

```yaml
MCP_PUBLIC_URL: "https://yourdomain.com"
```

Your MCP server will then be accessible at the configured HTTPS endpoint, ready for remote integrations.

---

## 🔐 Access & Authentication

Once deployed, you can access the platform through the local web interface or our official hosted client.

### 1. Access Interfaces

| Interface                          | URL                                       | Description                                                                                                                                         |
| ---------------------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Local Web App**            | `http://localhost:18080`                | Fully self-hosted web interface running locally.                                                                                                    |
| **Official Client**          | [https://mirobody.ai](https://mirobody.ai) | **Recommended.** Our official web client that connects securely to your local backend service.                                                |
| **MCP Server (Local)**       | `http://localhost:18080/mcp`            | For Claude Desktop / Cursor integration via local connection.                                                                                       |
| **MCP Server (Remote HTTP)** | `https://yourdomain.com/mcp`            | **🌐 HTTP Remote MCP Support** - For ChatGPT Apps and remote integrations. Set `MCP_PUBLIC_URL` in your config file to enable HTTPS access. |

#### MCP Integration

Mirobody supports both **local** and **remote HTTP** MCP connections:

**Local Connection (Cursor/Claude Desktop):**

```json
{
  "mirobody_mcp": {
    "command": "npx",
    "args": [
      "-y",
      "universal-mcp-proxy"
    ],
    "env": {
      "UMCP_ENDPOINT": "http://localhost:18080/mcp"
    }
  }
}
```

**Remote HTTP Connection (ChatGPT Apps, Cloud Deployments):**

Configure `MCP_PUBLIC_URL` in your `config.{env}.yaml`:

```yaml
MCP_PUBLIC_URL: "https://yourdomain.com"
```

Then access your MCP server via HTTPS at the configured URL. This enables:

- ✅ ChatGPT Apps integration
- ✅ Cross-network tool access
- ✅ Cloud-based deployments
- ✅ Secure OAuth-enabled remote MCP access

### 2. Login Methods

You can choose to configure your own authentication providers or use the pre-set demo account.

- **🔐 Social Login**: Google Account / Apple Account (Requires configuration in `config.yaml`)
- **📧 Email Login**: Email Verification Code (Requires email service configuration)
- **🎮 Demo Account** (Pre-configured in `config.localdb.yaml`):
  - **Email**: `demo1@mirobody.ai`, `demo2@mirobody.ai`, `demo3@mirobody.ai`
  - **Password**: `777777`

---

## 🔌 API Reference

Mirobody provides standard endpoints for integration:

| Endpoint         | Description            | Protocol          |
| ---------------- | ---------------------- | ----------------- |
| `/mcp`         | MCP Protocol Interface | JSON-RPC 2.0      |
| `/api/chat`    | AI Chat Interface      | OpenAI Compatible |
| `/api/history` | Session Management     | REST              |

---

## 🧪 Testing

```bash
pip install -e '.[test]'
pytest        # 260 tests, ~7s — no database, no network, no API key
```

Tests sit beside the code they cover, so bare `pytest` is the whole suite. Two
of them carry the project's public claims: `test_engine_coverage.py` is the
116/116 resolver number quoted above, and `pulse/gate_tests/` snapshots every
vendor payload against its standardized form.

**👉 [docs/testing.md](docs/testing.md)** — layout, markers, snapshot
regeneration, and the release gates (`lint-imports`, `check_wheel_data.py`).

---

## 📚 Documentation

Docs follow one rule: **each package carries a short `README.md` saying what it
is; long-form guides live in [`docs/`](docs/)** so a `pip install` doesn't drag
contributor documentation into `site-packages`.

| | Topic | Location |
| --- | --- | --- |
| | **Runnable examples** | [`examples/`](examples/README.md) |
| ① | Collect — the pulse engine | [`mirobody/pulse/`](mirobody/pulse/README.md) |
| ① | Writing a data provider | [docs/provider-guide.md](docs/provider-guide.md) |
| ① | Provider directory layout | [`mirobody/pulse/providers/`](mirobody/pulse/providers/README.md) |
| ① | File-processing pipeline | [docs/file-processing.md](docs/file-processing.md) |
| ① | Apple Health / CDA import | [docs/apple-health.md](docs/apple-health.md) |
| ② | Indicator search & resolution | [`mirobody/indicator/`](mirobody/indicator/README.md) |
| ② | Health indicators & units | [`mirobody/pulse/core/`](mirobody/pulse/core/README.md) |
| ③ | Agent development | [`mirobody/agent/`](mirobody/agent/README.md) |
| ③ | Tool development | [`mirobody/agent/tools/`](mirobody/agent/tools/README.md) |
| ③ | ChatGPT Apps widgets | [`mirobody/agent/resources/`](mirobody/agent/resources/README.md) |
| | Configuration guide | [`mirobody/utils/config/`](mirobody/utils/config/README.md) |
| | Testing | [docs/testing.md](docs/testing.md) |
| | Known gaps & deferred work | [docs/roadmap.md](docs/roadmap.md) |
| | Changelog · Security | [CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md) |

---

## 🤝 Contributing

Contributions are organized around the engine's three verbs — pick your lane:

| Lane                 | What to contribute                                                                                                                                                                                                                                                                                                               | Typical size |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| **① Collect** | A new device provider — implement [`BasePullProvider`](mirobody/pulse/providers/platform/base.py) in one `mirobody_<slug>/` directory and the platform discovers it at startup; [`mirobody_pgsql/`](mirobody/pulse/providers/mirobody_pgsql/) is the smallest reference, [`mirobody_whoop/`](mirobody/pulse/providers/mirobody_whoop/) the OAuth2 one. Or a new file format for the parser | medium       |
| **② Sort**    | **Make a term resolve.** Find one that comes back wrong or empty — `mirobody resolve "<term>"` — then add one row to [`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv) and one case to [`test_engine_coverage.py`](mirobody/test_engine_coverage.py). Any language. This is the lowest-barrier useful PR in the repo, and it moves a number we publish. Also: unit mappings, taxonomy fixes | tiny         |
| **③ Answer**  | An Agent Skill (`SKILL.md` package under [`mirobody/agent/skills/`](mirobody/agent/skills/) — copy [`lab-report-walkthrough`](mirobody/agent/skills/lab-report-walkthrough/SKILL.md)), an MCP tool, a chart schema                                                                                                                                                                                                                             | medium       |

Found a lab report that parses wrong, or an indicator name that doesn't resolve? **That's a great issue** — attach the (de-identified) sample. See the [Contributing Guide](CONTRIBUTING.md) for PR mechanics.

---

<div align="center">
