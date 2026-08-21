<div align="center">

# 🚀 Mirobody

**The AI-native health data engine — collect, standardize, and reason over labs, wearables & genomics.**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Benchmarks](https://img.shields.io/badge/%F0%9F%A4%97_Benchmarks-4k%2B_downloads_each-FFD21E.svg)](https://huggingface.co/healthmemoryarena)
[![arXiv](https://img.shields.io/badge/arXiv-2604.02834-b31b1b.svg)](https://arxiv.org/abs/2604.02834)
[![Docs](https://img.shields.io/badge/Docs-docs.mirobody.ai-black)](https://docs.mirobody.ai/)

**[📚 Documentation](https://docs.mirobody.ai/)** · **[💬 Hosted chat — chat.mirobody.ai](https://chat.mirobody.ai/)** · **[🔌 API platform — platform.mirobody.ai](https://platform.mirobody.ai/)**

**English** · **[简体中文](README.zh-CN.md)** · **[繁體中文](README.zh-TW.md)** · **[日本語](README.ja.md)**

*Blood tests, wearables, genomics, imaging — all fragmented, all incompatible.
Before AI can understand your health, someone has to unify these signals into a
single standard AI can actually read. That is what this engine does.*

<img src="docs/images/where-your-data-comes-from.svg" alt="From wearables to food photos — one standard format, ready for AI." width="920">

</div>

The engine does three things, and the codebase (and [Contributing](#-contributing)) is organized around exactly these three stages — the same **C · S · A** the [documentation](https://docs.mirobody.ai/en/api-reference/) uses:

| Stage                | What it means                                                                                                     | Where                                                   |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| **① Collect** | Pull signals in: 3 device providers + a SQL source · 7 file formats · Apple Health                                             | [`pulse/`](mirobody/pulse/) |
| **② Standardize**    | One standard: resolve any reading to canonical codes (LOINC · SNOMED CT · RxNorm), normalize units, land against FHIR-recognized code systems | [`indicator/`](mirobody/indicator/)                    |
| **③ Answers**  | Reason: agents read the*original documents* through a virtual filesystem and answer with charts & citations     | [`agent/`](mirobody/agent/)                  |

---

## ⚡ Try it in 60 seconds

No server, no key, no network — the terminology engine is a pip install:

```bash
pip install mirobody
mirobody resolve "hemoglobin" "血红蛋白" "血紅素" "ヘモグロビン"
# all four -> LOINC 718-7
```

```python
from mirobody.engine import resolve
resolve("血红蛋白").loinc   # -> '718-7'   offline: no key, no config, no network
```

The third one is the interesting case. `血紅素` is not `血红蛋白` in different
glyphs — Taiwan and the mainland use **different words** for haemoglobin, and a
character conversion of one gives you `血红素`, which a raw index answers with
the code for **HbA1c**: a different test. Script folding gets this wrong; the
vocabulary has to be curated. Most of what a standardization layer does is
this, not the easy rows.

### The two functions you will actually call

`resolve()` answers a **name**. `resolve_reading()` answers a **measurement** —
and they give different codes, because LOINC puts the unit and the result type
into the identity:

```python
from mirobody.engine import resolve, resolve_reading

resolve("total cholesterol").loinc                       # '2093-3'   [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L")    # '14647-2'  [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL")     # '2093-3'   unchanged

resolve("尿糖").loinc                                     # '2350-7'   [Mass/volume]
resolve_reading("尿糖", "阴性")                            # '2349-9'   [Presence]
resolve_reading("尿糖", "5.6", "mmol/L")                   # '15076-3'  [Moles/volume]
```

**If you have the value and the unit, pass them.** The same indicator goes
opposite ways depending on the reading, and filing a mmol/L result under the
mg/dL code — or a 阴性 result under a mass-concentration code — is how a series
ends up with two units in it and nobody notices. Both functions are offline,
deterministic, and safe to call in a loop (~25 µs each after the first).

Every answer says how it got there, and one value is not like the others:

```python
r = resolve("血红蛋白")
r.loinc, r.canonical, r.method     # '718-7', 'Hemoglobin [Mass/volume] in Blood', 'lexical'

resolve("绝对不存在的指标名xyzzy").method   # ''         never seen it
resolve("血糖(HbA1c)").method              # 'refused'  two different tests in one string
```

`""` is a gap and worth a second opinion; `"refused"` is the answer — `血糖(HbA1c)`
names glucose outside the parentheses and HbA1c inside, `血脂` is four analytes,
and no single code is right for either. A panel term that *has* a panel code is
not a refusal: `blood pressure` → `85354-9`, the code FHIR's vital-signs profile
mandates, which tells the caller to expect components.
**Only treat `method == "lexical"` as an identity** (a
grouping key, a "these are the same series" decision, a FHIR mirror). See
[Semantic recall](#-semantic-recall-opt-in-and-why-it-is-opt-in) for the other
kind.

### Units are compared, never assumed

```python
from mirobody.indicator.fhir.units import convert_value, convertible

convert_value(5.6, "mmol/L", "mg/dL", loinc_code="1558-6")   # 100.9  (molar-mass bridge)
convert_value(42.0, "U/L", "[IU]/L")                         # 42.0   (1:1, different families)
convert_value(24.0, "kg/m2", "mg/dL")                        # None   (BMI is not a concentration)
convertible("%", "10*9/L")                                   # False  (a fraction is not a count)
```

`None` is an answer, not a failure: report the readings separately rather than
scaling one to look like the other. Do **not** use `unit_family()` to decide
convertibility — it is a LOINC PROPERTY classifier and it is wrong in both
directions for that question (`kg/m2` and `mg/dL` share a family and cannot
convert; `U/L` and `[IU]/L` are in different families and are the same unit).

Then self-host the full thing (see [Quick Start](#-quick-start)), sign in, and
mint your **personal MCP URL** (web client → Settings → MCP Url). Point any MCP
client (Claude Desktop, Cursor, Cherry Studio) at it and talk to your own
health-data engine:

```json
{ "mcpServers": { "mirobody": { "url": "http://localhost:18080/mcp/<your-personal-secret>" } } }
```

The MCP surface is small on purpose:

| Tool | What it does | Needs |
| --- | --- | --- |
| `resolve_indicator` | any-language indicator name → canonical LOINC | nothing — offline, no user data |
| `normalize_unit` | free-text unit → canonical UCUM + comparability family | nothing — offline, no user data |
| `query_health_indicators` | your own records — search, read and aggregate in **one** call; every result carries its LOINC identity | your account |
| `get_genetic_data` | your variants by rsid | your account |

`tools/list` is honest per account: the two account-bound tools are listed only
when your account actually holds that kind of data. The server speaks
[MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/) —
the current stateless revision (per-request `_meta`, `server/discover`,
deterministic tool ordering) — and negotiates down to `2024-11-05` for older
clients.

Runnable walkthroughs: [`examples/`](examples/README.md) — five scripts from offline resolution to a full-server preflight, each verified to run.

---

## ① Collect — every signal, one intake

- **Production device providers behind one plugin contract** — battle-tested providers for **Garmin, Oura, Whoop** plus [300+ devices](mirobody/pulse/providers/README.md) via the pulse platform. A provider is a directory: drop it in, and discovery, OAuth and pull scheduling are wired for you.

  > **What a self-hosted deployment needs to actually turn these on.** Each
  > provider is an OAuth client of that vendor, so it stays dormant until you
  > supply credentials **you** obtained from the vendor's developer program —
  > `GARMIN_CLIENT_ID`/`SECRET`, `OURA_CLIENT_ID`/`SECRET`,
  > `WHOOP_CLIENT_ID`/`SECRET` (plus each one's redirect URL) in
  > `config.{env}.yaml`. Without them the module still loads and logs
  > `declined to start (not configured)` — which is the honest state, not a
  > failure. [`mirobody_pgsql/`](mirobody/pulse/providers/mirobody_pgsql/) is
  > the one you can try immediately: set `ENABLE_PGSQL_DEVICE: 1` and the
  > platform logs `loaded 1 providers` on the next boot.
  > **Step by step: [docs/provider-setup.md](docs/provider-setup.md)** — the
  > exact callback URLs, the config keys, and how to tell "not configured"
  > apart from "broken" in the boot log.

- **Apple Health is push-only, and needs an iOS app you build** — the endpoints
  are here ([`/apple/health`, `/apple/statistics`, `/apple/cda`](mirobody/server/routers/apple_router.py),
  with [CDA processing](mirobody/pulse/apple/README.md)), but they *receive*
  data; nothing in this repo can pull from HealthKit. HealthKit is only readable
  from a signed iOS app, on-device, after the user grants permission per data
  type — there is no web OAuth flow and no server-to-server API. So a
  self-hosted web deployment shows no "connect Apple Health" button, correctly:
  the missing piece is an iOS client with the HealthKit entitlement, and the
  API above is what such a client would POST to.
- **7 file formats parsed with AI** — PDF lab reports, Excel, CSV, images, audio, plain text, and **genetic exports (WeGene)**; LLM-powered indicator extraction ([`pulse/file_parser/`](mirobody/pulse/file_parser/), 13k lines).
- Ingest pipeline: staged intake → validate → normalize → daily rollups → [AI insights](mirobody/pulse/insight/) that feed back into the record — closing the loop.

## ② Standardize — one standard AI can actually read

The part none of the adjacent open-source projects have — a **semantic standardization layer**, not a lookup table:

- **Concept graph**: 440,961 nodes · 22,044,110 cross-vocabulary edges · **595,746 source ids** distilled into canonical concepts (LOINC · SNOMED CT · RxNorm bridges), shipped via Git LFS ([`indicator/`](mirobody/indicator/README.md)).
- **Embedding-based resolution**: free-text indicator names → canonical codes, with **49,253 multilingual aliases** (中文 22,578 · 日本語 16,809 · +5: de·es·fr·ko·ru) — `hemoglobin`, `血红蛋白`, `血紅素` and `ヘモグロビン` all land on LOINC 718-7.
- **繁體中文 is two problems, handled as two.** Script is mechanical: queries are folded zh-Hant → zh-Hans from a shipped 3,336-character table ([`zh_fold.py`](mirobody/indicator/zh_fold.py)), mirroring what the lexicon build already does to the corpus. Vocabulary is not: Taiwan clinical usage picks different words, and folding `血紅素` yields `血红素` → the HbA1c code. Those terms are curated under their Traditional spelling, and a curated row always beats a fold.
- **We measure that claim instead of asserting it.** [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) scores the offline resolver against the panels a physical actually orders — lipid, CBC, metabolic, liver, thyroid, hormones, tumour markers, urinalysis, vitals — written the way a report prints them, in English, 简体中文, 繁體中文 and 日本語 — plus the device/wearable vocabulary the platform API teaches (`steps`, `resting_heart_rate`, `sleep_duration`). **197/197 today; it scored 32/94 the day it was written.** It grades *clinical* correctness, not resolution rate: answering `血红蛋白` with the code for HbA1c is scored as a failure, and `血脂` (a category, not an observation) is required to resolve to *nothing*, because a confident wrong code is worse than an honest miss.
- **Surface algebra, so the spelling doesn't decide the answer** ([`indicator/lexical.py`](mirobody/indicator/lexical.py)): NFKC-lite folding (full-width, superscripts, the six dash variants) plus a CJK-aware tokenizer, and a guarded strip of the `名称(缩写)` shape a lab report prints. `ＦＢＧ`, `LDL–C`, `fasting_glucose`, `空腹血糖(GLU)` and `Cholesterol, total` all reach the same codes as their plain forms. When the two halves of `名称(缩写)` disagree — `血糖(HbA1c)` — the term stays **unresolved** rather than picking one.
- **The reading picks the code, not just the name** ([`engine.resolve_reading`](mirobody/engine.py)). LOINC codes the unit *and* the result type into the identity, so the unit picks `PROPERTY` and the value's kind picks `SCALE_TYP`. `5.0 mmol/L` → 14647-2, `193 mg/dL` → 2093-3, `阴性` → the `[Presence]` variant. Half the shipped corpus is non-`Qn` (38,687 rows of 79,368), so a resolver that only constrains numbers is blind to half of it.
- **Unit normalization** to UCUM families (~310), plus [conversion](mirobody/indicator/fhir/units/convert.py) — dimensional analysis, a molar-mass bridge keyed by LOINC code, and an explicit refusal for `%` vs `10*9/L`. 316 standard pulse indicators.
- Taxonomy of 25 clinical categories (Vital signs, Lab & Clinical, Body measures, …).

### 🧪 Semantic recall: opt-in, and why it is opt-in

Everything above is lexical — shipped vocabularies and table lookups. It abstains
when it does not know a term, which is a ceiling as well as a virtue. There is a
second tier ([`indicator/semantic.py`](mirobody/indicator/semantic.py)): cosine
recall over an embedding of the LOINC corpus, built by
[`scripts/build_loinc_embeddings.py`](scripts/build_loinc_embeddings.py).

```python
from mirobody.engine import resolve_with_semantic_fallback

out = await resolve_with_semantic_fallback(["空腹血糖", "some unheard-of assay"])
out[0].method    # 'lexical'  — the lexical tier answered; the fallback never saw it
out[1].method    # ''         — no matrix installed, so nothing to fall back TO
                 # 'semantic' once one is, and that means "a suggestion", not an identity
```

**No matrix ships**, so this is a no-op on a plain `pip install` and returns
exactly what `resolve()` would. Point `MIROBODY_SEMANTIC_INDEX` at one to
enable it; a matrix is ~198 MB and needs an embedding key, and corpus and query
MUST come from the same model — a mismatched pair does not fail, it returns
confident nonsense.

It stays opt-in even once installed, and the reason is a property of the tier
rather than a tuning problem: **cosine recall cannot abstain.** Asked about a
term it has never seen, it returns its nearest neighbour with the same
confidence it returns a correct answer, and LOINC's own questionnaire corpus
gives it plenty of plausible-looking neighbours to reach for. There is no score
threshold that separates the two.

What makes it useful anyway is that a reading is not a bare string by the time it
gets here. The extraction pass ahead of it returns nothing for non-health
content and hands over `{indicator, value, unit}`, so the tier can gate
candidates on what the reading implies — `SCALE_TYP` from the value's kind,
`PROPERTY` from the unit's dimension — and skip the non-clinical rows
`loinc_skip.txt` already lists.

So: use it to **suggest** a code that a human or a model then confirms. The
lexical tier is what you build identities on.

## ③ Answers — agents that read the originals

There are **two ways to consume this layer**, and an agent for each — the difference is *who runs the tool loop*:

| | **DeepAgent** — you run the engine | **BaseAgent** — your model consumes ours |
| --- | --- | --- |
| Tool loop runs | here, in your deployment | in the **LLM provider**, against `/mcp` over HTTP |
| For | self-hosting the whole thing | Claude Desktop · Cursor · ChatGPT Apps · any MCP client |
| Extras | virtual filesystem, QuickJS, Agent Skills, charts | whatever the MCP tool surface exposes — nothing hidden |

- **DeepAgent** — the primary agent, on [deepagents](https://github.com/langchain-ai/deepagents) 0.7 / LangChain 1.3. Multi-provider (OpenAI, Gemini, Anthropic, OpenRouter, any OpenAI-compatible endpoint); a **PostgreSQL-backed virtual filesystem** (`/uploads`, `/library`, `/memories`, `/skills`) lets the model `read_file` your *original* PDF — multimodally — instead of a lossy extraction; in-process JS interpreter (QuickJS) for real computation; per-turn model-call budget with graceful stop.
- **BaseAgent** — no LangChain, on purpose. It hands the MCP server to the provider (OpenAI Responses `mcp_server`, Gemini Interactions) and streams the result. That makes it our own rehearsal of the third-party experience: **anything BaseAgent can't do unaided is something an outside MCP client can't do either.** It does not chart — the consuming client brings its own visualization.
- **MCP server built in** ([`mcp/`](mirobody/mcp/)) — every tool doubles as an MCP tool over HTTP; works as MCP client *and* OAuth-enabled MCP server. **Agent Skills** (SKILL.md) served via deepagents' native SkillsMiddleware from [`mirobody/agent/skills/`](mirobody/agent/skills/).
- Care-circle sharing with per-person consent:

<div align="center"><img src="docs/images/your-care-circle.svg" alt="Your care circle — invite the people you trust by email; you stay in control: remove a member or unshare anytime, and health sharing stays off until you allow it." width="920"></div>

---

## 🏗️ Architecture

The engine is three stages — **① Collect → ② Standardize → ③ Answers** — and the package
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
│   ├── file_parser/         7 file formats → indicators via LLM extraction (needs DB)
│   ├── ingest/              StandardPulseData: the universal exchange format that
│   │                        every source above converges on (was `data_upload/`)
│   └── core/                domain models, daily rollups, insights (needs DB)
├── indicator/           ②  STANDARDIZE — one standard AI can actually read
│   └── fhir/                concept graph · embedding resolution · units → UCUM · taxonomy
├── res/                     the shipped data: LOINC/SNOMED bundles (Git LFS, see
│                            LICENSE-3RD-PARTY + *.NOTICE) · resolver_overrides.tsv · sql/
│
│  ── shared infrastructure: not a fourth stage, used BY the three ─────────
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
├── agent/               ③  ANSWERS — one roof for everything conversational
│   ├── deep_agent.py        DeepAgent — model 1: YOU run the engine. deepagents/
│   │                        LangChain, PG virtual fs, QuickJS, Agent Skills
│   ├── base_agent.py        BaseAgent — model 2: someone else's model consumes us
│   │                        over MCP. Hands /mcp to the provider, which drives
│   │                        the tool loop. No LangChain, on purpose.
│   ├── base/ · deep/        the two agents' internals (backends, middleware)
│   ├── chat/                sessions · messages · history replay · sharing · profile
│   ├── tools/               the MCP tool surface (MCP_TOOL_DIRS): terminology
│   │                        (② Standardize, offline), health records, genetics
│   ├── skills/              Agent Skills (SKILL.md) — deepagents SkillsMiddleware
│   ├── prompts/             Jinja system prompts
│   └── resources/           MCP UI widgets for ChatGPT Apps (see its README)
└── server/                  HTTP lifecycle + the FastAPI routers (server/routers/)

frontend/                    the bundled web client, shipped as a FIXED build —
                             outside the package on purpose: wheels ship the
                             engine, not 8MB of JS. Served when `frontend/`
                             exists next to the process (Docker/source). The
                             API + MCP surface is the real contract: build your
                             own frontend against it.
```

### What you get at each install size

| Install | What works | Footprint |
| --- | --- | --- |
| *the wheel + numpy only* | `from mirobody.engine import resolve` — the offline resolver | **33 MB** of mirobody (76 MB with numpy) |
| `pip install mirobody` | + `mirobody parse` (one LLM key) · file parsing (PDF/Excel/audio) · coded output | 233 MB, 90 packages |
| `pip install 'mirobody[server]'` | + the HTTP API and MCP endpoint | needs Postgres + Redis |
| `pip install 'mirobody[agents]'` | + DeepAgent/BaseAgent and `mirobody serve` (includes `[server]`) | + the LangChain stack |
| `pip install 'mirobody[indicator-build]'` | rebuilding the terminology bundles themselves | needs LOINC/UMLS sources |

Sizes measured on a clean venv, not estimated. **24 MB of mirobody's 33 MB is
the shipped LOINC data** — that is the resolver, not overhead, and it is what
makes standardization work with the network unplugged.

#### The minimum usable scope, and what is deliberately not in it

`pip install mirobody` carries exactly what `resolve()` reads, and nothing else:

| Shipped | What reads it |
| --- | --- |
| `res/fhir_loinc_bundle.tar.gz` | the 921k-key alias index, the LOINC axis table, the commonness prior |
| `res/fhir_meta.csv.gz` | the 677k-name corpus the alias index points into |
| `res/aliases_src/*.tsv` | 49,253 multilingual alias rows (中文 22,578 · 日本語 16,809 · +5: de·es·fr·ko·ru) |
| `res/resolver_overrides.tsv` | the hand-written corrections, and the deliberate non-answers |

Four artifacts used to ship and no longer do, **28 MB between them**. Nothing at
runtime read any of them: grep `server/`, `agent/`, `pulse/`, `mcp/` and `task/`
for `concept_graph` or `taxonomy` and it comes back empty. Three —
`fhir_concept_graph.bin`, `fhir_taxonomy.bin`, `fhir_snomed_ct_bundle.tar.gz` —
are still in the repo for [`indicator/`](mirobody/indicator/)'s bundle-build
tooling, which works from a git checkout, and for the v2 semantic pipeline, which
in addition needs an embedding matrix that is not distributed at all. The fourth,
`fhir_id_map.npy`, is **gone from the repo entirely**: it mapped canonical ids to
`fhir_indicators.id`, one database's primary keys, so it was never meaningful to
anyone else — regenerate your own with `indicator id-map`. Dropping the
SNOMED bundle also takes its Affiliate-Licence obligation off every pip user.
`scripts/check_wheel_data.py` now gates both directions — the five above present
and real, those four absent.

**What the minimum scope cannot do**: resolve a term the lexical layer misses.
Resolution is exact-key and alias-table lookup over shipped vocabularies, plus
the surface algebra in [`indicator/lexical.py`](mirobody/indicator/lexical.py).
There is no embedding recall — [`fhir/resolve/pipeline.py`](mirobody/indicator/fhir/resolve/)
implements it, and it needs a ~200 MB LOINC embedding matrix built by
[`scripts/build_loinc_embeddings.py`](scripts/build_loinc_embeddings.py) plus an
embedding API key. A miss here is an honest miss, and the fix is one row in
`resolver_overrides.tsv` — [see Contributing](#-contributing).

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
`utils/`, `user/` and `task/` are not stages; they are the infrastructure the
three stand on. One deliberate seam crosses the boundary today, recorded with
its exit plan in `pyproject.toml`'s `ignore_imports` and in
[docs/roadmap.md](docs/roadmap.md).

### Data flow, end to end

```
vendor APIs / files / Apple Health          ① pulse
        └─> StandardPulseData ─> validate ─> normalize ─> daily rollups
                 └─> indicator names ─> ② indicator: canonical codes (LOINC·SNOMED·RxNorm)
                          └─> coded rows in Postgres
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

Reproduce any of them with one command via **[mirobody-eval](https://github.com/thetahealth/mirobody-eval)** — our open evaluation framework. Its generator also produces the synthetic (PHI-free) trajectories that fill a fresh deployment's empty database — see [Seed it with data](#-seed-it-with-data).

We hold the engine itself to the same standard. **Resolver coverage** — can ② Standardize name the everyday tests on a real lab report? — runs in this repo, offline, in under a second:

```bash
pytest mirobody/test_engine_coverage.py -s
#   offline resolver coverage: 197/197 = 100%
```

It started at **32/94** — the benchmark has since grown to 197 cases. The gap was not the concept graph; it was that the index is built from LOINC long names, so it knew `LDL-C` but not `LDL cholesterol`, knew 葡萄糖 but not `血糖`, and answered `血红蛋白` with the code for HbA1c. Both classes of failure are one TSV row each to fix — [see Contributing](#-contributing).

---

## ⚡ Quick Start

### 📋 Prerequisites

- **Docker & Docker Compose**: Ensure these are installed and running.
- **Git**: To clone the repository.
- **Git LFS**: Required to pull binary data files (e.g. `fhir_concept_graph.bin`). Install via `apt install git-lfs` (Linux) or `brew install git-lfs` (macOS). Git for Windows includes it by default. Run `git lfs install` once after installing.

### Deploy via Docker

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

Three keys and one gotcha worth knowing before anything else:

> - **LLM key**: `OPENROUTER_API_KEY` powers the Deep agent.
> - **Embedding key** — a *different* one, and it is still a second key: the
>   worker's indicator sync embeds names into a `th_series_dim` vector column,
>   and only two providers have one. `EMBEDDING_PROVIDER` defaults to `gemini`
>   (`GOOGLE_API_KEY`); `qwen` (`DASHSCOPE_API_KEY`) is the other. With only an
>   OpenRouter key, chat works but **Health indicators stays 0** — embedding
>   fails in the worker log. Setting `EMBEDDING_PROVIDER: openrouter` does not
>   fix that and will not pretend to: it raises, naming the missing column,
>   because a sweep that quietly wrote nothing looks identical to a sweep with
>   nothing to do. That provider exists for `text_embedding` callers and the
>   [file-based semantic tier](#-semantic-recall-opt-in-and-why-it-is-opt-in),
>   neither of which touches a database column.
> - Keys go in `config.{env}.yaml`; Mirobody encrypts them at first load with
>   the generated `CONFIG_ENCRYPTION_KEY`.
> - First start takes ~1 minute (schema creation) — wait for
>   `SQL files initialization completed`.
>
> Full configuration guide: [CONFIG](mirobody/utils/config/README.md) ·
> [DATABASE](mirobody/schema/README.md) · [docs.mirobody.ai](https://docs.mirobody.ai/)

### 🐍 Local Python Development

Run the code on the host with pg/redis in Docker — the normal debug loop:

```bash
docker compose up -d pg redis
pip install -e '.[agents]'        # Python ≥3.12; engine-only is `pip install -e .`
echo "ENV=localdb" > .env
# create config.localdb.yaml overriding PG_HOST/PG_PORT/REDIS_* to the
# containers' published ports, add your LLM keys, then:
mirobody serve
```

The step-by-step walkthrough (ports, encryption key, `[cn]` extra) lives at
[docs.mirobody.ai](https://docs.mirobody.ai/) and in
[CONFIG](mirobody/utils/config/README.md).

**The CLI at a glance**

| Command | What it does |
| --- | --- |
| `mirobody parse <file>` | Lab report in, standardized LOINC table out — one LLM key, zero infrastructure |
| `mirobody resolve <terms…>` | Offline indicator-name resolution — no key, no config, no network |
| `mirobody serve` | Run the HTTP server (chat, MCP, API) — needs `[agents]` |
| `mirobody worker` | Run the background task worker (indicator sync, profile refresh) |

### 👤 First Login

Sign in with a pre-seeded demo account — the server prints these at startup:

- **Email**: `caregiver@mirobody.ai` — named for the role: you sign in as the
  caregiver, and the record you read belongs to someone else
- **Verification code**: `111111`

They come from `EMAIL_PREDEFINE_CODES` in `config.yaml`: with no SMTP
configured, only predefined addresses can sign in. Add your own address there,
or configure `EMAIL_SMTP_*` to send real codes.

Those three are an allowlist, not a limit on registration: any address that
passes verification is created on the spot (`add_or_get_user`). What the
allowlist gates is *verification* — with no SMTP configured, the only codes that
verify are the predefined ones, so "Send code" will report `No SMTP server
configured.` and you type the code you already know.

**Or skip codes entirely.** The code path needs Mandrill or SMTP, which a
deployment you cloned to try out does not have — so the login page opens on
**Sign in / Create account** and keeps **Email code** as a third tab. The API
underneath, if you would rather curl it:

```bash
curl -X POST localhost:18080/password/register -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com","password":"at-least-8-chars"}'
```

That returns a token and creates the account; `POST /password/login` with the
same body signs you back in. `username` works in place of `email`. The hash is
bcrypt computed inside Postgres by `pgcrypto` (`crypt()` / `gen_salt('bf', 12)`),
so no password is ever hashed, compared or logged in Python, and no default
password ships in this repo. `register` refuses an account that already has one
rather than overwriting it — that endpoint takes no proof of ownership, so
letting it rotate a password would be a takeover primitive. Wrong password and
unknown account answer identically, which is deliberate: telling them apart
enumerates accounts.

To add your own address to a Docker deployment without editing a tracked file,
pass the whole map as an environment variable — config precedence is
`env > config.{env}.yaml > config.yaml`, and a JSON string is parsed:

```bash
docker compose run -e EMAIL_PREDEFINE_CODES='{"you@example.com":"424242","caregiver@mirobody.ai":"111111"}' mirobody
```

It **replaces** the map rather than extending it, so re-list any `exp*` account
you still want. For real codes to arbitrary addresses, configure `EMAIL_SMTP_*`
instead and the allowlist stops mattering.

### 👨‍👩‍👧 The care circle demo — ask, then upload

`compose.yaml` sets `SEED_DEMO_DATA=true`, so the Docker path arrives with one
synthetic person already in your care circle: **Demo (synthetic)** — 244
indicators across two years, plus five markdown documents the agent can
`read_file`. Sign in as `caregiver@mirobody.ai` (code `111111`) and you own
nothing; the record you are reading is someone else's.

**Start with a question, not the data table.** On the Ask page:

> *"What was her latest LDL cholesterol and how does it compare to a year earlier?"*

which on a freshly seeded deployment answers from her real history:

```
| Date       | LDL (mmol/L) |
| 2024-04-16 | 3.4          |
| 2024-10-15 | 3.2          |
| 2025-04-15 | 3.1          |
```

…and then volunteers that the most recent panel is over a year old and worth
repeating. Which is the cue for the second half of the demo.

**Now hand it a file.** `mirobody/demo/lab_report_2025-10-15.pdf` is her *next*
panel, deliberately held out of the seed — so uploading it is not a no-op, it is
data the database does not have. Drop it on the Data page (or the ＋ in Ask) and
watch ① Collect and ② Standardize do their jobs: the PDF is read, twelve
analytes come out with their units, each resolves to a code, and the LDL series
gains a fourth point. Ask the same question again and the answer moves.

Other questions that land on seeded data: *"which of her results are outside the
reference range?"*, *"has her sleep changed since last winter?"*, *"summarise her
last lab panel for me"*.

Every value is synthetic. The trajectory was generated for
[ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench) by
[mirobody-eval](https://github.com/thetahealth/mirobody-eval) and vendored here
as one 200 KB file plus a 5 KB PDF, so the seed needs no network, no HuggingFace
download and no API key — and the PDF says "SYNTHETIC SAMPLE" across its head.
The seed is an upsert, so restarts do not duplicate it. Set
`SEED_DEMO_DATA=false` for a deployment that will hold real data.

Answering questions needs an LLM key; browsing the record and uploading do not.
Indicator names arrive in the source's own spelling
(`AlanineAminotransferase-ALT`) rather than the display names your own uploads
get, because that polish comes from the dim/embedding pass — configure an
embedding key and `IndicatorSyncTask` tidies them up.

### 🌱 Seed it with data

A fresh install signs you in to an empty database — a poor first impression, and
it makes any change to the agent impossible to judge. The sibling
[mirobody-eval](https://github.com/thetahealth/mirobody-eval) fills it with one
synthetic user's five-year trajectory, then scores your deployment:

```bash
uv run python -m generator.eslbench.prepare_data                   # ~20 MB from HuggingFace
uv run python -m generator.eslbench.seed_mirobody --users user5086@demo
uv run python -m benchmark.basic_runner eslbench sample200-20260430 \
    --target-type mirobody --limit 20
```

Seeding needs `pip install mirobody` pointed at the deployment you are filling,
plus an embedding key; scoring needs its HTTP server up (`MIROBODY_BASE_URL`,
default `http://localhost:18080`). Add `--hold-out-exams 1` to keep the latest
lab panel out of the database, and `generator.eslbench.labreport` renders it as
a PDF — so the upload path has something the database genuinely lacks. Every
value is synthetic, and the PDF says so on its front page.

### Extend It — Tools and Skills

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

#### 🔑 Your personal MCP URL

Every signed-in user can mint a **personal MCP URL**: open the web client →
**Settings → MCP Url → Copy**, and paste it into any MCP client (Claude
Desktop, Cursor, Cherry Studio…). The URL embeds a private credential scoped to
your account — no OAuth dance, and the client reads *your* indicators from the
first call. Treat it like a password.

---

## 🔌 The HTTP API

Two surfaces, on purpose.

**The records API — shaped like [Mirobody Cloud](https://docs.mirobody.ai/en/api-reference/).**
If you have read the platform docs, you already know these: same request bodies,
same response envelopes, same field names, so code you write against a
self-hosted deployment reads the same as code written against the hosted one.

| Endpoint | What it does |
| --- | --- |
| `POST /api/standardize` | Report text in, standardized readings out — `{object: "extraction", data: [...]}`. Dry-run unless `store=true`. |
| `POST /api/data` | Write structured records (`records[]`, ≤500 per call). Every write is standardized on the way in. |
| `GET /api/data` | Read them back, newest first — `{object: "list", data: [...], has_more}`, one object per reading with its `loinc_code`. |
| `DELETE /api/data` | Erase by `id`, by `indicator`, or `all=true`. |

```bash
curl localhost:18080/api/data -H "Authorization: Bearer $JWT" \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"indicator":"fasting_glucose","value":5.6,"unit":"mmol/L","time":"2026-08-19T07:30:00Z"}]}'
# {"status":"ok","ingested":1,"standardized":1}
```

**Three deliberate differences from the hosted contract**, all in the same
direction — this is your machine, not a multi-tenant platform:

- **No `/v1` prefix.** These paths are not the hosted contract and should not
  claim to be versioned alongside it.
- **No `user` / `retention` / `session_id` / `mb_live_*` keys.** Subjects, expiry
  scheduling and billing are operator machinery for a plane with many tenants;
  here your JWT says who you are and a row lives until something deletes it.
  `retention` and `session_id` are *accepted and ignored* rather than rejected —
  a 400 for a field the platform docs told you to send helps nobody.
- **`DELETE /api/data` needs an explicit scope.** The hosted endpoint reads "no
  filter" as "everything", which is fine behind a key an operator minted on
  purpose. Here a mistyped curl is one keystroke from a person's whole record,
  so the widest scope is `all=true`.

**The web client's API** (`/api/v1/health-indicators`, `/api/chat`,
`/api/v1/pulse/*`, `/files/*`, `/invitation/*`) keeps the house
`{code, msg, data}` envelope. It is what the bundled frontend talks to; build
against it if you are replacing the frontend, and against the records API above
if you are feeding data in or reading it out.

## 🔐 Where you use it

| Surface | URL | What it is |
| --- | --- | --- |
| **Your web client** | `http://localhost:18080` | The bundled app your deployment serves — everything below. |
| **Your MCP endpoint** | `http://localhost:18080/mcp` | For Claude Desktop / Cursor; mint a personal URL in Settings. Set `MCP_PUBLIC_URL` for HTTPS/remote (ChatGPT Apps, OAuth). |
| **Hosted chat** | [chat.mirobody.ai](https://chat.mirobody.ai/) | The hosted client, if you'd rather not run your own. |
| **API platform** | [platform.mirobody.ai](https://platform.mirobody.ai/) | Keys, usage, and the health-data API for building on top. |

The bundled web client is a full consumer app, not a demo shell:

- **Data** (`/data`) — drag-and-drop lab PDFs, report photos (HEIC included),
  Excel/CSV, audio, text/Markdown and raw genotype files. Extraction turns them
  into standardized indicators; every reading links back to its **source file**
  and can be **corrected or deleted in place** (your own record only).
- **Ask** (`/ask`) — chat over your own records: DeepAgent finds the data,
  charts it, and reports per-answer **token usage** (tokens, not invented
  dollar figures). Pick a second model to compare answers side by side.
- **Care circle** — upload and ask on behalf of the people who share with you,
  gated by per-person consent.

Login: email verification codes (SMTP), Google/Apple OAuth, or the pre-seeded
demo accounts above — all configured in `config.{env}.yaml`.

---

## 🧪 Testing

```bash
pip install -e '.[test]'
pytest        # 495 tests, ~9s — no database, no network, no API key
```

Tests sit beside the code they cover, so bare `pytest` is the whole suite. Two
of them carry the project's public claims: `test_engine_coverage.py` is the
197/197 resolver number quoted above, and `pulse/gate_tests/` snapshots every
vendor payload against its standardized form.

**👉 [docs/testing.md](docs/testing.md)** — layout, markers, snapshot
regeneration, and the release gates (`lint-imports`, `check_wheel_data.py`).

---

## 📚 Documentation

**[docs.mirobody.ai](https://docs.mirobody.ai/)** is the documentation platform —
deployment, the API platform, and this open-source engine, kept in sync as all
three evolve.

In-repo docs follow one rule: **each package carries a short `README.md` saying
what it is; long-form guides live in [`docs/`](docs/)** so a `pip install`
doesn't drag contributor documentation into `site-packages`.

| | Topic | Location |
| --- | --- | --- |
| | **Runnable examples** | [`examples/`](examples/README.md) |
| ① | Collect — the pulse engine | [`mirobody/pulse/`](mirobody/pulse/README.md) |
| ① | **Connecting Garmin / Oura / Whoop** | [docs/provider-setup.md](docs/provider-setup.md) |
| ① | Writing a data provider | [docs/provider-guide.md](docs/provider-guide.md) |
| ① | Provider directory layout | [`mirobody/pulse/providers/`](mirobody/pulse/providers/README.md) |
| ① | File-processing pipeline | [docs/file-processing.md](docs/file-processing.md) |
| ① | Apple Health / CDA import | [docs/apple-health.md](docs/apple-health.md) |
| ② | Indicator search & resolution | [`mirobody/indicator/`](mirobody/indicator/README.md) |
| ② | Health indicators & units | [`mirobody/pulse/standardize/`](mirobody/pulse/standardize/README.md) |
| ③ | Agent development | [`mirobody/agent/`](mirobody/agent/README.md) |
| ③ | Tool development | [`mirobody/agent/tools/`](mirobody/agent/tools/README.md) |
| ③ | ChatGPT Apps widgets | [`mirobody/agent/resources/`](mirobody/agent/resources/README.md) |
| | Configuration guide | [`mirobody/utils/config/`](mirobody/utils/config/README.md) |
| | Testing | [docs/testing.md](docs/testing.md) |
| | Known gaps & deferred work | [docs/roadmap.md](docs/roadmap.md) |
| | Changelog · Security | [CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md) |

---

## 🤝 Contributing

Contributions are organized around the engine's three stages — pick your lane:

| Lane                 | What to contribute                                                                                                                                                                                                                                                                                                               | Typical size |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| **① Collect** | A new device provider — implement [`BasePullProvider`](mirobody/pulse/providers/platform/base.py) in one `mirobody_<slug>/` directory and the platform discovers it at startup; [`mirobody_pgsql/`](mirobody/pulse/providers/mirobody_pgsql/) is the smallest reference, [`mirobody_whoop/`](mirobody/pulse/providers/mirobody_whoop/) the OAuth2 one. Or a new file format for the parser | medium       |
| **② Standardize**    | **Make a term resolve.** Find one that comes back wrong or empty — `mirobody resolve "<term>"` — then add one row to [`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv) and one case to [`test_engine_coverage.py`](mirobody/test_engine_coverage.py). Any language. This is the lowest-barrier useful PR in the repo, and it moves a number we publish. Also: unit mappings, taxonomy fixes | tiny         |
| **③ Answers**  | An Agent Skill (`SKILL.md` package under [`mirobody/agent/skills/`](mirobody/agent/skills/) — copy [`lab-report-walkthrough`](mirobody/agent/skills/lab-report-walkthrough/SKILL.md)), an MCP tool, a chart schema                                                                                                                                                                                                                             | medium       |

Found a lab report that parses wrong, or an indicator name that doesn't resolve? **That's a great issue** — attach the (de-identified) sample. See the [Contributing Guide](CONTRIBUTING.md) for PR mechanics.

---

<div align="center">

**[📚 docs.mirobody.ai](https://docs.mirobody.ai/)** · **[💬 chat.mirobody.ai](https://chat.mirobody.ai/)** · **[🔌 platform.mirobody.ai](https://platform.mirobody.ai/)**

Apache-2.0 · your data stays on your machine

</div>
