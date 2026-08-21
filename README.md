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

Indicator resolution is the engine's front door and needs no key, no config and
no network:

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" "血红蛋白" "ヘモグロビン" "空腹血糖(GLU)"
```

```python
from mirobody.engine import resolve, resolve_reading

resolve("血红蛋白").loinc                                # '718-7'   any language, one code
resolve("total cholesterol").loinc                     # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L")   # '14647-2' [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL")    # '2093-3'  the unit picks the code
resolve("血脂").resolved                                 # False    a category, not an observation
```

**Pass the value and the unit when you have them.** LOINC encodes the unit *and*
the result type into the identity, so the same name resolves to different codes —
filing a mmol/L result under a mg/dL code is how one series quietly ends up
holding two units. `resolve` abstains rather than guessing: `""` is a gap worth a
second look, `"refused"` is the answer.
→ [Engine reference](https://docs.mirobody.ai/en/engine/) ·
[Indicators](https://docs.mirobody.ai/en/concepts/indicators/)

---

## What the standardization layer actually is

Not a lookup table — this is the part adjacent open-source projects do not have:

- **Concept graph**: 440,961 nodes · 22,044,110 cross-vocabulary edges ·
  **595,746 source ids** distilled into canonical concepts (LOINC · SNOMED CT ·
  RxNorm bridges).
- **49,253 multilingual aliases** (中文 22,578 · 日本語 16,809 · +5:
  de·es·fr·ko·ru). `hemoglobin`, `血红蛋白`, `血紅素` and `ヘモグロビン` all land
  on LOINC 718-7.
- **繁體中文 is two problems, handled as two.** Script folding is mechanical
  (a shipped 3,336-character zh-Hant → zh-Hans table); vocabulary is not — Taiwan
  usage picks different words, and folding `血紅素` yields the HbA1c code. Those
  terms are curated under their Traditional spelling, and a curated row always
  beats a fold.
- **Units** normalized to ~310 UCUM families, with dimensional analysis, a
  molar-mass bridge keyed by LOINC code, and an explicit refusal for `%` vs
  `10*9/L`. 300 standard pulse indicators.
- **We measure the claim instead of asserting it.**
  [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) scores the offline
  resolver against the panels a physical actually orders, written the way a report
  prints them, in English, 简体中文, 繁體中文 and 日本語 — plus the wearable
  vocabulary the platform API teaches. **197/197 today; it scored 32/94 the day it
  was written.** It grades *clinical* correctness: answering `血红蛋白` with the
  HbA1c code is a failure, and `血脂` is required to resolve to nothing.

```bash
pytest mirobody/test_engine_coverage.py -s   # offline, about a second
```

→ [Standardization](https://docs.mirobody.ai/en/api-reference/standardization/) ·
[Architecture](https://docs.mirobody.ai/en/concepts/architecture/) ·
[Data flow](https://docs.mirobody.ai/en/concepts/data-flow/)

---

## 📊 Benchmarks — we don't say "trust us", we ship the eval

Our health-AI benchmarks are the **most-downloaded in their category on Hugging
Face** (4,000+ each):

| Benchmark | What it measures | Downloads |
| --- | --- | --- |
| [ESL-Bench](https://huggingface.co/datasets/healthmemoryarena/ESL-Bench) | Event-driven longitudinal health agents — 100 synthetic users, 10,000 queries, programmatic ground truth ([arXiv:2604.02834](https://arxiv.org/abs/2604.02834)) | 4,800+ |
| [MedHall-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHall-Bench) | Medical hallucination | 4,500+ |
| [MedHarm-Bench](https://huggingface.co/datasets/healthmemoryarena/MedHarm-Bench) | Harmful medical advice | 4,300+ |

Reproduce any of them with one command via
**[mirobody-eval](https://github.com/thetahealth/mirobody-eval)**, which also
seeds a deployment with synthetic (PHI-free) trajectories.

---

## 🚀 Run the whole thing

```bash
git clone https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs pull          # the engine's data bundles; `resolve` needs them
./deploy.sh           # Postgres + pgvector, Redis, server, worker
```

Then open **http://localhost:18080**. The server prints the accounts it accepts
at startup — the shipped one is `caregiver@mirobody.ai`, code `111111`, named for
the role it plays: you sign in as the caregiver and the record you read belongs to
someone else.

No mail provider? You do not need one. The sign-in page opens on **password**,
with email-code as a third tab:

```bash
curl -X POST localhost:18080/password/register -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com","password":"at-least-8-chars"}'
```

An LLM key is what you need to hold a conversation. An embedding key is optional.
→ [Docker deployment](https://docs.mirobody.ai/en/deployment/docker/) ·
[Configuration](https://docs.mirobody.ai/en/configuration/) ·
[Local Python setup](https://docs.mirobody.ai/en/development/setup/)

### 👨‍👩‍👧 A demo that answers, then asks you for a file

`SEED_DEMO_DATA` defaults to on, so you arrive with one synthetic person already
in your care circle: **Demo (synthetic)**, 244 indicators across two years, five
documents the agent can `read_file`. You own nothing; the record is hers.

<div align="center">
<img src="docs/images/your-care-circle.svg" alt="You have no data of your own; the record you read is hers." width="820">
</div>

**Start with a question, not the data table.** On the Ask page:

> *"What was her latest LDL cholesterol and how does it compare to a year earlier?"*

```
2024-04-16   3.4 mmol/L
2024-10-15   3.2
2025-04-15   3.1
```

…and it volunteers that the newest panel is over a year old and worth repeating.
Which is the cue for the second half.

**Now hand it a file.** `mirobody/demo/lab_report_2025-10-15.pdf` is her *next*
panel, deliberately held out of the seed — so uploading it is not a no-op. Drop it
on the Data page and watch ① Collect and ② Standardize work: twelve analytes come
out with their units, each resolves to a code, and the LDL series gains a fourth
point. Ask again and the answer moves.

Every value is synthetic — generated for ESL-Bench by
[mirobody-eval](https://github.com/thetahealth/mirobody-eval) and vendored, so the
seed needs no network and no key. Set `SEED_DEMO_DATA=false` for a deployment that
will hold real data.

---

## 🧩 Extend it

Five directory keys point at plugin roots; drop a file in and restart. Tools
become both agent tools and MCP tools with no extra wiring.

| You want | Drop it in | Docs |
| --- | --- | --- |
| A new tool | `mirobody/agent/tools/` | [Adding tools](https://docs.mirobody.ai/en/tools/adding-tools/) |
| An Agent Skill (SKILL.md) | `mirobody/agent/skills/` | [Skills](https://docs.mirobody.ai/en/tools/skills/) |
| A whole agent | `mirobody/agent/` | [Agents](https://docs.mirobody.ai/en/tools/agents/) |
| A device provider | `mirobody/pulse/providers/` | [Provider integration](https://docs.mirobody.ai/en/development/provider-integration/) |
| Someone else's MCP server | Settings → MCP | [MCP integration](https://docs.mirobody.ai/en/tools/mcp-integration/) |

Every tool the agent has is also served over MCP at `/mcp`, gated per user.
→ [Built-in tools](https://docs.mirobody.ai/en/tools/built-in/) ·
[MCP servers](https://docs.mirobody.ai/en/api-reference/mcp-servers/)

---

## 🔌 Use it from your own code

| Surface | For | Docs |
| --- | --- | --- |
| `pip install mirobody` | Resolution and file parsing, no server | [Engine](https://docs.mirobody.ai/en/engine/) |
| HTTP API | Your app talking to a deployment | [API overview](https://docs.mirobody.ai/en/api-reference/overview/) · [Data](https://docs.mirobody.ai/en/api-reference/data/) |
| MCP | Claude, Cursor, or any MCP client reading a user's record | [MCP servers](https://docs.mirobody.ai/en/api-reference/mcp-servers/) |
| Backbone mode | Your own agent, our data layer | [Backbone](https://docs.mirobody.ai/en/api-reference/backbone-mode/) |

Not sure which? → [Choose your API](https://docs.mirobody.ai/en/api-reference/choose-your-api/)

---

## 🏗️ How the repo is laid out

```
mirobody/
├── pulse/       ① Collect     — providers, file parsing, aggregation
├── indicator/   ② Standardize — the resolver, units, taxonomy (no DB, no network)
├── agent/       ③ Answers     — DeepAgent, tools, skills, chat
├── mcp/         the MCP server
├── schema/      the DDL, replayed at boot in dev
└── demo/        the care-circle fixture
```

**One rule, machine-enforced:** `indicator/` never imports the agent layer, so
`pip install mirobody` stays a 233 MB engine instead of pulling a framework.
Two import-linter contracts hold the line — `lint-imports` fails the build.

→ [Architecture](https://docs.mirobody.ai/en/concepts/architecture/) ·
[CONTRIBUTING.md](CONTRIBUTING.md)

---

## 📚 Documentation

Everything deeper lives at **[docs.mirobody.ai](https://docs.mirobody.ai/)** —
50 pages, English and 简体中文.

| | |
| --- | --- |
| [Quickstart](https://docs.mirobody.ai/en/quickstart/) · [Installation](https://docs.mirobody.ai/en/installation/) · [Self-host](https://docs.mirobody.ai/en/self-host/) | Getting it running |
| [Indicators](https://docs.mirobody.ai/en/concepts/indicators/) · [Providers](https://docs.mirobody.ai/en/concepts/providers/) · [File processing](https://docs.mirobody.ai/en/concepts/file-processing/) | How the three stages work |
| [API reference](https://docs.mirobody.ai/en/api-reference/) · [Streaming](https://docs.mirobody.ai/en/api-reference/streaming/) · [Function calling](https://docs.mirobody.ai/en/api-reference/function-calling/) | Building against it |
| [Contributing](https://docs.mirobody.ai/en/development/contributing/) · [Setup](https://docs.mirobody.ai/en/development/setup/) | Working on it |

In-repo, for contributors: [CONTRIBUTING.md](CONTRIBUTING.md) · [docs/roadmap.md](docs/roadmap.md) ·
[SECURITY.md](SECURITY.md)

---

## 🤝 Contributing

The highest-leverage contribution is a term the resolver gets wrong. Run
`mirobody resolve "<term>"`, and if the answer is wrong or empty add a row to
[`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv) plus a case to
[`test_engine_coverage.py`](mirobody/test_engine_coverage.py) — the coverage score
is the review.

```bash
pip install -e '.[test]' && pytest -q && lint-imports
```

→ [Contributing guide](https://docs.mirobody.ai/en/development/contributing/) ·
[CONTRIBUTING.md](CONTRIBUTING.md)

---

<div align="center">

**[📚 Docs](https://docs.mirobody.ai/)** · **[💬 Chat](https://chat.mirobody.ai/)** · **[🔌 Platform](https://platform.mirobody.ai/)** · **[🧪 Eval](https://github.com/thetahealth/mirobody-eval)**

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>
