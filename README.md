<div align="center">

# Mirobody

**The AI-native health data engine — Collect · Translate · Answer.**

Lab reports, wearables and genomics become one language AI can read:
LOINC-coded, UCUM-normalized, FHIR-ready. The resolver runs offline, and the
engine powers **[Theta Wellness](https://www.thetahealth.ai/)**, a live consumer
health product with 5,000+ registered users and 500+ daily active.

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Benchmarks](https://img.shields.io/badge/%F0%9F%A4%97_Benchmarks-4k%2B_downloads_each-FFD21E.svg)](https://huggingface.co/mirobody)
[![arXiv](https://img.shields.io/badge/arXiv-2604.02834-b31b1b.svg)](https://arxiv.org/abs/2604.02834)
[![Docs](https://img.shields.io/badge/Docs-docs.mirobody.ai-black)](https://docs.mirobody.ai/)
[![GitHub stars](https://img.shields.io/github/stars/thetahealth/mirobody?style=social)](https://github.com/thetahealth/mirobody/stargazers)

**[📚 Documentation](https://docs.mirobody.ai/)** · **[▶ Live demo](https://chat.mirobody.ai/)** · **[🔌 API platform](https://platform.mirobody.ai/)**

**English** · **[中文](README.zh-CN.md)**

</div>

## ⚡ Try it in 60 seconds

No key, no config, no network:

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

<p align="center">
  <img src="docs/images/resolve-demo.gif"
       alt="mirobody resolve: four languages landing on one LOINC code, fully offline" width="880">
</p>

`血红蛋白` and `ヘモグロビン` land on the same code as `hemoglobin`, LOINC `718-7`;
`空腹血糖(GLU)` on fasting glucose. `血脂` (lipids) is a category, not an
observation, so the resolver returns nothing rather than a plausible wrong code.

```python
from mirobody.engine import resolve, resolve_reading

resolve("血红蛋白").loinc                                # '718-7'   any language, one code
resolve("total cholesterol").loinc                     # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L")   # '14647-2' [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL")    # '2093-3'  the unit picks the code

resolve("中性粒细胞百分比").loinc                          # '26511-6' Neutrophils/Leukocytes
resolve_reading("中性粒细胞", "62 %", None).loinc          # '26511-6' a percentage...
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc       # '26499-4' ...and a count are two codes
resolve("血脂").resolved                                 # False    a category, not an observation
```

**Pass the value and the unit when you have them.** LOINC encodes the unit and
the result type into the identity, so the same name resolves to different codes.
`resolve` abstains rather than guessing: an empty code is a gap worth a second
look; `method="refused"` is a decision.
→ [Engine reference](https://docs.mirobody.ai/en/engine/) · [Indicators](https://docs.mirobody.ai/en/concepts/indicators/)

## Why Mirobody

- **Two reports, same test, different spelling and units.** That is the problem.
  Mirobody translates any reading, in any language, to LOINC + UCUM — and refuses
  to guess when it does not know the term.
- **AI can only reason over what it can read.** The agent reads the original
  documents, charts lab draws against sensor-derived series, and cites the page
  it read from.
- **Self-hosted, standards-based, Apache-2.0.** One `./deploy.sh` runs the whole
  stack on your own machine; what comes out is LOINC-coded, FHIR-ready records
  you can take anywhere, and the same tools are served over MCP to Claude
  Desktop, Cursor or your own agent.

## Collect · Translate · Answer

<p align="center">
  <img src="docs/images/where-your-data-comes-from.svg" alt="From wearables to food photos — one standard format, ready for AI." width="920">
</p>

The engine does three things, and the codebase, the docs and
[Contributing](#-contributing) are organized around exactly these three stages:

| Stage | What it means | Where |
| --- | --- | --- |
| **① Collect** | Pull signals in: 3 device providers + a SQL source · 7 file formats · Apple Health (receive-only: a signed iOS client POSTs it in) | [`pulse/`](mirobody/pulse/) |
| **② Translate** (standardize) | One standard: resolve any reading to canonical codes (LOINC · SNOMED CT · RxNorm), normalize units to UCUM, land on FHIR-recognized code systems | [`indicator/`](mirobody/indicator/) |
| **③ Answer** (agent) | Reason: an agent reads the *original documents* through a virtual filesystem and answers with charts and citations | [`agent/`](mirobody/agent/) |

## By the numbers

| | |
| --- | --- |
| Concept graph | 440,961 nodes · 22,044,110 cross-vocabulary edges · 595,746 source ids distilled into canonical concepts (LOINC · SNOMED CT · RxNorm bridges) |
| Aliases | 49,253 multilingual (中文 22,578 · 日本語 16,809 · de·es·fr·ko·ru); `hemoglobin`, `血红蛋白`, `血紅素` and `ヘモグロビン` all land on 718-7 |
| Traditional Chinese | a shipped 3,336-character zh-Hant → zh-Hans fold table, plus curated Traditional rows — a curated row always beats a fold |
| Units | ~310 UCUM families with dimensional analysis and a molar-mass bridge keyed by LOINC code; 305 standard pulse indicators |
| Coverage | **211/211** on the panels an ordinary checkup prints, in English, Chinese (Simplified and Traditional) and Japanese ([`test_engine_coverage.py`](mirobody/test_engine_coverage.py)) |
| Bundle | LOINC 2.82: `mirobody.BUNDLE_VERSION` → `loinc-2.82+2026.08.28-af2524b7a285` — release, cut date and a digest over the bundle's own members |
| Install | `pip install mirobody` is **2 packages, 52 MB**, on numpy only |

Why 2.82 and not 2.83, what LOINC covers of the wearable world, and the opt-in
semantic tier that cannot abstain: → [Standardization in depth](docs/standardization.md)

## 📊 Benchmarks — open and independently reproducible

The most-downloaded health-AI benchmarks in their category on Hugging Face,
4,000+ downloads a month each:
[ESL-Bench](https://huggingface.co/datasets/mirobody/ESL-Bench) (event-driven
longitudinal health agents — 100 synthetic users, 10,000 queries,
[arXiv:2604.02834](https://arxiv.org/abs/2604.02834)) ·
[MedHall-Bench](https://huggingface.co/datasets/mirobody/MedHall-Bench) (medical
hallucination) · [MedHarm-Bench](https://huggingface.co/datasets/mirobody/MedHarm-Bench)
(harmful medical advice). Reproduce any of them with one command via
**[mirobody-eval](https://github.com/thetahealth/mirobody-eval)**, which also seeds
a deployment with synthetic, PHI-free trajectories.

## 🚀 Run the whole thing

```bash
git clone https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull   # the engine's data bundles; a fresh clone holds pointer stubs until you do
./deploy.sh                       # Postgres + pgvector, Redis, server, worker → http://localhost:18060
```

Sign in as `caregiver@mirobody.ai`, code `111111`. No mail provider needed: the
sign-in page opens on password, and an account of your own is one request away:

```bash
curl -X POST localhost:18060/password/register -H 'Content-Type: application/json' \
     -d '{"email":"you@example.com","password":"at-least-8-chars"}'
```

**Your care circle.** The account is named for the role it plays: you sign in as
the caregiver, and the record you read belongs to someone else. A circle is the
unit of sharing — each member holds their own record and decides, on their own
row, whether the others may view it.

<div align="center">
<img src="docs/images/your-care-circle.svg" alt="Your own thin record next to hers — the thick one you can only view." width="820">
</div>

`SEED_DEMO_DATA` is on by default, so the circle is already populated: you own a
**thin** record — a few weeks of self-tracked vitals and one unremarkable
checkup — and one synthetic person shares a **thick** one with you,
**Demo (synthetic)**: **244 indicators, 14,273 readings** across two years, five
documents the agent can read. Same question, two records: *your* HbA1c answers
with one boring-normal value from data you own; *hers* with a two-year story from
data you can only view. The recording walks both sides — your own indicators and
files, then the switch to her shared record and its two years of HbA1c:

<p align="center">
  <img src="docs/images/care-circle-demo.gif"
       alt="Your own account's indicators and an uploaded report, then switching to Demo's shared record and opening two years of HbA1c" width="880">
</p>

The switch in that diagram is a column, not a promise:
`care_circle_members.health_access`, `NOT NULL DEFAULT 0`, on **your own** row.
Being invited into a circle shares nothing — the member decides, and no other
person's action can raise it. A route that forgets to check answers 403 instead of
handing over a record. [`examples/06_care_circle_rules.py`](examples/06_care_circle_rules.py)
prints the whole decision table offline. Set `SEED_DEMO_DATA=false` for a
deployment that will hold real data.

**One key runs everything.** Browsing the seeded record needs no key; the upload
and the questions below ride one. Put ONE key in the `.env` next to `compose.yaml`
and `docker compose restart` — the app re-reads `/app/.env`; a shell `export` does
not reach the containers. The `.env` is the only place for the key:
`config.llm.yaml` names the variable (`api_key: OPENROUTER_API_KEY`), never the
secret. An [OpenRouter key](https://openrouter.ai/keys) as
`OPENROUTER_API_KEY` is the recommended one; a DashScope key when openrouter.ai is
unreachable from your network; a Google, [OpenAI](https://platform.openai.com/api-keys)
(`OPENAI_API_KEY`), [Anthropic](https://platform.claude.com/settings/keys)
(`ANTHROPIC_API_KEY`) or DeepSeek key works alone as well. Every model decision —
which model chats, which reads report photos, which extracts the indicators, which
embeds — is a line in [`config.llm.yaml`](config.llm.yaml), where you can read and
change it (a self-hosted gateway is one `<PREFIX>_BASE_URL` in `.env`). The boot
log, and `mirobody doctor`, print what each surface selected and name the fix
where one has nothing.

**① Collect + ② Translate.** Drop [`demo/lab_report_2025-10-15.pdf`](demo/lab_report_2025-10-15.pdf)
on the Data page and twelve analytes come out with values and units, each
linking back to the page it was read from:

<p align="center">
  <img src="docs/images/upload-demo.gif"
       alt="Dropping a lab-report PDF on the Data page; twelve analytes extracted, each linked to its source file" width="880">
</p>

**③ Answer (agent).** Ask about her HbA1c and the agent finds the data, charts
the three lab draws against 104 sensor-derived estimates, and says plainly that
the improvement did not hold. Ask again about the report you just uploaded and it
reads that instead — the fourth scene of
[the four-minute walkthrough](docs/walkthrough.md).

<p align="center">
  <img src="docs/images/ask-circle-demo.gif"
       alt="Asking about the shared record's HbA1c; the agent queries, charts lab and sensor series together, and reads the trend" width="880">
</p>

→ [Docker deployment](https://docs.mirobody.ai/en/deployment/docker/) ·
[Configuration](https://docs.mirobody.ai/en/configuration/) ·
[Local Python setup](https://docs.mirobody.ai/en/development/setup/)

## 🔌 Use it, extend it

| You want | Do this | Docs |
| --- | --- | --- |
| Offline resolution and units in your code | `pip install mirobody` — 2 packages, no key, no network | [Engine](https://docs.mirobody.ai/en/engine/) |
| A document turned into readings | `pip install 'mirobody[parse]'` — PDF, image, Excel, Word, PowerPoint, text; only a scanned page reaches a vision model | [Engine](https://docs.mirobody.ai/en/engine/) |
| The agent harness as a library | `pip install 'mirobody[agent]'` — middleware, virtual-filesystem backends, checkpointer | [Bringing your own agent](CONTRIBUTING.md#-bringing-your-own-agent) |
| These tools in Claude Desktop, Cursor or your own loop | Settings → MCP: every agent tool is also served at `/mcp`, gated per user | [MCP servers](https://docs.mirobody.ai/en/api-reference/mcp-servers/) · [`examples/07_claude_agent_sdk.py`](examples/07_claude_agent_sdk.py) |
| Your app talking to a deployment | HTTP API, or backbone mode: your agent, our data layer | [API overview](https://docs.mirobody.ai/en/api-reference/overview/) · [Backbone](https://docs.mirobody.ai/en/api-reference/backbone-mode/) |
| A new tool, skill or device provider | Drop a file into `mirobody/agent/tools/`, `mirobody/agent/skills/` or `mirobody/pulse/providers/` and restart — or `pip install` a package declaring a `mirobody.providers` / `mirobody.tools` / `mirobody.agents` entry point | [Adding tools](https://docs.mirobody.ai/en/tools/adding-tools/) · [Skills](https://docs.mirobody.ai/en/tools/skills/) · [Providers](https://docs.mirobody.ai/en/development/provider-integration/) |
| Your own agent harness | `AGENT_DIRS` → your directory replaces the shipped agent | [`mirobody/agent/README.md`](mirobody/agent/README.md) |
| The LOINC axis table and alias sources at build time | `mirobody.bundle` — for generating a seed or a corpus | [`mirobody/bundle.py`](mirobody/bundle.py) |

## 🤝 Contributing

The highest-leverage contribution is a term the resolver gets wrong. Run
`mirobody resolve "<term>"`; if the answer is wrong or empty,
[report it](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml)
or add a row to [`resolver_overrides.tsv`](mirobody/res/resolver_overrides.tsv) plus a
case to [`test_engine_coverage.py`](mirobody/test_engine_coverage.py) — the
coverage score is the review.

```bash
pip install -e '.[test]' && pytest -q && lint-imports
```

→ [CONTRIBUTING.md](CONTRIBUTING.md) · [Contributing guide](https://docs.mirobody.ai/en/development/contributing/) ·
[Repository layout](docs/repository-layout.md) · [Roadmap](docs/roadmap.md) · [CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md)

## 📚 Documentation

**[docs.mirobody.ai](https://docs.mirobody.ai/)**, in English and Chinese — start at the
[Quickstart](https://docs.mirobody.ai/en/quickstart/), or go straight to the
[API reference](https://docs.mirobody.ai/en/api-reference/). Long-form guides for
contributors live in [`docs/`](docs/README.md).

## 🙏 Acknowledgements

Projects that shaped the kernel's rules — none of their code is included here:
[Open Wearables](https://github.com/the-momentum/open-wearables) (the
data-standardization failure modes `kernel/series` and `kernel/quality` close),
[Home Assistant](https://github.com/home-assistant/core) (`state_class`),
[Open mHealth](https://github.com/openmhealth/schemas) / IEEE 1752 (field names),
[wearipedia](https://github.com/Stanford-Health/wearipedia) (synthetic payloads),
[dlt](https://github.com/dlt-hub/dlt) / [Airbyte](https://github.com/airbytehq/airbyte-python-cdk) / [Singer](https://github.com/meltano/sdk) (connector shape),
[deepagents](https://github.com/langchain-ai/deepagents), LangChain and [langchain-quickjs](https://github.com/langchain-ai/langchain-quickjs) (the harness, the file-system projection, the `eval` REPL),
Regenstrief Institute (LOINC), UCUM, HL7 FHIR, OHDSI OMOP — see `LICENSE-3RD-PARTY`.

## ⭐ Star History

<div align="center">
<a href="https://www.star-history.com/#thetahealth/mirobody&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
  </picture>
</a>

*If it read a report for you, a star helps the next person find it.*

**[📚 Docs](https://docs.mirobody.ai/)** · **[▶ Demo](https://chat.mirobody.ai/)** · **[🔌 Platform](https://platform.mirobody.ai/)** · **[🧪 Eval](https://github.com/thetahealth/mirobody-eval)**

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>
