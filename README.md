<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/mirobody-icon-dark.svg">
  <img src="docs/images/mirobody-icon.svg" alt="Mirobody" width="72">
</picture>

# Mirobody

**Self-hosted AI health data engine: lab reports, wearables and genetic files in one record, and an agent that cites its sources.**

**English** · **[中文](README.zh-CN.md)**

[![PyPI](https://img.shields.io/pypi/v/mirobody?label=PyPI&color=3775A9)](https://pypi.org/project/mirobody/)
[![Docker Hub](https://img.shields.io/docker/v/thetahealth4mirobody/mirobody?label=Docker%20Hub&logo=docker&logoColor=white&color=2496ED)](https://hub.docker.com/r/thetahealth4mirobody/mirobody)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/thetahealth/mirobody?style=social)](https://github.com/thetahealth/mirobody/stargazers)

**[▶ Live demo, no sign-up](https://chat.mirobody.ai/demo)** · **[📚 Documentation](https://docs.mirobody.ai/en/self-host)** · **[🐳 Docker Hub](https://hub.docker.com/r/thetahealth4mirobody/mirobody)** · **[☁ Cloud API](https://platform.mirobody.ai/)**

</div>

---

Last year's checkup wrote `A1c`, this year's panel `HbA1c`, the new clinic
`Glycated Hemoglobin`. One test, three names, nothing to compare. Mirobody
reads health data from any source, in any format and any language, settles
every value onto one standard, and answers questions over that record with
each number traced back to the file it came from. How has my blood pressure
moved? Are mom's diabetes markers improving? What changed across my child's
checkups? All of it runs on your machine, on a model key you choose.

<p align="center">
  <img src="docs/images/ask-own-demo.gif"
       alt="Asking how cholesterol has changed: the agent finds three files that name the test differently, resolves them to one code, and charts the trend" width="880">
</p>

<p align="center"><em>Three files, three names for the same test, one code. The agent finds all
three, charts the trend, and names the file every number came from.</em></p>

## Run it in three commands

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh                          # pulls the image; Postgres, server and worker → http://localhost:18060
echo 'OPENROUTER_API_KEY=sk-or-...' >> .env && docker compose up -d   # one model key: extraction and answers
```

Docker is the only requirement: no Python, no Node.js, no GPU, no Git LFS. The
image `thetahealth4mirobody/mirobody:1.5.3` carries the vocabularies and the
web client. `deploy.sh` writes the database, signing and encryption secrets
into `.env`, switches to the `docker.1ms.run` mirror when Docker Hub is out of
reach, and builds from the checkout when the image cannot be pulled at all.
(Drop `--depth 1` if you plan to send a pull request.)

1. **Sign in** on the Email code tab as `you@mirobody.ai`, code `111111`.
   `SEED_DEMO_DATA` is on by default, so two accounts already hold
   **2,019 readings**: you, and `mom@mirobody.ai`, who shares her record with
   you view-only. Set it to `false` before the first start to hold real data.
2. **Drop a file on the Data page.** [`demo/upload/`](demo/) holds four files
   the seed leaves out: a lab PDF, a phone photo of a printed report, a
   spreadsheet and another lab's CSV. Each analyte comes out with a value, a
   unit and a code, linked to the page it was read from.
3. **Ask.** "How has my cholesterol moved?" finds every file that carries it,
   whatever the lab called it, charts the trend and names the file behind each
   number. Ask the same about the record shared with you and the answer is a
   different person's, from data you can only view.

<p align="center">
  <img src="docs/images/upload-demo.gif"
       alt="Dropping a lab-report PDF on the Data page; its analytes are extracted and appear in the indicators table, each with a LOINC code" width="880">
</p>

<p align="center">
  <img src="docs/images/ask-circle-demo.gif"
       alt="The same question asked on the shared record; the agent answers from a different person's files" width="880">
</p>

**Which key.** Any one of these runs every surface:
[OpenRouter](https://openrouter.ai/keys) (`OPENROUTER_API_KEY`),
[OpenAI](https://platform.openai.com/api-keys) (`OPENAI_API_KEY`),
[Gemini](https://aistudio.google.com/apikey) (`GOOGLE_API_KEY`),
[Anthropic](https://platform.claude.com/settings/keys) (`ANTHROPIC_API_KEY`),
DeepSeek, DashScope, or any OpenAI-compatible gateway through
`<PROVIDER>_BASE_URL`. Which model chats, which reads report photos, which
extracts indicators and which embeds are four lines in
[`config.llm.yaml`](config.llm.yaml), and that file names the variable
(`api_key: OPENROUTER_API_KEY`), never the secret.
`docker compose exec mirobody mirobody doctor` prints what each surface
selected. Without a key you can still sign in, browse the seeded record and
chart it; extraction, the journal and the agent wait for one.

→ [Self-host guide](https://docs.mirobody.ai/en/self-host) ·
[Configuration](https://docs.mirobody.ai/en/configuration) ·
[Deploy on a server](https://docs.mirobody.ai/en/deployment/production) ·
[Troubleshooting](https://docs.mirobody.ai/en/troubleshooting) ·
[Upgrading a 1.5.2 stack](docs/backup-restore.md#upgrading-from-152)

## What you get

- **Every source, one record.** Garmin, Oura and Whoop connect directly;
  anything a band, ring or scale writes into Apple Health comes with it; PDFs,
  phone photos, spreadsheets and exports, 23 file types in all, are read by
  kind. The source file is kept as it was.
- **One record for the whole family.** Invite a partner or a parent, or add a
  child who never signs in, and hold the household's history in one place.
  Sharing is a **care circle**: invite-only, off by default, and one function,
  `resolve_subject`, is the only way an account reaches a record that is not
  its own.
- **Say how you feel, in your own words.** Type `headache since last night,
  BP 150/95, no fever, metformin 500 mg morning and evening` into the journal.
  It files a headache and two blood-pressure readings, each on its standard
  code, puts metformin on your medication list, and leaves out the fever you
  said you do not have. Your model splits the sentence; the codes come from
  the vocabulary, never from the model.
- **No invented codes.** Every reading lands in one settled system, or the
  engine says it could not place it and keeps your words. Built and tested
  against real reports in English, Chinese, Japanese and Russian.
- **Genotypes as facts, not verdicts.** Upload a 23andMe, AncestryDNA,
  MyHeritage, FTDNA or WeGene export, or a VCF, and ask by rsID, gene or
  region. Calls at 489 pharmacogene sites are checked against both genome
  builds; every other row is kept as uploaded. A drug question gets CPIC
  coverage (which sites were called, missing or unreadable), never a
  phenotype or a change of medication. [How genetics works](docs/genetics.md).
- **The agent reasons only over coded data, and it need not be ours.** Trends
  from minutes to months, drawn as a chart; a baseline and how far a number
  moved, in one call; comparisons across labs, files and devices, because one
  code sits under all of them. Every tool is also served at `/mcp`, gated per
  user, to Claude Desktop, Cursor or your own loop.
- **Your model, your key, your data.** Model calls go to the provider you
  chose. Everything else stays on your machine.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/your-care-circle-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/images/your-care-circle.svg">
    <img src="docs/images/your-care-circle.svg" alt="How one person reaches another's health record: a request passes resolve_subject, which requires both memberships accepted and the subject's own health_access switch, and either returns access trimmed to the request or raises a 403" width="920">
  </picture>
</p>

→ [The four-minute walkthrough](docs/walkthrough.md) ·
[`examples/06_care_circle_rules.py`](examples/06_care_circle_rules.py) prints
the whole sharing decision table offline ·
[Device setup](docs/provider-setup.md) for Garmin, Oura and Whoop

## Collect · Translate · Agent

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/collect-translate-agent-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/collect-translate-agent.svg">
  <img src="docs/images/collect-translate-agent.svg" alt="Collect, Translate, Agent: three stages, left to right" width="920">
</picture>
</p>

A reading takes three steps from arriving to being cited, and each one leaves
a trace, so the answer at the end can be followed back to the page it came
off:

| Stage | What it does | Where |
| --- | --- | --- |
| **① Collect** | Lab reports, wearables, phone photos, genetic files, a sentence in the journal, all pulled in. The source is kept as it was, so every reading points back to the page it was read from. | [`collect/`](mirobody/collect/) |
| **② Translate** | One name to one code, one unit to UCUM, offline and deterministic. `A1c`, `HbA1c` and `Glycated Hemoglobin` become the same test here (LOINC), and `头疼` and `headache` the same complaint (ICPC-3). | [`engine/`](mirobody/engine/) · [`translate/`](mirobody/translate/) |
| **③ Agent** | Ask over the coded record. Trend a value by minute, hour, day, week or month; get count, min, max, avg or change over any window in one call; compare across labs and devices, because they share one code. It charts the result in its reply, reads medications and genetic variants too, and names the file every number came from. | [`agent/`](mirobody/agent/) |

## Try the engine alone

One command, five spellings, no key, no network, and with `uvx` no install
either. Watch which ones it recognises, and which one it refuses:

```bash
uvx mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

<p align="center">
  <img src="docs/images/resolve-demo.gif"
       alt="mirobody resolve: 血红蛋白 and ヘモグロビン landing on the same LOINC code, and one deliberate abstention" width="880">
</p>

`血红蛋白` and `ヘモグロビン`: two languages, one code, 718-7. `血脂` (lipids)
names a category, not one observation, so it resolves to **nothing**. A wrong
code would put two different tests on the same trend line, so the resolver
returns nothing rather than a guess.

```python
from mirobody.engine import resolve, resolve_reading, standardize_reading

resolve("血红蛋白").loinc                                     # '718-7'   any language, one code
resolve("total cholesterol").loinc                          # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2' [Moles/volume]: the unit picks the code
resolve_reading("total cholesterol", "193", "mg/dL").loinc   # '2093-3'
resolve("中性粒细胞百分比").loinc                               # '26511-6' Neutrophils/Leukocytes
resolve_reading("中性粒细胞", "62 %", None).loinc              # '26511-6' a percentage...
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc           # '26499-4' ...and a count are two codes
resolve("血脂").resolved                                     # False    a category, not an observation
standardize_reading("血红蛋白", "13.5", "g/dL")["code"]["coding"][0]["code"]  # '718-7'  the same answer as a FHIR Observation
```

Complaints and diagnoses have their own axis, ICPC-3, and the same rule: a
code or a stated refusal, never a guess.

```python
from mirobody.translate import resolve_symptom, resolve_condition

resolve_symptom("头疼").code           # 'NS01'         Headache; 'headache', '頭痛' and 'головная боль' answer the same
resolve_symptom("疼").outcome         # 'refused'      too broad to code; the words are kept, the code is not invented
resolve_condition("2型糖尿病").code    # 'TD72'         Type 2 diabetes mellitus
resolve_condition("糖尿病").outcome    # 'needs-input'  which type? asked, not assumed
```

**Pass the value and the unit when you have them.** A different unit means a
different test, and LOINC folds that into the code's identity, so one name is
deliberately several codes. `uvx mirobody mcp` serves the same vocabularies
over stdio to any MCP client: readings to FHIR Observations with their code,
complaints to ICPC-3, units to UCUM. No key, no database.

→ [Standardization in depth](docs/standardization.md) ·
[The library](https://docs.mirobody.ai/en/quickstart#a--the-library) ·
[`examples/`](examples/README.md)

## Privacy

Nothing leaves your machine except calls to the model you chose, and to a
device vendor once you link one. Reading a report photo, extracting indicators
from a PDF, splitting a journal sentence and answering a question call the
model; a linked Garmin, Oura or Whoop is called through its own API, for what
it recorded and nothing else. **② Translate stays local entirely**: a name to
a code, a unit to UCUM, looked up in a bundle that ships inside the package,
with no key, no network and no model. Your record lives in your own Postgres,
in containers you run, and nothing here reports usage anywhere. Encryption at
rest does not yet cover every field; before this reaches a network you do not
control, read [SECURITY.md](SECURITY.md), which also lists every call the
server makes off your machine.

## Numbers you can check

Every figure below comes with the command or the public dataset that produces
it.

| Claim | Check it |
| --- | --- |
| **296/296** on the tests an ordinary checkup prints, written the way a report prints them, in English, Chinese (Simplified and Traditional), Japanese, Russian and Estonian | [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py) prints the score when you run it |
| **328 UCUM units** with dimensional analysis, a molar-mass bridge, and an explicit refusal to convert a percentage into a count | [Standardization in depth](docs/standardization.md) |
| **316 standard device indicators**, and 13 wearable vendors read field by field: 289 of 447 fields carry a code, each with a confidence and the vendor document it came from; 71 are declined with the reason | [Device crosswalk](docs/device-crosswalk.md) |
| **13 readings and 26 complaint phrases** in five languages with their LOINC/UCUM or ICPC-3 outcome, including the phrases refused rather than guessed | [`benchmarks/health_records`](benchmarks/health_records/README.md) |
| **One genotype truth in ten file shapes**: 13 public 1000 Genomes calls across 12 genes, as five vendor layouts and as VCF in both builds, gzip, BGZF and ZIP | [`benchmarks/genomics`](benchmarks/genomics/README.md), offline |
| **Three open benchmarks** on public datasets: longitudinal health agents, medical hallucination, harmful medical advice | [mirobody-eval](https://github.com/thetahealth/mirobody-eval) · [datasets](https://huggingface.co/mirobody) · [arXiv:2604.02834](https://arxiv.org/abs/2604.02834) |
| The package names the vocabulary that answered you: `mirobody.BUNDLE_VERSION` is `loinc-2.83+2026.09.17-aacb2c715b56`, the release, the cut date and a digest over the bundle | `python -c "import mirobody; print(mirobody.BUNDLE_VERSION)"` |
| **`pip install mirobody` is 2 packages**, numpy the only dependency | `pip install mirobody && pip list` |

The engine powers **[Theta Wellness](https://www.thetahealth.ai/)**, a live
consumer health product with 5,000+ registered users.

## Use it, extend it

| You want | Do this |
| --- | --- |
| Offline resolution and units in your code | `pip install mirobody`: no key, no network, two packages |
| A document turned into readings | `pip install 'mirobody[parse]'`: PDF, image, Excel, Word, PowerPoint, text; only a scanned page reaches a vision model |
| These tools in Claude Desktop, Cursor or your own loop | Settings → MCP: every agent tool is also served at `/mcp`, gated per user |
| Coding in any MCP client, no server | `uvx mirobody mcp` (stdio): readings to FHIR Observations, complaints to ICPC-3, units; no key, no database |
| Your app talking to a deployment | The HTTP API, against the deployment you run: [self-hosted HTTP API](https://docs.mirobody.ai/en/http-api) |
| A new tool or device provider | Drop a file into `mirobody/agent/tools/` or `mirobody/collect/providers/` and restart, or `pip install` a package declaring a `mirobody.providers` / `mirobody.tools` / `mirobody.agents` entry point |
| Your own agent harness | `pip install 'mirobody[agent]'` for the middleware and virtual-filesystem backends, or point `AGENT_DIRS` at your directory to replace the shipped agent outright |
| The same engine without servers | [Mirobody Cloud](https://docs.mirobody.ai/en/api-reference/overview), the hosted `/v1` API |

→ [MCP integration](https://docs.mirobody.ai/en/tools/mcp-integration) ·
[Adding tools](https://docs.mirobody.ai/en/tools/adding-tools) ·
[The agent](https://docs.mirobody.ai/en/tools/agents) ·
[Bringing your own agent](CONTRIBUTING.md#-bringing-your-own-agent)

## Contributing

The highest-leverage contribution is a term the resolver gets wrong. Run
`mirobody resolve "<term>"`, or `resolve_symptom` for a complaint; if the
answer is wrong or empty,
[report it](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml)
or add a row to [`resolver_overrides.tsv`](mirobody/res/loinc/resolver_overrides.tsv)
plus a case to [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py).
A complaint phrase goes into [`benchmarks/health_records/cases.json`](benchmarks/health_records/cases.json).
The coverage score is the review.

```bash
pip install -e '.[app,test]'
pytest -q                                                            # the gates a clone ships
python -m unittest benchmarks.health_records.test_cases              # LOINC/UCUM and ICPC-3 decisions, five languages
python -m unittest discover -s benchmarks/genomics -p 'test_*.py'    # one genotype truth, ten file shapes
lint-imports && ruff check mirobody examples
```

→ [CONTRIBUTING.md](CONTRIBUTING.md) · [`benchmarks/`](benchmarks/README.md) ·
[AGENTS.md](AGENTS.md) for coding agents ·
[Repository layout](docs/repository-layout.md) · [Roadmap](docs/roadmap.md) ·
[CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md)

## Documentation, and what shaped this

**[docs.mirobody.ai](https://docs.mirobody.ai/en/self-host)**, in English and
Chinese: [self-hosting](https://docs.mirobody.ai/en/self-host),
[the quickstart](https://docs.mirobody.ai/en/quickstart),
[the walkthrough](https://docs.mirobody.ai/en/walkthrough) and the hosted
[API reference](https://docs.mirobody.ai/en/api-reference). The Open Source
pages there are rendered from this repository's own [`docs/`](docs/README.md)
at each release, so they cannot drift from the commands in the tree.

Mirobody's design draws on the following standards and projects, with thanks:
[HL7 FHIR](https://hl7.org/fhir/),
[Regenstrief Institute](https://www.regenstrief.org/) ([LOINC](https://loinc.org/)),
[UCUM](https://ucum.org/), [ICPC-3](https://icpc-3.info/) (WONCA),
[CPIC](https://cpicpgx.org/), [OHDSI OMOP](https://www.ohdsi.org/),
[Open Wearables](https://github.com/the-momentum/open-wearables),
[Open mHealth](https://github.com/openmhealth/schemas) / IEEE 1752,
[wearipedia](https://github.com/Stanford-Health/wearipedia),
[dlt](https://github.com/dlt-hub/dlt) / [Airbyte](https://github.com/airbytehq/airbyte-python-cdk) / [Singer](https://github.com/meltano/sdk),
[deepagents](https://github.com/langchain-ai/deepagents) and LangChain. The
terminology licences this ships under are in
[`LICENSE-3RD-PARTY`](LICENSE-3RD-PARTY).

<div align="center">

<a href="https://www.star-history.com/#thetahealth/mirobody&Date">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=thetahealth/mirobody&type=Date" />
  </picture>
</a>

*If it read a report for you, a star helps the next person find it.
Releases land most weeks; [Watch](https://github.com/thetahealth/mirobody/subscription) for them.*

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>

<!-- mcp-name: ai.thetahealth/mirobody -->
