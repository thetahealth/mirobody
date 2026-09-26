<div align="center">

# Mirobody

**Self-hosted AI health data engine: every source, one standard, answers that cite their source.**

**English** · **[中文](README.zh-CN.md)**

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-3776AB.svg?logo=python&logoColor=white)](pyproject.toml)
[![PyPI Downloads](https://img.shields.io/pepy/dt/mirobody?label=PyPI%20Downloads&color=orange)](https://pepy.tech/projects/mirobody)
[![Docs](https://img.shields.io/badge/Docs-docs.mirobody.ai-black)](https://docs.mirobody.ai/)
[![GitHub stars](https://img.shields.io/github/stars/thetahealth/mirobody?style=social)](https://github.com/thetahealth/mirobody/stargazers)

**[📚 Documentation](https://docs.mirobody.ai/)** · **[▶ Live demo — no sign-up](https://chat.mirobody.ai/demo)** · **[🔌 API platform](https://platform.mirobody.ai/)**

</div>

---

Last year's checkup wrote `A1c`, this year's panel `HbA1c`, the new clinic
`Glycated Hemoglobin`. One test, three names, nothing to compare. Mirobody takes
health information from any source, in any format, under any name, and settles
it into one language and one system, then answers over that record, every number
citing its source: traceable, comparable, chartable. How has my blood pressure
moved? Are mom's diabetes markers improving? What changed across my child's
checkups? Self-host it all, and your health record stays in your hands.

<p align="center">
  <img src="docs/images/ask-own-demo.gif"
       alt="Asking how cholesterol has changed: the agent finds three files that name the test differently, resolves them to one code, and charts the trend" width="880">
</p>

<p align="center"><em>Three files, three names for the same test, one standard code. The agent finds
all three, aggregates the trend, and names the file every number came from.</em></p>

## What Mirobody does

- **One record for the whole family.** Invite a partner, a parent, even a child
  who never signs in at all, and keep the household's health history in one
  place.
- **Every source, one record.** Garmin, Oura and Whoop connect directly;
  anything already written into Apple Health comes with it; PDFs, phone photos,
  spreadsheets, exports: 23 file types in all, and Mirobody reads them.
- **Say how you feel, in your own words.** Type `headache since last night,
  BP 150/95, no fever` into the journal and it files a headache and two
  blood-pressure readings, each on its standard code, and leaves out the fever
  you said you do not have. Your model splits the sentence; the codes come from
  the vocabulary, never from the model.
- **No hallucinations, everything traceable.** Every indicator lands in one
  settled system: either it gets a definite code, or it says it could not
  resolve one. It never invents one in between. Built and tested against real
  reports, in English, Chinese and Japanese.
- **The agent reasons only over coded data.** Trends by minute, hour, day,
  week or month, drawn as a chart; a baseline and how far a number has moved in
  one call; comparisons across labs, files and devices, because one standard
  (LOINC and UCUM) sits under all of them. It reads medications and genetic
  variants too.
- **Runs on a laptop.** Four containers, 791 MiB resident, under 5% CPU idle.
  No GPU, no Node.js.
- **Your model, your key, your data.** Model calls go to the model you chose.
  Everything else stays on your machine.

## Try it in 60 seconds

One command, five spellings: watch which ones it recognises, and which one it
refuses. No key, no config, no network, and with `uvx`, no install either:

```bash
uvx mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

<p align="center">
  <img src="docs/images/resolve-demo.gif"
       alt="mirobody resolve: 血红蛋白 and ヘモグロビン landing on the same LOINC code, and one deliberate abstention" width="880">
</p>

`血红蛋白` and `ヘモグロビン`: two languages, one code, 718-7. `血脂` (lipids)
names a category, not one observation, so it resolves to **nothing**. The
resolver would rather return nothing than guess a code, because a wrong one
puts two different tests on the same trend line.

```python
from mirobody.engine import resolve, resolve_reading

resolve("血红蛋白").loinc                                # '718-7'   any language, one code
resolve("total cholesterol").loinc                     # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2' [Moles/volume]
resolve_reading("total cholesterol", "193", "mg/dL").loinc   # '2093-3'  the unit picks the code

resolve("中性粒细胞百分比").loinc                          # '26511-6' Neutrophils/Leukocytes
resolve_reading("中性粒细胞", "62 %", None).loinc          # '26511-6' a percentage...
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc       # '26499-4' ...and a count are two codes
resolve("血脂").resolved                                 # False    a category, not an observation

from mirobody import standardize_reading                # the same answer as a FHIR Observation
standardize_reading("血红蛋白", "13.5", "g/dL")["code"]["coding"][0]["code"]  # '718-7'
```

**Pass the value and the unit when you have them.** A different unit means a
different test, and LOINC folds that into the code's own identity, so one name
is deliberately several codes.
→ [Engine reference](https://docs.mirobody.ai/en/engine/) · [Indicators](https://docs.mirobody.ai/en/concepts/indicators/)

## Collect · Translate · Agent

<p align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/collect-translate-agent-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/collect-translate-agent.svg">
  <img src="docs/images/collect-translate-agent.svg" alt="Collect, Translate, Agent: three stages, left to right" width="920">
</picture>
</p>

An indicator takes three steps from arriving to being cited. Each one leaves a
trace, so the answer at the end can be followed back to the page it came off:

| Stage | What it does | Where |
| --- | --- | --- |
| **① Collect** | Lab reports, wearables, phone photos, genetic files, all pulled in. The source file is kept as it was, so every indicator points back to the page it was read from. | [`collect/`](mirobody/collect/) |
| **② Translate** | One name to one code, one unit to UCUM, offline and deterministic. `A1c`, `HbA1c` and `Glycated Hemoglobin` become the same test here, and `头疼` and `headache` the same complaint (ICPC-3). | [`engine/`](mirobody/engine/) · [`translate/`](mirobody/translate/) |
| **③ Agent** | Ask over the coded record. Trend a value by minute, hour, day, week or month; get count, min, max, avg or change over any window in one call; compare across labs and devices, because they share one code. It charts the result in its reply, reads medications and genetic variants too, and names the file every number came from. | [`agent/`](mirobody/agent/) |

① records how the source spelled it, ② decides what it actually is, ③ answers
on that footing. Comparing a number across two labs, charting three years of
it, computing a baseline: all of it rests on the code ② hands over.

**The agent does not have to be ours.** Every tool it uses is served at `/mcp`
as well, gated per user. Claude Desktop, Cursor or your own loop run the same
tools over the same record, and get back the same indicators. The vocabularies
need no server at all: `uvx mirobody mcp` serves them over stdio, with no
database and no key, to any MCP client.

Garmin, Oura and Whoop connect with your own credentials from each vendor;
[the setup guide](docs/provider-setup.md) walks it through. Apple Health goes
another way: a client on the phone hands the data over, so any band, ring or
scale reaches your record the moment it writes into Apple Health, with nothing
to integrate here at all.

## Privacy

Nothing leaves your machine except calls to the model you chose, and to a
device vendor once you link one. Reading a photo of a report, pulling
indicators out of a PDF, splitting a sentence you typed into the journal,
answering your question: all four call the model. Which provider and which
model is the one key in your `.env`. A linked Garmin, Oura or Whoop is called
through its own API, for what it recorded and nothing else.

**② Translate** stays local entirely: a name to a code, a unit to UCUM, looked
up against a bundle that ships inside the package. No key, no network, no GPU,
no model. Your record lives in your own Postgres, in containers you run, and
nothing here reports usage anywhere.

**One key, and it is the only secret you hold.** Put an
[OpenRouter key](https://openrouter.ai/keys) (`OPENROUTER_API_KEY`), a
[Gemini key](https://aistudio.google.com/apikey) (`GOOGLE_API_KEY`), an
[OpenAI key](https://platform.openai.com/api-keys) (`OPENAI_API_KEY`) or an
[Anthropic key](https://platform.claude.com/settings/keys)
(`ANTHROPIC_API_KEY`) in the `.env` beside `compose.yaml`, then
`docker compose restart`. DeepSeek, DashScope or any OpenAI-compatible gateway
works alone too. Which model chats, which reads report photos, which extracts
indicators and which embeds are four lines in
[`config.llm.yaml`](config.llm.yaml), and that file names the *variable*
(`api_key: OPENROUTER_API_KEY`), never the secret. `mirobody doctor` prints
what each surface selected, and names the fix where one has nothing.

The quickstart ships its secrets as placeholders, and encryption at rest does
not yet cover every field. Before this reaches a network you do not control,
read [SECURITY.md](SECURITY.md): it also lists exactly what the server calls
off your machine.

## 🚀 See it end to end

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull   # the resolver's LOINC bundle, 13 MB; a fresh clone holds a pointer stub until you do
./deploy.sh                       # Postgres + pgvector, Redis, server, worker → http://localhost:18060
```

(`--depth 1` skips the history of superseded frontend builds; drop it if you
plan to send a pull request.)

Two things `deploy.sh` will stop and tell you about, both with the fix in the
message: one checkout at a time, because `compose.yaml` pins the stack's
subnet, so a second one needs a different `mirobody_network` subnet; and a
Docker that refuses named volumes (rootless, hardened) needs bind mounts
instead, which is what `compose.override.yaml.example` is for.

Sign in on the **Email code** tab as `you@mirobody.ai`, code `111111`, no mail
provider needed. An account of your own is one request away:

```bash
curl -X POST localhost:18060/password/register -H 'Content-Type: application/json' \
     -d '{"email":"me@example.com","password":"at-least-8-chars"}'
```

`SEED_DEMO_DATA` is on by default, so two accounts are already there with
**2,019 indicators** between them: you, and `mom@mirobody.ai`, who shares her
record with you view-only. Set it to `false` to hold real data and neither
account is created. **Settings → Add member** covers someone who will never
sign in at all, a parent, a child, with a record you hold on their behalf.

Drop a file on the Data page and watch it become indicators.
[`demo/upload/`](demo/) holds four files the seed deliberately leaves out: a
lab PDF, a phone photo of a printed report, a spreadsheet and another lab's
CSV export. Each analyte comes out with a value, a unit and a LOINC code,
linked back to the page it was read from.

<p align="center">
  <img src="docs/images/upload-demo.gif"
       alt="Dropping a lab-report PDF on the Data page; its analytes are extracted and appear in the indicators table, each with a LOINC code" width="880">
</p>

Ask how the cholesterol has moved and the agent finds every file that carries
it: one lab writes `Cholesterol, Total` where the others write
`Total Cholesterol-TC`, and both are **14647-2**. It charts the trend and names
the file each number came off: `4.60 → 4.45 → 4.38 mmol/L`. Ask for a baseline
or a monthly average instead and the same tool aggregates over the whole
record, rather than handing back rows for the model to add up itself.

Ask the same question of the record shared with you and it is a different
person's answer, from data you can only view. That sharing is a **care
circle**: invite-only, off by default, and strictly permission-checked.

<p align="center">
  <img src="docs/images/ask-circle-demo.gif"
       alt="The same question asked on the shared record; the agent answers from a different person's files" width="880">
</p>

Each of those three words is one check, and they all live in one function.
`resolve_subject` is the only way an account reaches a record that is not its
own — being in a circle together grants nothing by itself.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/your-care-circle-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/images/your-care-circle.svg">
    <img src="docs/images/your-care-circle.svg" alt="How one person reaches another's health record: a request passes resolve_subject, which requires both memberships accepted and the subject's own health_access switch, and either returns access trimmed to the request or raises a 403" width="920">
  </picture>
</p>

→ [The four-minute walkthrough](docs/walkthrough.md) ·
[`examples/06_care_circle_rules.py`](examples/06_care_circle_rules.py) prints the
whole sharing decision table offline ·
[Docker deployment](https://docs.mirobody.ai/en/deployment/docker/) ·
[Configuration](https://docs.mirobody.ai/en/configuration/)

## Check any of it yourself

Every figure below comes with its source: a command you can run, or a public
dataset.

- **296/296** on the tests an ordinary checkup prints, in English, Chinese
  (Simplified and Traditional), Japanese, Russian and Estonian. The set is
  deliberately the least flattering one — everyday panels, written the way a
  report prints them, which is what every new user tries in their first minute.
  [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py) prints the
  score when you run it.
- **13 wearable vendors, read field by field**: 289 of 447 fields carry a LOINC
  code, each with a confidence and the vendor document it came from, and 71
  quantities are declined with the reason rather than guessed.
  [The device crosswalk](docs/device-crosswalk.md) is the table.
- **Three open benchmarks**, public datasets, one command each: longitudinal
  health agents, medical hallucination, harmful medical advice.
  [mirobody-eval](https://github.com/thetahealth/mirobody-eval) ·
  [datasets](https://huggingface.co/mirobody) ·
  [arXiv:2604.02834](https://arxiv.org/abs/2604.02834).
- **The package names the vocabulary that answered you**:
  `mirobody.BUNDLE_VERSION` → `loinc-2.83+2026.09.17-aacb2c715b56`, the release,
  the cut date, and a digest over the bundle's own contents.
- **316 standard device indicators** and 328 UCUM units with dimensional
  analysis. The full counts, and what the LOINC 2.83 cut keeps and drops, are
  in [Standardization in depth](docs/standardization.md).
- **`pip install mirobody` is 2 packages**, numpy the only dependency.

The engine powers **[Theta Wellness](https://www.thetahealth.ai/)**, a live
consumer health product with 5,000+ registered users.

## 🔌 Use it, extend it

| You want | Do this |
| --- | --- |
| Offline resolution and units in your code | `pip install mirobody` — no key, no network |
| A document turned into indicators | `pip install 'mirobody[parse]'` — PDF, image, Excel, Word, PowerPoint, text; only a scanned page reaches a vision model |
| These tools in Claude Desktop, Cursor or your own loop | Settings → MCP: every agent tool is also served at `/mcp`, gated per user |
| Coding in any MCP client, no server | `uvx mirobody mcp` (stdio): readings to FHIR Observations with their code, complaints to ICPC-3, units; no key, no database |
| Your app talking to a deployment | The HTTP API, against the deployment you run — your app, your data layer |
| A new tool or device provider | Drop a file into `mirobody/agent/tools/` or `mirobody/collect/providers/` and restart, or `pip install` a package declaring a `mirobody.providers` / `mirobody.tools` / `mirobody.agents` entry point |
| Your own agent harness | `pip install 'mirobody[agent]'` for the middleware and virtual-filesystem backends, or point `AGENT_DIRS` at your directory to replace the shipped agent outright |

→ [API overview](https://docs.mirobody.ai/en/api-reference/overview/) ·
[MCP integration](https://docs.mirobody.ai/en/tools/mcp-integration/) ·
[Adding tools](https://docs.mirobody.ai/en/tools/adding-tools/) ·
[Bringing your own agent](CONTRIBUTING.md#-bringing-your-own-agent)

## 🤝 Contributing

The highest-leverage contribution is a term the resolver gets wrong. Run
`mirobody resolve "<term>"`; if the answer is wrong or empty,
[report it](https://github.com/thetahealth/mirobody/issues/new?template=wrong-term.yml)
or add a row to [`resolver_overrides.tsv`](mirobody/res/loinc/resolver_overrides.tsv)
plus a case to [`test_engine_coverage.py`](mirobody/tests/test_engine_coverage.py) —
the coverage score is the review.

```bash
pip install -e '.[test]' && pytest -q && lint-imports
```

→ [CONTRIBUTING.md](CONTRIBUTING.md) · [Local Python setup](https://docs.mirobody.ai/en/development/setup/) ·
[Repository layout](docs/repository-layout.md) ·
[Roadmap](docs/roadmap.md) · [CHANGELOG](CHANGELOG.md) · [SECURITY](SECURITY.md)

## 📚 Documentation, and what shaped this

**[docs.mirobody.ai](https://docs.mirobody.ai/)**, in English and Chinese — start
at the [Quickstart](https://docs.mirobody.ai/en/quickstart/) or the
[API reference](https://docs.mirobody.ai/en/api-reference/). The Quickstart also
ships with the code, as [`docs/quickstart.md`](docs/quickstart.md), so it cannot
drift from the commands in this repository; the contributor guides are in
[`docs/`](docs/README.md).

Mirobody's design draws on the following standards and projects, with thanks:
[HL7 FHIR](https://hl7.org/fhir/),
[Regenstrief Institute](https://www.regenstrief.org/) ([LOINC](https://loinc.org/)),
[UCUM](https://ucum.org/), [OHDSI OMOP](https://www.ohdsi.org/),
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
Releases land most weeks — [Watch](https://github.com/thetahealth/mirobody/subscription) for them.*

Apache 2.0 · © 2026 [Theta Health](https://thetahealth.ai)

</div>

<!-- mcp-name: ai.thetahealth/mirobody -->
