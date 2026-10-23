# Local harness regression: 100 questions

This directory evaluates the product HTTP chat path with **100 source-grounded
questions** against eight synthetic people, using the frontend's exact locale codes:
**60 English (`en`), 30 Simplified Chinese (`zh-CN`), 5 Japanese (`ja`) and
5 Traditional Chinese (`zh-TW`)**. `--smoke` selects 30 questions covering
every family; it includes genetics, pharmacogenomics, OCR documents and a real
follow-up conversation. The answering model retains the product's normal tools.
Documents use the configured reader, including **GLM-OCR-0.9B** for a local setup.

The suite is `harness100-20261023b`, a **development/regression set**. It is not an
unseen test set, a clinical validation or a new performance result. It asks the same
100 questions of the same loaded record as `harness100-20261021`, which marked
correct answers wrong when they stated sleep in hours or rounded a mean, and
`harness100-20261023`, which failed correct answers that said "there is no genotype
file", stated a value apart from its label, or answered a document question from
the readings stored from that document ([Scoring and review](#scoring-and-review));
do not compare pass totals across the three. The historical 30-question runs and their conclusions are in
[HISTORY.md](HISTORY.md); their frozen questions are
[results/2026-10-10/cases.jsonl](results/2026-10-10/cases.jsonl). Do not compare
pass totals across those suites either.

| Family | Questions | Distinct challenges |
| --- | ---: | --- |
| Device readings | 24 | Latest/date, extrema, sums, means, median, missing days, threshold counts, 92-day totals, leap day, calendar month, date boundaries, paired pressure, unit conversion, percent change, charts |
| Laboratory results | 20 | Named values/units, own reference range, ALT/AST binding, HbA1c versus glucose, qualitative results, printed flags, cross-report comparison, inequality references, upper-bound excess, ratios and conversions |
| Documents | 12 | Phone photo, scan, screenshot, XLSX, report narrative, previous/current column, handwritten vitals/form, crossed-out correction and source dates |
| Genetics / PGx | 12 | Exact sites and genotypes, off-catalog calls, no-call, gene/drug evidence, assay coverage limits |
| Medication records | 8 | Regimen, course dates, historical titration, PRN, current schedule, plan versus actual intake |
| Journal | 8 | First/latest symptom report, entry counts, year restriction; self-report is not a diagnosis |
| Missing evidence | 6 | No genotype file, absent sleep/medication/analyte, dates outside the record, absence is not normal |
| Conversation | 10 | Five scored two-turn chains: next window, comparisons, latest within prior window, sum after mean, conversion after recall; one requires two native context summaries before answering |

There are 95 independent session starts and five scored follow-up requests. Both turns
count in the 100 rows. One long-context case inserts up to **16 additional unscored
pressure turns** using distinct real synthetic source records, so the full run can
make up to 116 chat requests. The 30-case smoke omits that heavier chain. Pressure
turns are preserved separately and their usage is counted; they are not extra scored
questions. A selected follow-up automatically includes its prerequisite;
it receives the actual previous model answer in the same session. No fabricated
assistant history or hidden warm-up question is used. A failed prerequisite leaves
its follow-up recorded as blocked/failed, with null latency, in the denominator.
The long-context case must observe two different summary cutoffs and two completed
native summarizer calls before its final answer. Replaying hand-written summaries,
duplicating padding, or merely having a long prompt does not satisfy that requirement.
If the budget cannot trigger the required summaries, the case is unexercised/failed,
even if its final factual answer is otherwise correct. The final question checks the
original baseline dates and the earlier comparison label as well as new arithmetic.
The bounded stress runner requires production context receipts and a declared input
window no larger than 131,072 tokens. A missing receipt or larger working window is
reported as unexercised before pressure calls. Use and report a dedicated evaluation
profile's bounded working window; do not present it as a model's native context limit.

The default does not retry failed model turns. Optional `--retries` records failed
attempts and adds requests; report that setting when comparing runs.

## Source and reproducibility

[plan.json](plan.json) pins the mirobody-gen commit, seed/recipe, frozen `as_of`,
case-manifest hash and SHA-256 of generator truth and consumed input files.
[cases.py](cases.py) uses semantic source selection rather than old fixed filenames.
Each question records its evidence selectors. Values, dates, genotypes and ranges
come from generator fields; aggregates use independent Python arithmetic. Mirobody
outputs are never the answer key. Device windows are checked against the selected
document dates to avoid accidentally using a device-only oracle for mixed records.

The corpus was built with mirobody-gen **9723605**:

```bash
# In a clean mirobody-gen checkout at 9723605, with its render dependencies:
python -m mirobody_gen build --seed 7 --people 8 --render --clinical --handwriting --verify --out /path/to/corpus

# In the Mirobody checkout; checking is read-only and calls no model:
.venv/bin/python benchmarks/local_models/cases.py --corpus /path/to/corpus --check
.venv/bin/python benchmarks/local_models/run.py --size small --corpus /path/to/corpus --dry-run
.venv/bin/python benchmarks/local_models/run.py --size small --corpus /path/to/corpus --dry-run --smoke
```

**The upstream build verification did not pass.** Its population, clinical,
order/result and text-readability checks passed, but `privacy_output` reported
eight replay/lexical matches. Image OCR, image privacy, temporal counterfactual and
other-channel audits were not run. The handwriting readability check used its own
transcript, not independent OCR. The exact statuses and reasons remain in the
manifest. Do not describe this corpus as fully audited or suitable for release/training
on the strength of this benchmark. A build with `--verify` may exit nonzero while
leaving these diagnostic artifacts; inspect its `verification.json`.

Audit report/log hashes are recorded separately because logs can contain absolute
build paths and depend on local reference-index availability. Only deterministic
source artifacts gate loading/running. Moving the frozen corpus is supported. A
missing or changed source file fails closed. Rendering depends on the pinned fonts
and compatible PyMuPDF/Pillow builds; if bytes differ on another environment, inspect
the differences and create a new explicit suite baseline, not a relaxed hash check.

To intentionally regenerate the manifests from the pinned checkout:

```bash
.venv/bin/python benchmarks/local_models/cases.py --corpus /path/to/corpus --generator /path/to/mirobody-gen
```

Changing loaded sources means new accounts. Question-only edits or translations
require fresh result directories, while identical loaded records can be reused.
Account identity excludes environment-specific audit diagnostics; result identity
still requires the exact question manifest.
The frozen comparison clock is `2026-09-02T00:00:00+00:00`, sent as `as_of` on every
chat request. Enable `EVAL_AS_OF_ENABLED=true` in the isolated evaluation server's
configuration and restart it. The server rejects the override when disabled, and the
runner stops with a configuration error before any model call. Queries also name
their historical windows; the relative-calendar question states its reference date.
The generator
uses a full simulation timeline and does not certify causal independence from future
inputs. Development use and source identity are recorded explicitly.

Sleep gold uses the consumed source payload's start/end timestamps, including
sub-minute precision, and labels each night by its evening date under the 18:00
boundary. It does not substitute rounded minutes from the metadata or the calendar
date of a post-midnight start. The builder checks that this corpus has one interval
per sleep night; a corpus needing interval union must supply that separate oracle.

## Load once, then run

Use an isolated local test stack. The loader creates benchmark accounts through the
public APIs, ingests device batches and genotype files, and uploads only **13 selected
documents**, rather than all 142 corpus files. It creates symptom entries without
calling the sentence parser, and explicit medication schedule versions through
`POST /api/v1/medications`. A titration closes the preceding schedule on the day
before the new dose starts; it never fabricates taken, skipped or missed dose logs.

The accounts keep the `harness100-20261021` name: `harness100-20261023` and
`harness100-20261023b` changed only expected facts and tool paths, so the loaded
record and its identity are the same.

```bash
export BENCH_QA_GENERATION=harness100-20261021
.venv/bin/python benchmarks/local_models/load.py --corpus /path/to/corpus --qa
.venv/bin/python benchmarks/local_models/load.py --corpus /path/to/corpus --qa --verify

# 100 scored requests plus up to 16 long-context pressure turns; default part is qa:
.venv/bin/python benchmarks/local_models/run.py --size small --corpus /path/to/corpus --out /path/to/results/full

# A 30-case development smoke:
.venv/bin/python benchmarks/local_models/run.py --size small --corpus /path/to/corpus --smoke --out /path/to/results/smoke

# One follow-up plus its prerequisite (two scored requests):
.venv/bin/python benchmarks/local_models/run.py --size small --corpus /path/to/corpus --cases h100-conversation-4-followup --out /path/to/results/followup
```

The usual `MIROBODY_URL`, `LLAMA_ROUTER`, `LLAMA_ROUTER_FROM_APP`,
`MIROBODY_COMPOSE_DIR`, `SETUP_TOKEN` and `BENCH_PASSWORD` settings are in
[common.py](common.py). `--no-switch` uses the already selected model. `--model`
evaluates another local preset; `--ref` is an explicitly configured reference.
No paid critic/model is needed to build, check or score this set.

`load.py --verify` reports missing extracted rows. It may also confirm a file's date
through the same file-date API a user can use, recording the correction in load state.
This is a harness evaluation over a prepared record, so extraction failures and those
corrections must be reported separately. Optional `--parts extraction,journal` runs
the slower extraction/sentence passes; it is **outside the 100-question budget**.
The OCR warm pass and cached document text do not measure cold image-to-answer
latency. This suite covers OCR-readable health documents, not meal appearance or
other general visual reasoning that GLM-OCR does not establish.

## What latency means

All measurements start at the chat HTTP request after any session creation or
rate-limit wait. They are client-observed streaming latency, not a server-side
prefill benchmark. The runner reads SSE chunks promptly and keeps:

| Field | First observed event |
| --- | --- |
| `first_token_s` | Nonempty model reasoning **or** final text, whichever appears first |
| `first_reasoning_s` | Nonempty thinking/reasoning content |
| `first_tool_s` | Typed tool-call event |
| `first_text_s` | Nonempty final-answer text |
| `seconds` | End/error/timeout of the chat request |

For the long-context case, the `first_*` fields measure its final scored request.
Its `seconds` includes the pressure setup and that final request; the separate
`context_setup_seconds` and `final_request_seconds` preserve both components.
Each pressure turn retains its own timing and usage. Summary tables distinguish
whether the two-summary mechanism was exercised from whether the final answer passed.

A heartbeat, empty chunk or status marker is not a generated token. Reasoning is
valid visible progress and counts toward first-token responsiveness. Missing fields
stay null, including failed/blocked cases; summaries report p50/p90 with observed and
missing sample counts. They include failures that emitted content. State model load,
prompt-cache condition, hardware, concurrency and OCR state when comparing runs;
never present the historical `first_text_s` as if it measured visible thinking.

## Scoring and review

The scorer checks completion, a relevant tool path, language, expected facts,
requested chart, subject identity and fabricated citation IDs. Expected numeric
facts can bind to a labelled clause, table row, date or rsID. It rejects swapped
ALT/AST values, wrong dated points, a genotype attached to another site, wrong
numeric sign and an answer from a different synthetic subject. A device number passes
within half the step it is naturally stated in (1 for step counts, mmHg and bpm;
0.1 for kg, lb, hours and percent), so any correct rounding passes and a
neighbouring reading does not; a difference of two means allows a whole step. A
sleep total or mean whose unit the question leaves open is a duration, read as
hours, hours and minutes or minutes, within 3 minutes (one-decimal hours). A
clause that states a value without its label ("平均值是 77.7", a "结果：2.68" list
line) takes the label written before it in its sentence, or in its list or table
and the line introducing it, unless another measure is named there. Absence and
the limits of evidence are read as phrases ("no insulin value", "does not
establish") as well as words. A document question may be answered by reading
the file or from the readings stored from it. A
follow-up can cite and reuse its prerequisite's actual evidence without being forced into another call.

These are conservative **structural checks**, not a semantic or clinical judge.
Unusual valid phrasing or wide tables can produce false negatives; contradictions,
unrelated unsupported advice and incomplete explanations can still need review.
The legacy unsupported-number heuristic and citation checker are reported separately;
zero unsupported numbers is not a certificate of clinical grounding. Inspect claims,
completeness, honest absence, readability and reasoning/final-answer separation beside
automatic pass rates. The 100-question score is not a substitute for a separate,
person/source-isolated unseen set before post-training evaluation.

Summaries break out results by task family and request language. Japanese and the
two Chinese scripts are checked separately; a Chinese phrase made entirely of shared
characters is not penalized for lacking a script marker. Foreign filenames, source
labels and standard symbols can remain verbatim inside an otherwise localized answer.

Each saved `qa.json` embeds the exact case manifest, its hash, selected IDs and actual
transcripts. `meta.json` records corpus/audit provenance; `run.json` includes model and
machine details. Existing result directories with another question hash are refused.

```bash
.venv/bin/python benchmarks/local_models/score.py /path/to/results/full
# Historical transcripts require their own frozen questions:
.venv/bin/python benchmarks/local_models/score.py /path/to/historical/results --cases-file benchmarks/local_models/results/2026-10-10/cases.jsonl

.venv/bin/pytest -q tests/agent/test_local_eval_contract.py
```

The optional `--review-packets /path/to/review-packets.jsonl` export from `cases.py` can be passed to mirobody-gen's
`review-scenarios` without `--model` for deterministic packet/coverage checks.
Those checks do not independently judge medical truth or image quality.
