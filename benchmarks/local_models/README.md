# Local harness regression: 100 questions

This directory evaluates the product HTTP chat path with **100 source-grounded
questions** against eight synthetic people, using the frontend's exact locale codes:
**60 English (`en`), 30 Simplified Chinese (`zh-CN`), 5 Japanese (`ja`) and
5 Traditional Chinese (`zh-TW`)**. `--smoke` selects 30 questions covering
every family; it includes genetics, pharmacogenomics, OCR documents and a real
follow-up conversation. The answering model retains the product's normal tools.
Documents use the configured reader, including **GLM-OCR-0.9B** for a local setup.

The suite is `harness100-20261023c`, a **development/regression set**. It is not an
unseen test set, a clinical validation or a new performance result. It asks about the
same loaded record as `harness100-20261021`, `-20261023` and `-20261023b`; its 32
document and laboratory questions name the document by type and date instead of a
corpus filename the product never shows, and its scorer reads more correct phrasings
([Scoring and review](#scoring-and-review)). Do not compare pass totals across
suites. The historical 30-question runs and their conclusions are in
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

The accounts keep the `harness100-20261021` name: every later suite changed only
questions and expected facts, so the loaded record and its identity are the same.

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
clause or list item that names no test but opens with an attribute word ("mean",
"result", "参考范围") continues the label before it, a table cell is read with its
column header and row label, and the result of worked arithmetic ("74 − 50 = 24")
belongs to the label that opens its clause; another test's name stops that.
Absence, "this does not establish" and assay coverage are matched as phrases, so
"There is no genotype file" passes, while a reported value does not, nor does a
"no" followed by a determiner, a possessive or "other" ("no other tests were
abnormal"). A document
question may be answered from that file's stored readings as well as by reading
it. A month and day without a year take the year of the full date before them. A
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

## Extending the set: chart and language coverage

The shipped 100 questions ask for a chart on **two** of them and are **60% English**
(`en` 60, `zh-CN` 30, `ja` 5, `zh-TW` 5; per-person counts run from 24 to 4). That
makes the `chart` and `language` checks cheap for a model: almost every case passes
`chart` without drawing anything, and a model that only ever answers in English is
still measured mostly on English questions.

`expand_cases.py` builds extra cases in the same shape as `cases.jsonl` from the same
`mirobody-gen` build and `plan.json`, with every answer computed from the build (never
from product output). Days are the account's local days, as `cases.py` computes them;
a window never overlaps a loaded document's reading of the metric; tolerances are half
the natural step; questions are in the first person, never naming a corpus id:

```bash
python benchmarks/local_models/expand_cases.py --corpus <build> --out extra-cases.jsonl
# 140 cases on the pinned harness100 build; the script prints the count
```

It emits five families the current set under-covers:

| family | asks | checked as |
| --- | --- | --- |
| device-latest | newest value and its date | number + date facts |
| device-sum / device-count | total (steps only; rhr/weight are not additive) and covered days over 30 days | number |
| chart | a week of daily values **and** a `vis-chart` block | 7 numbers + `chart` |
| journal-count | how many entries name a symptom written one way | number |
| absence | a metric this person never recorded | words: states absence |

Device, chart and absence forms are generated once per locale, so a model that answers
a Chinese question in English now fails the language check. A journal question is asked
in its journal's language only, naming the symptom in the person's own words; a
translated name would be a word their journal never used.

Use `--as-of` when the build's clock is not the frozen suite's `2026-09-02`.

The file is deliberately **separate** from `cases.jsonl`: the maintained suite keeps
its provenance and its historical baseline. To run the extras through this harness,
append them to a copy of `cases.jsonl`, recompute `plan.json`'s `cases_sha256`, and
relax `provenance.validate_cases`'s "exactly 100" rule in that copy — that is what the
extras were developed against. Treat the result as an extended set with its own name,
not as a replacement for the maintained `harness100` suite, and report the two separately.

## Capability probes

`probes.py` builds a small diagnostic set from generator truth and the pinned loading
plan. It reads the actual flat `devices.jsonl` records (`metric`, `indicator`, `time`,
`value`, `unit`) in the account's time zone. Gold never comes from a product response.
The default selects at most two metrics, with at most **19 questions per person**;
`--max-metrics 1|2|3` changes that bound (10/19/28).

| capability | question | acceptance |
| --- | --- | --- |
| C3 window | average over an explicit historical window | source-derived mean in the requested unit |
| C4 aggregate | distinct recorded days or maximum in that window | source-derived day count or maximum |
| C6 provenance | numeric answer's citation | known row ID, with numeric support in the actual tool evidence |
| C7 absence | a metric absent from loaded source truth | explicit absence without a fabricated numeric reading |

Candidate windows occupy three disjoint thirds of the record, using up to 7, 14 and
21 observed days. Mean and maximum answers must differ by more than their combined
1%/0.05 tolerances; counts must differ exactly. Each retained operation needs at least
two distinguishable windows. A constant answer cannot pass all of them. Metrics with
no discriminating means are skipped, with reasons saved; they do not establish C3.

Duplicate local-day readings, nonfinite values and mixed units are omitted rather
than assigned an ambiguous gold. Windows cannot overlap loaded document readings of
the same metric. Sleep uses source intervals in minutes and the existing evening-date
policy. Absence checks all loaded document truth, including unreadable rows, short
device series and journal mentions; if absence cannot be established, C7 is omitted.
These are bounded record-access diagnostics, not a clinical reasoning benchmark or
an independent held-out test. C6 checks numeric traceability, not whether a clinical
interpretation follows from the evidence.

First inspect the source-derived questions without signing in or calling a model:

```bash
python benchmarks/local_models/probes.py --corpus <build> --plan <plan.json> \
    --person p001 --email <existing-loaded-account> \
    --out results/probes/p001.json --dry-run
```

Remove `--dry-run` to ask the already-loaded account. The plan's source hashes and
the account's person, completed loading state and record identity must match. A new
empty email account is rejected. The run uses the account's planned time zone,
English questions and the frozen evaluation clock (`2026-09-02`). It does not switch
models or reload records.

Results save the complete question manifest and its hash, source hashes, per-window
points and policy, full answers and tool transcripts, and citation diagnostics.
Numeric extraction excludes dates and citation IDs and rejects ambiguous multiple
values. Explicit incompatible units fail, including units inside parentheses,
brackets, quotes and nested formatting. UCUM's own brackets, such as `[IU]/L`,
are retained while outer formatting is removed. Per-capability counts keep selected,
completed, passed, failed runs and unattempted questions separate; C6 uses the same
selected numeric denominator. An incomplete turn stops further calls and leaves the
remaining questions unattempted. Session-creation failures are captured too. Results
are saved after each answer with the full selected denominator; `complete=false`
marks a model run that has not reached its final write.

Exit status is 0 only when all selected checks pass, 1 for a failed answer, citation
or run, and 2 when there are no discriminating C3 windows (including dry runs). A dry
run with usable C3 windows exits 0 without creating quality grades.
