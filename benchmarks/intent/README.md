# Personal health record evaluation

Mirobody supports **clinicians and personal health data management**. This suite
measures accurate use of a person's records: date joins, coverage, calculation,
version reconciliation and source-traceable handoffs. Its main set is **32 tasks:
24 capability tasks and 8 regression controls**. It also freezes **8 held-out
tasks** and **4 reliability pairs (8 episodes)**. The existing `local_models`
100-question product regression suite is unchanged.

The fixtures live in [`demo/intent/`](../../demo/intent/). All commands below run
from the repository root. These are authored synthetic tasks, not copied patient
messages, medical consultation cases or independently validated clinical labels.

## Compact coverage

| Main-set task | Capability | Regression |
| --- | ---: | ---: |
| Conditional, lagged and cross-signal analysis over 28 days | 12 | 0 |
| Medication schedule versions, dated reconstruction and PRN coverage | 4 | 0 |
| Longitudinal report lookup, date roles and sparse-record comparison | 4 | 2 |
| Clinician handoff with indicator/date/value/unit/source binding | 4 | 2 |
| Filing results against the report's supplied reference interval | 0 | 2 |
| Family sharing and revoked authorization | 0 | 2 |
| **Total** | **24** | **8** |

The eight analysis operations are conditional mean, next-day comparison,
complete-pair coverage, Pearson correlation, outlier exclusion, consecutive
streaks, observed-day weighted mean and personal baseline. Missing days, the declared
Asia/Shanghai account day, lag direction and tie rules are explicit. Numerical answers have
executable keys; a generic explanation or overall mean cannot satisfy the task.
Doctor summaries contain six observations across two indicators with dates,
not just a single latest result. Capability and regression scores stay separate.
These quotas are design choices, **not measured product traffic proportions**.

Development uses 20 synthetic people and has `zh-CN:20`, `en:11`, `ja:1`.
The eight held-out tasks use eight other people from a ten-person allocation.
Every visible main/held-out packet is unique. Held-out labels are outside the
public tree and have not been model-evaluated; they are patient-disjoint but
**not author-blind or template-disjoint**. Small counts do not support a
population-level model ranking or language-specific conclusions.

## Public benchmark research incorporated

| Primary source | Adopted task idea | What is actually shipped |
| --- | --- | --- |
| [PHIA](https://arxiv.org/abs/2406.06464) | Executable personal wearable statistics and time comparisons | New observations and authored questions with deterministic calculations |
| [LifeAgentBench](https://arxiv.org/abs/2601.13880) | Conditional queries and reasoning across lifestyle dimensions | Same-day joins, missing-pair denominators and threshold streaks |
| [WearableQA](https://arxiv.org/abs/2609.05405) | Longitudinal single/cross-signal data reasoning | Correlation, lag and personal-baseline tasks; no physiological diagnosis questions |
| [HealthBench](https://openai.com/index/healthbench/) | Per-task rubrics, context seeking and health-data fidelity | Record reconciliation and clinician handoff criteria; no consultation corpus |

These are **task-type adaptations on our own synthetic data**, not imported
original test items or scores on those benchmarks. Each case carries its task
reference. LifeAgentBench's data restrictions and WearableQA's
[CC BY-NC 4.0 license](https://github.com/facebookresearch/WearableQA/blob/main/LICENSE)
are reasons not to copy their records into a product repository. HealthBench's
large conversation collection is not used to infer Mirobody's workload mix.
The cloud `真实用户问题调研-20261024.md` informed report lookup, OCR ambiguity,
medication records, family sharing and visit preparation; its consultation-heavy
frequency estimates are not treated as this product's target distribution.

## Truth, visibility and provenance

A fresh mirobody-gen cohort (seed **20261025**, 30 people) supplies the records;
[`manifest.json`](../../demo/intent/manifest.json) pins its commit and all five
consumed source-file hashes. Each selected generator snapshot has a JSONL line
and field selectors; the builder checks its value and patient identity against
the source. Typed facts have units, tolerances and recomputable oracles. Small
hand-calculated tests independently check the analysis algorithms. A medication
change inherits omitted fields, including frequency, without applying future
changes early. Missing intake events do not establish missed doses.

These are **text snapshots**, not an image/OCR evaluation. The generator's
`collected` field is called `encounter_date`, never specimen collection time.
Later issue dates, OCR ambiguity, unavailable tools and sharing states are
explicitly authored controls. Latent diagnoses, hidden OCR truth, source paths,
rubrics, gold facts and split labels never enter `answer_packet`.
Source consistency does not certify clinical realism or medical validity;
clinical expert validation and the generator's image audit were not performed.

The reliability pairs cover two reordered daily charts, one history task and
one unavailable-tool scenario. Chart JSON, dates, points, units and order are
checked. The history stress requires receipts for two actual native summaries;
the failure stress requires a real fault-injection receipt. Authored history
and a supplied timeout message cannot establish those product events. Without
receipts, the relevant execution result is `not_exercised`.

## Offline validation and reproduction

```bash
python -m benchmarks.intent.run validate
pytest -q mirobody/tests/test_intent_evaluation.py
ruff check benchmarks/intent mirobody/tests/test_intent_evaluation.py
```

For byte-identical regeneration, use the generator commit in the manifest:

```bash
PYTHONPATH=/path/to/mirobody-gen python -m mirobody_gen build \
  --seed 20261025 --people 30 --clinical --out /private/intent-cohort
python -m benchmarks.intent.build \
  --corpus /private/intent-cohort --generator /path/to/mirobody-gen \
  --out demo/intent --held-out /private/intent-held-out.jsonl
```

Keep held-out labels outside the public bundle. No answering/review command
selects them. Rebuilding reveals them to the author; agree on an unsealing
policy before using them for a milestone. The manifest publishes only their
counts, patient-family IDs and hash.

## External LLM review and calibration

Calls require explicit provider, model and output directory. Credentials come
from process environment or `--env-file`; matching keys are bound to fixed
HTTPS endpoints and never enter results. Store outputs outside the frozen data,
usually in ignored `internal/` or outside the repository.

```bash
# Review task answerability and rubric alignment.
python -m benchmarks.intent.run audit \
  --provider openrouter --model anthropic/claude-opus-5.5 \
  --reasoning-effort medium --max-output-tokens 8192 --timeout-seconds 180 \
  --env-file /private/mirobody.env --out internal/intent-audit

# Main-set answer calibration; --role capability selects only the 24 core tasks.
python -m benchmarks.intent.run baseline \
  --provider openrouter --model openai/gpt-6-luna \
  --reasoning-effort medium --max-output-tokens 8192 --timeout-seconds 180 \
  --env-file /private/mirobody.env --out internal/intent-answers

# External semantic judge plus programmatic numeric checks.
python -m benchmarks.intent.run score \
  --provider openrouter --model anthropic/claude-opus-5.5 \
  --reasoning-effort medium --max-output-tokens 8192 --timeout-seconds 180 \
  --env-file /private/mirobody.env \
  --answers internal/intent-answers/answers.jsonl --out internal/intent-score

# Additional independent judge on the same saved answers.
python -m benchmarks.intent.run score \
  --provider nebula --model deepseek-v4-pro \
  --reasoning-effort high --max-output-tokens 8192 --timeout-seconds 180 \
  --env-file /private/mirobody.env \
  --answers internal/intent-answers/answers.jsonl --out internal/intent-nebula-score
```

The `nebula` provider uses the fixed `https://llm.ai-nebula.com/v1` endpoint
and reads `NEUBULA_API_KEY`, matching the existing project environment entry.
DeepSeek V4 Pro requests explicitly enable thinking and use `reasoning_effort`;
sampling temperature is omitted. This optional judge does not regenerate the
saved answers. Scores from different judges remain separate.
See the [Nebula model-list contract](https://docs.ai-nebula.com/en/api-reference/endpoint/list-models)
and [DeepSeek thinking controls](https://api-docs.deepseek.com/guides/thinking_mode/).

`--smoke` selects the first two tasks per domain (12 tasks); `--limit` further
bounds a run. Use the same selection for answers and scores. Reasoning models accept
`--reasoning-effort`, `--max-output-tokens` and `--timeout-seconds`; the complete
settings bind both the request and cache. Native OpenAI uses
`max_completion_tokens`, while OpenRouter uses `max_tokens`. `--dataset
reliability` selects the eight reliability episodes. `baseline` supplies visible
evidence directly: it does **not** run the production prompt, tools, database,
authorization or native compaction. Its score calibrates dataset difficulty,
not Mirobody harness performance. For real product answers, submit JSONL:

```json
{"id":"intent-development-interpretation-01","case_sha256":"<digest of the frozen case>","status":"ok","answer":"Actual rendered answer","receipts":{}}
```

`case_sha256` is required and equals `benchmarks.intent.schema.digest(case)` for
the entire frozen case; stale or unbound submissions fail before any paid call.
A product adapter must bind the synthetic patient to a fresh authorized test
account, retain actual SSE/tool traces and derive receipts from instrumented
execution. Answer text alone does not prove a tool call, recovery or compaction.

The judge cites numbered **actual answer lines**, evaluates each rubric and
extracts each fact as stated. Programs validate IDs and visible references,
check that extracted numbers occur in those lines, bind units to adjacent
numbers, their actual Markdown table column or enclosing section heading, then apply unit normalization
and numerical tolerances. Unit matching stays within a line and cell; explicit
unit columns and adjacent labeled value/unit fields bind to their own result.
Counts and Pearson r are dimensionless. Charts must
parse and match their source-derived oracle. Semantic fact identities and
paraphrased dates still depend on a fallible LLM judge; disputed cases require
adjudication, not blind acceptance.

Complete request hashes cover case, model, endpoint, prompt, sampling body,
evaluator version and receipts. Results retain raw responses, actual model IDs,
usage and failures. `answer_failure` and `evaluator_failure` remain in the fixed
selected denominator. A failed judge is not a wrong product answer. No answer
retries or post-hoc held-out filtering are hidden in a score.

To compare two same-case score files without dropping failed judgments:

```bash
python -m benchmarks.intent.compare \
  --left internal/score-a/score.jsonl --right internal/score-b/score.jsonl \
  --out internal/comparison.json
```

Measured calibration, failures, per-item model disagreements and limits are
recorded in [`validation.json`](validation.json). Do not use model agreement as
clinical truth or a 32-item development calibration as an independent leaderboard.

## Current calibration protocol

The current evaluation uses `anthropic/claude-opus-5.5` and `openai/gpt-6-luna`
through OpenRouter, with medium reasoning, an 8192-token output budget and a
180-second timeout. Both review the task set; answers are scored by the other
family and also by each common judge for a transparent comparison. Self-grades
are diagnostic, not independent confirmation. The [official Luna model page](https://developers.openai.com/api/docs/models/gpt-6-luna)
and [Claude model catalog](https://platform.claude.com/docs/en/models/overview)
identify the requested models. Actual response identities and settings are retained.

Modern review prompted corrections to local-day alignment, the explicit 21/7
baseline, task-specific rubrics, family-member identity and complete three-result
report comparisons. No task was selected or dropped based on which model won.
The main set remains 32. Unchanged visible packets may reuse the identical saved
answer with explicit source-case/request hashes; changed packets are re-answered.
Earlier packets, raw responses and their scores remain separate and must not be
mixed with the corrected dataset's hash. The eight held-out tasks remain uncalled.

Independent inspection found two false distinctions that the model audits had
missed: the excluded dates were graded in listing order, and medication strength
was treated as a per-dose amount. Evaluator 8 accepts either order of the two
distinct dates and extracts recorded strength separately from any explicit dose
instructions. All four changed medication review packets required new reviews;
old verdicts cannot count against the corrected rubric. The additional DeepSeek run now reviews all four corrected packets. Adjacent key/value table
rows now bind a pure numeric value to their own cited unit row, while borrowing
units across records remains invalid.

Evaluator 9 also accepts a plain value column under its own unit-bearing
section heading, without borrowing units across headings or overriding an
explicit row/column unit. Chinese commas and separators after ISO dates do not
merge into the next number; thousands separators still work. Saved raw verdicts
are revalidated under the new version. Invalid or truncated evaluations alone
may be retried with an explicit larger output budget; each original attempt
and its settings remain available.

Both answering models produced 32 usable answers. After OpenRouter's allowance
was exhausted, the user authorized a separate **DeepSeek V4 Pro common judge
through Nebula**. It completed all **32 task audits and 64 answer reviews**.
Requests use high thinking, 8192 output tokens and a 180-second timeout. Only
four invalid or truncated evaluations were retried with 16384 tokens and a
300-second timeout; no answer was regenerated. Two valid saved reviews were
recovered by evaluator 9 without another model call. Original attempts, actual
response models, settings and raw versus adjudicated grades remain separate.

| Answer model | Raw DeepSeek capability / regression | Source-adjudicated capability / regression | Source-adjudicated total |
| --- | --- | --- | --- |
| Claude Opus 5.5 | 24/24 · 8/8 | 23/24 · 8/8 | 31/32 |
| GPT-6 Luna | 21/24 · 8/8 | 21/24 · 8/8 | 29/32 |

Source inspection corrects one false failure for Luna's excluded dates and one
shared false pass: both models infer a 500 mg per-dose amount from a record
that supplies only strength. These are coding-agent checks of source contracts
and arithmetic, **not clinical expert validation**. Luna's aggregate stays the
same because two opposite corrections cancel; per-item records are essential.
The audit's two numerical objections use UTC dates rather than the declared
local days, and one abstention mistakes task auditing for answer auditing. All
raw objections and the independent calculations are retained.

Only **two confirmed capability items** distinguish these answer sets:
conditional-pair membership and local-date streak boundaries. Opus is near the
ceiling at 23/24 after adjudication. The current set is useful as a compact
regression foundation, but insufficient alone for measuring frontier-model
harness improvements. A next revision should predeclare six harder replacements
within the existing 24 capability slots: corrected-document date roles,
overlapping device uploads and local-day changes, dose/strength/version/intake
separation, sparse cross-signal joins, constrained source-traceable handoffs, and
actual context-summary retention with execution receipts. Validate keys before
model calls; do not select questions by which model wins. No such replacements
were silently added to this frozen run. Earlier partial OpenRouter reviews stay
historical and are not mixed into the DeepSeek common-judge totals.
