# Code-tool language comparison: local Python and JavaScript

The current comparison does **not** show Python universally outperforming
JavaScript. DeepSeek Flash, Claude Sonnet 5.5 and GPT-6 Luna pass all 16 cases
in both languages, including replay against changed source values. MiniCPM5-2B
can produce correct-looking answers while copying literal readings; neither
language passes that stronger source-dependence check on this small model.

The product's `code_exec` takes its language from where the model runs:
JavaScript, in-process QuickJS, for a cloud model, since the cloud models
here were equally correct in both; Python in a local container for a model on
the person's own hardware when a Docker daemon answers. Its original-data
integration is supported by the tests; a general claim about Python being the
stronger language for every model is not supported by these measurements.

## Current method: 2026-10-21

- Eight synthetic aggregation questions, two repetitions per language and
  model: 128 model conversations.
- Models: MiniCPM5-2B Q8_0 on local llama.cpp b11429; direct
  `deepseek-flash`; `anthropic/claude-sonnet-5.5` and
  `openai/gpt-6-luna` through their configured OpenRouter entries.
- Shared `harness.assemble`, middleware, source rows, question and requested
  output labels. The Python arm uses the production `python_exec` tool.
- The controlled readings service uses the production authenticated envelope
  adapter and compact renderer. It exposes original typed `Envelope`
  artifacts through hidden `ToolRuntime`; no model-pasted table is parsed.
  These are synthetic queries, not a live database or a clinical validation.
- Python 3.12 and Node 22 execute only in local networkless Docker containers,
  with the same transport, CPU, memory, PID and output limits. The Node tool
  exists only in this comparison; the product has one Python tool.
- Sampling and thinking use the same configured model settings per arm.
  The factory requests medium thinking. The benchmark removes language-specific
  code examples from both tool descriptions and uses equivalent mutable input
  globals, avoiding a JavaScript-only variable-redeclaration failure.
- Final numbers must match the requested labels and tolerance. A swapped
  systolic/diastolic answer does not pass. Printed Python dictionaries and
  nested value/mean fields with units, counts and rids are accepted.
- Successful generated code is replayed locally with each numeric reading
  doubled. Its output must change to the independently derived expected
  result; percentage change is invariant, while its starting and ending
  values double. Replay makes no additional model requests.

The initial local-only evidence below used a different, weaker numeric matcher.
During this review, preliminary comparisons also revealed a Python example in
the JavaScript description and unequal variable-binding behavior. Both were
corrected; the tables here use only the final comparison and replay.

## Results

“Answer” measures the labelled final reply. “Source calculation” additionally
requires a successful query, correct code output, and changed-input replay.

| Model | Python answer | JS answer | Python source calculation | JS source calculation |
|---|---:|---:|---:|---:|
| MiniCPM5-2B Q8_0 | 12/16 | 16/16 | 0/16 | 0/16 |
| DeepSeek Flash | 16/16 | 16/16 | 16/16 | 16/16 |
| Claude Sonnet 5.5 | 16/16 | 16/16 | 16/16 | 16/16 |
| GPT-6 Luna | 16/16 | 16/16 | 16/16 | 16/16 |

| Model | Python tool calls / failures | JS tool calls / failures | Python mean seconds | JS mean seconds |
|---|---:|---:|---:|---:|
| MiniCPM5-2B Q8_0 | 24 / 11 | 20 / 0 | 33.44 | 25.35 |
| DeepSeek Flash | 17 / 0 | 16 / 0 | 3.85 | 3.56 |
| Claude Sonnet 5.5 | 16 / 0 | 16 / 0 | 5.26 | 5.38 |
| GPT-6 Luna | 16 / 0 | 16 / 0 | 6.99 | 6.01 |

Failures count unsuccessful code-tool results, including governance refusals;
they are not all interpreter syntax failures. Latency includes model inference
and container setup and is subject to local scheduling and endpoint variation.
Two repetitions per question do not establish a statistical speed ranking.

All three cloud models compute correctly in either language on this set.
MiniCPM's discrepancy between answer accuracy and source calculation is the
material result: some code reads an input or prints its length, then performs
arithmetic on copied literals. An input-name check alone therefore passes code
whose reported numbers do not depend on the supplied readings.

Use the engine's existing statistics/time-bucket views where they answer the
question. Additional Python arithmetic can remain local, but the small model's
custom-calculation reliability remains an open item in
[the roadmap](../../docs/roadmap.md).

## Evidence and reproduction

The complete synthetic conversations, generated code, tool outputs and
changed-data replay results are in
[code_tool_language_runs.json](code_tool_language_runs.json).

```bash
docker pull python:3.12-slim
docker pull node:22-slim

python benchmarks/local_agent/code_tool_probe.py \
  --entries local,deepseek,claude-sonnet,gpt --runs 2 \
  --out /tmp/code-tools.json

python benchmarks/local_agent/code_tool_probe.py \
  --replay-report /tmp/code-tools.json --out /tmp/code-tools-verified.json

python benchmarks/local_agent/regress_code_tool.py \
  --entry deepseek --cases bp_q2,steps_august --out /tmp/python-gate.json
```

The local entry uses `--local-base http://127.0.0.1:8088/v1` by default.
Cloud credentials are read from the local configuration, never written into
the evidence file or passed into calculation containers.

Source replay is a useful literal-copy detector for these scalar tasks, not a
proof that arbitrary generated programs are correct for all datasets. The
study also does not test persistent REPL state, PTC, large dataframe workloads,
native Anthropic transport, real patient data or every model family.

## Historical MiniCPM probe

The earlier probe recorded:

| Arm | Answer pass | Code calls | Code errors | Mean seconds |
|---|---:|---:|---:|---:|
| QuickJS `eval` | 12/16 | 36 | 28 | 32.4 |
| Python | 16/16 | 14 | 0 | 18.5 |

Those observations show a language/tool mismatch in that particular setup:
the model frequently submitted Python syntax to the JavaScript interpreter.
They do not establish production source-calculation success. The matcher
accepted an unordered bag of numbers and allowed answers without querying or
executing code; the Python probe also injected original dictionaries while
the proposed product tool reparsed a lossy display table.

The historical implementation and original evidence are preserved in
[commit 63eb73f1](https://github.com/thetahealth/mirobody/tree/63eb73f1/benchmarks/local_agent).
The old interpreter, PTC bridge, model language switches and return-repair
middleware were removed from the current product. Local setup and the current
data path are documented in [Calculation in chat](../../docs/python-analysis.md).
