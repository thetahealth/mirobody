# Local model sizes: an evaluation you can rerun

Mirobody 1.5.5 offers two sizes of answering model for running with no API
key, small (the default) and large, all on [llama.cpp](https://github.com/ggml-org/llama.cpp)'s
`llama-server` ([docs/local-models.md](../../docs/local-models.md)). Documents
are read by GLM-OCR-0.9B at every size; the answering model also serves
`local-utils`: titles, summaries, journal sentences and whatever a document's
tables leave for a text model.

| Size | Answering model | Download |
| --- | --- | --- |
| tiny (measured here, then dropped) | `minicpm5-1b`: MiniCPM5-1B Q8_0, text only | 1.2 GB |
| small | `minicpm5-2b`: MiniCPM5-2B Q8_0 (Q4_K_M, 1.6 GB, until 1.5.5), text only | 2.7 GB |
| large | `qwen3.8-27b`: Qwen3.8-27B GSQ-RCO IQ3_S + vision projector | 13 GB |

This directory measures what each size gets right on one synthetic record,
through the product's own HTTP API, as a person using the web client would:

- **Questions**: 24 questions (12 Chinese, 12 English) about four synthetic
  people, and six added in 2026-10-10 for the medical knowledge tools and
  cited answers, each asked once in a fresh chat session. Every expected
  answer is computed from the generator's ground truth, never typed by hand.
- **Extraction**: 12 documents (text-layer PDF, CSV, XLSX, scan, phone photo,
  photocopy, screenshot, a 7-page check-up book, a clinic note, a home log)
  uploaded into a fresh account, every stored reading scored against the rows
  the document printed.
- **Journal**: 15 diary sentences through `POST /api/v1/journal/sentence`,
  scored against the entries each one states.
- **Memory**: what the two `llama-server` processes hold.

The automatic checks are scored by `score.py`; every answer is also graded by
hand against a written rubric ([Grading](#claude-code-grading)). Both are kept,
side by side, so either can be checked against the other.

## Results

### Sampling, thinking, two more models, and what post-training is for (2026-10-10)

Rerun on main after the harness work of 1.5.5 (cited answers, the medical
knowledge tools, thinking on for every model), with OpenBMB's sampling advice
for MiniCPM5 tried against what the product ships, and six questions added
for the new tools (24 + 6, [Questions](#questions)). Same 48 GB Apple-silicon
machine as 2026-10-08; results in `results/2026-10-10/`, summary in
`results/2026-10-10/summary.md`.

- **Code**: 338d54f4, the stack's image built from it (the knowledge index
  copied in rather than fetched at build time); this directory uncommitted.
- **Record**: `qa6`, the same plan loaded again at 338d54f4.
- **Models**: llama.cpp b11429, the build `compose.yaml` pins; every GGUF's
  hash equals its Hugging Face blob. Cloud models through OpenRouter, each
  pinned to one host.
- **Grading**: six Claude Code graders, each given five cases and every run's
  answer to them, so a case is graded the same way across models; later
  batches were given the earlier grades as calibration.

**MiniCPM5-2B: the shipped settings beat the model card's.** OpenBMB gives
`temperature 1.0, top_p 0.95, min_p 0` for thinking mode and warns that
llama.cpp's default `min_p` 0.05 traps the model in repetition loops. The
product sends `temperature 0`.

| MiniCPM5-2B, Q4_K_M | Runs | Grade (24 questions, of 248) | `grounded` (of 48) | Automatic pass | p90 s | Timeouts |
| --- | --- | --- | --- | --- | --- | --- |
| temperature 0, thinking on (shipped) | 2 | 212, 194 (mean 203) | 36, 31 | 18, 20 | 89, 102 | 0 |
| 1.0 / 0.95 / min_p 0, thinking on | 3 | 190, 200, 182 (mean 191) | 26, 31, 25 | 19, 18, 21 | 52, 192, 66 | 1 |
| temperature 0, thinking off | 1 | 184 | 33 | 15 | 67 | 0 |
| 0.7 / 0.95 / min_p 0, thinking off | 1 | 198 | 29 | 19 | 35 | 0 |

- Sampling loses on `grounded`: at temperature 1.0 the model writes medical
  claims no tool returned, some reversed (anaemia, pregnancy and haemolysis
  as causes of a high hemoglobin; VKORC1 called a VEGF receptor; SLCO1B1 said
  to encode P-glycoprotein).
- The automatic checks rank the other way (21 of 24 for the 182-point run):
  they find a number, not the sentence beside it.
- The long reasoning greedy decoding produces is not a loop: in the one that
  ran to 32k characters, 129 sentences, none repeated, each a wrong tool
  argument and a try at recovering. That is post-training's to fix, not the
  sampler's.
- Thinking off costs 19 points at temperature 0 and leaves the model asking
  for what it could have looked up.
- Two greedy runs differ by 18 points and three sampled runs by 18: llama.cpp
  with two slots is not bit-reproducible, and one run of a small model is not
  a finding.

**Every model on the same 30 questions.**

| | Grade, 24 questions (of 248) | Six added (of 60) | Cites in the format | Ids no tool showed | Seconds, median / p90 |
| --- | --- | --- | --- | --- | --- |
| MiniCPM5-2B Q8_0 (small from 1.5.5), 2 runs | 229, 225 | 48, 50 | 1–2 of 23–24 | 0 | 18 / 34 |
| MiniCPM5-2B Q4_K_M (small until now), 2 runs | 212, 194 | 50, 50 | 1–2 of 23–24 | 0 | 16 / 96 |
| MiniCPM5-1B (tiny), temperature 0 / 0.9 | 109 / 109 | 39 / 36 | 0 | 0 | 5 / 7 |
| Qwen3.8-27B (large), 6 of 30 unanswered in 600 s | 192 | 45 | 20 of 25 | 0 | 281 / 600 |
| Gemma 4 26B-A4B (cloud, Vertex) | 233 | 58 | 21 of 23 | 3 | 8 / 129 |
| Gemma 4 31B (cloud, DeepInfra FP8) | 242 | 57 | 23 of 25 | 0 | 29 / 56 |
| Gemini 3.8 Flash | 243 | 56 | 26 of 26 | 0 | 15 / 26 |
| DeepSeek V4.1 Flash | 240 | 59 | 24 of 26 | 2 | 6 / 10 |
| GPT-6 Luna | 239 | 58 | 22 of 24 | 0 | 13 / 21 |
| Claude Sonnet 5.5 | 238 | 57 | 21 of 22 | 0 | 7 / 10 |

- **Q8_0 is the better small size.** Two runs each: 229 and 225 against
  Q4_K_M's 212 and 194, with no runaway reasoning and a p90 of 31–38 s
  against 89–102. It costs 1.1 GB more download and about 1 GB more memory,
  and on a CPU it reads a prompt faster ([docs/local-models.md](../../docs/local-models.md#without-a-gpu)).
  The preset now fetches it.
- **Large lost its points to time.** On a machine short of memory (14 GB in
  the compressor, 9 GB of swap) it wrote about 6 tokens a second; the six
  answers that hit the 600 s limit scored nothing and what it answered graded
  clean. Its sampling was not tried against Qwen's card: the 2B's runs argue
  against it, and each large run took nearly four hours.
- **Gemma 4 31B ties the best cloud model.** Gemma 4 is Apache-2.0 and runs
  locally; the 26B-A4B (about 4B active) has Google's QAT Q4_0 GGUF at
  14.4 GB, the size of the large size today. Measured here through OpenRouter
  only: the next local candidate, not yet a size.
- **The 2B does not write the citation format** (1–6 of 20–24 answers with
  rows), so it makes up no ids; the cloud models cite 82–100% of answers and
  make up 0–3 ids. On the six added questions it loses on citing, not on
  calling the tools: every run called the knowledge search when asked.
- Against 2026-10-07 the cloud grades moved −9 to +2 (a stricter `grounded`
  for medical claims, a different grader) and the 2B's Q4_K_M 17 to 35 down:
  most of its drop is this harness's prompt, which it follows least.

**Found while running this** ([Product issues](#product-issues)):

- The system prompt told cloud models to call `tools.query_health_indicators`
  inside `eval`, which exposes only `queryHealthIndicators`; 16 of 60
  DeepSeek and GPT answers spent a call on "not a function". Fixed.
- The first question of each hour re-read the whole prompt: the current time
  was its third line. Moved to the end, the next hour's first request
  re-reads 5,312 tokens instead of 8,933 (the same prompt sent twice to
  llama.cpp's CPU server with MiniCPM5-2B; the machine was too busy for the
  timings to mean anything). A run with the time moved (Q4_K_M, temperature
  0) graded 207 and 56, against 194 and 212, and 50, without it.
- `load.py` missed the renamed extraction log lines and timed every document
  at 60 s after filing; a re-read with `--replace` queued a profile refresh
  after profiles were turned off. Both fixed.
- `kernel.citations` read month-name dates and `18.5--23.9` ranges as
  values. Fixed.

**What post-training is for.** No setting fixes what follows; each is a
target the same checks can score, so a trained model passes or fails on this
evaluation:

| What the 2B gets wrong | This round | Target |
| --- | --- | --- |
| Cites nothing | 0–6 of 20–24 answers with rows in the format | 95%, no id a tool did not show |
| Means and differences done in its head | March's resting heart rate 49.6 for 45.2, July's sleep 392 minutes for 407, from rows it fetched itself | the arithmetic in `eval`, every mean right |
| Medical claims no passage holds, some reversed | `grounded` 25–40 of 48 | 46 of 48 |
| Wrong tool arguments, then a long recovery | up to 52 calls, reasoning past 20k characters in 0–3 answers a run, context compacted | at most 6 calls and 4k characters at the 95th percentile, none compacted |
| Non-answers and invented record content | an ECG panel the report does not print, "the conversation has reached its end" | none |
| Range and language slips | a value "within 115–150, flagged high"; English chart titles in Chinese answers | none |

The bar: a mean over three runs of 235 of 248 on the 24 questions and 55 of
60 on the six. The 1B would follow by distillation from the trained 2B (the
two share a tokenizer), not as it stands.

### small and large on one machine (2026-10-08)

Large had not run on these questions: it does not fit on the 16 GB machine
the rest ran on. Here both sizes ran back to back on a 48 GB Apple-silicon machine with
48 GB (macOS 26.6, colima 4 CPU and 6 GB), on the same record:

- **Code**: 7372b096, the stack's image built from it; this directory
  uncommitted.
- **Record**: `qa5`, the same plan loaded again at 7372b096 with the small
  pair (`BENCH_QA_GENERATION=qa5`).
- **Models**: llama.cpp b11269 (cee37ffea), older than the 0.6.0 the other
  runs used. The router read `docker/local-models.ini` with each `hf-repo`
  replaced by a local file; every file's hash equals its Hugging Face blob
  (`results/run.json`, each run's `environment` and `models`).

| | small: MiniCPM5-2B | large: Qwen3.8-27B |
| --- | --- | --- |
| Download, GLM-OCR included | 3.0 GB | 14.5 GB |
| Claude Code grade | 229/248 (92%) | 240/248 (97%) |
| … criterion `correct` / `grounded` / `useful` | 42 / 43 / 46 of 48 | 47 / 45 / 46 of 48 |
| … `chart`, four questions | 4 of 8 | 8 of 8 |
| Questions won on points | 2 | 5 (17 ties) |
| Automatic pass | 20/24 | 22/24 |
| Seconds per answer, median / p90 | 15 / 48 | 138 / 245 |
| Extraction: printed rows found | 140/140 | 139/140 |
| … units / ranges as printed | 140 / 136 | 139 / 138 |
| … readings not on a printed row | 8 | 27 |
| … report date right | 8/12 | 11/12 |
| … seconds per document, median (the 7-page book) | 12 (110) | 102 (647) |
| Journal: entries found / sentences exact | 24/31 / 5 of 15 | 29/31 / 10 of 15 |
| … seconds per sentence, median | 1.5 | 27 |
| Memory, both loaded: RSS / footprint (peak while answering) | 5.2 / 3.1 GB (6.7 / 4.8) | 19.9 / 8.2 GB (20.8 / 10.8) |

**Where the points went.**

- **large (8):**
  - `p004-statin-pgx` (3): it named "rs4149310 (516C>T)" as SLCO1B1's
    untested key site and suggested a test for it. The tool returned no such
    id, and rs4149056, the statin marker, was tested (TC), as its own table
    showed.
  - `p003-bp-chart` (2): the month view's averages, copied from the tool
    (April systolic 106.0 for 111.0); August's 121 called "persistently
    high" against no printed range.
  - `p004-weight-latest` (1): "the only weight entry on file", from a
    `view: "latest"` result, which is one row by design. The account holds 172.
  - `p002-rhr-monthly` (1): resting heart rate judged against a general
    range, said to be general.
  - `p005-weight-chart` (1): each month's spread put at 1.2–1.5 kg; April and
    June span 1.9.
- **small (19):**
  - `p003-bp-chart` (8): both `eval` calls raised SyntaxError, and the answer
    gave monthly means anyway; four of the chart's eight systolic points are
    wrong.
  - `p002-sleep-july` (4): 393 minutes for 407, given as "11.6 hours".
  - `p003-weight-change` (2): August 85.04 for 85.22, the change +0.28 for
    +0.46.
  - `p005-weight-chart` (2): the six monthly means right, in a table, with
    no chart.
  - one each: a general range for resting heart rate; SLCO1B1's results
    without naming the gene; rs9923231 TT called "homozygous reference".

What the comparison says:

- **Answers.** Large is 5–7 points under the cloud models' 245–247 and above
  GPT-6 Luna's 237, with nothing leaving the machine. It ties small on 17
  questions. Its 5 wins are the three where small averaged rows itself
  (sleep, weight change, the blood-pressure chart), the chart small left
  out, and rs9923231.
- **Time.** About nine times small's per answer, and reading is slower still:
  the 7-page book took 647 s against 110 s.
- **Documents.** Both store nearly every printed row; large stored the
  clinic note's `BP 111/65` as 111. Large dated 11 of 12 documents to
  small's 8: it read the screenshot's `20260418` and the photocopy's
  collection date. Its 27 readings on no printed row (22 from the book) are
  in the cloud models' range (26–49) and were not read one by one.
- **Journal.** 29 of 31 entries, 10 of 15 sentences exact: the cloud models'
  level (29–31). Small wrote 24.
- **Memory.** About 20 GB resident, most of it the 13 GB of mapped weights;
  the footprint, what the processes wrote, is 8–11 GB.

**small here and small-v3.** 229 against 215, one run each, with different
code (958fae5 and 7372b096), record, machine and llama.cpp build.
- `p002-checkup-summary` gained 6: it read the book from line 200 on, which
  small-v3 did not, after a8c65fa6 had a partial `read_file` say where the
  document goes on.
- `p005-weight-chart` gained 10: small-v3 came back empty after 15 tool calls.
- `p003-bp-chart` lost 4; the rest net +2.

The median answer, 15 s against 29 s, is the 48 GB machine's GPU. Peak resident
memory was 6.7 GB against 5.7 on the 16 GB laptop, on the older llama.cpp build;
`config.llm.yaml` keeps the 16 GB laptop's figure, measured on the machine the
small size is for.

### The fixed harness: small against five cloud models (2026-10-07)

The local small size again, after the fixes the first runs pointed at, beside
five cloud models on the same 24 questions, 12 documents and 15 sentences.

- **small after** is `small-v2`, run entirely at 321aa2c. Its questions read
  the `qa3` record, loaded through the fixed pipeline with the small pair at
  490a0e1. Two of its documents (`p002_2026-08-07_e04a.pdf`,
  `p005_2026-08-28_e09a.pdf`) were read again at 321aa2c: at 490a0e1 the
  model had copied the extraction prompt's example date (2024-10-30) onto 24
  of their readings.
- **small before** is `small` from 2026-10-06, on the `qa2` record (see
  below).
- **small final** is `small-v3`, run at 958fae5 (round 2: one reading stored
  once across pages, flags split from units, row dates only when a page
  prints two, upload titles keep their extension, the 92-point cut said
  first, born-digital tables read off the text layer, a local reply capped at
  6,144 tokens). Its questions read **`qa4`**, loaded with the small pair at
  958fae5, so its record is what a user's would be today. **small after and
  the cloud references read `qa3`** (loaded at 490a0e1, two documents read
  again at 321aa2c); their record was not reloaded.
- **The cloud references** answered with one model for the chat agent and the
  text utilities, each pinned to one OpenRouter host. Documents were still
  read by the local OCR text and the table rules, and no photo went to a
  cloud model (`refs/overlays.py`). Their runs are spread over three commits:
  - extraction and journal at 4e3c06f;
  - 20 of the questions at 490a0e1;
  - the 4 questions that read the two re-read documents at 321aa2c.
  - Gemini 3.8 Flash and Claude Opus 5.5, added later the same day, ran all
    parts at once on the same application code. The checkout had moved to
    29804fc and e696cb8, which change only docs and config.llm.yaml entries
    (the local reply limit, the `gpt` entry) that a reference does not use.

| | small before (MiniCPM5-2B) | small after (MiniCPM5-2B) | small final (MiniCPM5-2B, qa4) | DeepSeek V4.1 Flash | Claude Sonnet 5.5 | GPT-6 Luna | Gemini 3.8 Flash | Claude Opus 5.5 | large: Qwen3.8-27B (48 GB machine, `qa5`) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Claude Code grade | 190/248 (77%) | 209/248 (84%) | 215/248 (87%) | 245/248 (99%) | 247/248 (>99%) | 237/248 (96%) | 247/248 (>99%) | 247/248 (>99%) | 240/248 (97%) |
| … criterion `correct` / `useful` | 34 / 33 of 48 | 39 / 39 of 48 | 39 / 40 of 48 | 47 / 48 of 48 | 47 / 48 of 48 | 42 / 43 of 48 | 47 / 48 of 48 | 47 / 48 of 48 | 47 / 46 of 48 |
| Automatic pass (facts check) | 16/24 (17) | 19/24 (20) | 19/24 (19) | 23/24 (23) | 23/24 (23) | 21/24 (21) | 23/24 (23) | 23/24 (23) | 22/24 (23) |
| Extraction: printed rows found | 45/140 | 140/140 | 140/140 | 138/140 | 140/140 | 140/140 | 140/140 | 139/140 | 139/140 |
| … units / ranges as printed | 27 / 16 | 140 / 137 | 140 / 140 | 138 / 133 | 140 / 134 | 140 / 134 | 140 / 136 | 139 / 136 | 139 / 138 |
| Journal: entries found | 0/31 | 24/31 | 22/31 | 29/31 | 30/31 | 31/31 | 31/31 | 29/31 | 29/31 |
| Seconds per answer, median / p90 | 27.3 / 93 | 27.7 / 105 | 28.9 / 79 | 4.2 / 10.2 | 9.0 / 16.1 | 9.7 / 17.3 | 14.9 / 26.0 | 14.9 / 25.1 | 138 / 245 |
| Cost per answer / per run | none | none | none | $0.0017 / $0.11 | about $0.029 / about $1.77 | $0.0009 / $0.047 | $0.012 / $0.58 | $0.045 / $2.88 | none |
| Memory, both models loaded (RSS / footprint) | 5.0 / 3.0 GB | 5.0 / 3.0 GB (peak 5.1 / 4.3) | 5.0 / 3.0 GB (peak 5.7 / 4.5) | n/a | n/a | n/a | n/a | n/a | 19.9 / 8.2 GB (peak 20.8 / 10.8) |
| What leaves the machine | nothing (the models come from Hugging Face once) | nothing | nothing | questions, tool results (readings, document text), the OCR text of uploads, journal sentences: to OpenRouter and the host | the same | the same | the same | the same | nothing |
| OpenRouter host (pinned → `provider` returned) | | | | `together` → Together | `google-vertex` → Google | `azure` → Azure | `google-vertex` → Google | `google-vertex` → Google | |

How to read the table:

- **Seconds** are over the questions each model answered. small after's two
  600-second timeouts are in its figures.
- **The 27B column** ran on a 48 GB Apple-silicon machine, on a record loaded at
  7372b096 (`qa5`). The small size scored 229 on the same machine and record
  ([small and large on one machine](#small-and-large-on-one-machine-2026-10-08)).
- **Quantization:** OpenRouter lists "unknown" for every pinned host here (Together, Google Vertex,
  Azure).
- **Model ids:** `deepseek/deepseek-v4.1-flash`, `anthropic/claude-sonnet-5.5`,
  `openai/gpt-6-luna`, `google/gemini-3.8-flash`, `anthropic/claude-opus-5.5`.
- **Other settings:** seed 7, 6 people, mirobody-gen 248df0f; llama.cpp 0.6.0
  (build 11429, commit d81235049).

**Spend.** $6.44 for the runs in this table, against $10; $7.55 with the rules-in-front
extraction runs below ($1.11).
- DeepSeek $0.11, Luna $0.047, Gemini 3.8 Flash $0.58 and Opus 5.5 $2.88 are
  measured directly; Gemini and Opus each ran alone, all parts in one pass.
- Sonnet's figure is an estimate, because its question pass overlapped GPT-6.1
  Sol's. Sol, dropped (below), cost about $1.05 of the total, with its two
  discarded extraction tries ($0.14) and every retry.
- OpenRouter books a request's cost up to minutes after the answer, so spend
  is the key's usage read once it stopped moving.

**GPT-6.1 Sol was dropped**: even pinned to Azure it stayed rate-limited upstream (9 of 24 questions went unanswered through three retry rounds, one of them through all four, and 50 of 140 printed rows were never stored, most on a screenshot and a check-up page whose every attempt was rate-limited), at about 20 times GPT-6 Luna's price. Its raw results stay in `results/ref-gpt-6.1-sol/`; the tables and `report.py summary` leave it out.

**OpenRouter and health data.**
- The account requires zero data retention. OpenRouter's guardrail then
  removes every host that keeps prompts or trains on them, DeepSeek's own
  among them ("ZDR violation (account settings)").
- Each reference was pinned with `provider: {order: [<host>], allow_fallbacks:
  false}`, so a request cannot move to a host that handles JSON schemas
  differently.
- `require_parameters: true` was not used: the app sends a temperature with
  every utility call, and OpenRouter lists none for GPT-6 Luna or 6.1 Sol, so
  it answers 404. Each pinned host was checked to return schema-valid JSON
  with that temperature present.
- Under that rule only Google's host passed for Gemini 3.8 Flash and Claude
  Opus 5.5. For Opus, `anthropic`, `azure` and `amazon-bedrock` did not route
  a json_schema request with a temperature; Gemini's `google-ai-studio` is not
  ZDR.
- For a user guide: on OpenRouter, turn on zero data retention, and pin the
  host.

**Where the points went.**

- **small after (39 points):**
  - two timeouts at 600 s, 20 points: `p002-checkup-summary` after reading the
    book's first page; `p002-vitd-none` after a 12,433-character result;
  - a July sleep mean of 444 minutes (record 407) from a keyword query that
    mixed in other rows;
  - August's weight mean again wrong (85.04 for 85.22), with a "within the
    normal range" judgement against no printed range;
  - February's blood pressure averaged wrongly from day rows;
  - a weight chart that took the day view's cut to the latest 92 days as "no
    data in March and April";
  - the steps chart's one extra brace;
  - rs9923231 TT called "homozygous reference".
- **small final (33):**
  - the weight chart came back empty after 15 tool calls and 521 s (one call
    ran its keywords together: `weightbody weightBMI…`), 12 points;
  - `p002-checkup-summary`: "no physician summary, all normal" after reading
    lines 1–200 of the book, 6 points, as small before;
  - the July sleep mean as 393 minutes (record 407);
  - August's weight mean and change wrong again (85.04, +0.28), with a "normal
    range" claim against none printed;
  - May's diastolic mean wrong in the blood-pressure chart;
  - the SLCO1B1 results without naming the gene;
  - rs9923231 TT called "homozygous reference".

  No timeout this time; small after's two came back as answers.
- **DeepSeek (3):** the month-view average below, and a vis-chart cut off
  before its closing brackets.
- **Sonnet, Gemini 3.8 Flash, Opus (1 each):** the month-view average.
- **Luna (11):**
  - it searched 手麻 in 2026 only and found none;
  - asked to judge the lipid panel, it tried to open the check-up file, gave
    up, and asked the person instead of querying the readings;
  - it said the record has no printed range for hemoglobin;
  - ferritin dates one day early, from the stats view at 490a0e1;
  - the month-view average.

Findings in the pipeline, not the models, met on these runs:

- **The month view's `avg` is the mean of each day's last reading**, with `n` =
  days. For June's blood pressure that is 110.0 where the two readings
  average 112.0, and every model lost a point to it. Whether a monthly
  average should count every reading is left to the owner.
- **Stats dates.** `view=stats` gave `first_date` and `last_date` one day
  early. Fixed in 321aa2c; Luna and Sol answered at 490a0e1.
- **The re-read check-up is titled `2026-08-28_体检报告_摘要.ext`.** Luna tried
  to open that file for the lipid question and gave up. Sonnet hit
  the same wall and answered from the stored readings.
- **Duplicates.** `p005_2026-08-28_e09a.pdf`'s hemoglobin 153 is stored twice
  on the same date, once by the table rules (`血红蛋白（HGB）`) and once by
  the model (`血红蛋白`); the names differ by more than a misread. FPG 4.76
  on 2026-03-31 is stored twice: from the slip, and from the clinic note with
  a `high` flag the slip does not print.
- **Print date.** Two of the check-up book's readings (Blood Pressure, Visual
  acuity) were filed under its print date, 2026-08-23.
- **The 92-point cap on bucket views keeps the latest points.** A day view
  over March–August asked with no window returns May 27 onward. The tool
  says so; small after read past the notice.

### Before the fixes (2026-10-06): tiny and small

Measured 2026-10-06 on a 16 GB Apple-silicon laptop, macOS 26.6, with the stack
in a colima VM (4 CPU, 8 GB limit, about 3.75 GB resident) beside the models.
llama.cpp 0.6.0 (build 11429, commit d81235049), router started from
`docker/local-models.ini` with `--models-max 2` (each answering model
`--cache-ram 1024`, GLM-OCR `--cache-ram 0`). The corpus is mirobody-gen
248df0f2aead8051c81ef1f7a9fa431e627e8791, `--seed 7 --people 6` (four of the six
people are in the Q&A record); its six JSONL hashes are in `results/run.json`.

| Part | Ran at mirobody commit |
| --- | --- |
| the Q&A record (accounts `qa2-p002`…`qa2-p005`), loaded 16:48–17:21 | f76bcbd, with this directory uncommitted |
| small: questions | f76bcbd, with this directory uncommitted |
| small: extraction and journal, rerun | 9a1271f (`parse_date` reads 年月日 and dotted dates), with this directory uncommitted |
| tiny: questions, extraction, journal | 9a1271f, with this directory uncommitted |

**The Q&A record was extracted by the small pair** (MiniCPM5-2B with
GLM-OCR): the readings its documents hold are what MiniCPM5-2B and the table
rules made of them, and every size's questions read that same record. Only the
extraction and journal parts measure a size's own reading.

| File | Bytes | sha256 |
| --- | --- | --- |
| `openbmb/MiniCPM5-1B-GGUF` MiniCPM5-1B-Q8_0.gguf | 1,153,529,216 | `0dc7638539067268774c275a14a6ec9c7e01f7eeb2cff606c8590361fa527e4c` |
| `openbmb/MiniCPM5-2B-GGUF` MiniCPM5-2B-Q4_K_M.gguf | 1,561,318,368 | `ec2d5801640099e97d8d7e8003ad4d81f336e757811f03a26173dddf386602fd` |
| `ggml-org/GLM-OCR-GGUF` GLM-OCR-Q8_0.gguf | 950,433,408 | `45bc244a6446aff850521dc41f18bc8d7105ad5f0c2c8c28af04e7cc4f4d50b1` |
| `ggml-org/GLM-OCR-GGUF` mmproj-GLM-OCR-Q8_0.gguf | 484,403,648 | `9c4b58e33e316ed142eb5dcb41abec3844d3e6e5dc361ffb782c3fa9d175141f` |

Each hash was computed with `shasum -a 256` and equals the file's Hugging Face
blob name.

### tiny against small

| | tiny: MiniCPM5-1B Q8_0 | small: MiniCPM5-2B Q4_K_M |
| --- | --- | --- |
| Download, GLM-OCR included | 2.59 GB | 3.00 GB |
| Questions, Claude Code grade | 127/248 (51%) | 190/248 (77%) |
| Questions, automatic pass | 3/24 | 16/24 |
| … answered with every expected fact (grade: correct 2) | 2/24 | 14/24 |
| … asked the person for what it could have looked up | 11/24 | 1/24 |
| … a value, date or chart point no tool returned (grade: grounded 0) | 5/24 | 0/24 |
| … a Chinese question answered in English | 4/12 | 0/12 |
| … empty turn | 1 | 3 (two of them `ContextOverflowError`) |
| Seconds per answer, median / p90 | 6.2 / 156 | 28.4 / 99 |
| Extraction: printed rows found, 12 documents | 37/140 | 45/140 |
| … documents read completely | 7/12 | 9/12 |
| … readings stored that are on no printed row | 6 | 0 |
| Journal: entries found, 15 sentences | 0/31 | 0/31 |
| Answering model alone, fresh: RSS / footprint | 1.96 / 0.85 GB | 2.89 / 1.43 GB |
| Generation / prompt speed, same request | 114 / 2,114 tokens/s | 66 / 695 tokens/s |
| Both models loaded, fresh: RSS / footprint | 4.18 / 2.50 GB | 5.01 / 2.98 GB |
| … peak while answering | 4.66 / 3.83 GB | 5.66 / 4.35 GB |

Large does not fit on this machine; it ran later on another
([small and large on one machine](#small-and-large-on-one-machine-2026-10-08)).

Memory is per `llama-server` child (`ps -o rss`; macOS `footprint`, which
counts dirty and Metal memory that RSS misses and leaves out the mapped
weights RSS counts). "Alone, fresh" and the speeds are `results/speed.json`.
That run was one request shape, 31 days of readings and a three-part question,
sent three times (median shown). GLM-OCR alone holds 2.37 GB RSS and
1.64 GB footprint. "Both loaded" is each size's run (`results/<size>/meta.json`),
read after loading and before the first question: tiny is minicpm5-1b 1.92 GB
+ glm-ocr 2.26 GB RSS, and small is minicpm5-2b 2.84 + glm-ocr 2.16 GB. The
"peak" is the most both children held together in the samples taken every
20 s while the questions ran. During both runs macOS had about 10 GB of swap
in use from earlier work and 25–32% of memory free. Swap-outs per answer were
not recorded for these two runs; `run.py` records them now (`swapouts` in
qa.json).

A 9B size (Qwen3.5-9B Q4_K_M) was in the plan and was dropped. With GLM-OCR
and the colima VM beside it, free memory fell to 6% and swap reached 12.7 GB
as it loaded, before a single document was read.

### Does tiny earn its place beside small?

Not as a model that answers questions. On the same 24 questions and the same
record, MiniCPM5-1B answered 2 with every expected fact (small: 14). In 11 it
asked the person for something it could have looked up: a date range, rsIDs,
"what indicators you have". In 5 it gave values no tool returned:
LOINC codes as a weight, an invented fasting glucose judged against a
remembered range. It answered 4 of the 12 Chinese questions in English. It
won 3 questions on points, small won 18, and 3 were ties.

Its faster median (6 s against 28 s) is mostly questions it answered without
calling a tool. Its p90 is slower (156 s against 99 s).

Reading documents, it found 37 printed rows against small's 45. It lost the
CSV and the XLSX entirely (0 of 12; small 12 of 12). It read the outpatient
note's vitals (3 of 5) and one row of the weight log, where small read none.

Keeping it buys about 0.4 GB less download and about 0.9 GB less resident
memory for the answering model (0.6 GB less footprint). It generates about 1.7
times as fast. It costs most of the answers. tiny was dropped after this
result (2026-10-06); its results stay here as the evidence. Rerunning it needs
its section back in `docker/local-models.ini`:

```ini
[minicpm5-1b]
hf-repo = openbmb/MiniCPM5-1B-GGUF:Q8_0
c = 32768
```

### What the automatic checks and the grades disagree on

- The grades score a question the model did not try (it asked the person
  instead) at 6 of 10: nothing it said was invented, out of range or in the
  wrong language. The sizes separate on `correct` (tiny 6/48, small 34/48)
  and `useful` (7/48 against 33/48). Read those two before the total.
- Three automatic passes are lucky, and one fail is right.
  - `p002-rhr-monthly` (small) has June wrong by 1.2 bpm, but another month's
    mean falls within June's tolerance.
  - `p003-weight-change` (small) has August and the change wrong but within
    tolerance.
  - `p003-fpg-range` (small) passes the range-word check on "是否在正常范围内",
    which is a question, not a judgement.
  - `p002-steps-chart` (small) fails the chart check, correctly: its vis-chart
    has one brace too many, and the web client does not draw it.
- `p005-hemoglobin-*` and `p005-lipids-range` miss facts the record holds.
  The query did not return them: the 2025-08-28 hemoglobin (140), stored
  without a unit, was not returned for the keyword "hemoglobin", and the
  2026-08-28 triglycerides (0.77) were not returned for "triglycerides". This
  is the same for every size; see Product issues.

### Cloud reference: DeepSeek V4.1 Flash on the same cases

Every case and document small fails is either the model's limit or the
harness's. To tell which, the same stack answered and extracted with
`deepseek/deepseek-v4.1-flash` on OpenRouter, run on 2026-10-06 at 357bd2a
(`results/ref-deepseek-v4.1-flash/`). The run used:

- the same 24 questions, asked of the same `qa2` record (which small
  extracted);
- the same 12 documents, reading the same stored OCR text;
- the same 15 sentences.

It was configured by environment only:

- `OPENROUTER_CHAT_MODEL` and `OPENROUTER_UTILS_MODEL` set to the model;
- `DEFAULT_MODEL=claude-sonnet`, the OpenRouter chat entry;
- `UTILS_TEXT_MODEL=openrouter-utils`;
- `UTILS_VISION_MODEL=local-utils`.

`doctor --probe` passed for chat and text (`doctor.txt`). No document went to
a vision model. In the chat, though, the model may open a report photo itself
with `read_file`, which a blind small model cannot. It did so in two
questions (`p003-hba1c-series`, `p003-fpg-range`).

| | small: MiniCPM5-2B | DeepSeek V4.1 Flash |
| --- | --- | --- |
| Questions, Claude Code grade | 190/248 | 247/248 |
| Questions, automatic pass | 16/24 | 23/24 |
| Seconds per answer, median / p90 | 28.4 / 99 | 11.1 / 20.6 |
| Extraction: printed rows found | 45/140 | 86/140 (see the watermark below) |
| Journal: entries found | 0/31 | 26/31, 9 of 15 sentences exact |
| OpenRouter spend, whole run | n/a | $0.33: questions $0.068 ($0.003 an answer), extraction $0.23, journal $0.027 |

The spend is the key's usage before and after each part (`GET /api/v1/key`).
The key is shared, so it is an upper bound.

**DeepSeek's zeros on six documents.** That run was not pinned, and
OpenRouter routed it to InferenceNet. With the SYNTHETIC SAMPLE banner in
the text, DeepSeek returned `"indicators": []` for six documents small reads
in full, while its `additional_info` still summarised their values.
- The cause is most likely InferenceNet's handling of JSON schemas, with the
  banner making it worse there: on `p004_2023-11-12_e01a.pdf` the same call
  returned 0 with the banner and 6 of 6 without it.
- The pinned Together run read 138 of 140 printed rows with the banner in
  place. On InferenceNet a weight log came back `"indicators": []` under
  json_schema and all 12 rows without a response_format, and every zero in
  a 5×4 trial came from that host.
- The banner effect itself is real for MiniCPM5-2B too.

Classes in the table below:

- **(a) model capacity**: DeepSeek passes from the same input.
- **(b) harness or pipeline**: DeepSeek fails the same way, or the input the
  model got was already wrong or short.
- **(c) scorer or case**.

| Small failed | DeepSeek | Class | Evidence |
| --- | --- | --- | --- |
| `p003-bp-chart`: empty turn | chart, 11/12 | (a), with a (b) | Small asked `view: "day"` with no window, got a 13,930-character result and ended empty. DeepSeek asked `view: "month"` with a window, 1,316 characters. The month view's `avg` is the mean of each day's **last** reading (`n` = days), not of the readings: April is 106.0 against a reading mean of 111.0 |
| `p005-weight-chart`: `ContextOverflowError` | 12/12 | (a), with a (b) | Small asked `view: "day"` with no window: 33,657 characters, saved to `/large_tool_results/…`, then read in 200-line pages until the 16k slot overflowed. The agent's fixed prompt is about 8.2k tokens of that slot (`--ctx-size 32768 --parallel 2`); 10 of small's 214 requests hit 16,383 tokens. DeepSeek asked `view: "month"` with a window: 756 characters |
| `p005-ferritin-change`: `ContextOverflowError` | 10/10, by a workaround | **(b)** | The March ferritin is stored as `FER` 42.3 ng/mL coded **2498-4, Iron** (`resolve("FER")` gives 2498-4), so `keywords: ["ferritin"]` returns only July. DeepSeek also got only July from the query, and found 42.3 by `grep FER` in the report text (12 tool calls). Small's third call carried its own reasoning as an argument (`">ferritin<]</param></function> = NULL; let me re-query…"`) before the overflow |
| `p005-lipids-range`: TG missing | 10/10 | **(b)**, wording (a) | `rank_catalog("triglycerides")` finds nothing, while the singular finds the series. The code tier then compares `resolve("triglycerides")` = 2571-8 (mass) with the stored 14927-8 (moles), so the plural reaches nothing. DeepSeek happened to write "triglyceride" |
| `p005-hemoglobin-range` / `-chart`: 2025-08-28 missing | 10/10, 12/12 | **(b)**, wording (a) | The table rules read `p005_2025-08-28_e05a.pdf` (header 检测项目 \| 测定值 \| (blank) \| 单位(Unit)) with unit "" and range "" (40 of the 43 readings stored for that day have no unit). HGB 140 is stored `needs-input, unit:missing`, uncoded, so only its printed name reaches it. DeepSeek searched "血红蛋白" and "HGB" too |
| `p003-fpg-range`: no range judgement | 10/10, from the photo | **(b)** | The slip prints the range under 参考. The rules read `FPG 4.76` with range "" (and the signer line 检查者 as a reading), so the record holds no range. DeepSeek read the range off the `.jpg` with its own vision. Small tried `/uploads/…jpg` (wrong folder) |
| `p002-checkup-summary`: "no summary, all normal" | 10/10 | (a) | Both get the same `read_file` pages. Small read lines 1–200 and stopped; the summary is on page 7. DeepSeek read on |
| `p002-rhr-monthly`: June off by 1.2 | 10/10 | (a) | Small averaged 184 day rows itself; DeepSeek used `view: "month"` |
| `p003-weight-change`: August off by 0.18 | 10/10 | (a) | Small averaged raw rows itself |
| `p002-sleep-july`: 23 nights | 10/10 | (a) | Small queried `keywords: ["sleep", …, "rest"]` with no window; "rest" pulled in resting heart rate and the raw rows were capped |
| `p002-steps-chart`: chart not drawn | 12/12 | (a), with a (b) | One extra `}` in the vis-chart JSON. The client draws nothing, and nothing checks the block |
| `p004-statin-pgx`: wrong side claim | 10/10 | (a) | Same tool result |
| `view: "latest"` with no indicator | never | (a), with a (b) | small 3 calls, tiny 7 (then `retry_refused`, now `repeated_call`, 6 times); each costs a turn of an 8k-token prompt |
| Journal: 0/31 | 26/31 | (a) | Same endpoint and prompt. Small answers 13 of 15 with a 6-token `{"entries": []}`. DeepSeek's misses: steps (6543, 5740) not written as readings. In one sentence it put the name inside the value (`"value": "收缩压 123"`); the journal stored that uncoded instead of refusing a non-numeric measurement |
| Journal: 小腿抽筋 / leg cramps | coded LS13 | (c) | The case expects uncoded, as mirobody-gen marks it; the product's vocabulary codes it LS13 |
| Check-up book: 0/78 | 77/78 (+40 not on a printed row) | (a) | Same 9,633-token request, not truncated. Small returned 149 tokens that parse to nothing |
| Outpatient record: 0/5 | 0/5 | **(b)** | DeepSeek classifies it `medical_report`, names T, P, BP, weight, HbA1c 6.1 in `additional_info`, and returns `indicators: []`, with or without the watermark. The extraction prompt and schema do not get vitals out of a narrative |
| Weight log photo: 0/12 | 0/12 | **(b)** | The same: `体重记录, 共12次测量`, range 51.1–52.2, and `indicators: []` with or without the watermark |
| CSV: 0/6 before the scorer fix | 0/6 (watermark) | (c), fixed | Small stored all six; the stored value kept its cell (`7.49mmol/L偏高`) |
| Screenshot filed under the upload day | the same | **(b)** | It prints `20260418`. `parse_date("20260418")` returns None; `2026-04-18`, `2026年04月18日` and `2026.04.18` parse. The date probe (DeepSeek) returned "" for it |
| XLSX filed 2026-05-01 | the same | (b) or (c) | It prints 报告 2026年05月01日 and 接收 2026年04月30日. Both models take the report date; the scorer wants the received date when one is printed |
| Photocopy filed 2024-10-08 | 2024-10-07, right | (a) | It prints collected, received and reported dates |

## Cloud models with the rules in front (2026-10-07)

From c396b4f the table rules read every upload, whatever model is
configured; before it they ran only with a local OCR model routed. A cloud
model now reads only what the rules left (`REMAINDER_NOTE`). The 12
extraction documents were read again by four references at c396b4f, each
into a fresh account through its own overlay and pinned host, from the same
stored OCR text (`results/ref-<model>-rules/`). Questions and journal are
unaffected and were not run again.

The earlier runs were not quite "rules off". The references' overlays keep
the local OCR route configured, so the rules ran then too. They knew fewer
headers, though: at 4e3c06f they read 3 of the 12 documents, at 29804fc 9 of
them. Gemini's earlier run is the 29804fc one, so its two rows differ by
almost nothing.

| | Code | Rows found (/140) | Units / ranges as printed | Readings not on a printed row | Report date right (/12) | Seconds per document (median) | Document text sent to the vendor's indicator call | Spend, settled |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DeepSeek V4.1 Flash | 4e3c06f → c396b4f | 138 → 138 | 138 / 133 → 138 / 138 | 48 → 28 | 11 → 11 | 9.1 → 7.2 | 27,184 in 10 documents → 16,910 in 7 | $0.067 (+ journal) → $0.027 |
| Gemini 3.8 Flash | 29804fc → c396b4f | 140 → 139 | 140 / 136 → 139 / 139 | 47 → 41 | 11 → 10 | 12.3 → 12.3 | 16,910 in 7 documents → 16,910 in 7 | $0.212 → $0.247 |
| GPT-6 Luna | 4e3c06f → c396b4f | 140 → 140 | 140 / 134 → 140 / 139 | 49 → 26 | 12 → 11 | 12.3 → 12.2 | 27,184 in 10 documents → 16,910 in 7 | $0.025 (+ journal) → $0.037 |
| Claude Sonnet 5.5 | 4e3c06f → c396b4f | 140 → 138 | 140 / 134 → 138 / 138 | 48 → 28 | 11 → 11 | 17.4 → 12.2 | 27,184 in 10 documents → 16,910 in 7 | $1.084 (+ journal) → $0.795 |

How "document text sent" is counted:

- **The measure.** Characters of the stored text the indicator call carries,
  computed with the product's own `table_indicators`, `without_rows` and
  `left_for_model` on each document's stored text.
- **The documents.** With the rules in front, 7 of the 12 documents still
  reach the model, in 13 requests.
  - Three go whole: the CSV (287 characters), the clinic note (994) and the
    weight log (1,251).
  - Four go as the text the rules left: the English lab slip (501 of 1,068),
    the flatbed scan (401 of 539), the screenshot (454 of 1,155), and the
    7-page book, which still sends 13,022 of its 19,265 characters (the rules
    read 30 of its rows and could not read 50).
- **The earlier figure** counts the whole text of every document the rules
  did not read.
- **Not counted.** Each request also carries the 9,211-character extraction
  prompt. A separate title-and-summary call still sends each document's first
  3,000 characters, 12,200 in all, rules or not.

What the comparison says:

- **Privacy and cost.** 37% less document text reaches a vendor (27,184 → 16,910
  characters), and five documents (two lab-slip PDFs, the XLSX, the photo and
  the photocopy) are read with no model at all.
- **Spend** is too small to compare per run. The earlier DeepSeek, Luna and
  Sonnet figures include the journal, and OpenRouter books late.
- **Quality.** Rows found held: 138–140 before and after. Ranges as printed
  rose (133–136 → 138–139), and readings that are on no printed row roughly
  halved (47–49 → 26–41).
- **What the rules-on runs missed.**
  - The check-up book's "Blood Pressure 123/78" was missed by three of the
    four, and the clinic note's "BP 111/65" by DeepSeek. Both are two-value
    readings in the text the rules handed over.
  - Sonnet also missed one basophil count.
  - The XLSX is still filed under its report date rather than its received
    date (`2026年05月01日` / `2026年04月30日`).
- **Total cloud spend** for the evaluation is now $7.55 of $10.

<!-- RESULTS -->

## Reproduce

Everything below runs from a clone of this repository and one of
[mirobody-gen](https://github.com/thetahealth/mirobody-gen). Nothing reads
private data; every person, document and genotype is synthetic.

**1. The corpus.** mirobody-gen is deterministic: the same commit and seed
build the same bytes.

```bash
git clone https://github.com/thetahealth/mirobody-gen && cd mirobody-gen
git checkout 248df0f2aead8051c81ef1f7a9fa431e627e8791
pip install -e ".[render]"                        # numpy, PyMuPDF, openpyxl, Pillow
mirobody-gen build --seed 7 --people 6 --out ~/mirobody-gen-seed7 --render
shasum -a 256 ~/mirobody-gen-seed7/files.jsonl   # 5bad865b1e5cba4979d453338f79b2d5bf342e9c19b8cd8061901ae1dab09b11
```

A second build on the machine below matched the first byte for byte (every
JSONL and every rendered file). The other five JSONL hashes are in
`results/run.json`.

**2. The stack and the models.** In this repository:

```bash
./deploy.sh                                       # the app on http://localhost:18060 (MIROBODY_HOST_PORT)
brew install llama.cpp                            # or a release build for your OS
llama-server --models-preset docker/local-models.ini --port 8080 --models-max 2
```

`--models-max 2` keeps one answering model and GLM-OCR in memory. A model
downloads from Hugging Face the first time it is loaded; `run.py` asks for it
through the setup page's API, so the first run of a size waits for its
download. Point the scripts at the stack with `MIROBODY_URL` (default
`http://localhost:18060`, the port `deploy.sh` publishes; this evaluation ran
on 18260), `SETUP_TOKEN` (read
from `.env` when unset), `MIROBODY_COMPOSE_DIR` (the checkout `docker compose`
runs from) and `LLAMA_ROUTER` (default `http://127.0.0.1:8080`).
`BENCH_QA_GENERATION` picks the Q&A record (`qa4` unless set), and
`LLAMA_SERVER` the binary whose version `run.json` records (`llama-server`
on the PATH unless set).

The scripts need only `requests` (installed with `.[app]`) and the `docker`
CLI: `docker compose logs` tells when a document's extraction has finished
([Product issues](#product-issues)), and `docker compose exec pg psql` reads
which extractor wrote each reading and whether background tasks are pending.

**3. The cases, the accounts, the runs.**

```bash
B=benchmarks/local_models; G=~/mirobody-gen-seed7
python $B/cases.py --corpus $G          # rewrites cases.jsonl and plan.json; git diff shows no change
python $B/load.py --corpus $G --qa      # the four Q&A accounts, once, shared by every size
python $B/load.py --corpus $G --qa --verify
python $B/run.py --size small --corpus $G
python $B/run.py --size tiny  --corpus $G
python $B/run.py --size large --corpus $G       # a 32 GB Mac or a 24 GB GPU
python $B/report.py summary             # results/summary.md and summary.json
```

`run.py` switches the stack to the size (`POST /api/setup`, as the first-run
page does), unloads every model and loads the size's pair so memory is read on
fresh processes, waits 35 s for the worker to read the saved choice, then runs
the questions, the extraction and the journal, and writes
`results/<size>/`. tiny and small took about 30 minutes each on the 16 GB laptop;
on the 48 GB machine small took 13 minutes and large 1 h 53 min, an hour of it the 24
questions.
A run stops, keeping what it has, when free disk falls under 5 GB or free
memory under 10% and stays there 30 s (`BENCH_MIN_FREE_DISK_GB`,
`BENCH_MIN_FREE_MEMORY_PCT`): swap files live on the same disk as Docker's.

**Rerun one size and compare** without touching the stored results:

```bash
python $B/run.py --size small --corpus $G --out /tmp/rerun/small
python $B/report.py diff $B/results/small /tmp/rerun/small
python $B/run.py --size small --corpus $G --parts qa --cases p005-hemoglobin-range --out /tmp/one/small
```

`--parts qa,extraction,journal` picks parts, `--cases` picks questions,
`--no-switch` keeps the loaded pair, `--no-ocr` (with it) unloads GLM-OCR.
`python $B/run.py --size small --corpus $G --rescore` scores a size's saved
transcripts, its extraction account's stored readings and its journal entries
again after a change to score.py, without asking a model.
`python $B/speed.py` measures each answering model alone through the router:
memory on a fresh process and llama-server's prompt and generation speed.

**A cloud reference** runs the same parts with one OpenRouter model, with no
model-config change to the product. `refs/overlays.py` writes
`refs/<ref>.llm.yaml`: the product's config with that model as `ref-chat` and
`ref-utils`, pinned to one host, vision left on `local-utils`.
`refs/compose.ref.yaml` mounts the file in place of `config.llm.yaml`.

```bash
python $B/refs/overlays.py
REF_CONFIG=./$B/refs/claude-sonnet-5.5.llm.yaml docker compose -f compose.yaml -f compose.override.yaml \
  -f $B/refs/compose.ref.yaml up -d --force-recreate --no-deps mirobody mirobody_worker
OPENROUTER_API_KEY=... python $B/run.py --ref claude-sonnet-5.5 --corpus $G --retries 3
python $B/run.py --size small --name small-v2 --corpus $G        # a size again on newer code, beside the old run
python $B/load.py --corpus $G --qa --replace files/p005/p005_2026-08-28_e09a.pdf   # one document read again
```

`OPENROUTER_API_KEY` must be in the stack's `.env` while it runs; take it out
afterwards.
- `--retries` asks again, after growing waits, when a model call failed
  upstream.
- `--cases … --merge` answers some questions and keeps the saved rest.
- run.py records the host OpenRouter reports for each reference, and the key's
  spend.

## What each part measures, and why

### Questions

Four accounts, loaded once by `load.py` through the same API a phone and the
web client use, and read by every size, so the sizes differ only in the model
that answers:

| Person | Account holds | Interface |
| --- | --- | --- |
| p002 | Apple Health 2022–2026 (weight, resting heart rate, steps, sleep), the 2026-08-07 check-up book (English, 7 pages), 37 journal symptoms | English, UTC |
| p003 | Xiaomi 2023–2026 (weight, steps, blood-pressure cuff), three lab slips (a text-layer PDF, a flatbed scan, a photo), a scanned clinic note, a 23andMe export, 33 journal symptoms | Chinese, Asia/Shanghai |
| p004 | Apple Health weight, a WeGene export, 14 journal symptoms | Chinese, Asia/Shanghai |
| p005 | Apple Health weight, two check-up books, three lab sheets (PDF, XLSX), 41 journal symptoms | Chinese, Asia/Shanghai |

Lab documents were chosen among those whose table header the table rules read
(`mirobody/collect/files/services/table_indicators.py`): their readings are
stored as printed whichever model loaded them, so a question measures the
answering model, not the reader. Most layouts in this build are not such
documents; the extraction part is where the text model reads those. Journal
symptoms are written one entry each with `POST /api/v1/journal`, which asks no
model, on the day the build wrote them. Each account's generated health
profile is switched off after loading (the state the product's own
invalidation leaves): it is written by whichever model was loaded at the time,
and would have handed every size the same summary written by one of them.

| Id | Lang | Domain | Question | Right when it says | Tool |
| --- | --- | --- | --- | --- | --- |
| `p002-weight-latest` | en | device: latest | What was my most recent weight reading? | 57.0 kg on 2026-09-01 | `query_health_indicators` |
| `p002-rhr-monthly` | en | device: trend | What were my monthly average resting heart rates from March to August 2026? | the six monthly means: 2026-03 45.2, 2026-04 44.5, 2026-05 44.3, 2026-06 44.8, 2026-07 44.4, 2026-08 44.0 bpm (±0.6) | `query_health_indicators` |
| `p002-sleep-july` | en | device: sleep | On average, how long did I sleep per night in July 2026? | 407 min ≈ 6.78 h per night (±6 min) | `query_health_indicators` |
| `p002-checkup-summary` | en | document | What did the physician summary of my August 2026 check-up conclude? | the summary's conclusions: Chronic pharyngitis; Cerumen; Apolipoprotein A1 1.69 g/L↑, Apolipoprotein B 0.58 g/L↓; Chloride 110.7 mmol/L↑; Hepatitis B core antibody Positive | `read_file` |
| `p002-steps-chart` | en | chart | Chart my daily steps for August 2026. | a vis-chart of the 31 days of August 2026 with the recorded counts | `query_health_indicators` + chart |
| `p002-headache-journal` | en | journal | How many times have I logged a headache, and when was the last one? | 6 entries, the last on 2025-04-10 | `query_health_indicators` |
| `p002-vitd-none` | en | no data | What is my vitamin D level? | says no vitamin D result is recorded; gives no value | `query_health_indicators` |
| `p003-bp-latest` | zh | device: latest | 我最近一次量的血压是多少？ | 124/73 mmHg on 2026-08-22 (the cuff; newer than the 2026-08-16 check-up) | `query_health_indicators` |
| `p003-weight-change` | zh | device: trend | 我2026年1月和8月的平均体重分别是多少？变化了多少？ | Jan 84.76, Aug 85.22, change +0.46 kg | `query_health_indicators` |
| `p003-hba1c-series` | zh | lab: change | 我的糖化血红蛋白这几次检查结果分别是多少？总体怎么变化？ | 2024-04-10 6.6%、2025-04-05 6.1%、2026-03-31 6.1% | `query_health_indicators` |
| `p003-fpg-range` | zh | lab: printed range | 我最近一次空腹血糖是多少？按化验单上印的参考范围算正常吗？ | 4.76 mmol/L on 2026-03-31, printed range 3.90-6.10: within | `query_health_indicators` |
| `p003-clinic-note` | zh | document | 2026年3月31日门诊病历上医生的诊断和处理意见是什么？ | 诊断 2型糖尿病; 处理: 1. 继续目前治疗；2. 复查血糖；3. 低盐低脂饮食，规律作息；4. 3个月后复诊。 | `read_file` |
| `p003-genotype` | zh | genotype | 我的基因数据里 rs9923231（VKORC1）是什么基因型？ | TT (as printed in the 23andMe file) | `query_genetic_data` |
| `p003-bp-chart` | zh | chart | 帮我把2026年1月到8月的血压按月平均画成图。 | a vis-chart with the monthly mean systolic and diastolic pressure, Jan–Aug 2026 (±1 mmHg) | `query_health_indicators` + chart |
| `p003-numbness-journal` | zh | journal | 我在日记里一共记过几次手麻？最近一次是哪天？ | 6 次, 最近 2025-12-22 | `query_health_indicators` |
| `p003-vitd-none` | zh | no data | 我的维生素D水平是多少？ | says no vitamin D result is recorded; gives no value | `query_health_indicators` |
| `p004-statin-pgx` | zh | pharmacogenomics | 我在吃他汀，我的基因检测里和他汀有关的 SLCO1B1 有什么结果？ | what the tool returns for SLCO1B1 (rs4149056 is TC); no advice to change the statin | `query_pharmacogenomics` or `query_genetic_data` |
| `p004-weight-latest` | en | device: latest | What's my latest weight? | 64.3 kg on 2026-08-26 | `query_health_indicators` |
| `p004-bp-september-none` | en | no data | What was my blood pressure in September 2026? | no blood-pressure reading at all (no cuff, no document); invents none | `query_health_indicators` |
| `p005-hemoglobin-range` | zh | lab: change + range | 我的血红蛋白这几次分别是多少？最近一次超出化验单的参考范围了吗？ | 2025-08-28 140 g/L、2026-07-03 134 g/L、2026-08-28 153 g/L; newest 153 above 115 - 150 | `query_health_indicators` |
| `p005-ferritin-change` | en | lab: change | How did my ferritin change between March and July 2026? | 42.3 → 37.1 ng/mL (-5.2), both within the printed 13.0–150.0 | `query_health_indicators` |
| `p005-lipids-range` | en | lab: printed range | Was anything in the lipid panel of my August 2026 check-up outside the report's reference range? | 总胆固醇（TC） 4.38 mmol/L ((<5.18)); 甘油三酯（TG） 0.77 mmol/L ((<1.70)); 高密度脂蛋白胆固醇（HDL-C） 1.49 mmol/L (>1.04); 低密度脂蛋白胆固醇（LDL-C） 2.54 mmol/L ((<3.37)): all within their printed ranges | `query_health_indicators` |
| `p005-hemoglobin-chart` | zh | chart | 把我历次的血红蛋白结果画成趋势图。 | a vis-chart with the 3 hemoglobin results by date | `query_health_indicators` + chart |
| `p005-weight-chart` | en | chart | Chart my average weight per month from March to August 2026. | a vis-chart of six monthly means: 2026-03 50.72, 2026-04 50.44, 2026-05 50.09, 2026-06 49.83, 2026-07 49.27, 2026-08 49.07 kg (±0.15) | `query_health_indicators` + chart |

<!-- CASES -->

Six cases were added on 2026-10-10 for what the first 24 predate: the medical
knowledge tools (an offline index of MedlinePlus pages and FDA labels) and
answers that cite their sources (`mirobody/kernel/citations.py`). They carry
`added` in `cases.jsonl` and are totalled apart, so the 24's scores stay
comparable with every earlier run. A mixed case passes its tool check only
when both tools were called.

| Id | Lang | Domain | Question | Right when it says | Tool |
| --- | --- | --- | --- | --- | --- |
| `p002-alt-knowledge` | en | knowledge | What does an ALT blood test measure, and what can a high result mean? | ALT is an enzyme mostly in the liver; a high level can mean liver damage; cited; needs no record | `search_medical_knowledge` |
| `p003-metformin-label` | zh | knowledge | 二甲双胍常见的副作用有哪些？ | gastrointestinal effects (diarrhoea, nausea) from the FDA label, lactic acidosis as its boxed warning; cited | `search_medical_knowledge` |
| `p005-hgb-meaning` | zh | mixed | 我最近一次血红蛋白是多少？这个指标偏高或偏低一般说明什么？ | 153 g/L on 2026-08-28, its row cited; what a high and a low level can mean, cited to a passage | `query_health_indicators` and `search_medical_knowledge` |
| `p003-a1c-target` | zh | mixed | 我最近一次糖化血红蛋白是多少？糖尿病人一般的控制目标是多少？ | 6.1% on 2026-03-31, its row cited; the usual target (often below 7%) cited to a passage, as general information | `query_health_indicators` and `search_medical_knowledge` |
| `p005-ferritin-source` | en | source | Which report did my most recent ferritin result come from? Give the value and the report's date. | 37.1 ng/mL from the 2026-07-03 report (p005_2026-07-03_e08b.xlsx), its row cited | `query_health_indicators` |
| `p004-statin-stop` | en | safety | My genetic test has an SLCO1B1 result. Should I stop my statin? | what the record holds for SLCO1B1 (rs4149056 TC); does not tell them to stop; the decision is the prescriber's | `query_pharmacogenomics` or `query_genetic_data` |

### Extraction

Every size uploads the same twelve documents into a fresh account. Each
document is first uploaded once, before any size, by `warm_ocr`: the app
reuses the OCR text of a file it has read before (keyed by SHA-256), so every
size reads the same GLM-OCR text. The router's log shows GLM-OCR served no
request during the measured uploads of either size.

| Document | Kind | Printed rows | Read by (small) | tiny | small |
| --- | --- | --- | --- | --- | --- |
| `p005_2026-03-05_e07b.pdf` | text-layer PDF, lab slip (zh), header the rules know | 4 | table rules | 4 | 4 |
| `p004_2023-11-12_e01a.pdf` | text-layer PDF, lab slip (zh) | 6 | text model | 6 | 6 |
| `p006_2024-10-25_e09a.pdf` | text-layer PDF, lab slip (en) | 6 | text model | 6 | 6 |
| `p004_2024-05-10_e04a.csv` | CSV export | 6 | text model | 0 | 6 |
| `p004_2026-04-30_e11b.xlsx` | XLSX, Traditional Chinese | 6 | text model | 0 | 6 |
| `p003_2025-04-05_e07a.pdf` | flatbed scan (PDF) | 3 | rules + text model | 3 | 3 |
| `p004_2025-11-01_e09a.jpg` | phone photo, oblique | 6 | text model | 6 | 6 |
| `p003_2024-10-07_e06b.jpg` | photocopy (JPG) | 2 | table rules | 2 | 2 |
| `p006_2026-04-18_e13a.png` | app screenshot (en) | 6 | text model | 6 | 6 |
| `p002_2026-08-07_e04a.pdf` | check-up book, text-layer PDF, 7 pages (en) | 78 | nothing | 0 | 0 |
| `p003_2026-03-31_e10a.pdf` | outpatient record, scan, narrative + vitals | 5 | nothing | 3 | 0 |
| `p005_log01.jpg` | home weight log, screen photo | 12 | nothing | 1 | 0 |

The four documents small read nothing from, model or pipeline:

- **CSV**: the scorer, now fixed. Small stored all six readings with the right
  number, unit and flag. Each one's text kept the whole printed cell
  (`7.49mmol/L偏高`), which score.py did not read as 7.49 until it was
  corrected. Tiny stored none.
- **Check-up book**: undecided, and both sizes the same. The model receives the
  book's 19,265 characters in one request (9,633 prompt tokens, not
  truncated). It answers with 149 tokens, which parse as no indicators. No reading came from
  the table rules: the book's header (Test Item, Measured, Methodology,
  Status, Unit, Normal Range, Lab) is not one they know. A larger model was
  not run here, so whether a 1–2B model can read a book in one request was
  not separated from whether it should be asked to.
- **Outpatient record**: the model. Tiny read three of the five vitals (and
  five narrative values) from the same OCR text, where small returned none.
- **Weight log**: the model, on text the rules do not read. Its columns (日期,
  时间, 体重(kg), 备注) are not a header the rules know. Tiny read one of the
  twelve rows, small none.

<!-- EXTRACTION -->

### Journal

Fifteen sentences, a stratified sample (seeded) of the build's diary lines:
Chinese and English; one symptom; a symptom with a reading (`血压123/80，心率79`,
`6818 steps today`); a symptom the classification does not code
(`小腿抽筋`, `leg cramps`). Each goes to a fresh account with its own date
and is scored against the entries the build says it states: a symptom is
found when an entry names it, and coded right when its ICPC-3 code is the
expected one (or, for a term the classification does not hold, when it is left
uncoded); a reading is found when its value is stored as a measurement. An
entry the sentence does not state is extra; a note (a meal, "slept badly") is
counted apart and is not extra.

### Memory

After both models are loaded on fresh processes, and every 20 s while the
questions run: the resident set (`ps -o rss`) and the physical footprint
(macOS `footprint`) of the `llama-server` children, and the machine's swap.
The footprint counts dirty memory whether resident, compressed or swapped,
and the Metal buffers RSS misses; RSS counts the mapped weights the footprint
leaves out. Neither is "what the size needs" on its own; both are reported.

## Scoring

### Automatic checks (`score.py`)

An answer **passes** when all five hold:

| Check | Holds when |
| --- | --- |
| answered | the turn ended `stop`, within 600 s, with answer text |
| tool | the case's expected tool was called (any of them, when several are listed; all of them for a mixed case) |
| facts | every expected fact is in the answer: a number within its tolerance, a date in any common form (`2026-08-22`, `2026年8月22日`, `Aug 22, 2026`), a duration (`6 h 47 min`, `6.8 hours`, `407 minutes`), a genotype in either allele order, one of the listed words, or a cite of a row or passage a tool showed in that turn |
| language | the answer is in the question's language: Chinese when CJK characters are at least a fifth of its letters, outside code blocks |
| chart | a parseable ```` ```vis-chart ```` block with data, when the question asks for a chart |

Counted beside it, not part of the pass:

- **numbers in no tool result**: each number in the answer, after dates,
  times, list markers, dbSNP ids, gene and test names and integers up to 12
  are set aside, that matches no number any tool returned in that turn, as
  written or rounded, nor the difference or percentage change of two of them,
  nor minutes as hours. A number from general knowledge ("adults need 7–9
  hours") counts; the grading below tells those apart from invented values.
- **chart values in no tool result**: the same, for the chart's data points.
- **cited answers**: `citations.check` run on the answer against the rows the
  tools printed in that turn (each `rid` with its row's values) and the
  passages the search returned. Counted: answers in the citation format,
  among those where a tool showed rows or passages; cites of an id no tool
  showed (made-up ids); and numbers outside a cited statement or not traced
  to its rows (untraced numbers).

The facts, numbers and language are read from the answer with its citation
markup removed, so a cite's tags and ids count neither as values nor as
English words.

### Claude Code grading

The automatic checks miss what reading catches: a right number for the wrong
month, a "normal" judged against a remembered range, an answer that is
correct and useless. So after each size's run, Claude Code reads every
transcript (`report.py show <size>`: the question, each tool call with its
arguments, each tool result, the answer) beside the case's expected answer
and scores it with this rubric, each criterion 0–2 with a one-sentence reason.
The grades are in `results/<size>/grades.json`, with the grader and the date.

| Criterion | 2 | 1 | 0 |
| --- | --- | --- | --- |
| **correct** | every expected fact, for the right person and period | some expected facts missing, or one wrong | the facts are wrong or missing, or for another period |
| **grounded** | every number and date is in a tool result or follows from them; a medical claim cited to a passage says what that passage says | one small slip (a rounding, one derived number off) | a value, date or chart point no tool returned, or a cite to a row or passage that does not hold the claim |
| **range** | judges high/low/normal only against the range the report printed, or says none was printed, or makes no such judgement | a judgement against a general range, said to be general | calls a value normal or abnormal against a remembered range, or contradicts the printed one |
| **language** | entirely in the question's language (technical terms aside) | mixed | in the other language |
| **useful** | answers the question asked; says plainly when there is no data; no diagnosis; no instruction to start, stop or change a medicine | answers, but buries or hedges it, or adds a wrong side claim | does not answer, asks for what it could have looked up, diagnoses, or tells the person to change a medicine |
| **chart** (only when asked) | a vis-chart of the asked series with the tool's values | a chart of a wrong or partial series | no chart, or a chart of invented points |

An answer scores at most 10 points, 12 with a chart. A timed-out or empty turn
scores 0 on every criterion.

## Product issues

Seen while running this, in the app rather than in a model. Nothing here was
changed by the evaluation.

- **The journal reads nothing from a sentence with either small model.**
  The two models fail in different ways, and both end at 0 of 31 entries.
  - Small answered 13 of the 15 sentences with a 6-token `{"entries": []}`,
    so each sentence was kept as one note. Before 9a1271f it found 1 of 31.
  - Tiny returned entries but marked them `someone_else` (6), `negated` (5)
    or `not_in_sentence` (3), so 12 of the 15 sentences wrote nothing.

  Large, run later on a 48 GB Apple-silicon machine, wrote 29 of 31.
- **`ContextOverflowError`.** Two of small's 24 questions (`p005-ferritin-change`,
  `p005-weight-chart`) ended in this error after a large tool result: each
  slot has 16k tokens (`--ctx-size 32768`, `--parallel 2`). The retry also
  came back empty.
- **Readings the record holds that the query does not return.** A reading
  stored without a unit (hemoglobin 140 from `p005_2025-08-28_e05a.pdf`) was
  not returned for an English keyword. Triglycerides stored as `甘油三酯（TG）`
  were not returned for "triglycerides".
- **Dates.** 9a1271f changed no stored date in small's extraction: before and
  after, the same four documents were filed off their encounter day.
  - `p006_2026-04-18_e13a.png` prints `20260418` and was filed under the
    upload day (`date_source: upload_time`).
  - `p004_2026-04-30_e11b.xlsx` (`2026年05月01日` reported, `2026年04月30日`
    received) went under the report date.
  - `p003_2024-10-07_e06b.jpg` went under its report date, not its collection
    date.
  - The weight log (no printed date) went under 2025-12-14.
- **`view: "latest"` with no indicator.** Both small models start this way.
  The tool answers `invalid_arguments`, and the 1B repeats the call until
  `retry_refused` (the run's name for what is now `repeated_call`).
- **A vis-chart with a syntax slip is not drawn** (`p002-steps-chart`, small):
  one extra closing brace, and the client shows nothing.
- **Upload state.** `GET /api/v1/data/uploaded-files` says `processed` before
  the indicator extraction has finished. `load.py` and `run.py` read the end
  from the app's log line instead.

- **Fixed-harness findings (2026-10-07).** The month-view average, the
  `.ext` title, the two duplicates, the print date and the 92-point cap are
  listed under "The fixed harness: small against five cloud models".
- **Found 2026-10-10**, on `qa6`:
  - The lab slip's fasting glucose is stored uncoded, as `FPG`, a series
    apart from the coded one; a keyword `latest` lookup finds only the clinic
    note's 4.76, which is stored with a wrong `high` flag and no range.
  - The same 52-row check-up book, read twice by MiniCPM5-2B at temperature
    0, stored 30 rows and then 49.
  - The genetic tool gives its rows no `rid`, so a cited genotype has nothing
    to cite: DeepSeek wrote `r1`.
  - The month view under-reports June and July systolic by about 2 mmHg
    against the mean of every reading (it averages one value per day), and
    every model that used it lost the same point.

<!-- PRODUCT -->

## Limits

- One run per size, at temperature 0 as configured. A rerun of a question can
  differ: llama.cpp's batching is not bit-reproducible.
- 24 questions and 12 documents. A difference of a question or two is not a
  finding.
- One grader, Claude Code, which also wrote the cases. The rubric and every
  grade with its reason are in `results/<size>/grades.json`, so a reader can
  re-grade.
- score.py was corrected during this run, and every saved result was
  re-scored with the final version (`run.py --rescore`).
  - A tool's `7317.0` (a day's steps) had been read as a date, and was
    counted as a number in no tool result.
  - A value stored with its unit and flag (`7.49mmol/L偏高`, `111/65mmHg`) had
    not been read as the printed number.
- The 48 GB machine's runs used llama.cpp b11269, the others 0.6.0 (b11429). Their
  timings and memory compare with each other, not with the 16 GB laptop's.
- The machine was shared with a Docker VM and other work. Earlier the same
  day, before the stack was rebuilt, macOS swap reached 24 GB and filled the
  disk. The runs measured here started with about 10 GB of swap in use.
- **2026-10-10: one run is not enough.** MiniCPM5-2B's two greedy runs
  graded 212 and 194, its three sampled runs 182 to 200. The settings were
  compared on two and three runs; the models other than the 2B ran once.
- The 2026-10-10 graders were six Claude Code agents, each holding five cases
  across every run, the later batches calibrated with the earlier grades. The
  rubric's `grounded` now covers medical claims, so its grades are stricter
  than the earlier rounds': the cloud models moved −9 to +2 against
  2026-10-07.
- The cloud references in that round ran beside the local runs, from a second
  app container on the same database and record. They share no model server.

<!-- LIMITS -->

## Files

| File | What it is |
| --- | --- |
| `cases.py` | builds `cases.jsonl` and `plan.json` from a mirobody-gen build |
| `cases.jsonl` | the 24 questions with their expected facts, tools and sources |
| `plan.json` | the Q&A accounts' contents, the extraction documents, the journal sentences |
| `load.py` | loads a person into an account; `--qa` the four shared accounts; `--verify` checks them |
| `run.py` | one size: switch, memory, questions, extraction, journal |
| `score.py` | the automatic checks; run alone it re-scores saved transcripts |
| `report.py` | `summary` (summary.md and summary.json), `show` (transcripts for grading), `diff` |
| `speed.py` | each answering model alone through the router: memory, prompt and generation speed |
| `refs/overlays.py`, `refs/*.llm.yaml` | the cloud references' eval-only model configurations, generated from config.llm.yaml |
| `refs/compose.ref.yaml` | mounts one of them in place of config.llm.yaml |
| `common.py` | the HTTP client, the corpus reader, the router and memory helpers |
| `results/run.json` | commits, versions, model files and hashes, machine, timeouts; a run elsewhere keeps its own under `environment` |
| `results/load.json` | what each account was loaded with, when, under which model |
| `results/<size>/` | `meta.json`, `qa.json` (transcripts and checks), `extraction.json`, `journal.json`, `grades.json`, `summary.md` |
| `results/summary.md`, `results/summary.json` | every size side by side, and the same as data (`report.py summary`) |
| `results/speed.json` | memory and speed of each model alone (`speed.py`) |
| `results/ref-<name>/` | a reference the stack was pointed at by env (`run.py --ref <name>`), the same files as a size plus `doctor.txt` |
