# Small local models: where 1.5.4 stands, and the plan

**English** · [中文](local-models-roadmap.zh-CN.md)

Mirobody can run with no API key: one model answers questions, another reads
documents, both on the same machine ([local-models.md](local-models.md) is how
to run them). This page records what was measured on 2026-09-30, what it means
for photos, and the plan for the whole thing to fit on an ordinary computer.

## Conclusion

- **Today's default** (1.5.4) is Qwen3.8-27B (GSQ-RCO IQ3_S, 13 GB) for the
  agent and GLM-OCR-0.9B (1.4 GB) for documents, both on llama.cpp. Its answers
  are as correct as a hosted model's on our questions, but it needs about 20 GB
  of memory: a Mac with 32 GB, or a GPU with 24 GB.
- **The goal** is two small models post-trained for Mirobody:
  **MiniCPM5-2B** as the agent and **GLM-OCR-0.9B** for documents, about 3 GB
  together, which runs in 8 to 16 GB of memory and without a GPU. Two models,
  not one merged model, and the harness changes first (below).
- **Photos**: GLM-OCR reads printed text and tables, nothing else. Understanding
  what a photo shows, such as the calories on a plate or a rash, needs a model
  that sees. The small pair will not, and Mirobody says so rather than guess.

## What was measured

Apple M4 Pro with 48 GB, llama.cpp b11269, the demo record. Eight questions,
each asked twice: a three-month trend chart, the latest value, change over
time, medications, a lab report, a genotype, a record shared by someone else,
and general knowledge. A run passes when the right tool is called with the right
view, a chart is drawn when asked for, the answer is in the question's
language, and it answers. Then every number in every answer was checked against
the database, and every "high" or "normal" against the range the report printed.

| Model | Size | Runs passed | Numbers not in the record | Judged against the printed range | Median / p90 per answer | Sees images |
| --- | --- | --- | --- | --- | --- | --- |
| Qwen3.8-27B GSQ-RCO IQ3_S (default) | 13.0 GB | 16/16 | 0 | once said a report printed no range (readings now carry it) | 134 / 216 s | yes |
| Qwen3.8-27B GSQ-RCO IQ2_S | 9.6 GB | 16/16 | 0 | yes | 109 / 224 s (machine under load) | with its mmproj (not tested) |
| Qwen3.8-27B Q4_K_M on Ollama | 17 GB | 16/16 | 0 | yes | 99 / 234 s | yes |
| Ternary-Bonsai-2-27B (PrismML's llama.cpp fork) | 6.6 GB | 16/16 | 0 | yes | 132 / 410 s | not tested |
| Qwen3.5-9B Q4_K_M | 5.7 GB | 16/16 | 0 | no: called the printed ranges "usual guidance" and missed a printed "high" | 40 / 76 s | yes |
| MiniCPM5-2B Q4_K_M | 1.6 GB | 14/16 (one question unanswered, both runs) | a whole invented chart, both runs (below) | not reached | 17 / 375 s | no |
| MiniCPM5-1B Q8_0 | 1.1 GB | 2/16 | gave a BMI of 29.4 as the weight in kg | not reached | 3 / 7 s | no |
| MiMo-V2.6-Distill-Qwen-9B Q4_K_M | 5.4 GB | 10/16 | in 4 of 10 answers with data | not reached | 23 / 736 s | not tested |
| Hosted (Claude Sonnet 5, GPT-5.6, Qwen3.8-flash) | | 16/16 each | | | 9–23 s | yes |

Timings compare only within one session on one machine. The evaluation harness
that produced these is not in the repository yet; publishing it under
`benchmarks/` is the first step of the plan.

### MiniCPM5-2B, in detail

It is fast (most answers in 2 to 70 s) and gets the structure right, and its
failures are narrow:

- Asked on 2026-09-30 for "the past three months", it passed no dates, received
  a year of real daily readings, and answered with October to December 2026:
  92 points and three monthly means that do not exist.
- One question (a shared record's cholesterol) had no answer in either run
  (594 s and 375 s).
- Asked for a reading as JSON, it put the value into the name.

MiniCPM5-1B, at Q8_0 so that quantization is not the excuse, called no tool in
12 of 16 runs and asked the user which "view" and time zone to use instead, so
the 2B model is the training base.

None of these is missing knowledge. They are dates, arithmetic and following
the tool's shape, which is what post-training fixes, and what the harness can
take away from the model altogether.

## Photos

GLM-OCR has four official prompts: `Text Recognition:`, `Table Recognition:`,
`Formula Recognition:`, and information extraction into a JSON template. Four
images (a monitor showing 128/91 and pulse 76, a Chinese nutrition table, the
FDA sample label, and a plate of food):

| Photo | GLM-OCR, text prompt | GLM-OCR, JSON template | Qwen3.8-27B, asked directly |
| --- | --- | --- | --- |
| Monitor 128/91, pulse 76 | 128, 91, 76 (the clock misread) | systolic 76, pulse empty: wrong | "128/91 mmHg, pulse 76" |
| Chinese nutrition table | every row; the table prompt too | energy "3" (the NRV% column): wrong | |
| FDA label | every line | fat 0 g for 8 g: wrong | |
| Plate of food, no text | nothing, which is right | invents "Noodle dish" | a calorie range with its reasoning, and the wrong dish |

So Mirobody sends GLM-OCR only its text and table prompts (the `local-ocr`
entry in `config.llm.yaml`), never a JSON template and never an open question,
and the model that reasons over the text decides that 128 and 91 are a blood
pressure. An estimate from a meal photo is a range at best, even from a model
that sees.

Since 1.5.4 the agent asks the model server whether its model can see
(llama.cpp's `/props`, Ollama's `/api/show`; `mirobody/utils/config/served.py`).
A model that cannot is sent a photo's OCR text instead of the image, told that
it is printed text only, and asked to say it cannot see the photo when the
question is about the picture.

## The plan

### First, the harness

These help every model, hosted ones included, and leave a small model less to
get wrong:

1. **Arithmetic in the tool.** Means, monthly means, differences and trends
   come back computed; the model reports them.
2. **Dates in the tool.** "The past three months" becomes a parameter the
   server resolves, so the model never computes a date.
3. **Charts cite the tool's rows.** The chart takes its points from the tool
   result instead of the model writing each one; the 92 invented points were
   written by hand.
4. **Every number checked before it is shown.** A number in an answer that no
   tool result or document contains is flagged, and the answer regenerated.
5. **A shorter prompt for small models**, measured the way the prompt in
   `benchmarks/local_agent/` was.

### Then, training

| Phase | Work | Passes when |
| --- | --- | --- |
| P0 | The harness changes above. The evaluation grows from 16 runs to 200–500 questions (languages, several people, date windows, photos), split into train, dev and test, with real reports held out for test, and is published under `benchmarks/` | MiniCPM5-2B's baseline is recorded |
| P1 | Agent: the default 27B model answers the training questions through the real harness; only runs that pass every check are kept; LoRA fine-tuning of MiniCPM5-2B on them | the test split at the 27B model's level: every run passes, no number outside the record, judged against printed ranges |
| P2 | Documents: GLM-OCR fine-tuned on rendered reports with phone-photo distortions (perspective, light, blur) to write rows of name, value, unit, range, flag and date | field accuracy and missed rows on the held-out real reports |
| P3 | Reinforcement learning (GRPO) on what still fails, with the number check as the reward | no number outside the record on the test split |

What is already in place:

- **Licences**: MiniCPM5-2B is Apache-2.0 and GLM-OCR's weights are MIT. Both
  publish fine-tuning routes (MiniCPM: TRL with PEFT, LLaMA-Factory, ms-swift,
  unsloth; GLM-OCR: a LLaMA-Factory guide).
- **A teacher**: the default 27B model passes every run and judges against the
  printed range.
- **A reward that can be computed**: whether each number in an answer is in the
  record is checked mechanically, which also filters the teacher's runs.
- **Data at no labelling cost**: the demo generator makes any number of people
  and series, and a report rendered from known values is labelled by
  construction.

Compute is modest: LoRA on a 2B model fits one 24–48 GB GPU, the 0.9B OCR model
less. The default changes only when a trained model passes the same checks as
the model it replaces.

### Out of scope, for now

- **Understanding photos.** MiniCPM5-2B is text-only and GLM-OCR reads text.
  With the small pair, a meal photo is answered with a request to describe the
  meal; photo understanding stays with a model that sees (the 27B default, or
  a hosted key). A small vision model is a separate project.
- **One merged model.** The two tasks need different data and different tests,
  and a regression in a merged model is hard to place. Mirobody already routes
  the agent and documents to separate entries, so two models drop in. A single
  small vision model doing both is worth measuring once both pass on their own.
- **Keeping it current.** A change to a tool's shape means another training
  run, so the evaluation runs nightly against the trained models.
