# Choosing a model

**English** · [中文](model-choice.zh-CN.md)

Mirobody calls a model for three jobs: answering questions in the chat,
reading documents into readings, and turning a journal sentence into entries.
Which model does them decides two things, and this page is for choosing with
both in view:

- **who reads your health data**: your own computer and nobody else, or a
  model vendor under its terms;
- **how good the answers need to be**, and what speed and cost buy them.

Every figure here comes from the same evaluation, run through the product's
own API on one synthetic record; where two runs differ in more than the
model, the notes under the results say so ([How it was measured](#how-it-was-measured-and-how-to-rerun-it)).

## The short answer

| Your situation | Choose | What leaves your computer |
| --- | --- | --- |
| Nothing may leave; an ordinary computer (16 GB, no GPU) | **Local, small**: MiniCPM5-2B answers, GLM-OCR-0.9B reads documents, both on llama.cpp | nothing |
| Nothing may leave; a 32 GB Mac or a 24 GB GPU | **Local, large**: Qwen3.8-27B answers and sees photos, GLM-OCR-0.9B reads documents | nothing |
| The best answers | **Claude Sonnet 5.5**, **Claude Opus 5.5** or **Gemini 3.8 Flash**, through OpenRouter with zero data retention: tied on this evaluation, Gemini at the lowest price | your questions, the rows the agent reads, your documents' text |
| The lowest cost, or the fastest answers | **GPT-6 Luna**, the cheapest per answer, or **DeepSeek V4.1 Flash** (open weights), the fastest, the same way | the same |
| Document images stay home, a cloud model answers | **A mix**: GLM-OCR on llama.cpp reads every photo and page, a cloud model answers | the same, without the page images |

## Three modes, and what leaves the machine

The first-run page offers the first two; the third is a few lines of `.env`
([below](#a-mix-documents-read-here-answers-from-the-cloud)).

| | 100% on this machine | A model key | Local reader, cloud answerer |
| --- | --- | --- | --- |
| Photos and scanned pages of your documents | read here by GLM-OCR | sent to the vendor's vision model | read here by GLM-OCR |
| The text of your documents | read here | sent to the vendor, except the table rows the rules read here; the first 3,000 characters go for a title and summary either way | the same |
| Your questions, and the rows the agent reads to answer them | stay here | sent to the vendor | sent to the vendor |
| A photo opened in the chat | read here: as its OCR text (small) or looked at (large) | sent, when the chat model sees | sent, when the chat model sees |
| Names to codes, units to UCUM (② Translate) | here, offline, from the package | the same | the same |
| Model weights | downloaded once from Hugging Face | none | GLM-OCR, 1.4 GB, once |

"Sent to the vendor" means under that vendor's terms, and on OpenRouter under
the terms of the host it routes to ([Health data on OpenRouter](#health-data-on-openrouter)).
The README's [What stays on your machine](../README.md#what-stays-on-your-machine)
is the same table for the whole stack.

## Local: two sizes, served by llama.cpp

**[llama.cpp](https://github.com/ggml-org/llama.cpp)'s `llama-server` serves
the local models; Mirobody runs no model itself.** One server started with the
preset [`docker/local-models.ini`](../docker/local-models.ini) serves the
document reader and either answering model, and downloads each from Hugging
Face the first time it is asked for. [local-models.md](local-models.md) has the
start command for Windows, Linux and macOS.

| | Small, the default | Large |
| --- | --- | --- |
| Answers | MiniCPM5-2B, Q4_K_M, text only | Qwen3.8-27B, IQ3_S (ISTA-DASLab GSQ-RCO), with its vision projector |
| Reads documents | GLM-OCR-0.9B | GLM-OCR-0.9B |
| Download, reader included | 3.0 GB | 14.5 GB |
| Memory, both models loaded | 5.7 GB at most while answering | about 20 GB |
| Runs on | any computer with 16 GB of memory, no GPU: Windows, Linux or macOS | a 32 GB Mac, or a 24 GB NVIDIA GPU |
| Median answer | 29 s, Apple M1 Pro 16 GB, on its GPU | about 2 min (134 s), Apple M4 Pro 48 GB, on its GPU |
| A photo in the chat | read as its OCR text: it cannot see | looked at |
| On the evaluation | 19 of 24 questions passed (grade 215 of 248), 140 of 140 printed rows, 22 of 31 journal entries | 16 of 16 runs of an earlier 8-question set, no number the record lacks; not run on the 24 questions below |

The answer times are Apple silicon's, where llama.cpp runs on the GPU. With
no GPU it is minutes, not seconds, and how many depends on the processor. In
llama.cpp's CPU image on 4 vCPUs (2026-10-07), MiniCPM5-2B wrote about 18
tokens a second on an Apple M1 Pro (colima's arm64 VM), a first answer in 2–3
minutes, and 3 to 7 on an Intel Xeon Gold 5220R (an external review), a first
answer in up to about 15 minutes with the download, a report photo in up to
about 9 and a PDF page in up to about 12. Later turns reuse the server's
prompt cache; the two models hold about 6.0 GiB, so Docker needs at least 8 GB
of memory ([local-models.md](local-models.md#without-a-gpu)).

On the evaluation below, on the final code, the small size passes 19 of 24
questions (Claude Code grade 215 of 248), stores all 140 printed rows of the
12 documents, every one with its unit and range as printed, and writes 22 of
31 journal entries, with no answer timed out. It got there in two rounds of
harness changes, each in the [CHANGELOG](../CHANGELOG.md):

| The small size | Grade | Passed | Printed rows | Journal |
| --- | ---: | ---: | ---: | ---: |
| before 1.5.4's changes | 190 of 248 | 16 of 24 | 45 of 140 | 0 of 31 |
| after the first round | 209 of 248 | 19 of 24 | 140 of 140 | 24 of 31 |
| on the final code | 215 of 248 | 19 of 24 | 140 of 140 | 22 of 31 |

The first round: worked examples in the journal's request, long reports read
page by page, notes and logs with their rows' dates, table rules that read the
headers most reports print, keyword recall across spellings and units, closed
JSON schemas, a bound on a looping answer, the context a page's leftover text
needs, and `view="stats"` naming a reading's own day. The second: a reading
printed on two pages stored once, a printed flag split from its unit, a page's
print date no longer dating its readings, a born-digital PDF's tables read off
its text layer, a cut answer saying so before its rows, and a local reply
stopped at 6,144 tokens. Where it still loses points: means it gets wrong
(July's sleep, August's weight, May's diastolic pressure), a weight chart that
came back empty after 15 tool calls, a check-up summary it stopped reading
after the first 200 lines, a "normal range" claim where the report printed
none, and a genotype misnamed.

### The document reader: GLM-OCR-0.9B

Both sizes read documents the same way: GLM-OCR turns a photo or a scanned
page into text and tables, the tables' rows are read by their headers with no
model, and the answering model reads what the rules leave. Three small OCR
models that upstream llama.cpp serves were run through that whole path on
synthetic reports ([`benchmarks/local_ocr/`](../benchmarks/local_ocr/README.md),
Mirobody fcfbf78, MiniCPM5-2B reading the leftover):

| Rows stored with the printed value | GLM-OCR 0.9B | PaddleOCR-VL-1.6 0.9B | MinerU2.5-Pro 1.2B |
| --- | ---: | ---: | ---: |
| Printed: 303 rows on 26 pages, the generator's banner in place | **283** (93.4%) | 278 (91.7%) | 279 (92.1%) |
| … the banner removed, as on a real report | **302** (99.7%) | 282 (93.1%) | 299 (98.7%) |
| Rows the page does not print (banner in place / removed) | **0 / 0** | 12 / 8 | 3 / 4 |
| Handwritten: 301 rows on 28 pages (banner in place / removed) | **99 / 219** | 62 / 136 | 55 / 171 |
| OCR seconds per printed page | 12.3 | 13.1 | 14.8 |
| Download, model and projector | 1.43 GB | 1.82 GB | 1.24 GB |
| Licence | MIT | Apache-2.0 | Apache-2.0 |

GLM-OCR stays the reader: the most rows right with the banner and without it,
none the page does not print, the fastest, and no change to the product.
PaddleOCR-VL-1.6 puts the most into its OCR text (99% of the printed rows,
against 85% for the other two), but it writes LaTeX and table markup the
product has to clean, looped to the token cap on 7 of the 28 handwritten
pages, and stored rows the page does not print. It is in the preset as an
option ([how to switch](local-models.md#the-document-reader)). On handwriting
all three read about 95% of the values; the rows are lost afterwards, where
the small answering model reads what the rules leave.

### Photos

GLM-OCR reads printed text and tables, nothing else. The small size cannot
see: a photo in the chat reaches it as its OCR text, and asked about a meal it
says it cannot see the photo and asks what was eaten. The large size looks at
the photo, and estimates a meal as a calorie range with its reasoning, which
can name the wrong dish ([what each model can read in a photo](local-models.md#what-each-model-can-read-in-a-photo)).

## Cloud: one open baseline, four closed references

Five cloud models ran through the same stack, on the same questions,
documents and journal sentences, with the same scoring as the small size:

| Model | Weights | Through | Host |
| --- | --- | --- | --- |
| DeepSeek V4.1 Flash | open | OpenRouter, zero data retention, no fallback | Together |
| Claude Sonnet 5.5 | closed | the same | Google Vertex |
| Claude Opus 5.5 | closed | the same | Google Vertex |
| Gemini 3.8 Flash | closed | the same | Google Vertex |
| GPT-6 Luna | closed | the same | Azure |

Each ran so that only the answering model differs from the small size:
documents were read on the machine by GLM-OCR and the table rules, and the
cloud model read the OCR text the rules left (stored once, so every model read
the same text); the chat model was configured not to see
(`supports_image: false`), so a photo reached it as its OCR text, as it
reaches the small size; extraction and the journal ran in a fresh account per
model. That is the [mix](#a-mix-documents-read-here-answers-from-the-cloud),
with every image kept home.

### Results

| | Runs on | Questions: grade | Questions: passed | Median answer | Printed rows stored, 12 documents | Journal entries, 15 sentences |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| MiniCPM5-2B, small | Apple M1 Pro 16 GB | 215 of 248 | 19 of 24 | 29 s | 140 of 140 | 22 of 31 |
| Qwen3.8-27B, large | Apple M4 Pro 48 GB | not run on these | 16 of 16, an earlier set | 134 s | 27 of 27, the four demo documents | not measured |
| DeepSeek V4.1 Flash | Together | 245 of 248 | 23 of 24 | 4.2 s | 138 of 140 | 29 of 31 |
| Claude Sonnet 5.5 | Google Vertex | 247 of 248 | 23 of 24 | 9.0 s | 140 of 140 | 30 of 31 |
| Claude Opus 5.5 | Google Vertex | 247 of 248 | 23 of 24 | 14.9 s | 139 of 140 | 29 of 31 |
| Gemini 3.8 Flash | Google Vertex | 247 of 248 | 23 of 24 | 14.9 s | 140 of 140 | 31 of 31 |
| GPT-6 Luna | Azure | 237 of 248 | 21 of 24 | 9.7 s | 140 of 140 | 31 of 31 |

- **Grade** is Claude Code's points against a written rubric: correct,
  grounded, judged against the printed range, in the question's language,
  useful, and the chart when one is asked for, 0 to 2 each.
  **Passed** is all five automatic checks: answered, the right tool, every
  expected fact, the question's language, and a chart that parses.
- **Printed rows** counts stored readings that match a row the document
  prints. Beside them every model stored readings that match none: the cloud
  models 47 to 49, the small size 9. They have not been read one by one; in
  an earlier DeepSeek run 40 of them came from the 7-page check-up book. With
  the table rules in front ([below](#with-the-table-rules-in-front)) the
  cloud models' fell to 26–41.
- The large size's row is the earlier measurement that `config.llm.yaml` and
  [local-models.md](local-models.md) carry: 8 questions asked twice, and the
  four demo documents. It does not fit on the 16 GB machine the rest ran on.
- **Records and commits.** The small size ran entirely at 958fae5, on a record
  (`qa4`) loaded through the final pipeline, as a user's would be today. The
  cloud models answered from the record loaded at 490a0e1 (`qa3`, two
  documents read again at 321aa2c), which was not reloaded. DeepSeek, Sonnet
  and Luna read the documents and the journal at 4e3c06f, answered 20 of the
  questions at 490a0e1 and the 4 that read the re-read documents at 321aa2c;
  Opus and Gemini ran every part on 321aa2c's application code.

What the table says:

- **Three models tie at the top**: Claude Sonnet 5.5, Claude Opus 5.5 and
  Gemini 3.8 Flash, 247 of 248 each, and each lost the same point, to the
  month view's average (a finding in the pipeline, not in the models). One
  set of 24 questions cannot rank them; price and speed can. Gemini costs
  $0.012 an answer, Sonnet about $0.029 and Opus $0.045, and Sonnet answers
  fastest of the three (9.0 s median, against 14.9 s).
- **DeepSeek V4.1 Flash**, the open-weights baseline, answers fastest of all
  (4.2 s median), 2 points behind the top, and missed 2 of the 140 printed
  rows.
- **GPT-6 Luna** costs the least an answer ($0.0009) and stored every printed
  row and journal entry. Its 11 lost points include a search limited to one
  year, a lipid question handed back to the person instead of queried, and a
  printed range it said was missing.
- **MiniCPM5-2B**, on a 16 GB laptop with nothing sent anywhere, stored every
  printed row too, every one with its unit and range as printed, and is 32
  points behind the top three on the questions.

### With the table rules in front

Since c396b4f the table rules read every upload, whatever model is
configured, and a cloud model reads only what they leave. Four of the
references read the 12 documents again on that code, from the same stored
OCR text, each in a fresh account through its pinned host:

| Four cloud models, 12 documents | Before | With the rules in front |
| --- | --- | --- |
| Printed rows found, of 140 | 138–140 | 138–140 |
| Ranges as printed | 133–136 | 138–139 |
| Readings on no printed row | about 48 (Gemini 47) | 26–28 (Gemini 41) |
| Document text sent to the vendor's indicator call | 27,184 characters | 16,910 (−37%) |

Five of the 12 documents (two lab-slip PDFs, the spreadsheet, the photo and
the photocopy) now need no model at all. What this does and does not measure:

- **It is not "rules off" against "rules on".** The references' overlays
  kept a local OCR route configured, so the rules ran in the earlier runs
  too; they knew fewer headers then. The comparison measures the wider header
  vocabulary. Gemini's earlier run was already on it, which is why its two
  runs differ by almost nothing.
- **Reading every upload by rule matters most where this did not look**: a
  deployment with a vendor key and no local OCR model, where no table was
  read by rule before c396b4f.
- **The title and summary call still sends each document's first 3,000
  characters** to the vendor, rules or not (12,200 characters for the 12).

### What it costs

| | Per answer | 100 documents like these twelve |
| --- | ---: | ---: |
| Local, either size | nothing per call | nothing per call |
| GPT-6 Luna | $0.0009 | about $0.31 |
| DeepSeek V4.1 Flash | $0.0017 | about $0.23 |
| Gemini 3.8 Flash | $0.012 | about $2.1 |
| Claude Sonnet 5.5 | about $0.029 | about $6.6 |
| Claude Opus 5.5 | $0.045 | about $8.5 |

Per answer is OpenRouter's charge for the 24 questions, read once it had
settled (OpenRouter books a request's cost up to minutes after the answer),
divided by 24; Sonnet's is an estimate, because its question runs overlapped
another model's on the same key. Per hundred documents is the charge for
reading the twelve with the table rules in front, times 100/12 (Opus's is its
own run's extraction, on code that sent the same document text): documents
like these (one-page lab slips, a CSV, a spreadsheet, scans and photos, a
7-page check-up book), whose OCR ran on the machine. With a vendor reading the
page images too, add its vision calls. The key was shared with other work, so
every figure is an upper bound. Every cloud run of this evaluation together
cost $7.55.

## Health data on OpenRouter

OpenRouter is one key for every model. Unless told otherwise it sends a
request to any of the hosts that serve the model, and falls back to another
when one fails. With health data that matters twice: each host has its own
terms for what it keeps, and the hosts of one model do not serve the same
thing. Measured on 2026-10-06, and not yet recorded under `benchmarks/`:

- **Precision.** OpenRouter's endpoint list for `deepseek/deepseek-v4.1-flash`
  (`/api/v1/models/deepseek/deepseek-v4.1-flash/endpoints`) named 32 hosts.
  Many serve the model quantized, labelled `fp4` (OpenInference, Sail
  Research, Decart) or `fp8` (Morph, DeepInfra, AtlasCloud, Novita and
  others); several list no `structured_outputs`, and DeepSeek's own lists
  `response_format` but not `structured_outputs`.
- **A host that empties a schema-bound answer.** The extraction request for a
  home weight log's OCR text (12 dated rows), under `response_format:
  json_schema`, was routed to InferenceNet (the response's `provider` field)
  and came back in 333 characters with `"indicators": []`, placed before
  `content_type`. The same request with no `response_format`, on the same
  host, returned all 12 rows with their dates. In a controlled check, 5
  trials of each of 4 schema variants, DeepSeek read all 5 rows of the demo
  lipid CSV in 3 of 5 trials under every variant, the unchanged schema
  included, and every empty answer came from InferenceNet: the routing, not
  the schema.
- **What it cost an evaluation.** The first DeepSeek V4.1 Flash run, unpinned
  and on the shipped schema-bound entry, stored 86 of the 140 printed rows,
  with no readings at all from six documents. Most likely the host; the
  generator's banner made it worse there (removing that line took one lab
  slip from 0 rows to 6 of 6). Pinned to Together, at a later commit, the same
  model stored 138 of 140 with the banner in place.

So the evaluation, and this guide, do two things:

1. **Zero data retention for the account.** In your OpenRouter account's
   settings, allow only hosts that keep nothing. OpenRouter then refuses a host that
   does not offer it rather than route there: on 2026-10-06 it refused
   DeepSeek's own API under this setting ("ZDR violation (account settings),
   Paid model training violation"),
   which is why DeepSeek V4.1 Flash ran on Together. A `DEEPSEEK_API_KEY` is
   that same API.
2. **One host per model, and no fallback.** `provider.order` names the host
   and `allow_fallbacks: false` stops OpenRouter from trying another. Each
   `require_parameters: true` was not
   used: OpenRouter lists no temperature for GPT-6 Luna and answers it with
   404. Before each run, the pinned host answered one request shaped
   like the entry's own, temperature and JSON format included (`host_probe`
   in each reference's `meta.json`). For Claude Opus 5.5 and Gemini 3.8 Flash
   only Google's host passed: `anthropic`, `azure` and `amazon-bedrock` gave
   Opus no endpoint for a JSON-schema request with a temperature, and zero
   data retention removes Gemini's `google-ai-studio`.

The host goes in a `config.llm.yaml` entry's `extra_body`, which Mirobody
sends as it is. The entries the evaluation used for DeepSeek V4.1 Flash on
Together were these (the other four are the same with their model and host,
and none of the DeepSeek-only lines):

```yaml
MODELS:                           # beside the entries already there
  deepseek-zdr:                   # answers in the chat
    llm_type: openai
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: deepseek/deepseek-v4.1-flash
    supports_image: false         # a photo reaches it as its OCR text; no image leaves
    extra_body:
      provider: {order: [together], allow_fallbacks: false}
  deepseek-zdr-utils:             # reads documents' text, the journal, titles
    llm_type: openai
    api_key: OPENROUTER_API_KEY
    base_url: https://openrouter.ai/api/v1
    model: deepseek/deepseek-v4.1-flash
    supports_image: false
    chat: false
    response_format: json_object  # DeepSeek takes JSON mode, not a schema
    extra_body:
      reasoning: {enabled: false}
      provider: {order: [together], allow_fallbacks: false}
```

Then route to them in `.env`: `DEFAULT_MODEL=deepseek-zdr` and
`UTILS_TEXT_MODEL=deepseek-zdr-utils`. GPT-6 Luna's utility entry carries
`reasoning_effort: none` instead of the two DeepSeek lines (as the shipped
`openai-utils` does), and Gemini 3.8 Flash's `extra_body` carries
`reasoning: {effort: low, exclude: true}` (as the shipped `openrouter-utils`
does: its reasoning cannot be turned off); Sonnet 5.5's and Opus 5.5's add
nothing beyond the host. The files
the evaluation mounted are in
[`benchmarks/local_models/refs/`](../benchmarks/local_models/refs/), written
by `overlays.py` from the shipped `config.llm.yaml`.

A source install reads the checkout's `config.llm.yaml`. The Docker image
carries its own copy, so mount yours over it in a `compose.override.yaml`
next to `compose.yaml` (Compose reads both; the file is gitignored, and if
you already have one, add these lines to it), then `docker compose up -d`:

```yaml
services:
  mirobody:
    volumes:
      - ./config.llm.yaml:/app/config.llm.yaml:ro
  mirobody_worker:
    volumes:
      - ./config.llm.yaml:/app/config.llm.yaml:ro
```

A key pasted on the setup page, or a model named in `OPENROUTER_CHAT_MODEL`,
pins no host: OpenRouter routes it under your account's settings.

## By situation

### Privacy first, on an ordinary computer

The small size: `./deploy.sh`, then **100% on this machine** on the page it
links. 16 GB of memory, no GPU, 3.0 GB of models to download and about 0.7 GB
of images. Expect about 29 s an answer on an M1 Pro's GPU; on a CPU alone a
first answer takes minutes, from 2–3 on an M1 Pro's cores to about 15 on a
4-vCPU x86 server. Expect every printed row of a lab report stored, and most
questions answered right (19 of 24); it is weakest where it works out an
average or a chart's window itself. Nothing about you leaves the machine.

### Privacy first, on a big machine

The large size, on a 32 GB Mac or a 24 GB NVIDIA GPU: pick it on the same
page. About 2 minutes an answer on an M4 Pro, and the only local size that
looks at a photo.

### The best answers

Claude Sonnet 5.5, Claude Opus 5.5 or Gemini 3.8 Flash, through OpenRouter
with zero data retention and the host pinned ([above](#health-data-on-openrouter)).
The three tied at 247 of 248, and on one set of 24 questions a tie is not a
ranking; price and speed separate them. Gemini 3.8 Flash costs $0.012 an
answer and about $2.1 per hundred documents, and is what an OpenRouter key's
utility surfaces use by default (`openrouter-utils`). Sonnet costs about
$0.029 and $6.6, and answers fastest of the three. Opus costs $0.045 and $8.5.

### The lowest cost

GPT-6 Luna on Azure or DeepSeek V4.1 Flash on Together, the same way: $0.0009
and $0.0017 an answer, about $0.31 and $0.23 per hundred documents. Luna
stored every printed row; DeepSeek answers fastest and its weights are open.

### A mix: documents read here, answers from the cloud

Keep GLM-OCR on your machine and let a cloud model answer. Every report
photo and scanned page is read here; what reaches the vendor is text: your
questions, the rows the agent reads, the document text the table rules did
not read, and each document's first 3,000 characters for its title and
summary. This is how the cloud references above were measured. In
`.env`, with `llama-server` running the preset (only `glm-ocr` is asked for):

```bash
OPENROUTER_API_KEY=sk-or-...
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1   # UTILS_OCR_MODEL reads every photo and page here
UTILS_VISION_MODEL=local-utils                           # an image with no text is not sent for a description
```

`UTILS_OCR_MODEL` takes report photos and pages from the vision model
whenever `LOCAL_OCR_BASE_URL` is set, whatever key is present. Without the
third line, a file whose text the reader cannot get (a meal photo uploaded
as a file, for one) still goes to the vendor's vision model to be described;
with it, such a file is described on your machine or not at all. A photo opened in the chat reaches a chat model that sees; the
pinned entries above say `supports_image: false`, so it reaches them as its
OCR text. The setup page does not offer this mode: it refuses local while
`.env` holds a key.

## How to switch

- **The setup page.** `./deploy.sh` prints its link; later it is Settings ›
  Model. Paste a key (the model it will use is shown beside it, and takes
  another name, checked with one real request), or choose **100% on this
  machine** and a size. The choice is stored encrypted and applies without a
  restart.
- **`.env`.** The key, and the model by the variable each `config.llm.yaml`
  entry names as its `model_env`: `OPENROUTER_CHAT_MODEL` (the chat) and
  `OPENROUTER_UTILS_MODEL` (documents, journal, titles) for an OpenRouter key;
  `LOCAL_MODEL` (`minicpm5-2b` or `qwen3.8-27b`) and `LOCAL_OCR_MODEL`
  (`glm-ocr`) for the local server. `DEFAULT_MODEL`, `UTILS_TEXT_MODEL`,
  `UTILS_VISION_MODEL` and `UTILS_OCR_MODEL` name the entry each surface uses.
  Then `docker compose up -d`: a `restart` does not read `.env` again.
- **GPT-6 Luna without editing anything.** With an OpenRouter key the chat's
  model menu also offers GPT-6 Luna (the `gpt` entry); `DEFAULT_MODEL=gpt`
  makes it the default. Like every shipped entry, it pins no host.
- **Check it:**

  ```bash
  docker compose exec mirobody mirobody doctor --probe
  ```

  It shows which entry each surface uses, sends each one real request (a tool
  call, a schema-bound answer, an image, the OCR passes) through the code the
  product uses, and checks that each local server runs the model its entry
  names.

## How it was measured, and how to rerun it

- **The record.** [mirobody-gen](https://github.com/thetahealth/mirobody-gen),
  Mirobody's synthetic record generator, which is being released as open
  source, at 248df0f, `--seed 7 --people 6`: deterministic, so the same commit
  and seed build the same bytes. Every expected answer is computed from its
  ground truth, never typed by hand. No real person's data is in it.
- **The cases.** 24 questions (12 Chinese, 12 English) about four people,
  each asked once in a fresh chat session through the product's HTTP API; 12
  documents (text-layer PDF, CSV, XLSX, scan, phone photo, photocopy,
  screenshot, a 7-page check-up book, a clinic note, a home log) scored row by
  row against what they print; 15 diary sentences scored against the entries
  each one states.
- **The grading.** Automatic checks (`score.py`), and Claude Code reading
  every transcript beside the expected answer against the rubric above; both
  are kept with every grade's reason (`grades.json`), so either can be checked
  against the other.
- **The OCR.** 28 printed pages (303 rows, plus 57 rows of two home logs) from
  the same build, and 28 handwritten pages (301 rows) from mirobody-gen's
  handwriting build (b4a8c50, `--seed 42 --people 18`), each page through the
  product's own extraction path.

To rerun ([`benchmarks/local_models/`](../benchmarks/local_models/README.md)
and [`benchmarks/local_ocr/`](../benchmarks/local_ocr/README.md) have every
step and option):

```bash
B=benchmarks/local_models; G=~/mirobody-gen-seed7
mirobody-gen build --seed 7 --people 6 --out $G --render     # in a mirobody-gen checkout
python $B/cases.py --corpus $G
python $B/load.py --corpus $G --qa
python $B/run.py --size small --corpus $G
python $B/run.py --ref deepseek-v4.1-flash-v2 --corpus $G     # a cloud reference; its README has the setup
python $B/report.py summary

python benchmarks/local_ocr/run.py --model glm-ocr-q8 --corpus <corpus> --models-dir <models>
python benchmarks/local_ocr/extract.py --model glm-ocr-q8 --corpus <corpus>
```

What the numbers cannot tell you:

- **Synthetic, one seed, one run.** One record, 24 questions and 12
  documents, each model run once. A question or two apart is not a finding,
  and llama.cpp's batching is not bit-reproducible.
- **The banner.** Every generated page prints "SYNTHETIC SAMPLE — GENERATED
  DATA, NOT A REAL PATIENT RECORD", and a small model reads it literally;
  real reports do not carry it, which is why the OCR table gives both.
- **Handwriting from fonts.** The handwritten pages are rendered from
  handwriting typefaces, then scanned or photographed: not written by people.
- **One grader**, Claude Code, which also wrote the cases. The rubric and
  every grade's reason are published, so a reader can re-grade.
- **Speed on two machines.** The local timings are an M1 Pro's and an M4
  Pro's, on their GPUs; the CPU-only figures above are a separate measurement
  on 4 vCPUs. The cloud ones depend on the host's load that night.
- **GPT-6.1 Sol was dropped**: even pinned to Azure, OpenRouter kept it
  rate-limited upstream (9 of 24 questions needed up to four retry rounds and
  one never got through; 50 of 140 document rows were never stored), at about
  20 times GPT-6 Luna's price ($2/$10 per million tokens against $0.10/$0.50).
- **The cloud models were measured blind**, reading the local OCR text. With
  their own vision on the page images they may read more, at more cost, and
  with the images sent.

## Coming in 1.6.0: a Mirobody model

Mirobody 1.6.0 will ship its own model: small and fast enough for an ordinary
computer, post-trained for Mirobody's own tools and documents, and served by
llama.cpp like the models above. With it, a 16 GB computer with no GPU runs
the whole loop, reading documents, answering and the journal, privately, on a
model made for this harness rather than adapted to it.

It is the plan in [local-models-roadmap.md](local-models-roadmap.md#then-training),
built on the two models the small size runs today:

- **The answering model**, post-trained from MiniCPM5-2B (Apache-2.0) on runs
  through the real harness, kept only when every check passes, then trained
  further with whether each number is in the record as the reward;
- **the document reader**, post-trained from GLM-OCR-0.9B (MIT) on rendered
  reports with phone-photo distortions, to write each row's name, value,
  unit, range, flag and date.

About 3 GB together, like the small size today. It replaces the default only
when it passes the same evaluation as the model it replaces, the one on this
page and in [`benchmarks/`](../benchmarks/README.md), and the results are
published with it.
