# Running on your own machine, with no API key

Mirobody talks to models over the OpenAI-compatible API, so a model server on
your own machine can take the place of every vendor key: your record, your
documents and your questions then never leave it. This page is the setup that
was measured, what it needs, and what it costs.

Two models, two jobs. **GLM-OCR-0.9B** reads report photos and pages into text
and tables; the tables' rows are read by their column headers, with no model,
then coded by ② Translate, which is offline and deterministic. **Qwen3.8-27B**
is the agent, and writes titles and summaries. Neither has to know a code.

Both run on [llama.cpp](https://github.com/ggml-org/llama.cpp)'s `llama-server`,
which has official builds for macOS, Linux (CPU, CUDA, ROCm, Vulkan) and
Windows. One server serves both: [`docker/local-models.ini`](../docker/local-models.ini)
is a preset for its router mode, and each model downloads from Hugging Face the
first time it is asked for.

## What it needs

| | Documents | Agent |
| --- | --- | --- |
| Model | GLM-OCR-0.9B Q8_0 (MIT) | Qwen3.8-27B, GSQ-RCO IQ3_S with its MTP head, by ISTA-DASLab (Apache-2.0) |
| Download | 1.4 GB | 13 GB |
| Memory while running | about 18–20 GB for both: a Mac with 32 GB, or a GPU with 24 GB | |

Measured on an Apple M4 Pro with 48 GB (2026-09-30, llama.cpp b11269): the
eight test questions answered 16 of 16 times with no number the record does
not hold. A question took from 20 seconds to four and a half minutes (a
three-month chart), two minutes typically. The four demo documents: 27 of 27
readings, all read by rule.

## Start the models

**macOS**

```bash
brew install llama.cpp
llama-server --models-preset docker/local-models.ini --port 8080
```

**Linux, or Windows with Docker Desktop, and an NVIDIA GPU**, next to the app:

```bash
docker compose --profile local up -d      # adds the `llama` service
```

**No GPU** (much slower; 32 GB of memory):

```bash
docker compose --profile local-cpu up -d  # the same service on the CPU image
```

**Windows with an AMD or Intel GPU, or no Docker for the models**: download
`llama-server` from the [llama.cpp releases](https://github.com/ggml-org/llama.cpp/releases)
(Vulkan, CUDA or CPU build) and run the same command as on macOS.

The first question after starting waits for the downloads. Later starts read
the cache.

## Point Mirobody at them

On the setup page (`./deploy.sh` prints its link; Settings → Model later),
choose **100% on this machine**. Mirobody looks for the server at
`host.docker.internal:8080`, then compose's `llama:8080`, then
`127.0.0.1:8080`, lists the models it serves, checks it serves the two you
choose (the preset's, unless you pick others), asks it to load them, and
shows their progress. The choice is stored encrypted in the database.

Or two lines in `.env`, then `docker compose up -d` (a restart does not reread `.env`):

```bash
LOCAL_BASE_URL=http://host.docker.internal:8080/v1        # app in Docker, models on the host
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1
# with `--profile local`:      http://llama:8080/v1 for both
# app and models on the host:  http://127.0.0.1:8080/v1 for both
```

## Other models, smaller ones included

The answering model is `qwen3.8-27b` and the document reader `glm-ocr` because
those are the names `config.llm.yaml` writes and the sections of
`docker/local-models.ini`. To run another model, add its section to the preset
(or serve it any other way), then either pick it on the setup page, which
lists what the server serves, or set the name in `.env`:

```bash
LOCAL_MODEL=minicpm5-2b        # answers questions, writes titles and summaries
LOCAL_OCR_MODEL=glm-ocr        # reads report photos and pages
```

A model chosen this way is checked the same way: the server has to serve it.
Smaller models answer faster on less memory and get more wrong; the measured
results for MiniCPM5-2B and others are in
[local-models-roadmap.md](local-models-roadmap.md). A model that cannot see is
detected from its server and sent a photo's text instead of the photo.

The same works for a vendor key: the setup page shows the model beside the key
and takes another name (`OPENROUTER_CHAT_MODEL=anthropic/claude-opus-5`, for
one), checked with one real request before it is kept. Every entry's variable
is its `model_env` in `config.llm.yaml`.

With a vendor key set as well, the key's models come first, so the setup page
refuses local while `.env` holds a key. To use the local ones anyway, set
`DEFAULT_MODEL=local`, `UTILS_VISION_MODEL=local-utils` and
`UTILS_TEXT_MODEL=local-utils`.

`compose.yaml` maps `host.docker.internal` for Docker Engine on Linux and keeps
it out of `HTTP_PROXY`, so a proxied deployment does not route model requests
through the proxy. On a Mac (Docker Desktop or colima) that name reaches the
host's loopback, so `llama-server` keeps its default `127.0.0.1`. On Linux it
points at the Docker bridge, so a server on the host has to listen there: give
it the bridge address (`--host 172.17.0.1`, from `ip -4 addr show docker0`),
not `0.0.0.0`, which also offers the server, with no key, to every machine on
your network. The `--profile local` service needs neither: the app reaches it
on compose's own network.

`failed to initialize router models: ... Is a directory` in the `llama` log
means Docker could not see the checkout, and mounted an empty directory where
the preset should be. Colima shares only your home directory by default; keep
the checkout under it, or add the path to colima's `mounts`.

## Check it

```bash
docker compose exec mirobody mirobody doctor --probe
```

`doctor` shows which entry each surface uses; `--probe` sends each one a real
request (a tool call, a schema-bound answer, a rendered image, the OCR passes)
through the code the product uses, and checks that each server runs the model
its entry names.

## Other servers

Any OpenAI-compatible server works: set the two addresses and the model names
it serves (`curl <address>/v1/models`), on the setup page or as `LOCAL_MODEL`
and `LOCAL_OCR_MODEL`.

**Ollama.** `ollama pull qwen3.8:27b` (17 GB) answered the same sixteen questions
correctly, and fastest. Two things to know:

- Ollama sets the context from the GPU's memory, and below 24 GB it is 4,096
  tokens, which cuts Mirobody's prompt short without an error. Set
  `OLLAMA_CONTEXT_LENGTH=65536` before `ollama serve`.
- Its `glm-ocr` does not stop after reading a page in 0.34.1 and later
  ([ollama/ollama#18609](https://github.com/ollama/ollama/issues/18609)). Keep
  the documents on llama.cpp until that is fixed.

**Ternary Bonsai 2 27B** is the same Qwen3.8-27B in 6.6 GB and answered as well,
but today only PrismML's [llama.cpp fork](https://github.com/PrismML-Eng/llama.cpp)
runs it. It becomes the default once upstream llama.cpp does.

## What each model can read in a photo

GLM-OCR reads printed text and tables. It cannot say what a photo shows: a
meal, a rash or a scene is beyond it. Its official prompts are the only ones
Mirobody sends (`Text Recognition:` and `Table Recognition:`, the `local-ocr`
entry's `ocr_prompts`). Its JSON information-extraction prompt was measured
and is not used: on a blood-pressure display, a Chinese nutrition table and
an FDA label it put a value in the wrong field every time (systolic 76 for a
128/91 reading, energy "3" from the NRV% column), and asked for the dish on a
meal photo with no text it invented one.

What that means for each kind of photo (Apple M4 Pro, 2026-09-30):

| Photo | GLM-OCR (text pass) | Qwen3.8-27B (the default agent, sees) |
| --- | --- | --- |
| Lab report, nutrition table, FDA label | every row read correctly | reads it |
| Monitor display 128/91, pulse 76 | the three numbers, no labels | "128/91 mmHg, pulse 76" |
| A plate of food | nothing (there is no text) | a calorie range with its reasoning, and a wrong dish name |

The default agent sees, so a photo in the chat reaches it as an image and a
meal can be estimated, roughly: expect a range, not a count. An agent that
cannot see (MiniCPM5-2B, or a quant served without its `mmproj`) is detected
from its server, gets the photo's OCR text instead, and is told that is all it
is; for a meal it says it cannot see the photo and asks what was eaten.

## Things that behave differently from a hosted model

- **Nothing streams while the prompt is read.** A turn that adds 6.6k new tokens
  waits over a minute for its first byte. The `local` entry allows 600 s of
  silence (`stream_chunk_timeout`); the library default of 120 s fails a long turn.
- **The first turn after loading is the slow one.** The server caches the
  prompt it has read, so later turns read only what changed.
- **A reply can be all reasoning.** The agent asks once more when a reply has
  no answer text and no tool call, and reports an error if the second one is
  empty too.
- **A table is read by its header.** Rows under a header the rules know (项目名称 /
  结果 / 参考值 / 单位, Analyte / Result / Unit, a CSV's first line) are stored as
  printed and labelled `rules:table@v1`, when they look like readings and the
  page's other copy (the text layer, or the OCR's text pass) shows the same
  value. Patient details are skipped. A row left unread, text outside the
  tables that holds a number or a finding, and a document with no table go to
  the text model, and a rule's row outranks the model's for the same printed
  row.
