# Running on your own machine, with no API key

Mirobody talks to models over the OpenAI-compatible API, so a model server on
your own machine can take the place of every vendor key: your record, your
documents and your questions then never leave it. This page is the setup that
was measured, what it needs, and what it costs.

**The local model runtime is [llama.cpp](https://github.com/ggml-org/llama.cpp).**
Mirobody does not run models itself: `llama-server` serves them, and Mirobody
calls it. The shipped preset, the compose profiles, the first-run page's
search for a server, the vision check (`/props`) and every measurement on this
page are llama.cpp's. Any other OpenAI-compatible server can stand in for it
([Other servers](#other-servers)), but llama.cpp is the one shipped and tested.

Two models, two jobs. **GLM-OCR-0.9B** reads report photos and pages into text
and tables; the tables' rows are read by their column headers, with no model,
then coded by ② Translate, which is offline and deterministic. A second model
**answers questions** and writes titles, summaries and journal entries; it
comes in two sizes, below. Neither has to know a code.

All of them run on [llama.cpp](https://github.com/ggml-org/llama.cpp)'s
`llama-server`, which has official builds for Windows, Linux and macOS (CPU,
Vulkan, CUDA, ROCm, Metal). One server serves them all:
[`docker/local-models.ini`](../docker/local-models.ini) is a preset for its
router mode, and each model downloads from Hugging Face the first time it is
asked for.

## Choose a size

The setup page offers the same two, with these figures. Download is the
GGUF files the preset fetches, document reader included; memory is the most
`llama-server` held with both models loaded while answering.

| Size | Answers | Download | Memory | Per answer | A photo in the chat | On the evaluation |
| --- | --- | --- | --- | --- | --- | --- |
| **Small**, the default | MiniCPM5-2B, Q4_K_M | 3.0 GB | 5.7 GB | 28 s median, Apple M1 Pro 16 GB | read as its OCR text | 16 of 24 questions passed, 14 with every expected fact |
| **Large** | Qwen3.8-27B, IQ3_S (ISTA-DASLab GSQ-RCO) | 14.5 GB | about 20 GB | about 2 min, Apple M4 Pro 48 GB | looked at | 16 of 16 earlier questions with no number the record lacks (a different question set) |

Small runs on any computer with 16 GB of memory and no GPU, Windows, Linux or
macOS; the stack beside it takes about 1 GB more. Large wants a 32 GB Mac or a
24 GB NVIDIA GPU. Two other sizes were measured and are not offered:
MiniCPM5-1B answered 2 of the 24 questions with every expected fact for 0.4 GB
less download, and Qwen3.5-9B did not fit beside the stack on 16 GB.

The answering model is the only difference: every size reads documents with
GLM-OCR and reads tables by rule, so a lab report's readings come out the same.
What changes is how well questions are answered, how fast, and whether a photo
in the chat is looked at (large) or read as its OCR text (small). [`benchmarks/local_models/`](../benchmarks/local_models/README.md) has
the evaluation behind the figures, its cases and how to rerun it.

## Start the models

Pick the line for your machine. Every one serves the same preset; start it in
the `mirobody` folder (the one with `deploy.sh`). `--models-max 2` keeps one
answering model and the reader in memory, so choosing another size on the
setup page unloads the one before.

**Any computer with Docker, no GPU needed** (Windows, Linux or macOS). The
slowest, and the one that needs nothing besides Docker:

```bash
COMPOSE_PROFILES=local-cpu ./deploy.sh     # the stack, plus llama.cpp's CPU image beside it
```

`deploy.sh` writes `COMPOSE_PROFILES` into `.env`, so a later
`docker compose up -d` keeps the model service. On a running stack, add the
line to `.env` and run `docker compose up -d`.

**Windows**, to use the GPU (Intel, AMD or NVIDIA, through Vulkan; the CPU when
there is none). winget's `ggml.llamacpp` is llama.cpp's own Vulkan release
build, for x64 and arm64. In PowerShell, with the preset from the checkout in
WSL (below; `Ubuntu` is the distribution's name in `wsl -l`):

```powershell
winget install --id ggml.llamacpp
llama-server --models-preset \\wsl.localhost\Ubuntu\home\<you>\mirobody\docker\local-models.ini --port 8080 --models-max 2
```

The setup page looks for it through Docker Desktop's `host.docker.internal`,
which on a Mac reaches a server on the host's `127.0.0.1` (measured with colima).
This path has not been run on Windows by us; if the page finds no server there,
use the Docker line above, which needs no host networking.

**Linux, or Windows with Docker Desktop, and an NVIDIA GPU**, next to the app
(Linux needs the NVIDIA Container Toolkit):

```bash
COMPOSE_PROFILES=local ./deploy.sh         # the stack, plus the `llama` service on the CUDA image
```

**macOS** (Apple silicon). A container on a Mac cannot use its GPU, so
llama.cpp runs on the Mac itself:

```bash
brew install llama.cpp
llama-server --models-preset docker/local-models.ini --port 8080 --models-max 2
```

**Linux with another GPU** (AMD, Intel) or no Docker for the models: a
[llama.cpp release](https://github.com/ggml-org/llama.cpp/releases) build for
it, and the same `llama-server` line.

The first question after choosing a size waits for its download. Later starts
read the cache. Where huggingface.co is unreachable, point the download at a
mirror: `HF_ENDPOINT=https://<mirror>` in `.env` for the compose services, or
in the shell before `llama-server`. The models are the only thing fetched;
nothing about you is sent.

**Mirobody itself on Windows** runs in Docker Desktop (WSL 2 backend): open a
WSL terminal (Ubuntu), clone the repository there and run `./deploy.sh`, as on
Linux. The scripts keep LF line endings on every checkout.

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

## Other models

The answering model is `minicpm5-2b` unless you pick the large size
(`qwen3.8-27b`), and the document reader is `glm-ocr`: the sections of
`docker/local-models.ini`, which the `local` entries of `config.llm.yaml` ask
for. The setup page writes the size you pick; in `.env` the same is a line:

```bash
LOCAL_MODEL=qwen3.8-27b        # the large size: answers questions, writes titles and summaries
LOCAL_OCR_MODEL=glm-ocr        # reads report photos and pages
```

To run another model, add its section to the preset (or serve it any other
way), then pick it on the setup page, which lists what the server serves, or
name it in `.env`. A model chosen this way is checked the same way: the server
has to serve it. The candidates measured on the way to these two are in
[local-models-roadmap.md](local-models-roadmap.md). A model that cannot see is
detected from its server and sent a photo's text instead of the photo.

The same works for a vendor key: the setup page shows the model beside the key
and takes another name (`OPENROUTER_CHAT_MODEL=anthropic/claude-opus-5.5`, for
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

| Photo | GLM-OCR (text pass) | Qwen3.8-27B (the large size, sees) |
| --- | --- | --- |
| Lab report, nutrition table, FDA label | every row read correctly | reads it |
| Monitor display 128/91, pulse 76 | the three numbers, no labels | "128/91 mmHg, pulse 76" |
| A plate of food | nothing (there is no text) | a calorie range with its reasoning, and a wrong dish name |

The large size sees, so a photo in the chat reaches it as an image and a meal
can be estimated, roughly: expect a range, not a count. The default size does
not (MiniCPM5-2B; so does any model served without its `mmproj`): that is
detected from its server, the agent gets the photo's OCR text instead and is
told that is all it is, and for a meal it says it cannot see the photo and asks
what was eaten.

## Things that behave differently from a hosted model

- **Nothing streams while the prompt is read.** A turn that adds 6.6k new tokens
  waits over a minute for its first byte. The `local` entry allows 600 s of
  silence (`stream_chunk_timeout`); the library default of 120 s fails a long turn.
- **The first turn after loading is the slow one.** The server caches the
  prompt it has read, so later turns read only what changed.
- **A reply can be all reasoning.** The agent asks once more when a reply has
  no answer text and no tool call; if the second one is empty too, the chat
  says it has no answer rather than showing a blank message.
- **A long report is read a page at a time.** Text over 3,000 characters
  with page headers goes to the model one page per request, two at once, and
  the pages' readings are joined: in one request MiniCPM5-2B returned none of
  a 7-page check-up book's 78 rows, page by page all of them. A log's rows
  keep the dates they print.
- **A table is read by its header.** Rows under a header the rules know (项目名称 /
  结果 / 参考值 / 单位, Analyte / Result / Unit, a CSV's first line) are stored as
  printed and labelled `rules:table@v1`, when they look like readings and the
  page's other copy (the text layer, or the OCR's text pass) shows the same
  value. Patient details are skipped. A row left unread, text outside the
  tables that holds a number or a finding, and a document with no table go to
  the text model, and a rule's row outranks the model's for the same printed
  row.
- **Any OCR model's answer is read the same way.** An answer's OTSL tables
  (PaddleOCR-VL, MinerU) become HTML, its LaTeX (`\(\mu mol/L\)`) the
  characters it typesets, and a line it repeats until the token cap one copy;
  each pass is capped at 8,192 tokens.
