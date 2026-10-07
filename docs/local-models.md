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

Whether to run locally at all, and how the small size compares with the
cloud models on the same evaluation, is [model-choice.md](model-choice.md).

## Choose a size

The setup page offers the same two, with these figures. Download is the
GGUF files the preset fetches, document reader included; memory is the most
`llama-server` held with both models loaded while answering.

| Size | Answers | Download | Memory | Per answer | A photo in the chat | On the evaluation |
| --- | --- | --- | --- | --- | --- | --- |
| **Small**, the default | MiniCPM5-2B, Q4_K_M | 3.0 GB | 5.7 GB | 29 s median, Apple M1 Pro 16 GB, on its GPU | read as its OCR text | 19 of 24 questions passed, 140 of 140 printed rows, 22 of 31 journal entries |
| **Large** | Qwen3.8-27B, IQ3_S (ISTA-DASLab GSQ-RCO) | 14.5 GB | about 20 GB | about 2 min (134 s median on the earlier set), Apple M4 Pro 48 GB, on its GPU | looked at | from an earlier set of 8 questions, each asked twice: 16 of 16 runs passed, no number the record lacks. Not yet run on the 24-question evaluation |

Small runs on any computer with 16 GB of memory and no GPU, Windows, Linux or
macOS; the stack beside it takes about 1 GB more. Large wants a 32 GB Mac or a
24 GB NVIDIA GPU. The two rows come from different question sets, so they do
not say which size answers better: the large one has not been run on the 24
questions yet. Two other answering models were measured and dropped:
MiniCPM5-1B answered 2 of the 24 questions with every expected fact for 0.4 GB
less download, and Qwen3.5-9B did not fit beside the stack on 16 GB.

The answering model is the only difference: both sizes read documents with
GLM-OCR and read tables by rule, so a lab report's readings come out the same.
What changes is how well questions are answered, how fast, and whether a photo
in the chat is looked at (large) or read as its OCR text (small).
[`benchmarks/local_models/`](../benchmarks/local_models/README.md) has the
evaluation behind the figures, its cases and how to rerun it.

### Without a GPU

The times above are Apple silicon's, where llama.cpp runs on the GPU. On the
CPU alone an answer takes minutes, and how many depends on the processor.
Both measurements below ran the small size in llama.cpp's CPU image (the
`local-cpu` profile below) with 4 vCPUs, on 2026-10-07:

| | Apple M1 Pro, in colima's arm64 VM | Intel Xeon Gold 5220R, Linux x86 (an external review) |
| --- | --- | --- |
| MiniCPM5-2B reads a prompt | about 50 tokens a second | not measured |
| MiniCPM5-2B writes | about 18 tokens a second | 3 to 7 tokens a second |
| A first answer | 2–3 minutes (a 6.8k-token prompt answered in 137 s) | up to about 15 minutes, the models' download included |
| A report photo | GLM-OCR read a page in about 17 s, its two passes together | up to about 9 minutes until the upload was processed |
| A PDF page | not measured | up to about 12 minutes until the upload was processed |

- Later turns reuse the server's prompt cache and read only what is new; a
  new conversation reads its whole prompt again (6.8k and 8.4k tokens in the
  two measurements). The preset
  gives each model two slots (`np = 2`), so a title or a summary written in
  the background takes cores from the answer.
- Both models loaded hold about 6.0 GiB in the container, so Docker's VM
  needs at least 8 GB of memory. Docker Desktop gives it half the computer's
  by default, 8 GB on a 16 GB machine; colima starts with 2 GB unless given
  `--memory 8`.
- The product's tool-call probe (`doctor --probe`) passed through this
  service, and both models load from its cache volume once downloaded.
- The cache is the `mirobody_models` volume of one compose project: a second
  checkout, or another `COMPOSE_PROJECT_NAME`, downloads the models again.
- This image logs `warning: no usable GPU found, --gpu-layers option will be
  ignored`: the preset asks for the GPU, which the `local` service uses, and
  the CPU build ignores the setting.

## Start the models

Pick the line for your machine. Every one serves the same preset; start it in
the `mirobody` folder (the one with `deploy.sh`). `--models-max 2` keeps one
answering model and the reader in memory, so choosing another size on the
setup page unloads the one before.

**Any computer with Docker, no GPU needed** (Windows, Linux or macOS). The
slowest: a first answer takes from 2–3 minutes on an M1 Pro's cores to about
15 on a 4-vCPU x86 server ([Without a GPU](#without-a-gpu)). It is the one
that needs nothing besides Docker, with at least 8 GB of memory for it, and
it downloads about 3.7 GB the first time: the 3.0 GB of models, and about
0.7 GB of images (llama.cpp's CPU image about 310 MB, the app about 230 MB,
Postgres about 160 MB), which take about 2 GB once unpacked (850, 640 and
460 MB on Linux x86):

```bash
COMPOSE_PROFILES=local-cpu ./deploy.sh     # the stack, plus llama.cpp's CPU image beside it
```

`deploy.sh` writes `COMPOSE_PROFILES` into `.env`, so a later
`docker compose up -d` keeps the model service, and points both local
entries at it (`LOCAL_BASE_URL` and `LOCAL_OCR_BASE_URL`,
`http://llama:8080/v1`), so the setup page has nothing to ask. A vendor key
exported in your shell is left out of `.env`, and one already there is named,
since its vendor would come first. The first question waits for the models'
download: `docker compose logs -f llama_cpu` shows it (`llama` for the GPU
service below). On a stack that is already
running, `docker compose --profile local-cpu up -d` starts it beside the app
(the line the setup page shows; `--profile local` for the GPU service below);
add `COMPOSE_PROFILES=local-cpu` to `.env` as well, or a `docker compose down`
removes it and the next `up` leaves it out.

**Windows**, to use the GPU (Intel, AMD or NVIDIA, through Vulkan; the CPU when
there is none). winget's `ggml.llamacpp` is llama.cpp's own Vulkan release
build, for x64 and arm64. In PowerShell, from the checkout in WSL (below;
`Ubuntu` is the distribution's name in `wsl -l`), so that the preset's
relative path, the one the setup page shows, resolves:

```powershell
winget install --id ggml.llamacpp
cd \\wsl.localhost\Ubuntu\home\<you>\mirobody
llama-server --models-preset docker\local-models.ini --port 8080 --models-max 2
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

The two compose services run llama.cpp build b11429, the one the evaluation
ran: `ghcr.io/ggml-org/llama.cpp:server-b11429` for `local-cpu` and
`server-cuda-b11429` for `local`. `LLAMA_CPU_IMAGE` and `LLAMA_IMAGE` in
`.env` replace them, then `docker compose up -d`: with a later build, or,
where ghcr.io is unreachable, with the same tag on a registry that mirrors
it (`LLAMA_CPU_IMAGE=<mirror>/ggml-org/llama.cpp:server-b11429`).

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
# with the local or local-cpu profile: http://llama:8080/v1 for both
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
your network. The `local` and `local-cpu` services need neither: the app
reaches them on compose's own network, and they publish no port. The app
itself is published on `127.0.0.1` only; `MIROBODY_BIND=0.0.0.0` in `.env`
offers it to your network, where the demo accounts' code, `111111`, is public
([SECURITY.md](../SECURITY.md) says what to change first).

`failed to initialize router models: ... Is a directory` in the model
service's log (`docker compose logs llama_cpu`, or `llama` on the GPU) means Docker could not see the checkout, and mounted an empty directory where
the preset should be. Colima shares only your home directory by default; keep
the checkout under it, or add the path to colima's `mounts`.

## The document reader

GLM-OCR-0.9B reads documents with either size. Three small OCR models that
upstream llama.cpp serves were run through the product's whole extraction
path on synthetic reports ([`benchmarks/local_ocr/`](../benchmarks/local_ocr/README.md)):
of 303 printed rows, GLM-OCR stored 283 with the printed value (302 with the
generator's "SYNTHETIC SAMPLE" banner removed, as on a real report) and none
the page does not print; PaddleOCR-VL-1.6 278 (282), with 12 (8) the page does
not print; MinerU2.5-Pro 279 (299), with 3 (4). Of 301 handwritten rows they
stored 99, 62 and 55 (219, 136 and 171 without the banner). GLM-OCR stays the
default; [model-choice.md](model-choice.md#the-document-reader-glm-ocr-09b)
has the whole table and the reasons.

PaddleOCR-VL-1.6 is in the preset as `[paddleocr-vl]` (Apache-2.0, 1.8 GB
with its vision projector), and since 1.5.4 the product reads its answers'
OTSL tables and LaTeX units. Its prompts are not GLM-OCR's, so
`LOCAL_OCR_MODEL=paddleocr-vl` alone is not enough: the `local-ocr` entry of
`config.llm.yaml` needs both changes:

```yaml
  local-ocr:
    model: paddleocr-vl           # the preset's section
    ocr_prompts:
      text: "OCR:"                # its own task prompts, from its model card
      tables: "Table Recognition:"
```

A source install reads the checkout's file; the Docker image carries its own,
so mount the edited one for `mirobody` and `mirobody_worker` in a
`compose.override.yaml` ([model-choice.md](model-choice.md#health-data-on-openrouter)
shows it), then `docker compose up -d` and `doctor --probe`.
Expect more rows in its OCR text, and on handwriting its passes looping to the
token cap (7 of 28 pages) and rows the page does not print.

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

**Ollama.** `ollama pull qwen3.8:27b` (17 GB) answered the large size's earlier
set (8 questions, asked twice) correctly, and fastest. Two things to know:

- Ollama sets the context from the GPU's memory, and below 24 GB it is 4,096
  tokens, which cuts Mirobody's prompt short without an error. Set
  `OLLAMA_CONTEXT_LENGTH=65536` before `ollama serve`.
- Its `glm-ocr` does not stop after reading a page in 0.34.1 and later
  ([ollama/ollama#18609](https://github.com/ollama/ollama/issues/18609)). Keep
  the documents on llama.cpp until that is fixed.

**Ternary Bonsai 2 27B** is the same Qwen3.8-27B in 6.6 GB and answered as well,
but today only PrismML's [llama.cpp fork](https://github.com/PrismML-Eng/llama.cpp)
runs it. Once upstream llama.cpp does, it is the candidate for the large size,
at half the download.

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

- **Nothing streams while the prompt is read.** A turn that adds 6.6k new
  tokens waits over a minute for its first byte on an M4 Pro, over two on an
  M1 Pro's 4 CPU cores, and longer on a slower processor
  ([Without a GPU](#without-a-gpu)). The `local` entry allows 600 s of silence (`stream_chunk_timeout`); the
  library default of 120 s fails a long turn.
- **The first turn after loading is the slow one.** The server caches the
  prompt it has read, so later turns read only what changed.
- **A reply can be all reasoning.** The agent asks once more when a reply has
  no answer text and no tool call; if the second one is empty too, the chat
  says it has no answer rather than showing a blank message.

## How a document is read, whichever model reads the rest

These hold with a vendor key as much as here: the vendor's model then
extracts readings only from what the table rules left, though it still writes
the file's title and summary from the document's text.

- **A long report is read a page at a time.** Text over 3,000 characters
  with page headers goes to the model one page per request, two at once, and
  the pages' readings are joined: in one request MiniCPM5-2B returned none of
  a 7-page check-up book's 78 rows, page by page all of them. A log's rows
  keep the dates they print.
- **A table is read by its header.** Rows under a header the rules know (项目名称 /
  结果 / 参考值 / 单位, Analyte / Result / Unit, a CSV's first line) are stored as
  printed and labelled `rules:table@v1`, when they look like readings and the
  page's other copy (the text layer, or the OCR's text pass) shows the same
  value. Patient details are skipped. The grid the OCR returns is read as the
  report printed it: a row whose empty cells moved is laid again by content,
  a header split over two rows or cells is joined, rows above a table's first
  header borrow it, and a page with no header is typed by its cells when
  nothing about it is ambiguous. A row left unread, text outside the tables
  that holds a number or a finding, and a document with no table go to the
  text model, without the text pass's copies of rows already read; a rule's
  row outranks the model's for the same printed row, under any name the
  vocabulary files under the same series.
- **Any OCR model's answer is read the same way.** An answer's OTSL tables
  (PaddleOCR-VL, MinerU) become HTML, its LaTeX (`\(\mu mol/L\)`) the
  characters it typesets, and a line it repeats until the token cap one copy;
  each pass is capped at 8,192 tokens.
