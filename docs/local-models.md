# Running on your own model server

Mirobody talks to models over the OpenAI-compatible API, so a model server on
your own machine can take the place of every vendor key. This page is the setup
that was measured, and what it costs.

Two models, two jobs. A document-OCR model (GLM-OCR-0.9B) reads report photos
and pages into text and HTML tables, and the tables' rows are read by their
column headers, with no model, then coded by ② Translate, which is offline
and deterministic. A general model (Bonsai-27B) is the agent, and writes the
titles and summaries and reads what no table holds. Neither has to know a code.

## What was measured

One Apple M4 Pro with 48 GB of unified memory, 2026-09-29, PrismML's
`llama-server` (build 10743, the fork that runs ternary GGUF).

| | GLM-OCR-0.9B Q8_0 (documents) | Ternary Bonsai 2 27B PTQ1_0 (agent) |
| --- | --- | --- |
| Files | 0.95 GB + 0.48 GB mmproj | 5.95 GB + 0.63 GB mmproj |
| Speed | one report photo: ~6 s text, ~10 s tables | ~100 tokens/s reading, ~16 writing |
| Demo uploads (PDF, photo, CSV, XLSX) | 27 of 27 readings, all by rule, all coded | |

A turn that reads one reading takes about 20 s. A turn that charts three months
of daily values took six minutes: a `vis-chart` block carries every point, so
the model writes each one out at 16 tokens a second.

## Setup

**1. Two model servers.** The entries speak the OpenAI-compatible API; these
are the servers that were measured:

```bash
# documents: GLM-OCR-0.9B (MIT), ggml-org/GLM-OCR-GGUF
llama-server -m GLM-OCR-Q8_0.gguf --mmproj mmproj-GLM-OCR-Q8_0.gguf \
  --host 127.0.0.1 --port 8081 -c 16384 --jinja -ngl 99

# the agent
llama-server -m Ternary-Bonsai-2-27B-PTQ1_0.gguf \
  --mmproj Ternary-Bonsai-2-27B-mmproj-Q8_0.gguf \
  --host 127.0.0.1 --port 8080 -c 65536 -np 2 --jinja -ngl 99 --reasoning on
```

`--jinja` is what turns on tool calls, and `--mmproj` is the vision half of each
model. GLM-OCR answers only its own task prompts (`Text Recognition:`,
`Table Recognition:`), which the `local-ocr` entry sends, one pass each.

**2. Two lines in `.env`:**

```bash
LOCAL_BASE_URL=http://127.0.0.1:8080/v1       # the agent, titles, summaries
LOCAL_OCR_BASE_URL=http://127.0.0.1:8081/v1   # report photos and pages
# in Docker (compose.yaml): http://host.docker.internal:8080/v1 and :8081/v1
```

They turn on the `local`, `local-utils` and `local-ocr` entries in
[`config.llm.yaml`](../config.llm.yaml). With no vendor key set they serve every
surface. With a key set, the key's models stay first; set `DEFAULT_MODEL=local`,
`UTILS_VISION_MODEL=local-utils` and `UTILS_TEXT_MODEL=local-utils` to use the
local servers anyway. Without `LOCAL_OCR_BASE_URL`, the agent's model reads the
documents too, and a model, not a rule, turns them into readings.

`compose.yaml` maps `host.docker.internal` for Docker Engine on Linux and keeps
it out of `HTTP_PROXY`, so a proxied deployment does not route model requests
through the proxy. On macOS (colima, measured) a container reaches a server
listening on `127.0.0.1`. On Linux the name points at the Docker bridge, which
cannot reach the host's loopback, so the server has to listen on the bridge
address (`--host 172.17.0.1` on a default install) instead.

**3. Check it:**

```bash
mirobody doctor --probe
```

`doctor` shows which entry each surface uses; `--probe` then sends each one a
real request (a tool call, a schema-bound answer, a rendered image, and the OCR
passes) through the code the product uses, and prints what the model did.

## Things that behave differently from a hosted model

- **Nothing streams while the prompt is read.** On the machine above, a turn
  that adds 6.6k new tokens waits 82 s for its first byte. The `local` entry
  allows 600 s of silence (`stream_chunk_timeout`); the library default of
  120 s fails a long turn.
- **The first turn after loading is the slow one.** The server caches the
  prompt it has read, so later turns read only what changed.
- **A reply can be all reasoning.** The agent asks once more when a reply has
  no answer text and no tool call, and reports an error if the second one is
  empty too.
- **A table is read by its header.** Rows under a header the rules know (项目名称 /
  结果 / 参考值 / 单位, Analyte / Result / Unit, a CSV's first line) are stored as
  printed and labelled `rules:table@v1`. When a table row is left unread, or a
  document has no table, the text model reads the document as well, and a rule's
  row outranks the model's for the same name.
