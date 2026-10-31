<a id="running-on-your-own-machine-with-no-api-key"></a>

# Run models locally

**English** · **[中文](local-models.zh-CN.md)**

Run Mirobody's models on your own machine to keep document processing and
questions local, without a model API key. The first start downloads model
weights; later starts reuse the cache.

Mirobody connects to [llama.cpp](https://github.com/ggml-org/llama.cpp) through
its OpenAI-compatible `llama-server` API. One model answers questions and
writes summaries; GLM-OCR-0.9B reads report images and scanned pages.

<a id="quick-start"></a>

## Choose your setup

| Platform | Model service | Requirements |
| --- | --- | --- |
| Windows, Linux or macOS without a GPU | Docker Compose with `local-cpu` | 16 GB RAM; at least 8 GB allocated to Docker |
| Mac with Apple silicon | Native `llama-server` | 16 GB RAM for the default model |
| Linux or Windows with an NVIDIA GPU | Docker Compose with `local` | A compatible GPU; NVIDIA Container Toolkit on Linux |
| Windows with an Intel, AMD or NVIDIA GPU | Native Vulkan `llama-server` | A Windows checkout and the matching llama.cpp build |

CPU responses can take minutes. For a Mac, running llama.cpp directly on the
host allows it to use the GPU; a Mac Docker container cannot use that GPU.

<a id="choose-a-size"></a>

## Choose a model

| Setup option | Answering model | Model download, including OCR | Hardware |
| --- | --- | --- | --- |
| **Small**, the default | MiniCPM5-2B, Q8_0 | About 4.1 GB | 16 GB RAM; no GPU required |
| **Large** | Qwen3.8-27B, IQ3_S | About 14.5 GB | A 32 GB Mac or an NVIDIA GPU with 24 GB VRAM |

Both options use GLM-OCR for documents. MiniCPM5-2B receives a chat photo's OCR
text; Qwen3.8-27B can also inspect the image. See
[model-choice.md](model-choice.md) for model comparisons and the
[benchmark records](../benchmarks/local_models/README.md) for measurements.

<a id="start-the-models"></a>

## Start the model service

Run these commands from the repository root, where `deploy.sh` is located.
The presets use relative paths, including `docker/minicpm5-2b.jinja`.
`--models-max 2` keeps the answering model and OCR model loaded; switching the
answering model unloads the previous one.

### Docker on a CPU

```bash
COMPOSE_PROFILES=local-cpu ./deploy.sh
```

The script starts the app and models, saves `COMPOSE_PROFILES` in `.env`, and
sets `LOCAL_BASE_URL` and `LOCAL_OCR_BASE_URL` to `http://llama:8080/v1`.
Watch the initial download with `docker compose logs -f llama_cpu`.

For an existing deployment, use `docker compose --profile local-cpu up -d`
and add `COMPOSE_PROFILES=local-cpu` to `.env` so the model service returns
after `docker compose down` and a later `up`.

### Mac with Apple silicon

Start llama.cpp on the host:

```bash
brew install llama.cpp
llama-server --models-preset docker/local-models.ini --port 8080 --models-max 2
```

### NVIDIA GPU with Docker

On Linux, install the NVIDIA Container Toolkit first. On Windows, use Docker
Desktop with its WSL 2 backend. Then run:

```bash
COMPOSE_PROFILES=local ./deploy.sh
```

### Native Windows model service

The official winget package uses Vulkan and supports Intel, AMD and NVIDIA
GPUs on x64 and arm64. In PowerShell:

```powershell
winget install --id ggml.llamacpp
git clone https://github.com/thetahealth/mirobody.git C:\src\mirobody
cd C:\src\mirobody
llama-server --models-preset docker\local-models.ini --port 8080 --models-max 2
```

Use a Windows drive, not a `\\wsl.localhost\...` UNC working directory, and
keep this checkout on the same release as the app's WSL checkout. For CPU
execution, use `docker\local-models-cpu.ini` instead.

For CUDA builds, choose a llama.cpp release that supports your GPU and
install its matching runtime libraries.

Mirobody itself runs in Docker Desktop. Enable **Settings → Resources → WSL
integration**, then clone the repository in a WSL terminal and run
`./deploy.sh`. If Docker reports `permission denied`, run
`sudo usermod -aG docker $USER`, then `wsl --shutdown` from Windows.

For other Linux GPUs or a model service without Docker, use a matching
[llama.cpp release](https://github.com/ggml-org/llama.cpp/releases) and the same
`llama-server` command.

<a id="point-mirobody-at-them"></a>

## Connect Mirobody

Open the setup link from `./deploy.sh`, or **Settings → Model**, and choose
**100% on this machine**. Mirobody checks `host.docker.internal:8080`,
`llama:8080`, then `127.0.0.1:8080`. Select the models; the page verifies that
the server provides them, loads them and shows progress. The selection is
stored encrypted in the database.

Alternatively, set the addresses in `.env`:

```bash
LOCAL_BASE_URL=http://host.docker.internal:8080/v1
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1
```

Use `http://llama:8080/v1` for both addresses with the `local` or `local-cpu`
Compose profile. If both app and models run directly on the host, use
`http://127.0.0.1:8080/v1`.

Cloud-provider keys in `.env` take priority, and the setup page blocks the
local choice while they are present. Remove them, or explicitly set
`DEFAULT_MODEL=local`, `UTILS_VISION_MODEL=local-utils` and
`UTILS_TEXT_MODEL=local-utils`. The local deployment script does not copy
provider keys from your shell into `.env` and reports any already in the file.

After editing `.env`, run `docker compose up -d`. A restart does not reload it.

<a id="check-it"></a>

## Verify the connection

```bash
docker compose exec mirobody mirobody doctor --probe
```

This checks the selected models with real requests, including tool calling,
structured extraction, images and OCR.

<a id="when-it-does-not-work"></a>
<a id="without-a-gpu"></a>

## Troubleshooting

| Symptom | Action |
| --- | --- |
| No model server found | Start it on port 8080. On Linux, check the host networking note below. |
| The first question waits | Models may still be downloading. Check `docker compose logs -f llama_cpu`, or `llama` for the GPU service. |
| Docker runs out of memory | Allocate at least 8 GB in Docker Desktop, or use `colima start --memory 8`. CPU deployments use `docker/local-models-cpu.ini`; GPU deployments use `docker/local-models.ini`. |
| The log says `Is a directory` for the model preset | Check Docker can access the checkout. Colima shares the home directory by default; move the checkout there or add its path to `mounts`. |
| Downloads cannot reach Hugging Face | Set `HF_ENDPOINT=https://<mirror>` in `.env` for Compose, or in the shell that starts `llama-server`. |
| Local requests fail through a system proxy | In the calling shell, set `export NO_PROXY=localhost,127.0.0.1`. This also applies to WSL inheriting a Windows proxy. |

The model cache is the Compose project's `mirobody_models` volume. A different
checkout or `COMPOSE_PROJECT_NAME` may create another cache and download again.
The images are pinned to llama.cpp b11429. Override them with `LLAMA_CPU_IMAGE`
or `LLAMA_IMAGE` in `.env`, then run `docker compose up -d` if you need another
build or a registry mirror.

**Host networking on Linux:** `host.docker.internal` points to the Docker
bridge. A host model server must listen on that bridge address, for example
`--host 172.17.0.1`; use `ip -4 addr show docker0` to check it. Listening on
`0.0.0.0` exposes the model server without authentication to the network.
Docker Desktop and Colima on Mac can reach the host's default `127.0.0.1`
listener. The Compose model services need no host port and communicate on the
internal network. The app also binds to `127.0.0.1` by default. Before changing
`MIROBODY_BIND=0.0.0.0`, review [SECURITY.md](../SECURITY.md), especially the
public demo code `111111`.

<a id="other-models"></a>
<a id="other-servers"></a>

## Use a different model or server

Add a model to the llama.cpp preset, then select it on the setup page, or set:

```bash
LOCAL_MODEL=qwen3.8-27b
LOCAL_OCR_MODEL=glm-ocr
```

The server must advertise the model you select. Other OpenAI-compatible
servers can use the same address and model-name settings; inspect their
model list with `curl <address>/v1/models`. The `local` entry uses
`temperature: 0`; additional model-specific settings belong in the preset.
For cloud entries, the equivalent override is the entry's `model_env` variable
in `config.llm.yaml`, such as `OPENROUTER_CHAT_MODEL=anthropic/claude-opus-5.5`.
The setup page verifies a changed model with a real request before saving it.

For Ollama, set `OLLAMA_CONTEXT_LENGTH=65536` before `ollama serve` to avoid
`exceed_context_size_error` with Mirobody's prompts. Its `glm-ocr` has a known
termination issue in 0.34.1 and later
([issue](https://github.com/ollama/ollama/issues/18609)); keep document OCR on
llama.cpp while that issue remains.

<a id="the-document-reader"></a>
<a id="what-each-model-can-read-in-a-photo"></a>
<a id="things-that-behave-differently-from-a-hosted-model"></a>
<a id="how-a-document-is-read-whichever-model-reads-the-rest"></a>

## Documents, photos and calculations

GLM-OCR reads text and tables, not scenes such as meals or rashes. With the
default answering model, chat receives OCR text rather than the image and
asks for a description when there is no readable text. An image-capable
model such as Qwen3.8-27B can inspect a photo, but food estimates remain
approximate.

Both local and cloud setups use the same document pipeline: long reports are
processed by page, recognized tables are extracted by rules, and remaining
content goes to the text model. See [pipeline.md](pipeline.md) for the process
and [model-choice.md](model-choice.md#the-document-reader-glm-ocr-09b) for OCR
alternatives and their configuration.

All chat models use `python_exec` for calculations over queried records
once its separate local Docker executor is enabled, as described in
[Local Python analysis](python-analysis.md); without one, the tool is left out.
