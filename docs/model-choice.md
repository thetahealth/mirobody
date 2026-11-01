# Choosing a model

**English** · **[中文](model-choice.zh-CN.md)** · **[日本語](model-choice.ja.md)**

Choose where your health data may be processed, then consider your hardware
and image-understanding needs. Mirobody supports fully local, cloud and hybrid
configurations. Chat, document extraction and journal entries can use different
models.

## Deployment options

| Your priority | Configuration | Requirements |
| --- | --- | --- |
| Keep all model processing local | MiniCPM5-2B for chat and GLM-OCR-0.9B for documents | A computer with 16 GB of memory; no GPU required |
| Keep processing local and interpret images directly | Qwen3.8-27B for chat and GLM-OCR-0.9B for documents | A 32 GB Mac or a 24 GB NVIDIA GPU |
| Use cloud models without local inference hardware | A configured cloud model | A model API key; the provider receives the data needed for its tasks |
| Keep document images local while using cloud answers | Local GLM-OCR with a cloud chat model | A local OCR server and a model API key |

The setup page supports local or cloud models; hybrid mode requires `.env`
settings. CPU-only answers can take several minutes, depending on the hardware,
model and conversation.

## Local models

The default local configuration uses MiniCPM5-2B with Q8_0 weights. The larger
configuration uses Qwen3.8-27B with IQ3_S weights and its vision projector.
Both use GLM-OCR-0.9B for document recognition.

Inference runs in [llama.cpp](https://github.com/ggml-org/llama.cpp)'s
`llama-server`, with models downloaded from Hugging Face on first use. See
[local setup](local-models.md) for platform-specific GPU and CPU configurations.

<a id="the-document-reader-glm-ocr-09b"></a>

### Documents and photos

GLM-OCR extracts text and tables from reports. Rules read recognized table
layouts, and a model processes the remaining text. The lightweight chat model
receives a photo's OCR text and cannot infer its visual appearance. The
larger chat model can inspect the image directly, but its interpretation,
including a meal's identity or calorie estimate, may still be wrong.

## Data sent to cloud models

Cloud models reduce local hardware requirements. After configuring a key,
choose an available entry in the model picker.

| Data or task | Fully local | Cloud configuration | Local OCR with cloud chat |
| --- | --- | --- | --- |
| Report photos and scanned pages | Processed locally | Sent to the vision model | Processed locally |
| Document text | Processed locally | Text left after local table extraction is sent; the first 3,000 characters also go to title and summary generation | The same text is sent |
| Questions and rows retrieved by the agent | Stay local | Sent to the chat provider | Sent to the chat provider |
| Images attached in chat | OCR text or local vision, depending on the model | Sent if the chat model supports images | Also sent if the chat model supports images |
| Terminology and unit normalization | Offline | Offline | Offline |

Local OCR keeps document images local; text can still leave. Chat images
follow the chat model's settings separately from uploaded files.

<a id="health-data-on-openrouter"></a>

### OpenRouter routing

OpenRouter may route requests among hosting providers, whose retention terms
and supported parameters differ. A setup-page key or `OPENROUTER_CHAT_MODEL`
does not pin a host. Review both account data settings and the host's terms.

The recorded evaluation required zero data retention, with one host per model
and no fallback. These settings use `provider.order` and
`allow_fallbacks: false` in `extra_body`; see the
[reference configurations](../benchmarks/local_models/refs/). They control
routing and retention, while processing remains external.

## Hybrid configuration

Start the local model server with GLM-OCR available, then set:

```bash
OPENROUTER_API_KEY=sk-or-...
LOCAL_OCR_BASE_URL=http://host.docker.internal:8080/v1
UTILS_VISION_MODEL=local-utils
```

`LOCAL_OCR_BASE_URL` sends report images to local OCR even with a cloud key
present. `UTILS_VISION_MODEL=local-utils` keeps files without extractable text,
such as meal photos, local too: they receive a local description or none.

For chat images, set `supports_image: false` on the cloud chat entry so it
receives OCR text. Configure hybrid mode in `.env` and the model entries.

## Switching and checking models

- **Setup page:** open the link printed by `./deploy.sh`, or go to
  **Settings → Model**. Enter a key and model name, or choose **100% on this
  machine** and a local configuration. The choice is checked, encrypted and
  applied without a restart.
- **Environment settings:** `LOCAL_MODEL` and `LOCAL_OCR_MODEL` select local
  models. `OPENROUTER_CHAT_MODEL` and `OPENROUTER_UTILS_MODEL` select the
  models behind an OpenRouter key. `DEFAULT_MODEL`, `UTILS_TEXT_MODEL`,
  `UTILS_VISION_MODEL` and `UTILS_OCR_MODEL` select configuration entries.
  After changing `.env`, run `docker compose up -d`; a restart alone does
  not reload it. See [Configuration](https://docs.mirobody.ai/en/configuration)
  and [`config.llm.yaml`](../config.llm.yaml) for advanced settings.

Check the selected routes with:

```bash
docker compose exec mirobody mirobody doctor --probe
```

The probe checks a streamed chat tool round trip and visible thinking,
plus document extraction, vision and OCR. It also verifies local model
names. A successful probe confirms those capabilities, not answer quality
or expected latency.

## Thinking and the final answer

Chat requests thinking through supported interfaces. `AGENT_THINKING` defaults
to `medium`; an enabled entry-level `thinking_level` can override it. Utility
tasks retain their settings. The work panel shows returned thinking and tool
activity; final text appears after its model step finishes. Some endpoints
withhold reasoning, which the probe reports; Mirobody does not invent it.


## Evaluation reference

See the [evaluation guide](../benchmarks/local_models/README.md),
[historical results](../benchmarks/local_models/HISTORY.md) and
[OCR benchmark](../benchmarks/local_ocr/README.md) for methods, measured results
and reproduction commands. Results use synthetic records and apply to the
recorded versions, hardware and providers.
