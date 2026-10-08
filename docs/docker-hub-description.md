# Docker Hub page copy

This is the copy for `thetahealth4mirobody/mirobody`. The `docker-hub.yml`
workflow publishes the short description to Docker Hub's repository summary
(100 characters at most) and the Markdown under "Full description" as the full
description. It leads with the result; implementation details belong on the
documentation site.

## Short description

Self-hosted AI health data engine: lab reports, wearables, genetics. Answers cite their sources.

## Full description

# Mirobody

**Turn scattered health data into answers you can trace.** Mirobody brings lab
reports, wearable data, Apple Health exports and genetic files into one
record in a Postgres you run. Names and units are resolved to one standard
offline, and the agent's answers point back to the file and page each number
came from.

## Start with Docker

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh
```

Open the setup link the script prints and paste one model key: OpenRouter,
OpenAI, Gemini, Anthropic, DeepSeek, DashScope or any OpenAI-compatible
gateway. The key is kept only after one real request through it works. Then
sign in at `http://localhost:18060` as `you@mirobody.ai`, code `111111`: a
synthetic demo record with two people's readings is already there.

No Git? The source tarball is the same checkout:

```bash
curl -L https://github.com/thetahealth/mirobody/archive/refs/heads/main.tar.gz | tar xz
cd mirobody-main && ./deploy.sh
```

This image is not a single container: it runs beside its own Postgres, which
`compose.yaml` starts, and `deploy.sh` generates the secrets the two share.
Run on its own (`docker run`), it stops within ten seconds and says it cannot
reach Postgres.

## Or keep every model on your machine

```bash
COMPOSE_PROFILES=local-cpu ./deploy.sh
```

This starts [llama.cpp](https://github.com/ggml-org/llama.cpp)'s CPU image
beside the stack, so no model key is needed and your documents and questions
stay on the machine; the models download once from Hugging Face. Allow 16 GB
of memory, at least 8 GB of it for Docker. With no GPU a first answer takes
minutes: 2–3 on an M1 Pro's cores, up to about 15 on a 4-vCPU x86 server.
`COMPOSE_PROFILES=local` uses an NVIDIA GPU instead, and on a Mac
`llama-server` runs natively on the GPU. See
[local models](https://github.com/thetahealth/mirobody/blob/main/docs/local-models.md).

## What it does

- Reads PDFs, report photos, spreadsheets, CSV exports, wearable data and genetic files.
- Resolves alternate names such as `A1c`, `HbA1c` and `Glycated Hemoglobin` to one LOINC identity, and units to UCUM, keeping the source page as evidence.
- Codes complaints and diagnoses with ICPC-3, and reports CPIC pharmacogenomic coverage without recommending a change of medication.
- Shares a family member's record only when they allow it, view or edit; joining a care circle alone shares nothing.
- Serves seven MCP tools, each scoped to the signed-in person: four read the record (`query_health_indicators`, `query_medications`, `query_genetic_data`, `query_pharmacogenomics`) and three resolve names and units (`resolve_indicator`, `convert_unit`, `normalize_unit`). Settings issues a personal link for Claude Code, Codex, Cursor or Gemini CLI.
- Refuses ambiguous category words instead of inventing a code.

## Use the engine without Docker

```bash
uvx --python 3.12 mirobody resolve "LDL cholesterol" 血红蛋白
```

The resolver and unit engine are offline and need no model key.

- [Documentation](https://docs.mirobody.ai/en/self-host)
- [Source and issues](https://github.com/thetahealth/mirobody)
- [Security policy](https://github.com/thetahealth/mirobody/blob/main/SECURITY.md)

Mirobody is Apache-2.0 licensed. Keep health data on infrastructure you
control, and review the terms of the model provider you choose.
