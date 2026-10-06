# Docker Hub page copy

This is the copy for `thetahealth4mirobody/mirobody`. Keep the short
description in Docker Hub's repository summary field and paste the Markdown
under “Full description”. It deliberately leads with the self-hosted result;
implementation details belong on the documentation site.

## Short description

Self-hosted health data engine for lab reports, wearables, genomics, and source-citing agent tools.

## Full description

# Mirobody

Mirobody turns lab reports, wearable data, Apple Health exports, and genetic files into one local health record. Its deterministic Translate layer standardizes names, units, complaints, diagnoses, and genetic data before an agent reasons over them. Every answer can point back to the source file and page.

## Start with Docker

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
OPENROUTER_API_KEY=sk-or-... ./deploy.sh
```

Open `http://localhost:18060` and sign in with the demo account the page
offers (`you@mirobody.ai`, code `111111`). No Git? The source tarball is the
same checkout:

```bash
curl -L https://github.com/thetahealth/mirobody/archive/refs/heads/main.tar.gz | tar xz
cd mirobody-main && OPENROUTER_API_KEY=sk-or-... ./deploy.sh
```

This image is not a single container: it runs beside its own Postgres, which
`compose.yaml` starts, and `deploy.sh` generates the secrets the two share.
Run on its own (`docker run`), it stops within ten seconds and says it cannot
reach Postgres. Use any supported model key or an OpenAI-compatible gateway;
the key stays in your `.env` file, and one added later needs
`docker compose up -d` (a `restart` does not read `.env` again).

## What it does

- Reads PDFs, report photos, spreadsheets, CSV exports, wearable data, and genetic files.
- Resolves alternate names such as `A1c`, `HbA1c`, and `Glycated Hemoglobin` to one LOINC identity.
- Normalizes units to UCUM and keeps the original source and page evidence.
- Standardizes complaints and diagnoses with ICPC-3, and handles common genetic formats and CPIC pharmacogenomics data.
- Serves seven user-scoped MCP tools: `resolve_indicator`, `convert_unit`, `normalize_unit`, `query_health_indicators`, `query_medications`, `query_genetic_data`, and `query_pharmacogenomics`. Settings issues a personal link for Claude Code, Codex, Cursor or Gemini CLI; Claude Desktop takes it through `npx -y mcp-remote <link>`.
- Refuses ambiguous category words instead of inventing a code.

## Use the engine without Docker

```bash
uvx --python 3.12 mirobody resolve "LDL cholesterol" 血红蛋白
```

The lexical resolver and unit engine are offline and need no model key. A key
is needed only when the document extraction path calls a model.

- [Documentation](https://docs.mirobody.ai/en/)
- [Self-host guide](https://docs.mirobody.ai/en/self-host)
- [Source and issues](https://github.com/thetahealth/mirobody)
- [Security policy](https://github.com/thetahealth/mirobody/blob/main/SECURITY.md)

Mirobody is Apache-2.0 licensed. Keep health data on infrastructure you
control and review the model provider and retention policy you choose.
