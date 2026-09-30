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
git clone --depth 1 https://github.com/thetahealth/mirobody.git
cd mirobody
./deploy.sh
echo 'OPENROUTER_API_KEY=sk-or-...' >> .env
docker compose up -d
```

Open `http://localhost:18060`. Docker is the only local prerequisite: the
image includes the server and worker, and Postgres runs as a separate Compose
service. Use any supported model key or an OpenAI-compatible gateway; the key
stays in your `.env` file.

## What it does

- Reads PDFs, report photos, spreadsheets, CSV exports, wearable data, and genetic files.
- Resolves alternate names such as `A1c`, `HbA1c`, and `Glycated Hemoglobin` to one LOINC identity.
- Normalizes units to UCUM and keeps the original source and page evidence.
- Standardizes complaints and diagnoses with ICPC-3, and handles common genetic formats and CPIC pharmacogenomics data.
- Serves seven user-scoped MCP tools: `resolve_indicator`, `convert_unit`, `normalize_unit`, `query_health_indicators`, `query_medications`, `query_genetic_data`, and `query_pharmacogenomics`.
- Refuses ambiguous category words instead of inventing a code.

## Use the engine without Docker

```bash
uvx mirobody resolve "LDL cholesterol" 血红蛋白
```

The lexical resolver and unit engine are offline and need no model key. A key
is needed only when the document extraction path calls a model.

- [Documentation](https://docs.mirobody.ai/en/)
- [Self-host guide](https://docs.mirobody.ai/en/self-host)
- [Source and issues](https://github.com/thetahealth/mirobody)
- [Security policy](https://github.com/thetahealth/mirobody/blob/main/SECURITY.md)

Mirobody is Apache-2.0 licensed. Keep health data on infrastructure you
control and review the model provider and retention policy you choose.
