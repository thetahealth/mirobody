# Quickstart

**English** · **[中文](quickstart.zh-CN.md)** · **[日本語](quickstart.ja.md)**

Try data standardization with one command, or start the full app for document
processing and health questions.

| Your goal | Start here | Requirements |
| --- | --- | --- |
| Standardize data in a terminal or Python | [Use the library](#a--the-library) | uv, or Python 3.12+; no API key |
| Try the app in your browser | [Codespaces](#try-it-in-codespaces) | A GitHub account and a model API key |
| Run the app and models locally | [Local Docker](#run-it-100-locally) | Docker Compose; 16 GB RAM, including 8 GB for Docker |
| Develop from source | [Source checkout](#c--a-checkout) | Python 3.12+, Postgres with pgvector, and a model API key or local model server |

To explore a synthetic health record before installing, open the
[Mirobody Web demo](https://chat.mirobody.ai/demo?ref=github-quickstart&lang=en).

<a id="a--the-library"></a>

## A · Use the library

[Install uv](https://docs.astral.sh/uv/getting-started/installation/), then run:

```bash
uvx --python 3.12 mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

uv downloads Python and dependencies as needed. After the initial download,
name and unit resolution work offline, with no model, API key or database.

<p align="center">
  <img src="images/resolve-demo.gif" alt="Blood-test names in Chinese and Japanese resolve to the same LOINC code; a broad category is left unresolved" width="880">
</p>

`血红蛋白` and `ヘモグロビン` both resolve to LOINC `718-7`. The broad category
`血脂` is left unresolved to avoid assigning a code for the wrong test.

For Python 3.12+ projects, install the library:

```bash
pip install mirobody
```

```python
from mirobody import resolve, resolve_reading

resolve("血红蛋白").loinc  # '718-7'
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2'
```

Include units when resolving readings: they can change which code applies.
The base library depends only on numpy. It also imports Apple Health exports:

```bash
mirobody import apple ~/Downloads/export.zip
```

For PDFs, photos and spreadsheets, install `pip install 'mirobody[parse]'`
and configure a model API key. For an MCP client, run
`uvx --python 3.12 mirobody mcp`: vocabulary tools work offline over stdio;
the document tool `standardize_report` requires `[parse]` and a model key.
See [standardization.md](standardization.md) for symptom and diagnosis coding,
unit conversion and FHIR output, or try the scripts in
[`examples/`](../examples/README.md).

<a id="b--the-stack"></a>

## B · Start the full app

The app includes the web client, server, background worker and Postgres with
pgvector. Choose a cloud or local setup.

<a id="try-it-in-codespaces"></a>

### Try it in Codespaces

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/thetahealth/mirobody?quickstart=1)

Create a codespace and wait for automatic setup to finish in its terminal.
Open the HTTPS setup link it prints and enter your own model API key, such as
[OpenRouter](https://openrouter.ai/keys) or
[OpenAI](https://platform.openai.com/api-keys). The setup page verifies it with
a real request. Port `18060` is forwarded privately by default.

The app and records are stored in GitHub's cloud; documents and questions sent
for model processing go to your model provider. Use the supplied synthetic
data to explore. [Codespaces billing](https://docs.github.com/en/billing/concepts/product-billing/github-codespaces)
and model charges apply. Stop the codespace when finished; reopening it starts
the app again.

<a id="run-it-100-locally"></a>

### Run it 100% locally

With Git, OpenSSL and Docker Compose installed:

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
COMPOSE_PROFILES=local-cpu ./deploy.sh
```

Use a machine with 16 GB RAM and allocate at least 8 GB to Docker. No GPU or
model API key is required. On Windows, run these commands in WSL 2.
The first start downloads images and model weights; subsequent document and
model processing stays on your machine. CPU responses can take minutes.
See [local-models.md](local-models.md) for Mac and GPU setup.

The script generates secrets in `.env`, selects the local models and prints
the app link. The prebuilt image includes the LOINC bundle, so Git LFS is not
needed for Docker deployment.

**To use a cloud model**, run `./deploy.sh` without a local profile. Open the
setup link at `http://localhost:18060` and enter a model API key. Records stay
on your host; documents and questions sent for model processing go to that
provider.

### Sign in and try an upload

On the Email code tab, sign in as `you@mirobody.ai` with code `111111`.
No mail service is needed. Ask “How has my cholesterol changed?”, then upload
a synthetic file from [`demo/upload/`](../demo/) on the Data page. Those files
are not included in the seeded records, so uploading one tests document processing.

**Before the first start of a deployment for real data, set
`SEED_DEMO_DATA=false`.** Otherwise the app creates the two public demo accounts.

You can also put a model key in `.env`: `OPENROUTER_API_KEY`, `OPENAI_API_KEY`,
`GOOGLE_API_KEY` or `ANTHROPIC_API_KEY`. Use `<PROVIDER>_BASE_URL` for an
OpenAI-compatible gateway. [`config.llm.yaml`](../config.llm.yaml) lists the
variable each entry reads; store secrets in the environment, not that file.
After changing `.env`, run `docker compose up -d`; `restart` does not reload it.

Check the selected models and send a test request to each:

```bash
docker compose exec mirobody mirobody doctor --probe
```

<a id="c--a-checkout"></a>

## C · Develop from source

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e '.[app]'
```

Start Postgres with pgvector, or use an existing instance:

```bash
docker run -d -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pw \
    -e POSTGRES_DB=mirobody \
    pgvector/pgvector:pg17
```

```bash
mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody
```

From another terminal:

```bash
curl -fsS http://127.0.0.1:18090/api/health
```

The response includes `version`. Run `dev` from the repository root to serve
the web client in `frontend/` alongside the API. The Python package alone does
not include the web client.

`dev` generates temporary secrets. To preserve sessions and encrypted
configuration across restarts, set `JWT_KEY` and `CONFIG_ENCRYPTION_KEY`. Set
`PG_ENCRYPTION_KEY` as well: without it, stored record fields are encrypted
under `config.yaml`'s public placeholder (SECURITY.md).
For deployment, `serve` reads the shipped configuration and
`config.{ENV}.yaml` and expects persistent secrets.

On Debian or Ubuntu, `scripts/install_native.sh` installs PostgreSQL, pgvector
and a Python environment without Docker; `scripts/start_native.sh` starts the
worker and server on port 18060.

<a id="when-it-does-not-come-up"></a>

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| No model is available | Open the setup link printed by `./deploy.sh`, then run `mirobody doctor`. |
| A fresh source checkout cannot resolve LOINC terms | Run `git lfs pull` to download the data bundle. |
| `dev` cannot find Postgres | Pass `--pg-url` or set `PG_URL` / `DATABASE_URL`. For `serve`, set `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD` and `PG_DBNAME`. |
| Device sync does not run | Check worker logs and the Postgres connection. |
| Docker Hub is unreachable | `deploy.sh` reports the failure and retries through a mirror. Set `DOCKER_MIRROR=<registry>/` in `.env` to choose one. |

<a id="next"></a>

## Next

Use the [self-host guide](https://docs.mirobody.ai/en/self-host) for deployment,
[walkthrough.md](walkthrough.md) for an app tour, or
[provider-setup.md](provider-setup.md) to connect Garmin, Oura or Whoop.
For development, see [repository-layout.md](repository-layout.md) and
[pipeline.md](pipeline.md).
