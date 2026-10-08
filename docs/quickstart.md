# Quickstart

**English** · **[中文](quickstart.zh-CN.md)**

**To see the whole product, start with Docker.** Two commands bring up the web
app with a synthetic demo record; a model key or the local models do the rest:

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh
```

The rest of this page is for developers who want one part of it. The three
ways in are alternatives, not steps:

| You want | Go to | Needs | Model key |
| --- | --- | --- | --- |
| Names and units resolved in your own code | [A · the library](#a--the-library) | Python 3.12 | none |
| The whole product running, with data in it | [B · the stack](#b--the-stack) | Docker with Compose | one, or none with the local models on llama.cpp |
| To change the code and see it | [C · a checkout](#c--a-checkout) | Python + a Postgres with pgvector | one, or a local model server |

For a hosted API key and `/v1` requests, use the separate [Cloud
quickstart](https://docs.mirobody.ai/en/api-reference/quickstart/). This page
covers the open-source engine and its own commands.

## A · the library

Two packages, numpy the only dependency. No key, no network, no database, and
no model runs on your machine: the resolver is a lexical index over LOINC,
not an LLM.

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

With [uv](https://docs.astral.sh/uv/) there is nothing to install:
`uvx --python 3.12 mirobody resolve ...`. The `--python 3.12` matters: on an
older default interpreter uv would pick a release from before 1.2.

<p align="center">
  <img src="images/resolve-demo.gif" alt="mirobody resolve: 血红蛋白 and ヘモグロビン landing on the same LOINC code, and one deliberate abstention" width="880">
</p>

`血脂` names a category rather than one observation, so it resolves to nothing.
That is the design: a wrong code puts two different tests on one trend line.

The same from Python. The unit is part of a reading's identity, so it can
change the code:

```python
from mirobody.engine import resolve, resolve_reading, standardize_reading

resolve("血红蛋白").loinc                                     # '718-7'   any language, one code
resolve("total cholesterol").loinc                          # '2093-3'  [Mass/volume]
resolve_reading("total cholesterol", "5.0", "mmol/L").loinc  # '14647-2' [Moles/volume]: the unit picks the code
resolve_reading("中性粒细胞", "62 %", None).loinc              # '26511-6' a percentage...
resolve_reading("中性粒细胞", "4.2", "10*9/L").loinc           # '26499-4' ...and a count are two codes
standardize_reading("血红蛋白", "13.5", "g/dL")["code"]["coding"][0]["code"]  # '718-7'  as a FHIR Observation
```

Complaints and diagnoses have their own axis, ICPC-3, with the same rule: a
code or a stated refusal, never a guess.

```python
from mirobody.translate import resolve_symptom, resolve_condition

resolve_symptom("头疼").code           # 'NS01'         Headache; 'headache' and '頭痛' answer the same
resolve_symptom("疼").outcome         # 'refused'      too broad to code; the words are kept
resolve_condition("2型糖尿病").code    # 'TD72'         Type 2 diabetes mellitus
resolve_condition("糖尿病").outcome    # 'needs-input'  which type? asked, not assumed
```

Reading a vendor export needs nothing further; `zipfile` and `xml.etree` are
both stdlib:

```bash
mirobody import apple ~/Downloads/export.zip
```

For the rest: `pip install 'mirobody[parse]'` turns a PDF, photo or
spreadsheet into readings (that one calls a model, so it needs a key), and
`pip install 'mirobody[app]'` installs the server. Neither is needed for the
above. `[agent]` is the agent harness as a library, `[test]` the test suite.

Without the stack, `uvx --python 3.12 mirobody mcp` serves the vocabulary to
an MCP client over stdio, offline and with no key. Its one tool that reads a
whole document, `standardize_report`, also needs the `[parse]` extra and a
model key. [standardization.md](standardization.md) covers ② Translate in
depth, and [`examples/`](../examples/README.md) has runnable scripts.

## B · the stack

Postgres with pgvector, the server and the worker, with the demo record
already seeded.

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh                       # → prints the setup link on http://localhost:18060
```

`deploy.sh` writes a `.env` with generated secrets and pulls the prebuilt
image, which already contains the LOINC bundle, so Docker users need no Git
LFS. It needs OpenSSL for the secrets. With no model yet it prints a link to
the first-run page: paste **one** model key there, or choose **100% on this
machine**. To run the models in Docker beside the stack, start with
`COMPOSE_PROFILES=local-cpu ./deploy.sh` instead; [local-models.md](local-models.md)
has the GPU and Mac routes and what each needs.

Sign in as `you@mirobody.ai` with code `111111` on the Email code tab; no mail
provider is involved. `SEED_DEMO_DATA` is on, so 2,019 readings across two
accounts are already there. Set it to `false` before the first start if you
intend to hold real data, and neither account is created. Four files in
[`demo/upload/`](../demo/) are deliberately not seeded, so dropping one on the
Data page walks the real path.

A key can also go in `.env` instead of the setup page: `OPENROUTER_API_KEY`,
`OPENAI_API_KEY`, `GOOGLE_API_KEY`, `ANTHROPIC_API_KEY`, or any
OpenAI-compatible gateway through `<PROVIDER>_BASE_URL`. Run
`docker compose up -d` afterwards; a `restart` does not reread `.env`.
[`config.llm.yaml`](../config.llm.yaml) names the variable each entry reads
(`api_key: OPENROUTER_API_KEY`), never the secret. To see what each model
surface selected:

```bash
docker compose exec mirobody mirobody doctor          # add --probe to send each a real request
```

[The self-host guide](https://docs.mirobody.ai/en/self-host) goes step by step,
and [walkthrough.md](walkthrough.md) tours the running stack.

## C · a checkout

`serve` is the deployment shape: it reads the shipped config and any
`config.{ENV}.yaml` overlay, and expects persistent secrets. `dev` runs the
same server in one process with an in-memory config and generated secrets:

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull
python3.12 -m venv .venv && . .venv/bin/activate
pip install -e '.[app]'
```

If you do not already have Postgres with pgvector, start one first:

```bash
docker run -d -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pw \
    -e POSTGRES_DB=mirobody \
    pgvector/pgvector:pg17
```

Then start the API, replacing the connection URL if you use an existing
database. Check its health endpoint from another terminal:

```bash
mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody
```

```bash
curl -fsS http://127.0.0.1:18090/api/health
```

The response includes a `version` field. Use pgvector rather than plain
Postgres: the schema creates a vector column on first start.

**`dev` serves the API, not the web client.** The built client lives at
repo-root `frontend/` and is outside the package, so it is there in a checkout
and absent from a `pip install`. What `dev` is for is the API, the MCP surface
and the agent; for the UI, use B.

The secrets `dev` generates are per-run: sessions and encrypted config values
do not survive a restart. Set `JWT_KEY` and `CONFIG_ENCRYPTION_KEY` in the
environment to keep them.

## When it does not come up

| What you see | What it is |
| --- | --- |
| `keys present : none` and every surface `--` from `mirobody doctor` | No model key and no local model server. ① Collect and ② Translate still work; extraction and answers do not. Open the link `./deploy.sh` printed. |
| A LOINC lookup raises on a fresh clone | `git lfs pull` has not run, so the bundle is still a pointer stub. |
| `mirobody dev` exits asking for a Postgres | `--pg-url`, or `PG_URL` / `DATABASE_URL` in the environment. |
| The server starts but device sync never runs | Check the worker logs and Postgres connection; device pulls and task state both use Postgres. |
| `mirobody serve` cannot connect to Postgres | Set `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD` and `PG_DBNAME` for that database. |

## Next

[walkthrough.md](walkthrough.md) for the tour of the running stack ·
[local-models.md](local-models.md) to keep every model on your machine ·
[pipeline.md](pipeline.md) for what happens to a reading ·
[repository-layout.md](repository-layout.md) for the map ·
[provider-setup.md](provider-setup.md) to turn on Garmin, Oura or Whoop.
