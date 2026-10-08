# Quickstart

Three ways in. They are not steps — pick the row that matches what you want,
and ignore the other two.

| You want | Go to | Needs | Key |
| --- | --- | --- | --- |
| Names and units resolved in your own code | [A · the library](#a--the-library) | Python 3.12 | none |
| The whole product running, with data in it | [B · the stack](#b--the-stack) | Docker | one for model features, or none with the local models on llama.cpp |
| To change the code and see it | [C · a checkout](#c--a-checkout) | Python + a Postgres | one |

For a hosted API key and `/v1` requests, use the separate [Cloud
quickstart](https://docs.mirobody.ai/en/api-reference/quickstart/). This page
covers the open-source engine and its own commands.

## A · the library

Two packages, numpy the only dependency. No key, no network, no database, and
no model runs on your machine — the resolver is a lexical index over LOINC,
not an LLM.

```bash
pip install mirobody
mirobody resolve "LDL cholesterol" 血红蛋白 ヘモグロビン "空腹血糖(GLU)" 血脂
```

`血脂` names a category rather than one observation, so it resolves to nothing.
That is the design: a wrong code puts two different tests on one trend line.

Reading a vendor export needs nothing further — `zipfile` and `xml.etree` are
both stdlib:

```bash
mirobody import apple ~/Downloads/export.zip
```

For the rest: `pip install 'mirobody[parse]'` to turn a PDF, photo or
spreadsheet into readings (that one calls a model, so it needs a key), and
`pip install 'mirobody[app]'` for the server. Neither is needed for the above.
(`[agent]` is the agent harness as a library, `[test]` the test suite.)

## B · the stack

Postgres + pgvector, the server and the worker, with the demo record already
seeded.

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
./deploy.sh                       # → prints the first-run page's link on http://localhost:18060
```

`deploy.sh` pulls the application image, which already contains the LOINC
bundle, and writes a `.env` with generated secrets on first run. Docker users
do not need Git LFS. With no model yet, it prints the link to the first-run
page: paste **one** model key there, or choose **100% on this machine** to run
every model locally on [llama.cpp](local-models.md) (`llama-server`, the local
model runtime Mirobody ships a preset for; the default pair needs 16 GB of
memory and no GPU, and without a GPU a first answer takes minutes: 2–3 on an M1 Pro's cores, up to about 15 on a 4-vCPU x86 server). A key
can also go in `.env`, followed by `docker compose up -d` (a `restart` does not
reread `.env`). Check available model features inside the server container:

```bash
docker compose exec mirobody mirobody doctor
```

Sign in as `you@mirobody.ai` with code `111111`; no mail provider is involved.
`SEED_DEMO_DATA` is on, so 2,019 readings across two accounts are already
there. Set it to `false` before the first start if you intend to hold real
data, and neither account is created.

Four files in [`demo/upload/`](../demo/) are deliberately NOT seeded, so
dropping one on the Data page walks the real path rather than doing nothing.

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
| A LOINC lookup raises on a fresh clone | `git lfs pull` has not run — the bundle is still a pointer stub. |
| `mirobody dev` exits asking for a Postgres | `--pg-url`, or `PG_URL` / `DATABASE_URL` in the environment. |
| The server starts but device sync never runs | Check the worker logs and Postgres connection; device pulls and task state both use Postgres. |
| `mirobody serve` cannot connect to Postgres | Set `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD` and `PG_DBNAME` for that database. |

## Next

[repository-layout.md](repository-layout.md) for the map ·
[pipeline.md](pipeline.md) for what happens to a reading ·
[walkthrough.md](walkthrough.md) for the four-minute tour of the running stack ·
[provider-setup.md](provider-setup.md) to turn on Garmin, Oura or Whoop.
