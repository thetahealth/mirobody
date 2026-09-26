# Quickstart

Three ways in. They are not steps — pick the row that matches what you want,
and ignore the other two.

| You want | Go to | Needs | Key |
| --- | --- | --- | --- |
| Names and units resolved in your own code | [A · the library](#a--the-library) | Python 3.12 | none |
| The whole product running, with data in it | [B · the stack](#b--the-stack) | Docker | one |
| To change the code and see it | [C · a checkout](#c--a-checkout) | Python + a Postgres | one |

The hosted [Quickstart](https://docs.mirobody.ai/en/quickstart/) covers the same
ground with screenshots. This page is the version that ships with the code, so
it cannot drift from the commands in this repository.

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

Two extras exist for the rest: `pip install 'mirobody[parse]'` to turn a PDF,
photo or spreadsheet into readings (that one calls a model, so it needs a key),
and `pip install 'mirobody[app]'` for the server. Neither is needed for the
above.

## B · the stack

Postgres + pgvector, Redis, the server and the worker, with the demo record
already seeded.

```bash
git clone --depth 1 https://github.com/thetahealth/mirobody.git && cd mirobody
git lfs install && git lfs pull   # the LOINC bundle, 13 MB; a fresh clone holds a pointer stub
./deploy.sh                       # → http://localhost:18060
```

`deploy.sh` writes a `.env` on first run and generates the secrets in it. Put
**one** model key in that file — any of the six works, and `mirobody doctor`
tells you which surfaces it lights up:

```bash
mirobody doctor
```

Sign in as `you@mirobody.ai` with code `111111`; no mail provider is involved.
`SEED_DEMO_DATA` is on, so 2,019 readings across two accounts are already
there. Set it to `false` before the first start if you intend to hold real
data, and neither account is created.

Four files in [`demo/upload/`](../demo/) are deliberately NOT seeded, so
dropping one on the Data page walks the real path rather than doing nothing.

## C · a checkout

`serve` is the deployment shape: it reads `config.{ENV}.yaml`, expects Redis,
and expects the secrets to exist. `dev` is the same server in one process with
none of that — no config file, generated secrets, Redis optional:

```bash
pip install -e '.[app]'
mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody
```

No Postgres at hand:

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=mirobody \
    pgvector/pgvector:pg17
```

pgvector, not plain postgres — the schema creates a vector column. The schema
itself is created on first start.

**`dev` serves the API, not the web client.** The built client lives at
repo-root `frontend/` and is outside the package, so it is there in a checkout
and absent from a `pip install`. What `dev` is for is the API, the MCP surface
and the agent; for the UI, use B.

The secrets `dev` generates are per-run: sessions and encrypted config values
do not survive a restart. Set `JWT_KEY`, `CONFIG_ENCRYPTION_KEY` and
`LOG_ENCRYPTION_KEY` in the environment to keep them.

## When it does not come up

| What you see | What it is |
| --- | --- |
| `keys present : none` from `mirobody doctor` | No model key. ① Collect and ② Translate still work; extraction and answers do not. |
| A LOINC lookup raises on a fresh clone | `git lfs pull` has not run — the bundle is still a pointer stub. |
| `mirobody dev` exits asking for a Postgres | `--pg-url`, or `PG_URL` / `DATABASE_URL` in the environment. |
| The server starts but device sync never runs | Redis is unreachable. Everything else degrades cleanly; the vendor pull does not. |
| `mirobody serve` finds no configuration | `config.yaml` is not in the wheel. That is what `dev` is for. |

## Next

[repository-layout.md](repository-layout.md) for the map ·
[pipeline.md](pipeline.md) for what happens to a reading ·
[walkthrough.md](walkthrough.md) for the four-minute tour of the running stack ·
[provider-setup.md](provider-setup.md) to turn on Garmin, Oura or Whoop.
