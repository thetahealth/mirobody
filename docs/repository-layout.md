# Repository layout

**English** · **[中文](repository-layout.zh-CN.md)**

Where each of the three stages — ① Collect, ② Translate (standardize), ③ Agent — lives,
and the two forms the code ships in. Moved here from the README in 1.4.1; the
README keeps the one-line map.


```
mirobody/
├── engine/      the front door — resolve(), resolve_reading(), parse_file()
├── units/       UCUM units, unit_family, conversions          ┐ the library:
├── lexical.py   surface folding + the CJK-aware tokenizer     │ numpy only,
├── bundle.py    build-time: the axis table and alias sources    │
├── res/         the shipped LOINC bundles, catalog/metrics.tsv  │
├── kernel/      what health data MEANS, as pure functions:       │
│                metrics · series · quality · overlay · meds ·    │
│                query · tools · ops · connect · sink · events ·  │
│                evidence · memory · decoders/                     ┘ 2 packages
├── documents/   a file becomes text, by kind — PDF text layer, OCR for scanned pages only, Office, text   [parse]
├── collect/       ① Collect     — providers, file parsing, store, read (Postgres)
├── translate/   ② Translate   — the pure seam (fold · parse · local_day · series · code ·
│                devices), the catalogue, units, aggregate/ and derive/
├── agent/       ③ Agent               — the agent: models/ fs/ wire/ middleware/ tools/ chat/
├── mcp/         the MCP server
├── server/      the HTTP application — routers, auth, the bundled web client
├── utils/       mechanisms a consumer binds: config, db, sse, net, llm_output, prompts, log
├── user/        identity and the care circle — who may read whose record
└── schema/      the DDL, replayed at boot in dev

demo/            the care-circle demo fixture, beside frontend/ — a checkout
frontend/        the bundled web client                          has them, a
                                                                 pip install
                                                                 does not
```

**Two forms, and they want opposite things.** The PyPI package is a LIBRARY and
is meant to be small enough that nobody has to think about it: `pip install
mirobody` is **2 packages, 67 MB** — the entries above the `documents/` line, on numpy.
`[parse]` adds document reading, `[agent]` the harness as a library; `[app]` is everything, and the only thing that
installs it is `requirements.txt`, because the Docker application is
`git clone && ./deploy.sh` and never a pip install.

**Machine-enforced, not documented:** four import-linter contracts hold the
lines — the library layer imports nothing but numpy, and the engine never
imports the agent layer — and `lint-imports` fails the build. A separate gate, `scripts/check_wheel_data.py`, keeps the bundle-build passes and the
v2 semantic pipeline — 19,000 lines nobody who installs the package can run —
out of the artifact.

→ [Architecture](https://docs.mirobody.ai/en/concepts/architecture/) ·
[CONTRIBUTING.md](../CONTRIBUTING.md)

---

