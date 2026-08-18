# Changelog

## 1.2.0 — unreleased

The first release in which `pip install mirobody` actually works, and the MCP
surface is on the current protocol. There are **breaking changes to the MCP tool
names**; see below.

### Upgrading from 1.0.x

Verified against a clean-venv install of the published `1.0.62` wheel: it
imports, and that is all it does. It ships **no CLI** (no console script, no
`main.py` in the wheel — nothing can start the server), **no
`mirobody.engine`**, and its resolver data files are 133-byte **Git-LFS
pointer stubs**, so no indicator ever resolved. There is therefore no working
pip-install workflow for this release to break — every pip-visible change is
"used to fail, now works".

Deployments running from a **git checkout of `main`** upgrade with three
things to know:

- The MCP tool renames below (`query_health_indicators` replaces the old
  pair) — the one real break, for MCP clients with hardcoded tool names.
- The web client's `/chat` and `/drive` URLs redirect to `/ask` and `/data`;
  bookmarks keep working.
- The database is forward-compatible by policy: schema files never
  `DROP COLUMN`/`DROP TABLE`, so an existing database keeps now-unused columns
  (e.g. `wechat_openid`) harmlessly.

### Breaking

- **`search_health_indicators` and `fetch_health_data` are removed**, replaced by
  a single `query_health_indicators`. The old pair forced every client into a
  minimum of two round-trips (search, read the name back, fetch), had no
  server-side aggregation, and dropped the LOINC code before the model saw it.

  ```jsonc
  // before — two calls, then compute the delta yourself from raw rows
  search_health_indicators({"keywords": ["LDL", "low-density lipoprotein"]})
  fetch_health_data({"indicators": ["LDL cholesterol"], "limit": 100})

  // after — one call, server-side stats, LOINC identity included
  query_health_indicators({"keywords": ["LDL"], "aggregate": "stats",
                           "start_time": "2026-01-01"})
  ```

  `query_health_indicators` also adds: `aggregate="stats"|"day"|"week"|"month"`
  computed in SQL over the whole window; a discovery mode (call with neither
  `keywords` nor `indicators` to get the user's indicator catalog, which is also
  what a no-match returns); pipe-table result compaction with constant columns
  factored out (~50% fewer characters than the old JSON); and a server-side
  ceiling on `limit`.

- **A user's saved prompt is now APPENDED to the agent's system prompt instead
  of replacing it.** Previously a user prompt whose name matched the request's
  `prompt_name` won outright and the shipped template was never consulted — so
  saving "answer in bullet points" silently discarded the whole `deep` prompt,
  the lab-report reading workflow and the no-diagnosis framing included, through
  a control that presents as a preference. The two are now composed, with the
  user's text under a header that states which half wins. Deployments that
  relied on full override must ship their own `PROMPTS_<AGENT>` template.

- **`GET /api/prompts` is scoped by agent** and takes `?agent=<name>` (default
  `deep`, so existing clients are unaffected). It used to hardcode
  `get_options_for_agent("deep")` and hand every caller Deep's list — including
  a caller on Base, for whom those instructions describe a virtual filesystem,
  QuickJS and chart tools that do not exist. An agent with no configured
  templates now returns an empty list, and the response echoes `agent` back.

### Added

- **`PROMPTS_BASE` does something.** The key shipped in `config.yaml` next to a
  working `PROMPTS_DEEP` while `BaseAgent` read its packaged `base.jinja`
  directly — a knob wired to nothing, which is worse than no knob. It now
  overrides the packaged template. BaseAgent has no per-request prompt
  selection (the provider runs the tool loop, so there is nowhere to branch on a
  name), so the first configured entry wins.

- **MCP 2026-07-28** — the stateless revision: per-request `_meta` protocol
  fields, `resultType` on every result, `server/discover`, deterministic
  `tools/list` ordering. Older revisions back to `2024-11-05` still negotiate.
- **Terminology over MCP** — `resolve_indicator` and `normalize_unit` expose
  ② Sort itself: any-language indicator name → LOINC, free-text unit → UCUM plus
  its comparability family. Both are offline, need no account, and read no user
  data.
- **Agent Skills** via deepagents' native `SkillsMiddleware`, mounted read-only
  at `/skills/`, with a shipped `lab-report-walkthrough` reference skill.
- `mirobody parse` / `mirobody resolve` CLI, and a resolver
  coverage benchmark (`mirobody/test_engine_coverage.py`) that is run in CI.
- **Reading correction over REST** — `GET /api/v1/health-indicators` (catalog /
  search / readings, the web Indicators tab's source) and
  `POST /api/v1/health-indicators/reading` to correct or soft-delete a single
  reading the caller owns. Extraction is an LLM reading a lab report; it
  mis-reads a value now and then, and the only fix used to be re-uploading the
  file. Readings now carry their `th_series_data` id and source `file_key`, so
  the UI can trace every number back to the original document.
- **Data-gated `tools/list`** — the personal MCP surface hides
  `get_genetic_data` when the account has no genotype rows and
  `query_health_indicators` when it has no health rows: a tool whose only
  possible answer is "no data" is schema every client pays for and none can
  use. Fails open per probe, so a database hiccup never shrinks a real user's
  tool surface.

### Fixed

- **A malformed tool call no longer ends the turn as an empty answer.**
  claude-sonnet (via OpenRouter, temperature 0.1) deterministically emitted
  `{"aggregate": none}` — Python's `None`, not JSON — LangChain parked the call
  in `invalid_tool_calls`, the react loop saw no valid calls and ENDED, and the
  user got "Answer Completed" over nothing, with no error anywhere.
  `InvalidToolCallRepairMiddleware` now answers each unparseable call with the
  parse error as a `ToolMessage` and jumps back to the model (two retries max).
- **Local-storage file URLs are relative.** `LocalStorage._build_url` hard-coded
  `http://localhost:18080/...` as its fallback, so on any deployment not on
  port 18080 every "view file" link pointed at a dead host. The frontend is
  same-origin with the server; `/files/<key>` works everywhere.
- **The upload gate matches what the parsers accept.** `SUPPORTED_EXTENSIONS`
  rejected every audio extension while `AudioHandler` sat unreachable behind
  it, and rejected `.md`/`.heic` outright. Audio, HEIC/HEIF and Markdown now
  pass, and `text/markdown` routes to the text handler.

### Changed — agent answers

- **`costStatistics` reports tokens only.** It used to multiply token counts by
  a hardcoded per-model price table; provider prices change faster than any
  table gets refreshed, so the dollar figures drifted into fiction while
  looking authoritative. Tokens are facts from the API; prices are not.
  Applies to BaseAgent too: its clients computed `total_cost` from
  `input_price`/`output_price` config keys that no longer exist, which had
  quietly become "always $0.00" — removed along with the keys.
- **The MCP tool descriptions lost a third of their weight** (get_genetic_data:
  half, and four of its eight parameters — filters that could only narrow an
  already-exact rsid match). Every behavioural rule survives; what left was
  restatement, and maintainer war stories that now live as code comments.

### Fixed — security

- **Cross-user credential leak in `resources/read`.** The widget cache was
  templated in place, so the first caller's JWT was baked into the shared
  resource and served to every later caller until restart.
- **Stored SQL injection in `get_genetic_data`.** rsids parsed out of an
  uploaded genotype file (no format validation) were interpolated into a query
  string on read.
- **Unauthenticated crash.** A JSON-RPC request without `id` raised `KeyError`
  out of the handler.
- **Credentials in logs.** A malformed request body printed every header,
  including `Authorization`, plus the whole body.
- **Care-circle sharing was broken** by a one-argument `isinstance()` that raised
  for every permission row that parsed correctly.
- Tool errors no longer return raw exception text to the caller.
- **Ages were reported one year too high** for anyone whose birthday had not
  yet occurred this year — the session profile subtracted birth year from
  current year, ignoring month and day, and fed that into the agent's context.
- The health profile injected into every DeepAgent turn failed **silently**;
  a failure there degrades every answer and left no log line to explain it.
- 21 `pulse/gate_tests` snapshots were stale rather than broken (they predate
  `metaInfo.windowFrom/windowTo`); re-recorded, and the suite is green.

### Fixed — packaging

- Wheels and sdists now contain the engine's data bundles. Previously the CI
  checkout omitted `lfs: true` and `package-data` omitted `*.gz`/`*.npy`/`*.tsv`,
  so published artifacts carried 133-byte LFS pointer stubs — `import mirobody`
  succeeded and every `resolve()` failed. A release gate now rejects both
  artifacts if the data is missing or stubbed.
- Declared the dependencies that were imported but never listed: `numpy`,
  `anthropic`, `python-multipart`. Removed ones that were listed but never
  imported: `setuptools`, `cachetools`, `defusedxml`, `openai-agents`.
- The web client moved out of the Python package to repo-root `frontend/` — the
  wheel no longer ships 8 MB of JavaScript.
- `utils/config` and `utils/db` no longer import the database stack at module
  scope. `mirobody.utils` sits on the import path of the whole engine, so a
  top-level `from sqlalchemy import text` made SQLAlchemy, psycopg and redis
  hard requirements of `resolve()` — which never opens a connection. Verified:
  the engine now imports and resolves with **numpy as the only third-party
  package installed**.

### Changed — MCP internals

- Tool input schemas are now generated by the **official MCP SDK**
  (`mcp>=2.0.0`, `func_metadata`) instead of a hand-rolled parser that matched
  *stringified* type annotations against a lookup table. That parser got four
  cases wrong, each verified: a bare `dict` became `"string"`; `Literal[...]`
  and `Enum` silently lost their constraint, so a model could send any string
  and the schema would not object; and `Annotated[int, Field(ge=…, le=…)]` was
  read as `"string"`, discarding both the type and the bounds. The hidden
  `user_info` argument is still hidden — the SDK's `skip_names` is the same
  mechanism — and `Args:` descriptions from docstrings are still merged in.
- `query_health_indicators` now declares `aggregate` as a `Literal` and `limit`
  as a bounded `Annotated[int, ...]`, so the constraints are enforced by the
  schema before any of our code runs.
- `tools/list`, `prompts/list` and `resources/list` carry 2026-07-28's `ttlMs` /
  `cacheScope`. Scope is `private`: the tool list is filtered per agent and
  resources are templated per request, so a shared cache must never serve one
  caller's copy to another. Verified against a recorded baseline of the previous
  behaviour — the other seven protocol cases are byte-identical.

### Changed — packaging layout

- **Tests no longer ship to PyPI.** 33 test modules and 21 recorded fixtures
  (~206 KB) were landing in every consumer's `site-packages`, importable as
  `mirobody.test_engine_coverage`. They stay in git — that file *is* the 98/98
  resolver score the README publishes, and CI runs all of them — but a custom
  build backend (`scripts/build_backend.py`) now prunes them from the wheel and
  sdist. `exclude-package-data` cannot express this: `test_*.py` files are
  package *code*, not data.
- **Schema DDL moved from `mirobody/res/sql/` to `mirobody/schema/`.** It only
  ever runs in dev (the bootstrap explicitly skips `TEST`/`GRAY`/`PROD`, which
  use a provisioned schema), and `mirobody/res/` is the licensed terminology
  data: `LICENSE-3RD-PARTY` describes everything there as derived from
  UMLS/SNOMED/LOINC, which our own DDL is not. A server with no `mirobody/schema/` directory
  now logs and continues rather than failing to start.
- Added `MANIFEST.in` so the sdist carries its own build backend. Without it
  `pip install mirobody-*.tar.gz` failed at `backend-path entry 'scripts' does
  not exist` — before any project code ran. CI now installs from the sdist as
  well as the wheel, because no wheel test can catch that.

### Changed — documentation and tests

- **`mirobody/pulse/CLAUDE.md` was shipping to PyPI.** An internal working-notes
  file, committed and packaged inside the wheel, under a filename that tells
  open-source readers to skip it. Its content was real module documentation and
  is now `mirobody/pulse/README.md`; `.gitignore` blocks the name at any depth.
- One convention, applied: **a package carries a short `README.md` saying what
  it is; long-form guides live in [`docs/`](docs/)** and stay out of the wheel.
  `FILE_PROCESSING_GUIDE.md`, `INTEGRATION_GUIDE.md`, `DESIGN.md`,
  `TEST_README.md` and a 1,960-line provider guide moved there and were renamed
  for the task they describe. In-package documentation went from 6,467 to 2,943
  lines, and every file in the package is now `README.md`.
- **The testing docs described a suite that did not exist.** The README told
  contributors to run `pytest tests/ -m mcp`; there is no `tests/` directory and
  those markers were never defined, so the command collected zero tests while
  appearing to pass. `testpaths` is now set — bare `pytest` runs all 105 — and
  the two markers that exist (`needs_db`, `needs_llm`) are declared. See
  [docs/testing.md](docs/testing.md).

### Removed

- ~1,100 lines of verified-dead code, each confirmed unreferenced by a
  repo-wide search including dynamic and string references: `chat/history.py`
  (whose function names shadowed the live `session.py`), `chat/mcp_loader.py`,
  `pulse/core/data_quality_service.py`, three `message.py` functions plus the
  `MessageType` class only they used, eight request helpers in
  `server/routers/middleware.py` that duplicated `server/middlewares.py`, and
  `mcp.read_global_resource` — which could only ever return `None`, because the
  `global_resources` dict it read was never assigned.

### Changed

- Resolver coverage on everyday panels went from 32/94 to 98/98, measured. Panel
  names (`blood pressure`, `lipid panel`) now resolve to *nothing* rather than to
  one arbitrary component — `blood pressure` used to return the diastolic code.
- The agent layer is one package: `mirobody/agent/` (was `mirobody/pub/` plus a
  top-level `mirobody/chat/`). HTTP routers moved to `mirobody/server/routers/`.
