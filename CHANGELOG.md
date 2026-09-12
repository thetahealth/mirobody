# Changelog

## Unreleased

### Added

- **`skills/dont-guess-my-labs/` — the resolver as a skill for someone else's
  agent.** Claude Code, Codex and OpenClaw read a `SKILL.md` from their own
  directory; until now the only skill in the tree was
  `mirobody/agent/skills/lab-report-walkthrough`, which cannot leave this
  project — it reads `/uploads/` and `/library/` through the shipped agent's
  virtual filesystem and emits `vis-chart` blocks, none of which exist in
  another harness. The new one needs `pip install mirobody` and nothing else:
  no key, no network, no Docker, no clone. It teaches an agent to resolve every
  indicator name on a report *before* explaining it, to pass the unit (`中性粒细胞
  62 %` is `26511-6`, `4.2 10*9/L` is `26499-4` — same name, two tests), and to
  report an unresolved name as unresolved.

  It lives at the repo root, outside `[tool.setuptools.packages.find]`, so the
  "2 packages" install is unchanged.

- **`tests/test_skills.py` — 68 assertions over that skill's prose.** Every
  LOINC code, refusal and term it prints, checked against the resolver, plus a
  gate that fails on a code quoted in the prose that no test derives. The
  reference tables are quoted in three files and derived in none, which is the
  same reasoning `test_readme_numbers.py` applies to the READMEs' figures.

  Four of those assertions pin answers that are **wrong today** — `维生素`
  ("vitamins") resolves to `96450-2`, a liver-cancer risk score; `尿常规`,
  `肿瘤标志物` and `电解质` land on a collection method, one specific marker and
  a 24-hour urine narrative. They fail when the resolver is fixed, which is how
  the prose describing them gets updated in the same commit.

### Fixed

- Nothing in the engine. The defects above are recorded, not repaired.

## 1.4.1

One key, every surface — decided in YAML, not in Python; and the failures that
used to be silent, said out loud. No answer changes: the resolver evaluation is
untouched.

### Breaking

- **`<PREFIX>_MODEL`, `<PREFIX>_VISION_MODEL` and `<PREFIX>_EMBEDDING_MODEL` are
  no longer read.** They chose a model per KEY, which presupposed a model table
  in Python to override. A model belongs to a `MODELS` entry now, and a
  surface's choice to `UTILS_VISION_MODEL` / `UTILS_TEXT_MODEL` /
  `UTILS_EMBEDDING_MODEL`. Setting one gets a WARNING naming where its value
  goes. `<PREFIX>_BASE_URL` stays, and now reaches chat entries too.
- **`PROVIDERS` is `MODELS`, `DEFAULT_PROVIDER` is `DEFAULT_MODEL`,
  `EMBEDDING_PROVIDER` is `UTILS_EMBEDDING_MODEL`.** In this project a
  *provider* is a device (`PROVIDER_DIRS`); the model table had borrowed the
  word. Old spellings are aliased at load time with one WARNING. `EMBEDDING_MODELS`
  is gone — an embedding model is a `MODELS` entry tagged with the vector-column
  family it writes, and a family name still resolves (`UTILS_EMBEDDING_MODEL: qwen`).
- **The Gemini SDK path is gone from the EXTRACTION surfaces**, which use the
  OpenAI-compatible endpoint like every other vendor; PDFs are rendered page by
  page there, since no compatibility endpoint takes a PDF part. The embedding
  entry stays on the REST API, the only one that takes `output_dimensionality`.
  `AIConfig`, `VisionProviderConfig`, `gemini_file_extract` and the
  doubao/zhipu/moonshot tier go with it (no caller, an SDK never installed).
  Chat entries may still be `google-genai` / `anthropic` / `google_anthropic_vertex`,
  and the shipped `gemini-flash` and `claude` are — each for a reason under Fixed.
- **`resolve_embedding_provider()` answers `""` with no usable key** rather than
  `"openrouter"`; the vector-column resolvers raise the sentence that fixes it
  instead of a 401 from a gateway nobody chose.
- **The `mirobody_pgsql` pull provider is removed.** It copied readings from a
  second PostgreSQL — a migration tool for one deployment, shipped as a device.
- **Firebase login is removed.** The seven `FIREBASE_*` settings, the web-config
  keys and the `/__/` helper routes are gone. Google and Apple sign-in stay,
  verified server-side against `GOOGLE_CLIENT_ID` and the `APPLE_*` keys. A web
  client still getting its tokens through Firebase shows neither button until it
  moves to Google Identity Services / Sign in with Apple JS.
- **Audio uploads are no longer accepted.** The handler was a stub — speech-to-text
  was never wired, so every audio file stored bytes and produced an empty summary.
  `tinytag` leaves `[parse]`. Stored objects still serve.

### Fixed

- **A `DEEPSEEK_API_KEY`-only deployment uploaded reports into silence**
  ([#68](https://github.com/thetahealth/mirobody/issues/68)). Five Python tables
  decided which provider a surface used; one knew the key and four did not, and
  whether a provider could read an image was recorded nowhere.

  Every such decision is in [`config.llm.yaml`](config.llm.yaml) now: `MODELS` is
  one table of entries (`llm_type`, `api_key` NAME, `base_url`, `model`,
  `supports_image`/`supports_pdf`, `response_format`, `reasoning_effort`, `chat`,
  `embedding`, `extra_body`), the picker lists entries whose key is present with
  the first as default, and the three `UTILS_*_MODEL` keys name what each surface
  may use — a list, so any ONE of six keys runs everything. Utility entries
  (`*-utils`, `chat: false`) are multimodal with thinking off: a text-only model
  on the vision surface is exactly #68. Python holds no model name;
  `test_one_key_defaults.py` pins the table to the entries below it.

- **Six keys now, and the three that only looked like they worked.**
  `ANTHROPIC_API_KEY` joins as the sixth. Measured against each live vendor:

  * `OPENROUTER_API_KEY`, the recommended key, could not read a report —
    `reasoning: {enabled: false}` answers `400 Reasoning is mandatory for this
    endpoint`. Now `{effort: minimal, exclude: true}`.
  * `OPENAI_API_KEY` could not call a tool — `gpt-5.6-terra` needs
    `reasoning_effort: none`, and with reasoning on it also rejects `temperature`.
  * `GEMINI_API_KEY` left the picker empty while `doctor` called chat healthy:
    the router knew the vendor's key aliases and the agent's resolver did not.

- **Anthropic uses the vendor's own API, not the compatibility layer.** There
  `response_format: {"type": "json_object"}` is REFUSED (400, "Input should be
  'json_schema'" — the compatibility page says it is ignored, and it is not), and
  a schema is taken only in OpenAI strict mode, which our extraction schemas do
  not carry. The native API constrains decoding with `output_config.format`, so
  the answer IS the document. An entry's `response_format`
  (`json_schema`|`json_object`|`none`) replaces the old boolean, because "what
  this endpoint takes" turned out to be three answers.

- **Every PDF uploaded through the web client extracted nothing, on every key.**
  The WebSocket upload buffers chunks into a `bytearray` and pypdfium2 refuses
  it, so the text layer came back empty and every model call after it was handed
  an empty string — while the upload reported success. `extract_text` normalises
  at the door, the one place every document kind passes through.

- **What an entry declares is now what gets sent.** `openai-utils` declared
  `reasoning_effort: none` and `RouteSpec` had no such field, so an
  `OPENAI_API_KEY`-only deployment extracted zero indicators. `unread_entry_keys()`
  now names any entry key no code path consumes, at boot — the class, not the
  instance.

- **Anthropic extraction returned nothing on a fresh install.** `anthropic` 1.x
  removed `temperature`/`top_p`/`top_k` and the floor was uncapped, so every call
  raised `TypeError`. The backend reads what the installed SDK's signature
  accepts and drops the rest. Behind it: the blocking call refuses a `max_tokens`
  large enough for a long report (claude-haiku-4-5 — 20000 passes, 32000 raises),
  and extraction asks for 32000, so every request streams. Floors are
  `anthropic>=1.5` and `langchain-google-genai>=4.4.0`, uncapped.

- **A `GEMINI_API_KEY` agent could not finish one tool call.** Gemini 3.x attaches
  a `thought_signature` to every function call and requires it back verbatim; the
  compatibility endpoint cannot carry it, and no switch on that path recovers it.
  `gemini-flash` is `llm_type: google-genai` — the one chat entry that is not
  OpenAI-compatible. Extraction is unaffected: one call, no second turn.

- **A PDF attached to a question 400'd, on the default chat model of the
  recommended key.** `read_file` delivers it inside a `ToolMessage`, and whether
  that is accepted is a property of the TRANSPORT: OpenRouter answers `400 tool
  messages must include a non-empty string tool_call_id`, OpenAI `400 Invalid
  value: 'file'` — both for models that genuinely read PDFs over their own vendor
  API. The transport decides now, and a profile may only narrow, never widen:
  that field has three origins and two describe the model. On an
  OpenAI-compatible endpoint a PDF is served as extracted text, which works.

- **Zero indicators now says why.** Four states rendered identically: no provider
  configured; a provider that failed every call; an extraction that raised; a
  document with genuinely none. The first three now mark the file failed with the
  sentence that fixes it — the third used to be written `complete` with
  `error: ""`, under a placeholder abstract reading "uploaded successfully".
  Underneath, the vision path caught every exception and returned `""`, so a
  `400 This model does not support image` looked exactly like a blank page.
  Selection happens once; a call that then fails is a failure, not a reason to
  try the next entry. Server and worker log one WARNING per surface without a
  provider, and an ERROR when there is none. The files API exposes the row's
  `error`, which it had withheld while the fix sat in the database.

- **The indicator sync sweep died without an embedding key.** `embed()` raises on
  a misconfigured `UTILS_EMBEDDING_MODEL` — correct for a direct caller — and the
  sweep let it reach the consumer loop, so an Anthropic- or DeepSeek-only
  deployment saw a traceback per signal instead of the documented fallback. The
  step is skipped with one warning and the sweep finishes.

- **The agent told users to re-upload a report it could see.** The prompt said
  `/uploads/` holds "the file I just uploaded", but that mount is only the
  current message's attachments; a Data-page upload lands in `/library/`.

- **An entry's thinking configuration was overwritten**
  ([#70](https://github.com/thetahealth/mirobody/issues/70)).
  `_vertex_anthropic_kwargs` assigned `model_kwargs["thinking"]` unconditionally,
  so an entry declaring the adaptive shape for Claude Opus 4.7+ had it replaced
  by a budget shape those models reject. On the OpenAI path an entry carrying a
  `reasoning` dict also received `reasoning_effort`, which `Responses.create()`
  rejects before any request. What an entry declares now stands.

- **The Indicators tab listed nothing, against a healthy endpoint**
  ([#62](https://github.com/thetahealth/mirobody/issues/62)). The shipped bundle
  predated the route's reshape and read keys it never sends. Three reads were
  wrong, not the one reported — the list, the readings drawer, and the
  **per-reading edit and delete buttons**, whose key is `row_id` where the client
  looked for `id`, so owner-only correction had disappeared from every row.
  Client-side only. `test_indicator_contract.py` pins the key sets here.

- **`./deploy.sh` picked a Docker mirror by asking the wrong process.** The probe
  was a `curl` from the shell, which has `http_proxy`; the daemon that pulls does
  not inherit it, and the probe hit the website rather than the registry. It pulls
  the smallest real image through the daemon now. Separately, `ENV` and the two
  encryption keys were written only into a `.env` the script created itself —
  dropping in one that holds just an API key left every boot missing them.

- **Two gates that were missing, both key-free and on every PR.**
  `test_upload_smoke.py` puts a real PDF through the real upload path;
  `test_file_block_transport.py` pins the transport rule in both directions.
  Three defects above were invisible for a release because nothing did this.

### Changed

- **The bundled web client is half its size**, 7.0 MB in 59 files to 3.7 MB in 42.
  The Vite legacy plugin emitted a second transpiled copy of every bundle for
  browsers that could not render the app anyway: it transpiles JavaScript only,
  and the stylesheet needs `@property`, `color-mix()`, `@layer` and `oklch()` —
  Tailwind v4's floor of Chrome 111 / Safari 16.4 / Firefox 128. Those targets
  were getting a working script and an unstyled page. Nothing changes for a
  browser that runs the app today; the floor is written down in `browserslist`
  and [`docs/frontend.md`](docs/frontend.md).

- **A `git clone` is half the size.** Deleting code does not shrink a repository —
  every blob stays in the pack. What a clone pays is 29 MB of git objects plus
  69 MB of Git LFS, and the LFS half is the slow one. Three LFS files no runtime
  reads (`scripts/check_wheel_data.py` has forbidden all three since 1.3.0) are
  release assets now, listed with checksums in
  [`mirobody/res/EXTERNAL.tsv`](mirobody/res/EXTERNAL.tsv) and fetched by
  `scripts/fetch_data.sh`, which `./deploy.sh` and CI call. This works without
  rewriting history, because git-lfs only fetches the checked-out commit's
  objects: with `--depth 1` a clone goes from **~98 MB to ~48 MB**. Semantic
  indicator search degrades to the lexical index when the concept graph is
  absent, exactly as it already did on a `pip install`.

- **The configuration is split by concern.** `config.yaml` keeps what the
  containers wire; `config.llm.yaml` is the file to open; `config.devices.yaml`
  holds Garmin/Oura/Whoop and the mobile sign-in block. An `INCLUDE` list at the
  top of `config.yaml` names them, and they load before your `config.{ENV}.yaml`.
  Gone with the move: `FRONTIERX_*`, `JINA_API_KEY`, a duplicate
  `DASHSCOPE_API_KEY`, and the `*_API_KEY: ""` lines that read as "put the key
  here" when the key goes in `.env`.

- **`.env` is where the key goes, and the boot log answers whether it worked.**
  `./deploy.sh` writes the key names into `.env` and no longer lists them in the
  generated overlay — there were two places to put a key, and #68's reporter
  chose the other. The boot banner ends with the table `mirobody doctor` prints:
  what each surface selected, and the fix where one has nothing.

- Model ids verified against each vendor's live catalogue on 2026-09-10:
  `gemini-3.8-flash`, `gpt-5.6-terra`, `anthropic/claude-sonnet-5`,
  `qwen3.8-flash`, `deepseek-flash`, `claude-haiku-4-5`. Network wording names
  the condition (openrouter.ai unreachable), never a region.

- **A dead-code sweep**: 52 files, +245 / −1,978 lines. Everything with zero
  callers here, downstream and in the tests — `utils/s3.py`, `utils/truncate.py`
  and with it the `tiktoken` dependency, `utils/data.py`, the sync
  PostgreSQL/Redis paths, the storage backends' `get_file_info`, the scheduler's
  unwired stop/status methods, and a dozen pulse service methods.

## 1.4.0

**One framework, three installs.** The pure rules live in `mirobody.kernel`;
the agent harness, the document reader and the utilities are libraries a
consumer imports; the server here is one consumer of them. A reading's day is
decided once, at write time; three surfaces read through one tool per data
class.

### Breaking

- **`query_health_indicators` keeps its name, loses five parameters, and
  medications get `query_medications`.** One tool per data class, every
  parameter applicable to every call: readings
  (`keywords | indicators, start, end, resolution, aggregate, limit, member` —
  eight), medications (`view=plan|log|history, keywords, start, end, member` —
  five), genetics. A draft of this release had folded medications into the
  readings tool behind a `kind` switch: seven of thirteen parameters were then
  invalid for one kind and every misuse cost a refusal round-trip, while the
  one promised benefit — both classes in one call — was never real, because
  `kind` was single-valued. Parameter changes to the readings tool:
  `start_time`/`end_time` → `start`/`end`; `aggregate="day|week|month"` →
  `resolution=` (and `stats` honours it: at `day` it averages DAYS); new
  `aggregate="latest"` and `member`; gone: `panel` (a panel is its member
  names), `exclude` (the catalogue is the second round), `order` (a baseline
  is `aggregate=stats`: `first`/`first_date`), `source` (rows carry theirs).
  The schema is `kernel.query.TOOL_SCHEMA` published verbatim to the agent and
  to MCP; an unknown parameter is a structured refusal by name. `tools/list`
  hides each data tool from a user who has none of that data.
  `GET /api/v1/health-indicators` gains `resolution` and narrows `aggregate`
  to `none|stats|latest`.
- **A comma no longer splits a list element.** `["1,25-Dihydroxyvitamin D"]`
  is one name; a bare string the model composed (`"LDL, HDL"`) still splits.
- **Import paths.** The pure modules are `mirobody.kernel.*` (`metrics`,
  `series`, `quality`, `overlay`, `meds`, `query`, `tools`, `ops`, `connect`,
  `sink`, `events`, `evidence`, `memory`, `vendors/`); `from mirobody import
  series` is `from mirobody.kernel import series`. The vocabulary layer
  (`engine`, `units`, `lexical`) stays where it was.
- **Agent config keys lose `_DEEP`:** `PROVIDERS`, `PROMPTS`, `ALLOWED_TOOLS`,
  `DISALLOWED_TOOLS`, `DEFAULT_PROVIDER`. **The old spelling still works** —
  it is renamed onto the new one as each config file merges, and says so once
  in the log. The alias is deliberate rather than a shim: the failure without
  it was silent, because the shipped `config.yaml` declares four of these
  itself, so a 1.3.x overlay carrying only `PROVIDERS_DEEP` was shadowed by
  the default and the agent booted with zero providers and an empty
  `/api/models`, with nothing to point at. Renaming at LOAD time — not as a
  read-time fallback, which the shipped default would have pre-empted — keeps
  ordinary layering: a later file's old spelling still overrides an earlier
  file's new one, and the environment variables alias the same way.
  `PRIVATE_AGENT_DIRS` and `MCP_RESOURCE_DIRS` are gone, and so are
  `HEARTBEAT_INTERVAL` / `HEARTBEAT_COUNTER_THRESHOLD`; those four are NOT
  aliased (the heartbeat's replacement has different semantics, and the
  directory keys have no successor) but a config that still carries one is
  named in the log instead of ignored in silence.
- **`/api/models` returns bare provider names**, `/api/prompts` returns
  `{"system": [...]}`, the chat request's `agent` field is ignored. Removed:
  `/api/agents`, `/api/providers`, `/api/user/prompt*`, `/api/user/mcp*`. The
  shipped web client calls none of them; `POST /personal/mcp` and `/mcp` are
  unchanged.
- **Provider contract.** `Provider.format_data(fmt_input)` is the one method
  (`.context` + `.payload`; `format_data_v2` gone); `_validate_credentials(
  credentials)` is the one credential hook, and `POST /{platform}/token` now
  actually calls it. `format_data` no longer returns medications.
- **The chat heartbeat:** `HEARTBEAT_INTERVAL` × `HEARTBEAT_COUNTER_THRESHOLD`
  (first ping after 40 s) is replaced by `SSE_HEARTBEAT_SECONDS` (default 8,
  `0` disables), fired on silence.
- **`eval/run_eval.py` is `benchmarks/run_eval.py`.** Tests moved from the
  package to `tests/` at the repo root, mirroring it; the wheel build fails if
  a test file reappears inside the package.
- **Where things live.** `mirobody/agent/` was 24 flat modules; the ones that
  answer one question together are now packages — `agent/models/` (build a chat
  model, read its messages, count its tokens), `agent/filesystem/` (the virtual
  filesystem: the backends, naming, upload-time preparation), `agent/wire/`
  (LangGraph's stream → what a client renders). `attachments.py` folded into
  `prompt.py` (both are text the harness writes for the model), `filetype.py`
  into `filesystem/naming.py`, and `cache_config.py` is gone — four of its five constants
  had no reader. The `HealthQuery` implementation left `agent/tools/`, where
  every other file is a tool the MCP loader publishes, for `mirobody/pulse/
  query.py`, beside `readings.py`, the one WRITER of the table it reads.
- **The artifact carries only what an install can run.** Five modules were
  shipping that raise on import from a wheel: `indicator/main.py` (it imports
  the two pruned build subtrees), `fhir/bridge.py` and `fhir/siblings.py`
  (polars, which only `[indicator-build]` provides), and `fhir/adapter.py` +
  `fhir/inspect.py` (they imported the pruned `embeddings/`). The last one
  mattered at runtime: `pulse/query.py` reaches `FhirAdapter` for semantic
  recall inside a broad `except`, so on a pip install the semantic tier failed
  at import and fell back to lexical without saying so. Fixed by moving the
  READER out of the build tree — `fhir/embeddings/local.py` is
  `fhir/index.py`, the same split `mirobody/_bundle.py` records for the
  tarball and stopped one module short — and by pruning the build CLI and its
  passes from the wheel and sdist. The indicator package now ships nine
  files, all of them importable, instead of twenty of which five were not.
  Also pruned: `fhir/loinc_lookups.py` (98 KB of generated LOINC Part tables
  that nothing imports) and the loose `res/analyte_digit_src/` build input.
  Dead code removed with it: `common.MappingRow`, `FhirAdapter._resolve_local`,
  and `resolve/axis.axis_rerank_bonus` with its weight table — a documented
  soft per-axis bonus that `git log -S` shows never had a caller in its
  history, while the adapter applies the hard-argmax bonus inline.
- **One parser for a bundle member that is a code list.** `loinc_skip.txt` was
  parsed in three places — the resolver's skip set, the semantic tier's, and
  the embedding index's mask builder — and the third did not drop `#` comment
  lines, so a commented entry would have reached `code_to_int` and raised.
  The member carries no comments today, which is why nothing caught it.
  `_bundle.read_code_list` is the one reader; each caller keeps its own shape.
- **The vocabulary build CLI is `scripts/vocabulary_build.py`.** It was
  `python -m mirobody.indicator`, which made the entry point of a build
  toolchain into library code — it shipped in every wheel while importing the
  subtrees the wheel prunes, so the documented command raised
  `ModuleNotFoundError` on any published install. It joins the three sibling
  passes already at the repo root, for the reason `benchmarks/run_eval.py`
  moved there earlier in this release. Each subcommand now imports its own pass
  on dispatch: `--help` and the offline `normalize` run without the build
  dependencies, and a missing one is reported against the pass that needs it
  (`siblings: needs polars`) instead of failing at start-up. Verified by
  installing the wheel into a clean virtualenv and running the README's
  documented commands against it.
- **Documentation moved to where its rule says it goes.** The second half of
  `mirobody/indicator/README.md` was a 446-line manual for rebuilding the
  terminology bundles from raw UMLS / SNOMED CT / LOINC / RxNorm releases —
  contributor documentation shipping to every install, against this
  repository's own convention that a package README is short and long-form
  guides live in `docs/`. It is [docs/vocabulary-build.md](docs/vocabulary-build.md).
  The section documenting `mirobody/units/` went to that package, which had no
  README, and the architecture block was three releases stale: it listed
  `lexical.py`, `zh_fold.py`, `value_scale.py` and `fhir/units/` inside the
  package, all of which moved to the package root in 1.3.0.
  `mirobody/indicator/requirements.txt` is gone — a second, incomplete
  declaration of the `indicator-build` extra that nothing read.
- **Authentication is `mirobody/user/auth/`.** Seven modules and about three
  thousand lines of sign-in mechanics (tokens, emailed codes, Apple, Google,
  Firebase, passkeys, the OAuth flow) sat beside the identity records under a
  package whose one-line description was "identity and the care circle".
  `user_service.py` is the seam that picks a validator and lands an account.
- **The demo fixture left the package.** `care_circle_demo.json.gz` and the lab
  report it asks you to upload are the repo-root `demo/`, beside `frontend/`
  and for the same reason: the application is `git clone && ./deploy.sh`, so
  230 KB of synthetic data was dead weight in every LIBRARY install.
  `mirobody/demo/__init__.py` is `mirobody/server/demo.py`, next to the
  bootstrap that calls it; `DEMO_DATA_DIR` overrides the location and a
  deployment with no fixture logs and starts anyway.

### Added

- **Three installs.** `[parse]` reads documents; `[agent]` is the harness as a
  library — middleware, virtual-filesystem backends, Postgres checkpointer,
  model-client factory, `hitl`, prompt renderer, the tool surface — with no
  HTTP server; `[app]` is both plus the server. `mirobody.utils` resolves its
  re-exports lazily (PEP 562), so a leaf helper imports alone.
- **`mirobody.kernel`**, with `__init__` drawing the stage → module map.
  New in it: `meds` (plans, scheduled doses by `(plan, local_date, slot)`,
  taken/skipped events, adherence, a small dose grammar, FHIR readers, and the
  `query_medications` contract with its three pure row projections; storage
  and terminology are ports), `overlay` (a correction is a layer applied at
  read time, never a rewrite), `vendors` (Garmin/WHOOP/Oura decoders as pure
  functions with shipped samples; `open_wearables` with a 93-row crosswalk),
  `connect`/`sink`/`ops`/`tools`/`query`/`events`/`evidence`/`memory`
  (contracts with the reference server as first consumer). Five
  body-composition metrics join the catalogue: 305 members.
- **`mirobody.documents`.** `detect.kind` by extension, content type and
  bytes; `extract.extract_text` by kind — PDF text layer page by page with
  ONLY scanned pages OCR'd (injected `Ocr`, concurrent), images downscaled
  then OCR'd, workbooks as markdown under a row budget, Word/PowerPoint as
  markdown, text through the encodings reports are saved in. `pipeline.py` is
  the two-phase upload (store, answer, then extract → sidecar → mine → status
  `pending|ok|empty_text|partial|failed`) with every product decision a
  `Ports` port. `engine.parse_file` extracts text first, so a born-digital
  report needs a text model key only.
- **`mirobody.agent` as a library:** `harness.assemble` (both halves of the
  no-subagent configuration, resolved for THIS model instance; read-only
  mounts; the middleware stack in order), `events_bridge` (LangGraph stream →
  `kernel.events`; every parallel tool result is reported, reasoning reaches
  `thinking`), `clients.build_chat_model` for every provider family
  (OpenAI-compatible with reasoning capture, Azure WIF, Claude on Vertex with
  cache breakpoint and thinking budget, Gemini on Vertex or AI Studio),
  `messages`, `usage.UsageAccumulator` (`costStatistics` gains
  `thought_tokens`, drops `cache_creation_tokens`), `document_backend`, `filesystem.naming`.
- **`mirobody.utils`:** `sse` (silence-triggered keepalive, `sse_headers`),
  `net` (`assert_public_url` judges every resolved address and every redirect
  hop; `fetch_bounded`), `llm_output` (sentinel and wrapper stripping, tolerant
  JSON), `prompts` (one `StrictUndefined` Jinja environment),
  `db.use_engines` (engine by injection), `log.user_tag` (keyed pseudonym).
- **`mirobody.testing`:** `phi_lint` (log statements carry ids, counts and
  type names only, with a shrink-only baseline), `prompts.lint_prompt`,
  `samples`, `contracts.check_sink`, `golden`, `coverage` (what a connector
  carries, generated from its decode table).
- **Plugins by `pip install`:** entry points `mirobody.providers`,
  `mirobody.tools`, `mirobody.agents` beside the directory keys; example in
  `examples/mirobody_example_plugin/`.
- **The day columns** (`schema/a4_series_data_day_authority.sql`):
  `local_date`, `series_key`, `source_class`, `fingerprint`, `elected` derived
  at write time through the metric's own window; `pulse/backfill.py` fills
  history at boot; an unfilled row reports `window_semantics` instead of
  pretending. **Write-side election** (`pulse/aggregate/election.py`): measurer
  > profile echo > coverage > measurement instant > deployment priority.
- **Medication storage** (`schema/a5_medications.sql`, `pulse/meds/`); Apple
  clinical records import medications. Provisional; see `docs/medications.md`.
- **Retry governance:** a call whose `(tool, normalised arguments)` already
  failed unrecoverably is refused; a per-turn cap removes the data tool rather
  than ending the turn.
- **The pull loop backs off:** three authorization failures expire a
  credential, each costs a cooldown.
- **`<PROVIDER>_MODEL`** and **`<PROVIDER>_EMBEDDING_MODEL`** per provider;
  `<PROVIDER>_BASE_URL` now reaches vision and structured extraction too
  ([#52](https://github.com/thetahealth/mirobody/issues/52)).
- **Readings know where their date came from**
  ([#53](https://github.com/thetahealth/mirobody/issues/53)): `report_date`,
  `date_source`, `date_confirmed` on file rows; `POST
  /api/v1/health-indicators/file-date` re-files one file's readings; the date
  is probed first and pushed as `report_date_detected` on the upload socket;
  in chat the agent asks with the new `ask_user` interrupt (agent-only, not on
  MCP).
- **Backup and restore documented** (`docs/backup-restore.md`, `shell/backup.sh`).
  Docs: `docs/pipeline.md`, `docs/answers.md`, `docs/medications.md`, README
  acknowledgements in four languages.

### Changed

- **Ten dependencies gone, about 470 MB less in a fresh install:**
  `langchain_community`, `pypdf`, `rarfile`, `py7zr` (declared, never
  imported); `langchain-google-vertexai` + `google-cloud-aiplatform` (171 MB;
  `init_chat_model` names the package a deployment configuring Vertex must
  install); `langchain-anthropic` (deepagents requires it); `mutagen`,
  `httpx`, `pytz`, `pdfplumber` (a second library for a job one already does);
  `pandas` (68 MB for two functions — `openpyxl` reads `.xlsx` directly).
- **One writer for `th_series_data`** (`pulse/readings.py:upsert_readings`,
  with the four collision policies named) and **the quality gate runs on every
  row**: impossible spans, future starts, non-finite numbers and percentages
  outside 0–100 are rejected, counted by reason code.
- **`kernel/query.compact`** is the one table renderer (columns inferred,
  containers summarised, 500-character cells, `empty=`).
- **The system prompt renders strictly:** a missing variable is the error the
  client sees, not a raw template sent to the model.
- **`evidence` and `memory` returned to the kernel** for the first consumer's
  insight detector and profile watermark (`mirobody:rewritten-at=`).
- **`/mirobody.json` emits every flag the web client reads** — mobile sources,
  `MCP` in new features, API config derived from what is installed; before,
  the device-provider UI was invisible on every deployment.
- **One response envelope** (`server/envelope.py`), **module loggers
  everywhere** (no emoji), and **a ruff gate** in CI with a narrow rule set.

### Fixed

- **Health data reached the logs:** the MCP handler logged the first 100
  characters of every result; the chat path logged tool arguments and results
  and streamed raw driver exceptions (which quote bound parameters). Logs now
  carry ids, counts and type names; clients get `client_safe_error`;
  `PHIPolicy` is on the root logger and `phi_baseline.txt` can only shrink.
- **`LIKE '%sleep%'` decided a reading's day:** it re-anchored 58 vendor-dated
  daily metrics by a day and missed `napDuration`. The catalogue's `window`
  decides. Note: the next aggregation writes those 58 metrics a day later than
  the last one did.
- **`vo2Maxs` declared `L/min/kg`** (unparseable, 1000× out); it is
  `mL/min/kg`. Stored values were always mL/min/kg.
- **A FHIR resource id was a cross-person medication plan id;** two people
  importing from one clinic collided. `plan_id_for(subject, record, concept)`.
  A dosage carrying only `Dosage.text` produced no schedule; it is parsed.
- **WHOOP calories ×4.184 instead of ÷; zone minutes ÷ 1/60000 instead of
  ÷ 60000.** Garmin sleep stages decoded to names no catalogue entry matched.
- **The chat model saw `Any | None` for every tool argument;** the MCP
  `inputSchema` is passed through, so `aggregate: none` fails validation. A
  tool taking `**kwargs` had every argument dropped. Three helper methods of a
  tool class were published as MCP tools (`__tools__` allow-list).
- **`eval` could not reach the data** (no `ptc`); the read-only tool is
  `tools.query_health_indicators` inside the REPL, with a per-eval budget. The
  prompt named two tools that no longer existed and a limit the runtime did
  not enforce; `tests/test_prompts.py` fails the build on either.
- **A day query padded its window a day either side;** it is an equality on
  `local_date`. Keyword recall ran the vector search first; the lexical rank
  over the person's own catalogue runs first. A window the caller did not ask
  for was reported as if the rows came from it.
- **A `tools` node running several calls in parallel reported only the last
  result.** `apple_router` sent `cdaData` as a list while `format_data`
  expected a dict; both shapes read.
- **The Volcengine tier had never run** (vendor SDK not a base dependency); it
  uses the OpenAI-compatible client like every other provider. An
  `ANTHROPIC_API_KEY`-only deployment got silence from the utility helpers;
  `claude` left the auto-selection list.
- **`GET /api/data` without a filter returned 500** (an untyped NULL bind).
- **CI's full suite had been the minimal suite** — both workflows installed a
  removed extra; it is `'.[app,test]'`.

### Removed

- **The ChatGPT Apps widgets and the MCP `resources` capability**
  (`agent/resources/`, `mcp/resource.py`, `MCP_RESOURCE_DIRS`): no tool ever
  pointed at them, and `resources/read` templated the caller's JWT into HTML.
- **Per-user prompt additions and the per-user MCP-server store**
  (`user_agent_prompt`, `user_mcp_config`): nothing in the client wrote them,
  nothing read them. The system prompt has one source, `PROMPTS`.
- **`BaseAgent` and the provider-native loops** (~1,500 lines, off by
  default); `DeepAgent` is `MirobodyAgent`, `agent/deep/` and `agent/utils/`
  are folded into `agent/`, `chat/agent.py` is `agent/registry.py` (the first
  class found is THE agent; `AGENT_DIRS` replaces, never adds),
  `chat/user_profile.py` is `user/profile.py`, `deep.jinja` is
  `mirobody.jinja`, the persona is `AGENT_NAME`.
- **`volcengine-python-sdk[ark]`** (245 MB for one call `openai` makes) and
  the dead LLM-config surface around it.
- **Dead code from the Vital era and the v1/v2 migration:** `pulse/core/
  constants.py` and `models.py` types nothing imported, the three-level
  database-service hierarchy, a 140-line magic-byte sniffer, the
  `pull_from_vendor_api_optimized` hook no provider defined.

## 1.3.0

**`pip install mirobody` is a library:** 2 packages and 52 MB instead of 93
and 245 MB. Document parsing is `mirobody[parse]`; the server is
`mirobody[app]`. `[agents]`, `[server]` and `[cn]` are removed — all three
map to `[app]`.

### Changed

- Base dependencies are `["numpy"]`. `mcp>=2.0.0` in base had made the
  package uninstallable beside `langchain-mcp-adapters` (`mcp<2`); it moved to
  `[app]`.
- `mirobody parse` checks for its extra up front and prints the pip command.
- The vocabulary layer (`units`, `lexical`, `value_scale`, `zh_fold`) moved
  from `indicator.fhir.*` to the package root.

### Added

- `mirobody.bundle` — the stable build-time surface over the LOINC axis
  table and alias sources (`load_axis`, `read_member`, `bundle_version`, …).
- `py.typed`; a declared public surface (`from mirobody import resolve,
  resolve_reading, parse_file, Resolution, Reading`, lazily);
  `mirobody.BUNDLE_VERSION` (`loinc-2.82+<date>-<digest>`) with a CI stamp
  check; `eval/run_eval.py` ships with `--testset`.

### Fixed

- **A percentage and a count are two analytes.** `中性粒细胞` at `62 %` is
  26511-6, at `4.2 10*9/L` is 26499-4; all five white-cell lines in four
  spellings, and the inline unit is read out of the value.
- `Hemoglobin A1c` answered 41995-2 while `HbA1c` answered 4548-4.
- A trailing acronym that REPEATS the stem is stripped (`总胆固醇 TC`); one
  that NARROWS it is not (`Protein CSF` abstains).
- `resolved=True, loinc=""` is unreachable: a deprecated corpus row no longer
  wins over the LOINC-bearing row behind it (+3.4 points coverage).
- A switched variant reported the pre-switch name; `呼吸频率` did not resolve;
  `eGFR` answered a heart-failure marker; a curated 乙肝e抗原 row pointed at
  nothing; `resolve()` now honours `loinc_skip.txt`.
- **The resolver holds 70% less and loads 3× faster** (514 → 156 MB, 1.09 →
  0.28 s cold, 0.76 → 0.07 ms per call): byte blobs plus int32 offsets
  instead of a pickled object array and CSV text. Identical on all 7,354 eval
  cases.
- The semantic tier's safety gates cannot silently vanish; one copy of the
  alias tables; the runtime no longer imports the build tooling; `boto3` is
  declared.

### Removed

- 17 MB of bundle members and 19,000 lines (`indicator/fhir/embeddings/`,
  `indicator/fhir/resolve/`) that no install can read or run — pruned from
  the wheel, kept in git, guarded by `scripts/check_wheel_data.py`.

## 1.2.2

### Added

- `indicator.fhir.units.pick_display_unit` — the unit a merged series
  displays in (most frequent, ties to the latest).

### Fixed

- `parse_value_unit` glued `240 10⁹/L` into `240109/L`; the author's
  whitespace boundary is respected.
- The pulse conversion tables drifted from the UCUM engine (lb, glucose molar
  mass); they derive from it now.
- An attachment-only chat turn was refused as empty; it becomes a stand-in
  question in the request's language (#39, #41).
- `/uploads/` paths moved under the model mid-turn as the descriptive
  filename arrived; the turn's attachments keep the names they were attached
  under (#40, #42).

## 1.2.1 — released 2026-08-23

The first release in which `pip install mirobody` works: 1.0.62 shipped no
CLI, no `mirobody.engine`, and LFS pointer stubs for its data. Version jumps
1.0.62 → 1.2.1. Checkout deployments: the MCP tool rename below is the one
real break; `/chat` and `/drive` redirect to `/ask` and `/data`; schema files
never drop columns.

### Breaking

- `search_health_indicators` + `fetch_health_data` → one
  `query_health_indicators` with server-side `aggregate`, a discovery mode,
  compact pipe-table results and a `limit` ceiling.
- A user's saved prompt is APPENDED to the system prompt instead of replacing
  it. `GET /api/prompts` takes `?agent=`.

### Added

- **One key runs everything:** `OPENROUTER_API_KEY` or `DASHSCOPE_API_KEY`
  drives chat, vision and embeddings; `<PROVIDER>_BASE_URL` for self-hosted
  endpoints; `PRODUCTION: true` refuses placeholders and demo codes;
  `BOOTSTRAP_SCHEMA: false` turns off DDL replay.
- Every demo account owns a thin record beside the shared synthetic one.
- A zero-key deployment marks uploads *failed* with the reason; `/api/models`
  lists only providers whose key resolves.
- MCP 2026-07-28 (stateless revision); `resolve_indicator` and
  `normalize_unit` over MCP; Agent Skills at `/skills/`; `mirobody parse` /
  `resolve` CLI; the resolver coverage benchmark in CI.
- `GET /api/v1/health-indicators` and `POST .../reading` (correct or
  soft-delete one reading); data-gated `tools/list`.
- `POST /api/standardize`, `POST`/`GET`/`DELETE /api/data` — shaped like the
  hosted API, without `/v1`, tenancy fields or an implicit delete-all;
  `engine.parse_text`.

### Fixed

- **Resolution.** NFKC-lite folding and a CJK tokenizer (`LDL–C`,
  `fasting_glucose`); `名称(缩写)` resolves and refuses when the halves
  disagree (0% → 100% on 6,641 terms); device vocabulary (`steps`, `SpO2`,
  `静息心率`, …); the abbreviation column (`HGB` had answered HbA1c, `HCT`
  calcitonin; `CA`/`PT`/`MG` are refused). Coverage 32/94 → 211/211.
- **Security.** Deleting a document did not stop the agent reading it (the
  workspace copy and the profile summary are withdrawn inline); cross-user JWT
  leak in `resources/read`; stored SQL injection in `get_genetic_data`;
  `KeyError` on a JSON-RPC request without `id`; credentials in error logs;
  care-circle sharing broken by a one-argument `isinstance()`; raw exception
  text to callers; ages one year too high.
- A malformed tool call ended the turn as an empty answer;
  `InvalidToolCallRepairMiddleware` bounces it back to the model.
- Local-storage file URLs hard-coded `localhost:18080`; the upload gate
  rejected audio, HEIC and Markdown its handlers accepted.
- **Packaging.** Wheels carry the data bundles (LFS in CI, `package-data`);
  undeclared `numpy`/`anthropic`/`python-multipart` declared; the web client
  left the wheel; `utils/config` and `utils/db` no longer import the database
  stack at module scope, so the engine runs with numpy alone; tests no longer
  ship; DDL moved to `mirobody/schema/`; `MANIFEST.in` so the sdist installs.

### Changed

- `costStatistics` reports tokens only; the dollar table is gone.
- MCP schemas come from the official SDK (`func_metadata`) — `dict`,
  `Literal`, `Enum` and `Annotated` bounds had all been read as `"string"`.
- Stage ② is `Standardize`, stage ③ is `Answers`. The agent layer is one
  package (`mirobody/agent/`); routers are `mirobody/server/routers/`.
- `pulse/CLAUDE.md` was shipping to PyPI; it is `pulse/README.md`. Long-form
  guides moved to `docs/`; bare `pytest` runs the whole suite.

### Removed

- The `/charts` mount, the remote-config fetch (`CONFIG_SERVER`), dead config
  keys (`FILE_ANALYSIS_PROVIDER`, `VITAL_*`, `RENPHO_*`), and ~1,100 lines of
  verified-dead code.
