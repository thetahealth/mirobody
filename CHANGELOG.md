# Changelog

## 1.4.1

Consolidation. Nothing here changes an answer: the 7,354-case resolver
evaluation is bit-identical to 1.4.0 (coverage 0.9631, wrong-rate 0.0322).

### Fixed

- **A credential slice was in the logs, and the PHI baseline was hiding it.**
  `providers/platform/database_service.py` logged the first 20 characters of a
  stored AES-GCM ciphertext on an `InvalidTag`. `phi_baseline.txt` carried the
  line as one long f-string rather than as the slice it interpolated, so
  nothing pointed at it; enabling `ISC` turned the explicit `+` concatenation
  into an implicit one and the linter finally saw the expression. It is a
  `secret_fingerprint` now — the same digest handle the OAuth paths use, which
  answers "is this the same stored value?" and carries none of it.
  `phi_lint` learned that `secret_fingerprint(...)` is safe, which made five
  OTHER call sites' baseline entries stale: they had been using it correctly all
  along and the baseline was grandfathering them as violations. Baseline
  661 → 653.
- **`Config.init` silently dropped an in-memory config.** `Config.__init__`
  has always accepted an `io.StringIO`, but `expand_yaml_filenames` skipped
  every non-string entry and `Config.init` then filtered on `os.path.exists`.
  A caller passing an overlay got the shipped defaults with nothing logged —
  found by `mirobody dev` coming up on `pg 127.0.0.1:5432` while printing the
  port it had been asked for. A readable stream now passes through; `None` and
  other garbage are still dropped.
- **`<PROVIDER>_MODEL` / `<PROVIDER>_VISION_MODEL` did not work uniformly.**
  `config.yaml` documents them as one scheme, and for two providers it was
  false: the vision table called Alibaba `qwen` and Volcengine `doubao` while
  every other table used the `LLMProvider` names, and each derived its override
  key from its own spelling — so structured extraction read `DASHSCOPE_MODEL`
  and vision read `QWEN_VISION_MODEL`. One table now names them
  (`config/llm._PROVIDER_DEFAULTS`), the canonical name is the enum value
  everywhere, and both old spellings still resolve for `provider=` and for the
  config key.
- **`.pre-commit-config.yaml` was gitignored.** `*.*.yaml` is there for the
  per-deployment `config.{env}.yaml` overlays that carry secrets; it also
  matched the one config file every contributor is supposed to install.
- **A personal MCP URL could not be taken back.** `POST /personal/mcp` mints
  `{origin}/mcp/{secret}`, and that secret is the whole credential —
  `/mcp/{secret}` needs no JWT and grants read access to that person's health
  record. It is a bearer token carried in a URL, so it ends up in desktop-client
  config files, screenshots and shell history. It was stored with a **hardcoded
  365-day** expiry and there was no revoke path; re-minting could not rotate it
  either, because the mint path returns the stored secret. `DELETE
  /personal/mcp` revokes one (idempotent, and it deletes the
  secret → user mapping first so the credential stops working even if the
  second delete fails), the next mint issues a fresh secret, and the TTL is
  `MCP_URL_TTL_DAYS` with a 30-day default. Mint and revoke share one
  authorization path so the two cannot drift on who may act on whose record.
  (`framework-optimization-proposal` P0-C item 3, filed as a security-audit
  leftover.)
- **The shipped web bundle is rebuilt, and the model picker had four bogus
  "Agent" tabs.** `GET /api/models` has answered with bare provider names since
  1.4.0, and the web client still split each one on `/` into
  `(agent, provider)` — so `"gpt"` parsed as `agent: "gpt"`, four models became
  four "agents", and the dropdown rendered a segmented control of four tabs
  with one model under each, captioning a tab labelled `gpt` with "Deep runs
  the tool loop here — virtual filesystem, QuickJS, charts". Measured by
  executing the old parse against the live response, not inferred — and it was
  SHIPPED, not latent: the parse is in the previous artifact's main chunk
  (`index-DqMMF6sB.js`, 1.37 MB) verbatim, and the segmented control is in the
  model-picker chunk (`index-DocncKpd.js`), legacy build included.

  Fixed in the client repo and rebuilt into `frontend/`: the model id IS
  the provider name, the agent tabs and their styles are gone,
  `getModelShowName` takes one argument, the compare pane key drops to
  `provider`, and `POST /api/chat` / `POST /api/rating` stop sending `agent`
  and `group_id`. Bindings for `/api/agents`, `/api/providers` and
  `/api/user/prompt*` — all removed in 1.4.0, all defined and never called,
  which is why nothing broke and nobody noticed — are deleted, and six i18n
  keys with no code reference go from all four languages. A stored model id
  from before this keeps its provider half rather than resetting the user's
  choice.

  `ChatStreamRequest` still ACCEPTS `agent`, `enable_mcp`, `group_id` and
  `reference_task_id` for one more release: a browser holding a cached older
  bundle still sends two of them, and `chat_handler` rejects unknown fields, so
  dropping them now would answer every message from such a client with -4.
- **The Indicators tab listed nothing, against a healthy endpoint (#62).**
  `c470b3d` reshaped `GET /api/v1/health-indicators` around one envelope on
  2026-09-07; the shipped bundle was built on 09-04 and read `catalog` /
  `indicators`, keys the route has never sent. It rendered "No indicators yet"
  over 35 indicators. Three reads were wrong, not the one the report found: the
  list, the readings drawer (`indicators[0].readings`, now `rows`), and the
  per-reading **edit and delete buttons — a reading's key is `row_id` and the
  client looked for `id`, so owner-only correction had quietly disappeared from
  every row**. The client repo now pins each field mapping against fixtures
  copied from this repo's `_catalog_row` / `_reading_row`, so the next shape
  change fails a test rather than a page. A contract test on this side is still
  worth having.

  Two notes for anyone changing that route again. It answers in two grains from
  one path — catalog without `keywords`/`indicators`, readings with them — and
  a search that matches nothing answers with the CATALOG, so a client cannot
  infer the grain from its own request. And `truncated` arrives `true` with
  `rows == total`, so it is not on its own a statement that anything was left
  out.
- **`POST /invitation/shared-by-me/list` failed 100% of calls.** It returned
  `ok([...])`, and `StandardResponse.data` is `dict[str, Any]` — pydantic
  refused the list, the handler's own `except Exception` caught the refusal, and
  the route answered `{"code": -1, "msg": "Could not list your circle."}`,
  which reads like a database fault. Nobody noticed because on an empty circle
  the failure is indistinguishable from success: the web client merges this
  list into "people I can ask for", so members an owner created never appeared
  in that selector and one generic line went to the log. It answers
  `{"members": [...]}` now — the shape `shared-with-me/list` next door already
  uses — and the bundle in this release reads it. Changing the wire shape cost
  nothing: a route that always fails has no working consumer.
  `tests/test_response_envelopes.py` grew an `ast` walk for the class of
  mistake, since it can never crash loudly.
- **A route whose only possible answer was 503.**
  `POST /vital/generate-sign-in-token` calls
  `platform_manager.get_platform("vital")`, and the installed providers are
  Garmin, Oura, WHOOP and pgsql — there is no `vital` platform in this
  repository, and no `vital_client` outside that one reference. Neither the
  shipped web bundle nor the frontend source calls it. Removed with the dead
  `VitalHealthRecord` model; `StandardPulseRecord` keeps the field set (rows in
  `th_series_data` were written against it) and now says why in its own words
  instead of pointing at a class that is gone.
- 36 `raise` statements inside `except` blocks lost their cause
  (`raise ... from`), so a traceback stopped at the re-raise. One loop-bound
  closure (`indicator/fhir/adapter.py`) now binds its arrays explicitly.

### Added

- **`mirobody dev`** — the server in one process, with no config file, no Redis
  requirement and dev secrets generated per run:

      pip install 'mirobody[app]'
      mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody

  `config.yaml` is not in the wheel, so this is the only shape in which a plain
  `pip install` can start something. Redis was already optional
  (`RedisConfig.get_async_client` returns None when it cannot ping and the
  server logs "local memory mode"); `dev` stops treating that as a blocker.
  The generated `JWT_KEY` / `CONFIG_ENCRYPTION_KEY` / `LOG_ENCRYPTION_KEY` go
  into the ENVIRONMENT, because `Config.__init__` builds its encrypter before
  it reads any YAML — a value in config could never satisfy them.
  `examples/05_agent_server_preflight.py` now reports which of its
  prerequisites `dev` produces for you: six missing becomes two.
- **A pre-commit config** with the two gates that are sub-second — `ruff` and
  `phi_lint` — deliberately not the whole battery, because a slow hook gets
  bypassed. `phi_lint.DEFAULT_TREES` is now the one place the baseline's tree
  list lives; the test and the hook both read it, because scanning a wider tree
  than the baseline covers reports hundreds of pre-existing lines as new.
- **`mcp.reset_global_tools()`** — the tool registry is an object with a
  `reset()`, not two module dicts that could only grow. A test can now load a
  directory, assert, and start clean instead of inheriting every tool a
  previous test published.

### Changed

- **The ruff rule set grows by six**, each catching a class of defect rather
  than a style: `B` (bugbear), `ISC`, `C4`, `PIE`, `PLE`, `RUF100`. `PLE0604`
  is excluded (it cannot see through a dynamically built `__all__`), and the
  reasons `G004`, `DTZ` and `TID252` are NOT selected are written down —
  `DTZ` in particular would ask us to break the naive-local-time storage
  contract to satisfy a linter. CI lints `examples` as well as `mirobody`.
- **`CacheableDatabaseService` is no longer an `ABC`.** After 1.4.0 folded the
  three-level hierarchy it declared no abstract method, so `ABC` told a reader
  to look for a contract that was not there.
- `STRUCTURED_OUTPUT_PRIORITY` moved out of a function body to module scope. It
  was rebuilt on every call and no test could see it.

### Not done, and why

- **The three response envelopes stay three.** Converging them is a WIRE
  change with two different clients on the other side: the shipped web bundle
  reads `success` off the chat and user endpoints, and `apple_router`'s three
  POST endpoints are the mobile client's ingest path. Both need a coordinated
  client release. `tests/test_response_envelopes.py` pins which layer speaks
  which shape so a fourth cannot appear by accident.
- **The four soft-delete predicates stay four.** They are four different
  COLUMNS on eighteen different tables with zero overlap — no table carries two
  — so "converging" means renaming columns across fourteen tables including
  `health_app_user`, `th_files` and `th_series_data`, in a project that replays
  DDL at every boot with no ledger. All 91 SQL blocks agree with their table
  today, so there is no defect to fix and a real data-visibility failure mode
  to risk. A test pins the agreement instead.
- Recorded while measuring the above: **a passkey, once registered, cannot be
  deleted.** `webauthn_credentials` declares `is_del`, `deleted_at` and
  `deleted_by`; the first is filtered on but never set and the other two are
  never touched, because `webauthn.py` exposes no revoke path. Adding one is
  security-sensitive design, not consolidation.

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
