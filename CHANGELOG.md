# Changelog

## 1.4.1

One key, every surface — decided in YAML, not in Python; and the failures that
used to be silent, said out loud. Nothing here changes an answer: the resolver
evaluation is untouched.

### Breaking

- **`<PREFIX>_MODEL`, `<PREFIX>_VISION_MODEL` and `<PREFIX>_EMBEDDING_MODEL`
  are no longer read** (`OPENROUTER_MODEL`, `GEMINI_VISION_MODEL`,
  `QWEN_VISION_MODEL`, `OPENROUTER_EMBEDDING_MODEL`, …). They chose a model per
  KEY, which presupposed a model table in Python for them to override. A model
  belongs to a `MODELS` entry now, and a surface's choice to
  `UTILS_VISION_MODEL` / `UTILS_TEXT_MODEL` / `UTILS_EMBEDDING_MODEL` in
  `config.llm.yaml`. A deployment that still sets one gets a WARNING at boot
  naming the key and where its value goes; the value itself is ignored.
  `<PREFIX>_BASE_URL` stays, and now reaches the chat entries too.
- **The Gemini SDK path is gone from the extraction surfaces.** Gemini is an
  OpenAI-compatible entry (`https://generativelanguage.googleapis.com/v1beta/openai/`)
  like every other vendor; PDFs are read page by page there as everywhere, since
  no compatibility endpoint takes a PDF part (Google's answers `Invalid content
  part type: file`, measured 2026-09-10). Its embedding entry stays on the REST
  API (`llm_type: google-genai`), which is the only one of the two that takes
  `output_dimensionality`. `gemini_file_extract`,
  `AIConfig`, `VisionProviderConfig`, `client_manager.get_async_*_client` and
  the `doubao_file_extract` / `async_get_doubao_structured_output` pair are
  removed; the Doubao/Volcengine tier, and the `zhipu` / `moonshot` enum
  members, with them (no verified default, no caller, an SDK declared in `[app]`
  and never installed). Chat entries may still use `llm_type: google-genai` /
  `anthropic` / `google_anthropic_vertex` through LangChain.
- **`PROVIDERS` is `MODELS`, `DEFAULT_PROVIDER` is `DEFAULT_MODEL`,
  `EMBEDDING_PROVIDER` is `UTILS_EMBEDDING_MODEL`.** In this project a
  *provider* is a device or data source (`PROVIDER_DIRS`,
  `mirobody/pulse/providers`); the model table had borrowed the word. The old
  spellings are read as the new ones as each file loads, with one WARNING, the
  way the `_DEEP` keys have been since 1.4.0 — so an overlay written for 1.4.0
  still boots; rename it anyway. `EMBEDDING_MODELS` (a name → id table) is
  gone: an embedding model is a `MODELS` entry like any other, marked with the
  vector-column family it writes (`embedding: openrouter | qwen | gemini |
  openai`), and `UTILS_EMBEDDING_MODEL` lists entries. A family name is still
  accepted there (`UTILS_EMBEDDING_MODEL: qwen`, the value 1.4.0 overlays
  carry) and resolves to the entry that writes it.
- **`resolve_embedding_provider()` answers `""` with no usable key** rather
  than `"openrouter"`; `text_embedding()` and the vector-column resolvers raise
  the sentence that fixes it instead of a 401 from a gateway the deployment
  never chose. `EMBEDDING_MODEL_IDS` is gone; the id is the entry's `model`.
- **The `mirobody_pgsql` pull provider is removed** (`PROVIDER_DIRS` no longer
  finds it). It copied readings from a second PostgreSQL into this one — a
  migration tool for one deployment, shipped as if it were a device, and the
  only reader of `DATABASE_DECRYPTION_KEY` outside the OAuth providers.
- **Firebase login is removed.** The seven `FIREBASE_*` settings, the
  `__FIREBASE_*__` web-config keys, `/__/auth/init.json`,
  `/__/firebase/init.json` and the `frontend/__/` helper routes are gone, and
  `/google/verify` accepts Google ID tokens only. Google and Apple sign-in
  stay, verified against `GOOGLE_CLIENT_ID` and the `APPLE_*` keys — the
  provider's own token, checked server-side, with no Firebase project in
  between, which is how the open-source peers do it. A web client that still
  obtains its tokens through Firebase shows neither button (its Firebase config
  is no longer served) until it moves to Google Identity Services / Sign in
  with Apple JS.
- **Audio uploads are no longer accepted** (`.wav .mp3 .aiff .aac .ogg .flac
  .m4a`, `audio/*`). The handler was a stub: speech-to-text was never wired,
  so every audio file stored bytes and produced an empty summary, and the only
  thing the chain computed was a `duration` field nothing read. `tinytag`
  leaves the `[parse]` extra with it. Objects already stored keep their
  `audio/*` content type and still serve.

### Fixed

- **A `DEEPSEEK_API_KEY`-only deployment uploaded reports into silence**
  ([#68](https://github.com/thetahealth/mirobody/issues/68)). `config.yaml`
  declared the key. Of the five Python tables that decided which provider a
  surface used, one — the endpoint table — knew it and four did not, so
  vision, structured extraction, titles and chat each selected nothing; the
  upload reported success; the Indicators tab said none. Whether a provider
  could read an image was recorded nowhere — it was implied by membership in
  the vision list, so "add a provider" and "declare what it can do" were the
  same act.

  Every one of those decisions is in `config.llm.yaml` now, where a user can
  read and change it: `MODELS` is one table of entries (alias → `llm_type`,
  `api_key` NAME, `base_url`, `model`, `supports_image`, `supports_pdf`,
  `response_format`, `chat`, `embedding`, `extra_body`), the chat picker lists the
  entries whose key is present with the FIRST as default, and
  `UTILS_VISION_MODEL`, `UTILS_TEXT_MODEL` and `UTILS_EMBEDDING_MODEL` each
  name the entries their
  surface may use — a list, so any ONE of the six keys still runs everything
  with zero further configuration; a single name, a `provider/model` string or
  an inline spec, to pin one. These are the keys mirovital's config-server
  already uses (`MODEL_PROVIDERS`, `UTILS_*_MODEL`), so a spec written for one
  reads in the other. Python holds no model name any more;
  `mirobody/test_one_key_defaults.py` pins the table at the top of the file to
  the entries below it and each key alone to every surface.

  The utility surfaces get their own entries (`openrouter-utils`,
  `qwen-utils`, `gemini-utils`, `openai-utils`, `anthropic-utils`,
  `deepseek-utils`; `chat: false`, so they never reach the picker): every one a multimodal model with
  thinking off, because report photos and scanned pages are images and
  extraction needs no reasoning trace — a text-only model in
  `UTILS_VISION_MODEL` is exactly #68, and the entry's `supports_image: true`
  is now what admits it. DeepSeek is `deepseek-flash` (DeepSeek-V4.1-Flash,
  released 2026-09-10 with native image input; the earlier V4 ids are retired
  upstream and routed to it). Measured against the live
  endpoint: it reads a report image; its JSON mode takes `json_object` and
  refuses `json_schema` ("This response_format type is unavailable now"), so the
  entry says `response_format: json_object` and extraction writes the schema into
  the prompt; thinking is on by default there and ignores temperature, so the
  utility entry turns it off. DeepSeek serves no embedding model, so semantic
  search on such a deployment answers from the lexical index, and the boot log
  and `mirobody doctor` say so instead of borrowing another gateway's name.

  Three smaller holes in the same set, closed with it: an `OPENAI_API_KEY`-only
  deployment had no chat entry (`gpt` reads the OpenRouter key) and no
  embedding path (`text-embedding-3-small` at 1024 dimensions, its own
  columns); `GEMINI_API_KEY` — the name Google's own docs use — was accepted on
  every surface except titles and summaries; and the agent's `MODELS`
  entries ignored `<PREFIX>_BASE_URL`, so pointing the whole stack at one
  gateway meant editing YAML for chat while every extraction path followed
  the variable. `tests/utils/llm/test_registry_drift.py` checks the entries
  against models.dev and OpenRouter's catalogue (skipped offline, and for an
  id a vendor released before the catalogues list it).

- **An `ANTHROPIC_API_KEY` runs the whole project too, and it is the sixth
  key.** `claude` (claude-sonnet-5) chats, `anthropic-utils`
  (claude-haiku-4-5) reads report photos and extracts indicators; Anthropic
  serves no embedding model, so semantic search falls back to the lexical
  index exactly as it does for DeepSeek, and `mirobody doctor` says so.

  Both entries are `llm_type: anthropic` — the vendor's own API, not the
  OpenAI-compatible layer this project speaks everywhere else — and the reason
  is measured, not stylistic. On that layer `response_format:
  {"type": "json_object"}` is REFUSED (400, "Input should be 'json_schema'";
  the compatibility page says it is ignored, and it is not), and a schema is
  accepted only in OpenAI strict mode — `strict: true` plus
  `additionalProperties: false` on every object, which the extraction schemas
  do not carry. What is left there is asking for JSON in the prompt and hoping,
  which is issue #68 with extra steps. The native API has
  `output_config.format`: decoding is constrained to the schema, so the answer
  IS the document. `anthropic.transform_schema` adapts our schemas to what the
  grammar compiler takes; the shipped indicator schema — nested objects,
  enums, arrays — passes unmodified through it. The new
  `utils/llm/backends_anthropic.py` holds the three surfaces (structured, text,
  vision), rendering PDFs page by page and merging them exactly as the
  OpenAI-compatible backend does, so only the request shape differs.
  `llm_type: anthropic` and the `anthropic/<model>` route shorthand both mean
  that path; the chat entry keeps prompt caching and the thinking channel,
  which the compatibility layer does not carry either.

  An entry's `response_format` (`json_schema` | `json_object` | `none`)
  replaces the `json_schema: false` boolean, because "what this endpoint takes"
  turned out to be three answers rather than two. And a model told to answer in
  JSON by the PROMPT wraps it in a ```json fence — the vision path had always
  stripped that, the structured path handed it straight to `json.loads`.

- **Three one-key paths were broken, and only running them found it.** Each is
  configuration, measured against the live vendor on 2026-09-10:

  * `OPENROUTER_API_KEY` — the recommended key — could not read a report or
    extract an indicator. `openrouter-utils` asked for `reasoning: {enabled:
    false}` and the endpoint behind `google/gemini-3.8-flash` answers 400,
    "Reasoning is mandatory for this endpoint and cannot be disabled". It is
    `{effort: minimal, exclude: true}` now: the least that endpoint allows,
    with no trace returned.
  * `OPENAI_API_KEY` — the agent could not call a single tool. `gpt-5.6-terra`
    on `/v1/chat/completions` answers "Function tools with reasoning_effort are
    not supported … set reasoning_effort to 'none'", and with reasoning on it
    also rejects the entry's `temperature: 0.1`. Both entries declare
    `reasoning_effort: none`, and an entry that declares one now keeps it — a
    thinking hint from the UI used to overwrite it, which would have undone the
    fix at the first request.
  * `GEMINI_API_KEY` alone left the chat picker empty while `mirobody doctor`
    reported the chat surface healthy. The route layer knows the vendor's
    aliases (`read_api_key`); the agent's `default_resolver` read the raw
    environment, so it built a `_PlaceholderClient` for an entry naming
    `GOOGLE_API_KEY`. One key must not get two answers.

- **Zero indicators now says why.** Three states rendered identically as "no
  indicators": no provider configured; a provider that failed every call; a
  document with none. The first two now fail the upload, or mark the file
  failed, with the sentence that fixes it — which key to set and where to get
  one, or which entry to point `UTILS_VISION_MODEL` at when the selected model
  cannot read images. Underneath, the OpenAI-compatible vision path caught
  every exception and returned `""`, so a provider's `400 This model does not
  support image` looked exactly like a blank page; a scan whose every page
  failed OCR came back as `""` too. Both are errors now (a single failed page
  inside a multi-page PDF is still a warning). Selection happens once; a call
  that then fails is a failure, not a reason to try the next entry — two
  reports of one person must not be read by two different models. The server
  and the worker log one WARNING per surface without a provider at boot, and
  an ERROR when there is none at all — a zero-key server used to boot in
  silence. `mirobody doctor` prints the same table on demand; it needs no
  database and no extra, and exits 1 when nothing at all is usable. The files
  API exposes the row's `error` alongside `upload_status`, which it had
  withheld "for backward compatibility" while the fix sat in the database.

- **An entry's thinking configuration was overwritten**
  ([#70](https://github.com/thetahealth/mirobody/issues/70)).
  `_vertex_anthropic_kwargs` assigned `model_kwargs["thinking"]`
  unconditionally, so an entry declaring the adaptive shape for Claude Opus
  4.7 and newer had it replaced by `{"type": "enabled", "budget_tokens": N}`,
  which those models reject with a 400 — and no configuration could avoid it.
  On the OpenAI path, an entry carrying a `reasoning` dict (the Responses API's
  `summary: auto`) also received `reasoning_effort`, which langchain-openai
  1.5 passes through as a bare kwarg and `Responses.create()` rejects before
  any request is sent. What an entry declares now stands (`setdefault`, the
  way `cache_control` was always applied); an effort level fills in only what
  the entry left unsaid. Claude 4.6 and later get the adaptive shape by
  default — `{"type": "adaptive", "display": "summarized"}` plus
  `output_config.effort` — and the 3.x / 4.0 / 4.1 / 4.5 releases keep the
  budget; `thinking_style: anthropic_budget` / `anthropic_adaptive` overrides
  the guess.

- **The Indicators tab listed nothing, against a healthy endpoint**
  ([#62](https://github.com/thetahealth/mirobody/issues/62)). `c470b3d`
  reshaped `GET /api/v1/health-indicators` around one envelope on 2026-09-07;
  the shipped bundle was built on 09-04 and read `catalog` / `indicators`, keys
  the route has never sent. It rendered "No indicators yet" over an account
  with data, while the Files tab, the tab badges and the chat beside it worked.
  Client-side only — the endpoint is unchanged.

  Three reads were wrong, not the one reported: the list, the readings drawer
  (`indicators[0].readings`, now `rows`), and the per-reading **edit and delete
  buttons — a reading's key is `row_id` and the client looked for `id`, so
  owner-only correction had quietly disappeared from every row**. The client
  repo pins each mapping against fixtures copied from `_catalog_row` /
  `_reading_row`, and `mirobody/server/routers/test_indicator_contract.py` pins
  the same key sets here, so the next reshape fails a test rather than a page.

  Two traps for anyone changing that route. It answers in two grains from one
  path — catalog without `keywords`/`indicators`, readings with them — and a
  search matching nothing answers with the CATALOG, so a client cannot infer
  the grain from its own request. And `truncated` arrives `true` on a COMPLETE
  catalog, because `_per_indicator_truncated` compares each row's `total` (the
  count over that indicator's whole series) against the one row carrying it, so
  it is not on its own a statement that anything was left out.

  `frontend/` is build output and this replaces the directory with a fresh
  build, so the bundle also carries two changes that are not this issue: the
  model picker stops splitting `/api/models` provider names into bogus "Agent"
  tabs, and the care-circle list reads `data.members`. The second needs a
  server fix that is not in this branch — `POST /invitation/shared-by-me/list`
  hands a bare list to a `dict`-typed envelope and answers `code: -1` on every
  call — which changes nothing for a reader, since that route has never
  returned a usable answer.

### Changed

- **The configuration is split by concern, and `config.yaml` names its
  siblings.** `config.yaml` keeps what the containers wire (server, database,
  login); `config.llm.yaml` is the file to open — the one-key table, `MODELS`
  and the three route keys, and no key: `.env` holds that; `config.devices.yaml`
  holds Garmin, Oura and Whoop — empty credentials, the vendor endpoints filled
  in — and the Google / Apple sign-in and `MIROBODY_WEB_CONFIG` block for the
  mobile clients (all of it sat in `config.yaml`, which a `git clone` +
  `./deploy.sh` deployment never touches for these). An `INCLUDE` list at the
  top of `config.yaml` names
  the two — Home Assistant's `!include`, spelled as a plain list — and they
  load right after it, before your `config.{ENV}.yaml`. An explicit list rather
  than a directory scan, so a `config.prod.yaml` next to them is never mistaken
  for a concern file. Gone with the move: `FRONTIERX_*` and `JINA_API_KEY`
  (read by nothing), a second `DASHSCOPE_API_KEY` declaration, and the
  `*_API_KEY: ""` lines in the model file — a declaration that read as "put the
  key here", when the key goes in `.env`.

- **`.env` is where the key goes, and the boot log answers whether it worked.**
  `./deploy.sh` writes the five key names, commented, into the `.env` it
  generates, and the `config.{ENV}.yaml` it generates no longer lists API keys
  — there was a second place to put them, and #68's reporter had chosen the
  other one. The banner the server prints at boot ends with the same table
  `mirobody doctor` prints: what each surface selected, and the fix where one
  has nothing. The README's run section says the same in five steps.

- Model ids refreshed and verified against each vendor's live catalogue on
  2026-09-10: `gemini-3.8-flash`, `gpt-5.6-terra`, `anthropic/claude-sonnet-5`,
  `qwen3.8-flash` (one Qwen for chat and for the utilities — it reads images
  and makes tool calls, measured), `deepseek-flash`. `safe_read_cfg()` reads
  the environment when no `Config` is loaded, as `Config.get_str` always did
  first. Network wording names the condition (openrouter.ai unreachable), not a
  region.
- **A dead-code sweep** (with the two removals above: 52 files, +245 / −1,978
  lines). Everything with zero callers in the repository, in its downstream
  consumer and in the tests: `utils/s3.py`, `utils/truncate.py` — and with it
  the `tiktoken` dependency; the profile chunker packs by the character
  estimate the module already fell back to on any host that could not reach
  the BPE download — `utils/data.py`, the sync PostgreSQL/Redis paths,
  `hipaa_policy.get_azure_deployment`, the storage backends' `get_file_info`,
  push_service's unreachable HTTP branch, the scheduler's unwired stop/status
  methods, and a dozen methods on pulse services. `utils/i18n.py` is one
  function and a cache; `utils/crypto.py` uses `AESGCM` in both directions
  (rows already encrypted decrypt unchanged); the storage factory tries an
  explicit backend list instead of `__subclasses__()`.

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
