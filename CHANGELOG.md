# Changelog

## 1.3.0

**Breaking, and the whole point: `pip install mirobody` is now a library.** It
was 93 packages and 245 MB, of which mirobody itself was 29 MB; it is now
**2 packages and 52 MB** — the vocabulary layer on numpy. Everything that made
up the other 216 MB moved behind two extras. If you were installing the default
for document parsing or the server, add the bracket:

| before | after |
| --- | --- |
| `pip install mirobody` (then `mirobody parse`) | `pip install 'mirobody[parse]'` |
| `pip install 'mirobody[agents]'` | `pip install 'mirobody[app]'` |
| `pip install 'mirobody[server]'` | `pip install 'mirobody[app]'` — `[server]` alone could never run `serve` or `worker` anyway |
| `pip install 'mirobody[cn]'` | `pip install 'mirobody[app]'` — see below |

### Changed

- **Base dependencies are `["numpy"]`.** The extraction stack, the model SDKs
  and the whole server/agent tree were base requirements, so a consumer of ②
  Standardize paid for PowerPoint parsing and three LLM clients to resolve a
  name. Worse than wasteful: `mcp>=2.0.0` in base made the package
  **uninstallable** alongside `langchain-mcp-adapters` (which caps `mcp<2.0.0`),
  and the only downstream workaround was to override this project's metadata in
  a lockfile. `mcp` moved to `[app]` with `mirobody.mcp`, which is server-side
  by construction (`mcp/service.py` imports psycopg_pool and redis at module
  level), and the conflict is gone.

- **Six extras became two.** `[parse]` is the one bracket a person types;
  `[app]` is what the image installs and is the only consumer of `[server]`,
  `[agents]` and `[cn]`, which are removed. Nothing referenced `[server]` or
  `[cn]` anywhere in the repository, and `[cn]` was not an axis: it paired an
  object-storage backend with a model vendor's SDK under a country name while
  their siblings sat in `[server]` and base — so installing Aliyun OSS (0.3 MB)
  also installed the Ark SDK (43 MB). `storage/factory.py` iterates
  `AbstractStorage.__subclasses__()`; the backends are peers in the code and
  were split by geography only in the packaging.

- **`mirobody parse` checks for its extra up front**, the way `serve` and
  `worker` already did, and prints the pip command instead of a
  `ModuleNotFoundError` from four modules deep.

- **The vocabulary layer moved to the package root.** `mirobody.units`,
  `mirobody.lexical`, `mirobody.value_scale` and `mirobody.zh_fold` were under
  `mirobody.indicator.fhir.units` and `mirobody.indicator.*`. The `fhir` in that
  path was wrong for UCUM units, and the modules a `pip install` can actually
  use now sit next to `engine.py` rather than inside the tree that is pruned
  from the wheel.

- **`requirements.txt` is `-e .[app]`.**

### Added

- **`mirobody/py.typed`.** The package is annotated throughout and, under
  PEP 561, every type checker was ignoring all of it — a consumer got `Any` for
  `Resolution` and `ParsedQuantity`.

- **A declared public surface.** `from mirobody import resolve, resolve_reading,
  parse_file, Resolution, Reading` — lazily (PEP 562), so `import mirobody`
  stays at ~17 ms and pulls in neither numpy nor the data bundle. `__all__` in
  `mirobody`, `mirobody.engine` and `mirobody.lexical` is the stable surface;
  everything else is internal.

- **`mirobody.BUNDLE_VERSION`** — which LOINC release the shipped bundle was cut
  from, as `loinc-2.82+2026.08.28-<12 hex>` where the hex is a digest over the
  bundle's own members. The package version and the corpus version are different facts, and
  only the first existed: a consumer generating a seed from the bundle at build
  time and pinning the package at runtime had nothing to assert the two agree
  against. `scripts/stamp_bundle_version.py --check` is the CI gate.

- **`eval/run_eval.py` ships.** It takes `--testset <path>`, so a downstream
  team can score this resolver against their own distribution without their
  cases reaching this repository. The maintained test set and its results stay
  out, as before; only the harness — the four metrics, the analyte-level
  grading rule, the strata — is the shared part.

### Fixed

- **The resolver holds 70% less and loads 3× faster.**

      resident        514 MB  ->  152 MB
      cold load       1.09 s  ->  0.28 s
      resolve()       0.76 ms ->  0.07 ms
      resolve_reading 1.60 ms ->  0.10 ms

  None of that was about the amount of data — 77 MB of text. It was about the
  shape the artifacts stored it in. `loinc_alias_index.npz` kept its 921,172
  keys as a *pickled object array* and `fhir_meta.csv.gz` / `loinc_axis.csv`
  are text, so reading them allocated roughly 1.6 million Python strings for
  tables that answer a few hundred lookups per call, and no amount of care in
  the loader could avoid it.

  The bundle now carries the same content as byte blobs plus int32 offsets
  (`scripts/build_runtime_index.py` cuts them; `mirobody/_strtab.py` reads
  them). `_posting` bisects the alias blob, `_pick` runs its specimen regexes
  against slices of the corpus-name blob, and only the row that wins is
  decoded. `variant_for_reading` also stopped scanning all 97,314 axis rows to
  find one code.

  On disk this is close to a wash — the tarball compresses a blob about as
  well as it compressed the pickle — and `fhir_meta.csv.gz` leaves the wheel
  entirely, so the whole change costs +3.2 MB of wheel (23.6 -> 26.8 MB).
  Deliberately NOT the memory-mapped variant, which would need the data
  uncompressed on disk and take the installed `res/` from 22 MB to ~120 MB to
  save the last few tens of MB.

  Verified identical on all 7,354 eval cases — coverage 0.927, precision
  0.967, wrong-rate 0.032, exact 0.698, unchanged to three decimals — and
  every one of the 97,314 axis rows plus 677,643 corpus names round-trips
  field-for-field against a fresh parse of the source it was cut from.

- **The semantic tier can no longer lose its safety gates quietly.** It parsed
  `loinc_axis.csv` into gate tables of its own and treated the file being
  absent as "leave them empty" — survivable while that CSV shipped, silent
  disaster once `axis_fields.bin` superseded it. Those gates are what stop
  cosine answering `total cholesterol` with a PhenX self-report survey item.
  `_bundle.load_axis()` is the one reader for both tiers now and raises,
  naming the rebuild command.

- **`resolve()` honours the skip list.** `loinc_skip.txt` is described in the
  bundle NOTICE as "LOINC codes excluded from resolve" — non-clinical CLASS
  (SURVEY, PHENX, DOC, ADMIN) plus DEPRECATED and DISCOURAGED status — and
  `resolve()` never consulted it. Only `_component_index` did, so the list
  gated which sibling a unit-aware lookup could switch TO while the first
  answer stayed ungated: `呼吸次数` came back as *First Respiration rate Set*, a
  nursing form field.

  A skipped winner now re-picks rather than refusing the term, which is where
  most of the value is: `已施用的药物` went from `27771-5 Medical social services
  treatment plan, Medication administered` to `29303-5 Medication
  administered`, the code that was sitting behind it all along. Free on the
  benchmark — coverage, precision, recall and wrong-rate unchanged to three
  decimals across all 7,354 eval cases, with exact-code agreement up from
  0.698 to 0.699.

- **`boto3` is declared.** `utils/config/llm.py::_build_bedrock` imports it for
  the sync client and it was never in any dependency list; it arrived
  transitively via `aioboto3`, the same class of bug as the undeclared
  `python-multipart`, `starlette` and `anthropic` before it.

- **One copy of the alias tables, not two.** `res/aliases_src/{lang}.tsv` was
  also stored inside `fhir_loinc_bundle.tar.gz` as `aliases/{lang}.tsv`,
  byte-identical, with the resolver reading the loose files and the lexicon
  build reading the bundle members. They had drifted: four rows added to
  `zh_curated.tsv` (DPA, DGLA, AA/EPA ×2) were live for the resolver and
  invisible to the build. The bundle members are gone, `load_all_aliases` reads
  the loose files, and a curated row now takes effect when it is written rather
  than at the next bundle re-cut.

- **The runtime no longer imports the build tooling.** `engine.py` read the
  shipped bundle through `indicator/fhir/embeddings/bundle.py` and folded index
  keys with the private `alias._normalize`. The read side is now
  `mirobody/_bundle.py` and the fold is `mirobody.lexical.index_fold`, which the
  build pass imports — one definition for a function the build and the runtime
  must agree on exactly.

### Removed

- **17 MB of bundle members that no install can read.** The pickled alias
  index, the axis CSV, and the dose/demote/analyte tables are inputs to the
  build passes; the runtime reads the blobs derived from them.
  `scripts/build_backend.py::_BUNDLE_RUNTIME_MEMBERS` repacks the shipped copy,
  and `check_wheel_data.py` now checks members inside the bundle, not just the
  bundle's presence. The full bundle stays in git — it is what a contributor
  rebuilds from.

- **19,000 lines that no install can run.** `indicator/fhir/embeddings/` (the
  bundle-build passes, which need raw LOINC/UMLS releases licensed per user) and
  `indicator/fhir/resolve/` (the v2 semantic pipeline, which needs a multi-GB
  embedding matrix that ships on a volume) are pruned from the wheel and the
  sdist. They stay in git for contributors. `scripts/check_wheel_data.py` fails
  the build if either comes back — the same standard already applied to 28 MB of
  data nothing reads.

### Known limit

The resolver's remaining 338 MB is 921k + 677k Python strings (the alias keys
and the corpus names), materialized because the shipped `.npz` and `.csv.gz`
store them as objects and text. Removing them needs the on-disk format to
become a memory-mappable blob, which trades roughly +96 MB of installed disk
for the RAM and is deliberately not in this release.
## 1.2.2

### Added

- **`indicator.fhir.units.pick_display_unit`** — pick the unit a merged series
  displays in: the most frequent unit wins, ties go to the latest measurement.
  One indicator must render as one series in one unit; this is the tie-break
  callers were each reimplementing.

### Fixed

- **`parse_value_unit` respects the author's whitespace boundary.** The
  whitespace collapse used to glue a numeric value onto a digit-leading count
  unit: `240 10⁹/L` became `240109/L` and parsed as value 240109, unit `/L` —
  a platelet count corrupted by three orders of magnitude. The first token is
  now tried as the whole value and the remainder as the whole unit before any
  collapsing (Path B0). Golden vectors in `mirobody/test_units.py`.

- **The pulse Collect tables no longer carry conversion constants of their
  own.** `pulse.standardize.units` had drifted from the UCUM engine it
  shadows: lb was 2.20462 there vs the exact 2.2046226… ([lb_av] =
  453.59237 g), glucose said 18.0182 vs the engine's 18.016 (C6H12O6 =
  180.16 g/mol). Imperial factors and the glucose/cholesterol/triglyceride
  molar masses now derive from `indicator.fhir.units.convert` at import, and
  `mirobody/test_units.py` walks the whole table asserting zero drift wherever
  both sides know the pair.

- **An attachment-only chat turn is a question.** Attaching a file and pressing
  send without typing anything was refused with `-3 Empty question.` before the
  agent was ever reached — `chat_handler` judged emptiness on the message text
  alone. It now judges the whole turn, and a turn that carries files but no
  words is normalised once, at the top of `handle_request`, into a stand-in
  question in the request's language (`mirobody/utils/locales/chat.json`). One
  field, so persistence, the session title, the resume hint, the `question`
  kwarg and the message the model receives all agree; no turn reaches a
  provider as a zero-length user message, which Anthropic rejects outright and
  LangGraph would checkpoint into the session and replay. (#39, #41)

- **`/uploads/` paths no longer move under the model mid-turn.** The upload
  pass asks an LLM for a descriptive filename and overwrites
  `th_files.file_name` with it, concurrently with the agent — so `ls` listed a
  file that `read_file` then could not open, and the agent told the user their
  report was unreadable. `/uploads/` now names this turn's attachments by what
  the request attached them under, and the note that announces those paths to
  the model is built from the mount itself rather than from the request, so it
  cannot name a path the mount does not serve: same-named attachments are
  announced apart, a `file_key` whose row is deleted or is not the caller's is
  not announced at all, and a listing shortened by the per-turn file cap says
  so. `/library/` still shows the stored, descriptive name. (#40, #42)

## 1.2.1 — released 2026-08-23

The first release in which `pip install mirobody` actually works, and the MCP
surface is on the current protocol. There are **breaking changes to the MCP tool
names**; see below. (1.2.0 was the working name of this release while it was
being hardened; it was never published — the version jumps 1.0.62 → 1.2.1.)

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

### Added — one key, two gateways, a production switch

- **One key runs everything.** Chat, vision file parsing and semantic
  indicator search all follow whichever single key is present —
  `OPENROUTER_API_KEY` (recommended) or `DASHSCOPE_API_KEY` (the drop-in
  fallback for networks where openrouter.ai is unreachable). The chat default,
  the vision provider and `EMBEDDING_PROVIDER` auto-select by available key;
  the shipped config pins none of them.
- **Self-hosted embeddings.** Any OpenAI-compatible provider accepts a
  `<PROVIDER>_BASE_URL` override (e.g. `OPENROUTER_BASE_URL`), so a deployment
  can serve the open-weights Qwen3-Embedding-8B itself and point the provider
  at it.
- **Explicit deployment posture.** `PRODUCTION: true` refuses to start while
  demo login codes or `REPLACE_THIS_VALUE_IN_PRODUCTION` placeholders remain,
  and skips the demo seed; `BOOTSTRAP_SCHEMA: false` turns off the boot-time
  DDL replay. Environment NAMES carry no behavior — `ENV` only selects a
  config overlay and tags log lines.
- **Every sign-in account owns data.** The demo seed gives each predefined
  account a thin record of its own (self-tracked vitals, one normal checkup)
  beside the shared synthetic record, so care-circle isolation is visible on
  screen, not just described.
- **Honest failure surfaces.** A zero-key deployment now marks an uploaded
  file *failed* with the missing-key reason instead of showing "processed"
  over an empty extraction; `/api/models` lists only providers whose key
  actually resolves; the tokenizer degrades to an estimate instead of
  crashing the first chat on hosts that cannot reach the OpenAI CDN
  (`TIKTOKEN_CACHE_DIR` pre-seeding is the exact-count remedy).

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
  ② Standardize itself: any-language indicator name → LOINC, free-text unit → UCUM plus
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

- **The spelling no longer decides the answer.** New
  `mirobody/lexical.py`: NFKC-lite folding (full-width, superscripts,
  the six Unicode dash variants) and a CJK-aware tokenizer, used to derive extra
  candidate surfaces for a lookup. The bundle's own normalizer is NFKC +
  casefold and must stay that way — it folded the index keys at build time — so
  `LDL–C` (en-dash) missed while `LDL-C` resolved, one invisible codepoint
  apart, and `fasting_glucose` missed while `fasting glucose` resolved. The
  latter is the convention the platform API documents in every `POST /data`
  example, so a caller following the docs got an unresolved row for a term the
  engine knows. Variants are tried **last**, after the term as written has
  missed, so they can only turn a miss into a hit.
  The deliberate-non-answer check runs over the variants too: without that,
  `blood_pressure` skipped a block written for `blood pressure`, tokenized to it
  anyway and came back 8462-4 (diastolic) — the exact bug the sentinel was
  written for, through the back door. Both spellings are pinned in
  `MUST_NOT_RESOLVE`.
- **`名称(缩写)` resolves — and refuses when the halves disagree.** The shape a
  lab report prints more often than not: on the hosted platform's production
  data, 147 of 868 distinct indicator names are `名称(缩写)` and 70 of those
  carried no code at all. `resolve` now strips the trailing parenthetical (all
  four bracket pairs, full-width included), resolves **both** halves, and takes
  the answer only when they agree or only one of them resolves. `空腹血糖(GLU)`,
  `总胆固醇(TC)`, `血小板计数（PLT）`, `尿素氮(BUN)` now resolve; `血糖(HbA1c)`
  and `胆固醇(HDL-C)` stay unresolved, because preferring the stem there would
  file an HbA1c reading into the glucose series. Measured over 6,641 terms that
  resolve when written plainly: **0% → 100%** survive being written in the
  `名称(缩写)` shape.
- **Device and wearable vocabulary resolves.** `POST /v1/data` calls device data
  the main form structured records take, and that vocabulary was the least
  covered: `steps`, `resting heart rate`, `sleep duration`, `body fat
  percentage`, `静息心率`, `睡眠时长`, `体脂率`, `FBG`/`FPG`, `血糖(空腹)` all
  missed, and `SpO2` was worse than missing — it answered a **deprecated**
  "Fractional oxyhemoglobin … Preductal" row carrying no LOINC at all
  (`resolved=True`, empty code). 18 rows in `resolver_overrides.tsv`, each
  verified against its target.
  `HRV` is deliberately left alone: it abbreviates human rhinovirus as well as
  heart rate variability (it answers 40991-2, Rhinovirus+Enterovirus RNA), so
  the right result is a decision rather than a lookup. The spelled-out form and
  `心率变异性` resolve to 76643-6.
- **The abbreviation column resolves, and the ambiguous ones are refused.**
  The short codes a CBC / 生化 printout puts beside each analyte were the
  worst-covered surface in the engine, and two of them were WRONG rather than
  missing — the 血红蛋白 → HbA1c near-miss again, wearing the short code instead
  of the word: `HGB` → 4548-4 **Hemoglobin A1c**, `HCT` → 1992-7
  **Calcitonin**. `HGB`/`Hb`/`HCT`/`PCV`/`PLT`/`RBC`/`WBC`/`TC`/`TG`/`GLU`/
  `Cr`/`CREA`/`UA`/`TP`/`CK` now resolve correctly. `CA`, `PT` and `MG` are
  blocked instead: each names more than one test (钙 vs 癌抗原; prothrombin time
  vs 前列腺素; 镁 vs the unit), and the index answers whichever the commonness
  prior likes — which is exactly how `HGB` became HbA1c.
  Resolver coverage: **116/116 → 175/175**.
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

- **Deleting a health document did not stop the agent reading it.** Walked
  through the UI on a running stack: upload a lab report, ask about it, delete it
  from the Files tab, ask again — and the answer came back with all twelve
  values. The object was gone from storage, the `th_files` row was soft-deleted
  and the readings were cascade-deleted. Two copies were not:

  - `deep_agent_workspace` scope `library`, the agent's virtual filesystem.
    `deep/backend.py` calls `/uploads/` and `/library/` "read-only projections of
    `th_files`", but each is its own row holding its own copy of the extracted
    CONTENT, so the projection outlived what it projected. The table has had a
    `deleted` column all along and nothing had ever set it.
  - `health_user_profile_by_system.common_part` and its
    `/memories/health_profile.md` mirror — a DERIVED summary that quotes the
    readings verbatim ("GLU 7.5 mmol/L, FBG 7.45, PBG 9.5, HbA1c 7.2% …"). It
    carries no `file_key`, so neither the file delete nor the reading cascade
    reached it. This was the copy that kept answering after the first fix.

  Both are now withdrawn inline with the delete the user asked for, not in the
  background cascade: a background failure leaves the file gone from the UI and
  still readable by the model. They are keyed differently on purpose — the
  workspace copy belongs to whoever UPLOADED, the profile to whoever OWNS the
  readings, and for a care-circle upload those are different people. The profile
  is invalidated rather than repaired, because a projection of a record that
  changed is wrong by definition and the refresh pass rebuilds it from what
  remains.

  Verified by the same walk-through: the agent now reports it searched
  `/uploads/`, `/library/` and the indicator store and found nothing. Nine tests,
  including one that fails if either call site is removed — the first version of
  those tests exercised the helpers directly and stayed green with both calls
  deleted.



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

### Added — a records API shaped like the hosted one

- **`POST /api/standardize` · `POST` · `GET` · `DELETE /api/data`**
  (`server/routers/records_router.py`). Same request bodies, response envelopes
  and field names as the corresponding endpoints on
  [docs.mirobody.ai](https://docs.mirobody.ai/en/api-reference/), so code
  written against a self-hosted deployment reads like code written against the
  hosted one instead of being a second API to learn.
  `POST /api/data` standardizes every record on the way in and answers
  `{"status":"ok","ingested":N,"standardized":M}`; `GET /api/data` returns
  `{"object":"list","data":[…],"has_more":…}` one object per reading, carrying
  the row `id` that `DELETE /api/data?id=` takes; `POST /api/standardize`
  returns `{"object":"extraction","data":[…]}` and is a dry-run unless
  `store=true`.
  Three deliberate departures, all the same direction — this is one machine, not
  a multi-tenant plane: **no `/v1` prefix** (these are not the hosted contract);
  **no `user` / `retention` / `session_id` / `mb_live_*`** (your JWT says who you
  are, and a row lives until something deletes it) — `retention` and
  `session_id` are accepted and ignored rather than rejected, because a 400 for
  a field the platform docs told you to send helps nobody; and **`DELETE
  /api/data` requires an explicit scope** (`id`, `indicator`, or `all=true`),
  where the hosted endpoint reads "no filter" as "everything".
  Errors on this surface use the platform envelope
  (`{"error":{message,type,code,param}}`); the web client's endpoints keep the
  house `{code,msg,data}`.
- **`engine.parse_text`** — the text half of `parse_file`, split out so
  `/api/standardize` can extract from a string without writing a temp file.

### Changed — naming

- **Stage ② is `Standardize`, stage ③ is `Answers`** — was `Sort` / `Answer`.
  The three stages are the spine of both this README and
  [docs.mirobody.ai](https://docs.mirobody.ai/), and they have to be the same
  three words in both: **① Collect → ② Standardize → ③ Answers**, C · S · A.
  `Sort` also under-described what `indicator/` does — it resolves codes and
  normalizes units, which is standardization, not ordering. Labels and prose
  only; no module, package or symbol was renamed.

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

- The `/charts` static mount, its Docker volume and `LOCAL_CHARTS_DIR`:
  nothing has generated chart PNGs since the ChartService tools went (the
  agent charts by writing a `vis-chart` fence the frontend renders), so the
  mount served an eternally empty directory and logged a misleading warning.
- The remote-config fetch (`CONFIG_SERVER` / `CONFIG_TOKEN`): it spoke a
  proprietary config-service API nothing in this repository implements or
  documents. Configuration is `config.yaml` + `config.{ENV}.yaml` overlays +
  environment variables.
- Dead configuration keys read by no code: `FILE_ANALYSIS_PROVIDER`,
  `VITAL_API_KEY`/`VITAL_ENVIRONMENT`, `RENPHO_*`. The Oura provider's real
  keys (`OURA_CLIENT_ID`/`OURA_CLIENT_SECRET`) gained the config slots they
  never had.
- ~1,100 lines of verified-dead code, each confirmed unreferenced by a
  repo-wide search including dynamic and string references: `chat/history.py`
  (whose function names shadowed the live `session.py`), `chat/mcp_loader.py`,
  `pulse/core/data_quality_service.py`, three `message.py` functions plus the
  `MessageType` class only they used, eight request helpers in
  `server/routers/middleware.py` that duplicated `server/middlewares.py`, and
  `mcp.read_global_resource` — which could only ever return `None`, because the
  `global_resources` dict it read was never assigned.

### Changed

- Resolver coverage on everyday panels went from 32/94 to 211/211, measured
  (`test_engine_coverage.py` prints the score). Panel names resolve honestly:
  `lipid panel` / `血脂` to *nothing* rather than to one arbitrary component,
  and `blood pressure` to the FHIR vital-signs panel code (85354-9) rather
  than the diastolic code it used to return.
- The agent layer is one package: `mirobody/agent/` (was `mirobody/pub/` plus a
  top-level `mirobody/chat/`). HTTP routers moved to `mirobody/server/routers/`.
