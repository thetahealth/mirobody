# Roadmap

Known gaps and deferred work, with the evidence that motivated each. Everything
here was found by measurement rather than intuition, so each item states what
was measured — a future maintainer should not have to rediscover the reasoning.

Ordered by (value ÷ risk) within each section.

---

## Capability gaps

### Reference ranges and abnormal flagging

**Status:** not started. The largest functional gap in ② Standardize.

Resolving `LDL cholesterol` to LOINC `13457-7` tells you *what the test is*. It
does not tell you whether `4.2 mmol/L` is high — and that is the question a
person actually has. LOINC itself carries no reference ranges; they vary by
laboratory, method, age and sex, so this needs a data source and a precedence
policy, not just code.

A comparable system (`theta-smart`) resolves ranges by
`condition → sex → default → union-of-all-sexes`, with that last fallback
existing so an indicator that only has sex-stratified ranges (GGT, for example)
still yields something usable for a user whose sex is unknown. That precedence
chain is worth copying; the data source is the open question.

Note the honesty constraint this inherits from the resolver: a wrong range is
worse than no range. Whatever ships must be able to say "no reference range for
this indicator" rather than guessing one.

### Unit conversion — **done**

**Status:** shipped in `indicator/fhir/units/convert.py`. Kept here because two
of the traps it walked into are worth not rediscovering.

Three tiers: same-dimension UCUM parsing, a molar-mass bridge keyed by LOINC
code for mass↔substance, and an explicit refusal for everything else.

The trap this entry originally missed: **do not use `unit_family()` to decide
convertibility.** It is a LOINC PROPERTY classifier and it is wrong in both
directions — `kg/m2` (BMI) and `mg/dL` are both `MCnc` and cannot convert, while
`U/L` (`CCnc`) and `[IU]/L` (`ACnc`) are in different families and are the same
unit. The MCP tool's own docstring asserted the family rule, so a model
following it would have turned a BMI of 24 into a mass concentration. Dimension
signatures reject the first and accept the second by construction.

The affine-unit warning above stands and is honoured by omission: `Cel` has no
offset declared, so it parses to `None` and converts only to itself.

Three conventions are written into `MOLAR_MASS` because they will otherwise be
got wrong: triglyceride uses a CONVENTIONAL average mass (triolein ≈ 885.4, not
a determinate molecule), BUN is reported as nitrogen while urea is the whole
molecule (2.14x apart, one row each, never shared), and conversion happens only
WITHIN one code — across codes is concept mapping.

### Japanese coverage: the data is there, the wrong data is there

**Status:** not started, and deliberately NOT the obvious fix.

`res/aliases_src/ja.tsv` is 16,809 rows and 715 KB, and **16,300 of them resolve
to nothing at all** — not "nothing in LOINC", nothing. Its right-hand side is
overwhelmingly SNOMED-shaped: `Jaagsiekte sheep retrovirus`,
`Ornithine transcarbamoylase deficiency`, `Abiotrophia defectiva endocarditis`.
Diseases and organisms, not observations, so the observation index has no key for
them.

**Cleaning up that file is not the fix, and was measured before being rejected:**

- Size: 715 KB of a 24.4 MB wheel — 3.0%.
- Memory: ~2.1 MB of the resolver's ~484 MB resident — 0.4%.
- Correctness: the plausible mechanism was shadowing. `_alias_source_files`
  reads ja.tsv BEFORE zh.tsv and the loader uses `setdefault`, so a shared kanji
  term takes ja's target. Measured: 25 shared keys, 10 with differing targets,
  and 5 of those 10 already resolve correctly anyway via the raw-index fallback
  (including `胆汁酸` → 14628-2, the only real analyte among them). The
  remaining 5 are conditions and procedures — 交換輸血, 幹細胞移植, 肝細胞癌,
  脊柱腫瘤, 膀胱腫瘤 — whose "recovered" codes would be a risk score and an
  aneuploidy panel. Recovering them would be a regression in spirit.
- Cost: the generator reads LOINC linguistic-variant sources that are **not in
  this repo**, so the change could be neither run nor verified here.

So the 16,300 rows are inert, not harmful, and the real gap is elsewhere: a spot
check of the everyday 健康診断 panel resolves **20 of 24**, and the misses were
specific words, not a shortage of data. `ja_curated.tsv` — the hand-written file
that takes precedence over the machine-generated one — has **4 rows**, all
header placeholder, against `zh_curated.tsv`'s 633.

The work is therefore curation, not cleanup: resolve a 健康診断 term, find the
miss, add a row to `ja_curated.tsv` or `resolver_overrides.tsv`, add a case to
`test_engine_coverage.py`. Same loop as the 中文 rows that took coverage from
32/94 to 176/176. If the machine-generated file is ever regenerated, filter it
by LOINC CLASS at generation time so observations survive and conditions do not.

### `resolved=True` with no code: a contract the resolver breaks 9.6% of the time

**Status:** not started. The one-line fix is safe; the useful part is not.

`resolve("eGFR")` returns `resolved=True`, `method="lexical"`, canonical
*"Glomerular filtration rate [Volume Rate/Area] ... (MDRD)/1.73 sq M"* — and
`loinc=""`. A caller who branches on `.resolved`, which is what the field is
for, gets a truthy answer holding no identity.

Measured over a uniform 30,000-key sample of the 921,172-key alias index:
**2,888 of the 30,000 resolve this way — 9.6%.** Every one has the same cause,
and it is not a shortage of data:

```
what the trap is made of (n=2,888)
  2,888   the matched LONG_COMMON_NAME is not in the ACTIVE axis at all
```

`_pick` chooses a display name from the wider name table, and the code lookup
then runs against the ACTIVE-filtered axis. When the winning name belongs to a
DEPRECATED row the name survives and the code does not. Most of the 2,888 say so
in their own text — *"Deprecated Oat IgG Ab RAST class"*, *"Deprecated JWH-018
butanol metabolite/Creatinine"* — so for those, withholding the code is right
and only `resolved` is lying.

**The damaging subset is the clinical terms that land in it.** `eGFR` is on
every metabolic panel printed anywhere, and its best name match happens to be a
retired MDRD row.

**The naive fix was prototyped and rejected.** Make `_pick` skip-aware: when the
top-ranked name has no ACTIVE code, walk down the ranking. It improved `血常规`
(→ 57021-8, a CBC panel — correct) and it sent **`eGFR` → 107231-3, *Natriuretic
peptide B*** — BNP, a cardiac marker, for a kidney-function term. Walking the
ranking crosses analyte boundaries silently, which is the exact failure this
project scores as worse than silence.

So the work splits into two independent pieces, and the second is the real one:

1. **Make the field honest.** `resolved` should be `bool(loinc)`. This cannot
   regress a correct answer — it only stops a codeless one from claiming to be
   one — and it converts 2,888 confident non-answers per 30,000 into honest
   misses. Do this first and separately.
2. **Curate the clinical terms it exposes.** Once `eGFR` reports as a miss it
   joins the same loop every other gap uses: one row in
   `resolver_overrides.tsv`, one case in `test_engine_coverage.py`. `eGFR` needs
   a target that is an alias key AND has an active code; the four obvious
   spellings (`GFR/1.73 sq M.predicted`, `estimated glomerular filtration rate`,
   …) are not alias keys, so this one needs a `zh_curated`/`en_curated` row
   rather than an override redirect.

### A parenthetical that NARROWS its stem is not a contradiction

**Status:** blocked on data we do not ship, and documented so it is not
"fixed" by accident.

`名称(缩写)` where the halves disagree must refuse — `血糖(HbA1c)` is glucose
outside and HbA1c inside, and preferring either half files a reading into the
wrong series. That rule is pinned in `test_engine_coverage.py` and it is right.

`血压(收缩压)` has the same *shape* and is not the same case. The parenthetical
narrows the stem: the stem is the BP panel (85354-9) and the parenthetical is
one of its two members (8480-6). 8480-6 is the defensible answer, and today the
term refuses.

Telling the two apart needs one fact: **is the parenthetical's code a child of
the stem's panel?** LOINC answers it, in the panel-hierarchy file
(`LOINC/AccessoryFiles/MultiAxialHierarchy`), which is not in the shipped
bundle — `loinc_axis.csv` carries the six axes and no membership. Without it the
only implementable rule is "prefer the parenthetical", which is precisely what
breaks `血糖(HbA1c)`.

Two ways forward, in preference order: ship the parent/child pairs for the
panels the resolver actually answers (a few hundred rows, not the whole
hierarchy), or hand-list the narrowing pairs in `resolver_overrides.tsv` the way
every other curated fact in this repo is handled. Do not implement it by
guessing from string containment — `收缩压` contains `压` and so does everything
else in the vicinity.

### LLM behaviour tests for `mirobody parse`

**Status:** not started. `parse` has **zero** tests that exercise a real model.

The extraction prompt in `mirobody/engine.py` can drift silently on a prompt
edit or a model upgrade, and nothing would fail. The pattern worth copying
asserts *structural invariants* rather than golden output — e.g. an ECG page
must collapse to one row and must never be split into per-measurement rows; a
sparse note must not sprout history sections the document never contained.

Gate them behind the existing `needs_llm` marker so they stay out of the default
suite (they cost money), and run them after prompt or model changes.

---

## Structural work

### `pulse/vendor/` — deleted, archived on a branch

**Status:** removed. The 24-source catalogue and `VENDORS.md` are gone from this
branch; the code is preserved at `archive/pulse-vendor` (a branch pointer, so
the port-spec docstrings stay recoverable in full).

This reverses the "false alarm" verdict recorded here earlier, and the earlier
measurement was not wrong — 1,008 of its lines really were ported port-spec
documentation (confirmed endpoints, auth style, why each operation is a stub),
which is the expensive part to reproduce. What changed is the weighing, not the
facts. Against it: the package never entered a runtime path (verified by
loading the whole `serve` assembly and finding no `pulse.vendor` module in
`sys.modules`), all 24 entries graded `metadata`, no concrete vendor defined a
single `async def`, `oauth2_token_request` was the one piece of working code and
had no caller outside its own test, and 12 of the 24 entries carried mangled
strings from the port (a stray leading quote in 17 fields). It also never
existed on `main`.

The conclusion that survives is the last paragraph of the old entry: the
`Vendor` transport contract and the `BasePullProvider` pipeline contract are
two shapes for one job. That unification is still the real work, and it starts
from `BasePullProvider` — which has four working implementations — rather than
from a catalogue of stubs. The research notes are one `git show` away when it is
time to implement a source.

**Connected:** four tables — `health_data_epic`, `health_data_oracle`,
`health_data_libre`, `health_vital_webhook` — have schema and no ingestion path.
`account_merge.py` legitimately lists them, so they are not dead code. Resolve
them alongside whichever integration lands first, and never drop a table without
an explicit migration: it is irreversible for anyone holding data.

### `pulse/` internal layout

**Status:** proposed, awaiting a decision.

Renaming `pulse/` to `vendor/` was considered and rejected on measurement: only
36% of the package is external-source integration. `core/` (aggregation,
insights, monitoring) is 41% and `file_parser/` is 31%. Naming the whole package
after one third of it would recreate the name/content mismatch this repo has
been removing.

The alternative is to make the mental model readable *inside* `pulse/` without
renaming it:

```
pulse/
├── vendor/   ① every external source (devices, platforms, EHR, Apple)
├── files/    ← file_parser, promoted to a sibling: a file is a source too
├── core/     processing: standardize → daily rollups → insights
└── ingest/  the single entry point, StandardPulseData
```

### `[server]` extra — finish the dependency split

**Status:** unblocked, awaiting a product decision.

The blocker is gone: `utils/config` and `utils/db` no longer import the database
stack at module scope, and the engine now resolves indicators with **numpy as
the only third-party package installed** (verified by blocking sqlalchemy,
psycopg, redis, aioboto3, boto3 and greenlet from `sys.meta_path`).

What remains is moving the DB and HTTP stack out of the core dependency list, so
`pip install mirobody` stops being 458 MB / 116 packages. That is not a
mechanical change: the README's Architecture section currently promises a bare install supports
file parsing and FHIR output, both of which need the database. Either the
dependency list or the promise has to move — a product call.

### Seam #4 — `user_profile`

**Status:** the last two `ignore_imports` entries in `pyproject.toml`.

`pulse/file_parser` and `task/profile_refresh` both import
`agent.chat.user_profile`, which is engine data living agent-side. It cannot
simply move: profile *generation* calls an LLM through the agent stack. The
split is read/store engine-side, generation agent-side.

### MCP transport migration (step 2)

**Status:** prerequisites verified; the swap itself is not done.

Step 1 shipped: tool schemas now come from the official SDK's `func_metadata`.
Step 2 is replacing our JSON-RPC dispatch with `MCPServer`, and it is a
*transport* swap rather than a handler swap — `MCPServer.streamable_http_app()`
returns a complete Starlette app and owns the route.

Verified as feasible: `Context[Any, Any]` parameters are injected and hidden
from the tool schema (bare `Context` fails Pydantic schema generation);
`add_tool()` accepts programmatic registration, so directory auto-discovery
survives; `token_verifier`, `middleware`, `cache_hints` and
`custom_starlette_routes` are the homes for our auth, per-agent filtering,
caching and OAuth endpoints respectively.

Blocking concerns:

- 389 lines of OAuth and personal-MCP token logic must move to
  `custom_starlette_routes` + `token_verifier`.
- Omitting `lifespan=` when mounting the SDK app silently fails to start the
  session manager — the symptom is intermittent production hangs, not a test
  failure.
- Acceptance requires a real OAuth round trip against ChatGPT Apps and Claude
  Desktop. Nothing in this repo can simulate that.

Do it on its own branch so it can be reverted independently.

### Sharing: one write path for `th_share_relationship`

**Status:** the file split landed; the duplicate write did not.

`server/routers/user_router.py` builds a "virtual user" and INSERTs into
`th_share_relationship` with its own ad hoc SQL and its own default-permission
literal (`{"all": 2}`) — the same table `SharingService` owns, never calling
into it. Two writers, two notions of the default permission, on a table that
decides who can read whose health data.

Not folded in with the split because the split was provably behaviour-neutral
(the OpenAPI schema is byte-identical) and this is not: it changes which code
path a live sharing write takes. It needs a test against a real database
first, and there is currently no integration coverage of either path.

### `user/`: two database access layers, undocumented

`sharing.py` and `account_merge.py` use `utils/db.py`'s SQLAlchemy-backed
`execute_query()` with `:named` params; `user.py`, `webauthn.py`,
`user_service.py` takes a raw `psycopg_pool` connection and
hand-roll `cur.execute(...)` with `%s`. The same table is queried both ways —
`health_app_user` at `sharing.py:251` and `user.py:327`.

This is most likely deliberate (multi-statement transactions need the raw
pool) but nothing says so, so the next person picks one by coin flip. Wants
one paragraph stating the rule, and a single `get_user_by_id()` instead of the
query being retyped in four places.

### Three independent "what type is this file" implementations

`utils/file_types.py:get_file_type` (extension → tag), `utils/s3.py:get_content_type` (8-entry if/elif ladder), and
`config/storage/abstract.py` (magic-byte sniffing plus stdlib `mimetypes`).
They disagree: the S3 one returns `application/octet-stream` for anything
outside its 8 extensions, where `mimetypes` resolves many correctly.

`AbstractStorage`'s pair is the one to keep. Deferred because changing which
content-type a file is stored with is a live behaviour change — it affects
what S3 serves and how a browser renders an existing object — and it deserves
its own before/after over real uploaded files rather than being folded into a
cleanup commit.

### Task delivery is at-most-once, with no re-drive

`task/base.py:_pop_batch` removes messages from Redis (BLPOP+LPOP) *before*
`consume()` runs, so a mid-consume crash discards the batch with no requeue.
`ProfileRefreshTask.consume` additionally swallows per-user failures and
continues, dropping that user rather than retrying.

Low impact today: `IndicatorSyncTask` is an idempotent full sweep, and any
later ingest re-triggers it. But file ingest
(`file_parser/services/database_services.py:628`) is the *only* producer for
either queue and there is no cron re-drive, so a sweep that dies partway
through stays undone until the next real upload.

### Empty-list/dict parameter defaults (~19 remaining)

An AST pass confirmed none is mutated in place, so these are latent rather
than live. A blanket conversion to `None` was attempted and reverted: it
touched 105 sites across 43 files and broke callers that iterate the
parameter directly. Worth doing per-signature when a file is being edited for
another reason, not as a sweep.

### Judgment call: `GET /user/settings` performs an UPDATE

`server/routers/user_router.py:196-226` force-enables MFA for
CommonWell-connected users inside a GET handler. A GET that mutates is wrong
by every REST convention — but this one is a security control being applied,
and removing it weakens that control for anyone who has not re-saved their
settings. Left alone deliberately; it needs a product decision, not a
refactor.


### Seam #4 revisited: the agent/pulse cycle, measured

Two facts, both verified rather than assumed:

* `agent/` reaches into **six** distinct internal `pulse.file_parser` modules
  (`services.db_utils`, `services.file_processing_service`,
  `services.file_db_service`, `services.file_abstract_extractor`,
  `services.database_services`, `handlers.genetic`). Not a seam — six.
* `pulse/file_parser/file_upload_manager.py` imports
  `agent.chat.user_profile`, and `task/profile_refresh.py` does the same. Both
  carry `ignore_imports` exemptions in pyproject, so the cycle is known and
  sanctioned, not accidental.

The direction is what makes it a cycle worth paying down: `pulse` is the data
gateway and should not depend on the reasoning layer. The existing seam-#4
plan (split `user_profile` into engine-side read/store and agent-side
generation) fixes the pulse -> agent edge. It does not address agent -> pulse,
where the fix is a narrow public surface on `file_parser` instead of six deep
imports.

Worth stating plainly since it comes up: file PARSING belongs in pulse. Turning
an uploaded PDF into indicators is the same job as pulling from WHOOP —
external artifact in, `StandardPulseRecord` out — and moving it agent-side
would put a database-and-LLM pipeline behind the langchain extra. The upload
TRANSPORT (multipart, WebSocket progress) already lives in `server/routers/`,
which is where it belongs. What is wrong is not the placement; it is the width
of the seam and the direction of the back-edge.

### `vendor/` is a catalogue, and the wheel ships it as code

24 `VendorInfo` entries, all `status=METADATA`, with `Vendor` subclasses whose
network operations raise. It is genuinely useful as a market survey and it is
honest about being one — but it is 2,301 lines of Python shipped to every
installer to express what is essentially a data table, alongside a second,
unrelated `oauth2.py` whose single helper (`oauth2_token_request`) has no
callers because every vendor that would call it is a stub.

Options, in increasing order of work: leave it (it is small and honest); move
the catalogue to a data file and keep one loader; or delete the stub classes
and keep `VendorInfo` + `registry` as the survey they actually are. Not done
here because the port spec docstrings are the valuable part and would need to
survive whichever option is picked.


### Two LLM provider-configuration systems

`utils/config/llm.py` (`LLMConfig`) and `utils/llm/config.py` (`AIConfig`) both
answer "which provider, what base_url, what key", and both end up constructing
an `AsyncOpenAI`. They overlap on openai / openrouter / dashscope / volcengine /
gemini. The near-identical import paths make them easy to confuse, which is why
both now carry docstrings pointing at each other.

Merging is a behaviour change, not a tidy-up:

* different provider sets — `LLMConfig` covers 11 (incl. deepseek, zhipu,
  moonshot, anthropic, vertex_ai, azure), `AIConfig` covers 6;
* different construction paths — `LLMConfig` is YAML-driven through
  `global_config().get_llm()`; `AIConfig` pairs with `clients.py`'s
  `client_manager` and a hardcoded table;
* consumers are split — `utils/embedding.py` uses the first, everything under
  `utils/llm/` and the file-processing path uses the second.

There are no live-model tests, so a regression here would surface as "this
provider stopped working in production", not as a red test. Needs a
characterisation test per provider (base_url, headers, key source, client class)
recorded BEFORE any merge, then the merge verified against it.


### utils/ audit — the confirmed findings not yet acted on

A 55-agent audit of `utils/` produced 42 findings that survived adversarial
verification (8 were refuted). The security- and correctness-relevant ones are
fixed; these remain, each already verified as real:

**Duplication with divergent behaviour** (consolidating changes output, so each
needs its own commit and characterisation test):

* Retry-with-backoff is hand-rolled independently in at least 3 live paths.
* "Strip a ```json fence, then json.loads" exists 5 times, unshared.
* Filename → MIME lookup is reimplemented in 4 places and **disagrees** on real
  extensions — the reason this cannot be a tidy-up.
* `STRUCTURED_OUTPUT_PRIORITY` (`llm/utils.py`) is a hand-maintained copy of
  `AIConfig._DEFAULT_PROVIDER_PRIORITY`. Worse, provider *validation* and
  provider *dispatch* consult the two different lists, so a provider can
  validate and then fail to dispatch.
* Three near-identical Gemini empty/blocked-response checks in
  `file_processors.py`; the same 5-line AsyncArk client construction three
  times; `_get_qwen_client()` rebuilds byte-for-byte what `client_manager`
  already caches.

**Dead code** (safe to delete, mechanical):

* `PgStore` — 550 lines, unreachable from any live path.
* Roughly half of `AIClientManager` (the whole sync-client family) plus
  `_GlobalClients`/`init_clients`.
* Five `AIConfig` methods; four `RedisConfig` methods; `LoggedConnection`;
  `after_async_sqlarchemy_cursor_execute`; `set_id_decoder`; most of
  `LocalWebSocketManager`; `AbstractStorage.get_content_type` (the ~140-line
  magic-byte sniffer, zero callers).

**Correctness, low impact:**

* `AbstractStorage.get_content_type`'s SVG branch cannot fire — `head` is
  already truncated to 512 bytes above it.
* `_InProcessSubscriber.__getattr__`'s `writer` special-case never fires;
  `writer` is a dataclass field, so `__getattr__` is not reached.
* `normalize.py.__all__` still lists three classes deleted from that module.

**Testability** (the pattern, stated once): `auth.py`, `crypto.py`, `db.py`,
`embedding.py`, `i18n.py`, `log.py` and `s3.py` all reach for `global_config()`
at *call* time. Each is untestable without constructing process-wide config,
and `crypto`/`auth` raise rather than degrade when it is absent. The fix is
constructor injection with the global as a default — worth doing per-module
when each is next touched, not as one sweep.


### pulse/core: the findings that need a live database

Confirmed by audit and reproduced where possible, but each needs a real
PostgreSQL with data to fix *honestly* — the failure modes are all "silently
wrong numbers", which is exactly what you cannot verify by reading:

* **`sql_aggregator._process_data_begin_split_aggregations` drops derived
  methods.** The >5000-task split path does not exclude the GMI and custom
  W2.7 methods the way `_process_data_begin_aggregations` does, so
  `morning_hr_jump`, `nighttime_resting_hr`, `sleep_onset_latency` and
  `gmi_14d` are neither computed nor logged once a `data_begin_utc` bucket
  exceeds MAX_TASKS_PER_SQL. Only large accounts hit it, and they get quietly
  incomplete summaries.
* **`convert_to_standard` fails silent.** An indicator with no conversion rule
  keeps its ORIGINAL unit and value and logs at DEBUG
  (`ingest/services/base.py:66-78`). One row stored in kOhm among mg/dL is
  invisible to every downstream aggregate. Fixing it means choosing between
  raising and quarantining, which changes ingest behaviour — needs a decision
  and a migration for whatever is already stored wrong.
* **The distributed lock fails open.** A process that has never reached Redis
  gets `None` from `get_redis_client()` and `try_acquire_execution_lock`
  fabricates success — so a container restarted during a Redis outage runs
  unlocked. Distinguishing "not configured" (dev, fine) from "configured but
  unreachable" (production, must fail closed) is the fix.
* **Two lookups disagree.** 9 `StandardIndicator` members share a wire-format
  `name`, and `_INDICATOR_LOOKUP` (last wins) resolves
  `sleepAnalysis_Asleep(Deep)` to a member with `aggregation_methods=None`
  while `get_indicator_by_str` (first wins) resolves it to one with
  `['total']`. `standard_unit` happens to match across every colliding group
  today, so nothing is numerically wrong *yet*; the next collision with
  differing units would be. At minimum, make a duplicate `name` fail at import.
* **`CacheableDatabaseService._clear_cache` is never called**, so a 300s TTL
  cache can serve the old name after `update_indicator_name` renames an
  indicator.

Plus ~20 lower-risk items (dead methods, duplicated IN-clause binding,
copy-pasted numeric regexes, a README describing method names that moved).

### Dependency floor: 51 MB of the 77 MB is data

After moving the server stack to `[server]`, the engine install is 90 packages
/ 233 MB, and the theoretical floor — wheel plus numpy — is 2 packages / 77 MB,
of which 51 MB is the shipped LOINC/SNOMED bundle.

Getting below that means the Hugging Face dataset idea already recorded above:
fetch the bundle into `~/.cache/mirobody/` on first use. A further `[parse]`
extra (pandas, Pillow, the PDF stack, the three LLM SDKs) would take the
default install to roughly the floor, but it changes what `pip install
mirobody` delivers — `mirobody parse` would need an extra — so it is a product
decision, not a cleanup.


---

## Verification debt

### `mirobody serve` end to end

Every gate so far is import-level or protocol-level. Route mounting, Agent
Skills reaching the system prompt, the OAuth flow and a real conversation have
never been exercised together. Needs PostgreSQL, Redis and a model key.

### Re-run the a007-mirovital duplication analysis

The "≈55% duplicated, ~24k lines" figures were produced by a Haiku subagent
before the model default was corrected. Those numbers are the basis for the
convergence recommendation, so they should be re-derived before anyone acts on
them.

---

## Distribution and positioning

- **Publish the resolver benchmark.** No public benchmark exists for
  multilingual indicator-name → LOINC resolution. Releasing ours (175 cases,
  scored on clinical correctness, with the 32/94 starting point stated) would
  define the metric for the category. Pairs naturally with the existing
  Hugging Face benchmark account.
- **Ship the LOINC bundle as a Hugging Face dataset** and fetch it into
  `~/.cache/mirobody/` on first use, the way tiktoken and spaCy do. The wheel
  drops from ~46 MB to under 5 MB, the data versions independently of the code,
  and it is one more high-download artifact on the same account. Cost: one
  network round trip on first run.
- **A documentation site.** `docs/` is now structured for MkDocs Material to be
  pointed at it directly.

---

## Found by a cold-start test against real reports (2026-08-17)

A fresh clone, `./deploy.sh`, and the six files in a real 体检 case folder — two
text-layer PDFs, a 9-page pure scan with no text layer, three phone photos of
printed panels. What the run fixed is in the log; what it exposed and did not
fix is here.

- **`名称(缩写)` — done.** Handled as a class in
  `OfflineResolver.resolve`: strip the trailing parenthetical, resolve BOTH
  halves, take the answer only when they agree or only one resolves. So
  `谷丙转氨酶(ALT)` → 1742-6, `总胆红素(TBIL)` → 1975-2,
  `神经元特异性烯醇化酶(NSE)` → 15060-7, while `血糖(HbA1c)` — two different
  tests in one string — stays unresolved.
  **The trap this entry predicted was real and is honoured**: a parenthetical
  containing no letter is a UNIT, not a name, so `中性粒细胞(%)` does not strip
  (`lexical.split_trailing_parenthetical`). Without that guard the stem answers
  the ABSOLUTE-count code while the value is a fraction.
- **Differential percentages: the panel disagrees with itself.** Re-measured
  2026-08-20, and it is not the blanket miss this entry first described — four of
  the five cells already answer a RATIO code, and one does not:

  ```
  淋巴细胞      26478-8  NFr   Lymphocytes/Leukocytes      ratio  ✓
  单核细胞      26485-3  NFr   Monocytes/Leukocytes        ratio  ✓
  嗜酸性粒细胞   26450-7  NFr   Eosinophils/Leukocytes      ratio  ✓
  嗜碱性粒细胞   30180-4  NFr   Basophils/Leukocytes        ratio  ✓
  中性粒细胞     751-8   NCnc  Neutrophils [#/volume]      COUNT  ✗
  ```

  So a five-cell differential lands four ratios and one absolute count, and
  `中性粒细胞 62 %` is filed as a cell count.

  `resolve_reading` cannot fix this and should not: it walks to siblings sharing
  the **full** COMPONENT, and `Neutrophils` and `Neutrophils/Leukocytes` are
  genuinely different measurements — `units/convert.py` refuses `%` ↔ `10*9/L`
  for the same reason.

  The mechanism that would fix it: LOINC writes a ratio into COMPONENT as
  `<numerator>/<denominator>`, so a fraction-family unit (`NFr`/`MFr`) on a
  reading whose code has a `/`-free COMPONENT should look for the
  `<component>/…` sibling, and an absolute-count unit on a ratio code should
  look for the numerator alone. Both directions are determined; neither is
  built. Targets for the forward direction are 770-8, 736-9, 5905-5, 713-8,
  706-2.

- **Imaging narratives resolve to serum enzymes.** A B超 report parses fine, but
  `肝脏` (an organ, with the finding "形态大小正常") answers
  13874-3 *Alkaline phosphatase.liver*, `胰腺` answers *Amylase.pancreatic*, and
  `肾脏` answers *Alkaline phosphatase.renal*. `胆囊` → *US Gallbladder* is the
  only right one. Narrative imaging findings are not lab observations; the
  resolver should decline them (`!unresolved`) rather than reach for an enzyme
  that merely shares the organ name.
- **Indicator extraction fails on a long report and nothing surfaces.**
  `async_get_structured_output` ran 175s against a 12-page panel and came back
  with truncated JSON (`Unterminated string`); with only one provider configured
  there is no fallback, so the file lands as **Processed** in the UI while Drive
  keeps showing `Health indicators 0`. Needs chunking or a length-aware retry,
  and a status the UI can show other than success.
- **The upload pipeline renames files, and the user cannot find their own
  document.** `scanned-9page.pdf` was stored as
  `2026-07-23_陈国跃_急性心肌梗死检查报告.pdf` — a genuinely impressive read off
  a pure scan, and also why the agent answered "I cannot find that file" when
  asked about it by the name the user uploaded. Keep the derived title, but keep
  the original filename addressable too.
- **pdfminer logs at DEBUG.** One PDF upload emits thousands of
  `psparser.nextobject` lines. `logging.getLogger("pdfminer").setLevel(WARNING)`
  at startup.

## Found by auditing the web client against this backend (2026-08-17)

Surfaced while writing the web team's optimisation plan. Recorded here because
they are **backend** defects — they were found from the UI, but no frontend
change can fix them, and a finding that lives only in the other repo's doc is a
finding nobody here will act on.

**Four of the five are FIXED** (`be5a897`): `/api/prompts` is agent-scoped and
echoes the agent back; BaseAgent honours `PROMPTS_BASE`; a user prompt is
appended to the agent's own prompt instead of replacing it. Behaviour verified
against the running deployment, and pinned by
`mirobody/agent/test_prompt_resolution.py`. The prompt-selection design question
they exposed — that a prompt belongs to an agent and should not be a
user-facing axis at all — was resolved on the client side: the shipped web
client no longer offers a prompt picker.

**One remains open:**

- **`/mirobody.json` publishes 3 of the 12 flags the client reads.** It returns
  `__IS_GOOGLE_LOGIN_ON__`, `__IS_APPLE_LOGIN_ON__`, `__IS_WEBAUTHN_ON__`; the
  client's `mirobody_config` also reads `__IS_EHR_CONFIG_ON__`,
  `__IS_API_CONFIG_ON__`, `__IS_HIE_CONFIG_ON__`, `__IS_MOBILE_SOURCE_ON__`,
  `__IS_INDICATOR_ON__`, `__IS_DEVELOPER_ON__`, `__IS_NEW_FEATURES_ON__`,
  `__IS_WX_LOGIN_ON__`, `__WECHAT_APP_ID__`. A missing key is indistinguishable
  from an off switch, so all nine stay off forever. The visible cost:
  `__IS_MOBILE_SOURCE_ON__` gates the client's device-provider UI, so
  **Garmin/Oura/Whoop and Apple Health — the README's headline ① Collect — are
  invisible in the shipped web client.** Derive the set from what is actually
  configured (`pulse/providers/installed.py` already knows which providers
  exist) and emit all of them.
