# Roadmap

Known gaps and deferred work, with the evidence that motivated each. Everything
here was found by measurement rather than intuition, so each item states what
was measured — a future maintainer should not have to rediscover the reasoning.

Ordered by (value ÷ risk) within each section.

---

## Capability gaps

### Reference ranges and abnormal flagging

**Status:** not started. The largest functional gap in ② Sort.

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

### Unit conversion (not just normalization)

**Status:** not started. Half of it already exists.

`normalize_unit` canonicalizes to UCUM and reports the LOINC PROPERTY family —
so we already know that `mg/dL` and `mmol/L` are both `MCnc`/`SCnc` and
therefore *comparable*. We do not convert between them.

The family table is the safety rail that makes conversion tractable: converting
across families is a category error and must be refused. Two rules to carry over
from prior art:

- **Affine units need an offset.** °F ↔ °C is not a multiplication. If a
  conversion has no declared offset and the unit is affine, refuse and warn
  rather than approximating.
- **Unknown unit ⇒ never convert.** Default to a "no conversion" class instead
  of guessing a factor.

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
  multilingual indicator-name → LOINC resolution. Releasing ours (112 cases,
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
