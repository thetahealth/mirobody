# Roadmap

Known gaps and deferred work, with the evidence that motivated each. Everything
here was found by measurement rather than intuition, so each item states what
was measured — a future maintainer should not have to rediscover the reasoning.

Ordered by (value ÷ risk) within each section.

---

## Capability gaps

### Word and PowerPoint uploads — **parsed**

**Status:** done for `.docx` and `.pptx`. Legacy `.doc`/`.ppt` stay out, and that
is the honest boundary rather than a gap.

`handlers/document.py` extracts both to markdown through the same seam Excel
uses — headings keep their level, tables keep their columns and separator row —
and `BaseFileHandler.process` then runs indicator extraction over the result. A
lab report saved as a Word document now gets what a lab report saved as a PDF
gets, and the `Name-ABBREV` cells such tables are full of resolve since
`surface_variants` learned that shape.

python-docx and python-pptx read the zip-based formats only, so `.doc` and
`.ppt` are refused at the gate instead of after the upload. The routing test
pins both directions, and its positive control is now `.doc` — the two previous
choices for that control (`.dwg`, then `.docx`) both stopped being refused,
which is why it is worth keeping one.

### Reference ranges and abnormal flagging

Readings are stored and charted, but nothing marks a value as outside its
reference range — the agent reasons about "high" and "low" from the model's
own knowledge instead of from data. One useful precedent resolves ranges by
`condition → sex → default → union-of-all-sexes`, with that last fallback
existing so an indicator that only has sex-stratified ranges (GGT, for example)
still yields something usable for a user whose sex is unknown. That precedence
chain is worth copying; the data source is the open question.

### Names a report prints that the resolver does not know

**Status:** the shape gaps are closed; four corpus gaps remain.

Running `mirobody parse` over the shipped demo report — the file the README tells
a new user to upload — produced **12 readings and 0 resolved codes**. It is 8/12
now, and the remaining four are a different kind of missing.

Closed:

* **Surface.** The extractor emits `Name-ABBREV` ("Total Cholesterol-TC").
  `surface_variants` offers the hyphen-stripped spelling as an additional
  variant — additive, the term as written still tried first, bounded so
  `High-Density Lipoprotein` and `25-Hydroxyvitamin D3` are untouched.
* **Vocabulary.** Thirteen override rows with their coverage cases: six that were
  WRONG answers inverting the reading (`HDL` → "Cholesterol non HDL", `LDL` and
  `低密度脂蛋白` → an LDL/HDL *ratio*, `高密度脂蛋白` → Lipoprotein.alpha), and
  seven misses the report actually prints (`Blood Glucose`,
  `Cholesterol/HDL Ratio`, `LDL/HDL Ratio`, `Lipid-Free Fatty Acids`,
  `Lipid-Phospholipids`, `Non-HDL Cholesterol-Non-HDL`,
  `Lipid-Low-Density Lipoprotein Calculated`). Benchmark 197 → 211.

Open, and NOT alias gaps — the shipped lexical bundle has no key for these
analytes at all, so an override row would have nothing to point at:

| term | what it needs |
| --- | --- |
| `Postprandial Blood Glucose-PBG` | a 2-hour post-meal glucose code |
| `Lipid-Oxidized Low-Density Lipoprotein` | oxidized LDL |
| `Lipid-Small Dense Low-Density Lipoprotein Cholesterol` | sdLDL-C |
| `Lipid-Low-Density Lipoprotein Particle Number` | LDL-P — a particle COUNT, not LDL-C. The nearest reachable code, 43727-7, is `Lipoprotein.beta.subparticle.small`, which is a different measurement; mapping to it would be a wrong answer of exactly the kind the six above were |

Closing these means adding the terms to `aliases_src/*_curated.tsv` and
rebuilding the bundle, not editing `resolver_overrides.tsv`.

## Structural work

### The corpus build pipeline — **tried splitting it out, reverted**

**Status:** reverted. It stays in `mirobody/indicator/`. What is left open is the
duplication the attempt exposed.

`mirobody/indicator/` is 28,841 lines and the shipped resolver imports ~6.7k of
them; the rest is the pipeline that turns licensed source files into
`mirobody/res/`. That looked like an obvious split — the `[indicator-build]`
extra already named its four dependencies — so it was done: 35 modules to an
`indicator_build/` tree at the repo root, kept out of the wheel by
`packages.find`, with a static boundary test and a `check_wheel_data` gate.

Then it was measured, and the measurements did not support it:

| claim | measured |
| --- | --- |
| gets 22k lines out of the artifact | wheel 24,503,731 → 24,150,062 bytes = **1.4%**. The wheel is 24 MB of LOINC data; the Python is noise. |
| a clean boundary | leaked: 46 lines of `cmd_*` still shipped inside the package, and the build tree imported back into `mirobody.indicator` at 59 sites |
| separates two codebases | 1 identical 8-line block across the two trees — so no copy-paste — but two *parallel implementations*, both older than the split |

The third row is why it was reverted rather than patched. The real duplication is:

* `concept_graph.py` and `taxonomy.py` — opening docstrings identical word for
  word except the noun ("Integer-ID concept graph / taxonomy: build, serialise,
  load, and query. Domain-specific subclasses override … The binary format,
  serialisation, and query API live here."), the same `XBuilder(load_*, load_*,
  build, _save)` + `X(get, _load_bin, …, stats)` pair, the same path-keyed cache
  policy, 17–50% line-level similarity. One design written twice.
* `fhir/adapter.py:resolve_many` (141 lines) against
  `fhir/resolve/pipeline.py:resolve_many` (421 lines) — two answers to "resolve
  these terms". `engine.py` documents this one as deliberate, and it may stay
  deliberate, but it is two implementations either way.

Both pairs pre-date the split (checked at `6c1787d`). Putting them on opposite
sides of a package boundary makes merging them harder, not easier — and a
1.4% artifact win does not pay for that. **Integrate first; split later, if ever.**

### The `concept_graph` / `taxonomy` duplication — **resolved by deletion**

**Status:** done. `taxonomy.py` and `fhir/taxonomy.py` are gone, 643 lines and a
183 KB artifact with them.

Unifying the two into one abstraction was the obvious move and would have been
wrong. `Taxonomy`, the reader half, had **zero importers** — checked by import
graph rather than by grep, because `\bget\b` and `\blabel\b` match unrelated
words all over this package and the grep version of this check reported dozens of
false hits. `TaxonomyBuilder` and `Label` had exactly one importer each
(`fhir/taxonomy.py`, the build path), so the whole chain was a build step writing
`fhir_taxonomy.bin` that nothing in the repo opened. The file was already
excluded from the wheel by `build_backend.py` and asserted absent by
`check_wheel_data.py`; the only thing that named it as an input was
`indicator/README.md`, which claimed a consumer — "`Taxonomy.get` (FHIR API
category view)" — that does not exist here. Our own README is not evidence.

So the duplicated design is gone rather than merged: half of it was dead.

The remaining pair is deliberate and stays. `fhir/adapter.py:resolve_many` (141
lines) is the lite lexical path that ships; `fhir/resolve/pipeline.py:resolve_many`
(421 lines) is the v2 semantic pipeline, which needs a ~200 MB embedding matrix
that is not distributed. `engine.py`'s module docstring already states which one
it is and why the other is not it.

The split attempt also left two real fixes behind: the architecture tree in
`indicator/README.md` was naming a `fhir/test.py` that does not exist, and listed
4 of the package's 12 top-level modules. Both corrected.


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

**Status:** the readable half is done; the moves are not proposed any more.

Renaming `pulse/` to `vendor/` was considered and rejected on measurement, and
the measurement has since changed: with `insight/` and `monitor/` deleted, the
package is ~30.5k lines of which external-source integration is `providers/`
(~6.4k) + `apple/` (~1.2k) + `file_parser/` (~8.5k), the pipeline is `ingest/` +
`standardize/` + `aggregate/` (~10.8k), and `core/` is down from 41% to ~8%.
Naming the whole package after one part of it would recreate the name/content
mismatch this repo has been removing, so the rejection stands.

What was actually wrong was legibility, not the names. The directory listing
sorts `aggregate/` before `providers/`, so the tree shows the pipeline in an
order it does not run in, and nothing said which directories are sources, which
are stages, and which are the floor they stand on. `pulse/__init__.py` and
`pulse/README.md` now both state that in flow order — sources, convergence,
meaning, rollups, with `core/` marked as infrastructure rather than a stage.
Sizes are given rounded (`~6.4k`), because an exact count in prose is stale the
week after it is written and the number is there to show proportion.

That was the whole benefit. Moving `file_parser/` to a `files/` sibling and
grouping the rest under `vendor/` would churn every import path in the package
to communicate what two paragraphs now communicate, so it is not carried here as
pending work.

### ~~`[server]` extra — finish the dependency split~~ (done in 1.3.0; base is `numpy`, extras are `[parse]` and `[app]`)

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

### Sharing: rebuilt as care circles — **done**

**Status:** done. `th_share_relationship`, `th_share_user_config` and
`th_share_permission_type` are dropped; `care_circles` + `care_circle_members`
replace all three.

The old entry here asked for "one write path" for a table that had four. The
table was the problem. `permissions jsonb DEFAULT '{"all": 1}'` is
read-everything, on by default, chosen by the other party — and
`docs/images/your-care-circle.svg`, the diagram the README embeds, promises the
opposite in four places: "acceptance required to join", "health stays off until
you allow it", "your switch — off by default", "mutual — each member controls
their own". The shipped code contradicted the picture on every one.

`care_circle_members.health_access` (0 none / 1 read / 2 read-write) is that
switch: on your own row, about your own record, `DEFAULT 0`, and no other party's
action can raise it. `POST /invitation/health-access` is the endpoint for it —
there was none before, because the concept did not exist.

Also fixed, each of which was its own defect:

* **Denial raises.** `get_query_user_id` returned `{"success": False, …}`; eleven
  callers each had to remember to read that key. `CareCircleDenied` cannot be
  mistaken for success, and one handler in `server.py` turns it into a 403 so a
  route that forgets costs a status code rather than a health record.
* **The parameters mean what they are named.** That function's first parameter
  was named `user_id` and documented as "the data owner", and every one of its
  call sites passed the TARGET there and the caller second, because the SQL
  required it. Its return value was then ignored.
* **The grant is trimmed to the request.** `require_write=False` on a read-write
  membership returns read.
* **`status`** is a SMALLINT with a CHECK, not a varchar whose default
  (`'pending'`) was never the value that authorized anything (`'authorized'`).
* **Real foreign keys** to `health_app_user(id)`, integer to integer. The old
  table held `VARCHAR(50)` ids against an `INTEGER` primary key with no FK, and
  the live database had a soft-deleted account sitting in a circle.
* **Nicknames** are one label per member on the membership row, which is what the
  diagram shows ("mom", "dad"). `th_share_user_config` keyed them by
  (setter, target, context) — one per viewer — with a `context` column nothing
  ever set to anything but `'default'`, and it needed three endpoints.

Seventeen `/invitation/*` endpoints became seven. The seventeen were a symptom of
the directed model: every operation needed a "by me" and a "with me" copy. The
web client calls exactly two of them (`shared-by-me/list`, `shared-by-me/remove`,
from a grep of `frontend/assets/*.js`), and both keep their paths and field names
— `status: "authorized"`, `share_id`, `query_user_id` — mapped at the router from
the integers the table stores. The wire stayed; the storage got fixed.

Verified against a live database end to end: pending is denied, accepted with the
switch off is denied, the owner's switch opens read but not write, the reverse
direction stays closed until that member sets their own switch, a plain member
cannot administer, removal revokes, re-invitation after removal works. Migration
of the dev database's 5 rows produced 1 circle, the owner at `health_access = 1`,
5 members at 0, and dropped the old tables. All three client-facing endpoints
answered over HTTP with a real token, including `/api/beneficiary-users`, which
is the one call the README's demo turns on.

### `user/`: two database access layers — **one rule, one query**

**Status:** done.

The previous version of this entry guessed the split was deliberate: "most
likely multi-statement transactions need the raw pool". Measured, that is true
of three functions and false everywhere else. `execute_query` runs ONE statement
inside its own `engine.begin()`, so anything that must commit together cannot
use it — `add_or_get_user` (SELECT then INSERT-or-UPDATE), `del_user` (two
tables), `account_merge` (an explicit `conn.transaction()`). Every other
raw-pool site was a single SELECT that took an injected `AsyncConnectionPool` to
run it: `webauthn._is_mfa_enabled` fetched one boolean that way. The rule is now
stated at the top of `user/user.py`, and it is about atomicity, not about files.

The retyping was worse than this entry said: not 20 sites but 25, in `user/`,
`server/routers/`, `pulse/core/`, `pulse/file_parser/`, `indicator/` and
`demo/`, each with its own column list. And the cost was not the duplication.
One of the copies — `get_user_info`, the profile the chat layer greets you with
— had no `is_del` filter, so a deleted account still answered with its name,
language and timezone. Twenty-five copies of a predicate is twenty-five chances
to omit it once, and it was omitted once.

`user.get_user(user_id= | email= | apple_sub=)` is the lookup: exactly one
selector, `is_del = false` not expressible as a parameter, email normalized the
way it is stored, string ids accepted (care-circle ids travel as VARCHAR). 19
call sites now use it. `test_user_lookup.py` fails on a new hand-rolled read,
with an `ALLOWED` list where every entry carries its reason — the two
transactions, the `crypt()` password comparison that must stay in SQL, the
find-or-create that deliberately SEES deleted rows so signing up again revives
the account, and the demo seeder's read-back.

Verified against the live database: all 13 columns exist, mixed-case email
matches, unknown returns None — and a soft-deleted probe user became invisible
to `get_user(user_id=)`, `get_user(email=)` and `get_user_info`, which is the
leak this closes.

### Three "what type is this file" implementations — **unified**

**Status:** done. One function, `utils/file_types.guess_mime`.

`utils/s3.get_content_type` (an 8-branch ladder that interpolated
`f"application/{ext}"`), `utils/config/storage/abstract.get_content_type_from_filename`
(bare `mimetypes`) and `agent/deep/filetype.guess_mime` (a small curated table)
all answered the same question and disagreed. `utils/file_types` is where the
shared one lives, because object storage and the presigned-URL helper are engine
layer and cannot import the agent layer.

The deferral in the previous version of this entry was right about the risk and
wrong about the direction. Changing which content-type a file is stored with IS a
live behaviour change — so it was measured first, over the 41 extensions this
project accepts or serves:

- Against this host: **0 differences.** Unifying on the curated implementation
  changes nothing that is currently stored.
- Against `mimetypes.MimeTypes(filenames=())` — the interpreter's built-in table
  alone, which is what a bare container has — **13 differ, 9 of them becoming
  `application/octet-stream`**: `.docx`, `.pptx`, `.flac`, `.m4a`, `.ogg`,
  `.flv`, `.wmv`, `.rar`, `.aac`.

So the real defect was not that the three disagreed with each other. It was that
two of them read the answer from the machine, and `Content-Type` is written into
the object at PUT time — making the build host part of the data. A spreadsheet
uploaded from a laptop opened as a spreadsheet; the same upload in Docker
downloaded as bytes. `MIME_BY_EXT` now pins every accepted-or-served extension,
`mimetypes` is the fallback for the rest, and the legacy `x-` forms
(`audio/x-aac`, `video/x-flv`) are kept rather than modernized to their newer
IANA names — changing one would leave a deployment serving two content-types for
the same extension depending on upload date.

`test_content_type.py` pins all three entry points to the same answers and fails
if an extension is added to `SUPPORTED_EXTENSIONS` or `MULTIMODAL_EXTS` without
being pinned. Also deleted `agent/deep/backend._guess_mime`, a fourth copy with
no callers.

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

### Resolved: `GET /user/settings` no longer performs an UPDATE

It used to force-enable MFA for CommonWell-connected users inside a GET
handler — a mutation in a GET, kept because it was a security control being
applied. The product decision it was waiting on arrived: this project is not
connected to CommonWell, and `commonwell_patient` is a table no baseline here
creates, so the three queries reading it could only ever raise
(`execute_query` re-raises). The control was not protecting anything; it was a
guaranteed 500 the moment `WEBAUTHN_RP_ID` was configured.

All three sites are gone, along with the `cw_connected` field. The shared
frontend keeps its HIE surface for the deployments that do use it and degrades
on its own: `security?.cw_connected` reads `undefined`, so the MFA switch is
simply enabled — the same graceful-degradation idiom as the opensource paths in
`Indicators/index.jsx` and `FileTable/index.jsx`. Nothing here is feature-gated
on an integration this project does not ship.


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
an `AsyncOpenAI`. They overlap on openai / openrouter / dashscope /
volcengine. The near-identical import paths make them easy to confuse, which is why
both now carry docstrings pointing at each other.

Merging is a behaviour change, not a tidy-up:

* different provider sets — `LLMConfig` covers 11 (incl. deepseek, zhipu,
  moonshot, anthropic, vertex_ai, azure), `AIConfig` covers 4 (the
  OpenAI-compatible ones: openai, openrouter, dashscope, volcengine) plus
  gemini in its auto-selection order;
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
* `STRUCTURED_OUTPUT_PRIORITY` (`utils/llm/utils.py`) is a hand-maintained copy of
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

Historical: after moving the server stack out of base, the engine install was 90 packages
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

---

## Distribution and positioning

- **Publish the resolver benchmark.** No public benchmark exists for
  multilingual indicator-name → LOINC resolution. Releasing ours (211 cases,
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

## Found by an external security review (2026-08-18)

A reviewer deployed the stack with `./deploy.sh` and one OpenRouter key, drove
it end to end, and audited the source. Every finding below was re-verified here
before being acted on — one turned out to be worse than reported, and the
counts in the review predate this branch.

**Fixed on this branch**, each with a regression test:

| Finding | Commit |
| --- | --- |
| `GET /files/{key}` served PHI with no authentication (confirmed live) | `require auth and ownership to read an uploaded file` |
| `?folder=` traversal → arbitrary file write outside the storage root | `stop an upload from choosing where on disk it lands` |
| Care-circle `share_id` authorize had no ownership predicate (IDOR) | `a care-circle share may only be authorized by a party to it` |
| OAuth `redirect_uri` never validated → auth-code theft | `validate redirect_uri against what the OAuth client registered` |
| Login code: no attempt cap, and reusable in the Redis branch | `make a login code single-use and cap how often it may be guessed` |
| `data-distribution?user_id=` unauthorized cross-user read | `authorize the data-distribution route like the one beside it` |
| Encryption key derivation erased the secret it could not encrypt | `stop an unusable encryption key from erasing the secret it cannot encrypt` |
| `pip install -e '.[test]'` aborted at collection | `make pip install -e '.[test]' run the tests it claims to` |
| CI installed pytest and never ran it | `run the tests in CI` |

The file-route fix has a client half, and it is done: the shipped web client
reached files four ways a browser sends no `Authorization` header on — an
`<a href>`, `window.open`, and two `<img src>` — so all four would have 401'd.
It now fetches with the session token and renders from a blob, keeping the
token out of the URL and forcing a non-executable MIME type (a `blob:` URL
inherits the page origin, and `.svg` is an uploadable extension). Already
rebuilt into `frontend/`.


Three remain open. None is a defect in code that exists; each is a feature that
does not, and two need a client change to be useful — which is why they are
here rather than half-built.

### The personal MCP URL cannot be revoked

`/mcp/<secret>` is bearer authority in a URL: whoever has the string is the
user, for 365 days. There is no revoke, no rotate, and re-generating returns
the SAME value, so a URL leaked through browser history, a screenshot, a shell
history file or a proxy log cannot be taken back by the person it belongs to.
Without Redis it degrades further to an unbounded in-process dict with no TTL
at all.

The fix is a `POST /personal/mcp/revoke` that invalidates the current secret and
mints a new one, plus a shorter default lifetime. It needs a UI affordance in
the same change or nobody will find it, which is the part this repo cannot do
alone — the web client ships as a build artifact from another repository.

### User-defined MCP servers are write-only

`/api/user/mcp/*` stores, lists and deletes user-configured MCP servers, and
`get_user_mcps` is called by exactly three functions: the list handler, the set
handler and the delete handler. **Neither agent's tool loader ever reads it.**
Compare `get_user_prompt_by_name` right beside it in the same module, which
`deep_agent.py:259` genuinely consumes.

So a user can add an MCP server in Settings, see it listed back, and nothing
ever connects to it. That is worse than the feature being absent: it looks like
it works.

Two honest ways to close it, and the choice is a product one:

  - load the enabled entries in `deep/tool_loader.py` alongside the built-in
    MCP tools, with a per-server timeout and failures degrading to "that server
    is unavailable" rather than failing the turn; or
  - delete the three endpoints and the UI that feeds them.

Not deleted unilaterally here because the endpoints are live API the shipped
web client calls.

### The config encryption key needs a real KDF, and that is a migration

`get_fernet_key` base64s the passphrase padded to 32 bytes. The byte/character
bug in it is fixed and the empty-passphrase fallback no longer fails silently,
but the derivation is still not a KDF: no salt, no iteration count, and a
passphrase shorter than 32 bytes is padded with a known constant.

PBKDF2HMAC-SHA256 with a stored salt is the right answer and cannot be dropped
in: every config already encrypted under the current derivation would become
unreadable. It needs a versioned key header (`v2$<salt>$<ct>`), a read path
that accepts both, and a one-shot `mirobody config reencrypt`. Worth doing;
worth doing as its own change.

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

---

## Found while recording the README walkthroughs in four languages (2026-08-23)

Twenty GIFs — five scenes × four READMEs — meant driving the shipped client in
简体中文, 繁體中文 and 日本語 rather than only English. Three defects showed up
that no English pass could have surfaced. All three are in the **web client**,
whose source is a separate checkout; fixing them means rebuilding the bundle,
which is why they are recorded rather than patched here.

- **A greeting that wraps is clipped and cannot be scrolled to.** The new-
  conversation screen centres its content with `justify-center` inside a
  `min-h-[366px]` box. In English and Chinese the greeting is one line and fits;
  in Japanese (「こんにちは、今日は何をお手伝いしましょうか？」) it wraps to two,
  the content becomes taller than the box, and flex centring pushes 40px above
  the scroll container's top — where `scrollTop` is already 0, so it is
  unreachable at any window height. Measured, not guessed: `greet.top = 111.5`
  against `scroller.top = 152`. The fix is `justify-start` (or `margin: auto`)
  once the content overflows.

- **`Analyzing...` and `Running tool: <name>` stay English in every locale.**
  They are the only untranslated strings in an otherwise fully localized screen,
  and they are on screen for the whole time the agent is working — which is most
  of what a walkthrough shows.

- **The upload help text still omits Word and PowerPoint.** It lists “PDF、画像、
  Excel/CSV、音声、テキスト/Markdown、遺伝子の生データ（txt）” — accurate before
  `handlers/document.py`, stale now that `.docx` and `.pptx` are parsed.

One finding on this side, already fixed here: the localized recordings were
first encoded at the terminal demo's pace (0.22 s per frame), which is right for
a typing animation and far too fast for a screenful of UI. Each localized GIF now
holds each frame for as long as its English sibling does — 1.5 s for
`care-circle`, 1.9 s for `upload`, 2.0 s for `ask-circle`, 1.76 s for `ask-own`.
