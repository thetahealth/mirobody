# Roadmap

Known gaps and deferred work, with the evidence that motivated each. Everything
here was found by measurement rather than intuition, so each item states what
was measured — a future maintainer should not have to rediscover the reasoning.
A closed item is deleted from this page: what changed is in CHANGELOG.md, and
how it was found is in git history.

Ordered by (value ÷ risk) within each section.

---

## Capability gaps

### Explicitly cancel a running chat turn

**Status:** open. The web client's Stop now aborts HTTP reception and isolates
late callbacks by question ID. `agent/chat/turn.py::stream_with_heartbeat`
deliberately leaves `_accumulate` running after a client disconnect so an
answer can still be saved. There is no turn-cancellation endpoint, so this
does not stop model computation; a history refresh can reveal the answer later.
An explicit cancellation command needs authenticated turn ownership, cancellation
of model/tool work, persistence semantics for partial turns and serialization
with the next turn's checkpoint writes. Do not implement it by treating every
network disconnect as a user's cancellation request.

### Genetics beyond the bundled pharmacogene index

**Status:** design questions. The shipped scope and its reasons are in
[genetics.md](genetics.md); each extension below needs evidence that the
bundled public sources cannot give.

- **Whole-chip mapping** needs redistributable manufacturer marker lists. The
  builder already verifies and reads NCBI's b157 bulk files (58.6 GB
  compressed). The two PGP exports that selected the bundled sites are
  observed marker sets, not manifests, and the index covers 262 of 625,705 and
  340 of 677,436 of their rsIDs. `th_genotype_set.chip_fingerprint` is
  reserved for recording which array produced an upload.
- **Star alleles, diplotypes and phenotypes** need phase, copy number and
  complete allele definitions, and agreement with the CDC GeT-RM consensus
  panel before a result could be shown. On the two-site public export,
  PharmCAT 3.4.0 lists candidate CYP2C19 diplotypes rather than one.
  `th_pgx_result` is reserved for such results.
- **Rare pathogenic interpretation** needs a curated truth set. The tools
  report array calls without a clinical classification.
- **CPIC currency.** The bundled v1.60.0 extract is pinned and `mirobody fetch
  cpic` installs a newer release, but an answer does not say how old the
  selected release is.
- **Multi-sample VCF.** The parser reads one named sample
  (`genotype_format.records(..., sample=...)`), but the upload has no field
  that names the person's sample, so such files are refused.

### Reference ranges and abnormal flagging

**Status:** the printed half is closed (1.5.4). A reading keeps the range
its report printed and a flag: `query_health_indicators` returns `ref`, the
range as printed, and `flag`, the report's own when a table rule read the row,
else the extracting model's high/low/normal against that range (CHANGELOG,
"Readings carry the printed range and a flag"). Open: a reading whose source
printed no range (a device, a CSV without one) has none, and the agent then
judges it from the model's own knowledge. One useful precedent resolves ranges by
`condition → sex → default → union-of-all-sexes`, with that last fallback
existing so an indicator that only has sex-stratified ranges (GGT, for example)
still yields something usable for a user whose sex is unknown. That precedence
chain is worth copying; the data source is the open question.

### Names a report prints that the resolver does not know

**Status:** the shape gaps are closed; four corpus gaps remain, and a real
hospital report shows a fifth kind.

Measured 2026-09-18 on a 2 MB Chinese hospital check-up PDF through the running
stack: 102 indicators extracted, and of the 110 catalogue entries afterwards
**51 carried a LOINC code and 59 did not**. Free-text ultrasound impressions
belong in the second group and are right to be there. The lab analytes are not:
`Absolute Neutrophil Count` resolves to nothing, while the Chinese the report
actually printed, `中性粒细胞绝对值`, resolves to 26499-4. So does `Neutrophils`
(751-8) and `NEUT#`. The code is lost in the extraction step, which renders the
analyte name in English, and in a phrasing no alias carries: `Neutrophil Count`
and `Absolute Neutrophils` miss too. Five of the eight differential counts go
this way. Either the extractor keeps the source spelling for resolution, or
these phrasings become aliases; a row in `resolver_overrides.tsv` fixes one at
a time.

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

### Four tables with no ingestion path

`health_data_epic`, `health_data_oracle`, `health_data_libre` and
`health_vital_webhook` have a schema and nothing writes them.
`account_merge.py` lists them, so they are not dead code. Resolve them
alongside whichever integration lands first, and never drop a table without
an explicit migration: it is irreversible for anyone holding data.

### `collect/` internal layout

**Status:** the readable half is done; the moves are not proposed any more.

Renaming the package after one part of it was considered and rejected on
measurement. The package is `collect/` now, which is the stage name, and the
rejection reads better than it did: external-source integration is
`providers/` (Apple included since 1.4.4) plus `files/`, the pipeline is
`ingest/` + `aggregate/`, and `core/` is down from 41% to ~8%. Standardization
left the package entirely in 1.4.4 and is now `mirobody/standardize/`, ②
Standardize's own stage.

What was actually wrong was legibility, not the names. The directory listing
sorts `aggregate/` before `providers/`, so the tree shows the pipeline in an
order it does not run in, and nothing said which directories are sources, which
are stages, and which are the floor they stand on. `collect/__init__.py` and
`collect/README.md` now both state that in flow order — sources, convergence,
meaning, rollups, with `core/` marked as infrastructure rather than a stage.
Sizes are given rounded (`~6.4k`), because an exact count in prose is stale the
week after it is written and the number is there to show proportion.

That was the whole benefit at the time. Half of it was reversed in 1.4.4:
`file_parser/` is `files/` now, because the cost changed. The objection was
churn across every import path, and 1.4.4 gave `collect` a front door
(`mirobody.collect` re-exports what the answer layer needs, held by an
import-linter contract), so a rename inside the package no longer reaches any
caller outside it. Grouping the rest under `vendor/` stays rejected, for the
reason above: naming the package after one part of it.

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

### A dose taken, said in the journal

"刚吃了一片布洛芬" becomes a medication plan, not a dose event in
`th_dose_event`: the journal has no way yet to say "one dose, not an ongoing
medication".

### Empty-list/dict parameter defaults (~19 remaining)

An AST pass confirmed none is mutated in place, so these are latent rather
than live. A blanket conversion to `None` was attempted and reverted: it
touched 105 sites across 43 files and broke callers that iterate the
parameter directly. Worth doing per-signature when a file is being edited for
another reason, not as a sweep.

### utils/ audit — what is left

A 55-agent audit of `utils/` produced 42 findings that survived adversarial
verification (8 were refuted). Rechecked 2026-09-29: the dead code it listed is
deleted (`PgStore`, the sync client family, `LoggedConnection`,
`set_id_decoder`, the magic-byte sniffer and the rest), the MIME lookups are one
(`utils/file_types.guess_mime`), and the Gemini, Ark and Qwen client copies went
with their providers. Two remain:

* "Strip a ```json fence, then json.loads" is still hand-written in
  `collect/files/services/file_abstract_extractor.py` and
  `utils/llm/file_processors/results.py`, beside the shared
  `utils/llm_output.py` helper.
* **Testability**: `server/auth.py`, `utils/db.py` and
  `utils/distributed_lock.py` reach for `global_config()` at *call* time, so
  each is untestable without constructing process-wide config, and `auth`
  raises rather than degrades when it is absent. The fix is constructor
  injection with the global as a default, per module when each is next touched.

### collect/core: the findings that need a live database

Confirmed by audit and reproduced where possible, but each needs a real
PostgreSQL with data to fix *honestly* — the failure modes are all "silently
wrong numbers", which is exactly what you cannot verify by reading:

* **`convert_to_standard` fails silent.** Spelling variants now convert
  through `mirobody.units`; a unit that does not convert is kept as given, with
  no log line at all (`collect/ingest/services/upload_health.py:_to_standard_unit`).
  One row stored in kOhm among mg/dL is invisible to every downstream
  aggregate. Fixing it means choosing between raising and quarantining, which
  changes ingest behaviour — needs a decision and a migration for whatever is
  already stored wrong.

Plus ~20 lower-risk items (dead methods, duplicated IN-clause binding,
copy-pasted numeric regexes, a README describing method names that moved).

## Verification debt

### OAuth `state` is bound to the account, not to the browser

`generate_authorization_url` stores `state` → user id and the callback takes it
once (`collect/providers/_platform/oauth2.py`). Nothing ties the callback to the
browser that started the link, so a consent page started by one account and
completed by another user puts that user's vendor tokens in the first account
(RFC 6749 §10.12). SECURITY.md tells operators so.

The fix is a short-lived HttpOnly, `SameSite=Lax` cookie that
`POST /user/providers/link` sets and the callback checks, for OAuth2 and for
Garmin's OAuth1 alike. It is not in yet because it changes every vendor's link
flow, and verifying it takes one real authorization per vendor, which no test
here can make. A client that starts the link in one user agent and finishes it
in another (an app opening the system browser) needs a different binding.

### Rejected readings have nowhere to go

`collect/observations.py:prepare` refuses a row the quality gate rejects; the
reason code and a count land on the extraction row and in the log. That is the
right record and still the wrong destination:
a person whose scale sent a 150% body-fat reading, or whose export carried a
48-hour "measurement", has no way to see that anything was refused. A
quarantine table keyed by `(user_id, reason_code)` with the offending row's
encrypted payload, and a line in the Data page, is the missing half. Until it
exists the gate is deliberately narrow — only the physically impossible — so
that what it drops is never something a person would want back.

### The aggregation worker's statistics, against a live series

`standardize/aggregate` computes about twenty statistics in SQL — percentiles,
time-in-range, CGM event detection, the derived sleep-onset methods. 1.4.0 put
the DAY BOUNDARY and the SOURCE ELECTION on the kernel and left those
statistics where they were, deliberately: routing them through
`series.aggregate`, which knows five policies, would have lost functionality.

They have unit tests and no end-to-end one. `series_data` is empty in the demo
record, so neither the rewritten trigger queries nor the statistics run against
real rows in the shipped fixture — only against a deployment's own. A demo
fixture that seeds `series_data` would close both at once.

### Medications have one consumer

The model, the arithmetic and the reference storage are pinned by 74 golden
tests, and the reference application is the only thing that reads them. The
shapes are marked `provisional` for that reason (`docs/medications.md`); a
second production consumer is what would settle them.

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
  and a status the UI can show other than success. **The extraction half is
  closed (Unreleased):** text over 3,000 characters with page headers is read a
  page at a time (78 of 78 rows of the 7-page check-up book MiniCPM5-2B had
  answered with none), a request is bounded by its text, and an answer cut at
  that bound keeps every value that closed before the cut. Not re-measured: the
  status a file shows when its extraction still fails.
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

A reviewer deployed the stack with `./deploy.sh` and one OpenRouter key,
drove it end to end, and audited the source. The nine defects it found were
fixed when it was filed, each with a regression test (the commits name
them). Two remain open, and both are migrations rather than defects: rotating
the database-content key, and the config encryption key's derivation.

### `PG_ENCRYPTION_KEY` cannot be rotated

Every field `encrypt_content()` covers is encrypted with one key, and nothing
re-encrypts them under a new one. A Compose stack first deployed with 1.5.2,
and a native install (`scripts/install_native.sh`) from 1.5.7 to 1.5.9, used
`config.yaml`'s placeholder, and upgrading keeps it, because a new key would
leave those fields unreadable. The fix is a command that reads each
covered column with the old key and writes it with the new one in one
transaction per table, and `deploy.sh` offering it on upgrade.

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
echoes the agent back; BaseAgent honoured `PROMPTS_BASE` (BaseAgent has since
been removed — `MirobodyAgent` is the one agent, see CHANGELOG 1.4.0); a user
prompt is appended to the agent's own prompt instead of replacing it. Behaviour verified
against the running deployment, and pinned by the local suite. The
prompt-selection design question
they exposed — that a prompt belongs to an agent and should not be a
user-facing axis at all — was resolved on the client side: the shipped web
client no longer offers a prompt picker.

**The fifth is FIXED too** (CHANGELOG 1.4.0 → Changed): `/mirobody.json`
now derives `__IS_MOBILE_SOURCE_ON__` from the installed provider directories,
puts `MCP` in `__IS_NEW_FEATURES_ON__` and turns `__IS_API_CONFIG_ON__` on, so
the six flags the shipped client reads are all present. The original finding,
kept for the record:

- **`/mirobody.json` published 3 of the 12 flags the client reads.** It returned
  `__IS_GOOGLE_LOGIN_ON__`, `__IS_APPLE_LOGIN_ON__`, `__IS_WEBAUTHN_ON__`; the
  client's `mirobody_config` also reads `__IS_EHR_CONFIG_ON__`,
  `__IS_API_CONFIG_ON__`, `__IS_HIE_CONFIG_ON__`, `__IS_MOBILE_SOURCE_ON__`,
  `__IS_INDICATOR_ON__`, `__IS_DEVELOPER_ON__`, `__IS_NEW_FEATURES_ON__`,
  `__IS_WX_LOGIN_ON__`, `__WECHAT_APP_ID__`. A missing key is indistinguishable
  from an off switch, so all nine stay off forever. The visible cost:
  `__IS_MOBILE_SOURCE_ON__` gates the client's device-provider UI, so
  **Garmin/Oura/Whoop and Apple Health — the README's headline ① Collect — are
  invisible in the shipped web client.** Derive the set from what is actually
  configured (`collect/providers/installed.py` already knows which providers
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

## Found by the backend quality pass (2026-10-07)

Twelve reviews read the backend line by line before 1.5.4 shipped, and what they
proved was fixed in the same release (CHANGELOG, 1.5.4). These were found too
and left open, each for the reason given.

**Vocabulary: refused rather than coded, each needing its own rule.**
- HbA1c printed in mmol/mol (IFCC) is refused: its code, 59261-8, has its own
  COMPONENT, so the unit cannot select it from the % code's row.
- A prothrombin time printed as a ratio in `%` (actual/normal) is refused:
  admitting `%` as RelTime would also admit it for INR, where it is wrong.
- An INR printed with the unit `1` is refused.
- Spirometry names resolve to PREDICTED codes: `FEV1`, `FEV1%`, `FEV1/FVC` and
  `Forced expiratory volume in 1 second` to 19925-7 (FEV1/FVC Predicted),
  `FVC` and `forced vital capacity` to 19869-7 (FVC Predicted). The measured
  codes are 20150-9 (FEV1), 19926-5 (FEV1/FVC), 20152-5 (FEV1 % of predicted)
  and 19870-5 (FVC), and only the last two have an index key a
  `resolver_overrides.tsv` row could point at. Until they do, `%` does not
  admit `VRto` or `RelVol` (`units/families.py`), so `FEV1 85 %` is refused
  rather than coded to the predicted ratio.

**Records.**
- `observations.amend` replaces a row's note with its `note` argument, which
  defaults to "", so correcting a symptom or a lab row drops its note.
- `StandardPulseRecord.comment` (a meal's details, for one) is never stored:
  notes are kept only for file, API and demo rows.
- Deleting an account soft-deletes the account row; its observations and
  `series_data` stay.
- `th_coding_decision.unit_ucum` is "" for a decision whose id includes the
  printed unit, which every decision for a unit that did not normalize has.

**Devices.** Each needs a real vendor account to verify, which the pass had not.
- Garmin's `activities` and `activityDetails` both decode the same activity,
  under different summary ids, so its active calories and steps may be counted
  twice; whether the two ids ever match is a question for real payloads.
- WHOOP's default scope still asks for `read:profile`, which nothing reads
  since profiles stopped being pulled. Narrowing it changes the scope sent on
  every refresh of an existing link, untested against WHOOP.
- A Garmin deregistration's raw row stores its (revoked) `userAccessToken`.
- The Apple CDA reader's `vital_signs` and `lab_results` sections are stubs that
  return nothing, and Apple's `%` statistics are passed through as given, where
  the push path found HealthKit fractions.

**Agent.** `EmptyAnswerRepairMiddleware` removes its nudge once an answer
arrives, which edits the prefix the answer's thinking was generated on;
`agent/middleware/model_budget.py` records that current Anthropic models refuse
an edited prefix. Unverified: it needs an empty answer from such a model with
thinking on, which is rare.

**A llama-server on the host takes no key.** The Mac and Windows guides run
`llama-server` on the host, where it answers without a key and allows every
CORS origin: any web page the person opens can call `localhost:8080`, load a
14.5 GB model or unload the running one. The `local` entries carry no
`api_key`, so starting it with `--api-key` would refuse Mirobody too; a
`LOCAL_API_KEY` the entries send is the missing half (external newcomer
review, 2026-10-07). The Compose services publish no port and are not
affected.

**Uploads.** `.svg` and `.markdown` are accepted, but an SVG is never readable
by OCR, and a Markdown file is read as text only when the client declares
`text/markdown`. `th_files.file_content` is updated read-modify-write, so two
writers at once can lose a key.

**Small-model calculation origin remains open (2026-10-21).** The local Python
tool now receives original record envelopes instead of reparsing compact
tables, and all chat models share it. In eight synthetic questions with two
repetitions per language, DeepSeek Flash, Claude Sonnet 5.5 and GPT-6 Luna
passed all original-input and changed-input calculations. MiniCPM5-2B produced
plausible answers but neither its Python nor JavaScript code passed the
changed-data replay (0/16 per arm): it repeatedly copied literal readings.
Do not treat the older 16/16 number-matching probe as proof that this model
computes the record. Prefer the engine's existing statistics/bucket views
and keep the strict source-calculation gate when evaluating the small model.
Evidence: [code-tool comparison](../benchmarks/local_agent/eval_language_results.md).
