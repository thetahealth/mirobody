# The pipeline: what happens to a reading

A number arrives — from a watch, a lab report, a phone, an API call — and
eleven things happen to it before anyone can ask a question about it. This page
is those eleven, each with the invariant it holds and the failure it prevents.

Written in two columns throughout: **what is implemented** and **what is
deliberately not**. A pipeline document that only describes the happy path is
how a reader comes to believe a boundary does not exist.

---

## The stages

| # | Stage | What it is | Where |
|---|---|---|---|
| 1 | **Connect** | a credential, its state machine, when to stop retrying | `kernel/connect.py` |
| 2 | **Pull** | ask the vendor for a window; keep the raw payload | `collect/providers/` |
| 3 | **Decode** | vendor JSON → `series.Fact`, in the catalogue's units | `kernel/decoders/` |
| 4 | **Resolve** | which metric IS this — name, code, shape | `kernel/metrics.py`, `engine.py` |
| 5 | **Gate** | reject the impossible; convert what is convertible | `kernel/quality.py`, applied in `collect/observations.py` |
| 6 | **Store** | one writer, one day column, one fingerprint, the coding beside the row | `collect/observations.py`, `mirobody/translate/` |
| 7 | **Aggregate** | a day of points → one number, under the metric's policy | `kernel/series.py`, `translate/aggregate/` |
| 8 | **Elect** | which source the day publishes from | `series.elect`, `translate/aggregate/election.py` |
| 8b | **Derive** | quantities nothing measured: sleep efficiency, HR range | `translate/derive/rules.py` |
| 9 | **Correct** | a person's edit, as a layer over the row | `kernel/overlay.py` |
| 10 | **Read** | one authority, one window semantics | `kernel/query.py` |
| 11 | **Answer** | one tool, an envelope, a model that narrates | `agent/tools/` |

---

## 1. Connect

**Implemented.** A credential is a small state machine (`linked → refreshing →
expired → revoked`) with a failure counter. `connect.record_failure` /
`may_attempt` back off after each authorization failure and stop entirely after
a threshold; the reference pull loop consults them
(`collect/providers/_platform/base.py`).

*The failure this prevents:* a person changes their vendor password, every tick
of the loop then fails, forever, at the loop's full rate — and some vendors read
that as an attack and lock the account the person is still using.

**Not implemented.** Token refresh, OAuth dances, secret storage. Those are the
deployment's, and `connect` never sees a secret — only that one failed.

## 2. Pull

**Implemented.** `connect.PullWindow`, `RawBatch` and `backfill_windows` (a long
history chunked into bounded windows, so one backfill cannot become one request
for four years). Each shipped provider keeps its raw payloads in its own table.

**Not implemented.** Scheduling, webhooks, multi-account fan-out, distributed
locks. This is a library and a reference application, not a sync platform —
see `docs/provider-guide.md` for when to put open-wearables in front of it.

## 3. Decode

**Implemented.** `decoders.decode(vendor, data_type, item, tz)` → `list[Fact]`,
pure. A decode table maps a vendor's field path to a catalogue metric and a
function that converts into **the catalogue's** unit — the unit itself is never
written in the table, so a decoder cannot disagree with the aggregator about
what `ms` means.

*The failure this prevents:* three repositories carried three copies of the same
translation inside three IO classes, each fixing bugs the others still had. One
of them had a kJ→kcal conversion the wrong way round.

**Not implemented.** Guessing. A metric key the catalogue does not know returns
nothing and is quarantined; it is never mapped to a neighbouring name.

## 4. Resolve

**Implemented.** `metrics.mapping_for(system, key)` answers name, canonical
`(system, code)` — LOINC where the code is public and undisputed, this
project's own namespace otherwise — `state_class`, `aggregation_policy` and the
day `window`. Free text resolves through `engine.resolve` (the offline
921k-alias index).

**Not implemented.** A confident code for a metric whose LOINC is disputed. The
catalogue carries 14 public LOINC codes and says `mirobody-device` for the rest,
because a confident wrong code is worse than an honest namespace: two writers
who both lack a code still agree, and nobody's chart silently merges two tests.

## 5. Gate

**Implemented.** `quality.time_gate` (no start, an end before its start, a span
over 36 hours, a measurement a day in the future) and `value_gate` (a
percentage outside 0–100, a non-finite number) run on every row inside the
one writer, `collect/observations.py:prepare`, before it is bound; a rejected row
is counted by reason code, on the extraction row and in the log, and not
written. `reconcile_unit`,
`overcount_suspect` and `is_echo` are the kernel's other gates: the first is
what a decoder uses to land a value in the catalogue's unit, the last two are
the aggregation pass's (`translate/aggregate/election.py` rejects an impossible
duration candidate rather than ranking it).

**Not implemented.** A quarantine table. A rejected row is dropped with its
reason code in the log; nothing stores it for a person to look at. That is the
honest gap, and it is in `docs/roadmap.md`.

Every rejection carries a reason CODE, never a free-text exception: a
quarantine you can only grep is a pile the next engineer cannot triage.

**Not implemented.** Reference ranges. "Heart rate 190 is high" is an
open-ended clinical asset with a maintenance cost and a locale; this library
maintains none. Only the physically impossible is rejected.

**Not implemented.** A marker for which readings were unit-converted.
`collect/ingest/services/base.py` converts a device reading's unit with
`translate.convert_to_standard`, and keeps the original value and unit when the
conversion raises, but neither path is tagged. `kernel/quality.py` computes a
`unit_converted` flag (`FLAG_UNIT_CONVERTED`) for exactly this purpose and
nothing downstream stores it, so a converted reading and one that arrived
already in the catalogue's unit are the same shape in `th_series_data` today.
`mirobody/translate/__init__.py` carries the plan to separate them.

## 6. Store

**Implemented.** One writer (`collect/observations.py`) for the observation
model (`mirobody/schema/30_observations.sql`): `th_observation` holds
what was printed, verbatim, beside the typed layer derived from it, and
`th_coding_current` holds the code and the series it belongs to. Derived at
write time, by `mirobody.translate`, and never again:

| Column | From | Why at write time |
|---|---|---|
| `local_date` | `translate.local_day` through the metric's `window` and the row's own `tz` | the day is a fact about the reading, and computing it at read time means computing it differently in each reader |
| `value_kind`, `value_num`, `unit_ucum` | `translate.parse_value` | a number is compared as a number, "阴性" is never averaged |
| `series_id` | the LOINC axes, or `local:<name>\|<unit>` | what may be plotted on one axis, across units and across people |
| `stream_key` | `name\|unit\|source:vendor` | two devices' curves must never be averaged together |
| `source_class` | the provenance | election's first criterion |
| `fingerprint` | the verbatim fields and the time | a re-sync that changed nothing writes nothing |

The table is append-only. A correction or a retraction is a new row that
points at the old one (`amends`), and the read view hides the old one; the
only DELETE is the privacy path. Election writes `th_day_authority`, its own
table, never a flag on the fact row.

*The failure this prevents:* the old table's `start_time` was a naive local
wall clock, so "which day is this" was answered at read time by casting it
and padding the window a day each way (a June window returned a May 31
bucket), and its standardization was a second UPDATE on the hot table, which
is where every production deadlock on it came from.

**Stated, not hidden.** A deployment upgraded in place keeps its history in
`th_series_data_retired_15` (`90_retire.sql` renames, never
drops) and answers from an empty catalogue until `mirobody
migrate-observations` has moved the rows through the same writer. A migrated
file reading carries `note_text = migrated:th_series_data`, because its name
was written in the user's language by the old extractor rather than as
printed.

## 7. Aggregate

**Implemented.** `series.aggregate(policy, facts, day, tz, window)` collapses a
window under one of five policies, and `metrics.LEGAL_POLICIES` refuses an
illegal pairing — a step count cannot be summarised as "the last delta of the
day".

| `state_class` | Policy | Because |
|---|---|---|
| `instant` | `mean_min_max` or `last` | a heart rate is a sample |
| `cumulative` | `sum_delta` | steps are deltas; the mean of running totals is meaningless |
| `interval` / `session` | `duration_union` | overlapping spans must not be counted twice |
| `provider_daily` | `provider_value` | the vendor already computed the day; project it, do not average it again |

The reference application's aggregation worker keeps its SQL for the twenty-odd
statistics it computes (percentiles, time-in-range, CGM event detection); the
kernel owns the day boundary, the shapes and the policies.

*What changed:* the trigger queries split their input with `LOWER(indicator)
LIKE '%sleep%'`. That matched 58 `daily…Sleep…` metrics which are
`provider_daily` — the vendor's own figure, already dated — and re-anchoring
those moved every one a day; and it missed `napDuration`, which is a real
interval belonging to the night. The catalogue's `window` answers both
(`translate/aggregate/windows.py`).

**Suggested extension.** `series.sessionize(segments, gap_ms)` — stitching stage
fragments into one night before `duration_union` — is designed and not built.
Today a night arrives as spans and `flatten_last_writer_wins` handles the
overlap.

## 8. Elect

**Implemented.** Two devices measure the same day and both are right about
themselves. `series.elect` ranks the sources — measurer over profile echo, then
coverage, then the MEASUREMENT instant (never the row's update time: an echo is
rewritten daily and looks fresh), then the deployment's own priority list — and
`translate/aggregate/election.py` marks the winner's rows `elected`.

A candidate is **rejected** rather than ranked when its numbers are impossible:
a total sleep time longer than the night it was measured in is arithmetic, not
opinion. When every candidate fails, the day keeps what it published and the
reason codes are logged.

*The failure this prevents:* election used to happen on the read side, in two
places, so a chat answer and a dashboard could show a person two different
numbers for the same Tuesday.

**Not implemented.** Blending two sources into one number. The elected source's
value is published as it was measured; a mean of two devices is a third number
no device recorded.

## 9. Correct

**Implemented.** `overlay.Override` — who changed which field, to what, when.
The stored row is never rewritten and a deletion is an override like any other,
so re-pushing the source changes the row and leaves the correction standing.
`th_override` is the reference application's append-only table.

*The failure this prevents:* every correction used to be an `UPDATE`, and every
one was silently undone the next time the source re-pushed the same record.

**Not implemented.** A correction UI. The vocabulary and the table are here; the
product decision about who may correct what is the consumer's.

## 10. Read

**Implemented.** One `query.HealthQuery` behind every surface. Windows resolve
in the subject's zone with an explicit `now`; day-grained reads take the elected
authority; the semantics of the window are REPORTED (`tz_exact` /
`date_padded_naive`) rather than assumed.

**Not implemented.** An events tool. A data class with no store, no port and
no consumer does not get a tool; it enters when a second consumer defines what
a clinical event is.

## 11. Answer

**Implemented.** One tool per data class — `query_health_indicators` (eight
parameters) and `query_medications` (five) — each with one schema on both
surfaces, a closed schema that turns an unknown parameter into a structured
refusal, and a `tools.Envelope` that carries status, provenance and
truncation beside the rendered text. Governance reads the envelope, never the
prose. See [answers.md](answers.md).

**Not implemented.** A judgement. The tool reports; the model narrates. Nothing
in this pipeline decides whether a number is good news.

---

## The invariants, in one list

1. A reading's day comes from the catalogue's window, decided once, at write time.
2. A metric's unit comes from the catalogue; a decoder converts INTO it and never declares it.
3. An unmapped metric is quarantined, never guessed.
4. Election happens once, before publication, and a day has one authority.
5. Overlapping spans are unioned, never summed.
6. A vendor's own daily figure is projected, never re-averaged, never summed with its own detail.
7. A time is never fabricated: a record without one decodes to nothing.
8. A correction is a layer; the stored row is not rewritten.
9. Every answer states which window semantics produced it.
10. A log line carries ids, counts, durations, status codes and type names — never a value.

Each one is a test in the maintainers' local suite, one module per subject:
series, quality, metrics, translate, observations, election, decoders and the
PHI baseline.
