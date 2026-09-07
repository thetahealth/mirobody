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
| 2 | **Pull** | ask the vendor for a window; keep the raw payload | `pulse/providers/` |
| 3 | **Decode** | vendor JSON → `series.Fact`, in the catalogue's units | `kernel/vendors/` |
| 4 | **Resolve** | which metric IS this — name, code, shape | `kernel/metrics.py`, `engine.py` |
| 5 | **Gate** | reject the impossible; convert what is convertible | `kernel/quality.py`, applied in `pulse/readings.py` |
| 6 | **Store** | one writer, one day column, one fingerprint | `pulse/readings.py` |
| 7 | **Aggregate** | a day of points → one number, under the metric's policy | `kernel/series.py`, `pulse/aggregate/` |
| 8 | **Elect** | which source the day publishes from | `series.elect`, `pulse/aggregate/election.py` |
| 9 | **Correct** | a person's edit, as a layer over the row | `kernel/overlay.py` |
| 10 | **Read** | one authority, one window semantics | `kernel/query.py` |
| 11 | **Answer** | one tool, an envelope, a model that narrates | `agent/tools/` |

---

## 1. Connect

**Implemented.** A credential is a small state machine (`linked → refreshing →
expired → revoked`) with a failure counter. `connect.record_failure` /
`may_attempt` back off after each authorization failure and stop entirely after
a threshold; the reference pull loop consults them
(`pulse/providers/platform/base.py`).

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

**Implemented.** `vendors.decode(vendor, data_type, item, tz)` → `list[Fact]`,
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
one writer, `pulse/readings.py:gate`, before it is bound; a rejected row is
counted in the log by reason code and not written. `reconcile_unit`,
`overcount_suspect` and `is_echo` are the kernel's other gates: the first is
what a decoder uses to land a value in the catalogue's unit, the last two are
the aggregation pass's (`pulse/aggregate/election.py` rejects an impossible
duration candidate rather than ranking it).

**Not implemented.** A quarantine table. A rejected row is dropped with its
reason code in the log; nothing stores it for a person to look at. That is the
honest gap, and it is in `docs/roadmap.md`.

Every rejection carries a reason CODE, never a free-text exception: a
quarantine you can only grep is a pile the next engineer cannot triage.

**Not implemented.** Reference ranges. "Heart rate 190 is high" is an
open-ended clinical asset with a maintenance cost and a locale; this library
maintains none. Only the physically impossible is rejected.

## 6. Store

**Implemented.** One writer (`pulse/readings.py`) for `th_series_data`. It
derives four columns at write time:

| Column | From | Why at write time |
|---|---|---|
| `local_date` | the metric's `window` | the day is a fact about the reading, and computing it at read time means computing it differently in each reader |
| `series_key` | `indicator\|source` | two devices' curves must never be averaged together |
| `source_class` | `task_id` / `source_table` | election's first criterion |
| `fingerprint` | the meaningful fields | a re-sync that changed nothing does not touch the row |

*The failure this prevents:* `start_time` is a naive local wall clock, so "which
day is this" was answered at read time by casting it and padding the window a
day each way. A June window returned a May 31 bucket.

**Not implemented (stated, not hidden).** Rows written before
`a4_series_data_day_authority.sql` have no `local_date` until the boot backfill
reaches them. Those are found by the padded window, and every answer that
touches one reports `window_semantics="date_padded_naive"` instead of claiming
an exact date.

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
(`pulse/aggregate/windows.py`).

**Suggested extension.** `series.sessionize(segments, gap_ms)` — stitching stage
fragments into one night before `duration_union` — is designed and not built.
Today a night arrives as spans and `flatten_last_writer_wins` handles the
overlap.

## 8. Elect

**Implemented.** Two devices measure the same day and both are right about
themselves. `series.elect` ranks the sources — measurer over profile echo, then
coverage, then the MEASUREMENT instant (never the row's update time: an echo is
rewritten daily and looks fresh), then the deployment's own priority list — and
`pulse/aggregate/election.py` marks the winner's rows `elected`.

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

Each one is a test. `tests/test_series.py`, `tests/test_quality.py`,
`tests/test_metrics.py`, `tests/pulse/test_readings.py`, `tests/pulse/aggregate/test_election.py`,
`tests/test_vendors.py`, `tests/test_phi_baseline.py`.
