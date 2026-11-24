# Medications

> **Status: provisional.** The vocabulary, the arithmetic and the reference
> storage are here and pinned by 77 golden tests. The reference application is
> the first consumer; a second production consumer will find things to move,
> and the shapes below may change in a minor release until one exists. Nothing
> derived is stored, so a change here is a change to code, not a migration.

A medication is **not a reading**, and every design decision on this page
follows from that. A reading has a value, a unit and an instant. A plan has a
schedule (a tuple of dosing instructions), a lifecycle (active → stopped →
resumed as a NEW course) and a code system of its own. Storing "takes metformin
500 mg twice a day" as a reading forces a choice between losing the schedule and
inventing a value, and makes every aggregation query step over rows that are not
measurements.

---

## The model

```
MedicationConcept  what the drug IS         text + codes → concept_key
DoseInstruction    how it is taken          one timing shape, never mixed
MedicationPlan     what the person intends  concept + schedule + dates + status
Prescription       what a clinician ordered separate from what is followed
Course             a period it was followed OMOP drug_exposure
DoseSlot           one planned intake       identity is (plan, local_date, slot)
DoseEvent          what happened            taken | skipped, and nothing else
Adherence          counts over a window     never a stored number
```

### `concept_key` is what gets indexed, joined and logged

```
rxnorm:860975              coded
rxnorm:6809,860975         multi-coded, order-independent
text:920edde59b13e5a5      a hash of the normalised name
```

A coded concept and a text-only one **never compare equal**, even for the same
drug. Auto-merging them is how a wrong code silently rewrites a person's
medication list; two spellings make two plans, the person sees both and deletes
one, which is the safer failure.

The key is safe to log. The NAME is not, and is kept out of every `repr`.

### `normalize_name` is deterministic and convergent only

NFKC folding (so full-width `５００ｍｇ` and `㎎` become ASCII), case folding,
whitespace dropped. **No** alias or brand→generic mapping: that is an
open-ended asset behind the `Terminology` port, and this framework ships none.

---

## Dose units and forms

A dose is an amount in a UCUM unit. Countable forms are UCUM annotations, and
`res/dose_forms.tsv` is the whole table — sixteen forms, with the aliases each
one accepts:

| UCUM annotation | Accepted as |
|---|---|
| `{tablet}` | tablet, tablets, tab, tabs, tbl, 片 |
| `{capsule}` | capsule, capsules, cap, caps, 胶囊, 粒 |
| `{drop}` | drop, drops, gtt, 滴 |
| `{puff}` | puff, puffs, inhalation, inhalations, 吸 |
| `{actuat}` | actuat, actuation, actuations |
| `{spray}` | spray, sprays, 喷 |
| `{patch}` | patch, patches, 贴, 贴片 |
| `{sachet}` | sachet, sachets, packet, packets, 袋, 包 |
| `{suppository}` | suppository, suppositories, 栓 |
| `{ampule}` | ampule, ampoule, ampoules, 支, 瓶 |
| `{vial}` | vial, vials |
| `{pill}` | pill, pills, 丸, 颗 |
| `{lozenge}` | lozenge, lozenges, 含片 |
| `{scoop}` | scoop, scoops, 勺 |
| `{dose}` | dose, doses, 剂 |
| `{application}` | application, applications |

Two notes on the table:

* **`{pill}` and `{tablet}` are different annotations.** They are not merged,
  because a "pill" in a person's words may be a capsule and the framework does
  not decide which. `dose_unit_family` puts both in `count`, so they compare as
  amounts without being claimed to be the same object.
* **`{dose}` and `{application}` carry no amount at all.** They are accepted so
  a schedule can be recorded, and they never enter arithmetic — a "dose" is not
  a quantity a total can be taken of.

---

## The instruction grammar

`parse_dose_instruction(text) -> Schedule | None`. Small and closed: one dose,
one frequency, optional clock times. The frequency grammar is one pattern,
not a list of phrases: prefix (每/一/隔, "every"/"q") + an optional count +
a period (天/日/周/星期, "day"/"week"/"wk"/"hour") + an optional trailing
count + 次/"times". What a phrase resolves to depends on where the count
sits: before the period it multiplies the period's length ("每2日" is every
2 days); after it, for a bare day, it is the per-day count ("每天2次" is
twice a DAY); after it, for a bare week, there is no field to hold it unless
it is one (`Schedule` has an interval and named weekdays, not "N times a
week") — refused rather than guessed, the same rule the per-day count
follows.

English says the same thing the other way round, count first: a count
("3 times", "3x", "three times", "twice") joined to its period ("daily",
"a day", "per day", "/day", "weekly", "a month") is ONE token, read whole.
A count is never a bare number ("2 tablets daily" is a dose of two, once a
day), and the bare "daily"/每天 is once a day only when nothing in the
sentence says how many. When the two were separate phrases, a count the
list did not spell out was skipped and the bare word after it read as once
(#349: "3 times daily" was once daily).

Every word is a whole word: a weekday is "Monday", "Mondays", "Mon",
"Tues", "Thurs" — never a word that starts like one ("month", "monitor",
"sunlight", #350). "Sat", "Sun" and "Wed" are also ordinary words, so
alone they count only after "on"/"every"/"each" or next to another day.

A frequency is read whole or the parse is `None`; what is read never
becomes a smaller regimen. A month or a year is a frequency `Schedule` has
no field for (a month is not 30 days), and so are hours that are not a
whole number of doses a day (every 5 hours), a count with no number
("several times a day", 每天数次), and days offered as a choice ("Mon or
Tue"): each is refused rather than keeping the dose with the frequency
dropped.

| Written | Parsed |
|---|---|
| `500 mg twice daily` | dose 500 mg, `doses_per_day=2` |
| `1 tablet at 08:00 and 20:00` | dose 1 `{tablet}`, `times=("08:00","20:00")` |
| `每天两次，每次一片` | dose 1 `{tablet}`, `doses_per_day=2` |
| `每日3次，每次1片` / `一日3次` / `1日3次` | dose 1 `{tablet}`, `doses_per_day=3` (每天/每日/一天/一日/1日, a digit or 一两二三四, then 次: one token) |
| `500mg 每天早晚` | dose 500 mg, `doses_per_day=2` (早晚 is two a day, 早中晚 three) |
| `one every other day` / `隔天` / `隔日` / `隔一日一次` | `period_days=2`, spelled out or not |
| `每2日1次` / `每两日一次` / `every 3 days` / `q3d` | `period_days=2` / `period_days=3`: the count before the period multiplies it |
| `每2周1次` / `每两周一次` / `every 2 weeks` | `period_days=14` |
| `每周` / `每周一次` / `weekly` / `once a week` | `period_days=7` |
| `q8h` | `doses_per_day=3` (the clock times are NOT invented) |
| `on Monday and Thursday` | `weekdays={1,4}` |
| `as needed for pain` | `as_needed=True`, the reason kept out of `repr` |
| `500 mg 3 times daily` / `1 tablet 2x daily` / `3x/day` / `two times a day` | `doses_per_day=3` / `2` / `3` / `2`: the count and its day are one token |
| `q24h` / `every 24 hours` / `每24小时一次` | once daily (`period_days=1`) |
| `q48h` / `every 72 hours` | `period_days=2` / `period_days=3` |
| `every eight hours` / `q8hr` / `每八小时一次` / `每隔8小时一次` | `doses_per_day=3` |
| `every two weeks` / `every second day` / `on alternate days` / `fortnightly` | `period_days=14` / `2` / `2` / `14` |
| `Mon-Fri` / `Monday through Friday` / `周一至周五` | `weekdays={1,2,3,4,5}`: a range is every day in it |
| `on weekdays` / `on weekends` / `工作日` / `周末` | `weekdays={1,2,3,4,5}` / `{6,7}` |
| `twice a week on Mon and Thu` / `每周一次，周日服用` | `weekdays={1,4}` / `{7}`: the named days restate the count |
| `at bedtime` / `qhs` / `every morning` / `每晚` / `睡前` | once daily; one named time of day qualifies a weekday or period ("every Monday morning") instead of competing with it. Said as when to take it: a bare "evening" ("evening primrose oil") invents no schedule |
| `1 tablet in the morning and at night` / `早上一片，晚上一片` | `doses_per_day=2`: two different times of day are two a day |
| `1 tab t.i.d.` / `bd` / `tds` / `qds` | `doses_per_day=3` / `2` / `3` / `4`, dotted or not |
| `1 tablet at 8 am and 8 pm` | `times=("08:00","20:00")` |
| `5 mg daily, monitor blood pressure` / `1 tablet daily for 3 months` | once daily: "monitor" and "months" are not Monday |
| `1-2 tablets` | **`None`** — a range is not a dose |
| `1 in the morning and 0.5 in the evening` | **`None`** — two regimens, not one |
| `with food` | **`None`** — no dose, no frequency |
| `每日10次` | **`None`** — a count outside 1–4 has no slot shape, so it is refused rather than guessed or read as once daily |
| `每周2次` / `twice weekly` / `2 times a week` | **`None`** — no "N times a week" field unless the weekdays are named; not read as once weekly |
| `每2天3次` | **`None`** — a period and a per-occurrence count other than one, and no field holds both |
| `5 times daily` / `6x daily` | **`None`** — the same 1–4 the zh count accepts |
| `10 mg once a month` / `monthly` / `every 3 months` / `每月一次` / `每年一次` | **`None`** — no field for a month or a year; not a Monday, and not the dose alone |
| `three times a week` / `3x weekly` / `thrice weekly` / `biweekly` | **`None`** — no "N times a week" field (and "biweekly" is both "twice a week" and "every 2 weeks") |
| `twice a week on Monday` | **`None`** — the count and the named days disagree |
| `every 5 hours` / `every 36 hours` / `每8小时2次` | **`None`** — not a whole number of doses a day, nor a whole number of days |
| `several times daily` / `每天数次` | **`None`** — a count with no number in it |
| `1 to 2 tablets` / `once or twice daily` / `每次1到2片` / `每周一到两次` | **`None`** — a range in words is still a range, not its upper bound |
| `Mon or Tue` / `every other Monday` | **`None`** — a choice of day, or a weekday that is not every week |
| `1 tablet daily for 3 days, then twice daily` / `每日1次，然后每日2次` | **`None`** — a step or taper is two regimens, even when the first one names no count |
| `daily except weekends` / `q4h while awake` | **`None`** — an exception no field holds |
| `three times daily at 08:00 and 20:00` / `twice daily in the morning` | **`None`** — the count and the times disagree |
| `每日2次 每天三次` / `每2周1次 每3周1次` | **`None`** — two different named counts or periods, two regimens |

**No partial parses.** Half a regimen presented as a whole one is worse than no
regimen: the caller keeps the original text either way, and a `None` is visible.

FHIR's `Dosage.text` goes through the same grammar when the structured fields
are empty, which R4B expects and pharmacy systems commonly send.

The structured `Timing.repeat` follows the same rule. `frequency` per
`period` in `d`, `wk` or `h` is read when a field holds it: q24h is once
daily, q48h every 2 days, two a day at two `timeOfDay` entries is those two
times, `dayOfWeek` with a one-week period is those days. Anything else —
`frequencyMax`/`periodMax` (a range), two doses every 3 days, twice a week
with no days or with a `timeOfDay`, a weekday list with a period or a bare
count beside it, hours that do not divide a day, a month — leaves the
instruction unscheduled with its text kept, never a smaller regimen.

---

## The state tables

### A plan's stored status vs its effective status

Stored: `active`, `stopped`, `entered_in_error`. Everything else is derived by
`effective_status(plan, today)` — with the subject's local date, never
`date.today()` on a server.

| Stored | Condition | Effective |
|---|---|---|
| `active` | `today < start` | `intended` |
| `active` | `start ≤ today ≤ end` | `active` |
| `active` | `end < today` | `completed` |
| `stopped` | — | `stopped` |
| `entered_in_error` | — | `entered_in_error` |

`entered_in_error` is FHIR's "recorded in error": terminal and retroactive. A
consumer whose "cancel" is reversible maps it to `stopped` with its own flag.

### A dose's state is derived from the clock

Stored: `taken`, `skipped`. That is the entire set.

| Condition | `slot_state` |
|---|---|
| an event answers the slot | `taken` / `skipped` |
| the slot has no instant (a DST gap) | `unschedulable` |
| `now < due` | `upcoming` |
| `due ≤ now ≤ deadline` | `due` |
| `now > deadline` | `missed` |

The deadline is the end of the slot's local day **in the slot's own zone** — the
zone the person was in when the dose was due — or `grace` minutes after the
instant.

*Why nothing derived is stored:* a stored `missed` is a lie the moment the
person opens the app and marks the dose taken.

### Importing a FHIR `MedicationStatement`

| R4B `status` | Becomes |
|---|---|
| `active`, `unknown` **with dates** | active |
| `completed` | active **with an end** — never open-ended |
| `intended` | imported only if its start is in the future; otherwise nothing |
| `on-hold`, `stopped` | stopped, on the asserted date |
| `not-taken`, `entered-in-error`, `unknown` without dates | **nothing** |
| R5 `recorded`, `draft` | `ValueError` — not supported |

**The plan id is never the resource's own `id`.** A FHIR resource id is unique
within the server that issued it; a plan id is unique across every person a
consumer stores. Two people importing documents from the same clinic would
collide and one medication list would overwrite another. `plan_id_for(subject,
record, concept_key)` keeps the idempotency that made the raw id tempting and
scopes it to the subject.

---

## Dose identity is not an instant

`DoseSlot.key` is `(plan_id, local_date, slot)`. `utc_ms` is DERIVED at
projection time.

A person flies from Shanghai to London. Their 08:00 dose is still their 08:00
dose; only the instant changed. Keying on the instant would make the same dose
two different doses, and a re-projection after a zone change would churn every
row.

**Daylight saving.** A slot at 02:30 on a spring-forward day has no instant.
`gap="shift_forward"`, the default, moves it forward by the length of the gap
(to 03:30 when the clocks jump an hour); `gap="skip"` leaves `utc_ms=None` and
`slot_state` answers `unschedulable`. The caller can choose, because a
blood-pressure tablet and a contraceptive want different answers.

---

## Adherence

```
percent = 100 × taken / (taken + skipped + missed)
```

over **elapsed** slots only, to one decimal.

* `None` when nothing has elapsed — an as-needed plan, or a window that has not
  started. Not `0`, which reads as "took nothing".
* `extra` counts taken doses with no slot; `extra_skipped` counts skips with no
  slot, which answer nothing and are reported separately.
* `unschedulable` slots are excluded from every other count.
* Roll-ups across plans **add counts**; they never average percentages.

---

## The tool: `query_medications`

Medications have their own tool, with four parameters — `view`, `keywords`,
`start`, `end` — because their grammar shares nothing with a reading's (see
[answers.md](answers.md)).

```
query_medications(view="plan")                        what they intend to take, with today's slot states
query_medications(view="log", start=…, end=…)         doses recorded taken/skipped (default: the 30 days ending today)
query_medications(view="history")                     courses: start, end, and why each ended
query_medications(keywords=["metformin"], start="2025-03-01", end="2025-03-31")
                                                      what they were on in March
```

`start` and `end` are inclusive local dates, so the dose log's default is the
30 dates ending today, both counted. Built as today minus 30, it read 31 days.

The rows are pure functions in `kernel.meds` — `plan_rows`, `log_rows`,
`history_rows` over the two store ports — so a consumer with its own storage
renders the same table; `agent/tools/medications_service.py` is the thin
binding.

`plan` carries the note "a plan is what the person intends to take; it is not a
record of doses taken", because a model that is not told this reports a
medication list as evidence of what was swallowed. And a dose missing from
`log` is not evidence it was not taken.

---

## Storage (the reference application)

`schema/31_medications.sql`: `th_medication_plan`, `th_medication_course`,
`th_dose_event`, `th_override`.

| Encrypted | In the clear |
|---|---|
| the drug name, the strength, each instruction's own words, the reason for a skip | `concept_key`, `plan_id`, dates, status, the schedule's STRUCTURE |

The schedule column carries structure only — times, counts, weekdays, dose
amounts. The person's own words live in the encrypted columns: "饭后" that no
parser turns into structure is kept in `instructions_text`, one entry per
instruction.

`collect/meds/store.py` implements `MedicationStore` and `DoseLogStore` and asks
`mirobody.kernel.meds` every question that has an answer there, rather than
re-deriving one in SQL.

### From a sentence (`/api/v1/journal/sentence`)

A person does not fill in a form to add a medication. They write "每天早晚吃
二甲双胍500mg" in the journal, the one box of 数据 › 记录, and:

1. `collect/sentence.py` has the model split the sentence and type each part.
   A medication part carries the drug, the dose and the frequency **as
   written**, and whether the person says they take it, stopped it, did not
   take it, or only wonders about it. The model is never asked for a code or
   a schedule.
2. The part becomes a `MedicationMention`, after the checks every part gets:
   it must quote the sentence, and be about the person, asserted.
3. `collect/meds/mentions.py` hands the mentions to `reconcile_mentions`,
   which decides: a drug not on the list is a new plan, unconfirmed and
   marked `source="journal"`, its schedule parsed from the words by
   `parse_dose_instruction` (words it cannot read are kept on the
   instruction); a drug already on the list is left alone; "stopped" stops
   the matching active plan and never creates one.
4. The journal lists the plan on the day it was written, and removing it
   there marks it `entered_in_error`.

The web client's 指标 › 用药 tab lists the plans; saving a correction there
confirms one.

### The HTTP surface (`/api/v1/medications`)

`server/routers/medication_router.py`: listing, correcting and the lifecycle.
`POST /` remains for an API client that has a structured plan in hand.

| Route | Does |
|---|---|
| `GET /` | the plans, minus `entered_in_error`; `?status=` filters on the effective status; `?target_user_id=` reads a family member's with a read grant |
| `GET /{plan_id}`, `GET /{plan_id}/courses` | one plan with its courses |
| `POST /` | a new plan and its opening course, in one transaction; `?target_user_id=` with a write grant |
| `PATCH /{plan_id}` | the fields sent; the rest stay as stored |
| `POST /{plan_id}/stop`, `POST /{plan_id}/resume` | `plan_status_transition`, with the course it closes or opens, in one transaction |
| `DELETE /{plan_id}` | marks it `entered_in_error`; nothing is erased |

Writes need the owner, or a member the owner granted write access; a read
grant lists and nothing more (`403`). The journal applies the same rule when a
caregiver logs "Dad stopped X", so the two ways in cannot disagree. `POST /`
takes `?target_user_id=` for that member's record. A plan the caller may not
read answers `404`, exactly like one that does not exist. An illegal move (stopping a stopped plan)
is the kernel's `ValueError`, returned as `400`.

A plan has at most one open course, found by `closed_by IS NULL`; its id is the
plan, the start and its place in the plan's history, so stopping and resuming on
one day leaves two courses, not one. A start date may change only on an active
plan, and the open course moves with it; a stopped plan's start is its last
course's and stays.

---

## Golden tests

The local suite holds 77 cases across four groups: **G** the grammar, **A**
adherence, **S** schedule projection and DST, **K** identity and FHIR
round-trips, plus the Apple import path against a synthetic sample that ships
with the package. The grammar's public evidence is in `mirobody/tests/`:
`test_meds_per_day_count.py` (#302), `test_meds_frequency_periods.py` and
`test_meds_frequency_grammar.py` (#349, #350: every count spelling crossed
with every day tail, whole-word weekdays and their ranges, months and
years, hours, times of day, Latin abbreviations, word ranges, clock times,
and the FHIR structured `Timing` cases).
