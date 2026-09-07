# Medications

> **Status: provisional.** The vocabulary, the arithmetic and the reference
> storage are here and pinned by 74 golden tests. The reference application is
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
one frequency, optional clock times.

| Written | Parsed |
|---|---|
| `500 mg twice daily` | dose 500 mg, `doses_per_day=2` |
| `1 tablet at 08:00 and 20:00` | dose 1 `{tablet}`, `times=("08:00","20:00")` |
| `每天两次，每次一片` | dose 1 `{tablet}`, `doses_per_day=2` |
| `one every other day` | `period_days=2` |
| `q8h` | `doses_per_day=3` (the clock times are NOT invented) |
| `on Monday and Thursday` | `weekdays={1,4}` |
| `as needed for pain` | `as_needed=True`, the reason kept out of `repr` |
| `1-2 tablets` | **`None`** — a range is not a dose |
| `1 in the morning and 0.5 in the evening` | **`None`** — two regimens, not one |
| `with food` | **`None`** — no dose, no frequency |

**No partial parses.** Half a regimen presented as a whole one is worse than no
regimen: the caller keeps the original text either way, and a `None` is visible.

FHIR's `Dosage.text` goes through the same grammar when the structured fields
are empty, which R4B expects and pharmacy systems commonly send.

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
`gap="shift_forward"` moves it to the first instant that exists;
`gap="skip"` leaves `utc_ms=None` and `slot_state` answers `unschedulable`.
Neither is a default — the caller says which, because a blood-pressure tablet
and a contraceptive want different answers.

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

Medications have their own tool, with five parameters — `view`, `keywords`,
`start`, `end`, `member` — because their grammar shares nothing with a
reading's `resolution` and `aggregate` (see [answers.md](answers.md)).

```
query_medications(view="plan")                        what they intend to take, with today's slot states
query_medications(view="log", start=…, end=…)         doses recorded taken/skipped (default: last 30 days)
query_medications(view="history")                     courses: start, end, and why each ended
query_medications(keywords=["metformin"], start="2025-03-01", end="2025-03-31")
                                                      what they were on in March
```

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

`schema/a5_medications.sql`: `th_medication_plan`, `th_medication_course`,
`th_dose_event`, `th_override`.

| Encrypted | In the clear |
|---|---|
| the drug name, the strength, the reason for a skip | `concept_key`, `plan_id`, dates, status, the schedule's STRUCTURE |

The schedule column carries structure only — times, counts, weekdays, dose
amounts. The person's own words live in the encrypted columns.

`pulse/meds/store.py` implements `MedicationStore` and `DoseLogStore` and asks
`mirobody.kernel.meds` every question that has an answer there, rather than
re-deriving one in SQL.

---

## Golden tests

`tests/test_meds.py` — 74 cases across four groups: **G** the grammar, **A**
adherence, **S** schedule projection and DST, **K** identity and FHIR
round-trips. `tests/pulse/apple/test_medications.py` — the import path,
against a synthetic sample that ships with the package.
