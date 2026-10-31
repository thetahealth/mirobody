# Medication records

Mirobody keeps medication plans separate from measurements. A plan describes
intended use; a dose log records what a person reported taking or skipping.

## Add and review a plan

Record medication use in the journal, including the name, dose and frequency.
For example, a synthetic entry could say “Example medicine, one tablet twice
daily.” Review the resulting plan in **Indicators › Medications** and correct
its details. Unrecognized instructions remain as text rather than becoming a
partially guessed schedule.

Stopping a medication closes its current course. Resuming it opens a new
course, preserving the history. Removing a journal-created plan marks the
record as entered in error.

## Query medication data

| `query_medications` view | Use |
| --- | --- |
| `plan` | Plans in effect during the requested window. |
| `log` | Recorded taken or skipped doses; defaults to the last 30 days, today included. |
| `history` | Courses, their dates and closure information. |

Use `keywords`, `start` and `end` to narrow the query:

```python
query_medications(view="plan", keywords=["<recorded medication name>"])
query_medications(view="log", start="2026-10-01", end="2026-10-21")
```

A plan is not proof that doses were taken. A dose missing from the log is not
proof that it was skipped. Due, upcoming and missed states depend on time and
recorded events; they can change when a person updates the log.

## Integration

The application exposes plan management under `/api/v1/medications`.
Writes require the owner's permission, or a care-circle write grant;
a read grant does not permit changes. Check your deployment's `/docs`
for current request schemas.

The medication model is provisional. Developers needing schedule grammar,
status transitions or FHIR details should use the
[medication reference](../medications.md).
