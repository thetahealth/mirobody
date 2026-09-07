"""The reference application's medication storage — `mirobody.kernel.meds` on Postgres.

`mirobody.kernel.meds` is the vocabulary and the arithmetic: what a plan is, how a
schedule projects into dose slots, what state a slot is in at a given instant,
how adherence is counted. It has no tables, on purpose — a consumer with an
EHR behind it, a consumer with a phone-local SQLite and this repository's own
reference application all need the same rules and none of the same storage.

This package is one implementation of the two ports (`MedicationStore` and
`DoseLogStore`) against the schema in `schema/a5_medications.sql`. It is the
first consumer of the medication kernel, and it is deliberately thin: every
question that has an answer in `mirobody.kernel.meds` is asked there, never
re-derived in SQL.
"""

from .store import PostgresDoseLogStore, PostgresMedicationStore, PostgresOverlayStore

__all__ = ["PostgresDoseLogStore", "PostgresMedicationStore", "PostgresOverlayStore"]
