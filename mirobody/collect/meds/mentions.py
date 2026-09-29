"""Medications a person wrote about, applied to their plans.

The decision is `kernel.meds.reconcile_mentions`: which mentions are new plans,
which the person already has, which say a drug was stopped. This carries it
out: a new plan is created as the kernel built it (unconfirmed, until the
person edits it) with `source="journal"`, so a list can say where it came
from; a plan the person says they stopped is stopped on `record_date`.

Nothing here reads a request or a clock, so the call that runs where a
sentence is read can later run out of band on the same mentions.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import date

from mirobody.kernel import meds

from .store import PostgresMedicationStore

logger = logging.getLogger(__name__)

SOURCE_JOURNAL = "journal"

#: What became of one mention. Tokens: a client words them.
ADDED = "added"
ALREADY_LISTED = "already_listed"
STOPPED = "stopped"
ALREADY_STOPPED = "already_stopped"
NOT_ON_LIST = "not_on_list"
NOT_STARTED = "not_started"
IGNORED = "ignored"


@dataclass(frozen=True)
class MentionOutcome:
    """One mention's result. `quote` and `name` are the person's words and
    stay out of `repr`, so an outcome can be logged by its action alone."""

    quote: str = field(repr=False)
    name: str = field(repr=False)
    action: str
    plan_id: str = ""
    reason: str = ""


async def apply_medication_mentions(
    subject_id: str,
    mentions: Sequence[meds.MedicationMention],
    *,
    record_date: date,
    source_record_id: str,
    store: PostgresMedicationStore | None = None,
) -> list[MentionOutcome]:
    """Apply one record's mentions to the subject's plans, and say what each
    became. `record_date` is the day the record was written, in the writer's
    zone: a new plan starts then unless the mention names its own start, and
    a stop takes effect then."""
    if not mentions:
        return []
    store = store or PostgresMedicationStore()
    existing = list(await store.list(subject_id))
    decision = meds.reconcile_mentions(
        existing, mentions,
        record_date=record_date, source_record_id=source_record_id, subject_id=str(subject_id),
    )
    first: dict[str, meds.MedicationMention] = {}
    for m in mentions:
        first.setdefault(m.concept_key, m)

    def outcome(key: str, action: str, plan_id: str = "", reason: str = "") -> MentionOutcome:
        m = first.get(key)
        return MentionOutcome(m.quote if m else "", m.name if m else "", action, plan_id, reason)

    def plans_for(key: str) -> list[meds.MedicationPlan]:
        return [p for p in existing if p.concept.concept_key == key]

    out: list[MentionOutcome] = []
    for plan in decision.create:
        plan = replace(plan, source=SOURCE_JOURNAL)
        created = await store.create(plan)
        out.append(outcome(plan.concept.concept_key, ADDED if created else ALREADY_LISTED, plan.plan_id))
    for key in decision.already_known:
        known = plans_for(key)
        active = [p for p in known if p.status == meds.PLAN_ACTIVE]
        out.append(outcome(key, ALREADY_LISTED, (active or known)[0].plan_id if known else ""))
    for key in decision.stopped_known:
        active = [p for p in plans_for(key) if p.status == meds.PLAN_ACTIVE]
        if not active:
            out.append(outcome(key, ALREADY_STOPPED))
            continue
        for p in active:
            try:
                await store.transition(str(subject_id), p.plan_id, "stop", today=record_date)
            except ValueError:
                # The kernel refuses to stop a plan that has not started; a
                # plan starting next week is not what "I stopped it" meant.
                out.append(outcome(key, NOT_STARTED, p.plan_id))
                continue
            out.append(outcome(key, STOPPED, p.plan_id))
    for key in decision.stopped_unknown:
        out.append(outcome(key, NOT_ON_LIST))
    for key, reason in decision.ignored:
        out.append(outcome(key, IGNORED, reason=reason))
    logger.info(
        "medication mentions applied: subject_id=%s created=%d known=%d stopped=%d unknown=%d ignored=%d",
        subject_id, len(decision.create), len(decision.already_known), len(decision.stopped_known),
        len(decision.stopped_unknown), len(decision.ignored),
    )
    return out


__all__ = [
    "ADDED",
    "ALREADY_LISTED",
    "ALREADY_STOPPED",
    "IGNORED",
    "MentionOutcome",
    "NOT_ON_LIST",
    "NOT_STARTED",
    "SOURCE_JOURNAL",
    "STOPPED",
    "apply_medication_mentions",
]
