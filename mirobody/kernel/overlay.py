"""Corrections as a layer over stored rows — the original row is never rewritten.

A user fixes a mis-parsed reading, a reviewer changes a medication's dose, a
caregiver marks an entry deleted: every one of these used to be an ``UPDATE``
on the row, and every one of them was silently undone the next time the
source re-pushed the same record. The invariant this module carries is that
a correction is a *separate fact about a row* — who changed which field to
what, when — and the corrected view is computed by laying those facts over
the row at read time. Re-pushing the source changes the row and leaves the
corrections standing; removing a correction restores the source value.

The same shape serves readings (the Correct stage of the pipeline) and
medication plans (per-field edits with an audit trail). Storage is the
consumer's: this module only knows the override record and how to apply a
set of them. Pure; stdlib only.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

#: The field name that marks a row deleted. A deletion is an override like any
#: other, so it is audited, reversible and survives a source re-push.
DELETED = "deleted"


@dataclass(frozen=True)
class Override:
    """One correction: ``field`` of the row ``target_id`` reads ``value``.

    ``at_ms`` orders overrides of the same field; ``seq`` breaks ties between
    two written in the same millisecond (a batch edit). ``actor`` is an
    opaque identifier — ``"user:…"``, ``"reviewer:…"``, ``"system"`` — never
    a name or email. ``value`` is any JSON-compatible value; ``None`` means
    "cleared".
    """

    target_id: str
    field: str
    value: object
    at_ms: int
    actor: str
    reason: str = ""
    seq: int = 0

    def __post_init__(self) -> None:
        if not self.target_id or not self.field:
            raise ValueError("Override needs a target_id and a field")


def tombstone(target_id: str, *, at_ms: int, actor: str, reason: str = "", seq: int = 0) -> Override:
    """The override that deletes a row."""
    return Override(target_id, DELETED, True, at_ms, actor, reason, seq)


def latest(overrides: Iterable[Override]) -> dict[str, Override]:
    """The winning override per field: the latest ``(at_ms, seq)``."""
    out: dict[str, Override] = {}
    for o in overrides:
        cur = out.get(o.field)
        if cur is None or (o.at_ms, o.seq) > (cur.at_ms, cur.seq):
            out[o.field] = o
    return out


def apply(row: Mapping[str, object], overrides: Iterable[Override]) -> dict[str, object]:
    """``row`` with every overridden field replaced by its latest override.

    The caller passes the overrides of *this* row (see :func:`by_target`);
    a mismatched ``target_id`` is a programming error and raises. A
    tombstone sets ``row[DELETED] = True``; nothing is removed from the
    mapping, so a reader can still show "deleted by …" from the audit.
    """
    overrides = list(overrides)
    ids = {o.target_id for o in overrides}
    if len(ids) > 1:
        raise ValueError(f"overrides for {len(ids)} different rows passed to apply()")
    over = latest(overrides)
    out = dict(row)
    for field, o in over.items():
        out[field] = o.value
    return out


def is_deleted(overrides: Iterable[Override]) -> bool:
    o = latest(overrides).get(DELETED)
    return bool(o and o.value)


def by_target(overrides: Iterable[Override]) -> dict[str, list[Override]]:
    """Group a store's overrides by the row they belong to."""
    out: dict[str, list[Override]] = {}
    for o in overrides:
        out.setdefault(o.target_id, []).append(o)
    return out


def audit(overrides: Iterable[Override]) -> tuple[Override, ...]:
    """The change history, oldest first — what an "edited" badge expands to."""
    return tuple(sorted(overrides, key=lambda o: (o.at_ms, o.seq, o.field)))


__all__ = ["DELETED", "Override", "apply", "audit", "by_target", "is_deleted", "latest", "tombstone"]
