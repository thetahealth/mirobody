"""The write contract: what it means to put rows somewhere.

A row written twice must mean the same thing the second time, and a batch
that changed nothing must not touch the store. Those two rules — idempotency
and fingerprint-skip — are the whole of what a sink promises; the SQL (or
the file, or the queue) is the consumer's.

``disposition`` names what a collision on the store's natural key means,
the way ``dlt`` spells it: ``replace`` (the new row is the truth), ``merge``
(fill what the new row knows, keep what the old one knew) or ``append``
(never collide; a duplicate is a second row). Pure; stdlib only.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from .series import stable_hash

REPLACE = "replace"
MERGE = "merge"
APPEND = "append"
DISPOSITIONS = frozenset({REPLACE, MERGE, APPEND})


@dataclass(frozen=True)
class WriteReport:
    """What one write did. ``skipped`` counts rows whose fingerprint the
    store already had — the number that should dominate on a re-sync."""

    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    fingerprints: tuple[str, ...] = field(default=())

    @property
    def written(self) -> int:
        return self.inserted + self.updated


class Sink(Protocol):
    def write(self, rows: Sequence[Mapping[str, object]], *, disposition: str) -> WriteReport: ...


def fingerprint(row: Mapping[str, object], fields: Sequence[str]) -> str:
    """A stable hash of the fields that carry meaning (not ``update_time``,
    not an autoincrement id). Two syncs of the same reading produce the same
    fingerprint; a corrected value produces a new one."""
    return stable_hash({f: row.get(f) for f in fields})


def dedupe(
    rows: Iterable[Mapping[str, object]], key: Callable[[Mapping[str, object]], Hashable]
) -> tuple[Mapping[str, object], ...]:
    """Within one batch, the last row per natural key wins — a vendor that
    sends a summary twice in one webhook is not sending two summaries."""
    out: dict[Hashable, Mapping[str, object]] = {}
    for r in rows:
        out[key(r)] = r
    return tuple(out.values())


def changed(
    rows: Iterable[Mapping[str, object]],
    known: Mapping[Hashable, str],
    *,
    key: Callable[[Mapping[str, object]], Hashable],
    fields: Sequence[str],
) -> tuple[tuple[Mapping[str, object], ...], int]:
    """Rows whose fingerprint differs from what the store holds for their
    key, and how many were skipped as unchanged. ``known`` is the store's
    ``key → fingerprint`` for the batch's keys."""
    keep: list[Mapping[str, object]] = []
    skipped = 0
    for r in rows:
        if known.get(key(r)) == fingerprint(r, fields):
            skipped += 1
        else:
            keep.append(r)
    return tuple(keep), skipped


class MemorySink:
    """The reference sink for tests and contract checks: a dict keyed by the
    natural key, honouring the three dispositions."""

    def __init__(self, key: Callable[[Mapping[str, object]], Hashable], fields: Sequence[str]):
        self._key = key
        self._fields = tuple(fields)
        self.rows: dict[Hashable, dict[str, object]] = {}
        self.appended: list[dict[str, object]] = []

    def write(self, rows: Sequence[Mapping[str, object]], *, disposition: str) -> WriteReport:
        if disposition not in DISPOSITIONS:
            raise ValueError(f"unknown disposition {disposition!r}")
        inserted = updated = skipped = 0
        prints: list[str] = []
        for r in rows:
            fp = fingerprint(r, self._fields)
            prints.append(fp)
            if disposition == APPEND:
                self.appended.append(dict(r))
                inserted += 1
                continue
            k = self._key(r)
            cur = self.rows.get(k)
            if cur is None:
                self.rows[k] = dict(r)
                inserted += 1
            elif fingerprint(cur, self._fields) == fp:
                skipped += 1
            elif disposition == REPLACE:
                self.rows[k] = dict(r)
                updated += 1
            else:  # MERGE: the new row fills, never erases
                merged = dict(cur)
                merged.update({kk: v for kk, v in r.items() if v is not None and v != ""})
                self.rows[k] = merged
                updated += 1
        return WriteReport(inserted, updated, skipped, tuple(prints))


__all__ = [
    "APPEND",
    "DISPOSITIONS",
    "MERGE",
    "MemorySink",
    "REPLACE",
    "Sink",
    "WriteReport",
    "changed",
    "dedupe",
    "fingerprint",
]
