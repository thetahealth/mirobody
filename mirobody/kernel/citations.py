"""Citation ids for a tool's rows: the short `rid` a model cites, and the map
back to the rows it stands for.

A model can only cite what it can name. A reading's `row_id` and `file_key`
are database handles: long, and a model handed only those cited one verbatim
as a value's source ("web_uploads/17eaf4f6-…-edbee3267ea6.pdf"; the fix then
gave rows a human `file` name). An aggregate row — a `stats` line, a
day/week/month bucket — has no row id at all, although every number in it
came from specific rows. So each row the readings tool shows carries a short
`rid` (r1, r2, …), and this module holds the map from a rid to the ids of the
rows behind it: the one row for a reading, the contributing row ids for an
aggregate — exactly the rows the SQL counted (`collect/query.py`'s
`ARRAY_AGG`), never a re-derived list that could disagree with the number.

Two properties, and how they hold:

* **Stable within a conversation.** Numbers mint in first-seen order, keyed
  by what the row IS (its `row_id`, or the sorted supporting ids of an
  aggregate), so the same row keeps one rid no matter how many calls show it.
  Scope is the record being read: one conversation reads one record
  (`_authz`), so a scope IS the conversation from the model's side.
* **Short.** r3, not a uuid. The model view costs a handful of tokens per
  table, not per row.

The tables live in process memory and are never persisted: a verifier checks
an answer's citations against the same process the tool ran in
(`citation_support`), which includes the eval REPL's `tools.<name>` calls —
they run in the agent's process. A restart restarts the numbering, and no
saved answer may present a rid as a permanent handle.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Hashable, Iterable

#: Rows of one scope kept resolvable. One conversation mints hundreds, in the
#: pathological case a few thousand (92 bucket rows x series x the turn's call
#: budget), so 2**16 is a wide margin; past it the least-recently-shown rid is
#: dropped and its row re-mints if shown again — a stale rid then resolves to
#: nothing rather than to the wrong row.
MAX_SCOPE_ENTRIES = 1 << 16
#: Scopes (records) kept at once; the oldest untouched goes. Self-hosting
#: means a handful; a busy shared server re-mints for evicted scopes, with the
#: same right-side failure (unknown, never wrong).
MAX_SCOPES = 512


class RidTable:
    """The mint and the reverse map for one scope. `key` identifies a row
    (its `row_id`, or an aggregate's sorted supporting ids); `supporting` is
    the row ids the verifier resolves the minted rid to, and is recorded only
    on first mint: a key determines its supporting rows by construction, so a
    repeat mint is always the same answer."""

    def __init__(self, max_entries: int = MAX_SCOPE_ENTRIES) -> None:
        self._max_entries = max_entries
        self._by_key: OrderedDict[Hashable, str] = OrderedDict()
        self._support: dict[str, tuple[str, ...]] = {}
        self._next = 0

    def rid_for(self, key: Hashable, supporting: Iterable[str] = ()) -> str:
        """The rid for `key`, minting `r<n>` on first sight."""
        rid = self._by_key.get(key)
        if rid is not None:
            self._by_key.move_to_end(key)
            return rid
        if len(self._by_key) >= self._max_entries:
            old_key, old_rid = next(iter(self._by_key.items()))
            del self._by_key[old_key]
            del self._support[old_rid]
        self._next += 1
        rid = f"r{self._next}"
        self._by_key[key] = rid
        # Support is frozen at mint (see the class docstring): recording a
        # repeat's value would silently move an already-cited rid to new rows.
        self._support[rid] = tuple(supporting)
        return rid

    def supporting(self, rid: str) -> tuple[str, ...]:
        """The ids of the rows behind `rid`; () when the rid is unknown here."""
        return self._support.get(rid, ())

    def __len__(self) -> int:
        return len(self._by_key)


_tables: OrderedDict[str, RidTable] = OrderedDict()


def table_for(scope: str) -> RidTable:
    """The mint for one scope (the record a conversation reads), kept
    least-recently-touched-out."""
    table = _tables.get(scope)
    if table is None:
        if len(_tables) >= MAX_SCOPES:
            _tables.popitem(last=False)
        table = _tables[scope] = RidTable()
    else:
        _tables.move_to_end(scope)
    return table


def citation_support(scope: str, rid: str) -> tuple[str, ...]:
    """The ids of the table rows a model's cited `rid` stands for in this
    process: one id for a reading row, the contributing ids for a stats or
    bucket row. () is "unknown rid" — evicted, another process, or made up;
    a verifier must treat all three as unverifiable, not as wrong."""
    table = _tables.get(scope)
    return table.supporting(rid) if table else ()


__all__ = [
    "MAX_SCOPE_ENTRIES",
    "MAX_SCOPES",
    "RidTable",
    "citation_support",
    "table_for",
]
