"""A read-only string table: one utf-8 blob plus an int32 offset array.

The resolver's four big tables — 921k alias keys, 677k corpus names, and the
LOINC axis table's nine fields across 97k rows — are all the same shape: a
fixed list of short strings, read a handful at a time, never mutated. Holding
them as Python objects cost about 1.6 million allocations and ~360 MB of the
resolver's resident memory, to serve lookups that touch a few hundred entries
per call.

A blob costs what the text costs, and nothing per entry. Everything here works
on ``bytes`` slices; ``get`` decodes, and callers only call it for the row they
actually answer with.

**Ordering is by utf-8 bytes**, which is the same order as by code point — utf-8
is order-preserving — so a table built from a Python-sorted list bisects
correctly here. ``scripts/build_runtime_index.py`` asserts that the shipped
arrays really are in that order rather than trusting it, because a bisect over
an unsorted table returns some other entry's row instead of failing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np


class StringTable:
    """``n`` strings in one blob. ``off`` has ``n + 1`` entries."""

    __slots__ = ("_blob", "_off", "_n")

    def __init__(self, blob: bytes, off: "np.ndarray") -> None:
        self._blob = blob
        self._off = off
        self._n = len(off) - 1

    def __len__(self) -> int:
        return self._n

    def raw(self, i: int) -> bytes:
        off = self._off
        return self._blob[off[i]:off[i + 1]]

    def get(self, i: int) -> str:
        return self.raw(i).decode("utf-8")

    def find(self, needle: bytes, order: "np.ndarray | None" = None) -> int:
        """Index of *needle*, or -1.

        Without *order* the table is assumed sorted and the answer is the entry
        index. With *order* — an int32 permutation putting the table in sorted
        order — the answer is ``order[i]``, i.e. the ROW the entry belongs to.
        That indirection is what lets one blob carry several sort orders (the
        axis table is bisected by LOINC code and by folded long name) without
        storing the text twice.
        """
        blob, off = self._blob, self._off
        lo, hi = 0, self._n
        if order is None:
            while lo < hi:
                mid = (lo + hi) // 2
                if blob[off[mid]:off[mid + 1]] < needle:
                    lo = mid + 1
                else:
                    hi = mid
            if lo < self._n and blob[off[lo]:off[lo + 1]] == needle:
                return lo
            return -1
        while lo < hi:
            mid = (lo + hi) // 2
            j = order[mid]
            if blob[off[j]:off[j + 1]] < needle:
                lo = mid + 1
            else:
                hi = mid
        if lo < self._n:
            j = int(order[lo])
            if blob[off[j]:off[j + 1]] == needle:
                return j
        return -1


class FieldTable(StringTable):
    """A string table read as ``n`` rows of ``width`` fields, row-major."""

    __slots__ = ("_width",)

    def __init__(self, blob: bytes, off: "np.ndarray", width: int) -> None:
        super().__init__(blob, off)
        self._width = width
        self._n = self._n // width

    def field_raw(self, row: int, field: int) -> bytes:
        return StringTable.raw(self, row * self._width + field)

    def field(self, row: int, field: int) -> str:
        return self.field_raw(row, field).decode("utf-8")

    def row(self, row: int, upto: int) -> tuple[str, ...]:
        base = row * self._width
        return tuple(StringTable.get(self, base + f) for f in range(upto))

    def find_field(self, needle: bytes, order: "np.ndarray", field: int) -> int:
        """Row whose *field* equals *needle*, or -1. *order* sorts rows by it."""
        blob, off, w = self._blob, self._off, self._width
        lo, hi = 0, self._n
        while lo < hi:
            mid = (lo + hi) // 2
            k = int(order[mid]) * w + field
            if blob[off[k]:off[k + 1]] < needle:
                lo = mid + 1
            else:
                hi = mid
        if lo < self._n:
            j = int(order[lo])
            k = j * w + field
            if blob[off[k]:off[k + 1]] == needle:
                return j
        return -1
