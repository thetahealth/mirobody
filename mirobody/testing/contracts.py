"""Invariants any ``mirobody.kernel.sink.Sink`` implementation must hold, runnable
against a real store (with a fixture) or the in-memory reference.

The four things a sink promises: a re-sent batch touches nothing; ``replace``
makes the new row the truth; ``merge`` never erases what the old row knew;
``append`` never collides. A consumer wires its own writer in and calls
:func:`check_sink`; the returned list of violations is empty when the sink
keeps its promises. Pure; stdlib only.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Mapping, Sequence

from ..kernel.sink import Sink


def check_sink(
    make_sink: Callable[[], Sink],
    *,
    row: Mapping[str, object],
    key: Callable[[Mapping[str, object]], Hashable],
    read: Callable[[Sink, Hashable], Mapping[str, object] | None],
    mutable_field: str,
    optional_field: str,
) -> list[str]:
    """Run the contract. ``row`` is one valid row for the store; ``read``
    fetches the stored row by natural key; ``mutable_field`` is a field whose
    change must be visible after ``replace``; ``optional_field`` is a field
    the ``merge`` disposition must keep when the new row leaves it empty."""
    problems: list[str] = []
    k = key(row)

    s = make_sink()
    first = s.write([row], disposition="replace")
    if first.inserted != 1:
        problems.append(f"first write inserted {first.inserted}, expected 1")
    again = s.write([row], disposition="replace")
    if again.inserted or again.updated or again.skipped != 1:
        problems.append(f"re-sending the same row should skip it, got {again}")
    stored = read(s, k)
    if stored is None or stored.get(mutable_field) != row[mutable_field]:
        problems.append("stored row does not match the written row")

    changed = {**row, mutable_field: _bump(row[mutable_field])}
    rep = s.write([changed], disposition="replace")
    stored = read(s, k)
    if rep.updated != 1 or stored is None or stored.get(mutable_field) != changed[mutable_field]:
        problems.append("replace did not make the new row the truth")

    hollow = {**changed, optional_field: None}
    s.write([hollow], disposition="merge")
    stored = read(s, k)
    if stored is None or stored.get(optional_field) != row[optional_field]:
        problems.append("merge erased a field the new row did not know")

    before = read(s, k)
    app = s.write([row], disposition="append")
    if app.inserted != 1 or read(s, k) != before:
        problems.append("append must add a row without touching the keyed one")
    return problems


def _bump(value: object) -> object:
    if isinstance(value, bool):
        return not value
    if isinstance(value, int | float):
        return value + 1
    return f"{value}'"


def check_dispositions_are_closed(sink: Sink, row: Mapping[str, object]) -> list[str]:
    """An unknown disposition must be refused, never defaulted."""
    try:
        sink.write([row], disposition="upsert")
    except ValueError:
        return []
    return ["an unknown disposition was accepted"]


__all__ = ["check_dispositions_are_closed", "check_sink"]


def _unused(_: Sequence[object]) -> None:  # keeps Sequence imported for type hints in consumers
    return None
