"""The citation registry (`kernel.citations`): short rids the model cites,
and the map back to the rows a number came from.

Library only: a `RidTable` is dicts and a counter; the module-level scope
store is what the readings tool mints through (`_attach_rids`), keyed by the
record being read.
"""

from __future__ import annotations

import pytest

from mirobody.kernel import citations


def test_mints_sequential_short_ids() -> None:
    table = citations.RidTable()
    assert table.rid_for(("row", "11"), ("11",)) == "r1"
    assert table.rid_for(("row", "12"), ("12",)) == "r2"
    assert table.rid_for(("agg", ("11", "12")), ("11", "12")) == "r3"


def test_same_key_keeps_one_rid() -> None:
    table = citations.RidTable()
    first = table.rid_for(("row", "11"), ("11",))
    assert table.rid_for(("row", "11"), ("11",)) == first
    assert len(table) == 1


def test_supporting_is_recorded_at_mint_and_unknown_resolves_empty() -> None:
    table = citations.RidTable()
    rid = table.rid_for(("agg", ("11", "12")), ("11", "12"))
    assert table.supporting(rid) == ("11", "12")
    assert table.supporting("r999") == ()


def test_table_name_collides_with_no_other_shape() -> None:
    # A raw row and an aggregate over that one row are different keys: the
    # aggregate's sorted-ids key never equals the row-id key by construction.
    table = citations.RidTable()
    raw = table.rid_for(("row", "11"), ("11",))
    agg = table.rid_for(("agg", ("11",)), ("11",))
    assert raw != agg


def test_eviction_drops_least_recently_shown_and_never_lies() -> None:
    table = citations.RidTable(max_entries=2)
    gone = table.rid_for(("row", "11"), ("11",))
    table.rid_for(("row", "12"), ("12",))
    table.rid_for(("row", "13"), ("13",))  # evicts r1
    assert table.supporting(gone) == (), "an evicted rid must resolve to unknown, never to another row"
    reminted = table.rid_for(("row", "11"), ("11",))
    assert reminted != gone
    assert table.supporting(reminted) == ("11",)


def test_repeat_mint_does_not_move_supporting() -> None:
    table = citations.RidTable()
    rid = table.rid_for(("row", "11"), ("11",))
    # A caller re-minting with a different value must not move an
    # already-cited rid to other rows; a key determines its rows.
    assert table.rid_for(("row", "11"), ("99",)) == rid
    assert table.supporting(rid) == ("11",)


@pytest.fixture()
def _clean_scopes():
    saved = dict(citations._tables)
    citations._tables.clear()
    yield
    citations._tables.clear()
    citations._tables.update(saved)


def test_scopes_mint_independently(_clean_scopes) -> None:
    assert citations.table_for("u1").rid_for(("row", "11"), ("11",)) == "r1"
    assert citations.table_for("u2").rid_for(("row", "11"), ("11",)) == "r1"
    assert citations.citation_support("u1", "r1") == ("11",)
    assert citations.citation_support("missing-scope", "r1") == ()


def test_scope_store_is_bounded(_clean_scopes, monkeypatch) -> None:
    monkeypatch.setattr(citations, "MAX_SCOPES", 3)
    citations.table_for("u1").rid_for(("row", "11"), ("11",))
    for scope in ("u2", "u3", "u4"):
        citations.table_for(scope)
    # u1 fell out of the store: its rids resolve to unknown, never to a
    # different scope's rows.
    assert citations.citation_support("u1", "r1") == ()
    assert citations.citation_support("u2", "r1") == ()  # never minted here
