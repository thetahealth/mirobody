"""A result that mixes series renders one compact table per series, but only
where that is shorter than the one table.

Why: `query.compact` hoists what is constant across a whole table, yet
indicator, unit, ref, system, code and file are constant only within a series,
so a mixed table repeats them on every row (#269 measured that cost at 10 pp
for a 2 B model). Blocking every series made a catalogue, stats or latest view
(one row per series) 31% to 326% longer instead, so a one-row series stays in
a shared table and the shorter rendering wins.
"""

from __future__ import annotations

from mirobody.agent.tools._render import _blocks, _readings_table
from mirobody.kernel import query

LOINC = "http://loinc.org"
COLUMNS = ("rid", "indicator", "label", "time", "value", "unit", "ref", "flag", "system", "code", "file")


def _reading(series: str, indicator: str, code: str, day: int, value: float, *, unit: str = "mmol/L") -> dict:
    return {"rid": f"r{series}{day}", "series": series, "indicator": indicator, "label": indicator.split(" [")[0],
            "time": f"2026-01-{day:02d}T08:00:00", "value": value, "unit": unit, "ref": "3.9-5.5", "flag": "",
            "system": LOINC, "code": code, "file": "report.pdf", "total": 1}


def _glucose(days: int = 3) -> list[dict]:
    return [_reading("s1", "Glucose [Moles/volume] in Serum or Plasma", "14749-6", d, 5.0 + d / 10)
            for d in range(1, days + 1)]


def _ldl(days: int = 3) -> list[dict]:
    return [_reading("s2", "Cholesterol in LDL [Moles/volume] in Serum or Plasma", "18262-6", d, 2.4 + d / 10)
            for d in range(1, days + 1)]


def _one_report(n: int = 12) -> list[dict]:
    """One lab report: n series with one reading each."""
    return [_reading(f"lab{i}", f"Analyte {i} [Moles/volume] in Serum or Plasma", f"{1000 + i}-{i % 10}", 1, 1.0 + i)
            for i in range(n)]


def test_one_series_is_one_table() -> None:
    rows = _glucose()
    assert _blocks(rows, COLUMNS) == query.compact(rows, COLUMNS)


def test_one_row_per_series_is_one_table() -> None:
    # A catalogue, stats, latest or one-report raw view: a block per series
    # would be a constants line per row.
    rows = _one_report()
    assert _blocks(rows, COLUMNS) == query.compact(rows, COLUMNS)
    catalog = [{"indicator": r["indicator"], "label": r["label"], "series": r["series"], "system": LOINC,
                "code": r["code"], "count": 3, "first_date": "2025-01-02", "last_date": "2026-01-01"} for r in rows]
    assert _readings_table(catalog) == query.compact(catalog, ("indicator", "label", "system", "code", "count",
                                                               "first_date", "last_date", "reason"))


def test_mixed_series_render_one_block_each() -> None:
    blocks = _blocks(_glucose(10) + _ldl(10), COLUMNS).split("\n\n")
    assert len(blocks) == 2
    for block, code in zip(blocks, ("14749-6", "18262-6"), strict=True):
        constants, header, *body = block.splitlines()
        assert constants.startswith("(constants: ") and f"code={code}" in constants and "unit=mmol/L" in constants
        assert header == "rid|time|value"
        assert len(body) == 10


def test_one_row_series_share_a_table() -> None:
    lone = _one_report(3)
    blocks = _blocks(_glucose(10) + lone + _ldl(10), COLUMNS).split("\n\n")
    assert len(blocks) == 3
    shared = blocks[-1].splitlines()
    assert shared[1].split("|")[:3] == ["rid", "indicator", "label"]  # a table, not three constants lines
    assert len(shared) == 2 + len(lone)


def test_no_cell_is_lost() -> None:
    rows = _glucose(10) + _one_report(3) + _ldl(10)
    text = _blocks(rows, COLUMNS)
    assert text != query.compact(rows, COLUMNS)
    for row in rows:
        for column in ("rid", "indicator", "code", "time", "value"):
            assert str(row[column]) in text


def test_never_longer_than_one_table() -> None:
    shapes = [_glucose(2) + _ldl(2), _glucose(1) + _ldl(2), _glucose(30) + _one_report(30),
              _one_report(30) + _ldl(2), _glucose(20) + _ldl(20) + _one_report(5)]
    for rows in shapes:
        assert len(_blocks(rows, COLUMNS)) <= len(query.compact(rows, COLUMNS))


def test_series_not_indicator_decides_the_block() -> None:
    # Two series an aggregate row names with a blank indicator stay apart ...
    apart = [{**r, "indicator": ""} for r in _glucose(10) + _ldl(10)]
    assert len(_blocks(apart, COLUMNS).split("\n\n")) == 2
    # ... and one series whose rows say None or "" stays together.
    together = [{**r, "series": "", "indicator": None if i % 2 else ""} for i, r in enumerate(_glucose(10))]
    assert _blocks(together, COLUMNS) == query.compact(together, COLUMNS)
