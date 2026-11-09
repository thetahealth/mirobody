"""A result that mixes indicators renders one compact table per indicator.

Why: a mixed table keeps `indicator`, `unit`, `system`, `code` and `file` on
every row because they differ across indicators, yet each is constant within
one. `query.compact` hoists per table, so a mixed table repeats them on every
row - measured cost on a paired benchmark is ~10 pp of accuracy for a 2 B model,
and pure tokens for a larger one. Grouping hoists each block's own constants and
changes no information: same rows, same cells, fewer repeated cells.
"""

from __future__ import annotations

from mirobody.agent.tools._render import _blocks
from mirobody.kernel import query

COLUMNS = ("indicator", "time", "value", "unit", "ref", "flag", "system", "code", "file")


def _row(indicator: str, day: str, value: float, unit: str, system: str, code: str, file: str) -> dict:
    return {"indicator": indicator, "time": day, "value": value, "unit": unit, "ref": "",
            "flag": "", "system": system, "code": code, "file": file}


def _two_indicators() -> list[dict]:
    rows = [_row("glucose", f"2026-01-0{i}", 5.0 + i, "mmol/L", "LOINC", "2339-0", "watch.csv")
            for i in range(1, 4)]
    rows += [_row("steps", f"2026-01-0{i}", 8000.0 + i, "count", "LOCAL", "steps", "phone.csv")
             for i in range(1, 4)]
    return rows


def test_single_indicator_is_unchanged() -> None:
    rows = [r for r in _two_indicators() if r["indicator"] == "glucose"]
    assert _blocks(rows, COLUMNS) == [query.compact(rows, COLUMNS)]


def test_mixed_indicators_render_one_block_each() -> None:
    blocks = _blocks(_two_indicators(), COLUMNS)
    assert len(blocks) == 2
    for block in blocks:
        # only the varying columns survive; the rest are hoisted per block
        assert block.splitlines()[-4].startswith("time|value")
        assert "constants:" in block


def test_no_cell_is_lost() -> None:
    rows = _two_indicators()
    blocks = "\\n\\n".join(_blocks(rows, COLUMNS))
    for row in rows:
        assert str(row["indicator"]) in blocks
        assert str(row["code"]) in blocks


def test_blank_indicator_still_groups() -> None:
    rows = _two_indicators() + [_row("", "2026-01-05", 1.0, "", "LOCAL", "", "notes.csv")]
    assert len(_blocks(rows, COLUMNS)) == 3
