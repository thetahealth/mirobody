"""`code_exec` computes on the rows the model read, under the names it read (#340).

Asked to compare blood lipids over three years and compute non-HDL
cholesterol and ApoB/ApoA1, deepseek-flash spent 20 steps and three failed
`code_exec` calls on the demo account. Two things it could not see:

- The table it read showed `indicator` as the label (高密度脂蛋白胆固醇); the
  rows `code_exec` received kept LOINC's long name there, so a filter on the
  name it read matched nothing.
- `data` was the last lookup. In one run that was a keyword lookup run
  beside the lipids that held only HDL; in another, a catalogue listing
  called after them, with no readings at all.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

pytest.importorskip("deepagents")
pytest.importorskip("langchain")

from langchain_core.messages import HumanMessage, ToolMessage

from mirobody.agent import code_tool
from mirobody.kernel import tools

HDL = "Cholesterol in HDL [Moles/volume] in Serum or Plasma"
LDL = "Cholesterol in LDL [Moles/volume] in Serum or Plasma"


def _row(rid: str, indicator: str, label: str, value: str, time: str = "2026-08-04 00:00:00") -> dict:
    return {"rid": rid, "indicator": indicator, "label": label, "time": time, "value": value,
            "unit": "mmol/L", "code": "14646-4" if indicator == HDL else "13457-7", "series": indicator}


def _lookup(call_id: str, rows: list, *, view: str = "raw", catalog: bool = False,
            name: str = "query_health_indicators") -> ToolMessage:
    meta = tools.Meta(view="" if catalog else view, row_count=len(rows), catalog_total=len(rows) if catalog else 0)
    return ToolMessage(content="table", name=name, tool_call_id=call_id,
                       artifact=tools.Envelope(tools.STATUS_OK, data=rows, meta=meta))


def _inputs(*lookups: ToolMessage) -> dict:
    runtime = SimpleNamespace(state={"messages": [HumanMessage("对比血脂变化"), *lookups]})
    return code_tool._inputs(runtime)[1]


LIPIDS = [_row("r1", HDL, "高密度脂蛋白胆固醇", "1.52"), _row("r2", LDL, "低密度脂蛋白胆固醇", "2.41")]
CATALOG = [{"indicator": HDL, "label": "高密度脂蛋白胆固醇", "count": 4, "first_date": "2023-03-21"}]


def test_a_row_is_named_as_the_table_named_it():
    data = _inputs(_lookup("lipids", LIPIDS))["data"]
    assert [r["indicator"] for r in data] == ["高密度脂蛋白胆固醇", "低密度脂蛋白胆固醇"]
    assert data[0]["long_name"] == HDL and data[0]["code"] == "14646-4"


def test_a_number_is_a_number_and_a_word_stays_a_word():
    rows = [_row("r1", HDL, "", "1.52"), _row("r2", HDL, "", "<0.5"), _row("r3", HDL, "", "120"),
            _row("r4", HDL, "", "阴性")]
    assert [r["value"] for r in _inputs(_lookup("q", rows))["data"]] == [1.52, "<0.5", 120, "阴性"]


def test_a_catalogue_called_after_the_readings_does_not_replace_them():
    """Run 3: the lipids, then the catalogue."""
    data = _inputs(_lookup("lipids", LIPIDS), _lookup("catalog", CATALOG, catalog=True))["data"]
    assert [r["rid"] for r in data] == ["r1", "r2"]


def test_a_narrower_lookup_beside_the_readings_joins_them():
    """Run 2: the catalogue, the four lipids and a keyword lookup that held
    only HDL, in parallel. HDL is the same reading in both, one rid."""
    hdl_only = [_row("r1", HDL, "高密度脂蛋白胆固醇", "1.52")]
    data = _inputs(_lookup("catalog", CATALOG, catalog=True), _lookup("lipids", LIPIDS),
                   _lookup("keyword", hdl_only))["data"]
    assert [r["rid"] for r in data] == ["r1", "r2"]


def test_another_view_or_tool_is_not_mixed_in():
    """A stats row has no reading's shape, and a medication is not a reading:
    `data` stays the last lookup; `results` has the rest."""
    stats = [{"rid": "a1", "indicator": HDL, "label": "高密度脂蛋白胆固醇", "mean": 1.4}]
    inputs = _inputs(_lookup("lipids", LIPIDS), _lookup("stats", stats, view="stats"))
    assert [r["rid"] for r in inputs["data"]] == ["a1"]
    assert [r["rid"] for r in inputs["results"][0]["data"]] == ["r1", "r2"]
    meds = [{"rid": "m1", "name": "Example medication"}]
    assert _inputs(_lookup("lipids", LIPIDS), _lookup("meds", meds, name="query_medications"))["data"] == meds


def test_only_a_catalogue_is_still_data():
    assert _inputs(_lookup("catalog", CATALOG, catalog=True))["data"][0]["count"] == 4


def test_a_row_with_nothing_to_rename_is_passed_as_it_came():
    rows = [{"rid": "r1", "indicator": "Weight", "value": 7.0, "unit": "kg"},
            {"rid": "r2", "rsid": "rs1", "GT": "0|1", "annotations": {"calls": [1, 2]}}]
    assert _inputs(_lookup("q", rows))["data"] == rows


async def test_the_models_own_filter_from_run_two_now_finds_the_rows():
    """The first call of run 2, written from the table: it threw
    `cannot read property 'value' of undefined`."""
    code = ("const pick = (name, d) => data.find(r => r.indicator === name && String(r.time).startsWith(d));"
            "console.log(JSON.stringify([pick('高密度脂蛋白胆固醇', '2026-08').value,"
            " pick('低密度脂蛋白胆固醇', '2026-08').value]))")
    runtime = SimpleNamespace(state={"messages": [HumanMessage("对比血脂变化"), _lookup("lipids", LIPIDS),
                                                  _lookup("catalog", CATALOG, catalog=True)]})
    text, artifact = await code_tool.javascript_code.coroutine(code=code, runtime=runtime)
    assert artifact["status"] == tools.STATUS_OK, text
    assert json.loads(text) == [1.52, 2.41]
