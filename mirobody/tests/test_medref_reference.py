"""The agent-only `search_medical_reference` tool and its two boundaries:

- it answers general-knowledge queries (zh and en) from the shipped offline
  index with citeable rows, and says so explicitly when nothing matches —
  never a bare empty result;
- it stays OUT of the MCP surface. The MCP surface is exactly seven tools
  (AGENTS.md); this suite asserts that list from the tool directory directly,
  so accidentally dropping the tool module into `agent/tools/` turns a test
  red here, not in a maintainer's suite downstream.

The built index itself is the fixture (res/medref/index.sqlite3, gitignored
build output of scripts/medref/build_index.py; builder-level coverage lives
beside it on a tiny fixture corpus). A checkout without the data — the index
not built yet — skips rather than fails, the same way test_skills treats the
repository-only `skills/` tree.
"""

from __future__ import annotations

import pytest

from mirobody._bundle import is_lfs_pointer
from mirobody.agent import medref

pytestmark = pytest.mark.skipif(
    not medref.INDEX_PATH.is_file() or is_lfs_pointer(str(medref.INDEX_PATH)),
    reason="medref index not built in this checkout (scripts/medref/build_index.py)",
)

EXPECTED_MCP_TOOLS = {
    "resolve_indicator", "convert_unit", "normalize_unit",
    "query_health_indicators", "query_medications",
    "query_genetic_data", "query_pharmacogenomics",
}


def test_rows_are_citeable_for_a_statin_query_en() -> None:
    result = medref.search_medref("statin side effects", k=8)
    assert result["ok"], result.get("note")
    assert result["rows"], "the shipped corpus must cover statins"
    for row in result["rows"]:
        assert row["rid"].startswith(("ref:medlineplus:", "ref:openfda:"))
        assert row["source"] in medref.SOURCES
        assert row["title"] and row["section"] and row["snippet"]
        assert row["url"].startswith("https://")
    titles = " ".join(r["title"].casefold() for r in result["rows"])
    assert "statin" in titles


def test_rows_are_citeable_for_a_statin_query_zh() -> None:
    result = medref.search_medref("他汀的副作用", k=8)
    assert result["ok"], result.get("note")
    assert result["zh_matched"] >= 1, "他汀 must resolve through the bundled synonym table"
    assert result["rows"], "a zh statin query must land passages via the alias columns"
    for row in result["rows"]:
        assert row["rid"].startswith("ref:")


def test_k_is_capped() -> None:
    result = medref.search_medref("blood pressure", k=50)
    assert len(result["rows"]) <= medref.MAX_K


def test_source_filter_is_enforced_and_validated() -> None:
    only_fda = medref.search_medref("metformin", k=8, source="openfda")
    assert only_fda["rows"] and {r["source"] for r in only_fda["rows"]} == {"openfda"}
    bad = medref.search_medref("metformin", source="medline-plus")
    assert not bad["ok"] and "note" in bad


def test_no_hits_is_explicit_not_empty() -> None:
    result = medref.search_medref("xylophone quokka nebula", k=5)
    assert result["ok"] and result["rows"] == []
    text = medref._render(result)
    assert "No passage" in text and "does not cover" in text


def test_render_cites_rids_and_scopes_the_claims() -> None:
    text = medref._render(medref.search_medref("hypertension", k=3))
    assert "ref:" in text
    assert "NOT anything from the person's own record" in text


def test_tool_is_agent_only_not_in_the_mcp_seven() -> None:
    from mirobody.mcp.tool import load_tools_from_directory

    mcp_tools, descriptions = load_tools_from_directory("mirobody/agent/tools")
    assert set(mcp_tools) == EXPECTED_MCP_TOOLS
    assert "search_medical_reference" not in mcp_tools
    assert all(d["name"] in EXPECTED_MCP_TOOLS for d in descriptions)
