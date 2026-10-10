"""The builder on a fixture corpus: two health topics and one drug label,
tiny enough to read in one screen. Asserts the shipped column layout (the
runtime in mirobody/agent/medref.py depends on it), the rid scheme and the
zh-alias path (an English passage answerable from a Chinese query term).

Not part of the shipped suite (pytest's testpaths are `mirobody` and `tests`);
run it explicitly after touching the builder:

    python3 -m pytest scripts/medref/test_build_index.py -q
    # or: python3 -m unittest scripts.medref.test_build_index -v
"""

from __future__ import annotations

import json
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import build_index

FIXTURE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<health-topics total="2">
<health-topic meta-desc="About cholesterol" title="High Blood Cholesterol" url="https://medlineplus.gov/highbloodcholesterol.html" id="101" language="English" date-created="01/01/2020">
  <also-called>Hypercholesterolemia</also-called>
  <full-summary>&lt;p&gt;Cholesterol is a waxy substance in your blood. Too much LDL cholesterol raises your risk of heart disease.&lt;/p&gt;
  &lt;p&gt;Statins are the most common medicines used to lower cholesterol. Diet and exercise also help.&lt;/p&gt;</full-summary>
  <mesh-heading><descriptor id="D002784">Cholesterol</descriptor></mesh-heading>
</health-topic>
<health-topic meta-desc="Sobre colesterol" title="Colesterol alto en sangre" url="https://medlineplus.gov/spanish/highbloodcholesterol.html" id="102" language="Spanish" date-created="01/01/2020">
  <full-summary>&lt;p&gt;El colesterol es una sustancia cerosa en la sangre. Las estatinas ayudan a bajarlo.&lt;/p&gt;</full-summary>
</health-topic>
</health-topics>
"""

FIXTURE_LABEL = {
    "_mirobody_query": {"generic": "METFORMIN", "fetched": "2026-10-08"},
    "results": [{
        "set_id": "aaaa-bbbb",
        "openfda": {"generic_name": ["METFORMIN HYDROCHLORIDE"], "brand_name": ["Glucophage"]},
        "indications_and_usage": ["Metformin is indicated as an adjunct to diet and exercise to improve glycemic control in adults with type 2 diabetes mellitus."],
        "warnings_and_cautions": ["Lactic acidosis has been reported with metformin use. Risk increases with renal impairment."],
    }],
}


class BuildIndexFixtureTest(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.xml = root / "topics.xml"
        self.xml.write_text(FIXTURE_XML, encoding="utf-8")
        self.labels = root / "openfda"
        self.labels.mkdir()
        (self.labels / "METFORMIN.json").write_text(json.dumps(FIXTURE_LABEL), encoding="utf-8")
        self.out = root / "index.sqlite3"
        stats = build_index.build_index(self.xml, self.labels, self.out)
        self.assertEqual(stats["medlineplus"], 3)  # each summary block is one merged chunk
        self.assertEqual(stats["openfda"], 2)
        self.con = sqlite3.connect(self.out)

    def tearDown(self) -> None:
        self.con.close()
        self.tmp.cleanup()

    def rows(self, match: str):
        return self.con.execute(
            "SELECT rid, source, lang, title, section, text FROM passages_fts "
            "WHERE passages_fts MATCH ? ORDER BY bm25(passages_fts)", (match,)).fetchall()

    def test_column_layout_is_the_runtime_contract(self) -> None:
        cols = [r[1] for r in self.con.execute("PRAGMA table_info(passages_fts)")]
        self.assertEqual(cols, ["rid", "source", "lang", "url", "title", "section", "text", "zh"])
        meta = dict(self.con.execute("SELECT key, value FROM meta"))
        self.assertTrue(meta["version"].startswith("medref+"))

    def test_english_query_finds_topic_and_label(self) -> None:
        # unicode61 has no stemming: the corpus token is "statins", so the
        # runtime turns 4+-letter Latin terms into prefix queries (statin*).
        hits = self.rows("statin*")
        self.assertTrue(any(r[1] == "medlineplus" and r[0].startswith("101:summary:") for r in hits))
        hits = self.rows('"metformin"')
        self.assertTrue(any(r[1] == "openfda" and r[3].startswith("METFORMIN") for r in hits))
        self.assertTrue(any(r[4] == "Indications and usage" for r in hits))

    def test_spanish_topic_is_searchable(self) -> None:
        hits = self.rows('"estatinas"')
        self.assertTrue(any(r[2] == "es" for r in hits))

    def test_chinese_term_lands_via_alias_column(self) -> None:
        hits = self.rows('"二甲双胍"')
        self.assertTrue(hits and all(r[1] == "openfda" for r in hits))
        hits = self.rows('"他汀"')
        self.assertTrue(any(r[1] == "medlineplus" for r in hits))

    def test_lang_and_source_filter_sql(self) -> None:
        hits = self.con.execute(
            "SELECT rid FROM passages_fts WHERE passages_fts MATCH '\"二甲双胍\"' AND source='medlineplus'").fetchall()
        self.assertEqual(hits, [])


if __name__ == "__main__":
    unittest.main()
