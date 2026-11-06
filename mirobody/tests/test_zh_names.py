"""#231: a coded reading's name in Chinese, and where that name reaches.

A Chinese reader saw `Cholesterol in LDL [Moles/volume] in Serum or Plasma` where
their report printed 低密度脂蛋白胆固醇: `th_concept.display_zh` was never written and
nothing else carried a Chinese name. These pin the names (`standardize.names`), the
two shipped files they come from, and the rows that carry them.
"""

from __future__ import annotations

import csv
import re

import pytest

from mirobody.standardize import LOINC_SYSTEM, names, series_of

CJK = re.compile(r"[一-鿿]")
HALF_WIDTH_BESIDE_CJK = re.compile(r"[一-鿿][,;:()]|[,;:()][一-鿿]")


def _translated_rows() -> list[dict[str, str]]:
    with open(names.NAMES_PATH, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@pytest.mark.parametrize("language", ["zh", "zh-CN", "zh_cn", "zh-TW", "zh-Hant", "ZH-cn"])
def test_every_chinese_spelling_is_chinese(language):
    assert names.is_chinese(language)


@pytest.mark.parametrize("language", ["", "en", "en-US", "ja", "zha", "fr-zh"])
def test_other_languages_get_no_chinese_name(language):
    assert not names.is_chinese(language)
    assert names.names_for("22748-8", language) is None


@pytest.mark.parametrize(("code", "display"), [
    # The two the issue names, and LDL's mass-and-calculated sibling: one test to a patient.
    ("22748-8", "低密度脂蛋白胆固醇"),
    ("13457-7", "低密度脂蛋白胆固醇"),
    ("4548-4", "糖化血红蛋白（HbA1c）"),
    # LOINC's generic components would name these as a different test
    # (红细胞, 血红蛋白, 观察); the display name names the measured quantity.
    ("788-0", "红细胞分布宽度（RDW）"),
    ("787-2", "平均红细胞体积（MCV）"),
    ("5902-2", "凝血酶原时间（PT）"),
    # A specimen other than blood is named first, as reports print it.
    ("5794-3", "尿隐血"),
])
def test_the_reported_tests_read_as_a_chinese_report_prints_them(code, display):
    found = names.names_for(code, "zh-CN")
    assert found is not None and found.display == display
    assert CJK.search(found.long)


def test_the_translation_file_names_bundle_codes_only_and_in_chinese():
    rows = _translated_rows()
    assert len(rows) > 17_000
    assert list(rows[0]) == ["LOINC_NUM", "LinguisticVariantDisplayName", "LONG_COMMON_NAME"]
    codes = [r["LOINC_NUM"] for r in rows]
    assert len(set(codes)) == len(codes)
    for r in rows:
        assert series_of(r["LOINC_NUM"]), f"{r['LOINC_NUM']} is not in the shipped bundle"
        for text in (r["LinguisticVariantDisplayName"], r["LONG_COMMON_NAME"]):
            # A report prints an HLA allele, pH and β-HCG as symbols; anything else
            # with no Chinese in it is a row nobody translated.
            assert CJK.search(text) or re.match(r"HLA-|pH$|β-HCG$", text), r
            assert not HALF_WIDTH_BESIDE_CJK.search(text), r


def test_a_code_without_a_translation_reads_from_its_reviewed_parts():
    table, order = names._parts()
    assert len(order) > 60_000
    translated = names._translated()
    for row in range(len(order)):
        code = table.field(row, 0)
        if code not in translated and series_of(code) not in names._catalogue_by_series():
            break
    component, prop = table.field(row, 1), table.field(row, 2)
    found = names.names_for(code, "zh")
    assert found is not None
    assert found.display == found.long
    assert found.long.startswith(f"{component} [{prop}]" if prop else component)


def test_a_catalogue_label_speaks_for_a_series_only_when_the_catalogue_agrees():
    # 93831-6 is 深度睡眠 for one member and 每日深度睡眠 for another.
    assert series_of("93831-6") not in names._catalogue_by_series()
    assert names._catalogue_by_series()[series_of("8867-4")] == "心率"


def test_identity_carries_the_chinese_name_only_for_a_chinese_request():
    pytest.importorskip("fastapi")
    from mirobody.collect.query import _identity
    from mirobody.utils.req_ctx import set_req_ctx

    row = {"display": "Cholesterol in LDL [Moles/volume] in Serum or Plasma", "series_id": series_of("22748-8"),
           "code_system": LOINC_SYSTEM, "code": "22748-8"}
    with set_req_ctx({"language": "zh-CN"}):
        zh = _identity(row)
    english = _identity(row)
    assert zh["indicator"] == english["indicator"] == row["display"]  # still the handle callers select by
    assert zh["label"] == "低密度脂蛋白胆固醇" and "物质的量" in zh["label_long"]
    assert "label" not in english and "label_long" not in english
    with set_req_ctx({"language": "zh-CN"}):
        uncoded = _identity({"name_text": "尿酸", "series_id": "local:尿酸|umol/l", "code_system": "", "code": ""})
    assert "label" not in uncoded


def test_the_model_reads_a_label_column_only_when_rows_carry_one():
    pytest.importorskip("fastapi")
    from mirobody.agent.tools._render import render_compact
    from mirobody.kernel import tools

    base = {"indicator": "Cholesterol in LDL [Moles/volume] in Serum or Plasma", "system": LOINC_SYSTEM,
            "code": "22748-8", "count": 3, "first_date": "2026-01-01", "last_date": "2026-07-23"}
    english = render_compact(tools.Envelope(status="ok", data=[base]))
    chinese = render_compact(tools.Envelope(status="ok", data=[{**base, "label": "低密度脂蛋白胆固醇"}]))
    assert "label" not in english
    assert "低密度脂蛋白胆固醇" in chinese
