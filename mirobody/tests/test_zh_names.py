"""#231: a coded reading's name for a person, and where that name reaches.

A Chinese reader saw `Cholesterol in LDL [Moles/volume] in Serum or Plasma` where
their report printed 低密度脂蛋白胆固醇: `th_concept.display_zh` was never written and
nothing else carried a Chinese name. Then the translated long name put LOINC's
syntax in front of them instead (`心率^静息 [计数型速率]`). These pin the names
(`standardize.names`): a plain display name and the facets that replace the long
name, the shipped files they come from, and the rows that carry them.
"""

from __future__ import annotations

import csv
import re

import pytest

from mirobody.standardize import LOINC_SYSTEM, names, series_of

CJK = re.compile(r"[一-鿿]")
HALF_WIDTH_BESIDE_CJK = re.compile(r"[一-鿿][,;:()]|[,;:()][一-鿿]")
#: What a long name is made of and a person does not read: `[Mass/volume]`, `^resting`.
LOINC_SYNTAX = re.compile(r"[\[\]^]")


def _translated_rows() -> list[dict[str, str]]:
    with open(names.NAMES_PATH, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@pytest.mark.parametrize("language", ["zh", "zh-CN", "zh_cn", "zh-TW", "zh-Hant", "ZH-cn"])
def test_every_chinese_spelling_is_chinese(language):
    assert names.is_chinese(language)


@pytest.mark.parametrize("language", ["", "en", "en-US", "ja", "zha", "fr-zh"])
def test_other_languages_get_no_chinese_name(language):
    assert not names.is_chinese(language)
    found = names.names_for("22748-8", language)
    assert found is not None and not CJK.search(found.display)


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
    assert found.facets and all(CJK.search(text) for _axis, text in found.facets)


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
        # The display name is what a person reads; the long name keeps LOINC's form.
        assert not LOINC_SYNTAX.search(r["LinguisticVariantDisplayName"]), r


def test_hemoglobin_reads_as_a_name_and_two_facets_not_a_long_name():
    # The drawer showed `血红蛋白 [质量/体积]，全血 / Hemoglobin [Mass/volume] in Blood`.
    zh = names.names_for("718-7", "zh-CN")
    assert zh == names.Names("血红蛋白", (("specimen", "全血"), ("property", "质量浓度")))
    en = names.names_for("718-7", "en")
    assert en == names.Names("Hemoglobin, Blood", (("specimen", "Blood"), ("property", "Mass Concentration")))


def test_a_qualifier_reads_in_brackets_and_a_placeholder_axis_is_left_out():
    # 40443-4 drew `心率^静息 [计数型速率]`: its system is ^Patient and its time a point.
    found = names.names_for("40443-4", "zh")
    assert found is not None and found.display == "静息心率"
    assert dict(found.facets) == {"property": "计数型速率"}
    donor = dict(names.names_for("1114-8", "zh").facets)
    assert donor["specimen"] == "红细胞（供体）"
    assert dict(names.names_for("1114-8", "en").facets)["specimen"] == "Red Blood Cells (Donor)"


def test_no_name_or_facet_shows_loinc_syntax():
    table, order = names._parts()
    codes = [table.field(row, 0) for row in range(0, len(order), 97)]  # ~650 codes, every class
    assert len(codes) > 600
    for code in codes:
        for found in (names.names_for(code, "zh"), names.names_for(code, "en")):
            if found is None:
                continue
            assert not LOINC_SYNTAX.search(found.display), (code, found)
            for _axis, text in found.facets:
                assert "^" not in text, (code, found)


def test_an_unranked_code_reads_by_the_consumer_name_it_shares():
    # LOINC gives 100158-5 the consumer name `Albumin, Urine`, translated for a
    # ranked code; its unspecified timing is not a facet.
    assert "100158-5" not in names._translated()
    found = names.names_for("100158-5", "zh-CN")
    assert found is not None and found.display == "白蛋白，尿液"
    assert "timing" not in dict(found.facets)


def test_a_code_with_nothing_translated_reads_from_its_reviewed_parts():
    table, order = names._parts()
    translated = names._translated()
    for row in range(len(order)):
        code = table.field(row, 0)
        if (code not in translated and not table.field(row, 7)
                and series_of(code) not in names._catalogue_by_series()
                and names._series_and_method(code) not in names._display_by_series_and_method()):
            break
    found = names.names_for(code, "zh")
    assert found is not None
    assert found.display.startswith(names._readable(table.field(row, 1), "zh"))


def test_english_without_a_consumer_name_has_facets_and_no_label():
    # LOINC has no consumer name for vital signs; the long name already reads well.
    found = names.names_for("8480-6", "en")
    assert found is not None and found.display == ""
    assert dict(found.facets)["property"] == "Pressure"


def test_a_molar_code_reads_as_its_translated_mass_sibling():
    # A report in mmol/L codes fasting glucose to 14771-0, which LOINC leaves
    # unranked (US practice is mg/dL) and so untranslated; its mass sibling
    # 1558-6 is 空腹血糖, and to a patient they are one test.
    assert "14771-0" not in names._translated()
    found = names.names_for("14771-0", "zh-CN")
    assert found is not None and found.display == "空腹血糖"
    assert dict(found.facets)["property"] == "物质的量浓度"  # its own property, not the sibling's


def test_a_sibling_by_another_method_does_not_lend_its_name():
    # 3183-1 is a Lee-White clotting time; its series holds the translated
    # thromboelastography one (52773-9), which must not name it.
    lent = names._translated()["52773-9"]
    assert series_of("3183-1") == series_of("52773-9")
    found = names.names_for("3183-1", "zh-CN")
    assert found is None or found.display != lent


def test_a_catalogue_label_speaks_for_a_series_only_when_the_catalogue_agrees():
    # 93831-6 is 深度睡眠 for one member and 每日深度睡眠 for another.
    assert series_of("93831-6") not in names._catalogue_by_series()
    assert names._catalogue_by_series()[series_of("8867-4")] == "心率"


def test_identity_carries_the_readers_name_and_a_catalogue_row_its_facets():
    pytest.importorskip("fastapi")
    from mirobody.collect.query import _identity
    from mirobody.utils.req_ctx import set_req_ctx

    row = {"display": "Cholesterol in LDL [Moles/volume] in Serum or Plasma", "series_id": series_of("22748-8"),
           "code_system": LOINC_SYSTEM, "code": "22748-8"}
    with set_req_ctx({"language": "zh-CN"}):
        zh = _identity(row, facets=True)
        reading = _identity(row)
    with set_req_ctx({"language": "en"}):
        english = _identity(row, facets=True)
    assert zh["indicator"] == english["indicator"] == row["display"]  # still the handle callers select by
    assert zh["label"] == "低密度脂蛋白胆固醇"
    assert zh["facets"] == {"specimen": "血清/血浆", "property": "物质的量浓度"}
    assert english["label"] == "LDL cholesterol, Blood"  # LOINC's ConsumerName, verbatim
    assert english["facets"]["property"] == "Substance Concentration"
    assert "facets" not in reading  # a reading row does not repeat its code's description
    with set_req_ctx({"language": "zh-CN"}):
        uncoded = _identity({"name_text": "尿酸", "series_id": "local:尿酸|umol/l", "code_system": "", "code": ""},
                            facets=True)
    assert "label" not in uncoded and "facets" not in uncoded


def test_the_model_reads_one_name_per_series():
    """The label where there is one, LOINC's long name where there is none or
    where two series in one answer share a label: never both, so a row does not
    name its series twice."""
    pytest.importorskip("fastapi")
    from mirobody.agent.tools._render import render_compact
    from mirobody.kernel import tools

    long = "Cholesterol in LDL [Moles/volume] in Serum or Plasma"
    base = {"indicator": long, "series": "synthetic:ldl", "system": LOINC_SYSTEM, "code": "22748-8", "count": 3,
            "first_date": "2026-01-01", "last_date": "2026-07-23"}
    unlabelled = render_compact(tools.Envelope(status="ok", data=[base]))
    labelled = render_compact(tools.Envelope(status="ok", data=[{**base, "label": "低密度脂蛋白胆固醇"}]))
    assert long in unlabelled and "label" not in unlabelled
    assert "低密度脂蛋白胆固醇" in labelled and long not in labelled and "label" not in labelled

    glucose = [{"indicator": f"Glucose [Moles/volume] in {specimen}", "series": f"synthetic:{specimen}",
                "label": "葡萄糖", "system": LOINC_SYSTEM, "code": code, "count": 1, "first_date": "2026-01-01",
                "last_date": "2026-01-01"}
               for specimen, code in (("Blood", "15074-8"), ("Serum or Plasma", "14749-6"))]
    shared = render_compact(tools.Envelope(status="ok", data=glucose))
    assert "in Blood" in shared and "in Serum or Plasma" in shared and "葡萄糖" not in shared


def _journal():
    pytest.importorskip("fastapi")
    import importlib

    # By path: `mirobody.server.routers` re-exports the APIRouter under the module's name.
    return importlib.import_module("mirobody.server.routers.journal_router")


def _journal_row(i: int, kind: str, text: str, system: str, code: str, display: str, value: str = "") -> dict:
    from datetime import UTC, date, datetime

    return {"id": i, "kind": kind, "local_date": date(2026, 11, 6), "observed_start": datetime(2026, 11, 6, 1, tzinfo=UTC),
            "name_text": text, "value_text": value, "unit_text": "mmHg" if value else "", "code_system": system,
            "code": code, "display": display, "outcome": "coded", "reason": None, "series_id": series_of(code) or None,
            "note_text": None}


def _journal_rows() -> list[dict]:
    """What the journal holds after 昨晚开始头疼，血压150/95，心率72: three readings on
    LOINC, one with an English consumer name, and a complaint on ICPC-3."""
    from mirobody.standardize import ICPC3_SYSTEM

    return [
        _journal_row(1, "measurement", "收缩压", LOINC_SYSTEM, "8480-6", "Systolic blood pressure", "150"),
        _journal_row(2, "measurement", "舒张压", LOINC_SYSTEM, "8462-4", "Diastolic blood pressure", "95"),
        _journal_row(3, "measurement", "心率", LOINC_SYSTEM, "8867-4", "Heart rate", "72"),
        _journal_row(4, "symptom", "头疼", ICPC3_SYSTEM, "NS01", "Headache"),
    ]


def _journal_answers(monkeypatch, language: str) -> tuple[dict, dict]:
    """`(listed, logged)`: the entries `GET /api/v1/journal` and `POST
    /api/v1/journal/sentence` answer with, by the person's words, for a request
    in `language`. Only the database and the text model are stood in for."""
    import asyncio

    from mirobody.collect import observations
    from mirobody.utils.req_ctx import set_req_ctx

    journal = _journal()

    async def owner(*args, **kwargs):
        return "7"

    async def rows(sql, params):
        return _journal_rows()

    async def zone(user_id):
        return "Asia/Shanghai"

    async def nothing(*args, **kwargs):
        return []

    async def read(text, **kwargs):
        return [], "{}"

    async def ingest(*args, **kwargs):
        return observations.Report(inserted=4, ids=[1, 2, 3, 4])

    monkeypatch.setattr(journal, "subject_for", owner)
    monkeypatch.setattr(journal, "execute_query", rows)
    monkeypatch.setattr(journal, "_medications", nothing)
    monkeypatch.setattr(journal.observations, "user_tz", zone)
    monkeypatch.setattr(journal.observations, "ingest", ingest)
    monkeypatch.setattr(journal.sentence, "available", lambda: True)
    monkeypatch.setattr(journal.sentence, "read", read)
    monkeypatch.setattr(journal.sentence, "plan", lambda parts, **kwargs: (["drafts"], []))
    monkeypatch.setattr(journal.sentence, "mentions", lambda parts, **kwargs: ([], []))
    with set_req_ctx({"language": language}):
        listed = asyncio.run(journal.list_entries(
            user_id="7", from_date="2026-11-01", to_date="2026-11-07", kind=None, target_user_id=None, tz=None,
            x_timezone=None))
        logged = asyncio.run(journal.log_sentence(
            journal.JournalSentence(text="昨晚开始头疼，血压150/95，心率72"), user_id="7", x_timezone=None))
    assert listed.code == 0 and logged.code == 0, (listed, logged)
    return ({e["text"]: e for day in listed.data["days"] for e in day["entries"]},
            {e["text"]: e for e in logged.data["written"]})


def test_the_journal_names_a_reading_as_the_indicator_list_does(monkeypatch):
    # A Chinese page read `收缩压 150 mmHg → Systolic blood pressure 8480-6`: the
    # journal sent `display` only, LOINC's long name, a year after the indicator
    # list stopped showing it to a Chinese reader.
    for entries in _journal_answers(monkeypatch, "zh-CN"):
        assert entries["收缩压"]["label"] == "收缩压"
        assert entries["舒张压"]["label"] == "舒张压"
        assert entries["心率"]["label"] == "心率"
        assert entries["收缩压"]["display"] == "Systolic blood pressure"  # the classification's own name stays


def test_an_english_journal_reads_as_it_did(monkeypatch):
    for entries in _journal_answers(monkeypatch, "en"):
        # LOINC gives vital signs no English consumer name; the long name is already plain.
        assert "label" not in entries["收缩压"] and "label" not in entries["舒张压"]
        assert entries["心率"]["label"] == "Heart rate"  # its ConsumerName, as the indicator list shows it


@pytest.mark.parametrize("language", ["zh-CN", "en"])
def test_a_complaint_keeps_the_icpc3_term_in_every_language(monkeypatch, language):
    # There is no Chinese ICPC-3 in the bundle, and CC BY-ND forbids writing one
    # (`res/icpc3/icpc3.NOTICE`): 头疼 is coded NS01 and named by WONCA's term.
    for entries in _journal_answers(monkeypatch, language):
        assert "label" not in entries["头疼"]
        assert entries["头疼"]["display"] == "Headache"
