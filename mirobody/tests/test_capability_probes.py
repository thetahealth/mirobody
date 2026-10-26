"""Rejection controls for source-derived capability probes, without model calls."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("requests")
_SPEC = importlib.util.spec_from_file_location("capability_probes", Path(__file__).resolve().parents[2] / "benchmarks/local_models/probes.py")
probes = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(probes)


@pytest.fixture
def source():
    records = [{"indicator": "bodyMasss", "metric": "weight", "loinc": "29463-7", "unit": "kg",
                "time": f"{date(2025, 1, 1) + timedelta(days=i)}T08:00:00+08:00", "value": 50 + i}
               for i in range(120)]
    corpus = SimpleNamespace(devices={"p001": {"records": records}}, files={}, journal=[])
    return corpus, {"tz": "Asia/Shanghai", "documents": []}


def turn(answer, tool="(constants: rid=r1, indicator=weight, avg=70, unit=kg)", **extra):
    return {"finish_reason": "stop", "blocks": [{"type": "tool_call", "name": "query_health_indicators", "args": {}},
            {"type": "tool_result", "content": tool}, {"type": "text", "text": answer}], **extra}


NUMERIC = {"capability": "C3", "operation": "mean", "metric": "weight", "gold": 70, "tol": .1}
ABSENT = {"capability": "C7", "metric": "glu", "gold": None, "expect_absent": True}


def test_real_flat_generator_schema_produces_compact_discriminating_windows(source):
    corpus, config = source
    cases, _ = probes.build(corpus, "p001", config)
    assert 2 <= sum(p["capability"] == "C3" for p in cases) <= 3
    assert len(cases) <= 19
    groups = defaultdict(list)
    for p in cases:
        if p["gold"] is None:
            continue
        groups[p["operation"]].append(p)
        values = [v for _, v in p["evidence"]["points"]]
        assert p["gold"] == pytest.approx({"mean": sum(values) / len(values), "days": len(values), "max": max(values)}[p["operation"]])
        assert len({d for d, _ in p["evidence"]["points"]}) == len(values)
    for group in groups.values():
        for i, p in enumerate(group):
            for other in group[i+1:]:
                assert p["end"] < other["start"]
                assert abs(p["gold"] - other["gold"]) > p["tol"] + other["tol"]


def test_flat_values_cannot_pretend_to_probe_window_discrimination(source):
    corpus, config = source
    for r in corpus.devices["p001"]["records"]:
        r["value"] = 70
    cases, skipped = probes.build(corpus, "p001", config)
    assert not any(p["capability"] == "C3" for p in cases)
    assert any(p["reason"] == "window answers overlap within tolerance" for p in skipped)


def test_always_answering_the_latest_window_cannot_pass_historical_windows(source):
    cases, _ = probes.build(source[0], "p001", source[1])
    means = [p for p in cases if p["capability"] == "C3"]
    copied = str(means[-1]["gold"])
    assert sum(probes.grade(p, turn(copied))["ok"] for p in means) == 1


@pytest.mark.parametrize("kind", ["short-device", "document", "journal"])
def test_absence_checks_all_source_truth_even_short_or_unreadable_rows(source, kind):
    corpus, config = source
    row = {"metric": "glu", "indicator": "blood glucose", "time": "2025-01-01T00:00:00+00:00", "unit": "mmol/L", "value": 5.5}
    if kind == "short-device":
        corpus.devices["p001"]["records"].append(row)
    elif kind == "document":
        corpus.files["lab.pdf"] = {"readings": [{"key": "glu", "observed": "2025-01-01", "readable": False}]}
        config["documents"].append("lab.pdf")
    else:
        corpus.journal.append({"person_id": "p001", "text": "blood glucose"})
    cases, _ = probes.build(corpus, "p001", config)
    assert all(p["metric"] != "glu" for p in cases if p.get("expect_absent"))


def test_ambiguous_daily_sources_and_mixed_units_do_not_generate_wrong_gold(source):
    corpus, config = source
    corpus.devices["p001"]["records"].append(copy.deepcopy(corpus.devices["p001"]["records"][0]))
    cases, skipped = probes.build(corpus, "p001", config)
    assert not any(p["gold"] is not None for p in cases)
    assert any("unambiguous" in p["reason"] for p in skipped)
    corpus.devices["p001"]["records"][-1]["unit"] = "lb"
    _, skipped = probes.build(corpus, "p001", config)
    assert any("single recorded unit" in p["reason"] for p in skipped)


def test_windows_do_not_cross_loaded_document_measurements(source):
    corpus, config = source
    corpus.files["lab.pdf"] = {"readings": [{"key": "weight", "observed": "2025-01-03"}]}
    config["documents"] = ["lab.pdf"]
    cases, _ = probes.build(corpus, "p001", config)
    assert all(not (p["start"] <= "2025-01-03" <= p["end"]) for p in cases if p["gold"] is not None)


def test_account_timezone_determines_local_day(source):
    corpus, config = source
    config["tz"] = "UTC"
    for r in corpus.devices["p001"]["records"]:
        r["time"] = r["time"].replace("T08:00", "T00:00")
    cases, _ = probes.build(corpus, "p001", config)
    assert cases[0]["start"] == "2024-12-31"


@pytest.mark.parametrize("answer, expected", [
    ("From 2025-01-01 to 2025-01-30, the mean was 70 kg<cite>[r12]</cite>", 70),
    ("70<cite>[r1]</cite>", 70), ("7,000<cite>[r12]</cite>", 7000),
    ("70 or 71", None), ("No number<cite>[r70]</cite>", None),
])
def test_numeric_extraction_ignores_dates_and_ids_but_rejects_ambiguity(answer, expected):
    assert probes.answer_number(answer) == expected


@pytest.mark.parametrize("answer", ["70<cite></cite>", "70<cite>[r999]</cite>", "70", "71<cite>[r1]</cite>"])
def test_c6_rejects_empty_unknown_missing_and_unsupported_citations(answer):
    assert not probes.grade(NUMERIC, turn(answer))["cited"]


def test_valid_numeric_answer_is_supported_and_full_evidence_is_saved():
    raw = turn("<statement>70 kg<cite>[r1]</cite></statement>")
    result = probes.grade(NUMERIC, raw)
    assert result["ok"] and result["cited"] and result["status"] == "completed"
    assert result["turn"] == raw and "tool_result" in json.dumps(result)


def test_dated_answer_keeps_its_value_and_citation_grade():
    result = probes.grade(NUMERIC, turn("From 2025-01-01 through 2025-01-30: <statement>70 kg<cite>[r1]</cite></statement>"))
    assert result["ok"] and result["cited"]


@pytest.mark.parametrize("extra", [{"timeout": True}, {"error": "upstream"}, {"configuration_error": True}, {"finish_reason": "length"}])
def test_incomplete_runs_cannot_receive_quality_or_citation_credit(extra):
    raw = turn("70<cite>[r1]</cite>")
    raw.update(extra)
    result = probes.grade(NUMERIC, raw)
    assert result["status"] == "run_failure" and not result["ok"] and not result["cited"]
    counts = probes.summarize([result])
    assert counts["C3"] == counts["C6"] == {"selected": 1, "completed": 0, "passed": 0, "run_failure": 1, "not_attempted": 0}


def test_absence_denial_with_fabricated_value_cannot_pass():
    assert not probes.grade(ABSENT, turn("No data was found; your blood glucose was 7.2 mmol/L."))["ok"]
    assert probes.grade(ABSENT, turn("No blood glucose records were found."))["ok"]
    assert not probes.grade(ABSENT, turn("Your blood glucose is fine."))["ok"]


def test_numeric_grade_rejects_an_explicit_wrong_unit():
    assert not probes.grade({**NUMERIC, "unit": "kg"}, turn("70 lb<cite>[r1]</cite>"))["ok"]


def test_unattempted_probes_keep_their_separate_selected_denominator():
    counts = probes.summarize([{**NUMERIC, "status": "not_attempted", "ok": False, "cited": False}])
    assert counts["C3"]["selected"] == counts["C6"]["selected"] == 1
    assert counts["C3"]["not_attempted"] == 1 and counts["C3"]["run_failure"] == 0


def test_account_provenance_requires_person_hash_and_completed_load():
    plan = {"qa_people": {"p001": {}}, "provenance": {"sha256": {"devices.jsonl": "synthetic-hash"}}}
    entry = {"person": "p001", "record_sha256": probes.record_identity(plan), "loaded_at": "2025-01-01", "steps": {"devices": {}}}
    probes.validate_account(plan, "p001", entry)
    for field in ("person", "record_sha256", "loaded_at", "steps"):
        bad = {**entry, field: ""}
        with pytest.raises(ValueError):
            probes.validate_account(plan, "p001", bad)


@pytest.mark.parametrize("flat", [False, True])
def test_dry_run_pins_sources_and_explains_saturation_without_network(source, tmp_path, monkeypatch, flat):
    corpus, config = source
    if flat:
        for row in corpus.devices["p001"]["records"]:
            row["value"] = 70
    payloads = {"files": [], "people": [{"person_id": "p001"}], "devices": [corpus.devices["p001"] | {"person_id": "p001"}],
                "genomics": [], "journal": [], "medications": []}
    hashes = {}
    for name, rows in payloads.items():
        path = tmp_path / f"{name}.jsonl"
        path.write_text("".join(json.dumps(r) + "\n" for r in rows))
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    plan = {"qa_people": {"p001": config}, "provenance": {"suite_id": "harness100-20261023c", "sha256": hashes}}
    plan_path, output = tmp_path / "plan.json", tmp_path / "probes.json"
    plan_path.write_text(json.dumps(plan))
    monkeypatch.setattr("sys.argv", ["probes.py", "--corpus", str(tmp_path), "--plan", str(plan_path),
                        "--person", "p001", "--email", "synthetic@example.invalid", "--out", str(output), "--dry-run"])
    import common
    def no_sign_in(*args, **kwargs):
        raise AssertionError("dry run must not access an account")
    monkeypatch.setattr(common, "sign_in", no_sign_in)
    assert probes.main() == (2 if flat else 0)
    packet = json.loads(output.read_text())
    assert packet["source_sha256"] == hashes and packet["results"] == []
    assert packet["cases_sha256"] == probes.digest_json(packet["case_manifest"])
    if flat:
        assert any("overlap" in r["reason"] for r in packet["skipped"])


def test_session_creation_failure_is_saved_with_the_full_selected_denominator(source, tmp_path, monkeypatch):
    import common
    import run as bench
    corpus, config = source
    plan = {"qa_people": {"p001": config}, "provenance": {"sha256": {"devices.jsonl": "synthetic"}}}
    entry = {"person": "p001", "record_sha256": probes.record_identity(plan), "loaded_at": "2025-01-01", "steps": {"devices": {}}}
    output = tmp_path / "result.json"
    monkeypatch.setattr(probes, "Corpus", lambda _: corpus)
    monkeypatch.setattr(probes, "read_json", lambda path, *args: plan if str(path) == "plan.json" else {"synthetic@example.invalid": entry})
    monkeypatch.setattr(probes, "validate_corpus", lambda *args: None)
    monkeypatch.setattr(common, "sign_in", lambda *args, **kwargs: object())
    def session_failed(*args, **kwargs):
        raise common.ApiError("synthetic session unavailable")
    monkeypatch.setattr(bench, "ask", session_failed)
    monkeypatch.setattr("sys.argv", ["probes.py", "--corpus", "synthetic", "--plan", "plan.json", "--person", "p001",
                                    "--email", "synthetic@example.invalid", "--out", str(output)])
    assert probes.main() == 1
    packet = json.loads(output.read_text())
    assert packet["complete"] and packet["results"][0]["status"] == "run_failure"
    assert all(r["status"] == "not_attempted" for r in packet["results"][1:])
    for capability in packet["capabilities"].values():
        assert capability["selected"] == capability["run_failure"] + capability["not_attempted"]
        assert capability["completed"] == capability["passed"] == 0


@pytest.mark.parametrize("format_unit", [
    "{}", "({})", "[{}]", "**({})**", "'{}'", "`{}`", "([{}])", "( {} )",
])
@pytest.mark.parametrize("unit, expected", [("kg", True), ("lb", False)])
def test_unit_wrappers_do_not_change_the_quality_grade(format_unit, unit, expected):
    answer = f"70 {format_unit.format(unit)}<cite>[r1]</cite>"
    result = probes.grade({**NUMERIC, "unit": "kg"}, turn(answer))
    assert result["ok"] is expected


def test_wrappers_preserve_ucum_brackets_inside_a_compound_unit():
    assert probes.units_match("70 ([IU]/L)", "[IU]/L")
    assert not probes.units_match("70 ([IU]/L)", "kg")
