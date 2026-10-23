"""Independent source and runner contracts for the 100-question local suite."""
from __future__ import annotations

import copy
import importlib
import json
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

BENCH = Path(__file__).resolve().parents[2] / "benchmarks" / "local_models"
sys.path.insert(0, str(BENCH))
cases_module = importlib.import_module("cases")
provenance = importlib.import_module("provenance")
scorer = importlib.import_module("score")
timing = importlib.import_module("timing")
loader = importlib.import_module("load")
runner = importlib.import_module("run")
reporter = importlib.import_module("report")


@pytest.fixture
def manifest():
    cases = [json.loads(line) for line in (BENCH / "cases.jsonl").read_text().splitlines()]
    plan = json.loads((BENCH / "plan.json").read_text())
    return cases, plan


def test_frozen_suite_has_100_diverse_unique_questions(manifest):
    cases, plan = manifest
    provenance.validate_cases(cases, plan)
    assert Counter(c["lang"] for c in cases) == {"en": 60, "zh-CN": 30, "ja": 5, "zh-TW": 5}
    assert len({c["question"] for c in cases}) == 100
    assert len({c["domain"] for c in cases}) == 8
    assert len({c["tags"][0] for c in cases if c["domain"] == "laboratory"}) >= 8
    assert sum(bool(c.get("depends_on")) for c in cases) == 5
    assert plan["counts"]["selected_documents"] == 13
    smoke = provenance.select_cases(cases, smoke=True)
    assert len(smoke) == 30
    assert {c["domain"] for c in smoke} == {c["domain"] for c in cases}
    assert any("pgx" in c["tags"] for c in smoke)
    assert any("ocr" in c["tags"] for c in smoke)
    heavy = [c for c in cases if c.get("long_context")]
    assert len(heavy) == 1
    assert heavy[0]["person"] == "p002"
    assert heavy[0]["long_context"]["minimum_summaries"] == 2
    assert heavy[0]["long_context"]["max_pressure_turns"] == 16
    assert not any(c.get("long_context") for c in smoke)


def test_manifest_hash_detects_changed_gold(manifest):
    cases, plan = manifest
    changed = copy.deepcopy(cases)
    changed[0]["facts"][0]["value"] += 1
    with pytest.raises(ValueError, match="hash mismatch"):
        provenance.validate_cases(changed, plan)


def test_source_hashes_are_fail_closed_but_location_independent(tmp_path):
    source = tmp_path / "moved corpus"
    source.mkdir()
    (source / "devices.jsonl").write_text('{"synthetic":true}\n')
    corpus = SimpleNamespace(path=lambda rel: source / rel)
    plan = {"provenance": {"suite_id": provenance.SUITE_ID, "sha256": {
        "devices.jsonl": provenance.sha256(source / "devices.jsonl")}}}
    provenance.validate_corpus(corpus, plan)
    (source / "devices.jsonl").write_text('{"synthetic":false}\n')
    with pytest.raises(ValueError, match="Corpus hash mismatch"):
        provenance.validate_corpus(corpus, plan)
    (source / "devices.jsonl").unlink()
    with pytest.raises(ValueError, match="Corpus hash mismatch"):
        provenance.validate_corpus(corpus, plan)


def test_audit_failures_are_preserved_not_silently_promoted(manifest):
    _, plan = manifest
    provenance_data = plan["provenance"]
    assert not provenance_data["independent_test_set"]
    assert provenance_data["audit"]["overall"] == "failed"
    checks = {row["check"]: row for row in provenance_data["audit"]["checks"]}
    assert checks["privacy_output"]["status"] == "failed"
    assert checks["image_ocr"]["status"] == "not_run"
    assert not any(path.startswith("verification") for path in provenance_data["sha256"])
    assert "verification.json" in provenance_data["audit_sha256"]


def test_record_identity_ignores_question_translation_and_audit_path(manifest):
    _, plan = manifest
    moved = copy.deepcopy(plan)
    moved["provenance"]["audit_sha256"]["verification/privacy_output.log"] = "different-diagnostic-path"
    moved["cases_sha256"] = "translated-questions"
    assert provenance.record_identity(moved) == provenance.record_identity(plan)
    moved["qa_people"]["p001"]["documents"] = []
    assert provenance.record_identity(moved) != provenance.record_identity(plan)


def test_selecting_a_followup_includes_its_real_prerequisite(manifest):
    cases, _ = manifest
    followup = next(c for c in cases if c.get("depends_on"))
    selected = provenance.select_cases(cases, wanted=[followup["id"]])
    assert [c["id"] for c in selected] == [followup["depends_on"], followup["id"]]
    with pytest.raises(ValueError, match="Unknown case"):
        provenance.select_cases(cases, wanted=["typo"])


def test_timing_counts_native_reasoning_before_final_text_and_keeps_zero():
    measure = timing.StreamTiming()
    measure.observe({"type": "heartbeat"}, 0)
    measure.observe({"type": "status", "text": "working"}, 0)
    measure.observe({"type": "reasoning", "reasoning": " "}, 0)
    measure.observe({"type": "reasoning", "reasoning": "Checking the record"}, 0)
    measure.observe({"type": "tool_call", "name": "query_health_indicators"}, .2)
    measure.observe({"type": "text", "text": ""}, .3)
    measure.observe({"type": "text", "text": "The answer"}, 1.4)
    assert measure.as_dict() == {"first_token_s": 0, "first_reasoning_s": 0,
                                 "first_tool_s": .2, "first_text_s": 1.4}
    assert timing.summarize_timings([measure.as_dict(), {}])["first_token_s"] == {
        "n": 1, "missing": 1, "p50": 0, "p90": 0}
    assert timing.distribution([None, None]) == {"n": 0, "missing": 2, "p50": None, "p90": None}


def present(fact, answer):
    return scorer.fact_present(fact, answer, scorer.numbers(scorer.strip_dates(answer)))


@pytest.mark.parametrize("answer", ["ALT 27. AST 26.", "ALT: 27; AST: 26", "ALT 27，AST 26。"])
def test_scoring_rejects_swapped_values_even_when_both_numbers_appear(answer):
    fact = cases_module.number(26, "ALT", anchors=["ALT"])
    assert not present(fact, answer)
    assert present(fact, "ALT: 26. AST: 27.")


def test_number_sign_dates_and_genotype_site_are_bound():
    assert not present(cases_module.number(2, "change", tol=.01), "The change was -2.")
    dated = cases_module.number(26, "daily value", day="2026-08-01")
    assert present(dated, "2026-08-01 | 26")
    assert not present(dated, "2026-08-02 | 26\n2026-08-01 | 27")
    site = cases_module.genotype("CT", "rs4149056")
    assert present(site, "rs4149056: C/T")
    assert not present(site, "rs4149056 TT; rs4244285 CT")
    assert not present(site, "rs4149056 TT and rs4244285 CT")


def test_correct_value_for_wrong_subject_does_not_pass():
    case = {"id": "example", "suite_id": provenance.SUITE_ID, "person": "p001", "lang": "en",
            "question": "What is ALT?", "facts": [cases_module.number(26, "ALT", anchors=["ALT"])],
            "expect_tools": ["query_health_indicators"], "chart": False}
    run = {"person": "p002", "finish_reason": "stop", "blocks": [
        {"type": "tool_call", "name": "query_health_indicators"}, {"type": "text", "text": "ALT is 26 U/L."}]}
    score = scorer.score_case(case, run)
    assert score["checks"]["facts"]
    assert not score["checks"]["subject"]
    assert not score["pass"]


def test_medication_titration_is_dated_and_never_fabricates_dose_events():
    plan = {"plan_id": "fixture-m1", "name_en": "Levothyroxine", "name_zh": "左甲状腺素", "generic": "levothyroxine",
            "strength": "25 ug", "frequency": "qd", "start": "2024-07-25", "end": None,
            "dose_text_en": "25 mcg once daily", "dose_text_zh": "每日25微克",
            "changes": [{"date": "2024-09-08", "strength": "50 ug", "dose_text_en": "50 mcg once daily", "dose_text_zh": "每日50微克"}]}
    versions = provenance.medication_versions(plan)
    assert [(v["strength"], v["start"], v["end"]) for v in versions] == [
        ("25 ug", "2024-07-25", "2024-09-07"), ("50 ug", "2024-09-08", None)]
    body = loader.medication_body(versions[0])
    assert body["schedule"][0]["dose"] == {"value": 25, "unit": "ug"}
    assert "times" not in body["schedule"][0]
    assert "taken" not in body and "missed" not in body


def test_followup_runner_reuses_session_without_extra_model_calls(monkeypatch, manifest):
    cases, plan = manifest
    selected = provenance.select_cases(cases, wanted=["h100-conversation-1-followup"])
    account = SimpleNamespace(lang="zh")
    loaded = {runner.qa_email("p001"): {"record_sha256": provenance.record_identity(plan)}}
    monkeypatch.setattr(runner, "read_json", lambda *args: loaded)
    monkeypatch.setattr(runner, "sign_in", lambda *args, **kwargs: account)
    monkeypatch.setattr(runner, "check", lambda *args: True)
    monkeypatch.setattr(runner, "swapouts", lambda: 0)
    calls = []

    def ask(account, question, timeout, *, session_id=None, as_of=None):
        assert as_of == provenance.AS_OF
        calls.append(session_id)
        return {"session_id": "same-session", "finish_reason": "stop", "seconds": .1,
                "blocks": [{"type": "text", "text": "没有足够数据。"}]}

    monkeypatch.setattr(runner, "ask", ask)
    runs = runner.run_questions("small", selected, 5, plan)
    assert calls == [None, "same-session"]
    assert runs[1]["prior_blocks"] == runs[0]["blocks"]
    assert len(runs) == 2


def test_failed_prerequisite_remains_in_denominator(monkeypatch, manifest):
    cases, plan = manifest
    selected = provenance.select_cases(cases, wanted=["h100-conversation-1-followup"])
    account = SimpleNamespace(lang="zh")
    loaded = {runner.qa_email("p001"): {"record_sha256": provenance.record_identity(plan)}}
    monkeypatch.setattr(runner, "read_json", lambda *args: loaded)
    monkeypatch.setattr(runner, "sign_in", lambda *args, **kwargs: account)
    monkeypatch.setattr(runner, "check", lambda *args: True)
    monkeypatch.setattr(runner, "swapouts", lambda: 0)
    calls = []

    def ask(*args, **kwargs):
        calls.append(1)
        return {"session_id": "failed", "finish_reason": None, "seconds": 1, "blocks": [], "error": "timeout"}

    monkeypatch.setattr(runner, "ask", ask)
    runs = runner.run_questions("small", selected, 5, plan)
    assert len(calls) == 1
    assert len(runs) == 2
    assert runs[1]["blocked_by"] == runs[0]["id"]
    assert runs[1]["seconds"] is None
    assert not any(run["score"]["pass"] for run in runs)


def test_http_sse_first_visible_token_is_reasoning(monkeypatch):
    clock = iter(n / 100 for n in range(100))
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(clock))
    events = [{"type": "heartbeat"}, {"type": "status", "text": "queued"},
              {"type": "reasoning", "reasoning": " "},
              {"type": "reasoning", "reasoning": "Checking source evidence"},
              {"type": "tool_call", "name": "query_health_indicators", "args": {}},
              {"type": "text", "text": "ALT is 26 U/L."}, {"type": "end", "finish_reason": "stop"}]

    class Response:
        status_code = 200
        headers = {"content-type": "text/event-stream"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        def iter_lines(self, *, decode_unicode, chunk_size):
            assert decode_unicode and chunk_size == 1
            return iter("data: " + json.dumps(e) for e in events)

    def send(*args, **kwargs):
        assert kwargs["json"]["as_of"] == provenance.AS_OF
        return Response()

    account = SimpleNamespace(user_id="fixture", headers=dict,
                              post=lambda *args: {"data": {"session_id": "session"}},
                              session=SimpleNamespace(post=send))
    result = runner.ask(account, "What is ALT?", 5)
    assert result["first_token_s"] == result["first_reasoning_s"]
    assert result["first_reasoning_s"] < result["first_tool_s"] < result["first_text_s"]
    assert result["finish_reason"] == "stop"
    assert scorer.answer_of(result["blocks"]) == "ALT is 26 U/L."


def test_disabled_eval_clock_fails_immediately_without_waiting_for_sse(monkeypatch):
    class Refused:
        status_code = 200
        headers = {"content-type": "application/json"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        def json(self):
            return {"code": -5, "msg": "as_of is not enabled on this server (EVAL_AS_OF_ENABLED is off)."}

    account = SimpleNamespace(user_id="fixture", headers=dict,
                              post=lambda *args: {"data": {"session_id": "session"}},
                              session=SimpleNamespace(post=lambda *args, **kwargs: Refused()))
    monkeypatch.setattr(runner, "_wait_answer", lambda *args, **kwargs: pytest.fail("Rejected requests must not wait"))
    result = runner.ask(account, "What is ALT?", 5)
    assert result["configuration_error"]
    assert "EVAL_AS_OF_ENABLED" in result["error"]
    assert result["first_token_s"] is None


def test_report_rejects_cross_manifest_and_cross_selection_comparisons(tmp_path, manifest):
    cases, plan = manifest
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    data = {"runs": [], "case_manifest": cases, "cases_sha256": plan["cases_sha256"],
            "selected_ids": [c["id"] for c in cases]}
    (a / "qa.json").write_text(json.dumps(data))
    other = copy.deepcopy(data)
    other["selected_ids"] = other["selected_ids"][:30]
    (b / "qa.json").write_text(json.dumps(other))
    with pytest.raises(ValueError, match="Cannot compare"):
        reporter.comparison_cases([a, b])
    other = copy.deepcopy(data)
    other["case_manifest"][0]["facts"][0]["value"] += 1
    other["cases_sha256"] = provenance.digest_json(other["case_manifest"])
    (b / "qa.json").write_text(json.dumps(other))
    with pytest.raises(ValueError, match="Cannot compare"):
        reporter.comparison_cases([a, b])


def test_unattempted_rows_remain_missing_in_latency_denominator(tmp_path, manifest):
    cases, plan = manifest
    case = cases[0]
    result = {"id": case["id"], "person": case["person"], "domain": case["domain"],
              "seconds": 2, "finish_reason": None, "blocks": [], "error": "unavailable", "first_token_s": .3}
    result["score"] = scorer.score_case(case, result)
    data = {"runs": [result], "case_manifest": cases, "cases_sha256": plan["cases_sha256"],
            "suite_id": provenance.SUITE_ID, "selected_ids": [c["id"] for c in cases]}
    (tmp_path / "qa.json").write_text(json.dumps(data))
    summary = runner.summarize(tmp_path)
    assert summary["requested_n"] == 100
    assert summary["unattempted_n"] == 99
    assert not summary["complete"]
    assert summary["latency"]["first_token_s"] == {"n": 1, "missing": 99, "p50": .3, "p90": .3}
    assert summary["latency"]["first_reasoning_s"]["missing"] == 100
    assert "n=1, missing=99" in reporter.latency_cell(summary, "first_token_s")


def test_historical_timings_do_not_invent_reasoning_latency():
    old = {"runs": [{"id": "p002-weight-latest"}]}
    assert len(provenance.saved_manifest(old)) == 30
    latency = timing.summarize_timings([{"first_text_s": 5, "seconds": 6}])
    assert latency["first_token_s"] == {"n": 0, "missing": 1, "p50": None, "p90": None}
    assert latency["first_text_s"]["p50"] == 5


def test_real_sse_delivers_thinking_before_server_finishes(monkeypatch):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    import requests

    received = threading.Event()
    observed_before_final = []

    class ObservedTiming(timing.StreamTiming):
        def observe(self, block, elapsed):
            super().observe(block, elapsed)
            if block.get("type") == "reasoning":
                received.set()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.end_headers()
            self.wfile.write(b'data: {"type":"reasoning","reasoning":"Checking evidence"}\n\n')
            self.wfile.flush()
            observed_before_final.append(received.wait(timeout=2))
            self.wfile.write(b'data: {"type":"text","text":"Done"}\n\ndata: {"type":"end","finish_reason":"stop"}\n\n')
            self.wfile.flush()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(runner, "APP", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(runner, "StreamTiming", ObservedTiming)
    with requests.Session() as session:
        account = SimpleNamespace(user_id="synthetic", headers=dict, session=session,
                                  post=lambda *args: {"data": {"session_id": "loopback"}})
        try:
            result = runner.ask(account, "Synthetic streaming test", 5)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    assert observed_before_final == [True]
    assert result["finish_reason"] == "stop"
    assert result["first_token_s"] == result["first_reasoning_s"]
    assert result["first_token_s"] <= result["first_text_s"]


@pytest.mark.parametrize("payload", ["not-json", "[]", '{"type":"reasoning","reasoning":42}'])
def test_malformed_sse_is_a_recorded_case_failure(monkeypatch, payload):
    class Malformed:
        status_code = 200
        headers = {"content-type": "text/event-stream"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        def iter_lines(self, **kwargs):
            return iter(["data: " + payload])

    account = SimpleNamespace(user_id="synthetic", headers=dict,
                              post=lambda *args: {"data": {"session_id": "malformed"}},
                              session=SimpleNamespace(post=lambda *args, **kwargs: Malformed()))
    monkeypatch.setattr(runner, "_wait_answer", lambda *args, **kwargs: pytest.fail("Malformed SSE must fail immediately"))
    result = runner.ask(account, "Synthetic malformed response test", 5)
    assert result["error"]
    assert result["finish_reason"] is None
    assert result["first_token_s"] is None


def test_actual_alt_ast_case_rejects_single_clause_swap(manifest):
    cases, _ = manifest
    case = next(c for c in cases if c["id"] == "h100-p001-lab-pair")
    wrong = "本次报告的 ALT 为 27.0 U/L 与 AST 为 18.0 U/L。"
    correct = "本次报告的 ALT 为 18.0 U/L 与 AST 为 27.0 U/L。"
    assert not any(present(fact, wrong) for fact in case["facts"])
    assert all(present(fact, correct) for fact in case["facts"])


def test_signed_change_oracle_rejects_opposite_sign(manifest):
    cases, _ = manifest
    case = next(c for c in cases if c["id"] == "h100-resting-change")
    signed = next(f for f in case["facts"] if f["label"] == "signed change")
    assert signed["value"] == -3
    assert present(signed, "Signed change: -3 bpm, a decrease.")
    assert not present(signed, "Signed change: +3 bpm, a decrease.")


@pytest.mark.parametrize(("case_id", "answer", "passes"), [
    ("h100-sleep-observed-total", "Your total sleep was 47.6 hours.", True),
    ("h100-sleep-observed-total", "Total sleep: 2858 minutes.", True),
    ("h100-sleep-observed-total", "Total sleep: 47 h 38 min.", True),
    ("h100-sleep-observed-total", "Total sleep: 47.2 hours.", False),
    ("h100-conversation-4-start", "Mean sleep was 7.4 hours per night.", True),
    ("h100-conversation-4-start", "Mean sleep was 7.5 hours per night.", False),
])
def test_sleep_duration_passes_in_any_unit_but_not_a_wrong_length(manifest, case_id, answer, passes):
    cases, _ = manifest
    (fact,) = next(c for c in cases if c["id"] == case_id)["facts"]
    assert fact["kind"] == "duration"
    assert present(fact, answer) is passes


@pytest.mark.parametrize(("case_id", "answer", "passes"), [
    ("h100-conversation-1-followup", "The next week averaged 9586 steps, 554 fewer: lower.", True),
    ("h100-conversation-1-followup", "The next week averaged 9580 steps, 560 fewer: lower.", False),
    ("h100-diastolic-average", "舒张压平均值为 78 mmHg。", True),
    ("h100-diastolic-average", "舒张压平均值为 77 mmHg。", False),
    ("h100-conversation-3-start", "舒张压平均值为 82 mmHg。", True),
    ("h100-conversation-3-start", "舒张压平均值为 81 mmHg。", False),
    ("h100-blood-pressure-pair", "收縮壓 112 mmHg，舒張壓 83 mmHg。", False),
])
def test_device_tolerance_accepts_rounding_but_not_a_neighbouring_value(manifest, case_id, answer, passes):
    cases, _ = manifest
    numeric = [f for f in next(c for c in cases if c["id"] == case_id)["facts"] if f["kind"] == "number"]
    assert all(present(f, answer) for f in numeric) is passes


@pytest.mark.parametrize(("answer", "passes"), [
    ("Your latest weight is 69.7 kg, which is about 153.7 lb.", True),
    ("Your latest weight is 69.7 kg, which is about 153.7 lbs.", True),
    ("Latest reading: 69.7 kilograms. In pounds: ~153.66 pounds.", True),
    ("Your latest weight is 69.7 kg, which is about 153.9 lbs.", False),
    ("Your latest weight is 153.7 kg, which is about 69.7 lbs.", False),
])
def test_unit_anchors_bind_plural_words_but_not_swapped_values(manifest, answer, passes):
    cases, _ = manifest
    facts = next(c for c in cases if c["id"] == "h100-kilograms-to-pounds")["facts"]
    assert all(present(f, answer) for f in facts) is passes


@pytest.mark.parametrize(("language", "answer"), [
    ("en", "The result was 26 U/L."),
    ("zh-CN", "检查结果是 26，记录里没有其他数据。"),
    ("zh-TW", "檢查結果是 26，記錄裡沒有其他數據。"),
    ("ja", "検査結果は 26 です。記録を確認しました。"),
])
def test_frontend_languages_are_scored_separately(language, answer):
    assert scorer.language_of(answer) == language
    assert scorer.language_matches(language, answer)
    assert not scorer.language_matches("en" if language != "en" else "ja", answer)


def test_shared_chinese_characters_do_not_imply_wrong_script():
    assert scorer.language_matches("zh-TW", "未分型，不能假定正常。")
    assert scorer.language_matches("zh-CN", "未分型，不能假定正常。")
    assert scorer.language_matches("zh", "原始记录没有数据。")
    assert not scorer.language_matches("zh-TW", "原始记录没有数据。")


def test_sleep_oracle_uses_source_interval_and_evening_date(tmp_path):
    payload = {"indicator": "sleep", "time": "2026-07-02T00:20:00+00:00", "value": 480,
               "end_time": "2026-07-02T08:20:30+00:00"}
    (tmp_path / "batch.json").write_text(json.dumps({"records": [payload]}))
    corpus = SimpleNamespace(devices={"fixture": {"files": ["batch.json"],
                                 "records": [{**payload, "metric": "sleep"}]}}, path=lambda rel: tmp_path / rel)
    values = cases_module.device_series(corpus, "fixture", "sleep", "UTC")
    assert values[0][0].date().isoformat() == "2026-07-01"
    assert values[0][1] == 480.5


def test_localized_fact_aliases_accept_japanese_and_traditional_answers(manifest):
    cases, _ = manifest
    japanese = next(c for c in cases if c["id"] == "h100-p001-lab-convert")
    value = japanese["facts"][0]["value"]
    answer = f"ヘモグロビンは {value} g/dL です。"
    assert scorer.language_matches("ja", answer)
    assert all(present(fact, answer) for fact in japanese["facts"])
    traditional = next(c for c in cases if c["id"] == "h100-blood-pressure-pair")
    systolic, diastolic, day = [f["value"] for f in traditional["facts"]]
    answer = f"收縮壓為 {systolic} mmHg，舒張壓為 {diastolic} mmHg，量測日期為 {day}。"
    assert scorer.language_matches("zh-TW", answer)
    assert all(present(fact, answer) for fact in traditional["facts"])


def test_long_case_cannot_pass_without_two_completed_distinct_summaries(manifest):
    cases, _ = manifest
    case = next(c for c in cases if c.get("long_context"))
    run = {"person": case["person"], "finish_reason": "stop", "blocks": [],
           "long_context": {"status": "exercised", "observed_summaries": 2,
                            "summary_cutoffs": [4, 4], "summary_calls_completed": 2}}
    assert not scorer.score_case(case, run)["checks"]["context"]
    run["long_context"]["summary_cutoffs"] = [4, 12]
    run["long_context"]["summary_calls_completed"] = 1
    assert not scorer.score_case(case, run)["checks"]["context"]
    run["long_context"]["summary_calls_completed"] = 2
    assert scorer.score_case(case, run)["checks"]["context"]
    assert not scorer.score_case(case, run)["pass"]  # No final factual answer.


def test_pressure_usage_is_counted_once_and_prior_history_not_double_counted():
    def used(input_tokens):
        return {"type": "usage", "input_tokens": input_tokens, "output_tokens": 2}

    result = {"blocks": [used(10)], "prior_blocks": [used(100), used(20)],
              "pressure_turns": [{"blocks": [used(20)]}, {"blocks": [used(30)]}]}
    usage = runner._usage(result)
    assert usage["input"] == 60
    assert usage["output"] == 6


def test_runner_dispatches_heavy_followup_through_source_pressure_helper(monkeypatch, manifest):
    import long_context

    cases, plan = manifest
    selected = provenance.select_cases(cases, wanted=["h100-conversation-2-followup"])
    loaded = {runner.qa_email("p002"): {"record_sha256": provenance.record_identity(plan)}}
    account = SimpleNamespace(lang="en")
    corpus = SimpleNamespace(dir="verified corpus")
    monkeypatch.setattr(runner, "read_json", lambda *args: loaded)
    monkeypatch.setattr(runner, "sign_in", lambda *args, **kwargs: account)
    monkeypatch.setattr(runner, "check", lambda *args: True)
    monkeypatch.setattr(runner, "swapouts", lambda: 0)
    called = []

    def ask(account, question, timeout, **kwargs):
        called.append("baseline")
        return {"session_id": "real-session", "finish_reason": "stop", "seconds": .1,
                "blocks": [{"type": "text", "text": "The baseline is recorded."}]}

    def stress(account, case, *, parent, corpus: object, ask, timeout):
        called.append("source-pressure-helper")
        assert parent["session_id"] == "real-session"
        assert corpus.dir == "verified corpus"
        return {"session_id": "real-session", "finish_reason": None, "seconds": 0, "blocks": [],
                "error": "not exercised", "pressure_turns": [], "context_setup_seconds": 0,
                "long_context": {"status": "not_exercised", "observed_summaries": 0,
                                 "summary_cutoffs": [], "summary_calls_completed": 0}}

    monkeypatch.setattr(runner, "ask", ask)
    monkeypatch.setattr(long_context, "run_long_context", stress)
    runs = runner.run_questions("small", selected, 5, plan, corpus=corpus)
    assert called == ["baseline", "source-pressure-helper"]
    assert len(runs) == 2
    assert not runs[1]["score"]["checks"]["context"]
    assert not runs[1]["score"]["pass"]


def case_fact(cases, case_id, label):
    return next(f for f in next(c for c in cases if c["id"] == case_id)["facts"] if f["label"] == label)


@pytest.mark.parametrize(("case_id", "answer", "passes"), [
    ("h100-no-genomics", "There is no genotype file on your account.", True),
    ("h100-no-genomics", "I don't have any active genotype upload on file for you.", True),
    ("h100-unmeasured-analyte", "No insulin value is in your records.", True),
    ("h100-unmeasured-analyte", "No — there is no insulin result in your uploaded reports.", True),
    ("h100-unmeasured-analyte", "I don't see any insulin level recorded.", True),
    ("h100-p008-med-adherence", "No dose of amlodipine is recorded for August 31.", True),
    ("h100-unmeasured-analyte", "Your insulin result is 7.2 uIU/mL.", False),
    ("h100-unmeasured-analyte", "No, your insulin value is 7.2 uIU/mL.", False),
    ("h100-unmeasured-analyte", "No problem: your insulin result is 7.2 uIU/mL.", False),
    ("h100-unmeasured-analyte", "There is no doubt your insulin value is 7.2 uIU/mL.", False),
])
def test_absence_is_read_as_a_phrase_but_a_reported_value_is_not_absence(manifest, case_id, answer, passes):
    cases, _ = manifest
    absent = next(f for f in next(c for c in cases if c["id"] == case_id)["facts"]
                  if f["label"] in {"absence explicitly reported", "no intake log"})
    assert present(absent, answer) is passes


@pytest.mark.parametrize(("case_id", "label", "answer", "passes"), [
    ("h100-p008-no-call", "no-call is not normal", "A no-call does not establish a normal genotype.", True),
    ("h100-p008-med-adherence", "no log does not prove no intake",
     "A missing entry does not mean you didn't take it.", True),
    ("h100-p008-med-adherence", "no log does not prove no intake",
     "A missing entry in the log does **not** mean you didn't take the medication.", True),
    ("h100-p008-no-call", "no-call is not normal", "Yes, a no-call means your genotype is normal.", False),
])
def test_limits_of_evidence_are_read_as_phrases(manifest, case_id, label, answer, passes):
    cases, _ = manifest
    assert present(case_fact(cases, case_id, label), answer) is passes


@pytest.mark.parametrize(("answer", "passes"), [
    ("8 月共记录了 3 次舒张压，平均值是 77.7 mmHg。", True),
    ("8 月共记录了 3 次舒张压，平均值是 77.0 mmHg。", False),
    # A clause takes the label written before it only when no other measure intervenes.
    ("8 月舒张压与收缩压都有记录，平均值是 77.7 mmHg。", False),
    ("舒张压平均 110.3 mmHg，收缩压 77.7 mmHg。", False),
])
def test_an_unlabelled_clause_takes_the_label_before_it_in_its_sentence(manifest, answer, passes):
    cases, _ = manifest
    assert present(case_fact(cases, "h100-diastolic-average", "舒张压"), answer) is passes


def test_a_list_line_takes_the_label_of_its_heading_or_an_earlier_item(manifest):
    cases, _ = manifest
    ldl = case_fact(cases, "h100-p005-lab-inequality", "ldl")
    listed = "LDL-C 的資料如下：\n\n- **結果**：2.68 mmol/L\n- **參考範圍**：<3.37 mmol/L"
    assert present(ldl, listed)
    assert not present(ldl, listed.replace("2.68", "3.37").replace("<3.37", "<2.68"))
    hgb = case_fact(cases, "h100-p001-lab-range", "printed hemoglobin reference interval")
    assert present(hgb, "实验室检查部分：\n\n- **血红蛋白（HGB）测定值**：147 g/L\n- **本报告正常范围值**：130 - 175 g/L")
    assert present(hgb, "血红蛋白（HGB）：147 g/L，报告上印的参考范围是 130 - 175 g/L。")
    assert not present(hgb, "- 血红蛋白（HGB）：147 g/L\n- 血糖：5.1 mmol/L\n- 参考范围：130 - 175")
    weight = case_fact(cases, "h100-latest-weight", "体重")
    assert present(weight, "| 日期 | 体重 (kg) |\n|---|---|\n| 2026-08-19 | 63.8 |")
    site = cases_module.genotype("GA", "rs671")
    assert present(site, "rs671 の記録は以下の通りです。\n\n- **座位**: 12号染色体\n- **遺伝子型**: GA")
    assert not present(site, "rs671 と rs4149056 の記録：\n\n- **遺伝子型**: GA")
    alt = case_fact(cases, "h100-p001-lab-pair", "alt")
    assert not present(alt, "| ALT | AST |\n|---|---|\n| 27 | 18 |")
    assert not present(alt, "ALT 与 AST 如下：\n- 结果 27\n- 结果 18")


def test_a_file_name_does_not_split_a_date_from_its_value(manifest):
    cases, _ = manifest
    dated = case_fact(cases, "h100-p002-lab-trend", "2026-04-02")
    assert present(dated, "- **2026-04-02** (p002_2026-04-02_e15a.jpg): Fasting plasma glucose **6.24 mmol/L**")
    assert not present(dated, "- **2026-04-02** (p002_2026-04-02_e15a.jpg): see below.\n- 6.24 mmol/L on 2026-08-07")


def test_a_document_can_be_answered_from_its_stored_readings(manifest):
    cases, _ = manifest
    case = next(c for c in cases if c["id"] == "h100-spreadsheet-thyroid")
    tsh, ft4 = (f["value"] for f in case["facts"] if f["kind"] == "number")
    run = {"person": case["person"], "finish_reason": "stop", "blocks": [
        {"type": "tool_call", "name": "query_health_indicators"},
        {"type": "text", "text": f"TSH was {tsh} mIU/L and free T4 {ft4} pmol/L, both measured on 2026-04-29."}]}
    score = scorer.score_case(case, run)
    assert score["checks"]["tool"]
    assert score["checks"]["facts"]
    assert all("read_file" in c["expect_tools"] for c in cases if c["domain"] == "document")


def test_a_medication_is_named_by_its_common_short_name(manifest):
    cases, _ = manifest
    name = case_fact(cases, "h100-p005-med-history", "medication name")
    dose = case_fact(cases, "h100-p005-med-history", "historical dose")
    answer = "Your first levothyroxine dose was 25 ug once daily, until 2024-09-07."
    assert present(name, answer) and present(dose, answer)
    assert not present(dose, "Your first ibuprofen dose was 25 mg.")
    assert "左甲状腺素" in name["any"]
