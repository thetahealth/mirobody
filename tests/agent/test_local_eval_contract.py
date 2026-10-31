"""Independent source and runner contracts for the 100-question local suite."""
from __future__ import annotations

import copy
import importlib
import json
import re
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


class _Raw:
    """`Response.raw` as the runner reads it: one part per read, then b''."""

    def __init__(self, *parts: bytes):
        self.parts = list(parts)

    def read1(self, amt, decode_content):
        return self.parts.pop(0) if self.parts else b""


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

        raw = _Raw(*(f"data: {json.dumps(e)}\n\n".encode() for e in events))

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


@pytest.mark.parametrize("framing", ["close", "chunked"])
def test_real_sse_large_event_arrives_before_the_deadline(monkeypatch, framing):
    """A photo in a `read_file` result is one SSE line of up to 1.5 MB. Read a
    byte at a time it took minutes, and the per-line deadline never ran."""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    import requests

    parts = [b'data: {"type":"tool_result","content":"' + b"A" * 300_000 + b'"}\n\n',
             b'data: {"type":"text","text":"Done"}\n\ndata: {"type":"end","finish_reason":"stop"}\n\n']

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1" if framing == "chunked" else "HTTP/1.0"

        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            if framing == "chunked":
                self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for part in parts:
                self.wfile.write(b"%x\r\n%s\r\n" % (len(part), part) if framing == "chunked" else part)
            if framing == "chunked":
                self.wfile.write(b"0\r\n\r\n")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(runner, "APP", f"http://127.0.0.1:{server.server_port}")
    done: dict = {}
    with requests.Session() as session:
        account = SimpleNamespace(user_id="synthetic", headers=dict, session=session,
                                  post=lambda *args: {"data": {"session_id": "loopback"}})
        # In a thread, so the stall this guards against fails the test instead
        # of hanging it: the old reader was still on this event after 280 s.
        asking = threading.Thread(target=lambda: done.update(
            result=runner.ask(account, "Synthetic large event test", 5)), daemon=True)
        try:
            asking.start()
            asking.join(timeout=10)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    assert "result" in done, "the runner was still reading one 300 KB event after 10 s"
    result = done["result"]
    assert result["finish_reason"] == "stop"
    assert [len(b["content"]) for b in result["blocks"] if b["type"] == "tool_result"] == [300_000]
    assert result["seconds"] < 2


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

        raw = _Raw(("data: " + payload).encode())

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


def facts_of(cases, case_id, labels=None):
    facts = next(c for c in cases if c["id"] == case_id)["facts"]
    return [f for f in facts if labels is None or f["label"] in labels]


@pytest.mark.parametrize(("case_id", "label", "answer", "passes"), [
    ("h100-no-genomics", "absence explicitly reported", "There is no genotype file on your account right now.", True),
    ("h100-no-genomics", "absence explicitly reported", "There's no active genotype upload on file for you.", True),
    ("h100-no-genomics", "absence explicitly reported", "I don't have any active genotype upload on file for you.", True),
    ("h100-unmeasured-analyte", "absence explicitly reported", "No insulin value is in your records.", True),
    ("h100-unmeasured-analyte", "absence explicitly reported", "I don't see any insulin level recorded in your lab reports.", True),
    ("h100-p008-med-adherence", "no intake log", "No dose of amlodipine is recorded for August 31.", True),
    ("h100-unmeasured-analyte", "absence explicitly reported", "Your insulin was 8.2 uIU/mL on 2026-07-16.", False),
    ("h100-unmeasured-analyte", "absence explicitly reported", "Insulin is not listed, but your glucose is 5.06 mmol/L.", False),
    ("h100-unmeasured-analyte", "absence explicitly reported", "No your insulin result is 7.2 uIU/mL.", False),
    ("h100-unmeasured-analyte", "absence explicitly reported", "Your insulin was 7.2; no other tests were abnormal.", False),
    ("h100-unmeasured-analyte", "absence explicitly reported", "There is no other insulin result; the one on file is 7.2.", False),
    ("h100-unmeasured-analyte", "absence explicitly reported", "I don't see any other insulin result besides 7.2.", False),
    ("h100-unmeasured-analyte", "absence explicitly reported", "The report shows no flag; insulin was 7.2.", False),
    ("h100-no-genomics", "absence explicitly reported", "Both lookups returned no genotype data.", True),
    ("h100-p008-med-adherence", "no intake log", "Yes, you took 5 mg of amlodipine on August 31.", False),
    ("h100-p008-no-call", "no-call is not normal", "No. A no-call does not establish a normal genotype.", True),
    ("h100-p008-med-adherence", "no log does not prove no intake", "A missing entry does **not** mean you didn't take it.", True),
    ("h100-p008-no-call", "no-call is not normal", "Yes, a no-call here means your genotype is normal.", False),
    ("h100-p004-pgx-evidence", "coverage limitation", "Only 7 of the 35 sites the guideline needs were called.", True),
    ("h100-p004-pgx-evidence", "coverage limitation", "The array did not type enough of the gene for a phenotype.", True),
    ("h100-p004-pgx-evidence", "coverage limitation", "Your file has every site for this gene.", False),
    ("h100-p005-lab-inequality", "relation to bound", "LDL-C 是 2.68 mmol/L，參考上限是 3.37，比上限低。", True),
    ("h100-p005-lab-inequality", "relation to bound", "LDL-C 是 2.68 mmol/L，在參考範圍內。", True),
    ("h100-p005-lab-inequality", "relation to bound", "LDL-C 是 2.68 mmol/L，比上限高。", False),
])
def test_absence_negation_and_coverage_are_read_as_phrases(manifest, case_id, label, answer, passes):
    cases, _ = manifest
    (fact,) = facts_of(cases, case_id, {label})
    assert present(fact, answer) is passes


@pytest.mark.parametrize(("case_id", "answer", "passes"), [
    ("h100-diastolic-average", "8 月共记录了 3 次舒张压，平均值是 77.7 mmHg（范围 71–83 mmHg）。", True),
    ("h100-diastolic-average", "8 月共记录了 3 次舒张压，收缩压平均值是 77.7 mmHg。", False),
    ("h100-p005-lab-inequality", "LDL-C 的資料如下：\n\n- **結果**：2.68 mmol/L\n- **參考範圍**：<3.37 mmol/L", True),
    ("h100-p005-lab-inequality", "LDL-C 的資料如下：\n\n- **HDL-C 結果**：2.68 mmol/L\n- **參考範圍**：<3.37 mmol/L", False),
    ("h100-p001-lab-range", "- **血红蛋白（HGB）测定值**：147 g/L\n- **本报告正常范围值**：130 - 175 g/L", True),
    ("h100-p001-lab-range", "- **血红蛋白（HGB）测定值**：147 g/L\n- **血糖参考范围**：130 - 175", False),
    ("h100-latest-weight", "共记录了 5 次：\n\n| 日期 | 体重 (kg) |\n|---|---|\n| **2026-08-19** | **63.8** |", True),
    ("h100-latest-weight", "| 日期 | 步数 |\n|---|---|\n| 2026-08-19 | 63.8 |", False),
    ("h100-p002-lab-trend", ("- **2026-04-02** (p002_2026-04-02_e15a.jpg): glucose **6.24 mmol/L**\n"
                             "- **2026-08-07**: glucose 7.25 mmol/L. Change: 7.25 − 6.24 = 1.01 mmol/L."), True),
    ("h100-p007-lab-excess", "ALT 74 U/L; upper limit 50 U/L.\n- Excess above the limit: **74 − 50 = 24 U/L**", True),
    ("h100-p007-lab-excess", "ALT 74 U/L; excess 50 U/L; upper limit: 74 − 50 = 24 U/L.", False),
])
def test_anchor_binds_across_a_continuing_clause_list_or_table(manifest, case_id, answer, passes):
    cases, _ = manifest
    facts = [f for f in facts_of(cases, case_id) if f["kind"] in {"number", "range"}]
    assert all(present(f, answer) for f in facts) is passes


@pytest.mark.parametrize(("answer", "passes"), [
    ("ALT 与 AST：\n- ALT：62\n- AST：74", False),
    ("- ALT：62 U/L\n- AST：74 U/L\n- 结果：74", False),
    ("ALT 62，AST 74，平均值 74。", False),
    ("| 项目 | 结果 |\n|---|---|\n| ALT | 62 |\n| AST | 74 |", False),
    ("| 项目 | ALT | AST |\n|---|---|---|\n| 结果 | 62 | 74 |", False),
    ("| 项目 | 结果 |\n|---|---|\n| ALT | 74 |\n| AST | 62 |", True),
    ("| 项目 | ALT | AST |\n|---|---|---|\n| 结果 | 74 | 62 |", True),
])
def test_continuation_never_rebinds_a_swapped_alt_ast(answer, passes):
    fact = cases_module.number(74, "ALT", anchors=["ALT"])
    fact["scope"]["other_anchors"] = ["AST"]
    assert present(fact, answer) is passes


@pytest.mark.parametrize(("answer", "passes"), [
    ("| Site | Genotype |\n|---|---|\n| rs4149056 (chr12:21331549, GRCh37) | TC — heterozygous |", True),
    ("rs4149056 (chr12:21331549, GRCh37) | SLCO1B1 | TC — heterozygous (0/1)", True),
    ("rs4149056 (SLCO1B1, chr12:21331549) is TC.", True),
    ("遺伝子ファイルで rs671 に関する記録は以下の通りです。\n\n- **座位**: 12号染色体\n- **遺伝子型**: TC", True),
    ("| Site | Genotype |\n|---|---|\n| rs4149056 | TT |\n| rs2306283 | TC |", False),
    ("rs4149056 (SLCO1B1) is TT, rs2306283 (chr12) is TC.", False),
])
def test_genotype_binds_in_table_rows_and_bracketed_sites(answer, passes):
    rsid = "rs671" if "rs671" in answer else "rs4149056"
    assert present(cases_module.genotype("TC", rsid), answer) is passes


def test_document_answer_may_use_that_files_stored_readings(manifest):
    cases, _ = manifest
    case = next(c for c in cases if c["id"] == "h100-spreadsheet-thyroid")
    run = {"person": case["person"], "finish_reason": "stop", "blocks": [
        {"type": "tool_call", "name": "query_health_indicators"},
        {"type": "text", "text": "TSH 2.336 mIU/L and free T4 19.46 pmol/L, both measured on 2026-04-29."}]}
    assert scorer.score_case(case, run)["checks"]["tool"]
    run["blocks"][0]["name"] = "query_medications"
    assert not scorer.score_case(case, run)["checks"]["tool"]


@pytest.mark.parametrize(("case_id", "answer", "passes"), [
    ("h100-sleep-hours", "In July you averaged about **7.5 hours a night**.", True),
    ("h100-sleep-hours", "In July your steps averaged 7.5 an hour.", False),
    ("h100-p005-med-history", "Your first levothyroxine dose was 25 µg once daily; it ended on 2024-09-07.", True),
    ("h100-p005-med-history", "Your first ibuprofen dose was 25 mg; it ended on 2024-09-07.", False),
    ("h100-p007-lab-qualitative", "- **PRO (protein) — Negative — Normal**", True),
    ("h100-p007-lab-qualitative", "- PRO (protein) — 1+\n- GLU (glucose) — Negative", False),
])
def test_common_short_names_bind_without_repeating_the_question(manifest, case_id, answer, passes):
    cases, _ = manifest
    facts = [f for f in facts_of(cases, case_id) if f["kind"] != "date"]
    assert all(present(f, answer) for f in facts) is passes


def test_month_day_takes_the_year_of_the_date_before_it():
    assert {"2026-08-01", "2026-08-31"} <= scorer.dates_in("2026年8月1日から8月31日の全月です。")
    assert "2026-08-31" in scorer.dates_in("From Aug 1, 2026 to Aug 31.")
    assert not scorer.dates_in("Your log opens with an entry for 12 April.")
    assert "2026-01-01" not in scorer.dates_in("8月31日の13,039歩、2026年1月の集計")


def test_questions_name_documents_as_their_owner_sees_them(manifest):
    cases, _ = manifest
    documents = [c for c in cases if c["domain"] in {"laboratory", "document"}]
    assert len(documents) == 32
    for case in documents:
        assert not any(Path(e["file"]).stem in case["question"] for e in case["evidence"] if "file" in e)
        assert not re.search(r"p00\d_|\.(?:pdf|jpg|png|xlsx)\b", case["question"])
    assert next(c for c in cases if c["id"] == "h100-handwritten-form")["question"].startswith("請讀取我 2026-08-28 的手寫表格")


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


def test_extra_cases_use_account_days_first_person_and_journal_language(tmp_path):
    expand = importlib.import_module("expand_cases")
    # A UTC account whose device writes +08:00: 07:30 local is the previous UTC day.
    steps = [{"metric": "steps", "indicator": "steps", "time": f"2026-07-{d:02d}T07:30:00+08:00", "value": 1000 + d}
             for d in range(2, 9)]
    journal = [{"person_id": "p901", "lang": "zh", "date": f"2026-07-0{i}", "text": "t", "expected": e}
               for i, e in enumerate([[{"kind": "symptom", "name": "腰痛", "symptom_id": "back"}],
                                      [{"kind": "symptom", "name": "腰痛", "symptom_id": "back"}],
                                      [{"kind": "symptom", "name": "乏力", "symptom_id": "tired"}],
                                      [{"kind": "symptom", "name": "没劲", "symptom_id": "tired"}],
                                      [{"kind": "symptom", "name": "疲倦", "symptom_id": "tired"}]], 1)]
    files = {"files.jsonl": [], "people.jsonl": [{"person_id": "p901"}], "genomics.jsonl": [], "medications.jsonl": [],
             "devices.jsonl": [{"person_id": "p901", "files": [], "records": steps}], "journal.jsonl": journal}
    for name, rows in files.items():
        (tmp_path / name).write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
    plan = {"qa_people": {"p901": {"tz": "UTC", "lang": "zh-CN", "documents": []}}}
    extra = expand.build(expand.Corpus(tmp_path), plan, 1, expand.AS_OF)
    chart = next(c for c in extra if c["id"] == "x-chart-steps-p901-en")
    assert [f["value"] for f in chart["facts"]] == [1002, 1003, 1004, 1005, 1006, 1007, 1008]
    assert [f["scope"]["date"] for f in chart["facts"]][0] == "2026-07-01"
    assert chart["evidence"][0]["tz"] == "UTC" and chart["chart"]
    assert {c["lang"] for c in extra if c["domain"] == "absence"} == {"zh-CN", "zh-TW", "ja", "en"}
    assert {c["id"] for c in extra if c["domain"] == "absence"} == {f"x-absence-rhr-p901-{lang}" for lang in expand.LANGS}
    (journal_case,) = [c for c in extra if c["domain"] == "journal"]
    assert journal_case["lang"] == "zh-CN" and "腰痛" in journal_case["question"]
    assert journal_case["facts"][0]["value"] == 2
    assert not any("p901" in c["question"] for c in extra)
