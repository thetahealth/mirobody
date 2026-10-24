"""What the judge is given, and what it refuses to accept as a verdict.

The judge is the second reader for answers the literal checks reject over wording, so the
properties here are about trust in its input and output: the frozen manifest decides which
gold is used, every tool result of the turn and of the earlier turns reaches it, a duration
keeps its kind, a malformed reply is an evaluator failure, and the cache key covers the
rubric, the judge, the gold and the evidence.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

_JUDGE = Path(__file__).resolve().parents[2] / "benchmarks" / "local_models" / "judge.py"
_spec = importlib.util.spec_from_file_location("local_judge", _JUDGE)
_judge = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_judge)

answer_text = _judge.answer_text
cache_key = _judge.cache_key
evidence_text = _judge.evidence_text
facts_text = _judge.facts_text
saved_manifest = _judge.saved_manifest
validate = _judge.validate

CASE = {
    "id": "c1",
    "question": "What was my resting heart rate in August?",
    "facts": [
        {"kind": "number", "value": 53.0, "tol": 0.5, "unit": "bpm", "label": "first"},
        {"kind": "words", "any": ["rise", "fall"], "label": "direction"},
    ],
}
RUN = {
    "prior_blocks": [{"type": "tool_result", "content": "prior turn row"}],
    "blocks": [
        {"type": "tool_result", "status": "ok", "name": "query_health_indicators",
         "content": "rid|name|value\nr1|heart|53\nr2|heart|50"},
        {"type": "reasoning", "reasoning": "thinking"},
        {"type": "text", "text": "It fell from 53 to 50."},
    ],
}


def test_facts_keep_tolerance_and_unit() -> None:
    text = facts_text(CASE)
    assert "first: 53.0 (number unit=bpm tol=±0.5)" in text
    assert "direction: one of ['rise', 'fall']" in text


def test_evidence_includes_prior_turns_and_excludes_reasoning() -> None:
    text = evidence_text(RUN)
    assert "prior turn row" in text
    assert "r1|heart|53" in text
    assert "thinking" not in text


def test_answer_is_only_the_text_blocks() -> None:
    assert answer_text(RUN) == "It fell from 53 to 50."


def test_a_malformed_verdict_is_not_accepted() -> None:
    for bad in ({}, {"correct": "yes"}, {"correct": True, "missing": "x"},
                {"missing": [], "unsupported": []}):
        try:
            validate(bad)
        except ValueError:
            continue
        raise AssertionError(f"accepted a malformed verdict: {bad}")


def test_a_well_formed_verdict_round_trips() -> None:
    verdict = validate({"correct": False, "missing": ["first"], "unsupported": ["99"], "note": "wrong"})
    assert json.loads(json.dumps(verdict))["unsupported"] == ["99"]


def test_the_cache_key_covers_gold_evidence_and_model() -> None:
    answer = answer_text(RUN)
    base = cache_key(CASE, RUN, answer, "judge-a")
    changed_model = cache_key(CASE, RUN, answer, "judge-b")
    changed_gold = cache_key({**CASE, "facts": [{"kind": "number", "value": 999, "label": "first"}]},
                             RUN, answer, "judge-a")
    changed_answer = cache_key(CASE, RUN, answer + " ", "judge-a")
    assert len({base, changed_model, changed_gold, changed_answer}) == 4


def test_the_frozen_manifest_is_the_default_case_list() -> None:
    run_data = {"case_manifest": [CASE], "runs": []}
    assert saved_manifest(run_data)[0]["id"] == "c1"
    try:
        saved_manifest({"runs": []})
    except SystemExit:
        return
    raise AssertionError("a run without a manifest must not be judged against live cases")
