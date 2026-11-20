"""A journal sentence's complaints, coded as a person writes them (#321).

A self-hoster wrote "下午太阳穴跳痛、头晕嗜睡" (synthetic). `头晕嗜睡` names two
complaints, came back from the model as one entry in one run and as two in
the next, and as one it could not be coded: the vocabulary is matched whole.
`嗜睡`, `视线发糊` and a temple pain had no Chinese row at all, while the
English for the same complaints codes through ICPC-3's own inclusions.

The vocabulary cases need only the library; the journal's split needs the
``[app]`` extra and skips without it.
"""

from datetime import UTC, datetime
from importlib import import_module

import pytest

from mirobody.standardize import resolve_symptom


@pytest.fixture
def sentence():
    pytest.importorskip("sqlalchemy")
    return import_module("mirobody.collect.sentence")


@pytest.mark.parametrize("text,code,english", [
    # ICPC-3 lists drowsiness and drowsy as inclusions of AS99.
    ("嗜睡", "AS99", "drowsiness"),
    ("犯困", "AS99", "drowsy"),
    ("困倦", "AS99", "drowsiness"),
    # And blurred vision under FS05, beside 视物模糊 and 眼花.
    ("视线发糊", "FS05", "blurred vision"),
    ("视线模糊", "FS05", "blurred vision"),
    ("看东西模糊", "FS05", "blurred vision"),
    # The temple is the side of the head, not the face (NS02 excludes headache).
    ("太阳穴痛", "NS01", "headache"),
    ("太阳穴疼", "NS01", "headache"),
    ("太阳穴跳痛", "NS01", "headache"),
])
def test_an_everyday_chinese_spelling_codes_as_its_english_does(text, code, english):
    coding, reference = resolve_symptom(text), resolve_symptom(english)
    assert coding.coded and coding.code == code
    assert reference.code == code


@pytest.mark.parametrize("name,halves", [
    ("头晕嗜睡", ("头晕", "嗜睡")),
    ("头晕乏力", ("头晕", "乏力")),
    ("头痛头晕", ("头痛", "头晕")),
    ("頭暈嗜睡", ("頭暈", "嗜睡")),
    # A leading qualifier stays with the complaint it qualifies.
    ("有点头晕嗜睡", ("有点头晕", "嗜睡")),
])
def test_two_complaints_written_back_to_back_are_two(sentence, name, halves):
    assert sentence._two_complaints(name) == halves
    assert all(resolve_symptom(h).coded for h in halves)


@pytest.mark.parametrize("name", [
    "头晕",            # one complaint, coded whole
    "视物模糊",         # coded whole, though 视物 and 模糊 are words
    "头晕不嗜睡",       # the second half denies its complaint, and does not code
    "头晕嗜睡乏力",     # three complaints: not exactly two halves
    "头晕xx",          # a half that codes nothing
    "头痛太阳穴痛",     # one headache said twice: both halves are NS01
    "头痛头疼",         # two spellings of NS01
    "头晕头晕",         # a literal repeat
    "视物模糊眼花",     # both FS05
    "headache dizziness",  # Latin script separates its words already
    "",
])
def test_anything_else_stays_one_entry(sentence, name):
    assert sentence._two_complaints(name) is None


def _part(sentence, name, *, quote="下午太阳穴跳痛、头晕嗜睡", assertion="present", detail=""):
    return sentence.Part(quote=quote, kind=sentence.KIND_SYMPTOM, name=name, assertion=assertion, detail=detail)


def test_the_journal_writes_two_coded_complaints(sentence):
    """The reported sentence, through `plan` and the writer's coding: two
    complaints run together are two coded entries, and both keep the
    part's detail; the temple pain codes on its own."""
    from mirobody.collect import observations

    text = "下午太阳穴跳痛、头晕嗜睡"
    now = datetime(2026, 11, 20, 18, 0, tzinfo=UTC)
    parts = [_part(sentence, "太阳穴跳痛"), _part(sentence, "头晕嗜睡", detail="下午")]
    drafts, skipped = sentence.plan(parts, sentence=text, now=now, default_at=now, tz="UTC")
    assert not skipped
    assert [(d.name_text, d.kind, d.note_text) for d in drafts] == [
        ("太阳穴跳痛", observations.KIND_SYMPTOM, ""),
        ("头晕", observations.KIND_SYMPTOM, "下午"),
        ("嗜睡", observations.KIND_SYMPTOM, "下午"),
    ]
    provenance = observations.Provenance(observations.MODALITY_SELF, observations.SOURCE_MANUAL, "journal:synthetic")
    codes = [observations.coding_for(observations.prepare(d, provenance, "synthetic-321", "UTC", now=now), {}).code
             for d in drafts]
    assert codes == ["NS01", "NS09", "AS99"]


def test_a_denied_pair_is_two_denials(sentence):
    """`没有头晕嗜睡` denies both: each half is skipped as negated, and
    neither is written."""
    text = "今天没有头晕嗜睡"
    now = datetime(2026, 11, 20, 18, 0, tzinfo=UTC)
    drafts, skipped = sentence.plan([_part(sentence, "头晕嗜睡", quote="没有头晕嗜睡", assertion="negated")],
                                    sentence=text, now=now, default_at=now)
    assert drafts == []
    assert [(s.name, s.reason) for s in skipped] == [("头晕", sentence.SKIP_NEGATED), ("嗜睡", sentence.SKIP_NEGATED)]
