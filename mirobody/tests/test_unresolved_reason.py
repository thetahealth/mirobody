"""Every surface says why a name got no code, as a word and in a sentence.

`mirobody resolve 血脂` said "a category or several tests in one name"; over
MCP, `resolve_indicator` answered `resolved: false` and `standardize_reading`
`method: refused`, with no reason on either. A client could not tell "this
names a panel, ask for one test" from "this spelling is missing", which is
what a reader of the resolver demo asked for.

The sentence alone was not enough. Every deliberate non-answer in
`resolver_overrides.tsv` was one `!unresolved`, so `KET` (one urine test whose
abbreviation names three analytes), `发烧` (a symptom) and `BMR` (a number
LOINC has no code for) all came back "a category or several tests in one
name". `reason_kind` is the answer a program branches on, and each block row
now says which kind it is.
"""

import pytest

from mirobody.engine.observation import decision, standardize_reading
from mirobody.engine.resolver import _REASON_SENTENCES, resolve, resolve_reading, unresolved_kind, unresolved_reason
from mirobody.standardize.terminology import resolve_indicators

CATEGORY = "a category or several tests in one name, and no code is given rather than a guessed one"
MISSING = "not in the lexical index, and no code is given rather than a guessed one"


@pytest.mark.parametrize(("name", "kind"), [
    ("血脂", "category"),           # four analytes
    ("肿瘤标志物", "category"),      # a report heading
    ("测定", "category"),           # a word from a name
    ("发烧", "condition"),          # a symptom
    ("糖尿病", "condition"),        # a diagnosis
    ("KET", "ambiguous"),           # ketones, or a gene, or ketamine
    ("酸碱度", "ambiguous"),        # urine or blood
    ("BMR", "uncoded"),             # LOINC has only a panel for it
    ("RDW-SD", "uncoded"),          # 2.83 dropped the entitic-volume code
    ("血糖(OGTT)", "timing"),       # one draw of a series, not which one
    ("XYZ指数", "unknown"),
])
def test_each_refusal_names_its_kind(name, kind):
    found = resolve(name)
    assert not found.resolved
    assert unresolved_kind(found) == kind
    assert unresolved_reason(found)


def test_the_sentence_for_a_kind_without_a_reason_of_its_own():
    assert unresolved_reason(resolve("血脂")) == CATEGORY
    assert unresolved_reason(resolve("XYZ指数")) == MISSING
    assert "ICPC-3" in unresolved_reason(resolve("发烧"))
    assert unresolved_reason(resolve("血红蛋白")) == "" and unresolved_kind(resolve("血红蛋白")) == ""


def test_a_tie_says_the_unit_decides():
    found = resolve("TLC")
    assert unresolved_kind(found) == "ambiguous"
    assert "26464-8" in unresolved_reason(found) and "unit" in unresolved_reason(found)


def test_a_unit_that_fits_no_code_is_its_own_kind():
    found = resolve_reading("hemoglobin", "14", "%")
    assert unresolved_kind(found) == "unit_conflict"
    assert found.rejected_code == "718-7"


def test_every_kind_has_a_sentence():
    # A refusal without a `rejected_reason` falls back to its kind's sentence;
    # a kind missing from the table would raise for that name.
    assert set(_REASON_SENTENCES) == {"category", "condition", "ambiguous", "uncoded", "timing", "unit_conflict", "unknown"}


def test_resolve_indicator_carries_the_kind_and_reason_only_when_unresolved():
    rows = {r["name"]: r for r in resolve_indicators(["血脂", "XYZ指数", "发烧", "血红蛋白"])["results"]}
    assert (rows["血脂"]["reason_kind"], rows["血脂"]["reason"]) == ("category", CATEGORY)
    assert (rows["XYZ指数"]["reason_kind"], rows["XYZ指数"]["reason"]) == ("unknown", MISSING)
    assert rows["发烧"]["reason_kind"] == "condition"
    assert "reason" not in rows["血红蛋白"] and "reason_kind" not in rows["血红蛋白"]


def test_standardize_reading_carries_the_kind_in_its_decision():
    found = decision(standardize_reading("血脂", "5.0", "mmol/L"))
    assert (found["reasonKind"], found["reason"]) == ("category", CATEGORY)
    assert decision(standardize_reading("XYZ指数", "3"))["reasonKind"] == "unknown"
    coded = decision(standardize_reading("LDL-C", "96", "mg/dL"))
    assert "reason" not in coded and "reasonKind" not in coded


def test_a_complaint_keeps_its_own_reason_and_gets_no_second_one():
    found = decision(standardize_reading("发烧"))
    assert "standardize_complaint" in found["rejectedReason"]
    assert found["reasonKind"] == "condition"
    assert "reason" not in found
