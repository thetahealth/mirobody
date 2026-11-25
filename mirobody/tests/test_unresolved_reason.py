"""Every surface says why a name got no code, in the same words.

`mirobody resolve 血脂` said "a category or several tests in one name"; over
MCP, `resolve_indicator` answered `resolved: false` and `standardize_reading`
`method: refused`, with no reason on either. A client could not tell "this
names a panel, ask for one test" from "this spelling is missing", which is
what a reader of the resolver demo asked for.
"""

from mirobody.engine.observation import decision, standardize_reading
from mirobody.engine.resolver import resolve, unresolved_reason
from mirobody.standardize.terminology import resolve_indicators

CATEGORY = "a category or several tests in one name, and no code is given rather than a guessed one"
MISSING = "not in the lexical index, and no code is given rather than a guessed one"


def test_the_reason_names_the_kind_of_non_answer():
    assert unresolved_reason(resolve("血脂")) == CATEGORY
    assert unresolved_reason(resolve("XYZ指数")) == MISSING
    assert unresolved_reason(resolve("血红蛋白")) == ""


def test_a_tie_keeps_the_resolvers_own_reason():
    reason = unresolved_reason(resolve("TLC"))
    assert "ties" in reason and "26464-8" in reason


def test_resolve_indicator_carries_the_reason_only_when_unresolved():
    rows = {r["name"]: r for r in resolve_indicators(["血脂", "XYZ指数", "血红蛋白"])["results"]}
    assert rows["血脂"]["reason"] == CATEGORY
    assert rows["XYZ指数"]["reason"] == MISSING
    assert "reason" not in rows["血红蛋白"]


def test_standardize_reading_carries_the_reason_in_its_decision():
    assert decision(standardize_reading("血脂", "5.0", "mmol/L"))["reason"] == CATEGORY
    assert decision(standardize_reading("XYZ指数", "3"))["reason"] == MISSING
    assert "reason" not in decision(standardize_reading("LDL-C", "96", "mg/dL"))


def test_a_complaint_keeps_its_own_reason_and_gets_no_second_one():
    found = decision(standardize_reading("发烧"))
    assert "standardize_complaint" in found["rejectedReason"]
    assert "reason" not in found
