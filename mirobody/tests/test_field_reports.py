"""Deterministic regressions for behaviours field testers confirmed or broke.

A round of outside testing on a public pre-release turned up a mix of
confirmed good behaviours and small defects. Each test below is one
behaviour, synthetic and reproducible without any of the original material:
a tester's name, platform or deployment detail is never load-bearing here and
none is recorded. Every number is invented.

- **G004** — a tester called `convert_unit` (the stdio MCP tool,
  `mirobody/mcp/`) across mass and molar concentration without the reading's
  LOINC code and it refused, with a reason, rather than guess; supplying the
  code let the conversion through. `mirobody.standardize.terminology.convert_unit`
  is the one body behind every surface that offers the tool (see its module
  docstring), so testing it here covers the MCP tool too.
- **G005** — three testers asked a demo profile seeded with exactly one
  reading "what changed", and each time it said there was not enough data
  for a trend instead of inventing a direction. Pinned both in the pure
  trend computation (`kernel.query.trend_rows`) and in what the agent tool
  renders from it.
- **F014** — two testers had to split a time- or frequency-qualified
  complaint ("最近老是头疼") into its bare symptom word by hand before
  `standardize_complaint` would code it. Stripping a closed, enumerated list
  of leading qualifiers that say WHEN or HOW OFTEN, never WHETHER, is safe:
  it cannot change which complaint is named, so every modifier is tested
  against the code its bare term already carries, and a negation (which is
  never in the list) is tested to confirm it still abstains exactly as
  before.

F020 (whether re-uploading an identical report duplicates its readings) is a
database case and lives in `test_postgres_regressions.py`, next to the other
tests that write through `collect/observations.py`.
"""

from __future__ import annotations

import pytest

from mirobody.kernel.query import trend_rows
from mirobody.standardize.icpc3 import resolve_condition, resolve_symptom
from mirobody.standardize.terminology import convert_unit, standardize_complaint

# --- G004: convert_unit refuses mass<->molar without a LOINC code, and gives why ---


def test_convert_unit_refuses_mass_to_molar_without_a_loinc_code_and_says_why():
    """A tester called `convert_unit(212, "mg/dL", "mmol/L")` with no LOINC
    code. It must refuse (a null `converted`, never a guessed number) and the
    refusal must say a code is what is missing, not just that the units
    differ — the regression #271 fixed: a model that read "not
    interconvertible" told its user two cholesterol readings could not be
    compared at all."""
    refusal = convert_unit(212, "mg/dL", "mmol/L")
    assert refusal["success"] is True
    assert refusal["converted"] is None
    assert "loinc" in refusal["reason"].lower() and "code" in refusal["reason"].lower()


def test_convert_unit_succeeds_once_the_loinc_code_is_given():
    """The same conversion, now with the reading's LOINC code (2093-3, total
    cholesterol): it must return the correct mg/dL -> mmol/L number, not
    another refusal."""
    result = convert_unit(212, "mg/dL", "mmol/L", "2093-3")
    assert result["success"] is True
    assert result["converted"] == pytest.approx(5.482994956679168, rel=1e-9)


def test_convert_unit_refuses_the_reverse_direction_too():
    """The refusal is symmetric: mmol/L -> mg/dL with no code refuses the
    same way, and with the code converts back to (about) the original value."""
    refusal = convert_unit(5.48, "mmol/L", "mg/dL")
    assert refusal["converted"] is None
    result = convert_unit(5.48, "mmol/L", "mg/dL", "2093-3")
    assert result["converted"] == pytest.approx(211.884, rel=1e-3)


# --- G005: a single reading states no direction and no fabricated change ---


def test_single_reading_trend_row_states_insufficient_not_a_fabricated_direction():
    """Three testers asked a profile seeded with exactly one cholesterol
    reading for its trend and the demo said there was not enough data,
    rather than inventing "up" or "down". `trend_rows` on one point must
    answer `direction="insufficient"` and leave every computed figure out
    (`None`, never zero or a guess)."""
    rows = [{"indicator": "Total Cholesterol", "period": "2026-01-01", "avg": 5.2, "unit": "mmol/L"}]
    row = trend_rows(rows)[0]
    assert row["direction"] == "insufficient"
    assert row["shape"] == "unavailable"
    assert row["reason"] == "insufficient_points"
    for key in ("first_third_mean", "last_third_mean", "delta", "pct_change_from_start", "slope_per_day", "min", "max"):
        assert row[key] is None


async def test_single_reading_trend_is_rendered_as_insufficient_not_fabricated():
    """The same one-point series, through the agent tool's own rendering
    (`health_indicators_service.py` + `_render.py`): the sentence it builds
    must say the data is insufficient and must not print a fabricated
    percentage (`None%`) or claim a direction the row does not have."""
    pytest.importorskip("mirobody.agent.tools.health_indicators_service")
    from unittest.mock import Mock

    from mirobody.agent.tools._render import render_compact
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
    from mirobody.collect.query import _bucket_row

    row = _bucket_row({"display": "Total Cholesterol", "series_id": "cholesterol-series",
                       "code_system": "LOINC", "code": "2093-3", "unit": "mmol/L",
                       "period": "2026-01-01", "avg": 5.2, "n": 1, "total": 1})
    hq = Mock()
    hq.tz.return_value = "UTC"
    hq.on_read.return_value = None
    hq.buckets.return_value = [row]
    result = await HealthIndicatorsService(hq, outside_note=False).envelope(
        {"user_id": "1"}, indicators=["Total Cholesterol"], view="trend")
    assert result.data[0]["direction"] == "insufficient"
    text = render_compact(result)
    assert "None%" not in text
    summary = " ".join(result.assumptions)
    assert "insufficient" in summary
    assert "direction=up" not in summary and "direction=down" not in summary


# --- F014: a leading time/frequency modifier never changes a complaint's code ---


@pytest.mark.parametrize("modifier", ["最近", "老是", "经常", "总是", "一直", "有点", "有些"])
def test_a_leading_chinese_modifier_never_changes_the_code(modifier):
    """A tester had to extract the bare symptom word from a journal sentence
    by hand; `有点发烧` (and every other enumerated modifier in front of
    `发烧`) must now code to exactly the same thing `发烧` alone does, with
    the modifier recorded as evidence rather than silently dropped."""
    baseline = resolve_symptom("发烧")
    modified = resolve_symptom(modifier + "发烧")
    assert baseline.outcome == "coded" and baseline.code == "AS03"
    assert modified.outcome == "coded" and modified.code == baseline.code
    assert f"modifiers={modifier}" in modified.evidence


@pytest.mark.parametrize("modifier", ["recently", "always", "often", "a bit"])
def test_a_leading_english_modifier_never_changes_the_code(modifier):
    """The English equivalents behave the same way: `recently headache`,
    `always headache`, `often headache` and `a bit headache` all code to the
    same complaint as bare `headache`."""
    baseline = resolve_symptom("headache")
    modified = resolve_symptom(f"{modifier} headache")
    assert baseline.outcome == "coded" and baseline.code == "NS01"
    assert modified.outcome == "coded" and modified.code == baseline.code


def test_stacked_modifiers_still_resolve_to_the_bare_complaints_code():
    """The reported case itself: a tester's `最近老是头疼` ("recently, always
    headache") stacks two modifiers in front of `头疼`. Both must be
    stripped and the result must still code to `头疼`'s own NS01, with both
    words named, in order, in the decision's evidence."""
    baseline = resolve_symptom("头疼")
    stacked = resolve_symptom("最近老是头疼")
    assert baseline.outcome == "coded" and baseline.code == "NS01"
    assert stacked.outcome == "coded" and stacked.code == baseline.code
    assert stacked.evidence[-1] == "modifiers=最近,老是"


@pytest.mark.parametrize("text", ["不头疼", "没有发烧", "没发烧", "not fever", "no fever"])
def test_negation_is_never_stripped_and_does_not_code_to_the_symptom(text):
    """A negation says the complaint is ABSENT, not just qualified, and must
    never be coded to the symptom it denies: `不头疼`/`没有发烧`/`没发烧` and
    their English equivalents must keep abstaining exactly as they did before
    modifier-stripping existed (none of 不/没/没有/not/no is in the
    enumerated modifier list)."""
    coding = resolve_symptom(text)
    assert coding.outcome != "coded"
    assert coding.reason == "icpc3:no-match"


def test_a_modifier_also_resolves_on_the_diagnosis_axis():
    """The same stripping applies to `kind="condition"`: `最近高血压`
    ("recently, hypertension") must still code to the same diagnosis as bare
    `高血压`, on the diagnosis axis only — it must not also resolve on the
    symptom axis, which has no code for a diagnosis name."""
    baseline = resolve_condition("高血压")
    modified = resolve_condition("最近高血压")
    assert baseline.outcome == "coded"
    assert modified.outcome == "coded" and modified.code == baseline.code
    assert resolve_symptom("最近高血压").outcome != "coded"


def test_standardize_complaint_surfaces_the_stripped_modifiers_as_evidence():
    """Through the public tool body (what the MCP tool and the chat tool
    both call): the modifiers stripped before coding must reach the
    returned decision, not be lost between `icpc3.resolve` and the
    Observation it builds."""
    from mirobody.engine.observation import decision

    obs = standardize_complaint("最近老是头疼")
    assert obs["code"]["coding"][0]["code"] == "NS01"
    info = decision(obs)
    assert info["outcome"] == "coded"
    assert any(e.startswith("modifiers=") for e in info["evidence"])
