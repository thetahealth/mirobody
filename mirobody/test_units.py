"""Golden vectors for the units engine's shipped claims (see CHANGELOG 1.2.2).

Three things live here as executable evidence, so a contributor touching
``_clean``/``parse_value_unit`` or the conversion tables gets a red test
instead of a silent regression:

* **Path B0** — ``parse_value_unit`` honours the author's whitespace boundary.
  ``240 10⁹/L`` is a platelet count of 240 in ``10*9/L``; before the fix the
  whitespace collapse glued the count scale into the value and read 240109/L —
  off by three orders of magnitude.
* **pick_display_unit** — the unit a merged series displays in: most readings
  wins, ties go to the latest measurement.
* **Convergence guard** — ``pulse.standardize.units`` (device-side Collect)
  derives every constant it shares with this engine FROM this engine. The two
  tables had measurably drifted before they were converged: lb was 2.20462
  there vs the exact 2.2046226… here, glucose 18.0182 vs 18.016. This test
  walks the whole Collect table and pins the drift at zero wherever both
  sides know the pair.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mirobody.units import (
    conversion_factor,
    normalize_unit,
    parse_value_unit,
    pick_display_unit,
)
from mirobody.units.convert import MOLAR_MASS


# ── Path B0: the whitespace boundary is a split signal ──────────────────────

@pytest.mark.parametrize(
    "text, comparator, value, unit",
    [
        # The motivating bug: digit-leading count units after a space.
        ("240 10^9/L", "", 240.0, "10*9/L"),
        ("240 10⁹/L", "", 240.0, "10*9/L"),
        ("4.5 10*12/L", "", 4.5, "10*12/L"),
        # Ordinary spaced inputs keep parsing exactly as before.
        ("5.6 mmol/L", "", 5.6, "mmol/L"),
        ("<5 mg/dL", "<", 5.0, "mg/dL"),
        # Path A untouched: a whole-input unit never invents a value.
        ("10*9/L", "", None, "10*9/L"),
    ],
)
def test_parse_value_unit_golden(text, comparator, value, unit):
    got = parse_value_unit(text)
    assert (got.comparator, got.value, got.unit) == (comparator, value, unit)


# ── pick_display_unit: majority, then recency ────────────────────────────────

def test_pick_display_unit_majority_then_recency():
    t1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    # Most readings wins outright, even against a newer minority.
    assert pick_display_unit([("mmol/L", 5, t1), ("mg/dL", 2, t2)]) == "mmol/L"
    # Ties go to the latest measurement; a None timestamp sorts oldest.
    assert pick_display_unit([("mmol/L", 3, t1), ("mg/dL", 3, t2)]) == "mg/dL"
    assert pick_display_unit([("mmol/L", 3, None), ("mg/dL", 3, t1)]) == "mg/dL"
    # Nothing usable → empty string, not an exception.
    assert pick_display_unit([]) == ""
    assert pick_display_unit([("", 9, t2)]) == ""


# ── Convergence guard: the Collect tables carry no constants of their own ───

def test_collect_table_has_zero_drift_against_the_engine():
    from mirobody.pulse.standardize.units import UNIT_CONVERSIONS

    checked = 0
    drift: list[tuple[str, str, float, float]] = []
    for src, targets in UNIT_CONVERSIONS.items():
        ns = normalize_unit(src)
        if ns is None:
            continue
        for dst, factor in targets.items():
            nd = normalize_unit(dst)
            if nd is None:
                continue
            engine = conversion_factor(ns, nd)
            if engine is None:
                continue  # the engine declines (device alias, atomic unit)
            checked += 1
            if abs(engine - factor) > 1e-12 * max(abs(engine), abs(factor)):
                drift.append((src, dst, factor, engine))
    assert not drift, f"Collect table disagrees with the engine: {drift}"
    # Guard the guard: if normalize_unit stops resolving these spellings the
    # loop silently checks nothing and this test would pass vacuously.
    assert checked >= 90, f"only {checked} pairs were comparable"


def test_substance_factors_come_from_molar_mass():
    from mirobody.pulse.standardize import StandardIndicator
    from mirobody.pulse.standardize.units import INDICATOR_SPECIFIC_CONVERSIONS

    glucose = MOLAR_MASS["2345-7"][0]
    chol = MOLAR_MASS["2093-3"][0]
    tg = MOLAR_MASS["2571-8"][0]

    gl = INDICATOR_SPECIFIC_CONVERSIONS[StandardIndicator.BLOOD_GLUCOSE]["conversions"]
    assert gl["mmol/L"]["to_standard"](1.0) == pytest.approx(glucose / 10.0)

    ldl = INDICATOR_SPECIFIC_CONVERSIONS[StandardIndicator.CHOLESTEROL_LDL]["conversions"]
    assert ldl["mg/dL"]["to_standard"](1.0) == pytest.approx(10.0 / chol)
    assert ldl["g/L"]["from_standard"](1.0) == pytest.approx(chol / 1000.0)

    trig = INDICATOR_SPECIFIC_CONVERSIONS[StandardIndicator.CHOLESTEROL_TRIGLYCERIDES]["conversions"]
    assert trig["mg/dL"]["from_standard"](1.0) == pytest.approx(tg / 10.0)
    # The 2.29x lesson stays pinned: TG must NOT use the cholesterol factor.
    assert trig["mg/dL"]["to_standard"](150.0) == pytest.approx(1.694, abs=1e-3)
