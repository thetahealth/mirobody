"""A trend is a computation, not an impression.

`stats` reports `first`/`last`, which are single readings: on a sleep series that went
6.18 -> 7.2 a 9 % decline between its thirds read as a rise, and every model asked to
classify that series answered "no trend". These tests pin the arithmetic and the vocabulary
that `view="trend"` adds, on shapes whose answer is known.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from mirobody.kernel.query import trend_rows


def series(values: list[float], indicator: str = "x") -> list[dict]:
    return [{"indicator": indicator, "period": (date(2025, 1, 1) + timedelta(days=i)).isoformat(),
             "avg": value, "unit": "u"}
            for i, value in enumerate(values)]


def test_a_rise_is_up_with_a_positive_slope() -> None:
    row = trend_rows(series([1 + 0.3 * i for i in range(30)]))[0]
    assert row["direction"] == "up"
    assert row["delta_pct"] > 100
    assert row["slope_per_day"] > 0


def test_a_fall_is_down() -> None:
    row = trend_rows(series([10 - 0.3 * i for i in range(30)]))[0]
    assert row["direction"] == "down"
    assert row["delta_pct"] < -100


def test_a_series_that_only_jitters_is_flat() -> None:
    row = trend_rows(series([7.0 + (0.05 if i % 2 else -0.05) for i in range(30)]))[0]
    assert row["direction"] == "flat"
    assert abs(row["delta_pct"]) < 2


def test_a_single_hump_is_named_not_called_flat() -> None:
    row = trend_rows(series([7.0 + 1.5 * math.sin(math.pi * i / 29) for i in range(30)]))[0]
    assert row["shape"] == "hump"


def test_a_single_dip_is_named() -> None:
    row = trend_rows(series([7.0 - 2.0 * math.sin(math.pi * i / 29) for i in range(30)]))[0]
    assert row["shape"] == "dip"


def test_a_step_down_reads_as_down() -> None:
    row = trend_rows(series([8.0] * 15 + [6.0] * 15))[0]
    assert row["direction"] == "down"
    assert row["shape"] == "down"


def test_first_and_last_alone_would_have_missed_the_decline() -> None:
    """The series that started this: falling thirds, rising ends."""
    values = [6.183, 7.883, 6.183, 8.05, 7.967, 7.717, 7.433, 8.133, 8.833, 6.733,
              4.65, 8.2, 7.35, 6.983, 8.717, 6.833, 7.983, 8.367, 8.65, 6.9,
              7.817, 7.467, 6.083, 4.817, 8.5, 8.217, 5.667, 7.2]
    row = trend_rows(series(values))[0]
    assert values[-1] > values[0]           # ends say "up"
    assert row["direction"] == "down"       # thirds say "down", and that is the trend
    assert row["delta_pct"] < 0


def test_too_few_points_says_so() -> None:
    row = trend_rows(series([1.0, 2.0]))[0]
    assert row["direction"] == "insufficient"


STATISTICS = {"first_third_mean", "last_third_mean", "delta", "delta_pct", "slope_per_day", "min", "max"}


def test_missing_dates_do_not_turn_sample_slope_into_daily_slope():
    rows = series([1.0, 2.0, 3.0])
    for row, day in zip(rows, ("2025-01-01", "2025-01-11", "2025-01-21"), strict=True):
        row["period"] = day
    assert trend_rows(rows)[0]["slope_per_day"] == pytest.approx(0.1)


def test_changing_weight_units_does_not_manufacture_a_hump():
    rows = series([70.0, 154.3236, 70.0], "Body weight")
    for row, unit in zip(rows, ("kg", "lb", "kg"), strict=True):
        row.update(unit=unit, series="weight")
    result = trend_rows(rows[::-1])[0]
    assert result["unit"] == "kg"
    assert result["max"] == pytest.approx(70, abs=0.001)
    assert result["direction"] == result["shape"] == "flat"


def test_known_loinc_molar_conversion_uses_the_same_numeric_basis():
    rows = series([5.0, 90.08, 5.0], "Glucose")
    for row, unit in zip(rows, ("mmol/L", "mg/dL", "mmol/L"), strict=True):
        row.update(unit=unit, series="glucose", system="LOINC", code="2345-7")
    result = trend_rows(rows)[0]
    assert result["unit"] == "mmol/L"
    assert result["min"] == result["max"] == 5.0
    assert result["direction"] == "flat"


@pytest.mark.parametrize("units, code", [(("kg", "s", "kg"), ""),
                                         (("Cel", "[degF]", "Cel"), ""),
                                         (("g/L", "mmol/L", "g/L"), "unknown")])
def test_unreliable_cross_unit_comparisons_abstain(units, code):
    rows = series([1.0, 2.0, 3.0])
    for row, unit in zip(rows, units, strict=True):
        row.update(unit=unit, series="one", system="LOINC", code=code)
    result = trend_rows(rows)[0]
    assert result["direction"] == "incomparable"
    assert result["shape"] == "unavailable"
    assert result["reason"] == "incompatible_units"
    assert all(result[key] is None for key in STATISTICS)


def test_zero_centered_temperature_deltas_keep_their_direction():
    rows = series([-1.0, 0.0, 1.0], "Temperature Delta")
    for row in rows:
        row["unit"] = "Cel"
    result = trend_rows(rows)[0]
    assert result["direction"] == "up"
    assert result["delta"] == 2
    assert result["delta_pct"] == 300


def test_all_zero_values_have_no_relative_percentage():
    result = trend_rows(series([0.0] * 6))[0]
    assert result["direction"] == result["shape"] == "flat"
    assert result["delta_pct"] is None
    assert result["slope_per_day"] == 0


def test_repeated_significant_peaks_are_swings_not_a_single_hump():
    assert trend_rows(series([7, 8, 10, 6, 10, 6, 10, 8, 7]))[0]["shape"] == "swing"


def test_stable_series_identity_survives_changed_display_names():
    rows = series([1, 2, 3], "old label")
    for row in rows:
        row.update(series="one", system="LOINC", code="old")
    rows[-1].update(indicator="new label", code="new")
    result = trend_rows(rows[::-1])
    assert len(result) == 1
    assert {key: result[0][key] for key in ("indicator", "series", "system", "code")} == {
        "indicator": "new label", "series": "one", "system": "LOINC", "code": "new"}
    assert result[0]["n"] == 3


def test_equal_labels_do_not_merge_distinct_series_or_fallback_identities():
    first, second = series([1, 2, 3]), series([10, 20, 30])
    for row in first:
        row.update(series="one", system="LOINC", code="a")
    for row in second:
        row.update(series="two", system="LOINC", code="b")
    assert len(trend_rows(first + second)) == 2
    for row in first + second:
        row.pop("series")
    assert len(trend_rows(first + second)) == 2


def test_coverage_and_evidence_flags_include_days_without_numeric_values():
    rows = series([1, None, 2, 3], "x")
    for row in rows:
        row.update(series="one", total=401, day_known=True, unverified=False, provenance="measured")
    rows[1].update(unverified=True, day_known=False)
    result = trend_rows(rows)[0]
    assert result["n"] == 3
    assert result["source_days"] == 4 and result["total_days"] == 401
    assert result["input_truncated"] is True and result["missing_numeric_days"] == 1
    assert result["unverified"] is True and result["day_known"] is False
    assert result["first_date"] == "2025-01-01" and result["last_date"] == "2025-01-04"


@pytest.mark.parametrize("reported", [False, True])
def test_nonnumeric_or_reported_series_are_not_an_empty_catalogue_fallback(reported):
    rows = series([None, None, None], "reported headache" if reported else "qualitative result")
    for row in rows:
        row.update(provenance="reported" if reported else "measured", kind="symptom" if reported else "measurement")
    result = trend_rows(rows)
    assert len(result) == 1 and result[0]["direction"] == "unsupported"
    assert result[0]["shape"] == "unavailable" and result[0]["reason"]
    assert all(result[0][key] is None for key in STATISTICS)
    if reported:
        assert result[0]["provenance"] == "reported" and result[0]["kind"] == "symptom"


def test_insufficient_points_keep_the_complete_shape_and_coverage_schema():
    result = trend_rows(series([1.0, 2.0]))[0]
    assert result["direction"] == "insufficient" and result["n"] == 2
    assert result["source_days"] == result["total_days"] == 2
    assert result["input_truncated"] is False and result["missing_numeric_days"] == 0
    assert all(key in result and result[key] is None for key in STATISTICS)


def test_nonfinite_values_count_as_missing_not_as_numeric_observations():
    result = trend_rows(series([1.0, float("nan"), float("inf"), 2.0, 3.0]))[0]
    assert result["n"] == 3 and result["missing_numeric_days"] == 2
    assert result["min"] == 1 and result["max"] == 3


def test_invalid_dates_abstain_instead_of_inventing_daily_spacing():
    rows = series([1, 2, 3])
    rows[1]["period"] = "not a date"
    result = trend_rows(rows)[0]
    assert result["direction"] == "unsupported" and result["reason"] == "invalid_dates"
    assert result["slope_per_day"] is None

def test_a_small_scale_series_keeps_its_means_and_its_movement() -> None:
    """The case a fixed decimal place destroys: 0.0001 -> 0.0003 g/L.

    Rounded to three decimals every statistic became 0 while `direction` still read "up", so
    the row claimed a movement it could not express. Significant figures keep both.
    """
    from mirobody.kernel.query import _significant
    values = [0.0001, 0.0002, 0.0003]
    first, last = _significant(values[0]), _significant(values[-1])
    assert first == 0.0001 and last == 0.0003
    assert _significant(last - first) == 0.0002
    assert _significant(0.0) == 0.0

def test_small_scale_extrema_are_not_flattened() -> None:
    """min/max kept three decimals while the means used significant figures.

    `[0.0001, 0.0002, 0.0003] g/L` returned `min=0.0, max=0.0` beside a nonzero mean and an
    upward direction - the row contradicted itself.
    """
    from mirobody.kernel.query import _significant
    assert _significant(min([0.0001, 0.0002, 0.0003])) == 0.0001
    assert _significant(max([0.0001, 0.0002, 0.0003])) == 0.0003

def test_a_series_that_opens_on_its_midpoint_still_reads_as_swing() -> None:
    """The first sample used to fix the extreme, so the opening reversal was invisible.

    `[100, 101.5, 98.5, 100, 101.5, 98.5, 100]` read as flat while the same series without
    its first value read as swing.
    """
    from mirobody.kernel.query import _trend_shape
    series = [100, 101.5, 98.5, 100, 101.5, 98.5, 100]
    assert _trend_shape(series, 100, "flat") == "swing"
    assert _trend_shape(series, 100, "flat") == _trend_shape(series[1:], 100, "flat")

def test_equivalent_units_reach_the_same_direction_and_shape() -> None:
    """The same readings in mmol/L and umol/L must not straddle a threshold.

    `[9.9, 10, 10.1]` landed on `flat` in one unit and `up` in the other; the same happened to
    the shape of `[9.9, 10.1, 10.1, 9.9]`. The comparisons now ignore floating point noise.
    """
    from mirobody.kernel.query import trend_rows

    def verdict(values: list[float], unit: str) -> tuple[str, str]:
        rows = [{"indicator": "g", "period": f"2026-01-{i + 1:02d}", "avg": value, "unit": unit}
                for i, value in enumerate(values)]
        row = trend_rows(rows)[0]
        return row["direction"], row["shape"]

    assert verdict([9.9, 10, 10.1], "mmol/L") == verdict([9900, 10000, 10100], "umol/L")
    assert verdict([9.9, 10.1, 10.1, 9.9], "mmol/L") == verdict([9900, 10100, 10100, 9900], "umol/L")


@pytest.mark.parametrize("values", [
    [1e-8, 1.01e-8, 1e-8], [1e-8, 1.02e-8, 1e-8], [1e-8, 2e-8, 1e-8],
])
def test_low_scale_unit_conversion_preserves_shape(values):
    from mirobody.units import conversion_factor

    factor = conversion_factor("g/L", "ng/L")
    assert factor is not None
    def verdict(numbers, unit):
        rows = series(numbers, "Synthetic concentration")
        for row in rows:
            row["unit"] = unit
        result = trend_rows(rows)[0]
        return result["direction"], result["shape"]
    assert verdict(values, "g/L") == verdict([v * factor for v in values], "ng/L")
