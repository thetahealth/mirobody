"""Every series a device decoder writes reaches a day (#245).

A series becomes a daily reading only through the statistics its catalogue
row declares, and `standardize/aggregate/rule_generator.py` skips a SERIES
metric that declares none. Apple HRV (SDNN), walking asymmetry and nine more
were decoded and stored, then shown nowhere: not in the indicator list, the
dashboard or `query_health_indicators`. Eleven more from Garmin, Whoop, Oura
and Open Wearables had the same empty column.

Sleep stages (`duration_union`) are exempt: their nightly figures come from
the derive rules (`standardize/derive/rules.py`), not from a daily statistic.
"""

import pytest

from mirobody.kernel import decoders, metrics

# Stored on purpose and never shown as a reading. The reason is the crosswalk's
# own classification, so the two cannot drift apart silently.
NOT_A_READING = {
    # res/crosswalks/unmappable.tsv: NOT_MEASUREMENT, "the profile ceiling, not
    # the day's measured maximum". A daily row would sit beside the measured
    # maximum (heartRateMax, dailyHeartRateMax) and be read as one.
    "maxHeartRateProfile",
}


def _decoded_series() -> set[str]:
    return {
        name
        for vendor in decoders.DECODERS
        for name in decoders.metrics_of(vendor)
        if metrics.METRICS[name].data_type == "series"
    }


def _silent() -> list[tuple[str, str]]:
    return sorted(
        (vendor, name)
        for vendor in decoders.DECODERS
        for name in decoders.metrics_of(vendor)
        if metrics.METRICS[name].data_type == "series"
        and metrics.METRICS[name].aggregation_policy != metrics.POLICY_DURATION_UNION
        and not metrics.METRICS[name].aggregation_methods
        and name not in NOT_A_READING
    )


def test_every_series_a_decoder_writes_declares_a_daily_statistic():
    assert not _silent(), f"decoded and stored, then never shown: {_silent()}"


def test_what_is_not_a_reading_stays_off_the_day():
    """An exemption that a decoder no longer writes, or that gained a statistic,
    no longer exempts anything and would hide the next silent metric's name."""
    for name in NOT_A_READING:
        assert name in _decoded_series(), name
        assert not metrics.METRICS[name].aggregation_methods, name


def test_the_statistic_follows_the_policy():
    """HRV (SDNN) keeps all three its `mean_min_max` policy names; `last` is
    what 581 other series declare; `sum_delta` sums, as its 12 siblings do."""
    assert metrics.METRICS["hrvSDNN"].aggregation_methods == ("avg", "min", "max")
    for name in ("walkingAsymmetryPercentage", "wristTemperatures", "recoveryScore"):
        assert metrics.METRICS[name].aggregation_methods == ("last",), name
    assert metrics.METRICS["alcohol"].aggregation_methods == ("total",)


def test_hrv_sdnn_has_daily_rules():
    try:
        from mirobody.standardize.aggregate.rule_generator import get_rules_by_source_indicator
    except ImportError as missing:  # pragma: no cover - an install without [app]
        pytest.skip(f"needs the [app] extra: {missing}")
    methods = {rule.aggregation_type for rule in get_rules_by_source_indicator("hrvSDNN")}
    assert {"avg", "min", "max"} <= methods
