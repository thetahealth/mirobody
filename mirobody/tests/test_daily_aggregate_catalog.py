"""#333: an Apple export's own daily figures were listed as
`dailyMinOxygenSaturations.apple_health_export`, with no label in either
language and sleep stuck in `ms`. None of the aggregator's generated
`daily{Method}{Indicator}` names is in `res/catalog/metrics.tsv`, so the read
side (`standardize.label_for`) had nothing to look up.

`kernel.metrics.daily_aggregate_metric` reads a generated name back to the
catalogue row and aggregation method that produced it -- the same rule
`standardize.aggregate.rule_generator` writes with (`build_indicator_name`),
so the two cannot drift apart. This pins that EVERY name the aggregator can
write resolves to a reader's label, in both shipped languages, and that the
write-side identity change (`collect.observations.catalog_alias`, a `total`
row only) is scoped the way #333's own analysis requires: three statistics
of one metric (avg/min/max) must not collapse into the source's single LOINC
series, which `th_day_authority` would then publish only one of.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest

from mirobody import standardize
from mirobody.collect.observations import catalog_alias
from mirobody.kernel import metrics


def test_every_generated_rule_derives_from_a_series_metrics_own_methods():
    """`rule_generator` only ever reads a SERIES row's `aggregation_methods`
    (a `mix` row's, such as blood pressure's `avg,max`, is explicitly
    skipped); the read side's map must cover exactly the same ground, or a
    name the aggregator writes could still have nothing to read it back."""
    sources = metrics._daily_aggregate_sources()
    assert len(sources) > 500  # 661 at #333; a floor against an empty table
    for name, (source, method) in sources.items():
        assert source.data_type == metrics.DATA_SERIES, name
        assert method in source.aggregation_methods, name
        assert name == metrics.build_indicator_name("day", method, source.name)


def test_every_generated_daily_name_resolves_to_a_label_in_both_languages():
    missing = {
        lang: [name for name in metrics._daily_aggregate_sources()
               if not standardize.label_for(metrics.SYSTEM_DEVICE, name, lang)]
        for lang in ("zh", "en")
    }
    assert missing == {"zh": [], "en": []}


def test_a_source_suffix_the_aggregator_appends_does_not_change_the_label():
    """apple_health (the phone) and apple_health_export (an export) write the
    same generated name with a different `.source` suffix; #333's own words,
    'they already share a series id, only the shown name differs' -- true
    now of the label too."""
    base = standardize.label_for(metrics.SYSTEM_DEVICE, "dailyMinOxygenSaturations", "zh")
    assert base
    for suffix in ("apple_health", "apple_health_export", "apple_health_watch"):
        assert standardize.label_for(
            metrics.SYSTEM_DEVICE, f"dailyMinOxygenSaturations.{suffix}", "zh"
        ) == base


def test_an_unrecognized_name_still_has_no_label():
    assert metrics.daily_aggregate_metric("not-a-real-metric") is None
    assert standardize.label_for(metrics.SYSTEM_DEVICE, "not-a-real-metric", "zh") == ""


def test_a_hand_curated_summary_row_is_not_mistaken_for_a_generated_one():
    """`dailyTotalSleepTime` is already its own catalogue entry (#296); it is
    not what `rule_generator` would have named a summed `sleepAnalysis_*`
    series (`dailyTotalSleepAnalysis_Asleep(Total)`), so the two must stay
    distinct names the read side resolves differently."""
    assert metrics.METRICS.get("dailyTotalSleepTime") is not None
    assert metrics.daily_aggregate_metric("dailyTotalSleepTime") is None


def test_a_single_method_metrics_daily_total_takes_the_sources_confident_code():
    """`steps` has one aggregation method (`total`): its generated daily row
    is the same quantity on a different time axis (`standardize/__init__.py`'s
    own words), so it may safely take `steps`' own LOINC code -- nothing else
    is ever filed under `dailyTotalSteps`'s identity to collide with."""
    alias = catalog_alias("dailyTotalSteps.apple_health_export")
    assert alias is not None
    assert (alias.code_system, alias.code) == (standardize.LOINC_SYSTEM, "55423-8")


def test_a_multi_method_metrics_statistics_keep_their_own_separate_identity():
    """`oxygenSaturations` has three methods (`avg`, `min`, `max`): giving all
    three the source's single LOINC code would give them one series_id, and
    `th_day_authority` publishes only one observation per (series, day) --
    today's minimum would silently vanish behind the average. Each keeps its
    own per-name device identity instead, same as before #333, with only the
    label fixed."""
    names = ["dailyAvgOxygenSaturations", "dailyMinOxygenSaturations", "dailyMaxOxygenSaturations"]
    aliases = [catalog_alias(f"{n}.apple_health_export") for n in names]
    assert aliases == [None, None, None]  # the caller's own device-namespace fallback applies
    assert len({n.split(".", 1)[0] for n in names}) == 3


def test_a_metrics_non_total_statistic_never_borrows_a_different_concepts_code():
    """`sleepAnalysis_InBed` aggregates `total` (time in bed) AND
    `sleep_onset_latency` (time to fall asleep) from the SAME series: two
    different quantities, not two statistics of one. Only the `total` row --
    time in bed, on a different time axis -- may take the source's code; the
    latency row must not, or a day's time-in-bed and time-to-fall-asleep
    would collide in one series."""
    total = catalog_alias("dailyTotalSleepAnalysis_InBed.apple_health_export")
    latency = catalog_alias("dailySleepOnsetLatencySleepAnalysis_InBed.apple_health_export")
    assert total is not None and (total.code_system, total.code) == (standardize.LOINC_SYSTEM, "103213-5")
    assert latency is None


def test_an_unconfident_sources_total_stays_in_the_device_namespace():
    """`sleepAnalysis_Asleep(Core)` (LIGHT_SLEEP) carries 93830-8 `unverified`:
    an unconfident code is evidence, never an identity (`standardize.devices`),
    so its daily total must not borrow it either."""
    assert catalog_alias("dailyTotalSleepAnalysis_Asleep(Core).apple_health_export") is None


# --- the database regression --------------------------------------------------
#
# The issue's own reproduction: a synthetic export.xml over blood oxygen,
# steps and a night of sleep, stored through `cli._store_export` -- the same
# decoder, `StandardHealthService.process_standard_data` and
# `AggregateIndicatorService.recalculate_date_range` calls an import or a
# push both take -- then read back the way the web client's Indicators page
# does, `PostgresHealthQuery.catalog` (`server/routers/indicator_router.py`
# -> `HealthIndicatorsService.envelope`).

DSN = os.environ.get("MIROBODY_TEST_POSTGRES_DSN", "")

_RECORDS = (
    # Blood oxygen: HealthKit's own fraction form, min/avg/max all computed
    # by the aggregator (`oxygenSaturations`: avg, min, max -- no `total`).
    ('<Record type="HKQuantityTypeIdentifierOxygenSaturation" sourceName="Apple Watch" unit="%" '
     'startDate="2026-01-01 08:00:00 +0000" endDate="2026-01-01 08:00:00 +0000" value="0.95"/>'),
    ('<Record type="HKQuantityTypeIdentifierOxygenSaturation" sourceName="Apple Watch" unit="%" '
     'startDate="2026-01-01 09:00:00 +0000" endDate="2026-01-01 09:00:00 +0000" value="0.98"/>'),
    # Steps: one aggregation method (`total`), and a confident LOINC code.
    ('<Record type="HKQuantityTypeIdentifierStepCount" sourceName="iPhone" unit="count" '
     'startDate="2026-01-01 10:00:00 +0000" endDate="2026-01-01 10:10:00 +0000" value="3200"/>'),
    # A night of sleep, 18:00-windowed onto 2026-01-01: in bed 23:00-07:00,
    # asleep (core, then deep, then REM) from 23:30. Every stage is well
    # inside the 8-hour in-bed span, so the duration bound never rejects it.
    ('<Record type="HKCategoryTypeIdentifierSleepAnalysis" sourceName="Apple Watch" '
     'value="HKCategoryValueSleepAnalysisInBed" '
     'startDate="2026-01-01 23:00:00 +0000" endDate="2026-01-02 07:00:00 +0000"/>'),
    ('<Record type="HKCategoryTypeIdentifierSleepAnalysis" sourceName="Apple Watch" '
     'value="HKCategoryValueSleepAnalysisAsleepCore" '
     'startDate="2026-01-01 23:30:00 +0000" endDate="2026-01-02 05:00:00 +0000"/>'),
    ('<Record type="HKCategoryTypeIdentifierSleepAnalysis" sourceName="Apple Watch" '
     'value="HKCategoryValueSleepAnalysisAsleepDeep" '
     'startDate="2026-01-02 05:00:00 +0000" endDate="2026-01-02 06:00:00 +0000"/>'),
    ('<Record type="HKCategoryTypeIdentifierSleepAnalysis" sourceName="Apple Watch" '
     'value="HKCategoryValueSleepAnalysisAsleepREM" '
     'startDate="2026-01-02 06:00:00 +0000" endDate="2026-01-02 07:00:00 +0000"/>'),
)


@pytest.mark.needs_db
@pytest.mark.skipif(not DSN, reason="MIROBODY_TEST_POSTGRES_DSN is not set")
async def test_apple_export_daily_rows_get_labels_codes_and_the_right_unit(database, monkeypatch, tmp_path):  # noqa: F811
    from mirobody import cli
    from mirobody.collect.query import PostgresHealthQuery
    from mirobody.utils.config import Config

    export = tmp_path / "synthetic-export.xml"
    export.write_text("<HealthData>" + "".join(_RECORDS) + "</HealthData>")
    monkeypatch.setattr(Config, "init", AsyncMock())
    args = _import_args(export)
    stored = await cli._store_export(args)
    assert stored["added"] > 0

    rows = await PostgresHealthQuery().catalog("1", None)
    assert rows, "the export produced no catalogue rows at all"

    # 1. No catalogue row's indicator or label still carries the storage key
    # plus a `.apple_health*` suffix -- the bug's own symptom.
    for r in rows:
        assert ".apple_health" not in r["indicator"], r
        assert ".apple_health" not in r.get("label", ""), r

    # 2. Every row has a Chinese label (LOINC's own translation covers every
    # code here, even the three with no English ConsumerName below) and a
    # clean English name, as a `label` or -- for a LOINC code with no
    # ConsumerName, 93831-6/93829-0/103213-5 among them, the long name
    # already reads well and is what `indicator` falls back to -- as that.
    for r in rows:
        assert standardize.label_for(r["system"], r["code"], "zh"), r
        assert r.get("label") or r["indicator"], r
        assert ".apple_health" not in (r.get("label") or r["indicator"])

    # 3. No sleep DURATION total reads in `ms` any more (#296's mechanism,
    # reached now that a confident source's `total` carries its own LOINC
    # code): in bed, core, deep and REM sleep all read in `h` or `min`.
    by_code = {r["code"]: r for r in rows}
    for code, unit in (("103213-5", "h"), ("93831-6", "min"), ("93829-0", "min")):
        assert code in by_code, f"{code} missing: {sorted(by_code)}"
        assert by_code[code]["unit"] != "ms", by_code[code]
        assert by_code[code]["unit"] == unit, by_code[code]

    # 4. steps: single aggregation method, confident source -> the source's
    # own LOINC code, not the device-namespace generated name.
    assert "55423-8" in by_code
    assert by_code["55423-8"]["system"] == standardize.LOINC_SYSTEM

    # 5. oxygen: three methods (avg/min/max) sharing one source -- each KEEPS
    # its own per-name device identity (no LOINC switch), each with its own
    # label, so a reader still sees all three, not one election winner.
    device_oxygen = [r for r in rows if r["system"] == metrics.SYSTEM_DEVICE and "Oxygen" in r["code"]]
    assert len(device_oxygen) == 3, device_oxygen
    assert len({r["code"] for r in device_oxygen}) == 3  # avg, min and max: three distinct series
    for r in device_oxygen:
        assert ".apple_health" not in r["indicator"]
        assert standardize.label_for(r["system"], r["code"], "zh").endswith("血氧饱和度")


# Reuse the existing isolated PostgreSQL schema seam and export-import helper.
from .test_postgres_regressions import database, _import_args  # noqa: F401
