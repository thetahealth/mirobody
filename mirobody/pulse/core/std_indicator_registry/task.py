"""
Standard Indicator Registry Task

Publishes the in-code StandardIndicator enum and the derived aggregation
rules (from rule_generator) into standard_indicators_device, so
downstream consumers (UI, FHIR mapping, monitoring) have a queryable
catalog instead of needing to import Python enums.

Idempotent UPSERT by `id` (camelCase indicator name). If the target table
does not exist the task no-ops cleanly — environments that haven't run
migration 94 will simply skip registration on every tick.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from ..aggregate_indicator.rule_generator import get_all_aggregation_rules
from ..indicators_info import HealthDataType, StandardIndicator
from ..scheduler import PullTask, ScheduleType
from ....utils import execute_query


TABLE_NAME = "standard_indicators_device"

# Methods whose output unit is not the source indicator's unit. Used to
# pick a canonical_unit for aggregation rows. Anything not listed inherits
# the source indicator's standard_unit.
_AGG_UNIT_OVERRIDES = {
    "count": "count",
    "hypo_event_count": "count",
    "hypo_event_times": "count",
    "tir_70_180": "%",
    "tir_70_140": "%",
    "pct_above_180": "%",
    "pct_above_140": "%",
    "pct_below_70": "%",
    "time_of_max": "time",
    "time_of_min": "time",
}


def _slugify_category(name: str) -> str:
    """'Vital Signs' -> 'vital_signs'. Matches the existing `system`
    column convention in standard_indicators (lowercase, underscored)."""
    return name.strip().lower().replace(" ", "_")


def _collect_source_rows() -> List[Dict]:
    """Build one row per StandardIndicator enum value.

    Skips pure-SERIES indicators (data_type == HealthDataType.SERIES): the
    raw points of those land in series_data, not th_series_data, so they
    don't belong in this th_series_data-oriented catalog. Their daily
    aggregations (kind='aggregation') are still registered separately by
    _collect_aggregation_rows() because the aggregated rows do land in
    th_series_data.

    SUMMARY and MIX indicators are kept — both land in th_series_data
    (MIX writes to both tables).
    """
    rows: List[Dict] = []
    for indicator in StandardIndicator:
        info = indicator.value
        if not info.name:
            # Defensive: every IndicatorInfo should have `name` set, but
            # skip anything malformed rather than crash the task.
            logging.warning(
                f"[StdIndicatorRegistry] Skipping enum {indicator.name}: empty info.name"
            )
            continue
        if info.data_type == HealthDataType.SERIES:
            continue
        rows.append({
            "id": info.name,
            "name_en": info.name,
            "name_zh": info.name_zh or info.name,
            "kind": "scalar",
            "system": _slugify_category(info.category.name),
            "canonical_unit": info.standard_unit or None,
            "description": info.description or None,
            "description_zh": info.description_zh or None,
        })
    return rows


def _collect_aggregation_rows() -> List[Dict]:
    """Build one row per derived aggregation rule (e.g. dailyAvgHeartRates)."""
    # Index source indicators by camelCase name for lookup.
    source_by_name = {
        ind.value.name: ind.value
        for ind in StandardIndicator
        if ind.value.name
    }

    rows: List[Dict] = []
    for rule in get_all_aggregation_rules():
        source_info = source_by_name.get(rule.source_indicator)
        if source_info is None:
            # rule_generator only produces rules for known indicators, but
            # custom rules registered elsewhere could reference unknown
            # sources. Skip them so we don't write garbage `system`.
            logging.warning(
                f"[StdIndicatorRegistry] Skipping rule with unknown source: "
                f"{rule.source_indicator} -> {rule.target_indicator}"
            )
            continue

        unit = _AGG_UNIT_OVERRIDES.get(rule.aggregation_type, source_info.standard_unit)

        # Aggregation rows borrow the source indicator's description verbatim
        # — the source description describes the underlying physiological
        # measure, which is still what each daily-aggregation computes. The
        # aggregation method (avg/max/min/...) is already encoded in the id.
        rows.append({
            "id": rule.target_indicator,
            "name_en": rule.target_indicator,
            "name_zh": rule.target_indicator,
            "kind": "aggregation",
            "system": _slugify_category(source_info.category.name),
            "canonical_unit": unit or None,
            "description": source_info.description or None,
            "description_zh": source_info.description_zh or None,
        })
    return rows


async def _table_exists() -> bool:
    """Return True iff standard_indicators_device exists in the current DB."""
    result = await execute_query(
        f"SELECT to_regclass('{TABLE_NAME}') AS reg",
        log_sql=False,
    )
    if not result:
        return False
    return result[0].get("reg") is not None


# Upsert: writes the auto-managed columns and leaves curated fields
# (specimen_type, reference_low/high, unit_alternatives) untouched so an
# operator can hand-enrich a row later without it getting clobbered on
# the next tick.
_UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME}
    (id, name_en, name_zh, kind, system, canonical_unit,
     description, description_zh, is_active, updated_at)
VALUES
    (:id, :name_en, :name_zh, :kind, :system, :canonical_unit,
     :description, :description_zh, true, now())
ON CONFLICT (id) DO UPDATE SET
    name_en        = EXCLUDED.name_en,
    name_zh        = EXCLUDED.name_zh,
    kind           = EXCLUDED.kind,
    system         = EXCLUDED.system,
    canonical_unit = EXCLUDED.canonical_unit,
    description    = EXCLUDED.description,
    description_zh = EXCLUDED.description_zh,
    is_active      = EXCLUDED.is_active,
    updated_at     = now()
"""


class RegisterStandardIndicatorsTask(PullTask):
    """
    Publish StandardIndicator enum + derived aggregation rules into
    standard_indicators_device. Runs once a day — catalog data
    only changes when code is deployed.
    """

    def __init__(self):
        super().__init__(
            provider_slug="register_standard_indicators",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=60,  # scheduler checks hourly
            execution_interval_hours=2400.0,  # ~100 days — effectively manual-trigger only
        )

    async def execute(self) -> bool:
        try:
            if not await _table_exists():
                logging.info(
                    f"[StdIndicatorRegistry] Table {TABLE_NAME} not present; "
                    f"skipping registration (run migration 94 to enable)."
                )
                return True  # no-op, not an error

            rows = _collect_source_rows() + _collect_aggregation_rows()
            if not rows:
                logging.info("[StdIndicatorRegistry] No rows to register")
                return True

            # Dedupe by id — defensive: if a source and an aggregation
            # ever collide on the same camelCase name, the later (agg)
            # would silently overwrite the former. Drop dupes upfront so
            # the loser is logged rather than mysteriously absent.
            seen, deduped = set(), []
            for row in rows:
                if row["id"] in seen:
                    logging.warning(
                        f"[StdIndicatorRegistry] Duplicate id, keeping first: {row['id']}"
                    )
                    continue
                seen.add(row["id"])
                deduped.append(row)

            await execute_query(_UPSERT_SQL, field_list=deduped, log_sql=False)

            stats = {
                "executed_at": datetime.now().isoformat(),
                "total_rows": len(deduped),
                "source_rows": len(_collect_source_rows()),
                "aggregation_rows": len(_collect_aggregation_rows()),
            }
            await self.save_task_stats(stats)

            logging.info(
                f"[StdIndicatorRegistry] Upserted {len(deduped)} rows into "
                f"{TABLE_NAME} (sources={stats['source_rows']}, "
                f"aggregations={stats['aggregation_rows']})"
            )
            return True

        except Exception as e:
            logging.error(f"[StdIndicatorRegistry] Execution error: {e}")
            return False

    async def get_task_info(self) -> Dict:
        full_status = await self.get_full_status()
        full_status.update({
            "task_name": "Standard Indicator Registry",
            "description": (
                "Publish in-code StandardIndicator enum and derived "
                "aggregation rules to standard_indicators_device"
            ),
            "execution_frequency": "Manual (2400h / ~100 day interval)",
        })
        return full_status
