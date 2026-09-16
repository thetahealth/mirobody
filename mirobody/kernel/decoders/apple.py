"""Apple HealthKit records → facts. Pure; the type names are Apple's own.

Two front doors produce the item this module decodes: the push endpoint
(`collect/providers/apple`) and the `export.zip` a person makes in the Health
app. Both are keyed by the HealthKit identifier
(`HKQuantityTypeIdentifierStepCount`), because that is what `export.xml`
carries natively and what HealthKit itself names a type.

The unit is per record, not per type: `unit` is CDATA in Apple's DTD and
follows the device's region, so one file can hold `mg/dL` and `mmol/L` for
the same type. Every value is converted by reading its own unit, never by
assuming the type's.
"""

from __future__ import annotations

from datetime import datetime

from mirobody import units
from mirobody.kernel import metrics
from mirobody.kernel.series import Fact
from ._common import fact, number

#: Apple writes `"2014-09-13 10:27:54 +0100"`: a space, and a numeric offset
#: rather than an IANA zone. The offset is what the device's clock said, so
#: it is taken literally and the account timezone is not consulted.
_TS = "%Y-%m-%d %H:%M:%S %z"

#: HealthKit identifier → catalogue metric. The same targets the mobile path
#: already maps to, so both front doors agree on the name.
QUANTITY: dict[str, str] = {
    "HKQuantityTypeIdentifierHeartRate": "heartRates",
    "HKQuantityTypeIdentifierRestingHeartRate": "restingHeartRates",
    "HKQuantityTypeIdentifierWalkingHeartRateAverage": "walkingHeartRates",
    # SDNN by name, so it is the SDNN row and not the generic HRV series:
    # five other vendors publish RMSSD, a different statistic, and a shared
    # row would have averaged the two.
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "hrvSDNN",
    "HKQuantityTypeIdentifierPeripheralPerfusionIndex": "perfusionIndex",
    "HKQuantityTypeIdentifierRespiratoryRate": "respiratoryRates",
    "HKQuantityTypeIdentifierOxygenSaturation": "oxygenSaturations",
    "HKQuantityTypeIdentifierBodyTemperature": "bodyTemperatures",
    "HKQuantityTypeIdentifierBasalBodyTemperature": "reproductiveBasalBodyTemperature",
    "HKQuantityTypeIdentifierAppleSleepingWristTemperature": "wristTemperatures",
    "HKQuantityTypeIdentifierBloodGlucose": "bloodGlucoses",
    "HKQuantityTypeIdentifierBloodPressureSystolic": "systolicPressures",
    "HKQuantityTypeIdentifierBloodPressureDiastolic": "diastolicPressures",
    "HKQuantityTypeIdentifierStepCount": "steps",
    "HKQuantityTypeIdentifierFlightsClimbed": "floors",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "walkingRunningDistances",
    "HKQuantityTypeIdentifierDistanceCycling": "cyclingDistances",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "activeCalories",
    "HKQuantityTypeIdentifierBasalEnergyBurned": "basalCalories",
    "HKQuantityTypeIdentifierAppleExerciseTime": "exerciseMinutes",
    "HKQuantityTypeIdentifierVO2Max": "vo2Maxs",
    "HKQuantityTypeIdentifierBodyMass": "bodyMasss",
    "HKQuantityTypeIdentifierBodyMassIndex": "bmis",
    "HKQuantityTypeIdentifierBodyFatPercentage": "bodyFatPercentages",
    "HKQuantityTypeIdentifierHeight": "heights",
    "HKQuantityTypeIdentifierLeanBodyMass": "bodyFatFreeWeight",
    "HKQuantityTypeIdentifierWaistCircumference": "waistCircumferences",
    "HKQuantityTypeIdentifierWalkingSpeed": "walkingSpeeds",
    "HKQuantityTypeIdentifierCyclingSpeed": "cyclingSpeeds",
    "HKQuantityTypeIdentifierWalkingDoubleSupportPercentage": "walkingDoubleSupportPercentage",
    "HKQuantityTypeIdentifierWalkingAsymmetryPercentage": "walkingAsymmetryPercentage",
    "HKQuantityTypeIdentifierStairAscentSpeed": "stairAscentSpeed",
    "HKQuantityTypeIdentifierStairDescentSpeed": "stairDescentSpeed",
    "HKQuantityTypeIdentifierSixMinuteWalkTestDistance": "sixMinuteWalkDistance",
    "HKQuantityTypeIdentifierHeartRateRecoveryOneMinute": "recoveryes",
    "HKQuantityTypeIdentifierUVExposure": "uvExposures",
    "HKQuantityTypeIdentifierDietaryEnergyConsumed": "energyes",
    "HKQuantityTypeIdentifierDietaryProtein": "proteins",
    "HKQuantityTypeIdentifierDietaryCarbohydrates": "carbohydrates",
    "HKQuantityTypeIdentifierDietaryFatTotal": "fats",
    "HKQuantityTypeIdentifierDietaryWater": "waters",
}

#: Category types whose reading is a name, not a number. `export.xml` carries
#: the `HKCategoryValue*` string in the same `value` attribute a quantity uses,
#: so the fact is text and the catalogue unit for these is `enum`.
CATEGORY: dict[str, str] = {
    "HKCategoryTypeIdentifierMenstrualFlow": "reproductiveMenstruationFlow",
    "HKCategoryTypeIdentifierCervicalMucusQuality": "reproductiveCervicalMucusQuality",
    "HKCategoryTypeIdentifierOvulationTestResult": "reproductiveOvulationTestResult",
    "HKCategoryTypeIdentifierPregnancyTestResult": "reproductivePregnancyTestResult",
    "HKCategoryTypeIdentifierProgesteroneTestResult": "reproductiveProgEstrogenTestResult",
    "HKCategoryTypeIdentifierSexualActivity": "reproductiveSexualActivity",
    "HKCategoryTypeIdentifierIntermenstrualBleeding": "reproductiveIntermenstrualBleeding",
    "HKCategoryTypeIdentifierLactation": "reproductiveLactation",
    "HKCategoryTypeIdentifierPregnancy": "reproductivePregnancy",
    "HKCategoryTypeIdentifierContraceptive": "reproductiveContraceptive",
}

#: Not here on purpose: `bodyWater`, `bodyBone`, `bodyMuscle`, `bodySubFat`,
#: `bodyVisFat`, `bodyProtein`, `bodySinew`, `bodyAge`. A body-composition
#: scale writes those through HealthKit, but HealthKit itself declares no such
#: identifiers, so nothing in an Apple export can produce them. They belong to
#: whichever scale integration reads that vendor's own API.

#: `HKCategoryValueSleepAnalysis*` → catalogue metric. A sleep record carries
#: no number: its value is the stage and its measurement is the span, so the
#: duration is `endDate - startDate` in milliseconds.
SLEEP_STAGES: dict[str, str] = {
    "HKCategoryValueSleepAnalysisInBed": "sleepAnalysis_InBed",
    "HKCategoryValueSleepAnalysisAsleepUnspecified": "sleepAnalysis_Asleep(Unspecified)",
    "HKCategoryValueSleepAnalysisAsleep": "sleepAnalysis_Asleep(Unspecified)",
    "HKCategoryValueSleepAnalysisAwake": "sleepAnalysis_Awake",
    "HKCategoryValueSleepAnalysisAsleepDeep": "sleepAnalysis_Asleep(Deep)",
    "HKCategoryValueSleepAnalysisAsleepCore": "sleepAnalysis_Asleep(Core)",
    "HKCategoryValueSleepAnalysisAsleepREM": "sleepAnalysis_Asleep(REM)",
}

SLEEP_TYPE = "HKCategoryTypeIdentifierSleepAnalysis"
BLOOD_PRESSURE = "HKCorrelationTypeIdentifierBloodPressure"

DATA_TYPES: tuple[str, ...] = (*QUANTITY, *CATEGORY, SLEEP_TYPE, BLOOD_PRESSURE)

#: Every catalogue metric this table can emit. Derived, so it cannot drift
#: from what `decode` produces; `connect.Coverage` is built from it.
METRICS: frozenset[str] = (
    frozenset(QUANTITY.values()) | frozenset(CATEGORY.values()) | frozenset(SLEEP_STAGES.values())
)

#: Conversions `mirobody.units` declines, measured 2026-09-15. Fahrenheit is
#: affine and the library is factor-based (`convertible("[degF]", "Cel")` is
#: False); `mi` folds to the US survey mile, which has no metre factor there,
#: while Apple means the international mile.
_MI_TO_M = 1609.344
#: `mmol/L` → `mg/dL` needs the molar mass, which the library reaches through
#: a LOINC code. 2339-0 is glucose in blood.
_GLUCOSE_LOINC = "2339-0"


def _to_catalogue(metric: str, value: float, unit: str) -> float | None:
    """`value` in the metric's catalogue unit, or ``None`` when this record's
    unit cannot be converted. Both units are folded to UCUM first: the
    catalogue writes `°C` and `count/min` where Apple writes `degC` and
    `count/min`, and only the folded forms are comparable.

    ``None`` is skipped and counted, never stored. An unconverted number
    filed as if it were converted is the silent wrong number this table
    exists to prevent.
    """
    raw = (unit or "").strip()
    target = metrics.METRICS[metric].standard_unit
    if not raw or raw == target:
        return value
    want = units.normalize_unit(target) or target
    if raw in ("mi", "[mi_i]", "[mi_us]") and want == "m":
        return value * _MI_TO_M
    if raw in ("degF", "[degF]") and want == "Cel":
        return (value - 32.0) * 5.0 / 9.0
    got = units.normalize_unit(raw)
    if not got:
        return None
    if got == want:
        return value
    loinc = _GLUCOSE_LOINC if metric == "bloodGlucoses" else ""
    return units.convert_value(value, got, want, loinc_code=loinc)


def record_time_ms(value: str | int | float | None) -> int:
    """An Apple timestamp → unix ms, or ``0`` when it cannot be read. Never
    "now": a fabricated time files a reading under the wrong day.

    `export.xml` writes `"2014-09-13 10:27:54 +0100"`. A client that already
    holds epoch milliseconds sends the number instead, and both reach the same
    decode table.
    """
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, int | float):
        return int(value) if value > 0 else 0
    try:
        return int(datetime.strptime(value.strip(), _TS).timestamp() * 1000)
    except (ValueError, AttributeError):
        return 0


def decode(
    data_type: str, item: dict, tz: str, *, pulled_at_ms: int = 0, source_record_id: str = "", ingested_at_ms: int = 0
) -> list[Fact]:
    """Facts from one HealthKit record. Unknown types and records without a
    parsable time decode to nothing, never to a guessed time. ``tz`` and
    ``pulled_at_ms`` are accepted for signature parity: an Apple record
    carries its own offset, so neither is consulted.
    """
    if not isinstance(item, dict):
        return []
    start = record_time_ms(item.get("startDate"))
    if not start:
        return []
    end = record_time_ms(item.get("endDate")) or start
    common = {"source_record_id": source_record_id, "ingested_at_ms": ingested_at_ms}

    if data_type == BLOOD_PRESSURE:
        panel = f"blood_pressure:{start}"
        out: list[Fact] = []
        for key, metric in (("systolic", "systolicPressures"), ("diastolic", "diastolicPressures")):
            v = number(item.get(key))
            if v is None:
                continue
            converted = _to_catalogue(metric, v, str(item.get("unit") or ""))
            if converted is None:
                continue
            out.append(fact(metric, float(converted), start, end, panel_id=panel, **common))
        return out

    if data_type == SLEEP_TYPE:
        metric = SLEEP_STAGES.get(str(item.get("value") or ""))
        if not metric or end <= start:
            return []
        return [fact(metric, float(end - start), start, end, **common)]

    metric = CATEGORY.get(data_type)
    if metric:
        #: The reading is the stage or result name Apple wrote, kept verbatim:
        #: renaming it here would put this module in the business of deciding
        #: what "eggWhite" means, which is the answer layer's job.
        text = str(item.get("value") or "").strip()
        return [fact(metric, None, start, end, text=text, **common)] if text else []

    metric = QUANTITY.get(data_type)
    if not metric:
        return []
    v = number(item.get("value"))
    if v is None:
        return []
    converted = _to_catalogue(metric, v, str(item.get("unit") or ""))
    if converted is None:
        return []
    #: Apple writes a blood pressure as two separate records. They are one
    #: measurement when they share a start and a source, which is what a cuff
    #: produces, so the panel is derived rather than carried.
    panel = ""
    if metric in ("systolicPressures", "diastolicPressures"):
        panel = f"blood_pressure:{start}:{item.get('sourceName') or ''}"
    return [fact(metric, float(converted), start, end, panel_id=panel, **common)]
