"""
Indicator Category Aliases

Maps abstract indicator categories (used in recipes) to actual indicator names
in th_series_data (which differ between demo users and real users).

Usage:
    recipe declares required_categories = ["heartRate", "sleep"]
    framework calls resolve_indicator("heartRate", user_actual_indicators)
    returns the first matching actual indicator name, or None
"""

from typing import Dict, List, Optional, Set

# Category -> list of possible actual indicator names, ordered by preference.
# First match wins.
# Priority order:
#   1. SQLAggregator output (dailyAvg*, dailyTotal*, dailyLast* — active, supports .source suffix)
#   2. Demo/synthetic names (RestingHeartRate-RHR, etc.)
#   3. Raw indicator names (heartRates, steps — may need aggregation)
#   4. legacy `daily_stats_*` (DEPRECATED, no longer produced; historical rows only)
#
# Suffix matching: resolve_indicator() supports prefix match, so
# "dailyAvgHeartRates" will match "dailyAvgHeartRates.apple_health" in user data.

INDICATOR_ALIASES: Dict[str, List[str]] = {
    # =========================================================================
    # Heart Rate
    # =========================================================================
    "heartRate": [
        "dailyAvgRestingHeartRates",       # SQLAggregator
        "dailyAvgHeartRates",              # SQLAggregator
        "RestingHeartRate-RHR",            # demo/synthetic
        "DailyAverageHeartRate",           # demo/synthetic
        "restingHeartRates",               # raw
        "heartRates",                      # raw
        "daily_stats_restingHeartRatesAvg", # legacy (deprecated)
        "daily_stats_heartRatesAvg",       # legacy (deprecated)
    ],
    "heartRateExercise": [
        "dailyAvgWalkingHeartRates",       # SQLAggregator
        "AverageHeartRateDuringExercise",   # demo
        "MaxHeartRateDuringExercise",       # demo
        "walkingHeartRates",               # raw
        "daily_stats_walkingHeartRatesAvg", # legacy (deprecated)
    ],

    # =========================================================================
    # Sleep
    # =========================================================================
    "sleepDeep": [
        "dailyTotalSleepAnalysis_Asleep(Deep)",  # SQLAggregator (prefix matches .apple_health etc.)
        "DeepSleepPercentage",                    # demo
        "DeepSleepDuration",                      # demo
        "sleepAnalysis_Asleep(Deep)",              # raw
        "daily_stats_sleepAnalysis_Asleep(Deep)Sum", # legacy (deprecated)
    ],
    "sleepTotal": [
        "dailyTotalSleepAnalysis_Asleep(Total)",  # SQLAggregator
        "dailyTotalSleepAnalysis_InBed",           # SQLAggregator
        "DailySleepDuration",                      # demo
        "sleepAnalysis_Asleep(Total)",              # raw
        "sleepAnalysis_InBed",                     # raw
        "daily_stats_sleepAnalysis_Asleep(Total)Sum", # legacy (deprecated)
    ],
    "sleepLight": [
        "dailyTotalSleepAnalysis_Asleep(Core)",   # SQLAggregator
        "LightSleepPercentage",                    # demo
        "LightSleepDuration",                      # demo
        "sleepAnalysis_Asleep(Core)",               # raw
        "daily_stats_sleepAnalysis_Asleep(Core)Sum", # legacy (deprecated)
    ],
    "sleepRem": [
        "dailyTotalSleepAnalysis_Asleep(REM)",    # SQLAggregator
        "REMSleepPercentage",                      # demo
        "REMSleepDuration",                        # demo
        "sleepAnalysis_Asleep(REM)",                # raw
        "daily_stats_sleepAnalysis_Asleep(REM)Sum", # legacy (deprecated)
    ],
    "sleepAwake": [
        "dailyTotalSleepAnalysis_Awake",           # SQLAggregator
        "sleepAnalysis_Awake",                     # raw
    ],
    "sleepEfficiency": [
        "dailyAvgSleepEfficiency",                 # SQLAggregator
        "SleepEfficiency",                         # demo
        "derivedSleepEfficiency",                  # derived
    ],

    # =========================================================================
    # Steps & Activity
    # =========================================================================
    "steps": [
        "dailyTotalSteps",                         # SQLAggregator
        "DailyStepCount",                          # demo
        "steps",                                   # raw
        "daily_stats_stepsSum",                    # legacy (deprecated)
    ],
    "exerciseDuration": [
        "dailyTotalExerciseMinutes",               # SQLAggregator
        "DailyExerciseDuration",                   # demo
        "exerciseMinutes",                         # raw
        "daily_stats_exerciseMinutesSum",           # legacy (deprecated)
    ],
    "activeCalories": [
        "ActiveCalories",                          # demo
        "ActivityEnergyExpenditure-AEE",            # demo
        "activeCalories",                          # raw
    ],
    "distance": [
        "dailyTotalWalkingRunningDistances",        # SQLAggregator
        "DailyDistance",                            # demo
        "daily_stats_walkingRunningDistancesSum",   # legacy (deprecated)
    ],

    # =========================================================================
    # HRV
    # =========================================================================
    "hrv": [
        "dailyAvgHrvDatas",                        # SQLAggregator
        "HeartRateVariability-HRV",                 # demo
        "HRV-RMSSD",                               # demo
        "hrvRMSSD",                                # raw
        "hrvDatas",                                # raw
        "daily_stats_hrvDatasAvg",                 # legacy (deprecated)
    ],

    # =========================================================================
    # Blood Glucose
    # =========================================================================
    "bloodGlucose": [
        "dailyAvgBloodGlucoses",                   # SQLAggregator
        "FastingBloodGlucose-FBG",                  # demo
        "bloodGlucoses",                           # raw
        "daily_stats_bloodGlucosesAvg",            # legacy (deprecated)
    ],
    "hba1c": [
        "EstimatedHbA1c-eA1C",                     # demo
    ],
    "glucoseTAR": [
        "dailyTir70180BloodGlucoses",              # SQLAggregator (TIR 70-180)
        "GlucoseAboveRange-TAR",                    # demo
    ],
    "glucoseTBR": [
        "dailyPctBelow70BloodGlucoses",            # SQLAggregator
        "GlucoseBelowRange-TBR",                    # demo
    ],

    # =========================================================================
    # Blood Pressure
    # =========================================================================
    "bpSystolic": [
        "dailyAvgSystolicPressures",               # SQLAggregator
        "dailyMaxSystolicPressures",               # SQLAggregator
        "24-HourAverageSystolicBP",                 # demo
        "DaytimeAverageSystolicBP",                 # demo
        "systolicPressures",                       # raw
        "bloodPressureSystolics",                  # raw
        "daily_stats_systolicPressuresAvg",        # legacy (deprecated)
        "daily_stats_bloodPressureSystolicsAvg",   # legacy (deprecated, old naming)
    ],
    "bpDiastolic": [
        "dailyAvgDiastolicPressures",              # SQLAggregator
        "dailyMaxDiastolicPressures",              # SQLAggregator
        "24-HourAverageDiastolicBP",                # demo
        "DaytimeAverageDiastolicBP",                # demo
        "diastolicPressures",                      # raw
        "bloodPressureDiastolics",                 # raw
        "daily_stats_diastolicPressuresAvg",       # legacy (deprecated)
        "daily_stats_bloodPressureDiastolicsAvg",  # legacy (deprecated, old naming)
    ],

    # =========================================================================
    # Stress & Recovery
    # =========================================================================
    "stress": [
        "DailyStressScore",                        # demo
        "stressLevel",                             # raw
    ],
    "bodyBattery": [
        "BodyBattery",                             # demo
    ],

    # =========================================================================
    # SpO2 & Respiratory
    # =========================================================================
    "spo2": [
        "dailyAvgOxygenSaturations",               # SQLAggregator
        "dailyMinOxygenSaturations",               # SQLAggregator
        "DailyMinSpO2",                             # demo
        "oxygenSaturations",                       # raw
        "daily_stats_oxygenSaturationsAvg",        # legacy (deprecated)
    ],
    "respiratoryRate": [
        "dailyAvgRespiratoryRates",                # SQLAggregator
        "AverageRespiratoryRate",                    # demo
        "respiratoryRates",                        # raw
        "daily_stats_respiratoryRatesAvg",         # legacy (deprecated)
    ],

    # =========================================================================
    # Body Composition
    # =========================================================================
    "weight": [
        "dailyLastBodyMasss",                      # SQLAggregator
        "BodyWeight",                              # demo
        "bodyMasss",                               # raw
        "weight",                                  # raw
        "daily_stats_bodyMasssLast",               # legacy (deprecated)
    ],
    "bmi": [
        "dailyLastBmis",                           # SQLAggregator
        "BodyMassIndex-BMI",                        # demo
    ],
    "bodyFat": [
        "BodyFatPercentage-BFP",                    # demo
        "bodyFatPercentages",                      # raw
    ],
    "visceralFat": [
        "VisceralFatLevel-VFL",                     # demo
        "visceralFat",                             # raw
    ],

    # =========================================================================
    # Cardiorespiratory Fitness
    # =========================================================================
    "crf": [
        "vo2Maxs",                                 # raw
        "CardiorespiratoryFitness-CRF",             # demo
    ],

    # =========================================================================
    # Temperature
    # =========================================================================
    "temperature": [
        "BasalBodyTemperature-BBT",                 # demo
        "bodyTemperatures",                        # raw
        "wristTemperatures",                       # raw
        "temperatureDelta",                        # raw
    ],
}

# Reverse index: actual indicator name -> category
_REVERSE_INDEX: Dict[str, str] = {}
for _cat, _names in INDICATOR_ALIASES.items():
    for _name in _names:
        _REVERSE_INDEX[_name] = _cat


def resolve_indicator(category: str, user_indicators: Set[str]) -> Optional[str]:
    """Find the actual indicator name for a category from the user's available indicators.

    Supports both exact match and prefix match for source-suffixed indicators.
    e.g. alias "dailyTotalSteps" matches "dailyTotalSteps.apple_health" in user data.
    Exact match is preferred over prefix match.

    Args:
        category: Abstract category key, e.g. "heartRate"
        user_indicators: Set of actual indicator names the user has in th_series_data

    Returns:
        First matching indicator name (exact or suffixed), or None if no match
    """
    aliases = INDICATOR_ALIASES.get(category, [])
    for alias in aliases:
        # Exact match first
        if alias in user_indicators:
            return alias
        # Prefix match: alias.{source} (e.g. dailyTotalSteps.apple_health)
        for ui in user_indicators:
            if ui.startswith(alias + "."):
                return ui
    return None


def resolve_all(user_indicators: Set[str]) -> Dict[str, str]:
    """Resolve all categories for a user at once.

    Args:
        user_indicators: Set of actual indicator names

    Returns:
        Dict of category -> matched indicator name (only categories with matches)
    """
    result = {}
    for category in INDICATOR_ALIASES:
        matched = resolve_indicator(category, user_indicators)
        if matched:
            result[category] = matched
    return result


def get_category(indicator_name: str) -> Optional[str]:
    """Reverse lookup: given an actual indicator name, find its category.

    Args:
        indicator_name: Actual indicator name from DB

    Returns:
        Category key, or None if not mapped
    """
    return _REVERSE_INDEX.get(indicator_name)


def get_all_categories() -> List[str]:
    """Return all defined category keys."""
    return list(INDICATOR_ALIASES.keys())
