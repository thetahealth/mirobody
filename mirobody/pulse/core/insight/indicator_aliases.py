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
INDICATOR_ALIASES: Dict[str, List[str]] = {
    # =========================================================================
    # Heart Rate
    # =========================================================================
    "heartRate": [
        # demo users
        "RestingHeartRate-RHR",
        "DailyAverageHeartRate",
        # real users (holywell stage2)
        "daily_stats_restingHeartRatesAvg",
        "daily_stats_heartRatesAvg",
        # real users (SQLAggregator)
        "dailyAvgRestingHeartRates",
        "dailyAvgHeartRates",
        # raw
        "restingHeartRates",
        "heartRates",
    ],
    "heartRateExercise": [
        "AverageHeartRateDuringExercise",
        "MaxHeartRateDuringExercise",
        "daily_stats_walkingHeartRatesAvg",
        "dailyAvgWalkingHeartRates",
        "walkingHeartRates",
    ],

    # =========================================================================
    # Sleep
    # =========================================================================
    "sleepDeep": [
        "DeepSleepPercentage",
        "DeepSleepDuration",
        "daily_stats_sleepAnalysis_Asleep(Deep)Sum",
        "dailyTotalSleepAnalysis_Asleep(Deep)",
        "sleepAnalysis_Asleep(Deep)",
    ],
    "sleepTotal": [
        "DailySleepDuration",
        "daily_stats_sleepAnalysis_Asleep(Total)Sum",
        "dailyTotalSleepAnalysis_Asleep(Total)",
        "sleepAnalysis_Asleep(Total)",
        "sleepAnalysis_InBed",
    ],
    "sleepLight": [
        "LightSleepPercentage",
        "LightSleepDuration",
        "daily_stats_sleepAnalysis_Asleep(Core)Sum",
        "sleepAnalysis_Asleep(Core)",
    ],
    "sleepRem": [
        "REMSleepPercentage",
        "REMSleepDuration",
        "daily_stats_sleepAnalysis_Asleep(REM)Sum",
        "sleepAnalysis_Asleep(REM)",
    ],
    "sleepEfficiency": [
        "SleepEfficiency",
        "derivedSleepEfficiency",
    ],

    # =========================================================================
    # Steps & Activity
    # =========================================================================
    "steps": [
        "DailyStepCount",
        "daily_stats_stepsSum",
        "dailyTotalSteps",
        "steps",
    ],
    "exerciseDuration": [
        "DailyExerciseDuration",
        "daily_stats_exerciseMinutesSum",
        "dailyTotalExerciseMinutes",
        "exerciseMinutes",
    ],
    "activeCalories": [
        "ActiveCalories",
        "ActivityEnergyExpenditure-AEE",
        "activeCalories",
    ],
    "distance": [
        "DailyDistance",
        "daily_stats_walkingRunningDistancesSum",
        "dailyTotalWalkingRunningDistances",
    ],

    # =========================================================================
    # HRV
    # =========================================================================
    "hrv": [
        "HeartRateVariability-HRV",
        "HRV-RMSSD",
        "daily_stats_hrvDatasAvg",
        "dailyAvgHrvDatas",
        "hrvRMSSD",
        "hrvDatas",
    ],

    # =========================================================================
    # Blood Glucose
    # =========================================================================
    "bloodGlucose": [
        "FastingBloodGlucose-FBG",
        "bloodGlucoses",
        "dailyAvgBloodGlucoses",
    ],
    "hba1c": [
        "EstimatedHbA1c-eA1C",
    ],
    "glucoseTAR": [
        "GlucoseAboveRange-TAR",
        "dailyTarBloodGlucoses",
    ],
    "glucoseTBR": [
        "GlucoseBelowRange-TBR",
        "dailyTbrBloodGlucoses",
    ],

    # =========================================================================
    # Blood Pressure
    # =========================================================================
    "bpSystolic": [
        "24-HourAverageSystolicBP",
        "DaytimeAverageSystolicBP",
        "daily_stats_bloodPressureSystolicsAvg",
        "bloodPressureSystolics",
        "systolicPressures",
    ],
    "bpDiastolic": [
        "24-HourAverageDiastolicBP",
        "DaytimeAverageDiastolicBP",
        "daily_stats_bloodPressureDiastolicsAvg",
        "bloodPressureDiastolics",
        "diastolicPressures",
    ],

    # =========================================================================
    # Stress & Recovery
    # =========================================================================
    "stress": [
        "DailyStressScore",
        "stressLevel",
    ],
    "bodyBattery": [
        "BodyBattery",
    ],

    # =========================================================================
    # SpO2 & Respiratory
    # =========================================================================
    "spo2": [
        "DailyMinSpO2",
        "daily_stats_oxygenSaturationsAvg",
        "dailyAvgOxygenSaturations",
        "oxygenSaturations",
    ],
    "respiratoryRate": [
        "AverageRespiratoryRate",
        "daily_stats_respiratoryRatesAvg",
        "dailyAvgRespiratoryRates",
        "respiratoryRates",
    ],

    # =========================================================================
    # Body Composition
    # =========================================================================
    "weight": [
        "BodyWeight",
        "bodyMasss",
        "weight",
    ],
    "bmi": [
        "BodyMassIndex-BMI",
        "dailyLastBmis",
    ],
    "bodyFat": [
        "BodyFatPercentage-BFP",
        "bodyFatPercentages",
    ],
    "visceralFat": [
        "VisceralFatLevel-VFL",
        "visceralFat",
    ],

    # =========================================================================
    # Cardiorespiratory Fitness
    # =========================================================================
    "crf": [
        "CardiorespiratoryFitness-CRF",
        "vo2Maxs",
    ],

    # =========================================================================
    # Temperature
    # =========================================================================
    "temperature": [
        "BasalBodyTemperature-BBT",
        "bodyTemperatures",
        "wristTemperatures",
        "temperatureDelta",
    ],
}

# Reverse index: actual indicator name -> category
_REVERSE_INDEX: Dict[str, str] = {}
for _cat, _names in INDICATOR_ALIASES.items():
    for _name in _names:
        _REVERSE_INDEX[_name] = _cat


def resolve_indicator(category: str, user_indicators: Set[str]) -> Optional[str]:
    """Find the actual indicator name for a category from the user's available indicators.

    Args:
        category: Abstract category key, e.g. "heartRate"
        user_indicators: Set of actual indicator names the user has in th_series_data

    Returns:
        First matching indicator name, or None if no match
    """
    aliases = INDICATOR_ALIASES.get(category, [])
    for alias in aliases:
        if alias in user_indicators:
            return alias
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
