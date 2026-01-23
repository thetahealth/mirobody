"""
Health Indicator Data Definitions - Refactored Version

Define health indicators based on new data structures, providing type safety and better extensibility.
This module is completely self-contained and does not depend on the original indicators.py file.
"""
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set, Any


# ============================================================================
# CORE ENUMS AND DATA STRUCTURES
# ============================================================================

class HealthDataType(Enum):
    """
    Enum representing the type of health data.
    
    Attributes:
        SUMMARY: Data that is a single, aggregated value over a period (e.g., daily step count, average heart rate).
        SERIES: Data consisting of a sequence of timestamped data points (e.g., a minute-by-minute heart rate log).
    """
    SUMMARY = "summary"
    SERIES = "series"
    MIX = "mix"


@dataclass
class CategoryInfo:
    """Category information"""
    name: str
    name_zh: str


@dataclass
class IndicatorInfo:
    """Indicator information"""
    category: CategoryInfo
    standard_unit: str
    data_type: HealthDataType = HealthDataType.SERIES
    name: str = ""  # lowerCamelCase
    name_zh: str = ""
    description: str = ""
    description_zh: str = ""
    aggregation_methods: Optional[List[str]] = None  # For aggregate_indicator module
    """
    Aggregation methods for converting series data to summary data.
    
    Supported methods:
        - 'avg': Average value
        - 'max': Maximum value
        - 'min': Minimum value
        - 'total': Total/Sum (preferred over 'sum')
        - 'count': Count of records
        - 'last': Latest value by time
        - 'first': Earliest value by time
        - 'stddev': Standard deviation
        - 'variance': Variance
        - 'median': Median value
        - 'p95': 95th percentile
    
    Examples:
        - ['avg', 'max', 'min']: For heart rate, blood pressure
        - ['total']: For steps, distance, calories
        - ['last']: For weight, body mass (take latest measurement)
        - None or []: No aggregation
    
    Generated target indicators follow pattern: daily{Method}{Indicator}
    E.g., heartRates + ['avg'] → dailyAvgHeartRates
    """


class Categories(Enum):
    """Health indicator categories with embedded CategoryInfo"""

    VITAL_SIGNS = CategoryInfo(
        name_zh="生命体征",
        name="Vital Signs"
    )

    BODY_COMPOSITION = CategoryInfo(
        name_zh="身体成分",
        name="Body Composition"
    )

    ACTIVITY = CategoryInfo(
        name_zh="活动指标",
        name="Activity Metrics"
    )

    METABOLIC = CategoryInfo(
        name_zh="代谢指标",
        name="Metabolic Metrics"
    )

    SLEEP = CategoryInfo(
        name_zh="睡眠指标",
        name="Sleep Metrics"
    )

    PERFORMANCE = CategoryInfo(
        name_zh="运动表现",
        name="Performance Metrics"
    )

    MEDICAL = CategoryInfo(
        name_zh="医疗指标",
        name="Medical Metrics"
    )

    DEVICE_SPECIFIC = CategoryInfo(
        name_zh="设备特定",
        name="Device Specific"
    )

    NUTRITION = CategoryInfo(
        name_zh="营养摄入",
        name="Nutrition Intake"
    )

    LIFESTYLE = CategoryInfo(
        name_zh="生活方式",
        name="Lifestyle"
    )

    HEALTH = CategoryInfo(
        name_zh="健康指标",
        name="Health Metrics"
    )

    MENTAL = CategoryInfo(
        name_zh="心理健康",
        name="Mental Health"
    )

    REPRODUCTIVE = CategoryInfo(
        name_zh="生殖健康",
        name="Reproductive Health"
    )


class StandardIndicator(Enum):
    """Standard health indicator enumeration with embedded IndicatorInfo"""

    ACTIVE_TIME = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="min",
        name="exerciseMinutes",
        name_zh="活动时间",
        description="Active exercise time",
        description_zh="活跃运动时间",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total exercise minutes
    )
    DAILY_ACTIVITY_INTENSITY_HIGH = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="min",
        name="dailyActivityIntensityHigh",
        name_zh="高强度活动时间",
        description="High intensity activity time",
        description_zh="高强度活动的时间",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_ACTIVITY_INTENSITY_MEDIUM = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="min",
        name="dailyActivityIntensityMedium",
        name_zh="中强度活动时间",
        description="Medium intensity activity time",
        description_zh="中强度活动的时间",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_ACTIVITY_INTENSITY_LOW = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="min",
        name="dailyActivityIntensityLow",
        name_zh="低强度活动时间",
        description="Low intensity activity time",
        description_zh="低强度活动的时间",
        data_type=HealthDataType.SUMMARY,
    )
    ALTITUDE_CHANGE = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="m",
        name="altitudeChange",
        name_zh="海拔变化",
        description="Net altitude change during exercise",
        description_zh="运动过程中的海拔净变化量",
        data_type=HealthDataType.SERIES,
    )
    ALTITUDE_GAIN = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="m",
        name="altitudeGain",
        name_zh="海拔增益",
        description="Total altitude gain during exercise",
        description_zh="运动过程中的海拔上升总量",
        data_type=HealthDataType.SERIES,
    )
    HOURLY_APNEA_INDEX = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count/hour",
        name="hourlyApneaIndex",
        name_zh="呼吸暂停指数",
        description="Number of apnea events per hour",
        description_zh="每小时呼吸暂停事件次数",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_APNEA_INDEX = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="dailyApneaIndex",
        name_zh="呼吸暂停指数",
        description="Number of apnea events per sleep",
        description_zh="每小时呼吸暂停事件次数",
        data_type=HealthDataType.SUMMARY,
    )
    AWAKE_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Awake",
        name_zh="清醒时间",
        description="Awake time during sleep period",
        description_zh="睡眠期间的清醒时间",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total awake time during sleep
    )
    BIA_RESISTANCE = IndicatorInfo(
        category=Categories.DEVICE_SPECIFIC.value,
        standard_unit="Ω",
        name="biaResistance",
        name_zh="生物电阻抗",
        description="Bioelectrical impedance analysis measurement value",
        description_zh="生物电阻抗测量值",
        data_type=HealthDataType.MIX,
    )
    BIRTH_DATE = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="date",
        name="birthDate",
        name_zh="出生日期",
        description="User birth date",
        description_zh="用户出生日期",
        data_type=HealthDataType.SERIES,
    )
    BLOOD_GLUCOSE = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="mg/dL",
        name="bloodGlucoses",
        name_zh="血糖",
        description="Glucose concentration in blood",
        description_zh="血液中的葡萄糖浓度",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['avg', 'max', 'min', 'last'],  # Daily blood glucose stats
    )
    BLOOD_OXYGEN = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="%",
        name="oxygenSaturations",
        name_zh="血氧饱和度",
        description="Oxygen saturation level in blood",
        description_zh="血液中氧气饱和度",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['avg', 'min'],  # Daily avg/min oxygen saturation
    )
    BLOOD_PRESSURE_DIASTOLIC = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="mmHg",
        name="diastolicPressures",
        name_zh="舒张压",
        description="Blood pressure during heart diastole",
        description_zh="心脏舒张时的血压",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
        aggregation_methods=['avg', 'max'],  # Daily avg/max diastolic pressure
    )
    BLOOD_PRESSURE_SYSTOLIC = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="mmHg",
        name="systolicPressures",
        name_zh="收缩压",
        description="Blood pressure during heart systole",
        description_zh="心脏收缩时的血压",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
        aggregation_methods=['avg', 'max'],  # Daily avg/max systolic pressure
    )
    BMI = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="count",
        name="bmis",
        name_zh="身体质量指数",
        description="Ratio of weight to height",
        description_zh="体重与身高的比值",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['last'],  # Daily latest BMI
    )
    BMR = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="kcal",
        name="basalMetabolicRate",
        name_zh="基础代谢率",
        description="Energy required to maintain basic physiological functions",
        description_zh="维持基本生理功能所需的能量",
        data_type=HealthDataType.MIX,
    )
    BODY_AGE = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="count",
        name="bodyAge",
        name_zh="身体年龄",
        description="Physiological age based on body composition",
        description_zh="基于身体成分的生理年龄",
        data_type=HealthDataType.MIX,
    )
    BODY_FAT_PERCENTAGE = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="%",
        name="bodyFatPercentages",
        name_zh="体脂率",
        description="Percentage of body fat relative to total body weight",
        description_zh="体内脂肪占体重的百分比",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['last'],  # Daily latest body fat percentage
    )
    BODY_SINEW = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="count",
        name="bodySinew",
        name_zh="肌腱",
        description="Tendon mass",
        description_zh="肌腱量",
        data_type=HealthDataType.SERIES,
    )
    BODY_TEMPERATURE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="°C",
        name="bodyTemperatures",
        name_zh="体温",
        description="Human body core temperature",
        description_zh="人体核心温度",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
        aggregation_methods=['avg'],  # Daily avg body temperature
    )
    BODY_WATER_PERCENTAGE = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="%",
        name="bodyWater",
        name_zh="体水分率",
        description="Percentage of body water relative to total body weight",
        description_zh="体内水分占体重的百分比",
        data_type=HealthDataType.MIX,
    )
    BONE_MASS = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="kg",
        name="bodyBone",
        name_zh="骨量",
        description="Bone mass weight",
        description_zh="骨骼重量",
        data_type=HealthDataType.MIX,
    )
    CALORIES_ACTIVE = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="kcal",
        name="activeCalories",
        name_zh="活动卡路里",
        description="Calories burned through exercise",
        description_zh="运动消耗的卡路里",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total active calories
    )
    CALORIES_BASAL = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="kcal",
        name="basalCalories",
        name_zh="基础卡路里",
        description="Calories burned through basal metabolism",
        description_zh="基础代谢消耗的卡路里",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total basal calories
    )
    DAILY_CALORIES_TOTAL = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="kcal",
        name="dailyTotalCalories",
        name_zh="总卡路里",
        description="Total calories including active and basal metabolism",
        description_zh="总消耗卡路里（活动+基础代谢）",
        data_type=HealthDataType.SUMMARY,
    )
    CHOLESTEROL_HDL = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="mmol/L",
        name="bodyCholesterolHDL",
        name_zh="高密度脂蛋白胆固醇",
        description="High-density lipoprotein cholesterol level",
        description_zh="高密度脂蛋白胆固醇水平",
        data_type=HealthDataType.SERIES,
    )
    CHOLESTEROL_LDL = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="mmol/L",
        name="bodyCholesterolLDL",
        name_zh="低密度脂蛋白胆固醇",
        description="Low-density lipoprotein cholesterol level",
        description_zh="低密度脂蛋白胆固醇水平",
        data_type=HealthDataType.SERIES,
    )
    CHOLESTEROL_TOTAL = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="mmol/L",
        name="bodyCholesterol",
        name_zh="总胆固醇",
        description="Total cholesterol level in blood",
        description_zh="血液中总胆固醇水平",
        data_type=HealthDataType.SERIES,
    )
    CHOLESTEROL_TRIGLYCERIDES = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="mmol/L",
        name="bodyCholesterolTriglycerides",
        name_zh="甘油三酯",
        description="Triglyceride level in blood",
        description_zh="血液中甘油三酯水平",
        data_type=HealthDataType.SERIES,
    )
    CYCLING_DISTANCE = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="m",
        name="cyclingDistances",
        name_zh="骑行距离",
        description="Total cycling distance",
        description_zh="骑行总距离",
        data_type=HealthDataType.SERIES,
    )
    CYCLING_DURATION = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="ms",
        name="cyclingDuration",
        name_zh="骑行时长",
        description="Duration of cycling",
        description_zh="骑行时长",
        data_type=HealthDataType.SERIES,
    )
    CYCLING_SPEED = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="m/s",
        name="cyclingSpeeds",
        name_zh="骑行速度",
        description="Average cycling speed",
        description_zh="骑行平均速度",
        data_type=HealthDataType.SERIES,
    )
    DAILY_CALORIES_ACTIVE = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="kcal",
        name="dailyCaloriesActive",
        name_zh="每日活动卡路里",
        description="Daily summary of calories burned through exercise",
        description_zh="每日运动消耗的卡路里汇总",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_CALORIES_BASAL = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="kcal",
        name="dailyCaloriesBasal",
        name_zh="每日基础卡路里",
        description="Daily summary of calories burned through basal metabolism",
        description_zh="每日基础代谢消耗的卡路里汇总",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_DISTANCE = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="m",
        name="dailyDistance",
        name_zh="每日距离",
        description="Daily summary of walking or exercise distance",
        description_zh="每日行走或运动距离汇总",
        data_type=HealthDataType.SUMMARY,
    )
    DEEP_SLEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Deep)",
        name_zh="深度睡眠",
        description="Deep sleep duration",
        description_zh="深度睡眠时间",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total langchain sleep time
    )
    DIETARY_ALCOHOL = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="alcohol",
        name_zh="酒精摄入",
        description="Alcohol intake amount",
        description_zh="酒精摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_CAFFEINE = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="caffeine",
        name_zh="咖啡因摄入",
        description="Caffeine intake amount",
        description_zh="咖啡因摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_CARBS = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="carbohydrates",
        name_zh="碳水化合物摄入",
        description="Carbohydrate intake amount",
        description_zh="碳水化合物摄入量",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
    )
    DIETARY_CHOLESTEROL = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="cholesterol",
        name_zh="胆固醇摄入",
        description="Dietary cholesterol intake amount",
        description_zh="膳食胆固醇摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_SODIUM = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="mg",
        name="sodium",
        name_zh="钠摄入",
        description="Dietary sodium intake amount",
        description_zh="膳食钠摄入量",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
    )
    DIETARY_ENERGY = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="kcal",
        name="energyes",
        name_zh="能量摄入",
        description="Total energy intake amount",
        description_zh="总能量摄入量",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
    )
    DIETARY_FATS = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="fats",
        name_zh="脂肪摄入",
        description="Fat intake amount",
        description_zh="脂肪摄入量",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
    )
    DIETARY_FIBRE = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="fibre",
        name_zh="纤维摄入",
        description="Dietary fiber intake amount",
        description_zh="膳食纤维摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_MONOUNSATURATED = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="monounsaturated",
        name_zh="单不饱和脂肪酸摄入",
        description="Monounsaturated fatty acid intake amount",
        description_zh="单不饱和脂肪酸摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_OMEGA3 = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="omega3",
        name_zh="Omega-3摄入",
        description="Omega-3 fatty acid intake amount",
        description_zh="Omega-3脂肪酸摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_OMEGA6 = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="omega6",
        name_zh="Omega-6摄入",
        description="Omega-6 fatty acid intake amount",
        description_zh="Omega-6脂肪酸摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_POLYUNSATURATED = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="polyunsaturated",
        name_zh="多不饱和脂肪酸摄入",
        description="Polyunsaturated fatty acid intake amount",
        description_zh="多不饱和脂肪酸摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_PROTEIN = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="proteins",
        name_zh="蛋白质摄入",
        description="Protein intake amount",
        description_zh="蛋白质摄入量",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
    )
    DIETARY_SATURATED = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="saturated",
        name_zh="饱和脂肪酸摄入",
        description="Saturated fatty acid intake amount",
        description_zh="饱和脂肪酸摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_SUGAR = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="g",
        name="sugar",
        name_zh="糖分摄入",
        description="Sugar intake amount",
        description_zh="糖分摄入量",
        data_type=HealthDataType.SERIES,
    )
    DIETARY_WATER = IndicatorInfo(
        category=Categories.NUTRITION.value,
        standard_unit="L",
        name="waters",
        name_zh="饮水量",
        description="Daily water intake",
        description_zh="每日饮水量",
        data_type=HealthDataType.SERIES,
    )
    DISTANCE = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="m",
        name="walkingRunningDistances",
        name_zh="距离",
        description="Walking or exercise distance",
        description_zh="行走或运动距离",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total walking/running distance
    )
    WALKING_RUNNING_DURATION = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="ms",
        name="walkingRunningDuration",
        name_zh="步行和跑步时长",
        description="Duration of walking and running",
        description_zh="步行和跑步时长",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltage",
        name_zh="心电图电压",
        description="Electrocardiogram voltage signal",
        description_zh="心电图电压信号",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD1 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLead1",
        name_zh="心电图I导联",
        description="Electrocardiogram lead I voltage",
        description_zh="心电图第I导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD2 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLead2",
        name_zh="心电图II导联",
        description="Electrocardiogram lead II voltage",
        description_zh="心电图第II导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD3 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLead3",
        name_zh="心电图III导联",
        description="Electrocardiogram lead III voltage",
        description_zh="心电图第III导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_AVF = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadAvf",
        name_zh="心电图aVF导联",
        description="Electrocardiogram aVF lead voltage",
        description_zh="心电图aVF导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_AVL = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadAvl",
        name_zh="心电图aVL导联",
        description="Electrocardiogram aVL lead voltage",
        description_zh="心电图aVL导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_AVR = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadAvr",
        name_zh="心电图aVR导联",
        description="Electrocardiogram aVR lead voltage",
        description_zh="心电图aVR导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V1 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV1",
        name_zh="心电图V1导联",
        description="Electrocardiogram V1 lead voltage",
        description_zh="心电图V1导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V2 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV2",
        name_zh="心电图V2导联",
        description="Electrocardiogram V2 lead voltage",
        description_zh="心电图V2导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V3 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV3",
        name_zh="心电图V3导联",
        description="Electrocardiogram V3 lead voltage",
        description_zh="心电图V3导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V3R = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV3R",
        name_zh="心电图V3R导联",
        description="Electrocardiogram V3R lead voltage",
        description_zh="心电图V3R导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V4 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV4",
        name_zh="心电图V4导联",
        description="Electrocardiogram V4 lead voltage",
        description_zh="心电图V4导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V4R = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV4R",
        name_zh="心电图V4R导联",
        description="Electrocardiogram V4R lead voltage",
        description_zh="心电图V4R导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V5 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV5",
        name_zh="心电图V5导联",
        description="Electrocardiogram V5 lead voltage",
        description_zh="心电图V5导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V5R = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV5R",
        name_zh="心电图V5R导联",
        description="Electrocardiogram V5R lead voltage",
        description_zh="心电图V5R导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V6 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV6",
        name_zh="心电图V6导联",
        description="Electrocardiogram V6 lead voltage",
        description_zh="心电图V6导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V7 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV7",
        name_zh="心电图V7导联",
        description="Electrocardiogram V7 lead voltage",
        description_zh="心电图V7导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V8 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV8",
        name_zh="心电图V8导联",
        description="Electrocardiogram V8 lead voltage",
        description_zh="心电图V8导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_LEAD_V9 = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltageLeadV9",
        name_zh="心电图V9导联",
        description="Electrocardiogram V9 lead voltage",
        description_zh="心电图V9导联电压",
        data_type=HealthDataType.SERIES,
    )
    ELECTROCARDIOGRAM_VOLTAGE_POSTERIOR_LEADS = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="mV",
        name="electrocardiogramVoltagePosteriorLeads",
        name_zh="心电图后壁导联",
        description="Electrocardiogram posterior lead voltage",
        description_zh="心电图后壁导联电压",
        data_type=HealthDataType.SERIES,
    )
    FAT_FREE_WEIGHT = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="kg",
        name="bodyFatFreeWeight",
        name_zh="去脂体重",
        description="Fat-free body weight",
        description_zh="除脂肪外的体重",
        data_type=HealthDataType.MIX,
    )
    FLOORS_CLIMBED = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="count",
        name="floors",
        name_zh="爬楼层数",
        description="Number of floors climbed",
        description_zh="爬升的楼层数",
        data_type=HealthDataType.SERIES,
    )
    FLOORS_CLIMBED_DURATION = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="ms",
        name="floorsDuration",
        name_zh="爬楼时长",
        description="Duration of floors climbed",
        description_zh="爬升的楼层时长",
        data_type=HealthDataType.SERIES,
    )
    DAILY_FLOORS_CLIMBED = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="count",
        name="dailyFloors",
        name_zh="爬楼层数",
        description="Number of floors climbed",
        description_zh="爬升的楼层数",
        data_type=HealthDataType.SUMMARY,
    )
    HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="heartRates",
        name_zh="心率",
        description="Number of heartbeats per minute",
        description_zh="每分钟心跳次数",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['avg', 'max', 'min'],  # Daily avg/max/min heart rate
    )
    HEART_RATE_MAX = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="bpm",
        name="heartRateMax",
        name_zh="最大心率",
        description="Maximum heart rate during time range",
        description_zh="运动期间的最大心率",
        data_type=HealthDataType.SERIES,
    )
    DAILY_HEART_RATE_MAX = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="bpm",
        name="dailyHeartRateMax",
        name_zh="最大心率",
        description="Maximum heart rate during time range",
        description_zh="运动期间的最大心率",
        data_type=HealthDataType.SUMMARY,
    )
    HEART_RATE_MIN = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="bpm",
        name="heartRateMin",
        name_zh="最小心率",
        description="Minimum heart rate during time range",
        description_zh="运动期间的最小心率",
        data_type=HealthDataType.SERIES,
    )
    DAILY_HEART_RATE_MIN = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="bpm",
        name="dailyHeartRateMin",
        name_zh="最小心率",
        description="Minimum heart rate during time range",
        description_zh="运动期间的最小心率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_HEART_RATE_AVG = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="bpm",
        name="dailyHeartRateAvg",
        name_zh="平均心率",
        description="Avg heart rate during time range",
        description_zh="运动期间的平均心率",
        data_type=HealthDataType.SUMMARY,
    )
    RESTING_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="restingHeartRates",
        name_zh="静息心率",
        description="Heart rate at rest",
        description_zh="静息状态下的心率",
        data_type=HealthDataType.SERIES,
    )
    DAILY_HEART_RATE_RESTING = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailyRestingHeartRates",
        name_zh="静息心率",
        description="Heart rate at rest",
        description_zh="静息状态下的心率",
        data_type=HealthDataType.SUMMARY,
    )
    HEIGHT = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="m",
        name="heights",
        name_zh="身高",
        description="Body height",
        description_zh="身体高度",
        data_type=HealthDataType.SERIES,
    )
    HRV = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="hrvDatas",
        name_zh="心率变异性",
        description="Heart rate variability",
        description_zh="心跳间隔的变化",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['avg'],  # Daily avg HRV
    )
    HRV_RMSSD = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="hrvRMSSD",
        name_zh="心率变异性RMSSD",
        description="Root mean square of successive differences in heart rate",
        description_zh="相邻心跳间隔差的均方根",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['avg'],  # Daily avg HRV RMSSD
    )
    IGE_BAHA_GRASS = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="FSU",
        name="igeBahaGrass",
        name_zh="巴哈草过敏原IgE",
        description="Baha grass allergen-specific IgE antibody level",
        description_zh="巴哈草过敏原特异性IgE抗体水平",
        data_type=HealthDataType.SERIES,
    )
    IGG_DAIRY = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="FSU",
        name="iggDairy",
        name_zh="乳制品IgG",
        description="Dairy-specific IgG antibody level",
        description_zh="乳制品特异性IgG抗体水平",
        data_type=HealthDataType.SERIES,
    )
    INSULIN_INJECTION_LONG_ACTING = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="unit",
        name="insulinInjectionLongActing",
        name_zh="长效胰岛素注射",
        description="Long-acting insulin injection amount",
        description_zh="长效胰岛素注射量",
        data_type=HealthDataType.SERIES,
    )
    INSULIN_INJECTION_RAPID_ACTING = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="unit",
        name="insulinInjectionRapidActing",
        name_zh="速效胰岛素注射",
        description="Rapid-acting insulin injection amount",
        description_zh="速效胰岛素注射量",
        data_type=HealthDataType.SERIES,
    )
    LIGHT_SLEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Core)",
        name_zh="浅度睡眠",
        description="Light sleep duration",
        description_zh="浅度睡眠时间",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total light/core sleep time
    )
    MAX_HEART_RATE_PROFILE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="maxHeartRateProfile",
        name_zh="个人最大心率",
        description="Maximum heart rate set in user profile",
        description_zh="用户个人资料中设定的最大心率值",
        data_type=HealthDataType.SERIES,
    )
    MINDFULNESS_MINUTES = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="min",
        name="mindfulnessMinutes",
        name_zh="正念时间",
        description="Mindfulness meditation time",
        description_zh="正念冥想时间",
        data_type=HealthDataType.SERIES,
    )
    MUSCLE_PERCENTAGE = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="%",
        name="bodyMuscle",
        name_zh="肌肉率",
        description="Percentage of muscle relative to total body weight",
        description_zh="肌肉占体重的百分比",
        data_type=HealthDataType.MIX,
    )
    PROTEIN_PERCENTAGE = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="%",
        name="bodyProtein",
        name_zh="蛋白质率",
        description="Percentage of protein relative to total body weight",
        description_zh="蛋白质占体重的百分比",
        data_type=HealthDataType.MIX,
    )
    RECOVERY_SCORE = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="%",
        name="recoveryScore",
        name_zh="恢复分数",
        description="Body recovery status score percentage based on HRV, resting heart rate and other indicators",
        description_zh="身体恢复状态评分百分比，基于心率变异性、静息心率等指标",
        data_type=HealthDataType.SERIES,
    )
    RECOVERY_TIME = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="count/min",
        name="recoveryes",
        name_zh="恢复时间",
        description="Recommended recovery time",
        description_zh="建议的恢复时间",
        data_type=HealthDataType.SERIES,
    )
    REM_SLEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(REM)",
        name_zh="REM睡眠",
        description="Rapid eye movement sleep duration",
        description_zh="快速眼动睡眠时间",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total REM sleep time
    )
    RESPIRATORY_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="respiratoryRates",
        name_zh="呼吸频率",
        description="Number of breaths per minute",
        description_zh="每分钟呼吸次数",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['avg'],  # Daily avg respiratory rate
    )
    RESPIRATORY_RATE_MIN = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="respiratoryRateMin",
        name_zh="最小呼吸频率",
        description="Minimum respiratory rate",
        description_zh="最小呼吸频率",
        data_type=HealthDataType.SUMMARY,
    )
    RESPIRATORY_RATE_MAX = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="respiratoryRateMax",
        name_zh="最大呼吸频率",
        description="Maximum respiratory rate",
        description_zh="最大呼吸频率",
        data_type=HealthDataType.SUMMARY,
    )
    RESPIRATORY_RATE_AVG = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="respiratoryRateAvg",
        name_zh="平均呼吸频率",
        description="Average respiratory rate",
        description_zh="平均呼吸频率",
        data_type=HealthDataType.SUMMARY,
    )
    CADENCE = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="steps/min",
        name="cadence",
        name_zh="步频",
        description="Step cadence during exercise",
        description_zh="运动时的步频",
        data_type=HealthDataType.SUMMARY,
    )
    CADENCE_MIN = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="steps/min",
        name="cadenceMin",
        name_zh="最小步频",
        description="Minimum step cadence",
        description_zh="最小步频",
        data_type=HealthDataType.SUMMARY,
    )
    CADENCE_MAX = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="steps/min",
        name="cadenceMax",
        name_zh="最大步频",
        description="Maximum step cadence",
        description_zh="最大步频",
        data_type=HealthDataType.SUMMARY,
    )
    HRV_MIN = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="hrvMin",
        name_zh="最小心率变异性",
        description="Minimum heart rate variability",
        description_zh="最小心率变异性",
        data_type=HealthDataType.SUMMARY,
    )
    HRV_MAX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="hrvMax",
        name_zh="最大心率变异性",
        description="Maximum heart rate variability",
        description_zh="最大心率变异性",
        data_type=HealthDataType.SUMMARY,
    )
    STRESS_LEVEL_MIN = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="stressLevelMin",
        name_zh="最小压力水平",
        description="Minimum stress level",
        description_zh="最小压力水平",
        data_type=HealthDataType.SUMMARY,
    )
    STRESS_LEVEL_MAX = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="stressLevelMax",
        name_zh="最大压力水平",
        description="Maximum stress level",
        description_zh="最大压力水平",
        data_type=HealthDataType.SUMMARY,
    )
    QTC_INTERVAL = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="ms",
        name="qtcInterval",
        name_zh="QTc间期",
        description="Corrected QT interval (heart electrical activity)",
        description_zh="校正QT间期（心脏电活动）",
        data_type=HealthDataType.SUMMARY,
    )
    QTC_INTERVAL_MIN = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="ms",
        name="qtcIntervalMin",
        name_zh="最小QTc间期",
        description="Minimum corrected QT interval",
        description_zh="最小校正QT间期",
        data_type=HealthDataType.SUMMARY,
    )
    QTC_INTERVAL_MAX = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="ms",
        name="qtcIntervalMax",
        name_zh="最大QTc间期",
        description="Maximum corrected QT interval",
        description_zh="最大校正QT间期",
        data_type=HealthDataType.SUMMARY,
    )
    SHOCK_LEVEL = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="shockLevel",
        name_zh="冲击水平",
        description="Exercise shock/impact level",
        description_zh="运动冲击水平",
        data_type=HealthDataType.SUMMARY,
    )
    SHOCK_LEVEL_MIN = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="shockLevelMin",
        name_zh="最小冲击水平",
        description="Minimum shock level",
        description_zh="最小冲击水平",
        data_type=HealthDataType.SUMMARY,
    )
    SHOCK_LEVEL_MAX = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="shockLevelMax",
        name_zh="最大冲击水平",
        description="Maximum shock level",
        description_zh="最大冲击水平",
        data_type=HealthDataType.SUMMARY,
    )
    STRAIN_MIN = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="strainMin",
        name_zh="最小应变",
        description="Minimum strain level",
        description_zh="最小应变水平",
        data_type=HealthDataType.SUMMARY,
    )
    STRAIN_MAX = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="strainMax",
        name_zh="最大应变",
        description="Maximum strain level",
        description_zh="最大应变水平",
        data_type=HealthDataType.SUMMARY,
    )
    HR_ZONE_1_TIME = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="s",
        name="hrZone1Time",
        name_zh="心率区间1时间",
        description="Time spent in heart rate zone 1 (50-60% max HR)",
        description_zh="在心率区间1的时间（50-60%最大心率）",
        data_type=HealthDataType.SUMMARY,
    )
    HR_ZONE_2_TIME = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="s",
        name="hrZone2Time",
        name_zh="心率区间2时间",
        description="Time spent in heart rate zone 2 (60-70% max HR)",
        description_zh="在心率区间2的时间（60-70%最大心率）",
        data_type=HealthDataType.SUMMARY,
    )
    HR_ZONE_3_TIME = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="s",
        name="hrZone3Time",
        name_zh="心率区间3时间",
        description="Time spent in heart rate zone 3 (70-80% max HR)",
        description_zh="在心率区间3的时间（70-80%最大心率）",
        data_type=HealthDataType.SUMMARY,
    )
    HR_ZONE_4_TIME = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="s",
        name="hrZone4Time",
        name_zh="心率区间4时间",
        description="Time spent in heart rate zone 4 (80-90% max HR)",
        description_zh="在心率区间4的时间（80-90%最大心率）",
        data_type=HealthDataType.SUMMARY,
    )
    HR_ZONE_5_TIME = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="s",
        name="hrZone5Time",
        name_zh="心率区间5时间",
        description="Time spent in heart rate zone 5 (90-100% max HR)",
        description_zh="在心率区间5的时间（90-100%最大心率）",
        data_type=HealthDataType.SUMMARY,
    )
    NORMAL_BEATS_COUNT = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="normalBeatsCount",
        name_zh="正常心跳次数",
        description="Number of normal heartbeats",
        description_zh="正常心跳的次数",
        data_type=HealthDataType.SUMMARY,
    )
    ABNORMAL_BEATS_COUNT = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="abnormalBeatsCount",
        name_zh="异常心跳次数",
        description="Number of abnormal heartbeats",
        description_zh="异常心跳的次数",
        data_type=HealthDataType.SUMMARY,
    )
    UNKNOWN_BEATS_COUNT = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="unknownBeatsCount",
        name_zh="无法识别心跳次数",
        description="Number of unrecognized heartbeats",
        description_zh="无法识别的心跳次数",
        data_type=HealthDataType.SUMMARY,
    )
    NORMAL_RHYTHM_PERCENTAGE = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="%",
        name="normalRhythmPercentage",
        name_zh="正常心律百分比",
        description="Percentage of normal heart rhythm",
        description_zh="正常心律占比",
        data_type=HealthDataType.SUMMARY,
    )
    ABNORMAL_RHYTHM_PERCENTAGE = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="%",
        name="abnormalRhythmPercentage",
        name_zh="异常心律百分比",
        description="Percentage of abnormal heart rhythm",
        description_zh="异常心律占比",
        data_type=HealthDataType.SUMMARY,
    )
    ARRHYTHMIA_NORMAL_SEGMENTS = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="arrhythmiaNormalSegments",
        name_zh="正常心律段数",
        description="Number of normal rhythm segments",
        description_zh="正常心律的分析段数",
        data_type=HealthDataType.SUMMARY,
    )
    ARRHYTHMIA_AFIB_SEGMENTS = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="arrhythmiaAfibSegments",
        name_zh="房颤段数",
        description="Number of atrial fibrillation segments detected",
        description_zh="检测到的心房颤动分析段数",
        data_type=HealthDataType.SUMMARY,
    )
    ARRHYTHMIA_OTHER_SEGMENTS = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="arrhythmiaOtherSegments",
        name_zh="其他异常心律段数",
        description="Number of other arrhythmia segments",
        description_zh="其他异常心律的分析段数",
        data_type=HealthDataType.SUMMARY,
    )
    ARRHYTHMIA_NOISE_SEGMENTS = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="arrhythmiaNoiseSegments",
        name_zh="噪声段数",
        description="Number of noise segments",
        description_zh="噪声干扰的分析段数",
        data_type=HealthDataType.SUMMARY,
    )
    ARRHYTHMIA_AFIB_DURATION = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="s",
        name="arrhythmiaAfibDuration",
        name_zh="房颤持续时间",
        description="Total duration of atrial fibrillation",
        description_zh="心房颤动的总持续时间",
        data_type=HealthDataType.SUMMARY,
    )
    ARRHYTHMIA_AFIB_PERCENTAGE = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="%",
        name="arrhythmiaAfibPercentage",
        name_zh="房颤占比",
        description="Percentage of atrial fibrillation time",
        description_zh="心房颤动时间占比",
        data_type=HealthDataType.SUMMARY,
    )
    SKIN_TEMPERATURE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="°C",
        name="skinTemperature",
        name_zh="皮肤温度",
        description="Skin surface temperature measurement",
        description_zh="皮肤表面温度测量值",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ANALYSIS_ASLEEP_CORE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Core)",
        name_zh="睡眠分析-核心睡眠",
        description="Core sleep stage duration in sleep analysis",
        description_zh="睡眠分析中的核心睡眠阶段时长",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ANALYSIS_ASLEEP_DEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Deep)",
        name_zh="睡眠分析-深度睡眠",
        description="Deep sleep stage duration in sleep analysis",
        description_zh="睡眠分析中的深度睡眠阶段时长",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ANALYSIS_ASLEEP_REM = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(REM)",
        name_zh="睡眠分析-REM睡眠",
        description="REM sleep stage duration in sleep analysis",
        description_zh="睡眠分析中的REM睡眠阶段时长",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ANALYSIS_ASLEEP_UNSPECIFIED = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Unspecified)",
        name_zh="睡眠分析-未指定睡眠",
        description="Unspecified sleep stage duration in sleep analysis",
        description_zh="睡眠分析中的未指定睡眠阶段时长",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ANALYSIS_AWAKE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Awake",
        name_zh="睡眠分析-清醒",
        description="Awake stage duration in sleep analysis",
        description_zh="睡眠分析中的清醒阶段时长",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ASLEEP_CORE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Core)",
        name_zh="核心睡眠",
        description="Core sleep time",
        description_zh="核心睡眠时间",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ASLEEP_DEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Deep)",
        name_zh="深度睡眠",
        description="Deep sleep time",
        description_zh="深度睡眠时间",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_ASLEEP_REM = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(REM)",
        name_zh="REM睡眠",
        description="REM sleep time",
        description_zh="REM睡眠时间",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_AWAKE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Awake",
        name_zh="清醒时间",
        description="Awake time during sleep period",
        description_zh="睡眠期间的清醒时间",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_CONSISTENCY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="sleepConsistency",
        name_zh="睡眠一致性",
        description="Sleep consistency percentage measuring sleep routine regularity",
        description_zh="睡眠时间一致性百分比，衡量睡眠作息的规律性",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_DISTURBANCES = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="count",
        name="sleepDisturbances",
        name_zh="睡眠干扰次数",
        description="Number of disturbances and awakenings during sleep",
        description_zh="睡眠期间的干扰和觉醒次数",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_DURATION = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="hours",
        name="sleepDuration",
        name_zh="睡眠时长",
        description="Total sleep time",
        description_zh="总睡眠时间",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_EFFICIENCY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="sleepEfficiency",
        name_zh="睡眠效率",
        description="Percentage of actual sleep time relative to time in bed",
        description_zh="实际睡眠时间占在床时间的百分比",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_IN_BED = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_InBed",
        name_zh="在床时间",
        description="Total time in bed",
        description_zh="在床上的总时间",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_LATENCY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="seconds",
        name="sleepLatency",
        name_zh="入睡时间",
        description="Time from lying down to falling asleep",
        description_zh="从躺下到入睡的时间",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_NAP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="napDuration",
        name_zh="午睡时长",
        description="Short daytime nap duration",
        description_zh="短时间白天小憩时长",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_PERFORMANCE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="sleepPerformance",
        name_zh="睡眠表现",
        description="Sleep performance score percentage based on actual vs needed sleep time",
        description_zh="睡眠表现评分百分比，基于实际睡眠时间与需要睡眠时间的比较",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_QUALITY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="sleepQuality",
        name_zh="睡眠质量",
        description="Sleep quality score",
        description_zh="睡眠质量评分",
        data_type=HealthDataType.SERIES,
    )
    SLEEP_UNSPECIFIED = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Unspecified)",
        name_zh="未分类睡眠",
        description="Unclassified sleep time",
        description_zh="未分类的睡眠时间",
        data_type=HealthDataType.SERIES,
    )
    SPEED = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="m/s",
        name="speeds",
        name_zh="速度",
        description="Exercise speed",
        description_zh="运动速度",
        data_type=HealthDataType.SERIES,
    )
    WALKING_SPEED = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="m/s",
        name="walkingSpeeds",
        name_zh="步行速度",
        description="Walking speed",
        description_zh="步行速度",
        data_type=HealthDataType.SERIES,
    )
    STEPS = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="count",
        name="steps",
        name_zh="步数",
        description="Walking step count",
        description_zh="行走步数统计",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['total'],  # Daily total steps
    )
    STEP_DURATION = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="ms",
        name="stepDuration",
        name_zh="步行时长",
        description="Walking step duration",
        description_zh="步行时长",
        data_type=HealthDataType.SERIES,
    )
    DAILY_STEPS = IndicatorInfo(
        category=Categories.ACTIVITY.value,
        standard_unit="count",
        name="dailySteps",
        name_zh="每日步数",
        description="Daily total step count summary",
        description_zh="每日步数汇总",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_AVG_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailyAvgHeartRate",
        name_zh="每日平均心率",
        description="Daily average heart rate summary",
        description_zh="每日平均心率汇总",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_WEIGHT = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="kg",
        name="dailyWeight",
        name_zh="每日体重",
        description="Daily weight measurement summary",
        description_zh="每日体重测量汇总",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_EFFICIENCY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="dailySleepEfficiency",
        name_zh="每日睡眠效率",
        description="Daily sleep efficiency summary",
        description_zh="每日睡眠效率汇总",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_DURATION = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailySleepDuration",
        name_zh="每日在床时间",
        description="Daily total time in bed",
        description_zh="每日总在床时间",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_TOTAL_SLEEP_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailyTotalSleepTime",
        name_zh="每日总睡眠时间",
        description="Daily total sleep time",
        description_zh="每日总睡眠时间",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_DEEP_SLEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailyDeepSleep",
        name_zh="每日深度睡眠",
        description="Daily langchain sleep duration",
        description_zh="每日深度睡眠时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_LIGHT_SLEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailyLightSleep",
        name_zh="每日浅睡眠",
        description="Daily light sleep duration",
        description_zh="每日浅睡眠时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_REM_SLEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailyRemSleep",
        name_zh="每日REM睡眠",
        description="Daily REM sleep duration",
        description_zh="每日REM睡眠时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_AWAKE_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailyAwakeTime",
        name_zh="每日睡眠清醒时间",
        description="Daily awake time during sleep",
        description_zh="每日睡眠期间清醒时间",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_LATENCY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailySleepLatency",
        name_zh="每日入睡时间",
        description="Daily sleep onset latency",
        description_zh="每日入睡时间",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_AVG_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailySleepAvgHeartRate",
        name_zh="每日睡眠平均心率",
        description="Daily average heart rate during sleep",
        description_zh="每日睡眠期间平均心率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_LOWEST_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailySleepLowestHeartRate",
        name_zh="每日睡眠最低心率",
        description="Daily lowest heart rate during sleep",
        description_zh="每日睡眠期间最低心率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_AVG_HRV = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="dailySleepAvgHrv",
        name_zh="每日睡眠平均HRV",
        description="Daily average HRV during sleep",
        description_zh="每日睡眠期间平均HRV",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_WORKOUT_AVG_HRV = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="dailyWorkoutAvgHrv",
        name_zh="平均HRV",
        description="Daily average HRV during workout",
        description_zh="每日平均HRV",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_RESPIRATORY_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailySleepRespiratoryRate",
        name_zh="每日睡眠呼吸率",
        description="Daily average respiratory rate during sleep",
        description_zh="每日睡眠期间平均呼吸率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_SKIN_TEMPERATURE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="°C",
        name="dailySleepSkinTemperature",
        name_zh="每日睡眠皮肤温度",
        description="Daily average skin temperature during sleep",
        description_zh="每日睡眠期间平均皮肤温度",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_TEMPERATURE_DELTA = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="°C",
        name="dailySleepTemperatureDelta",
        name_zh="每日睡眠体温偏差",
        description="Daily temperature deviation from baseline",
        description_zh="每日体温偏差",
        data_type=HealthDataType.SUMMARY,
    )
    STRAIN = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="strain",
        name_zh="身体压力",
        description="Strain score, cardiovascular load indicator calculated from heart rate data",
        description_zh="身体压力评分，基于心率数据计算的心血管负荷指标",
        data_type=HealthDataType.SUMMARY,
    )
    STRESS_LEVEL = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="%",
        name="stressLevel",
        name_zh="压力水平",
        description="Body stress level score",
        description_zh="身体压力水平评分",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
    )
    SUBCUTANEOUS_FAT = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="%",
        name="bodySubFat",
        name_zh="皮下脂肪",
        description="Subcutaneous fat percentage",
        description_zh="皮下脂肪百分比",
        data_type=HealthDataType.MIX,
    )
    TEMPERATURE_DELTA = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="°C",
        name="temperatureDelta",
        name_zh="体温变化",
        description="Body temperature change during sleep",
        description_zh="睡眠期间体温变化",
        data_type=HealthDataType.SERIES,
    )
    TOTAL_SLEEP = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepAnalysis_Asleep(Total)",
        name_zh="总睡眠时间",
        description="Total actual sleep time",
        description_zh="实际睡眠的总时间",
        data_type=HealthDataType.SERIES,
    )
    TOTAL_SLEEP_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="seconds",
        name="totalSleepTime",
        name_zh="总睡眠时间",
        description="Total actual sleep time",
        description_zh="实际睡眠的总时间",
        data_type=HealthDataType.SERIES,
    )
    TRAINING_LOAD = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="trainingLoad",
        name_zh="训练负荷",
        description="Training intensity and load score",
        description_zh="训练强度和负荷评分",
        data_type=HealthDataType.SERIES,
    )
    DAILY_TRAINING_LOAD = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="dailyTrainingLoad",
        name_zh="训练负荷",
        description="Training intensity and load score",
        description_zh="训练强度和负荷评分",
        data_type=HealthDataType.SUMMARY,
    )
    USER_GENDER = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="enum",
        name="userGender",
        name_zh="性别",
        description="User gender information",
        description_zh="用户性别信息",
        data_type=HealthDataType.MIX,
    )
    USER_SEX = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="enum",
        name="userSex",
        name_zh="生理性别",
        description="User biological sex",
        description_zh="用户生理性别",
        data_type=HealthDataType.SERIES,
    )
    UV_EXPOSURE = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="count",
        name="uvExposures",
        name_zh="紫外线暴露",
        description="UV exposure index",
        description_zh="紫外线暴露指数",
        data_type=HealthDataType.SERIES,
    )
    VISCERAL_FAT = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="%",
        name="bodyVisFat",
        name_zh="内脏脂肪",
        description="Visceral fat level around organs",
        description_zh="内脏周围脂肪等级",
        data_type=HealthDataType.MIX,
    )
    VO2_MAX = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="L/min/kg",
        name="vo2Maxs",
        name_zh="最大摄氧量",
        description="Maximum oxygen uptake capacity",
        description_zh="最大氧气摄取能力",
        data_type=HealthDataType.MIX,  # MIX type: writes to both series_data and th_series_data
        aggregation_methods=['avg', 'max'],  # Daily avg/max VO2 Max
    )
    WAIST_CIRCUMFERENCE = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="m",
        name="waistCircumferences",
        name_zh="腰围",
        description="Waist circumference",
        description_zh="腰部围度",
        data_type=HealthDataType.SERIES,
    )
    WALKING_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="walkingHeartRates",
        name_zh="步行心率",
        description="Heart rate during walking",
        description_zh="步行时的心率",
        data_type=HealthDataType.SERIES,
    )
    DAILY_WALKING_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailyWalkingHeartRates",
        name_zh="步行心率",
        description="Heart rate during walking",
        description_zh="步行时的心率",
        data_type=HealthDataType.SUMMARY,
    )
    WEIGHT = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="kg",
        name="bodyMasss",
        name_zh="体重",
        description="Total body weight",
        description_zh="身体总重量",
        data_type=HealthDataType.SERIES,
        aggregation_methods=['last'],  # Daily latest weight (multiple measurements take last)
    )
    WHEELCHAIR_USE = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="boolean",
        name="wheelchairUse",
        name_zh="轮椅使用",
        description="Whether wheelchair is used",
        description_zh="是否使用轮椅",
        data_type=HealthDataType.SERIES,
    )
    WORKOUT_DURATION = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="min",
        name="workoutDuration",
        name_zh="锻炼时长",
        description="Total workout duration",
        description_zh="总锻炼时间",
        data_type=HealthDataType.MIX,
    )
    WORKOUT_DURATION_HIGH = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="min",
        name="workoutDurationHigh",
        name_zh="高强度锻炼时长",
        description="High-intensity workout duration",
        description_zh="高强度锻炼时间",
        data_type=HealthDataType.SERIES,
    )
    WORKOUT_DURATION_LOW = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="min",
        name="workoutDurationLow",
        name_zh="低强度锻炼时长",
        description="Low-intensity workout duration",
        description_zh="低强度锻炼时间",
        data_type=HealthDataType.SERIES,
    )
    WORKOUT_DURATION_MEDIUM = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="min",
        name="workoutDurationMedium",
        name_zh="中强度锻炼时长",
        description="Medium-intensity workout duration",
        description_zh="中强度锻炼时间",
        data_type=HealthDataType.SERIES,
    )
    WORKOUT_STRAIN = IndicatorInfo(
        category=Categories.PERFORMANCE.value,
        standard_unit="score",
        name="workoutStrain",
        name_zh="锻炼压力",
        description="Body stress score for single workout",
        description_zh="单次锻炼的身体压力评分",
        data_type=HealthDataType.SUMMARY,
    )
    WRIST_TEMPERATURE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="°C",
        name="wristTemperatures",
        name_zh="手腕温度",
        description="Wrist skin temperature",
        description_zh="手腕皮肤温度",
        data_type=HealthDataType.SERIES,
    )

    # sleep Structure Indicators
    DAILY_DEEP_SLEEP_PERCENTAGE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="dailyDeepSleepPercentage",
        name_zh="深睡比例",
        description="Deep sleep percentage of total sleep",
        description_zh="深度睡眠占总睡眠的比例",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_LIGHT_SLEEP_PERCENTAGE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="dailyLightSleepPercentage",
        name_zh="浅睡比例",
        description="Light sleep percentage of total sleep",
        description_zh="浅睡阶段占总睡眠的比例",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_REM_SLEEP_PERCENTAGE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="dailyRemSleepPercentage",
        name_zh="REM睡眠比例",
        description="REM sleep percentage of total sleep",
        description_zh="REM睡眠占总睡眠的比例",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_CYCLE_TIMES = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="count",
        name="dailySleepCycleTimes",
        name_zh="睡眠周期数",
        description="Number of sleep cycles",
        description_zh="睡眠周期的总数",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_CONTINUITY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ratio",
        name="dailySleepContinuity",
        name_zh="睡眠连续性指数",
        description="Sleep continuity index",
        description_zh="睡眠连续性的衡量指标",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_SFFCY = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ratio",
        name="dailySleepSffcy",
        name_zh="睡眠效率",
        description="Sleep efficiency assessment indicator",
        description_zh="睡眠效率的评估指标",
        data_type=HealthDataType.SUMMARY,
    )

    # Respiratory Indicators
    DAILY_RESPIRATORY_RATES = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailyRespiratoryRates",
        name_zh="平均呼吸",
        description="Average respiratory rate during sleep",
        description_zh="本次睡眠的平均呼吸频率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_MAX_RESPIRATORY_RATES = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailyMaxRespiratoryRates",
        name_zh="最高呼吸率",
        description="Highest respiratory rate during sleep",
        description_zh="睡眠过程中最高的呼吸频率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_MIN_RESPIRATORY_RATES = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailyMinRespiratoryRates",
        name_zh="最低呼吸率",
        description="Lowest respiratory rate during sleep",
        description_zh="睡眠过程中最低的呼吸频率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_BASE_RESPIRATORY_RATES = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailyBaseRespiratoryRates",
        name_zh="当次基准呼吸",
        description="Baseline respiratory rate for this sleep session",
        description_zh="本次睡眠的基准呼吸频率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_FAST_RESPIRATORY_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailyFastRespiratoryDuration",
        name_zh="呼吸过快时长",
        description="Duration of fast respiratory rate",
        description_zh="呼吸过快的总时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLOW_RESPIRATORY_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailySlowRespiratoryDuration",
        name_zh="呼吸过慢时长",
        description="Duration of slow respiratory rate",
        description_zh="呼吸过慢的总时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_HYPOPNEA_INDEX = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="count",
        name="dailyHypopneaIndex",
        name_zh="低通气次数",
        description="Number of hypopnea events during sleep",
        description_zh="睡眠中低通气的次数",
        data_type=HealthDataType.SUMMARY,
    )

    # Cardiovascular Indicators
    DAILY_SLEEP_BASE_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailySleepBaseHeartRate",
        name_zh="当次基准心率",
        description="Baseline heart rate for this sleep session",
        description_zh="本次睡眠的基准心率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_MAX_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailySleepMaxHeartRate",
        name_zh="最高心率",
        description="Maximum heart rate during sleep",
        description_zh="睡眠过程中最高的心率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_MIN_HEART_RATE = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="count/min",
        name="dailySleepMinHeartRate",
        name_zh="最低心率",
        description="Minimum heart rate during sleep",
        description_zh="睡眠过程中最低的心率",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_FAST_HEART_RATE_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailySleepFastHeartRateDuration",
        name_zh="心率过快时长",
        description="Duration of fast heart rate during sleep",
        description_zh="心率过快的总时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_SLOW_HEART_RATE_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailySleepSlowHeartRateDuration",
        name_zh="心率过慢时长",
        description="Duration of slow heart rate during sleep",
        description_zh="心率过慢的总时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_HEART_RATE_OVER_115PCT_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailySleepHeartRateOver115pctDuration",
        name_zh="心率超过基准115%时长",
        description="Duration when heart rate exceeds 115% of baseline",
        description_zh="心率超过基准1.15倍的时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_HEART_RATE_OVER_125PCT_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailySleepHeartRateOver125pctDuration",
        name_zh="心率超过基准125%时长",
        description="Duration when heart rate exceeds 125% of baseline",
        description_zh="心率超过基准1.25倍的时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_HEART_RATE_BELOW_75PCT_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailySleepHeartRateBelow75pctDuration",
        name_zh="心率低于基准75%时长",
        description="Duration when heart rate is below 75% of baseline",
        description_zh="心率低于基准75%的时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_HEART_RATE_BELOW_85PCT_DURATION = IndicatorInfo(
        category=Categories.VITAL_SIGNS.value,
        standard_unit="ms",
        name="dailySleepHeartRateBelow85pctDuration",
        name_zh="心率低于基准85%时长",
        description="Duration when heart rate is below 85% of baseline",
        description_zh="心率低于基准85%的时长",
        data_type=HealthDataType.SUMMARY,
    )

    # Heart Rate Variability Indicators
    SLEEP_LONG_TERM_BASE_SDNN = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepLongTermBaseSDNN",
        name_zh="长期SDNN",
        description="Long-term baseline SDNN for heart rate variability",
        description_zh="长期心率变异性SDNN基准值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_TOTAL_HEART_ENERGY = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms²",
        name="sleepTotalHeartEnergy",
        name_zh="心脏总能量",
        description="Total heart energy measurement",
        description_zh="心脏活动的总能量测量值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_BASE_TOTAL_HEART_ENERGY = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms²",
        name="sleepBaseTotalHeartEnergy",
        name_zh="心脏总能量基准值",
        description="Baseline reference for total heart energy",
        description_zh="心脏活动总能量的基准参考值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_VAGAL_TONE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms²",
        name="sleepVagalToneIndex",
        name_zh="迷走神经张力指数",
        description="Vagal tone index measurement",
        description_zh="迷走神经活动的实际测量值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_BASE_VAGAL_TONE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms²",
        name="sleepBaseVagalToneIndex",
        name_zh="迷走神经张力指数基准值",
        description="Baseline reference for vagal tone index",
        description_zh="迷走神经活动的基准参考值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_SYMPATHETIC_TONE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms²",
        name="sleepSympatheticToneIndex",
        name_zh="交感神经张力指数",
        description="Sympathetic tone index measurement",
        description_zh="交感神经活动的实际测量值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_BASE_SYMPATHETIC_TONE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms²",
        name="sleepBaseSympatheticToneIndex",
        name_zh="交感神经张力指数基准值",
        description="Baseline reference for sympathetic tone index",
        description_zh="交感神经活动的基准参考值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_AUTONOMIC_BALANCE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ratio",
        name="sleepAutonomicBalanceIndex",
        name_zh="自主神经平衡指数",
        description="Autonomic nervous system balance measurement",
        description_zh="自主神经系统平衡的实际测量值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_BASE_AUTONOMIC_BALANCE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ratio",
        name="sleepBaseAutonomicBalanceIndex",
        name_zh="自主神经平衡指数基准值",
        description="Baseline reference for autonomic balance index",
        description_zh="自主神经系统平衡的基准参考值。DC > 4.5 ms：迷走神经调节能力正常，猝死风险低。DC 在 2.6 - 4.5 ms：迷走神经调节能力下降，猝死风险中等。DC  2.5 ms：迷走神经调节能力显著下降，猝死风险高。",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_HEART_RATE_DECELERATION_CAPACITY = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepHeartRateDecelerationCapacity",
        name_zh="心率减速力",
        description="Heart rate deceleration capacity reflecting vagal nerve regulation ability",
        description_zh="心率变化的减速能力指标，反映了迷走神经对心率减速的调节能力",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_LONG_TERM_HEART_RATE_DECELERATION_CAPACITY = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepLongTermHeartRateDecelerationCapacity",
        name_zh="长期心率减速力",
        description="Long-term heart rate deceleration capacity",
        description_zh="长期心率变化的减速能力指标，反映了迷走神经对心率减速的调节能力",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_ARRHYTHMIA_RISK_INDEX = IndicatorInfo(
        category=Categories.MEDICAL.value,
        standard_unit="ms",
        name="sleepArrhythmiaRiskIndex",
        name_zh="心律失常风险指数",
        description="Arrhythmia risk index reflecting overall heart rhythm variability",
        description_zh="心律失常风险指数，反映了心脏跳动节律的总体变异性，可用于预测心律失常发生风险",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_TEMP_VASOMOTOR_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepTempVasomotorIndex",
        name_zh="体温及血管舒缩指数",
        description="Temperature and vasomotor index",
        description_zh="体温及血管舒缩指数",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_BASE_TEMP_VASOMOTOR_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepBaseTempVasomotorIndex",
        name_zh="体温及血管舒缩指数基准值",
        description="Baseline temperature and vasomotor index (30-day baseline)",
        description_zh="体温及血管舒缩指数基准值（近30天基线数据）",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_FEMALE_HORMONE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepFemaleHormoneIndex",
        name_zh="女性荷尔蒙指数",
        description="Female hormone index",
        description_zh="女性荷尔蒙指数",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_BASE_FEMALE_HORMONE_INDEX = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepBaseFemaleHormoneIndex",
        name_zh="女性荷尔蒙指数基准值",
        description="Baseline female hormone index (30-day baseline)",
        description_zh="女性荷尔蒙指数基准值（近30天基线数据）",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_HEART_RATE_RMSSD = IndicatorInfo(
        category=Categories.METABOLIC.value,
        standard_unit="ms",
        name="sleepHeartRateRMSSD",
        name_zh="RMSSD",
        description="Parasympathetic (vagal) regulation ability of the heart",
        description_zh="副交感神经（迷走神经）对心脏的调节能力",
        data_type=HealthDataType.SUMMARY,
    )

    # Sleep Quality Indicators
    DAILY_SLEEP_IN_BED_PILLOW_OFF_COUNT = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="count",
        name="dailySleepInBedPillowOffCount",
        name_zh="睡眠中离枕次数",
        description="Number of times leaving pillow during sleep",
        description_zh="睡眠过程中离开枕头的次数",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_TOTAL_PILLOW_OFF_COUNT = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="count",
        name="dailySleepTotalPillowOffCount",
        name_zh="离枕总次数",
        description="Total number of times leaving pillow",
        description_zh="离开枕头的总次数",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_BODY_MOVEMENT_COUNT = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="count",
        name="dailySleepBodyMovementCount",
        name_zh="体动次数",
        description="Number of body movements during sleep",
        description_zh="睡眠过程中身体活动的次数",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_AVG_BODY_MOVEMENT_DUR = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailySleepAvgBodyMovementDur",
        name_zh="体动平均时长",
        description="Average duration of body movements",
        description_zh="体动的平均时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_AWAKE_TIMES = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="count",
        name="dailyAwakeTimes",
        name_zh="清醒次数",
        description="Number of awakenings during sleep",
        description_zh="睡眠过程中清醒的次数",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_AWAKE_DURATION = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailyAwakeDuration",
        name_zh="清醒时长",
        description="Total duration of awake time",
        description_zh="清醒状态的总时长",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_AWAKE_DURATION_PERCENTAGE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="dailyAwakeDurationPercentage",
        name_zh="清醒比例",
        description="Percentage of awake time in total sleep",
        description_zh="清醒状态占总睡眠的比例",
        data_type=HealthDataType.SUMMARY,
    )

    # Time Indicators
    SLEEP_END_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepEndTime",
        name_zh="结束睡眠",
        description="Sleep end timestamp",
        description_zh="睡眠结束的时间戳",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_START_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepStartTime",
        name_zh="开始睡眠时间",
        description="Sleep start timestamp",
        description_zh="睡眠开始的时间戳",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_REPORT_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="sleepReportTime",
        name_zh="报告开始时间",
        description="Sleep report start timestamp",
        description_zh="睡眠报告开始的时间戳",
        data_type=HealthDataType.SUMMARY,
    )
    IN_BED_START_TIME = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="inBedStartTime",
        name_zh="上床起始时间",
        description="Time when getting into bed",
        description_zh="上床时间",
        data_type=HealthDataType.SUMMARY,
    )
    END_SLEEP_REPORT_TIME_OFFSET = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="endSleepReportTimeOffset",
        name_zh="睡眠结束偏移",
        description="Sleep end time offset from report start",
        description_zh="睡眠结束时间相对于报告开始的偏移",
        data_type=HealthDataType.SUMMARY,
    )
    START_SLEEP_REPORT_TIME_OFFSET = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="startSleepReportTimeOffset",
        name_zh="睡眠开始偏移",
        description="Sleep start time offset from report start",
        description_zh="睡眠开始时间相对于报告开始的偏移",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_IN_BED_DURATION = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="ms",
        name="dailyInBedDuration",
        name_zh="在床时间",
        description="Total time in bed",
        description_zh="在床的总时长",
        data_type=HealthDataType.SUMMARY,
    )

    # Disease Related Indicators
    HEALTH_CORONARY_HEART_DISEASE = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="bool",
        name="healthCoronaryHeartDisease",
        name_zh="冠心病",
        description="Whether user has coronary heart disease",
        description_zh="用户是否患有冠心病",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_HYPERTENSION = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="bool",
        name="healthHypertension",
        name_zh="高血压",
        description="Whether user has hypertension",
        description_zh="用户是否患有高血压",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_DIABETES_MELLITUS = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="bool",
        name="healthDiabetesMellitus",
        name_zh="糖尿病",
        description="Whether user has diabetes mellitus",
        description_zh="用户是否患有糖尿病",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_DISEASE_COUNT = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="count",
        name="healthDiseaseCount",
        name_zh="疾病数量",
        description="Number of diseases user has",
        description_zh="用户患有疾病的数量",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_HYPERTENSION_CONTROL = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthHypertensionControl",
        name_zh="高血压管控",
        description="Hypertension control level assessment",
        description_zh="高血压的控制水平评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_BLOOD_PRESSURE_REGULATION = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthBloodPressureRegulation",
        name_zh="血压调节机能",
        description="Blood pressure regulation ability assessment",
        description_zh="血压调节能力的评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_CORONARY_DISEASE_CONTROL = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthCoronaryDiseaseControl",
        name_zh="冠心病管控",
        description="Coronary disease control level assessment",
        description_zh="冠心病的控制水平评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_CORONARY_ARTERY_FUNCTION = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthCoronaryArteryFunction",
        name_zh="冠状动脉机能",
        description="Coronary artery function assessment",
        description_zh="冠状动脉功能的评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_DIABETES_CONTROL = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthDiabetesControl",
        name_zh="糖尿病管控",
        description="Diabetes control level assessment",
        description_zh="糖尿病的控制水平评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_BLOOD_SUGAR_BALANCE = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthBloodSugarBalance",
        name_zh="血糖平衡机能",
        description="Blood sugar regulation ability assessment",
        description_zh="血糖调节能力的评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_CHRONIC_DISEASE_INDEX = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthChronicDiseaseIndex",
        name_zh="慢病防控指数",
        description="Chronic disease prevention and control ability assessment",
        description_zh="慢性病防控能力的评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_INFECTION_RISK = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthInfectionRisk",
        name_zh="感染风险",
        description="Infection risk assessment during sleep",
        description_zh="睡眠过程中感染风险评估",
        data_type=HealthDataType.SUMMARY,
    )
    HEALTH_IMMUNE_BALANCE = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthImmuneBalance",
        name_zh="免疫平衡",
        description="Immune system balance level",
        description_zh="免疫系统的平衡程度",
        data_type=HealthDataType.SUMMARY,
    )

    # Mental and Emotional Indicators
    DAILY_SLEEP_ANXIETY_LEVEL = IndicatorInfo(
        category=Categories.MENTAL.value,
        standard_unit="%",
        name="dailySleepAnxietyLevel",
        name_zh="焦虑水平",
        description="Anxiety level assessment during sleep",
        description_zh="睡眠过程中的焦虑程度评估",
        data_type=HealthDataType.SUMMARY,
    )
    DAILY_SLEEP_EMOTIONAL_STRESS = IndicatorInfo(
        category=Categories.MENTAL.value,
        standard_unit="%",
        name="dailySleepEmotionalStress",
        name_zh="情绪应激",
        description="Emotional stress level during sleep",
        description_zh="睡眠过程中情绪应激水平",
        data_type=HealthDataType.SUMMARY,
    )
    MENTAL_EMOTIONAL_STATE = IndicatorInfo(
        category=Categories.MENTAL.value,
        standard_unit="%",
        name="mentalEmotionalState",
        name_zh="情绪心理",
        description="Emotional and mental state assessment",
        description_zh="情绪和心理状态的评估",
        data_type=HealthDataType.SUMMARY,
    )
    MENTAL_STRESS_RESISTANCE = IndicatorInfo(
        category=Categories.MENTAL.value,
        standard_unit="%",
        name="mentalStressResistance",
        name_zh="抗压能力",
        description="Stress resistance ability assessment",
        description_zh="用户的抗压能力评估",
        data_type=HealthDataType.SUMMARY,
    )

    # Other Indicators
    HEALTH_OVERALL_SCORE = IndicatorInfo(
        category=Categories.HEALTH.value,
        standard_unit="%",
        name="healthOverallScore",
        name_zh="健康值",
        description="Overall health status assessment score",
        description_zh="整体健康状况的评估值",
        data_type=HealthDataType.SUMMARY,
    )
    SLEEP_OVERALL_SCORE = IndicatorInfo(
        category=Categories.SLEEP.value,
        standard_unit="%",
        name="sleepOverallScore",
        name_zh="睡眠值",
        description="Overall sleep quality assessment score",
        description_zh="整体睡眠质量的评估值",
        data_type=HealthDataType.SUMMARY,
    )
    FEMALE_CHARM_SCORE = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="%",
        name="femaleCharmScore",
        name_zh="魅力值",
        description="Female user overall report score",
        description_zh="女性用户报告总评分",
        data_type=HealthDataType.SUMMARY,
    )
    FEMALE_MOISTURE_INDEX = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="ratio",
        name="femaleMoistureIndex",
        name_zh="水润指数",
        description="Female user moisture index",
        description_zh="女性用户水润指数",
        data_type=HealthDataType.SUMMARY,
    )
    FEMALE_WEIGHT_GAIN_INDEX = IndicatorInfo(
        category=Categories.LIFESTYLE.value,
        standard_unit="ratio",
        name="femaleWeightGainIndex",
        name_zh="易胖指数",
        description="Female user weight gain tendency index",
        description_zh="女性用户易胖指数",
        data_type=HealthDataType.SUMMARY,
    )

    # Demographics Indicators
    FEMALE_MENSTRUAL_CYCLE_PHASE = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="enum",
        name="femaleMenstrualCyclePhase",
        name_zh="当前月经周期",
        description="Current menstrual cycle phase",
        description_zh="1月经期、2卵泡期、3排卵期、4黄体期",
        data_type=HealthDataType.SUMMARY,
    )
    FEMALE_IS_PREDICTED_OVULATION_DAY = IndicatorInfo(
        category=Categories.BODY_COMPOSITION.value,
        standard_unit="bool",
        name="femaleIsPredictedOvulationDay",
        name_zh="是否为预测排卵日",
        description="Whether it is predicted ovulation day",
        description_zh="是否为预测排卵日",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_BASAL_BODY_TEMPERATURE = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="°C",
        name="reproductiveBasalBodyTemperature",
        name_zh="基础体温",
        description="Basal body temperature",
        description_zh="基础体温",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_CERVICAL_MUCUS_QUALITY = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveCervicalMucusQuality",
        name_zh="宫颈粘液质量",
        description="Cervical mucus quality",
        description_zh="1=dry, 2=sticky, 3=creamy, 4=water, 5=eggWhite",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_CONTRACEPTIVE = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveContraceptive",
        name_zh="避孕方式",
        description="Contraceptive method",
        description_zh="1=unspecified, 2=implant, 3=injection, 4=intrauterineDevice, 5=intravaginalRing, 6=oral, 7=patch",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_INFREQUENT_MENSTRUAL_CYCLES = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveInfrequentMenstrualCycles",
        name_zh="月经稀发",
        description="Infrequent menstrual cycles",
        description_zh="月经稀发",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_INTERMENTSTRUAL_BLEEDING = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveIntermenstrualBleeding",
        name_zh="月经间出血",
        description="Intermenstrual bleeding",
        description_zh="月经间出血, 0=有",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_IRREGULAR_MENSTRUAL_CYCLES = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveIrregularMenstrualCycles",
        name_zh="月经不规律",
        description="Irregular menstrual cycles",
        description_zh="月经不规律",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_LACTATION = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveLactation",
        name_zh="哺乳",
        description="Lactation",
        description_zh="哺乳, 0=有",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_MENSTRUATION_FLOW = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveMenstruationFlow",
        name_zh="月经流量",
        description="Menstruation flow",
        description_zh="月经流量, 1=unspecified, 2=light, 3=medium, 4=heavy, 5=none",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_OVULATION_TEST_RESULT = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveOvulationTestResult",
        name_zh="排卵试纸结果",
        description="Ovulation test result",
        description_zh="排卵试纸结果, 1=negative, 2=positive, 3=indeterminate, 4=estrogenSurge",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_PERSISTENT_INTERMENSTRUCAL_BLEEDING = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductivePersistentIntermenstrualBleeding",
        name_zh="持续月经间出血",
        description="Persistent intermenstrual bleeding",
        description_zh="持续月经间出血",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_PREGNANCY = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductivePregnancy",
        name_zh="怀孕",
        description="Pregnancy",
        description_zh="怀孕, 0=有",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_PREGNANCY_TEST_RESULT = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductivePregnancyTestResult",
        name_zh="怀孕试纸结果",
        description="Pregnancy test result",
        description_zh="怀孕试纸结果, 1=negative, 2=positive, 3=indeterminate",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_PROGESTERONE_TEST_RESULT = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveProgEstrogenTestResult",
        name_zh="孕酮试纸结果",
        description="Progesterone test result",
        description_zh="孕酮试纸结果, 1=negative, 2=positive, 3=indeterminate",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_PROLONGED_MENSTRUAL_PERIODS = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveProlongedMenstrualPeriods",
        name_zh="月经周期延长",
        description="Prolonged menstrual periods",
        description_zh="月经周期延长",
        data_type=HealthDataType.SUMMARY,
    )
    REPRODUCTIVE_SEXUAL_ACTIVITY = IndicatorInfo(
        category=Categories.REPRODUCTIVE.value,
        standard_unit="enum",
        name="reproductiveSexualActivity",
        name_zh="性活动",
        description="Sexual activity",
        description_zh="性活动",
        data_type=HealthDataType.SUMMARY,
    )

    @property
    def identifier(self) -> str:
        """Return the string identifier for backward compatibility."""
        return self.value.name


# ============================================================================
# BACKWARD COMPATIBILITY
# ============================================================================

# ============================================================================
# UTILITY VARIABLES AND FUNCTIONS
# ============================================================================

# Valid indicators set for fast lookup (unique names only for backward compatibility)
VALID_INDICATORS: Set[str] = {indicator.identifier for indicator in StandardIndicator}

# Create a dictionary for efficient lookup
_INDICATOR_LOOKUP = {
    indicator.value.name: indicator.value for indicator in StandardIndicator
}


def is_summary_indicator(indicator: str) -> bool:
    """
    Check if an indicator is a summary indicator
    
    Summary indicators are those that contain daily, weekly, or hourly keywords,
    or have data_type=HealthDataType.SUMMARY in their definition
    """
    if not indicator:
        return False

    # First check if it's a defined standard indicator
    std_indicator = get_indicator_by_str(indicator)
    if std_indicator is not None:
        return std_indicator.value.data_type == HealthDataType.SUMMARY or std_indicator.value.data_type == HealthDataType.MIX

    return False

def is_series_indicator(indicator: str) -> bool:
    """
    Check if an indicator is a series indicator

    Summary indicators are those that contain daily, weekly, or hourly keywords,
    or have data_type=HealthDataType.SUMMARY in their definition
    """
    if not indicator:
        return True

    std_indicator = get_indicator_by_str(indicator)
    if std_indicator is not None:
        return std_indicator.value.data_type == HealthDataType.SERIES or std_indicator.value.data_type == HealthDataType.MIX

    return False


def is_valid_indicator(indicator: str) -> bool:
    """Check if indicator is a valid standard indicator"""
    return indicator in VALID_INDICATORS


def get_standard_unit(indicator: str) -> str:
    """Get standard unit for indicator"""
    info = _INDICATOR_LOOKUP.get(indicator)
    if info:
        return info.standard_unit
    raise ValueError(f"Unknown indicator: {indicator}")


def get_indicator_by_str(indicator: str) -> Optional['StandardIndicator']:
    """
    Get StandardIndicator enum member by string identifier

    Args:
        indicator: The indicator string to search for

    Returns:
        StandardIndicator enum member if found, None otherwise
    """
    if not indicator:
        return None

    for std_indicator in StandardIndicator:
        if std_indicator.value.name == indicator:
            return std_indicator
    logging.warning(f"indicator {indicator} not found in StandardIndicator")
    return None


def get_all_indicators_info() -> Dict[str, Any]:
    """
    Get all indicator information (for frontend display)
    
    Returns:
        Dictionary containing all indicator information
    """
    # Build result structure matching original indicators.py format
    result = {
        "categories": {},
        "indicators": {},
        "total_indicators": 0,
        "generated_at": datetime.now().isoformat(),
    }

    # Group indicators by category
    category_indicators = {}
    for indicator in StandardIndicator:
        category_key = indicator.value.category.name.lower()
        if category_key not in category_indicators:
            category_indicators[category_key] = {
                "category_info": indicator.value.category,
                "indicators": []
            }
        category_indicators[category_key]["indicators"].append(indicator)

    # Process categories data
    for category_key, data in category_indicators.items():
        category_info = data["category_info"]
        indicators_in_category = data["indicators"]

        # Deduplication: use set to avoid duplicate calculations
        unique_indicators_in_category = []
        seen_names = set()

        for indicator in indicators_in_category:
            indicator_name = indicator.value.name
            if indicator_name not in seen_names:
                seen_names.add(indicator_name)
                unique_indicators_in_category.append(indicator)

        result["categories"][category_key] = {
            "name": category_info.name_zh,
            "name_en": category_info.name,
            "count": len(unique_indicators_in_category),  # Use deduplicated count
            "indicators": [],
        }

        for indicator in unique_indicators_in_category:
            indicator_data = {
                "key": indicator.value.name,
                "name": indicator.value.name_zh,
                "name_en": indicator.value.name,
                "description": indicator.value.description_zh,
                "standard_unit": indicator.value.standard_unit,
                "supported_units": [indicator.value.standard_unit],
                "category": category_key,
            }

            result["categories"][category_key]["indicators"].append(indicator_data)
            result["indicators"][indicator.value.name] = indicator_data

    # Update total indicators count
    result["total_indicators"] = len(result["indicators"])

    return result
