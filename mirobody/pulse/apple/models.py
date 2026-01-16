"""
Apple Health
Health data type enums and mappings
Common enums shared across data server modules
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator

# Import standard enums from core module
from ..core import StandardIndicator

class FlutterHealthTypeEnum(str, Enum):
    """Flutter Health Types - strict type constraints for Apple Health data"""

    HEART_RATE = "HEART_RATE"
    RESPIRATORY_RATE = "RESPIRATORY_RATE"
    BODY_TEMPERATURE = "BODY_TEMPERATURE"
    BLOOD_GLUCOSE = "BLOOD_GLUCOSE"
    BLOOD_OXYGEN = "BLOOD_OXYGEN"
    BLOOD_PRESSURE_SYSTOLIC = "BLOOD_PRESSURE_SYSTOLIC"
    BLOOD_PRESSURE_DIASTOLIC = "BLOOD_PRESSURE_DIASTOLIC"
    WALKING_HEART_RATE = "WALKING_HEART_RATE"
    RESTING_HEART_RATE = "RESTING_HEART_RATE"
    HEART_RATE_VARIABILITY_SDNN = "HEART_RATE_VARIABILITY_SDNN"
    STEPS = "STEPS"
    CYCLING_SPEED = "CYCLING_SPEED"
    WALKING_SPEED = "WALKING_SPEED"
    FLIGHTS_CLIMBED = "FLIGHTS_CLIMBED"
    DISTANCE_WALKING_RUNNING = "DISTANCE_WALKING_RUNNING"
    EXERCISE_TIME = "EXERCISE_TIME"
    DISTANCE_CYCLING = "DISTANCE_CYCLING"
    VO2_MAX = "VO2_MAX"
    HEART_RATE_RECOVERY_ONE_MINUTE = "HEART_RATE_RECOVERY_ONE_MINUTE"
    HEIGHT = "HEIGHT"
    WEIGHT = "WEIGHT"
    BODY_FAT_PERCENTAGE = "BODY_FAT_PERCENTAGE"
    BODY_MASS_INDEX = "BODY_MASS_INDEX"
    WAIST_CIRCUMFERENCE = "WAIST_CIRCUMFERENCE"
    SLEEPING_WRIST_TEMPERATURE = "SLEEPING_WRIST_TEMPERATURE"
    SLEEP_ASLEEP = "SLEEP_ASLEEP"
    SLEEP_AWAKE = "SLEEP_AWAKE"
    SLEEP_DEEP = "SLEEP_DEEP"
    SLEEP_IN_BED = "SLEEP_IN_BED"
    SLEEP_LIGHT = "SLEEP_LIGHT"
    SLEEP_REM = "SLEEP_REM"
    DIETARY_PROTEIN_CONSUMED = "DIETARY_PROTEIN_CONSUMED"
    DIETARY_CARBS_CONSUMED = "DIETARY_CARBS_CONSUMED"
    DIETARY_FATS_CONSUMED = "DIETARY_FATS_CONSUMED"
    DIETARY_ENERGY_CONSUMED = "DIETARY_ENERGY_CONSUMED"
    DIETARY_WATER = "DIETARY_WATER"
    UV_EXPOSURE = "UV_EXPOSURE"

    # Compatible with renpho body scale
    BASAL_METABOLIC_RATE = "BASAL_METABOLIC_RATE"
    BASAL_ENERGY_BURNED = "BASAL_ENERGY_BURNED"
    BODY_WATER = "BODY_WATER"
    BODY_AGE = "BODY_AGE"
    BODY_MUSCLE = "BODY_MUSCLE"
    BODY_BONE = "BODY_BONE"
    BODY_SUB_FAT = "BODY_SUB_FAT"
    BODY_VIS_FAT = "BODY_VIS_FAT"
    BODY_FAT_FREE_WEIGHT = "BODY_FAT_FREE_WEIGHT"
    BODY_SINEW = "BODY_SINEW"
    BODY_PROTEIN = "BODY_PROTEIN"

    BASAL_BODY_TEMPERATURE = "BASAL_BODY_TEMPERATURE"
    CERVICAL_MUCUS_QUALITY = "CERVICAL_MUCUS_QUALITY"
    CONTRACEPTIVE = "CONTRACEPTIVE"
    INFREQUENT_MENSTRUAL_CYCLES = "INFREQUENT_MENSTRUAL_CYCLES"
    INTERMENTSTRUAL_BLEEDING = "INTERMENTSTRUAL_BLEEDING"
    IRREGULAR_MENSTRUAL_CYCLES = "IRREGULAR_MENSTRUAL_CYCLES"
    LACTATION = "LACTATION"
    MENSTRUATION_FLOW = "MENSTRUATION_FLOW"
    OVULATION_TEST_RESULT = "OVULATION_TEST_RESULT"
    PERSISTENT_INTERMENSTRUAL_BLEEDING = "PERSISTENT_INTERMENSTRUAL_BLEEDING"
    PREGNANCY = "PREGNANCY"
    PREGNANCY_TEST_RESULT = "PREGNANCY_TEST_RESULT"
    PROGESTERONE_TEST_RESULT = "PROGESTERONE_TEST_RESULT"
    PROLONGED_MENSTRUAL_PERIODS = "PROLONGED_MENSTRUAL_PERIODS"
    SEXUAL_ACTIVITY = "SEXUAL_ACTIVITY"


FLUTTER_TO_RECORD_TYPE_MAPPING = {
    # Vital signs
    FlutterHealthTypeEnum.HEART_RATE: StandardIndicator.HEART_RATE.value.name,
    FlutterHealthTypeEnum.RESPIRATORY_RATE: StandardIndicator.RESPIRATORY_RATE.value.name,
    FlutterHealthTypeEnum.BODY_TEMPERATURE: StandardIndicator.BODY_TEMPERATURE.value.name,
    FlutterHealthTypeEnum.BLOOD_GLUCOSE: StandardIndicator.BLOOD_GLUCOSE.value.name,
    FlutterHealthTypeEnum.BLOOD_OXYGEN: StandardIndicator.BLOOD_OXYGEN.value.name,
    FlutterHealthTypeEnum.BLOOD_PRESSURE_SYSTOLIC: StandardIndicator.BLOOD_PRESSURE_SYSTOLIC.value.name,
    FlutterHealthTypeEnum.BLOOD_PRESSURE_DIASTOLIC: StandardIndicator.BLOOD_PRESSURE_DIASTOLIC.value.name,
    FlutterHealthTypeEnum.WALKING_HEART_RATE: StandardIndicator.WALKING_HEART_RATE.value.name,
    FlutterHealthTypeEnum.RESTING_HEART_RATE: StandardIndicator.RESTING_HEART_RATE.value.name,
    FlutterHealthTypeEnum.HEART_RATE_VARIABILITY_SDNN: StandardIndicator.HRV.value.name,
    # Activity and fitness
    FlutterHealthTypeEnum.STEPS: StandardIndicator.STEPS.value.name,
    FlutterHealthTypeEnum.CYCLING_SPEED: StandardIndicator.CYCLING_SPEED.value.name,
    FlutterHealthTypeEnum.WALKING_SPEED: StandardIndicator.WALKING_SPEED.value.name,
    FlutterHealthTypeEnum.FLIGHTS_CLIMBED: StandardIndicator.FLOORS_CLIMBED.value.name,
    FlutterHealthTypeEnum.DISTANCE_WALKING_RUNNING: StandardIndicator.DISTANCE.value.name,
    FlutterHealthTypeEnum.EXERCISE_TIME: StandardIndicator.ACTIVE_TIME.value.name,
    FlutterHealthTypeEnum.DISTANCE_CYCLING: StandardIndicator.CYCLING_DISTANCE.value.name,
    FlutterHealthTypeEnum.VO2_MAX: StandardIndicator.VO2_MAX.value.name,
    FlutterHealthTypeEnum.HEART_RATE_RECOVERY_ONE_MINUTE: StandardIndicator.RECOVERY_TIME.value.name,
    # Body measurements
    FlutterHealthTypeEnum.HEIGHT: StandardIndicator.HEIGHT.value.name,
    FlutterHealthTypeEnum.WEIGHT: StandardIndicator.WEIGHT.value.name,
    FlutterHealthTypeEnum.BODY_FAT_PERCENTAGE: StandardIndicator.BODY_FAT_PERCENTAGE.value.name,
    FlutterHealthTypeEnum.BODY_MASS_INDEX: StandardIndicator.BMI.value.name,
    FlutterHealthTypeEnum.WAIST_CIRCUMFERENCE: StandardIndicator.WAIST_CIRCUMFERENCE.value.name,
    FlutterHealthTypeEnum.SLEEPING_WRIST_TEMPERATURE: StandardIndicator.WRIST_TEMPERATURE.value.name,
    # Nutrition
    FlutterHealthTypeEnum.DIETARY_PROTEIN_CONSUMED: StandardIndicator.DIETARY_PROTEIN.value.name,
    FlutterHealthTypeEnum.DIETARY_CARBS_CONSUMED: StandardIndicator.DIETARY_CARBS.value.name,
    FlutterHealthTypeEnum.DIETARY_FATS_CONSUMED: StandardIndicator.DIETARY_FATS.value.name,
    FlutterHealthTypeEnum.DIETARY_ENERGY_CONSUMED: StandardIndicator.DIETARY_ENERGY.value.name,
    FlutterHealthTypeEnum.DIETARY_WATER: StandardIndicator.DIETARY_WATER.value.name,
    # Sleep types
    FlutterHealthTypeEnum.SLEEP_IN_BED: StandardIndicator.SLEEP_IN_BED.value.name,
    FlutterHealthTypeEnum.SLEEP_ASLEEP: StandardIndicator.SLEEP_ANALYSIS_ASLEEP_UNSPECIFIED.value.name,
    FlutterHealthTypeEnum.SLEEP_AWAKE: StandardIndicator.SLEEP_AWAKE.value.name,
    FlutterHealthTypeEnum.SLEEP_DEEP: StandardIndicator.SLEEP_ASLEEP_DEEP.value.name,
    FlutterHealthTypeEnum.SLEEP_LIGHT: StandardIndicator.SLEEP_ASLEEP_CORE.value.name,
    FlutterHealthTypeEnum.SLEEP_REM: StandardIndicator.SLEEP_ASLEEP_REM.value.name,
    # UV exposure
    FlutterHealthTypeEnum.UV_EXPOSURE: StandardIndicator.UV_EXPOSURE.value.name,
    # Renpho body scale related
    FlutterHealthTypeEnum.BASAL_METABOLIC_RATE: StandardIndicator.BMR.value.name,
    FlutterHealthTypeEnum.BASAL_ENERGY_BURNED: StandardIndicator.CALORIES_BASAL.value.name,
    FlutterHealthTypeEnum.BODY_WATER: StandardIndicator.BODY_WATER_PERCENTAGE.value.name,
    FlutterHealthTypeEnum.BODY_AGE: StandardIndicator.BODY_AGE.value.name,
    FlutterHealthTypeEnum.BODY_MUSCLE: StandardIndicator.MUSCLE_PERCENTAGE.value.name,
    FlutterHealthTypeEnum.BODY_BONE: StandardIndicator.BONE_MASS.value.name,
    FlutterHealthTypeEnum.BODY_SUB_FAT: StandardIndicator.SUBCUTANEOUS_FAT.value.name,
    FlutterHealthTypeEnum.BODY_VIS_FAT: StandardIndicator.VISCERAL_FAT.value.name,
    FlutterHealthTypeEnum.BODY_FAT_FREE_WEIGHT: StandardIndicator.FAT_FREE_WEIGHT.value.name,
    FlutterHealthTypeEnum.BODY_SINEW: StandardIndicator.BODY_SINEW.value.name,
    FlutterHealthTypeEnum.BODY_PROTEIN: StandardIndicator.PROTEIN_PERCENTAGE.value.name,
    # Reproductive health
    FlutterHealthTypeEnum.BASAL_BODY_TEMPERATURE: StandardIndicator.REPRODUCTIVE_BASAL_BODY_TEMPERATURE.value.name,
    FlutterHealthTypeEnum.CERVICAL_MUCUS_QUALITY: StandardIndicator.REPRODUCTIVE_CERVICAL_MUCUS_QUALITY.value.name,
    FlutterHealthTypeEnum.CONTRACEPTIVE: StandardIndicator.REPRODUCTIVE_CONTRACEPTIVE.value.name,
    # FlutterHealthTypeEnum.INFREQUENT_MENSTRUAL_CYCLES: StandardIndicator.REPRODUCTIVE_INFREQUENT_MENSTRUAL_CYCLES.value.name,
    FlutterHealthTypeEnum.INTERMENTSTRUAL_BLEEDING: StandardIndicator.REPRODUCTIVE_INTERMENTSTRUAL_BLEEDING.value.name,
    # FlutterHealthTypeEnum.IRREGULAR_MENSTRUAL_CYCLES: StandardIndicator.REPRODUCTIVE_IRREGULAR_MENSTRUAL_CYCLES.value.name,
    FlutterHealthTypeEnum.LACTATION: StandardIndicator.REPRODUCTIVE_LACTATION.value.name,
    FlutterHealthTypeEnum.MENSTRUATION_FLOW: StandardIndicator.REPRODUCTIVE_MENSTRUATION_FLOW.value.name,
    FlutterHealthTypeEnum.OVULATION_TEST_RESULT: StandardIndicator.REPRODUCTIVE_OVULATION_TEST_RESULT.value.name,
    # FlutterHealthTypeEnum.PERSISTENT_INTERMENSTRUAL_BLEEDING: StandardIndicator.REPRODUCTIVE_INTERMENTSTRUAL_BLEEDING.value.name,
    FlutterHealthTypeEnum.PREGNANCY: StandardIndicator.REPRODUCTIVE_PREGNANCY.value.name,
    FlutterHealthTypeEnum.PREGNANCY_TEST_RESULT: StandardIndicator.REPRODUCTIVE_PREGNANCY_TEST_RESULT.value.name,
    FlutterHealthTypeEnum.PROGESTERONE_TEST_RESULT: StandardIndicator.REPRODUCTIVE_PROGESTERONE_TEST_RESULT.value.name,
    # FlutterHealthTypeEnum.PROLONGED_MENSTRUAL_PERIODS: StandardIndicator.REPRODUCTIVE_PROLONGED_MENSTRUAL_PERIODS.value.name,
    FlutterHealthTypeEnum.SEXUAL_ACTIVITY: StandardIndicator.REPRODUCTIVE_SEXUAL_ACTIVITY.value.name,
}


class MetaInfo(BaseModel):
    """Request metadata"""

    # userId: str = Field(..., description="User ID")
    timezone: str = Field(default="UTC", description="timezone")
    taskId: Optional[str] = Field(None, description="task id")
    directly_from_watch: Optional[bool] = Field(False, description="Whether the data is directly from watch")


class AppleHealthRecord(BaseModel):
    """Apple Health record"""

    uuid: str = Field(..., description="Unique record identifier")
    sourceId: Optional[str] = Field(None, description="Data source ID")
    sourceName: Optional[str] = Field(None, description="Data source name")
    sourcePlatform: Optional[str] = Field(None, description="Data source platform")
    sourceDeviceId: Optional[str] = Field(None, description="Device ID")
    type: Union[FlutterHealthTypeEnum, str] = Field(..., description="Data type")
    dateFrom: Optional[int] = Field(None, description="Start timestamp (milliseconds)")
    dateTo: Optional[int] = Field(None, description="End timestamp (milliseconds)")
    timezone: str = Field(default="UTC", description="Timezone")
    value: Dict[str, Any] = Field(default_factory=dict, description="Numeric data")
    unit: Optional[str] = Field(None, description="Unit")
    unitSymbol: Optional[str] = Field(None, description="Unit symbol")
    recordingMethod: Optional[str] = Field(None, description="Recording method")
    createdAt: Optional[int] = Field(None, description="Creation timestamp (milliseconds)")

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: Union[FlutterHealthTypeEnum, str]) -> str:
        """Validate health data type, lenient mode: accept all types"""
        if isinstance(v, FlutterHealthTypeEnum):
            return v.value
        elif isinstance(v, str):
            # Lenient mode: silently accept all string types
            return v
        else:
            raise ValueError(f"Invalid type format: {v}")
    
    def is_known_type(self) -> bool:
        """Check if it's a known health data type"""
        try:
            FlutterHealthTypeEnum(self.type)
            return True
        except ValueError:
            return False


class AppleHealthRequest(BaseModel):
    """Apple Health data request"""

    request_id: Optional[str] = Field(None, description="Request ID")
    metaInfo: MetaInfo = Field(..., description="Metadata information")
    healthData: List[AppleHealthRecord] = Field(..., description="Health data records")
