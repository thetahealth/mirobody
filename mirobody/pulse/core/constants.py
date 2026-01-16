"""
Core constants and enumerations module

Defines constants, enumerations and configurations shared by all Platforms and Providers
"""

from enum import Enum
from typing import Dict, Set


class LinkType(str, Enum):
    """Connection type enumeration"""

    OAUTH1 = "oauth1"  # OAuth 1.0a (like Garmin)
    OAUTH2 = "oauth2"  # OAuth 2.0 (like Whoop)
    OAUTH = "oauth"   # Keep for backward compatibility, defaults to OAuth2
    PASSWORD = "password"
    TOKEN = "token"
    API_KEY = "api_key"
    LINK_TOKEN = "link_token"
    EMAIL = "email"
    SERVICE = "service"
    PLATFORM = "platform"  # Platform-level virtual provider
    CUSTOMIZED = "customized"  # Customized connection with dynamic fields (e.g., database connections)
    NONE = "none"


class ProviderStatus(str, Enum):
    """Provider status enumeration"""

    AVAILABLE = "available"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECT = "reconnect"  # Need to reconnect (reconnect=1 in database)
    ERROR = "error"
    MAINTENANCE = "maintenance"


class DataType(str, Enum):
    """Health data type enumeration"""

    # Summary types
    ACTIVITY = "activity"
    BODY = "body"
    MEAL = "meal"
    MENSTRUAL_CYCLE = "menstrual_cycle"
    PROFILE = "profile"
    SLEEP = "sleep"
    SLEEP_CYCLE = "sleep_cycle"
    WORKOUT = "workouts"
    WORKOUT_STREAM = "workout_stream"

    # Timeseries types
    CALORIES_ACTIVE = "calories_active"
    CALORIES_BASAL = "calories_basal"
    DISTANCE = "distance"
    FLOORS_CLIMBED = "floors_climbed"
    STEPS = "steps"
    VO2_MAX = "vo2_max"
    HEART_RATE = "heartrate"
    HRV = "hrv"
    RESPIRATORY_RATE = "respiratory_rate"
    BLOOD_PRESSURE = "blood_pressure"
    BLOOD_OXYGEN = "blood_oxygen"
    GLUCOSE = "glucose"
    WEIGHT = "weight"
    FAT = "fat"
    BODY_TEMPERATURE = "body_temperature"
    WATER = "water"
    STRESS_LEVEL = "stress_level"


class ResourceType(str, Enum):
    """Resource type enumeration (Vital compatible)"""

    # Summary types
    ACTIVITY = "activity"
    BODY = "body"
    MEAL = "meal"
    MENSTRUAL_CYCLE = "menstrual_cycle"
    PROFILE = "profile"
    SLEEP = "sleep"
    SLEEP_CYCLE = "sleep_cycle"
    WORKOUT = "workouts"
    WORKOUT_STREAM = "workout_stream"

    # Activity Timeseries
    CALORIES_ACTIVE = "calories_active"
    CALORIES_BASAL = "calories_basal"
    DISTANCE = "distance"
    FLOORS_CLIMBED = "floors_climbed"
    STEPS = "steps"
    VO2_MAX = "vo2_max"
    WORKOUT_DURATION = "workout_duration"

    # Vitals Timeseries
    BLOOD_PRESSURE = "blood_pressure"
    BLOOD_OXYGEN = "blood_oxygen"
    CHOLESTEROL = "cholesterol"
    ELECTROCARDIOGRAM_VOLTAGE = "electrocardiogram_voltage"
    GLUCOSE = "glucose"
    IGE = "ige"
    IGG = "igg"
    INSULIN_INJECTION = "insulin_injection"
    HEART_RATE = "heartrate"  # Note: Vital uses heartrate
    HRV = "hrv"
    RESPIRATORY_RATE = "respiratory_rate"

    # Body Timeseries
    FAT = "fat"
    WEIGHT = "weight"
    BODY_TEMPERATURE = "body_temperature"
    BODY_TEMPERATURE_DELTA = "body_temperature_delta"

    # Nutrition Timeseries
    CAFFEINE = "caffeine"
    CARBOHYDRATES = "carbohydrates"
    WATER = "water"

    # Wellness Timeseries
    MINDFULNESS_MINUTES = "mindfulness_minutes"
    STRESS_LEVEL = "stress_level"

    # Other
    ELECTROCARDIOGRAM = "electrocardiogram"
    UNKNOWN = ""


class DocType(int, Enum):
    """Health document type enumeration"""

    VITAL = 1  # Vital platform data
    APPLE_HEALTH = 2  # Apple Health data
    MEDICAL_REPORT = 3  # Medical report
    LAB_RESULT = 4  # Laboratory test result
    DEVICE_DATA = 5  # Device data
    MANUAL_INPUT = 6  # Manual input


class DocStatus(int, Enum):
    """Health document status enumeration"""

    BE_DB = 0  # Stored in database
    BE_AI = 1  # AI processed
    DONE = 2  # Processing completed
    ERROR = 3  # Processing error


class ProcessAction(int, Enum):
    """Processing action enumeration"""

    NONE = 0
    WEBHOOK = 1
    HISTORY = 2
    API = 3
    LABTEST = 5
    PROVIDER = 6


class TokenConfig:
    """Token Configuration"""
    
    # Default expiration time for Vital Link Token (5 minutes)
    VITAL_LINK_TOKEN_TTL = 5 * 60
    
    # Default expiration time for JWT Token (30 days)
    JWT_TOKEN_TTL = 30 * 24 * 60 * 60
    
    # Default expiration time for Client Token (10 minutes)
    CLIENT_TOKEN_TTL = 10 * 60


class CacheConfig:
    """Cache configuration"""

    DEFAULT_TTL = 24 * 60 * 60  # 24 hours
    PROVIDER_CACHE_TTL = 24 * 60 * 60  # Provider cache 24 hours
    USER_CACHE_TTL = 5 * 60  # User info cache 5 minutes


class CommonConfig:
    """Common configuration"""

    # Unsupported providers (general configuration)
    UNSUPPORTED_PROVIDERS: Set[str] = {
        # "whoop_v2",
        "dexcom_v3",
        "my_fitness_pal_v2",
        "map_my_fitness",
        "hammerhead",
        "beurer_api",
    }

    # Provider descriptions
    PROVIDER_DESCRIPTIONS: Dict[str, str] = {
        "abbott_libreview": "Diabetes management system for Abbott CGMs",
        "omron": "Home blood pressure monitors",
        "garmin": "Fitness and activity tracking watches",
        "beurer_api": "Monitors for blood pressure and glucose levels",
        "fitbit": "Activity trackers with comprehensive health insights",
        "oura": "Smart ring for sleep and activity tracking",
        "cronometer": "App for detailed nutrition tracking",
        "eight_sleep": "Smart mattress for sleep optimization",
        "withings": "Smart scales, watches, and health monitors",
        "hammerhead": "Advanced cycling computers for athletes",
        "peloton": "Indoor cycling and fitness equipment",
        "renpho": "Smart body fat scales",
        "wahoo": "Indoor bike trainers and cycling computers",
        "zwift": "Virtual cycling app",
        "polar": "Sports technology devices for fitness enthusiasts",
        "ultrahuman": "Real-time nutrition and fitness tracking",
        # "whoop_v2": "Smart wearable for activity and recovery tracking",
        "dexcom_v3": "Continuous glucose monitoring devices",
        "map_my_fitness": "Tracking workouts and fitness activities",
    }

    # Provider order
    PROVIDER_ORDER = [
        # supported
        "abbott_libreview",
        "omron",
        "ihealth",
        "garmin",
        "beurer_api",
        "fitbit",
        "oura",
        "cronometer",
        "eight_sleep",
        "withings",
        "hammerhead",
        "peloton",
        "wahoo",
        "zwift",
        "polar",
        "ultrahuman",
        # unsupported
        # "whoop_v2",
        "dexcom_v3",
        "my_fitness_pal_v2",
        "map_my_fitness",
    ]

    # Resource types not supported by AI processing
    AI_UNSUPPORTED_RESOURCES: Set[ResourceType] = {
        ResourceType.ACTIVITY,
        ResourceType.BODY,
        ResourceType.MENSTRUAL_CYCLE,
        ResourceType.PROFILE,
        ResourceType.WORKOUT,
        ResourceType.WORKOUT_STREAM,
        ResourceType.BODY_TEMPERATURE_DELTA,
    }

    # Summary type resources
    SUMMARY_RESOURCES: Set[ResourceType] = {
        ResourceType.ACTIVITY,
        ResourceType.BODY,
        ResourceType.MEAL,
        ResourceType.MENSTRUAL_CYCLE,
        ResourceType.PROFILE,
        ResourceType.SLEEP,
        ResourceType.SLEEP_CYCLE,
        ResourceType.WORKOUT,
        ResourceType.WORKOUT_STREAM,
    }
