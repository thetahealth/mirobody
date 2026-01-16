# Apple Health Platform Integration Guide

## 📋 Overview

The Apple Health Platform specializes in integrating Apple Health export data and CDA documents, using an event-based architecture to process different types of health data.

## 🎯 When to Choose Apple Health Platform

**Use Cases:**
- ✅ Processing Apple Health export files
- ✅ Processing CDA (Clinical Document Architecture) documents
- ✅ Bulk importing historical health data
- ✅ Scenarios without real-time synchronization requirements

**Technical Features:**
- ✅ API reception mode
- ✅ Event-driven processing
- ✅ Batch data processing
- ✅ gzip compression support

## 🚀 Integration Steps

### Step 1: Send Data via API Endpoint

Apple Health data is received through HTTP API endpoints with gzip compression support.

**API Endpoint**: `POST /apple/health`

**Performance Optimization Features**:
- **Batch Processing**: Automatically processes large amounts of data in batches (1000 records per batch) to avoid memory overflow
- **Timezone Caching**: Caches ZoneInfo objects to reduce repeated creation overhead
- **Zero-Copy Optimization**: Directly uses Pydantic object properties to avoid model_dump() serialization overhead
- **Efficient Time Processing**: Unified conversion to UTC time to reduce timezone conversion operations

**Request Format**:
```json
{
    "request_id": "unique_request_id",
    "metaInfo": {
        "userId": "user_123",
        "timezone": "Asia/Shanghai"
    },
    "healthData": [
        {
            "uuid": "550e8400-e29b-41d4-a716-446655440000",  // Required, unique record identifier
            "type": "HEART_RATE",  // Required, FlutterHealthTypeEnum type
            "dateFrom": 1705284600000,  // Optional, start timestamp (milliseconds)
            "dateTo": 1705284600000,  // Optional, end timestamp (milliseconds)
            "value": {"numericValue": 72},  // Required, numeric data
            "unitSymbol": "bpm",  // Optional, unit symbol
            "sourceId": "com.apple.health",  // Optional, data source ID
            "timezone": "Asia/Shanghai",  // Optional, defaults to UTC
            "sourceName": "Apple Health",  // Optional
            "sourcePlatform": "iOS",  // Optional
            "sourceDeviceId": "device123",  // Optional
            "recordingMethod": "automatic",  // Optional
            "createdAt": 1705284600000  // Optional, creation timestamp
        }
    ]
}
```

**Supported Data Types** (Complete FlutterHealthTypeEnum):

**Vital Signs**:
- `HEART_RATE` - Heart Rate → heartRates
- `RESPIRATORY_RATE` - Respiratory Rate → respiratoryRates
- `BODY_TEMPERATURE` - Body Temperature → bodyTemperatures
- `BLOOD_GLUCOSE` - Blood Glucose → bloodGlucoses
- `BLOOD_OXYGEN` - Oxygen Saturation → oxygenSaturations
- `BLOOD_PRESSURE_SYSTOLIC` - Systolic Blood Pressure → systolicPressures
- `BLOOD_PRESSURE_DIASTOLIC` - Diastolic Blood Pressure → diastolicPressures
- `WALKING_HEART_RATE` - Walking Heart Rate → walkingHeartRates
- `RESTING_HEART_RATE` - Resting Heart Rate → restingHeartRates
- `HEART_RATE_VARIABILITY_SDNN` - Heart Rate Variability → hrvRMSSD

**Activity & Fitness**:
- `STEPS` - Steps → steps
- `CYCLING_SPEED` - Cycling Speed → cyclingSpeeds
- `WALKING_SPEED` - Walking Speed → speeds
- `FLIGHTS_CLIMBED` - Flights Climbed → floors
- `DISTANCE_WALKING_RUNNING` - Walking/Running Distance → walkingRunningDistances
- `EXERCISE_TIME` - Exercise Time → exerciseMinutes
- `DISTANCE_CYCLING` - Cycling Distance → cyclingDistances
- `VO2_MAX` - VO2 Max → vo2Maxs
- `HEART_RATE_RECOVERY_ONE_MINUTE` - Heart Rate Recovery → recoveryes

**Body Measurements**:
- `HEIGHT` - Height → heights
- `WEIGHT` - Weight → bodyMasss
- `BODY_FAT_PERCENTAGE` - Body Fat Percentage → bodyFatPercentages
- `BODY_MASS_INDEX` - BMI → bmis
- `WAIST_CIRCUMFERENCE` - Waist Circumference → waistCircumferences
- `SLEEPING_WRIST_TEMPERATURE` - Wrist Temperature → wristTemperatures

**Sleep**:
- `SLEEP_IN_BED` - Time In Bed → sleepAnalysis_InBed
- `SLEEP_ASLEEP` - Sleep Time → sleepAnalysis_Asleep(Unspecified)
- `SLEEP_AWAKE` - Awake Time → sleepAnalysis_Awake
- `SLEEP_DEEP` - Deep Sleep → sleepAnalysis_Asleep(Deep)
- `SLEEP_LIGHT` - Light Sleep → sleepAnalysis_Asleep(Core)
- `SLEEP_REM` - REM Sleep → sleepAnalysis_Asleep(REM)

**Nutrition**:
- `DIETARY_PROTEIN_CONSUMED` - Protein Intake → proteins
- `DIETARY_CARBS_CONSUMED` - Carbohydrate Intake → carbohydrates
- `DIETARY_FATS_CONSUMED` - Fat Intake → fats
- `DIETARY_ENERGY_CONSUMED` - Energy Intake → energyes
- `DIETARY_WATER` - Water Intake → waters

**Others**:
- `UV_EXPOSURE` - UV Exposure → uvExposures

**Body Composition Analysis** (Compatible with Renpho body fat scale):
- `BASAL_METABOLIC_RATE` - Basal Metabolic Rate → basalMetabolicRate
- `BODY_WATER` - Body Water → bodyWater
- `BODY_AGE` - Body Age → bodyAge
- `BODY_MUSCLE` - Skeletal Muscle Rate → bodyMuscle
- `BODY_BONE` - Bone Weight → bodyBone
- `BODY_SUB_FAT` - Subcutaneous Fat → bodySubFat
- `BODY_VIS_FAT` - Visceral Fat → bodyVisFat
- `BODY_FAT_FREE_WEIGHT` - Fat-Free Body Weight → bodyFatFreeWeight
- `BODY_SINEW` - Sinew → bodySinew
- `BODY_PROTEIN` - Protein Percentage → bodyProtein

Note: Values after the arrow are mapped StandardIndicator values

**Request Example** (using curl):
```bash
curl -X POST https://your-api-domain/apple/health \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Encoding: gzip" \
  -d @- << EOF | gzip
{
    "request_id": "req_123",
    "metaInfo": {
        "userId": "user_123",
        "timezone": "Asia/Shanghai"
    },
    "healthData": [
        {
            "uuid": "test-uuid-123",
            "type": "HEART_RATE",
            "dateFrom": 1705284600000,
            "dateTo": 1705284600000,
            "value": {"numericValue": 72},
            "unitSymbol": "bpm",
            "sourceId": "com.apple.health"
        }
    ]
}
EOF
```

### Step 2: Handle Response

Success response format:
```json
{
    "success": true,
    "data": {"request_id": "unique_request_id"},
    "message": "Apple Health data processed successfully"
}
```

Failure response format:
```json
{
    "success": false,
    "message": "Error message"
}
```

## 🔧 Adding New Data Type Support

### 1. Create Event Provider

```python
# apple/event_providers/blood_pressure.py

from typing import Any, Dict, List
from ...data_upload.models.requests import StandardPulseRecord
from .base import BaseAppleEventProvider

class BloodPressureEventProvider(BaseAppleEventProvider):
    """Blood Pressure Event Provider"""
    
    @property
    def supported_data_types(self) -> List[str]:
        return [
            "HKQuantityTypeIdentifierBloodPressureSystolic",
            "HKQuantityTypeIdentifierBloodPressureDiastolic"
        ]
    
    async def format_records(
        self, 
        raw_records: List[Dict[str, Any]], 
        user_id: str
    ) -> List[StandardPulseRecord]:
        formatted_records = []
        
        for record in raw_records:
            # Implement blood pressure data formatting logic
            data_type = record.get("type")
            
            if data_type == "HKQuantityTypeIdentifierBloodPressureSystolic":
                indicator = "blood_pressure_systolic"
            else:
                indicator = "blood_pressure_diastolic"
            
            pulse_record = StandardPulseRecord(
                indicator=indicator,
                value=float(record.get("value", 0)),
                unit="mmHg",
                timestamp=record.get("startDate"),
                metadata={
                    "source": "apple_health",
                    "data_type": data_type
                }
            )
            formatted_records.append(pulse_record)
        
        return formatted_records
```

### 2. Register Event Provider

In `apple/event_providers/registry.py`:

```python
from .blood_pressure import BloodPressureEventProvider

def _auto_register_providers(self):
    # Existing registrations
    self.register_provider(HeartRateEventProvider())
    
    # Add blood pressure provider
    self.register_provider(BloodPressureEventProvider())
```

## 📊 Supported Apple Health Data Type Examples

### Vital Signs
- `HKQuantityTypeIdentifierHeartRate` - Heart Rate
- `HKQuantityTypeIdentifierBloodPressureSystolic` - Systolic Blood Pressure
- `HKQuantityTypeIdentifierBloodPressureDiastolic` - Diastolic Blood Pressure
- `HKQuantityTypeIdentifierBodyTemperature` - Body Temperature
- `HKQuantityTypeIdentifierRespiratoryRate` - Respiratory Rate

### Body Measurements
- `HKQuantityTypeIdentifierBodyMass` - Body Mass
- `HKQuantityTypeIdentifierHeight` - Height
- `HKQuantityTypeIdentifierBodyMassIndex` - BMI
- `HKQuantityTypeIdentifierBodyFatPercentage` - Body Fat Percentage

### Activity Data
- `HKQuantityTypeIdentifierStepCount` - Step Count
- `HKQuantityTypeIdentifierDistanceWalkingRunning` - Walking/Running Distance
- `HKQuantityTypeIdentifierActiveEnergyBurned` - Active Energy Burned
- `HKQuantityTypeIdentifierBasalEnergyBurned` - Basal Energy Burned

### Nutrition Data
- `HKQuantityTypeIdentifierDietaryWater` - Water Intake
- `HKQuantityTypeIdentifierDietaryEnergyConsumed` - Energy Consumed
- `HKQuantityTypeIdentifierDietaryProtein` - Protein
- `HKQuantityTypeIdentifierDietaryCarbohydrates` - Carbohydrates

### Sleep Data
- `HKCategoryTypeIdentifierSleepAnalysis` - Sleep Analysis

## 🔍 Debugging Tips

1. **View Logs**: Logs record detailed information about data processing
2. **Generic Processing**: Data types without specific providers will use generic processing methods
3. **Performance Optimization**: Large amounts of data will be processed in batches to improve performance

## ⚠️ Important Notes

1. **Time Format**: Apple Health uses ISO 8601 format with timezone information
2. **Data Volume**: Large amounts of data may need to be processed in batches
3. **Duplicate Data**: Deduplication logic for duplicate data needs to be handled at the application layer
4. **Unit Conversion**: Ensure units are consistent with system standard units
