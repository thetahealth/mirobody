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

There is no per-metric provider class. One enum and one mapping, both in
[`mirobody/pulse/apple/models.py`](../mirobody/pulse/apple/models.py), decide
what the endpoint accepts and where a record lands.

**1. Declare the type** on `FlutterHealthTypeEnum`:

```python
class FlutterHealthTypeEnum(str, Enum):
    ...
    BLOOD_PRESSURE_SYSTOLIC = "BLOOD_PRESSURE_SYSTOLIC"
```

**2. Map it to a standard indicator**, in the same file:

```python
FLUTTER_TO_RECORD_TYPE_MAPPING = {
    ...
    FlutterHealthTypeEnum.BLOOD_PRESSURE_SYSTOLIC:
        StandardIndicator.BLOOD_PRESSURE_SYSTOLIC.value.name,
}
```

**3. If that indicator does not exist yet**, add it to `StandardIndicator` in
[`mirobody/pulse/standardize/indicators_info.py`](../mirobody/pulse/standardize/indicators_info.py)
with its canonical unit — see that package's
[README](../mirobody/pulse/standardize/README.md).

Step 1 without step 2 is silent data loss, not an error. `type` validation is
deliberately lenient, so the record is accepted and then dropped in
`_prepare_record_optimized` with:

```
This record will be DISCARDED. Please add mapping to
FLUTTER_TO_RECORD_TYPE_MAPPING if needed.
```

## 📊 What the endpoint accepts

**`type` is a `FlutterHealthTypeEnum` value, not an Apple HealthKit
identifier.** Send `HEART_RATE`, not `HKQuantityTypeIdentifierHeartRate` — no
`HK*` string appears anywhere in this codebase, and because validation is
lenient, sending one is accepted and then discarded exactly as above.

64 members are declared; **60 carry a mapping**:

| Group | Mapped | Examples |
| --- | --- | --- |
| Vital signs | 10 | `HEART_RATE`, `BLOOD_PRESSURE_SYSTOLIC`, `BLOOD_OXYGEN` |
| Renpho body-scale | 12 | `BASAL_METABOLIC_RATE`, `BODY_WATER`, `VISCERAL_FAT` |
| Reproductive health | 11 | `BASAL_BODY_TEMPERATURE`, `MENSTRUATION_FLOW` |
| Activity and fitness | 9 | `STEPS`, `DISTANCE_WALKING_RUNNING`, `VO2_MAX` |
| Body measurements | 6 | `HEIGHT`, `WEIGHT`, `BODY_MASS_INDEX` |
| Sleep | 6 | `SLEEP_IN_BED`, `SLEEP_DEEP`, `SLEEP_REM` |
| Nutrition | 5 | `DIETARY_PROTEIN_CONSUMED`, `DIETARY_WATER` |
| UV exposure | 1 | `UV_EXPOSURE` |

This table is a summary. The mapping in the source is the only authoritative
list, and it is one command away — so nothing here can drift into being a
second, wrong copy of it:

```bash
python -c "from mirobody.pulse.apple.models import FLUTTER_TO_RECORD_TYPE_MAPPING as m; \
           print(len(m)); [print(k.value) for k in m]"
```

The remaining **4 declared-but-unmapped** members — `INFREQUENT_MENSTRUAL_CYCLES`,
`IRREGULAR_MENSTRUAL_CYCLES`, `PERSISTENT_INTERMENSTRUAL_BLEEDING`,
`PROLONGED_MENSTRUAL_PERIODS` — have their mapping rows commented out even though
the target `StandardIndicator` members exist. A client may legally send them and
the records are dropped. Treat that as a known gap, not a design.

## 🔍 Debugging Tips

1. **View Logs**: Logs record detailed information about data processing
2. **Unmapped types are dropped, not rejected**: grep the worker log for `will be DISCARDED` to see which `type` values a client is sending that `FLUTTER_TO_RECORD_TYPE_MAPPING` has no row for
3. **Performance Optimization**: Large amounts of data will be processed in batches to improve performance

## ⚠️ Important Notes

1. **Time Format**: Apple Health uses ISO 8601 format with timezone information
2. **Data Volume**: Large amounts of data may need to be processed in batches
3. **Duplicate Data**: Deduplication logic for duplicate data needs to be handled at the application layer
4. **Unit Conversion**: Ensure units are consistent with system standard units
