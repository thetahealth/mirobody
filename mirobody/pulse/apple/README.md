# Apple Health Platform

The Apple Health Platform is used to process Apple Health export data and CDA (Clinical Document Architecture) documents.

## Architecture Overview

```
Apple Platform
├── AppleHealthPlatform      # Main platform class
├── Providers/               # Data providers
│   ├── AppleHealthProvider  # Apple Health data processing
│   └── CDAProvider          # CDA document processing
├── Event Providers/         # Event handlers
│   ├── HeartRateEventProvider
│   └── [Other event handlers]
└── Database Service         # Database service
```

## Features

- **No Authentication Required**: Apple Health receives data via API without OAuth or password authentication
- **Event-Driven Architecture**: Uses event handlers to process different types of health data
- **Batch Processing**: Supports batch data processing and performance optimization

## Usage

### 1. Upload Data via API Endpoint

The Apple Health Platform provides RESTful API endpoints to receive health data:

**API Endpoints**: 
- Apple Health data: `POST /apple/health`
- CDA document data: `POST /apple/cda`

**Apple Health Data Format**:
```json
{
    "request_id": "unique_request_id",
    "metaInfo": {
        "timezone": "Asia/Shanghai"
    },
    "healthData": [
        {
            "uuid": "550e8400-e29b-41d4-a716-446655440000",  // Required
            "type": "HEART_RATE",  // Required, FlutterHealthTypeEnum
            "dateFrom": 1705284600000,  // Millisecond timestamp
            "dateTo": 1705284600000,
            "value": {"numericValue": 72},  // Required
            "unitSymbol": "bpm",
            "sourceId": "com.apple.health",
            "timezone": "Asia/Shanghai"
        }
    ]
}
```

**Data Validation**:
- Uses Pydantic models for strict data validation
- `uuid` and `type` fields are required
- `type` must be a valid `FlutterHealthTypeEnum` value
- Invalid data will return a 400 error

**Data Type Mapping**:
- The `type` field (FlutterHealthTypeEnum) in Apple Health data is automatically mapped to standard indicators
- Supports 50+ health data types, including vital signs, activity & fitness, body measurements, sleep, nutrition, etc.
- Sleep data is specially mapped to StandardIndicator sleep types
- For detailed mapping relationships, refer to `FLUTTER_TO_RECORD_TYPE_MAPPING` in `models.py`

**Features**:
- Supports gzip compression (add `Content-Encoding: gzip` header)
- Batch data processing (1000 records per batch)
- Asynchronous task processing
- Performance optimizations:
  - Timezone caching to avoid repeated ZoneInfo object creation
  - Direct use of Pydantic object properties to avoid model_dump()
  - Reduced logging calls to improve processing speed

### 2. Supported Data Types

Currently implemented event handlers:
- **Heart Rate Data** (HeartRateEventProvider)
  - `HKQuantityTypeIdentifierHeartRate`
  - `HKQuantityTypeIdentifierRestingHeartRate`
  - `HKQuantityTypeIdentifierWalkingHeartRateAverage`

For data types without specific handlers, a generic processing method will be used.

### 3. Extending New Data Types

Create a new event handler:

```python
# apple/event_providers/your_type.py
from .base import BaseAppleEventProvider
from ...data_upload.models.requests import StandardPulseRecord

class YourTypeEventProvider(BaseAppleEventProvider):
    @property
    def supported_data_types(self) -> List[str]:
        return ["HKQuantityTypeIdentifierYourType"]
    
    async def format_records(self, raw_records, user_id):
        # Implement data formatting logic
        pass
```

Then register in `registry.py`:

```python
def _auto_register_providers(self):
    # ...
    self.register_provider(YourTypeEventProvider())
```

## Data Flow

1. Client sends Apple Health data via API
2. Platform receives and parses the data
3. Dispatches to corresponding event handler based on data type
4. Event handler formats data into `StandardPulseRecord`
5. Calls `VitalHealthService` to store data into `series_data` table

## Important Notes

- Apple Health data uses ISO time format with timezone information
- All data is ultimately converted to standard `StandardPulseData` format
- Supports batch data processing, a single upload can contain multiple data types
