# 🗄️ Database Structure & Initialization

Mirobody uses PostgreSQL with the `theta_ai` schema.

## 🏗️ Initialization Process

Database initialization is **automatic** for local and development environments.

1.  **Trigger**: When the server starts (`mirobody/server/server.py`).
2.  **Condition**: The `ENV` environment variable is **NOT** `TEST`, `GRAY`, or `PROD`.
3.  **Action**: The server executes all SQL files found in `mirobody/res/sql/` in alphabetical order.

### Bootstrap Files (`mirobody/res/sql/`)
- `00_init_schema.sql`: Creates extensions, schema, and base tables.
- `01_basedata.sql`: Inserts static dictionary data.
- `02_settings.sql`: application settings.
- ...and other migration scripts.

## 🧩 Schema Overview

All tables reside in the `theta_ai` schema.

### Extensions
The following PostgreSQL extensions are enabled:
- **`vector`**: For AI embeddings and semantic search.
- **`pg_trgm`**: For fast text similarity search.
- **`pgcrypto`**: For cryptographic functions.

### Core Tables

#### User & Auth
- **`health_app_user`**: Main user profile table.
- **`health_user_provider`**: Stores connection info for external providers (Google, Apple, etc.).

#### Data Sharing
- **`th_share_relationship`**: Tracks who shares data with whom.
- **`th_share_permission_type`**: Defines granular permissions (e.g., "All Data", "Device Data").

#### Health Data

- **`health_data_{provider}`**: Raw data storage for specific providers (e.g., `health_data_garmin`).
- **`th_task_flow`**: Tracks data processing tasks and status.

#### Agent Workspace

- **`deep_agent_workspace`**: PostgreSQL-backed storage for MCP file tools (`ls`, `read_file`, `write_file`, `edit_file`, `glob`, `grep`).

Provides persistent, session-isolated file workspaces with intelligent parsing (PDF/DOCX/images), content deduplication via SHA256 hashing, and audit trails. Primary keys: `(session_id, user_id, key)`.

**Implementation:** `mirobody/pub/agents/deep/backend.py` (PostgresBackend) | Schema: `mirobody/res/sql/90_deepagents.sql`

## 🛠️ Manual Initialization

If you need to manually initialize the database (e.g., for production):

1.  Ensure the database exists.
2.  Run the SQL files in order using `psql`:

```bash
psql -h $PG_HOST -U $PG_USER -d $PG_DBNAME -f mirobody/res/sql/00_init_schema.sql
psql -h $PG_HOST -U $PG_USER -d $PG_DBNAME -f mirobody/res/sql/01_basedata.sql
# ... run remaining files
```

---

# Health Data Indicators and Unit Management

## 📋 **Overview**

Provides unified health indicator and unit management services for all Pulse platforms, enabling data standardization and normalization.

**Core Design Principle**: Platform layer handles standardization, Provider focuses on data formatting.

## 🎯 **Core Features**

- **Indicator Enumeration**: Define standard health indicator enums
- **Unit Validation**: Validate if units are in the valid unit set
- **Auto Conversion**: Platform layer automatically converts units for StandardPulseData
- **Error Detection**: Detect invalid indicators and units, print error logs

## 📁 **File Structure**

```
backend_py/mirobody/pulse/core/
├── indicators_info.py         # Indicator enums and validation
├── units.py             # Unit validation and conversion
├── standardization.py   # Platform layer StandardPulseData standardization
└── README.md           # Usage documentation
```

## 🚀 **Core Usage**

### **Platform Layer Usage (Required)**

```python
from ..core import standardize_pulse_data

class ThetaPlatform(Platform):
    async def post_data(self, provider_slug: str, data: Dict[str, Any], msg_id: str) -> bool:
        # 1. Provider formats data
        provider = self.get_provider(provider_slug)
        pulse_data = await provider.format_data(data)
        
        # 2. Platform layer must call standardization function
        standardized_pulse_data = standardize_pulse_data(pulse_data)
        
        # 3. Process standardized data
        from ...data_upload.services import VitalHealthService
        vital_health_service = VitalHealthService()
        success = await vital_health_service.process_standard_data(
            standardized_pulse_data, user_id
        )
        
        return success
```

### **Provider Layer Usage (Simplified Implementation)**

```python
class ThetaRenphoProvider(BaseThetaProvider):
    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        # Provider only needs to build StandardPulseData, no standardization handling
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=user_id, ...),
            healthData=[
                StandardPulseRecord(
                    source="theta.renpho",
                    type="Weight(kg)",     # Can use original indicator names
                    timestamp=timestamp_ms,
                    unit="lb",             # Can use any reasonable unit
                    value=154.5,           # Platform layer will auto-convert
                    timezone="UTC"
                )
            ]
        )
```

### **Utility Function Usage (Testing and Validation)**

```python
from ..core import (
    convert_to_standard,
    StandardIndicator,
    is_valid_indicator,
    get_standard_unit,
    get_all_units_info
)

# Check if indicator is valid
is_valid_indicator("heartRates")      # True
is_valid_indicator("body_weight")     # False (not in standard enum)

# Get standard unit
get_standard_unit("heartRates")       # "count/min"

# Unit conversion (main API, includes indicator-specific conversion logic)
value, unit = convert_to_standard(
    StandardIndicator.WEIGHT,
    154.5,
    "lb"
)
# Returns: (70.1, "kg")

# Get all unit information (frontend API)
units_info = get_all_units_info()
print(f"Total units: {units_info['total_units']}")
```

## 📊 **Standard Indicator Enumeration**

### Vital Signs
- `heart_rate` → Heart Rate (bpm)
- `blood_pressure_systolic` → Systolic Blood Pressure (mmHg)
- `blood_pressure_diastolic` → Diastolic Blood Pressure (mmHg)
- `blood_oxygen` → Blood Oxygen Saturation (%)
- `blood_glucose` → Blood Glucose (mg/dL)

### Body Composition
- `weight` → Weight (kg)
- `body_fat_percentage` → Body Fat Percentage (%)
- `body_water_percentage` → Body Water Percentage (%)
- `muscle_percentage` → Muscle Percentage (%)
- `bone_mass` → Bone Mass (kg)
- `visceral_fat` → Visceral Fat (level)
- `bmi` → Body Mass Index (kg/m²)
- `bmr` → Basal Metabolic Rate (kcal)

### Activity Indicators
- `steps` → Steps (count)
- `distance` → Distance (m)
- `calories_active` → Active Calories (kcal)
- `calories_basal` → Basal Calories (kcal)

### Device Specific
- `bia_resistance` → Bioelectrical Impedance (Ω)

## 🔧 **Platform Layer Auto Conversion**

### Conversion Examples
```python
# Provider input: weight=154.5, unit="lb"
# Platform standardization: weight=70.1, unit="kg" (auto-converted)

# Provider input: temperature=98.6, unit="°F" 
# Platform standardization: temperature=37.0, unit="°C" (auto-converted)

# Provider input: glucose=5.5, unit="mmol/L"
# Platform standardization: glucose=99.1, unit="mg/dL" (auto-converted)
```

### Supported Conversions
- **Mass**: g, lb, oz → kg
- **Length**: cm, mm, ft, in, km → m
- **Temperature**: °F, F, K → °C
- **Pressure**: kPa, psi → mmHg
- **Energy**: cal, kJ, J → kcal
- **Blood Glucose**: mmol/L, g/L → mg/dL
- **Frequency**: Hz, count/min, /min → bpm

## 📋 **Platform Layer Standardization Flow**

### 1. Provider Invocation
- Platform receives raw data
- Calls Provider's `format_data` method
- Provider returns raw StandardPulseData

### 2. Indicator Check
- Platform checks if `type` is in `StandardIndicator` enum
- If invalid, print ERROR log, keep original record

### 3. Unit Validation
- Platform checks if `unit` is in `STANDARD_UNITS` set
- If invalid, print ERROR log, use standard unit

### 4. Unit Conversion
- If current unit is not standard unit, attempt conversion
- Conversion success: Update value and unit, print INFO log
- Conversion failure: Print ERROR log, keep original value but use standard unit

### 5. Data Processing
- Pass standardized data to subsequent processing services
- Update statistics in `processingInfo`

## 🚨 **Error Log Examples**

```
ERROR: Invalid indicator: 'body_weight' - not in standard indicator enum
ERROR: Invalid unit: 'pounds' for indicator 'weight' - not in standard unit set
ERROR: Failed to convert unit from 'xyz' to 'kg' for indicator 'weight'
INFO: Converted weight: 154.5 lb → 70.1 kg
INFO: Standardization completed: 5 records processed, 4 successful, 1 errors, 2 conversions
```

## ✅ **Responsibility Division**

### Platform Layer Responsibilities
- ✅ Call `standardize_pulse_data` for standardization
- ✅ Check error logs after standardization
- ✅ Pass standardized data to subsequent processing
- ✅ Ensure data quality and consistency

### Provider Layer Responsibilities
- ✅ Build reasonable StandardPulseData
- ✅ Use reasonable indicator names (standard indicators recommended)
- ✅ Use reasonable unit names (can be original units)
- ✅ Focus on data formatting, no standardization handling

### Developer Responsibilities
- ✅ Platform developers: Ensure standardization function is called
- ✅ Provider developers: Focus on data accuracy, not standardization
- ✅ Test developers: Validate standardization results

## 🔍 **Testing and Validation**

### Platform Layer Testing
```python
async def test_Platform_standardization():
    Platform = ThetaPlatform()
    
    # Test data with non-standard indicators and units
    test_data = {
        "user_id": "123",
        "data": {
            "Weight(kg)": 70.5,
            "body_weight_lb": 155.0  # Non-standard indicator and unit
        }
    }
    
    # Call Platform processing
    success = await Platform.post_data("theta_renpho", test_data, "msg_123")
    assert success
    
    # Validate standardization logs
    # Should see logs for unit conversion and indicator mapping
```

### Provider Layer Testing
```python
async def test_provider_format():
    provider = ThetaRenphoProvider()
    raw_data = {...}
    
    # Test Provider output
    result = await provider.format_data(raw_data)
    assert isinstance(result, StandardPulseData)
    assert len(result.healthData) > 0
    
    # Provider doesn't need to validate standardization
    # Platform layer handles standardization
```

### End-to-End Testing
```python
async def test_end_to_end_standardization():
    Platform = ThetaPlatform()
    
    # Test complete flow
    success = await Platform.post_data("theta_renpho", test_data, "msg_123")
    assert success
    
    # Validate final data standardization
    # Check if data in database uses standard units
```

## 🎯 **Advantages**

1. **Separation of Concerns**: Provider focuses on formatting, Platform handles unified standardization
2. **Simplified Development**: Provider developers don't need to worry about standardization logic
3. **Unified Management**: All Platforms use the same standardization flow
4. **Strong Fault Tolerance**: Errors don't interrupt processing, ensuring system stability
5. **Easy Maintenance**: Standardization logic centralized in Platform layer
6. **Flexible Extension**: Adding new indicators and conversion rules is simple

## 🚀 **Use Cases**

- **Theta Platform**: Standardize raw data returned from device APIs
- **Vital Platform**: Standardize health indicators in webhook events
- **Data Import**: Unify health data in various formats
- **Quality Control**: Platform layer ensures data conforms to unified standards

## 📈 **Development Workflow**

### 1. Provider Development
```python
# Provider only needs to focus on data formatting
class MyProvider(Provider):
    async def format_data(self, raw_data):
        # Build StandardPulseData, use original indicators and units
        return StandardPulseData(...)
```

### 2. Platform Development
```python
# Platform handles standardization
class MyPlatform(Platform):
    async def post_data(self, provider_slug, data, msg_id):
        pulse_data = await provider.format_data(data)
        standardized_data = standardize_pulse_data(pulse_data)  # Must call
        # Process standardized data...
```

### 3. Testing and Validation
```python
# Validate indicators and units
assert is_valid_indicator(final_data.healthData[0].type)
# Unit conversion is automatically handled during upload
```

This design ensures separation of concerns, simplifies Provider development, unifies standardization management, and improves system maintainability and data quality.
