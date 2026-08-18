# `pulse/standardize` — indicators, units and standardization

The layer that decides what a value *means*: which indicators exist, what unit
each is stored in, and the conversion every provider's raw number passes through
on the way to `StandardPulseData`.

> Database schema and initialization used to be the first 60 lines of this file,
> which made it two unrelated documents in one. They now live with the DDL they
> describe, in [`mirobody/schema/README.md`](../../schema/README.md).

## 📋 **Overview**

Provides unified health indicator and unit management services for all Pulse platforms, enabling data standardization and normalization.

**Core Design Principle**: Platform layer handles standardization, Provider focuses on data formatting.

## 🎯 **Core Features**

- **Indicator Enumeration**: Define standard health indicator enums
- **Unit Validation**: Validate if units are in the valid unit set
- **Auto Conversion**: Platform layer automatically converts units for StandardPulseData
- **Error Detection**: Detect invalid indicators and units, print error logs

## 📁 File structure

```
mirobody/pulse/standardize/
├── indicators_info.py       # StandardIndicator: the catalogue — name, category,
│                            #   stored unit, aggregation methods, per language
├── units.py                 # convert_to_standard() and the conversion tables
├── value_range_validator.py # per-indicator plausible-range checks (rules from DB)
├── fhir_mapping.py          # indicator name -> fhir_id cache for series_data writes
├── std_indicator_registry/  # scheduled task publishing the catalogue to the DB;
│                            #   the one module here allowed to import pulse.core
│                            #   (scheduler + aggregation rules are wiring, not meaning)
└── README.md
```

This package was extracted from `pulse/core`, where these files sat between
auth, scheduler and push-service infrastructure. The dependency rule that keeps
the split meaningful: everything except `std_indicator_registry/` must stay
importable without `pulse.core` — pure data plus DB reads via `mirobody.utils`.

## 🚀 Where standardization actually happens

You do not call a standardization function yourself. Conversion happens once,
inside the ingest layer, on the path every source converges on:

```
provider.format_data(raw)          # vendor shape -> StandardPulseData
        └─> StandardHealthService.process_standard_data(...)
                └─> BaseHealthService._convert_value(...)      pulse/ingest/services/base.py
                        └─> convert_to_standard(indicator, value, unit)
```

`ProviderPlatform.post_data()` drives it (`pulse/providers/platform/platform.py`), so a
**provider author has nothing to do**: report your vendor's native unit in
`format_data()` and it is converted on the way in.

One behaviour worth knowing, because it is a deliberate trade-off: an unknown
indicator or a conversion that fails is logged and the **original value and unit
are kept** rather than raising (`base.py:66-78`). That keeps one odd row from
failing a whole sync — but it also means a value can land unconverted, so a new
indicator must be added to the catalogue, not merely sent.

### **Utility Function Usage (Testing and Validation)**

```python
from mirobody.pulse.standardize import (
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

## 📊 Standard indicators — a sample

Members are **UPPERCASE** (`StandardIndicator.HEART_RATE`), and the unit shown
is the one values are STORED in after conversion — not necessarily the unit a
device reports. The full catalogue is `mirobody/pulse/standardize/indicators_info.py`;
this table is generated from it.

| Member | Stored unit | Name |
| --- | --- | --- |
| `StandardIndicator.CALORIES_ACTIVE` | `kcal` | activeCalories |
| `StandardIndicator.CALORIES_BASAL` | `kcal` | basalCalories |
| `StandardIndicator.DISTANCE` | `m` | walkingRunningDistances |
| `StandardIndicator.STEPS` | `count` | steps |
| `StandardIndicator.BMI` | `count` | bmis |
| `StandardIndicator.BMR` | `kcal` | basalMetabolicRate |
| `StandardIndicator.BODY_FAT_PERCENTAGE` | `%` | bodyFatPercentages |
| `StandardIndicator.BODY_WATER_PERCENTAGE` | `%` | bodyWater |
| `StandardIndicator.BONE_MASS` | `kg` | bodyBone |
| `StandardIndicator.MUSCLE_PERCENTAGE` | `%` | bodyMuscle |
| `StandardIndicator.VISCERAL_FAT` | `%` | bodyVisFat |
| `StandardIndicator.WEIGHT` | `kg` | bodyMasss |
| `StandardIndicator.BIA_RESISTANCE` | `Ω` | biaResistance |
| `StandardIndicator.BLOOD_GLUCOSE` | `mg/dL` | bloodGlucoses |
| `StandardIndicator.BLOOD_OXYGEN` | `%` | oxygenSaturations |
| `StandardIndicator.BLOOD_PRESSURE_DIASTOLIC` | `mmHg` | diastolicPressures |
| `StandardIndicator.BLOOD_PRESSURE_SYSTOLIC` | `mmHg` | systolicPressures |
| `StandardIndicator.HEART_RATE` | `count/min` | heartRates |

Note `HEART_RATE` stores `count/min`, not `bpm` — `bpm` is accepted as input
and converted. This section previously listed the members in lower case, which
raises `AttributeError`, and gave three wrong units. Generating it from the
catalogue is what stops that recurring; `test_readme_claims.py` checks it.
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
    Platform = ProviderPlatform()
    
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
    provider = RenphoProvider()
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
    Platform = ProviderPlatform()
    
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

- **Provider platform**: Standardize raw data returned from device APIs
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
