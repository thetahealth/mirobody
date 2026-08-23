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
and converted. The member list and units above are generated from the
catalogue itself, so this table cannot drift from the code — hand-written
revisions of it carried casing and unit errors.

## 🔧 Conversion examples

What `convert_to_standard()` does on the ingest path, verbatim from the tables
in `units.py`:

```python
convert_to_standard(StandardIndicator.WEIGHT, 154.5, "lb")          # (70.08, "kg")
convert_to_standard(StandardIndicator.BODY_TEMPERATURE, 98.6, "°F") # (37.0, "°C")
convert_to_standard(StandardIndicator.BLOOD_GLUCOSE, 5.5, "mmol/L") # (99.1, "mg/dL")
convert_to_standard(StandardIndicator.HEART_RATE, 75.0, "bpm")      # (75.0, "count/min")
```

Note the last line: `bpm` is accepted on input but the stored unit is
`count/min` — the same fact the unit table above records.

Triglycerides convert with their own molar mass (~885.4 g/mol), not the
cholesterol family's ~387: sharing one factor across the four lipids reads a
normal TG of 150 mg/dL as "severely elevated" (3.879 mmol/L instead of
~1.69). A regression test in the maintainers' suite pins the split.

## ✅ Responsibility division

- **Provider authors**: build `StandardPulseData` in `format_data()` with your
  vendor's native units, and use catalogue indicator names. Nothing else — do
  NOT convert units yourself.
- **The ingest layer** (`pulse/ingest/services/base.py`) converts every record
  once, on the one path all sources share. There is no standardization
  function for a platform to call — an earlier version of this document
  described a `standardize_pulse_data()` entry point that never existed; the
  flow diagram in "Where standardization actually happens" above is the
  real contract.
- **Failure mode to know**: an unknown indicator or failed conversion is
  logged and the original value/unit kept (see the trade-off note above), so
  new indicators must be added to the catalogue, not merely sent.
