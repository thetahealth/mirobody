# `mirobody/translate` — indicators, units and standardization

The layer that decides what a value *means*: which indicators exist, what unit
each is stored in, the conversion a device's raw number passes through on its
way from `StandardPulseData` to a stored row, and the code a printed reading is
filed under.

> Database schema and initialization used to be the first 60 lines of this file,
> which made it two unrelated documents in one. They now live with the DDL they
> describe, in [`mirobody/schema/README.md`](../schema/README.md).

## What is here

The pure seam (`fold`, `parse`, `local_day`, `series`, `outcome`, `code`,
`icpc3`) folds a printed name, types a value, places its day and codes it;
it has no database, clock or model. Around it: the device crosswalks
(`devices.py`, `device_bundle.py`), the terminology tools' bodies
(`terminology.py`), the genotype and pharmacogenomics lookups (`genotype*.py`,
`pgx.py`, `cpic_extract.py`), and the device indicator catalogue with its
units and ranges:

```
mirobody/translate/
├── indicators_info.py       # StandardIndicator: the catalogue: name, category,
│                            #   stored unit, aggregation methods, per language
├── canonical_units.py       # convert_to_standard(), on mirobody.units
├── value_range_validator.py # per-indicator plausible-range checks (rules from DB)
├── aggregate/               # daily summaries and election (aggregate/README.md)
└── derive/                  # quantities computed from those summaries
```

`mirobody/translate/__init__.py` lists every module and the names other
packages import.

## Where a device reading is converted

You do not call a standardization function yourself. A device reading is
converted once, on the path every device source converges on:

```
provider.format_data(raw)          # vendor shape -> StandardPulseData
        └─> StandardHealthService.process_standard_data(...)   collect/ingest/services/upload_health.py
                └─> convert_to_standard(indicator, value, unit)
```

`ProviderPlatform.post_data()` drives it (`collect/providers/_platform/platform.py`), so a
**provider author has nothing to do**: report your vendor's native unit in
`format_data()` and it is converted on the way in.

One behaviour worth knowing, because it is a deliberate trade-off: an
indicator the catalogue does not know, a value that is not a number, or a
unit that does not convert keeps its **original value and unit**, and nothing
is logged. That keeps one odd row from failing a whole sync, but it also
means a value can land unconverted, so a new indicator must be added to the
catalogue, not merely sent.

### **Utility Function Usage (Testing and Validation)**

```python
from mirobody.translate import (
    convert_to_standard,
    StandardIndicator,
    is_valid_indicator,
    get_standard_unit,
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
```

## 📊 Standard indicators — a sample

Members are **UPPERCASE** (`StandardIndicator.HEART_RATE`), and the unit shown
is the one values are STORED in after conversion — not necessarily the unit a
device reports. The full catalogue is `mirobody/translate/indicators_info.py`;
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

What `convert_to_standard()` does on the ingest path, its arithmetic from `mirobody.units`:

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
- **The ingest layer** (`collect/ingest/services/upload_health.py`) converts
  every record once, on the one path all device sources share. There is no
  standardization function for a platform to call; the flow in "Where a
  device reading is converted" above is the contract.
- **Failure mode to know**: an unknown indicator or a unit that does not
  convert keeps the original value and unit (see the trade-off note above),
  so new indicators must be added to the catalogue, not merely sent.
