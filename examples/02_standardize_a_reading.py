"""Standardize a vendor reading: unit conversion + the indicator catalogue.

    pip install mirobody
    python examples/02_standardize_a_reading.py

Still offline — no key, no database, no network. This is the half of ① Collect
that runs before anything is stored: a device sends `154.5 lb`, and what lands
in the record is `70.08 kg`, because every downstream comparison assumes one
unit per indicator.

The conversion is not a nicety. A trend line built from a mix of lb and kg, or
mg/dL and mmol/L, is silently wrong rather than obviously wrong.
"""

from mirobody.pulse.standardize.indicators_info import StandardIndicator
from mirobody.pulse.standardize.units import convert_to_standard, get_all_units_info


# ── 1. what a provider sends vs what gets stored ─────────────────────────────
print("Providers report whatever their region uses. One unit per indicator gets stored:\n")

incoming = [
    (StandardIndicator.WEIGHT,           154.5, "lb"),
    (StandardIndicator.BODY_TEMPERATURE,  98.6, "°F"),
    (StandardIndicator.HEART_RATE,        75.0, "bpm"),
]

for indicator, value, unit in incoming:
    converted, standard_unit = convert_to_standard(indicator, value, unit)
    print(f"  {indicator.name:<18} {value:>7} {unit:<4}  ->  {converted:>8.2f} {standard_unit}")


# ── 2. the indicator catalogue carries more than a unit ──────────────────────
print("\nEach indicator knows what it is, in every language the catalogue covers:\n")

for indicator in (StandardIndicator.WEIGHT, StandardIndicator.HEART_RATE):
    info = indicator.value
    print(f"  {indicator.name}")
    print(f"      name        {info.name}  /  {info.name_zh}")
    print(f"      category    {info.category.name}  /  {info.category.name_zh}")
    print(f"      stored as   {info.standard_unit}")
    print(f"      aggregates  {', '.join(info.aggregation_methods)}")


# ── 3. an unknown indicator is an error, not a guess ─────────────────────────
print("\nAn indicator the catalogue does not know is refused, not guessed at:\n")
try:
    convert_to_standard("weight", 154.5, "lb")          # a string, not the enum
except ValueError as e:
    print(f"  convert_to_standard('weight', ...) -> ValueError: {e}")
print("  (pass the StandardIndicator member; a typo must not become a silent no-op)")


# ── 4. the whole unit table, for a UI ────────────────────────────────────────
units = get_all_units_info()
print(f"\nget_all_units_info() -> {len(units)} entries, for populating a unit picker.")
