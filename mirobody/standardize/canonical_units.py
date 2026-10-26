"""A device indicator's value, in the unit the catalogue declares for it.

`convert_to_standard(StandardIndicator.WEIGHT, 154, "lb")` gives kilograms,
because that is what `bodyMasss` is stored in. One target per indicator, and
the target comes from the catalogue next door in `indicators_info`.

The arithmetic is `mirobody.units`, the UCUM engine in the library layer: both
spellings are normalized to UCUM there and converted with `convert_value`, so
`mmol/l`, `lbs`, `beats/min` and `℉` convert exactly as `mmol/L`, `lb`, `bpm`
and `°F` do. This module adds only what a unit cannot say: which LOINC code an
indicator's molar mass is keyed by, and that a mass of water is a volume.
"""

from __future__ import annotations

from mirobody.units import conversion_factor, convert_value, normalize_unit

from .indicators_info import StandardIndicator

#: The LOINC code whose molar mass (`units.MOLAR_MASS`) bridges mass and
#: substance concentration for an indicator. Triglycerides need their own: the
#: cholesterol molar mass turned TG 150 mg/dL into 3.88 mmol/L, read as
#: severely elevated, where the right answer is 1.69.
_MOLAR_MASS_CODE: dict[StandardIndicator, str] = {
    StandardIndicator.BLOOD_GLUCOSE: "2339-0",
    StandardIndicator.CHOLESTEROL_TOTAL: "2093-3",
    StandardIndicator.CHOLESTEROL_LDL: "2089-1",
    StandardIndicator.CHOLESTEROL_HDL: "2085-9",
    StandardIndicator.CHOLESTEROL_TRIGLYCERIDES: "2571-8",
}

#: Indicators stored as a volume that devices report as a mass, in kilograms
#: per litre: water logged in grams.
_DENSITY_KG_PER_L: dict[StandardIndicator, float] = {
    StandardIndicator.DIETARY_WATER: 1.0,
}

#: The spellings providers and the catalogue write, which `UNIT_CONVERSIONS`
#: covers. Only the spellings are listed here; every factor is the engine's.
_SPELLINGS: tuple[str, ...] = (
    "kg", "g", "lb", "oz",
    "m", "cm", "mm", "km", "ft", "in",
    "ms", "s", "min", "h", "seconds", "minutes", "hours",
    "mmHg", "kPa",
    "kcal", "cal", "kJ", "J",
    "mg/dL", "g/L", "mg/L",
    "count/min", "bpm", "/min", "breaths/min",
    "%", "percent", "spo2",
    "L", "mL", "ml", "cup",
    "°C", "degC",
    "m/s", "km/hr",
    "mL/kg/min", "mL/(min·kg)",
)


def _factor_table(spellings: tuple[str, ...]) -> dict[str, dict[str, float]]:
    """`table[a][b]` multiplies a value in `a` into `b`, for every pair the
    engine converts by a factor."""
    ucum = {s: normalize_unit(s) for s in spellings}
    table: dict[str, dict[str, float]] = {}
    for a in spellings:
        for b in spellings:
            if a == b or not ucum[a] or not ucum[b]:
                continue
            factor = conversion_factor(ucum[a], ucum[b])
            if factor is not None:
                table.setdefault(a, {})[b] = factor
    return table


#: `UNIT_CONVERSIONS[a][b]`: the factor from spelling `a` to spelling `b`.
#: Provider plugins import it from `mirobody.collect`.
UNIT_CONVERSIONS: dict[str, dict[str, float]] = _factor_table(_SPELLINGS)


def convert_to_standard(indicator: StandardIndicator, value: float, unit: str) -> tuple[float, str]:
    """`(value, unit)` in the catalogue's standard unit for `indicator`.

    An empty unit is taken to be the standard one. Units that do not convert
    come back as given, value and unit both, so a number is never stored
    under a unit it was not measured in.

        convert_to_standard(StandardIndicator.WEIGHT, 70000.0, "g")    # (70.0, "kg")
        convert_to_standard(StandardIndicator.HEART_RATE, 75.0, "bpm")  # (75.0, "count/min")

    Raises ValueError when `indicator` is not a `StandardIndicator` member.
    """
    if not isinstance(indicator, StandardIndicator):
        raise ValueError(f"Invalid indicator: {indicator}")
    standard_unit = indicator.value.standard_unit
    if not unit or not unit.strip() or unit == standard_unit:
        return value, standard_unit

    src, dst = normalize_unit(unit), normalize_unit(standard_unit)
    if not src or not dst:
        return value, unit
    density = _DENSITY_KG_PER_L.get(indicator)
    to_kilograms, from_litres = conversion_factor(src, "kg"), conversion_factor("L", dst)
    if density and to_kilograms is not None and from_litres is not None:
        return value * to_kilograms / density * from_litres, standard_unit
    converted = convert_value(value, src, dst, loinc_code=_MOLAR_MASS_CODE.get(indicator, ""))
    if converted is None:
        return value, unit
    return converted, standard_unit


__all__ = ["UNIT_CONVERSIONS", "convert_to_standard"]
