"""
Health Unit Standardization Management

Provides unit conversion functionality with automatic bidirectional conversion generation.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Set, Tuple

# Import StandardIndicator for type hints
try:
    from .indicators_info import StandardIndicator
except ImportError:
    StandardIndicator = None  # Fallback for circular import

# ============================================================================
# STANDARD UNITS SET
# ============================================================================

STANDARD_UNITS: Set[str] = {
    # Mass
    "kg", "g",
    # Length  
    "m", "cm", "mm",
    # Time
    "ms", "s", "min", "h",
    # Pressure
    "mmHg", "kPa",
    # Energy
    "kcal", "kJ", "J",
    # Power (mechanical power output in exercise training)
    "W",
    # Concentration
    "mg/dL", "mmol/L", "g/L", "mg/L",
    # Frequency
    "bpm", "Hz", "count/min",
    # Volume
    "L", "mL",
    # Temperature
    "°C", "°F", "K", "degC",
    # Percentage
    "%",
    # Composite units
    "kg/m²", "kg/m^2", "Ω", "count", "level", "years", "date", "enum", "boolean", "bool",
    "score", "ratio", "ms²", "L/min/kg", "FSU", "unit", "mV", "seconds", "hours",
    "count/hour", "°", "psi", "lb", "oz", "ft", "in", "km", "/min", "breaths/min",
    "spo2", "percent", "minutes", "ml", "cup", "cal", "C", "F", "m/s", "rmssd",
    # VO2 Max units
    "mL/(min·kg)", "mL/kg/min",
}

# ============================================================================
# RAW UNIT CONVERSIONS (Simple Configuration)
# ============================================================================

# Only configure base unit conversions - all bidirectional conversions will be auto-generated
# Format: base_unit: {target_unit: conversion_factor}
# Where: 1 base_unit = conversion_factor × target_unit
# Example: 1 kg = 1000 g, so "kg": {"g": 1000}
_RAW_UNIT_CONVERSIONS: Dict[str, Dict[str, float]] = {
    # Mass: base unit kg
    # 1 kg = 1000 g = 2.20462 lb = 35.274 oz
    "kg": {
        "g": 1000,
        "lb": 2.20462,
        "oz": 35.274,
    },

    # Length: base unit m
    # 1 m = 100 cm = 1000 mm = 0.001 km = 3.28084 ft = 39.3701 in
    "m": {
        "cm": 100,
        "mm": 1000,
        "km": 0.001,
        "ft": 3.28084,
        "in": 39.3701,
    },

    # Time - milliseconds: base unit ms
    # 1 ms = 0.001 s = 0.0000166667 min = 0.000000277778 h
    # Also includes 'rmssd' as alias for HRV measurements
    "ms": {
        "s": 0.001,
        "min": 1 / 60000,
        "h": 1 / 3600000,
        "rmssd": 1,  # Alias for HRV RMSSD (Root Mean Square of Successive Differences)
    },

    # Time - minutes: base unit min (for activity time indicators)
    # 1 min = 60 s = 60000 ms = 0.0166667 h = 1 minutes (alias)
    "min": {
        "minutes": 1,  # Vital uses 'minutes', standard is 'min'
        "s": 60,
        "ms": 60000,
        "h": 1 / 60,
    },

    # Time - seconds: base unit s (for some sleep indicators)
    # 1 s = 1000 ms = 0.0166667 min = 1 seconds (alias)
    "s": {
        "seconds": 1,  # Alias
        "ms": 1000,
        "min": 1 / 60,
        "h": 1 / 3600,
    },

    # Time - hours: base unit h
    # 1 h = 60 min = 3600 s = 1 hours (alias)
    "h": {
        "hours": 1,  # Alias
        "min": 60,
        "s": 3600,
        "ms": 3600000,
    },

    # Pressure: base unit mmHg
    # 1 mmHg = 0.133322 kPa = 0.0193368 psi
    "mmHg": {
        "kPa": 0.133322,
        "psi": 0.0193368,
    },

    # Energy: base unit kcal
    # 1 kcal = 1000 cal = 4.184 kJ = 4184 J
    "kcal": {
        "cal": 1000,
        "kJ": 4.184,
        "J": 4184,
    },

    # Concentration - mass-based: base unit mg/dL
    # 1 mg/dL = 10 mg/L = 0.01 g/L (mass concentration, substance-independent)
    # Note: mg/dL <-> mmol/L is substance-dependent, should NOT use generic conversion
    #   Use INDICATOR_SPECIFIC_CONVERSIONS for accurate conversion
    "mg/dL": {
        "g/L": 0.01,  # Mass concentration conversion (substance-independent)
    },

    # Concentration - molar: g/L base (for mass concentration)
    # 1 g/L = 100 mg/dL = 1000 mg/L
    "g/L": {
        "mg/dL": 100,
        "mg/L": 1000,
    },

    # Frequency - count/min: base unit count/min (for heart rate)
    # 1 count/min = 1 bpm = 1 /min = 0.0166667 Hz = 1 breaths/min
    "count/min": {
        "bpm": 1,  # Vital uses 'bpm', standard is 'count/min'
        "/min": 1,
        "Hz": 1 / 60,
        "breaths/min": 1,  # Vital respiratory rate uses 'breaths/min'
    },

    # Percentage: base unit %
    # 1% = 0.01 ratio = 1 spo2 = 1 percent
    "%": {
        "ratio": 0.01,  # Vital efficiency uses ratio (0.97), standard is % (97%)
        "spo2": 1,  # spo2 is already a percentage
        "percent": 1,
    },

    # Volume: base unit L
    # 1 L = 1000 mL = 4.22675 cups
    "L": {
        "mL": 1000,
        "ml": 1000,
        "cup": 4.22675,
    },

    # Temperature: aliases (non-linear conversion handled separately)
    "°C": {
        "degC": 1,  # degC is an alias for °C
        "C": 1,  # C is also an alias
    },

    # Speed: base unit m/s
    # 1 m/s = 3.6 km/hr
    "m/s": {
        "km/hr": 3.6,
    },
    
    # VO2 Max: base unit L/min/kg
    # 1 L/min/kg = 1000 mL/(min·kg) = 1000 mL/kg/min
    "L/min/kg": {
        "mL/(min·kg)": 1000,
        "mL/kg/min": 1000,
    },
}


# ============================================================================
# AUTO-GENERATE COMPLETE CONVERSIONS
# ============================================================================

def _build_complete_conversions(raw_conversions: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, float]]:
    """
    Auto-generate complete bidirectional and transitive conversions from raw config
    
    Example:
        Input:  {"ms": {"s": 1000, "min": 60000}}
        Output: {
            "ms": {"s": 1000, "min": 60000},
            "s": {"ms": 0.001, "min": 60},          # Auto-generated: reverse + transitive
            "min": {"ms": 1/60000, "s": 1/60}       # Auto-generated: reverse + transitive
        }
    
    Args:
        raw_conversions: Raw configuration with only base unit conversions
        
    Returns:
        Complete conversion mapping with all possible conversion paths
    """
    result = {}

    # 1. Copy raw mappings and collect unit groups
    unit_groups = {}  # {base_unit: [base_unit, unit1, unit2, ...]}

    for base_unit, conversions in raw_conversions.items():
        if base_unit not in result:
            result[base_unit] = {}
        result[base_unit].update(conversions)

        # Collect units in the same group
        units_in_group = [base_unit] + list(conversions.keys())
        unit_groups[base_unit] = units_in_group

    # 2. Generate all mutual conversions for each unit group
    for base_unit, units in unit_groups.items():
        base_conversions = raw_conversions[base_unit]

        for from_unit in units:
            for to_unit in units:
                if from_unit == to_unit:
                    continue

                # Ensure from_unit has a conversion dictionary
                if from_unit not in result:
                    result[from_unit] = {}

                # Skip if conversion rule already exists
                if to_unit in result[from_unit]:
                    continue

                # Calculate conversion factor: from_unit -> base_unit -> to_unit
                # from_unit -> base_unit
                if from_unit == base_unit:
                    from_to_base_factor = 1.0
                else:
                    from_to_base_factor = 1.0 / base_conversions[from_unit]

                # base_unit -> to_unit
                if to_unit == base_unit:
                    base_to_to_factor = 1.0
                else:
                    base_to_to_factor = base_conversions[to_unit]

                # Complete conversion factor
                result[from_unit][to_unit] = from_to_base_factor * base_to_to_factor

    return result


# Module-level auto-generation of complete conversions
UNIT_CONVERSIONS: Dict[str, Dict[str, float]] = _build_complete_conversions(_RAW_UNIT_CONVERSIONS)

# ============================================================================
# INDICATOR-SPECIFIC CONVERSIONS (Optional Special Cases)
# ============================================================================

# Only configure indicators that need special conversion logic
INDICATOR_SPECIFIC_CONVERSIONS: Dict[Any, Dict[str, Any]] = {}


# Populate at runtime to avoid circular import
def _populate_indicator_specific_conversions():
    """Populate indicator-specific conversions at runtime
    
    Format: conversions define how to convert FROM the source unit TO standard unit
    {
        "source_unit": {
            "to_standard": lambda v: ...,    # source -> standard
            "from_standard": lambda v: ...   # standard -> source (optional, auto-calculated if not provided)
        }
    }
    """
    if StandardIndicator is None:
        return
    
    # Dietary Water: standard unit is L
    # Water density conversion: 1 L = 1000 mL = 1000 g (at 4°C) = 1 kg
    INDICATOR_SPECIFIC_CONVERSIONS[StandardIndicator.DIETARY_WATER] = {
        "conversions": {
            "g": {
                "to_standard": lambda v: v * 0.001,  # g -> L (assuming water density)
                "from_standard": lambda v: v * 1000,  # L -> g
            },
            "kg": {
                "to_standard": lambda v: v,  # kg -> L (1 kg water = 1 L)
                "from_standard": lambda v: v,  # L -> kg
            }
        }
    }
    
    # Blood Oxygen: standard unit is %
    # Special conversion for PaO2 (arterial oxygen partial pressure)
    # Formula: SpO2 = PaO2 * 0.7 + 30 (non-linear/affine transformation)
    INDICATOR_SPECIFIC_CONVERSIONS[StandardIndicator.BLOOD_OXYGEN] = {
        "conversions": {
            "pao2": {
                "to_standard": lambda v: v * 0.7 + 30,  # PaO2 -> SpO2%
                # Reverse: PaO2 = (SpO2 - 30) / 0.7
                "from_standard": lambda v: (v - 30) / 0.7,  # SpO2% -> PaO2
            },
            "spo2": {
                "to_standard": lambda v: v,  # Already in %
                "from_standard": lambda v: v,
            }
        }
    }

    # Blood Glucose: standard unit is mg/dL
    # mmol/L <-> mg/dL conversion (molar mass: ~180 g/mol)
    # 1 mg/dL = 0.0555 mmol/L
    # 1 mmol/L = 18.0182 mg/dL
    INDICATOR_SPECIFIC_CONVERSIONS[StandardIndicator.BLOOD_GLUCOSE] = {
        "conversions": {
            "mmol/L": {
                "to_standard": lambda v: v * 18.0182,  # mmol/L -> mg/dL
                "from_standard": lambda v: v * 0.0555,  # mg/dL -> mmol/L
            }
        }
    }

    # Cholesterol indicators: standard unit is mmol/L
    # mg/dL <-> mmol/L conversion (molar mass: ~387 g/mol)
    # 1 mg/dL = 0.02586 mmol/L
    # 1 mmol/L = 38.67 mg/dL
    # g/L <-> mmol/L conversion
    # 1 g/L = 2.586 mmol/L
    for cholesterol_indicator in [
        StandardIndicator.CHOLESTEROL_LDL,
        StandardIndicator.CHOLESTEROL_HDL,
        StandardIndicator.CHOLESTEROL_TOTAL,
        StandardIndicator.CHOLESTEROL_TRIGLYCERIDES,
    ]:
        INDICATOR_SPECIFIC_CONVERSIONS[cholesterol_indicator] = {
            "conversions": {
                "mg/dL": {
                    "to_standard": lambda v: v * 0.02586,  # mg/dL -> mmol/L
                    "from_standard": lambda v: v * 38.67,  # mmol/L -> mg/dL
                },
                "g/L": {
                    "to_standard": lambda v: v * 2.586,  # g/L -> mmol/L
                    "from_standard": lambda v: v * 0.387,  # mmol/L -> g/L
                }
            }
        }


# ============================================================================
# CORE API - Public Interface
# ============================================================================

def convert_to_standard(indicator: StandardIndicator, value: float, unit: str, ) -> Tuple[float, str]:
    """
    Convert value to standard unit for the indicator (Core API)
    
    This is the main entry point for unit conversion.
    
    Args:
        indicator: StandardIndicator enum
        value: Value to convert
        unit: Current unit
        
    Returns:
        Tuple[float, str]: (converted_value, standard_unit)
        
    Raises:
        ValueError: If indicator is invalid or conversion fails critically
        
    Examples:
        >>> convert_to_standard(StandardIndicator.HEART_RATE, 75.0, "bpm")
        (75.0, "count/min")
        
        >>> convert_to_standard(StandardIndicator.WEIGHT, 70000.0, "g")
        (70.0, "kg")
    """
    # Validate indicator
    if not indicator or not hasattr(indicator, 'value'):
        raise ValueError(f"Invalid indicator: {indicator}")

    # Get standard unit
    standard_unit = indicator.value.standard_unit

    # If unit is not provided or empty, assume already in standard unit
    if not unit or unit.strip() == "":
        return value, standard_unit

    # Use UnifiedUnitConverter for conversion
    converted_value, result_unit, success = UnifiedUnitConverter.convert(indicator, value, unit)

    if not success:
        # Keep original value and unit (fail gracefully)
        logging.debug(f"No conversion rule for {unit} -> {standard_unit}, keeping original")
        return value, unit

    return converted_value, result_unit


# ============================================================================
# UNIFIED UNIT CONVERTER (Internal Implementation)
# ============================================================================

class UnifiedUnitConverter:
    """
    Unified unit conversion service
    
    Priority:
    1. Indicator-specific conversion logic (if configured)
    2. Generic unit conversion with O(1) lookup
    3. No conversion if no rule exists
    """

    @staticmethod
    def convert(
            indicator: Any,  # StandardIndicator type
            value: float,
            from_unit: str
    ) -> Tuple[float, str, bool]:
        """
        Convert value to standard unit for the indicator
        
        Args:
            indicator: StandardIndicator enum type
            value: Value to convert
            from_unit: Current unit
            
        Returns:
            Tuple[float, str, bool]: (converted_value, target_unit, success)
        """
        # Get standard unit from indicator
        try:
            if hasattr(indicator, 'value') and hasattr(indicator.value, 'standard_unit'):
                standard_unit = indicator.value.standard_unit
            else:
                # Fallback: treat indicator as string
                from .indicators_info import get_standard_unit
                standard_unit = get_standard_unit(str(indicator))
        except Exception as e:
            logging.error(f"Failed to get standard unit for indicator {indicator}: {e}")
            return value, from_unit, False

        # Same unit, no conversion needed
        if from_unit == standard_unit:
            return value, standard_unit, True

        # Check for indicator-specific conversion
        if indicator in INDICATOR_SPECIFIC_CONVERSIONS:
            conversions = INDICATOR_SPECIFIC_CONVERSIONS[indicator].get("conversions", {})
            if from_unit in conversions:
                try:
                    converter = conversions[from_unit]

                    # New format: dict with to_standard/from_standard
                    if isinstance(converter, dict):
                        if "to_standard" in converter:
                            converted_value = converter["to_standard"](value)
                            return converted_value, standard_unit, True
                    # Legacy format: callable or number
                    elif callable(converter):
                        converted_value = converter(value)
                        return converted_value, standard_unit, True
                    else:
                        converted_value = value * converter
                        return converted_value, standard_unit, True
                except Exception as e:
                    logging.warning(f"Indicator-specific conversion failed for {indicator}, falling back to generic: {e}")

        # Generic unit conversion with O(1) lookup
        converted_value, success = convert_unit(value, from_unit, standard_unit)
        if success:
            return converted_value, standard_unit, True
        else:
            # No conversion rule found, keep original
            logging.debug(f"No conversion rule found for {from_unit} -> {standard_unit}")
            return value, from_unit, False


# ============================================================================
# UNIT CONVERSION FUNCTIONS
# ============================================================================

def convert_unit(value: float, from_unit: str, to_unit: str) -> Tuple[float, bool]:
    """
    Generic unit conversion with O(1) lookup (Internal utility function)
    
    NOTE: This function does NOT include indicator-specific conversions.
    For complete conversion with indicator context, use convert_to_standard() instead.
    
    This is used internally by UnifiedUnitConverter for generic conversions.
    
    Examples:
        # Generic conversions (no indicator context)
        convert_unit(1000, "g", "kg")   # (1.0, True)
        convert_unit(1, "kg", "g")      # (1000.0, True)
        convert_unit(1000, "ms", "s")   # (1.0, True)
    
    Args:
        value: Value to convert
        from_unit: Source unit
        to_unit: Target unit
        
    Returns:
        Tuple[float, bool]: (converted_value, success)
    """
    if from_unit == to_unit:
        return value, True

    # Temperature special handling (non-linear)
    if to_unit in ["°C", "°F", "F", "K"] or from_unit in ["°C", "°F", "F", "K"]:
        try:
            converted = _convert_temperature(value, from_unit, to_unit)
            return converted, True
        except Exception:
            return value, False

    # One lookup for conversion (pre-processed at module load time)
    if from_unit in UNIT_CONVERSIONS and to_unit in UNIT_CONVERSIONS[from_unit]:
        return value * UNIT_CONVERSIONS[from_unit][to_unit], True

    return value, False


def _convert_temperature(value: float, from_unit: str, to_unit: str) -> float:
    """Temperature conversion (non-linear)"""
    if from_unit == to_unit:
        return value

    # Handle aliases
    if from_unit in ["degC", "C"]:
        from_unit = "°C"
    if to_unit in ["degC", "C"]:
        to_unit = "°C"
    if from_unit == "F" and from_unit != "°F":
        from_unit = "°F"
    if to_unit == "F" and to_unit != "°F":
        to_unit = "°F"

    # Convert to Celsius
    if from_unit == "°F":
        celsius = (value - 32) * 5 / 9
    elif from_unit == "K":
        celsius = value - 273.15
    elif from_unit == "°C":
        celsius = value
    else:
        raise ValueError(f"Unknown temperature unit: {from_unit}")

    # Convert from Celsius to target unit
    if to_unit == "°F":
        return celsius * 9 / 5 + 32
    elif to_unit == "K":
        return celsius + 273.15
    elif to_unit == "°C":
        return celsius
    else:
        raise ValueError(f"Unknown temperature unit: {to_unit}")


# ============================================================================
# UNIT INFORMATION FOR FRONTEND
# ============================================================================

def get_all_units_info() -> Dict[str, Any]:
    """
    Get all unit information (for frontend display)
    
    This function returns comprehensive unit information including:
    - Unit categories
    - Unit details (name, symbol, category)
    - All unit conversions (auto-generated)
    - Total unit count
    
    Returns:
        Dictionary containing all unit information
    """
    # Unit categories
    unit_categories = {
        "mass": {
            "name": "质量",
            "name_en": "Mass",
            "standard_unit": "kg",
            "units": ["kg", "g"],
            "convertible_units": ["lb", "oz"],
        },
        "length": {
            "name": "长度",
            "name_en": "Length",
            "standard_unit": "m",
            "units": ["m", "cm", "mm"],
            "convertible_units": ["ft", "in", "km"],
        },
        "time": {
            "name": "时间",
            "name_en": "Time",
            "standard_unit": "ms",
            "units": ["ms", "s", "min", "h", "seconds", "minutes", "hours"],
            "convertible_units": [],
        },
        "pressure": {
            "name": "压力",
            "name_en": "Pressure",
            "standard_unit": "mmHg",
            "units": ["mmHg", "kPa"],
            "convertible_units": ["psi"],
        },
        "energy": {
            "name": "能量",
            "name_en": "Energy",
            "standard_unit": "kcal",
            "units": ["kcal", "kJ", "J"],
            "convertible_units": ["cal"],
        },
        "concentration": {
            "name": "浓度",
            "name_en": "Concentration",
            "standard_unit": "mg/dL",
            "units": ["mg/dL", "g/L", "mg/L"],
            "convertible_units": [],
        },
        "concentration_molar": {
            "name": "摩尔浓度",
            "name_en": "Molar Concentration",
            "standard_unit": "mmol/L",
            "units": ["mmol/L"],
            "convertible_units": [],
        },
        "frequency": {
            "name": "频率",
            "name_en": "Frequency",
            "standard_unit": "count/min",
            "units": ["count/min", "bpm", "Hz"],
            "convertible_units": ["/min", "breaths/min"],
        },
        "volume": {
            "name": "体积",
            "name_en": "Volume",
            "standard_unit": "L",
            "units": ["L", "mL", "ml"],
            "convertible_units": ["cup"],
        },
        "temperature": {
            "name": "温度",
            "name_en": "Temperature",
            "standard_unit": "°C",
            "units": ["°C", "°F", "K"],
            "convertible_units": ["F", "degC", "C"],
        },
        "percentage": {
            "name": "百分比",
            "name_en": "Percentage",
            "standard_unit": "%",
            "units": ["%"],
            "convertible_units": ["ratio", "percent", "spo2"],
        },
        "power": {
            "name": "功率",
            "name_en": "Power",
            "standard_unit": "W",
            "units": ["W"],
            "convertible_units": [],
        },
        "speed": {
            "name": "速度",
            "name_en": "Speed",
            "standard_unit": "m/s",
            "units": ["m/s"],
            "convertible_units": ["km/hr"],
        },
        "composite": {
            "name": "复合单位",
            "name_en": "Composite Units",
            "standard_unit": "",
            "units": ["kg/m²", "Ω", "count", "level", "years", "score", "L/min/kg", "ms²", "count/hour"],
            "convertible_units": ["mL/(min·kg)", "mL/kg/min"],
        },
        "special": {
            "name": "特殊单位",
            "name_en": "Special Units",
            "standard_unit": "",
            "units": ["FSU", "unit", "mV", "date", "enum", "boolean", "bool", "ratio"],
            "convertible_units": [],
        },
    }

    # Unit details
    unit_details = {
        # Mass
        "kg": {"name": "千克", "name_en": "Kilogram", "symbol": "kg", "category": "mass"},
        "g": {"name": "克", "name_en": "Gram", "symbol": "g", "category": "mass"},
        "lb": {"name": "磅", "name_en": "Pound", "symbol": "lb", "category": "mass"},
        "oz": {"name": "盎司", "name_en": "Ounce", "symbol": "oz", "category": "mass"},

        # Length
        "m": {"name": "米", "name_en": "Meter", "symbol": "m", "category": "length"},
        "cm": {"name": "厘米", "name_en": "Centimeter", "symbol": "cm", "category": "length"},
        "mm": {"name": "毫米", "name_en": "Millimeter", "symbol": "mm", "category": "length"},
        "ft": {"name": "英尺", "name_en": "Foot", "symbol": "ft", "category": "length"},
        "in": {"name": "英寸", "name_en": "Inch", "symbol": "in", "category": "length"},
        "km": {"name": "千米", "name_en": "Kilometer", "symbol": "km", "category": "length"},

        # Time
        "ms": {"name": "毫秒", "name_en": "Millisecond", "symbol": "ms", "category": "time"},
        "s": {"name": "秒", "name_en": "Second", "symbol": "s", "category": "time"},
        "min": {"name": "分钟", "name_en": "Minute", "symbol": "min", "category": "time"},
        "h": {"name": "小时", "name_en": "Hour", "symbol": "h", "category": "time"},
        "seconds": {"name": "秒", "name_en": "Seconds", "symbol": "seconds", "category": "time"},
        "minutes": {"name": "分钟", "name_en": "Minutes", "symbol": "minutes", "category": "time"},
        "hours": {"name": "小时", "name_en": "Hours", "symbol": "hours", "category": "time"},

        # Pressure
        "mmHg": {"name": "毫米汞柱", "name_en": "Millimeter of Mercury", "symbol": "mmHg", "category": "pressure"},
        "kPa": {"name": "千帕", "name_en": "Kilopascal", "symbol": "kPa", "category": "pressure"},
        "psi": {"name": "磅每平方英寸", "name_en": "Pounds per Square Inch", "symbol": "psi", "category": "pressure"},

        # Energy
        "kcal": {"name": "千卡", "name_en": "Kilocalorie", "symbol": "kcal", "category": "energy"},
        "kJ": {"name": "千焦", "name_en": "Kilojoule", "symbol": "kJ", "category": "energy"},
        "J": {"name": "焦耳", "name_en": "Joule", "symbol": "J", "category": "energy"},
        "cal": {"name": "卡路里", "name_en": "Calorie", "symbol": "cal", "category": "energy"},

        # Power
        "W": {"name": "瓦特", "name_en": "Watt", "symbol": "W", "category": "power"},

        # Concentration
        "mg/dL": {"name": "毫克每分升", "name_en": "Milligrams per Deciliter", "symbol": "mg/dL", "category": "concentration"},
        "mmol/L": {"name": "毫摩尔每升", "name_en": "Millimoles per Liter", "symbol": "mmol/L", "category": "concentration"},
        "g/L": {"name": "克每升", "name_en": "Grams per Liter", "symbol": "g/L", "category": "concentration"},
        "mg/L": {"name": "毫克每升", "name_en": "Milligrams per Liter", "symbol": "mg/L", "category": "concentration"},

        # Frequency
        "bpm": {"name": "次每分钟", "name_en": "Beats per Minute", "symbol": "bpm", "category": "frequency"},
        "Hz": {"name": "赫兹", "name_en": "Hertz", "symbol": "Hz", "category": "frequency"},
        "count/min": {"name": "计数每分钟", "name_en": "Count per Minute", "symbol": "count/min", "category": "frequency"},
        "/min": {"name": "每分钟", "name_en": "Per Minute", "symbol": "/min", "category": "frequency"},
        "breaths/min": {"name": "呼吸每分钟", "name_en": "Breaths per Minute", "symbol": "breaths/min", "category": "frequency"},

        # Volume
        "L": {"name": "升", "name_en": "Liter", "symbol": "L", "category": "volume"},
        "mL": {"name": "毫升", "name_en": "Milliliter", "symbol": "mL", "category": "volume"},
        "ml": {"name": "毫升", "name_en": "Milliliter", "symbol": "ml", "category": "volume"},
        "cup": {"name": "杯", "name_en": "Cup", "symbol": "cup", "category": "volume"},

        # Temperature
        "°C": {"name": "摄氏度", "name_en": "Degree Celsius", "symbol": "°C", "category": "temperature"},
        "°F": {"name": "华氏度", "name_en": "Degree Fahrenheit", "symbol": "°F", "category": "temperature"},
        "K": {"name": "开尔文", "name_en": "Kelvin", "symbol": "K", "category": "temperature"},
        "F": {"name": "华氏度", "name_en": "Fahrenheit", "symbol": "F", "category": "temperature"},
        "degC": {"name": "摄氏度", "name_en": "Degree Celsius", "symbol": "degC", "category": "temperature"},
        "C": {"name": "摄氏度", "name_en": "Celsius", "symbol": "C", "category": "temperature"},

        # Percentage
        "%": {"name": "百分比", "name_en": "Percentage", "symbol": "%", "category": "percentage"},
        "ratio": {"name": "比率", "name_en": "Ratio", "symbol": "ratio", "category": "percentage"},
        "percent": {"name": "百分比", "name_en": "Percent", "symbol": "percent", "category": "percentage"},
        "spo2": {"name": "血氧饱和度", "name_en": "SpO2", "symbol": "spo2", "category": "percentage"},

        # Speed
        "m/s": {"name": "米每秒", "name_en": "Meters per Second", "symbol": "m/s", "category": "speed"},
        "km/hr": {"name": "公里每小时", "name_en": "Kilometers per Hour", "symbol": "km/hr", "category": "speed"},

        # Composite
        "kg/m²": {"name": "千克每平方米", "name_en": "Kilograms per Square Meter", "symbol": "kg/m²", "category": "composite"},
        "Ω": {"name": "欧姆", "name_en": "Ohm", "symbol": "Ω", "category": "composite"},
        "count": {"name": "计数", "name_en": "Count", "symbol": "count", "category": "composite"},
        "level": {"name": "等级", "name_en": "Level", "symbol": "level", "category": "composite"},
        "years": {"name": "年", "name_en": "Years", "symbol": "years", "category": "composite"},
        "score": {"name": "评分", "name_en": "Score", "symbol": "score", "category": "composite"},
        "L/min/kg": {"name": "升每分钟每千克", "name_en": "Liters per Minute per Kilogram", "symbol": "L/min/kg", "category": "composite"},
        "mL/(min·kg)": {"name": "毫升每分钟每千克", "name_en": "Milliliters per Minute per Kilogram", "symbol": "mL/(min·kg)", "category": "composite"},
        "mL/kg/min": {"name": "毫升每千克每分钟", "name_en": "Milliliters per Kilogram per Minute", "symbol": "mL/kg/min", "category": "composite"},
        "ms²": {"name": "毫秒平方", "name_en": "Milliseconds Squared", "symbol": "ms²", "category": "composite"},
        "count/hour": {"name": "计数每小时", "name_en": "Count per Hour", "symbol": "count/hour", "category": "composite"},

        # Special
        "FSU": {"name": "食物敏感单位", "name_en": "Food Sensitivity Unit", "symbol": "FSU", "category": "special"},
        "unit": {"name": "单位", "name_en": "Unit", "symbol": "unit", "category": "special"},
        "mV": {"name": "毫伏", "name_en": "Millivolt", "symbol": "mV", "category": "special"},
        "date": {"name": "日期", "name_en": "Date", "symbol": "date", "category": "special"},
        "enum": {"name": "枚举", "name_en": "Enum", "symbol": "enum", "category": "special"},
        "boolean": {"name": "布尔值", "name_en": "Boolean", "symbol": "boolean", "category": "special"},
        "bool": {"name": "布尔值", "name_en": "Bool", "symbol": "bool", "category": "special"},
    }

    # Build return data
    # Include UNIT_CONVERSIONS and add temperature conversions (handled separately)
    conversions_with_temperature = UNIT_CONVERSIONS.copy()
    
    # Add temperature conversions (non-linear, handled by _convert_temperature)
    # Note: These are placeholders showing relationships, actual conversion uses special function
    conversions_with_temperature["°C"] = {
        "°F": "(v * 9/5 + 32)",  # Formula placeholder
        "K": "(v + 273.15)",
        "degC": 1,  # Alias
        "C": 1,     # Alias
    }
    conversions_with_temperature["°F"] = {
        "°C": "((v - 32) * 5/9)",  # Formula placeholder
        "K": "((v - 32) * 5/9 + 273.15)",
        "F": 1,  # Alias
    }
    conversions_with_temperature["K"] = {
        "°C": "(v - 273.15)",  # Formula placeholder
        "°F": "((v - 273.15) * 9/5 + 32)",
    }
    
    result = {
        "categories": {},
        "units": {},
        "conversions": conversions_with_temperature,  # Include temperature conversions
        "total_units": len(STANDARD_UNITS),
        "generated_at": datetime.now().isoformat(),
    }

    # Process category data
    for category_key, category_info in unit_categories.items():
        all_units = category_info["units"] + category_info["convertible_units"]
        result["categories"][category_key] = {
            "name": category_info["name"],
            "name_en": category_info["name_en"],
            "standard_unit": category_info["standard_unit"],
            "count": len(all_units),
            "units": [],
        }

        for unit in all_units:
            if unit in unit_details:
                unit_data = {
                    "symbol": unit,
                    "name": unit_details[unit]["name"],
                    "name_en": unit_details[unit]["name_en"],
                    "category": category_key,
                    "is_standard": unit in STANDARD_UNITS,
                    "is_convertible": unit in category_info["convertible_units"],
                }

                result["categories"][category_key]["units"].append(unit_data)
                result["units"][unit] = unit_data

    return result


# Initialize indicator-specific conversions at module load
_populate_indicator_specific_conversions()
