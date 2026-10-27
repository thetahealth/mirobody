"""Mass <-> substance conversions must give the physiologically right number.

`normalize_unit` answering UCUM only says the unit was recognised. Whether a
value becomes the right number is a different mechanism: mass <-> substance
needs the analyte's molar mass, keyed by LOINC code in `units.convert`.
A wrong molar mass turned 150 mg/dL of triglyceride into 3.88 mmol/L instead of
1.69 — the module comments record that exact bug, which is why this test exists.

Every expected value below is chemistry (IUPAC atomic weights), not a
measurement from any dataset: the same conversion rules hold for a hospital
report and for a research spreadsheet.
"""

from __future__ import annotations

from mirobody.standardize import terminology

#: analyte, from, to, value, expected, loinc ("" when no code is needed)
CONVERSIONS = (
    ("glucose", "mg/dL", "mmol/L", 90.0, 4.995, "2339-0", "MW 180.16"),
    ("cholesterol", "mg/dL", "mmol/L", 200.0, 5.172, "2093-3", "MW 386.65"),
    ("triglycerides", "mg/dL", "mmol/L", 150.0, 1.693, "2571-8", "MW 885.4, TG-specific"),
    ("HDL", "mg/dL", "mmol/L", 50.0, 1.293, "2085-9", "MW 386.65"),
    ("LDL", "mg/dL", "mmol/L", 130.0, 3.362, "2089-1", "MW 386.65"),
    ("creatinine", "mg/dL", "umol/L", 1.0, 88.4, "2160-0", "MW 113.12"),
    ("calcium", "mg/dL", "mmol/L", 9.5, 2.371, "17861-6", "MW 40.08"),
    ("hemoglobin", "g/dL", "g/L", 14.0, 140.0, "718-7", "decimal prefix"),
    ("weight", "lb", "kg", 154.0, 69.85, "", "0.45359237"),
    ("height", "in", "cm", 70.0, 177.8, "", "2.54"),
    ("temperature", "Cel", "[degF]", 37.0, 98.6, "8310-5", "affine, not a factor"),
)

TOLERANCE = 0.01


def _convert(frm, to, value, loinc):
    return terminology.convert_unit(value, frm, to, loinc)


def test_physiological_conversions_are_correct():
    """Each pair must land within 1% of its textbook equivalence."""
    wrong = []
    for name, frm, to, value, expected, loinc, _note in CONVERSIONS:
        out = _convert(frm, to, value, loinc)
        if not out.get("success"):
            wrong.append((name, frm, to, "conversion refused", out.get("error")))
            continue
        got = out["converted"]
        rel = abs(got - expected) / abs(expected)
        if rel > TOLERANCE:
            wrong.append((name, frm, to, round(got, 4), round(expected, 4)))
    assert not wrong, f"conversions outside 1%: {wrong}"


def test_molar_mass_is_per_analyte():
    """Triglyceride must not borrow the cholesterol molar mass.

    150 mg/dL of TG is 1.69 mmol/L; with cholesterol's 386.65 it would read 3.88,
    which is the difference between "normal" and "severely elevated".
    """
    out = _convert("mg/dL", "mmol/L", 150.0, "2571-8")
    assert out.get("success"), out
    assert 1.6 < out["converted"] < 1.8, out["converted"]


def test_affine_conversion_is_not_a_factor():
    """Temperature is affine: 37 C is 98.6 F, and 0 C must be 32 F, not 0."""
    out = _convert("Cel", "[degF]", 0.0, "")
    assert out.get("success"), out
    assert abs(out["converted"] - 32.0) < 0.5, out["converted"]


def test_unknown_unit_is_refused_not_guessed():
    """A unit we do not know must leave the value unconverted, with a reason.

    `success` reports that the CALL was well formed; `converted is None` plus a
    `reason` is how a refusal is expressed. A number that passed through
    unchanged would be the dangerous outcome.
    """
    out = _convert("furlongs", "m", 1.0, "")
    assert out.get("converted") is None, out
    assert out.get("reason"), out


def test_printed_unit_spellings_normalize_without_changing_their_scale():
    from mirobody.units import normalize_unit, parse_value_unit

    spellings = {
        "kg/m**2": "kg/m2", "ｋｇ／ｍ＊＊２": "kg/m2", "Novolin R, IU": "[IU]",
        "胰岛素，IU": "[IU]", "ohms": "Ohm", "1000 cell/uL": "10*3/uL",
        "million cells/uL": "10*6/uL", "mmol/Kg": "mmol/kg", "kcal/day": "kcal/d",
    }
    for printed, expected in spellings.items():
        assert normalize_unit(printed) == expected, printed
    for printed, expected in (("24 kg/m**2", "kg/m2"), ("240 1000 cell/uL", "10*3/uL"),
                              ("4.5 million cells/uL", "10*6/uL")):
        assert parse_value_unit(printed).unit == expected, printed


def test_comma_separated_competing_units_are_refused():
    from mirobody.units import normalize_unit

    for printed in ("mg/dL, mmol/L", "mmol/L, mg/dL", "mg/dL, mg/L", "mg/dL，mmol/L"):
        assert normalize_unit(printed) is None, printed
        assert _convert(printed, "mg/dL", 90.0, "2339-0")["converted"] is None


if __name__ == "__main__":  # runs without pytest, same as the axis test
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  PASS {name}")
        except AssertionError as exc:
            failures += 1
            print(f"  FAIL {name}: {str(exc)[:200]}")
        except Exception as exc:
            failures += 1
            print(f"  ERROR {name}: {exc!r}")
    raise SystemExit(1 if failures else 0)
