"""A reading as a FHIR Observation, the shape `standardize_reading` answers in.

The person's words stay in `code.text`; the LOINC coding is there only when
the resolver answered; a value that is not a number stays as printed
(`valueString`); `_mirobody` says how the answer was reached. An abstention
is an Observation with no `coding`, never an error: "no code" is a result a
caller can store and show.
"""

from __future__ import annotations

from typing import Any

from mirobody.engine.resolver import Resolution, resolve_reading

LOINC_SYSTEM = "http://loinc.org"
UCUM_SYSTEM = "http://unitsofmeasure.org"

#: Comparators FHIR's `Quantity.comparator` admits, keyed by what a report prints.
_COMPARATORS = {"<": "<", "<=": "<=", "≤": "<=", ">": ">", ">=": ">=", "≥": ">="}


def standardize_reading(name: str, value: str | float | None = None, unit: str | None = None) -> dict[str, Any]:
    """One reading, as a FHIR Observation with its LOINC coding.

        standardize_reading("血红蛋白", "13.5", "g/dL")["code"]["coding"][0]["code"]  # "718-7"

    Offline and key-free. The unit takes part in the choice of code (LOINC
    codes the PROPERTY into the identity), so pass it whenever the report
    prints one.
    """
    text = "" if value is None else str(value).strip()
    return observation(name, text, unit or "", resolve_reading(name, text or None, unit or None))


def observation(name: str, value: str, unit: str, resolution: Resolution) -> dict[str, Any]:
    """The Observation for a reading already resolved (`parse_file`'s rows)."""
    from mirobody._bundle import bundle_version

    code: dict[str, Any] = {"text": name}
    if resolution.resolved and resolution.loinc:
        code["coding"] = [{"system": LOINC_SYSTEM, "code": resolution.loinc, "display": resolution.canonical}]
    out: dict[str, Any] = {"resourceType": "Observation", "status": "final", "code": code}
    out.update(_value(value, unit))
    meta: dict[str, Any] = {
        "method": resolution.method or "unresolved",
        "evidence": list(resolution.evidence),
        "axes": list(resolution.axes),
        "unit_recognized": resolution.unit_recognized,
        "bundle": bundle_version(),
    }
    if resolution.rejected_code:
        meta["rejected_code"] = resolution.rejected_code
    if resolution.rejected_reason:
        meta["rejected_reason"] = resolution.rejected_reason
    out["_mirobody"] = meta
    return out


def _value(value: str, unit: str) -> dict[str, Any]:
    """`valueQuantity` for a number, `valueString` for anything else. The unit
    carries its UCUM code only when it normalizes; otherwise it is kept as
    printed and nothing claims it is UCUM."""
    from mirobody.units import normalize_unit, parse_value_unit

    if not value:
        return {}
    # A unit in its own column is read on its own, so one we do not know still
    # leaves the number a number; only a unit glued to the value is parsed out.
    parsed = parse_value_unit(value)
    if parsed.value is None:
        return {"valueString": value}
    printed = unit or ""
    ucum = normalize_unit(printed) if printed else (parsed.unit or "")
    quantity: dict[str, Any] = {"value": parsed.value}
    comparator = _COMPARATORS.get(parsed.comparator or "")
    if comparator:
        quantity["comparator"] = comparator
    if printed or ucum:
        quantity["unit"] = printed or ucum
    if ucum:
        quantity["system"] = UCUM_SYSTEM
        quantity["code"] = ucum
    return {"valueQuantity": quantity}


__all__ = ["LOINC_SYSTEM", "UCUM_SYSTEM", "observation", "standardize_reading"]
