"""A reading as a FHIR Observation, the shape `standardize_reading` answers in.

The person's words stay in `code.text`; the LOINC coding is there only when
the resolver answered; a value that is not a number stays as printed
(`valueString`); the `coding-decision` extension says how the answer was
reached, and `decision()` reads it back. An abstention is an Observation with
no `coding`, never an error: "no code" is a result a caller can store and show.

The decision is an extension because R4 JSON admits nothing else at that
level: a top-level `_mirobody` key failed strict validation (fhir.resources).
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from mirobody.engine.resolver import Resolution, resolve_reading

LOINC_SYSTEM = "http://loinc.org"
UCUM_SYSTEM = "http://unitsofmeasure.org"
DECISION_URL = "https://mirobody.ai/fhir/StructureDefinition/coding-decision"

#: Comparators FHIR's `Quantity.comparator` admits, keyed by what a report prints.
_COMPARATORS = {"<": "<", "<=": "<=", "≤": "<=", ">": ">", ">=": ">=", "≥": ">="}


def standardize_reading(name: str, value: str | float | None = None, unit: str | None = None) -> dict[str, Any]:
    """One reading, as a FHIR Observation with its LOINC coding.

        standardize_reading("血红蛋白", "13.5", "g/dL")["code"]["coding"][0]["code"]  # "718-7"

    Offline and key-free. The unit takes part in the choice of code (LOINC
    codes the PROPERTY into the identity), so pass it whenever the report
    prints one.
    """
    if not str(name or "").strip():
        raise ValueError("a reading needs a name")
    text = "" if value is None else str(value).strip()
    resolution = resolve_reading(name, text or None, unit or None)
    complaint = _complaint(name, text)
    if complaint:
        # 发烧 reached 103717-5 (Crimean-Congo hemorrhagic fever virus RNA) and
        # 高血压 41218-9 (urine hippurate): the lexical index cannot abstain
        # from a word that is not an analyte. A diagnosis with a value is kept
        # (`HIV 阴性` is a test result); a complaint never is.
        resolution = replace(resolution, resolved=False, loinc="", axes=("", "", ""),
                             rejected_code=resolution.loinc, **complaint)
    return observation(name, text, unit or "", resolution)


def _complaint(name: str, value: str) -> dict[str, Any] | None:
    """What `standardize_complaint` would answer, when the name is a complaint
    (or a bare diagnosis) rather than something measured."""
    from mirobody.translate.icpc3 import resolve_condition, resolve_symptom

    for kind, coding in (("symptom", resolve_symptom(name)), ("condition", None if value else resolve_condition(name))):
        if coding is not None and coding.outcome == "coded" and coding.code:
            return {
                "method": "refused",
                "rejected_reason": f"{name!r} is a {kind}, not a reading: use standardize_complaint (ICPC-3 {coding.code})",
            }
    return None


def decision_extension(fields: dict[str, Any]) -> dict[str, Any]:
    """The `coding-decision` extension over `fields`, one sub-extension per
    value (a list repeats its key); empty values are left out."""
    parts = []
    for key, value in fields.items():
        for v in value if isinstance(value, list | tuple) else [value]:
            if v is None or v == "":
                continue
            parts.append({"url": key, "valueBoolean": v} if isinstance(v, bool) else {"url": key, "valueString": str(v)})
    return {"url": DECISION_URL, "extension": parts}


def decision(resource: dict[str, Any]) -> dict[str, Any]:
    """The `coding-decision` extension of an Observation as a dict. A key that
    occurs more than once (`evidence`) reads as a list."""
    out: dict[str, Any] = {}
    for ext in resource.get("extension") or []:
        if ext.get("url") != DECISION_URL:
            continue
        for part in ext.get("extension") or []:
            value = part.get("valueString", part.get("valueBoolean"))
            key = part["url"]
            if key in out:
                out[key] = [*(out[key] if isinstance(out[key], list) else [out[key]]), value]
            else:
                out[key] = [value] if key == "evidence" else value
    return out


def observation(name: str, value: str, unit: str, resolution: Resolution) -> dict[str, Any]:
    """The Observation for a reading already resolved (`parse_file`'s rows)."""
    from mirobody._bundle import bundle_version

    code: dict[str, Any] = {"text": name}
    if resolution.resolved and resolution.loinc:
        code["coding"] = [{"system": LOINC_SYSTEM, "code": resolution.loinc, "display": resolution.canonical}]
    out: dict[str, Any] = {"resourceType": "Observation", "status": "final", "code": code}
    out.update(_value(value, unit))
    prop, scale, system = (list(resolution.axes) + ["", "", ""])[:3]
    out["extension"] = [decision_extension({
        "method": resolution.method or "unresolved",
        "evidence": list(resolution.evidence),
        "property": prop,
        "scale": scale,
        "system": system,
        "unitRecognized": resolution.unit_recognized,
        "bundle": bundle_version(),
        "rejectedCode": resolution.rejected_code,
        "rejectedReason": resolution.rejected_reason,
    })]
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


__all__ = ["DECISION_URL", "LOINC_SYSTEM", "UCUM_SYSTEM", "decision", "decision_extension", "observation", "standardize_reading"]
