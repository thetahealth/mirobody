"""The terminology tools' bodies, shared by every surface that offers them.

`resolve_indicator`, `normalize_unit` and `convert_unit` are served by the
HTTP MCP server and the chat agent (`agent/tools/terminology_service.py`) and
by the stdio server (`mcp/stdio.py`); `standardize_complaint` by the stdio
server. The bodies live here so that one set of answers backs all of them:
two copies of a tool drift, and a client cannot tell which one it met.

No user data, no network, no key: each answer comes from the vocabularies
shipped in the package.
"""

from __future__ import annotations

from typing import Any

MAX_BATCH = 200


def resolve_indicators(names: list[str]) -> dict[str, Any]:
    """Names as printed to LOINC codes, in order."""
    if not isinstance(names, list) or not names:
        return {"success": False, "error": "names must be a non-empty list of strings."}
    if len(names) > MAX_BATCH:
        return {"success": False, "error": f"Too many names in one call (max {MAX_BATCH})."}
    from mirobody.engine import get_resolver

    resolver = get_resolver()
    results = []
    for raw in names:
        if not isinstance(raw, str) or not raw.strip():
            continue
        r = resolver.resolve(raw)
        results.append({
            "name": raw,
            "resolved": r.resolved,
            "loinc": r.loinc,
            "canonical": r.canonical,
            "candidates": r.candidates,
        })
    matched = sum(1 for x in results if x["resolved"])
    return {"success": True, "message": f"{matched}/{len(results)} resolved", "results": results}


def normalize_units(units: list[str]) -> dict[str, Any]:
    """Units as printed to canonical UCUM and their LOINC PROPERTY family."""
    if not isinstance(units, list) or not units:
        return {"success": False, "error": "units must be a non-empty list of strings."}
    if len(units) > MAX_BATCH:
        return {"success": False, "error": f"Too many units in one call (max {MAX_BATCH})."}
    from mirobody.units import normalize_unit, unit_family

    results = []
    for raw in units:
        if not isinstance(raw, str) or not raw.strip():
            continue
        ucum = normalize_unit(raw) or ""
        results.append({"unit": raw, "ucum": ucum, "family": (unit_family(ucum) or "") if ucum else ""})
    matched = sum(1 for x in results if x["ucum"])
    return {"success": True, "message": f"{matched}/{len(results)} normalized", "results": results}


def convert_unit(value: float, from_unit: str, to_unit: str, loinc_code: str = "") -> dict[str, Any]:
    """One value between two units; `converted` is None when they do not convert."""
    from mirobody.units import convert_value, normalize_unit

    src = normalize_unit(from_unit) or from_unit
    dst = normalize_unit(to_unit) or to_unit
    try:
        converted = convert_value(float(value), src, dst, loinc_code=(loinc_code or "").strip())
    except (TypeError, ValueError):
        return {"success": False, "error": "value must be a number."}
    if converted is None:
        return {
            "success": True,
            "converted": None,
            "from_ucum": src,
            "to_ucum": dst,
            "reason": (
                "These units are not interconvertible. Either they measure "
                "different things (a percentage is not an absolute count), "
                "or the conversion needs a molar mass this engine does not "
                "carry for that code. Report the readings separately."
            ),
        }
    return {"success": True, "converted": converted, "from_ucum": src, "to_ucum": dst}


def standardize_complaint(text: str, kind: str = "symptom") -> dict[str, Any]:
    """A complaint (`kind="symptom"`) or a named diagnosis (`"condition"`) in
    the person's own words, as a FHIR Observation coded on ICPC-3. The words
    stay in `code.text`; `coding` is absent when the vocabulary abstains, and
    the `coding-decision` extension's `reason` says why."""
    from mirobody.translate.icpc3 import ICPC3_SYSTEM, resolve_condition, resolve_symptom

    words = (text or "").strip()
    if not words:
        return {"success": False, "error": "text must be a non-empty string."}
    if kind not in ("symptom", "condition"):
        return {"success": False, "error": 'kind must be "symptom" or "condition".'}
    coding = resolve_symptom(words) if kind == "symptom" else resolve_condition(words)
    code: dict[str, Any] = {"text": words}
    if coding.outcome == "coded" and coding.code:
        code["coding"] = [{"system": ICPC3_SYSTEM, "code": coding.code, "display": coding.display}]
    from mirobody.engine.observation import decision_extension

    fields = {"kind": kind, "outcome": coding.outcome, "release": coding.release, "reason": coding.reason}
    return {"resourceType": "Observation", "status": "final", "code": code, "extension": [decision_extension(fields)]}


__all__ = ["MAX_BATCH", "convert_unit", "normalize_units", "resolve_indicators", "standardize_complaint"]
