"""Terminology tools — ② Translate, exposed over MCP.

This is the engine's differentiator on the tool surface. Every other health MCP
server hands the model *vendor-shaped* data and leaves naming to chance:
"LDL cholesterol", "低密度脂蛋白胆固醇" and "LDL-C" arrive as three unrelated
strings. These tools turn any of them into the same canonical LOINC identity.

Two properties worth stating plainly, because they are unusual for an MCP tool:

* **No user data.** Nothing here reads a user's records, so the tools work for
  an anonymous caller and disclose nothing. They are pure terminology.
* **No network, no API key.** Resolution runs against the data bundles shipped
  inside the package (~46 MB, Git LFS). A client can be air-gapped and these
  still answer — which is exactly the property health data deserves.

Measured coverage of the resolver these tools call: see
``mirobody/test_engine_coverage.py`` (94 everyday panel terms across English,
中文 and 日本語).
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class TerminologyService:
    """Canonical-code lookup for health indicator names."""

    def __init__(self):
        self.name = "Terminology Service"
        self.version = "1.0.0"

    async def resolve_indicator(self, names: list[str]) -> dict[str, Any]:
        """
        Resolve health indicator names to canonical LOINC codes. Offline, no
        user data — safe for any term. Any language and clinical shorthand:
        "LDL-C", "低密度脂蛋白胆固醇" and "ヘモグロビン" all resolve.

        Use whenever a name needs a standard identity: storing a reading,
        comparing values from different labs or devices, or a test name
        written in another language.

        Args:
            names: Names exactly as printed. Pass the whole batch in one call.

        Returns:
            results: per input, in order — name (unchanged), resolved, loinc
                (e.g. "718-7", empty when unresolved), canonical (LOINC long
                common name), candidates (matched corpus rows; a large number
                means genuine ambiguity and one sensible default was chosen —
                surface that when precision matters).

        Notes for LLMs:
            - Unresolved is an honest "no": report it unmatched, never invent
              a code.
            - PANEL names ("blood pressure", "血圧") deliberately do not
              resolve — ask for the specific measurement (systolic/diastolic).
            - Same code from two names = same test. That, not string equality,
              decides whether two readings are comparable.
        """
        if not isinstance(names, list) or not names:
            return {"success": False, "error": "names must be a non-empty list of strings."}
        if len(names) > 200:
            return {"success": False, "error": "Too many names in one call (max 200)."}

        try:
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
            return {
                "success": True,
                "message": f"{matched}/{len(results)} resolved",
                "results": results,
            }
        except Exception as e:
            logger.error(f"[resolve_indicator] {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    #-------------------------------------------------------------------------

    async def convert_unit(
        self,
        value: float,
        from_unit: str,
        to_unit: str,
        loinc_code: str = "",
    ) -> dict[str, Any]:
        """
        Convert one measurement between units. Offline, no user data. Use this
        before comparing or charting readings that were recorded in different
        units — never scale a value by hand.

        Args:
            value: The number as recorded, e.g. 5.6.
            from_unit: Unit it is in now, as printed, e.g. "mmol/L".
            to_unit: Unit wanted, e.g. "mg/dL".
            loinc_code: The reading's LOINC code. Required only to cross between
                mass and substance concentration (mg/dL <-> mmol/L), which needs
                that analyte's molar mass. Omit for same-dimension conversions.

        Returns:
            converted: the value in `to_unit`, or null when the two units cannot
                be converted — which is an ANSWER, not a failure: report the
                readings separately with their own units rather than scaling
                one to look like the other. Percentages and absolute counts, and
                anything needing a molar mass this engine does not carry, land
                here. `reason` says which case it was.
        """
        from mirobody.units import (
            convert_value, normalize_unit as _norm,
        )

        src = _norm(from_unit) or from_unit
        dst = _norm(to_unit) or to_unit
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
        return {
            "success": True,
            "converted": converted,
            "from_ucum": src,
            "to_ucum": dst,
        }

    async def normalize_unit(self, units: list[str]) -> dict[str, Any]:
        """
        Normalize free-text measurement units to canonical UCUM form. Offline,
        no user data. The same unit gets written many ways ("mg/dL", "MG/DL",
        "毫摩尔每升") — normalize before comparing or converting values.

        Args:
            units: Unit strings as printed, e.g. ["mg/dL", "毫摩尔每升", "次/分"].

        Returns:
            results: per input, in order — unit (unchanged), ucum (canonical
                form; empty means unrecognized — say so rather than assuming),
                family (LOINC PROPERTY, e.g. "SCnc"). `family` classifies the
                property; it does NOT tell you what converts — `kg/m2` (BMI)
                and `mg/dL` share the family MCnc and cannot convert, while
                `U/L` and `[IU]/L` are in different families and are the same
                unit. Call `convert_unit` for that question.
        """
        if not isinstance(units, list) or not units:
            return {"success": False, "error": "units must be a non-empty list of strings."}
        if len(units) > 200:
            return {"success": False, "error": "Too many units in one call (max 200)."}

        try:
            from mirobody.units import normalize_unit, unit_family

            results = []
            for raw in units:
                if not isinstance(raw, str) or not raw.strip():
                    continue
                ucum = normalize_unit(raw) or ""
                results.append({
                    "unit": raw,
                    "ucum": ucum,
                    "family": (unit_family(ucum) or "") if ucum else "",
                })

            matched = sum(1 for x in results if x["ucum"])
            return {
                "success": True,
                "message": f"{matched}/{len(results)} normalized",
                "results": results,
            }
        except Exception as e:
            logger.error(f"[normalize_unit] {e}", exc_info=True)
            return {"success": False, "error": str(e)}
