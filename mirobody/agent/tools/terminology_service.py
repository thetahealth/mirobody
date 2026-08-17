"""Terminology tools — ② Sort, exposed over MCP.

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

from typing import Any


class TerminologyService:
    """Canonical-code lookup for health indicator names."""

    def __init__(self):
        self.name = "Terminology Service"
        self.version = "1.0.0"

    async def resolve_indicator(self, names: list[str]) -> dict[str, Any]:
        """
        Resolve health indicator names to canonical LOINC codes. Offline: no
        network, no API key, no user data — safe to call for any term.

        Use this whenever an indicator name needs a standard identity: before
        storing a reading, when comparing values that came from different labs
        or devices, or when the user writes a test name in another language.
        Accepts any language and everyday clinical shorthand — "LDL cholesterol",
        "LDL-C", "低密度脂蛋白胆固醇" and "ヘモグロビン" all resolve.

        Args:
            names: Indicator names exactly as they appear on the report or
                device. Pass the whole batch in one call rather than looping —
                e.g. ["hemoglobin", "空腹血糖", "HbA1c"].

        Returns:
            results: one entry per input name, in order, each with
                - name: the input, unchanged
                - resolved: whether a canonical identity was found
                - loinc: the LOINC code (e.g. "718-7"), empty when unresolved
                - canonical: the LOINC long common name
                - candidates: how many corpus rows matched. A large number means
                  the term was genuinely ambiguous (specimen, method or timing)
                  and one sensible default was chosen — surface that to the user
                  when precision matters.

        Notes for LLMs:
            - An unresolved term is an honest "no", never a guess. Do not
              invent a code for it; report it as unmatched.
            - Names that describe a PANEL rather than one observation (e.g.
              "blood pressure", "血圧") deliberately do not resolve — ask for
              the specific measurement (systolic / diastolic) instead.
            - Same code from two different names means the same test. This is
              the reliable way to decide whether two readings are comparable;
              string equality is not.
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
            import logging
            logging.error(f"[resolve_indicator] {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    #-------------------------------------------------------------------------

    async def normalize_unit(self, units: list[str]) -> dict[str, Any]:
        """
        Normalize free-text measurement units to canonical UCUM form. Offline,
        no user data.

        Lab reports and devices write the same unit many ways — "mg/dL",
        "MG/DL", "毫摩尔每升", "次每分钟". Normalize before comparing or
        converting values, otherwise unit strings silently disagree.

        Args:
            units: Unit strings as printed, e.g. ["mg/dL", "毫摩尔每升", "次/分"].

        Returns:
            results: one entry per input, in order, each with
                - unit: the input, unchanged
                - ucum: the canonical UCUM unit, empty when unrecognized
                - family: the LOINC PROPERTY family (e.g. "SCnc" substance
                  concentration). Two units in the SAME family are convertible;
                  across families they are not comparable at all.

        Notes for LLMs:
            - Use `family` as the guard before any unit conversion. Converting
              across families (e.g. a mass concentration to a count) is a
              category error, not an arithmetic one.
            - An empty `ucum` means the string was not recognized — say so
              rather than assuming a unit.
        """
        if not isinstance(units, list) or not units:
            return {"success": False, "error": "units must be a non-empty list of strings."}
        if len(units) > 200:
            return {"success": False, "error": "Too many units in one call (max 200)."}

        try:
            from mirobody.indicator.fhir.units import normalize_unit, unit_family

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
            import logging
            logging.error(f"[normalize_unit] {e}", exc_info=True)
            return {"success": False, "error": str(e)}
