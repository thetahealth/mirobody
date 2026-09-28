"""Terminology tools: ② Translate, exposed over MCP.

This is the engine's differentiator on the tool surface. Every other health MCP
server hands the model *vendor-shaped* data and leaves naming to chance:
"LDL cholesterol", "低密度脂蛋白胆固醇" and "LDL-C" arrive as three unrelated
strings. These tools turn any of them into the same canonical LOINC identity.

Two properties worth stating plainly, because they are unusual for an MCP tool:

* **No user data.** Nothing here reads a user's records, so the tools work for
  an anonymous caller and disclose nothing. They are pure terminology.
* **No network, no API key.** Resolution runs against the data bundles shipped
  inside the package (~46 MB, Git LFS). A client can be air-gapped and these
  still answer, which is exactly the property health data deserves.

Measured coverage of the resolver these tools call: see
``mirobody/tests/test_engine_coverage.py`` (94 everyday panel terms across English,
中文 and 日本語). The bodies are `mirobody.translate.terminology`, which the stdio
server (`mcp/stdio.py`) calls too, so both transports answer alike.
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
        user data: safe for any term. Any language and clinical shorthand:
        "LDL-C", "低密度脂蛋白胆固醇" and "ヘモグロビン" all resolve.

        Use whenever a name needs a standard identity: storing a reading,
        comparing values from different labs or devices, or a test name
        written in another language.

        Args:
            names: Names exactly as printed. Pass the whole batch in one call.

        Returns:
            results, per input, in order: name (unchanged), resolved, loinc
                (e.g. "718-7", empty when unresolved), canonical (LOINC long
                common name), candidates (matched corpus rows; a large number
                means genuine ambiguity and one sensible default was chosen:
                surface that when precision matters).

        Notes for LLMs:
            - Unresolved is an honest "no": report it unmatched, never invent
              a code.
            - A PANEL name answers with the panel's code ("blood pressure"
              gives 85354-9), which is not the code of any one value in it:
              resolve the member ("systolic blood pressure") for a reading.
              A name for a family of tests ("血脂", "lipid panel") is refused.
            - Same code from two names = same test. That, not string equality,
              decides whether two readings are comparable.
        """
        from mirobody.translate.terminology import resolve_indicators

        try:
            return resolve_indicators(names)
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
        units, never scale a value by hand.

        Args:
            value: The number as recorded, e.g. 5.6.
            from_unit: Unit it is in now, as printed, e.g. "mmol/L".
            to_unit: Unit wanted, e.g. "mg/dL".
            loinc_code: The reading's LOINC code. Required only to cross between
                mass and substance concentration (mg/dL <-> mmol/L), which needs
                that analyte's molar mass. Omit for same-dimension conversions.

        Returns:
            converted: the value in `to_unit`, or null when the two units cannot
                be converted, which is an ANSWER, not a failure: report the
                readings separately with their own units rather than scaling
                one to look like the other. Percentages and absolute counts, and
                anything needing a molar mass this engine does not carry, land
                here. `reason` says which case it was.
        """
        from mirobody.translate.terminology import convert_unit

        return convert_unit(value, from_unit, to_unit, loinc_code)

    async def normalize_unit(self, units: list[str]) -> dict[str, Any]:
        """
        Normalize free-text measurement units to canonical UCUM form. Offline,
        no user data. The same unit gets written many ways ("mg/dL", "MG/DL",
        "毫摩尔每升"): normalize before comparing or converting values.

        Args:
            units: Unit strings as printed, e.g. ["mg/dL", "毫摩尔每升", "次/分"].

        Returns:
            results, per input, in order: unit (unchanged), ucum (canonical
                form; empty means unrecognized: say so rather than assuming),
                family (LOINC PROPERTY, e.g. "SCnc"). `family` classifies the
                property; it does NOT tell you what converts: `kg/m2` (BMI)
                and `mg/dL` share the family MCnc and cannot convert, while
                `U/L` and `[IU]/L` are in different families and are the same
                unit. Call `convert_unit` for that question.
        """
        from mirobody.translate.terminology import normalize_units

        try:
            return normalize_units(units)
        except Exception as e:
            logger.error(f"[normalize_unit] {e}", exc_info=True)
            return {"success": False, "error": str(e)}
