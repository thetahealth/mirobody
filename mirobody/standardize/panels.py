"""Names that are a measurement only under their section's heading.

An ECG page prints `HR`, `RV5` and `SV1`, and nothing in the alias index
answers them: each was stored under a `local:` key of its own (#281). The
abbreviation alone does not say what was measured, so it is not taught to
the resolver, which answers a name wherever it is printed. Here it is
answered only when the reading carries its panel's heading
(`observations.Draft.panel_text`): `HR` under `心电图` is a heart rate, and
`HR` on a lab page is still nobody's guess.

What this is not: a panel code. A heading is not coded, and a name that the
resolver answers right on its own is answered by the resolver whatever its
heading. The rows are `res/loinc/panel_terms.tsv`; this module is its reader
and the rule that tells which panel a printed heading names.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
from functools import lru_cache
from importlib import resources

from mirobody.zh_fold import fold_to_hans

from .fold import name_key

PANEL_TERMS_FILE = "panel_terms.tsv"

PANEL_ECG = "ecg"
PANEL_URINE = "urine"
PANEL_THYROID = "thyroid"

#: What a printed heading says, after NFKC, casefold and the Traditional fold.
#: `心电` covers `心电图`, `心电图检查` and, folded, `心電圖`/`心電図`; the Latin
#: abbreviations stand alone, so a word that merely contains the letters is
#: no ECG. A urine panel prints `尿常规`, `尿液分析`, `尿液干化学` or
#: `Urinalysis`; `尿沉渣` (microscopy, where `LEU` is a count per field) and
#: `尿酸` (uric acid, a serum test) are not it. A thyroid panel prints
#: `甲状腺功能`, `甲功五项` or `Thyroid Function`.
_HEADINGS = {
    PANEL_ECG: re.compile(r"心电|electrocardiogra|(?<![a-z])(?:ecg|ekg)(?![a-z])"),
    PANEL_URINE: re.compile(r"尿常规|尿液(?!沉渣)|尿干化学|尿检|urinalysis|(?<![a-z])urine(?![a-z])"),
    PANEL_THYROID: re.compile(r"甲状腺|甲功|thyroid"),
}


def panel_of(panel_text: str) -> str:
    """The panel a printed section heading names (`PANEL_ECG`,
    `PANEL_URINE`, `PANEL_THYROID`), or `""` when it names none this module
    has terms for."""
    if not panel_text:
        return ""
    folded = fold_to_hans(unicodedata.normalize("NFKC", panel_text).casefold())
    return next((panel for panel, heading in _HEADINGS.items() if heading.search(folded)), "")


@lru_cache(maxsize=1)
def _terms() -> dict[tuple[str, str], str]:
    """`(panel, name_key) -> LOINC` from the shipped TSV."""
    text = resources.files("mirobody").joinpath("res", "loinc", PANEL_TERMS_FILE).read_text(encoding="utf-8")
    body = "\n".join(line for line in text.splitlines() if line and not line.startswith("#"))
    return {
        (r["panel"].strip(), name_key(r["term"])): r["loinc"].strip()
        for r in csv.DictReader(io.StringIO(body), delimiter="\t")
    }


def panel_code(panel_text: str, key: str) -> tuple[str, str] | None:
    """`(panel, loinc)` for a reading whose folded name is `key` and which
    was printed under `panel_text`; None when the heading names no panel or
    the panel has no row for the name."""
    panel = panel_of(panel_text)
    loinc = _terms().get((panel, key)) if panel else None
    return (panel, loinc) if loinc else None


__all__ = ["PANEL_ECG", "PANEL_TERMS_FILE", "PANEL_THYROID", "PANEL_URINE", "panel_code", "panel_of"]
