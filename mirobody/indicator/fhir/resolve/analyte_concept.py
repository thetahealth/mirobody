"""Analyte-concept gate — hard-filter LOINC candidates whose name
carries only the opposite of the query's last Ag/Ab marker.

Query rule (last-token wins): scan the query text for any Ag/Ab
marker; the rightmost match decides the required concept. Indicator
paths like ``血型单特异性抗体鉴定·A抗原`` carry both ``抗体`` and
``抗原`` — the leaf analyte (``A 抗原``) is the intent, so taking the
LAST occurrence yields ``Ag``.

Filter rule (conservative): given the query concept,

* ``Ag`` → drop LOINC rows whose name has Ab/antibody but NOT
  Ag/antigen (Ab-only rows are wrong-side; combo Ag+Ab panels survive).
* ``Ab`` → drop LOINC rows whose name has Ag/antigen but NOT
  Ab/antibody (mirror).

Rows containing neither token, and combo rows containing both, are
unaffected — they're not on the wrong side of the Ag/Ab boundary.

LOINC consistently uses the ``Ag`` / ``Ab`` two-letter abbreviation,
but ~4k report / panel / multi-analyte names use the long forms
``antigen`` / ``antibody`` instead (``Human platelet antigen report
Document``, ``Blood group antibody screen``, ``Monoclonal Antibody``).
The corpus masks cover both surface forms.
"""

from __future__ import annotations

import logging
import re

import numpy as np

from ..common import SYSTEM_TO_CODE, _CODE_BITS

log = logging.getLogger(__name__)


# Multilingual surface markers per concept. Edit-friendly: append the
# language to the appropriate list and recompile.
#
# Long forms (English / European / Cyrillic / CJK / Korean) compile
# case-insensitively. CJK + Hangul + Kana markers match as bare
# substrings — regex ``\b`` doesn't work between consecutive letter
# characters in those scripts (``抗原检测``: both 原 and 检 are word
# chars, the boundary fails), so suffix-attached uses like ``抗原检测``
# / ``항원이다`` / ``抗原を測定`` are still caught. Latin / Cyrillic
# markers wrap with ``\b...\b`` so ``antigen`` doesn't fire inside
# ``antigenic`` and ``антиген`` doesn't fire inside ``антигенный``.
_LONG_MARKERS: dict[str, list[str]] = {
    "Ag": [
        "antigen", "antigens",               # English
        "antígeno", "antígenos",             # Spanish
        "antigène", "antigènes",             # French
        "Antigen", "Antigene",               # German (case-insens covers all forms)
        "антиген", "антигены",               # Russian (sg / pl nominative)
        "抗原",                              # Chinese (simp+trad), Japanese (kanji)
        "항원",                              # Korean
    ],
    "Ab": [
        "antibody", "antibodies",            # English
        "anticuerpo", "anticuerpos",         # Spanish
        "anticorps",                         # French (sg+pl identical)
        "Antikörper",                        # German (sg+pl identical)
        "антитело", "антитела",              # Russian (sg / pl nominative)
        "抗体",                              # Chinese (simp), Japanese (kanji)
        "抗體",                              # Chinese (trad)
        "항체",                              # Korean
    ],
}

# Two-letter LOINC abbreviations. CASE-SENSITIVE so ``\bAb\b`` doesn't
# fire on blood-type ``AB`` (``AB 血型`` / ``AB blood group``) and
# ``\bAg\b`` doesn't fire on caps-only ``AG``. LOINC display names use
# canonical-case ``Ag`` / ``Ab``, so corpus and query share one form.
_SHORT_MARKERS: dict[str, list[str]] = {
    "Ag": ["Ag"],
    "Ab": ["Ab"],
}

# Scripts where ``\b`` is unreliable between consecutive letter chars.
# Markers containing any of these chars match as bare substrings.
_BARE_SUBSTRING_SCRIPT_RE = re.compile(
    r"[぀-ヿ"       # Hiragana, Katakana
    r"㐀-䶿"        # CJK Extension A
    r"一-鿿"        # CJK Unified Ideographs
    r"가-힯]"       # Hangul Syllables
)


def _compile_token_re() -> tuple[re.Pattern[str], dict[str, str]]:
    """Build the combined query regex and ``token-lowercased → concept``
    classification table from the marker lists above."""
    parts: list[str] = []
    classify: dict[str, str] = {}
    for concept, markers in _LONG_MARKERS.items():
        for m in markers:
            classify[m.lower()] = concept
            esc = re.escape(m)
            if _BARE_SUBSTRING_SCRIPT_RE.search(m):
                parts.append(rf"(?i:{esc})")
            else:
                parts.append(rf"(?i:\b{esc}\b)")
    for concept, markers in _SHORT_MARKERS.items():
        for m in markers:
            classify[m.lower()] = concept
            parts.append(rf"\b{re.escape(m)}\b")
    return re.compile("|".join(parts)), classify


_QUERY_AGAB_RE, _TOKEN_TO_CONCEPT = _compile_token_re()


def query_analyte_concept(text: str) -> str | None:
    """Return ``"Ag"`` / ``"Ab"`` / ``None`` based on the LAST Ag/Ab
    marker in *text*.

    Recognizes the surface marker in English, Chinese (simp + trad),
    Japanese, Korean, Spanish, German, French, Russian, plus the
    two-letter LOINC abbreviations ``Ag`` / ``Ab`` (case-sensitive to
    avoid colliding with blood-type ``AB``).

    Last-wins: indicator hierarchies (``parent · child``) put the
    actual analyte at the leaf. ``血型单特异性抗体鉴定·A抗原`` has
    ``抗体`` in the parent class and ``抗原`` in the child — the child
    is the intent. Same logic for English ``Antibody panel · A
    antigen``, Korean ``항체 패널 · A 항원``, etc.
    """
    if not text:
        return None
    last = None
    for m in _QUERY_AGAB_RE.finditer(text):
        last = m
    if last is None:
        return None
    return _TOKEN_TO_CONCEPT.get(last.group(0).lower())


# Mirror the query-side rule: exact-case ``Ag`` / ``Ab`` (LOINC's
# canonical spelling), case-insensitive long forms.
_LOINC_AG_NAME_RE = re.compile(r"\bAg\b|(?i:\bantigens?\b)")
_LOINC_AB_NAME_RE = re.compile(r"\bAb\b|(?i:\bantibod(?:y|ies)\b)")


def loinc_concept_masks(cache: dict) -> tuple[np.ndarray, np.ndarray] | None:
    """Return (ag_mask, ab_mask) bool[N] over LOINC rows, cached on
    *cache* under ``_analyte_concept_masks``. Returns ``None`` when the
    cache has no display names loaded.

    Non-LOINC rows are always False so AND-merging with the system mask
    doesn't accidentally include them.
    """
    cached = cache.get("_analyte_concept_masks")
    if cached is not None:
        return cached
    names = cache.get("names")
    if names is None:
        return None
    canonical = np.asarray(cache["canonical"])
    sys_arr = (canonical >> _CODE_BITS) & 0x7
    is_loinc = sys_arr == SYSTEM_TO_CODE["LOINC"]
    n = canonical.shape[0]
    ag = np.zeros(n, dtype=bool)
    ab = np.zeros(n, dtype=bool)
    for i in np.where(is_loinc)[0]:
        nm = names[i]
        if not nm:
            continue
        if _LOINC_AG_NAME_RE.search(nm):
            ag[i] = True
        if _LOINC_AB_NAME_RE.search(nm):
            ab[i] = True
    cache["_analyte_concept_masks"] = (ag, ab)
    log.info(
        "analyte concept masks: Ag=%d, Ab=%d, both=%d / %d LOINC rows",
        int(ag.sum()), int(ab.sum()), int((ag & ab).sum()),
        int(is_loinc.sum()),
    )
    return ag, ab


def concept_keep_mask(cache: dict, concept: str) -> np.ndarray | None:
    """Compose the conservative keep-mask for *concept* (``"Ag"`` or
    ``"Ab"``): drop opposite-only rows, keep everything else.

    Returns ``None`` when masks aren't available (no display names) —
    caller skips the AND-merge in that case.
    """
    masks = loinc_concept_masks(cache)
    if masks is None:
        return None
    ag, ab = masks
    if concept == "Ag":
        # drop rows with Ab but NOT Ag (Ab-only); keep Ag-only, both, neither
        return ~(ab & ~ag)
    if concept == "Ab":
        # mirror
        return ~(ag & ~ab)
    return None


__all__ = [
    "query_analyte_concept",
    "loinc_concept_masks",
    "concept_keep_mask",
]
