"""What KIND of result is this, and therefore which LOINC scales can produce it.

Much of what a lab report prints is not a number, and over two-fifths of the
shipped LOINC axis is not ``Qn`` (mostly ``Ord``, then ``Nom``, ``SemiQn`` and
``OrdQn``). 尿蛋白 阴性, 便隐血 ++, 血型 O, HBsAg non-reactive are all real
readings whose correct code is not quantitative, so a resolver that only knows
how to constrain numbers is blind to much of the corpus.

``SCALE_TYP`` is the axis that answers it, and an observed value tells you which
class it must belong to:

    "5.6"           qn      Qn      SemiQn  OrdQn
    "++" / "微量"    ord     Ord     OrdQn   SemiQn
    "阴性" / "O+"    nom     Nom     Ord     OrdQn
    free prose      nar     Nar     Doc

(`nom` admitting `Ord` is not sloppiness: see :data:`GATE_SCALES`. LOINC codes
a urine dipstick negative as ``Ord``, and urine glucose has no ``Nom`` variant
at all, so a Nom-only filter admits nothing.)

The consequence for a gate is that a non-numeric value is not "no information".
It is the OPPOSITE constraint: a `阴性` reading must not be answered with a
``Qn`` mass-concentration code any more than a `5.6 mmol/L` reading may be
answered with an ``Ord`` presence code.

One vocabulary for two readers: the resolver constrains a code's SCALE with
it (`engine.resolver`) and `translate.parse` classifies a value's kind with it,
so the two cannot disagree about what `阴性` or `-2.5` is.
"""

from __future__ import annotations

import re

__all__ = [
    "GATE_SCALES",
    "VALUE_NOM_TOKENS",
    "classify_value",
    "scales_for_value",
]

# Ordinal markers: the graded results a report prints for a dipstick or a
# smear. Simplified and traditional Chinese, Japanese, Korean, plus "+/-" and
# ASCII "trace". A bare ``+`` / ``-`` is ambiguous with positive/negative
# (which is Nom, not Ord); this matches only when the marker is the WHOLE
# token, so "+" is Ord while "(+)" reaches the nominal list below.
_VALUE_ORD_RE = re.compile(
    r"^\s*(?:"
    r"[+-]{1,4}"                     # +, ++, +++, ++++, -
    r"|\+/\-"                        # +/-
    r"|[1-4]\s*\+"                   # 1+, 2+
    r"|trace"
    r"|微量|可疑"
    r"|微量陽性|微量阳性"
    r")\s*$",
    re.IGNORECASE,
)

# Numeric, optionally signed and with a comparator and a unit: "20", "20.5",
# "-2.5", "1.2e-3", "<10", ">100", "20 mg/dL", "5.0×10^6/L". The comparator is
# for below- and above-limit reports, which are numbers with an edge, not
# prose. A bare "+" or "-" never gets here: the ordinal test runs first.
_VALUE_QN_RE = re.compile(
    r"^\s*[<>≤≥]?\s*[-+]?"
    r"\d+(?:\.\d+)?(?:[eE][+-]?\d+)?"
    r"(?:\s*[×x*]\s*10[\^]?[+-]?\d+)?"
    r"(?:\s*[^\d].*)?$"              # any trailing unit/text
)

# A signed whole number and nothing else. "+1" and "+2" are how many reports
# print a dipstick grade, and read as a quantity they coded 尿蛋白 +1 to the
# mass-concentration code; with a decimal or a unit ("-2.5", "-2 mmol/L") a
# signed number is a quantity.
_SIGNED_WHOLE_RE = re.compile(r"^\s*[-+]\s*\d+\s*$")

# Nominal tokens, short categorical labels: blood-type letters (A/B/AB/O ±
# Rh), positive/negative, reactive/non-reactive serology. Multilingual because
# the report is.
VALUE_NOM_TOKENS: frozenset[str] = frozenset({
    # English
    "positive", "negative", "pos", "neg", "reactive", "non-reactive",
    "nonreactive", "detected", "not detected", "present", "absent",
    "(+)", "(-)",
    # Blood types
    "a", "b", "ab", "o", "a+", "a-", "b+", "b-", "ab+", "ab-", "o+", "o-",
    "rh+", "rh-", "rh positive", "rh negative",
    # CJK
    "阳性", "阴性", "陽性", "陰性",
    "陽", "陰",
    "陽性反応", "陰性反応",
    "양성", "음성",
    # Spanish / German / French / Russian
    "positivo", "negativo",
    "positiv", "negativ",
    "positif", "négatif", "negatif",
    "положительный", "отрицательный",
})

def classify_value(value: str | None) -> str | None:
    """An observed value -> its scale class, or None when there is nothing to go on.

    Order matters: ordinal first, so a bare ``"+"`` is not swallowed by the
    numeric branch's optional leading sign; then numeric; then the nominal
    tokens by exact match; anything else with content left is narrative.
    """
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if _VALUE_ORD_RE.match(s):
        return "ord"
    if s.lower() in VALUE_NOM_TOKENS:
        return "nom"
    if _VALUE_QN_RE.match(s) and not _SIGNED_WHOLE_RE.match(s):
        return "qn"
    return "nar"


#: scale class -> the SCALE_TYP values that may ADMIT it: a hard filter, so
#: being narrow here is wrong, not conservative. ``nom`` shows why. LOINC is not
#: consistent about scaling a positive/negative result: a urine dipstick is
#: ``Ord`` and has no ``Nom`` variant at all, so a Nom-only filter admits
#: nothing and the gate starves, while a few interpretations are ``Nom``
#: (``Choriogonadotropin [Interpretation]``). The gate accepts both.
GATE_SCALES: dict[str, tuple[str, ...]] = {
    "qn": ("Qn", "SemiQn", "OrdQn"),
    "ord": ("Ord", "OrdQn", "SemiQn"),
    "nom": ("Nom", "Ord", "OrdQn"),
    "nar": ("Nar", "Doc"),
}


def scales_for_value(value: str | None) -> frozenset[str] | None:
    """The admissible ``SCALE_TYP`` set for a value, or None to place no constraint.

    None means the value was empty or unclassifiable, not that anything goes,
    but that this signal has nothing to say and some other one must decide.
    """
    cls = classify_value(value)
    if cls is None:
        return None
    admissible = GATE_SCALES.get(cls)
    return frozenset(admissible) if admissible else None
