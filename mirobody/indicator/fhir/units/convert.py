"""UCUM unit conversion — so one indicator can be one series in one unit.

Three tiers, degrading in order:

    T1  same dimension    :func:`scale` parses UCUM prefix × base unit into a
                          (dimension signature, factor). Zero domain knowledge:
                          cm↔m, kg↔[lb_av], mg/dL↔g/L, U/L↔[IU]/L (1:1),
                          10*9/L↔/uL all live here.
    T2  mass ↔ substance  Off by a molar mass. :data:`MOLAR_MASS` is keyed by
                          LOINC code (see the three conventions in its comment).
                          Not in the table ⇒ no conversion, degrade to T3.
    T3  genuinely not     `%` (a fraction) vs `10*9/L` (an absolute count) needs
                          that draw's own WBC total: a DERIVED calculation, not
                          a unit conversion. Callers keep those as two series.

**Do not use** :func:`~.families.unit_family` **to decide convertibility.** It
is a LOINC PROPERTY classifier, not a dimension table, and it is wrong in both
directions for this purpose — measured, not supposed:

    kg/m2 (BMI)  -> MCnc      mg/dL -> MCnc      same family, NOT convertible
    U/L          -> CCnc      [IU]/L -> ACnc     different families, 1:1 IDENTICAL

"Same family ⇒ convertible" therefore turns a BMI of 24 into a mass
concentration, and refuses a conversion that is the identity. A dimension
signature rejects the first and accepts the second by construction.

`None` from :func:`scale` is **not an error** — it means "atomic unit": equal
only to a unit spelled exactly the same way. `%`, `mm[Hg]`, `个/HP`, `meq/L` all
take that path. Better to decline than to guess.

Original values are never rewritten. Provenance, right-to-be-forgotten and FHIR
fidelity all depend on the reading as recorded; conversion is for the caller
that is about to compare or chart, and it hands back a number rather than
editing one.

The tier split and the conventions below come from a design worked out and
validated against real unit-conversion cases, not invented for this module.

**Not to be confused with** :func:`mirobody.pulse.standardize.units.convert_to_standard`,
which is a different job on the other side of the pipeline: it takes a
``StandardIndicator`` enum member and converts to *that device indicator's*
declared canonical unit (① Collect, one target per indicator). This module takes
two arbitrary UCUM strings and asks whether they are interconvertible at all
(② Standardize, no target). Use that one to canonicalize a device sample; use
this one to compare two readings.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Optional

from .families import UCUM_FAMILY

__all__ = [
    "DimScale",
    "scale",
    "convertible",
    "conversion_factor",
    "convert_value",
    "partition_units",
    "MOLAR_MASS",
]

#: SI prefix → power of ten. `da` must be tried before `d`; `_atom` sorts by
#: length descending to guarantee longest-match.
_PREFIX: dict[str, float] = {
    "y": 1e-24, "z": 1e-21, "a": 1e-18, "f": 1e-15, "p": 1e-12, "n": 1e-9,
    "u": 1e-6, "m": 1e-3, "c": 1e-2, "d": 1e-1, "da": 1e1, "h": 1e2,
    "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
}

#: Base unit → (dimension tag, relative factor). The tags are private notation
#: whose only job is deciding whether two units can convert:
#:   M mass · V volume · N amount of substance · L length · T time · A activity
#: `1` is dimensionless (counts); its factor folds into the scalar and stays OUT
#: of the signature — otherwise `10*9/L` and `/L` would read as different
#: dimensions.
_BASE: dict[str, tuple[str, float]] = {
    "g": ("M", 1.0),
    "L": ("V", 1.0),
    "l": ("V", 1.0),
    "mol": ("N", 1.0),
    # `eq` (equivalents) is deliberately ABSENT: mEq↔mmol differs by valence
    # (1 mmol Ca2+ = 2 mEq), so a 1:1 conversion is silently off by a factor of
    # two. `meq/L` therefore parses as None — equal only to itself.
    "m": ("L", 1.0),
    "s": ("T", 1.0),
    "min": ("T", 60.0),
    "h": ("T", 3600.0),
    "d": ("T", 86400.0),
    "U": ("A", 1.0),
    "[IU]": ("A", 1.0),   # in LOINC usage U/L ≡ [IU]/L
    "[lb_av]": ("M", 453.59237),
    "[oz_av]": ("M", 28.349523125),
    "[in_i]": ("L", 0.0254),
    "[ft_i]": ("L", 0.3048),
    "1": ("1", 1.0),
}

_DIMENSIONLESS = "1"

#: (dimension signature, factor to that dimension's own base). Equal signatures
#: convert directly (T1).
DimScale = tuple[tuple[tuple[str, int], ...], float]


def _atom(token: str) -> Optional[tuple[str, float]]:
    """One UCUM atom → (dimension tag, factor); None when unrecognized."""
    if not token:
        return None
    if token in _BASE:
        return _BASE[token]
    if token.startswith("10*"):          # count scales: 10*9, 10*12, 10*3
        try:
            return (_DIMENSIONLESS, 10.0 ** int(token[3:]))
        except ValueError:
            return None
    # Longest prefix first: `da` before `d`, and `m` (milli) must not eat
    # `min` / `mol` — both already matched in _BASE above.
    for prefix in sorted(_PREFIX, key=len, reverse=True):
        if token.startswith(prefix):
            rest = token[len(prefix):]
            if rest in _BASE:
                dim, factor = _BASE[rest]
                if dim == _DIMENSIONLESS:
                    return None          # nonsense combinations like `m1`
                return (dim, factor * _PREFIX[prefix])
    return None


def scale(ucum: str | None) -> Optional[DimScale]:
    """UCUM string → (dimension signature, factor), or None when unparseable.

    Only units :data:`~.families.UCUM_FAMILY` already knows are parsed, so noise
    like `mgL` cannot be composed into a plausible-looking factor. That is the
    same guard :func:`~.normalize.normalize_unit` applies.
    """
    if not ucum or ucum not in UCUM_FAMILY:
        return None
    numerator, _, denominator = ucum.partition("/")
    dims: dict[str, int] = defaultdict(int)
    factor = 1.0
    for part, sign in ((numerator, 1), (denominator, -1)):
        if not part:
            continue
        for token in part.split("."):
            exponent = 1
            # Trailing power: `m2` → m^2. The digits in `10*9` are a scale, not
            # a power, so that form is excluded.
            if len(token) > 1 and token[-1].isdigit() and not token.startswith("10*"):
                token, exponent = token[:-1], int(token[-1])
            atom = _atom(token)
            if atom is None:
                return None
            dim, atom_factor = atom
            if dim != _DIMENSIONLESS:
                dims[dim] += sign * exponent
            factor *= atom_factor ** (sign * exponent)
    signature = tuple(sorted((d, e) for d, e in dims.items() if e))
    return (signature, factor)


#: Mass concentration (M/V) ↔ substance concentration (N/V), bridged by molar
#: mass: LOINC code → (g_per_mol, basis, note).
#:
#: **Keyed by code, never by indicator name.** Three conventions that will be
#: got wrong if they are not written down:
#:
#:   1. Triglyceride uses a CONVENTIONAL average molar mass (triolein ≈ 885.4),
#:      not the mass of one determinate molecule. The 88.57 factor is an
#:      industry convention, not something computed from a formula.
#:   2. BUN is reported as NITROGEN, urea as the whole molecule — a factor of
#:      ~2.14 apart. They get one row each and must never share.
#:   3. Conversion happens only WITHIN one code. Across codes, however close
#:      clinically, is concept mapping, and this table does not do that.
#:
#: A finite, checkable table of physical constants with a golden vector per row
#: — not an open-ended correction KB.
MOLAR_MASS: dict[str, tuple[float, str, str]] = {
    # Glucose metabolism
    "1558-6": (180.16, "C6H12O6", "Fasting glucose; 1 mmol/L = 18.016 mg/dL"),
    "2345-7": (180.16, "C6H12O6", "Glucose, serum/plasma"),
    "2339-0": (180.16, "C6H12O6", "Glucose, blood"),
    # Lipids. The cholesterol family shares 386.65; triglyceride is convention.
    "2093-3": (386.65, "C27H46O", "Total cholesterol; 1 mmol/L = 38.67 mg/dL"),
    "2089-1": (386.65, "C27H46O", "LDL-C"),
    "13457-7": (386.65, "C27H46O", "LDL-C (calculated)"),
    "2085-9": (386.65, "C27H46O", "HDL-C"),
    "2571-8": (885.40, "conventional average", "Triglyceride; factor 88.57 is convention, not composition"),
    # Renal / metabolites
    "2160-0": (113.12, "C4H7N3O", "Creatinine; 1 mg/dL = 88.4 umol/L"),
    "3084-1": (168.11, "C5H4N4O3", "Urate; 1 mg/dL = 59.48 umol/L"),
    "3094-0": (28.02, "2xN", "BUN — reported as NITROGEN; 2.14x from urea (6299-2), never shared"),
    "6299-2": (60.06, "CH4N2O", "Urea — reported as the whole molecule"),
    # Bilirubin
    "1975-2": (584.66, "C33H36N4O6", "Total bilirubin; 1 mg/dL = 17.10 umol/L"),
    "1968-7": (584.66, "C33H36N4O6", "Direct (conjugated) bilirubin"),
    "1971-1": (584.66, "C33H36N4O6", "Indirect (unconjugated) bilirubin"),
    # Electrolytes / minerals
    "17861-6": (40.08, "Ca", "Calcium; 1 mg/dL = 0.2495 mmol/L"),
    "2777-1": (30.97, "P", "Inorganic phosphate — as the phosphorus atom, not phosphate"),
    "19123-9": (24.305, "Mg", "Magnesium"),
    "2498-4": (55.845, "Fe", "Iron"),
    "2823-3": (39.098, "K", "Potassium"),
    "2951-2": (22.990, "Na", "Sodium"),
}

_MASS_PER_VOLUME = (("M", 1), ("V", -1))
_SUBSTANCE_PER_VOLUME = (("N", 1), ("V", -1))


def conversion_factor(from_unit: str, to_unit: str, *, loinc_code: str = "") -> Optional[float]:
    """`value_in_to_unit = value_in_from_unit * factor`; None when not convertible.

    T1 compares factors within one dimension; T2 crosses M/V ↔ N/V when
    `loinc_code` has a molar mass.
    """
    if from_unit == to_unit:
        return 1.0
    src, dst = scale(from_unit), scale(to_unit)
    if src is None or dst is None:
        return None
    (src_sig, src_factor), (dst_sig, dst_factor) = src, dst
    if src_sig == dst_sig:                                   # T1
        return src_factor / dst_factor
    molar = MOLAR_MASS.get(loinc_code or "")
    if molar is None:                                        # no bridge: T3
        return None
    grams_per_mol = molar[0]
    # Base dimensions are g/L and mol/L: g/L → mol/L divides by g/mol.
    if src_sig == _MASS_PER_VOLUME and dst_sig == _SUBSTANCE_PER_VOLUME:
        return (src_factor / grams_per_mol) / dst_factor
    if src_sig == _SUBSTANCE_PER_VOLUME and dst_sig == _MASS_PER_VOLUME:
        return (src_factor * grams_per_mol) / dst_factor
    return None


def convertible(from_unit: str, to_unit: str, *, loinc_code: str = "") -> bool:
    """Whether the two units can be interconverted (T1 or T2)."""
    return conversion_factor(from_unit, to_unit, loinc_code=loinc_code) is not None


def convert_value(value: float, from_unit: str, to_unit: str, *, loinc_code: str = "") -> Optional[float]:
    """Convert one number; None when not convertible.

    **Nothing is rounded.** Full precision is kept; format at the output layer,
    where the display unit's usual precision is known. A None must not be
    treated as zero — it means these readings belong to separate series.
    """
    factor = conversion_factor(from_unit, to_unit, loinc_code=loinc_code)
    return None if factor is None else value * factor


def partition_units(units: Iterable[str], *, loinc_code: str = "") -> list[list[str]]:
    """Split a set of units into mutually-convertible classes.

    Membership propagates (a↔b and b↔c ⇒ one class), so `mg/dL` and `umol/L`
    join `mmol/L` and `mg/L` in one class when a molar-mass bridge exists. Input
    order is preserved inside each class.
    """
    classes: list[list[str]] = []
    for unit in units:
        if not unit:
            continue
        for members in classes:
            if convertible(members[0], unit, loinc_code=loinc_code):
                members.append(unit)
                break
        else:
            classes.append([unit])
    return classes
