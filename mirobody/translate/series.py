"""The series key: what may be plotted on one axis.

A LOINC code is more specific than a series. 2093-3 (cholesterol, mass
concentration) and 14647-2 (cholesterol, molar concentration) are two codes
for one thing measured in two units, and a chart that splits them shows a
person two half-histories. LOINC's own group file says which codes belong
together (LG55-6 folds mass and molar concentration, LG100-4 folds methods);
the key below is the rule those groups follow, applied to the six axes:

    series_id = loinc:COMPONENT|SYSTEM|TIME|SCALE|dim(PROPERTY)

METHOD is rolled up (a calculated LDL and a direct LDL are one series),
PROPERTY is folded to its dimension so that MCnc and SCnc agree, and the
molar-mass bridge in `mirobody.units` is what makes the VALUES agree too.
An uncoded reading has a series just the same, keyed by its own fold:

    series_id = local:<name_key>|<unit>

Two people's `local:` keys can name different things; two people's `loinc:`
keys cannot. That is the whole difference the prefix carries.
"""

from __future__ import annotations

from dataclasses import dataclass

LOINC_PREFIX = "loinc:"
LOCAL_PREFIX = "local:"

#: PROPERTY values whose mass and substance forms are one dimension. LOINC
#: spells the pair with an M or S prefix; the fold replaces both with Q.
_MASS_MOLAR_PAIRS = frozenset({"Cnc", "Cnt", "Rat", "Fr", "Crto", "Rto", "CncDiff", "CntDiff"})


@dataclass(frozen=True)
class Axes:
    """The six LOINC axes of one code, verbatim from the release."""

    component: str
    property: str
    time: str
    system: str
    scale: str
    method: str = ""


def property_dim(prop: str) -> str:
    """`MCnc` and `SCnc` -> `QCnc`; everything else stays itself."""
    p = (prop or "").strip()
    if len(p) > 1 and p[0] in "MS" and p[1:] in _MASS_MOLAR_PAIRS:
        return "Q" + p[1:]
    return p


def series_id(axes: Axes) -> str:
    return LOINC_PREFIX + "|".join((
        axes.component.strip(),
        axes.system.strip(),
        axes.time.strip(),
        axes.scale.strip(),
        property_dim(axes.property),
    ))


def local_series_id(local_key: str) -> str:
    return LOCAL_PREFIX + local_key


def is_standard(series: str) -> bool:
    return series.startswith(LOINC_PREFIX)


__all__ = ["Axes", "LOCAL_PREFIX", "LOINC_PREFIX", "is_standard", "local_series_id", "property_dim", "series_id"]
