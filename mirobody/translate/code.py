"""`code()`: a name, a unit and a value kind become one Coding, or a reason.

Pure over the shipped vocabulary. The caller looks up any alias a person
confirmed and hands it in; the clock and the database stay outside, so the
same inputs give the same `decision_id` on every machine and a recode under
a newer release is a replay of this function over the frozen extraction.

The order of authority:

1. a confirmed alias, user scope before global: a person's word about their
   own report outranks the vocabulary;
2. the lexical resolver, `mirobody.engine.resolve_reading`, which picks the
   analyte by name and the variant by unit. Only its `lexical` method is
   accepted as an identity; a semantic guess cannot abstain and is not one;
3. the scale gate: a number is not coded to an ordinal presence code, and a
   "negative" is not coded to a mass concentration.

Anything else is `needs-input` with the reason spelled out, never a guessed
code and never an empty identity: the reading keeps its local series.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache

from mirobody import units
from .outcome import (
    LOINC_SYSTEM,
    OUTCOME_CODED,
    OUTCOME_NEEDS_INPUT,
    OUTCOME_REFUSED,
    Alias,
    Coding,
)
from .parse import KIND_ABSENT, KIND_NARRATIVE, KIND_NOMINAL, KIND_ORDINAL, KIND_QUANTITY
from .series import Axes, local_series_id, series_id

RULE_ALIAS = "alias"
RULE_ENGINE = "engine:lexical"

#: The SCALE_TYP values a value kind may be coded to. `absent` places no
#: constraint. The nominal row admits `Ord` on purpose: LOINC codes a urine
#: dipstick negative as ordinal and has no nominal variant for it.
_SCALES: dict[str, frozenset[str]] = {
    KIND_QUANTITY: frozenset({"Qn", "OrdQn", "SemiQn"}),
    KIND_ORDINAL: frozenset({"Ord", "OrdQn", "SemiQn"}),
    KIND_NOMINAL: frozenset({"Nom", "Ord", "OrdQn"}),
    KIND_NARRATIVE: frozenset({"Nar", "Doc", "Nom"}),
}


@lru_cache(maxsize=1)
def release() -> str:
    """The vocabulary release every coding names. The bundle's own version
    string, or `none` when no bundle is installed: a coding made without
    a vocabulary must say so rather than claim one."""
    try:
        from mirobody._bundle import bundle_version
        return bundle_version() or "none"
    except Exception:
        return "none"


def decision_id(name_key: str, unit_ucum: str, value_kind: str, rel: str, rule: str) -> str:
    """Sixteen hex characters of SHA-256 over the inputs a coding depends
    on. Two readings that fold to the same key, unit and kind share one
    decision under one release and one rule, by construction."""
    joined = "\x1f".join((name_key, unit_ucum or "", value_kind, rel, rule))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def axes_for(loinc: str) -> tuple[Axes, str] | None:
    """`(axes, display)` of a code in the shipped bundle, or `None`.

    The bundle carries five of the six axes; TIME is not among them and is
    left empty until the 2.83 tables ship. A series key built without TIME
    is still a key, and a recode under the next release rewrites it.
    """
    from mirobody.engine import get_resolver

    row = get_resolver().axes_of(loinc)
    if row is None:
        return None
    component, prop, scale, system, method, display = row
    return Axes(component, prop, "", system, scale, method), display


def code(
    name_text: str,
    *,
    name_key: str,
    local_key: str,
    value_kind: str,
    value_text: str = "",
    unit_text: str = "",
    unit_ucum: str = "",
    value_num: float | None = None,
    alias: Alias | None = None,
) -> Coding:
    """One reading to one `Coding`. See the module docstring for the order
    of authority. `local_key` is the series of the reading when it stays
    uncoded; `name_key` is what the decision is shared under."""
    rel = release()
    local = local_series_id(local_key)

    if alias is not None:
        rule = f"{RULE_ALIAS}:{alias.scope}"
        did = decision_id(name_key, unit_ucum, value_kind, rel, rule)
        if not alias.code:
            return Coding(OUTCOME_REFUSED, local, did, rule, rel, reason="alias:not-standard")
        if alias.code_system and alias.code_system != LOINC_SYSTEM:
            # A code in another namespace (the device catalogue's own) has no
            # LOINC axes; its series is the code itself, shared by everyone.
            return Coding(
                OUTCOME_CODED, f"{alias.code_system}:{alias.code}", did, rule, rel,
                code_system=alias.code_system, code=alias.code, display=alias.code, evidence=("alias",),
            )
        return _coded(alias.code, did, rule, rel, local, value_kind, value_num, unit_ucum, ("alias",))

    from mirobody.engine import resolve_reading

    hit = resolve_reading(name_text, value_text or None, unit_text or unit_ucum or None)
    evidence = (
        f"term={name_text}", f"method={hit.method}", f"candidates={hit.candidates}",
        "axes=" + ",".join(hit.evidence), f"unit_recognized={hit.unit_recognized}",
    )
    did = decision_id(name_key, unit_ucum, value_kind, rel, RULE_ENGINE)
    if hit.rejected_code and not hit.loinc:
        # The name reached a code and the printed unit contradicts every code
        # of that analyte: one of the two was read wrong, and a person can say
        # which. Not a refusal (that is a decision never to answer) and never
        # the contradicted code.
        return Coding(
            OUTCOME_NEEDS_INPUT, local, did, RULE_ENGINE, rel, reason="unit:conflict",
            evidence=evidence + (f"rejected={hit.rejected_code}", f"why={hit.rejected_reason}"),
        )
    if hit.method == "refused" and hit.rejected_reason:
        # "Plateletcrit (PCT)": the name and its parenthetical resolve to two
        # analytes and the resolver will not pick by position. A person can,
        # so this is an open question, not the closed refusal a category
        # word ("血脂") gets.
        return Coding(
            OUTCOME_NEEDS_INPUT, local, did, RULE_ENGINE, rel, reason="name:ambiguous",
            evidence=evidence + (f"why={hit.rejected_reason}",),
        )
    if hit.method == "refused":
        return Coding(OUTCOME_REFUSED, local, did, RULE_ENGINE, rel, reason="engine:refused", evidence=evidence)
    if not hit.resolved or not hit.loinc:
        return Coding(OUTCOME_NEEDS_INPUT, local, did, RULE_ENGINE, rel, reason="engine:no-match", evidence=evidence)
    if hit.method != "lexical":
        return Coding(OUTCOME_NEEDS_INPUT, local, did, RULE_ENGINE, rel, reason=f"engine:untrusted:{hit.method}", evidence=evidence)
    return _coded(hit.loinc, did, RULE_ENGINE, rel, local, value_kind, value_num, unit_ucum, evidence + (f"canonical={hit.canonical}",))


def _coded(
    loinc: str,
    did: str,
    rule: str,
    rel: str,
    local: str,
    value_kind: str,
    value_num: float | None,
    unit_ucum: str,
    evidence: tuple[str, ...],
) -> Coding:
    found = axes_for(loinc)
    if found is None:
        return Coding(OUTCOME_NEEDS_INPUT, local, did, rule, rel, reason=f"axes:unknown-code:{loinc}", evidence=evidence)
    axes, display = found
    admissible = _SCALES.get(value_kind)
    if value_kind != KIND_ABSENT and admissible is not None and axes.scale and axes.scale not in admissible:
        return Coding(
            OUTCOME_NEEDS_INPUT, local, did, rule, rel,
            reason=f"scale:{value_kind}-vs-{axes.scale}", evidence=evidence + (f"candidate={loinc}",),
        )
    canonical_value = canonical_unit = None
    if value_kind == KIND_QUANTITY and value_num is not None and unit_ucum:
        folded = units.canonicalize(value_num, unit_ucum, loinc_code=loinc)
        canonical_value, canonical_unit = folded.value, folded.unit
    return Coding(
        OUTCOME_CODED, series_id(axes), did, rule, rel,
        code_system=LOINC_SYSTEM, code=loinc, display=display, axes=axes,
        value_canonical=canonical_value, unit_canonical=canonical_unit, evidence=evidence,
    )


__all__ = ["RULE_ALIAS", "RULE_ENGINE", "axes_for", "code", "decision_id", "release"]
