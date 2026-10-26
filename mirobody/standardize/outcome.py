"""What a coding attempt produced, and why.

Three outcomes and never a fourth. `coded` carries a code and the series it
puts the reading in. `needs-input` means the vocabulary could not say and a
person can (the reading is stored, in its own local series, and listed for
review). `refused` means the vocabulary could say and the answer is "not one
code": a panel name, a term naming two tests, an alias a person confirmed as
"not a standard item". A refusal is a decision and a later release does not
overturn it by itself.

Every outcome names its `decision_id`: the shared record of the rule, the
release and the evidence that produced it, so "why is this coded 14647-2"
is a lookup, not an archaeology.
"""

from __future__ import annotations

from dataclasses import dataclass

from .series import Axes

LOINC_SYSTEM = "http://loinc.org"

OUTCOME_CODED = "coded"
OUTCOME_NEEDS_INPUT = "needs-input"
OUTCOME_REFUSED = "refused"


@dataclass(frozen=True)
class Alias:
    """A mapping a person confirmed, looked up by the caller and handed to
    `code()`. `code is None` means "confirmed: not a standard item"."""

    scope: str
    code_system: str | None
    code: str | None


@dataclass(frozen=True)
class Coding:
    outcome: str
    series_id: str
    decision_id: str
    rule: str
    release: str
    reason: str = ""
    code_system: str | None = None
    code: str | None = None
    display: str = ""
    group_id: str | None = None
    value_canonical: float | None = None
    unit_canonical: str | None = None
    axes: Axes | None = None
    evidence: tuple[str, ...] = ()

    @property
    def coded(self) -> bool:
        return self.outcome == OUTCOME_CODED


__all__ = ["Alias", "Coding", "LOINC_SYSTEM", "OUTCOME_CODED", "OUTCOME_NEEDS_INPUT", "OUTCOME_REFUSED"]
