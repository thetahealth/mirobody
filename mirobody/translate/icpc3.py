"""The ICPC-3 axes: what a person SAYS is wrong, and what they have been TOLD they have.

Two functions beside `code.py`. That one answers "which analyte is this row of
a lab report"; these answer "which complaint is the person describing"
(`resolve_symptom`) and "which condition are they naming" (`resolve_condition`).
All three are pure over shipped vocabulary, all three return a `Coding`, and
all three abstain rather than guess: a wrong code on a record is worse than an
uncoded one, because nothing downstream can tell it was wrong.

The vocabulary is ICPC-3, the classification primary care uses, in the two
components a personal health record needs. `res/icpc3/icpc3.tsv` carries both
verbatim; `res/icpc3/symptoms_{zh,en}.tsv` and `res/icpc3/conditions_{zh,en}.tsv` are ours,
the everyday spellings a person actually types, because ICPC-3 is written in
clinical English and nobody logs "epigastric pain".

    S    331 codes    symptoms, complaints and abnormal findings
    D    887 codes    diagnoses and diseases

They are separate indexes, and the CALLER picks which. That is what keeps a
second axis from creating the "one phrase, two codes, which is the identity"
problem: 发烧 is a complaint and has no answer on the D axis, 高血压 is a
diagnosis and has none on the S axis, and neither function is ever consulted
about the other's question.

The order of authority inside one axis, highest first:

1. a confirmed alias, user scope before global: a person's word about their
   own body outranks the vocabulary;
2. our curated surfaces, which resolve what ICPC-3 leaves ambiguous and carry
   the languages it is not published in;
3. ICPC-3 `preferred`, the term the classification leads with;
4. ICPC-3 `inclusions`, the synonyms it lists.

The classification's index words would be a fifth tier and are deliberately
not shipped: 40.6% of them are SNOMED CT description text, and `res/` carries
no SNOMED CT derivative. `translate_build/icpc3.py` has the measurement.

Inside one tier, two codes means no code, and the tie abstains as
`needs-input` rather than being settled by file order.

The match is exact over the folded surface. These are term resolvers, not
sentence readers: "fever" resolves, "had a fever on Tuesday" does not. Pulling
terms out of free text is an extraction decision, it belongs with the other
extraction decisions, and it must not be smuggled in here where every caller
would inherit it silently.

Chinese never translates an ICPC-3 term. The licence is CC BY-ND and WONCA
licenses translations separately, so what the curated files hold is our own
writing about which phrase points at which code, the same shape as
`aliases_src/zh_curated.tsv` on the LOINC side. Display names stay in ICPC-3's
English. See `res/icpc3/icpc3.NOTICE`.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources

from .fold import name_key
from .outcome import (
    OUTCOME_CODED,
    OUTCOME_NEEDS_INPUT,
    OUTCOME_REFUSED,
    Alias,
    Coding,
)
from .series import local_series_id, symptom_series_id

#: What an ICPC-3 coding names. The canonical URI HL7 Terminology publishes
#: (NamingSystem-ICPC3, publisher WONCA ICPC Foundation), so a
#: `th_coding_current` row and a FHIR export agree without a second table.
ICPC3_SYSTEM = "http://terminology.hl7.org/CodeSystem/ICPC-3"

ICPC3_FILE = "icpc3.tsv"

COMPONENT_SYMPTOM = "S"
COMPONENT_CONDITION = "D"

#: Our surfaces, per axis. Both files of an axis are one tier: a surface may be
#: declared in either language file and the fold is the same.
CURATED = {
    COMPONENT_SYMPTOM: ("symptoms_en.tsv", "symptoms_zh.tsv"),
    COMPONENT_CONDITION: ("conditions_en.tsv", "conditions_zh.tsv"),
}

RULE_ALIAS = "alias"
RULE_CURATED = "icpc3:curated"
RULE_PREFERRED = "icpc3:preferred"
RULE_INCLUSION = "icpc3:inclusion"

#: Sentinels a curated row carries instead of a code. `!too-broad` is a
#: decision never to answer: "pain" names no complaint ICPC-3 codes, it names
#: fifteen. `!ambiguous` is an open question a person can close: 打嗝 is both a
#: belch (DS08) and a hiccough (RS99.00).
BLOCK_TOO_BROAD = "!too-broad"
BLOCK_AMBIGUOUS = "!ambiguous"


@dataclass(frozen=True)
class Term:
    """One row of the shipped ICPC-3 tabular list, verbatim."""

    code: str
    component: str
    chapter: str
    kind: str
    preferred: str
    description: str
    inclusions: tuple[str, ...]
    exclusions: tuple[str, ...]


def _read(name: str) -> str:
    return resources.files("mirobody").joinpath("res", "icpc3", name).read_text(encoding="utf-8")


def _rows(name: str) -> list[dict[str, str]]:
    """A curated TSV: `surface`, `code`, `note`, with `#` lines for prose."""
    body = "\n".join(
        line for line in _read(name).splitlines() if line and not line.startswith("#")
    )
    return list(csv.DictReader(io.StringIO(body), delimiter="\t"))


def _split(cell: str) -> tuple[str, ...]:
    return tuple(p.strip() for p in (cell or "").split(";") if p.strip())


@lru_cache(maxsize=1)
def terms() -> dict[str, Term]:
    """The shipped tabular list, by code, both components together."""
    out: dict[str, Term] = {}
    for r in csv.DictReader(io.StringIO(_read(ICPC3_FILE)), delimiter="\t"):
        code = (r["code"] or "").strip()
        if not code:
            continue
        out[code] = Term(
            code=code,
            component=r["component"].strip(),
            chapter=r["chapter"].strip(),
            kind=r["kind"].strip(),
            preferred=r["preferred"].strip(),
            description=r["description"].strip(),
            inclusions=_split(r["inclusions"]),
            exclusions=_split(r["exclusions"]),
        )
    return out


@lru_cache(maxsize=1)
def release() -> str:
    """The vocabulary a coding names: `icpc-3+<12 hex>` over the shipped terms.
    ICPC-3 publishes no release number in the data we hold, and a stamp that
    cannot disagree with the content beats one we invent. The digest is over
    parsed fields rather than file bytes, so a checkout whose line endings were
    rewritten still reports the same release. It covers everything a coding
    depends on: the component, which picks the axis, and our surface files,
    which pick most codes. Without them, moving AS03 to another axis or
    remapping 发烧 changed codes under an unchanged stamp."""
    h = hashlib.sha256()
    for code, t in sorted(terms().items()):
        h.update("\x1f".join((code, t.component, t.preferred, *t.inclusions)).encode("utf-8"))
        h.update(b"\x1e")
    for component in sorted(CURATED):
        for name in CURATED[component]:
            for r in _rows(name):
                h.update("\x1f".join((component, r["surface"], r["code"].strip())).encode("utf-8"))
                h.update(b"\x1e")
    return "icpc-3+" + h.hexdigest()[:12]


@lru_cache(maxsize=4)
def _lookup(component: str) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, ...]]]:
    """One axis: `(surface key -> (code, rule), surface key -> codes that tied)`.

    Built tier by tier, highest authority first. A surface a higher tier
    claimed is left alone; a surface claimed twice INSIDE a licensed tier is
    struck from the index and remembered, so a caller is told the term was
    ambiguous rather than told nothing. A tie inside OUR files is a defect in
    them and raises here, on the first lookup.
    """
    hit: dict[str, tuple[str, str]] = {}
    tied: dict[str, set[str]] = {}

    def tier(pairs: list[tuple[str, str, str]], rule: str, *, strict: bool) -> None:
        seen: dict[str, str] = {}
        for surface, code, where in pairs:
            key = name_key(surface)
            if not key or key in hit:
                continue
            if key in seen and seen[key] != code:
                if strict:
                    raise ValueError(f"{where}: {surface!r} is both {seen[key]} and {code}")
                tied.setdefault(key, {seen[key]}).add(code)
                continue
            seen[key] = code
        for key, code in seen.items():
            if key not in tied:
                hit[key] = (code, rule)

    tier(
        [(r["surface"], r["code"].strip(), name)
         for name in CURATED[component] for r in _rows(name)],
        RULE_CURATED, strict=True,
    )

    rows = {c: t for c, t in terms().items() if t.component == component}
    tier([(t.preferred, c, ICPC3_FILE) for c, t in rows.items()], RULE_PREFERRED, strict=False)
    tier(
        [(s, c, ICPC3_FILE) for c, t in rows.items() for s in t.inclusions],
        RULE_INCLUSION, strict=False,
    )
    return hit, {k: tuple(sorted(v)) for k, v in tied.items()}


def decision_id(key: str, component: str, rel: str, rule: str) -> str:
    """Sixteen hex over what an ICPC-3 coding depends on. The LOINC side keys
    on (name, unit, value kind); a complaint has no unit and no number, so the
    folded surface, the axis, the release and the rule are the whole input.
    The axis is in it because 发烧 is coded on one and needs input on the
    other: sharing one id, the second axis's evidence was dropped on insert."""
    return hashlib.sha256("\x1f".join((key, component, rel, rule)).encode("utf-8")).hexdigest()[:16]


def _alias_applies(alias: Alias, component: str) -> bool:
    """A confirmed alias names a code in one vocabulary and on one axis. A
    LOINC alias on the same words answered `icpc3:unknown-code:718-7`, and a
    diagnosis alias answered a KD code on the complaint axis."""
    if not alias.code:
        return True
    if alias.code_system and alias.code_system != ICPC3_SYSTEM:
        return False
    term = terms().get(alias.code)
    return term is None or term.component == component


def resolve(text: str, component: str, *, alias: Alias | None = None) -> Coding:
    """One term to one `Coding` on one axis, or an abstention that says why."""
    rel = release()
    key = name_key(text)
    local = local_series_id(key)

    if alias is not None and _alias_applies(alias, component):
        rule = f"{RULE_ALIAS}:{alias.scope}"
        did = decision_id(key, component, rel, rule)
        if not alias.code:
            return Coding(OUTCOME_REFUSED, local, did, rule, rel, reason="alias:not-standard")
        return _coded(alias.code, did, rule, rel, local, ("alias",))

    if not key:
        did = decision_id(key, component, rel, RULE_CURATED)
        return Coding(OUTCOME_NEEDS_INPUT, local, did, RULE_CURATED, rel, reason="icpc3:empty")

    hit, tied = _lookup(component)
    found = hit.get(key)
    if found is None and key.endswith("了"):
        # 发烧了, 头疼了: the completive particle, not part of the complaint.
        found = hit.get(key[:-1])
    if found is None:
        if key in tied:
            did = decision_id(key, component, rel, RULE_INCLUSION)
            return Coding(
                OUTCOME_NEEDS_INPUT, local, did, RULE_INCLUSION, rel, reason="icpc3:ambiguous",
                evidence=(f"term={text}", "candidates=" + ",".join(tied[key])),
            )
        did = decision_id(key, component, rel, RULE_CURATED)
        return Coding(
            OUTCOME_NEEDS_INPUT, local, did, RULE_CURATED, rel,
            reason="icpc3:no-match", evidence=(f"term={text}", f"component={component}"),
        )

    code, rule = found
    did = decision_id(key, component, rel, rule)
    if code == BLOCK_TOO_BROAD:
        return Coding(OUTCOME_REFUSED, local, did, rule, rel, reason="icpc3:too-broad", evidence=(f"term={text}",))
    if code == BLOCK_AMBIGUOUS:
        return Coding(OUTCOME_NEEDS_INPUT, local, did, rule, rel, reason="icpc3:ambiguous", evidence=(f"term={text}",))
    return _coded(code, did, rule, rel, local, (f"term={text}", f"rule={rule}"))


def resolve_symptom(text: str, *, alias: Alias | None = None) -> Coding:
    """A complaint in a person's own words to one ICPC-3 S code.

        resolve_symptom("发烧")   -> coded AS03 Fever
        resolve_symptom("头疼")   -> coded NS01 Headache
        resolve_symptom("疼")     -> refused, icpc3:too-broad
        resolve_symptom("高血压") -> needs-input, icpc3:no-match (a diagnosis)
    """
    return resolve(text, COMPONENT_SYMPTOM, alias=alias)


def resolve_condition(text: str, *, alias: Alias | None = None) -> Coding:
    """A condition a person has been diagnosed with to one ICPC-3 D code.

        resolve_condition("高血压")  -> coded KD... Hypertension
        resolve_condition("发烧")    -> needs-input, icpc3:no-match (a complaint)
    """
    return resolve(text, COMPONENT_CONDITION, alias=alias)


def _coded(code: str, did: str, rule: str, rel: str, local: str, evidence: tuple[str, ...]) -> Coding:
    term = terms().get(code)
    if term is None:
        return Coding(
            OUTCOME_NEEDS_INPUT, local, did, rule, rel,
            reason=f"icpc3:unknown-code:{code}", evidence=evidence,
        )
    return Coding(
        OUTCOME_CODED, symptom_series_id(code), did, rule, rel,
        code_system=ICPC3_SYSTEM, code=code, display=term.preferred, evidence=evidence,
    )


__all__ = [
    "BLOCK_AMBIGUOUS",
    "BLOCK_TOO_BROAD",
    "COMPONENT_CONDITION",
    "COMPONENT_SYMPTOM",
    "ICPC3_SYSTEM",
    "Term",
    "decision_id",
    "release",
    "resolve",
    "resolve_condition",
    "resolve_symptom",
    "terms",
]
