"""`resolve_symptom()`: a complaint in a person's own words becomes one ICPC-3 code, or nothing.

The axis beside `code.py`. That one answers "which analyte is this row of a
lab report"; this one answers "which complaint is the person describing". Both
are pure over shipped vocabulary, both return a `Coding`, and both abstain
rather than guess: a wrong symptom on a record is worse than an uncoded one,
because nothing downstream can tell it was wrong.

The vocabulary is the S component of ICPC-3, the reason-for-encounter axis of
primary care: 319 codes for what a person arrives saying, as opposed to what a
clinician concludes. `res/icpc3_s_component.tsv` carries it verbatim;
`res/symptoms_zh.tsv` and `res/symptoms_en.tsv` are ours, the everyday
spellings a person actually types, because ICPC-3 is written in clinical
English and nobody logs "epigastric pain".

The order of authority, highest first:

1. a confirmed alias, user scope before global: a person's word about their
   own body outranks the vocabulary;
2. our curated surfaces, which resolve what ICPC-3 leaves ambiguous and carry
   the languages it is not published in;
3. ICPC-3 `preferred`, the term the classification leads with;
4. ICPC-3 `inclusions`, the synonyms it lists.

Inside one tier, two codes means no code. No surface in the shipped release
ties (measured: zero across 319 preferred terms and their inclusions), so the
guard costs nothing today; what it buys is that a tie introduced by the next
release abstains as `needs-input` instead of being settled by file order.

The match is exact over the folded surface. This is a term resolver, not a
sentence reader: "fever" resolves, "had a fever on Tuesday" does not. Pulling
terms out of free text is an extraction decision, it belongs with the other
extraction decisions, and it must not be smuggled in here where every caller
would inherit it silently.

Chinese never translates an ICPC-3 term. The licence is CC BY-ND, so a
translation would be a derivative work we may not publish. What
`symptoms_zh.tsv` holds is our own writing about which patient phrase points at
which code, the same shape as `aliases_src/zh_curated.tsv` on the LOINC side.
Display names stay in ICPC-3's English.
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

#: What a symptom coding names. The canonical URI HL7 Terminology publishes for
#: ICPC-3 (NamingSystem-ICPC3, publisher WONCA ICPC Foundation), so a
#: `th_coding_current` row and a FHIR export agree without a second table.
ICPC3_SYSTEM = "http://terminology.hl7.org/CodeSystem/ICPC-3"

ICPC3_FILE = "icpc3_s_component.tsv"
CURATED_FILES = {"en": "symptoms_en.tsv", "zh": "symptoms_zh.tsv"}

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
    """One row of the shipped ICPC-3 S component, verbatim."""

    code: str
    chapter: str
    kind: str
    preferred: str
    inclusions: tuple[str, ...]
    exclusions: tuple[str, ...]


def _read(name: str) -> str:
    return resources.files("mirobody").joinpath("res", name).read_text(encoding="utf-8")


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
    """The shipped S component, by code."""
    out: dict[str, Term] = {}
    for r in csv.DictReader(io.StringIO(_read(ICPC3_FILE)), delimiter="\t"):
        code = (r["code"] or "").strip()
        if not code:
            continue
        out[code] = Term(
            code=code,
            chapter=r["chapter"].strip(),
            kind=r["kind"].strip(),
            preferred=r["preferred"].strip(),
            inclusions=_split(r["inclusions"]),
            exclusions=_split(r["exclusions"]),
        )
    return out


@lru_cache(maxsize=1)
def release() -> str:
    """The vocabulary a symptom coding names: `icpc-3+<12 hex>` over the
    shipped terms. ICPC-3 publishes no release number in the data we hold, and
    a stamp that cannot disagree with the content beats one we invent. The
    digest is over parsed fields rather than file bytes, so a checkout whose
    line endings were rewritten still reports the same release."""
    h = hashlib.sha256()
    for code, t in sorted(terms().items()):
        h.update("\x1f".join((code, t.preferred, *t.inclusions)).encode("utf-8"))
        h.update(b"\x1e")
    return "icpc-3+" + h.hexdigest()[:12]


@lru_cache(maxsize=1)
def _lookup() -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, ...]]]:
    """`(surface key -> (code, rule), surface key -> the codes that tied)`.

    Built tier by tier, highest authority first. A surface a higher tier
    claimed is left alone; a surface claimed twice INSIDE a licensed tier is
    struck from the index and remembered, so a caller is told the term was
    ambiguous rather than told nothing. A tie inside OUR files is a defect in
    them and raises here, at import of the first lookup.
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

    curated = [
        (r["surface"], r["code"].strip(), name)
        for name in CURATED_FILES.values()
        for r in _rows(name)
    ]
    tier(curated, RULE_CURATED, strict=True)

    rows = terms()
    tier([(t.preferred, c, ICPC3_FILE) for c, t in rows.items()], RULE_PREFERRED, strict=False)
    tier(
        [(inc, c, ICPC3_FILE) for c, t in rows.items() for inc in t.inclusions],
        RULE_INCLUSION, strict=False,
    )
    return hit, {k: tuple(sorted(v)) for k, v in tied.items()}


def decision_id(key: str, rel: str, rule: str) -> str:
    """Sixteen hex over what a symptom coding depends on. The LOINC side keys
    on (name, unit, value kind); a complaint has no unit and no number, so the
    folded surface, the release and the rule are the whole input."""
    return hashlib.sha256("\x1f".join((key, rel, rule)).encode("utf-8")).hexdigest()[:16]


def resolve_symptom(text: str, *, alias: Alias | None = None) -> Coding:
    """One complaint to one `Coding`, or an abstention that says why.

        resolve_symptom("发烧")   -> coded AS03 Fever
        resolve_symptom("头疼")   -> coded NS01 Headache
        resolve_symptom("疼")     -> refused, symptom:too-broad
        resolve_symptom("my arm") -> needs-input, symptom:no-match
    """
    rel = release()
    key = name_key(text)
    local = local_series_id(key)

    if alias is not None:
        rule = f"{RULE_ALIAS}:{alias.scope}"
        did = decision_id(key, rel, rule)
        if not alias.code:
            return Coding(OUTCOME_REFUSED, local, did, rule, rel, reason="alias:not-standard")
        return _coded(alias.code, did, rule, rel, local, ("alias",))

    if not key:
        did = decision_id(key, rel, RULE_CURATED)
        return Coding(OUTCOME_NEEDS_INPUT, local, did, RULE_CURATED, rel, reason="symptom:empty")

    hit, tied = _lookup()
    found = hit.get(key)
    if found is None:
        if key in tied:
            did = decision_id(key, rel, RULE_INCLUSION)
            return Coding(
                OUTCOME_NEEDS_INPUT, local, did, RULE_INCLUSION, rel, reason="symptom:ambiguous",
                evidence=(f"term={text}", "candidates=" + ",".join(tied[key])),
            )
        did = decision_id(key, rel, RULE_CURATED)
        return Coding(
            OUTCOME_NEEDS_INPUT, local, did, RULE_CURATED, rel,
            reason="symptom:no-match", evidence=(f"term={text}",),
        )

    code, rule = found
    did = decision_id(key, rel, rule)
    if code == BLOCK_TOO_BROAD:
        return Coding(OUTCOME_REFUSED, local, did, rule, rel, reason="symptom:too-broad", evidence=(f"term={text}",))
    if code == BLOCK_AMBIGUOUS:
        return Coding(OUTCOME_NEEDS_INPUT, local, did, rule, rel, reason="symptom:ambiguous", evidence=(f"term={text}",))
    return _coded(code, did, rule, rel, local, (f"term={text}", f"rule={rule}"))


def _coded(code: str, did: str, rule: str, rel: str, local: str, evidence: tuple[str, ...]) -> Coding:
    term = terms().get(code)
    if term is None:
        return Coding(
            OUTCOME_NEEDS_INPUT, local, did, rule, rel,
            reason=f"symptom:unknown-code:{code}", evidence=evidence,
        )
    return Coding(
        OUTCOME_CODED, symptom_series_id(code), did, rule, rel,
        code_system=ICPC3_SYSTEM, code=code, display=term.preferred, evidence=evidence,
    )


__all__ = [
    "BLOCK_AMBIGUOUS",
    "BLOCK_TOO_BROAD",
    "ICPC3_SYSTEM",
    "Term",
    "decision_id",
    "release",
    "resolve_symptom",
    "terms",
]
