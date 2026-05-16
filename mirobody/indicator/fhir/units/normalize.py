"""Free-text unit string → canonical UCUM.

Pipeline:

  raw input
    └─ unicode NFKC  (full-width digits, ligatures, superscripts)
       └─ symbol fold (µ/μ → u, × → x, etc.)
          └─ whitespace collapse
             └─ flat alias lookup  → canonical UCUM
                └─ if miss: tokenize-compose via morphemes

We deliberately do **not** lowercase indiscriminately. UCUM is
case-sensitive — ``mg`` is milligram, ``MG`` is megagram, ``m`` is meter
versus ``M`` is mega-. Variants like ``MG/DL`` are handled explicitly in
the ``alias`` layer rather than by blanket case folding.

Data lives in :mod:`.tokens` as two Python dicts keyed by
canonical UCUM. Two layers:

* **morpheme** — atomic tokens that the scanner concatenates into a
  composed UCUM string. ``Millimol pro Liter`` is tokenized as
  ``Millimol`` (→ ``mmol``) + ``pro`` (→ ``/``) + ``Liter`` (→ ``L``)
  and composed back to ``mmol/L``. Composed result is only accepted if
  it lands in :data:`UCUM_FAMILY` (so nonsense like ``mgL`` is rejected).

* **alias** — full-string variants that map directly to a canonical
  without decomposition (``mmHg`` → ``mm[Hg]``, ``mg%`` → ``mg/dL``,
  Russian compound digits like ``10⁹/л`` → ``10*9/L``).

Edit :mod:`.tokens` directly to add units or language variants.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache

from .tokens import ALIASES, MORPHEMES
from .families import UCUM_FAMILY

__all__ = [
    "normalize_unit", "parse_value_unit", "ParsedQuantity",
    "scan_value_units",
]


@dataclass(frozen=True)
class ParsedQuantity:
    """Parsed result of a value+unit free-text string.

    Fields are independently optional:

    * ``"5.6 mmol/L"``  → ``ParsedQuantity("", 5.6, "mmol/L")``
    * ``"<5.6"``         → ``ParsedQuantity("<", 5.6, None)``
    * ``"mmol/L"``       → ``ParsedQuantity("", None, "mmol/L")``
    * ``"90次每分钟"``    → ``ParsedQuantity("", 90.0, "/min")``
    * ``"negative"``     → ``ParsedQuantity("", None, None)``
    """

    comparator: str       # '', '<', '<=', '>', '>=', '~', '≤', '≥', '≈', ...
    value: float | None
    unit: str | None


# Symbol normalization applied AFTER NFKC. NFKC handles most things
# (full-width digits, superscript ², ligatures); these are the leftover
# unicode characters NFKC doesn't fold but UCUM cares about.
_SYMBOL_FOLD: dict[str, str] = {
    "µ":  "u",   # U+00B5 MICRO SIGN
    "μ":  "u",   # U+03BC GREEK SMALL LETTER MU
    "×":  "x",   # multiplication sign
    "·":  ".",   # middle dot — UCUM uses . for multiplication
    "⋅":  ".",   # dot operator
    "°":  "deg",
    " ":  "",    # collapse internal whitespace
    "\t": "",
}


# UCUM annotations ``{xxx}`` carry semantic meaning only — they do not
# change the dimension of the unit. Stripped at lookup-time (not in
# :func:`_clean`) so that canonicals like ``mL/min/{1.73_m2}`` (eGFR
# — where the annotation IS the denominator) still match themselves
# exactly and preserve their ArVRat family. For ``mL/min/{1.73_m2}``
# the ``/?`` also eats the preceding slash, so the stripped form is
# ``mL/min`` rather than the truncated ``mL/min/``.
_ANNOTATION = re.compile(r"/?\s*\{[^}]*\}")


def _clean(s: str) -> str:
    """Unicode-normalize and symbol-fold without case changes."""
    s = unicodedata.normalize("NFKC", s).strip()
    if not s:
        return s
    out = []
    for ch in s:
        out.append(_SYMBOL_FOLD.get(ch, ch))
    return "".join(out)


def _strip_annotations(s: str) -> str:
    return _ANNOTATION.sub("", s)


# Leading value pattern with capture groups. ``.sub("", s)`` strips the
# whole match (used by :func:`normalize_unit` as a fallback); ``.match()``
# exposes the comparator and number separately for :func:`parse_value_unit`.
# Handles ``90``, ``5.6``, ``<5.6``, ``>=5.6``, ``-5.6``, ``~5.6``,
# ``≤5.6`` and similar. Canonical UCUM forms that legitimately start
# with digits (``10*9/L``, ``10*12/L``) hit the alias table before this
# pattern runs, so they're not clobbered.
_VALUE_PREFIX = re.compile(r"^([-<>=+~≤≥≈]*)([0-9]+(?:[.,][0-9]+)?)")


def _invert_to_token_map(source: dict[str, list[str]]) -> dict[str, str]:
    """Invert ``{canonical: [tokens]}`` to ``{cleaned_token: canonical}``.

    Pre-cleaning every token at load time means a scanned input (also
    pre-cleaned) finds its canonical with one dict lookup.
    """
    out: dict[str, str] = {}
    for canon, tokens in source.items():
        for tok in tokens:
            cleaned = _clean(tok)
            if cleaned:
                out[cleaned] = canon
    return out


@lru_cache(maxsize=1)
def _alias_table() -> dict[str, str]:
    alias = _invert_to_token_map(ALIASES)
    # Every canonical UCUM unit is a valid input — register it as
    # mapping to itself so callers don't need to special-case "already
    # canonical". Explicit alias entries take precedence.
    for canon in UCUM_FAMILY:
        alias.setdefault(_clean(canon), canon)
    return alias


@lru_cache(maxsize=1)
def _morpheme_table() -> tuple[dict[str, str], tuple[str, ...]]:
    """Return (token_map, sorted_keys_desc_by_len) for tokenize-compose.

    Auto-injects atomic UCUM canonicals (no ``/`` or ``.``) so the
    scanner recognizes ``mg`` / ``mmol`` / ``L`` inside longer inputs
    without each language repeating them. Compound canonicals stay out
    of the morpheme layer — registering ``/L`` here would let the
    greedy scanner gobble it across a numerator/denominator boundary
    (``Millimol/Liter`` → ``Millimol`` + ``/L`` + leftover ``iter``).

    The sorted-keys tuple is precomputed so :func:`_tokenize_compose`
    doesn't re-sort on every call.
    """
    morpheme = _invert_to_token_map(MORPHEMES)
    # Atomic UCUM auto-inject.
    for canon in UCUM_FAMILY:
        if "/" in canon or "." in canon:
            continue
        morpheme.setdefault(_clean(canon), canon)
    # Universal separator — every language uses ``/`` for "per".
    morpheme.setdefault("/", "/")
    keys = tuple(sorted(morpheme, key=len, reverse=True))
    return morpheme, keys


def _tokenize_compose(text: str) -> str | None:
    """Greedy longest-match-first tokenization + UCUM composition.

    Returns the composed UCUM string if it lands in :data:`UCUM_FAMILY`,
    else ``None``. Unmatched characters break the parse — anything left
    over means we don't fully recognize the input.
    """
    table, keys = _morpheme_table()
    out: list[str] = []
    pos = 0
    n = len(text)
    while pos < n:
        for tok in keys:
            if text.startswith(tok, pos):
                out.append(table[tok])
                pos += len(tok)
                break
        else:
            # unmatched char — refuse the parse rather than emit garbage
            return None
    composed = "".join(out)
    if composed in UCUM_FAMILY:
        return composed
    return None


def _resolve_strict(cleaned: str) -> str | None:
    """Alias → annotation-strip retry → tokenize-compose. No value-strip.

    Shared by :func:`normalize_unit` (which wraps it with a value-strip
    fallback) and :func:`parse_value_unit` (which uses it standalone so
    the value/unit split stays clean).
    """
    if not cleaned:
        return None
    alias = _alias_table()
    hit = alias.get(cleaned)
    if hit is not None:
        return hit
    stripped = _strip_annotations(cleaned)
    if stripped != cleaned:
        hit = alias.get(stripped)
        if hit is not None:
            return hit
    return _tokenize_compose(stripped)


# Cap on greedy-edge resolve attempts. Real CJK unit phrases sit at
# 1-6 chars (``克``, ``毫升``, ``毫摩尔``, ``毫摩尔/升``); past that
# we'd be eating into the surrounding noun. Mirrors the cap used in
# ``scan_value_units``' CJK pass.
_EDGE_RESOLVE_CAP = 8


def _resolve_prefix(text: str) -> str | None:
    """Longest-prefix resolve. Returns the UCUM for the longest
    ``text[:end]`` that ``_resolve_strict`` accepts, else ``None``.

    Used by :func:`parse_value_unit` to peel a unit token off the
    front of post-value rest text — handles compact CJK inputs like
    ``70克葡萄糖`` where the unit (``克``) butts directly against a
    surrounding noun with no whitespace to split on.
    """
    if not text:
        return None
    for end in range(min(len(text), _EDGE_RESOLVE_CAP), 0, -1):
        hit = _resolve_strict(text[:end])
        if hit is not None:
            return hit
    return None


def _resolve_suffix(text: str) -> str | None:
    """Longest-suffix resolve — mirror of :func:`_resolve_prefix` for
    the pre-value side. Handles ``葡萄糖70克`` (noun before value,
    unit after) where the unit might be the rightmost edge of the
    text. The function name describes the SLICE strategy (suffix of
    *text*), not whether the unit lies before or after the value."""
    if not text:
        return None
    n = len(text)
    for start in range(max(0, n - _EDGE_RESOLVE_CAP), n):
        hit = _resolve_strict(text[start:])
        if hit is not None:
            return hit
    return None


def normalize_unit(text: str | None) -> str | None:
    """Canonicalize a free-text unit string to UCUM.

    Lookup: cleaned input → alias → annotation strip → tokenize-compose.
    Last resort: strip a leading value (``90`` in ``90次每分钟``, ``<5.6``
    in ``<5.6 mg/dL``) and retry. Canonicals that start with digits
    (``10*9/L``) hit the alias table first, so they're not clobbered.
    """
    if not isinstance(text, str) or not text:
        return None
    cleaned = _clean(text)
    if not cleaned:
        return None
    hit = _resolve_strict(cleaned)
    if hit is not None:
        return hit
    no_value = _VALUE_PREFIX.sub("", cleaned)
    if no_value and no_value != cleaned:
        return _resolve_strict(no_value)
    return None


_VALUE_ANYWHERE = re.compile(r"[0-9]+(?:[.,][0-9]+)?")


# ``parse_value_unit`` expects a clean ``<value><unit>`` standalone
# string; resolver / corpus-side users need to find ``75 g`` inside
# longer text (LOINC names: ``--2 hours post 75 g glucose PO``). The
# scanner below covers that use case.
#
# Restricting to dose-relevant families keeps the scanner from
# colliding with axes the resolver already covers:
#   - TIME_ASPCT axis handles ``2 hours`` / ``24 hour`` (Time family)
#   - PROPERTY axis handles concentrations (MCnc/SCnc, e.g. ``mg/dL``)
# Dose unit families are the gap — these encode challenge doses
# (75 g, 100 g, 50 mL), body-weight-normalized variants
# (1.75 g/kg pediatric OGTT), and biological-activity units
# (``500 U penicillin``, ``5 IU insulin``) that no axis predicts.
# Adding non-dose families here would double-fire with axis bonus
# and over-promote.
_DOSE_FAMILIES = frozenset({"Mass", "Vol", "CCnt", "MCnt", "Arb"})

# Two-pass scan: one regex per script class.
#
# **Latin pass** — number then optional whitespace then a Latin /
# micro-sign / degree-shaped token. Body allows letters, digits, and
# UCUM compositors (``./*[]+-``) so compounds like ``g/kg`` and
# ``10*9/L`` round-trip through ``_resolve_strict``; whitespace is
# excluded so the body can't run across tokens. ``\b`` at the tail
# rejects partial matches like ``75 grms`` (typo) by requiring a real
# word boundary — but only Latin/digit-vs-non-Latin counts, which is
# why this pass alone misses compact CJK input.
_LATIN_VALUE_UNIT_SCAN = re.compile(
    r"([0-9]+(?:[.,][0-9]+)?)"
    r"\s*"
    r"([A-Za-zμµ°][A-Za-z0-9./*\[\]+\-]{0,11})"
    # Tail: not followed by another Latin-alnum (rejects ``75 grms``
    # mid-word). Plain ``\b`` would *also* reject ``75g 后`` because
    # ``g`` and the CJK ``后`` are both ``\w`` — no boundary fires
    # between them. Explicit negative lookahead targets the right
    # adjacency: another Latin/digit char means the body cut short.
    r"(?![A-Za-z0-9])",
    re.UNICODE,
)

# **CJK pass** — number anchor only; the unit body is parsed by
# greedy-longest-match against the morpheme table. ``\b`` between two
# CJK ideographs doesn't fire (they're both ``\w``), so any
# regex-only body would either eat the trailing noun
# (``75克葡萄糖`` → ``克葡萄糖``) or refuse to start. Instead we find
# digit runs, skip optional whitespace, and tokenize forward using
# the same morpheme table the rest of the module uses — stops at the
# first un-tokenizable char, which is exactly where the unit ends.
_DIGIT_RUN = re.compile(r"[0-9]+(?:[.,][0-9]+)?", re.UNICODE)


def scan_value_units(text: str | None) -> list[tuple[float, str]]:
    """Find every ``(value, UCUM unit)`` pair in *text* belonging to a
    dose-relevant unit family.

    For each numeric run in *text*, look ahead for a unit token, run it
    through :func:`_resolve_strict` to canonicalize, and accept only
    when the resulting UCUM lives in :data:`_DOSE_FAMILIES` (Mass /
    Vol / CCnt). Returns deduped list in encounter order.

    ::

        scan_value_units("post 75 g glucose PO")        -> [(75.0, "g")]
        scan_value_units("Glucose --2 hours post 100 g") -> [(100.0, "g")]
        scan_value_units("OGTT 口服 75 克 葡萄糖")        -> [(75.0, "g")]
        scan_value_units("5.6 mmol/L")                  -> []
        scan_value_units("2 hours post")                -> []

    Mass/Vol/CCnt are the gap not covered by LOINC's TIME_ASPCT or
    PROPERTY axis bonuses, so a positive hit here is information the
    resolver doesn't already have. ``mmol/L`` belongs to the SCnc
    concentration family (PROPERTY-axis territory) and is intentionally
    rejected — bonusing on it would double-count with axis rerank.
    """
    if not isinstance(text, str) or not text:
        return []
    # Preserve whitespace as a token boundary — the standard ``_clean``
    # collapses spaces (correct for compact UCUM input, wrong for
    # arbitrary text scanning). NFKC + symbol fold are still applied so
    # full-width digits and the µ/μ variants normalize before regex.
    nfkc = unicodedata.normalize("NFKC", text)
    folded = []
    for ch in nfkc:
        if ch in (" ", "\t"):
            folded.append(" ")
        else:
            folded.append(_SYMBOL_FOLD.get(ch, ch) if ch not in (" ", "\t") else ch)
    scan_text = "".join(folded)

    out: list[tuple[float, str]] = []
    seen: set[tuple[float, str]] = set()

    def _record(raw_value: str, ucum: str | None) -> None:
        if ucum is None:
            return
        fam = UCUM_FAMILY.get(ucum)
        if fam not in _DOSE_FAMILIES:
            return
        try:
            value = float(raw_value.replace(",", "."))
        except ValueError:
            return
        key = (value, ucum)
        if key in seen:
            return
        seen.add(key)
        out.append(key)

    # Pass 1 — Latin / micro / degree-shaped unit tokens.
    for m in _LATIN_VALUE_UNIT_SCAN.finditer(scan_text):
        _record(m.group(1), _resolve_strict(_clean(m.group(2))))

    # Pass 2 — CJK greedy unit parse via the shared edge-resolve
    # primitive ``_resolve_prefix``. Same primitive
    # :func:`parse_value_unit` uses for its compact-CJK fallback —
    # both paths agree on what counts as a unit token sitting next
    # to a value with no whitespace.
    n = len(scan_text)
    for m in _DIGIT_RUN.finditer(scan_text):
        pos = m.end()
        while pos < n and scan_text[pos] in (" ", "\t"):
            pos += 1
        # Skip if next char is Latin/digit — Pass 1's territory.
        # Dedup would still drop the duplicate, but cleaner to gate
        # up front.
        if pos >= n or scan_text[pos].isascii():
            continue
        _record(m.group(0), _resolve_prefix(scan_text[pos:]))

    return out


def parse_value_unit(text: str | None) -> ParsedQuantity:
    """Parse a free-text "value + unit" string into its components.

    Lookup order:
      1. Whole input as unit — preserves ``10*9/L`` and other canonicals
         that legitimately start with digits.
      2. Value at start: ``<5.6 mg/dL`` → comparator + value + unit.
      3. Value anywhere: ``每分钟90次`` (Chinese SVO) → ``(0, 90, /min)``.
         Only succeeds if the leftover text resolves to a unit; otherwise
         we refuse rather than return a half-parsed result.

    See :class:`ParsedQuantity` for the return shape.
    """
    empty = ParsedQuantity("", None, None)
    if not isinstance(text, str) or not text:
        return empty
    cleaned = _clean(text)
    if not cleaned:
        return empty

    # ── Path A: whole input is a unit ────────────────────────────────
    direct = _resolve_strict(cleaned)
    if direct is not None:
        return ParsedQuantity("", None, direct)

    # ── Path B: value at start (with optional comparator) ────────────
    m = _VALUE_PREFIX.match(cleaned)
    if m:
        raw_cmp, raw_num = m.group(1), m.group(2)
        # ``-``/``+`` are signs (part of the number), not comparators.
        sign = ""
        if raw_cmp == "-":
            sign, raw_cmp = "-", ""
        elif raw_cmp == "+":
            raw_cmp = ""
        try:
            value: float | None = float((sign + raw_num).replace(",", "."))
        except ValueError:
            value = None
        rest = cleaned[m.end():]
        if not rest:
            return ParsedQuantity(raw_cmp, value, None)
        unit = _resolve_strict(rest)
        if unit is None:
            # Compact CJK fallback: ``70克葡萄糖`` — the unit butts
            # against a noun with no whitespace, so ``rest`` as a whole
            # can't tokenize-compose, but its longest prefix can. Bound
            # is small so we can't eat across a value/unit boundary.
            unit = _resolve_prefix(rest)
        if unit is not None:
            return ParsedQuantity(raw_cmp, value, unit)
        # Rest exists but didn't resolve — fall through rather than emit
        # a half-parsed (value-only) result for inputs like "5.6/3.2".

    # ── Path C: value anywhere (Chinese SVO: "每分钟90次") ───────────
    nm = _VALUE_ANYWHERE.search(cleaned)
    if nm:
        try:
            value = float(nm.group(0).replace(",", "."))
        except ValueError:
            value = None
        left = cleaned[:nm.start()]
        right = cleaned[nm.end():]
        # Try joined first — preserves SVO behavior ("每分钟90次":
        # left ``每分钟`` + right ``次`` tokenize-composes to /min).
        # Then each side independently for the value-at-edge CJK cases
        # (``葡萄糖70克``: right side ``克`` resolves on its own).
        unit = _resolve_strict(left + right) if (left or right) else None
        if unit is None:
            unit = _resolve_prefix(right) or _resolve_suffix(left)
        if value is not None and unit is not None:
            return ParsedQuantity("", value, unit)

    return empty
