"""Query-text preprocessing shared across resolve entry points.

The transforms in here run BEFORE the multilingual embedding call.
They exist to bridge specific representational gaps between how
clinical indicators are written and how LOINC names them — gaps the
embedding can't always close on its own.

Three transforms ship today:

- :func:`augment_time_markers` — appends minute-form aliases for
  decimal-hour time markers (``0.5h`` → ``30 minutes``) so LOINC's
  minute-canonical naming matches the user's hour-canonical input.

- :func:`augment_zh_aliases` — appends canonical Latin/English forms
  for non-English clinical phrases, driven by ``aliases/*.tsv``
  members in ``fhir_loinc_bundle.tar.gz`` (see :mod:`.lexicon`).
  Function name is historical — it is multilingual (zh / ja / ko /
  fr / es / ru / de currently shipped), the bundle decides what's
  loaded. Same composability: scan the original query for any
  source-language phrase in the lexicon, append the canonical EN
  to the end of the text, leaving the original untouched.

- :func:`normalize_roman_numerals` — substitutes Roman-numeral
  subtype markers with Arabic digits in-place. Handles CJK Roman
  code points (``Ⅰ`` / ``Ⅱ`` / ``Ⅲ`` / ``Ⅳ`` / …, unambiguous), and
  ASCII Roman (``I`` / ``II`` / …) only in analyte-position context
  (``I型``, ``II型``, ``Type I``, ``Type II``) — never bare. Used by
  both the embedding side (so cosine sees ``Herpes Simplex Virus
  Type 2`` instead of ``Type II``) and the picker side
  (:func:`pipeline.query_analyte_digit` reads the normalized text so
  the analyte-digit family rerank can fire for Roman-subtype
  queries). Without this transform, ``HSV-I`` queries get hijacked
  by ``Chlamydia trachomatis I`` and ``HTLV I`` LOINC rows that
  share the ``I IgM/IgG Ab`` orthographic shape.

The augment chain runs as
``augment_zh_aliases(augment_time_markers(normalize_roman_numerals(q)))``
in both :func:`pipeline.resolve_many` and
:func:`FhirAdapter.resolve_many`. The original ``queries[i]`` (un-
augmented) is preserved separately for downstream keyword gates
(``apply_deterministic_class_filter``, ``_is_section_header_term``)
that should NOT see the substitutions. Roman normalization is also
applied inside :func:`query_analyte_digit` so the picker observes
the same digit the embedding does.

Keep each transform narrow and well-justified: every change here
affects every resolve call site.
"""

from __future__ import annotations

import re
from functools import lru_cache

# Match a decimal or integer number followed by an hour token across
# the languages mirobody is plausibly deployed against. Accepts:
#
#   English        ``0.5 hours``, ``0.5 hour``, ``0.5 hr``, ``0.5h``
#   Chinese        ``0.5 小时`` (Simplified), ``0.5 小時`` (Traditional)
#   Japanese       ``0.5 時間``
#   Korean         ``0.5 시간``
#   German         ``0.5 Stunde``, ``0.5 Stunden``
#   French         ``0.5 heure``, ``0.5 heures``
#   Spanish/Pt     ``0.5 hora``, ``0.5 horas``
#   Russian        ``0.5 час``, ``0.5 часа``, ``0.5 часов``
#
# Single-character tokens (``時``, ``시``, ``ч``, ``Std``) are
# intentionally omitted — too short to disambiguate from unrelated
# text without false-positives.
#
# Negative lookahead on bare English ``h`` prevents matching things
# like ``0.5pH`` / ``0.5hz`` (pH / hz are not hour units).
_HOURS_RE = re.compile(
    r"(?P<num>\d+(?:\.\d+)?)\s*"
    r"(?:hours?|hrs?|h(?![A-Za-z])"
    r"|小时|小時"
    r"|時間"
    r"|시간"
    r"|Stunden?"
    r"|heures?"
    r"|horas?"
    r"|час(?:а|ов)?)",
    re.IGNORECASE,
)


def augment_time_markers(text: str) -> str:
    """Append minute-form aliases for sub-hour decimal time markers.

    LOINC display names express 0.5-hour timepoints as ``30 minutes``,
    not ``0.5 hours`` — so a query that says ``0.5h`` / ``0.5 小时``
    cosines closer to the broad ``X.5 hours`` family (``1.5h``,
    ``2.5h``, ``4.5h``) than to the actual ``30 minutes post dose``
    LOINC. Appending the minute form gives the embedding the LOINC-
    aligned representation alongside the original, so cosine to the
    correct row strengthens without losing the original signal.

    Only triggers on durations strictly between 0 and 1 hour. Integer
    hours and ``≥ 1`` decimals (1.5h, 2h, 2.5h, …) align with LOINC
    naming directly and pass through unchanged.

    Returns *text* with the augmented tokens appended; idempotent on
    repeated application up to the order of duplicate tokens.
    """
    if not text:
        return text
    extras: list[str] = []
    seen: set[int] = set()
    for m in _HOURS_RE.finditer(text):
        try:
            hours = float(m.group("num"))
        except ValueError:
            continue
        if not (0 < hours < 1):
            continue
        minutes = round(hours * 60)
        if minutes <= 0 or minutes in seen:
            continue
        seen.add(minutes)
        # Emit both English and Chinese minute forms — LOINC display
        # names are English; the bilingual embedding picks up the zh
        # form too in case the corpus side ever carries Chinese variant
        # names.
        extras.append(f"{minutes} minutes {minutes} 分钟")
    if not extras:
        return text
    return text + " " + " ".join(extras)


# Roman-numeral normalization.
#
# CJK Roman codepoints (Unicode block "Number Forms", U+2160 onward)
# unambiguously represent Roman numerals — they have no other reading
# in clinical text. We substitute them character-for-character.
#
# ASCII Roman ("I" / "II" / "III" / ...) is ambiguous in general text
# (``IgG`` / ``ID`` / ``Vit`` all start with ``I``), so we only
# substitute when the Roman sits in an analyte-position context:
#
#   - immediately before CJK ``型`` (``I型`` / ``IV型``)
#   - immediately after ``Type``/``type`` + whitespace
#     (``Type I`` / ``type IV``)
#
# Other ``\bI\b``/``\bV\b``/``\bX\b`` tokens are left alone to avoid
# rewriting unrelated abbreviations.
#
# Coverage limited to I-XII — clinical subtypes rarely exceed 12
# (Coagulation factors max at XIII, but those appear in LOINC names
# already as Arabic digits).
_CJK_ROMAN_TRANS = str.maketrans({
    "Ⅰ": "1", "Ⅱ": "2", "Ⅲ": "3", "Ⅳ": "4", "Ⅴ": "5",
    "Ⅵ": "6", "Ⅶ": "7", "Ⅷ": "8", "Ⅸ": "9", "Ⅹ": "10",
    "Ⅺ": "11", "Ⅻ": "12",
    "ⅰ": "1", "ⅱ": "2", "ⅲ": "3", "ⅳ": "4", "ⅴ": "5",
    "ⅵ": "6", "ⅶ": "7", "ⅷ": "8", "ⅸ": "9", "ⅹ": "10",
    "ⅺ": "11", "ⅻ": "12",
})

_ASCII_ROMAN_VALUES: dict[str, str] = {
    "I": "1", "II": "2", "III": "3", "IV": "4", "V": "5",
    "VI": "6", "VII": "7", "VIII": "8", "IX": "9", "X": "10",
    "XI": "11", "XII": "12",
}

# Match an ASCII Roman numeral immediately followed by CJK ``型``
# (no whitespace allowed between — the catalog uses tight forms like
# ``I型`` / ``IV型``). The left lookbehind rejects only Latin /
# digit / underscore preceding — that way ``I型`` after a CJK glyph
# (``单纯疱疹病毒I型``) matches, but ``BI型`` (B + I forming a longer
# identifier) does not. ``\b`` alone wouldn't work here: CJK chars
# and Latin letters are both ``\w``, so there's no word boundary
# between them and the regex would never fire on the catalog forms.
_ASCII_ROMAN_BEFORE_TYPE_CJK = re.compile(
    r"(?<![A-Za-z0-9_])(I{1,3}|IV|V|VI{0,3}|IX|X|XI{0,2})型"
)

# Match ``Type`` / ``type`` followed by whitespace and an ASCII Roman
# numeral. Captures the Roman group separately so we can substitute
# only the numeric part, preserving the original case of ``Type``.
_ASCII_ROMAN_AFTER_TYPE_EN = re.compile(
    r"\b([Tt]ype)\s+(I{1,3}|IV|V|VI{0,3}|IX|X|XI{0,2})\b"
)


def _replace_ascii_roman_before_cjk(m: "re.Match[str]") -> str:
    roman = m.group(1)
    return f"{_ASCII_ROMAN_VALUES.get(roman, roman)}型"


def _replace_ascii_roman_after_type(m: "re.Match[str]") -> str:
    label, roman = m.group(1), m.group(2)
    return f"{label} {_ASCII_ROMAN_VALUES.get(roman, roman)}"


def normalize_roman_numerals(text: str) -> str:
    """Substitute Roman-numeral subtype markers with Arabic digits.

    CJK Roman code points (``Ⅰ`` / ``Ⅱ`` / …) substitute unconditionally
    — they have no other reading. ASCII Roman (``I`` / ``II`` / …)
    substitutes only in tight analyte-position contexts (``I型`` /
    ``Type II``) — never bare, to avoid breaking ``IgG`` / ``Vit`` /
    proper-noun ``I``.

    Idempotent: ``normalize_roman_numerals(normalize_roman_numerals(t))
    == normalize_roman_numerals(t)`` (Arabic digits don't re-trigger
    any of the patterns).

    No-op on empty input.
    """
    if not text:
        return text
    text = text.translate(_CJK_ROMAN_TRANS)
    text = _ASCII_ROMAN_BEFORE_TYPE_CJK.sub(_replace_ascii_roman_before_cjk, text)
    text = _ASCII_ROMAN_AFTER_TYPE_EN.sub(_replace_ascii_roman_after_type, text)
    return text


@lru_cache(maxsize=1)
def _zh_alias_data() -> tuple[dict[str, str], "re.Pattern | None"]:
    """Load the multilingual alias lexicon plus a compiled scanner regex.

    Returns ``({}, None)`` when the bundle (or its ``aliases/*.tsv``
    members) is missing — augmentation becomes a no-op and the rest
    of the pipeline continues unchanged.

    Rationale: Qwen3 / Gemini multilingual embeddings have weak bridges
    for Latin-binomial clinical names (出芽短梗霉 ↔ *Aureobasidium
    pullulans*, etc.) — tokens that rarely co-occur with their Latin
    equivalents in pretraining. The embedding falls back on character-
    level semantics ("出芽" budding → Candida) and picks the wrong
    species. The existing alias-bonus mechanism (+0.04 capped at +0.08)
    is a tie-breaker, not enough to cross a 0.1+ cosine gap. Appending
    the canonical Latin form to the embedding input shifts the cosine
    itself toward the right cluster — same mechanism as the
    ``source|name_en|abbrev`` multi-field input that works empirically.

    Loaded from ``aliases/{lang}.tsv`` members in ``fhir_loinc_bundle
    .tar.gz`` — one TSV per language, unioned at load time. See
    :mod:`.lexicon` for the build pipeline and bundle layout.

    The scanner regex alternates over all keys sorted longest-first.
    Python's ``re`` is leftmost-first (not leftmost-longest), so the
    longest matching key at each starting position is found by listing
    the longest alternative first in the pattern.
    """
    try:
        from .lexicon import load_all_aliases
        d = load_all_aliases()
    except Exception:
        return {}, None
    if not d:
        return {}, None
    keys = sorted(d.keys(), key=lambda k: -len(k))
    pattern = "|".join(re.escape(k) for k in keys)
    rx = re.compile(pattern)
    return d, rx


def augment_zh_aliases(text: str) -> str:
    """Append canonical Latin/English forms for CN clinical phrases.

    Scans *text* for CN substrings present in the bundled simplified-
    Chinese alias lexicon (built from LOINC zhCN linguistic variants).
    For each unique match, appends the canonical EN form to the end of
    the text — leaving the original untouched. The embedding model then
    sees both the original CN query AND the LOINC-aligned EN species
    name, lifting cosine on the correct species cluster.

    Dedup is done on the EN value side, so multiple CN keys mapping to
    the same EN canonical (``出芽短梗霉`` and ``短梗霉``) only emit one
    extra token. Order of appended tokens follows first-match order in
    the query — not strictly necessary for cosine but keeps the
    augmented text human-readable for debugging.

    No-op when the lexicon is missing or no CN tokens match.
    """
    if not text:
        return text
    d, rx = _zh_alias_data()
    if rx is None:
        return text
    seen: set[str] = set()
    extras: list[str] = []
    for m in rx.finditer(text):
        v = d.get(m.group(0))
        if v and v not in seen:
            seen.add(v)
            extras.append(v)
    if not extras:
        return text
    return text + " " + " ".join(extras)
