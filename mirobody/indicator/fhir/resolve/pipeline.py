"""Resolve v2 — cosine + family rerank + deterministic LOINC pool filters.

Deterministic by design: every gate is a hard ``keep`` / ``drop``
predicate over LOINC rows. No cosine-magnitude penalties, no fitted
weights — swap the Gemini embedding for Qwen3 (or anything else) and
all six filters keep working without retuning. The picker is just
``argmax over (LOINC pool ∩ AND-of-filter-masks)``, with one optional
family-rerank swap for top-1.

Pipeline:

  1. **Recall** — embed the query, cosine vs every corpus row.

  2. **Deprecate strip** (setup-time) — corpus rows whose display
     name starts with ``Deprecated `` are removed from every
     system's candidate pool. LOINC marks codes ``Deprecated`` only
     when a non-deprecated successor exists, so the hard drop loses
     no clinically reachable answer.

  3. **Per-query LOINC keep-mask pipeline** — each filter returns a
     bool[N] keep mask (or ``None`` to skip). Composed via AND, then
     passed to the picker. See ``_LOINC_FILTERS`` for the full list:

       - ``transfusion_subject`` — drop ``from Donor`` / ``from Blood
         product unit`` unless query says ``donor / 输血 /
         transfusion / 血制品 / ...``.
       - ``analyte_concept`` — drop opposite Ag/Ab side. Last-marker-
         wins parses ``Ag``, ``Ab``, ``抗原``, ``抗体``,
         ``antigen``, ``antibody``, plus equivalents in Chinese
         (simp+trad) / Japanese / Korean / Spanish / German / French
         / Russian.
       - ``letter_analyte`` — when the query is in an Ag/Ab context
         AND carries a standalone single Latin letter (``抗D抗体`` /
         ``Hep B 表面抗原`` / ``A 抗体``), require that letter to
         appear standalone in the LOINC name. Resolves the
         generic-vs-specific tie that family rerank can't bridge
         (``50401-9 Blood group antibody titered [Identifier]`` vs
         ``100281-5 D Ab [Units/volume]``).
       - ``challenge_time`` — hybrid strict: query has time *T* and
         corpus has T-matching rows ⇒ keep only T-matching; query has
         *T* but no T-match ⇒ keep no-time generic; query has a
         challenge marker (``空腹`` / ``fasting`` / ``postprandial``)
         but no specific time ⇒ drop ALL time-qualified rows so the
         canonical no-qualifier code wins; query has neither ⇒ skip.
       - ``post_meal`` — drop ``--post meal`` / ``--postprandial``
         unless query carries an explicit meal marker (``餐后 /
         postprandial / nach der Mahlzeit / ...``).
       - ``intake_recall`` — drop 24-hour-intake dietary-survey
         codes unless query says ``摄入 / 膳食 / dietary / ...``.
       - ``xxx_challenge`` — drop the LOINC ``--post XXX challenge``
         placeholder rows (querying for ``insulin 1 hour`` should
         land on a specific protocol, not the generic placeholder).
       - ``explicit_dose`` — drop rows with ``--post N g/mg/...``
         dose qualifiers unless the query types an explicit matching
         ``(value, unit)`` pair (``一小时75克血糖`` keeps 75 g rows;
         ``胰岛素(一小时)`` falls to the generic ``post dose glucose``).

  4. **Family rerank (LOINC top-1 only)** — within the cosine top-1's
     COMPONENT-base family pool, prefer:
       - members whose name carries the same analyte digit as the
         query (``HPV-43`` → ``HPV 43 *``);
       - if the query has no digit, members whose name also has no
         digit (``A 抗原`` → ``A Ag *`` over ``A1 Ag *``).

  5. **Deterministic keyword overrides** (sims-matrix hard masks
     applied BEFORE the per-position keep_mask + picker):

       - **CLASS FIRST-wins**
         (:func:`.axis.apply_deterministic_class_filter`) — earliest
         match in query of any phrase in ``CLASS_KEYWORD_GATES``
         (ALLERGY / MICRO) hard-masks LOINC rows outside the gated
         CLASS. Top-K probe falls back when no candidate species
         exists in the gated CLASS.
       - **Section-header LAST-wins**
         (:func:`.category._is_section_header_term`) — last segment
         of query (split on ``,，|·・``) exact-match against the
         multilingual whitelist (``Discussion`` / ``讨论`` / ``考察``
         / ``토론`` / ...) hard-masks sims to the section-header
         pool (~2k record-artifact / narrative rows). Defeats source-
         prefix bias (``骨密度,讨论`` → DXA bone density before the
         override).

Consumers:
  - ``mirobody.indicator.resolve.cmd_resolve`` (the user-facing CLI)
  - ``benchmarks/run_resolve.py`` (CSV-IO wrapper, resumable)

Query-side augmentation (``preprocess.augment_zh_aliases``) appends
canonical Latin/English forms for non-English clinical phrases via
the multilingual ``aliases/{lang}.tsv`` bundle members. The augmented
text feeds the embedding call only — keyword overrides (sections 5
above) and ``_compose_loinc_keep`` filters always inspect the
un-augmented ``queries[i]``.

Deliberately omits the full ``FhirAdapter`` stack (axis rerank,
soft specificity demotes, tag gates, alias index, dose-bonus index,
archetype strategy gating, etc.). When a consumer needs that path,
go through ``FhirAdapter.resolve_many`` directly.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Callable

import numpy as np

from ...search import ResolveResult
from ..common import (
    SYSTEM_TO_CODE,
    SYSTEMS,
    _CODE_BITS,
    _CODE_MASK,
    fhir_id_to_code,
    int_to_code,
)
from ..embeddings.local import load as _load_local_cache

log = logging.getLogger(__name__)


# ── Caller-supplied value → SCALE_TYP scale class ────────────────────
#
# Many LOINC analytes carry both quantitative ("Glucose [Mass/volume]
# in Blood", SCALE_TYP=Qn) and qualitative ("Glucose [Presence] in
# Urine by Test strip", SCALE_TYP=Ord) variants. Text-only resolution
# can't tell the two apart without an observed value — both encode the
# same analyte. ``resolve_many``'s ``values`` parameter lets the caller
# break the tie: classify each observed value to a scale class, then
# pass that class into the family rerank as a SCALE_TYP-compatibility
# filter (see :func:`_build_scale_match_rank`, :func:`_loinc_picks_topk`).
#
# Hard rerank rather than weighted boost: the picker swaps the top-1
# to a scale-matching member of the same LOINC family (``loinc_family_key``
# strips bracketed scale markers, so Qn / Ord variants of one analyte
# share a family) when one exists. No score magnitude, no provider-
# specific tuning. Analytes whose only LOINC variant is in a different
# scale (e.g. sodium has no Ord variant) gracefully fall back to the
# cosine top-1 — scale is the softer constraint and relaxes first.

# Ordinal markers — patient-side report tokens for graded results.
# Includes simplified/traditional Chinese, Japanese, Korean, plus "+/-"
# and ASCII "trace". Single ``+`` / ``-`` are ambiguous with "positive
# / negative" (Nom); the regex below matches *only* when they're the
# whole token, so "+" alone classifies as Ord and "(+)" as Nom (via the
# nominal token list below).
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

# Numeric value (optionally with comparator and unit). Examples that
# classify as Qn: "20", "20.5", "1.2e-3", "<10", ">100", "20 mg/dL",
# "0.42 mIU/mL", "5.0×10^6/L". The leading optional comparator is for
# below-limit / above-limit reports.
_VALUE_QN_RE = re.compile(
    r"^\s*[<>≤≥]?\s*"
    r"\d+(?:\.\d+)?(?:[eE][+-]?\d+)?"
    r"(?:\s*[×x*]\s*10[\^]?[+-]?\d+)?"
    r"(?:\s*[^\d].*)?$"              # any trailing unit/text
)

# Nominal tokens — short categorical labels. Includes blood-type letters
# (A/B/AB/O ± Rh+/-), positive/negative markers, reactive/non-reactive
# serology results. Multilingual: Chinese (simp+trad), Japanese, Korean,
# Spanish, German, French, Russian.
_VALUE_NOM_TOKENS: frozenset[str] = frozenset({
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

# Scale-class → ordered SCALE_TYP preference. SCALE_TYP values come
# from LoincTableCore.csv; only the ones we actively bias toward are
# listed here (others — "Multi", "Set", "" — never qualify as a match).
# Tuple position = preference tier (lower index wins): for ``"qn"``,
# a same-family ``Qn`` row beats ``SemiQn`` which beats ``OrdQn`` even
# when cosine ranks them otherwise — keeps a stray ``[Titer]`` SemiQn
# from snatching the top slot from the proper ``[Units/volume]`` Qn
# whenever cosine puts them within tie distance. Within one tier,
# cosine breaks ties as before.
_SCALE_COMPAT: dict[str, tuple[str, ...]] = {
    "qn":  ("Qn", "SemiQn", "OrdQn"),
    "ord": ("Ord", "OrdQn", "SemiQn"),
    "nom": ("Nom",),
    "nar": ("Nar", "Doc"),
}

# Default scale class when no value is observed and no qualitative
# trigger appears in the query text. ``"qn"`` because clinical lab
# indicators are overwhelmingly quantitative (sodium, glucose, CBC,
# CMP, lipids, liver/kidney panels, etc.); the Ord variants exist
# for specific POC / dipstick / rapid-test methods that the override
# below catches via query-side keywords. The choice fixes a provider-
# dependent ambiguity — without a default, a near-tie between Qn and
# Ord variants of the same analyte resolves differently under Gemini
# vs Qwen embeddings; "qn" pins the tie-break direction.
_DEFAULT_SCALE: str = "qn"


# Multilingual qualitative-test trigger markers — when any appears in
# the query text AND no explicit value is supplied, the default scale
# flips from ``"qn"`` to ``"ord"`` for that query. Targets the genuine
# Ord-variant clinical contexts: urine dipstick, rapid antigen, POC
# testing, screening / triage protocols. Markers wrapped by
# :func:`mirobody.indicator.fhir.resolve.specificity._compile_marker_pattern`
# — Latin / Cyrillic / Greek get ``\\b`` word boundaries; CJK
# (Han + Kana + Hangul) match as bare substrings.
_QUALITATIVE_TRIGGER_MARKERS: list[str] = [
    # English (Latin)
    "dipstick", "dip stick", "test strip", "urine strip",
    "urinalysis strip", "reagent strip",
    "rapid", "rapid test", "rapid antigen",
    "POC", "point-of-care", "point of care", "bedside",
    "qualitative", "qual",
    "visual",
    # Simplified Chinese
    "试纸", "尿试纸", "试纸条", "试条",
    "快速", "快速检测", "快速试验", "快速试纸", "速检", "床旁",
    "定性",
    "目测", "肉眼",
    # Traditional Chinese
    "試紙", "尿試紙", "試紙條", "試條",
    "快速檢測", "快速試驗", "快速試紙", "速檢", "床旁",
    "定性",
    "目測",
    # Japanese (Han + Katakana + Hiragana)
    "試験紙", "試験ストリップ", "ストリップ",
    "迅速", "迅速検査", "迅速試験",
    "定性",
    "ベッドサイド",
    "簡易検査",
    # Korean
    "시험지", "시험 스트립",
    "신속", "신속검사",
    "정성",
    # Spanish
    "tira reactiva", "tira de orina", "tira urinaria",
    "prueba rápida", "rápido", "rapida",
    "cualitativo", "cualitativa",
    # German
    "Teststreifen", "Urinstreifen", "Harnteststreifen",
    "Schnelltest", "Schnelldiagnostik",
    "qualitativ",
    # French
    "bandelette", "bandelette urinaire", "bandelette réactive",
    "test rapide", "rapide",
    "qualitatif", "qualitative",
    # Russian
    "тест-полоска", "полоска",
    "экспресс-тест", "экспресс",
    "качественный",
]


def _qualitative_trigger_re() -> "re.Pattern[str]":
    """Lazy-compile the multilingual qualitative-trigger pattern.

    Compiled at first call and cached on the function object — defers
    the :mod:`.specificity` import until ``resolve_many`` actually
    runs, keeping module import cheap for callers that only use the
    helper functions exported from :mod:`.pipeline`.
    """
    cached = getattr(_qualitative_trigger_re, "_cached", None)
    if cached is not None:
        return cached
    from .specificity import _compile_marker_pattern
    pat = _compile_marker_pattern(_QUALITATIVE_TRIGGER_MARKERS)
    _qualitative_trigger_re._cached = pat       # type: ignore[attr-defined]
    return pat


def _classify_value(value: str | None) -> str | None:
    """Map an observed value string to a SCALE_TYP scale class.

    Returns one of ``"qn"`` (numeric, w/ or w/o unit), ``"ord"`` (graded
    +/− markers), ``"nom"`` (positive/negative/blood-type tokens), or
    ``"nar"`` (free narrative text). ``None`` for empty / unparseable
    input — caller skips scale rerank for that term.

    Order: ord-first (so ``"+"`` doesn't fall into the qn-trailing-unit
    branch as a sign), then qn, then nom (token-exact), else nar if the
    remaining text is non-empty.
    """
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if _VALUE_ORD_RE.match(s):
        return "ord"
    if s.lower() in _VALUE_NOM_TOKENS:
        return "nom"
    if _VALUE_QN_RE.match(s):
        return "qn"
    return "nar"


def _build_scale_match_rank(
    scale_class: str,
    axis_data: dict,
    n_rows: int,
) -> "np.ndarray | None":
    """Build (N,) int8 rank array flagging each LOINC row's tier within
    :data:`_SCALE_COMPAT` [*scale_class*].

    Values: ``-1`` for rows whose SCALE_TYP isn't in the compat tuple
    (or have no axis data), ``0`` for the strongest-preferred tier,
    ``1`` for the next, etc. Used by :func:`_loinc_picks_topk` to sort
    stage-2 matching rows by ``(rank, -cosine)`` — keeps a true ``Qn``
    above a ``SemiQn`` titer when both share an analyte family and sit
    close on cosine, while preserving cosine order within one tier.

    Hard rerank — no weighted score, no magnitude tuning. Preserves
    provider-independence ("swap Gemini for Qwen3 without retuning")
    because scale disambiguation reads only corpus structure, not the
    cosine distribution of any specific embedding model.

    Non-LOINC rows and LOINC rows missing axis data are always ``-1`` —
    the rank only governs the LOINC-vs-LOINC family rerank, never
    cross-system ranking.

    Returns ``None`` when *scale_class* has no compatibility entry, or
    when ``axis_data`` lacks ``SCALE_TYP`` (e.g. axis bundle unavailable
    in a stripped deployment).
    """
    compat = _SCALE_COMPAT.get(scale_class)
    if not compat:
        return None
    values_list = axis_data.get("values", {}).get("SCALE_TYP")
    row_value_idx = axis_data.get("row_value_idx", {}).get("SCALE_TYP")
    if values_list is None or row_value_idx is None:
        return None
    # Map centroid_idx → rank tier. Values not in compat get no entry,
    # so rows pointing at them keep the default -1.
    centroid_rank: dict[int, int] = {}
    for tier, v in enumerate(compat):
        for i, vv in enumerate(values_list):
            if vv == v:
                centroid_rank[i] = tier
    if not centroid_rank:
        return None
    rank = np.full(n_rows, -1, dtype=np.int8)
    for centroid_idx, tier in centroid_rank.items():
        rank[row_value_idx == centroid_idx] = tier
    return rank


# ── Analyte-digit extraction ─────────────────────────────────────────
#
# Integer 1–999 that names a subtype within an analyte family (``HPV-43``,
# ``曲霉菌1型``, ``Vit K2``, ``CD4``, ``Coxsackie B5``, ``FEV1``).
# Excluded: time intervals (``1秒``/``2小时``), dose magnitudes (``75g``),
# product codes. Strict integer boundary so ``1.5 hours`` doesn't surface
# the ``5``.

_INT_TOK = re.compile(r"(?<![.\d])(\d{1,3})(?![.\d])")

# Tokens that, when *following* a digit, redefine the digit as a
# time/duration rather than an analyte subtype. Two alternation blocks:
#
#   - CJK tokens (``秒/小时/分钟/天/...``): no ``\b`` — CJK has no
#     regex word-boundary between consecutive CJK chars, so ``\b``
#     between ``秒`` and ``率`` (as in ``1秒率``) FAILS, leaving the
#     digit incorrectly classified as analyte.
#   - English tokens (``hours/min/day/...``): ``\b`` required so
#     ``min`` doesn't fire inside ``mineral``.
_AFTER_DIGIT_IS_TIME = re.compile(
    r"\s*(?:"
    r"小时|分钟|时辰|月份|[秒天周月年岁]"
    r"|(?:hours?|hrs?|h|min(?:ute)?s?|days?|weeks?|months?|years?|sec(?:ond)?s?)\b"
    r")",
    re.IGNORECASE,
)

# Slash-alternation (``HIV-1/2``, ``Type 1/2``) and clock time
# (``24:00``, ``8:30``) — bail out of digit extraction entirely. The
# combined panel code (``HIV 1+2 Ab``) wins on cosine; forcing a
# digit constraint would push to a single-subtype variant instead.
_QUERY_NON_ANALYTE_DIGIT_CONTEXT = re.compile(r"\d+\s*[/:]\s*\d+")


def query_analyte_digit(text: str) -> int | None:
    """First analyte-position digit in *text*, or None.

    Roman-numeral subtype markers are normalized to Arabic digits
    before parsing (``Ⅰ`` / ``I型`` / ``Type II`` → ``1`` / ``1型`` /
    ``Type 2``) — see :func:`preprocess.normalize_roman_numerals` for
    the substitution surface. This keeps the picker in sync with the
    embedding side, which sees the same normalized text.
    """
    from ..embeddings.preprocess import normalize_roman_numerals
    text = normalize_roman_numerals(text)
    if _QUERY_NON_ANALYTE_DIGIT_CONTEXT.search(text):
        return None
    for m in _INT_TOK.finditer(text):
        if _AFTER_DIGIT_IS_TIME.match(text[m.end():]):
            continue
        return int(m.group(1))
    return None


def loinc_analyte_digits(name: str) -> frozenset[int]:
    """All analyte-position digits in a LOINC name.

    Excludes ``+``-grouped multi-analyte panels (``HPV 6+11+42+43+44``
    contributes nothing) and challenge/duration suffixes (``--2 hours
    post dose``). Includes glued-digit analyte names (``FEV1``, ``CD4``,
    ``Vit B12``).
    """
    if not name:
        return frozenset()
    digits: set[int] = set()
    for m in _INT_TOK.finditer(name):
        s, e = m.start(), m.end()
        if (s > 0 and name[s - 1] == "+") or (e < len(name) and name[e] == "+"):
            continue
        if _AFTER_DIGIT_IS_TIME.match(name[e:]):
            continue
        digits.add(int(m.group(1)))
    return frozenset(digits)


# Chemical / biological analyte name → numeric subtype map. Used by
# ``_build_family_index`` to augment ``row_digits`` and
# ``row_name_has_digit`` so the strict-digit null-out guard in
# ``_loinc_picks_topk`` can distinguish K1 (= LOINC's Phytonadione)
# from K2 (= no LOINC code, Menaquinone in literature).
#
# The map is loaded from the bundle (``analyte_digit.tsv`` auto-mined
# from LOINC ``RELATEDNAMES2`` + SNOMED CT ``(substance|product)``
# synonyms, plus ``analyte_digit_curated.tsv`` for misses both sources
# lack) via :func:`load_analyte_digit_aliases`. See
# :mod:`mirobody.indicator.fhir.embeddings.analyte_digit` for the build
# pipeline and TSV schema. When the bundle is missing or the TSV
# members aren't present, the loader returns an empty dict — the
# resolver continues to work without aliasing (digit-null guard simply
# admits the cosine top-1 for families without digit-bearing members).
#
# In-scope families: Vitamin B (Thiamine/Riboflavin/Niacin/…),
# Vitamin K (Phytonadione/Menaquinone/Menadione), IGF (Insulin-like
# growth factor-I/II), Vitamin A (Retinol), Vitamin D (Calcidiol).
# Other digit-subtype patterns surveyed in the indicator catalog
# (HPV-N / CD-N / IgG-N / Omega-N / Vit D2/D3 / CA-N / L-N spine)
# already carry their digit in the LOINC display name (``CD4`` /
# ``Omega 3 fatty acids`` / ``(Vit D2)``), so the existing
# ``loinc_analyte_digits`` extractor handles them without aliasing.


def _build_analyte_alias_re(alias_keys: list[str]) -> "re.Pattern | None":
    """Compile a ``\\b``-anchored, case-insensitive alternation over
    the alias keys. Listed longest-first so Python's leftmost-first
    alternation produces leftmost-longest matches (avoids ``calciferol``
    swallowing the inner ``cholecalciferol``).

    Returns ``None`` when *alias_keys* is empty — callers treat that
    as "no aliasing".
    """
    if not alias_keys:
        return None
    return re.compile(
        r"\b(?:"
        + "|".join(re.escape(k) for k in sorted(alias_keys, key=lambda x: -len(x)))
        + r")\b",
        re.IGNORECASE,
    )


def _analyte_alias_state() -> tuple[dict[str, int], "re.Pattern | None"]:
    """Lazy loader: returns the alias dict and compiled regex,
    materializing them on first call. Returns ``({}, None)`` when the
    bundle has no analyte-digit TSV — the pipeline degrades to the
    pre-alias behavior (cosine fallback for non-numeric-named families).
    """
    cached = getattr(_analyte_alias_state, "_cache", None)
    if cached is not None:
        return cached
    from ..embeddings.analyte_digit import load_analyte_digit_aliases
    aliases = load_analyte_digit_aliases()
    rx = _build_analyte_alias_re(list(aliases.keys()))
    _analyte_alias_state._cache = (aliases, rx)  # type: ignore[attr-defined]
    return aliases, rx


def analyte_alias_digits(name: str) -> frozenset[int]:
    """All numeric subtypes inferred from chemical-name aliases in *name*.

    Complements :func:`loinc_analyte_digits` for LOINC display names
    whose canonical analyte word lacks the numeric subtype the
    indicator catalog uses (``Thiamine`` for ``B1``, ``Phytonadione``
    for ``K1``, ``Insulin-like growth factor-I`` for ``IGF-1``).
    Empty set when no alias key matches OR when the alias table is
    empty (bundle missing the TSV members).
    """
    if not name:
        return frozenset()
    aliases, rx = _analyte_alias_state()
    if rx is None:
        return frozenset()
    return frozenset(
        aliases[m.group(0).lower()] for m in rx.finditer(name)
    )


# Family-key normalization. Strip ``+``-chains, bracketed qualifiers
# (``[Presence]`` / ``[Mass/volume]`` — SCALE_TYP variants, not family
# boundaries), ``--<challenge tail>``, and analyte-position digits.
# Result is a coarse-grained family signature.
_PLUS_CHAIN = re.compile(r"\d{1,3}(?:\+\d{1,3})+")
_BRACKETED = re.compile(r"\[[^\]]*\]")
_DOUBLE_DASH_TAIL = re.compile(r"\s*--.*$")
_STANDALONE_DIGIT = re.compile(r"(?<![.\d+])\d{1,3}(?![.\d+])")
_WS = re.compile(r"\s+")


def loinc_family_key(name: str) -> str:
    if not name:
        return ""
    s = _DOUBLE_DASH_TAIL.sub("", name)
    s = _BRACKETED.sub(" ", s)
    s = _PLUS_CHAIN.sub(" ", s)
    s = _STANDALONE_DIGIT.sub(" ", s)
    return _WS.sub(" ", s).strip().lower()


# ── Cached cache-state helpers ───────────────────────────────────────
#
# Both the family index and the deprecated penalty vector are derived
# purely from ``cache["names"]`` / ``cache["canonical"]`` and don't
# depend on the query — so they're built once per cache and stashed on
# the cache dict under their descriptive keys (``_family_index``,
# ``_deprecated_drop_mask``, ``_loinc_has_*_mask``, ...).


def _build_family_index(
    cache: dict,
    loinc_idx: np.ndarray,
) -> tuple[
    list[frozenset[int]],
    list[str],
    dict[str, list[int]],
    np.ndarray,
]:
    """Return (row_analyte_digits, row_family_key, family_to_rows,
    row_name_has_digit), each indexed by full corpus row (non-LOINC
    rows hold sentinel values / False). Cached on *cache* under
    ``_family_index``.

    ``row_name_has_digit`` is a coarser predicate than
    ``loinc_analyte_digits`` — True iff any digit appears anywhere in
    the display name AFTER stripping the ``--<challenge tail>`` suffix.
    Includes ``+``-chain panel digits (``HPV 6+11+42+43+44 DNA``) and
    plain standalone subtype digits (``HPV 16 DNA``) but excludes
    challenge-time qualifiers (``Glucose --2 hours post dose glucose``
    becomes "Glucose ..." with no digit). Used by the strict-digit
    null-out guard to detect families that enumerate subtypes by
    number even when ``loinc_analyte_digits`` (which excludes
    ``+``-chains on purpose) reports an empty set.
    """
    cached = cache.get("_family_index")
    if cached is not None:
        return cached
    names = cache["names"]
    n_rows = len(names)
    row_digits: list[frozenset[int]] = [frozenset()] * n_rows
    row_key: list[str] = [""] * n_rows
    family_to_rows: dict[str, list[int]] = {}
    row_name_has_digit = np.zeros(n_rows, dtype=bool)
    # Roman-numeral subtype markers in LOINC display names mirror what
    # we already normalize on the query side (``Procollagen type III``
    # ↔ Type 3, ``HTLV I+II`` ↔ HTLV 1+2). Without this, the family
    # rerank can't tell ``Ⅳ型胶原`` apart from ``Procollagen type III``
    # — both carry zero Arabic digits in their canonical forms, so
    # passes_digit falls through and the wrong-Roman-numeral cosine
    # top-1 wins. We normalize once, then derive ``row_digits`` /
    # ``row_name_has_digit`` / ``family_key`` from the normalized text
    # so same-base different-Roman variants land in the same family
    # and the strict-digit guard can null-out wrong subtypes.
    #
    # ``row_name_has_digit`` MUST stay consistent with
    # ``loinc_analyte_digits``: both encode "this name enumerates an
    # analyte subtype by number". A looser ``_ANY_DIGIT.search`` rule
    # would mis-flag structural-position digits (``Procollagen type
    # 3.N-terminal propeptide`` — the ``3`` is dropped by
    # ``loinc_analyte_digits`` because of the trailing ``.``, but a
    # bare digit search would still match and the null-out guard would
    # then drop the correct N-terminal-propeptide match). The right
    # rule fires when the row actually contributes analyte / alias
    # digits, OR carries a ``+``-chain panel (``HPV 6+11+42+43+44`` —
    # every digit is dropped from ``loinc_analyte_digits`` because of
    # the ``+`` adjacency, but the chain itself signals a numeric-
    # enumeration family).
    from ..embeddings.preprocess import normalize_roman_numerals
    t0 = time.perf_counter()
    for i in loinc_idx:
        ri = int(i)
        nm = normalize_roman_numerals(names[ri] or "")
        analyte_digits = loinc_analyte_digits(nm)
        # Augment analyte digits with subtype numbers inferred from
        # chemical-name aliases (Thiamine ↔ 1, Phytonadione ↔ 1,
        # Cobalamin ↔ 12, …). Keeps the strict-digit null-out guard
        # honest for queries like ``维生素K2`` where the cosine top-1
        # (Phytonadione = K1) carries no digit in its display name but
        # is semantically the wrong subtype.
        alias_digits = analyte_alias_digits(nm)
        if alias_digits:
            analyte_digits = analyte_digits | alias_digits
        row_digits[ri] = analyte_digits
        key = loinc_family_key(nm)
        row_key[ri] = key
        if key:
            family_to_rows.setdefault(key, []).append(ri)
        if analyte_digits or _PLUS_CHAIN.search(nm):
            row_name_has_digit[ri] = True
    log.info(
        "v2 family index: %d LOINC rows, %d unique families (%.2fs)",
        len(loinc_idx), len(family_to_rows), time.perf_counter() - t0,
    )
    cache["_family_index"] = (
        row_digits, row_key, family_to_rows, row_name_has_digit,
    )
    return cache["_family_index"]


def _build_deprecated_drop_mask(cache: dict, n_rows: int) -> np.ndarray:
    """Return bool[N] with True on rows whose display name starts with
    ``Deprecated `` (case-insensitive prefix). Cached on *cache* under
    ``_deprecated_drop_mask``.

    Used as a hard drop in :func:`resolve_many`: deprecated rows are
    stripped from every system's candidate pool before picking. LOINC
    marks codes ``Deprecated`` only when a non-deprecated successor
    exists in the same concept space, so dropping them outright loses
    no clinically reachable answer. The previous ``-2.0`` cosine demote
    was equivalent in effect but its magnitude was tuned to a specific
    embedding provider — a hard drop is embedding-agnostic.
    """
    cached = cache.get("_deprecated_drop_mask")
    if cached is not None:
        return cached
    names = cache.get("names")
    mask = np.zeros(n_rows, dtype=bool)
    if names is not None:
        for ri, nm in enumerate(names):
            if nm and nm.lower().startswith("deprecated "):
                mask[ri] = True
    log.info(
        "v2 deprecated drop mask: %d rows flagged",
        int(mask.sum()),
    )
    cache["_deprecated_drop_mask"] = mask
    return mask


# ── Per-query LOINC keep-mask filter pipeline ───────────────────────
#
# Each filter is a tiny adapter over a domain module (analyte_concept,
# specificity, challenge_time, ...). Given a query text and the loaded
# cache, it returns a bool[N] keep mask (True = candidate survives) or
# ``None`` to signal "nothing to filter on for this query, skip me".
#
# The main loop AND-merges every non-None result with the static
# deprecate-stripped pool. AND is commutative — registry order is for
# readability, not semantics.
#
# Adding a filter = write a small ``foo_keep(query_text, cache)``
# function and append to ``_LOINC_FILTERS``. No change to the main
# loop. Filters cache their static corpus-side state on the cache
# dict, so repeated calls within one ``resolve_many`` invocation
# pay the build cost at most once.
#
# Conservative semantics across all filters: when the query carries a
# signal, drop ONLY the opposing-signal rows; rows that carry the
# signal AND rows that carry neither survive. Combo / panel rows
# (e.g. ``HIV 1+2 Ab + p24 Ag``) survive in both modes for analyte
# concept; rows with no time qualifier survive challenge-time
# filtering. Drop hard, never demote — the picker stays free of
# embedding-specific cosine magnitudes.


_LoincKeepFilter = Callable[[str, dict], "np.ndarray | None"]


def _transfusion_subject_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop ``from Donor`` / ``from Blood product unit`` LOINC rows
    when the query carries no transfusion-medicine license marker.
    Genuine blood-bank queries (``供血者 / 输血 / donor / transfusion
    ...``) license the family and pass through unchanged.
    """
    from .specificity import (
        _ensure_loinc_masks,
        query_licensed_families,
    )
    masks = _ensure_loinc_masks(cache)
    m = masks.get("transfusion_subject")
    if m is None:
        return None
    if "transfusion_subject" in query_licensed_families(query_text):
        return None
    return ~m


def _analyte_concept_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop LOINC rows whose name carries the opposite Ag/Ab token to
    the query's LAST marker. Multilingual (English / Chinese / Japanese
    / Korean / Spanish / German / French / Russian). Rows with neither
    token survive; combo Ag+Ab panels survive in both modes.
    """
    from .analyte_concept import (
        concept_keep_mask,
        query_analyte_concept,
    )
    concept = query_analyte_concept(query_text)
    if concept is None:
        return None
    return concept_keep_mask(cache, concept)


# Standalone Latin uppercase letter as a single-letter analyte marker.
# ``(?<![A-Za-z])([A-Z])(?![A-Za-z])`` — the letter is preceded and
# followed by anything that isn't a Latin letter (CJK chars, digits,
# whitespace, ``-``, ``+``, start / end of string). Catches:
#
#   - ``D`` in ``抗D抗体`` (Chinese: anti-D antibody)
#   - ``B`` in ``Hep B 表面抗原`` / ``Hepatitis B surface antigen``
#   - ``A`` in ``A 抗原`` / ``A1 Ag``
#   - ``K`` in ``维生素 K`` / ``Vitamin K`` (but only if a concept
#     marker is also present, see ``_letter_analyte_keep``)
#
# Rejects:
#   - ``CD`` / ``IgG`` / ``CEA`` / ``PSA`` / ``HCG`` / ``FEV`` —
#     multi-letter abbreviations don't surface as single-letter
#     analyte markers.
#   - ``甲`` / ``乙`` / ``丙`` / ``丁`` / ``戊`` — Chinese / Japanese
#     heavenly-stems ordinals (A..E). DELIBERATELY EXCLUDED: in
#     Chinese medical text these chars are overwhelmingly compound
#     stems for chemistry / anatomy (``甲状腺`` thyroid, ``甲胎蛋白``
#     alpha-fetoprotein, ``乙酰胆碱`` acetylcholine, ``丙氨酸``
#     alanine, ``丁酸`` butyrate). Mapping ``甲→A`` etc. would
#     misroute these to letter-A LOINC codes. Genuine ordinal usage
#     (``乙型肝炎``, ``丁型`` ) is statistically rarer in the
#     indicator corpora than these compound usages — keep the filter
#     scope tight.
_LETTER_ANALYTE_RE = re.compile(r"(?<![A-Za-z])([A-Z])(?![A-Za-z])")


def _build_loinc_letter_index(cache: dict) -> dict[str, np.ndarray]:
    """``{letter: rows}`` over LOINC rows whose display name carries
    that uppercase letter as a standalone analyte marker. Cached on
    *cache* under ``_loinc_letter_index``.
    """
    cached = cache.get("_loinc_letter_index")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    if not names:
        cache["_loinc_letter_index"] = {}
        return cache["_loinc_letter_index"]
    canonical = cache["canonical"]
    sys_arr = (canonical >> _CODE_BITS).astype(np.int8)
    loinc_sys = SYSTEM_TO_CODE["LOINC"]
    per_letter: dict[str, list[int]] = {}
    for r in np.where(sys_arr == loinc_sys)[0]:
        nm = names[r]
        if not nm:
            continue
        for m in _LETTER_ANALYTE_RE.finditer(nm):
            per_letter.setdefault(m.group(1), []).append(int(r))
    index = {L: np.asarray(rows, dtype=np.int64) for L, rows in per_letter.items()}
    log.info(
        "loinc letter analyte index: %s",
        ", ".join(f"{L}={len(v)}" for L, v in sorted(per_letter.items())),
    )
    cache["_loinc_letter_index"] = index
    return index


def _letter_analyte_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Require LOINC names to carry the query's standalone uppercase-
    letter analyte (e.g. ``抗D抗体`` → require ``D``; ``Hep B 表面
    抗原`` → require ``B``). Scoped to queries that already match
    ``_analyte_concept_keep`` (have an Ag/Ab marker) — outside that
    scope the filter is silent, so analyte / vitamin / chemistry
    queries (``维生素 K`` → Phytonadione, ``CRP``, ``甲胎蛋白``)
    stay on their cosine top.

    Hybrid: strict when at least one corpus row carries one of the
    query letters as standalone; skip when no row matches (avoids
    emptying the pool for letters the corpus simply doesn't use as
    standalone analyte markers).

    Resolves the ``D Ab`` vs ``Blood group antibody titered`` tie that
    cosine alone can't break — family rerank can't bridge the two
    because their COMPONENT-base family keys differ.

    Multilingual: Latin uppercase letters are the analyte-marker
    convention across English / Spanish / German / French / Russian /
    most international medical writing, so a single regex catches them
    all. CJK heavenly-stems ordinals (``甲乙丙丁戊``) are intentionally
    excluded — they're overwhelmingly compound stems in Chinese medical
    text (``甲状腺`` / ``丙氨酸`` / ``乙酰`` …); a naive ordinal-to-
    letter map would create more false positives than it'd fix.
    """
    from .analyte_concept import query_analyte_concept
    if query_analyte_concept(query_text) is None:
        return None
    letters = set()
    for m in _LETTER_ANALYTE_RE.finditer(query_text):
        letters.add(m.group(1))
    if not letters:
        return None
    index = _build_loinc_letter_index(cache)
    if not index:
        return None
    n = int(cache["arr"].shape[0])
    matching = np.zeros(n, dtype=bool)
    for L in letters:
        rows = index.get(L)
        if rows is not None:
            matching[rows] = True
    if matching.any():
        return matching
    return None


def _intake_recall_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop LOINC rows whose name carries ``intake N hour Measured /
    Estimated`` (209 dietary-survey instrument codes — ``Vitamin B1
    (Thiamine) intake 24 hour Measured``, ``Iron intake 24 hour
    Estimated``, ``Potassium intake 24 hour Estimated``, …) when the
    query carries no dietary / intake marker.

    Without this filter, lab-indicator queries like ``维生素 B1`` /
    ``维生素 B2`` / ``铁`` cosine-match the intake-survey variant
    because the embedding sees ``Vitamin B1`` / ``Iron`` verbatim and
    the ``intake N hour`` qualifier is too subtle a signal for the
    encoder to penalize. Licensed by ``摄入 / 膳食 / 饮食 / intake /
    dietary / Ernährung / 食事 / 식이 / ...`` (full multilingual list
    in production's ``specificity.FAMILIES['intake_recall']``).
    """
    from .specificity import (
        _ensure_loinc_masks,
        query_licensed_families,
    )
    masks = _ensure_loinc_masks(cache)
    m = masks.get("intake_recall")
    if m is None:
        return None
    if "intake_recall" in query_licensed_families(query_text):
        return None
    return ~m


def _explicit_dose_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Match LOINC ``--post N <unit>`` dose qualifiers to explicit
    ``(value, unit)`` tokens in the query. Strict: when the query
    carries no explicit dose, every dose-qualified row is dropped —
    a query like ``胰岛素(三小时)`` shouldn't land on ``--3 hours post
    75 g glucose PO`` because the user never typed ``75 g``; the
    generic ``--3 hours post dose glucose`` is the correct level of
    specificity.

    Symmetric to ``_challenge_time_keep``:
      - Query has dose set *D* AND corpus has rows matching some d∈D:
        keep matching rows AND no-dose rows.
      - Query has dose set *D* but no corpus match:
        keep only no-dose rows.
      - Query has no dose token (most queries):
        drop every dose-qualified row.

    Implicit/context-based doses (e.g. ``OGTT`` implies 75 g glucose)
    are NOT honored here — the user's deterministic principle is
    "what the query types is what gets matched". Production's
    ``context_implied_doses`` would soften this; v2 deliberately keeps
    it strict so embedding-agnostic literal matching survives provider
    swaps.
    """
    from ..units.normalize import scan_value_units

    dose_index = cache.get("dose_index") or {}
    if not dose_index:
        return None
    has_dose = cache.get("_loinc_has_dose_mask")
    if has_dose is None:
        n = int(cache["arr"].shape[0])
        has_dose = np.zeros(n, dtype=bool)
        for rows in dose_index.values():
            has_dose[rows] = True
        cache["_loinc_has_dose_mask"] = has_dose
    doses = set(scan_value_units(query_text))
    matching = np.zeros(len(has_dose), dtype=bool)
    for key in doses:
        rows = dose_index.get(key)
        if rows is not None:
            matching[rows] = True
    return matching | ~has_dose


def _xxx_challenge_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop LOINC rows whose name carries the ``XXX challenge``
    placeholder. ``XXX`` is LOINC's parent-code marker for an
    unspecified challenge agent — virtually never the right pick for
    real clinical queries where ``--post 75 g glucose PO`` /
    ``--post dose glucose`` / ``--post meal`` variants are available.
    Markers contain only literal ``XXX`` (which queries almost never
    use), so this family effectively always fires.
    """
    from .specificity import (
        _ensure_loinc_masks,
        query_licensed_families,
    )
    masks = _ensure_loinc_masks(cache)
    m = masks.get("xxx_challenge")
    if m is None:
        return None
    if "xxx_challenge" in query_licensed_families(query_text):
        return None
    return ~m


# Pre-compiled HPV display-name patterns. Matched against LOINC names
# to bucket each type-specific row into Ag (1990s serology, 17xxx
# range) or DNA / RNA (modern molecular probe, 61xxx / 95xxx range).
_HPV_AG_RE = re.compile(r"\bHuman papilloma virus (\d+) Ag\b")
_HPV_DNA_RE = re.compile(r"\bHuman papilloma virus (\d+) (?:DNA|RNA)\b")


def _build_hpv_ag_dna_index(cache: dict) -> tuple[dict[int, list[int]], frozenset[int]]:
    """Return ``({digit: ag_rows}, {digit: ...DNA-or-RNA-bearing})`` for
    HPV display names. Cached on *cache* under ``_hpv_ag_dna_index``.
    """
    cached = cache.get("_hpv_ag_dna_index")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    if not names:
        empty: dict[int, list[int]] = {}
        cache["_hpv_ag_dna_index"] = (empty, frozenset())
        return cache["_hpv_ag_dna_index"]
    canonical = cache["canonical"]
    sys_arr = (canonical >> _CODE_BITS).astype(np.int8)
    loinc_sys = SYSTEM_TO_CODE["LOINC"]
    ag_by_digit: dict[int, list[int]] = {}
    dna_digits: set[int] = set()
    for r in np.where(sys_arr == loinc_sys)[0]:
        nm = names[r]
        if not nm:
            continue
        m = _HPV_AG_RE.search(nm)
        if m:
            ag_by_digit.setdefault(int(m.group(1)), []).append(int(r))
            continue
        m = _HPV_DNA_RE.search(nm)
        if m:
            dna_digits.add(int(m.group(1)))
    log.info(
        "hpv ag/dna index: Ag types %s, DNA types %d",
        sorted(ag_by_digit), len(dna_digits),
    )
    cache["_hpv_ag_dna_index"] = (ag_by_digit, frozenset(dna_digits))
    return cache["_hpv_ag_dna_index"]


def _hpv_serology_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop legacy HPV serology Ag rows (``Human papilloma virus N
    Ag``, 17xxx range) for queries asking about a specific HPV type
    WHEN LOINC also has a molecular ``Human papilloma virus N
    (DNA|RNA)`` peer for that same type. Methodology consistency:
    cervical HPV detection is uniformly DNA/RNA probe in modern
    practice, but the embedding can't reliably prefer one method over
    the other so the picked methodology bounces type-by-type.

    Conditional, not blanket: types LOINC never molecularized (5 and
    43 in the current corpus) keep their Ag code, because it's the
    only type-specific target available — dropping it would push the
    pick to a multi-type panel (``HPV 6+11+42+43+44``) or to the
    wrong type number.
    """
    digit = query_analyte_digit(query_text)
    if digit is None:
        return None
    ag_by_digit, dna_digits = _build_hpv_ag_dna_index(cache)
    if digit not in dna_digits or digit not in ag_by_digit:
        return None
    n = int(cache["arr"].shape[0])
    mask = np.ones(n, dtype=bool)
    mask[ag_by_digit[digit]] = False
    return mask


def _post_meal_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop LOINC rows whose name ends in ``--post meal`` /
    ``--postprandial`` (a specific 75-g-glucose-vs-meal protocol
    distinction the embedding can't reliably make) when the query
    carries no explicit meal-context marker. Mirrors the
    transfusion_subject pattern: corpus-side mask + multilingual
    license markers, both shared with production's ``specificity.py``.
    """
    from .specificity import (
        _ensure_loinc_masks,
        query_licensed_families,
    )
    masks = _ensure_loinc_masks(cache)
    m = masks.get("post_meal")
    if m is None:
        return None
    if "post_meal" in query_licensed_families(query_text):
        return None
    return ~m


def _challenge_time_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Filter LOINC rows by ``--N hours? post X`` challenge-time match
    against the query's explicit time tokens. Hybrid strict-with-
    fallback semantics:

      - Query carries no time token AND no challenge marker
        (``A 抗原``, ``镁``): filter is silent (returns None).
      - Query carries time *T* AND corpus has rows matching *T*:
        STRICT — keep only T-matching rows. The user explicitly named
        a time; no-time generics like ``Insulin [Presence] in Unknown
        substance`` are dropped to keep the picker from cosine-tying
        them with the correct time-qualified code.
      - Query carries time *T* but corpus has NO rows matching *T*
        (e.g. ``维生素 C 半小时`` — no challenge-time vitamin C exists):
        FALLBACK — keep the no-time generic codes. Avoids returning
        empty when the analyte simply has no challenge variant.
      - Query carries a CHALLENGE MARKER but no specific time (``空腹
        血糖``, ``fasting glucose``, ``postprandial`` without ``2h``):
        GENERIC — keep only no-time rows. Defeats the cosine pull
        toward ``--pre 12 hour fast`` / ``--8 hours fasting`` siblings
        when the user only asked for the canonical ``Fasting glucose``
        code (1558-6). The user didn't name 8/10/12 hours; we don't
        get to invent one for them.

    Fixes (a) ``C-肽(半小时)`` previously cosine-matching ``--4.5
    hours post meal``, (b) ``胰岛素(半小时)`` previously cosine-
    matching ``[Presence] in Unknown substance``, and (c) ``空腹血糖``
    previously cosine-matching ``--pre 12 hour fast`` instead of the
    canonical Fasting glucose code.
    """
    from .challenge_time import (
        _build_loinc_time_index,
        query_time_intervals,
    )
    from .specificity import query_licensed_families
    times = query_time_intervals(query_text)
    has_challenge_marker = "challenge_test" in query_licensed_families(query_text)
    if not times and not has_challenge_marker:
        return None
    idx = _build_loinc_time_index(cache)
    if not idx:
        return None
    # Static "has any time qualifier" mask, cached on the cache dict —
    # the OR of every key's row set, doesn't depend on the query.
    has_time = cache.get("_loinc_has_time_mask")
    if has_time is None:
        n = int(cache["arr"].shape[0])
        has_time = np.zeros(n, dtype=bool)
        for rows in idx.values():
            has_time[rows] = True
        cache["_loinc_has_time_mask"] = has_time
    if not times:
        # Challenge marker present but no specific time → drop all
        # time-qualified rows. Keeps the canonical no-qualifier code.
        return ~has_time
    matching = np.zeros(len(has_time), dtype=bool)
    for key in times:
        rows = idx.get(key)
        if rows is not None:
            matching[rows] = True
    if matching.any():
        return matching          # strict: only T-matching rows
    return ~has_time             # fallback: no-time rows when no T match


_LOINC_FILTERS: list[tuple[str, _LoincKeepFilter]] = [
    ("transfusion_subject", _transfusion_subject_keep),
    ("analyte_concept",     _analyte_concept_keep),
    ("letter_analyte",      _letter_analyte_keep),
    ("hpv_serology",        _hpv_serology_keep),
    ("challenge_time",      _challenge_time_keep),
    ("post_meal",           _post_meal_keep),
    ("intake_recall",       _intake_recall_keep),
    ("xxx_challenge",       _xxx_challenge_keep),
    ("explicit_dose",       _explicit_dose_keep),
]


_TOPK_PROBE_FALLBACK_FILTERS = frozenset({"analyte_concept"})


def _compose_loinc_keep(
    query_text: str,
    cache: dict,
    sims_row: "np.ndarray | None" = None,
    loinc_idxs: "np.ndarray | None" = None,
) -> "np.ndarray | None":
    """Walk ``_LOINC_FILTERS`` and AND-merge every non-None keep mask.
    Returns ``None`` when no filter fires for the query (fast path —
    caller skips the mask intersection in ``_loinc_picks_topk``).

    For filters in :data:`_TOPK_PROBE_FALLBACK_FILTERS`, if the filter's
    mask would eliminate every LOINC row in the raw-cosine top-10
    candidate set for this query, the filter is skipped (sims says
    the species / intent lives in the masked-out region — e.g. an
    "X抗体" query whose species X only has Ag-only rows in LOINC).
    Same fallback shape as :func:`apply_deterministic_class_filter`.
    Other filters (transfusion / challenge_time / explicit_dose / …)
    are pure noise-reduction by design and stay unconditional.

    Roman-numeral subtype markers are normalized to Arabic digits
    before filter dispatch so all filters see the same canonical text
    the embedding and picker see. Without this, ``_letter_analyte_keep``
    would treat ``HSV-I型`` as a standalone-letter-I query and mask
    every LOINC row that lacks ``I`` as a free token — the very
    ``Herpes simplex virus 1`` rows the embedding is trying to surface.
    """
    from ..embeddings.preprocess import normalize_roman_numerals
    query_text = normalize_roman_numerals(query_text)
    keep: "np.ndarray | None" = None
    topk_idx: "np.ndarray | None" = None
    if sims_row is not None and loinc_idxs is not None and len(loinc_idxs) > 0:
        k = min(10, len(loinc_idxs))
        topk_idx = loinc_idxs[
            np.argpartition(-sims_row[loinc_idxs], k - 1)[:k]
        ]
    for name, flt in _LOINC_FILTERS:
        m = flt(query_text, cache)
        if m is None:
            continue
        if name in _TOPK_PROBE_FALLBACK_FILTERS and topk_idx is not None:
            if not m[topk_idx].any():
                continue
        keep = m if keep is None else (keep & m)
    return keep


# ── Picking ──────────────────────────────────────────────────────────


# Cosine top-N window used by the SCALE_TYP rerank. N must be wide
# enough to capture both Qn and Ord variants of one analyte even when
# they sit in different LOINC families (``Glucose [Mass/volume] in
# Urine`` vs ``Glucose [Presence] in Urine by Test strip`` — the "by
# Test strip" suffix forces different ``loinc_family_key`` outputs).
# 50 is generous: the same-analyte sibling pool rarely exceeds 30 rows,
# and the rerank is O(N) so the extra rows cost nothing meaningful.
_SCALE_RERANK_WINDOW: int = 50


def _loinc_picks_topk(
    sims_row: np.ndarray,
    query_text: str,
    loinc_idxs: np.ndarray,
    family_index: tuple | None,
    top_k: int,
    *,
    keep_mask: np.ndarray | None = None,
    scale_match_rank: np.ndarray | None = None,
) -> list[int]:
    """Pick up to *top_k* LOINC rows for one query.

    Stage 0 (binary keep filter, if *keep_mask* is set): drop rows
    where the mask is False — covers the AND-merge of analyte concept
    + transfusion subject (and any future per-query binary gates).
    Empty intersection ⇒ caller sees empty LOINC for this query, the
    desired null behavior.

    Stage 1: cosine top-N from the filtered LOINC pool, where N is
    :data:`_SCALE_RERANK_WINDOW` when *scale_match_rank* is set and
    *top_k* otherwise. The wider window gives the scale rerank room to
    surface a same-analyte sibling that sits in a different LOINC
    family (Qn variants share ``glucose in urine`` while Ord variants
    sit in ``glucose in urine by test strip`` — the "by Test strip"
    suffix forces different family keys).

    Stage 2 (scale rerank, if *scale_match_rank* set): partition the
    top-N into scale-matching (``rank >= 0``) and non-matching, sort
    matching by ``(rank, -cosine)`` so the strongest-preferred tier
    leads — ``Qn`` beats ``SemiQn`` beats ``OrdQn`` within one analyte
    family even when cosine puts them within tie distance — then
    concatenate matching-first. Hybrid-strict — when no row in the
    window matches (analyte has no compatible scale variant, e.g.
    sodium has no Ord), the cosine order survives unchanged. No score
    magnitude, so the rerank decision depends only on which rows the
    cosine surfaces, not on the cosine distribution of any specific
    embedding model.

    Stage 3 (family rerank, if *family_index*): swap position 0 to the
    cosine-top-1's family member that best satisfies the per-query
    constraints. Two constraints get composed in the same family pool,
    stricter-first with graceful fallback:

      * **analyte digit** — when the query carries a standalone digit
        (``HPV-43``, ``CD4``), require pool members to carry that digit;
        when it doesn't, require pool members to carry NO digit
        (``A 抗原`` → ``A Ag *`` over ``A1 Ag *``).
      * **SCALE_TYP** (if *scale_match_rank* given) — require pool
        members to have a SCALE_TYP compatible with the per-query
        scale class (``rank >= 0``). Composes with the cross-family
        scale rerank in stage 2: stage 2 picks the right family for
        the scale, stage 3 then keeps the digit-correct member within
        it.

    Composition: try digit-AND-scale; if empty, drop scale and keep
    digit; if still empty, return cosine picks unchanged.
    """
    if keep_mask is not None:
        keep = keep_mask[loinc_idxs]
        effective_idxs = loinc_idxs[keep]
        if len(effective_idxs) == 0:
            return []
    else:
        effective_idxs = loinc_idxs

    # Pick a window large enough for the scale rerank when active —
    # otherwise just top_k. argpartition is O(N) regardless of k so
    # the cost is identical.
    sub = sims_row[effective_idxs]
    window = (
        _SCALE_RERANK_WINDOW
        if (scale_match_rank is not None and family_index is not None)
        else top_k
    )
    k = min(window, len(sub))
    if k < len(sub):
        part = np.argpartition(-sub, k - 1)[:k]
    else:
        part = np.arange(len(sub))
    order = part[np.argsort(-sub[part])]
    topN_picks: list[int] = [int(effective_idxs[o]) for o in order]

    # Stage 2 — scale-prefer rerank across the cosine top-N, ANCHORED to
    # cos_top's analyte family. Anchor = bidirectional family_key prefix
    # match: ``"glucose in urine"`` and ``"glucose in urine by test strip"``
    # anchor each other (one is the prefix of the other), so Qn / Ord
    # variants of one analyte qualify even when LOINC's ``by X`` suffix
    # splits them into different family_keys; but sodium-related rows
    # never anchor to glucose, so an unrelated Ord row can't float over
    # a high-cosine Qn match for an analyte that has no Ord variant.
    # Hybrid-strict fallback: no anchored match in the window ⇒ cosine
    # order survives (correct for analytes that simply have no scale
    # variant — e.g. sodium has no Ord; ``钠+`` keeps the Qn sodium top).
    # Matching rows then sort by (rank, -cosine) so a ``Qn`` row beats a
    # same-family ``SemiQn`` even when cosine prefers the SemiQn — keeps
    # ``[Units/volume]`` ahead of an incidental ``[Titer]`` sibling.
    cosine_picks = topN_picks[:top_k]
    if scale_match_rank is not None and family_index is not None and topN_picks:
        _, row_key, _, _ = family_index
        cos_top = topN_picks[0]
        cos_family = row_key[cos_top]

        def _shares_analyte(r: int) -> bool:
            fk = row_key[r]
            return fk.startswith(cos_family) or cos_family.startswith(fk)

        matching = [
            r for r in topN_picks
            if scale_match_rank[r] >= 0 and _shares_analyte(r)
        ]
        if matching:
            matching.sort(key=lambda r: (int(scale_match_rank[r]), -sims_row[r]))
            matching_set = set(matching)
            non_matching = [r for r in topN_picks if r not in matching_set]
            cosine_picks = (matching + non_matching)[:top_k]

    if family_index is None:
        return cosine_picks

    row_digits, row_key, family_to_rows, row_name_has_digit = family_index
    q_digit = query_analyte_digit(query_text)
    cos_top = cosine_picks[0]
    family = row_key[cos_top]
    pool = family_to_rows.get(family, [cos_top])

    def passes_keep(r: int) -> bool:
        return keep_mask is None or bool(keep_mask[r])

    def passes_digit(r: int) -> bool:
        if q_digit is not None:
            return q_digit in row_digits[r]
        return not row_digits[r]

    def passes_scale(r: int) -> bool:
        return scale_match_rank is None or scale_match_rank[r] >= 0

    # Strict: digit ∧ scale ∧ keep. Relax scale first (analyte may not
    # have a matching scale variant in this family), then attempt a
    # cross-family digit rescue, then null out / fall back depending on
    # whether the query specified a subtype digit at all.
    filtered = [
        r for r in pool
        if passes_keep(r) and passes_digit(r) and passes_scale(r)
    ]
    if not filtered and scale_match_rank is not None:
        filtered = [r for r in pool if passes_keep(r) and passes_digit(r)]
    if not filtered:
        if q_digit is not None and any(
            row_name_has_digit[r] for r in pool
        ):
            # The family enumerates subtypes by number AND none of those
            # numbers matches the query: a wrong-digit same-family
            # sibling is the most we could emit (``HPV-58`` for
            # ``HPV-8``, ``HPV-73`` for ``HPV-23``, the
            # ``HPV 6+11+42+43+44`` panel for ``HPV-46``). The honest
            # answer is null — LOINC has no concept for this subtype.
            #
            # We use ``row_name_has_digit`` (any digit anywhere in the
            # name) rather than ``row_digits`` (analyte-position digits
            # only) so that ``+``-chain panel rows count as "this family
            # enumerates by number". Without this, the HPV combo panel
            # 21441-1 (digits all in ``+``-chain → empty
            # ``loinc_analyte_digits``) would slip through the guard
            # and become the answer for ``HPV-46``.
            #
            # Restricted to families that already carry digit-bearing
            # members so we don't punish families whose canonical names
            # use a chemical / Roman-numeral form while the query uses
            # a numeric alias:
            #   - Vitamin B1 / B2 / ... ↔ Thiamine / Riboflavin / ...
            #   - Vitamin K1 / K2 ↔ Phytonadione
            #   - IGF-1 ↔ Insulin-like growth factor-I (Roman)
            #   - 2小时血糖 ↔ Glucose --2 hours post dose glucose
            #     (the "2" sits in the dash-tail and isn't extracted as
            #     an analyte digit by ``loinc_analyte_digits``)
            # In all of these the family pool has zero digit-bearing
            # members, so cosine top-1 is the correct call.
            #
            # We deliberately do NOT rescue across families: a global
            # scan for any LOINC row carrying q_digit catches unrelated
            # organisms with matching numbers (``Herpes virus 8`` for
            # ``HPV-8``, ``blaOXA-23`` for ``HPV-23``, ``HTLV II g46``
            # for ``HPV-46``, ``Borrelia 83/93kD`` for ``HPV-83``). The
            # embedding already lands on the correct broad-analyte
            # family when the right code exists (``HPV-43 Ag`` wins
            # cosine for ``HPV-43`` even though most HPV codes are
            # DNA), so null is the honest answer. SNOMED is picked
            # separately and unaffected by this gate.
            return []
        return cosine_picks

    # Within the filtered pool, pick by (rank, -cosine) when ranks are
    # available — keeps a same-family ``Qn`` ahead of a ``SemiQn`` titer
    # whose cosine happens to edge it out — else pure cosine.
    if scale_match_rank is not None:
        rerank_top = min(
            filtered,
            key=lambda r: (
                int(scale_match_rank[r]) if scale_match_rank[r] >= 0 else 99,
                -sims_row[r],
            ),
        )
    else:
        rerank_top = max(filtered, key=lambda r: sims_row[r])
    if rerank_top == cos_top:
        return cosine_picks
    # Swap position 0; dedupe in case rerank_top was already in tail.
    out = [rerank_top]
    for r in cosine_picks:
        if r != rerank_top and len(out) < top_k:
            out.append(r)
    return out


def _non_loinc_picks_topk(
    sims_row: np.ndarray,
    idxs: np.ndarray,
    top_k: int,
) -> list[int]:
    sub = sims_row[idxs]
    k = min(top_k, len(sub))
    if k < len(sub):
        part = np.argpartition(-sub, k - 1)[:k]
    else:
        part = np.arange(len(sub))
    order = part[np.argsort(-sub[part])]
    return [int(idxs[o]) for o in order]


# SNOMED Body-Structure anatomy-bias picker.
#
# Detection window: when the cosine top-N rows in the full SNOMED pool
# already contain at least :data:`_SNOMED_BS_TRIGGER_MIN` body-structure
# concepts, the query is reading as anatomy-relevant — restrict the
# eventual pick to the body-structure subset so the SNOMED column carries
# a clean anatomy concept (``Structure of greater trochanter of right
# femur (body structure)``) rather than a competing procedure /
# observable entity / finding sibling of the same analyte. When the
# top-N has fewer than the threshold, the query is reading as a lab
# measurement / procedure (``Hemoglobin``, ``Aspergillus Ab``) — keep
# the current full-SNOMED behavior so those rows still resolve.
#
# Both knobs are integer counts within a fixed-size top-N window, not
# cosine magnitudes — provider-swap safe (Gemini cosine spread vs Qwen
# spread doesn't change which rows sit in the top-N).
_SNOMED_BS_DETECT_WINDOW: int = 10
_SNOMED_BS_TRIGGER_MIN: int = 2


def _snomed_picks_topk(
    sims_row: np.ndarray,
    idxs: np.ndarray,
    top_k: int,
    *,
    body_structure_mask: np.ndarray | None = None,
) -> list[int]:
    """SNOMED top-k picker with auto-detected anatomy bias.

    When *body_structure_mask* is given and the cosine top-N over the
    full SNOMED pool contains ≥ :data:`_SNOMED_BS_TRIGGER_MIN`
    body-structure rows, the picker restricts the eventual selection to
    rows where the mask is True. Otherwise it falls back to the plain
    full-SNOMED cosine top-k (same behavior as :func:`_non_loinc_picks_topk`).

    Detection-window count, not score: avoids any cosine-magnitude
    threshold so the rule survives an embedding-provider swap.
    Empty-fallback at every step keeps lab / procedure queries
    untouched and never returns ``[]`` when the full pool has rows.
    """
    if body_structure_mask is None or len(idxs) == 0:
        return _non_loinc_picks_topk(sims_row, idxs, top_k)
    sub = sims_row[idxs]
    detect_k = min(_SNOMED_BS_DETECT_WINDOW, len(sub))
    if detect_k < len(sub):
        part = np.argpartition(-sub, detect_k - 1)[:detect_k]
    else:
        part = np.arange(len(sub))
    detect_picks = idxs[part]
    bs_count = int(body_structure_mask[detect_picks].sum())
    if bs_count < _SNOMED_BS_TRIGGER_MIN:
        return _non_loinc_picks_topk(sims_row, idxs, top_k)
    bs_idxs = idxs[body_structure_mask[idxs]]
    if len(bs_idxs) == 0:
        return _non_loinc_picks_topk(sims_row, idxs, top_k)
    return _non_loinc_picks_topk(sims_row, bs_idxs, top_k)


# ── Public entry point ───────────────────────────────────────────────


async def resolve_many(
    terms: list[str],
    top_k: int = 1,
    systems: list[str] | None = None,
    *,
    values: list[str | None] | None = None,
    default_scale: str | None = _DEFAULT_SCALE,
    bundle_dir: str | None = None,
    family_rerank: bool = True,
    multi_span: bool = True,
    chunk: int = 100,
) -> list[list[ResolveResult]]:
    """Resolve free-text *terms* via the v2 algorithm.

    Returns one ``list[ResolveResult]`` per input term, containing up to
    *top_k* per system (in the order *systems* is given, or the full
    SYSTEMS order when None). Empty list for any blank term, and silently
    returns all-empty lists when the local FHIR cache is missing.

    *values*: optional one-per-term observed value string. When given,
    each value is classified to a SCALE_TYP scale class (``qn`` / ``ord``
    / ``nom`` / ``nar``); the picker then prefers same-family LOINC rows
    whose SCALE_TYP is compatible with that class (see
    :func:`_loinc_picks_topk` for the family rerank). Disambiguates
    LOINC analytes that carry both quantitative and qualitative variants
    (Glucose [Mass/volume] vs [Presence], hCG [Units/volume] vs [Presence],
    blood-group antigens, urine dipstick panels). Hard rerank — no
    score magnitude — so behavior is deterministic across embedding
    providers.

    *default_scale*: scale class to apply when no value is observed AND
    the query text carries no qualitative trigger. Defaults to
    :data:`_DEFAULT_SCALE` (``"qn"``) — clinical lab indicators are
    overwhelmingly quantitative, and pinning the default removes the
    provider-dependent ambiguity that pure cosine introduces for
    dual-scale analytes. Pass ``None`` to opt out (current behavior:
    pure cosine, no scale bias).

    Per-term precedence (highest wins):
      1. ``values[i]`` is given and classifies to a known scale class →
         that class.
      2. The query text matches any of :data:`_QUALITATIVE_TRIGGER_MARKERS`
         (multilingual: dipstick / 试纸 / 試紙 / 試験紙 / 시험지 /
         tira reactiva / bandelette / Teststreifen / тест-полоска / …) →
         ``"ord"``.
      3. ``default_scale`` (default ``"qn"``).
      4. None of the above → no scale rerank applied to that term.

    *multi_span*: when True (default), each query is embedded under
    multiple framings — the full query, the indicator-only chunk
    (after ``|``-split), and the first/last hierarchical segments of
    that chunk (after ``,，·・``-split). Per-LOINC-row cosine is the
    ``max`` across framings, so parent·child queries where the child
    carries the actual specificity recover the child's match instead
    of being dragged toward the parent's high-cosine concept. See
    :mod:`.multi_span` for span generation. Pass ``False`` for the
    legacy single-string-embed baseline (useful for A/B benchmarking
    the routing fix).
    """
    from mirobody.utils.embedding import text_embedding

    out: list[list[ResolveResult]] = [[] for _ in terms]
    if not terms:
        return out

    if values is not None and len(values) != len(terms):
        raise ValueError(
            f"values length ({len(values)}) must match terms length ({len(terms)})"
        )
    if default_scale is not None and default_scale not in _SCALE_COMPAT:
        raise ValueError(
            f"default_scale={default_scale!r} not one of "
            f"{sorted(_SCALE_COMPAT)} or None"
        )

    cache = _load_local_cache(bundle_dir=bundle_dir)
    if cache is None:
        log.warning("resolve.pipeline: no local FHIR cache; returning empty results")
        return out

    names = cache.get("names") or []
    if not names:
        log.warning("resolve.pipeline: meta sidecar missing; returning empty results")
        return out

    embs = cache["embs"]
    canonical = cache["canonical"]
    fhir_ids = cache["arr"]["fhir_id"]
    code_strs = cache.get("code_strs") or {}
    n_rows = int(embs.shape[0])

    # Axis bundle is loaded for two reasons:
    # 1. SCALE_TYP rerank (when ``values`` or ``default_scale`` is set).
    # 2. Deterministic CLASS hard-filter (always, when LOINC is a target
    #    system) — see ``apply_deterministic_class_filter``.
    # Both consumers tolerate ``axis_data is None`` (fall back to plain
    # cosine), so a single load attempt covers both.
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None or "SCALE_TYP" not in axis_data.get("values", {}):
        log.warning(
            "resolve.pipeline: SCALE_TYP axis data unavailable; "
            "scale rerank disabled for this call"
        )
        axis_data = None

    # Pre-strip deprecated rows from every system's candidate pool.
    # ``sys_indices`` is the deprecate-filtered base pool every picker
    # works on; the family index built below inherits the filtering.
    deprecated_drop = _build_deprecated_drop_mask(cache, n_rows)
    target_systems = list(systems) if systems else list(SYSTEMS)
    sys_arr = (canonical >> _CODE_BITS).astype(np.int8)
    sys_indices: dict[str, np.ndarray] = {}
    for s in target_systems:
        sys_int = SYSTEM_TO_CODE.get(s.upper())
        if sys_int is None:
            continue
        idxs = np.where(sys_arr == sys_int)[0]
        idxs = idxs[~deprecated_drop[idxs]]
        if len(idxs) > 0:
            sys_indices[s] = idxs

    embs_f32 = embs.astype(np.float32, copy=False)

    family_index = None
    if family_rerank and "LOINC" in sys_indices:
        family_index = _build_family_index(cache, sys_indices["LOINC"])

    # Per-query LOINC keep-mask pipeline (see ``_LOINC_FILTERS``).
    # Filters lazy-build their static corpus state on demand, cached
    # on the cache dict — no eager precomputation here.

    # Process in chunks so we don't allocate (B × N) score matrix for
    # arbitrarily large batches.
    for start in range(0, len(terms), chunk):
        batch = terms[start : start + chunk]
        # Keep one entry per non-blank term; ``slot_of[pos]`` maps from
        # query position (0..len(queries)-1) back to the slot in ``out``.
        slot_of: list[int] = [
            start + i for i, t in enumerate(batch) if t and t.strip()
        ]
        if not slot_of:
            continue
        queries = [terms[s] for s in slot_of]

        # Query-side preprocessing — augment the embedding input only.
        # Original ``queries`` strings are kept verbatim for downstream
        # keyword-based gates (``_compose_loinc_keep`` matches literal
        # tokens like ``挑战``/``challenge``/``空腹``/``fasting`` — we
        # don't want the appended Latin species names polluting those).
        from ..embeddings.preprocess import (
            augment_time_markers,
            augment_zh_aliases,
            normalize_roman_numerals,
        )

        def _augment(s: str) -> str:
            return augment_zh_aliases(
                augment_time_markers(normalize_roman_numerals(s))
            )

        # Multi-span embedding (see :mod:`.multi_span` for the framings).
        # Each query becomes 1-4 spans; we embed them all in one batched
        # API call and reduce per-LOINC-row cosine to ``max`` across
        # spans, so child-specific framings can outrank a parent-
        # dominated full-query cosine without losing the full-query
        # signal where it actually wins. When ``multi_span=False``, we
        # fall back to the legacy one-string-per-query embed for A/B.
        if multi_span:
            from .multi_span import generate_anchor_spans
            spans_per_q: list[list[str]] = [
                generate_anchor_spans(q) for q in queries
            ]
        else:
            spans_per_q = [[q] for q in queries]

        flat_inputs: list[str] = []
        flat_to_qi: list[int] = []
        for qi, qspans in enumerate(spans_per_q):
            for s in qspans:
                flat_inputs.append(_augment(s))
                flat_to_qi.append(qi)

        q_embs = await text_embedding(flat_inputs, provider="gemini", cache=True)
        Q = np.asarray(q_embs, dtype=np.float32)
        norms = np.linalg.norm(Q, axis=1, keepdims=True)
        Q = np.divide(Q, norms, out=np.zeros_like(Q), where=norms > 0)

        span_sims = Q @ embs_f32.T             # (S, N) per-span cosine
        # Per-query max-pool across spans. ``sims[qi, r]`` ends up as
        # the strongest cosine any framing of query ``qi`` has to LOINC
        # row ``r`` — order-independent, so flat_to_qi can be a plain
        # bucket sort. Each query is guaranteed at least one span (the
        # full query) by ``generate_anchor_spans``, so no row stays at
        # the -inf sentinel.
        sims = np.full(
            (len(queries), embs_f32.shape[0]), -np.inf, dtype=np.float32,
        )
        for spi, qi in enumerate(flat_to_qi):
            np.maximum(sims[qi], span_sims[spi], out=sims[qi])

        # Deterministic CLASS routing — hard-filter LOINC candidates to
        # the predicted CLASS for queries whose context licenses a
        # gated CLASS prediction (e.g. ``过敏性肺炎筛查`` → ALLERGY).
        # See :func:`apply_deterministic_class_filter` for the gate
        # logic and the fallback when no in-CLASS candidate exists.
        if axis_data is not None and "LOINC" in sys_indices:
            from .axis import apply_deterministic_class_filter
            apply_deterministic_class_filter(
                sims, axis_data, queries, sys_arr, SYSTEM_TO_CODE["LOINC"],
            )

        # Section-header keyword override: queries whose last indicator
        # segment is a known multilingual section phrase (Discussion /
        # 讨论 / 诊断意见 / 考察 / Impression / …) route to the section-
        # header corpus pool (~1.8k record-artifact / narrative rows)
        # regardless of source-prefix bias. Without this, ``骨密度,讨论``
        # picks a DXA bone-density code because ``骨密度`` dominates
        # cosine. Hard-mask via -inf, mirroring the CLASS filter shape.
        #
        # ``sh_pos`` carries the per-position trigger so the result
        # loop below can enforce ``STRATEGY_SECTION_HEADER`` min_score
        # floor + ``fallback="null"`` semantics (v1's FhirAdapter
        # applies these in adapter.py; v2 used to skip them, which let
        # 0.58-0.59 "Fetal Skeletal Narrative" rows leak out for
        # ``骨密度·讨论`` / ``骨密度·随访`` when no genuine bone-density
        # narrative concept exists).
        from .category import (
            STRATEGY_SECTION_HEADER,
            _is_section_header_term,
            section_header_pool_mask,
        )
        sh_mask_arr: "np.ndarray | None" = None
        sh_pos: list[bool] = [False] * len(queries)
        for _pos, _term in enumerate(queries):
            if not _is_section_header_term(_term):
                continue
            if sh_mask_arr is None:
                sh_mask_arr = section_header_pool_mask(cache)
            sims[_pos, ~sh_mask_arr] = -np.inf
            sh_pos[_pos] = True

        # Resolve a SCALE_TYP class per term (precedence: explicit value
        # → multilingual qualitative trigger → ``default_scale``). The
        # class becomes a hard family-rerank constraint inside
        # ``_loinc_picks_topk`` — no score magnitude, just a per-pool
        # tier — keeping the picker provider-agnostic. Rank arrays per
        # class are computed at most once per call (4 classes).
        scale_match_for_pos: list[np.ndarray | None] = [None] * len(slot_of)
        if axis_data is not None:
            qual_trigger_re = _qualitative_trigger_re()
            rank_cache: dict[str, np.ndarray] = {}
            for pos, slot in enumerate(slot_of):
                v = (
                    values[slot]
                    if (values is not None and slot < len(values))
                    else None
                )
                cls = _classify_value(v)
                if cls is None:
                    # No explicit value — fall back to query-text trigger,
                    # then ``default_scale``. Trigger fires per multilingual
                    # markers (dipstick / 试纸 / 試驗紙 / tira reactiva /
                    # …); see :data:`_QUALITATIVE_TRIGGER_MARKERS`.
                    term_text = terms[slot]
                    if term_text and qual_trigger_re.search(term_text):
                        cls = "ord"
                    elif default_scale is not None:
                        cls = default_scale
                if cls is None:
                    continue
                rank = rank_cache.get(cls)
                if rank is None:
                    rank = _build_scale_match_rank(cls, axis_data, n_rows)
                    if rank is None:
                        continue
                    rank_cache[cls] = rank
                scale_match_for_pos[pos] = rank

        for pos, (slot, term) in enumerate(zip(slot_of, queries)):
            loinc_keep = _compose_loinc_keep(
                term, cache, sims[pos], sys_indices.get("LOINC"),
            )

            results: list[ResolveResult] = []
            for s in target_systems:
                idxs = sys_indices.get(s)
                if idxs is None:
                    continue
                if s == "LOINC":
                    picks = _loinc_picks_topk(
                        sims[pos], term, idxs, family_index, top_k,
                        keep_mask=loinc_keep,
                        scale_match_rank=scale_match_for_pos[pos],
                    )
                elif s == "SNOMED_CT":
                    picks = _snomed_picks_topk(
                        sims[pos], idxs, top_k,
                        body_structure_mask=cache.get("snomed_body_structure_mask"),
                    )
                else:
                    picks = _non_loinc_picks_topk(sims[pos], idxs, top_k)
                for pick in picks:
                    score_f = float(sims[pos, pick])
                    # Section-header strategy floor + null fallback: when
                    # this position was routed to the section-header pool
                    # and the pick falls below the per-vocab min_score,
                    # drop it. Matches the v1 FhirAdapter gating in
                    # adapter.py so v2 emits the same null cells reviewers
                    # rely on (``no LOINC concept exists`` vs ``weak
                    # match``). Threshold is read from STRATEGY_SECTION_HEADER
                    # so the floor stays a single source of truth.
                    if sh_pos[pos]:
                        floor = STRATEGY_SECTION_HEADER.min_score.get(s)
                        if floor is not None and score_f < floor:
                            continue
                    try:
                        sys_name, code = fhir_id_to_code(int(fhir_ids[pick]))
                    except NotImplementedError:
                        # DCM / THETA hash rows — reverse via meta sidecar.
                        sys_name = s
                        code = code_strs.get(pick, "") if code_strs else ""
                        if not code:
                            code = f"row:{pick}"
                    results.append(ResolveResult(
                        system=sys_name,
                        code=code,
                        name=names[pick],
                        score=round(score_f, 4),
                    ))
            out[slot] = results

    return out


__all__ = [
    "query_analyte_digit",
    "loinc_analyte_digits",
    "loinc_family_key",
    "resolve_many",
]
