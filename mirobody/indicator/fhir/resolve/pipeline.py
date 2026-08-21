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
import unicodedata
from typing import Callable

import numpy as np

from ...search import AxisCode, ResolveResult


# ── FHIR Coding.display ASCII normalization ─────────────────────────
#
# FHIR R4 ``code.coding[].display`` is intended for human-readable
# labels and the spec permits any UTF-8 — but downstream systems
# (CDA-bridging EHRs, terminology servers with ASCII URL params,
# HL7 v2 gateways) routinely mangle non-ASCII. We pin every name
# field we emit to ASCII English so the display string survives
# any of these legacy hops untouched.
#
# Only ~0.06% of SNOMED FSN rows carry non-ASCII (~246/386 k); the
# bulk is accented Latin in eponymous structures (Lieberkühn,
# Ménière, Guillain-Barré, Brown-Séquard), with a handful of Greek
# letters (α fetoprotein) and one trademark / subscript / non-break
# hyphen. NFKD canonical decomposition strips combining marks and
# expands compatibility codepoints (₂ → 2), so accented Latin →
# ASCII is lossless-readable. Greek letters do NOT decompose under
# NFKD, so we hand-map the lowercase + uppercase Greek alphabet to
# its Latin transliteration (α → alpha) to avoid silent information
# loss for clinical eponyms.
_GREEK_TO_LATIN: dict[str, str] = {
    "α": "alpha", "β": "beta", "γ": "gamma", "δ": "delta",
    "ε": "epsilon", "ζ": "zeta", "η": "eta", "θ": "theta",
    "ι": "iota", "κ": "kappa", "λ": "lambda", "μ": "mu",
    "ν": "nu", "ξ": "xi", "ο": "omicron", "π": "pi",
    "ρ": "rho", "σ": "sigma", "ς": "sigma", "τ": "tau",
    "υ": "upsilon", "φ": "phi", "χ": "chi", "ψ": "psi", "ω": "omega",
    "Α": "Alpha", "Β": "Beta", "Γ": "Gamma", "Δ": "Delta",
    "Ε": "Epsilon", "Ζ": "Zeta", "Η": "Eta", "Θ": "Theta",
    "Ι": "Iota", "Κ": "Kappa", "Λ": "Lambda", "Μ": "Mu",
    "Ν": "Nu", "Ξ": "Xi", "Ο": "Omicron", "Π": "Pi",
    "Ρ": "Rho", "Σ": "Sigma", "Τ": "Tau", "Υ": "Upsilon",
    "Φ": "Phi", "Χ": "Chi", "Ψ": "Psi", "Ω": "Omega",
}


def _ascii_display(s: str) -> str:
    """Normalize a clinical concept name to FHIR-safe ASCII English.

    Pipeline:

    1. Hand-map Greek letters to Latin transliterations
       (``α`` → ``alpha``, ``β`` → ``beta``). NFKD doesn't decompose
       these and a blind ASCII encode would drop them silently,
       losing semantic content in eponyms / analyte names.
    2. NFKD canonical decomposition splits accented Latin into base
       letter + combining mark (``é`` → ``e`` + acute) and expands
       compatibility codepoints (``₂`` → ``2``).
    3. ASCII encode with ``errors='ignore'`` drops every remaining
       non-ASCII codepoint (combining marks from step 2, plus any
       Cyrillic / CJK / trademark / non-break-hyphen that slipped
       past step 1).
    4. Collapse whitespace runs introduced by stripped characters.

    No-op on plain ASCII input. Returns the input unchanged when
    given ``None`` or empty string.
    """
    if not s:
        return s
    for k, v in _GREEK_TO_LATIN.items():
        if k in s:
            s = s.replace(k, v)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", errors="ignore").decode("ascii")
    return " ".join(s.split())
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

# The value-kind vocabulary and the scale-compatibility table now live in
# `mirobody.indicator.value_scale`, so the lexical resolver and the small
# LOINC-only semantic tier gate on the same definitions this pipeline reranks
# with, rather than on a second copy that drifts. Names are re-bound to the
# module-private spellings the rest of this file uses.
from mirobody.indicator.value_scale import (  # noqa: E402
    SCALE_COMPAT as _SCALE_COMPAT,
    VALUE_NOM_TOKENS as _VALUE_NOM_TOKENS,
    classify_value as _classify_value,
)

# Specimen preference for the within-family final tiebreak. Tuple
# position = preference tier (lower index wins). Applied AFTER scale
# rerank (so a Qn ``Ser/Plas`` still wins over a Qn ``Bld`` of the same
# analyte family even when cosine puts them within tie distance) and
# AFTER ``_nonspecific_specimen_keep`` (which already drops XXX-with-
# peer rows). For most clinical-lab analytes, ``Ser/Plas`` is the
# canonical form; ``Bld`` (whole-blood) follows for analytes that
# require erythrocytes (HbA1c, Lead — those families have no S/P
# variant, so this tuple doesn't displace them); plasma-only or
# serum-only forms come next; capillary / arterial / venous / cord
# blood variants come last. SYSTEM values NOT in this tuple get
# tier ``-1`` and don't affect the sort key (no SYSTEM_PREFERENCE
# bias). Tuple is intentionally short — every entry is a high-volume
# clinical-lab specimen; rare specimens (Body fld, Tiss, ...) stay
# at -1 and ride cosine + scale alone.
_SYSTEM_PREFERENCE: tuple[str, ...] = (
    "Ser/Plas", "Bld", "Plas", "Ser",
    "Bld.cap", "BldA", "BldV", "Bld.dot", "BldC",
)

# Sources whose implicit specimen is NOT Ser/Plas — the cross-specimen
# rerank (:func:`_apply_system_pref`) would wrongly demote the
# domain-canonical specimen toward the global default. Blood-gas
# panels (``血气分析``) operate on arterial blood; analytes drawn from
# that sample (``血气分析,葡萄糖`` / ``血气分析,氯``) should stay in
# BldA, not be promoted to Ser/Plas.
_SYSTEM_PREF_SKIP_SOURCES: frozenset[str] = frozenset({
    "血气分析", "动脉血气", "动脉血气分析", "ABG", "arterial blood gas",
    "血气", "血气分析A", "血气分析V",
})

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


# Semi-quantitative trigger markers — when present, the SCALE_TYP class
# flips from default ``qn`` to ``semiqn`` (see :data:`_SCALE_COMPAT`).
# Targets indicators whose value form is a titer / grade / 1+/2+/3+
# (D-dimer 半定量, RPR titer, antibody grade), distinct from both
# quantitative (Mass/vol) and qualitative (Presence/Ord).
_SEMIQUANTITATIVE_TRIGGER_MARKERS: list[str] = [
    # CN / Han
    "半定量", "半定量分析", "半定量测定", "半定量檢測",
    # Latin
    "semi-quantitative", "semi quantitative", "semiquantitative",
    "semi-quantification", "titer", "Titer", "titre",
    # JA / KR
    "半定量検査", "반정량",
    # Romance / Germanic / Slavic
    "semicuantitativo", "semicuantitativa",
    "semiquantitatif", "semiquantitative",
    "halbquantitativ",
    "полуколичественный",
]


def _semiquantitative_trigger_re() -> "re.Pattern[str]":
    """Lazy-compile the semi-quantitative trigger pattern.

    Mirror of :func:`_qualitative_trigger_re` — same compile-once
    pattern, separate scale-class. Tested before the qualitative
    trigger so ``半定量`` queries don't get classified as ``ord``.
    """
    cached = getattr(_semiquantitative_trigger_re, "_cached", None)
    if cached is not None:
        return cached
    from .specificity import _compile_marker_pattern
    pat = _compile_marker_pattern(_SEMIQUANTITATIVE_TRIGGER_MARKERS)
    _semiquantitative_trigger_re._cached = pat   # type: ignore[attr-defined]
    return pat


def _build_system_match_rank(cache: dict) -> "np.ndarray | None":
    """Build (N,) int8 rank array over the LOINC corpus, marking each
    row's tier within :data:`_SYSTEM_PREFERENCE`.

    Values: ``-1`` for non-LOINC rows, LOINC rows missing axis data,
    and LOINC rows whose SYSTEM isn't in the preference tuple; ``0``
    for ``Ser/Plas``, ``1`` for ``Bld``, etc.

    Query-independent — built once per cache and reused across all
    queries. Cached under ``_system_match_rank``. Returns ``None`` if
    the axis bundle is unavailable (deployment without
    ``fhir_loinc_bundle.tar.gz`` — the picker falls back to scale +
    cosine alone).

    Consumed by :func:`_loinc_picks_topk` as the second sort key after
    scale tier, so a ``Qn Ser/Plas`` row beats a ``Qn Bld`` sibling
    within the same analyte family even when cosine puts them within
    tie distance. Analyte families that have no ``Ser/Plas`` variant
    in LOINC (HbA1c, Lead, Hb, Hct — those live exclusively in ``Bld``)
    are unaffected: the tier-0 slot is empty for them and tier-1 ``Bld``
    wins on its own merit.
    """
    cached = cache.get("_system_match_rank")
    if cached is not None:
        return cached
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_system_match_rank"] = None
        return None
    values_list = axis_data.get("values", {}).get("SYSTEM")
    row_value_idx = axis_data.get("row_value_idx", {}).get("SYSTEM")
    if values_list is None or row_value_idx is None:
        cache["_system_match_rank"] = None
        return None
    centroid_rank: dict[int, int] = {}
    for tier, v in enumerate(_SYSTEM_PREFERENCE):
        for i, vv in enumerate(values_list):
            if vv == v:
                centroid_rank[i] = tier
                break
    if not centroid_rank:
        cache["_system_match_rank"] = None
        return None
    n_rows = int(np.asarray(cache["canonical"]).shape[0])
    rank = np.full(n_rows, -1, dtype=np.int8)
    for centroid_idx, tier in centroid_rank.items():
        rank[row_value_idx == centroid_idx] = tier
    cache["_system_match_rank"] = rank
    log.info(
        "system preference rank: %d / %d LOINC rows tiered "
        "(%s)",
        int((rank >= 0).sum()), n_rows,
        ", ".join(f"{v}={i}" for i, v in enumerate(_SYSTEM_PREFERENCE)),
    )
    return rank


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
#   - Stereochemistry locants (``顺式 / 反式 / cis / trans``): in
#     fatty-acid IUPAC names the digit is a Δ-position descriptor
#     (``十六碳烯酸（9-顺式）`` = hexadec-9-enoic = Δ9 = ω-7 for C16),
#     not an analyte-subtype number. LOINC's display names use ω
#     numbering (``C16:1w7``) so a Δ-position digit will never match
#     a row's ω-digit; treating it as analyte digit empties the
#     family pool. Same shape as the time-token bypass.
_AFTER_DIGIT_IS_TIME = re.compile(
    r"\s*(?:"
    r"小时|分钟|时辰|月份|[秒天周月年岁]"
    r"|[- ]?(?:顺式|反式|順式|反式)"
    r"|[- ]?(?:cis|trans)\b"
    r"|(?:hours?|hrs?|h|min(?:ute)?s?|days?|weeks?|months?|years?|sec(?:ond)?s?)\b"
    r")",
    re.IGNORECASE,
)

# Stereochemistry prefix immediately before a digit (``反式-11-`` /
# ``cis-9-`` / ``trans-Δ9-``). Skips that digit as a Δ-position locant
# (IUPAC fatty-acid numbering) — not an analyte subtype. Tail-anchored
# so we only skip the digit directly after the stereo descriptor.
_BEFORE_DIGIT_IS_STEREO = re.compile(
    r"(?:^|[\s,，·•・/-])(?:反式|顺式|順式|cis|trans|Δ)\s*-?\s*$",
    re.IGNORECASE,
)

# Slash-alternation (``HIV-1/2``, ``Type 1/2``) and clock time
# (``24:00``, ``8:30``) — bail out of digit extraction entirely. The
# combined panel code (``HIV 1+2 Ab``) wins on cosine; forcing a
# digit constraint would push to a single-subtype variant instead.
_QUERY_NON_ANALYTE_DIGIT_CONTEXT = re.compile(
    # Combo panels — ``HIV 1/2``, ``HIV 1:2``, ``HPV 6+11``: the digit
    # is a panel-component identifier, not an analyte type to filter
    # by.
    r"\d+\s*[/:]\s*\d+"
    # T-helper subset markers — ``辅助性 T 细胞 1`` / ``T 淋巴细胞 17`` /
    # ``Th1`` / ``Th17`` / ``T helper 1``. The trailing digit names a
    # functional Th-subset, not an analyte / CD digit. LOINC has no
    # Th1/Th2/Th17-specific codes (the closest match is ``CD3+CD4+
    # (T4 helper) cells`` with digits 3, 4) so requiring the query
    # digit to appear in row_digits null-outs the family rerank.
    r"|T\s*细胞\s*\d+|T\s*淋巴细胞\s*\d+"
    r"|Th\s*\d+"
    r"|T[- ]?helper[- ]?\d+|T\s+helper\s+\d+"
)


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
        if _BEFORE_DIGIT_IS_STEREO.search(text[:m.start()]):
            # Fatty-acid IUPAC Δ-locant prefix (``反式-11-十八碳烯酸``,
            # ``cis-9-hexadecenoic``). LOINC names enumerate by ω-
            # position, not Δ; treating Δ as analyte subtype empties
            # the family pool. See ``_AFTER_DIGIT_IS_TIME`` parens
            # form for the other half of the stereo bypass.
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


# Specimen tail strip — used ONLY by ``_shares_analyte`` in stage 2 of
# the LOINC picker to widen the matching pool across same-analyte
# specimen variants for the SYSTEM_PREFERENCE rerank. ``loinc_family_key``
# itself keeps the specimen because most filter logic groups by full
# (analyte + specimen) family. Specimens listed here are the high-
# volume clinical-lab ones plus generic Specimen — others (Body fld,
# Tiss, ...) are left intact so the rerank doesn't accidentally
# unify rare-specimen variants with the canonical S/P form.
_SPECIMEN_TAIL_RE = re.compile(
    r"\s+in\s+("
    # Multi-word specimens FIRST — alternation is left-to-right
    # greedy, and ``blood`` would otherwise consume the leading
    # ``arterial`` / ``venous`` / ``capillary`` qualifier from
    # ``arterial blood`` etc., leaving a dangling ``arterial`` tail.
    # The full set tracks :data:`_SYSTEM_PREFERENCE` tiers so the
    # cross-specimen rerank unifies sodium/lactate/bicarbonate
    # variants — without all tiers stripped, ``cur_bare`` doesn't
    # match the candidates' bare keys and the swap silently no-ops.
    r"serum, plasma or blood|serum or plasma or blood"
    r"|serum, plasma, or blood"
    r"|mixed venous blood|central venous blood"
    r"|arterial blood|venous blood|capillary blood"
    r"|dried blood spot|cord blood"
    r"|serum or plasma|red blood cells"
    r"|specimen|blood|plasma|serum|urine"
    r")(\s+by\b.*)?$",
    re.IGNORECASE,
)


def _strip_specimen_tail(family_key: str) -> str:
    """Strip the trailing ``in <specimen> [by <method>]`` from a
    family key so specimen variants of one analyte can be matched as
    the same analyte. ``"calcium in blood"`` and ``"calcium in serum
    or plasma"`` both collapse to ``"calcium"``. Used by the picker's
    stage-2 ``_shares_analyte`` only — full ``loinc_family_key``
    grouping is preserved everywhere else.
    """
    return _SPECIMEN_TAIL_RE.sub("", family_key)


# Predicted/expected qualifier strip — used by the picker's base-
# sibling promotion when the ``predicted`` filter drops the unfiltered
# cosine top-1. Matches the LOINC display-name patterns:
#     ``FEV1 Predicted`` / ``FEV1 measured/predicted``
#     ``(Diffusion capacity/Alveolar volume)/predicted``
#     ``Predicted RMR`` / ``Expected ...``
# Order matters: the multi-token ``measured/predicted`` form must be
# tried before the bare ``predicted`` alternative, otherwise the bare
# alternative consumes the trailing ``predicted`` and leaves a dangling
# ``measured/`` in the stripped name.
_PREDICTED_TOKEN_RE = re.compile(
    r"\s*\bmeasured\s*/\s*predicted\b"
    r"|\s*/\s*predicted\b"
    r"|\s*\b(?:predicted|expected)\b",
    re.IGNORECASE,
)


def _family_prefix_match(s: str, prefix: str) -> bool:
    """``s`` starts with ``prefix`` AND the next character (if any) is
    a token-boundary (whitespace OR comma). Used by the picker's
    family rerank to anchor cross-family analyte matches:
    ``"glucose in urine"`` is the whitespace-prefix of
    ``"glucose in urine by test strip"`` so they anchor each other;
    ``"hepatitis b virus e ag in serum"`` followed by ``","`` (in
    ``"... in serum, plasma or blood by rapid immunoassay"``) is the
    same analyte across a multi-specimen variant and also anchors.
    But ``"fev"`` is NOT a token-boundary prefix of ``"fev.5"`` —
    next char is ``.``, which is an intra-token continuation (FEV.5
    is a distinct analyte from FEV1, not a specificity variant).
    """
    if not prefix or not s.startswith(prefix):
        return False
    if len(s) == len(prefix):
        return True
    return s[len(prefix)] in " ,"


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


# ALLERGY-CLASS antibody-class bias. The CLASS gate in
# :func:`apply_deterministic_class_filter` routes ``过敏,X`` queries to
# the LOINC ``CLASS=ALLERGY`` pool, but that pool intermixes IgE / IgG
# / IgM / IgA antibody variants and functional-assay methodology
# variants. Bare allergen queries (``过敏,姜``, ``过敏,点青霉``) imply
# IgE Ab by clinical convention — Gemini regularly lands on the IgG
# sibling when its cosine is marginally higher (``Ginger IgG Ab`` over
# ``Ginger IgE Ab``), and methodology variants (``Pigweed triggered
# histamine release`` for ``过敏,苋``) similarly outrank the canonical
# IgE Ab row. Without this filter, the ALLERGY CLASS gate does the
# heavy lifting (drops the wrong CLASS) but leaves the within-CLASS
# wrong-Ig / wrong-method picks intact.
_ALLERGY_NON_IGE_NAME_RE = re.compile(
    # IgG / IgM / IgA Ab — explicit non-IgE antibody class. Includes
    # IgG-subclass variants (``IgG4 Ab``, ``IgG1 Ab``).
    r"\bIg[GMA](?:[1-4])?\s+Ab\b"
    # Functional-assay methodology variants. Default allergy panels
    # are static Ab levels; trigger-release and CAST-ELISA assays are
    # specialized and only appropriate when the query asks for them.
    r"|\btriggered\s+(?:histamine|leukotriene)\s+release\b"
    r"|\bCAST[- ]?ELISA\b",
    re.IGNORECASE,
)
_ALLERGY_IG_LICENSE_RE = re.compile(
    # Latin / abbreviated antibody class
    r"\bIg[GMA](?:[1-4])?\b"
    # Spelled-out forms in multiple languages — covers the "总 IgG"
    # / ``全 IgM`` / "specific IgA" framing seen in clinical lab forms
    r"|\b(?:immunoglobulin|immuneglobulin)\s*[GMA]\b"
    r"|免疫球蛋白\s*[GMA]"
    r"|免疫蛋白\s*[GMA]"
    # Functional-assay license — user explicitly named the methodology
    r"|histamine\s+release|leukotriene\s+release|CAST[- ]?ELISA"
    r"|嗜碱(?:性|粒).*活化",
    re.IGNORECASE,
)


def _build_allergy_non_ige_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose name carries IgG/IgM/IgA Ab (or a trigger-
    release / CAST-ELISA methodology) — non-IgE allergy variants the
    ALLERGY-gated picker should avoid by default. Cached on *cache*."""
    cached = cache.get("_loinc_allergy_non_ige_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _ALLERGY_NON_IGE_NAME_RE.search(nm):
            mask[i] = True
    log.info(
        "allergy non-IgE mask: %d / %d rows flagged (IgG/IgM/IgA Ab + "
        "trigger-release methodology)",
        int(mask.sum()), n,
    )
    cache["_loinc_allergy_non_ige_mask"] = mask
    return mask


def _allergy_ige_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """For ALLERGY-gated queries (CLASS_KEYWORD_GATES ``过敏 / allergen
    / atopic / ...``) carrying no explicit antibody-class or
    methodology marker, drop LOINC rows whose name carries IgG / IgM
    / IgA Ab or a trigger-release functional assay.

    Clinical contract: bare allergen queries in 过敏 context
    (``过敏,姜``, ``过敏原检测,Penicillin``) are uniformly IgE Ab
    panels. The IgG / IgM / IgA variants are food-intolerance or
    delayed-hypersensitivity tests — distinct clinical use cases that
    callers MUST tag explicitly (``牛奶 IgG`` / ``面筋 IgG`` etc.).

    Two modes:
      1. **Query carries no Ig-class marker**: ALLERGY default is IgE
         → drop IgG/IgM/IgA Ab rows (and trigger-release methodology).
      2. **Query carries explicit Ig-class marker(s)** (e.g.
         ``过敏,X IgG``): drop rows with a non-matching Ig class.

    Silent when query is not ALLERGY-gated — let the rest of the
    pipeline decide via centroid CLASS / cosine.
    """
    from .axis import _class_gate_res, _earliest_gate_class
    gates = _class_gate_res()
    if not gates:
        return None
    if _earliest_gate_class(query_text, gates) != "ALLERGY":
        return None
    q_classes = _query_ig_classes(query_text)
    if q_classes:
        return _build_ig_class_match_mask(cache, frozenset(q_classes))
    if _ALLERGY_IG_LICENSE_RE.search(query_text):
        # Methodology marker only (no specific class) — respect.
        return None
    return ~_build_allergy_non_ige_mask(cache)


# Food-sensitivity (delayed hypersensitivity) IgG bias — the mirror of
# `_allergy_ige_keep`. Food-immune-reactivity / chemical-immune-
# reactivity panels in CN clinical catalogs are IgG-mediated tests
# (specialty labs marketing "food sensitivity" / "food intolerance"
# panels), distinct from IgE-mediated allergic reactions. Without this
# filter, the picker routinely lands on the IgE sibling (``Wheat IgE``,
# ``Halibut IgE``, ``Blueberry IgE``) when the query intent is the
# IgG-class food-sensitivity row.
_FOOD_IGG_CONTEXT_RE = re.compile(
    r"食物免疫反应筛查"          # CN: food immune reactivity screen
    r"|食物敏感性?"              # CN: food sensitivity
    r"|食物不耐受|食物耐受"      # CN: food intolerance
    r"|食物迟发(?:型)?超敏"      # CN: delayed food hypersensitivity
    r"|化学免疫反应筛查"          # CN: chemical immune reactivity screen
    r"|食物.{0,4}IgG"             # CN: 食物 IgG 检测
    r"|麸质.*交叉反应|麸质.*敏感性?"  # CN: gluten cross-reactivity panel
    # Wheat/gluten proteome reactivity & autoimmunity — Cyrex-style
    # specialty panel. Indicators look like ``小麦/麸质蛋白组反应性与
    # 自身免疫·ω-麦醇溶蛋白 17 聚体 IgA``. The class semantics are
    # IgG/IgA-mediated (delayed/autoimmune response to wheat proteins),
    # not IgE (acute allergic). Without this marker, the picker lands
    # on ``Wheat IgE Ab`` (6276-0) for ``小麦 IgA`` and ``Wheat IgG Ab``
    # (107658-7) for ``谷吗啡肽 IgA`` — both cross-class.
    r"|麸质\s*蛋白组反应性?"        # CN: gluten proteome reactivity
    r"|小麦\s*[/／]\s*麸质\s*蛋白组" # CN: 小麦/麸质蛋白组
    r"|wheat\s*[/／]?\s*gluten\s+proteome"
    r"|gluten\s+proteome\s+reactivity"
    # Multilingual context markers
    r"|food\s+sensitivity"
    r"|food\s+intolerance"
    r"|food\s+immune\s+reactivity"
    r"|delayed\s+food\s+hypersensitivity"
    r"|chemical\s+immune\s+reactivity",
    re.IGNORECASE,
)
_FOOD_NON_IGG_NAME_RE = re.compile(
    # IgE Ab — explicit non-IgG class incompatible with food-sensitivity
    r"\bIgE\s+Ab\b"
    # Functional-assay variants — same reason as in `_allergy_ige_keep`.
    r"|\btriggered\s+(?:histamine|leukotriene)\s+release\b"
    r"|\bCAST[- ]?ELISA\b",
    re.IGNORECASE,
)


_IGE_AB_RE = re.compile(
    r"\bIgE\s+Ab(?:\s*/\s*Ig[GMAE]\s+total)?\b", re.IGNORECASE,
)
_IGG_AB_RE = re.compile(
    r"\bIgG\s+Ab(?:\s*/\s*Ig[GMAE]\s+total)?\b", re.IGNORECASE,
)
# Matches ``Ig[GMAE]`` (with optional subclass digit) Ab row in LOINC
# name. Captures the class letter for sibling-aware logic. The
# optional ``/Ig[GMAE] total`` suffix (``Broccoli IgE Ab/IgE total in
# Serum``) is consumed together so the stripped name's family key
# aligns with the bare ``IgG Ab in Serum`` sibling.
_ANY_IG_AB_RE = re.compile(
    r"\bIg([GMAE])(?:[1-4])?\s+Ab(?:\s*/\s*Ig[GMAE]\s+total)?\b",
    re.IGNORECASE,
)
# Matches the same Ig class markers on the query side. Includes
# multilingual long forms (``免疫球蛋白G`` / ``immunoglobulin M``).
_IG_CLASS_QUERY_RE = re.compile(
    r"\bIg([GMAE])(?:[1-4])?\b"
    r"|\b(?:immunoglobulin|immuneglobulin)\s+([GMAE])\b"
    r"|免疫球蛋白\s*([GMAE])"
    r"|免疫蛋白\s*([GMAE])",
    re.IGNORECASE,
)


def _query_ig_classes(query_text: str) -> set[str]:
    """Return the set of Ig classes ({'G', 'M', 'A', 'E'}) explicitly
    named in *query_text*. Empty set when the query carries no Ig-class
    marker. Used by :func:`_food_igg_keep` / :func:`_allergy_ige_keep`
    to drop non-matching Ig-class rows when the user IS specific
    (``汞化合物 IgM`` ⇒ keep IgM Ab, drop IgE/IgG/IgA siblings)."""
    out: set[str] = set()
    for m in _IG_CLASS_QUERY_RE.finditer(query_text):
        for g in m.groups():
            if g:
                out.add(g.upper())
                break
    return out


def _build_ig_class_match_mask(
    cache: dict, q_classes: frozenset[str], *, strict: bool = False,
) -> np.ndarray:
    """Mask that keeps LOINC rows whose Ig class is in *q_classes* OR
    which carry no Ig-class marker. Drops rows with an EXPLICIT non-
    matching Ig class.

    Two modes:

    - ``strict=False`` (food-sensitivity default): drop the non-matching
      row only when the SAME ANALYTE has a sibling row in one of
      *q_classes*. Niche foods/gums have only IgE rows in LOINC; the
      sibling check avoids dropping the only food-specific code when no
      class-matching alternative exists, falling back to ``Almond IgE``
      for ``杏仁 IgG`` rather than null. Used for queries whose context
      tolerates Ig-class drift (food sensitivity / gluten cross-react).

    - ``strict=True`` (chemical / heavy-metal / inhalant default):
      drop EVERY non-matching-class row, regardless of family-sibling
      existence. Chemical immune-reactivity panels are *defined* by Ig
      class — ``Mercury IgM`` and ``Mercury IgE`` are different immune
      responses (delayed hypersensitivity vs allergic-type), not
      interchangeable, so the "no IgM sibling → keep IgE" fallback
      produces clinically wrong codes. When the strict mask leaves no
      candidate, the picker emits null (correct: ``LOINC doesn't
      enumerate this Ig-class variant``).

    Cached per ``(q_classes, strict)`` set on *cache*.
    """
    cache_key = (
        f"_loinc_ig_class_keep_"
        f"{''.join(sorted(q_classes))}{'_strict' if strict else ''}"
    )
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])

    q_fams: set[str] | None
    if strict:
        q_fams = None
    else:
        # Pass 1 (non-strict only): collect family_keys for rows in EACH
        # Ig class so we can ask "does this analyte have a matching-
        # class sibling somewhere in LOINC?".
        fam_by_class: dict[str, set[str]] = {c: set() for c in "GMAE"}
        for nm in names:
            if not nm:
                continue
            m = _ANY_IG_AB_RE.search(nm)
            if not m:
                continue
            cls = m.group(1).upper()
            stripped = (nm[:m.start()] + " " + nm[m.end():]).strip()
            fk = _antibody_family_key(stripped)
            if fk:
                fam_by_class[cls].add(fk)
        q_fams = set().union(*(fam_by_class[c] for c in q_classes))

    # Pass 2: drop non-matching-class rows. Strict mode drops them
    # unconditionally; non-strict mode keeps them when no same-family
    # matching-class sibling exists in LOINC.
    mask = np.ones(n, dtype=bool)
    n_dropped = 0
    n_kept_no_sibling = 0
    for i, nm in enumerate(names):
        if not nm:
            continue
        m = _ANY_IG_AB_RE.search(nm)
        if not m:
            continue
        cls = m.group(1).upper()
        if cls in q_classes:
            continue
        if strict:
            mask[i] = False
            n_dropped += 1
            continue
        assert q_fams is not None
        stripped = (nm[:m.start()] + " " + nm[m.end():]).strip()
        fk = _antibody_family_key(stripped)
        if fk and fk in q_fams:
            mask[i] = False
            n_dropped += 1
        else:
            n_kept_no_sibling += 1
    log.info(
        "ig class match mask (q_classes=%s, strict=%s): %d rows kept, "
        "%d dropped, %d non-match kept (no same-family sibling)",
        sorted(q_classes), strict, int(mask.sum()),
        n_dropped, n_kept_no_sibling,
    )
    cache[cache_key] = mask
    return mask
_TRIGGER_RELEASE_RE = re.compile(
    r"\btriggered\s+(?:histamine|leukotriene)\s+release\b"
    r"|\bCAST[- ]?ELISA\b",
    re.IGNORECASE,
)
# Antibody-measurement method suffixes that should NOT gate same-
# analyte sibling lookup. The food-sensitivity filter wants any IgG
# variant of the analyte to count as a sibling, regardless of which
# method appears on the IgE row OR the IgG row. Empirically scanned
# all ``Ig[GE] Ab`` rows in LOINC 2.82: enumerated below are every
# method suffix appearing on ≥3 rows, plus a few niche substrate-
# based IIF methods. Applied symmetrically to IgE and IgG sides so
# the mask's family-key comparison ignores method dimension entirely.
#
# Trailing ``--Nth specimen`` qualifiers are stripped earlier by
# ``loinc_family_key``'s ``_DOUBLE_DASH_TAIL`` rule, so the patterns
# here don't need to anchor on them.
_AB_METHOD_SUFFIX_RE = re.compile(
    r"\s+by\s+Radioallergosorbent\s+test\s*\(RAST\)"
    r"|\s+by\s+Radioimmunoassay\s*\(RIA\)"
    r"|\s+by\s+Immune\s+diffusion\s*\(ID\)"
    r"|\s+by\s+Flow\s+cytometry\s*\(FC\)"
    r"|\s+by\s+Cell\s+binding\s+immunofluorescent\s+assay\b"
    r"|\s+by\s+Rapid\s+immunoassay\b"
    r"|\s+by\s+Latex\s+agglutination\b"
    r"|\s+by\s+Multiple\s+allergens\b"
    r"|\s+by\s+Hemagglutination\s+inhibition\b"
    r"|\s+by\s+Complement\s+fixation\b"
    r"|\s+by\s+Neutralization\s+test\b"
    r"|\s+by\s+Sabin\s+dye\s+test\b"
    r"|\s+by\s+(?:Monkey|Human|Guinea\s+pig)\s+\w+(?:\s+\w+)?\s+substrate\b"
    r"|\s+by\s+Immunofluorescence\b"
    r"|\s+by\s+Immunoblot\b"
    r"|\s+by\s+Line\s+blot\b"
    r"|\s+by\s+Multidisk\b"
    r"|\s+by\s+Immunobead\b"
    r"|\s+by\s+Microarray\b"
    r"|\s+by\s+Agglutination\b"
    r"|\s+by\s+Immunoassay\b"
    r"|\s+RAST\s+class\b",
    re.IGNORECASE,
)


# Allergen component-resolved diagnostic (CRD) markers — LOINC encodes
# recombinant / native / nUbi-tagged single epitopes of an allergen
# under names like ``Peanut recombinant (rAra h) 1 IgE Ab in Serum``,
# ``Apple nUbi recombinant (rMal d) 1 IgE Ab``, ``Birch native (nBet
# v) 1 IgE Ab``. The component qualifier produces a unique family key
# per epitope, so the sibling-aware drop in :func:`_build_food_non_igg_mask`
# misses the broader analyte-level IgG sibling that exists in LOINC
# (``Peanut IgG Ab`` 107768-4 for Peanut rAra h component IgE; ``Celery
# IgG Ab`` 107853-4 for Celery rApi g component IgE).
#
# Strip the component qualifier so all per-epitope rows fold into the
# bare-analyte family for sibling lookup. The actual LCN row stays
# unchanged in the candidate pool; only the family_key used by the
# antibody-class drop logic is normalized.
_AB_COMPONENT_RE = re.compile(
    r"\s+(?:recombinant|native|nUbi(?:\s+recombinant)?)\s*"
    r"\([^)]*\)"                      # (rAra h), (nBet v), (rMal d)
    r"(?:\s+\d+(?:\s*\+\s*\d+)?[a-z]?)?",  # 1, 1+5b, 5b — optional epitope number
    re.IGNORECASE,
)


def _antibody_family_key(name: str) -> str:
    """Sibling-aware family key for antibody-class detection. Drops
    legacy method suffixes (:data:`_AB_METHOD_SUFFIX_RE`), allergen
    component-resolved markers (:data:`_AB_COMPONENT_RE`), and the
    trailing specimen tail (:func:`_strip_specimen_tail`) before
    delegating to :func:`loinc_family_key`. Component/specimen strip
    means ``Peanut recombinant (rAra h) 1 IgE Ab in Serum`` and
    ``Peanut IgG Ab in Serum or Plasma by Immunoassay`` both reduce
    to family key ``peanut`` — the sibling-aware drop in
    :func:`_build_food_non_igg_mask` and the IgG sibling index in
    :func:`_build_ig_class_match_mask` see the same canonical analyte
    across CRD-component and bulk-analyte rows. Used symmetrically on
    the IgE side (mask candidates) and the IgG side (sibling index).
    """
    s = _AB_METHOD_SUFFIX_RE.sub("", name)
    s = _AB_COMPONENT_RE.sub("", s)
    fk = loinc_family_key(s)
    return _strip_specimen_tail(fk)


def _build_food_non_igg_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows the food-sensitivity-gated picker should avoid:

    1. IgE Ab rows WHERE a same-family IgG Ab sibling exists in
       LOINC. Niche foods/gums (Tragacanth, Carrageenan, Guar gum,
       Xanthan, Wild Rice, Tilapia, Halibut, Scallop, Brazilian Rubber
       Tree, Bromelain, Tropomyosin, ...) have only IgE variants in
       LOINC — dropping their IgE row would push the pick to a cross-
       analyte IgG row (Agar IgG, Karaya IgG, Bass Black IgG, ...)
       which is worse than the same-analyte IgE.
    2. Trigger-release / CAST-ELISA functional assays — unconditional
       (these are specialized methodologies the user must license
       explicitly).

    Family key derivation: strip the ``IgE Ab`` / ``IgG Ab`` token
    from the name then run :func:`loinc_family_key`. Same-family
    means "everything but the antibody class matches" — picks up
    cross-specimen variants too (Serum vs Plasma vs by Immunoassay).
    """
    cached = cache.get("_loinc_food_non_igg_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])

    # Pass 1: index family-keys of all IgG Ab rows.
    igg_family_keys: set[str] = set()
    for nm in names:
        if not nm:
            continue
        m = _IGG_AB_RE.search(nm)
        if not m:
            continue
        stripped = (nm[:m.start()] + " " + nm[m.end():]).strip()
        fk = _antibody_family_key(stripped)
        if fk:
            igg_family_keys.add(fk)

    # Pass 2: mark IgE rows ONLY if their family has an IgG sibling.
    # Trigger-release rows always marked (no class-sibling fallback).
    mask = np.zeros(n, dtype=bool)
    n_ige_dropped_with_igg = 0
    n_ige_kept_no_igg = 0
    n_trigger = 0
    for i, nm in enumerate(names):
        if not nm:
            continue
        m = _IGE_AB_RE.search(nm)
        if m:
            stripped = (nm[:m.start()] + " " + nm[m.end():]).strip()
            fk = _antibody_family_key(stripped)
            if fk and fk in igg_family_keys:
                mask[i] = True
                n_ige_dropped_with_igg += 1
            else:
                n_ige_kept_no_igg += 1
        elif _TRIGGER_RELEASE_RE.search(nm):
            mask[i] = True
            n_trigger += 1
    log.info(
        "food non-IgG mask: %d rows flagged (IgE-with-IgG-sibling=%d, "
        "trigger-release=%d, IgE-without-IgG-sibling kept=%d)",
        int(mask.sum()), n_ige_dropped_with_igg, n_trigger,
        n_ige_kept_no_igg,
    )
    cache["_loinc_food_non_igg_mask"] = mask
    return mask


# Coagulation-factor assay-type preference. A bare factor query
# (``凝血因子V`` / ``Coagulation Factor V``) means the *functional*
# clotting assay — the activity level is the routine measurement;
# the antigen (``Ag``) assay is a specialized add-on ordered only when
# explicitly named. Raw cosine can't separate the two reliably: for
# FVIII/FIX/FX/FXI/FXII the activity row happens to win, but for FII
# and FV the ``Ag`` row edges ahead (3288-8 Prothrombin Ag, 3194-8
# factor V Ag), giving an inconsistent panel. This drops the ``Ag``
# variant — but ONLY when the same factor also has an activity sibling
# in LOINC, so antigen-only analytes (Plasminogen, where no activity
# code exists) keep their Ag row instead of nulling out.
_COAG_FACTOR_NAME_RE = re.compile(r"^(?:Coagulation factor|Prothrombin)\b")
_COAG_AG_TOKEN_RE = re.compile(r"\bAg\b")
_COAG_ACTIVITY_TOKEN_RE = re.compile(r"\bactivity\b")
_COAG_ASSAY_SPLIT_RE = re.compile(r"\s+(?:Ag|activity)\b")
# A "clean" activity level (a valid drop-in replacement for the antigen
# assay) excludes ratio forms (``activity actual/normal`` — a RelTime
# ratio, not a level) and ``--``-qualified niche assays (depleted-plasma
# substitution studies). Factor II / Prothrombin has ONLY those two
# forms — no plain ``factor II activity [Units/volume]`` — so it has no
# clean sibling and its ``Prothrombin Ag`` row stays put.
_COAG_ACTIVITY_RATIO_RE = re.compile(r"actual/normal", re.IGNORECASE)

# Query carries a coagulation-factor name (not PT/TT/antithrombin —
# those don't share the Ag/activity ambiguity).
_COAG_FACTOR_QUERY_RE = re.compile(
    r"凝血因子|coagulation\s+factor|clotting\s+factor",
    re.IGNORECASE,
)
# Explicit antigen request — keep the Ag row when the user asked for it.
_COAG_ANTIGEN_LICENSE_RE = re.compile(r"抗原|antigen|\bAg\b", re.IGNORECASE)


def _build_coag_factor_ag_mask(cache: dict) -> np.ndarray:
    """Flag ``Coagulation factor X Ag`` / ``Prothrombin Ag`` rows whose
    factor *also* has an activity variant in LOINC.

    Base analyte = name truncated at the first `` Ag``/`` activity``
    token (``Coagulation factor V Ag [..]`` and ``Coagulation factor V
    activity [..]`` both → ``Coagulation factor V``; ``Prothrombin Ag``
    and ``Prothrombin activity actual/normal`` both → ``Prothrombin``).
    Scoped to coagulation-factor / prothrombin names so unrelated
    analytes carrying ``Ag`` (HBsAg) or ``activity`` (``Amylase
    [Enzymatic activity/volume]``) are untouched.
    """
    cached = cache.get("_loinc_coag_factor_ag_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])

    activity_bases: set[str] = set()
    for nm in names:
        if not nm or not _COAG_FACTOR_NAME_RE.match(nm):
            continue
        if (
            _COAG_ACTIVITY_TOKEN_RE.search(nm)
            and not _COAG_ACTIVITY_RATIO_RE.search(nm)
            and "--" not in nm
        ):
            activity_bases.add(_COAG_ASSAY_SPLIT_RE.split(nm, maxsplit=1)[0])

    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if not nm or not _COAG_FACTOR_NAME_RE.match(nm):
            continue
        if not _COAG_AG_TOKEN_RE.search(nm):
            continue
        base = _COAG_ASSAY_SPLIT_RE.split(nm, maxsplit=1)[0]
        if base in activity_bases:
            mask[i] = True
    log.info(
        "coag-factor Ag mask: %d antigen rows flagged "
        "(%d factor bases have an activity sibling)",
        int(mask.sum()), len(activity_bases),
    )
    cache["_loinc_coag_factor_ag_mask"] = mask
    return mask


def _coag_factor_activity_keep(
    query_text: str, cache: dict
) -> "np.ndarray | None":
    """Prefer the activity assay over the antigen assay for bare
    coagulation-factor queries. See :func:`_build_coag_factor_ag_mask`.

    Silent unless the query names a coagulation factor and carries no
    explicit antigen marker (``抗原`` / ``antigen`` / ``Ag``).
    """
    if not _COAG_FACTOR_QUERY_RE.search(query_text):
        return None
    if _COAG_ANTIGEN_LICENSE_RE.search(query_text):
        return None
    mask = _build_coag_factor_ag_mask(cache)
    if not mask.any():
        return None
    return ~mask


# Sleep-context off-topic filter — polysomnography / 睡眠报告 queries
# in CN clinical catalogs carry many multi-axis combinations
# (``周期性腿动·清醒``, ``呼吸暂停·非REM``, ``CO2·非快速眼动`` —
# event-axis × statistic × posture × sleep-stage breakdowns) that
# LOINC does not enumerate as specific codes. The picker routinely
# falls onto PhenX/PROMIS PRO instruments (subjective sleep-quality
# questionnaires), ECG/QRS rows (cardiology, picked up by ``持续时间``
# substring overlap), or blood-pressure rows (matched on ``左侧/右侧``
# laterality). Drop these obvious off-topic rows when the query is
# in sleep context and didn't explicitly ask for the questionnaire
# or modality.
_SLEEP_CONTEXT_RE = re.compile(
    r"睡眠"
    r"|呼吸暂停|呼吸浅慢|呼吸紊乱|通气不足"
    r"|周期性腿动|腿部动作"
    r"|微觉醒|觉醒"
    r"|鼾声|打鼾"
    r"|血氧饱和度|氧饱和度"
    r"|多导睡眠图|多导睡眠"
    r"|快速眼动|非快速眼动"
    r"|睡眠阶段"
    r"|sleep\b|apnea\b|hypopnea\b|polysomnograph|periodic\s+leg\s+movement|PLMS?\b"
    r"|REM\s+sleep|non[- ]?REM"
    r"|arousal\b|microarousal",
    re.IGNORECASE,
)
_SLEEP_OFF_TOPIC_NAME_RE = re.compile(
    # Survey / PRO instruments — bracketed acronym suffixes. PhenX /
    # PROMIS / TIMP / DI-PAD / M3 / Penn / similar PRO questionnaires
    # are not objective sleep measurements.
    r"\[PhenX\]|\[PROMIS|\[TIMP\]|\[DI[- ]?PAD\]|\[M3\]|\[Penn"
    r"|\[NeuroQol\]|\[Neuro-?QOL\]|\[IPAQ\]|\[GPAQ\]|\[PSQI\]|\[ESS\]"
    # Cardiology — ECG / echocardiography / EKG. Sleep studies use
    # EEG not EKG; cardiac morphology rows (heart waves / valve
    # measurements / ECG leads / Doppler US) match sleep queries on
    # generic ``持续时间`` / ``阶段`` tokens. Matches the full ECG
    # family (QRS / single-letter waves P/Q/R/S/T/U with optional
    # prime / Reference beat / intervals / segments), echocardiography
    # via ``by US`` and valve names, and lead notation.
    r"|\bQRS\b"
    r"|\b[PQRSTU]['‘’]?\s+wave\b"
    r"|\bReference\s+beat\b"
    r"|\b(?:ST|PR|RR|Q-?T|QTc|P-?R|R-?R)\s+(?:interval|segment|elevation|depression)\b"
    r"|\bin\s+lead\s+(?:I|II|III|AV[RFL]|V[1-9])\b"
    r"|\b(?:Tricuspid|Mitral|Aortic|Pulmonary)\s+valve\b"
    r"|\bby\s+US\b"
    r"|\bEKG\b|\bECG\b"
    r"|electrocardiograph"
    # Blood pressure — cardiology / vital signs panel; can sneak in
    # via ``lying`` / ``L-lateral`` / ``R-lateral`` body-position
    # phrases shared with sleep-side queries (``左侧卧位``).
    r"|\bblood\s+pressure\b|\bdiastolic\b|\bsystolic\b"
    # Imaging modalities — clearly not sleep-study output. MG =
    # Mammography (LOINC convention), MR / MRI / CT / XR / US.
    r"|\bMR\s|\bMRI\b|\bCT\s|\bXR\b|\bMG\s+(?:Breast|Mammary|Bilateral)"
    r"|\bradiograph|\bx[- ]?ray\b"
    # Lateral-head-righting motor-development infant assessment.
    r"|Lateral\s+head\s+righting",
    re.IGNORECASE,
)
_SLEEP_LICENSE_RE = re.compile(
    # Query explicitly asked for the questionnaire / blood pressure /
    # ECG / imaging — respect.
    r"问卷|调查|PhenX|PROMIS"
    r"|心电图|ECG|EKG"
    r"|血压|blood\s+pressure"
    r"|CT|MR|MRI|radiograph",
    re.IGNORECASE,
)


def _build_sleep_off_topic_mask(cache: dict) -> np.ndarray:
    """Tag PhenX/PROMIS questionnaire rows, ECG/QRS rows, blood-pressure
    rows, and imaging-modality rows — clearly off-topic for sleep-
    context queries. Cached on *cache*."""
    cached = cache.get("_loinc_sleep_off_topic_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _SLEEP_OFF_TOPIC_NAME_RE.search(nm):
            mask[i] = True
    log.info(
        "sleep off-topic mask: %d / %d rows flagged (PhenX/PROMIS + "
        "ECG/QRS + BP + imaging)", int(mask.sum()), n,
    )
    cache["_loinc_sleep_off_topic_mask"] = mask
    return mask


# Chinese-specific tests with no LOINC equivalent — force-null when
# query unambiguously names one. These tests live in CN hemorheology
# panels (体外血栓 X — in-vitro thrombus weight/length/formation on
# thrombelastograph) and similar specialty workflows. Without this,
# cosine falls onto unrelated ``Erythrocyte deformability narrative``
# / ``Specific gravity of Red Blood Cells`` rows that mislead more
# than null does.
_CN_NO_LOINC_NULL_RE = re.compile(
    r"体外血栓(?:形成|长度|湿重|干重|重量)",
)


def _cn_no_loinc_null(query_text: str, cache: dict) -> "np.ndarray | None":
    """Force-null for queries that LOINC truly doesn't cover.
    Returns an all-False mask so the picker yields no LOINC result,
    rather than cosine-falling onto a misleading nearby row."""
    if not _CN_NO_LOINC_NULL_RE.search(query_text):
        return None
    n = int(np.asarray(cache["canonical"]).shape[0])
    return np.zeros(n, dtype=bool)


def _sleep_focus_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """For sleep-context queries (polysomnography / PLMS / arousal /
    apnea / hypopnea / snore / SpO2-time / sleep-stage breakdowns)
    carrying no explicit questionnaire / cardiology / imaging marker,
    drop LOINC rows that clearly don't belong in a sleep study output:
    PhenX/PROMIS PRO instruments, ECG/QRS/EKG rows, blood-pressure
    rows, and CT/MR imaging rows.

    Silent when:
      - query has no sleep-context marker.
      - query explicitly names questionnaire / cardiology / imaging.
    """
    if not _SLEEP_CONTEXT_RE.search(query_text):
        return None
    if _SLEEP_LICENSE_RE.search(query_text):
        return None
    return ~_build_sleep_off_topic_mask(cache)


# PSG analyte-head preservation. Companion to :func:`_sleep_focus_keep`.
#
# Sleep-study indicator names are multi-axis breakdowns: ``<analyte>·
# <sleep-stage>·<statistic>·<posture>`` (``周期性腿动·N3·均值``,
# ``呼吸浅慢·非REM·持续时间``, ``身体姿势·左侧·睡眠时长``). The cosine
# top routinely matches on the *qualifier* axis (REM / Wake / Supine /
# Non-REM) rather than the *analyte* axis (PLM / hypopnea / desat /
# posture), landing on the wrong LOINC family — typically the sleep-
# duration skeleton (``Light/Deep/REM/Sleep duration``, ``Wake time
# after sleep onset``, ``Number of awakenings``, ``Duration in bed``)
# which carries every common sleep-stage qualifier in its name but
# none of the analyte heads.
#
# Each family ``(query_re, lcn_re)`` pair lets us derive — from the
# LOINC LCN axis — the row set that *encodes* analyte X, and the
# query token set that *asks for* analyte X. Multilingual query-side
# coverage: CN + Latin alphabet variants (cover EN / DE / FR / ES via
# Latin stems; JA / KO / RU via locally-typical clinical forms).
# Stems come from real LOINC LCNs so adding a new sleep code with a
# known analyte head inherits the rule automatically.
_PSG_ANALYTE_FAMILIES: tuple[tuple[str, str, str], ...] = (
    # (family_name, query_re, lcn_stem_re)
    (
        "leg_movement",
        # CN / JA periodic-leg-movement phrasing + EN abbreviations
        r"周期性腿动|腿部动作|周期性肢动|肢体运动"
        r"|周期性下肢運動|周期性四肢運動"        # JA
        r"|주기성\s*다리|주기성\s*사지"           # KO
        r"|periodic\s+(?:leg|limb)\s+movement"
        r"|PLM[SW]?\b",
        # LOINC has no current PLM code; matching nothing is correct
        # — the row-set is empty and only the skeleton-fallback fires.
        r"(?:periodic\s+)?(?:leg|limb)\s+movement",
    ),
    (
        "apnea",
        r"呼吸暂停|無呼吸|无呼吸|無呼吸發作"
        r"|무호흡"
        r"|\bapnea\b|\bapnoea\b|апноэ",
        r"\bapnea\b|\bapnoea\b",
    ),
    (
        "hypopnea",
        r"呼吸浅慢|低通气|低呼吸|低換気"
        r"|저호흡|저환기"
        r"|\bhypopnea\b|\bhypopnoea\b|гипопноэ",
        r"\bhypopnea\b|\bhypopnoea\b",
    ),
    (
        # Steady-state SpO2 / SaO2 — the *measurement* (0-100%).
        # Disjoint family bit from `desaturation` (the event). LOINC
        # has dozens of `Oxygen saturation` rows (2708-6 arterial,
        # 2709-4 capillary, 20564-1 blood, …); none of them carry the
        # `desaturation` token, so the LCN regex distinguishes
        # cleanly. Queries that mention both (``血氧饱和度下降``)
        # trigger both families and accept either — cosine picks.
        "oxygen_saturation",
        r"血氧(?:饱和度|飽和度)|氧饱和度|氧飽和度|血氧分压"
        r"|酸素飽和度"
        r"|산소\s*포화도"
        r"|Sa?O[\s_]?2\b|SpO[\s_]?2\b"
        r"|\boxygen\s+saturation\b"
        r"|sat(?:uration)?\s+(?:de\s+)?l?[''']?(?:O2|oxyg[eè]ne?|ox[ií]geno)",
        r"\boxygen\s+saturation\b",
    ),
    (
        # Desaturation *event* (drop in SpO2). The LCN regex excludes
        # plain `oxygen saturation` rows by requiring the literal
        # `desaturation` morpheme.
        "desaturation",
        r"血氧(?:饱和度|飽和度).{0,6}(?:下降|降低|事件)|血氧.{0,4}下降"
        r"|低酸素|酸素低下"                          # JA
        r"|저산소|산소\s*포화도\s*감소"             # KO
        r"|\bdesaturation\b"
        r"|d[eé]saturation|desaturaci[oó]n",
        r"\bdesaturation\b",
    ),
    (
        "respiration_rate",
        r"呼吸频率|呼吸率|呼吸次数|呼吸数"
        r"|호흡(?:수|률)"                          # KO
        r"|respirat(?:ion|ory)\s+rate|breath(?:ing)?\s+rate"
        r"|fr[eé]quence\s+respiratoire"
        r"|frecuencia\s+respiratoria"
        r"|atemfrequenz",
        # LOINC canonical name is ``Respiratory rate`` (9279-1, 76170-0,
        # 19841-6, ...); ``Respiration rate`` only appears in non-rate
        # contexts. Include both for robustness.
        r"respirat(?:ion|ory)\s+rate|breath(?:ing)?\s+rate|breaths?\s+per\s+min",
    ),
    (
        "body_position",
        r"身体姿势|身體姿勢|睡眠体位|睡眠體位|体位|體位"
        r"|体勢|姿勢"                              # JA
        r"|체위|자세"                              # KO
        r"|body\s+position|body\s+posture|sleep\s+position|posture"
        r"|posici[oó]n\s+corporal|k[oö]rperlage|position\s+du\s+corps",
        r"\bbody\s+position\b|\bposture\b|\bsleep\s+position\b",
    ),
    (
        "snore",
        r"鼾声|鼾聲|打鼾|呼噜"
        r"|いびき"                                  # JA
        r"|코골이"                                  # KO
        r"|храп"                                    # RU
        r"|snor(?:e|ing|es)\b|ronq[uü]i|ronflement|schnarch",
        r"\bsnor",
    ),
    (
        "arousal",
        r"微觉醒|微覺醒|觉醒\b|覺醒\b|短暂觉醒|短暫覺醒"
        r"|微小覚醒|覚醒(?:反応)?"                  # JA
        r"|미세\s*각성|각성"                       # KO
        r"|пробуждени"                              # RU
        r"|micro[- ]?arousal|\barousal\b"
        r"|micro[- ]?[eé]veil",
        r"\barousal\b|\bawakening\b(?!\s+duration|\s+count|s\b)",
    ),
    (
        # Respiratory disturbance index — composite measure (apnea +
        # hypopnea + RERA) that gets its own LOINC code 90566-1. The
        # query side fires on explicit RDI / 紊乱 markers so the row
        # is licensed for queries like ``呼吸紊乱指数(RDI)`` and stays
        # competitive against the more specific apnea/hypopnea/RERA
        # rows.
        "respiratory_disturbance",
        r"呼吸紊乱|紊乱指数|RDI\b"
        r"|respiratory\s+disturbance",
        r"respiratory\s+disturbance",
    ),
    (
        # Sleep hypoventilation — distinct from hypopnea (reduced
        # ventilation vs reduced airflow). LOINC has zero codes for
        # sleep hypoventilation as of 2.78. The cosine top routinely
        # lands on 90555-4 / 90556-2 / 90558-8 (hypopnea family),
        # which is the wrong concept. Force-null via _PSG_NO_LOINC_FAMILY_NAMES.
        "hypoventilation",
        r"通气不足|通氣不足|低通气量|低通氣量|换气不足|換氣不足"
        r"|低換気|肺胞低換気"                       # JA
        r"|저환기|환기\s*저하"                      # KO
        r"|гиповентил"                              # RU
        r"|hypoventilat",
        # LCN regex deliberately unmatchable — no LOINC row encodes
        # this concept. Stays in the no-LOINC family bitset below.
        r"(?!)",
    ),
    (
        # Flow limitation (upper-airway resistance event marker on
        # PSG). LOINC has no specific code; cosine drifts to RDI /
        # AHI rows on the shared ``指数`` token.
        "flow_limitation",
        r"气流受限|氣流受限|气流限制|氣流限制"
        r"|気流制限"                                # JA
        r"|기류\s*제한"                             # KO
        r"|ограничени.{0,4}поток"                   # RU
        r"|flow\s*[- ]?\s*limit(?:ation)?|UARS\b",
        r"(?!)",
    ),
)
# Compiled forms — pre-build once at import time.
_PSG_QUERY_RES: tuple[tuple[int, "re.Pattern[str]"], ...] = tuple(
    (1 << i, re.compile(qp, re.IGNORECASE))
    for i, (_, qp, _) in enumerate(_PSG_ANALYTE_FAMILIES)
)
_PSG_LCN_RES: tuple[tuple[int, "re.Pattern[str]"], ...] = tuple(
    (1 << i, re.compile(lp, re.IGNORECASE))
    for i, (_, _, lp) in enumerate(_PSG_ANALYTE_FAMILIES)
)

# Generic sleep-time skeleton: LOINC rows whose LCN is a pure sleep-
# stage / wake / awakening duration marker. These rows carry every
# common sleep-stage qualifier (REM / Wake / N1-N3 / Light / Deep /
# Non-REM) in their canonical name, so they cosine-match any sleep-
# context query — fine when the query genuinely asks for total sleep
# time / WASO / # awakenings, wrong when the query carries an analyte
# head (PLM / hypopnea / desaturation / posture). Matched by full-LCN
# patterns so future LOINC additions in the same family inherit the
# mask automatically.
_SLEEP_SKELETON_NAME_RE = re.compile(
    r"^Wake\s+time\s+after\s+sleep\s+onset$"
    r"|^Awakening\s+duration$"
    r"|^Number\s+of\s+awakenings$"
    r"|^Duration\s+in\s+bed$"
    r"|^Duration\s+of\s+falling\s+asleep$"
    r"|^(?:Light|Deep|REM)\s+sleep\s+duration$"
    r"|^Sleep\s+duration$"
    r"|^Sleep\s+stage\b"
    r"|^State\s+of\s+arousal$"
    r"|^Nighttime\s+awakening(?:\s+duration)?$"
    r"|^Satisfaction\s+with\s+sleep\b"
    r"|^Sleep\s+quality\b"
    # Over-broad PSG parents — never the right answer when query
    # carries a specific analyte head. The skeleton gate only fires
    # when qbits != 0 (some PSG family matched the query) so these
    # stay reachable as cosine fallbacks for generic queries.
    r"|^Polysomnography\s+panel$"
    r"|^Polysomnography\s+\(sleep\)\s+study$"
    r"|^Restless\s+sleep$"
    r"|^OSQ$"
    # Sleep-medicine clinical notes (CLASS=DOC.ONTOLOGY) — these
    # describe encounter documentation, not the measurements a PSG
    # report indicator names. Cosine routinely surfaces them when
    # the analyte family LCN regex finds no row (``Sleep medicine
    # Diagnostic study note`` shadows ``Central apnea [#]`` for
    # ``中枢性呼吸暂停·REM``).
    r"|^Sleep\s+medicine\b",
    re.IGNORECASE,
)
# RDI ``Respiratory disturbance index`` (90566-1) and AHI codes
# (90559-6 / 90561-2 / 90562-0 / 90563-8 / 90564-6 / 69990-0 / 70002-1
# / 70003-9) are NOT in skel — they are valid LOINC concepts that
# specific queries (``呼吸紊乱指数 / RDI``, ``呼吸暂停低通气指数 /
# AHI``) want. They sit under the apnea / hypopnea / arousal family
# LCN regexes (``\\bapnea\\b``, ``\\bhypopnea\\b``, ``\\barousal\\b``)
# and stay reachable through ``(bits & qbits) != 0``. The
# ``respiratory_disturbance`` family below licenses 90566-1 for
# explicit RDI queries.


def _build_psg_family_bits(cache: dict) -> tuple[np.ndarray, np.ndarray]:
    """For each LOINC row, return (analyte_bits, skeleton_mask).
    ``analyte_bits[r]`` is a uint16 bitmask of :data:`_PSG_ANALYTE_FAMILIES`
    indices whose LCN stem the row matches. ``skeleton_mask[r]`` flags
    rows whose LCN is a generic sleep-skeleton phrase carrying no
    analyte head. Cached on *cache* under ``_loinc_psg_family_bits``.
    """
    cached = cache.get("_loinc_psg_family_bits")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    bits = np.zeros(n, dtype=np.uint16)
    skel = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if not nm:
            continue
        b = 0
        for bit, rx in _PSG_LCN_RES:
            if rx.search(nm):
                b |= bit
        bits[i] = b
        if b == 0 and _SLEEP_SKELETON_NAME_RE.search(nm):
            skel[i] = True
    log.info(
        "psg family bits: %d / %d rows carry an analyte head, "
        "%d skeleton-only rows",
        int((bits != 0).sum()), n, int(skel.sum()),
    )
    cache["_loinc_psg_family_bits"] = (bits, skel)
    return (bits, skel)


# PSG analyte families for which LOINC has no clinically usable codes
# in the sleep-study context. Force-null when these families match the
# query — any cosine pick is more misleading than emitting nothing.
#
#   - ``leg_movement``: LOINC has 62786-9 (PhenX RLS protocol panel)
#     and 100777-2 (Restless sleep PROMIS). Neither is a PLM count /
#     index / arousal-linked measurement. The cosine top routinely
#     lands on ``Restless sleep`` or ``Polysomnography panel``,
#     both confusing.
#   - ``body_position``: LOINC has 8360-0 / 8361-8 (vital-signs body
#     position for BP) and 72282-7 (sagittal plane CLIN). None encode
#     "time spent in left / right / supine / prone DURING SLEEP",
#     which is what sleep-study indicators ask for. Cosine drifts to
#     ``Left contact lens Axis.DS`` and similar laterality noise.
_PSG_NO_LOINC_FAMILY_NAMES: frozenset[str] = frozenset({
    "leg_movement", "body_position",
    # No LOINC code for sleep hypoventilation (≠ hypopnea) or
    # flow-limitation events (UARS) as of LOINC 2.78. Cosine drifts to
    # the hypopnea family / RDI row, both clinically wrong.
    "hypoventilation", "flow_limitation",
})
_PSG_NO_LOINC_FAMILY_BITS: int = sum(
    1 << i
    for i, (name, _, _) in enumerate(_PSG_ANALYTE_FAMILIES)
    if name in _PSG_NO_LOINC_FAMILY_NAMES
)


# Arousal cause sub-axis. LOINC only has 90557-0 / 90565-3 for
# respiratory-effort-related arousals (RERA / RERA index). The other
# microarousal sub-types in clinical PSG reports — desaturation-
# related, heart-rate-related, snore-related, PLM-related, and the
# generic ``total / 总数`` aggregate — have no LOINC equivalent.
# Cosine drifts to 90565-3 because it's the only arousal-index row,
# producing systematic false positives ("Microarousal·Snore MA" ⇒
# RERA index). Treatment: when the query carries arousal context AND
# a non-RERA cause marker AND no explicit RERA / effort-related
# license, force-null the arousal family by adding it to qbits'
# no-LOINC region.
_AROUSAL_CAUSE_NON_RERA_RE = re.compile(
    # Desaturation / SpO2 drop
    r"血氧.{0,6}(?:下降|降低|相关|相關)|desat(?:uration)?\s*(?:related|MA)?"
    # Heart-rate-related arousal
    r"|心率.{0,6}(?:变化|變化|相关|相關)|heart\s*rate\s*(?:related|MA)"
    # Snore-related arousal
    r"|鼾声相关|鼾聲相關|打鼾相关|打鼾相關|snore\s*(?:related|MA)"
    # PLM-related arousal
    r"|肢动.{0,4}觉醒|肢動.{0,4}覺醒|PLM\s*(?:related|arousal)|leg\s*movement\s*arousal"
    # Spontaneous arousal
    r"|自发(?:性)?(?:觉醒|微觉醒)|自發(?:性)?(?:覺醒|微覺醒)|spontaneous\s*(?:micro)?arousal"
    # Generic aggregate (no cause specifier) — also no LOINC code
    r"|微觉醒.{0,4}总数|微覺醒.{0,4}總數|微觉醒.{0,4}百分比|微覺醒.{0,4}百分比"
    r"|total\s+(?:micro)?arousals?|arousals?\s+total|arousal\s+index\s+total",
    re.IGNORECASE,
)
# RERA / respiratory-effort-related — the ONE thing LOINC 90557-0 /
# 90565-3 actually encode. License the arousal family when these
# markers fire.
_AROUSAL_RERA_LICENSE_RE = re.compile(
    r"呼吸努力相关|呼吸努力相關|呼吸用力相关|呼吸用力相關"
    r"|respiratory\s+effort[- ]?related|RERA\b"
    r"|effort\s+related\s+arousal",
    re.IGNORECASE,
)
_AROUSAL_FAMILY_NAME = "arousal"
_AROUSAL_FAMILY_BIT: int = sum(
    1 << i
    for i, (name, _, _) in enumerate(_PSG_ANALYTE_FAMILIES)
    if name == _AROUSAL_FAMILY_NAME
)
# Families whose query tokens can fire as *modifiers* of arousal
# ("desat-related microarousal", "snore-related microarousal"). When
# arousal-cause null fires, these bits are stripped from qbits so the
# null gate sees a pure-arousal query.
_AROUSAL_CAUSE_FAMILY_NAMES: frozenset[str] = frozenset({
    "desaturation", "oxygen_saturation", "snore",
    "respiration_rate", "leg_movement",
})
_AROUSAL_CAUSE_FAMILY_BITS: int = sum(
    1 << i
    for i, (name, _, _) in enumerate(_PSG_ANALYTE_FAMILIES)
    if name in _AROUSAL_CAUSE_FAMILY_NAMES
)


# Apnea / hypopnea exclusive sub-split. When the query carries ONLY
# an apnea marker (no hypopnea token) or ONLY a hypopnea marker,
# drop LOINC rows whose LCN carries the OTHER token — i.e. drop the
# combined ``Apnea + hypopnea`` / ``Obstructive apnea hypopnea index``
# / ``Central apnea hypopnea index`` family from solo queries. Cosine
# routinely matches the combined rows when the query says only
# ``阻塞性呼吸暂停`` (obstructive apnea) because the combined row's
# LCN still contains ``apnea`` and dominates on the qualifier axis.
_APNEA_FAMILY_NAME = "apnea"
_HYPOPNEA_FAMILY_NAME = "hypopnea"
_APNEA_FAMILY_BIT: int = sum(
    1 << i
    for i, (name, _, _) in enumerate(_PSG_ANALYTE_FAMILIES)
    if name == _APNEA_FAMILY_NAME
)
_HYPOPNEA_FAMILY_BIT: int = sum(
    1 << i
    for i, (name, _, _) in enumerate(_PSG_ANALYTE_FAMILIES)
    if name == _HYPOPNEA_FAMILY_NAME
)


# Snore family — duration vs count split. LOINC carries both rows:
#   103220-0 Duration of snoring   (time)
#   103221-8 Number of snoring episodes  (count)
# Cosine routinely prefers the wrong one (``绝对打鼾时间·仰卧`` →
# 103221-8 in v46). Within the snore-family keep set, an explicit
# duration / count marker in the query picks the matching half.
_SNORE_FAMILY_NAME = "snore"
_SNORE_FAMILY_BIT: int = sum(
    1 << i
    for i, (name, _, _) in enumerate(_PSG_ANALYTE_FAMILIES)
    if name == _SNORE_FAMILY_NAME
)
_SNORE_DURATION_QUERY_RE = re.compile(
    r"时长|時長|时间|時間|持续时间|持續時間"
    r"|時間|持続"                            # JA
    r"|시간|지속\s*시간"                       # KO
    r"|\bdur(?:ation|ée)\b|\btime\b"
    r"|\bdauer\b|\btiempo\b|\bdurée\b|\bдлительность\b",
    re.IGNORECASE,
)
_SNORE_COUNT_QUERY_RE = re.compile(
    r"次数|次數|事件数|事件數|episode(?:s)?\b|count\b|number\b"
    r"|回数"                                  # JA
    r"|횟수|건수",                            # KO
    re.IGNORECASE,
)
_SNORE_DURATION_LCN_RE = re.compile(
    r"^Duration\s+of\s+snor",
    re.IGNORECASE,
)
_SNORE_COUNT_LCN_RE = re.compile(
    r"^Number\s+of\s+snor",
    re.IGNORECASE,
)


def _build_snore_split_masks(cache: dict) -> tuple[np.ndarray, np.ndarray]:
    """Return (duration_mask, count_mask) flagging Duration-of-snoring
    vs Number-of-snoring-episodes LOINC rows. Cached on *cache*."""
    cached = cache.get("_loinc_snore_split_masks")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    dur = np.zeros(n, dtype=bool)
    cnt = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if not nm:
            continue
        if _SNORE_DURATION_LCN_RE.search(nm):
            dur[i] = True
        elif _SNORE_COUNT_LCN_RE.search(nm):
            cnt[i] = True
    cache["_loinc_snore_split_masks"] = (dur, cnt)
    return (dur, cnt)


# PSG signal artifact / non-sleep stage labels — recording-quality
# markers and motion-activity stages, not sleep-state analytes. LOINC
# has no codes for ``artifact / movement / activity duration during
# sleep stage X``. Cosine drifts to ``Sleep duration`` or
# ``Polysomnography panel`` on these queries.
_PSG_ARTIFACT_QUERY_RE = re.compile(
    r"睡眠.{0,10}伪迹|睡眠.{0,10}偽跡|sleep\s+artif?act"
    r"|睡眠阶段.{0,8}活动|睡眠阶段.{0,8}动作|sleep\s+stage.{0,15}(?:movement|activity|motion)"
    r"|アーチファクト|睡眠.{0,8}運動"          # JA
    r"|아티팩트|아티펙트|수면.{0,8}활동",     # KO
    re.IGNORECASE,
)

# Stage-stratified physiologic measurements outside the PSG analyte set
# (CO2 / SpO2 / heart rate / temperature broken down by REM / non-REM /
# wake / light / deep / N1-N4). LOINC has the analyte rows and the
# sleep-stage duration rows separately, but no combined ``analyte X
# during sleep stage Y`` codes. Force-null when both axes are present
# and the analyte isn't already covered by the PSG-analyte families
# above (apnea / hypopnea / arousal / RDI etc.).
_PSG_NON_RESP_ANALYTE_RE = re.compile(
    r"二氧化碳|CO2\b|carbon\s+dioxide"
    r"|心率|心率变化|心率變化|heart\s+rate|HRV?\b"
    r"|体温|體溫|temperature"
    r"|血压|血壓|blood\s+pressure",
    re.IGNORECASE,
)
_PSG_STAGE_BREAKDOWN_RE = re.compile(
    r"快速眼动|快速眼動|非快速眼动|非快速眼動|REM\b|non[- ]?REM"
    r"|N1\b|N2\b|N3\b|N4\b|stage\s+[1-4]\b|阶段\s*[1-4]"
    r"|深睡眠|淺睡眠|浅睡眠|deep\s+sleep|light\s+sleep|slow\s+wave",
    re.IGNORECASE,
)


# Wake-vs-stage sleep duration. Multiple LOINC ``X sleep duration``
# rows exist (93829-0 REM / 93830-8 Light / 93831-6 Deep / 93832-4
# Sleep). For ``睡眠阶段·清醒·持续时间``, the right answer is
# 103215-0 Wake time after sleep onset (WASO) or 103210-1 Awakening
# duration — but cosine ranks Deep sleep duration above the wake-
# specific rows because the wake LCN doesn't carry the ``sleep stage``
# token. Drop the non-wake stage-duration rows when the query
# explicitly names a wake / awake / 清醒 stage.
_SLEEP_WAKE_QUERY_RE = re.compile(
    r"清醒|清晨醒|醒\s*(?:期|时间|時間|状态|狀態)?"
    r"|wake(?:fulness)?\b|awake\b|awakening\b"
    r"|覚醒|覚醒期"                            # JA
    r"|각성|기상",                              # KO
    re.IGNORECASE,
)
_SLEEP_DURATION_QUERY_RE = re.compile(
    r"持续时间|持續時間|时长|時長|duration|time",
    re.IGNORECASE,
)
_SLEEP_STAGE_DURATION_DROP_LCN_RE = re.compile(
    r"^(?:Light|Deep|REM)\s+sleep\s+duration$"
    r"|^Sleep\s+duration$",
    re.IGNORECASE,
)


def _build_sleep_stage_duration_drop_mask(cache: dict) -> np.ndarray:
    """Tag the 4 sleep-stage duration LOINC rows (93829-0 / 93830-8 /
    93831-6 / 93832-4). Used to drop them when the query names a wake
    stage that has its own dedicated code (103215-0 / 103210-1).
    """
    cached = cache.get("_loinc_sleep_stage_duration_drop_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _SLEEP_STAGE_DURATION_DROP_LCN_RE.search(nm):
            mask[i] = True
    cache["_loinc_sleep_stage_duration_drop_mask"] = mask
    return mask


# Positive wake-duration keep set. When the query asks for wake-stage
# duration during sleep, restrict to the dedicated wake codes (rather
# than dropping the wrong ones and letting cosine drift to ``Usual
# duration of chest discomfort`` or similar non-sleep ``duration``
# rows).
_SLEEP_WAKE_DURATION_KEEP_LCN_RE = re.compile(
    r"^Wake\s+time\s+after\s+sleep\s+onset$"
    r"|^Awakening\s+duration$"
    r"|^Number\s+of\s+awakenings$"
    r"|^Nighttime\s+awakening(?:\s+duration)?$",
    re.IGNORECASE,
)


def _build_sleep_wake_keep_mask(cache: dict) -> np.ndarray:
    """Tag wake-related sleep-duration LOINC rows (WASO / Awakening
    duration / Nighttime awakening / Number of awakenings). Cached on
    *cache*."""
    cached = cache.get("_loinc_sleep_wake_keep_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _SLEEP_WAKE_DURATION_KEEP_LCN_RE.search(nm):
            mask[i] = True
    log.info(
        "sleep wake-duration keep mask: %d / %d LOINC rows flagged "
        "(WASO / awakening duration / number of awakenings)",
        int(mask.sum()), n,
    )
    cache["_loinc_sleep_wake_keep_mask"] = mask
    return mask


def _wake_duration_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """When a sleep-context query asks for wake/清醒 stage duration,
    restrict to the dedicated wake codes (103215-0 WASO / 103210-1
    Awakening duration / 93827-4 Nighttime awakening / 93828-2 Nighttime
    awakening duration). Cosine on Chinese ``睡眠阶段·清醒·持续时间``
    otherwise drifts to ``Deep sleep duration`` (sleep-stage analogy)
    or even ``Usual duration of chest discomfort`` (duration-token
    collision).
    Silent unless the query carries both a wake marker AND a duration
    marker AND a sleep-context marker.
    """
    if not _SLEEP_CONTEXT_RE.search(query_text):
        return None
    if _SLEEP_LICENSE_RE.search(query_text):
        return None
    if not (_SLEEP_WAKE_QUERY_RE.search(query_text)
            and _SLEEP_DURATION_QUERY_RE.search(query_text)):
        return None
    return _build_sleep_wake_keep_mask(cache)


def _psg_no_loinc_extras_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Force-null for PSG queries outside the analyte-family region:
    PSG signal artifact tracking, stage-stratified non-respiratory
    measurements (CO2 / HR / temperature / BP × REM / non-REM / N1-N4).
    LOINC has no codes for these compounds. Mirrors the corresponding
    branches in :func:`should_force_null_loinc` for the axes pipeline.
    """
    if _PSG_ARTIFACT_QUERY_RE.search(query_text):
        n = int(np.asarray(cache["canonical"]).shape[0])
        return np.zeros(n, dtype=bool)
    if _PSG_NON_RESP_ANALYTE_RE.search(query_text) and \
            _PSG_STAGE_BREAKDOWN_RE.search(query_text):
        if not _SLEEP_CONTEXT_RE.search(query_text):
            return None
        n = int(np.asarray(cache["canonical"]).shape[0])
        return np.zeros(n, dtype=bool)
    return None


def should_force_null_loinc(query_text: str) -> bool:
    """Return True when *query_text* asks for a sleep-study concept
    LOINC truly doesn't enumerate, so callers should yield no LOINC
    code rather than fall onto a misleading nearby row.

    Currently covers:
      - PSG no-LOINC region:
        - PLM count / PLM index / PLMS-by-stage breakdowns
        - Body posture during sleep (left/right/supine/prone duration)
        - Sleep hypoventilation (≠ hypopnea, no LOINC code exists)
        - Flow-limitation / UARS events
        - Microarousal sub-types with non-RERA cause (desat / HR /
          snore / PLM / spontaneous / generic ``total``) — LOINC only
          encodes RERA (90557-0 / 90565-3), so other causes have no
          code. License markers (``RERA``, ``呼吸努力相关``) re-enable
          the family.
      - Chemical immune-reactivity haptens with no LOINC enumeration
        (BPA / TBBPA / BPA-BP / tetrachloroethylene / parabens / mixed
        heavy metals / isocyanate IgG-or-IgA). See
        :func:`_chemical_irs_no_loinc_null`.

    Mirror of the force-null branches inside :func:`_sleep_analyte_keep`
    and :func:`_chemical_irs_no_loinc_null`, exposed for the per-axis
    vocab pipeline (:mod:`..embeddings.axes`) which doesn't share the
    legacy row-level filter chain.
    """
    if _CHEMICAL_IMMUNE_CONTEXT_RE.search(query_text) and \
            _CHEMICAL_IRS_NO_LOINC_HAPTEN_RE.search(query_text):
        return True
    if _CONCEPT_NO_LOINC_QUERY_RE.search(query_text):
        return True
    if not _SLEEP_CONTEXT_RE.search(query_text):
        return False
    if _SLEEP_LICENSE_RE.search(query_text):
        return False
    # PSG signal artifact — no LOINC code for ``artifact duration during
    # sleep stage``. Cosine drifts to Sleep duration / Polysomnography
    # panel, both clinically meaningless for artifact tracking.
    if _PSG_ARTIFACT_QUERY_RE.search(query_text):
        return True
    # Stage-stratified non-respiratory physiologic measurements (CO2 /
    # HR / temperature / BP broken down by REM / non-REM / N1-N4).
    # LOINC has the analyte rows and the sleep-stage duration rows
    # separately but no combined ``X during stage Y`` codes.
    if _PSG_NON_RESP_ANALYTE_RE.search(query_text) and \
            _PSG_STAGE_BREAKDOWN_RE.search(query_text):
        return True
    qbits = 0
    for bit, rx in _PSG_QUERY_RES:
        if rx.search(query_text):
            qbits |= bit
    if qbits == 0:
        return False
    effective_no_loinc_bits = _PSG_NO_LOINC_FAMILY_BITS
    if (qbits & _AROUSAL_FAMILY_BIT) and _AROUSAL_CAUSE_NON_RERA_RE.search(query_text):
        if not _AROUSAL_RERA_LICENSE_RE.search(query_text):
            # The cause tokens (desat / HR / snore / PLM) are modifiers
            # of arousal, not primary topics. Strip the cause family
            # bits from qbits so the force-null gate sees ``arousal-
            # only`` and triggers. Otherwise ``微觉醒·血氧下降相关``
            # leaks via the DESATURATION bit treated as a real family.
            qbits &= ~(_AROUSAL_CAUSE_FAMILY_BITS)
            effective_no_loinc_bits |= _AROUSAL_FAMILY_BIT
    return (qbits & ~effective_no_loinc_bits) == 0


def _sleep_analyte_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """When a sleep-context query names a specific PSG analyte (PLM /
    apnea / hypopnea / oxygen desaturation / respiration rate / body
    posture / snoring / arousal), require the LOINC row's LCN to
    carry the same analyte family — or carry no analyte at all (and
    not be a skeleton row). Drop rows whose LCN carries a *different*
    PSG analyte family, and rows that are pure sleep-time skeleton.

    Force-null when the only matched family is one LOINC truly can't
    serve (PLM, sleep posture — see :data:`_PSG_NO_LOINC_FAMILY_BITS`).
    Cosine fallbacks in that regime land on ``Restless sleep`` /
    ``Polysomnography panel`` / ``Left contact lens Axis.DS``, all of
    which mislead more than null.

    Snore-family in-split refinement: when the query carries an explicit
    duration vs episode-count marker, drop the opposite half of
    103220-0 / 103221-8 (Duration of snoring / Number of snoring
    episodes). Other PSG families don't have this dichotomy.

    Multilingual via :data:`_PSG_ANALYTE_FAMILIES` per-family query
    regexes (CN/EN/JA/KO/DE/FR/ES/RU stems).

    Silent when:
      - query has no sleep-context marker.
      - query has no PSG-analyte token (passes through to
        :func:`_sleep_focus_keep`).
      - query explicitly names questionnaire / cardiology / imaging
        (license check shared with :func:`_sleep_focus_keep`).
    """
    if not _SLEEP_CONTEXT_RE.search(query_text):
        return None
    if _SLEEP_LICENSE_RE.search(query_text):
        return None
    qbits = 0
    for bit, rx in _PSG_QUERY_RES:
        if rx.search(query_text):
            qbits |= bit
    if qbits == 0:
        return None
    # Arousal cause sub-axis. The arousal family has LOINC codes only
    # for RERA (90557-0 / 90565-3). When the query carries a non-RERA
    # cause marker (desat / HR / snore / PLM / spontaneous / total
    # aggregate) AND no explicit RERA license, treat arousal as a
    # no-LOINC family for this query so it joins the force-null region.
    effective_no_loinc_bits = _PSG_NO_LOINC_FAMILY_BITS
    if (qbits & _AROUSAL_FAMILY_BIT) and _AROUSAL_CAUSE_NON_RERA_RE.search(query_text):
        if not _AROUSAL_RERA_LICENSE_RE.search(query_text):
            # The cause tokens (desat / HR / snore / PLM) are modifiers
            # of arousal, not primary topics. Strip the cause family
            # bits from qbits so the force-null gate sees ``arousal-
            # only`` and triggers. Otherwise ``微觉醒·血氧下降相关``
            # leaks via the DESATURATION bit treated as a real family.
            qbits &= ~(_AROUSAL_CAUSE_FAMILY_BITS)
            effective_no_loinc_bits |= _AROUSAL_FAMILY_BIT
    # Force-null: every fired family lives in the no-LOINC region
    # (PLM, body position, hypoventilation, flow-limitation, and
    # arousal-when-non-RERA-cause). When the query also names a
    # LOINC-covered family (e.g. ``肢动伴觉醒`` → leg_movement |
    # arousal, with no non-RERA cause marker), arousal can still
    # resolve.
    if qbits and (qbits & ~effective_no_loinc_bits) == 0:
        n = int(np.asarray(cache["canonical"]).shape[0])
        return np.zeros(n, dtype=bool)
    bits, skel = _build_psg_family_bits(cache)
    # Keep when:
    #   (a) the row's PSG bits are a SUBSET of the query's qbits —
    #       i.e. the row encodes *only* analyte families the query
    #       asked for. Subset (not overlap) is what drops combined
    #       ``Apnea + hypopnea`` / ``Obstructive apnea hypopnea
    #       index`` rows on apnea-only or hypopnea-only queries. The
    #       solo and combined-but-mentioned-by-query cases are
    #       unaffected (``AHI`` ⇒ qbits=APNEA|HYPOPNEA covers the
    #       combined row's bits).
    #   (b) row carries no PSG analyte AND isn't a sleep skeleton —
    #       these are generic non-PSG rows, let cosine decide.
    # Cast qbits to bits.dtype before bitwise NOT — numpy's uint16
    # rejects the Python-int sign-extended ~qbits otherwise. The mask
    # we want is "row carries only families the query asked for", so
    # ``bits & ~qbits_typed == 0`` excludes rows that carry any extra
    # PSG family (drops Apnea+hypopnea / OAHI / CAHI on solo apnea or
    # solo hypopnea queries).
    qbits_typed = bits.dtype.type(qbits) if qbits != 0 else np.zeros((), dtype=bits.dtype)
    keep = (bits != 0) & ((bits & ~qbits_typed) == 0)
    keep |= (bits == 0) & ~skel
    # Snore duration vs count refinement — only when snore is the
    # licensed family in this query. Asymmetric markers: when the
    # query has *one* polarity, drop the opposite-polarity rows.
    if qbits & _SNORE_FAMILY_BIT:
        has_dur = bool(_SNORE_DURATION_QUERY_RE.search(query_text))
        has_cnt = bool(_SNORE_COUNT_QUERY_RE.search(query_text))
        if has_dur ^ has_cnt:
            dur_mask, cnt_mask = _build_snore_split_masks(cache)
            keep &= ~(cnt_mask if has_dur else dur_mask)
    return keep


# Per-hour-rate routing. Indicator names like ``每小时尿钙排泄`` /
# ``每小时肌酐排泄`` ask for an *hourly* rate (TIME_ASPCT=1H or
# point-in-time mass/time). When the source category contains
# ``24小时尿液检测`` the augmented embedding cosine-anchors on
# multi-hour timed collections (``in 24 hour Urine`` / ``in 12 hour
# Urine`` / ``in 2 hour Urine``), beating the correct 1H sibling.
# Drop multi-hour LCNs when the indicator explicitly says per-hour.
_PER_HOUR_QUERY_RE = re.compile(
    r"每\s*小时|每\s*小時|每\s*1\s*小[时時]"
    r"|per\s+hour|hourly\b|per\s+1\s+hour|/\s*hour\b|/\s*hr\b"
    r"|1\s*時間ごと|時間ごと|時間あたり"      # JA
    r"|시간당|매\s*시간"                       # KO
    r"|por\s+hora"
    r"|pro\s+Stunde|st[üu]ndlich"
    r"|par\s+heure"
    r"|в\s+час",
    re.IGNORECASE,
)
# LOINC LCN substring that flags a multi-hour timed-collection
# specimen: ``in N hour(s) Urine / Stool / Sweat / Blood / Body
# fluid / Specimen``. Anchored on ``in`` to skip ``per hour`` /
# rate-expressed PROPERTY rows.
_MULTI_HOUR_COLLECTION_RE = re.compile(
    r"\bin\s+(?:2|3|4|5|6|8|10|12|18|24|48|72)\s+hours?\s+"
    r"(?:Urine|Stool|Sweat|Body\s+fluid|Specimen|Saliva|Bile)\b",
    re.IGNORECASE,
)


def _build_multi_hour_collection_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose LCN encodes a multi-hour timed-collection
    specimen (``in 2/4/12/24/... hour Urine`` and similar). Cached on
    *cache*."""
    cached = cache.get("_loinc_multi_hour_collection_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _MULTI_HOUR_COLLECTION_RE.search(nm):
            mask[i] = True
    log.info(
        "multi-hour collection mask: %d / %d rows flagged "
        "(in N hour Urine/Stool/Body fluid/Specimen)",
        int(mask.sum()), n,
    )
    cache["_loinc_multi_hour_collection_mask"] = mask
    return mask


def _per_hour_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """When the query carries an explicit ``每小时 / per hour /
    hourly`` marker, drop LOINC rows whose LCN encodes a multi-hour
    timed collection (``in 24 hour Urine``, ``in 12 hour Urine``,
    ...) so the picker prefers TIME_ASPCT=1H or point-in-time
    rate variants.

    Multilingual via :data:`_PER_HOUR_QUERY_RE`. Silent when no
    per-hour marker is present — multi-hour collections survive in
    all other contexts.
    """
    if not _PER_HOUR_QUERY_RE.search(query_text):
        return None
    return ~_build_multi_hour_collection_mask(cache)


# Source-prefix → specimen-family routing. The CSV benchmark feeds the
# pipeline ``{source},{indicator}`` joined queries, where ``source`` is
# a clinical-domain category that strongly implies specimen type:
# ``尿液`` → Urine, ``粪便`` → Stool, ``血生化 / 血常规 / 血气分析 /
# 血脂`` → Serum or Plasma or Blood, ``体液检查`` → body-fluid family.
# Gemini's embedding doesn't always respect this — C-peptide under
# ``糖尿病筛查`` cosine-picks the Urine variant, RBP under ``尿液 ·
# 随机尿液中的有机酸`` cosine-picks the Serum variant.
#
# Build a per-query keep mask that prefers specimen-matching LOINC rows
# AND retains non-specimen rows (vital signs, surveys, narrative —
# anything without an ``in <specimen>`` qualifier in its LCN). Add the
# filter to :data:`_TOPK_PROBE_FALLBACK_FILTERS` so a query whose
# indicator legitimately picks a different specimen (``血生化 · 尿肌酐
# 清除率``) falls back to plain cosine when the source-implied pool
# clears the top-10.
#
# Source-family detection is on the FIRST segment of the query (split
# on ``,`` — see ``run_resolve.py`` for the join format). The benchmark
# uses ``,`` as the join separator; the production v2 CLI does the same
# via the same join code path. Indicators consumed without a source
# prefix (no leading ``<category>,``) pass through silently.

# CN source-category → SYSTEM family. The right-hand side is a
# canonical-form key into :data:`_SPECIMEN_LCN_RES`; multiple sources
# can share one specimen pool (e.g. ``血生化 / 血常规 / 血脂`` all
# → ``serum_or_blood``).
#
# Conservatively chosen: sources whose indicators are *predominantly*
# bound to one specimen family. The picker also runs a top-K probe
# fallback (see :data:`_TOPK_PROBE_FALLBACK_FILTERS`) so legitimate
# cross-specimen analytes still resolve when LOINC's cosine top-10
# clearly lives in a different specimen pool.
#
# Sources NOT in this map even though they look specimen-typed:
#   - ``粪便`` (stool): stool indicators frequently borrow Body-fluid
#     codes (粪便·巨噬细胞 → Macrophages in Body fluid; CTCs in Blood);
#     LOINC's Stool coverage is thinner than the indicators imply.
#   - ``神经递质`` / ``营养`` / ``免疫类``: highly mixed specimens
#     (some serum, some urine, some CSF); a uniform prior over-fires.
_SOURCE_TO_SPECIMEN: dict[str, str] = {
    # Urine sections
    "尿液":               "urine",
    "尿常规":             "urine",
    "尿沉渣":             "urine",
    "尿液分析":           "urine",
    # Stool sections
    "大便常规":           "stool",
    "粪便常规":           "stool",
    "便常规":             "stool",
    "粪便":               "stool",
    # Blood / serum / plasma sections
    "血生化":             "serum_or_blood",
    "血常规":             "serum_or_blood",
    "血气分析":           "serum_or_blood",
    "血脂":               "serum_or_blood",
    "血糖":               "serum_or_blood",
    "血液专项检查":       "serum_or_blood",
    "糖尿病筛查":         "serum_or_blood",
    "肿瘤标志物":         "serum_or_blood",
    "激素":               "serum_or_blood",
    "甲状腺":             "serum_or_blood",
    "甲状腺功能":         "serum_or_blood",
    "炎性指标":           "serum_or_blood",
    "心脏指标":           "serum_or_blood",
    "凝血功能":           "serum_or_blood",
    "肾功能":             "serum_or_blood",
    "肝功能":             "serum_or_blood",
    "肝功能 A 组":        "serum_or_blood",
    "肝功能 B 组":        "serum_or_blood",
    "肝功能A组":          "serum_or_blood",
    "肝功能B组":          "serum_or_blood",
    "电解质":             "serum_or_blood",
    "心肌酶":             "serum_or_blood",
    "免疫":               "serum_or_blood",
    "免疫学":             "serum_or_blood",
    "免疫类":             "serum_or_blood",
    "风湿":               "serum_or_blood",
    "类风湿":             "serum_or_blood",
    # Nutrition labs are serum/plasma by clinical convention
    # (amino-acid / fatty-acid / electrophoresis panels). Urine
    # variants for these analytes are rare and carry an explicit
    # ``尿液`` marker in the indicator, picked up by the per-row
    # cosine even with this routing in place.
    "营养":               "serum_or_blood",
}

# Indicator-side specimen markers. Used when the source prefix doesn't
# carry specimen routing (``特殊实验室检查``, ``体液检查``, …) but the
# indicator itself names a specimen explicitly (``心包液检验·X``,
# ``2200 胃肠功能 - 粪便·Y``). Matched against the indicator portion
# (post-source-comma). First-match wins, in declaration order — put
# specific markers (``心包液``) before generic ones (``血``).
_INDICATOR_TO_SPECIMEN: tuple[tuple["re.Pattern[str]", str], ...] = (
    (re.compile(r"心包液|心包腔|pericardial\s+fluid"), "pericardial_fluid"),
    (re.compile(r"腹腔液|腹水|腹腔积液|peritoneal\s+fluid|ascit"), "peritoneal_fluid"),
    (re.compile(r"胸腔液|胸水|胸腔积液|pleural\s+fluid"), "pleural_fluid"),
    (re.compile(r"关节液|关节腔液|滑膜液|synovial\s+fluid"), "synovial_fluid"),
    (re.compile(r"脑脊液|cerebrospinal\s+fluid|\bCSF\b"), "csf"),
    (re.compile(r"羊水|amniotic\s+fluid"), "amniotic_fluid"),
    (re.compile(r"精液\b|semen\b"), "semen"),
    (re.compile(r"粪便|大便|stool\b"), "stool"),
    (re.compile(r"唾液|saliva"), "saliva"),
    (re.compile(r"汗液|\bsweat\b"), "sweat"),
    (re.compile(r"阴道分泌物|阴道涂片|阴道(?:液|样)|vaginal\s+(?:fluid|discharge|secretion)"),
     "vaginal_fluid"),
    # Amino-acid / fatty-acid / metabolite panels (Genova / Doctor's
    # Data / Cyrex style) are serum or plasma by clinical convention.
    # The ``特殊实验室检查`` source covers many specimens so doesn't
    # route; the indicator-side panel name is the only specimen hint.
    # Anchored to the explicit panel markers — bare ``氨基酸`` would
    # mis-fire on urinary amino-acid screens (which carry ``尿`` /
    # ``urine`` markers picked up by the ``_RANDOM_URINE_QUERY_RE``).
    (re.compile(
        r"氨基酸和代谢物|氨基酸與代謝物|amino\s+acids?\s+and\s+metabolites?"
        r"|有机酸和代谢物|有機酸和代謝物|organic\s+acids?\s+and\s+metabolites?"
        r"|必需与代谢脂肪酸|必需與代謝脂肪酸|essential\s+(?:and|&)\s+metabolic\s+fatty\s+acids?",
    ), "serum_or_blood"),
    # Lymphocyte-MAP / lymphocyte-subset flow-cytometry panel — CD3 /
    # CD4 / CD8 / CD16 / CD19 / CD56 / Treg markers are clinically
    # measured on peripheral blood (the assay requires viable cells in
    # suspension). ``特殊实验室检查`` source covers many specimens so
    # doesn't route; without indicator-side anchoring, the picker
    # leaks Body fluid / Bone marrow / Specimen-generic rows (LOINC
    # carries 18266-7 CD4/CD8 Body fluid, 32534-0 CD8 Bone marrow,
    # 32533-2 T-helper Bone marrow, 80222-3 CD8 Specimen as siblings).
    # Routing to ``serum_or_blood`` confines the candidate pool to the
    # canonical Blood variants (54218-3 / 14135-8 / 8101-8). Anchored
    # on the panel-name patterns to avoid mis-firing on stand-alone
    # mentions in unrelated indicators.
    (re.compile(
        r"淋巴细胞图谱|淋巴細胞圖譜"
        r"|淋巴细胞亚群|淋巴細胞亞群"
        r"|lymphocyte\s+(?:map|subsets?|profile|panel)",
        re.IGNORECASE,
    ), "serum_or_blood"),
)

# Specimen-family LCN regexes for the indicator-side detector. Single
# specimen per family (no multi-fluid sites), so simpler than the
# source-routed urine/stool/serum_or_blood patterns.
_INDICATOR_SPECIMEN_LCN_RES: dict[str, "re.Pattern[str]"] = {
    "pericardial_fluid": re.compile(r"\bin\s+Pericardial\s+(?:fluid|fld)\b", re.IGNORECASE),
    "peritoneal_fluid":  re.compile(r"\bin\s+Peritoneal\s+(?:fluid|fld)\b|\bin\s+Ascites\b", re.IGNORECASE),
    "pleural_fluid":     re.compile(r"\bin\s+Pleural\s+(?:fluid|fld)\b", re.IGNORECASE),
    "synovial_fluid":    re.compile(r"\bin\s+Synovial\s+(?:fluid|fld)\b", re.IGNORECASE),
    "csf":               re.compile(r"\bin\s+(?:Cerebral\s+spinal\s+fluid|CSF)\b", re.IGNORECASE),
    "amniotic_fluid":    re.compile(r"\bin\s+Amniotic\s+(?:fluid|fld)\b", re.IGNORECASE),
    "semen":             re.compile(r"\bin\s+Semen\b", re.IGNORECASE),
    "saliva":            re.compile(r"\bin\s+Saliva\b", re.IGNORECASE),
    "sweat":             re.compile(r"\bin\s+Sweat\b", re.IGNORECASE),
    "vaginal_fluid":     re.compile(r"\bin\s+(?:Vaginal\s+(?:fluid|fld)|Cervix(?:/Vag)?|Cvx/Vag)\b", re.IGNORECASE),
}

# Random-urine refinement: when the indicator explicitly says ``随机
# 尿液`` / ``random urine`` / ``尿液(随机)``, drop multi-hour timed
# collection rows (``in 24 hour Urine`` etc.) within the urine family.
# Reuses ``_MULTI_HOUR_COLLECTION_RE`` for the LCN check.
_RANDOM_URINE_QUERY_RE = re.compile(
    r"随机尿|spot\s+urine|random\s+urine"
    r"|尿液(?:\s*\(|（)\s*随机",
    re.IGNORECASE,
)

# Specimen-family → LCN regex matching the ``in <specimen>`` qualifier.
# ``in N hour <specimen>`` variants are caught by the same patterns —
# the LCN word boundary is on the specimen token, not the time prefix.
_SPECIMEN_LCN_RES: dict[str, "re.Pattern[str]"] = {
    "urine": re.compile(
        r"\bin\s+(?:\d+\s+hour\s+)?Urine\b"
        r"|\bin\s+Urine\b|\bin\s+Random\s+urine\b"
        r"|\bin\s+Urine\s+sediment\b",
        re.IGNORECASE,
    ),
    "stool": re.compile(
        r"\bin\s+Stool\b|\bin\s+\d+\s+hour\s+Stool\b",
        re.IGNORECASE,
    ),
    "serum_or_blood": re.compile(
        # Serum / Plasma / Blood family — both the canonical "Serum or
        # Plasma" and the bare individual forms, plus DBS (dried blood
        # spot), whole blood, capillary blood, arterial blood, etc.
        # Includes the specialized blood preparations clinically used
        # for specific lab families (Platelet poor plasma for
        # coagulation; RBC lysate for hemoglobinopathies / membrane
        # studies; mixed-venous / central-venous for ICU panels).
        # Without these, routine routing (``凝血功能`` →
        # serum_or_blood) drops the canonical PPP rows for INR / PT /
        # aPTT / fibrinogen and falls onto less-canonical Blood
        # variants.
        r"\bin\s+(?:Serum|Plasma|Blood|Serum\s+or\s+Plasma"
        r"|Serum,\s+Plasma\s+or\s+Blood"
        r"|Serum\s+or\s+Plasma\s+or\s+Blood"
        r"|Arterial\s+blood|Venous\s+blood|Capillary\s+blood"
        r"|Whole\s+blood|Dried\s+blood\s+spot|Red\s+Blood\s+Cells"
        r"|RBC\.lysate|RBC\b|Platelets|Mixed\s+venous\s+blood"
        r"|Platelet\s+poor\s+plasma|Central\s+venous\s+blood"
        r"|Cord\s+blood|Cord\s+arterial\s+blood|Cord\s+venous\s+blood)\b",
        re.IGNORECASE,
    ),
}

# Any-specimen detector — used to decide which rows carry NO specimen
# qualifier (vital signs, narrative, surveys, etc.). Rows without an
# ``in <something>`` clause are always kept regardless of source family.
_ANY_SPECIMEN_LCN_RE = re.compile(
    r"\bin\s+(?:\d+\s+hour\s+)?"
    r"(?:Urine|Random\s+urine|Stool|Serum|Plasma|Blood|Serum\s+or\s+Plasma"
    r"|Whole\s+blood|Arterial\s+blood|Venous\s+blood|Capillary\s+blood"
    r"|Dried\s+blood\s+spot|Red\s+Blood\s+Cells|Body\s+fluid"
    r"|Cerebral\s+spinal\s+fluid|CSF|Saliva|Semen|Sweat|Tissue"
    r"|Bile|Sputum|Bronchoalveolar|Synovial\s+fluid|Pericardial\s+fluid"
    r"|Pleural\s+fluid|Peritoneal\s+fluid|Amniotic\s+fluid"
    r"|Bone\s+marrow|Urine\s+sediment|Isolate"
    # Variant blood specimens — LOINC distinguishes RBC lysate, platelet-
    # poor plasma, mixed-venous blood, central-venous blood, etc.
    # Treating them as ``no specimen`` lets cosine surface unrelated-
    # specimen rows on source-routed queries (``营养·脂肪酸谱·甲酸``
    # picking 99621-5 in RBC.lysate over 2292-1 in Ser/Plas).
    r"|RBC\.lysate|RBC|WBC|Platelet\s+poor\s+plasma|Mixed\s+venous\s+blood"
    r"|Central\s+venous\s+blood|Cord\s+blood|Cord\s+arterial\s+blood"
    r"|Cord\s+venous\s+blood|Vitreous\s+fluid"
    # Food / culture media — LOINC stores select microbiology codes
    # in Milk / Food / Water / Air / Dialysis fluid. These are
    # specific specimens (NOT ``no specimen``) so source-routed
    # queries (``粪便·X``) correctly drop them.
    r"|Milk|Food|Water|Air|Dialysis\s+fluid|Dial\s+fld"
    # Stool-routed queries (``粪便·胆固醇``, ``粪便·粪肠球菌``,
    # ``粪便·芽孢杆菌``) previously leaked Cholesterol-in-DBS,
    # E. faecalis-in-Positive-blood-culture, Bacillus-in-Duodenal-fluid
    # rows because these specimens fell through the ``no specimen``
    # branch of :func:`_build_specimen_keep_mask`. ``Dried blood spot``
    # is already covered above; this row adds the abbreviation form
    # (``DBS``) plus Duodenal-fluid and Blood-culture variants.
    r"|DBS|Duodenal\s+(?:fluid|fld)"
    r"|Positive\s+blood\s+culture|Negative\s+blood\s+culture"
    r"|Blood\s+culture"
    # Genital-tract specimens — Vaginal fluid / Cervix / Cvx/Vag
    # composite. Vaginal-discharge queries (``阴道分泌物``) need to
    # filter Blood-specimen rows out of the candidate set.
    r"|Vaginal\s+(?:fluid|fld)|Cervix(?:/Vag)?|Cvx/Vag"
    # Bare ``Specimen`` qualifier — LOINC uses ``in Specimen`` as a
    # specimen-agnostic catchall (e.g. ``CD8 cells/Lymphocytes in
    # Specimen`` 80222-3, sibling of the Blood-specific ``CD3+CD8+
    # cells/cells in Blood`` 8101-8). Treating ``in Specimen`` as
    # "no specimen" silently leaks the generic row past source-routed
    # specimen filters, beating the canonical Blood/Serum variant on
    # raw cosine. Counting it as a specimen marker drops it on
    # routed queries while leaving un-routed queries unaffected.
    r"|Specimen)\b",
    re.IGNORECASE,
)

# Source-prefix split pattern — accept ``,`` (benchmark/v2-CLI join
# separator) and ``，`` (Chinese full-width comma) at position 0 only.
_SOURCE_PREFIX_RE = re.compile(r"^\s*([^,，\n]+)[,，]")


def _build_specimen_keep_mask(
    cache: dict, specimen_family: str,
) -> "np.ndarray | None":
    """Return a keep-mask matching LOINC rows whose LCN carries an
    ``in <specimen>`` qualifier compatible with *specimen_family*, OR
    rows with no specimen qualifier at all. Cached on *cache* keyed by
    *specimen_family*.

    No analyte-family fallback. The filter is wired into
    :data:`_TOPK_PROBE_FALLBACK_FILTERS`, so a query whose cosine
    top-10 is entirely outside the source-implied specimen pool (e.g.
    NMP-22 under ``肿瘤标志物`` — LOINC only stores it in Urine) skips
    the filter and falls back to plain cosine.
    """
    key = f"_loinc_specimen_keep_{specimen_family}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    pat = _SPECIMEN_LCN_RES.get(specimen_family)
    if pat is None:
        cache[key] = None
        return None
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    keep = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if not nm:
            keep[i] = True
            continue
        if pat.search(nm):
            keep[i] = True
        elif not _ANY_SPECIMEN_LCN_RE.search(nm):
            # No specimen qualifier — vital signs, surveys, narrative,
            # etc. Always reachable regardless of source-family hint.
            keep[i] = True
    cache[key] = keep
    return keep


# Role-identifier leaf patterns. When the indicator (post-source-prefix)
# is *entirely* a role / team / examiner label, the picker routinely
# surfaces a NOTE-class LOINC code (``Attending Outpatient Note``,
# ``Emergency medicine team Conference note``) because the LCN tokens
# overlap with the query — but the query is really asking for the
# document's *author* or *care-team* field, not the note itself. LOINC
# has sparse identifier rows (52526-1 ``Attending physician name``,
# 22028-5 ``Physician [Identifier]``, ...) that the embedding can't
# reliably surface, so we force null and let the consumer treat this
# as "structural metadata, no measurement code".
#
# Matches whole-indicator: ``门诊报告,医生`` fires; ``急诊报告,急诊
# 记录·主管医生`` does NOT (the leaf is inside a multi-segment path,
# so the indicator is asking about a record section, not the role
# label itself).
_ROLE_IDENTIFIER_LEAF_RE = re.compile(
    # CN role / team / examiner labels
    r"^(?:"
    r"医生|医师|主任医生|主任医师|主管医生|主治医生|主治医师"
    r"|申请医生|开单医生|检查医生|执行医生|报告医生|审核医生"
    r"|检查者|操作者|报告者|审核者|签名者|签字医生|签字医师"
    r"|医疗团队|医护团队|护理团队|治疗团队|诊疗团队|医生团队"
    # EN equivalents
    r"|physician|doctor|attending(?:\s+physician)?|clinician"
    r"|examiner|reporter|operator|signer"
    r"|care\s+team|healthcare\s+team|medical\s+team|nursing\s+team"
    r"|treatment\s+team"
    r")$",
    re.IGNORECASE,
)


def _role_identifier_null(query_text: str, cache: dict) -> "np.ndarray | None":
    """Force-null for queries whose indicator is *entirely* a role /
    team / examiner identifier (``医生`` / ``医疗团队`` / ``care
    team`` / …). LOINC's note-class rows shadow these on cosine
    despite being a fundamentally different concept (the document
    vs the role attribute on the document).

    Indicator extraction: strip the leading ``<source>,`` (or full-
    width ``，``) prefix, then exact-match against
    :data:`_ROLE_IDENTIFIER_LEAF_RE`. Multi-segment indicators
    (``出院小结·一般信息-医生、出入院日期``) survive because the leaf
    isn't a stand-alone role label.
    """
    m = _SOURCE_PREFIX_RE.match(query_text)
    indicator = query_text[m.end():].strip() if m else query_text.strip()
    if not indicator:
        return None
    # Strip trailing ASCII / fullwidth zero-width punctuation that the
    # benchmark CSV sometimes carries (``医生​`` ends with U+200B ZWSP).
    indicator = indicator.strip("​‌‍﻿ \t")
    if not _ROLE_IDENTIFIER_LEAF_RE.match(indicator):
        return None
    n = int(np.asarray(cache["canonical"]).shape[0])
    return np.zeros(n, dtype=bool)


# Section-header demote — narrows the section-header pool for queries
# routed there by ``_is_section_header_term``. Two demote families,
# both gated on section-header context so they never affect plain
# measurement queries:
#
#   A. ``Family history of <Condition> [Narrative]`` rows shadow the
#      generic ``History of family member diseases`` when the query is
#      just ``家族史`` (no condition mentioned).
#
#   B. ``<Specialty>(\s+\w+){0,3} <DocType> Note/Summary`` rows shadow
#      generic concept rows when the query has no specialty marker.
#      ``评估/计划`` falls onto ``Multi-specialty program Outpatient
#      Progress note`` instead of ``Evaluation + Plan note``; ``药物``
#      falls onto ``Pharmacology Outpatient Note`` instead of generic
#      ``History of Medication use``.
#
#   C. ``Physical findings of <BodyPart> [Narrative]`` rows shadow
#      ``Physical findings of General status`` for plain ``体格检查``.
#
#   D. ``History of <Subtype>`` rows when query has no matching marker
#      — ``治疗史`` falling onto ``History of Outpatient visits``,
#      ``社会史·职业`` likewise.
_SECTION_DEMOTE_FAMILY_HISTORY_RE = re.compile(
    # ``Family history of <X>`` with X being any condition (the bare
    # ``Family history of <X> [Narrative]`` form). The generic
    # ``History of family member diseases`` doesn't match this regex.
    r"^Family history of (?!family member diseases\b).+",
    re.IGNORECASE,
)
_SECTION_DEMOTE_BODY_PART_PE_RE = re.compile(
    # ``Physical findings`` followed by anything BUT ``of General
    # status``. Covers both ``Physical findings of <body part>
    # Narrative`` (10191-5 ... 10215-2 family) and the DEEDS variant
    # ``Physical findings - DEEDS 1.0 <bodypart>`` (69300-2 family)
    # — both shadow ``Physical findings of General status Narrative``
    # for plain ``体格检查`` queries.
    r"^Physical findings(?!\s+of\s+General status\b).+",
    re.IGNORECASE,
)
_SECTION_DEMOTE_SCENARIO_NARRATIVE_RE = re.compile(
    # Pre-hospital EMS (NEMSIS), disaster/emergency-response, and
    # other scenario-narrative LCN patterns that share the section-
    # header pool with generic concept rows. Cosine for ``急诊报告 ·
    # 急诊记录·系统回顾 / 病史·家族史 / 评估总结`` drifts to these
    # scenario rows (``EMS past medical history NEMSIS`` / ``Mission
    # related to the emergency response activation`` / ``Emergency
    # response plan problem analysis``) because the source ``急诊``
    # tokens overlap. Demote them so the picker falls back to the
    # generic narrative concepts already in the pool (Review of
    # systems / History of family member diseases / Evaluation +
    # Plan note).
    r"^EMS\b"
    r"|^Mission related to\b"
    r"|^Emergency response plan\b"
    r"|^Emergency operations\b"
    r"|^Disaster\b",
)
_SECTION_DEMOTE_HISTORY_SUBTYPE_RE = re.compile(
    # ``History of <Subtype>`` rows where Subtype is a specific concept.
    # The truly generic anchors (``family member diseases`` / ``Past
    # illness`` / ``Present illness``) survive; specific subtypes that
    # the query didn't ask for (``Childhood diseases`` / ``Outpatient
    # visits`` / ``Surgical procedures of cardiovascular system``) get
    # demoted.
    r"^History of (?!"
    r"family member diseases\b|Past illness\b|Present illness\b"
    r"|Immunization\b|Allergies\b|allergies\b|Medication use\b"
    r"|Surgical procedures(?: Narrative| note)?$"
    r"|Tobacco use\b|Alcohol use\b|Blood transfusion\b"
    r"|Occupation\b|Outpatient visits\b"
    r").+",
    re.IGNORECASE,
)


def _build_section_demote_mask(cache: dict) -> np.ndarray:
    """LOINC rows that should fall behind a generic section-concept peer
    when the query is routed by ``_is_section_header_term`` and carries
    no licensing marker. See the four ``_SECTION_DEMOTE_*_RE`` regexes
    above for the exact patterns and rationale.

    Cached on *cache* under ``_loinc_section_demote_mask``.
    """
    cached = cache.get("_loinc_section_demote_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if not nm:
            continue
        if (
            _SECTION_DEMOTE_FAMILY_HISTORY_RE.search(nm)
            or _SECTION_DEMOTE_BODY_PART_PE_RE.search(nm)
            or _SECTION_DEMOTE_HISTORY_SUBTYPE_RE.search(nm)
            or _SECTION_DEMOTE_SCENARIO_NARRATIVE_RE.search(nm)
        ):
            mask[i] = True
    cache["_loinc_section_demote_mask"] = mask
    return mask


# CN tokens that license a specialty-prefixed Note row (the indicator
# explicitly names the specialty). When present, the demote stays
# silent — keeps the specialty note in play. Empty for now; users add
# to this set when they need a specific specialty routing.
_SECTION_DEMOTE_QUERY_LICENSE_RE = re.compile(
    r"癌|肿瘤|cancer|tumor"        # licenses Family history of Cancer
    r"|心脏|cardio|heart"           # licenses Family history of Heart
    r"|肺|lung|pulmonary"
    r"|脑|brain|neuro"
    r"|肝|liver|hepatic"
    r"|肾|kidney|renal"
    r"|糖尿病|diabetes"
    r"|高血压|hypertension"
    r"|childhood|儿童|儿科|pediatric"
    r"|药学|pharmacology|药剂"
    r"|麻醉|anesthesi"
    r"|眼|ophthalm|eye"
    r"|皮肤|dermatolog|skin"
    r"|精神|psychiatr|mental"
    r"|外科|surger"
    r"|妇产|obstetric|gynecolog",
    re.IGNORECASE,
)


def _section_demote_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """When the query routes to section_header (last-segment is a
    multilingual section phrase) AND carries no specialty/condition
    licensing marker, drop the over-specialized section rows in
    :func:`_build_section_demote_mask`. Otherwise silent.
    """
    from .category import _is_section_header_term
    if not _is_section_header_term(query_text):
        return None
    if _SECTION_DEMOTE_QUERY_LICENSE_RE.search(query_text):
        return None
    return ~_build_section_demote_mask(cache)


# ``Specimen X acceptable`` QC rows (CLASS=SPEC) — 3 rows in 2.82:
# 58400-3 Specimen pH acceptable, 19150-2 Specimen creatinine acceptable,
# 19152-8 Specimen specific gravity acceptable. These flag whether a
# specimen passed pre-analytical QC, not the analyte value itself. Cosine
# shadows the real analyte code when the query carries the analyte name
# in a urinalysis context (``尿常规·PH值`` lands on 58400-3 instead of
# 2756-5/5803-2). Drop them whenever the query lacks a QC/acceptability
# marker.
_SPECIMEN_ACCEPTABLE_LCN_RE = re.compile(
    r"^Specimen\s+\S.*\bacceptable\b", re.IGNORECASE,
)
_SPECIMEN_ACCEPTABLE_QUERY_LICENSE_RE = re.compile(
    r"标本质量|标本.{0,5}合格|质量评估|前分析|预分析"
    r"|specimen.{0,15}(?:acceptab|adequac|quality|integrit)"
    r"|sample.{0,15}(?:acceptab|adequac|quality|integrit)"
    r"|pre[- ]?analytic",
    re.IGNORECASE,
)


def _build_specimen_acceptable_mask(cache: dict) -> np.ndarray:
    cached = cache.get("_loinc_specimen_acceptable_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _SPECIMEN_ACCEPTABLE_LCN_RE.search(nm):
            mask[i] = True
    log.info(
        "specimen-acceptable mask: %d / %d LOINC rows flagged "
        "(``Specimen X acceptable`` QC rows)",
        int(mask.sum()), n,
    )
    cache["_loinc_specimen_acceptable_mask"] = mask
    return mask


def _specimen_acceptable_demote_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop ``Specimen X acceptable`` QC rows for queries that don't
    carry a specimen-quality / acceptability marker. Silent when the
    query explicitly asks for QC."""
    if _SPECIMEN_ACCEPTABLE_QUERY_LICENSE_RE.search(query_text):
        return None
    return ~_build_specimen_acceptable_mask(cache)


# CLASS=DOC.ONTOLOGY rows (~3600 in 2.82) — clinical-encounter notes,
# reports, summaries, evaluations, attestation letters. Cosine routinely
# shadows real physical-exam findings (H&P.PX) / clinical observations
# (EYE.PX / SURVEY.ESRD) when the query is a bare anatomy / specialty
# token (``内科·肺`` → 83787-2 Pulmonary Attending Note, ``耳鼻喉科·咽喉``
# → 78649-1 Otolaryngology Initial evaluation note). Drop them whenever
# the query lacks a documentation marker (note / report / summary /
# 记录 / 报告 / 小结 / 病案 / 病程 / etc.). When the entire top-K is
# DOC.ONTOLOGY (no physical-finding code exists for the body part),
# top-K probe lets the row through.
_DOC_NOTE_QUERY_LICENSE_RE = re.compile(
    r"记录|报告|小结|摘要|病案|病程|病历|住院志|出院"
    r"|抄送|证明书|证明信|签字|签名|文档|文书|备注|说明"
    r"|阅片|读片|审核|签发|主诉|现病史|既往史"
    r"|note|notes|report|reports|summary|summaries|narrative"
    r"|consult(?:ation)?|letter|attestation|dictation|transcript"
    r"|discharge|admission|progress|attending|consult|referral"
    r"|certificate|signed?\b|authorized?\b|chart\b|record\b"
    r"|history\s+and\s+physical|h&p\b",
    re.IGNORECASE,
)


def _build_doc_ontology_mask(cache: dict) -> np.ndarray:
    """Tag rows whose CLASS axis == ``DOC.ONTOLOGY``. Cached."""
    cached = cache.get("_loinc_doc_ontology_class_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_loinc_doc_ontology_class_mask"] = mask
        return mask
    class_values: list[str] = axis_data["values"].get("CLASS") or []
    class_ridx: np.ndarray | None = axis_data["row_value_idx"].get("CLASS")
    if class_ridx is None:
        cache["_loinc_doc_ontology_class_mask"] = mask
        return mask
    doc_idxs = {
        i for i, v in enumerate(class_values)
        if v and v.upper() == "DOC.ONTOLOGY"
    }
    if not doc_idxs:
        cache["_loinc_doc_ontology_class_mask"] = mask
        return mask
    for r in range(min(n, len(class_ridx))):
        ci = int(class_ridx[r])
        if ci >= 0 and ci in doc_idxs:
            mask[r] = True
    log.info(
        "doc-ontology class mask: %d / %d LOINC rows flagged "
        "(CLASS == DOC.ONTOLOGY)",
        int(mask.sum()), n,
    )
    cache["_loinc_doc_ontology_class_mask"] = mask
    return mask


def _doc_ontology_demote_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop CLASS=DOC.ONTOLOGY note/report rows when the query carries
    no documentation marker. Wired into
    :data:`_TOPK_PROBE_FALLBACK_FILTERS` so queries whose body part has
    no physical-finding LOINC code (and only documentation rows match
    in cosine top-10) fall through to plain cosine instead of nulling.
    """
    if _DOC_NOTE_QUERY_LICENSE_RE.search(query_text):
        return None
    return ~_build_doc_ontology_mask(cache)


# HDL / LDL routing — bare CN `高密度脂蛋白` / `低密度脂蛋白` (without
# `胆固醇`) in clinical lab reports universally means HDL-C / LDL-C
# (the cholesterol fraction). Cosine for the bare form drifts to two
# wrong families:
#   (a) 14813-0 / 14814-8 ``Lipoprotein.alpha / Lipoprotein.beta by
#       Electrophoresis`` — historical α-/β-lipoprotein naming (≡ HDL /
#       LDL by particle mobility), valid synonyms but deprecated method.
#   (b) 2576-7 ``Lipoproteins [Mass/volume] in Serum or Plasma`` and
#       17085-2 ``Lipoproteins [Presence]`` — generic, no fraction
#       specifier.
#   (c) 20510-4 / 49280-1 ``Lipoprotein fractions by Electrophoresis``
#       — interpretation pattern, not a measurement.
# Demote all three families when query carries a density specifier
# (``高密度`` / ``低密度`` / ``HDL`` / ``LDL``) but no electrophoresis
# license. Avoid a bare ``高密度脂蛋白`` alias (substring-matches and
# regresses ``氧化/直接/颗粒数低密度脂蛋白`` + ``胆固醇/HDL`` ratio
# orientation).
_LIPOPROTEIN_NONSPECIFIC_LCN_RE = re.compile(
    # Historical α-/β-/pre-β-lipoprotein names ONLY — exclude the modern
    # ``.subparticle`` family (17782-4 / 54434-6 LDL-P count, 92712-9…
    # large-/small-/very-small subclass counts). Those are NMR / ion-
    # mobility particle counts, a distinct concept from electrophoresis
    # fractions and the right target for `低密度脂蛋白颗粒数`.
    r"^Lipoprotein\.(?:alpha|beta|pre[- ]?beta)\b(?!\.subparticle)"
    r"|^Lipoproteins?\s+(?:\[|fractions?\b)"
    # ``Cholesterol in (HDL|LDL|VLDL) … by Electrophoresis`` — modern
    # HDL-C / LDL-C codes also have an electrophoresis-method variant
    # (12772-0, 49130-8) that cosine-shadows the direct/calculation
    # canonical (2085-9, 39469-2) for bare CN density queries.
    r"|^Cholesterol\s+in\s+(?:HDL|LDL|VLDL)\b.{0,80}\bby\s+Electrophoresis\b",
    re.IGNORECASE,
)
_HDL_LDL_DENSITY_QUERY_RE = re.compile(
    r"高密度脂蛋白|低密度脂蛋白|极低密度脂蛋白"
    r"|高\s*密\s*度\s*脂\s*蛋\s*白|低\s*密\s*度\s*脂\s*蛋\s*白"
    r"|\bHDL\b|\bLDL\b|\bVLDL\b"
    r"|high\s+density\s+lipoprotein|low\s+density\s+lipoprotein",
    re.IGNORECASE,
)
_LIPOPROTEIN_ELECTROPHORESIS_QUERY_LICENSE_RE = re.compile(
    r"电泳|電泳|electrophoresis"
    r"|α\s*[- ]?\s*脂蛋白|alpha\s*[- ]?\s*lipoprotein|Lipoprotein\.alpha"
    r"|β\s*[- ]?\s*脂蛋白|beta\s*[- ]?\s*lipoprotein|Lipoprotein\.beta"
    r"|前\s*β\s*脂蛋白|pre[- ]?beta\s*lipoprotein|Lipoprotein\.pre[- ]?beta"
    r"|脂蛋白\s*(?:图谱|分型|分布|条带|带型|pattern|fractions?|profile|分级)",
    re.IGNORECASE,
)


def _build_lipoprotein_nonspecific_mask(cache: dict) -> np.ndarray:
    cached = cache.get("_loinc_lipoprotein_nonspecific_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _LIPOPROTEIN_NONSPECIFIC_LCN_RE.search(nm):
            mask[i] = True
    log.info(
        "lipoprotein non-specific mask: %d / %d LOINC rows flagged "
        "(α-/β-/pre-β-lipoprotein + generic Lipoproteins + fractions)",
        int(mask.sum()), n,
    )
    cache["_loinc_lipoprotein_nonspecific_mask"] = mask
    return mask


def _lipoprotein_nonspecific_demote_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop ``Lipoprotein.{alpha,beta,pre-beta}`` / generic
    ``Lipoproteins`` / ``Lipoprotein fractions`` rows when the query
    carries an HDL/LDL density specifier but no electrophoresis
    license. Otherwise silent."""
    if not _HDL_LDL_DENSITY_QUERY_RE.search(query_text):
        return None
    if _LIPOPROTEIN_ELECTROPHORESIS_QUERY_LICENSE_RE.search(query_text):
        return None
    return ~_build_lipoprotein_nonspecific_mask(cache)


def _build_indicator_specimen_keep_mask(
    cache: dict, specimen_family: str,
) -> "np.ndarray | None":
    """Indicator-side specimen keep-mask. Mirror of
    :func:`_build_specimen_keep_mask` for the
    :data:`_INDICATOR_SPECIMEN_LCN_RES` body-fluid families
    (Pericardial / Peritoneal / Pleural / Synovial / CSF / Semen /
    Saliva / Sweat / Amniotic). Cached on *cache* keyed by family.
    """
    key = f"_loinc_indicator_specimen_keep_{specimen_family}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    pat = _INDICATOR_SPECIMEN_LCN_RES.get(specimen_family)
    if pat is None:
        cache[key] = None
        return None
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    keep = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if not nm:
            keep[i] = True
            continue
        if pat.search(nm):
            keep[i] = True
        elif not _ANY_SPECIMEN_LCN_RE.search(nm):
            keep[i] = True
    cache[key] = keep
    return keep


def _build_multi_hour_urine_drop_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose LCN encodes a multi-hour timed Urine
    collection (``in 2/4/12/24 hour Urine``). Used to drop these from
    random-urine queries within the urine family. Cached on *cache*.
    """
    cached = cache.get("_loinc_multi_hour_urine_drop_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    pat = re.compile(
        r"\bin\s+(?:2|3|4|5|6|8|10|12|18|24|48|72)\s+hours?\s+Urine\b",
        re.IGNORECASE,
    )
    for i, nm in enumerate(names):
        if nm and pat.search(nm):
            mask[i] = True
    cache["_loinc_multi_hour_urine_drop_mask"] = mask
    return mask


def _source_specimen_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Source-prefix → specimen-family LOINC keep mask, with indicator-
    side specimen fallback.

    Two-stage routing:
      1. Source prefix maps to a high-volume specimen family (urine /
         stool / serum_or_blood) via :data:`_SOURCE_TO_SPECIMEN`.
      2. Indicator text carries an explicit body-fluid marker (``心包
         液 / 腹水 / 胸水 / 关节液 / 脑脊液 / 精液 / 唾液 / 汗液 /
         羊水 / 粪便``) via :data:`_INDICATOR_TO_SPECIMEN`. Fires
         when the source has no mapping (``特殊实验室检查``, ``体液
         检查``) or when the indicator-side marker is more specific.

    Random-urine refinement: within the urine family, when the
    indicator explicitly says ``随机尿`` / ``spot urine``, drop the
    multi-hour timed Urine collection rows (``in 24 hour Urine`` etc.)
    so the point-in-time random-urine code (``in Urine``) wins.

    Non-specimen rows (no ``in X`` clause) survive unconditionally.
    Wired into :data:`_TOPK_PROBE_FALLBACK_FILTERS` so legitimate
    cross-specimen indicators (``血生化 · 24h 尿肌酐清除率``) fall
    back to plain cosine — the indicator's own specimen tokens already
    drag cosine into the correct pool.
    """
    # Indicator-side specimen detection: scan the post-source portion.
    m = _SOURCE_PREFIX_RE.match(query_text)
    if m:
        indicator_text = query_text[m.end():]
        source = m.group(1).strip()
    else:
        indicator_text = query_text
        source = ""
    for ind_re, fam in _INDICATOR_TO_SPECIMEN:
        if ind_re.search(indicator_text):
            # Shared families (``stool`` / ``urine`` / ``serum_or_blood``)
            # already have richer LCN patterns in ``_SPECIMEN_LCN_RES``;
            # delegate to the source-routed builder for those. Body-fluid
            # families live in ``_INDICATOR_SPECIMEN_LCN_RES``.
            if fam in _SPECIMEN_LCN_RES:
                mask = _build_specimen_keep_mask(cache, fam)
            else:
                mask = _build_indicator_specimen_keep_mask(cache, fam)
            if mask is not None:
                return mask
    # Fall back to source-based routing.
    family = _SOURCE_TO_SPECIMEN.get(source)
    if family is None:
        return None
    keep = _build_specimen_keep_mask(cache, family)
    if keep is None:
        return None
    # Random-urine refinement: drop multi-hour Urine collection rows.
    if family == "urine" and _RANDOM_URINE_QUERY_RE.search(indicator_text):
        keep = keep & ~_build_multi_hour_urine_drop_mask(cache)
    return keep


# Context markers that *require* strict Ig-class matching — chemical,
# heavy-metal, mold, and inhalant immune-reactivity panels are *defined*
# by Ig class (IgG / IgM / IgA are different immune responses tested
# for different clinical purposes). Falling back to a cross-class same-
# analyte row (Mercury IgE for ``汞 IgM``) is clinically wrong here.
# Food sensitivity panels stay non-strict — niche foods have only IgE
# rows in LOINC and the same-analyte fallback is preferable to null.
_CHEMICAL_IMMUNE_CONTEXT_RE = re.compile(
    r"化学免疫反应筛查"                  # CN
    r"|重金属.{0,4}IgG|重金属.{0,4}IgM|重金属.{0,4}IgA"
    r"|霉菌.{0,4}IgG|霉菌.{0,4}IgM|霉菌.{0,4}IgA"  # CN: mold panel
    # Wheat/gluten proteome reactivity & autoimmunity — STRICT antibody-
    # class semantics. Indicators like ``小麦 IgA`` in this panel mean
    # anti-wheat-protein IgA autoantibody, not acute IgE allergy. When
    # LOINC has no IgA sibling for the analyte, honest-null beats the
    # IgE fallback (the diagnostic intent is class-defined: IgE positive
    # ≠ IgA positive, they signal different immune-response types).
    r"|麸质\s*蛋白组反应性?"
    r"|wheat\s*[/／]?\s*gluten\s+proteome"
    r"|gluten\s+proteome\s+reactivity"
    # Multiple autoimmune reactivity screen — Cyrex MAP, class-strict.
    r"|多重自身免疫反应筛查"
    r"|multiple\s+autoimmune\s+reactivity\s+screen"
    r"|chemical\s+immune\s+reactivity"
    r"|heavy\s+metal\s+immune"
    r"|mold\s+immune\s+reactivity",
    re.IGNORECASE,
)

# Chemical-IRS haptens with no LOINC enumeration. The Cyrex-style
# chemical immune reactivity panel asks about specific xenobiotic
# antibodies — most have no LOINC code as of 2.78. Cosine drifts to
# the nearest hapten LCN (TBBPA → Isocyanate; BPA-binding-protein →
# Benzene ring; Tetrachloroethylene → Trichothecene) which is
# clinically meaningless. Force null when the chemical-IRS context
# is active AND the indicator names one of these specific haptens.
#
# Empirical compile of v1 chemical-IRS rows: LOINC has Benzene ring,
# Phthalic anhydride (TMA), Aflatoxin, Formaldehyde, Isocyanate IgM,
# Trimellitic anhydride, Trichothecene, Stachybotrys — but NOT BPA /
# TBBPA / BPA-binding-protein / Tetrachloroethylene / parabens /
# mixed-heavy-metals. Add new haptens here as new gaps surface.
_CHEMICAL_IRS_NO_LOINC_HAPTEN_RE = re.compile(
    r"四溴双酚\s*A|tetrabromobisphenol\s*A?|\bTBBPA\b"
    r"|双酚\s*A\s*结合蛋白|bisphenol\s*A\s*binding|BPA[- ]?BP\b|BPA[- ]?binding"
    r"|双酚\s*A(?!\s*结合)|bisphenol\s*A(?!\s*binding)|\bBPA\b(?![- ]?(?:BP|binding))"
    r"|四氯乙烯|tetrachloroethylene|perchloroethylene|\bPCE\b"
    r"|对羟基苯甲酸酯|paraben"
    r"|混合\s*重金属|mixed\s+heavy\s+metals?"
    # Isocyanate IgG+IgA: LOINC only carries 17028-2 (IgM). Match
    # only when the query asks for IgG or IgA (IgM ⇒ Isocyanate is
    # resolvable and shouldn't force null).
    r"|异氰酸酯(?=.{0,20}(?:IgG|IgA))|isocyanate(?=.{0,20}(?:IgG|IgA))"
    # Aflatoxin combined IgG+IgA: LOINC has Aflatoxin IgG (29985-7) and
    # Aflatoxin IgA (32657-9) separately but no combined-class code.
    # Cosine drifts to IgA-only — wrong since the panel reports
    # combined reactivity. Match only when explicit ``IgG+IgA`` marker;
    # bare ``黄曲霉毒素 IgG`` or ``IgA`` resolves normally.
    r"|黄曲霉毒素\s*IgG\s*\+\s*IgA|aflatoxins?\s*IgG\s*\+\s*IgA",
    re.IGNORECASE,
)


def _chemical_irs_no_loinc_null(query_text: str, cache: dict) -> "np.ndarray | None":
    """Force-null for chemical-IRS queries naming a hapten LOINC
    doesn't enumerate. Mirror of :func:`_cn_no_loinc_null` for the
    Cyrex-style chemical-immune-reactivity space — same idea (LOINC
    coverage gap), narrower scope (only fires when
    :data:`_CHEMICAL_IMMUNE_CONTEXT_RE` confirms the panel context).
    """
    if not _CHEMICAL_IMMUNE_CONTEXT_RE.search(query_text):
        return None
    if not _CHEMICAL_IRS_NO_LOINC_HAPTEN_RE.search(query_text):
        return None
    n = int(np.asarray(cache["canonical"]).shape[0])
    return np.zeros(n, dtype=bool)


# Non-fatty-acid organic acids in a fatty-acid panel context. Indicators
# like ``营养·脂肪酸谱·甲酸`` (formate) end up under a ``脂肪酸谱``
# (fatty acid panel) section, but the leaf analyte is a small-molecule
# organic acid that LOINC encodes as its own COMPONENT (Formate / Acetate /
# Propanoate / Pyruvate / Oxalate). Cosine drifts to ``Omega N fatty
# acids`` / ``Polyunsaturated fatty acids`` because the panel token
# dominates. Restrict to rows whose COMPONENT exactly matches the
# query leaf when one of the known organic acids fires.
#
# Tuple is (leaf_re, component_name). Add new entries here when a new
# non-FA organic acid + ``脂肪酸谱`` confusion surfaces.
#
# Leaf anchoring: the acid token must START the leaf segment (after an
# optional short prefix ``总 / 游离`` for "total / free") and may be
# followed only by a small set of measurement suffixes (``盐`` salt,
# ``百分比`` / ``含量`` / ``定量`` / ``测定`` / ``浓度``) or end of
# string. This rejects substituted derivatives (``苯乙酸`` =
# phenylacetate, ``乙酰乙酸`` = acetoacetate, ``4-羟基苯乙酸`` /
# 4-hydroxyphenylacetate) and chemical-state qualifiers (``沉渣草酸
# 盐二水合物`` = calcium oxalate dihydrate crystals — LOINC has a
# dedicated crystal-microscopy code 99898-9, not the ion measurement).
_ACID_LEAF_SUFFIX = (
    r"(?:盐|鹽|百分比|含量|定量|测定|測定|浓度|濃度"
    # Ratio-form leaves (``/肌酐比`` / ``/Creatinine``) for urine
    # organic-acid panels: extends disambiguation to ``X/肌酐比``
    # queries, otherwise the regex's ``$`` anchor stops the filter
    # from firing on the bench's dominant query shape.
    r"|\s*/\s*(?:肌酐比|肌酐|creatinine\s*(?:ratio)?)"
    r"|\s*\(.*?\)"
    r")?$"
)
_ACID_LEAF_PREFIX = r"^\s*(?:总|總|游离|游離)?"
_NON_FA_ORGANIC_ACIDS: tuple[tuple["re.Pattern[str]", str], ...] = (
    (re.compile(_ACID_LEAF_PREFIX + r"(?:甲酸|蚁酸|蟻酸|formic\s+acid|formate)"
                + _ACID_LEAF_SUFFIX, re.IGNORECASE), "Formate"),
    (re.compile(_ACID_LEAF_PREFIX + r"(?:乙酸|醋酸|acetic\s+acid|acetate)"
                + _ACID_LEAF_SUFFIX, re.IGNORECASE), "Acetate"),
    (re.compile(_ACID_LEAF_PREFIX + r"(?:丙酸|propionic\s+acid|propanoate|propionate)"
                + _ACID_LEAF_SUFFIX, re.IGNORECASE), "Propanoate"),
    (re.compile(_ACID_LEAF_PREFIX + r"(?:草酸|oxalic\s+acid|oxalate)"
                + _ACID_LEAF_SUFFIX, re.IGNORECASE), "Oxalate"),
    # 2-Oxoisocaproate (leucine keto-acid) vs the cosine-twin
    # 2-Oxoisovalerate (valine keto-acid). 异己 = isohexa = isocapro
    # (C6 with 4-methyl branch); 异戊 = isoval (C5).
    (re.compile(_ACID_LEAF_PREFIX + r"(?:2-?\s*氧代异己酸|2-?\s*Oxoisocaproate"
                r"|alpha[- ]?keto-?isocaproate|4-?methyl-?2-?oxopentanoate)"
                + _ACID_LEAF_SUFFIX, re.IGNORECASE), "2-Oxoisocaproate"),
    # Succinylacetone (4,6-dioxoheptanoate) vs the cosine-twin
    # Succinate. Different chemistry — diketone-acid vs di-acid.
    (re.compile(_ACID_LEAF_PREFIX + r"(?:丁二酰丙酮|succinylacetone"
                r"|4,?6-?dioxoheptanoate)"
                + _ACID_LEAF_SUFFIX, re.IGNORECASE), "Succinylacetone"),
)


def _build_component_match_mask(
    cache: dict, component_name: str,
) -> np.ndarray:
    """Tag LOINC rows whose COMPONENT axis is exactly *component_name*
    (case-insensitive). Cached on *cache* keyed by component."""
    key = f"_loinc_component_match_{component_name.lower()}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache[key] = mask
        return mask
    components: list[str | None] | None = axis_data.get("row_components")
    if components is None:
        cache[key] = mask
        return mask
    tgt = component_name.lower()
    for r in range(min(n, len(components))):
        c = components[r]
        if c and c.strip().lower() == tgt:
            mask[r] = True
    cache[key] = mask
    return mask


# Microbiology / species-name license — leaf segments that name a
# bacterium / fungus / virus / parasite carrying an organic-acid
# substring (``产甲酸草酸杆菌`` = Oxalobacter formigenes, ``丙酸杆菌``
# = Propionibacterium). The organic-acid analyte disambiguation must
# not fire on these — the query is about the organism, not the acid.
_MICROBIOLOGY_LEAF_RE = re.compile(
    r"杆菌|球菌|链球菌|鏈球菌|双球菌|雙球菌|弧菌|螺菌|螺旋体|螺旋體"
    r"|放线菌|放線菌|分枝杆菌|衣原体|衣原體|支原体|支原體|立克次体|立克次體"
    r"|微生物|细菌|細菌|真菌|霉菌|黴菌|酵母|酵母菌|念珠菌"
    r"|寄生虫|寄生蟲|原虫|原蟲|线虫|線蟲|绦虫|絛蟲|吸虫|吸蟲"
    r"|蛔虫|蛔蟲|鞭虫|鞭蟲|蛲虫|蟯蟲|钩虫|鉤蟲"           # specific worms
    r"|滴虫|滴蟲|阿米巴|amoeba|amoebic"                # protozoa
    r"|病毒|virus\b|bacterium|bacterial|bacteria|fungus|fungal"
    # Bare 菌 suffix — Chinese microorganism names not anchored on the
    # specific morphology (``普拉梭菌`` = Faecalibacterium prausnitzii,
    # ``阿克曼氏菌`` = Akkermansia, ``产气柯林斯氏菌`` = Collinsella,
    # ``罗氏菌属`` = Roseburia spp.). All clinical uses of 菌 in
    # Chinese refer to microorganisms — no measurable false-positive
    # risk on lab indicator text.
    r"|菌(?:属|科|株|种|種)?$|菌(?:属|科|株|种|種)"
    r"|\b[A-Z][a-z]+(?:\s+[a-z]+){0,2}\s+(?:sp|spp)\.?\b",
    re.IGNORECASE,
)


def _organic_acid_disambiguate_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Restrict to COMPONENT=<organic-acid> rows when the query's *leaf*
    segment (after the last ``·``) names a specific non-FA organic
    acid (Formate / Acetate / Propanoate / Oxalate). Mirror of
    :func:`_bicarbonate_disambiguate_keep` for the fatty-acid-panel
    confusion.

    Leaf-only matching avoids false positives where the analyte token
    appears mid-word. Microbiology license — bacterial/fungal/parasite
    species names that embed the analyte token (``产甲酸草酸杆菌`` /
    *Oxalobacter formigenes* contains 甲酸 + 草酸; ``丙酸杆菌`` /
    *Propionibacterium*) must not fire — the query is about the
    organism. Silent when no organic-acid marker fires in the leaf
    segment OR when the leaf is a species name.
    """
    # Extract the leaf — last ``·`` -delimited segment of the indicator
    # (post-source-comma). Falls back to the whole indicator when no
    # ``·`` separator is present.
    m = _SOURCE_PREFIX_RE.match(query_text)
    indicator = query_text[m.end():] if m else query_text
    leaf = indicator.rsplit("·", 1)[-1].strip()
    if not leaf:
        return None
    if _MICROBIOLOGY_LEAF_RE.search(leaf):
        return None
    # ``/肌酐比`` / ``/Cr`` ratio marker in leaf — LOINC encodes ratio
    # rows with COMPONENT ``X/Creatinine`` (not bare ``X``), so when
    # the query asks for a ratio the bare-COMPONENT mask alone would
    # mask the ratio rows away and the picker would fall to bare
    # ``X [Presence]`` or out-of-specimen DBS variants.
    ratio_marker = re.search(
        r"/\s*(?:肌酐|creatinine)|Cr\s*ratio", leaf, re.IGNORECASE,
    )
    for q_re, comp in _NON_FA_ORGANIC_ACIDS:
        if q_re.search(leaf):
            mask = _build_component_match_mask(cache, comp)
            if ratio_marker is not None:
                mask = mask | _build_component_match_mask(
                    cache, f"{comp}/Creatinine",
                )
            if mask.any():
                return mask
    return None


# Bicarbonate vs Carbonate disambiguation. The Chinese ``碳酸氢根``
# (literally "carbonic acid hydrogen radical") is HCO3- = Bicarbonate;
# pure ``Carbonate`` (CO3^2-) is a different anion. The embedder maps
# ``碳酸氢根`` close to LOINC's ``Carbonate`` COMPONENT because the
# tokens share ``碳酸``, surfacing 2033-9 (Carbonate in Arterial blood)
# over 1960-4 (Bicarbonate in Arterial blood). Drop Carbonate-COMPONENT
# rows when the query explicitly asks for bicarbonate.
_BICARBONATE_QUERY_RE = re.compile(
    r"碳酸氢根|碳酸氢盐|碳酸氫根|碳酸氫鹽|重碳酸"
    r"|\bbicarbonate\b|\bHCO3-?\b|\bHCO\s*3\b",
    re.IGNORECASE,
)


def _build_carbonate_component_mask(cache: dict) -> "np.ndarray | None":
    """Tag LOINC rows whose COMPONENT axis is bare ``Carbonate`` (NOT
    ``Bicarbonate`` / ``Carbonate dehydratase`` / similar compounds).
    Cached on *cache*."""
    cached = cache.get("_loinc_carbonate_component_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_loinc_carbonate_component_mask"] = mask
        return mask
    components: list[str | None] | None = axis_data.get("row_components")
    if components is None:
        cache["_loinc_carbonate_component_mask"] = mask
        return mask
    for r in range(min(n, len(components))):
        c = components[r]
        if c and c.strip().lower() == "carbonate":
            mask[r] = True
    cache["_loinc_carbonate_component_mask"] = mask
    return mask


def _bicarbonate_disambiguate_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop COMPONENT=Carbonate rows when the query asks for bicarbonate.
    Silent otherwise.
    """
    if not _BICARBONATE_QUERY_RE.search(query_text):
        return None
    return ~_build_carbonate_component_mask(cache)


# Chinese-analyte → LCN-substring disambiguation. Embedder fails to
# map a Chinese analyte name to its English LOINC counterpart, cosine
# drifts to lexically-similar wrong analytes (``核基质蛋白`` →
# ``Neuronal nuclear``; ``胰岛素瘤抗原 2`` → ``Insulin Ab``;
# ``转谷氨酰胺酶`` → null after digit-rerank rejects close cousins).
# Each entry is (leaf-anchored CN regex, LCN-prefix substring) — when
# the leaf carries the CN marker, restrict to LOINC rows whose LCN
# starts with the English term. Leaf anchoring (with optional class /
# subtype suffix) avoids over-firing on compound species names.
#
# Add new entries when an embedder-gap surfaces in sample review.
_CN_ANALYTE_LCN_TABLE: tuple[tuple["re.Pattern[str]", str], ...] = (
    # NMP-22 — Nuclear matrix protein 22. ``核基质蛋白`` is unique
    # to this analyte in clinical Chinese (vs ``Neuronal nuclear``).
    # Anchor: bare ``核基质蛋白`` (no following digit), or with the
    # explicit ``22`` subtype. Excludes ``核基质蛋白2`` (Mi-2 Ab,
    # myositis marker — Nuclear matrix protein 2, LOINC 56637-2).
    (re.compile(r"核基质蛋白(?:[\s\-]*22|(?![0-9]))", re.IGNORECASE),
     "Nuclear matrix protein 22"),
    # Tissue transglutaminase Ab (tTG / tTG-2). The ``-2`` digit is
    # the tissue isoform; LOINC carries the analyte as
    # ``Tissue transglutaminase`` without a digit. Match drops the
    # ``-2`` requirement so the canonical LOINC code wins despite
    # analyte_digit family rerank.
    (re.compile(r"组织转谷氨酰胺酶|転谷氨酰胺酶|转谷氨酰胺酶|tissue\s+transglutaminase|tTG\b",
                re.IGNORECASE),
     "Tissue transglutaminase"),
    # IA-2 / Islet antigen 2 / Islet cell 512 Ab. LOINC encodes this
    # as ``Islet cell 512 Ab`` (31209-0 / 32636-3); the Chinese name
    # ``胰岛素瘤抗原 2`` literally means "insulinoma antigen 2" and
    # cosine drifts to plain ``Insulin Ab``.
    (re.compile(r"胰岛素瘤抗原\s*2|islet\s+antigen\s+2|islet\s+cell\s+512|\bIA[- ]?2\b",
                re.IGNORECASE),
     "Islet cell 512 Ab"),
)


def _build_component_lcn_prefix_mask(
    cache: dict, lcn_prefix: str,
) -> np.ndarray:
    """Tag LOINC rows whose LCN starts with *lcn_prefix* (case-
    insensitive). Cached on *cache* keyed by prefix."""
    key = f"_loinc_lcn_prefix_{lcn_prefix.lower()}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    tgt = lcn_prefix.lower()
    for i, nm in enumerate(names):
        if nm and nm.lower().startswith(tgt):
            mask[i] = True
    cache[key] = mask
    return mask


def _cn_analyte_disambiguate_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Restrict to LCN-prefix-matching rows when the query's *leaf*
    segment carries a CN analyte name that the embedder fails to map
    (``核基质蛋白`` / ``转谷氨酰胺酶`` / ``胰岛素瘤抗原 2``).
    Mirror of :func:`_organic_acid_disambiguate_keep` for analytes
    LOINC enumerates by English name only.

    Microbiology / species-name license — bacterial genera carrying
    Chinese characters that happen to share substring with these
    analyte names skip via :data:`_MICROBIOLOGY_LEAF_RE`.
    """
    m = _SOURCE_PREFIX_RE.match(query_text)
    indicator = query_text[m.end():] if m else query_text
    leaf = indicator.rsplit("·", 1)[-1].strip()
    if not leaf:
        return None
    if _MICROBIOLOGY_LEAF_RE.search(leaf):
        return None
    for q_re, lcn_prefix in _CN_ANALYTE_LCN_TABLE:
        if q_re.search(leaf):
            mask = _build_component_lcn_prefix_mask(cache, lcn_prefix)
            if mask.any():
                return mask
    return None


# NKT (Natural killer T cell) vs NK polarity disambiguator. NK is
# CD3-CD56+; NKT is CD3+CD56+ — one minus sign apart. Cosine alone
# cannot distinguish (``Natural killer`` matches both LCN strings)
# and the picker drifts ``自然杀伤 T 细胞`` queries to NK codes
# (8112-5 ``CD3-CD16+CD56+`` / 53936-1 ``CD3-CD56+``) instead of
# the NKT codes (17135-5 / 26858-1 ``CD3+CD56+``). Restrict to
# CD3+CD56+ rows when the query carries the NKT anchor (Chinese
# ``自然杀伤 T 细胞`` with an explicit T between, or English NKT /
# ``natural killer T``).
#
# Asymmetric — bare-NK queries (rows 557/560/567/570 of buggy
# audit) already resolve correctly, so no NK-side restriction.
_NKT_QUERY_RE = re.compile(
    r"自然杀伤\s*T\s*细胞|自然殺傷\s*T\s*細胞"
    r"|\bNKT\b|\bNK\s+T\s+cells?\b|natural\s+killer\s+T",
    re.IGNORECASE,
)
_NKT_LCN_RE = re.compile(
    # CD3-positive co-expressed with CD56 (allow optional space and
    # other markers between, e.g. ``CD3+CD8+CD56+``). Anchor on the
    # ``+`` to exclude the CD3-negative NK rows.
    r"CD3\s*\+[^,]*CD56\s*\+"
    r"|natural\s+killer\s+T|\bNKT\b",
    re.IGNORECASE,
)


def _build_nkt_lcn_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose LCN carries an NKT-positive signature
    (CD3+ co-expressed with CD56+, or ``Natural killer T`` / NKT
    literal). Cached on *cache*."""
    cached = cache.get("_loinc_nkt_lcn_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _NKT_LCN_RE.search(nm):
            mask[i] = True
    log.info(
        "NKT LCN mask: %d / %d LOINC rows tagged (CD3+...CD56+ or NKT lit)",
        int(mask.sum()), n,
    )
    cache["_loinc_nkt_lcn_mask"] = mask
    return mask


def _nkt_vs_nk_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Restrict to CD3+CD56+ (NKT) LOINC rows when the query carries
    an NKT anchor — ``自然杀伤 T 细胞`` / ``NKT`` / ``natural killer T``.
    Drops the bare-NK rows (CD3-CD56+ / CD3-CD16+CD56+) which cosine
    over-picks because ``Natural killer`` matches both LCN families.
    Silent otherwise.
    """
    if not _NKT_QUERY_RE.search(query_text):
        return None
    return _build_nkt_lcn_mask(cache)


# T-cell receptor (αβ / γδ) subset demote. Total-T queries
# (``总 T 细胞`` / ``Total T cell`` / ``%T cell``) ask for the pan-T
# marker which LOINC encodes as ``CD3 cells`` (8122-4 / 8124-0 /
# 90304-7) or ``T Lymphocytes.CD3+CD19-`` (106950-9). The picker
# drifts to TCRαβ-specific codes (50975-2 ``TCR alpha beta cells
# [#/volume] in Blood`` etc.) because ``TCR αβ+`` cells dominate
# peripheral T cells clinically but are a measured *subset*, not
# the canonical pan-T marker. When the query carries no TCR / α / β
# / γ / δ anchor, drop LCN rows containing TCR-receptor subset
# markers so the CD3-anchored canonical wins.
_TCR_LICENSE_QUERY_RE = re.compile(
    # Chinese / English explicit TCR mentions
    r"\bTCR\b|T\s*细胞\s*受体|T[- ]?cell\s+receptor"
    # Greek-letter subset markers
    r"|α\s*β|γ\s*δ|αβ|γδ"
    # Alphabetic alpha/beta/gamma/delta only when adjacent to T (or
    # TCR/T-cell preceding). Bare ``alpha`` / ``beta`` would mis-license
    # plenty of unrelated chemistry queries (β-glucuronidase etc.).
    r"|T\s*cells?\s*[\(\[]?\s*(?:alpha|beta|gamma|delta)"
    r"|(?:alpha|beta|gamma|delta)\s*[/／]\s*(?:alpha|beta|gamma|delta)\s+T\s*cells?",
    re.IGNORECASE,
)
_TCR_SUBSET_LCN_RE = re.compile(
    r"\bTCR\s+alpha\s+beta\b|\bTCR\s+gamma\s+delta\b"
    r"|\bCells\.CD3\+TCR",
    re.IGNORECASE,
)


def _build_tcr_subset_mask(cache: dict) -> np.ndarray:
    cached = cache.get("_loinc_tcr_subset_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    for i, nm in enumerate(names):
        if nm and _TCR_SUBSET_LCN_RE.search(nm):
            mask[i] = True
    log.info(
        "TCR subset mask: %d / %d LOINC rows tagged (TCR αβ / γδ subsets)",
        int(mask.sum()), n,
    )
    cache["_loinc_tcr_subset_mask"] = mask
    return mask


def _tcr_subset_demote_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop TCR αβ / γδ subset LOINC rows when the query carries no
    TCR-receptor license marker. Pan-T queries (``总 T 细胞数`` /
    ``Total T cell``) should land on the CD3 pan-T markers
    (8122-4 / 8124-0); without this filter, 50975-2 ``TCR alpha
    beta cells [#/volume] in Blood`` outranks 8122-4 on cosine.
    """
    if _TCR_LICENSE_QUERY_RE.search(query_text):
        return None
    return ~_build_tcr_subset_mask(cache)


# Concept-level honest-null patterns surfaced in sample-100 v8 review.
# These ask for combined concepts LOINC doesn't enumerate (or where
# the cosine top systematically drifts to a clinically-wrong analogue).
# Force null and let the consumer treat as "no measurement code".
#
# - Sleep efficiency / Sustained sleep efficiency (``持续睡眠效率`` /
#   ``睡眠效率``) — no LOINC code as of 2.78. Cosine drifts to
#   ``Sleep duration``, a different concept (numerator vs ratio).
# - Generic Z-score in screening context (``糖尿病筛查·Z 分数``,
#   without an explicit analyte) — LOINC has dozens of analyte-
#   specific Z-scores; cosine picks one at random (typically IGF-I).
#   No "diabetes risk Z-score" code exists.
# - Generic imaging examination header (``影像学检查`` alone) —
#   document-section label, not a measurement code. Cosine picks
#   modality-specific narrative codes (US / CT / MR study
#   observation) which are wrong context.
# - Parvalbumin (``小清蛋白``) in food-IRS context — no LOINC code
#   for parvalbumin specifically; cosine drifts to ``Mozzarella
#   cheese IgG``.
# - FGFR3 autoantibody (``IgG vs FGFR3``) — LOINC has FGFR3 gene
#   mutations but no autoantibody code.
# - Sleep-time-fraction stratified by posture (``睡眠时间比例
#   (百分比)·左侧卧位``) — body posture × sleep fraction has no
#   LOINC code; cosine drifts to plain ``Light sleep duration``.
_CONCEPT_NO_LOINC_QUERY_RE = re.compile(
    # Sleep efficiency family
    r"睡眠效率|持续睡眠效率|sleep\s+efficien"
    # Generic Z-score without a license analyte (require ``糖尿病`` /
    # ``筛查`` source to scope — analyte-specific Z-scores still
    # resolve via context)
    r"|糖尿病筛查,\s*Z\s*[- ]?分数|^\s*Z\s*[- ]?score\s*$"
    # Imaging examination section header
    r"|辅助检查·影像学检查|影像学检查$|imaging\s+examinations?$"
    # Food-IRS parvalbumin
    r"|食物.{0,15}小清蛋白|food.{0,15}parvalbumin"
    # FGFR3 autoantibody (LOINC has only mutation codes). Real-world
    # queries arrive in Chinese with the spelled-out target
    # ``成纤维细胞生长因子受体3`` (fibroblast growth factor receptor 3),
    # never the ``FGFR3`` abbreviation — match both, in IgG/vs context.
    r"|IgG\s*(?:vs|对|抗)\s*FGFR3|FGFR3\s+(?:IgG|autoantibody|Ab)"
    r"|IgG\s*[/／]?\s*成纤维细胞生长因子受体\s*3"
    r"|成纤维细胞生长因子受体\s*3\s*(?:抗体|IgG|Ab)"
    # Sleep-time-fraction by posture
    r"|睡眠时间比例\s*[（\(]?\s*百分比\s*[）\)]?\s*[·•・].{0,8}(?:仰卧|俯卧|左侧|右侧|侧卧)"
    r"|sleep\s+time\s+fraction.{0,15}(?:supine|prone|lateral)"
    # Sample-100 v9 (2026-05-26) — pure honest-null:
    # - Bifidobacterium longum subsp. longum in stool panel: LOINC has
    #   B. longum at species level but no subsp.; cosine drifts to
    #   ``Bacteria identified in Duodenal fluid``.
    r"|长双歧杆菌长亚种|Bifidobacterium\s+longum\s+subsp\.?\s+longum"
    # - RBC electrophoresis time: no LOINC; cosine drifts to ``Viscosity
    #   of Blood`` (whole-blood viscosity panel sibling, different
    #   measurement).
    r"|红细胞电泳时间|red\s+blood\s+cell\s+electrophoresis\s+time"
    r"|RBC\s+electrophoresis\s+time"
    # - Stage-stratified AHI (REM / non-REM): plain AHI has 69990-0 but
    #   per-stage AHI is not enumerated; cosine drifts to ``Apnea +
    #   hypopnea [#]`` (count, not index).
    r"|(?:AHI|呼吸暂停低通气指数|呼吸暂停低通氣指數|apnea[- ]?hypopnea\s+index)"
    r"\s*[·•・]\s*(?:快速眼动|非快速眼动|REM|non[- ]?REM)"
    # - Visit diagnoses (住院报告·就诊诊断): no encounter-dx narrative
    #   LOINC; cosine drifts to 78375-3 Discharge dx (wrong scope).
    r"|^就诊诊断$|住院报告,\s*就诊诊断$|visit\s+diagnos[ie]s$"
    # - Health guidance recommendation: document-section label, not a
    #   measurement; cosine picks 100894-5 Recommended screening
    #   frequency (much narrower).
    r"|建议\s*[·•・]\s*健康指导|recommendation.{0,5}health\s+guidance"
    # - ESR in pericardial fluid: not enumerated; cosine drifts to
    #   pericardial-fluid RBC count.
    r"|心包液.{0,20}(?:红细胞沉降率|血沉)"
    r"|pericardial\s+fluid.{0,20}(?:ESR\b|erythrocyte\s+sed)"
    # - ED note vital signs: section-vs-panel mismatch; cosine drifts
    #   to 78303-5 ED Flowsheet (different concept). Generic 8716-3
    #   panel routing would be the right answer; null is the stopgap.
    r"|急诊记录\s*[·•・]\s*生命体征|ED\s*note.{0,5}vital\s*signs"
    # - Th17/Treg ratio: no LOINC; cosine drifts to Treg/T4-helper
    #   ratio (different cell-pair ratio).
    r"|Th17\s*[/／]\s*Treg|Th17\s*[/／]\s*regulatory"
    # - TRU body score · muscle score: body-composition vendor score,
    #   not muscle strength; cosine drifts to 80322-1 Muscle strength.
    r"|真实身体评分.{0,10}肌肉评分|TRU\s*body\s*score.{0,15}muscle\s*score"
    # - Body composition · Protein: a body-composition analyzer's protein
    #   *mass* readout has no LOINC. Cosine drifts to dietary NUTRITION
    #   concepts (``Protein intake Estimated`` 9079-5 → ``Protein need``
    #   75290-7), neither of which is a measured body-protein quantity.
    r"|身体成分\s*[·•・]\s*蛋白(?:质)?|body\s*composition.{0,8}protein"
    # - Telomere length: LOINC 35463-9 is identifier/nominal, not
    #   measurement; 38291-1/99135-6 may exist but picker drifts.
    #   TODO: enable when SCALE_TYP=Len routing exists.
    r"|端粒长度|telomere\s+length"
    # - Sleep recording analysis stop time (lights-on marker): no LOINC;
    #   cosine drifts to 105991-4 Usual bedtime (regular bedtime, not
    #   recording terminus).
    r"|分析停止时间|analysis\s+stop\s+time"
    # - Food-IRS tragacanth IgG: only IgE LOINC (7743-8) exists; food-
    #   panel context is IgG, so cross-class swap is wrong.
    r"|食物免疫反应筛查测试.{0,15}黄蓍胶"
    r"|food\s+immune\s+reactivity.{0,15}(?:gum\s+)?tragacanth"
    # Deeper-fix stopgaps (clinically wrong specimen / anatomy / aspect;
    # proper LOINC exists but routing is not yet in place — null beats
    # wrong-specimen for analytics):
    # - β-glucuronidase in 2200 stool panel (proper: 13586-3 Stool).
    r"|2200.{0,30}β[- ]?\s*葡萄糖醛酸苷酶"
    r"|2200.{0,30}beta[- ]?glucuronidase"
    r"|GI\s+Effects.{0,30}beta[- ]?glucuronidase"
    # - Blood-gas lactate (proper: 32693-4 Bld / 1996-8 ArtBld; cosine
    #   keeps Ser/Plas 2524-7 since blood-gas source signal is weak).
    r"|血气分析,\s*乳酸|血气分析,\s*Lactic\s+acid|血气分析,\s*Lactate"
    # - Femoral neck T-score (proper: 80949-1 right / 80950-9 left;
    #   cosine drifts to broader DXA Femur).
    r"|股骨颈\s*[·•・]\s*T\s*值|femoral\s+neck.{0,5}T[- ]?score"
    # - Mean plasma glucose (proper: 41653-7 mean; cosine drifts to
    #   spot glucose 14749-6).
    r"|糖尿病筛查,\s*平均血浆葡萄糖|mean\s+plasma\s+glucose"
    r"|mean\s+glucose\s*$"
    # 尿液 source audit (2026-05-26, Pattern C — generic ``Organic
    # acids`` fallback for analytes with no LOINC enumeration):
    # - Bare urine sediment as a panel header (``随机尿液中的有机酸·
    #   尿沉渣``). LOINC has UA-microscopy panel codes but no
    #   ``urinary sediment`` as a single concept; cosine drifts to
    #   10972-8 Organic acids panel (unrelated).
    r"|随机尿液中的有机酸\s*[·•・]\s*尿沉渣|^尿沉渣$"
    # - Calprotectin in urine context. Calprotectin is a stool marker
    #   (38445-3 / 51631-0); the curated `钙卫蛋白 → Calprotectin`
    #   alias surfaces those stool codes, but the indicator carries
    #   ``尿液中的`` / ``随机尿`` source context, so picker can't pick
    #   stool. No urine calprotectin LOINC exists.
    r"|随机尿液中的有机酸\s*[·•・]\s*钙卫蛋白"
    r"|spot\s+urine.{0,10}calprotectin|urine\s+calprotectin"
    # 尿液 source audit (2026-05-26, Pattern A/E — no LOINC bridge):
    # - 2-Oxo-3-methylvalerate (keto-acid of isoleucine): LOINC has
    #   only the 2-HYDROXY form (29505-5), not 2-OXO. Different
    #   functional group; pick was wrong.
    r"|2-?\s*氧代\s*-?\s*3-?\s*甲基戊酸"
    r"|2-?\s*oxo-?3-?methylvalerate|2-?\s*oxo-?3-?methylpentanoate"
    # - 尿液分析·微生物 (generic ``microbial items`` in UA panel):
    #   no LOINC for ``microbial findings`` as a single concept.
    #   Picker drifted to Cervical-specimen Microorganism code.
    r"|尿液分析\s*[·•・]\s*微生物(?!\s*\S)"
    # - 尿液分析·无机盐类 (``inorganic salts``): no LOINC for the
    #   panel concept; picker over-specified to Phosphate alone.
    r"|尿液分析\s*[·•・]\s*无机盐类"
    # Fix #17 follow-up — alias-collateral honest-null (2026-05-26):
    # - 反式油酸 (elaidic acid, trans-18:1 n-9): LOINC's only candidate
    #   is 48368-5 ``trans-Octadecanoate (C18:0)`` whose name carries a
    #   contradiction (saturated C18:0 cannot have trans isomers — the
    #   intent is trans-C18:1 elaidate), so we don't bridge it. Note:
    #   ``反油酸`` (without 式) means trans-Vaccenate (C18:1w7) — a
    #   DIFFERENT trans-monoene — and IS bridged via curated alias.
    r"|反式油酸|elaidate|elaidic\s+acid"
    r"|trans[- ]oleate|C18:1\s*trans"
    # - 粪便分泌型免疫球蛋白 A (secretory IgA in stool, sIgA): LOINC
    #   has only Bordetella-pertussis-specific secretory IgA codes,
    #   nothing for the bare gut-mucosal sIgA marker. The new
    #   `免疫球蛋白 → Immunoglobulin Ig` alias from fix #17 (B) lifted
    #   the picker off the Rh-globulin pollution but landed it on
    #   Lactoferrin (wrong protein).
    r"|粪便\s*分泌型\s*免疫球蛋白\s*A|分泌型\s*免疫球蛋白\s*A\s*[（(]?\s*粪"
    r"|fecal\s+secretory\s+IgA|sIgA\s+in\s+stool"
    # Genova 3540 fatty-acid panel (2026-05-27) ─ share/ratio markers
    # with no LOINC enumeration. ω-9 already nulls naturally (no alias
    # bridge); ω-3 / ω-6 share queries drift to 48382-6 ω6/ω3 ratio
    # which is a DIFFERENT concept (the share = X/total; the ratio =
    # X/Y between two specific families).
    r"|ω\s*-?\s*3\s*脂肪酸\s*占比|ω\s*-?\s*6\s*脂肪酸\s*占比"
    r"|omega\s*-?\s*3\s*fatty\s*acids?\s*(?:share|percent)"
    r"|omega\s*-?\s*6\s*fatty\s*acids?\s*(?:share|percent)"
    # LA/DGLA ratio (亚油酸 / 二高 γ-亚麻酸): LOINC has LA and DGLA
    # separately but no ratio code; cosine drifts to LA alone (which
    # is wrong for a ratio query).
    r"|亚油酸\s*/\s*二高\s*-?\s*γ-?\s*亚麻酸"
    r"|linoleate\s*/\s*(?:dihomo|homo)[- ]?gamma[- ]?linolenate"
    r"|LA\s*/\s*DGLA"
    # Triene/Tetraene ratio (Mead acid C20:3w9 / Arachidonate C20:4w6):
    # marker of essential fatty-acid deficiency. LOINC has Mead acid
    # and Arachidonate separately but no enumerated ratio code; cosine
    # drifts to 48382-6 ω-6/ω-3 ratio (DIFFERENT chemical pair).
    r"|三烯四烯比率|三烯\s*[/／]\s*四烯|triene\s*[-/／\s]+\s*tetraene"
    r"|mead\s*acid\s*/\s*arachidonate"
    # Lymphocyte MAP audit (2026-05-28) ─ Th1 / Th2 / Th17 single-cell
    # count or percent. LOINC enumerates only the pan T-helper marker
    # CD3+CD4+ (24467-3 count / 8123-2 ratio) and a TH1/TH2 cytokine
    # panel (107613-2); the cytokine-defined functional subsets Th1
    # (IFN-γ+) / Th2 (IL-4+) / Th17 (IL-17+) are not coded. Cosine
    # mis-routes Th17 → CD4+CD45RA+ (naive) / CD3+CD4+CD45RO+ (memory)
    # and Th1/Th2 → pan T-helper-by-rapid-immunoassay (92736-8). The
    # TH1/TH2 *ratio* and Th17/Treg *ratio* are handled above via the
    # ratio merger / honest-null; this rule covers the single-marker
    # form. Anchored on the 辅助性 T 细胞 qualifier (CN) or
    # T-Helper-N / Th-N (EN) so unrelated digits don't fire.
    r"|辅助性\s*T\s*细胞\s*(?:17|1|2)(?!\d)"
    r"|T[- ]?[Hh]elper[- ]?(?:17|1|2)(?!\d)"
    r"|\bTh\s*(?:17|1|2)(?!\d)\s*(?:cell|cells|百分比|占比|数|count|%)"
    # T-cell / B-cell ratio — LOINC has lymphocyte-subset count and
    # percent codes (CD3 / CD19 / CD4 / CD8) but no T-cell ÷ B-cell
    # ratio code (T/B is rarely reported clinically; the standard
    # Lymphocyte MAP reports CD4/CD8 ratio instead). Cosine drifts
    # to 26478-8 ``Lymphocytes/Leukocytes in Blood`` (lymph fraction
    # of WBCs — a DIFFERENT ratio).
    r"|T\s*细胞\s*[/／]\s*B\s*细胞\s*比"
    r"|T\s*cell\s*[/／]\s*B\s*cell\s*ratio"
    r"|\bT\s*[/／]\s*B\s+ratio"
    # ``病毒检测`` audit (2026-05-28):
    # - RSV type-A / type-B antigen — LOINC enumerates pan-RSV Ag
    #   (5874-3 / 5876-8 / 33045-6 / etc.) and type-specific RNA
    #   (77022-2 RSV A RNA, 105213-3 RSV B RNA) but NO type-specific
    #   antigen. Cosine drifts to the RNA codes (wrong analyte
    #   class — nucleic acid ≠ protein antigen).
    r"|RSV\s*[AB]\s*型\s*抗原|RSV\s+(?:type[- ]?)?[AB]\s+(?:antigen|Ag)"
    r"|呼吸道合胞病毒\s*[AB]\s*型\s*抗原"
    r"|respiratory\s+syncytial\s+virus\s+(?:type[- ]?)?[AB]\s+(?:antigen|Ag)"
    # - Generic "乙肝病毒抗体" / "乙肝病毒抗原" without surface / core /
    #   e / s / X qualifier. LOINC has no pan-HBV Ab/Ag measurement
    #   code (only 95235-8 Interpretation Narrative / 77176-6 e Ag+Ab
    #   panel header). Cosine drifts to a random subset code (83100-8
    #   core IgG+IgM, 51659-1 surface Ag in Body fluid). The
    #   sibling-specific queries (乙肝表面/核心/e + 抗原/抗体) coexist
    #   in the panel, so the generic form is clinically meant as the
    #   panel-aggregate which LOINC doesn't encode.
    r"|乙(?:型)?肝(?:炎)?(?:病毒)?(?:抗体|抗原)(?![A-Za-z一-鿿·•・/／])"
    r"|hepatitis\s+B(?:\s+virus)?\s+(?:antibod|antigen)"
    r"(?!\s+(?:surface|core|e\b|s\b|x\b))"
    # report_case_03 audit (2026-05-28):
    # - 眼科·晶体 — ophthalmology "lens (of eye)". LOINC has no eye-
    #   lens observation; cosine drifts to 42764-1 "Crystals [type] in
    #   Vitreous fluid by Light microscopy" (urine/joint-fluid crystal
    #   identification, completely unrelated to ocular anatomy). The
    #   source prefix `眼科` disambiguates from urinary 结晶/晶体. The
    #   crystalline-lens body-structure lives in SNOMED (78076003).
    r"|眼科\s*[,，·•・]\s*晶[体體](?![/／·•・])"
    r"|ophthalmolog\w*\s+lens(?:\s+of\s+eye)?"
    r"|crystalline\s+lens"
    # Sample-200 v8 audit (2026-05-29):
    # ─ Cyrex-style permeability screens (intestinal antigen / blood-
    #   brain barrier). LOINC has NO codes for anti-LPS / anti-occludin
    #   / anti-zonulin / anti-BBB-protein antibodies. Cosine drifts to:
    #     - 76487-8 Total IgA in Serum (for ``LPS IgA``)
    #     - 99399-8 Zonulin in Stool (the protein, not the antibody)
    #     - 27030-6 Myelin-associated-glycoprotein IgM in CSF
    #   None of these are clinically equivalent to the proprietary
    #   permeability-marker antibody panel. Honest-null beats wrong
    #   analyte / wrong specimen / wrong measurement type.
    r"|肠道(?:抗原)?\s*通透性\s*筛查"
    r"|intestinal\s+antigenic\s+permeability\s+screen"
    r"|血脑屏障\s*(?:通透性)?\s*筛查"
    r"|blood[- ]brain\s+barrier\s+permeability\s+screen"
    # ─ TB T-SPOT / IGRA T - N score difference (结核菌T-N值). LOINC
    #   has 71773-6 / 71774-4 nil/mitogen response counts and 88516-0
    #   T-SPOT interpretation, but the bare ``T - N value`` is a
    #   computed difference not directly enumerated. Cosine drifts to
    #   17296-5 MTB rRNA Probe (a nucleic-acid test, different concept
    #   class entirely).
    r"|结核菌?\s*T\s*[-‐]\s*N\s*值"
    r"|结核\s*分枝杆菌?\s*T\s*[-‐]\s*N"
    r"|tuberculosis.{0,15}T\s*[-‐]\s*N\s*value"
    # ─ Short-chain fatty acids total (% lookup). The 2200 GI panel
    #   reports SCFA components and a Total column. LOINC enumerates
    #   individual SCFA (Acetate / Butyrate / Propanoate / Valerate) in
    #   stool but has no "Total SCFA" code. Cosine drifts to 27169-2
    #   Butyrate alone — which is one component, not the total.
    r"|短链脂肪酸[\s（(]*(?:总量|总(?:百分比|%)|总)"
    r"|SCFA[\s（(]*(?:total|总|总量|百分比\s*总)"
    r"|short[- ]chain\s+fatty\s+acids?\s+total"
    # ─ Acetate/Propionate/Butyrate share-of-total ``%`` rows in the
    #   2200 stool panel. LOINC has the raw mass measurements but no
    #   ``acid%`` (fraction-of-SCFA-total) variant. Cosine drifts to
    #   55818-9 ``Organic acids [Moles/mass] in Stool`` (generic acids
    #   panel, not the specific share).
    r"|(?:乙酸盐?|丙酸盐?|丁酸盐?|戊酸盐?)\s*百分比"
    r"|(?:acetate|propionate|butyrate|valerate)\s*%"
    r"|stool.{0,10}(?:acetate|propionate|butyrate|valerate)\s*%"
    # ─ Hospital ``结果概述`` / ``Summary of Results`` document section
    #   header. LOINC has no general physical-exam-summary code; cosine
    #   drifts to 18810-2 EKG study observation (cardiology-specific
    #   narrative). Bare report section, not a measurement.
    r"|体检报告\s*[,，]\s*结果概述"
    r"|^\s*结果概述\s*$"
    r"|^\s*summary\s+of\s+results\s*$"
    # Known mis-routes NOT covered above (documented for the next pass,
    # to be addressed via alias / leaf-anchor work, not honest-null):
    # - ``羟赖氨酸`` (Hydroxylysine 20641-7) drifts to ``Lysine`` 20650-8
    #   because the 羟- prefix isn't bridged in zh aliases.
    # - ``Cryptosporidium spp`` drifts to 48059-0 Giardia+Crypto combined
    #   Ag instead of the single-organism 18756-7. Needs a combined-
    #   panel demote when the query names a single canonical genus.
    ,
    re.IGNORECASE,
)


def _concept_no_loinc_null(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Force-null for queries naming concepts LOINC doesn't enumerate
    (sleep efficiency, generic Z-score, imaging section header,
    parvalbumin food panel, FGFR3 autoantibody, sleep-time-fraction-
    by-posture). See :data:`_CONCEPT_NO_LOINC_QUERY_RE`.
    """
    if not _CONCEPT_NO_LOINC_QUERY_RE.search(query_text):
        return None
    n = int(np.asarray(cache["canonical"]).shape[0])
    return np.zeros(n, dtype=bool)


# Rapid-immunoassay POC sibling-aware drop. LOINC ``by Rapid
# immunoassay`` rows are point-of-care qualitative tests that
# surface in cosine top-N for general antibody/antigen queries
# (``乙肝e抗原`` → 75408-5 ``Hepatitis B virus e Ag [Presence] in
# Serum, Plasma or Blood by Rapid immunoassay``) and beat the
# canonical non-Rapid variant on raw cosine because the LCN
# matches more tokens. When the query carries no rapid / POC
# license, drop these rows — but only when a non-Rapid LOINC
# sibling exists in the same analyte family (method-stripped
# family key); otherwise the Rapid row is the only canonical
# option (NGAL 74099-3 / 74103-3 — both Rapid; killing them sends
# the picker to a wrong analyte).
_RAPID_IMMUNOASSAY_LCN_RE = re.compile(
    r"\bby\s+Rapid\s+immunoassay\b", re.IGNORECASE,
)


def _method_stripped_family_key(name: str) -> str:
    """Family key with ``by <Method>`` suffix stripped via
    :data:`_AB_METHOD_SUFFIX_RE`. Used to detect cross-method
    siblings — 75408-5 (Rapid) and 5191-2 (Immunoassay) share
    ``hepatitis b virus e ag in serum...`` after the strip."""
    return loinc_family_key(_AB_METHOD_SUFFIX_RE.sub("", name))


def _build_rapid_immunoassay_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows where (a) LCN carries ``by Rapid immunoassay``
    AND (b) a non-Rapid LOINC sibling exists with the same
    method-stripped family key. Cached on *cache*."""
    cached = cache.get("_loinc_rapid_immunoassay_mask")
    if cached is not None:
        return cached
    names = cache.get("names") or []
    n = int(np.asarray(cache["canonical"]).shape[0])
    # Pass 1: collect method-stripped family keys of non-Rapid rows.
    non_rapid_fams: set[str] = set()
    for nm in names:
        if not nm or _RAPID_IMMUNOASSAY_LCN_RE.search(nm):
            continue
        fk = _method_stripped_family_key(nm)
        if fk:
            non_rapid_fams.add(fk)
    # Pass 2: mark Rapid rows whose family has a non-Rapid sibling.
    mask = np.zeros(n, dtype=bool)
    n_rapid = 0
    n_rapid_solo = 0
    for i, nm in enumerate(names):
        if not nm or not _RAPID_IMMUNOASSAY_LCN_RE.search(nm):
            continue
        n_rapid += 1
        fk = _method_stripped_family_key(nm)
        if fk in non_rapid_fams:
            mask[i] = True
        else:
            n_rapid_solo += 1
    log.info(
        "Rapid immunoassay POC mask: %d / %d Rapid LOINC rows tagged "
        "(non-Rapid sibling exists); %d Rapid-only rows kept "
        "(NGAL-style sole variant)",
        int(mask.sum()), n_rapid, n_rapid_solo,
    )
    cache["_loinc_rapid_immunoassay_mask"] = mask
    return mask


def _rapid_immunoassay_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop ``by Rapid immunoassay`` LOINC rows when (a) the query
    carries no rapid / POC / 快速 / 床旁 license AND (b) a non-Rapid
    sibling exists in the same method-stripped analyte family.
    Targeted fix for HBeAg 75408-5 (Rapid Presence) → 31845-1
    (Units/volume in Serum) and similar.

    Sibling-aware so analytes whose only LOINC variant is Rapid
    (NGAL, some POC-only antigens) keep their Rapid row.

    Wired into :data:`_TOPK_PROBE_FALLBACK_FILTERS`.
    """
    if _qualitative_trigger_re().search(query_text):
        return None
    return ~_build_rapid_immunoassay_mask(cache)


# Panel-row demote. LOINC ``PANEL.*`` CLASS rows aggregate multiple
# analytes under one code (``Gas and Lactate panel``, ``Hepatic
# function panel``, ``BMP``). When the query names a single analyte
# (``血气分析,乳酸``), single-analyte rows are clinically correct;
# the panel only fits queries that name the panel itself
# (``组合/panel/profile/group/检查``).
_PANEL_QUERY_LICENSE_RE = re.compile(
    r"组合|套餐|检测组|检測組|panel|profile|group\b|battery"
    r"|パネル|プロファイル"                    # JA
    r"|패널|프로파일"                          # KO
    r"|перечень|панель",                       # RU
    re.IGNORECASE,
)


def _build_panel_class_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose CLASS axis starts with ``PANEL.`` —
    aggregate multi-analyte codes. Cached on *cache*."""
    cached = cache.get("_loinc_panel_class_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_loinc_panel_class_mask"] = mask
        return mask
    class_values: list[str] = axis_data["values"].get("CLASS") or []
    class_ridx: np.ndarray | None = axis_data["row_value_idx"].get("CLASS")
    if class_ridx is None:
        cache["_loinc_panel_class_mask"] = mask
        return mask
    panel_idxs = {
        i for i, v in enumerate(class_values)
        if v and v.upper().startswith("PANEL.")
    }
    if not panel_idxs:
        cache["_loinc_panel_class_mask"] = mask
        return mask
    for r in range(min(n, len(class_ridx))):
        ci = int(class_ridx[r])
        if ci >= 0 and ci in panel_idxs:
            mask[r] = True
    log.info(
        "panel class mask: %d / %d LOINC rows flagged "
        "(CLASS starts with PANEL.)",
        int(mask.sum()), n,
    )
    cache["_loinc_panel_class_mask"] = mask
    return mask


def _panel_demote_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop PANEL.* CLASS rows when the query has no panel-license
    marker (``组合 / panel / profile / group / battery / etc.``).
    Wired into :data:`_TOPK_PROBE_FALLBACK_FILTERS` so true panel
    queries (``肝功能组合``) fall back to plain cosine when no
    single-analyte alternative exists in the cosine pool.
    """
    if _PANEL_QUERY_LICENSE_RE.search(query_text):
        return None
    return ~_build_panel_class_mask(cache)


_MICRO_INCOMPATIBLE_CLASS_PREFIXES: tuple[str, ...] = (
    # Chemistry: Organic acids 55818-9, Butyrate 27169-2, CO2 14040-0,
    # D-Lactate 74436-7, Phosphate 88713-3 — same-stool-panel cosine
    # drag. Genova GI panel reports both chemistry markers (SCFA) and
    # microorganism counts on the same page, so the embedder pulls
    # ``Faecalibacterium prausnitzii`` (organism) toward ``Organic
    # acids in Stool`` (chemistry).
    "CHEM",
    "PANEL.CHEM",
)


def _build_micro_incompatible_class_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose CLASS axis falls in
    :data:`_MICRO_INCOMPATIBLE_CLASS_PREFIXES` — families that should
    never surface for an organism-leaf query (CHEM / SPEC / DOC /
    IO_OUT / ADMIN / ATTACH / HL7). Cached on *cache*."""
    cached = cache.get("_loinc_micro_incompatible_class_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_loinc_micro_incompatible_class_mask"] = mask
        return mask
    class_values: list[str] = axis_data["values"].get("CLASS") or []
    class_ridx: np.ndarray | None = axis_data["row_value_idx"].get("CLASS")
    if class_ridx is None:
        cache["_loinc_micro_incompatible_class_mask"] = mask
        return mask
    incompatible_idxs: set[int] = set()
    for i, v in enumerate(class_values):
        if not v:
            continue
        up = v.upper()
        for pfx in _MICRO_INCOMPATIBLE_CLASS_PREFIXES:
            if up == pfx or up.startswith(pfx + "."):
                incompatible_idxs.add(i)
                break
    if not incompatible_idxs:
        cache["_loinc_micro_incompatible_class_mask"] = mask
        return mask
    for r in range(min(n, len(class_ridx))):
        ci = int(class_ridx[r])
        if ci >= 0 and ci in incompatible_idxs:
            mask[r] = True
    log.info(
        "micro-incompatible class mask: %d / %d LOINC rows flagged "
        "(CLASS ∈ %s)",
        int(mask.sum()), n, _MICRO_INCOMPATIBLE_CLASS_PREFIXES,
    )
    cache["_loinc_micro_incompatible_class_mask"] = mask
    return mask


def _micro_chem_demote_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop CLASS=CHEM / PANEL.CHEM rows when the indicator leaf names
    a microorganism (``菌 / 球 / 杆 / 螺旋体 / 寄生虫 / 病毒 / 阿米巴
    / Bacterium sp``) via :data:`_MICROBIOLOGY_LEAF_RE`.

    Catches the Genova 2200 GI panel where Faecalibacterium /
    Akkermansia / Roseburia / Prevotella / Barnesiella organism
    queries drift to chemistry-panel rows (Organic acids 55818-9,
    Butyrate 27169-2, CO2 14040-0, D-Lactate 74436-7) — same-stool-
    panel cosine drag where SCFA markers and organism counts coexist
    on one Genova lab report.

    CLASS=MICRO, HEM/BC (leukocyte counts), SPEC (color / consistency),
    DOC, ALLERGY all survive — the drop targets chemistry-only.

    Silent when no microbiology-leaf marker matches. NOT wired into
    :data:`_TOPK_PROBE_FALLBACK_FILTERS` — for organism queries where
    LOINC genuinely has no matching MICRO row, null is the honest
    answer.
    """
    m = _SOURCE_PREFIX_RE.match(query_text)
    indicator = query_text[m.end():] if m else query_text
    leaf = indicator.rsplit("·", 1)[-1].strip()
    if not leaf:
        return None
    if not _MICROBIOLOGY_LEAF_RE.search(leaf):
        return None
    return ~_build_micro_incompatible_class_mask(cache)


# Chinese → canonical organism genus (and alternate-genus synonyms).
# Used by :func:`_organism_anchor_keep` to mask LOINC rows that do
# not name the same organism the query asks for.
#
# Build rule: each key is the Chinese organism name as written in the
# panel (genus, species, or shorthand). Value is a space-separated
# list of GENUS-level LCN tokens — never species epithets. Multi-
# token values list alternate-genus synonyms (``Bacteroides
# Phocaeicola`` for ``B. vulgatus`` after the 2020 reclassification,
# ``Clostridium Clostridioides`` for ``C. difficile`` post-2016,
# ``Trichomonas Pentatrichomonas`` for the GI panel ``毛滴虫``).
#
# Species epithets MUST NOT appear — bare ``nana`` matches the
# *S. pneumoniae* ``nanA`` gene; bare ``caninum`` matches *Neospora
# caninum* / *Toxocara caninum*; bare ``coli`` matches *E. coli* on
# *Balantidium coli* / *Entamoeba coli* queries. Use the canonical
# genus only.
#
# Add new entries when a clinical CN organism term surfaces in
# indicator data and the cosine resolver picks a wrong-genus LOINC
# row. Coverage need not be exhaustive — when a CN organism is absent
# from this table, the filter silently passes (returns None), so the
# only risk is "missed null", not "false null".
_CN_ORGANISM_CANONICAL: dict[str, str] = {
    # ─ Genova 2200 GI Effects panel (gut microbiome species/genera) ─
    # Many of these organisms have no LOINC enumeration. The filter
    # then nulls them out (correct) instead of falling onto a wrong-
    # genus row (Faecalibacterium → Organic acids / Clostridioides;
    # Akkermansia → Organic acids; Roseburia → Stools 10 hour).
    "普拉梭菌":           "Faecalibacterium",
    "嗜黏蛋白阿克曼氏菌":     "Akkermansia",
    "阿克曼氏菌":          "Akkermansia",
    "产甲酸草酸杆菌":       "Oxalobacter",
    "产气柯林斯氏菌":       "Collinsella",
    "柯林斯氏菌":          "Collinsella",
    "产气肠杆菌":          "Enterobacter",
    "肠杆菌":            "Enterobacter",
    "人结肠厌氧截短菌":      "Anaerotruncus",
    "马赛厌氧截短菌":       "Anaerotruncus",
    "厌氧截短菌":          "Anaerotruncus",
    "假黄瘤胃球菌":        "Pseudoflavonifractor",
    "单形拟杆菌":          "Bacteroides",
    "普通拟杆菌":          "Bacteroides Phocaeicola",  # post-2020 reclassification
    "拟杆菌":            "Bacteroides",
    "双歧杆菌":           "Bifidobacterium",
    "巴恩斯氏菌":          "Barnesiella",
    "布氏瘤胃球菌":         "Ruminococcus",
    "瘤胃球菌":           "Ruminococcus",
    "粪肠球菌":           "Enterococcus",
    "肠球菌":            "Enterococcus",
    "乳杆菌":            "Lactobacillus",
    "大肠杆菌":           "Escherichia",   # not Enterobacter (despite 肠杆菌 substring)
    "大肠埃希氏菌":         "Escherichia",
    "埃希氏菌":           "Escherichia",
    "普雷沃氏菌":          "Prevotella",
    "罗氏菌":            "Roseburia",
    "脱硫弧菌":           "Desulfovibrio",
    "迟钝脱硫弧菌":         "Desulfovibrio",
    "韦荣氏球菌":          "Veillonella",
    "臭气杆菌":           "Odoribacter",
    "芽孢杆菌":           "Bacillus",
    # ─ Parasites: amoebae ─
    "溶组织内阿米巴":        "Entamoeba",
    "迪斯帕内阿米巴":        "Entamoeba",
    "哈氏内阿米巴":         "Entamoeba",
    "波列基内阿米巴":        "Entamoeba",
    "结肠内阿米巴":         "Entamoeba",
    "内阿米巴":           "Entamoeba",
    "微小内蜒阿米巴":        "Endolimax",
    "内蜒阿米巴":          "Endolimax",
    "布氏嗜碘阿米巴":        "Iodamoeba",
    "嗜碘阿米巴":          "Iodamoeba",
    "脆弱双核阿米巴":        "Dientamoeba",
    "双核阿米巴":          "Dientamoeba",
    # ─ Parasites: other protozoa ─
    "贾第虫":            "Giardia",
    "微小隐孢子虫":         "Cryptosporidium",
    "人隐孢子虫":          "Cryptosporidium",
    "隐孢子虫":           "Cryptosporidium",
    "环孢子虫":           "Cyclospora",
    "等孢球虫":           "Cystoisospora Isospora",  # genus rename 2005
    "迈氏唇鞭毛虫":         "Chilomastix",
    "唇鞭毛虫":           "Chilomastix",
    "结肠小袋纤毛虫":        "Balantidium",
    "纤毛虫":            "Balantidium",
    "毛滴虫":            "Trichomonas Pentatrichomonas",
    "芽囊原虫":           "Blastocystis",
    # ─ Parasites: helminths (worms) ─
    "蛔虫":             "Ascaris",
    "鞭虫":             "Trichuris",
    "蛲虫":             "Enterobius",
    "钩口属":            "Ancylostoma",
    "板口属":            "Necator",
    "粪类圆线虫":          "Strongyloides",
    "圆线虫":            "Strongyloides",
    "菲律宾毛细线虫":        "Capillaria",
    "毛细线虫":           "Capillaria",
    # Cestodes (绦虫)
    "缩小膜壳绦虫":         "Hymenolepis",
    "微小膜壳绦虫":         "Hymenolepis",
    "膜壳绦虫":           "Hymenolepis",
    "犬复孔绦虫":          "Dipylidium",
    "复孔绦虫":           "Dipylidium",
    "阔节裂头绦虫":         "Diphyllobothrium",
    "裂头绦虫":           "Diphyllobothrium",
    "绦虫属":            "Taenia",
    # Trematodes (吸虫)
    "血吸虫":            "Schistosoma",
    "华支睾吸虫":          "Clonorchis",
    "后睾吸虫":           "Opisthorchis",
    "异形吸虫":           "Heterophyes",
    "次睾吸虫":           "Metagonimus",
    "片形吸虫":           "Fasciola",
    "布氏姜片吸虫":         "Fasciolopsis",
    "姜片吸虫":           "Fasciolopsis",
    "并殖吸虫":           "Paragonimus",
    # ─ Pathogenic / pre-existing zh_curated.tsv companions ─
    # (Subset of the curated alias bundle, repeated here so the
    # organism-anchor filter doesn't depend on bundle loading.)
    "幽门螺杆菌":          "Helicobacter",
    "沙门氏菌":           "Salmonella",
    "志贺氏菌":           "Shigella",
    "弯曲杆菌":           "Campylobacter",
    "白色念珠菌":          "Candida",
    "都柏林念珠菌":         "Candida",
    "念珠菌":            "Candida",
    "梭杆菌":            "Fusobacterium",
    "梭菌":             "Clostridium Clostridioides",  # 2016 reclassification
    "艰难梭菌":           "Clostridioides Clostridium",
    "肠胆毛滴虫":          "Pentatrichomonas Trichomonas",
    "贾第鞭毛虫":          "Giardia",
    "嗜热放线菌":          "Actinomycetes Actinomyces",
    "嗜肺军团菌":          "Legionella",
    "结核杆菌":           "Mycobacterium",
    "新型隐球菌":          "Cryptococcus",
    "组织胞浆菌":          "Histoplasma",
    "肺炎支原体":          "Mycoplasma",
    "解脲支原体":          "Ureaplasma",
    "解脲脲原体":          "Ureaplasma",
    "枯草杆菌":           "Bacillus",
    "球孢子菌":           "Coccidioides",
    "卡氏肺孢子虫":         "Pneumocystis",
    "肺孢子菌":           "Pneumocystis",
    "麻风杆菌":           "Mycobacterium",
    "弓形体":            "Toxoplasma",
    "弓形虫":            "Toxoplasma",
    "曲霉":             "Aspergillus",
    "根霉":             "Rhizopus",
    "毛霉":             "Mucor",
    "丝衣霉":            "Byssochlamys",
    "假丝酵母":           "Candida",
}


def _build_organism_anchor_mask(
    cache: dict, canonical: str,
) -> np.ndarray:
    """Tag LOINC rows whose LCN names the organism *canonical* via a
    word-boundary match on the genus token (first whitespace-split
    word) — and additionally any extra tokens listed (synonym genera
    like ``Phocaeicola`` for *Bacteroides vulgatus*).

    Species epithets are NOT used for matching on their own — bare
    ``nana`` would match the *S. pneumoniae* ``nanA`` gene; bare
    ``caninum`` would match *Neospora caninum* / *Toxocara caninum*.
    Genus-level anchoring keeps the filter precise: rows naming the
    same genus pass, all others drop.

    Word-boundary is regex-anchored (``\\b``) so ``Bacillus`` does
    not match ``Bacilli`` or ``Bacilliform``. Case-insensitive.
    Cached per canonical.
    """
    cache_key = f"_loinc_organism_anchor_mask_{canonical}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    tokens = [t for t in canonical.split() if t]
    if not tokens:
        cache[cache_key] = mask
        return mask
    # The first token is the genus (or primary genus). All remaining
    # tokens are alternate-genus synonyms (Phocaeicola for vulgatus,
    # Clostridioides for Clostridium difficile, Pentatrichomonas for
    # Trichomonas). Species epithets aren't in the canonical strings.
    pat = re.compile(
        r"\b(?:" + "|".join(re.escape(t) for t in tokens) + r")\b",
        re.IGNORECASE,
    )
    names = cache.get("names") or []
    for i, nm in enumerate(names):
        if nm and pat.search(nm):
            mask[i] = True
    cache[cache_key] = mask
    return mask


def _organism_anchor_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Restrict LOINC rows to those whose LCN names the same organism
    the indicator leaf asks for.

    Triggers only when the indicator leaf contains a CN organism key
    from :data:`_CN_ORGANISM_CANONICAL`. The longest matching key
    wins (so ``缩小膜壳绦虫`` matches the species-level entry, not
    just ``膜壳绦虫``).

    Force-null when no LOINC LCN contains any canonical token —
    LOINC genuinely lacks a code for the species (Faecalibacterium
    prausnitzii, Akkermansia muciniphila, Roseburia spp., Barnesiella
    spp., most gut-microbiome / GI parasite species). Cosine fallbacks
    in that regime always pick a wrong-genus row (Faecalibacterium →
    Clostridioides difficile; Akkermansia → Helicobacter pylori; etc.)
    that's clinically misleading.

    Silent when:
      - leaf doesn't carry a microbiology marker (handled by
        :func:`_micro_chem_demote_keep` family).
      - leaf doesn't match any known CN organism key (this filter is
        a deny-list, not an allow-list — unknown organisms pass).
    """
    m = _SOURCE_PREFIX_RE.match(query_text)
    indicator = query_text[m.end():] if m else query_text
    leaf = indicator.rsplit("·", 1)[-1].strip()
    if not leaf:
        return None
    # Toxin / metabolite carve-out — when the indicator names a microbial
    # toxin or mycotoxin (黄曲霉毒素 / botulinum toxin / endotoxin /
    # 霍乱弧菌毒素), the analyte is the toxin MOLECULE (small-molecule
    # hapten, often measured as antibody response), not the producing
    # organism. Organism-anchor would mis-restrict the candidate set
    # to fungal/bacterial rows and drop the correct hapten-Ab code.
    if re.search(r"毒素|毒\b|toxin|mycotoxin|endotoxin", leaf, re.IGNORECASE):
        return None
    # Longest-match wins so species-level entries override genus-level.
    best_key: str | None = None
    best_canonical: str | None = None
    for cn, en in _CN_ORGANISM_CANONICAL.items():
        if cn in leaf:
            if best_key is None or len(cn) > len(best_key):
                best_key = cn
                best_canonical = en
    if best_canonical is None:
        return None
    mask = _build_organism_anchor_mask(cache, best_canonical)
    # When the canonical organism has zero LOINC representation, the
    # mask is all-False — return as the keep mask (force-null), since
    # any fallback would be a cross-genus / cross-species pick.
    return mask


# Mean-time-aspect promotion. LOINC has dedicated ``^mean`` /
# ``^median`` TIME_ASPCT variants for trended analytes (``Glucose
# [Mass/volume] mean in Serum or Plasma`` 93791-2 vs the plain
# point-in-time 14749-6). Queries with ``平均 / mean / average / 均值``
# should prefer the mean variant when one exists.
_MEAN_QUERY_RE = re.compile(
    r"平均|均值|平均值|平均\s*值"
    r"|平均\s*血"                              # 平均血浆/血糖
    r"|\bmean\b|\baverage\b|\bavg\b"
    r"|\bestimated\s+average\b"
    r"|平均\s*グルコース|平均\s*血糖",           # JA
    re.IGNORECASE,
)


def _build_mean_time_aspect_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose TIME_ASPCT axis contains ``mean`` /
    ``median`` (``Stdy^mean``, ``RptPeriod^mean``, ``12H^mean``, …).
    Cached on *cache*."""
    cached = cache.get("_loinc_mean_time_aspect_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_loinc_mean_time_aspect_mask"] = mask
        return mask
    time_values: list[str] = axis_data["values"].get("TIME_ASPCT") or []
    time_ridx: np.ndarray | None = axis_data["row_value_idx"].get("TIME_ASPCT")
    if time_ridx is None:
        cache["_loinc_mean_time_aspect_mask"] = mask
        return mask
    mean_idxs = {
        i for i, v in enumerate(time_values)
        if v and ("mean" in v.lower() or "median" in v.lower())
    }
    if not mean_idxs:
        cache["_loinc_mean_time_aspect_mask"] = mask
        return mask
    for r in range(min(n, len(time_ridx))):
        ti = int(time_ridx[r])
        if ti >= 0 and ti in mean_idxs:
            mask[r] = True
    log.info(
        "mean time-aspect mask: %d / %d LOINC rows flagged "
        "(TIME_ASPCT contains mean/median)",
        int(mask.sum()), n,
    )
    cache["_loinc_mean_time_aspect_mask"] = mask
    return mask


# Duration analytes are per-event (apnea duration, awakening duration,
# snore duration). ``平均X持续时间`` clinically means "average duration
# per event over many events" — a data-aggregation concept LOINC
# doesn't encode. Without this exemption the ``mean_time_aspect``
# filter drops the canonical per-event duration row (60821-6 Apnea
# duration) and cosine falls onto unrelated ``^mean`` rows (``PAP
# usage 1 month mean``).
_DURATION_ANALYTE_QUERY_RE = re.compile(
    r"持续时间|持續時間|时长|時長|duration|time(?!\s+of\s+day)",
    re.IGNORECASE,
)


def _mean_time_aspect_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """When the query asks for a mean / average value AND LOINC has at
    least one ``^mean`` TIME_ASPCT row in cosine reach, restrict to
    mean / median TIME_ASPCT rows. Wired into
    :data:`_TOPK_PROBE_FALLBACK_FILTERS` so queries whose top-K
    contains no mean variant (LOINC has no ^mean code for the
    analyte) fall back to plain cosine.

    Duration-analyte exemption: when the query also carries a
    duration marker (``持续时间 / duration``), the ``平均`` refers
    to per-event-duration averaging — LOINC has per-event codes
    (60821-6 Apnea duration etc.) but no event-mean-duration variant.
    Skip the filter so cosine picks the per-event row.
    """
    if not _MEAN_QUERY_RE.search(query_text):
        return None
    if _DURATION_ANALYTE_QUERY_RE.search(query_text):
        return None
    return _build_mean_time_aspect_mask(cache)


# Semi-quantitative scale keep mask. Mirror of :func:`_mean_time_aspect_keep`
# but for the SCALE_TYP axis — when the query asks for the SemiQn /
# titer / grade variant (``D-二聚体半定量``, ``RPR titer``, ``HPV 半定量``),
# restrict the candidate pool to LOINC rows whose SCALE_TYP is one of
# ``SemiQn`` / ``OrdQn``. The legacy ``_build_scale_match_rank`` is a
# SOFT in-family rerank; this filter is a HARD drop that also penetrates
# the axes pipeline via :func:`_filter_axes_picks`. Without it, axes
# can return a Qn-row pick with higher cosine that beats the legacy's
# scale-tier-reranked SemiQn pick.
def _build_semiqn_scale_mask(cache: dict) -> np.ndarray:
    """Tag LOINC rows whose SCALE_TYP is SemiQn or OrdQn."""
    cached = cache.get("_loinc_semiqn_scale_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_loinc_semiqn_scale_mask"] = mask
        return mask
    scale_values: list[str] = axis_data["values"].get("SCALE_TYP") or []
    scale_ridx: np.ndarray | None = axis_data["row_value_idx"].get("SCALE_TYP")
    if scale_ridx is None:
        cache["_loinc_semiqn_scale_mask"] = mask
        return mask
    keep_idxs = {
        i for i, v in enumerate(scale_values)
        if v and v.lower() in ("semiqn", "ordqn")
    }
    if not keep_idxs:
        cache["_loinc_semiqn_scale_mask"] = mask
        return mask
    for r in range(min(n, len(scale_ridx))):
        ti = int(scale_ridx[r])
        if ti >= 0 and ti in keep_idxs:
            mask[r] = True
    log.info(
        "semiqn scale mask: %d / %d LOINC rows flagged "
        "(SCALE_TYP ∈ SemiQn / OrdQn)",
        int(mask.sum()), n,
    )
    cache["_loinc_semiqn_scale_mask"] = mask
    return mask


def _semiqn_scale_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """For queries carrying an explicit semi-quantitative marker
    (``半定量``, ``semi-quantitative``, ``titer``, ``滴度``, ``grade``),
    restrict candidates to LOINC rows whose SCALE_TYP is ``SemiQn`` or
    ``OrdQn``.

    Wired into :data:`_TOPK_PROBE_FALLBACK_FILTERS` so analytes with no
    SemiQn variant in cosine reach (rare assays where LOINC enumerates
    only Qn or Ord) fall back to plain cosine instead of nulling out.
    Mirrors the architecture of :func:`_mean_time_aspect_keep`.

    This is a HARD drop, where the in-family
    :func:`_build_scale_match_rank` was a SOFT rerank. The hard form is
    needed so axes-pipeline picks (which bypass the rerank) also get
    masked via :func:`_filter_axes_picks`.
    """
    if not _semiquantitative_trigger_re().search(query_text):
        return None
    return _build_semiqn_scale_mask(cache)


def _food_igg_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """For food-sensitivity / chemical-immune-reactivity queries
    (``食物免疫反应筛查测试·X``, ``化学免疫反应筛查·X``, ``麸质
    交叉反应食物·X``), enforce antibody-class matching.

    Two modes (mirror of :func:`_allergy_ige_keep`):
      1. **Query carries no Ig-class marker**: food sensitivity is
         IgG-mediated by clinical convention → drop IgE Ab rows
         (sibling-aware via :func:`_build_food_non_igg_mask`).
      2. **Query carries explicit Ig-class marker(s)** (``IgG+IgA``,
         ``IgM``, ``免疫球蛋白G``): drop rows with a non-matching Ig
         class. ``汞化合物 IgM`` ⇒ keep Mercury IgM Ab; for chemical /
         heavy-metal panels (see :data:`_CHEMICAL_IMMUNE_CONTEXT_RE`)
         use STRICT mode — drop Mercury IgE entirely rather than fall
         back across Ig classes, since the panel's diagnostic intent is
         class-specific. Empty mask ⇒ picker emits null.

    Silent when query carries no food-sensitivity context marker.
    """
    if not _FOOD_IGG_CONTEXT_RE.search(query_text):
        return None
    q_classes = _query_ig_classes(query_text)
    if q_classes:
        strict = bool(_CHEMICAL_IMMUNE_CONTEXT_RE.search(query_text))
        return _build_ig_class_match_mask(
            cache, frozenset(q_classes), strict=strict,
        )
    if _ALLERGY_IG_LICENSE_RE.search(query_text):
        # Methodology marker (histamine/leukotriene release, CAST-ELISA)
        # without specific Ig class — respect by going silent.
        return None
    return ~_build_food_non_igg_mask(cache)


def _predicted_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop LOINC rows whose display name carries ``predicted`` /
    ``expected`` / 预计 / 参考值 / etc. when the query carries no
    such reference-value marker.

    Resolves the FEV1/FVC vs FEV1/FVC Predicted (LOINC 19926-5 vs
    19925-7) sibling pair — they differ only by METHOD_TYP (empty vs
    ``Predicted``) and the cosine gap is ~0.0016 in Gemini's space,
    too narrow for the augment to trust. Queries that genuinely ask
    for the predicted variant (``预计 FEV1``, ``FEV1 reference``,
    ``Expected FVC``) license the family via :func:`query_licensed_families`
    and pass through unchanged — historical data still surfaces.
    """
    from .specificity import (
        _ensure_loinc_masks,
        query_licensed_families,
    )
    masks = _ensure_loinc_masks(cache)
    m = masks.get("predicted")
    if m is None:
        return None
    if "predicted" in query_licensed_families(query_text):
        return None
    return ~m


def _make_spec_mask_keep(family_key: str) -> _LoincKeepFilter:
    """Build a generic keep-filter that drops LOINC rows in a
    :data:`specificity.FAMILIES` mask when the query carries no marker
    licensing that family. Same boilerplate as
    :func:`_transfusion_subject_keep` and :func:`_predicted_keep` —
    those two are kept as standalone functions for case-study
    docstrings; the bulk of specificity families use this factory.

    Wired families (see ``_LOINC_FILTERS`` registration):

      - ``challenge_test`` — ``--N hour post 75g challenge`` etc.;
        prevents plain ``空腹血糖`` queries landing on post-load codes
        when no challenge marker is given.
      - ``baseline`` — ``--baseline`` qualifier rows; ``皮质醇`` query
        without ``基线 / basal`` shouldn't pick the baseline variant.
      - ``trough_peak`` — ``--trough`` / ``--peak`` drug-level rows;
        plain Vancomycin shouldn't land on a trough variant.
      - ``posture`` — ``--supine``/``standing``/etc. rows; bare
        Aldosterone shouldn't pick a posture-specific variant.
      - ``respiratory_phase`` — ``--pre/post bronchodilation``,
        ``on ventilator`` rows; the analog of ``predicted`` for the
        bronchodilator-state axis (resolves ``用力肺活量`` ↔ bare FVC
        vs the ``--pre bronchodilation`` variant).
      - ``dialysis`` — ``--pre/post dialysis`` rows.
      - ``dexamethasone`` — DST suppression-test rows.
      - ``time_of_day`` — ``--8AM specimen`` / morning-sample rows.
      - ``population_specific`` — ``--in newborn``/pediatric rows.
      - ``contrast_imaging`` — ``w contrast`` / ``gadolinium`` imaging
        rows.
      - ``any_dash`` — catchall for any ``--`` qualifier when query
        carries NO specificity marker from any family (markers union).

    The runtime contract matches every other filter: ``None`` means
    "no opinion" (pass through); a boolean ndarray is a row-keep
    mask. :func:`_compose_loinc_keep` AND-merges all returned masks.
    """
    def _keep(query_text: str, cache: dict) -> "np.ndarray | None":
        from .specificity import (
            _ensure_loinc_masks,
            query_licensed_families,
        )
        masks = _ensure_loinc_masks(cache)
        m = masks.get(family_key)
        if m is None:
            return None
        if family_key in query_licensed_families(query_text):
            return None
        return ~m
    _keep.__name__ = f"_{family_key}_keep"
    _keep.__qualname__ = _keep.__name__
    return _keep


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
#   - ``13C`` / ``14C`` / ``15N`` — isotope-labeled tracers (PET imaging,
#     urea breath tests). The trailing ``C`` / ``N`` / ``H`` is a
#     stable-isotope label, not an analyte marker. Without excluding
#     digit-adjacent letters, ``13C-尿素呼气试验`` requires every LOINC
#     row to carry a standalone ``C``, draining the H pylori pool
#     (whose names contain neither standalone ``C`` nor standalone
#     ``H`` — only ``Helicobacter pylori ... by urea breath test``).
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
_LETTER_ANALYTE_RE = re.compile(r"(?<![A-Za-z0-9])([A-Z])(?![A-Za-z0-9])")


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


_NONSPECIFIC_SPECIMEN_LICENSE_RE = re.compile(
    r"\bspecimen\b|\bsample\b|\bfood\b|\bfoodstuff\b|\bsupplement\b"
    r"|\bdietary supplement\b|\bvitamin (?:supplement|tablet|capsule)\b"
    r"|样品|标本|检体|食物|食品|补充剂|营养品|保健品"
    r"|樣品|標本|檢體|食物|食品|補充劑|營養品|保健品"
    r"|サンプル|検体|食品|サプリ"
    r"|시료|식품|보충제"
    r"|muestra|alimento|suplemento"
    r"|échantillon|aliment|supplément"
    r"|Probe|Lebensmittel|Nahrungsergänzung"
    r"|образец|пища|добавка",
    re.IGNORECASE,
)


def _build_nonspecific_specimen_mask(cache: dict) -> np.ndarray:
    """LOINC rows with SYSTEM='XXX' (generic Specimen) AND a same-COMPONENT
    peer with a non-XXX SYSTEM. True = drop unless query licenses XXX.

    Built once per cache via ``load_axis_centroids``'s
    ``row_components`` + ``row_value_idx['SYSTEM']``.

    Two-pass over LOINC corpus:
      1. Collect set of COMPONENTs that have any non-XXX SYSTEM peer.
      2. Mark every SYSTEM=XXX row whose COMPONENT is in that set.

    A row is NOT marked when XXX is the ONLY specimen LOINC ships for
    its COMPONENT (HPV DNA in Specimen, Vit A / E mass/mass, CD8/Lymph
    in Specimen) — those are the canonical code and must survive.
    """
    cached = cache.get("_nonspecific_specimen_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_nonspecific_specimen_mask"] = mask
        return mask
    sys_values: list[str] = axis_data["values"].get("SYSTEM") or []
    try:
        xxx_idx = sys_values.index("XXX")
    except ValueError:
        cache["_nonspecific_specimen_mask"] = mask
        return mask
    sys_arr: np.ndarray | None = axis_data["row_value_idx"].get("SYSTEM")
    components: list[str | None] | None = axis_data.get("row_components")
    if sys_arr is None or components is None:
        cache["_nonspecific_specimen_mask"] = mask
        return mask

    components_with_specific: set[str] = set()
    for r in range(n):
        c = components[r]
        if c and int(sys_arr[r]) >= 0 and int(sys_arr[r]) != xxx_idx:
            components_with_specific.add(c)
    for r in range(n):
        if int(sys_arr[r]) == xxx_idx:
            c = components[r]
            if c and c in components_with_specific:
                mask[r] = True
    cache["_nonspecific_specimen_mask"] = mask
    log.info(
        "nonspecific-specimen mask: %d LOINC rows flagged "
        "(SYSTEM=XXX with non-XXX peer in same COMPONENT)",
        int(mask.sum()),
    )
    return mask


def _nonspecific_specimen_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """Drop LOINC rows with SYSTEM='XXX' (``in Specimen``) when a
    same-COMPONENT peer with a specific specimen exists AND the query
    carries no specimen license.

    Lab queries (``钙 / 镁 / 磷 / 铁 / 维生素D3``) default to the
    canonical Ser/Plas / Bld / Urine form: bare ``钙`` resolves to
    ``Calcium in Serum or Plasma`` (17861-6) instead of the generic
    ``Calcium in Specimen`` (87477-6). Food / supplement queries that
    explicitly use ``样品 / specimen / food / 补充剂 / 营养品 / ...``
    license XXX and the filter steps aside.

    COMPONENTs with NO non-XXX peer (HPV DNA, Vit A / E, CD8/Lymph)
    are not in the mask — the generic-Specimen code IS the canonical
    one and must survive.
    """
    if _NONSPECIFIC_SPECIMEN_LICENSE_RE.search(query_text):
        return None
    mask = _build_nonspecific_specimen_mask(cache)
    if not mask.any():
        return None
    return ~mask


# ── Ratio-required (symmetric mirror of ``mass_ratio``) ────────────────
# When the query has an explicit pair-of-analyte ratio / fraction /
# percentage marker, drop LOINC rows that are NOT ratio-shaped. The
# ``mass_ratio`` specificity family already enforces the inverse
# (drop ratio rows when query carries no marker); this filter enforces
# the symmetric direction so queries like ``单核细胞比率`` / ``谷草/
# 谷丙`` / ``尿素氮/肌酐`` / ``高密度/总胆固醇`` route to the pair-
# form code (5905-5 / 1916-6 / 3097-3 / 9830-1) instead of the single-
# analyte count (742-7 Monocytes #) or organ-system panel (24324-6
# Hepatic function panel, 24362-6 Renal function panel).
#
# License markers are STRICTER than ``FAMILIES['mass_ratio'].markers``:
# bare ``比`` / ``率`` / ``相对`` / ``指数`` license the negative-side
# demote but are too noisy for a positive symmetric drop (``心率`` /
# ``速率`` / ``频率`` end in ``率`` but mean *rate*; ``指数`` matches
# HOMA-IR style single-COMPONENT indices). Only multi-char compound
# ratio markers license here.
#
# Note: bare ``X/Y`` glyph is NOT in the license. ``/`` carries two
# distinct clinical meanings in CN/EN reports — ``谷草/谷丙`` (ratio,
# divide) and ``白色念珠菌/都柏林念珠菌`` / ``卵巢/睾丸`` (alternative,
# "or"). Mass-filtering by a bare slash systematically wrecks the
# "or" cases (entire panels of alternatives default to one wrong-
# class ratio code). Pair-orientation handling moves to the final-
# pick post-rerank step in :func:`_loinc_picks_topk` where the
# overlay's ability to bridge both sides decides whether the slash
# actually means ratio.
_RATIO_REQUIRED_QUERY_RE = re.compile(
    # Latin compound markers
    r"\bratios?\b|\bfractions?\b|\bpercentages?\b|\bpercent\b"
    # CJK compound markers (≥2 chars, Simp + Trad + JP + KO)
    r"|比率|比值|比例|百分比|百分率|百分数|百分數|分率|分数|分數|占比"
    r"|對比|割合|비율|비례|분율|분획",
    re.IGNORECASE,
)

# LOINC PROPERTY axis pattern matching ratio / fraction / titer rows.
#
# Suffix forms cover all PROPERTY values ending in ``Rto`` (ratios:
# NRto/MRto/CRto/VRto/SRto/TRto/RelRto/PresRto/PPresRto/ColorRto/...)
# or ``Fr`` (fractions: NFr/MFr/AFr/VFr/CFr/SFr/EngFr/LenFr/AreaFr/
# CircFr/TimeFr/SatFr/VFrDiff/...) or ``.DF`` (dilution factor
# variants: NFr.DF/MFr.DF/...).
#
# Exact-match terms cover ``RelTime`` (INR-style single-COMPONENT
# ratios where the test name *is* the ratio but no ``X/Y`` shape
# appears in the LCN — INR 6301-6 is the canonical instance), bare
# ``DF`` (titers), and ``MoM`` (Multiples of Median, prenatal screening
# derived ratios).
#
# Deliberately EXCLUDES the bare ``Ratio`` property — 38 LCN rows use
# ``[Ratio]`` as a dimensionless-index unit on a single COMPONENT
# (Body mass index 39156-5, Platelet distribution width 51631-0,
# Fractional excretion of X, Lymphocyte proliferation stimulated by
# X, antibody avidity ratios). These are NOT pair-of-analyte ratios —
# treating them as such gives them an unwarranted picker bonus that
# steals queries like ``大血小板比例`` from the true pair-form code
# (Platelets Large/Platelets 97994-8, PROPERTY=NFr).
#
# Also EXCLUDES ``TimeFr`` (via negative lookbehind on the ``Fr$``
# branch). TimeFr rows are duty-cycle / usage-statistic style
# ("Percentage of time CGM device worn" 104637-4, "Inspiratory time
# percent" 75932-4) — not clinical fractions/ratios in the sense
# ``百分比`` / ``比值`` queries intend. Without the exclusion, a
# ``持续睡眠效率（百分比）`` query gets a +bonus on irrelevant
# "Percentage of time X" rows, dragging the pick off ``Sleep
# duration`` 93832-4 onto an unrelated TimeFr row.
_RATIO_PROPERTY_PAT = re.compile(
    r"(?:Rto|(?<!Time)Fr|\.DF)$|^(?:RelTime|DF|MoM)$"
)


# Score bonus applied to ratio-mask rows when the query carries an
# explicit ratio marker (比率/比值/百分比/ratio/percent/fraction).
# Matches the FhirAdapter-side ``_RATIO_GUARD_BONUS`` so both pipelines
# apply the same nudge magnitude. Just large enough to flip a true
# pair-form ratio over a same-analyte count/panel sibling within a
# ~0.05 cosine gap (``单核细胞比率`` Monocytes count 0.7889 vs
# Monocytes/Leukocytes 0.7826 — bonus puts the ratio at 0.8326),
# small enough that unrelated ratio rows can't beat a high-cosine
# non-ratio when LOINC genuinely lacks the requested ratio
# (``TH1/TH2 比值`` panel 0.78 vs unrelated ratio 0.65 + bonus = 0.69;
# ``丙酸盐百分比`` Organic acids in Stool raw 0.7600 vs unrelated bile
# acid SFr ratio raw 0.7186 + 0.04 bonus = 0.7586 — non-ratio wins).
# 0.04 is tuned to match the empirical gap distribution: true pair-
# form ratios beat their single-analyte sibling by < 0.04 cosine; the
# gap to unrelated analyte ratios is typically > 0.04.
_RATIO_PICK_BONUS: float = 0.04


def _build_loinc_ratio_mask(cache: dict) -> np.ndarray:
    """Boolean mask over LOINC corpus: True if the row's PROPERTY axis
    matches :data:`_RATIO_PROPERTY_PAT` OR the row matches the
    ``mass_ratio`` specificity family's name regex.

    The PROPERTY pattern is the source of truth — every pair-of-analyte
    ratio or fraction carries an ``Rto`` / ``Fr`` / ``.DF`` suffix in
    LOINC's IDF dimension, and single-COMPONENT ratio concepts (INR,
    BMI, MoM) carry one of the named ratio properties. The mass_ratio
    name-regex union is a belt-and-braces fallback for rows where the
    axis bundle is unavailable or the per-row PROPERTY metadata is
    missing.
    """
    cached = cache.get("_loinc_ratio_mask")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.zeros(n, dtype=bool)
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is not None:
        prop_values: list[str] | None = (
            axis_data.get("values", {}).get("PROPERTY")
        )
        prop_ridx: np.ndarray | None = (
            axis_data.get("row_value_idx", {}).get("PROPERTY")
        )
        if prop_values is not None and prop_ridx is not None:
            ratio_idx_set = {
                i for i, v in enumerate(prop_values)
                if _RATIO_PROPERTY_PAT.search(v)
            }
            if ratio_idx_set:
                ratio_arr = np.fromiter(ratio_idx_set, dtype=np.int32)
                mask = np.isin(prop_ridx, ratio_arr)
    # Union with mass_ratio name-pattern mask — catches rows whose
    # PROPERTY axis is missing/blank but whose LCN encodes a pair shape.
    from .specificity import _ensure_loinc_masks
    specs = _ensure_loinc_masks(cache)
    mr_mask = specs.get("mass_ratio")
    if mr_mask is not None and mr_mask.shape == mask.shape:
        mask = mask | mr_mask
    log.info(
        "ratio-row mask: %d / %d LOINC rows flagged "
        "(PROPERTY ∈ Rto/Fr/DF/Ratio/RelTime/MoM ∪ mass_ratio name pattern)",
        int(mask.sum()), n,
    )
    cache["_loinc_ratio_mask"] = mask
    return mask


def _ratio_required_keep(query_text: str, cache: dict) -> "np.ndarray | None":
    """When the query carries an explicit pair-of-analyte ratio marker,
    keep ONLY rows that are ratio-shaped. Symmetric mirror of the
    ``mass_ratio`` family's negative demote.

    Clinical contract: ``单核细胞比率`` is the Monocytes/Leukocytes
    fraction (5905-5 / 26485-3 / 71875-9), NOT the Monocytes count
    (742-7). ``谷草/谷丙`` is the AST/ALT ratio (1916-6), NOT the
    hepatic function panel (24324-6). ``尿素氮/肌酐`` is the BUN/Cr
    ratio (3097-3), NOT the renal function panel (24362-6).

    Registered in :data:`_TOPK_PROBE_FALLBACK_FILTERS`: when cosine
    top-10 contains no ratio rows (LOINC has no ratio code for the
    queried concept, e.g. ``TH1/TH2 比值`` where LOINC only has a
    panel of TH1 and TH2 cytokines), the filter steps aside so the
    closest non-ratio match survives. Without the fallback, queries
    like ``TH1/TH2 比值`` would null-out — wrong, since LOINC has no
    better answer than the panel.
    """
    if not _RATIO_REQUIRED_QUERY_RE.search(query_text):
        return None
    mask = _build_loinc_ratio_mask(cache)
    if not mask.any():
        return None
    return mask


# ── Ratio orientation (X/Y numerator-denominator alignment) ────────────
# When a query carries an explicit ``X/Y`` glyph pair, the picked LOINC
# row's COMPONENT axis should be ``X/Y``, not ``Y/X``. Pure cosine
# regularly picks the reversed-orientation variant because the LCN
# tokens are identical — only word order differs, which the embedder
# under-weights (cosine for ``谷草/谷丙`` lands ALT/AST 16325-3 at 0.6898
# while AST/ALT 1916-6 sits 0.7-0.07 below at 0.6835).
#
# Short-form CN clinical abbreviations used in ratio queries. The
# bundled ``aliases/zh.tsv`` covers FULL clinical names (``谷草转氨酶``
# → AST) but ratio queries usually elide the suffix (``谷草/谷丙``).
# This is a normalization layer for orientation matching — each CN key
# maps to a tuple of canonical-form token strings that appear in the
# matching LOINC ``COMPONENT`` axis value.
#
# Limited deliberately to common CN report abbreviations. NOT a per-case
# patch table — every entry here applies to any X/Y query that contains
# the key (longest-prefix match), not just the reported test case.
_RATIO_SHORTFORM_OVERLAY: dict[str, tuple[str, ...]] = {
    # Liver enzymes (most common ratio in clinical chemistry)
    "谷草": ("Aspartate aminotransferase",),
    "谷丙": ("Alanine aminotransferase",),
    "谷酰转肽": ("Gamma glutamyl transferase",),
    "谷氨酰转肽": ("Gamma glutamyl transferase",),
    # Lipids
    "高密度": ("Cholesterol in HDL", "HDL"),
    "低密度": ("Cholesterol in LDL", "LDL"),
    "总胆固醇": ("Cholesterol.total", "Total cholesterol"),
    "甘油三酯": ("Triglyceride",),
    # Renal
    "尿素氮": ("Urea nitrogen", "Urea"),
    "尿素": ("Urea nitrogen", "Urea"),
    "肌酐": ("Creatinine",),
    "尿酸": ("Urate",),
    # Differentials
    "中性粒": ("Neutrophils",),
    "淋巴": ("Lymphocytes",),
    "单核": ("Monocytes",),
    "嗜酸": ("Eosinophils",),
    "嗜碱": ("Basophils",),
    "白细胞": ("Leukocytes",),
    "红细胞": ("Erythrocytes",),
    "血小板": ("Platelets",),
    # Proteins
    "白蛋白": ("Albumin",),
    "球蛋白": ("Globulin",),
    "前白蛋白": ("Prealbumin",),
    "总蛋白": ("Protein",),
}


_RATIO_PAIR_QUERY_RE = re.compile(
    r"(?P<head>(?:[一-鿿]{2,}|[A-Za-z][\w-]{2,}))"
    r"\s*[/／]\s*"
    r"(?P<tail>(?:[一-鿿]{2,}|[A-Za-z][\w-]{2,}))"
)


def _ratio_side_tokens(side: str) -> tuple[set[str], bool]:
    """Resolve one half of a ratio query to canonical-EN tokens.

    CJK side: longest-prefix lookup in :data:`_RATIO_SHORTFORM_OVERLAY`.
    Latin side: the substring itself (already in LOINC's vocabulary).
    Returns ``(tokens, has_mapping)`` — when ``has_mapping`` is False
    the caller should skip the orientation check rather than risk a
    false drop.
    """
    s = side.strip()
    if not s:
        return set(), False
    if any('一' <= ch <= '鿿' for ch in s):
        for key in sorted(_RATIO_SHORTFORM_OVERLAY.keys(), key=lambda x: -len(x)):
            if key in s:
                return set(_RATIO_SHORTFORM_OVERLAY[key]), True
        return set(), False
    return {s}, True


def _build_loinc_ratio_orientations(
    cache: dict,
) -> "list[tuple[str, str] | None]":
    """For each corpus row, return ``(head, tail)`` from the row's
    COMPONENT axis split on the FIRST ``/``, or ``None`` if the row's
    COMPONENT is missing or doesn't carry a pair shape.

    Uses ``axis_data['row_components']`` — LOINC's authoritative pair
    encoding (``Aspartate aminotransferase/Alanine aminotransferase``,
    ``Cholesterol in HDL/Cholesterol.total``, ``Apolipoprotein B/
    Apolipoprotein A-I``). Splitting on the first ``/`` correctly
    delimits the pair: nested ``in`` particles (``Cholesterol in HDL``)
    survive on the correct side; subsequent ``/`` glyphs (rare, e.g.
    multi-component panels) get folded into the tail and won't false-
    match against any short-form overlay token.
    """
    cached = cache.get("_loinc_ratio_orientations")
    if cached is not None:
        return cached
    n = int(np.asarray(cache["canonical"]).shape[0])
    out: list[tuple[str, str] | None] = [None] * n
    from .axis import load_axis_centroids
    axis_data = load_axis_centroids(cache)
    if axis_data is None:
        cache["_loinc_ratio_orientations"] = out
        return out
    components = axis_data.get("row_components") or []
    n_pairs = 0
    for r in range(min(n, len(components))):
        c = components[r]
        if not c or "/" not in c:
            continue
        head, _, tail = c.partition("/")
        head, tail = head.strip(), tail.strip()
        if head and tail:
            out[r] = (head, tail)
            n_pairs += 1
    log.info(
        "ratio-orientation index: %d / %d LOINC rows have ``X/Y`` COMPONENT",
        n_pairs, n,
    )
    cache["_loinc_ratio_orientations"] = out
    return out


def _ratio_orientation_keep(
    query_text: str, cache: dict,
) -> "np.ndarray | None":
    """Drop ratio rows whose COMPONENT orientation is reversed
    relative to the ``X/Y`` query.

    ``谷草/谷丙`` (AST/ALT intent) — drop 16325-3 (ALT/AST), keep
    1916-6 (AST/ALT). ``高密度/总胆固醇`` (HDL/Total intent) — drop
    32309-7 (Total/HDL), keep 9830-1 (HDL/Total).

    Sibling-aware: only drops a row when its COMPONENT matches the
    query in REVERSED orientation AND not in correct orientation.
    Rows whose COMPONENT pair has no token overlap with either query
    side are left alone (un-involved pairs the filter has no opinion
    on).

    Silent when either query side can't be bridged to canonical EN
    tokens (e.g. obscure CJK pair not in the short-form overlay) —
    refusing to act on partial information is preferable to dropping
    every row that happens to share one analyte token with the query.
    """
    m = _RATIO_PAIR_QUERY_RE.search(query_text)
    if not m:
        return None
    q_head_toks, h_ok = _ratio_side_tokens(m.group("head"))
    q_tail_toks, t_ok = _ratio_side_tokens(m.group("tail"))
    if not (h_ok and t_ok and q_head_toks and q_tail_toks):
        return None
    orientations = _build_loinc_ratio_orientations(cache)
    n = int(np.asarray(cache["canonical"]).shape[0])
    mask = np.ones(n, dtype=bool)
    n_reversed = 0
    n_uninvolved = 0
    qh_lower = [t.lower() for t in q_head_toks]
    qt_lower = [t.lower() for t in q_tail_toks]
    # Two-pass: first find any correct-orientation row to confirm LOINC
    # ships the pair in the right direction; without such an anchor we
    # leave uninvolved rows alone (rare-pair fallback). Reversed rows
    # are always dropped when correct anchor exists.
    has_correct_anchor = False
    correct_flags = np.zeros(n, dtype=bool)
    reversed_flags = np.zeros(n, dtype=bool)
    for r in range(n):
        orient = orientations[r]
        if orient is None:
            continue
        rh_l = orient[0].lower()
        rt_l = orient[1].lower()
        correct = (
            any(t in rh_l for t in qh_lower)
            and any(t in rt_l for t in qt_lower)
        )
        reversed_match = (
            any(t in rt_l for t in qh_lower)
            and any(t in rh_l for t in qt_lower)
        )
        if correct:
            correct_flags[r] = True
            has_correct_anchor = True
        if reversed_match and not correct:
            reversed_flags[r] = True
    if not has_correct_anchor:
        # LOINC doesn't ship the pair — let other layers decide rather
        # than null-out every ratio row (some niche pairs have only
        # reversed-orientation rows in LOINC).
        return None
    for r in range(n):
        orient = orientations[r]
        if orient is None:
            continue
        if correct_flags[r]:
            continue
        if reversed_flags[r]:
            mask[r] = False
            n_reversed += 1
            continue
        # Uninvolved ratio row (mentions neither query analyte in the
        # right slot, nor the wrong slot). Drop — the user specified a
        # concrete pair and we have at least one correct-orientation
        # anchor, so any other ratio code is a wrong-pair noise.
        mask[r] = False
        n_uninvolved += 1
    if n_reversed or n_uninvolved:
        log.info(
            "ratio orientation: dropped %d reversed + %d uninvolved ratio "
            "rows for query ``%s``", n_reversed, n_uninvolved, query_text,
        )
    return mask


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


# Body-axis contradiction filter — drop LOINC rows whose anatomical
# side / level label contradicts the query (left/right, upper/lower,
# etc.). Symmetric POS/NEG siblings differ by ~0.005 cosine in
# Gemini's space — too small to trust over source/abbrev noise — so
# we resolve axes deterministically. Imported here directly (no
# pipeline-side wrapper) because the module exports a keep-mask
# function with the exact ``(query_text, cache) -> ndarray|None``
# shape :data:`_LOINC_FILTERS` expects. See :mod:`.laterality`.
from .laterality import axis_filter_keep as _axis_filter_keep


_LOINC_FILTERS: list[tuple[str, _LoincKeepFilter]] = [
    # ── per-family hard-drop filters (standalone, case-study docstrings)
    ("transfusion_subject", _transfusion_subject_keep),
    ("analyte_concept",     _analyte_concept_keep),
    ("coag_factor_activity", _coag_factor_activity_keep),
    ("letter_analyte",      _letter_analyte_keep),
    ("hpv_serology",        _hpv_serology_keep),
    ("challenge_time",      _challenge_time_keep),
    ("post_meal",           _post_meal_keep),
    ("intake_recall",       _intake_recall_keep),
    ("nonspecific_specimen", _nonspecific_specimen_keep),
    ("xxx_challenge",       _xxx_challenge_keep),
    ("explicit_dose",       _explicit_dose_keep),
    ("allergy_ige",         _allergy_ige_keep),
    ("chemical_irs_no_loinc", _chemical_irs_no_loinc_null),
    ("concept_no_loinc",    _concept_no_loinc_null),
    ("rapid_immunoassay",   _rapid_immunoassay_keep),
    ("cn_analyte",          _cn_analyte_disambiguate_keep),
    ("nkt_vs_nk",           _nkt_vs_nk_keep),
    ("tcr_subset_demote",   _tcr_subset_demote_keep),
    ("organic_acid",        _organic_acid_disambiguate_keep),
    ("bicarbonate",         _bicarbonate_disambiguate_keep),
    ("panel_demote",        _panel_demote_keep),
    ("micro_chem_demote",   _micro_chem_demote_keep),
    ("organism_anchor",     _organism_anchor_keep),
    ("mean_time_aspect",    _mean_time_aspect_keep),
    ("semiqn_scale",        _semiqn_scale_keep),
    ("food_igg",            _food_igg_keep),
    ("sleep_focus",         _sleep_focus_keep),
    ("sleep_analyte",       _sleep_analyte_keep),
    ("wake_duration",       _wake_duration_keep),
    ("psg_no_loinc_extras", _psg_no_loinc_extras_keep),
    ("per_hour",            _per_hour_keep),
    ("source_specimen",     _source_specimen_keep),
    ("role_identifier",     _role_identifier_null),
    ("section_demote",      _section_demote_keep),
    ("specimen_acceptable", _specimen_acceptable_demote_keep),
    ("doc_ontology_demote", _doc_ontology_demote_keep),
    ("lipoprotein_nonspecific_demote", _lipoprotein_nonspecific_demote_keep),
    ("cn_no_loinc_null",    _cn_no_loinc_null),
    ("predicted",           _predicted_keep),
    # ── specificity-mask families wired via factory; same shape as
    #    _predicted_keep, see :func:`_make_spec_mask_keep` for docstring
    ("challenge_test",      _make_spec_mask_keep("challenge_test")),
    ("baseline",            _make_spec_mask_keep("baseline")),
    ("trough_peak",         _make_spec_mask_keep("trough_peak")),
    ("posture",             _make_spec_mask_keep("posture")),
    ("respiratory_phase",   _make_spec_mask_keep("respiratory_phase")),
    ("dialysis",            _make_spec_mask_keep("dialysis")),
    ("dexamethasone",       _make_spec_mask_keep("dexamethasone")),
    ("time_of_day",         _make_spec_mask_keep("time_of_day")),
    ("population_specific", _make_spec_mask_keep("population_specific")),
    ("contrast_imaging",    _make_spec_mask_keep("contrast_imaging")),
    ("factor_substitution", _make_spec_mask_keep("factor_substitution")),
    ("particle_length",     _make_spec_mask_keep("particle_length")),
    ("mass_ratio",          _make_spec_mask_keep("mass_ratio")),
    ("detection_limit",     _make_spec_mask_keep("detection_limit")),
    # Note: ratio handling is NOT a filter:
    #   * Explicit ratio markers (比率/比值/百分比/ratio/percent/fraction)
    #     apply a soft :data:`_RATIO_PICK_BONUS` to the ratio mask
    #     inside :func:`_loinc_picks_topk` Stage 0.5. Soft so concepts
    #     LOINC doesn't enumerate as a ratio (TH1/TH2 panel, sleep
    #     efficiency, propionate %) keep their high-cosine non-ratio
    #     match instead of getting null-outed.
    #   * Pair orientation (correct vs reversed ``X/Y`` numerator) runs
    #     as a post-rerank promote step in
    #     :func:`_loinc_picks_topk._finalize`. Only the final pick step
    #     considers the slash, so bare ``X/Y`` queries whose two sides
    #     cannot be bridged to canonical EN tokens (alternative species
    #     ``白色念珠菌/都柏林念珠菌``, anatomy ``卵巢/睾丸``, section names
    #     ``小麦/麸质蛋白组反应性与自身免疫·...``) fall through to the
    #     cosine top-1 unchanged.
    ("any_dash",            _make_spec_mask_keep("any_dash")),
    ("treatment_goal",      _make_spec_mask_keep("treatment_goal")),
    # ── axis-based contradiction filter (laterality / upper-lower)
    ("axis_filter",         _axis_filter_keep),
]


# Filters that fall back to top-K cosine ordering when their mask
# would eliminate every LOINC row in the top-10 candidate set. Used
# for filters whose row-flag heuristic is broad enough that an
# unforeseen query family could legitimately want rows in the mask
# (e.g. ``analyte_concept`` can't enumerate every Ag/Ab token
# polarity; ``any_dash`` covers all 6599 ``--``-bearing rows, some of
# which are the correct top-1 for niche queries; ``contrast_imaging``
# at 1502 rows includes routine modalities where ``contrast`` is
# baked into the name even without a query-side marker;
# ``letter_analyte`` over-matches when the query's standalone letter
# is a non-analyte token — protein subunit (``尿素酶A`` → urease
# subunit A, not "A Ab"), antibody class buried in ``IgM``
# (``免疫球蛋白M`` ≠ "M Ab"), or Roman numeral (``β2糖蛋白I`` —
# Roman ``I`` for "type 1", not "I Ab"). The top-K probe lets these
# slip through to cosine when the cosine pool clearly doesn't
# contain any standalone-letter LOINC row).
_TOPK_PROBE_FALLBACK_FILTERS = frozenset({
    "analyte_concept",
    # Soft prior: a factor whose activity sibling is out of cosine reach
    # (only its Ag row recalled) should fall back to that Ag row rather
    # than null out.
    "coag_factor_activity",
    "any_dash",
    "contrast_imaging",
    "letter_analyte",
    # ``panel_demote`` is a soft prior: some single-analyte queries
    # genuinely have only panel codes in LOINC (e.g. comprehensive
    # cytokine panels with no individual-analyte rows). Top-K probe
    # lets those fall through to cosine.
    "panel_demote",
    # ``mean_time_aspect`` is a soft prior: ``平均血浆葡萄糖`` wants
    # a ^mean variant but only specific analytes (Glucose, BP, …)
    # have one. When the top-K has no ^mean variant in cosine reach
    # (the analyte has only point-in-time codes), fall back to plain
    # cosine instead of nulling out.
    "mean_time_aspect",
    # ``semiqn_scale`` is a soft prior: drops Qn/Ord rows for ``半定量``
    # queries. Some analytes have no SemiQn variant in LOINC (rare D-
    # dimer specimen variants, niche antibody titers); top-K probe lets
    # those fall back to plain cosine instead of nulling out.
    "semiqn_scale",
    # ``food_igg`` mirrors ``allergy_ige``: drops IgE-Ab rows for food-
    # sensitivity queries. But niche foods/gums often have only IgE
    # rows in LOINC (no IgG sibling exists for Tragacanth, Carrageenan,
    # Guar gum, Xanthan, Wild Rice, Tilapia specifically, etc.). When
    # cosine top-10 is all IgE Ab, the food simply has no IgG variant
    # in LOINC — falling back to IgE is the honest answer.
    "food_igg",
    # ``source_specimen`` is a soft prior derived from the CSV source
    # column. Most cross-specimen lookups carry the right token in the
    # indicator itself (``血生化 · 尿肌酐清除率``) and that token has
    # already pulled cosine into the cross-specimen pool by the time
    # this filter sees the query. Top-K probe protects those.
    "source_specimen",
    # ``rapid_immunoassay`` is a soft prior: drops ``by Rapid
    # immunoassay`` POC rows when the query has no rapid / POC /
    # 快速 / 床旁 license. Some analytes only have a Rapid variant
    # in LOINC for a given specimen (rare); top-K probe lets those
    # fall through.
    "rapid_immunoassay",
})

# ``doc_ontology_demote`` is intentionally NOT in the probe-fallback
# set — for body-part queries whose cosine top-10 is entirely DOC.
# ONTOLOGY (``耳鼻喉科·耳`` → 8 ENT note variants), the real Physical-
# finding / History sibling (10195-6 / 10169-1) lives deeper in cosine.
# Strict mode forces the picker to scan past top-10 into the H&P.PX /
# H&P.HX pool. ``specimen_acceptable`` is likewise strict — only 3 rows
# in the bundle, every drop is intentional.

# Note: ``sleep_focus`` is intentionally NOT in the top-K fallback set.
# Sleep queries with multi-axis breakdowns (``睡眠阶段·活动·持续时间``)
# can have cosine top-10 entirely populated by lexical-collision
# rows — every ``QRS duration in lead X`` matches ``...持续时间``
# through the embedded ``duration`` token. Falling back to cosine in
# that case would defeat the filter; strict mode forces the picker
# to look past the top-10 window into the real sleep-measurement
# pool (which exists in LOINC, just at lower cosine rank).


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
    # Filters whose mask is dropped when a stronger analyte-anchor
    # already restricted the candidate set. Analyte-level specificity
    # (``核基质蛋白-22`` ⇒ COMPONENT="Nuclear matrix protein 22") wins
    # over source-based routing (``肿瘤标志物`` ⇒ Ser/Plas family),
    # because the analyte may only exist in a specimen outside the
    # source's pool (NMP-22 lives only in Urine). Same for the
    # nonspecific-specimen filter and source-implied scale.
    _ANCHOR_OVERRIDES = {"source_specimen", "nonspecific_specimen"}
    cn_analyte_fired = False
    for name, flt in _LOINC_FILTERS:
        m = flt(query_text, cache)
        if m is None:
            continue
        if name in _TOPK_PROBE_FALLBACK_FILTERS and topk_idx is not None:
            if not m[topk_idx].any():
                continue
        if name == "cn_analyte":
            cn_analyte_fired = True
        if cn_analyte_fired and name in _ANCHOR_OVERRIDES:
            continue
        keep = m if keep is None else (keep & m)

    # Chemical-immune analyte-anchor null: when the query is a chemical
    # immune-reactivity panel with explicit non-IgE class AND the
    # cosine top-1 (pre-filter) LOINC row's analyte family has no
    # class-matching variant in LOINC, force null. Catches the
    # ``汞化合物 IgM`` / ``混合重金属 IgG+IgA`` regime where LOINC has
    # only IgE rows for heavy metals — without this, the picker either
    # surfaces the wrong-class Mercury IgE (lenient mode) or a cross-
    # analyte Isocyanate IgM (strict mode). Both clinically wrong.
    #
    # The check runs AFTER filter composition (the strict mask already
    # dropped wrong-class rows); this is the second guard that says
    # "and if the actual queried analyte has no class match either,
    # don't fall back to any cross-analyte row — null is correct".
    if (
        sims_row is not None
        and loinc_idxs is not None
        and len(loinc_idxs) > 0
        and _CHEMICAL_IMMUNE_CONTEXT_RE.search(query_text)
    ):
        q_classes = _query_ig_classes(query_text)
        # Restrict to non-IgE classes — IgE chemical-immune queries
        # (allergic-style) DO have heavy-metal IgE rows in LOINC.
        if q_classes and q_classes - {"E"}:
            family_index = cache.get("_family_index")
            if family_index is not None:
                _, row_key, family_to_rows, _ = family_index
                pre_top1 = int(loinc_idxs[
                    int(np.argmax(sims_row[loinc_idxs]))
                ])
                fam = row_key[pre_top1] if pre_top1 < len(row_key) else ""
                if fam:
                    names = cache.get("names") or []
                    has_class_match = False
                    for r in family_to_rows.get(fam, ()):
                        nm = names[r] if r < len(names) else ""
                        if not nm:
                            continue
                        mm = _ANY_IG_AB_RE.search(nm)
                        if mm and mm.group(1).upper() in q_classes:
                            has_class_match = True
                            break
                    if not has_class_match:
                        n = int(np.asarray(cache["canonical"]).shape[0])
                        return np.zeros(n, dtype=bool)
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
    system_match_rank: np.ndarray | None = None,
    cache: dict | None = None,
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

    Stage 1.5 (predicted base-sibling promotion, if *cache* and
    *family_index* given): when the ``predicted`` filter dropped the
    unfiltered cosine top-1 (e.g. ``FEV1 Predicted``), find a non-
    predicted sibling sharing the predicted-stripped family_key
    (``FEV1`` → family_key ``fev``) inside the cosine window and
    surface it at position 0. Without this, the picker falls through
    to whichever row had the next-highest cosine, which on Gemini
    routinely picks a tokenization-twin from a different analyte
    family (``FEV1 Predicted`` → ``FEV.5``, cosine 0.787 vs FEV1 base
    0.779) instead of the natural base reading. Skipped when the query
    licenses ``predicted`` (the filter is silent — base sibling already
    competes on raw cosine, no recovery needed).

    Stage 0.5 (ratio-marker soft bonus, if *cache* given): when the
    query carries an explicit ratio / fraction / percentage marker
    (``比率``, ``比值``, ``百分比``, ``ratio``, ``percent``, ``fraction``,
    ...), add :data:`_RATIO_PICK_BONUS` to the cosine of every row in
    the ratio mask (PROPERTY ∈ Rto/Fr/DF/RelTime/MoM ∪ mass_ratio
    name pattern). Replaces the earlier hard ``ratio_required`` filter
    — that filter null-outed concepts LOINC doesn't enumerate as a
    ratio (TH1/TH2 panel, sleep efficiency, propionate %) by dropping
    the closest non-ratio match. Soft bonus lets cosine retain
    primacy: when LOINC has a true pair-form variant for the analyte
    its cosine is close enough that ~0.05 flips it (单核细胞比率 →
    Monocytes/Leukocytes [Fr]); when it doesn't, the high-cosine
    non-ratio survives (TH1/TH2 → panel 107613-2). Bonus is applied
    to a LOCAL copy of *sims_row*; the caller's score reporting sees
    the raw cosine.
    """
    # Stage 0.5 — ratio-marker soft bonus. See docstring.
    if cache is not None and _RATIO_REQUIRED_QUERY_RE.search(query_text):
        ratio_mask = _build_loinc_ratio_mask(cache)
        if ratio_mask.shape == sims_row.shape and ratio_mask.any():
            sims_row = sims_row.copy()
            sims_row[ratio_mask] += _RATIO_PICK_BONUS

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

    # Stage 1.5 — compute the predicted base-sibling promotion target
    # (see docstring). Applied as a final swap at every return point so
    # downstream stages (scale rerank, family rerank, digit filter)
    # can't undo it. Stage 3's ``passes_digit`` rule in particular would
    # demote ``FEV1`` (row_digits={1}) when ``query_analyte_digit`` can't
    # extract the ``1`` from ``第一秒用力呼气容积`` — the promotion is the
    # authoritative anchor and must outlast that rule.
    promoted_row: int | None = None
    if (
        cache is not None
        and family_index is not None
        and keep_mask is not None
        and topN_picks
    ):
        sims_l = sims_row[loinc_idxs]
        pre_top = int(loinc_idxs[int(np.argmax(sims_l))])
        if not keep_mask[pre_top]:
            from .specificity import _ensure_loinc_masks
            pred_mask = _ensure_loinc_masks(cache).get("predicted")
            if pred_mask is not None and pred_mask[pre_top]:
                pre_name = (cache.get("names") or [""])[pre_top] or ""
                base_fam = loinc_family_key(
                    _PREDICTED_TOKEN_RE.sub(" ", pre_name)
                )
                if base_fam:
                    _, _, family_to_rows, _ = family_index
                    topN_set = set(topN_picks)
                    cached_names = cache.get("names") or []
                    # Bare-name only: no ``--`` qualifier tail. Otherwise
                    # the promotion would just swap one over-specific
                    # qualifier (``predicted``) for another (``--pre
                    # bronchodilation``, ``--post exercise``) — at least
                    # as bad as the dropped row. When the family has no
                    # clean bare sibling (e.g. ``FEF 25-75%`` only exists
                    # in ``--pre/post bronchodilation`` variants in
                    # LOINC), let the pure cosine top-2 fallback handle
                    # it instead of surfacing a misleading qualified row.
                    surviving = [
                        int(r) for r in family_to_rows.get(base_fam, ())
                        if (
                            r in topN_set
                            and not pred_mask[r]
                            and "--" not in (cached_names[r] or "")
                        )
                    ]
                    if surviving:
                        promoted_row = max(surviving, key=lambda r: sims_row[r])

    def _apply_promote(picks: list[int]) -> list[int]:
        if promoted_row is None or not picks or picks[0] == promoted_row:
            return picks
        if promoted_row in picks:
            return [promoted_row] + [r for r in picks if r != promoted_row]
        return ([promoted_row] + picks)[:top_k]

    # Stage 4 (SYSTEM_PREFERENCE cross-specimen rerank): after the
    # within-family rerank settles on a winner, look for a same-analyte
    # row (specimen-stripped family-key match) in ``topN_picks`` with a
    # strictly better ``_SYSTEM_PREFERENCE`` tier and swap it to slot 0.
    # Why a separate stage: ``loinc_family_key`` keeps the specimen in
    # the key (``calcium in blood`` vs ``calcium in serum or plasma``
    # are distinct families) so stages 2-3 never put specimen variants
    # in the same pool. Stripping the specimen here is scoped to the
    # tier-promote rerank only, avoiding the IgE↔IgG / lactate Capillary
    # false-positives that a broader ``_shares_analyte`` widening
    # caused. Bare-key match (not prefix) avoids unrelated-prefix hits
    # (``beef ige ab`` ≠ ``beef igg ab`` — different analytes).
    #
    # PROPERTY equality guard (when *cache* is available): require the
    # candidate row's PROPERTY axis value to match ``cur``'s. The
    # specimen-stripped family_key drops bracketed PROPERTY tokens
    # (``[Moles/volume]`` vs ``[Mass/volume]``) along with the specimen
    # tail, so without this guard the rerank flips Norepinephrine
    # ``Plasma SCnc`` (substance concentration) → ``Serum or Plasma
    # MCnc`` (mass concentration) on a pure specimen-tier improvement.
    # Both PROPERTY tiers measure the same analyte but in different
    # units; the Chinese query (``神经递质·去甲肾上腺素``) doesn't
    # license either direction, so the right behavior is to preserve
    # whatever PROPERTY the picker's earlier stages chose.
    property_ridx: "np.ndarray | None" = None
    if cache is not None:
        from .axis import load_axis_centroids
        _axis_data_for_prop = load_axis_centroids(cache)
        if _axis_data_for_prop is not None:
            property_ridx = _axis_data_for_prop.get("row_value_idx", {}).get(
                "PROPERTY"
            )

    def _apply_system_pref(picks: list[int]) -> list[int]:
        if system_match_rank is None or not picks or family_index is None:
            return picks
        # Skip rerank when the source carries an implicit non-Ser/Plas
        # specimen (blood gas → BldA). Without this, the rerank would
        # demote the domain-canonical specimen toward the global default
        # Ser/Plas (``血气分析,葡萄糖`` → wrong Ser/Plas instead of BldA).
        m = _SOURCE_PREFIX_RE.match(query_text)
        if m and m.group(1).strip() in _SYSTEM_PREF_SKIP_SOURCES:
            return picks
        cur = picks[0]
        cur_tier = (
            int(system_match_rank[cur])
            if system_match_rank[cur] >= 0 else 99
        )
        if cur_tier <= 0:
            return picks  # already at best tier (Ser/Plas), nothing to gain
        cur_family = row_key[cur] if cur < len(row_key) else ""
        cur_bare = _strip_specimen_tail(cur_family)
        if not cur_bare:
            return picks
        cur_property = (
            int(property_ridx[cur])
            if property_ridx is not None and cur < len(property_ridx)
            else -1
        )
        best_swap = None
        best_key = (cur_tier, -sims_row[cur])
        for r in topN_picks:
            if r == cur:
                continue
            if keep_mask is not None and not keep_mask[r]:
                continue
            if r >= len(row_key):
                continue
            r_bare = _strip_specimen_tail(row_key[r])
            if r_bare != cur_bare:
                continue
            if property_ridx is not None and r < len(property_ridx):
                r_property = int(property_ridx[r])
                # Both rows must carry a known PROPERTY and agree.
                # ``-1`` (missing axis data) blocks the swap to stay
                # safe — we'd rather miss a Ser/Plas promotion than
                # silently flip PROPERTY on rows without axis support.
                if r_property < 0 or cur_property < 0 or r_property != cur_property:
                    continue
            r_tier = (
                int(system_match_rank[r])
                if system_match_rank[r] >= 0 else 99
            )
            if r_tier >= cur_tier:
                continue
            cand_key = (r_tier, -sims_row[r])
            if cand_key < best_key:
                best_key = cand_key
                best_swap = r
        if best_swap is None:
            return picks
        if best_swap in picks:
            return [best_swap] + [r for r in picks if r != best_swap]
        return ([best_swap] + picks)[:top_k]

    def _apply_ratio_orientation(picks: list[int]) -> list[int]:
        """Final-pick step: when the query carries an ``X/Y`` glyph pair
        AND both sides bridge to canonical EN tokens via the short-form
        overlay AND a correct-orientation LOINC row sits in the cosine
        top-N window, promote that row to position 0.

        Deliberately a no-op when either query side fails to bridge —
        bare ``/`` is ambiguous (ratio vs alternative), and overlay
        membership is the deciding signal. Alternative-style slashes
        (``白色念珠菌/都柏林念珠菌`` species, ``卵巢/睾丸`` anatomy,
        ``小麦/麸质蛋白组反应性与自身免疫`` section names) have neither
        side in the overlay → step aside, let cosine pick.

        Runs as the LAST stage of :func:`_finalize` so it overrides
        any prior promote (predicted base-sibling, system-tier) — those
        upstream stages operate on cosine-driven groupings and would
        otherwise lock in a non-pair pick before orientation can
        consider the candidate pool.
        """
        if not picks or cache is None:
            return picks
        m_pair = _RATIO_PAIR_QUERY_RE.search(query_text)
        if not m_pair:
            return picks
        qh, h_ok = _ratio_side_tokens(m_pair.group("head"))
        qt, t_ok = _ratio_side_tokens(m_pair.group("tail"))
        if not (h_ok and t_ok and qh and qt):
            return picks
        qh_lower = [t.lower() for t in qh]
        qt_lower = [t.lower() for t in qt]
        orientations = _build_loinc_ratio_orientations(cache)
        best_correct: int | None = None
        best_cos = -np.inf
        for r in topN_picks:
            orient = (
                orientations[r] if r < len(orientations) else None
            )
            if orient is None:
                continue
            rh_l = orient[0].lower()
            rt_l = orient[1].lower()
            correct = (
                any(t in rh_l for t in qh_lower)
                and any(t in rt_l for t in qt_lower)
            )
            if correct and float(sims_row[r]) > best_cos:
                best_correct = r
                best_cos = float(sims_row[r])
        if best_correct is None or picks[0] == best_correct:
            return picks
        if best_correct in picks:
            return [best_correct] + [
                r for r in picks if r != best_correct
            ]
        return ([best_correct] + picks)[:top_k]

    def _finalize(picks: list[int]) -> list[int]:
        return _apply_ratio_orientation(
            _apply_promote(_apply_system_pref(picks))
        )

    # Stage 2 — scale-prefer rerank across the cosine top-N, ANCHORED to
    # cos_top's analyte family. Anchor = bidirectional family_key prefix
    # match WITH token-boundary guard: ``"glucose in urine"`` and
    # ``"glucose in urine by test strip"`` anchor each other (one is the
    # whitespace-prefix of the other), so Qn / Ord variants of one
    # analyte qualify even when LOINC's ``by X`` suffix splits them
    # into different family_keys; but ``fev`` and ``fev.5`` do NOT
    # anchor (next char is ``.``, not whitespace) — they're distinct
    # analytes, not specificity variants. Token-boundary guard prevents
    # the unrelated-prefix false-positive (``fev`` ⊂ ``fev.5``,
    # ``mean`` ⊂ ``mean platelet volume``) without losing the legit
    # ``by Spirometry`` / ``by Test strip`` cross-family analyte links.
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
            return _family_prefix_match(fk, cos_family) or _family_prefix_match(
                cos_family, fk
            )

        matching = [
            r for r in topN_picks
            if scale_match_rank[r] >= 0 and _shares_analyte(r)
        ]
        if matching:
            # Tie-break order: scale tier (Qn > SemiQn) → system tier
            # (Ser/Plas > Bld > ...) → cosine. System tier added as the
            # second key so same-analyte Ser/Plas wins over Bld whenever
            # both survive cosine; LOINC families lacking Ser/Plas
            # (HbA1c, Lead) get system tier -1 → no displacement.
            def _key(r: int) -> tuple:
                s_tier = (
                    int(system_match_rank[r])
                    if system_match_rank is not None and system_match_rank[r] >= 0
                    else 99
                )
                return (int(scale_match_rank[r]), s_tier, -sims_row[r])
            matching.sort(key=_key)
            matching_set = set(matching)
            non_matching = [r for r in topN_picks if r not in matching_set]
            cosine_picks = (matching + non_matching)[:top_k]

    if family_index is None:
        return _finalize(cosine_picks)

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
        return _finalize(cosine_picks)

    # Within the filtered pool, pick by (scale_rank, system_rank,
    # -cosine) when ranks are available — keeps a same-family ``Qn``
    # ahead of ``SemiQn`` (scale tier) and ``Ser/Plas`` ahead of ``Bld``
    # within the same scale tier (system tier) — else pure cosine.
    def _final_key(r: int) -> tuple:
        scale_t = (
            int(scale_match_rank[r])
            if scale_match_rank is not None and scale_match_rank[r] >= 0
            else 99
        )
        system_t = (
            int(system_match_rank[r])
            if system_match_rank is not None and system_match_rank[r] >= 0
            else 99
        )
        return (scale_t, system_t, -sims_row[r])

    if scale_match_rank is not None or system_match_rank is not None:
        rerank_top = min(filtered, key=_final_key)
    else:
        rerank_top = max(filtered, key=lambda r: sims_row[r])
    if rerank_top == cos_top:
        return _finalize(cosine_picks)
    # Swap position 0; dedupe in case rerank_top was already in tail.
    out = [rerank_top]
    for r in cosine_picks:
        if r != rerank_top and len(out) < top_k:
            out.append(r)
    return _finalize(out)


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


# Phase 1 hybrid axis output — SYSTEM-axis SNOMED-fallback gate.
#
# Empirically a pure absolute-score floor (e.g. ``pick_score < 0.55``)
# fails for the canonical motivating case: ``心包液检验·红细胞沉降率``
# picks LOINC ``43402-7 ESR in Blood by 15M reading`` whose SYSTEM=``Bld``
# scores 0.68 against the query — above any reasonable absolute floor —
# yet the query's strongest SYSTEM centroid is ``Pericard fld`` at 0.77.
# Multi-language body-fluid terms broadly activate the body-fluid
# centroid neighborhood, so absolute thresholds can't separate
# right-SYSTEM from wrong-but-thematically-adjacent-SYSTEM.
#
# The gate that actually fires is **relative**: the query's predicted
# top-1 SYSTEM centroid must be (a) above the absolute confidence floor,
# (b) different from the LOINC pick's SYSTEM value, and (c) ahead of
# the pick's SYSTEM by a clear margin. All three conditions together
# capture "query points at a SYSTEM that LOINC pre-coordinated codes
# don't cover" without flipping high-confidence in-pool picks.
#
# Thresholds copied from :data:`AXIS_THRESHOLDS["SYSTEM"]` so the
# soft-bonus axis prediction and the hard fallback share one tuning
# point.
_HYBRID_SYSTEM_TOP1_MIN: float = 0.55
_HYBRID_SYSTEM_PICK_MARGIN: float = 0.05

# Centroid-bearing axes pulled from the LOINC top-1 pick's row in the
# axis bundle. COMPONENT is high-cardinality and centroid-less (handled
# via ``row_components`` instead); SYSTEM is handled separately by the
# score gate; CLASS is the CLASS-routing axis and never participates in
# FHIR Observation output.
_HYBRID_LOINC_AXES: tuple[str, ...] = (
    "PROPERTY", "TIME_ASPCT", "SCALE_TYP", "METHOD_TYP",
)


# SNOMED hybrid SYSTEM candidate specimen-modifier families. Same
# deterministic keep / drop pattern the LOINC pipeline uses (cf.
# :data:`.specificity.FAMILIES`): each entry pairs a corpus-side FSN
# regex with a multilingual query-side marker regex. When the query
# carries the marker, the modifier-qualified rows stay licensed; when
# absent, those rows are dropped from the candidate pool BEFORE the
# SNOMED top-1 cosine pick. Generic concepts always survive — only
# variants get pruned.
#
# Concretely fixes: ``2200 GI Effects ... Stool·Lactobacillus`` with
# no time keyword previously cosine-picked ``Twenty four hour stool
# specimen`` over the canonical ``Stool specimen``. With this filter
# in place, the time-window rows are dropped and the generic stool
# concept wins.
#
# Per-row corpus masks lazy-build on first SNOMED-fallback firing and
# cache on the cache dict (``_snomed_spec_mod_<family>_mask``), so the
# regex scan over ~38 k anatomy+specimen FSNs is paid once per
# resolve_many call.
_SNOMED_SPEC_MODIFIER_FAMILIES: tuple[tuple[str, "re.Pattern[str]", "re.Pattern[str]"], ...] = (
    # ── Time-qualified collection windows ───────────────────────────
    # SNOMED has 5 such specimens (12 h / 24 h / 48 h / 72 h urine,
    # 24 h / 48 h stool). Without the marker, indicator queries like
    # ``粪便·乳杆菌属`` should land on the generic ``Stool specimen``.
    (
        "time_window",
        re.compile(
            r"\b(?:two|three|six|twelve|twenty\s+four|forty\s+eight|seventy\s+two)\s+hour\b"
            r"|\b(?:two|three|seven|fourteen)\s+day\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b\d+\s*(?:hr|h|hour|hours|day|days)\b"
            r"|\d+\s*(?:小时|小時|時間|시간|天|日)"
            r"|\b(?:two|three|six|twelve|twenty[-\s]four|forty[-\s]eight|seventy[-\s]two)\s+hour\b"
            r"|[半一两二三四六七八九十百]+\s*(?:小时|小時|시간|天|日)",
            re.IGNORECASE,
        ),
    ),
    # ── State-qualified physical characterization ───────────────────
    # 19 specimens: Soft / Hot / Bloody / Liquid stool variants,
    # Frozen / Refrigerated sample variants. Real-world queries
    # rarely qualify by physical state unless dietary-collection
    # protocols are involved.
    (
        "phys_state",
        re.compile(
            r"\b(?:frozen|refrigerated|hot|warm|cold|soft|liquid|bloody|purged|sieved)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:frozen|refrigerated|chilled|hot|warm|cold|soft|liquid|bloody|purged|sieved)\b"
            r"|冷冻|冷藏|冷凍|軟便|软便|血便|液态|液態|液体|液體|稀便|硬便",
            re.IGNORECASE,
        ),
    ),
    # ── Preparation method ──────────────────────────────────────────
    # 148 specimens: ``Cytologic material from <site>``, ``Smear of …``,
    # ``Spun pericardial fluid specimen``, ``Centrifuged …``, plus
    # culture / aspirate / aliquot variants. Cytology is the heavyweight
    # — Pap-smear / FNA queries (``宫颈刮片·细胞项目``) legitimately
    # want these rows, so the multilingual cytology / smear / culture
    # markers must trigger keep.
    (
        "prep_method",
        re.compile(
            r"\b(?:cytologic\s+material|smear|spun|centrifuged|cultured|aspirate|aliquot)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:cytology|cytologic|smear|spin|centrifug(?:ed|al)|culture|aspirate|aspiration)\b"
            r"|细胞学|細胞學|涂片|塗片|刮片|抹片|培养|培養|离心|離心|穿刺|抽吸",
            re.IGNORECASE,
        ),
    ),
)


def _ensure_snomed_spec_modifier_mask(
    cache: dict, family: str, corpus_re: "re.Pattern[str]", names: list[str],
) -> np.ndarray:
    """Lazy-build per-family bool[N] mask flagging SNOMED specimen /
    anatomy FSNs that carry a modifier prefix / token. Cached on the
    request dict — paid once per ``resolve_many`` call regardless of
    how many queries trigger a SNOMED fallback."""
    cache_key = f"_snomed_spec_mod_{family}_mask"
    mask = cache.get(cache_key)
    if mask is not None:
        return mask
    anatomy_mask = cache.get("snomed_anatomy_mask")
    n = len(names)
    out = np.zeros(n, dtype=bool)
    for r in range(n):
        if anatomy_mask is not None and not anatomy_mask[r]:
            continue
        nm = names[r]
        if nm and corpus_re.search(nm):
            out[r] = True
    cache[cache_key] = out
    return out


def _filter_snomed_specimen_candidates(
    query_text: str,
    cache: dict,
    candidate_idxs: np.ndarray,
    names: list[str],
) -> np.ndarray:
    """Apply the SNOMED specimen-modifier keep filter per query.

    For each modifier family: if the query carries the matching marker
    regex, keep all candidates; otherwise drop rows whose FSN matches
    the family's corpus regex. Per-family masks AND-merged. Returns a
    sub-array of *candidate_idxs* (possibly all of them, possibly
    empty — caller falls back to the unfiltered pool when the filter
    over-drops).
    """
    if len(candidate_idxs) == 0 or not query_text:
        return candidate_idxs
    keep = np.ones(len(candidate_idxs), dtype=bool)
    for family, corpus_re, marker_re in _SNOMED_SPEC_MODIFIER_FAMILIES:
        if marker_re.search(query_text):
            continue
        mask = _ensure_snomed_spec_modifier_mask(cache, family, corpus_re, names)
        keep &= ~mask[candidate_idxs]
    return candidate_idxs[keep]


def _build_hybrid_axes(
    pos: int,
    pick_row: int,
    axis_data: dict,
    sys_centroid_sims: "np.ndarray | None",
    hybrid_snomed_bs_idxs: "np.ndarray | None",
    sims_row: np.ndarray,
    fhir_ids: np.ndarray,
    code_strs: dict,
    names: list[str],
    query_text: str,
    cache: dict,
) -> dict[str, AxisCode]:
    """Derive the per-axis hybrid output for a LOINC top-1 pick.

    Reads the 5 LOINC-owned axes from the pick's entry in
    ``axis_data["row_value_idx"][axis]``. SYSTEM is score-gated: when
    ``cos(query, LOINC_SYSTEM_centroid_for_pick) < _HYBRID_SYSTEM_GATE``
    AND a SNOMED ``body structure`` candidate pool exists, SYSTEM is
    re-resolved as the SNOMED body-structure top-1 over the same
    multi-span max-pool cosine matrix used for the LOINC pick (so
    SYSTEM and COMPONENT share one query-side embedding pass).
    """
    row_value_idx = axis_data["row_value_idx"]
    values_by_axis = axis_data["values"]
    row_components = axis_data.get("row_components") or []
    out: dict[str, AxisCode] = {}

    # COMPONENT lives outside the centroid path (too high cardinality
    # to centroid) — it's loaded as a per-row string list.
    if pick_row < len(row_components):
        comp = row_components[pick_row]
        if comp:
            out["COMPONENT"] = AxisCode(
                system="LOINC", code="", name=_ascii_display(comp),
            )

    for axis in _HYBRID_LOINC_AXES:
        ridx_arr = row_value_idx.get(axis)
        if ridx_arr is None:
            continue
        vidx = int(ridx_arr[pick_row])
        if vidx < 0:
            continue
        out[axis] = AxisCode(
            system="LOINC", code="",
            name=_ascii_display(values_by_axis[axis][vidx]),
        )

    sys_ridx_arr = row_value_idx.get("SYSTEM")
    if sys_ridx_arr is None:
        return out
    sys_vidx = int(sys_ridx_arr[pick_row])
    if sys_vidx < 0:
        # Pick has no SYSTEM (rare — radiology / panel rows). Skip the
        # gate; the consumer just sees no SYSTEM entry.
        return out

    loinc_sys_name = values_by_axis["SYSTEM"][sys_vidx]
    snomed_fallback = False
    if sys_centroid_sims is not None and hybrid_snomed_bs_idxs is not None:
        # Relative-margin gate: top-1 predicted SYSTEM must (1) clear the
        # absolute confidence floor, (2) name a different SYSTEM value
        # than the pick, and (3) outscore the pick's SYSTEM by ≥ the
        # pick-margin. See module-level constants for rationale.
        sys_row = sys_centroid_sims[pos]
        top1_vidx = int(np.argmax(sys_row))
        top1_score = float(sys_row[top1_vidx])
        pick_score = float(sys_row[sys_vidx])
        snomed_fallback = (
            top1_score >= _HYBRID_SYSTEM_TOP1_MIN
            and top1_vidx != sys_vidx
            and (top1_score - pick_score) >= _HYBRID_SYSTEM_PICK_MARGIN
        )
    if snomed_fallback:
        # Pre-filter the candidate pool with the specimen-modifier keep
        # rules (cf. ``_SNOMED_SPEC_MODIFIER_FAMILIES``) so over-specific
        # variants (Twenty four hour stool / Cytologic material / etc.)
        # don't outrank the generic concept when the query carries no
        # licensing marker. Falls back to the unfiltered pool when the
        # filter empties the candidate set (corner case: every match is
        # a qualified variant and there's no generic concept).
        filtered_idxs = _filter_snomed_specimen_candidates(
            query_text, cache, hybrid_snomed_bs_idxs, names,
        )
        if len(filtered_idxs) == 0:
            filtered_idxs = hybrid_snomed_bs_idxs
        sub = sims_row[filtered_idxs]
        if len(sub) > 0:
            sn_pick = int(filtered_idxs[int(np.argmax(sub))])
            try:
                sn_sys, sn_code = fhir_id_to_code(int(fhir_ids[sn_pick]))
            except NotImplementedError:
                sn_sys = "SNOMED_CT"
                sn_code = code_strs.get(sn_pick, "") if code_strs else ""
                if not sn_code:
                    sn_code = f"row:{sn_pick}"
            out["SYSTEM"] = AxisCode(
                system=sn_sys, code=sn_code,
                name=_ascii_display(names[sn_pick]),
            )
            return out

    out["SYSTEM"] = AxisCode(
        system="LOINC", code="", name=_ascii_display(loinc_sys_name),
    )
    return out


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
    emit_axes: bool = False,
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

    *emit_axes*: when True, the LOINC top-1 :class:`ResolveResult` for
    each term carries an ``axes`` dict mapping each LOINC axis name
    (``COMPONENT`` / ``PROPERTY`` / ``TIME_ASPCT`` / ``SYSTEM`` /
    ``SCALE_TYP`` / ``METHOD_TYP``) to an :class:`AxisCode`. SYSTEM is
    routed to SNOMED ``body structure`` when the cosine of the query
    against the LOINC pick's SYSTEM-value centroid falls below
    :data:`_HYBRID_SYSTEM_GATE` (Phase 1 of the hybrid-output rollout —
    see the internal resolving design note, page 9 — not published in this repo). Other axes
    stay on LOINC. No-op when the LOINC axis bundle is unavailable
    (``axis_data is None``).
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

    # Phase 1 hybrid axis output — pre-derive the SNOMED candidate pool
    # used as the SYSTEM-axis fallback. Prefers ``snomed_anatomy_mask``
    # (body_structure ∪ specimen, sourced from ``mirobody/res/snomed_axes/``
    # FSN-tag dumps) because it excludes morphologic-abnormality / cell
    # / cell-structure contamination that the broader bundled
    # ``snomed_body_structure_mask`` (IS-A descendants of 123037004)
    # let through. Falls back to the bundled mask when ``snomed_axes/``
    # isn't shipped with the deployment. Built independently of
    # ``target_systems`` so the axes dict can route to SNOMED even when
    # the caller didn't ask for SNOMED_CT results in the legacy output.
    hybrid_snomed_bs_idxs: np.ndarray | None = None
    if emit_axes:
        # `or` on ndarray triggers "truth value is ambiguous" — pick explicitly.
        bs_mask = cache.get("snomed_anatomy_mask")
        if bs_mask is None:
            bs_mask = cache.get("snomed_body_structure_mask")
        if bs_mask is not None:
            snomed_int = SYSTEM_TO_CODE.get("SNOMED_CT")
            if snomed_int is not None:
                sn_idxs = np.where(sys_arr == snomed_int)[0]
                sn_idxs = sn_idxs[~deprecated_drop[sn_idxs]]
                sn_idxs = sn_idxs[bs_mask[sn_idxs]]
                if len(sn_idxs) > 0:
                    hybrid_snomed_bs_idxs = sn_idxs

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

        # provider=None reads EMBEDDING_PROVIDER (default gemini). It was
        # hardcoded to "gemini", which meant the corpus matrix and the query
        # vectors could silently disagree: a deployment configured for any other
        # provider still embedded its QUERIES with gemini, and cosine against a
        # non-gemini matrix is noise. The two sides have to be the same model.
        q_embs = await text_embedding(flat_inputs, provider=None, cache=True)
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

        # Phase 1 hybrid axis output — per-query × SYSTEM-centroid cosine
        # matrix. Built only when ``emit_axes`` is on AND the axis bundle
        # is loaded. Same multi-span max-pool reduction as the main
        # ``sims`` matrix, so SYSTEM scoring respects whichever framing
        # of the query best matches a centroid (e.g. ``心包液检验·红细
        # 胞沉降率``'s leading-segment framing ``心包液检验`` is the one
        # that scores SYSTEM=Pericard fld, not the analyte-tail framing).
        sys_centroid_sims: np.ndarray | None = None
        if emit_axes and axis_data is not None:
            sys_cents = axis_data["centroids"].get("SYSTEM")
            if sys_cents is not None and sys_cents.shape[0] > 0:
                span_sys = Q @ sys_cents.T              # (S, K_SYSTEM)
                sys_centroid_sims = np.full(
                    (len(queries), sys_cents.shape[0]),
                    -np.inf, dtype=np.float32,
                )
                for spi, qi in enumerate(flat_to_qi):
                    np.maximum(
                        sys_centroid_sims[qi], span_sys[spi],
                        out=sys_centroid_sims[qi],
                    )

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
            semiqn_trigger_re = _semiquantitative_trigger_re()
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
                    # then ``default_scale``. Triggers fire per multilingual
                    # markers; see :data:`_SEMIQUANTITATIVE_TRIGGER_MARKERS`
                    # (``半定量``/titer/grade — checked first) and
                    # :data:`_QUALITATIVE_TRIGGER_MARKERS` (dipstick / 试纸
                    # / 試驗紙 / tira reactiva / …).
                    term_text = terms[slot]
                    if term_text and semiqn_trigger_re.search(term_text):
                        cls = "semiqn"
                    elif term_text and qual_trigger_re.search(term_text):
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
                        system_match_rank=_build_system_match_rank(cache),
                        cache=cache,
                    )
                elif s == "SNOMED_CT":
                    picks = _snomed_picks_topk(
                        sims[pos], idxs, top_k,
                        body_structure_mask=cache.get("snomed_body_structure_mask"),
                    )
                else:
                    picks = _non_loinc_picks_topk(sims[pos], idxs, top_k)
                for i_pick, pick in enumerate(picks):
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
                    result = ResolveResult(
                        system=sys_name,
                        code=code,
                        name=_ascii_display(names[pick]),
                        score=round(score_f, 4),
                    )
                    # Phase 1 hybrid axis output — attach the per-axis
                    # tuple to every LOINC pick (top-1 + tail). Each
                    # pick has its own row in the axis bundle so axes
                    # are per-row, not per-query. SNOMED / RxNorm / etc.
                    # rows don't have LOINC axis data → axes stays None
                    # for those.
                    if (
                        emit_axes
                        and s == "LOINC"
                        and axis_data is not None
                    ):
                        result.axes = _build_hybrid_axes(
                            pos, pick, axis_data,
                            sys_centroid_sims, hybrid_snomed_bs_idxs,
                            sims[pos], fhir_ids, code_strs, names,
                            term, cache,
                        )
                    results.append(result)
            out[slot] = results

    return out


__all__ = [
    "query_analyte_digit",
    "loinc_analyte_digits",
    "loinc_family_key",
    "resolve_many",
]
