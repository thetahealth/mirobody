"""Challenge-test time-interval matching.

Many LOINC names encode the sampling-time of a challenge / dose / meal
test inline rather than via the TIME_ASPCT axis:

  - ``Glucose [Mass/volume] in Serum or Plasma --1 hour post xxx challenge``
  - ``Insulin [Units/volume] in Serum or Plasma --1.5 hours post XXX challenge``
  - ``C peptide [Mass/volume] in Serum or Plasma --30 minutes post 75 g glucose``

Queries that name a specific timing (``胰岛素(一小时)``, ``C-肽(0.5h)``,
``(2 hours)``) need to be matched against the corresponding ``--N hour``
or ``--N minutes`` suffix in the candidate name. Otherwise the resolver
sees every post-challenge variant as roughly equivalent and the cosine
top-1 collapses on whichever variant has the best embedding (often the
wrong timing — observed on indicators_excel: all four ``胰岛素(N小时)``
queries landed on the same ``--1.5 hours post`` code).

This module mirrors the ``dose_index`` pattern from
:mod:`embeddings.local._load_dose_index` but runtime-built from LOINC
display names (no bundle artifact required). The query-side scanner
handles CJK numerals (``半``→0.5, ``一``→1, ``两``→2, ``三``→3, ...)
plus Arabic digits, both bracketed (``(一小时)``) and free
(``2 hour post``).

Time-unit normalization: minutes < 60 stay as ``min``; minutes ≥ 60
convert to ``h`` so ``(120 分钟)`` matches ``--2 hours post``. The
build phase also indexes minute→hour equivalents so each row is
reachable from either canonical form.
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache

import numpy as np

from ..common import _CODE_BITS, SYSTEM_TO_CODE

log = logging.getLogger(__name__)


# Magnitude of the time-match bonus for the challenge_time pass
# (``--N hours post X`` LOINC suffix). The candidate set for any
# given analyte+challenge query has near-zero cosine spread (all
# rows share the same analyte and challenge protocol; only the
# timing differs), so the bonus needs to be load-bearing — 0.06.
BONUS_WEIGHT: float = 0.06

# Magnitude of the bonus for the circadian clock-time pass
# (``--N AM/PM specimen`` LOINC suffix). Half the challenge_time
# weight: clock-time candidates can span analyte families (Cortisol
# --4 PM vs Corticotropin --4 PM share the time qualifier but
# differ in the analyte), so a too-strong bonus drags wrong-analyte
# rows into competition with the right one. 0.03 ≈ axis-bonus
# magnitude — flips ties without overpowering the cosine analyte
# signal.
CLOCK_BONUS_WEIGHT: float = 0.03


# ── CJK numeral conversion ────────────────────────────────────────────


# Common Chinese numerals appearing in lab-indicator time intervals.
# ``两`` is the colloquial alternative to ``二`` when counting time
# spans (``两小时`` = ``2 hours`` ≠ ``二小时``). ``半`` is the only
# fractional CJK numeral used in lab contexts.
_CJK_NUMERAL_MAP: dict[str, float] = {
    "半": 0.5,
    "零": 0,
    "一": 1, "两": 2, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
}


def _parse_number(s: str) -> float | None:
    """Parse CJK numeral OR Arabic digit string to float. Returns
    ``None`` on garbage. CJK numerals are looked up character-by-char
    (single-char only — ``十一`` isn't supported, but lab indicators
    rarely use compound numerals above 10).
    """
    s = s.strip()
    if not s:
        return None
    if s in _CJK_NUMERAL_MAP:
        return _CJK_NUMERAL_MAP[s]
    try:
        return float(s)
    except ValueError:
        return None


# ── Corpus-side: build LOINC --N hour/minute index ────────────────────


# Match LOINC's ``--…`` time-qualifier suffix. Captures the numeric
# value and the unit token. Covers four surface forms shipped by LOINC:
#
#   - ``--N hours post X``           (standard challenge-time form)
#   - ``--N hours pre X``            (rare ``pre`` ordering)
#   - ``--pre N hour fast``          (fasting-glucose-specific ordering;
#                                    LOINC reverses ``pre`` before the
#                                    numeral here, e.g. ``1550-3``)
#   - ``--N hours fasting``          (alternate fasting form, e.g.
#                                    ``17865-7`` ``--8 hours fasting``)
#   - ``--N min p X``                (``p`` = ``post`` abbreviation in
#                                    thromboelastography panels)
#
# The trailing ``(?:post|pre|fast|fasting|p)\b`` anchor is essential —
# LOINC also ships *specimen-duration* suffixes that share the surface
# form but are NOT challenge times: ``--24 hour specimen`` (PD fluid
# collection window) and ``--2 hour dwell specimen`` (PD-fluid dwell
# time). Without the context anchor, those get indexed as (24, h) /
# (2, h) and the strict branch of :func:`_challenge_time_keep` filters
# the candidate pool to those specimen-collection rows when a query
# carries a 24-hour-urine specimen-duration phrase — dropping the
# correct ``Calcium [Mass/time] in 24 hour Urine`` row and surfacing
# ``Cortisol Free 24h Urine --24 hours post dose corticotropin`` /
# ``Urea nitrogen ... in Peritoneal dialysis fluid --24 hour specimen``
# instead.
_LOINC_TIME_RE = re.compile(
    r"--\s*"
    r"(?:pre\s+)?"
    r"([0-9]+(?:\.[0-9]+)?)\s*"
    r"(hour|hr|minute|min)s?"
    r"\s+(?:post|pre|fast|fasting|p)\b",
    re.IGNORECASE,
)


def _normalize_loinc_unit(raw: str) -> str:
    """LOINC unit token → canonical ``h`` / ``min``."""
    raw = raw.lower()
    if raw in ("hour", "hr"):
        return "h"
    return "min"


def _build_loinc_time_index(cache: dict) -> dict[tuple[float, str], np.ndarray]:
    """Scan LOINC names for ``--N hour(s)/minute(s) (post|pre)`` and
    index by canonical ``(value, unit)``. Caches the result on
    ``cache["_challenge_time_index"]``.

    Each row is also reachable from the ``minute ↔ hour`` equivalent
    of its native unit (``--30 minutes post`` indexed at both (30,
    ``min``) and (0.5, ``h``)), so the query scanner doesn't need to
    pick one canonical form to enforce — whatever the user wrote
    matches against its own representation directly.
    """
    cached = cache.get("_challenge_time_index")
    if cached is not None:
        return cached

    canonical = np.asarray(cache["canonical"])
    sys_arr = (canonical >> _CODE_BITS) & 0x7
    is_loinc = sys_arr == SYSTEM_TO_CODE["LOINC"]
    names = cache.get("names")
    if not names:
        cache["_challenge_time_index"] = {}
        return {}

    keys: dict[tuple[float, str], list[int]] = {}
    n_hits = 0
    for i in np.where(is_loinc)[0]:
        nm = names[i]
        if not nm or "--" not in nm:
            continue
        for m in _LOINC_TIME_RE.finditer(nm):
            value = float(m.group(1))
            unit = _normalize_loinc_unit(m.group(2))
            keys.setdefault((value, unit), []).append(int(i))
            # Add the minute↔hour equivalent unconditionally, so queries
            # in either unit reach the same row: ``--30 minutes post``
            # is reachable via (0.5, "h") AND (30, "min"); ``--2 hours
            # post`` via (2, "h") AND (120, "min").
            if unit == "min":
                keys.setdefault((value / 60, "h"), []).append(int(i))
            else:  # unit == "h"
                keys.setdefault((value * 60, "min"), []).append(int(i))
            n_hits += 1

    index = {
        k: np.unique(np.asarray(v, dtype=np.int32)) for k, v in keys.items()
    }
    cache["_challenge_time_index"] = index
    log.info(
        "challenge_time index: %d unique (value, unit) keys, %d LOINC rows tagged",
        len(index), n_hits,
    )
    return index


# ── Query-side: extract (value, unit) intervals from free text ───────


# Bracketed CJK time interval: ``(一小时)`` / ``(两小时)`` /
# ``(0.5小时)`` / ``(30 分钟)`` / ``（半小时）``. The opening bracket
# may be ASCII or fullwidth.
_QUERY_TIME_BRACKETED_CJK = re.compile(
    r"[（(]\s*"
    r"([半零一两二三四五六七八九十0-9.]+)\s*"
    r"(小时|小時|分钟|分鐘|时|時)"
    r"\s*[)）]",
)

# Bracketed Latin time interval: ``(2 hours)`` / ``(0.5 h)`` /
# ``(30 min)``. Single-char ``h``/``m`` units stay tolerant by
# anchoring on the bracket so we don't false-fire mid-word.
_QUERY_TIME_BRACKETED_LATIN = re.compile(
    r"[（(]\s*"
    r"([0-9]+(?:\.[0-9]+)?)\s*"
    r"(hours?|hrs?|h|minutes?|mins?|min)"
    r"\s*[)）]",
    re.IGNORECASE,
)

# Unbracketed CJK time interval: ``2小时血糖`` / ``半小时血糖`` /
# ``30分钟胰岛素``. Chinese indicator names commonly carry the time as
# part of the compound noun without parentheses. CJK-only (Latin
# ``2 hour`` is intentionally NOT scanned here — would over-fire on
# machine-translated en columns and LOINC-name fragments leaking into
# query text). The full CJK numeral set + 小时/分钟 is sufficient for
# the indicator domain.
_QUERY_TIME_BARE_CJK = re.compile(
    r"(?<![一-鿿])"  # not immediately preceded by another CJK char
    r"([半零一两二三四五六七八九十0-9.]+)\s*"
    r"(小时|小時|分钟|分鐘)",
)

# Specimen-collection marker: a CJK time-window phrase that names a
# SPECIMEN, not a challenge time — ``小时尿`` (N-hour urine), ``小时
# 粪`` (N-hour stool), ``小时唾`` (saliva), ``小时血液`` (blood),
# ``小时痰`` (sputum), ``小时汗液`` (sweat). When any of these appears
# anywhere in the query, EVERY bare-CJK ``N小时`` instance in that
# query is collection-duration vocabulary — including instances where
# the analyte name (``白蛋白`` / ``肌酐`` / ``钙``) sits between
# ``小时`` and the specimen noun (``24小时白蛋白定量`` inside the
# ``24小时尿液检测·…`` panel). LOINC encodes these in the SPECIMEN
# axis (``Albumin ... in 24 hour Urine``), never via the ``--N hours
# post X`` qualifier, so :func:`query_time_intervals` must suppress
# bare-CJK extraction wholesale for this query class.
#
# Bracketed forms (``(2小时)`` / ``(half hour)``) still parse — those
# are the canonical Chinese indicator notation for challenge times
# (``C-肽(半小时)``, ``葡萄糖(三小时)``) and never appear in
# timed-collection panel names.
_SPECIMEN_DURATION_RE = re.compile(
    r"小时(?:尿|粪|大便|唾液|血液|痰|汗液)"
)


def _normalize_query_unit(raw: str) -> str:
    """Query-side unit token → ``h`` / ``min``."""
    raw = raw.lower().strip()
    if raw in ("hour", "hours", "hr", "hrs", "h", "小时", "小時", "时", "時"):
        return "h"
    return "min"


def query_time_intervals(text: str) -> set[tuple[float, str]]:
    """Extract every ``(value, canonical-unit)`` tuple from *text*.

    Three forms scanned: bracketed CJK ``(一小时)``, bracketed Latin
    ``(2 hours)``, and bare CJK ``2小时血糖``. Bare LATIN is
    deliberately NOT scanned — would over-fire on machine-translated
    en columns ("Two-hour Postprandial..." etc.).

    Bare-CJK is suppressed entirely when *text* carries a
    specimen-collection marker (:data:`_SPECIMEN_DURATION_RE` —
    ``小时尿`` / ``小时粪`` / ``小时唾液`` / …). In those queries
    every ``N小时`` instance is collection-duration vocabulary, not a
    challenge time. Bracketed forms remain — they're the canonical
    Chinese notation for challenge times (``C-肽(半小时)``) and never
    appear in timed-collection panel names.
    """
    if not text:
        return set()
    out: set[tuple[float, str]] = set()
    for m in _QUERY_TIME_BRACKETED_CJK.finditer(text):
        v = _parse_number(m.group(1))
        if v is None:
            continue
        out.add((v, _normalize_query_unit(m.group(2))))
    for m in _QUERY_TIME_BRACKETED_LATIN.finditer(text):
        v = _parse_number(m.group(1))
        if v is None:
            continue
        out.add((v, _normalize_query_unit(m.group(2))))
    if not _SPECIMEN_DURATION_RE.search(text):
        for m in _QUERY_TIME_BARE_CJK.finditer(text):
            v = _parse_number(m.group(1))
            if v is None:
                continue
            out.add((v, _normalize_query_unit(m.group(2))))
    return out


# ── Clock-time → AM/PM specimen ───────────────────────────────────────


# LOINC names encode circadian sampling time as ``--N AM specimen`` or
# ``--N PM specimen`` (N is 1–12, 12-hour clock). Queries from Chinese
# indicator panels write the same concept as bracketed 24-hour clock
# (``（16:00）``, ``（8:00）``, ``（24:00）``). The two notations need
# bridging:
#
#   - extract the bracketed clock-time from the query
#   - normalize to (12-hour N, "AM" / "PM")
#   - boost LOINC rows that carry the matching ``--N (AM|PM)`` suffix
#
# Bracketing is a HARD requirement on the query side — bare ``\d+:\d+``
# would over-match lipid carbon-double-bond nomenclature (``18:3 n-6``,
# ``20:4 n-6``, ``22:4 n-6``) that appears verbatim in fatty-acid
# indicator names. Clock-time in clinical indicators is overwhelmingly
# parenthetical; fatty-acid C:D ratios are bare. The bracket gate
# preserves that asymmetry.


# Bracketed 24-hour clock: ``（16:00）`` / ``(8:00)`` / ``（24:00）``.
# Accept both ASCII and fullwidth brackets, both ASCII and fullwidth
# colons. Hours 0-24 and minutes 00-59 — bounds checked after parsing,
# not in the regex, so the pattern stays readable.
_QUERY_CLOCK_RE = re.compile(
    r"[（(]\s*"
    r"([0-9]{1,2})\s*[:：]\s*([0-9]{2})\s*"
    r"[)）]",
)


def _clock_to_12h(hour: int, minute: int) -> tuple[int, str] | None:
    """24-hour ``(hour, minute)`` → 12-hour ``(N, "AM"|"PM")``.

    Returns ``None`` for invalid times (hour > 24, minute > 59).
    Midnight (0:00 / 24:00) maps to ``(12, "AM")`` and noon (12:00)
    to ``(12, "PM")`` — matches the LOINC convention where
    ``--12 AM`` is midnight and ``--12 PM`` is noon.
    """
    if minute > 59 or hour > 24:
        return None
    if hour == 0 or hour == 24:
        return (12, "AM")
    if hour == 12:
        return (12, "PM")
    if hour < 12:
        return (hour, "AM")
    return (hour - 12, "PM")


def query_clock_times(text: str) -> set[tuple[int, str]]:
    """Extract every bracketed ``HH:MM`` from *text*, normalize to
    12-hour ``(N, period)`` tuples. Empty set if none / invalid.
    """
    if not text:
        return set()
    out: set[tuple[int, str]] = set()
    for m in _QUERY_CLOCK_RE.finditer(text):
        try:
            h = int(m.group(1))
            mn = int(m.group(2))
        except ValueError:
            continue
        norm = _clock_to_12h(h, mn)
        if norm is not None:
            out.add(norm)
    return out


# LOINC name pattern: ``--N AM specimen`` / ``--N PM specimen`` /
# ``--N AM`` / ``--N PM``. The ``specimen`` suffix is the dominant
# form but some variants drop it; both should index. N is 1-12.
_LOINC_CLOCK_RE = re.compile(
    r"--\s*(1[0-2]|[1-9])\s*(AM|PM)\b",
    re.IGNORECASE,
)


def _build_loinc_clock_index(cache: dict) -> dict[tuple[int, str], np.ndarray]:
    """Scan LOINC names for ``--N AM/PM`` suffix; index by
    ``(N, "AM"|"PM")``. Cached on ``cache["_clock_time_index"]``.
    """
    cached = cache.get("_clock_time_index")
    if cached is not None:
        return cached
    canonical = np.asarray(cache["canonical"])
    sys_arr = (canonical >> _CODE_BITS) & 0x7
    is_loinc = sys_arr == SYSTEM_TO_CODE["LOINC"]
    names = cache.get("names")
    if not names:
        cache["_clock_time_index"] = {}
        return {}
    keys: dict[tuple[int, str], list[int]] = {}
    n_hits = 0
    for i in np.where(is_loinc)[0]:
        nm = names[i]
        if not nm or "--" not in nm:
            continue
        for m in _LOINC_CLOCK_RE.finditer(nm):
            n = int(m.group(1))
            period = m.group(2).upper()
            keys.setdefault((n, period), []).append(int(i))
            n_hits += 1
    index = {
        k: np.unique(np.asarray(v, dtype=np.int32)) for k, v in keys.items()
    }
    cache["_clock_time_index"] = index
    log.info(
        "clock_time index: %d unique (hour, AM/PM) keys, %d LOINC rows tagged",
        len(index), n_hits,
    )
    return index


# ── Context-implied default doses ──────────────────────────────────────


# Test-context markers that imply a default dose for the challenge
# variant. When the query carries one of these markers AND a time
# interval, :func:`context_implied_doses` emits the standard dose
# tuple so the adapter's existing dose-index pass picks the right
# post-dose LOINC even when the query omits the explicit dose string.
#
# OGTT defaults to 75 g glucose PO per WHO standard for non-pregnant
# adults. The dose_index pre-shipped in the bundle already contains
# ``(75, "g")`` keyed to LOINC ``--N hours post 75 g glucose PO``
# rows; this table just wires the source-context wording to that dose
# so the bonus fires automatically.
#
# Each entry is ``(license_markers, time_required, default_dose)``:
#   - ``license_markers``: multilingual marker list (same compile
#     convention as :data:`specificity.FAMILIES.markers`).
#   - ``time_required``: emit the dose ONLY when ``query_time_intervals``
#     also returns at least one interval. Guards against firing on
#     non-challenge queries (e.g. ``糖尿病筛查·空腹血糖`` → fasting,
#     not OGTT; no time bracket so no default dose).
#   - ``default_dose``: ``(value, ucum_unit)`` tuple keyed into
#     ``cache["dose_index"]``.
CONTEXT_DOSE_DEFAULTS: list[tuple[list[str], bool, tuple[float, str]]] = [
    (
        [
            "糖尿病筛查", "糖耐量", "口服糖耐量", "葡萄糖耐量",
            "糖尿病篩查", "糖耐量試驗",
            "OGTT",
            "oral glucose tolerance", "glucose tolerance test",
            "tolerancia a la glucosa", "prueba de tolerancia",
            "Glukosetoleranztest", "Glukosetoleranz",
            "épreuve de tolérance au glucose",
            "тест толерантности к глюкозе",
        ],
        True,
        (75.0, "g"),
    ),
]


@lru_cache(maxsize=1)
def _context_dose_res() -> list[tuple["object", bool, tuple[float, str]]]:
    """Compile context license markers to regex. Lazy to defer the
    specificity import."""
    from .specificity import _compile_marker_pattern
    return [
        (_compile_marker_pattern(markers), time_required, dose)
        for markers, time_required, dose in CONTEXT_DOSE_DEFAULTS
    ]


def context_implied_doses(text: str) -> set[tuple[float, str]]:
    """Default doses implied by test-context keywords in *text*.

    Emits the dose tuple when the query carries a context license
    marker AND (if the entry requires it) a time interval. Empty set
    otherwise. Same shape as :func:`mirobody.units.scan_value_units`
    so the adapter can union both into a single dose-index lookup pass.
    """
    if not text:
        return set()
    out: set[tuple[float, str]] = set()
    times: set[tuple[float, str]] | None = None  # lazy
    for rx, time_required, dose in _context_dose_res():
        if not rx.search(text):
            continue
        if time_required:
            if times is None:
                times = query_time_intervals(text)
            if not times:
                continue
        out.add(dose)
    return out


__all__ = [
    "BONUS_WEIGHT",
    "CLOCK_BONUS_WEIGHT",
    "CONTEXT_DOSE_DEFAULTS",
    "_build_loinc_clock_index",
    "_build_loinc_time_index",
    "context_implied_doses",
    "query_clock_times",
    "query_time_intervals",
]
