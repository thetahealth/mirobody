"""Body-axis contradiction filter framework.

LOINC names encode anatomical position along several bidirectional
axes: left/right, upper/lower, anterior/posterior, proximal/distal,
medial/lateral, dorsal/ventral. The cosine gap between symmetric
siblings (``DXA Hip - left`` vs ``- right``, ``X-ray upper limb`` vs
``- lower limb`` …) is roughly noise floor — Gemini's multilingual
embedding does not reliably bridge the CJK side tokens to the LOINC
English ones, so source/abbrev/neighboring tokens drift the result
across the contradiction line with no real semantic basis.

Rather than rerank, we hard-drop the contradicting half of each axis
before the picker sees it. Each axis is a small config in :data:`AXES`;
adding a new axis is purely additive — register the markers and the
shared :func:`axis_filter_keep` entry point picks it up. Currently
ships TWO axes (``left_right``, ``upper_lower``); axes fire
independently and AND-merge their keep masks, so a query like
``右上叶结节`` correctly intersects (drop left AND drop lower).

## Tri-state per axis (POS / NEG / BOTH)

  * query = POS → keep POS-tagged ∪ BOTH-tagged ∪ unspecified  (drop NEG-only)
  * query = NEG → keep NEG-tagged ∪ BOTH-tagged ∪ unspecified  (drop POS-only)
  * query = BOTH → keep BOTH-tagged ∪ unspecified              (drop POS-only, NEG-only)
  * query carries 0 or >1 markers → no filter fires on this axis.

The ``>1`` branch is the catch-all for ambiguous queries (``左右心室``
comparison rows, ``左侧/右侧均可`` notes). Better to let cosine decide
than guess which side was meant.

POS / NEG orientation is arbitrary per axis — readability only, the
filter is symmetric. ``BOTH`` is optional: not every anatomical axis
has a "both states present" LOINC qualifier. left/right has
``bilateral`` (851 rows); upper/lower has no canonical "both upper
and lower" form. Axes without a BOTH state omit ``both_markers`` /
``name_both_re`` and the tri-state collapses to two states.

## Multilingual query markers

Query-side detection mirrors :mod:`.specificity`: each marker list
carries the Latin (English / Spanish / German / French), Cyrillic
(Russian), and CJK (Chinese Simplified + Traditional, Japanese,
Korean) variants. Compile happens through the shared helper
:func:`mirobody.indicator.fhir.resolve.specificity._compile_marker_pattern`,
which wraps Latin / Cyrillic markers with ``\\b`` and matches CJK
markers as bare substrings (Python's ``\\b`` doesn't fire on
ideographic edges). LOINC display names are English-only, so the
``name_*_re`` patterns stay simple word-boundary regexes.

False positives that ride the same token (chirality ``左旋`` / ``右旋``
for chemistry / drug names) are stripped from the query before marker
matching via :data:`.AXES[...]['query_exclude_re']` — keep the
exclusion **inside** the axis config so per-axis FP guards live with
the rest of that axis's intent.

## Why ``left_right`` looks like a no-op on the current benchmark

On ``benchmarks/indicators_excel.csv`` under the default ``source|
indicator`` query template, the 87 laterality-bearing indicator rows
already top-1 onto the correct side under plain cosine — this filter
changes *zero* picks (_1 vs _3 diff = 0 bytes). The zero rests on
three coincidences that won't survive a configuration change:

  1. Gemini's multilingual embedding does carry a faint ``左 ↔ left``
     directional signal — but only ~0.005 cosine, well inside the
     run-to-run noise floor.
  2. The ``source`` values in this corpus (``骨密度``, ``体检报告``,
     ``肢体骨骼肌质量``) are *domain-narrowing but laterality-neutral*:
     they concentrate the candidate pool into the right LOINC
     neighborhood without biasing left vs right.
  3. Inside each narrowed neighborhood LOINC ships symmetric POS /
     NEG siblings (``DXA Hip - left`` vs ``- right`` etc.), so
     condition (1)'s 0.005 signal is the only remaining asymmetry —
     and it decides correctly.

Drop the source field (``indicator``-only ablation _2.csv) or swap to
a heterogeneous panel name, and condition (2) breaks: cosine spreads
across unrelated procedure subspaces, the 0.005 left-bias is buried,
and the bone-density ``左髋·T值`` row top-1s onto ``DXA Hip - right``
because the right candidate happens to sit 0.011 cosine higher than
the left one within the wider procedure pool.

This filter promotes a three-condition lucky alignment into a hard
guarantee. ``git blame`` reader who sees zero benchmark delta and
considers removal: do not. The current benchmark just happens to
satisfy all three conditions; any new upstream catalog with different
source-column conventions will not.

## Adding a new axis

The decision criterion is the **reference vocabulary**, not the
current indicator benchmark. ``indicators_excel.csv`` is a snapshot
of one upstream catalog; LOINC + SNOMED CT are the actual contradiction
space every future query could hit. A 2026-05-18 survey of LOINC
LONG_COMMON_NAMEs (97 k ACTIVE rows) by symmetric-pair count:

  axis                  pos    neg   pairs  pair%
  left_right           2400   2039   1850   71.5%   ✓ shipped
  upper_lower (merged   462    588    266   33-35%  ✓ shipped (en upper+superior,
    upper+superior /                                  zh 上+上腔+上叶+..., etc.)
    lower+inferior)
  proximal_distal        21     18     13   50.0%   marginal — small volume
  anterior_posterior     77     84     16   11.0%   skip — 中文 ``前后位`` is
                                                    unitary, not contradiction
  medial_lateral         17    547     17    3.1%   skip — 530+ ``lateral``
                                                    rows are radiology views
                                                    (``XR Sella turcica Lateral``)
  internal_external     113     38     23   18.0%   skip — ``Internal medicine``,
                                                    ``External defibrillator`` FP
  inner_outer            15     18      4   13.8%   skip — only OCT macular grid
  dorsal_ventral         15      0      0    0.0%   skip
  superior_inferior      42     63     27   34.6%   merged into upper_lower

When adding the next axis: re-run the same survey, look for ≥30%
pair rate **and** sample the unpaired token usages — if the unpaired
side is dominated by non-axis usages (``lateral view`` radiology,
``internal medicine`` clinical domain), the filter will have high
false-positive rate even if the pair count looks decent. Multilingual
marker coverage matters even when the current indicator corpus is
monolingual — the resolve pipeline is consumed in many locales, and
a future deployment shouldn't quietly lose the guarantee. Per-axis
false-positive guards (chirality ``左旋``/``右旋`` for left_right;
``颞下颌`` / ``上午``/``下午`` / ``以上``/``以下`` for upper_lower)
belong in the per-axis ``query_exclude_re``, not in shared code.

The current architecture filters LOINC only — :data:`mirobody.
indicator.fhir.resolve.pipeline._LOINC_FILTERS` has no SNOMED
counterpart. SNOMED CT names also carry laterality (``Structure of
left kidney (body structure)`` etc.) and likely the upper/lower axis,
so a future ``_SNOMED_FILTERS`` chain would re-use the same
:data:`AXES` config but apply against the SNOMED row slice. That's a
pipeline-level change, not an axis-table change.

When evidence lands: append a new entry to :data:`AXES` with the
same key shape as ``left_right``. Multilingual marker coverage
matters even when the indicator corpus is monolingual — the resolve
pipeline is consumed by callers in many locales, and a future
Japanese / Korean / European deployment shouldn't quietly lose the
guarantee. Per-axis false-positive guards (``左旋`` / ``右旋`` for
chirality on left_right; ``颞下颌关节`` for any upper/lower axis;
``前后位`` for an anterior/posterior axis) belong in the per-axis
``query_exclude_re``, not in shared code.
"""

from __future__ import annotations

import logging
import re

import numpy as np

from ..common import _CODE_BITS, SYSTEM_TO_CODE
# Private import: `_compile_marker_pattern` is the shared multilingual
# compiler used by both specificity.py and laterality.py. Both modules
# follow the same "markers list, CJK bare + Latin/Cyrillic boundary"
# pattern. If a third module ever needs it, graduate the helper to
# common.py and update both call sites.
from .specificity import _compile_marker_pattern

log = logging.getLogger(__name__)


# ── Tri-state codes ──────────────────────────────────────────────────
POS = "P"
NEG = "N"
BOTH = "B"


# ── Axis config table ────────────────────────────────────────────────
# Each entry:
#   pos_markers / neg_markers — required. Multilingual literal token
#       lists for the two contradicting states. Compiled via
#       :func:`_compile_marker_pattern`: Latin / Cyrillic get word
#       boundaries, CJK matches bare.
#   both_markers — optional. Marker list for "both states present at
#       once" (e.g. ``bilateral`` / ``双侧``). Omit when the axis has
#       no such canonical compound.
#   query_exclude_re — optional. Regex of substrings to **strip from
#       the query text** before marker matching. Used for axis-
#       specific false-positive compounds that share characters with
#       a marker (``左旋`` / ``右旋`` chirality on left/right).
#   name_pos_re / name_neg_re — required. LOINC display-name regexes
#       (English; LOINC names are English-only). Word-boundary at
#       Latin-letter edges.
#   name_both_re — optional, paired with both_markers.

AXES: dict[str, dict] = {
    "upper_lower": {
        # English: ``upper`` and ``superior`` are synonymous in
        # anatomical naming — LOINC uses both (``Upper GI tract``,
        # ``Superior vena cava``). The CJK side uses one character
        # ``上`` / ``下`` for both English forms, so we merge them
        # into one axis. No canonical "both upper and lower" LOINC
        # form, so ``both_markers`` / ``name_both_re`` are omitted.
        "pos_markers": [
            # Latin (English, Spanish — "superior" is shared).
            "upper", "superior",
            # Chinese — bare ``上`` is too noisy (``上午``, ``上限``,
            # ``上述``, ``上升``, ``以上``, ``上半``, ``上次``,
            # ``上面``, ``上海`` …), so list unambiguous clinical
            # compounds explicitly. Simplified script:
            "上肢", "上腹", "上腹部", "上消化道", "上腔",
            "上颌", "上叶", "上段", "上极", "上眼睑", "上嘴唇",
            # Traditional script — differs from Simplified in
            # ``颌→頜``, ``叶→葉``, ``睑→瞼``. Compounds without
            # these chars (上肢/上腹/上消化道/上腔/上段/上极) are
            # identical to Simplified and dedup at compile.
            "上頜", "上葉", "上眼瞼", "上嘴脣",
            # Japanese — clinical kanji share form with Traditional
            # Chinese. ``上葉`` (jōyō, upper lobe of lung) is the
            # canonical radiology term. The 上肢 / 上腹部 / 上葉
            # entries dedup against the zh entries.
            "上腹部",
            # Korean — sang- prefix on body parts.
            "상지", "상복부", "상엽",
            # German — bare ``ober-`` isn't a standalone word; key
            # declensions plus the canonical compounds.
            "obere", "oberer", "oberes", "oberen", "oberem",
            "Oberbauch", "Oberkiefer", "Oberlappen",
            # French.
            "supérieur", "supérieure", "supérieurs", "supérieures",
            # Russian — masculine / feminine / neuter nominative
            # plus the common oblique cases.
            "верхний", "верхняя", "верхнее", "верхние",
            "верхнего", "верхней", "верхних",
        ],
        "neg_markers": [
            "lower", "inferior",
            "下肢", "下腹", "下腹部", "下消化道", "下腔",
            "下颌", "下叶", "下段", "下极", "下眼睑", "下嘴唇",
            "下頜", "下葉", "下眼瞼", "下嘴脣",
            "하지", "하복부", "하엽",
            "untere", "unterer", "unteres", "unteren", "unterem",
            "Unterbauch", "Unterkiefer", "Unterlappen",
            "inférieur", "inférieure", "inférieurs", "inférieures",
            "нижний", "нижняя", "нижнее", "нижние",
            "нижнего", "нижней", "нижних",
        ],
        # FP guard against the Chinese non-anatomical compounds that
        # share ``上`` / ``下`` with the anatomical markers. Strip
        # before marker matching so a query like ``下午抽血`` (PM blood
        # draw) doesn't false-fire NEG. ``颞下颌`` is the TMJ joint —
        # one anatomical unit, not a "lower jaw" axis pole.
        "query_exclude_re": re.compile(
            r"上午|下午"                       # AM / PM (time)
            r"|上限|下限|以上|以下"             # numeric ranges
            r"|上述|下述"                       # discourse references
            r"|上升|下降"                       # trend direction
            r"|上半|下半"                       # halves
            r"|上次|下次"                       # ordinal time
            r"|上面|下面"                       # textual position
            r"|颞下颌|颞上颌"                   # TMJ — single joint
        ),
        # LOINC names: ``\\b`` boundary on Latin letters.
        "name_pos_re":  re.compile(
            r"(?<![A-Za-z])(?:upper|superior)(?![A-Za-z])",
            re.IGNORECASE,
        ),
        "name_neg_re":  re.compile(
            r"(?<![A-Za-z])(?:lower|inferior)(?![A-Za-z])",
            re.IGNORECASE,
        ),
    },
    "left_right": {
        "pos_markers": [
            # English
            "left",
            # Chinese Simp / Trad + Japanese — same character ``左``,
            # one entry covers all three scripts (CJK bare substring).
            "左",
            # Korean (clinical compound; bare ``좌`` is too noisy —
            # also means "seat", "left wing", etc.).
            "좌측", "좌편",
            # Spanish (gendered)
            "izquierdo", "izquierda", "izquierdos", "izquierdas",
            # German (declension family)
            "links", "linke", "linker", "linkes", "linken", "linkem",
            # French
            "gauche",
            # Russian (key inflections)
            "слева", "левый", "левая", "левое", "левого",
            "левому", "левых", "левой",
        ],
        "neg_markers": [
            "right",
            "右",
            "우측", "우편",
            "derecho", "derecha", "derechos", "derechas",
            "rechts", "rechte", "rechter", "rechtes", "rechten", "rechtem",
            "droit", "droite",
            "справа", "правый", "правая", "правое", "правого",
            "правому", "правых", "правой",
        ],
        "both_markers": [
            # English (medical Latin-derived)
            "bilateral",
            # Chinese Simp
            "双侧", "两侧", "双眼", "两眼", "双耳", "两耳",
            # Chinese Trad
            "雙側", "兩側", "雙眼", "兩眼", "雙耳", "兩耳",
            # Japanese
            "両側", "両眼", "両耳",
            # Korean
            "양측", "양쪽",
            # Spanish
            "ambos lados",
            # German
            "beidseitig", "beidseits", "bilateral",
            # French
            "bilatéral", "bilatérale", "bilatéraux", "bilatérales",
            # Russian
            "двусторонний", "двусторонняя", "двустороннее",
            "обе стороны",
        ],
        # Chirality-prefix compounds: ``左旋`` (L-, levo) and ``右旋``
        # (D-, dextro) on carnitine / DOPA / dextran / etc. share the
        # ``左`` / ``右`` characters with laterality but mean chirality,
        # not body side. Strip them before marker matching so the
        # remaining text is correctly classified (chirality-only queries
        # → None; chirality + real laterality → the side wins).
        "query_exclude_re": re.compile(r"左旋|右旋"),
        # LOINC names are English-only, so plain Latin-boundary regex.
        "name_pos_re":  re.compile(r"(?<![A-Za-z])left(?![A-Za-z])", re.IGNORECASE),
        "name_neg_re":  re.compile(r"(?<![A-Za-z])right(?![A-Za-z])", re.IGNORECASE),
        "name_both_re": re.compile(r"(?<![A-Za-z])bilateral(?![A-Za-z])", re.IGNORECASE),
    },
}


# ── Compile-time query patterns ──────────────────────────────────────
# One regex per (axis, state); reused across every query.
_AXIS_QUERY_RES: dict[str, dict[str, re.Pattern]] = {}
for _axis_name, _axis in AXES.items():
    _per_state: dict[str, re.Pattern] = {
        POS: _compile_marker_pattern(_axis["pos_markers"]),
        NEG: _compile_marker_pattern(_axis["neg_markers"]),
    }
    if _axis.get("both_markers"):
        _per_state[BOTH] = _compile_marker_pattern(_axis["both_markers"])
    _AXIS_QUERY_RES[_axis_name] = _per_state


# ── Query-side state detection ───────────────────────────────────────


def query_axis_state(text: str, axis_name: str) -> str | None:
    """Return ``POS`` / ``NEG`` / ``BOTH`` if *text* carries exactly
    one marker for the named axis; ``None`` otherwise.

    Multi-marker queries return ``None`` deliberately — the filter
    stays silent on that axis and the picker falls back to plain
    cosine + the other deterministic filters.
    """
    if not text:
        return None
    axis = AXES.get(axis_name)
    if axis is None:
        return None
    excl_re = axis.get("query_exclude_re")
    if excl_re is not None:
        text = excl_re.sub("", text)
    res = _AXIS_QUERY_RES[axis_name]
    has_p = bool(res[POS].search(text))
    has_n = bool(res[NEG].search(text))
    has_b = bool(res[BOTH].search(text)) if BOTH in res else False
    flags = int(has_p) + int(has_n) + int(has_b)
    if flags != 1:
        return None
    if has_p:
        return POS
    if has_n:
        return NEG
    return BOTH


# ── Corpus-side mask building (cached per axis) ──────────────────────


def _build_axis_masks(
    cache: dict,
    axis_name: str,
    axis: dict,
) -> dict[str, np.ndarray]:
    """Tag each LOINC row's state for one axis. Returns ``{POS: bool[N],
    NEG: bool[N], BOTH: bool[N]}``; a row absent from all three is
    unspecified and survives every query state on this axis.
    """
    canonical = np.asarray(cache["canonical"])
    sys_arr = (canonical >> _CODE_BITS) & 0x7
    is_loinc = sys_arr == SYSTEM_TO_CODE["LOINC"]
    names = cache.get("names")
    if not names:
        return {}
    n = canonical.shape[0]
    m_p = np.zeros(n, dtype=bool)
    m_n = np.zeros(n, dtype=bool)
    m_b = np.zeros(n, dtype=bool)
    name_both_re = axis.get("name_both_re")
    for i in np.where(is_loinc)[0]:
        nm = names[i]
        if not nm:
            continue
        if axis["name_pos_re"].search(nm):
            m_p[i] = True
        if axis["name_neg_re"].search(nm):
            m_n[i] = True
        if name_both_re is not None and name_both_re.search(nm):
            m_b[i] = True
    log.info(
        "axis %s mask: pos=%d neg=%d both=%d / %d LOINC rows",
        axis_name,
        int(m_p.sum()),
        int(m_n.sum()),
        int(m_b.sum()),
        int(is_loinc.sum()),
    )
    return {POS: m_p, NEG: m_n, BOTH: m_b}


def _ensure_axis_masks(cache: dict) -> dict[str, dict[str, np.ndarray]]:
    """Lazy cached accessor — one build per cache load, all axes."""
    cached = cache.get("_axis_masks")
    if cached is None:
        cached = {
            name: _build_axis_masks(cache, name, axis)
            for name, axis in AXES.items()
        }
        cache["_axis_masks"] = cached
    return cached


# ── Filter entry point ───────────────────────────────────────────────


def axis_filter_keep(
    query_text: str, cache: dict
) -> "np.ndarray | None":
    """AND-merge the keep mask across every configured axis.

    Returns ``None`` when no axis fires (no opinion → caller skips the
    intersection in :func:`mirobody.indicator.fhir.resolve.pipeline.
    _compose_loinc_keep`).
    """
    all_masks = _ensure_axis_masks(cache)
    if not all_masks:
        return None
    keep: "np.ndarray | None" = None
    for axis_name in AXES:
        state = query_axis_state(query_text, axis_name)
        if state is None:
            continue
        masks = all_masks.get(axis_name) or {}
        if not masks:
            continue
        if state == POS:
            m = ~masks[NEG]
        elif state == NEG:
            m = ~masks[POS]
        else:  # BOTH
            m = ~(masks[POS] | masks[NEG])
        keep = m if keep is None else (keep & m)
    return keep


# Backward-compat alias so ``pipeline.py`` and any external callers
# can import the laterality-era name. New code should use the
# axis-agnostic name.
laterality_keep = axis_filter_keep


__all__ = [
    "POS",
    "NEG",
    "BOTH",
    "AXES",
    "query_axis_state",
    "axis_filter_keep",
    "laterality_keep",
]
