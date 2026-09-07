"""LOINC axis bundle accessors + per-axis-value centroid rerank.

Two layers in one module:

1. **Bundle utilities** — locate and open the LOINC axis bundle
   (``fhir_loinc_bundle.tar.gz`` shipped with mirobody). The bundle holds the
   per-code axis tuple ``(COMPONENT, PROPERTY, TIME_ASPCT, SYSTEM,
   SCALE_TYP, METHOD_TYP)`` plus the skip / demote code lists. Consumed
   by :mod:`fhir.index` for skip/demote mask derivation and by
   the centroid builder below.

2. **Per-axis-value centroids** — for each value V of each axis A
   (except COMPONENT — too high cardinality, flat corpus cosine
   already captures it), the centroid is the unit-normalized mean
   embedding of every LOINC code carrying that value. At query time
   the resolver cosines the query against every axis-value centroid
   and hard-selects a top-1 axis value prediction
   (:func:`predict_axis_top1`). The soft per-axis bonus that sentence
   used to offer as the alternative never had a caller — `adapter.py`
   applies the hard-argmax bonus inline — and was deleted with its
   superseded weight table.

3. **Deterministic CLASS routing** (:func:`apply_deterministic_class_filter`)
   — CLASS is special-cased: not the soft +0.04 bonus the other axes
   use, but a hard sims-matrix mask driven by keyword position. The
   FIRST occurrence in query of any phrase in :data:`CLASS_KEYWORD_GATES`
   (currently ALLERGY / MICRO) picks the CLASS; rows in OTHER classes
   get sims = -inf. A top-K probe falls back to soft when the gated
   CLASS has no candidate species (e.g. P. aeruginosa has zero
   ALLERGY rows in LOINC). First-wins matches Chinese clinical write-
   up convention — outer scope (``细菌病毒检查``) leftmost, sub-panel
   (``过敏性肺炎筛查``) nested rightward; the report-owner CLASS wins.

Why centroids beat hand seeds: the archetype seed system
(:mod:`category._SEED_TERMS`) is a tiny hand-curated approximation to
"which family of measurement is this". LOINC already enumerates every
family through its CLASS / axis columns — so we use LOINC's own data
instead. New corpora (allergens, gut microbiome, sleep PSG, anything)
get classified by the same centroids without retuning.

Cost
----

Build phase (once per process per cache): scan the ~97k-row LOINC axis
bundle, group corpus row indices by (axis, value), mean their cached
embeddings, L2-normalize. Total runtime ~2-3s, ~50 MB of fp32 centroid
matrices.

Query phase: for each axis A with K_A values, one (B × D) @ (D × K_A)
GEMM produces (B × K_A) per-axis cosines. Per-row bonus is a vectorized
gather + mask. Adds ~5-10ms per batch of ~100 queries.
"""

from __future__ import annotations

import csv
import logging
import os
from functools import lru_cache

import numpy as np

from ..common import SYSTEM_TO_CODE, _CODE_BITS, _CODE_MASK, int_to_code

log = logging.getLogger(__name__)


# ── Bundle path + stream opener ───────────────────────────────────────


# Built by ``benchmarks/build_loinc_bundle.py``. Used as the default
# when ``LOINC_TABLE_CSV`` env var / explicit path are unset, so
# deployments don't need to ship the upstream 25 MB CSV.
# Path layers from this file ``mirobody/indicator/fhir/resolve/axis.py``
# up to the package root ``mirobody/`` then into ``res/``. Easy to break
# when the module moves; if the bundle isn't found, ``_resolve_csv_path``
# falls back gracefully (logs WARNING and disables axis rerank), so any
# silent disable shows up in the log first.
# Canonical name of the unified LOINC-derived bundle. All static
# resolver lookups (axis CSV, skip/demote text lists, rank bonus
# array, multilingual alias index) live inside this one tarball — see
# :mod:`..embeddings.bundle`. The constant stays here as the
# axis-rerank consumer's view; other readers use
# :data:`embeddings.bundle.BUNDLE_PATH` directly.
_BUNDLED_LOINC_BUNDLE = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "res",
        "fhir_loinc_bundle.tar.gz",
    )
)
_LOINC_AXIS_MEMBER = "loinc_axis.csv"


@lru_cache(maxsize=1)
def _resolve_csv_path(explicit: str | None) -> str | None:
    path = explicit or os.environ.get("LOINC_TABLE_CSV")
    if path and os.path.isfile(path):
        return path
    if os.path.isfile(_BUNDLED_LOINC_BUNDLE):
        return _BUNDLED_LOINC_BUNDLE
    log.warning(
        "axis re-rank disabled: LoincTableCore.csv not found at %r and "
        "bundled extract missing at %r (rebuild with "
        "benchmarks/build_loinc_bundle.py)",
        path, _BUNDLED_LOINC_BUNDLE,
    )
    return None


def _open_axis_text(path: str, *, member: str | None = None):
    """Open a CSV/TSV stream from a plain file, a .gz, or a member of a .tar.gz.

    Tar members are read fully into memory and returned as a StringIO —
    the bundle is small (~2 MB) and callers are wrapped in lru_cache,
    so the per-call cost is paid at most once per axis.
    """
    if path.endswith(".tar.gz") or path.endswith(".tgz"):
        import io
        import tarfile
        if not member:
            raise ValueError(f"member name required to read from {path!r}")
        with tarfile.open(path, "r:gz") as tf:
            f = tf.extractfile(member)
            if f is None:
                raise FileNotFoundError(f"{member!r} not in {path!r}")
            data = f.read().decode("utf-8")
        return io.StringIO(data)
    if path.endswith(".gz"):
        import gzip
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return open(path, encoding="utf-8", newline="")


# ── Per-axis-value centroids ──────────────────────────────────────────


# Axes covered. COMPONENT is excluded — its ~49k unique values would
# mean a 49k-row centroid matrix that essentially replicates the full
# corpus flat search. SCALE_TYP has only ~11 values but still useful
# (Qn vs Nom vs Doc is a strong type signal). SYSTEM and METHOD_TYP
# have ~2.5k values each — manageable.
#
# CLASS is not a LOINC axis but lives alongside them in
# LoincTableCore.csv (~400 distinct values). Distinguishes lab-flavor
# from scenario-flavor codes: CHAL vs CHAL.ROUTINE, CHEM vs
# ATHLETIC.WEARABLE, PANEL.CHEM vs CHEM, VITAL vs HEM/BC. Wired here so
# the same centroid+bonus pipeline applies — no separate
# infrastructure.
_RANK_AXES: tuple[str, ...] = (
    "PROPERTY", "TIME_ASPCT", "SCALE_TYP", "METHOD_TYP", "SYSTEM", "CLASS",
)

# Axis values to skip when grouping rows. Only the empty string is
# skipped (= column missing from the source row). ``XXX`` and ``-`` are
# kept because LOINC uses them as meaningful axis values — ``XXX`` is
# the "pre-coordinated / not applicable" marker for measurements not
# bound to a specific specimen (Mean heart rate has SYSTEM=XXX
# precisely because heart rate is "of the patient" not "of a sample"),
# and ``-`` is the "no property applicable" marker. Skipping them
# would erase the centroid that anchors most vital-sign codes.
_AXIS_VALUE_SKIP = frozenset({""})

# Default per-axis bonus weights. The flat corpus cosine is in 0.5-0.85
# range for a correct hit; each axis bonus is also 0-1 range but typically
# 0.2-0.5 (centroid is the mean of all rows sharing that axis value, so
# breadth pulls the cosine down). Per-axis weight ~0.03 means a correct
# multi-axis match contributes up to ~0.15 to the combined score —
# enough to flip a borderline candidate, not enough to swamp the flat
# cosine signal.
def load_axis_centroids(cache: dict) -> dict | None:
    """Build per-axis-value centroids over LOINC corpus rows.

    Returned dict (also stored on ``cache`` under ``_axis_centroids``):

    - ``centroids[axis]`` → ``np.ndarray`` ``(K_axis, D)``, each row is a
      unit-L2-normalized centroid embedding for one axis value.
    - ``values[axis]`` → ``list[str]`` in centroid-row order.
    - ``row_value_idx[axis]`` → ``np.ndarray`` ``(N,) int32``: for each
      corpus row, the index into ``values[axis]`` (centroids' row index),
      or ``-1`` if the row is non-LOINC or the axis value is missing/skipped.
    - ``axis_names`` → ordered ``list[str]`` of axes that successfully built.

    Returns ``None`` if the axis bundle is unavailable (deployments
    without the bundled ``fhir_loinc_bundle.tar.gz`` fall back to plain flat
    cosine — no re-rank, but no crash).
    """
    cached = cache.get("_axis_centroids")
    if cached is not None:
        return cached

    path = _resolve_csv_path(None)
    if not path:
        log.warning("axis centroids unavailable: no LOINC axis bundle found")
        cache["_axis_centroids"] = None
        return None

    embs = cache["embs"]
    canonical = cache["canonical"]
    n_rows = int(embs.shape[0])
    d = int(embs.shape[1])
    loinc_sys = SYSTEM_TO_CODE["LOINC"]

    # 1. Load LOINC code → axis values map from the bundle.
    #
    # ``code_to_axes`` only carries the centroid-bearing axes (in
    # :data:`_RANK_AXES`) — COMPONENT is too high-cardinality (~50 k
    # values) to centroid, so it's dropped here. ``code_to_component``
    # carries the COMPONENT axis value separately for the hybrid-output
    # consumer (cf. ``_build_hybrid_axes`` in :mod:`.pipeline`), which
    # needs per-row COMPONENT names but never builds a centroid matrix
    # for them.
    code_to_axes: dict[str, dict[str, str]] = {}
    code_to_component: dict[str, str] = {}
    member = _LOINC_AXIS_MEMBER if path.endswith((".tar.gz", ".tgz")) else None
    with _open_axis_text(path, member=member) as f:
        for row in csv.DictReader(f):
            code = (row.get("LOINC_NUM") or "").strip()
            if not code:
                continue
            axes_dict: dict[str, str] = {}
            for axis in _RANK_AXES:
                v = (row.get(axis) or "").strip()
                if v and v not in _AXIS_VALUE_SKIP:
                    axes_dict[axis] = v
            if axes_dict:
                code_to_axes[code] = axes_dict
            comp = (row.get("COMPONENT") or "").strip()
            if comp:
                code_to_component[code] = comp

    # 2. Join with the cache: for each LOINC corpus row, look up its axis tuple.
    # Iterate canonical once; non-LOINC rows are left with row_axes[r] = None.
    row_axes: list[dict[str, str] | None] = [None] * n_rows
    row_components: list[str | None] = [None] * n_rows
    n_with_axes = 0
    for r in range(n_rows):
        cid = int(canonical[r])
        if (cid >> _CODE_BITS) != loinc_sys:
            continue
        try:
            code = int_to_code(cid & _CODE_MASK, "LOINC")
        except Exception:
            continue
        axes = code_to_axes.get(code)
        if axes:
            row_axes[r] = axes
            n_with_axes += 1
        comp = code_to_component.get(code)
        if comp:
            row_components[r] = comp
    log.info(
        "axis centroids: %d/%d corpus rows have LOINC axis data",
        n_with_axes, n_rows,
    )

    # 3. Per axis, group corpus rows by value, mean & normalize.
    centroids: dict[str, np.ndarray] = {}
    values: dict[str, list[str]] = {}
    row_value_idx: dict[str, np.ndarray] = {}

    for axis in _RANK_AXES:
        value_to_rows: dict[str, list[int]] = {}
        for r in range(n_rows):
            axes = row_axes[r]
            if axes is None:
                continue
            v = axes.get(axis)
            if v is None:
                continue
            value_to_rows.setdefault(v, []).append(r)

        if not value_to_rows:
            log.warning("axis %s: no corpus rows found", axis)
            continue

        sorted_values = sorted(value_to_rows.keys())
        n_values = len(sorted_values)
        cent_mat = np.zeros((n_values, d), dtype=np.float32)
        for i, v in enumerate(sorted_values):
            rows = value_to_rows[v]
            sub = np.asarray(embs[rows], dtype=np.float32)
            # Defensive renormalization: cached fp16 vectors are
            # stored unit-normalized but conversion can drift slightly.
            sub_norms = np.linalg.norm(sub, axis=1, keepdims=True)
            sub = np.divide(
                sub, sub_norms, out=np.zeros_like(sub), where=sub_norms > 0
            )
            c = sub.mean(axis=0)
            cn = float(np.linalg.norm(c))
            if cn > 0:
                c = c / cn
            cent_mat[i] = c
        centroids[axis] = cent_mat
        values[axis] = sorted_values

        # row → value-index lookup; -1 marks rows lacking this axis value.
        value_idx_map = {v: i for i, v in enumerate(sorted_values)}
        ridx = np.full(n_rows, -1, dtype=np.int32)
        for r in range(n_rows):
            axes = row_axes[r]
            if axes is None:
                continue
            v = axes.get(axis)
            if v is None:
                continue
            ridx[r] = value_idx_map.get(v, -1)
        row_value_idx[axis] = ridx

        log.info(
            "axis %s: %d values, %d corpus rows tagged",
            axis, n_values, int((ridx >= 0).sum()),
        )

    out = {
        "centroids": centroids,
        "values": values,
        "row_value_idx": row_value_idx,
        "axis_names": list(centroids.keys()),
        # Per-LOINC-row COMPONENT axis value (high-cardinality, no
        # centroid). Used by hybrid axis output (:mod:`.pipeline`'s
        # ``_build_hybrid_axes``); ``None`` for non-LOINC rows or rows
        # whose COMPONENT column is blank.
        "row_components": row_components,
    }
    cache["_axis_centroids"] = out
    return out


# Per-axis confidence thresholds + rerank weights. Empirically derived
# from the indicators_excel.csv Phase 2 mining (see
# ``benchmarks/mine_phase2.py``):
#
#   axis          predict-accuracy  notes
#   TIME_ASPCT    94.7%             trustworthy; default thresholds
#   SCALE_TYP     86.0%             SemiQn over-predicts (57 cases of
#                                   SemiQn→Qn); tighten margin
#   PROPERTY      76.1%             default thresholds; EntSub clusters
#                                   shadow SCnc for fatty-acid panels
#   SYSTEM        59.5%             borderline; tighten min_score so
#                                   only confident system predictions
#                                   pull (HPV ``Cvx`` cervix-context
#                                   over-predicted vs actual ``XXX``
#                                   specimen-agnostic)
#   METHOD_TYP    39.7%             net-negative; ~2300 method values
#                                   produce noisy centroids and 24% of
#                                   ``predicted`` cases had actual=empty
#                                   (LOINC has no METHOD). bonus weight
#                                   set to 0 — predictions stay in the
#                                   diagnostic column for inspection.
AXIS_THRESHOLDS: dict[str, tuple[float, float]] = {
    # (min_score, min_margin)
    "PROPERTY":   (0.50, 0.02),
    "TIME_ASPCT": (0.50, 0.02),
    "SCALE_TYP":  (0.50, 0.04),
    "METHOD_TYP": (0.65, 0.05),
    "SYSTEM":     (0.55, 0.03),
    # CLASS prediction via centroid is retained only as a fallback
    # diagnostic — the production CLASS routing path is the keyword-
    # position based :func:`apply_deterministic_class_filter` (FIRST-
    # wins over :data:`CLASS_KEYWORD_GATES`). Centroid argmax for
    # CLASS proved unreliable when ALLERGY/SERO or MICRO/SERO compete
    # within ~0.01 cosine — keyword position is a stronger signal.
    "CLASS":      (0.40, 0.02),
}

# Per-axis rerank bonus magnitude (added to flat cosine when a
# candidate row's axis value matches the predicted top-1). METHOD_TYP=0
# disables the bonus despite the per-row prediction being computed —
# diagnostic value preserved, harmful net effect removed.
AXIS_WEIGHTS: dict[str, float] = {
    "PROPERTY":   0.04,
    "TIME_ASPCT": 0.04,
    "SCALE_TYP":  0.03,
    "METHOD_TYP": 0.00,
    "SYSTEM":     0.04,
    # CLASS is no longer a soft bonus axis: it's a hard deterministic
    # filter (see ``apply_deterministic_class_filter``) applied to the
    # raw cosine matrix BEFORE the other axis bonuses. The centroid
    # table built for CLASS in ``load_axis_centroids`` is still loaded
    # because ``apply_deterministic_class_filter`` reads
    # ``axis_data["values"]["CLASS"]`` and ``row_value_idx["CLASS"]``
    # to map per-row CLASS labels; the centroids matrix itself is
    # unused after the keyword-position gate replaced centroid argmax.
    "CLASS":      0.00,
}


def predict_axis_top1(
    query_embs: np.ndarray,
    axis_data: dict,
    thresholds: dict[str, tuple[float, float]] | None = None,
) -> dict[str, np.ndarray]:
    """Per-axis top-1 value index for each query, confidence-gated.

    For each axis A, compute cosine of every query against every value
    centroid of A, take ``argmax`` along the value dimension, and mark
    the prediction as low-confidence (-1) when either
    ``top1_cosine < min_score`` or ``(top1 - top2) < min_margin``.
    Per-axis thresholds (:data:`AXIS_THRESHOLDS`) tune the gate to each
    axis's empirical signal-to-noise — tight on METHOD_TYP (noisy
    2.3k-value centroids), looser on TIME_ASPCT (clean 94% accuracy).

    Returns ``{axis: (B,) int32 ndarray}`` where each entry is the
    centroid-row index into ``axis_data["values"][axis]`` (joinable
    back to LOINC's named axis value), or ``-1`` for low-confidence
    queries on that axis.
    """
    thresholds = thresholds or AXIS_THRESHOLDS
    centroids = axis_data["centroids"]
    axis_names = axis_data["axis_names"]
    predictions: dict[str, np.ndarray] = {}
    for axis in axis_names:
        min_score, min_margin = thresholds.get(axis, (0.50, 0.02))
        cent = centroids[axis]                       # (K, D)
        scores = query_embs @ cent.T                 # (B, K)
        top1_idx = scores.argmax(axis=1).astype(np.int32)
        rows = np.arange(scores.shape[0])
        top1_score = scores[rows, top1_idx]
        if scores.shape[1] > 1:
            top2_score = np.partition(scores, -2, axis=1)[:, -2]
        else:
            top2_score = np.full_like(top1_score, -np.inf)
        confident = (top1_score >= min_score) & (
            (top1_score - top2_score) >= min_margin
        )
        predictions[axis] = np.where(confident, top1_idx, -1).astype(np.int32)
    return predictions


# ── Per-CLASS-value keyword gate ──────────────────────────────────────


# Specific CLASS values that over-fire on the indicators_excel benchmark:
# the centroid argmax is confident enough to fire under the (0.40, 0.02)
# threshold, but the prediction drags the resolver onto a wrong-sibling
# code (correct analyte is in a DIFFERENT class). Examples:
#
#   - ``总免疫球蛋白E``    pred=ALLERGY  but right code is CHEM
#   - ``双酚A IgG+IgA``    pred=ALLERGY  but right code is DRUG/TOX
#   - ``粪便·白细胞``      pred=MICRO    but right code is HEM/BC
#
# Mitigation: per-CLASS-value keyword gate. When the prediction is one
# of these values, require the query to contain at least one licensing
# keyword from the multilingual list below. If not, treat the
# prediction as low-confidence (veto). Other CLASS values pass through
# unchanged — preserves the T-cell %, UA-strip, occult-blood,
# physical-exam wins from the original CLASS axis addition.
#
# Markers use the same Latin-with-``\b`` / CJK-as-substring compile
# convention as :data:`specificity.FAMILIES` — see
# :func:`mirobody.indicator.fhir.resolve.specificity._compile_marker_pattern`.
CLASS_KEYWORD_GATES: dict[str, list[str]] = {
    "ALLERGY": [
        # Deliberately EXCLUDES bare ``IgE`` / ``immunoglobulin E``: a
        # query like ``总免疫球蛋白E`` is a generic immune-profile
        # measurement, not an allergen panel. The gate fires only for
        # explicit allergen-panel context.
        "过敏", "过敏原", "致敏",
        "過敏", "過敏原",
        "アレルギー", "アレルゲン",
        "알레르기", "알레르겐",
        "allergen", "allergy", "allergic", "atopic", "allergens",
        "alérgico", "alergeno", "alergia",
        "Allergen", "Allergie", "allergisch",
        "allergène", "allergie",
        "аллерген", "аллергия",
    ],
    "MICRO": [
        # Explicit microbiology terms. ``粪便·白细胞`` (stool WBC) has
        # neither ``培养`` nor ``菌`` nor ``虫`` etc., so the gate blocks
        # the wrong MICRO bonus. A genuine ``粪便·细菌`` query DOES
        # license — gate is selective, not blanket.
        "微生物", "细菌", "真菌", "酵母", "病毒", "寄生虫", "培养", "革兰", "霉菌",
        "菌群", "球菌", "杆菌",
        "細菌", "酵母菌", "寄生蟲", "培養", "革蘭", "黴菌",
        "ウイルス",
        "미생물", "세균", "바이러스", "기생충", "배양",
        "microorganism", "bacterial", "bacteria", "fungus", "fungal",
        "yeast", "virus", "viral", "parasite", "parasitic", "culture",
        "microbiology", "gram stain", "pathogen",
        "microbio", "microbiología", "hongo", "parásito", "cultivo",
        "Mikrobiologie", "Bakterium", "Bakterien", "Pilz", "Hefe",
        "Parasit", "Kultur",
        "microbiologie", "bactérie", "champignon", "levure", "parasite",
        "микробиология", "бактерия", "грибы", "вирус", "паразит", "посев",
    ],
}


@lru_cache(maxsize=1)
def _class_gate_res() -> dict[str, object]:
    """Compile the gate keyword tables to regexes. Lazy to defer the
    specificity import (axis is imported during cache load before
    specificity)."""
    from .specificity import _compile_marker_pattern
    return {k: _compile_marker_pattern(v) for k, v in CLASS_KEYWORD_GATES.items()}


# Multilingual narrative-history markers that DISABLE every CLASS gate
# when present. These queries (``过敏史 / 病史 / family history /
# anamnesis``) are clinical-narrative document concepts, not lab
# measurements — the LOINC right answer lives in the HX / DOC /
# survey CLASSes (e.g. 10155-0 ``History of allergies, reported``,
# CLASS=HX.MEDS), NOT in ALLERGY / MICRO. Without this carve-out the
# ALLERGY hard-lock masks every history-narrative row when the query
# happens to contain ``过敏``, returning empty.
#
# ``史`` alone is the CJK signal: all benchmark indicators carrying
# it (个人史 / 家族史 / 既往史 / 过敏史 / 用药史 / 现病史 / 手术史 /
# 社会史) are narrative concepts. Latin uses the multi-word forms
# (``past medical`` / ``history of``) to avoid spurious match on
# ``hxA1c`` etc. — bare ``history`` would over-match.
_CLASS_GATE_DISABLERS: list[str] = [
    # CJK
    "史", "既往", "现病", "现病史", "病史", "家族史", "用药史", "手术史",
    # Latin (multi-word to avoid spurious hits)
    "history of", "past medical", "medical record", "anamnesis",
    "antécédent", "antecedente", "Anamnese",
    # Korean / Japanese
    "병력", "기왕력", "既往歴", "現病歴",
    # Russian
    "анамнез",
]


@lru_cache(maxsize=1)
def _class_gate_disabler_re() -> object:
    from .specificity import _compile_marker_pattern
    return _compile_marker_pattern(_CLASS_GATE_DISABLERS)


def _earliest_gate_class(query_text: str, gates: dict) -> str | None:
    """Return the gated CLASS whose keyword appears EARLIEST in
    *query_text*. ``None`` if no gate keyword matches.

    First-wins ordering captures Chinese clinical write-up convention:
    outer scope (报告 / 科室 / panel-level category like "细菌病毒检查",
    "过敏原检测") is written LEFTMOST, with inner sub-panels and the
    indicator itself trailing rightward. The outermost CLASS marker
    is the report owner; nested context further right modifies but
    doesn't override.

    Mirrors the analyte-side LAST-wins rule in
    :func:`.analyte_concept.query_analyte_concept` — CLASS context
    earlier-wins, analyte type later-wins — both reflecting "outer
    context → inner specifics" Chinese parse order.

    Narrative-history disabler: queries carrying any
    :data:`_CLASS_GATE_DISABLERS` marker (``史`` /
    ``past medical`` / ``anamnesis`` …) bypass every CLASS gate
    unconditionally — these are document/section concepts living in
    LOINC's HX / DOC / survey classes, NOT lab classes.
    """
    if _class_gate_disabler_re().search(query_text):
        return None
    best_class = None
    best_pos: int | None = None
    for cls, rx in gates.items():
        m = rx.search(query_text)
        if m is None:
            continue
        if best_pos is None or m.start() < best_pos:
            best_pos = m.start()
            best_class = cls
    return best_class


def apply_deterministic_class_filter(
    sims: np.ndarray,
    axis_data: dict,
    query_texts: list[str] | None,
    sys_arr: np.ndarray,
    loinc_sys_code: int,
) -> None:
    """Hard CLASS routing — mutates *sims* in place.

    For each query whose text contains ANY gated CLASS keyword
    (currently ALLERGY / MICRO), pick the CLASS whose keyword appears
    EARLIEST in the query (first-wins; see
    :func:`_earliest_gate_class` for rationale) and mask out every
    LOINC row whose CLASS differs from the winner.

    Non-LOINC rows and queries with no gate keyword present are
    untouched. Centroid embedding is intentionally NOT consulted —
    when the user types "过敏" / "细菌" the keyword IS the
    authoritative signal; centroid argmax adds noise (the 0.40
    threshold + 0.02 margin can misfire when ALLERGY/SERO or
    MICRO/SERO compete by ~0.01).

    Fallback: if the hard mask would leave a query with zero finite
    LOINC scores (no row in the keyword-winning CLASS within the
    current cosine pool), skip the mask for that query — better an
    embedding-best fallback than an empty result.
    """
    if query_texts is None:
        return
    if "CLASS" not in axis_data.get("values", {}):
        return
    values_list = axis_data["values"]["CLASS"]
    ridx_class = axis_data["row_value_idx"]["CLASS"]    # (N,) int32; -1 = non-LOINC or missing

    gates = _class_gate_res()
    if not gates:
        return
    # Map CLASS name → centroid index for the mask comparison.
    class_to_idx = {v: i for i, v in enumerate(values_list)}

    is_loinc = sys_arr == loinc_sys_code
    # Top-K probe size for the gate-class sanity check. Captures the
    # species/intent neighborhood without being so wide that any LOINC
    # row in the gate's CLASS sneaks in just by being present.
    _TOPK_PROBE = 10
    B = sims.shape[0]
    for b in range(B):
        if b >= len(query_texts):
            continue
        qt = query_texts[b]
        if not qt:
            continue
        winner = _earliest_gate_class(qt, gates)
        if winner is None:
            continue
        cls_idx = class_to_idx.get(winner)
        if cls_idx is None:
            continue
        match_row = (ridx_class == cls_idx) & is_loinc        # (N,) bool
        if not match_row.any():
            continue
        # Sanity check: if the raw-cosine top-K for this query has NO
        # row in the gate-winning CLASS, cosine is telling us the
        # species/intent lives elsewhere (e.g. 绿脓杆菌抗体 has zero
        # ALLERGY-class Pseudomonas rows — every P. aeruginosa code is
        # MICRO). Hard-filtering would force a wrong-species result
        # (Parakeet droppings instead of P. aeruginosa). Skip the
        # filter and let cosine route freely.
        loinc_rows = np.where(is_loinc)[0]
        if loinc_rows.size == 0:
            continue
        live_loinc = loinc_rows[np.isfinite(sims[b, loinc_rows])]
        if live_loinc.size == 0:
            continue
        # ALLERGY hard-lock: skip the top-K probe escape. Clinical rule
        # is unambiguous — when a query carries 过敏 / allergen /
        # atopic, the answer is in the LOINC ALLERGY class (IgE /
        # allergen panels). Drug-allergen queries like ``过敏,左氧氟沙星``
        # frequently have their ``Levofloxacin IgE Ab`` row outside the
        # cosine top-10 because ``levoFLOXacin Susceptibility`` (MICRO)
        # and ``levoFLOXacin Tox`` (DRUG/TOX) rows crowd in; the top-K
        # probe would otherwise let those wrong-CLASS rows win. The
        # only fallback retained is "ALLERGY pool entirely empty within
        # the live cosine window" (e.g. keep_mask already zeroed every
        # ALLERGY row) — there the mask would produce an empty result
        # and we let cosine route freely. The MICRO gate keeps the
        # top-K probe escape (legitimate cases like ``粪便·阿米巴``
        # where cosine puts the right microscopy code at top despite
        # the parasitology-class spread).
        if winner == "ALLERGY":
            if not match_row[live_loinc].any():
                continue
        else:
            k = min(_TOPK_PROBE, live_loinc.size)
            # argpartition for an unsorted top-K — sufficient for set
            # membership; sorting saves us nothing here.
            topk_row_idx = live_loinc[
                np.argpartition(-sims[b, live_loinc], k - 1)[:k]
            ]
            if not match_row[topk_row_idx].any():
                continue
        non_match_loinc = is_loinc & (ridx_class != cls_idx)
        sims[b, non_match_loinc] = -np.inf
