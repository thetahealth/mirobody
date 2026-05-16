"""Stage-2 functional tags: orthogonal-to-archetype semantic markers.

Each tag is a centroid-driven binary classifier over per-query
embeddings, plus an optional corpus-side LOINC name pattern that
restricts the candidate pool when the tag fires:

- ``drug_allergy_panel`` → pool = LOINC rows whose name contains the
  ``IgE`` token. Catches drug allergens with no LOINC IgE Ab variant
  (Imipenem, Ertapenem, Sulbactam) that otherwise fall through to TDM
  ``[Mass/volume]`` or ``[Susceptibility]`` codes via raw cosine — the
  pool restriction surfaces only IgE Ab candidates, and the
  caller-visible LOINC is empty when none survives.
- ``document_section``   → pool delegated to
  :func:`category.section_header_pool_mask` (record-artifact /
  narrative subset). Same mask the archetype router uses for
  section_header — applying it via the tag layer too is idempotent.
- ``microbiology_panel`` → pool = organism-detection LOINC rows
  across culture (``by ... Culture``), microscopy / morphology
  (``identified in/by ...``), and molecular / immuno presence
  (``Ag [Presence]``, ``DNA [Presence]``, ``RNA [Presence]``). The
  three-way union anchors stool-culture queries while admitting
  HPV / Chlamydia / Norovirus tests whose canonical LOINCs are
  Ag/DNA/RNA-based rather than culture-based.
- ``vital_signs`` is CLASSIFY-ONLY (no pool wired) — its centroid
  bleeds into generic lab analytes and any pool mask killed correct
  lab matches en masse. See ``TAG_NAME_PATTERNS`` for details.

Replaces the hand-curated ``CATEGORY_ANALYTE_RULES`` and
``CATEGORY_HARD_ANALYTE`` rules in the old axis module. Centroid-driven
so non-Chinese inputs and unseen-category labels classify by gestalt
rather than substring match.

Tags are **orthogonal to** archetype routing (:mod:`category`). A query
can be archetype=lab AND tags.drug_allergy_panel=True; archetype gates
the per-vocab thresholds, the tag enforces analyte specificity. Tags
can also fire jointly (e.g. ``document_section`` + ``vital_signs`` for
a "Vital signs note" header) — the per-tag pool masks AND together,
which is the right semantic (only rows that fit every active tag).
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np

log = logging.getLogger(__name__)


# English exemplar seeds per tag. Curate to be CENTRAL to the tag's
# concept so the mean (centroid) anchors tightly without drag from edge
# cases. Mix register variants (FSN-style + colloquial) to span Gemini's
# multilingual embedding space — the seeds are English but Chinese queries
# classify against the same centroids via cross-lingual alignment.
TAG_SEEDS: dict[str, tuple[str, ...]] = {
    "drug_allergy_panel": (
        "Penicillin IgE Ab serum",
        "Cefuroxime IgE antibody serum",
        "drug allergen specific IgE",
        "antibiotic allergy IgE measurement",
        "Tobramycin IgE Ab",
        "Ciprofloxacin IgE Ab",
        "drug hypersensitivity panel",
        "beta-lactam IgE antibody",
        "Cefazolin allergy test serum",
        # Food / environmental allergens — IgE Ab class spans drugs +
        # foods + pollens + dander, and the resolver behavior is the
        # same (IgE pool restriction). Adding allergen-class breadth
        # here keeps the centroid coherent for non-drug allergens.
        "House dust mite IgE antibody serum",
        "Peanut IgE Ab serum",
        "Cat dander IgE Ab",
        "Birch pollen IgE Ab",
        "Cow milk IgE Ab",
    ),
    # Contrast tag with NO pool restriction — its sole purpose is to
    # absorb argmax away from drug_allergy_panel for immune-lab
    # queries (autoantibodies, immunoglobulin subclass quantitation,
    # T-cell counts, complement). Without it, queries like ``抗La抗体
    # / IgG1 / 辅助性T淋巴细胞绝对计数`` argmaxed to drug_allergy_panel
    # (both clusters share the "antibody + serum" semantic) and the
    # IgE-pool restriction nulled their proper autoimmune / Ig LOINC
    # codes, forcing them to land on wrong-IgE rows like ``Cortisone
    # IgE Ab`` or ``Trichothecene IgE Ab``.
    "immune_lab": (
        "Antinuclear antibody serum",
        "Anti-Smith extractable nuclear antibody",
        "Anti-double-stranded DNA antibody",
        "Anti-cyclic citrullinated peptide antibody",
        "Rheumatoid factor IgG serum",
        "Glutamic acid decarboxylase antibody",
        "Anti-La SSB antibody serum",
        "Anti-ribonucleoprotein antibody",
        "IgG subclass 1 serum",
        "Immunoglobulin G quantitative",
        "CD3 CD4 T helper lymphocyte count",
        "CD4 CD8 cells ratio blood",
        "Complement C3 mass volume serum",
    ),
    "vital_signs": (
        # FSN-style anchored seeds — anatomical location + measurement
        # method + concrete units. The generic forms ("heart rate",
        # "body weight") had too much embedding overlap with lab test
        # rows (blood glucose, serum cholesterol) because all are
        # "measurement on a person"; the LOINC bedside-monitoring
        # phrasing tightens the centroid toward true vital signs.
        "Heart rate by Pulse oximetry",
        "Heart rate.atrial by EKG",
        "Systolic blood pressure at right arm sitting by Sphygmomanometer",
        "Diastolic blood pressure at left arm supine by Noninvasive",
        "Mean blood pressure noninvasive",
        "Respiratory rate by Observation",
        "Oxygen saturation in Arterial blood by Pulse oximetry",
        "Body temperature Tympanic membrane",
        "Body temperature Oral",
        "Pulse rate Peripheral artery by Palpation",
        "Body mass index Ratio",
        "Body weight Measured",
        "Body height Measured",
        "Bedside vital signs panel",
    ),
    "document_section": (
        "discharge summary narrative",
        "chief complaint narrative",
        "history of present illness narrative",
        "physical findings narrative",
        "diagnostic impression narrative",
        "diagnosis narrative",
        "findings narrative",
        "consult note synoptic",
        "recommendation narrative",
        "hospital discharge summary",
    ),
    "microbiology_panel": (
        "stool bacterial culture",
        "Escherichia coli identified in stool",
        "Bifidobacterium identified in stool by culture",
        "fecal microbiome organism identification",
        "intestinal bacteria culture",
        "stool ova and parasite microscopy",
        "Clostridium difficile in stool",
        "Lactobacillus identified by culture",
    ),
}


# Activation gating. ``min_score`` is the cosine floor for the top-1 tag —
# below this the query is "none of the above" (no tag fires). ``min_margin``
# is the lead over the runner-up: tied scores (top1 - top2 < margin) mean
# the centroid space couldn't distinguish, so no tag fires either. Empirical
# defaults tuned on the indicators_excel.csv corpus:
#
# - The four tags have well-separated centroids (pairwise centroid-cosine
#   < 0.65), so a query that genuinely belongs to one tag scores ~0.05+
#   above the others.
# - Negative samples (typical lab tests like ``血糖``) score 0.4-0.5 on
#   every tag, so a 0.55 floor keeps them in the no-tag bucket.
_MIN_SCORE: float = 0.55
_MIN_MARGIN: float = 0.03


# Process-cached centroids — seeds are static and embedding is
# deterministic, so one warmup call per process suffices. Key is the
# embedding provider so a config change rebuilds.
_centroids_cache: dict[str, dict[str, "np.ndarray"]] = {}


async def _load_centroids(provider: str) -> dict[str, "np.ndarray"]:
    """Embed seeds and compute one unit-normalized centroid per tag.

    One flat batch is cheaper than per-tag calls (single API roundtrip vs
    N). Cached per provider for the process lifetime.
    """
    import numpy as np
    from mirobody.utils.embedding import text_embedding

    if provider in _centroids_cache:
        return _centroids_cache[provider]

    all_seeds: list[str] = []
    offsets: list[tuple[str, int, int]] = []
    for tag, seeds in TAG_SEEDS.items():
        start = len(all_seeds)
        all_seeds.extend(seeds)
        offsets.append((tag, start, len(all_seeds)))

    embs = await text_embedding(all_seeds, provider=provider, cache=True)  # type: ignore[arg-type]
    arr = np.asarray(embs, dtype=np.float32)
    arr = np.nan_to_num(arr)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    arr = np.divide(arr, norms, out=np.zeros_like(arr), where=norms > 0)

    centroids: dict[str, np.ndarray] = {}
    for tag, lo, hi in offsets:
        c = arr[lo:hi].mean(axis=0)
        n = np.linalg.norm(c)
        if n > 0:
            c = c / n
        centroids[tag] = c

    _centroids_cache[provider] = centroids
    log.info(
        "functional tag centroids built: %d tags, %d seed embeddings",
        len(centroids), len(all_seeds),
    )
    return centroids


# Tag → corpus-side LOINC name pattern for candidate-pool restriction.
# Each pattern is matched (case-sensitive) against the LOINC display
# name; rows that match form the tag's allowed-candidate pool. When a
# query fires the tag, the resolver intersects this pool into the
# LOINC system mask so only matching rows compete.
#
# ``document_section`` deliberately has no entry here — it delegates
# to :func:`category.section_header_pool_mask`, which already
# maintains the record-artifact / narrative pattern with the right
# breadth for the existing archetype router.
TAG_NAME_PATTERNS: dict[str, "re.Pattern[str]"] = {
    "drug_allergy_panel": re.compile(r"\bIgE\b"),
    # ``vital_signs`` is INTENTIONALLY absent: its centroid sits too
    # close to generic "measurement on a person" lab analytes (blood
    # glucose, total cholesterol, serum insulin) for the classifier to
    # cleanly separate, so applying a pool mask whenever the tag fires
    # killed correct lab matches en masse in the indicators_excel.csv
    # full benchmark. The tag still classifies (annotation-only) until
    # the seed list / threshold can be reworked to anchor more
    # firmly in bedside-monitoring concepts.
    # ``microbiology_panel`` pool admits the three shapes that any
    # legitimate organism-detection LOINC takes:
    #   1. Culture-based ID (``by Organism specific Culture``,
    #      ``Bacteria identified by Aerobe Culture``).
    #   2. Microscopy / morphology ID (``Ova and parasites identified
    #      in Stool by Trichrome stain``, ``Virus identified in Sputum
    #      by Electron microscopy``).
    #   3. Molecular / immuno presence (``Ag [Presence]``,
    #      ``DNA [Presence]``, ``RNA [Presence]``) — added to the
    #      original culture-only pattern after the HPV / Chlamydia /
    #      HSV / Norovirus regressions: those queries' correct top-1s
    #      sit in the Ag/DNA/RNA family, not culture, so a
    #      culture-only mask demoted ~0.82 specific picks to ~0.66
    #      ``Virus identified by Culture`` generics. Including Ag/DNA/
    #      RNA tokens is safe vs Ab serology (which uses ``IgG Ab`` /
    #      ``IgM Ab`` / ``IgA Ab`` and never ``Ag``/``DNA``/``RNA``).
    "microbiology_panel": re.compile(
        r"by (?:Organism specific |Aerobe |Anaerobe |Mycobacterial "
        r"|Fungal |Yeast )?[Cc]ulture\b"
        r"|\bidentified (?:in|by)\b"
        r"|\b(?:Ag|DNA|RNA)\s*\[",
    ),
}


def tag_pool_mask(tag: str, cache: dict) -> "np.ndarray | None":
    """Return the LOINC-row mask for *tag*, computed lazily and cached on *cache*.

    The mask is restricted to LOINC system rows whose display name
    matches the tag's pattern (or, for ``document_section``, the
    record-artifact / narrative subset from
    :func:`category.section_header_pool_mask`).

    Returns ``None`` when the tag has no defined pool restriction or
    the cache lacks display names. Cached results live on the cache
    dict under ``_tag_pool_mask::<tag>``.
    """
    import numpy as np

    if tag == "document_section":
        from .category import section_header_pool_mask
        return section_header_pool_mask(cache)
    pattern = TAG_NAME_PATTERNS.get(tag)
    if pattern is None:
        return None
    names = cache.get("names")
    if names is None:
        return None
    key = f"_tag_pool_mask::{tag}"
    cached = cache.get(key)
    if cached is not None:
        return cached
    mask = np.fromiter(
        (bool(pattern.search(n)) for n in names),
        dtype=bool, count=len(names),
    )
    cache[key] = mask
    log.info(
        "tag pool mask %s: %d / %d names matched",
        tag, int(mask.sum()), len(names),
    )
    return mask


def classify_sync(
    query_embs: "np.ndarray",
    centroids: dict[str, "np.ndarray"],
) -> dict[str, "np.ndarray"]:
    """Per-tag bool flag for each query in *query_embs*.

    Sync because the caller preloads *centroids* once (async warmup via
    :func:`_load_centroids`) and reuses them across batches — the GEMM
    here is microseconds at typical batch sizes.

    Each tag fires when its centroid cosine is both above :data:`_MIN_SCORE`
    AND ahead of every other tag's cosine by :data:`_MIN_MARGIN`. The
    mutual-exclusion via top-1+margin keeps closely-related tags
    (drug_allergy_panel vs microbiology_panel both cluster in the
    "specimen+organism+test" semantic neighborhood) from co-firing on
    borderline queries.

    Returns ``{tag_name: (B,) bool ndarray}`` for every tag in
    :data:`TAG_SEEDS`.
    """
    import numpy as np

    tags = list(centroids)
    if not tags:
        return {}
    cmat = np.stack([centroids[t] for t in tags])  # (T, D)

    Q = np.asarray(query_embs, dtype=np.float32)
    norms = np.linalg.norm(Q, axis=1, keepdims=True)
    Q = np.divide(Q, norms, out=np.zeros_like(Q), where=norms > 0)

    scores = Q @ cmat.T  # (B, T)
    top1_idx = scores.argmax(axis=1)
    top1_score = scores[np.arange(scores.shape[0]), top1_idx]
    if scores.shape[1] > 1:
        top2_score = np.partition(scores, -2, axis=1)[:, -2]
    else:
        top2_score = np.full_like(top1_score, -np.inf)
    confident = (top1_score >= _MIN_SCORE) & (
        (top1_score - top2_score) >= _MIN_MARGIN
    )

    out: dict[str, np.ndarray] = {}
    for i, tag in enumerate(tags):
        out[tag] = confident & (top1_idx == i)
    return out
