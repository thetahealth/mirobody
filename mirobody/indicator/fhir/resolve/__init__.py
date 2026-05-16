"""Resolve helpers — shared utilities + v2 entry point.

Two coexisting pipelines call into the modules here:

  * **v2** — :mod:`.pipeline` (``resolve_many``): deterministic cosine
    + LOINC keep-mask filter chain + family rerank + deterministic
    CLASS / section-header keyword routing. Used by the CLI
    (``python -m mirobody.indicator resolve``) and the benchmark
    runner. No fitted weights; swappable embedding provider.

  * **v1** — :class:`mirobody.indicator.fhir.adapter.FhirAdapter`:
    full stack with archetype routing, axis rerank, specificity
    demote, alias/dose bonuses, tag gates, and (since 2026-05) the
    same deterministic CLASS / section-header / analyte routing
    overrides that v2 uses. Used by the indicator service for
    production resolution.

The shared modules below feed both paths — v2 uses :mod:`.specificity`
masks, :mod:`.analyte_concept` keep filter, :mod:`.challenge_time`
indices, and :mod:`.axis` (SCALE_TYP row index for caller-supplied
``values=`` boost AND ``apply_deterministic_class_filter``).
v1 uses every module here.

Both paths apply three keyword-position routing overrides BEFORE the
flat cosine scoring, mirrored on opposite sides of the indicator
hierarchy (outer scope vs inner specifics):

  - **CLASS FIRST-wins** (:func:`.axis.apply_deterministic_class_filter`)
    — earliest match in query for any phrase in
    :data:`.axis.CLASS_KEYWORD_GATES` (currently ALLERGY / MICRO)
    decides CLASS routing; hard-masks LOINC rows outside the gated
    CLASS. Top-K probe falls back to soft if no candidate species
    exists in the gated CLASS (e.g. P. aeruginosa has zero ALLERGY
    rows in LOINC).

  - **Analyte LAST-wins** (:func:`.analyte_concept.query_analyte_concept`)
    — last Ag/Ab marker in query text drives the analyte-concept
    keep mask. Top-K probe falls back when species has only the
    opposite marker.

  - **Section-header LAST-wins** (:func:`.category._is_section_header_term`)
    — last segment (split on ``,，|·・``) exact-match against a
    multilingual whitelist forces ``STRATEGY_SECTION_HEADER`` and
    hard-masks to the section-header pool (~2k record-artifact /
    narrative rows).

Centroid-based archetype routing (originally Phase 1 of the v1
pipeline) still picks ``lab`` / ``imaging_pure`` / ``imaging_measure``
/ ``clinical`` / ``genetic`` / ``sleep_report`` strategies via
``strategies_for_terms``, but ``section_header`` is now reached
exclusively via the keyword override above (the seed-based centroid
proved too easy to derail with allergen / anatomy bare nouns).

Query-side augmentation runs in :mod:`..embeddings.preprocess` BEFORE
the embedding call: a multilingual src→canonical-EN lexicon
(``aliases/*.tsv`` members in ``fhir_loinc_bundle.tar.gz``, ~3.4K
entries across zh / ja / ko / fr / es / ru / de) appends the
Latin/English form to source-side queries. This bridges Latin-binomial
gaps the multilingual embedding handles weakly (出芽短梗霉 →
Aureobasidium pullulans, アスペルギルス・フミガーツス → Aspergillus
fumigatus, etc.). Augmentation never modifies the original query text
that the keyword gates see — gates always inspect ``queries[i]``, the
un-augmented form.

For v1, the per-term pipeline order is:

1. **Archetype routing** (:mod:`.category`) — picks strategy
   (lab / sleep / imaging / clinical / genetic) via centroid argmax,
   with a multilingual exact-match keyword override that promotes
   ``section_header`` regardless of centroid.

2. **Axis bonuses** (:mod:`.axis`) — soft per-axis +0.04 cosine
   bonus for PROPERTY / TIME_ASPCT / SCALE_TYP / METHOD_TYP / SYSTEM
   matches (CLASS is no longer here — see deterministic filter
   below).

3. **Functional tags** (:mod:`.functional`) — pool restrictions for
   drug_allergy_panel / vital_signs / document_section /
   microbiology_panel.

4. **Deterministic CLASS / analyte / section-header routing**
   (hard-mask via -inf on the sims matrix; see modules above).

5. **Cosine resolve** (``FhirAdapter._resolve_local_batch``) — flat
   cosine + bonuses, masked by the intersections from steps 1-4.
   Deprecated / demoted candidates pushed below non-deprecated peers.
   Per-vocab top-k emitted.

Re-exports below mirror the per-phase module surfaces so callers can
``from mirobody.indicator.fhir.resolve import strategies_for_terms,
predict_axis_top1, classify_sync`` without reaching into the submodules.
"""

from .axis import (
    _BUNDLED_LOINC_BUNDLE,
    AXIS_THRESHOLDS,
    AXIS_WEIGHTS,
    DEFAULT_AXIS_WEIGHTS,
    axis_rerank_bonus,
    load_axis_centroids,
    predict_axis_top1,
)
from .category import (
    STRATEGY_CLINICAL,
    STRATEGY_GENETIC,
    STRATEGY_IMAGING_MEASURE,
    STRATEGY_IMAGING_PURE,
    STRATEGY_LAB,
    STRATEGY_SECTION_HEADER,
    STRATEGY_SLEEP,
    ResolveStrategy,
    section_header_pool_mask,
    strategies_for_terms,
)
from .functional import (
    TAG_SEEDS,
    classify_sync,
    tag_pool_mask,
)
from .functional import _load_centroids as _load_tag_centroids
from .specificity import (
    DEMOTE_WEIGHT as SPECIFICITY_DEMOTE_WEIGHT,
    FAMILIES as SPECIFICITY_FAMILIES,
    query_licensed_families,
    specificity_penalty,
)
from .analyte_concept import (
    concept_keep_mask as analyte_concept_keep_mask,
    loinc_concept_masks as analyte_loinc_concept_masks,
    query_analyte_concept,
)

__all__ = [
    # Phase 1 — archetype routing
    "ResolveStrategy",
    "STRATEGY_LAB",
    "STRATEGY_SLEEP",
    "STRATEGY_IMAGING_PURE",
    "STRATEGY_IMAGING_MEASURE",
    "STRATEGY_CLINICAL",
    "STRATEGY_GENETIC",
    "STRATEGY_SECTION_HEADER",
    "strategies_for_terms",
    "section_header_pool_mask",
    # Phase 2 — LOINC axis prediction
    "load_axis_centroids",
    "predict_axis_top1",
    "axis_rerank_bonus",
    "AXIS_THRESHOLDS",
    "AXIS_WEIGHTS",
    "DEFAULT_AXIS_WEIGHTS",
    # Phase 3 — functional tags
    "TAG_SEEDS",
    "classify_sync",
    "tag_pool_mask",
    "_load_tag_centroids",
    # Phase 2b — specificity-match demote
    "SPECIFICITY_DEMOTE_WEIGHT",
    "SPECIFICITY_FAMILIES",
    "query_licensed_families",
    "specificity_penalty",
    # Phase 2c — analyte concept (Ag/Ab) hard filter
    "query_analyte_concept",
    "analyte_loinc_concept_masks",
    "analyte_concept_keep_mask",
    # Bundle path (used by embeddings.local for skip/demote derivation).
    # Other callers should prefer ``embeddings.bundle.BUNDLE_PATH``.
    "_BUNDLED_LOINC_BUNDLE",
]
