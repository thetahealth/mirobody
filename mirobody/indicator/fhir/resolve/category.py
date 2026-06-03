"""Per-term resolve strategy.

The lab axis-aware pipeline (LOINC six-axis + SNOMED bridge + RxNorm
ingredient) was designed for routine lab tests. Applied unchanged to
sleep reports, imaging studies, or clinical observation forms it
produces predictable failures: LOINC ``Light sleep duration`` matching
a "PLMS in N1 average" row, RxNorm dopamine agonists matching every
sleep row, ``Bone densimetry abnormal`` matching an "Impression"
section header in a DXA report.

Each indicator category has its own vocabulary fit. This module owns
that mapping via :func:`strategies_for_terms`: every term is embedded
and ranked against archetype centroids derived from a short English
seed list per archetype (:data:`_SEED_TERMS`). The argmax archetype's
strategy gates the resolver's output — which vocab columns to return,
the minimum cosine threshold, and the fallback behaviour.

Two consequences of being centroid-driven and English-anchored:

- **Source-column labels don't route.** A row's archetype is decided
  by the term itself, not by the upstream catalog's ``source`` column.
  A document-section term sitting in an imaging catalog is still
  classified as a section header; a vital-sign term sitting in a
  sleep catalog is still classified as clinical.

- **Multilingual for free.** Gemini's embedding model aligns CJK +
  Korean + English in one space, so Japanese / Korean / Spanish
  inputs classify into the same English-anchored centroid without
  per-language seed lists. Adding a new archetype is 6-12 English
  seed terms plus a strategy entry.

For section headers specifically, the centroid path proved too easy
to derail with bare allergen / anatomy nouns (``刺柏`` / ``姜``
argmaxing to ``section_header`` because the short English seeds were
too close to short nouns in embedding space). A multilingual
keyword override now wins: :func:`_is_section_header_term` checks
the LAST indicator segment (split on ``,，|·・``) for exact match
against a curated whitelist (``Discussion`` / ``Impression`` /
``讨论`` / ``诊断意见`` / ``考察`` / ``토론`` / ...). When it fires,
``STRATEGY_SECTION_HEADER`` wins regardless of centroid argmax. This
matches the LAST-wins convention used by analyte-concept routing
(``query_analyte_concept``) — inner specifics override outer scope.

Downstream code receives the same ``ResolveResult`` shape with
``excluded`` vocabs nulled out.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Literal

log = logging.getLogger(__name__)


# ── Strategy schema ─────────────────────────────────────────────────


@dataclass(frozen=True)
class ResolveStrategy:
    """How to gate resolver output for one archetype of indicator.

    ``excluded`` vocabs are dropped from the returned tuple (or
    null-emitted, depending on how the caller renders).

    ``min_score`` is a per-vocab cosine threshold; a top-1 result below
    threshold is dropped. The threshold compensates for vocab-coverage
    gaps (e.g. LOINC has no PLMS-by-sleep-stage concept, so any LOINC
    match for a PLMS query is by definition a near-miss).

    ``fallback`` controls behaviour when nothing crosses the bar:
    ``best_effort`` returns whatever ranked top-1 anyway (with the
    low score visible); ``null`` returns an empty cell so reviewers
    can tell ``no good match`` apart from ``weakly matched``.
    """
    name: str
    excluded:  frozenset[str]   = frozenset()
    min_score: dict[str, float] = field(default_factory=dict)
    fallback:  Literal["best_effort", "null"] = "best_effort"


# ── Archetypes ──────────────────────────────────────────────────────


# Default for typical lab tests — LOINC primary, SNOMED secondary,
# never RxNorm (this catalog has no medication category; RxNorm only
# bubbles up as semantic noise like ``alanine 5.7 MG/ML`` for ALT).
STRATEGY_LAB = ResolveStrategy(
    name="lab",
    excluded=frozenset({"RXNORM"}),
    min_score={"LOINC": 0.65, "SNOMED_CT": 0.65},
)

# Sleep reports: LOINC has 93xxx sleep-stage panel codes but no
# PLMS-by-stage / arousal-by-stage / desat-by-stage triples, so most
# LOINC top-1s are semantic near-misses (``Light sleep duration`` for
# a PLMS query). Raised threshold + null fallback keeps the LOINC
# column blank for the gaps rather than silently picking a wrong code.
STRATEGY_SLEEP = ResolveStrategy(
    name="sleep_report",
    excluded=frozenset({"RXNORM"}),
    min_score={"LOINC": 0.78, "SNOMED_CT": 0.65},
    fallback="null",
)

# Pure imaging studies (CT, pathology): LOINC has DOC.* / RAD.* codes
# for radiology reports but rarely encodes the finding itself — SNOMED
# procedure / disorder is the right anchor. Drop LOINC entirely; the
# embedding's LOINC top-1 here is always a different concept type.
STRATEGY_IMAGING_PURE = ResolveStrategy(
    name="imaging_pure",
    excluded=frozenset({"RXNORM", "LOINC"}),
    min_score={"SNOMED_CT": 0.65},
)

# DXA-style imaging where LOINC actually has well-coded T-score /
# Z-score / BMD measurements. Keep LOINC but require a high score —
# matches like ``Bone density {Dental arch} Branemark scale`` for
# ``Right Hip BMD`` score in the 0.6s and should null out. Threshold
# 0.76 sits in the narrow band between the worst genuine match in this
# catalog (LOINC 90266-8 ``Hip fracture 10-year probability`` at 0.7639
# for the 10-year fracture risk row) and the highest random-anatomy
# noise (LOINC 80950-9 ``DXA Calcaneus - left [T-score]`` at 0.7514
# for the Impression section header). Tightening above 0.76 nulls real
# matches; loosening below 0.76 readmits the Calcaneus noise.
STRATEGY_IMAGING_MEASURE = ResolveStrategy(
    name="imaging_measure",
    excluded=frozenset({"RXNORM"}),
    min_score={"LOINC": 0.76, "SNOMED_CT": 0.70},
    fallback="null",
)

# Clinical reports / body composition / functional tests: LOINC has
# vital-signs and observation panels but lots of structural / catalog-
# header fields have no LOINC code. Null fallback — concepts with no
# LOINC equivalent (e.g. SpO2 "baseline-during-time-window" segments,
# administrative fields) used to leak a sub-threshold near-miss (the
# ``20341-4 Time/Time 2 by US`` cosine-0.58 class) under best_effort.
# Reviewers couldn't tell ``weak match`` from ``no match at all``;
# null cells make the distinction explicit.
#
# LOINC threshold 0.65 sits in the gap between the wrong-code 0.58
# band (Time-by-US et al.) and the genuine-vital-sign 0.69-0.78 band
# (``103205-1 Mean heart rate`` 0.6934, ``98148-0 12h mean SpO2``
# 0.6982). Tightening above 0.65 nulls the heart-rate aggregates the
# user originally flagged; loosening below readmits the 0.58 junk.
STRATEGY_CLINICAL = ResolveStrategy(
    name="clinical",
    excluded=frozenset({"RXNORM"}),
    min_score={"LOINC": 0.65, "SNOMED_CT": 0.65},
    fallback="null",
)

# Molecular genetics: LOINC MOLPATH class for variant / karyotype
# reports. RxNorm irrelevant. SNOMED gene/variant secondary.
STRATEGY_GENETIC = ResolveStrategy(
    name="genetic",
    excluded=frozenset({"RXNORM"}),
    min_score={"LOINC": 0.65, "SNOMED_CT": 0.65},
)

# Report-section labels (``Discussion`` / ``Impression`` / ``Follow up``
# and their localized equivalents) are not Observation concepts — they
# belong in the document-section / record-artifact space. Resolving
# them against the full corpus picks topical noise (``Bone densimetry
# abnormal`` for "Impression" in a DXA report). The caller restricts
# the candidate pool to section-header rows via
# :func:`section_header_pool_mask` and routes here for output gating.
STRATEGY_SECTION_HEADER = ResolveStrategy(
    name="section_header",
    excluded=frozenset({"RXNORM"}),
    # Pool is ~1.8k narrative / record-artifact rows (vs 678k full),
    # so cosines are 0.05–0.10 below the lab band — a 0.60 floor
    # admits the right ``Narrative comment section of imaging report``
    # / ``Other follow-up method Narrative`` family while still
    # rejecting the long tail (``Frozen section breast specimen`` and
    # such only sneak in via embedding noise around 0.45–0.50).
    min_score={"LOINC": 0.60, "SNOMED_CT": 0.60},
    fallback="null",
)


# Multilingual section-header phrases. Last-segment exact match here
# overrides centroid-based archetype routing — defeats source-prefix
# bias where queries like ``骨密度,讨论`` (DXA report's Discussion
# section) get pulled into ``imaging_measure`` archetype because
# ``骨密度`` dominates the centroid cosine. Exact-match-only, so bare
# allergen / anatomy names that happen to share a substring with these
# phrases don't accidentally trigger.
_SECTION_HEADER_PHRASES: frozenset[str] = frozenset({
    # English (case-folded for lookup)
    "discussion", "impression", "diagnostic impression",
    "findings", "conclusion", "conclusions",
    "summary", "summary note", "recommendation", "recommendations",
    "comments", "comment", "narrative",
    "follow up", "follow-up", "followup",
    "addendum", "remarks",
    # Imaging / examination report sections (also typed as last
    # segments in radiology / DXA / endoscopy / electrophysiology
    # reports — "CT,影像表现", "电生理,检查所见", "病理,病理诊断" all
    # name what the report itself records, not a measured analyte).
    "examination findings", "exam findings", "study findings",
    "imaging findings", "radiology findings",
    "examination results", "study results",
    "abnormal results", "abnormal findings",
    "health guidance", "diagnosis",
    "history of present illness", "chief complaint",
    # Simplified Chinese
    "讨论", "诊断意见", "诊断印象", "印象",
    "随访", "总结", "概要", "评论", "建议", "结论",
    "备注", "意见", "补充", "诊断",
    "影像表现", "影像所见", "检查所见", "检查结果", "所见",
    "异常结果", "异常结果的解释和建议",
    "健康指导", "检查或治疗的建议",
    "主诉", "现病史", "病理诊断",
    # Generic-imaging / general-examination section headings — the
    # last segment names the modality of an imaging report, not a
    # specific measurement (``[门诊报告] 辅助检查·影像学检查`` falls onto
    # ``MR Axilla - left`` without this routing).
    "影像学检查", "影像学", "辅助检查", "实验室检查",
    "心电图检查", "超声检查", "CT 检查", "MRI 检查",
    # Clinical-report section headers — generic section labels that
    # appear as the last segment of multi-segment indicators in
    # outpatient / ED / inpatient report templates. Each of these has
    # a concrete LOINC concept-row target included in
    # :data:`_SECTION_HEADER_NAME_PATTERN`; only phrases with a clean
    # pool target belong here. Phrases like ``医疗决策 / 急诊治疗 /
    # 操作 / 治疗计划 / 出院计划 / 出院指导`` were tried and removed —
    # LOINC has no concept row for them and routing into the pool
    # only drops the working specialty-prefixed Note matches in
    # favor of NEMSIS / Emergency-response narratives.
    "体格检查", "系统回顾", "系统性回顾",
    "评估总结", "评估和计划", "评估计划", "评估/计划",
    "患者活动问题清单", "问题清单",
    "出院诊断", "就诊诊断", "出院医嘱",
    "用药指导",
    # CN 既往史 / 社会史 / 家族史 sub-section leaves — the multi-
    # segment indicator ``社会史·职业 / 既往史·预防接种史 / 家族史`` ends
    # on a section-concept leaf. Without routing here, cosine drifts
    # to over-specific Narrative variants (``Family history of Cancer``
    # / ``History of Childhood diseases`` / ``History of Outpatient
    # visits``) regardless of the right generic concept.
    "家族史", "既往史", "既往病史", "现病史简述",
    "个人史", "社会史", "手术史", "输血史", "过敏史",
    "预防接种史", "用药史", "饮食习惯", "婚姻状况",
    "职业", "喝酒史", "抽烟史",
    # Traditional Chinese
    "討論", "診斷意見", "診斷印象", "隨訪", "總結", "概要", "結論", "備註",
    "診斷", "影像表現", "影像所見", "檢查所見", "檢查結果",
    "主訴", "現病史", "病理診斷",
    # Japanese
    "考察", "所見", "結論", "経過", "経過観察", "追跡",
    "コメント", "フォローアップ", "ディスカッション",
    "画像所見", "検査所見", "検査結果", "主訴", "現病歴",
    # Korean
    "토론", "결론", "요약", "추적", "추적관찰", "코멘트",
    "영상소견", "검사소견", "검사결과", "주소", "현병력",
})

# Separator alphabet for splitting query into source-prefix vs the
# actual indicator label. The LOINC bundle's indicator strings join
# source / sub-panel / indicator with these (the resolve CLI surfaces
# them verbatim from upstream catalog CSV). Mirrors the analyte-side
# LAST-wins logic in :func:`.analyte_concept.query_analyte_concept`.
_INDICATOR_SEPARATOR_RE = re.compile(r"[,，|·・]")


_TRAILING_ORDINAL_RE = re.compile(r"[0-9０-９一二三四五六七八九十]+$")


def _last_indicator_segment(term: str) -> str:
    """Return the LAST non-empty segment of *term* split on common
    indicator-separators, lower-cased. ``骨密度,讨论`` → ``讨论``.
    Empty string when *term* is empty or has no separable segment.
    """
    if not term:
        return ""
    parts = [p.strip() for p in _INDICATOR_SEPARATOR_RE.split(term) if p.strip()]
    return parts[-1].lower() if parts else ""


def _is_section_header_term(term: str) -> bool:
    """Last-segment match against multilingual section-header phrases
    in :data:`_SECTION_HEADER_PHRASES`. Two passes:
      1. Exact match — fast path.
      2. After stripping a trailing ordinal (``补充意见1`` → ``补充意见``,
         ``Note 2`` → ``Note``). Catches enumerated section headers
         that source CSVs emit when a single section appears multiple
         times in the report (``补充意见1`` / ``补充意见2`` /
         ``addendum 1`` / ``addendum 2``).
    """
    seg = _last_indicator_segment(term)
    if seg in _SECTION_HEADER_PHRASES:
        return True
    stripped = _TRAILING_ORDINAL_RE.sub("", seg).strip()
    return bool(stripped) and stripped in _SECTION_HEADER_PHRASES


# ── Section-header candidate pool (corpus-side, one-shot) ───────────


# Names of corpus rows that should serve as the section_header pool:
# SNOMED ``(record artifact)`` hierarchy, LOINC narrative / document-
# section codes (names ending in "Narrative", containing "section" or
# "Study observation"), plus a handful of standalone DCM heading codes
# ("Impression", "Findings", …). Permissive: the embedding ranks within
# the pool, so a few topical near-misses cost precision only, never the
# right answer.
#
# Note: this is a CORPUS-side regex, not a per-term routing regex. It
# scans LOINC/SNOMED display names once at startup to decide which rows
# are eligible candidates for section_header queries. Per-term routing
# is centroid-driven (see :func:`strategies_for_terms`).
_SECTION_HEADER_NAME_PATTERN = re.compile(
    # SNOMED ``(record artifact)`` is the gold marker for the hierarchy.
    r"\(record artifact\)|"
    # LOINC narrative codes (typically end in or contain "Narrative").
    r"\b[Nn]arrative\b|"
    r"Study observation|"
    # Document/comment section names — anchored phrases so a generic
    # "tissue section" / "Cross-Sectional" specimen concept doesn't
    # leak in.
    r"comment section|Document section|section Document|"
    r"section [Ss]et|section ID|"
    # Single-word DCM heading codes.
    r"^(Impression|Findings|Conclusion|Discussion|Addendum|"
    r"Recommendation|Comments?|Remarks?|Summary)s?$|"
    # Concrete section-concept LOINC rows that don't carry the
    # ``Narrative`` marker but ARE genuine report sections — clinical
    # report queries like ``辅助检查·实验室检查 / 实验室检查 / 影像学检查
    # / 评估总结 / 患者活动问题清单`` route through ``_is_section_header_term``
    # but the right answer (``Laboratory report`` / ``Diagnostic imaging
    # study`` / ``Evaluation + Plan note`` / ``Problem list``) was
    # filtered out by the narrow Narrative-only pool. Include them as
    # eligible section candidates.
    r"^Laboratory report$|"
    r"^Clinical pathology Laboratory report$|"
    r"^Laboratory studies\b|"
    r"^Laboratory data\b|"
    r"^Laboratory results?\b|"
    r"^Relevant diagnostic tests\b|"
    r"^Interpretation and review of laboratory results$|"
    r"^Diagnostic imaging study\b|"
    r"^Imaging study set\b|"
    r"^Imaging report\b|"
    r"^Problem list\b|"
    r"^Plan of care( note)?$|"
    r"^Hospital course note$|"
    r"^Evaluation \+ Plan note$|"
    r"^Assessment and plan( note)?$|"
    r"^Vital signs note$|"
    r"^Past medical history\b|"
    r"^Past surgical history\b|"
    r"^Hospital discharge studies summary\b|"
    # ``History of <X>`` rows (CLASS=H&P.HX, PROPERTY=Hx) — section
    # concepts for past illness / surgical / immunization / medication /
    # tobacco / alcohol / occupation / outpatient-visits / family-member
    # diseases. Some don't carry the ``Narrative`` marker (``History of
    # Immunization note`` / ``History of family member diseases note``)
    # so the bare-anchor lift is required to surface them.
    r"^History of [A-Za-z]|"
    r"^Family history\b",
)


_section_header_mask_cache: dict[int, "np.ndarray"] = {}


def section_header_pool_mask(cache: dict) -> "np.ndarray":
    """Boolean mask over ``cache['embs']`` rows: True iff the row is a
    section-header concept. Cached by cache instance id — the same loaded
    cache is reused across resolve_many calls, so the regex scan over
    ~700k names runs once per process per cache.
    """
    import numpy as np

    key = id(cache)
    cached = _section_header_mask_cache.get(key)
    if cached is not None:
        return cached
    names = cache.get("names")
    if names is None:
        raise RuntimeError(
            "section_header_pool_mask requires cache loaded with load_meta=True"
        )
    pat = _SECTION_HEADER_NAME_PATTERN
    mask = np.fromiter(
        (bool(pat.search(n)) for n in names), dtype=bool, count=len(names)
    )
    _section_header_mask_cache[key] = mask
    log.info(
        "section_header pool mask built: %d rows out of %d (%.2f%%)",
        int(mask.sum()), len(names), 100.0 * mask.sum() / max(len(names), 1),
    )
    return mask


# ── Centroid-based term routing ─────────────────────────────────────


# Seed exemplars per archetype. ~8-15 English terms each is enough for
# a stable centroid given Gemini's 1024-dim multilingual embeddings; the
# whole table builds with a single batch API call (<1s).
#
# English-only by design: Gemini aligns CJK / Korean / etc. into the
# same embedding space, so a Chinese or Japanese query (e.g. the
# Chinese equivalent of "mean heart rate") classifies against the
# English centroid without per-language seed tables. Verified
# empirically — see commit history.
#
# Curate seeds to be CENTRAL to the archetype. The centroid is a mean,
# so distinctive central terms anchor it tightly; edge cases drag it
# toward other archetypes. When two archetypes share surface tokens
# (e.g. "oxygen saturation" appears in both clinical SpO2 measurements
# and sleep desat-event aggregates), seed each side with DISTINCTIVE
# variants — clinical gets ``mean oxygen saturation`` / ``minimum
# oxygen saturation``, sleep_report gets ``desaturation count below 90
# percent`` / ``longest desaturation duration`` — so the centroids
# separate cleanly.
_SEED_TERMS: dict[str, tuple[str, ...]] = {
    "lab": (
        "white blood cell count", "blood glucose", "creatinine",
        "total cholesterol", "thyroxine", "ALT", "uric acid",
        "potassium", "bilirubin",
        # Allergen / IgE Ab panel — without these, short allergen names
        # ("Alternaria", "Juniper", "Ertapenem") fall to centroid argmax
        # noise (section_header / imaging_pure) because no seed pulls.
        "Aspergillus fumigatus IgE antibody serum",
        "cow milk IgE antibody serum",
        "drug allergy IgE panel",
        "food allergen mix IgE",
        # Stool microbiology — gut-microbiome panel organism names
        # ("Clostridium", "Anaerotruncus", "Entamoeba") were drifting
        # to imaging_pure on the same OOD-substance-name dynamic.
        "bacteria identified in stool by culture",
        "stool occult blood",
        "stool parasite microscopy",
        # HPV DNA / Ag typing — LOINC files these under MICRO (pathogen
        # detection), not MOLPATH. Without an HPV anchor, the famous
        # high-risk strain "Human papillomavirus type 16" embeds closer
        # to the genetic centroid (BRCA1/EGFR oncogene neighborhood)
        # than to lab, while HPV-11/18/etc. land in lab — splitting an
        # otherwise homogeneous block. This seed keeps all HPV rows in
        # lab.
        "human papillomavirus DNA test",
    ),
    "sleep_report": (
        "sleep latency", "REM sleep duration",
        "periodic limb movement index",
        "oxygen desaturation index", "deep sleep duration", "AHI",
        "obstructive sleep apnea severity",
        # Desat-event seeds use distinctive PSG phrasing so they don't
        # bleed into clinical SpO2 measurement centroids.
        "desaturation count below 90 percent",
        "longest desaturation duration",
        "oxygen saturation drop magnitude",
        # PSG respiratory-event seeds — "Respiratory Analysis · Central /
        # Hypopnea / Mixed / Obstructive" rows were drifting to clinical
        # (sharing "Respiratory" token with "respiratory rate") despite
        # being PSG concepts. These seeds pull them back without
        # touching bare-RR / SpO2-measurement embeddings.
        "obstructive hypopnea count",
        "central apnea count",
        "mixed apnea event",
        "respiratory effort related arousal",
        "flow limitation event count",
        # Body-position-stratified PSG metrics — "Respiratory rate
        # supine" / "Oxygen saturation upright" type queries lose to
        # the clinical centroid (which seeds "respiratory rate" /
        # "baseline oxygen saturation") because the supine/upright
        # qualifier alone isn't distinctive enough. These body-position
        # PSG seeds tighten the sleep_report centroid for those rows.
        "respiratory rate during supine sleep",
        "oxygen saturation supine baseline polysomnogram",
        "body position duration sleep",
        "polysomnogram recording start time",
        "airflow analysis polysomnogram",
    ),
    "imaging_pure": (
        "chest CT scan", "abdominal MRI", "thyroid ultrasound",
        "gastroscopy biopsy pathology", "cervical biopsy", "mammography",
    ),
    "imaging_measure": (
        "hip T-score", "femoral neck Z-score",
        "lumbar bone mineral density", "DXA BMD",
        "total body bone mineral content",
    ),
    # Vital signs in this archetype so DXA / sleep / clinic-report
    # sources that happen to carry heart-rate / SpO2 / RR rows all
    # converge here regardless of upstream category label. Aggregate
    # forms ("mean heart rate", "minimum oxygen saturation") are seeded
    # explicitly because the bare-word centroid otherwise loses ties to
    # sleep_report on qualified row shapes.
    "clinical": (
        "height", "weight", "BMI", "heart rate", "blood pressure",
        "waist circumference", "grip strength", "FEV1",
        "oxygen saturation", "respiratory rate", "body temperature",
        "mean heart rate", "maximum heart rate", "minimum heart rate",
        "mean respiratory rate",
        # Physical-exam findings ("liver palpation / lung auscultation"
        # and equivalents) were drifting to imaging_pure because the
        # organ tokens overlap with abdominal-MRI / chest-CT centroids.
        # Anchoring exam-shape phrases here pulls them back.
        "physical findings of liver",
        "physical findings of thorax and lungs",
        "physical examination general",
        # NOTE: dropped seeds and why —
        # - ``family history of disease`` / ``history of present
        #   illness narrative``: pulled report-section queries
        #   (``基本信息·过敏史``, ``既往病史``) into clinical →
        #   OOD-fallback → lab. The history-narrative form belongs in
        #   section_header now.
        # - ``baseline oxygen saturation`` / ``mean oxygen saturation``
        #   / ``minimum oxygen saturation``: aggregate SpO2 forms are
        #   PSG-flavored; they pulled ``氧饱和度·基线/平均/最低`` from
        #   sleep_report into clinical (then to OOD → lab). Sleep
        #   report keeps its own desaturation/SpO2 seeds.
    ),
    "genetic": (
        "HLA-B27 genotype", "BRCA1 mutation", "karyotype analysis",
        "EGFR mutation", "trisomy 21",
    ),
    "section_header": (
        "Discussion", "Impression", "Conclusion", "Findings",
        "Recommendation", "Summary", "Follow up", "Comments",
        "Narrative", "Diagnostic impression",
        # NOTE: tried "Past medical history narrative" / "Allergies
        # and adverse reactions history" etc. to pull ``基本信息·过敏史
        # / 既往病史`` into section_header. Side-effect: bare-noun
        # allergen names (``刺柏 / 姜 / 桃``) argmaxed to section_header
        # too, and the record-artifact pool mask then nulled their
        # legitimate IgE Ab LOINC matches. Reverted — history rows
        # stay in STRATEGY_LAB, where cosine still finds the proper
        # ``History of X`` codes via the full corpus.
    ),
}

# archetype name → strategy. Must stay in sync with _SEED_TERMS keys.
_ARCHETYPE_STRATEGIES: dict[str, ResolveStrategy] = {
    "lab":               STRATEGY_LAB,
    "sleep_report":      STRATEGY_SLEEP,
    "imaging_pure":      STRATEGY_IMAGING_PURE,
    "imaging_measure":   STRATEGY_IMAGING_MEASURE,
    "clinical":          STRATEGY_CLINICAL,
    "genetic":           STRATEGY_GENETIC,
    "section_header":    STRATEGY_SECTION_HEADER,
}


# Process-cached centroids. Each entry is a unit-L2-normalized 1024-dim
# vector. Cache key is the embedding provider so a config change rebuilds.
_centroids_cache: dict[str, dict[str, "np.ndarray"]] = {}


async def _load_centroids(provider: str) -> dict[str, "np.ndarray"]:
    """Embed seeds and compute one unit-normalized centroid per archetype.

    Cached per provider for the process lifetime — seeds are static and
    embedding is deterministic, so one warmup call suffices.
    """
    import numpy as np
    from mirobody.utils.embedding import text_embedding

    if provider in _centroids_cache:
        return _centroids_cache[provider]

    # One flat batch is cheaper than per-archetype calls (single API
    # roundtrip vs N). Track offsets to slice back into archetypes.
    all_seeds: list[str] = []
    offsets: list[tuple[str, int, int]] = []
    for archetype, seeds in _SEED_TERMS.items():
        start = len(all_seeds)
        all_seeds.extend(seeds)
        offsets.append((archetype, start, len(all_seeds)))

    embs = await text_embedding(all_seeds, provider=provider, cache=True)  # type: ignore[arg-type]
    arr = np.asarray(embs, dtype=np.float32)
    # Drop any failed rows (text_embedding returns None for invalid input).
    # Seeds are static so this is defensive — should never trigger in
    # practice — but a single None row would NaN the mean for that archetype.
    arr = np.nan_to_num(arr)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    arr = np.divide(arr, norms, out=np.zeros_like(arr), where=norms > 0)

    centroids: dict[str, np.ndarray] = {}
    for archetype, lo, hi in offsets:
        c = arr[lo:hi].mean(axis=0)
        n = np.linalg.norm(c)
        if n > 0:
            c = c / n
        centroids[archetype] = c

    _centroids_cache[provider] = centroids
    log.info("category centroids built: %d archetypes, %d seed embeddings",
             len(centroids), len(all_seeds))
    return centroids


# OOD fallback thresholds. The argmax must beat the runner-up by at
# least :data:`_OOD_MIN_MARGIN` AND clear :data:`_OOD_MIN_SCORE`; queries
# that miss either condition default to STRATEGY_LAB. Margin was
# loosened from 0.03 → 0.02 once section_header gained its own
# OOD exemption — body-position-stratified PSG metrics and
# report-section narratives sit in the 0.02-0.03 margin band where the
# argmax is correct but not dominant.
_OOD_MIN_SCORE:  float = 0.70
_OOD_MIN_MARGIN: float = 0.02

# Archetypes whose own strategy has a high-threshold null fallback —
# they filter ambiguous matches more conservatively than ``STRATEGY_LAB``
# (0.65 LOINC + best_effort emit). Argmax into one of these — even at
# low absolute cosine — means the centroid found a credible signal in
# that semantic neighborhood; trust the archetype's own null-fallback
# rather than swapping in LAB, which would let the corpus's wrong-but-
# topical near-misses leak through (e.g. ``REM sleep duration`` for a
# CO2-by-sleep-stage row, or ``Respiratory disturbance index`` for a
# PLMS-by-stage row).
_OOD_FALLBACK_EXEMPT = frozenset({"sleep_report", "imaging_measure"})


async def strategies_for_terms(
    terms: list[str],
    provider: str | None = None,
) -> list[ResolveStrategy]:
    """Resolve one strategy per term via centroid argmax with OOD fallback.

    Every term is embedded once and cosine-ranked against archetype
    centroids (:data:`_SEED_TERMS`). The argmax archetype's strategy
    wins **unless** the centroid call is low-confidence — top-1 cosine
    below :data:`_OOD_MIN_SCORE` or margin to runner-up below
    :data:`_OOD_MIN_MARGIN`. Low-confidence terms default to
    :data:`STRATEGY_LAB`, matching the old regex era's behaviour for
    catalog rows whose ``source`` column matched no category pattern.

    Without the fallback, short OOD terms (single-word allergen
    names, short drug names, physical-exam labels) land on whichever
    centroid happens to be nearest in embedding space — often a wrong
    archetype (section_header pool or imaging_pure that excludes
    LOINC) that nulls a code lab
    would have admitted. Falling back to lab restores the IgE-Ab /
    Physical-findings / panel-marker LOINC codes for these terms at a
    cost of ~0 effort to maintain (vs adding 50+ allergen seeds).

    Cost: one embedding-API call covering every unique term in the
    batch (cached on disk). Centroid GEMM is an (N × K) matmul with
    K = number of archetypes — microseconds at typical batch sizes.
    """
    import numpy as np

    if not terms:
        return []

    from mirobody.utils.config import safe_read_cfg
    from mirobody.utils.embedding import text_embedding

    provider = provider or safe_read_cfg("EMBEDDING_PROVIDER", "gemini")
    centroids = await _load_centroids(provider)
    archetypes = list(centroids)
    cmat = np.stack([centroids[a] for a in archetypes])  # (K, D)

    embs = await text_embedding(terms, provider=provider, cache=True)  # type: ignore[arg-type]
    emb_arr = np.nan_to_num(np.asarray(embs, dtype=np.float32))
    norms = np.linalg.norm(emb_arr, axis=1, keepdims=True)
    emb_arr = np.divide(emb_arr, norms, out=np.zeros_like(emb_arr), where=norms > 0)

    scores = emb_arr @ cmat.T  # (N, K)
    best_idx = scores.argmax(axis=1)

    out: list[ResolveStrategy] = []
    for i, b in enumerate(best_idx):
        # Keyword override (highest precedence): when the term's LAST
        # indicator-segment is a known section-header phrase, force
        # section_header regardless of centroid argmax. Without this,
        # ``骨密度,讨论`` (DXA Discussion section) routes to
        # ``imaging_measure`` because ``骨密度`` dominates the embedding
        # — the resolver then picks a DXA bone-density code that's not
        # even a section concept. Exact-match-only avoids the false-
        # positive trap that killed an earlier seed-based attempt
        # (allergen names like ``刺柏 / 姜 / 桃`` being pulled in).
        if _is_section_header_term(terms[i]):
            out.append(STRATEGY_SECTION_HEADER)
            continue
        row = scores[i]
        top1 = float(row[b])
        runner_up = float(np.partition(row, -2)[-2]) if row.size > 1 else -np.inf
        archetype = archetypes[int(b)]
        low_confidence = top1 < _OOD_MIN_SCORE or (top1 - runner_up) < _OOD_MIN_MARGIN
        if low_confidence and archetype not in _OOD_FALLBACK_EXEMPT:
            out.append(STRATEGY_LAB)
        else:
            out.append(_ARCHETYPE_STRATEGIES[archetype])
    return out
