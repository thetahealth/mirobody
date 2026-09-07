"""Build per-axis LOINC vocabularies + their multilingual embeddings.

Two subcommands form a 2-step pipeline:

  ``loinc-axis-vocab``  reads ``$LOINC_DIR`` (default ``~/ref/Loinc_2.82``)
                        and writes:
                        - ``mirobody/res/loinc_axes/<AXIS>.tsv`` — one TSV
                          per axis (6 LOINC axes + CLASS), columns
                          ``part_name`` / ``count`` / ``translations``
                          (locale-ordered, deduped, comma-joined foreign
                          segments from all 22 LinguisticVariant locales;
                          entries identical to ``part_name`` dropped).
                          ⚠️ Translations themselves can contain commas
                          (Italian / Dutch postpositive modifiers like
                          ``Cancro al polmone, panel``), so consumers
                          can't naively split on ``,`` to recover per-
                          locale strings — they use the column as one
                          opaque multilingual blob.
                        - ``mirobody/res/loinc_axes/code_index.npz`` —
                          per-LOINC-code 6-axis row indices + viable
                          anchor masks (powers the resolve LOINC lookup).
                        - ``mirobody/indicator/fhir/loinc_lookups.py`` —
                          8 hand-keyed dicts for controlled-vocab Part
                          types (SCALE, PROPERTY, TIME, TIME_MODIFIER,
                          RAD_LATERALITY, RAD_LATERALITY_PRESENCE,
                          RAD_MODALITY_TYPE, RAD_PHARM_ROUTE). Per-
                          PartType-scoped translation extraction so
                          string collisions like ``IA`` = Immunoassay
                          (METHOD) vs Intra-arterial
                          (Rad.Pharmaceutical.Route) cannot pollute.

  ``loinc-axis-emb``    reads the TSVs and writes a single
                        ``mirobody/res/loinc_axes_embeddings.npz`` —
                        for each axis, two row-aligned members:
                        ``<AXIS>`` (fp16 ``(N, 1024)`` L2-normalized
                        embeddings) and ``<AXIS>_names`` (object array
                        of part-name strings). Per-row embed input is
                        ``part_name,translations`` (matches the TSV
                        serialization minus tab + count). With names
                        in the bundle, the runtime resolver doesn't
                        need the TSVs at all — they become a build-
                        time-only artifact (useful for review / debug,
                        excludable from the deployed package). Uses
                        the SQLite cache at
                        ``~/.cache/mirobody/text_embedding.sqlite`` —
                        re-runs only re-hit the API for changed rows.

Translation alignment (vocab step): LOINC LinguisticVariant translators
preserve the structural separators (``^`` ``+`` ``>`` ``&`` ``.``)
positionally — ``Glucose^post CFST`` ↔ ``葡萄糖^刺激后`` decomposes
into (Glucose ↔ 葡萄糖) and (post CFST ↔ 刺激后). For each
``(lang, en_segment)`` pair seen in the corpus we record the most
common foreign segment.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import shutil
from argparse import Namespace
from collections import Counter, defaultdict

import numpy as np

from ..common import EMBEDDING_DIM
from ..index import RES_DIR

log = logging.getLogger(__name__)


# ─── Constants ───────────────────────────────────────────────────────

# axes.py lives at mirobody/indicator/fhir/embeddings/axes.py; the
# auto-generated lookup module sits one level up.
_LOOKUPS_PY = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "loinc_lookups.py")
)

# Single npz bundle for all per-axis vocab embeddings AND the per-LOINC-
# code axis index — sibling of the build-time ``loinc_axes/`` TSV dir
# under ``res/``. Two build commands both load-modify-save:
#   ``loinc-axis-vocab`` refreshes the code-index members
#   ``loinc-axis-emb``   refreshes the per-axis embedding + names members
# Members:
#   <AXIS>             fp16 (N_axis, 1024)  vocab embeddings
#   <AXIS>_names       object (N_axis,)     vocab part-name strings
#   codes              object (M,)          LOINC code strings (sorted)
#   code_longnames     object (M,)          LongCommonName per code
#   idx_<AXIS>         int32 (M,)           code's row in axis vocab; -1
#   viable_<AXIS>      bool (N_axis,)       vocab row used as a full
#                                           LOINC axis value (COMPONENT
#                                           additionally drops ``^``-compounds)
_BUNDLE_BASENAME = "fhir_loinc_axes_embeddings.npz"

# Controlled-vocab PartTypes that don't make sense to embed (codes like
# ``Qn`` / ``MCnc`` / ``Pt``); emit these as Python dict literals.
_LOOKUP_PART_TYPES: list[tuple[str, str]] = [
    ("SCALE",                   "SCALE"),
    ("PROPERTY",                "PROPERTY"),
    ("TIME",                    "TIME"),
    ("TIME_MODIFIER",           "TIME MODIFIER"),
    ("RAD_LATERALITY",          "Rad.Anatomic Location.Laterality"),
    ("RAD_LATERALITY_PRESENCE", "Rad.Anatomic Location.Laterality.Presence"),
    ("RAD_MODALITY_TYPE",       "Rad.Modality.Modality Type"),
    ("RAD_PHARM_ROUTE",         "Rad.Pharmaceutical.Route"),
]

# PartTypeName → parent axis. Anything not listed is skipped (currently:
# Document.*, Rad.Subject, Rad.Reason for Exam — they don't align with
# the 6 axes; revisit when we start handling clinical-document LOINCs).
_AXIS_MAP: dict[str, str] = {
    "COMPONENT": "COMPONENT",
    "ADJUSTMENT": "COMPONENT",
    "CHALLENGE": "COMPONENT",
    "COUNT": "COMPONENT",
    "DIVISOR": "COMPONENT",
    "GENE": "COMPONENT",
    "NUMERATOR": "COMPONENT",
    "SUFFIX": "COMPONENT",
    "PROPERTY": "PROPERTY",
    "TIME": "TIME_ASPCT",
    "TIME MODIFIER": "TIME_ASPCT",
    "Rad.Timing": "TIME_ASPCT",
    "SYSTEM": "SYSTEM",
    "SUPER SYSTEM": "SYSTEM",
    "Rad.Anatomic Location.Region Imaged": "SYSTEM",
    "Rad.Anatomic Location.Imaging Focus": "SYSTEM",
    "Rad.Anatomic Location.Laterality": "SYSTEM",
    "Rad.Anatomic Location.Laterality.Presence": "SYSTEM",
    "SCALE": "SCALE_TYP",
    "METHOD": "METHOD_TYP",
    "Rad.Modality.Modality Type": "METHOD_TYP",
    "Rad.Modality.Modality Subtype": "METHOD_TYP",
    "Rad.View.View Type": "METHOD_TYP",
    "Rad.View.Aggregation": "METHOD_TYP",
    "Rad.Maneuver.Maneuver Type": "METHOD_TYP",
    "Rad.Pharmaceutical.Substance Given": "METHOD_TYP",
    "Rad.Pharmaceutical.Route": "METHOD_TYP",
    "Rad.Guidance for.Action": "METHOD_TYP",
    "Rad.Guidance for.Presence": "METHOD_TYP",
    "Rad.Guidance for.Object": "METHOD_TYP",
    "Rad.Guidance for.Approach": "METHOD_TYP",
    "CLASS": "CLASS",
}

# Fields per row in Loinc.csv / LinguisticVariant CSVs used to build the
# translation lookup. The 6 axes + CLASS + the two display strings.
_ALIGN_FIELDS = (
    "COMPONENT", "PROPERTY", "TIME_ASPCT", "SYSTEM",
    "SCALE_TYP", "METHOD_TYP", "CLASS",
    "LONG_COMMON_NAME", "SHORTNAME",
)

# Structural separators in LOINC's display grammar. Translators preserve
# them positionally, so splitting on the same regex yields aligned tokens.
_SEP_RE = re.compile(r"(\^|\+|>|&|\.)")

# The 6 standard LOINC axes — CLASS is intentionally excluded (it's a
# taxonomic bucket, not a measurement-semantic axis). Drives both
# the vocab build's ``code_index.npz`` columns and the resolve-axes
# query phase.
_RESOLVE_AXES: tuple[str, ...] = (
    "COMPONENT", "PROPERTY", "TIME_ASPCT",
    "SYSTEM", "SCALE_TYP", "METHOD_TYP",
)


# TIME_ASPCT auto-fill — LOINC LinguisticVariants don't cover number-
# prefixed duration codes like ``100D`` / ``1.5H post`` (they translate
# round-number forms like ``1H`` / ``2H`` but skip the long tail), so
# the corresponding TSV rows ship empty and the embedder loses cross-
# lingual signal for OGTT-style timing terms. The synthesis below kicks
# in only when the LinguisticVariant lookup is empty for a row — it
# never overrides translator output. Pattern: ``<number><unit>[ post]``,
# matched after each axis's locale lookup runs.
#
# ``post`` is rendered as "after" (后 / nach / post / 후 / …) because
# LOINC's TIME_ASPCT semantic is "measured N units AFTER a challenge,
# dose, or meal" (e.g., ``Glucose 2H post CFST`` = glucose at +2h
# after challenge); rendering as 前/ago/before would invert it.
_TIME_UNIT_LOCALE: dict[str, dict[str, str]] = {
    "H":  {"zh": "小时", "de": "Stunden", "es": "horas", "fr": "heures",
           "it": "ore", "ko": "시간", "nl": "uur", "pt": "horas",
           "ru": "часов", "cs": "hodin", "pl": "godzin", "el": "ώρες",
           "tr": "saat", "ar": "ساعات", "uk": "годин", "et": "tundi"},
    "M":  {"zh": "分钟", "de": "Minuten", "es": "minutos", "fr": "minutes",
           "it": "minuti", "ko": "분", "nl": "minuten", "pt": "minutos",
           "ru": "минут", "cs": "minut", "pl": "minut", "el": "λεπτά",
           "tr": "dakika", "ar": "دقائق", "uk": "хвилин", "et": "minutit"},
    "D":  {"zh": "天", "de": "Tage", "es": "días", "fr": "jours",
           "it": "giorni", "ko": "일", "nl": "dagen", "pt": "dias",
           "ru": "дней", "cs": "dnů", "pl": "dni", "el": "ημέρες",
           "tr": "gün", "ar": "أيام", "uk": "днів", "et": "päeva"},
    "W":  {"zh": "星期", "de": "Wochen", "es": "semanas", "fr": "semaines",
           "it": "settimane", "ko": "주", "nl": "weken", "pt": "semanas",
           "ru": "недель", "cs": "týdnů", "pl": "tygodni", "el": "εβδομάδες",
           "tr": "hafta", "ar": "أسابيع", "uk": "тижнів", "et": "nädala"},
    "Mo": {"zh": "个月", "de": "Monate", "es": "meses", "fr": "mois",
           "it": "mesi", "ko": "개월", "nl": "maanden", "pt": "meses",
           "ru": "месяцев", "cs": "měsíců", "pl": "miesięcy", "el": "μήνες",
           "tr": "ay", "ar": "أشهر", "uk": "місяців", "et": "kuu"},
    "Y":  {"zh": "年", "de": "Jahre", "es": "años", "fr": "ans",
           "it": "anni", "ko": "년", "nl": "jaren", "pt": "anos",
           "ru": "лет", "cs": "let", "pl": "lat", "el": "έτη",
           "tr": "yıl", "ar": "سنوات", "uk": "років", "et": "aastat"},
    "S":  {"zh": "秒", "de": "Sekunden", "es": "segundos", "fr": "secondes",
           "it": "secondi", "ko": "초", "nl": "seconden", "pt": "segundos",
           "ru": "секунд", "cs": "sekund", "pl": "sekund",
           "tr": "saniye", "uk": "секунд"},
    "ms": {"zh": "毫秒", "de": "Millisekunden", "es": "milisegundos",
           "fr": "millisecondes", "it": "millisecondi", "ko": "밀리초",
           "nl": "milliseconden", "pt": "milissegundos", "ru": "миллисекунд"},
}
_TIME_POST_LOCALE: dict[str, str] = {
    "zh": "后", "de": "danach", "es": "post", "fr": "post", "it": "dopo",
    "ko": "후", "nl": "na", "pt": "pós", "ru": "после", "cs": "po",
    "pl": "po", "el": "μετά", "tr": "sonra", "ar": "بعد", "uk": "після",
    "et": "pärast",
}
# Mo before M so the engine doesn't strip "M" off "2Mo" and leave "o".
_TIME_PATTERN_RE = re.compile(r"^(\d+(?:\.\d+)?)(Mo|H|M|D|W|Y|S|ms)(\s+post)?$")
# CJK locales render numerals adjacent to the unit (``1小时``, ``1시간``);
# everything else gets a space (``1 Stunde``).
_NO_SPACE_LANGS = frozenset({"zh", "ja", "ko"})


def _synthesize_time_translations(part_name: str, lang_tags: list[str]) -> list[str]:
    """Locale-ordered list of synthesized translations for time-aspect
    codes the LinguisticVariant translators skip (e.g. ``100D``,
    ``1.5H post``). Returns ``[]`` if *part_name* doesn't match the
    duration-code pattern — the caller leaves the row empty in that
    case (covers ``*`` / ``-`` / ``Post fatty meal`` / ``WO & W``).
    """
    m = _TIME_PATTERN_RE.match(part_name)
    if not m:
        return []
    n, unit, post = m.groups()
    unit_trans = _TIME_UNIT_LOCALE.get(unit, {})
    out: list[str] = []
    seen: set[str] = set()
    for tag in lang_tags:
        lang = tag.split("-", 1)[0]
        suf = unit_trans.get(lang)
        if not suf:
            continue
        sep = "" if lang in _NO_SPACE_LANGS else " "
        s = f"{n}{sep}{suf}"
        if post:
            post_suf = _TIME_POST_LOCALE.get(lang)
            if post_suf:
                s = f"{s}{sep}{post_suf}"
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


# ─── Phase 1: Vocab build (TSVs + loinc_lookups.py) ──────────────────

def _load_active_rows(src: str) -> dict[str, dict[str, str]]:
    """Read Loinc.csv keeping clinical-lab ACTIVE rows only.

    STATUS filter: keeps only ``ACTIVE``; ``DEPRECATED`` / ``DISCOURAGED``
    are obsolete, ``TRIAL`` is provisional (excluded from the vocab —
    legacy resolver soft-demotes them, but for the axis vocab we want
    only stable ground truth).

    CLASS / CLASSTYPE filter: mirrors :data:`..common._SKIP_CLASS_PREFIXES`
    + :data:`..common._SKIP_CLASSTYPES` so that surveys (PROMIS / PHENX),
    document templates (DOC.\\*), claim attachments, and admin codes
    don't pollute the per-axis vocab or the LOINC-lookup code_index
    with non-clinical part values.
    """
    from ..common import _SKIP_CLASS_PREFIXES, _SKIP_CLASSTYPES
    csv.field_size_limit(2**24)
    out: dict[str, dict[str, str]] = {}
    n_status_drop = n_class_drop = 0
    with open(src, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("STATUS") or "").upper() != "ACTIVE":
                n_status_drop += 1
                continue
            cls = row.get("CLASS") or ""
            if any(cls.startswith(p) for p in _SKIP_CLASS_PREFIXES):
                n_class_drop += 1
                continue
            if (row.get("CLASSTYPE") or "") in _SKIP_CLASSTYPES:
                n_class_drop += 1
                continue
            cells: dict[str, object] = {k: (row.get(k) or "") for k in _ALIGN_FIELDS}
            try:
                # COMMON_TEST_RANK ranks codes by real-lab usage frequency
                # (1 = most common, 0 = unranked). LOINC ships it in
                # Loinc.csv; the resolve-axes LOINC lookup uses it to
                # break ties when the per-axis filter chain can't narrow
                # to a single code (e.g. ``叶酸`` lacks specimen info, so
                # 7 codes survive; sorting by rank surfaces ``2284-8``
                # Folate in Serum/Plasma = rank 265 = clinical default).
                cells["__rank__"] = int(row.get("COMMON_TEST_RANK") or 0)
            except ValueError:
                cells["__rank__"] = 0
            out[row["LOINC_NUM"]] = cells  # type: ignore[assignment]
    log.info("LOINC filter: kept %d codes (skipped %d non-ACTIVE, %d non-clinical class/type)",
             len(out), n_status_drop, n_class_drop)
    return out


def _discover_variants(variants_dir: str) -> list[tuple[str, str]]:
    """Return ``[(lang_tag, csv_path), ...]`` sorted by lang_tag."""
    out: list[tuple[str, str]] = []
    for fname in os.listdir(variants_dir):
        if not fname.endswith("LinguisticVariant.csv"):
            continue
        stem = fname[:-len("LinguisticVariant.csv")]
        if not stem or stem == "s":  # skip the LinguisticVariants.csv index
            continue
        lang = stem[:2]
        country = "".join(c for c in stem[2:] if c.isalpha())
        tag = f"{lang}-{country}" if country else lang
        out.append((tag, os.path.join(variants_dir, fname)))
    return sorted(out)


def _load_variant(path: str) -> dict[str, dict[str, str]]:
    csv.field_size_limit(2**24)
    out: dict[str, dict[str, str]] = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            cells = {k: (row.get(k) or "").strip() for k in _ALIGN_FIELDS}
            cells = {k: v for k, v in cells.items() if v}
            if cells:
                out[row["LOINC_NUM"]] = cells
    return out


def _align_segments(en: str, foreign: str) -> list[tuple[str, str]] | None:
    """Split EN + foreign on structural separators and pair positionally.

    Returns the list of ``(en_segment, foreign_segment)`` pairs if the
    two strings have the same separator skeleton, else ``None``.
    """
    en_parts = _SEP_RE.split(en)
    fo_parts = _SEP_RE.split(foreign)
    if len(en_parts) != len(fo_parts):
        return None
    for i in range(1, len(en_parts), 2):
        if en_parts[i] != fo_parts[i]:
            return None
    return [
        (en_parts[i].strip(), fo_parts[i].strip())
        for i in range(0, len(en_parts), 2)
        if en_parts[i].strip() and fo_parts[i].strip()
    ]


def _build_translation_lookup(
    active_rows: dict[str, dict[str, str]],
    by_lang: dict[str, dict[str, dict[str, str]]],
) -> dict[tuple[str, str], str]:
    """For each (lang_tag, en_segment), pick the most common foreign segment.

    Records both whole-axis-value pairs (catches composite Parts like
    ``Velocity.systolic.maximum``) and positional segment pairs (catches
    atomic sub-Parts like ``systolic``).
    """
    pair_counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for loinc, en_cells in active_rows.items():
        for field in _ALIGN_FIELDS:
            en_val = en_cells.get(field, "")
            if not en_val:
                continue
            for lang_tag, lang_data in by_lang.items():
                fo_val = lang_data.get(loinc, {}).get(field, "")
                if not fo_val:
                    continue
                pair_counts[(lang_tag, en_val)][fo_val] += 1
                pairs = _align_segments(en_val, fo_val)
                if not pairs:
                    continue
                for en_seg, fo_seg in pairs:
                    if en_seg != en_val:
                        pair_counts[(lang_tag, en_seg)][fo_seg] += 1
    return {key: counter.most_common(1)[0][0] for key, counter in pair_counts.items()}


def _collect_part_counts(
    loinc_dir: str, active_codes: set[str],
) -> dict[tuple[str, str], Counter[str]]:
    """Return ``{(axis, part_type): {part_name: count}}`` over ACTIVE LOINCs."""
    link_primary = os.path.join(loinc_dir, "AccessoryFiles", "PartFile", "LoincPartLink_Primary.csv")
    link_suppl   = os.path.join(loinc_dir, "AccessoryFiles", "PartFile", "LoincPartLink_Supplementary.csv")

    buckets: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    skipped: Counter[str] = Counter()
    csv.field_size_limit(2**24)
    for link_path in (link_primary, link_suppl):
        with open(link_path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row["LoincNumber"] not in active_codes:
                    continue
                part_type = row["PartTypeName"]
                axis = _AXIS_MAP.get(part_type)
                if axis is None:
                    skipped[part_type] += 1
                    continue
                buckets[(axis, part_type)][row["PartName"]] += 1

    if skipped:
        log.info("Unmapped PartTypes (not written):")
        for pt, n in sorted(skipped.items(), key=lambda kv: -kv[1]):
            log.info("  %-40s %8d links", pt, n)
    return buckets


def _aggregate_axis_counts(
    buckets: dict[tuple[str, str], Counter[str]],
) -> dict[str, Counter[str]]:
    """Collapse per-PartType buckets into one Counter per axis."""
    axis_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for (axis, _part_type), counts in buckets.items():
        for name, n in counts.items():
            axis_counts[axis][name] += n
    return axis_counts


def _write_axis_tsvs(
    out_dir: str,
    axis_counts: dict[str, Counter[str]],
    lang_tags: list[str],
    lookup: dict[tuple[str, str], str],
) -> None:
    """Write one TSV per axis with comma-joined multilingual translations."""
    log.info("Per-axis TSVs (comma-joined multilingual):")
    for axis in sorted(axis_counts):
        path = os.path.join(out_dir, f"{axis}.tsv")
        counts = axis_counts[axis]

        translated = 0
        synthesized = 0
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter="\t", lineterminator="\n")
            w.writerow(["part_name", "count", "translations"])
            for name, n in sorted(counts.items()):
                seen: set[str] = set()
                joined: list[str] = []
                for tag in lang_tags:
                    val = lookup.get((tag, name), "")
                    if not val or val == name or val in seen:
                        continue
                    seen.add(val)
                    joined.append(val)
                # Fill duration-code gaps in TIME_ASPCT (e.g. `100D` /
                # `1.5H post`) — LinguisticVariant translators skip
                # these but they carry real cross-lingual signal.
                if axis == "TIME_ASPCT" and not joined:
                    syn = _synthesize_time_translations(name, lang_tags)
                    if syn:
                        joined = syn
                        synthesized += 1
                if joined:
                    translated += 1
                w.writerow([name, n, ",".join(joined)])

        pct = (100 * translated / len(counts)) if counts else 0
        extra = f" (+{synthesized} synthesized)" if synthesized else ""
        log.info("  %-14s %6d parts · %5d w/ ≥1 translation (%4.1f%%)%s  →  %s",
                 axis, len(counts), translated, pct, extra, path)


def _write_loinc_code_index(
    bundle_path: str,
    active_rows: dict[str, dict[str, str]],
    axis_counts: dict[str, Counter[str]],
) -> None:
    """Refresh the code-index members of the unified axes bundle.

    Powers the resolve-axes phase: ``resolve_axes_many`` computes a
    cosine + top-1 vocab index per axis, then this index lets the
    LOINC-code lookup filter the 97k ACTIVE codes via per-axis vector
    equality (single numpy op per axis) with no Loinc.csv re-parse.

    Load-modify-saves into *bundle_path* so the per-axis embedding +
    name members produced by ``loinc-axis-emb`` survive a vocab rebuild.

    Code-index members:
      codes          (M,) object  — LOINC code strings, sorted
      code_longnames (M,) object  — LongCommonName (may be empty)
      idx_<axis>     (M,) int32   — code's row in ``<axis>.tsv``
                                    (sorted alphabetically), ``-1`` when
                                    the axis value is blank or not in
                                    the vocab
      viable_<axis>  (N_axis,) bool — vocab row appears as a full LOINC
                                      axis value somewhere; COMPONENT
                                      additionally drops ``^``-compounds
    """
    axes_order = _RESOLVE_AXES
    # Same alphabetical sort as ``_write_axis_tsvs``'s ``sorted(counts.items())``
    # so row indices here line up with row indices in ``<AXIS>.tsv``.
    value_to_idx: dict[str, dict[str, int]] = {
        axis: {name: i for i, name in enumerate(sorted(axis_counts.get(axis, {})))}
        for axis in axes_order
    }

    codes = sorted(active_rows.keys())
    m = len(codes)
    code_longnames = np.empty(m, dtype=object)
    common_test_rank = np.zeros(m, dtype=np.int32)
    idx_arrays: dict[str, np.ndarray] = {
        axis: np.full(m, -1, dtype=np.int32) for axis in axes_order
    }
    for i, code in enumerate(codes):
        row = active_rows[code]
        code_longnames[i] = row.get("LONG_COMMON_NAME", "")
        common_test_rank[i] = int(row.get("__rank__") or 0)
        for axis in axes_order:
            v = row.get(axis, "")
            if not v:
                continue
            idx_arrays[axis][i] = value_to_idx[axis].get(v, -1)

    # Per-axis "viable anchor" mask: vocab row is True iff it appears as
    # the full axis value of at least one ACTIVE LOINC code. Catches the
    # atomic-vs-compound mismatch — e.g., ``24H specimen`` is in the
    # COMPONENT vocab (PartLink decomposes ``Creatinine^24H specimen``
    # into atomic ``24H specimen``), but no LOINC code has COMPONENT ==
    # ``"24H specimen"`` literally, so picking it as a COMPONENT anchor
    # always yields zero candidates. The resolver masks these to -inf
    # before argmax so top-1 lands on an anchorable value.
    #
    # Note: ``^``-compounds (``Calcium^4H post X challenge`` etc.) are
    # NOT excluded here — the resolver uses a two-stage cosine where
    # compounds inherit their atomic head's cosine for top-K selection
    # via ``head_idx_COMPONENT`` (see below), so they don't unfairly
    # outscore the atomic analyte but stay reachable for queries where
    # the time / challenge modifier is the intended target (e.g.
    # ``C-肽(半小时)`` → ``C peptide^30M post meal``).
    viable: dict[str, np.ndarray] = {}
    sorted_values_by_axis = {
        axis: sorted(axis_counts.get(axis, {})) for axis in axes_order
    }
    for axis in axes_order:
        sorted_values = sorted_values_by_axis[axis]
        n_vocab = len(sorted_values)
        mask = np.zeros(n_vocab, dtype=bool)
        for i in set(idx_arrays[axis].tolist()) - {-1}:
            if 0 <= i < n_vocab:
                mask[i] = True
        viable[axis] = mask

    # Atomic-head lookup for COMPONENT: for each entry containing ``^``,
    # point to the atomic head (left of ``^``) if it exists in the
    # vocab. Atomic entries (no ``^``) point to themselves. Powers the
    # two-stage cosine: compound's "selection cosine" = its head's full
    # cosine, so compounds aren't penalized when query doesn't match the
    # modifier. Within-top-K is then ranked by each row's own full cosine.
    component_values = sorted_values_by_axis["COMPONENT"]
    component_to_idx = value_to_idx["COMPONENT"]
    head_idx_component = np.arange(len(component_values), dtype=np.int32)
    n_remapped = 0
    for i, name in enumerate(component_values):
        if "^" in name:
            head = name.split("^", 1)[0]
            head_i = component_to_idx.get(head, -1)
            if head_i >= 0:
                head_idx_component[i] = head_i
                n_remapped += 1
            # else: stays at self — atomic head missing from vocab

    # Load-modify-save: preserve any per-axis embedding / names members
    # the embed step has already written; refresh only the code-index
    # portion. Strip stale code-index members first so removed axes /
    # mask changes don't leave orphans.
    bundled: dict = {}
    if os.path.isfile(bundle_path):
        with np.load(bundle_path, allow_pickle=True) as z:
            bundled = {k: z[k] for k in z.files}
        for k in list(bundled):
            if (k == "codes" or k == "code_longnames"
                    or k.startswith("idx_") or k.startswith("viable_")):
                del bundled[k]
    bundled["codes"] = np.asarray(codes, dtype=object)
    bundled["code_longnames"] = code_longnames
    bundled["common_test_rank"] = common_test_rank
    bundled["head_idx_COMPONENT"] = head_idx_component
    for a in axes_order:
        bundled[f"idx_{a}"] = idx_arrays[a]
        bundled[f"viable_{a}"] = viable[a]

    np.savez(bundle_path, **bundled)
    coverage = {a: int((idx_arrays[a] >= 0).sum()) for a in axes_order}
    viable_counts = {
        a: f"{int(viable[a].sum())}/{viable[a].size}" for a in axes_order
    }
    n_ranked = int((common_test_rank > 0).sum())
    log.info(
        "code index refreshed in %s: %d LOINC codes (%d w/ COMMON_TEST_RANK); "
        "axis coverage: %s; viable vocab: %s; COMPONENT ^-compounds remapped "
        "to atomic head: %d",
        bundle_path, m, n_ranked, coverage, viable_counts, n_remapped,
    )


def _load_partlink_by_part(
    loinc_dir: str, active_codes: set[str],
) -> dict[tuple[str, str], set[str]]:
    """Return ``{(part_type, part_name): {LOINC, …}}`` over ACTIVE rows."""
    link_primary = os.path.join(loinc_dir, "AccessoryFiles", "PartFile", "LoincPartLink_Primary.csv")
    link_suppl   = os.path.join(loinc_dir, "AccessoryFiles", "PartFile", "LoincPartLink_Supplementary.csv")
    out: dict[tuple[str, str], set[str]] = defaultdict(set)
    csv.field_size_limit(2**24)
    for link_path in (link_primary, link_suppl):
        with open(link_path, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row["LoincNumber"] in active_codes:
                    out[(row["PartTypeName"], row["PartName"])].add(row["LoincNumber"])
    return out


def _build_scoped_translations(
    part_type: str,
    parts_by_part: dict[tuple[str, str], set[str]],
    active_rows: dict[str, dict[str, str]],
    by_lang: dict[str, dict[str, dict[str, str]]],
) -> dict[str, dict[str, str]]:
    """Per ``part_name`` translations for a single controlled-vocab PartType.

    Mines translations only from LOINC codes that actually contain that
    (PartType, PartName) pair (per PartLink), so cross-PartType string
    collisions like ``IA`` = Immunoassay (METHOD) vs Intra-arterial
    (Rad.Pharm.Route) can't pollute the result.
    """
    axis = _AXIS_MAP[part_type]
    result: dict[str, dict[str, str]] = {}
    for (pt, pn), loincs in parts_by_part.items():
        if pt != part_type:
            continue
        pair_counts: dict[str, Counter[str]] = defaultdict(Counter)
        for loinc in loincs:
            en_val = (active_rows.get(loinc) or {}).get(axis, "")
            if not en_val:
                continue
            en_segs = _SEP_RE.split(en_val)
            positions = [
                i for i in range(0, len(en_segs), 2)
                if en_segs[i].strip() == pn
            ]
            if not positions and en_val.strip() == pn:
                positions = [0]
            if not positions:
                continue
            for lang_tag, lang_data in by_lang.items():
                fo_val = lang_data.get(loinc, {}).get(axis, "")
                if not fo_val:
                    continue
                fo_segs = _SEP_RE.split(fo_val)
                if len(fo_segs) != len(en_segs):
                    if en_val.strip() == pn and fo_val.strip():
                        pair_counts[lang_tag][fo_val.strip()] += 1
                    continue
                if any(en_segs[i] != fo_segs[i] for i in range(1, len(en_segs), 2)):
                    continue
                for pos in positions:
                    fo_seg = fo_segs[pos].strip()
                    if fo_seg:
                        pair_counts[lang_tag][fo_seg] += 1
        translations = {
            tag: counter.most_common(1)[0][0]
            for tag, counter in pair_counts.items()
            if counter
        }
        result[pn] = translations
    return result


def _write_python_lookups(
    parts_by_part: dict[tuple[str, str], set[str]],
    active_rows: dict[str, dict[str, str]],
    by_lang: dict[str, dict[str, dict[str, str]]],
) -> None:
    """Emit Python dict literals for the controlled-vocab PartTypes.

    Each entry is ``part_name -> {lang_tag: foreign_value}`` with empty
    translations dropped (callers use ``.get(lang)`` for soft fallback).
    Translation extraction is PartType-scoped to avoid cross-PartType
    string collisions.
    """
    log.info("Python lookup tables → %s", _LOOKUPS_PY)
    os.makedirs(os.path.dirname(_LOOKUPS_PY), exist_ok=True)
    with open(_LOOKUPS_PY, "w", encoding="utf-8", newline="\n") as f:
        f.write(
            '"""Multilingual lookup tables for LOINC controlled-vocab Part types.\n\n'
            'AUTO-GENERATED by ``python scripts/vocabulary_build.py loinc-axis-vocab``\n'
            'from LOINC 2.82 + 22 LinguisticVariant locales. DO NOT EDIT — re-run\n'
            'the command instead.\n\n'
            'Each table maps a LOINC Part name (which IS the English value) to a\n'
            '``{lang_tag: foreign_value}`` dict. Locales with no translation are\n'
            'omitted; use ``.get(lang)`` for soft fallback.\n'
            '"""\n'
            'from __future__ import annotations\n\n'
        )
        for var_name, part_type in _LOOKUP_PART_TYPES:
            translations = _build_scoped_translations(part_type, parts_by_part, active_rows, by_lang)
            translated = sum(1 for v in translations.values() if v)
            f.write(f"# {part_type} — {len(translations)} entries\n")
            f.write(f"{var_name}: dict[str, dict[str, str]] = {{\n")
            for name in sorted(translations):
                inner = json.dumps(translations[name], ensure_ascii=False, sort_keys=True)
                f.write(f"    {json.dumps(name, ensure_ascii=False)}: {inner},\n")
            f.write("}\n\n")
            log.info("  %-26s %4d entries · %4d w/ translation",
                     var_name, len(translations), translated)


def cmd_loinc_axis_vocab(args: Namespace) -> None:
    """Build per-axis TSVs + ``loinc_lookups.py`` from LOINC source.

    Pure local computation against LOINC accessory CSVs — no DB or
    network. Synchronous; the central dispatcher in ``main.py`` calls
    it without an ``asyncio.run`` wrapper.
    """
    loinc_dir = args.loinc_dir
    if not loinc_dir or not os.path.isdir(loinc_dir):
        raise FileNotFoundError(
            f"LOINC release dir not found at {loinc_dir!r}; "
            "pass --loinc-dir or set MIROBODY_REF_DIR."
        )

    res_dir = args.res_dir or RES_DIR
    out_dir = os.path.join(res_dir, "loinc_axes")
    os.makedirs(out_dir, exist_ok=True)

    loinc_csv = os.path.join(loinc_dir, "LoincTable", "Loinc.csv")
    variants_dir = os.path.join(loinc_dir, "AccessoryFiles", "LinguisticVariants")
    if not os.path.isfile(loinc_csv):
        raise FileNotFoundError(loinc_csv)

    active_rows = _load_active_rows(loinc_csv)
    log.info("ACTIVE LOINC codes: %s", f"{len(active_rows):,}")

    log.info("Loading LinguisticVariants:")
    variants = _discover_variants(variants_dir)
    by_lang: dict[str, dict[str, dict[str, str]]] = {}
    for tag, path in variants:
        by_lang[tag] = _load_variant(path)
        log.info("  %-6s %6d rows  ← %s", tag, len(by_lang[tag]), os.path.basename(path))
    lang_tags = [tag for tag, _ in variants]

    log.info("Building EN→lang segment lookup")
    lookup = _build_translation_lookup(active_rows, by_lang)
    log.info("  %s (lang, en_segment) pairs", f"{len(lookup):,}")

    buckets = _collect_part_counts(loinc_dir, set(active_rows))
    axis_counts = _aggregate_axis_counts(buckets)
    _write_axis_tsvs(out_dir, axis_counts, lang_tags, lookup)
    bundle_path = os.path.join(res_dir, _BUNDLE_BASENAME)
    _write_loinc_code_index(bundle_path, active_rows, axis_counts)

    parts_by_part = _load_partlink_by_part(loinc_dir, set(active_rows))
    _write_python_lookups(parts_by_part, active_rows, by_lang)

    # Sweep prior layouts: the abandoned ``multilingual/`` folder, the
    # per-PartType ``<AXIS>/`` subdirs from the old per-locale-column
    # format (replaced by flat ``<AXIS>.tsv``), and the standalone
    # ``code_index.npz`` from before the bundle merge (2026-05-21).
    legacy_dirs = {"multilingual"} | set(_AXIS_MAP.values())
    for entry in os.listdir(out_dir):
        sub = os.path.join(out_dir, entry)
        if os.path.isdir(sub) and entry in legacy_dirs:
            shutil.rmtree(sub)
    legacy_code_index = os.path.join(out_dir, "code_index.npz")
    if os.path.isfile(legacy_code_index):
        os.remove(legacy_code_index)
        log.info("removed legacy %s (merged into %s)",
                 legacy_code_index, bundle_path)
    legacy_emb_bundle = os.path.join(res_dir, "loinc_axes_embeddings.npz")
    if os.path.isfile(legacy_emb_bundle):
        os.remove(legacy_emb_bundle)
        log.info("removed legacy %s (renamed to %s)",
                 legacy_emb_bundle, bundle_path)


# ─── Phase 2: Embed (npy per axis) ───────────────────────────────────

# Caller-side chunk for progress visibility — text_embedding itself
# already fans out concurrently per provider batch limit (Gemini = 100
# per call), so this only affects when we log progress, not throughput.
_PROGRESS_BATCH = 1024


def _build_input(part_name: str, translations: str) -> str:
    if translations:
        return f"{part_name},{translations}"
    return part_name


def _load_axis_inputs(tsv_path: str) -> list[str]:
    with open(tsv_path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader, None)
        if not header:
            return []
        try:
            i_pn = header.index("part_name")
            i_tx = header.index("translations")
        except ValueError as e:
            raise ValueError(f"{tsv_path}: missing column ({e})") from e
        return [
            _build_input(row[i_pn], row[i_tx] if i_tx < len(row) else "")
            for row in reader
            if row
        ]


async def _embed_axis(
    texts: list[str], provider: str | None,
) -> np.ndarray:
    """Embed *texts* and return an L2-normalized fp32 ``(N, 1024)`` array."""
    from mirobody.utils.embedding import text_embedding

    n = len(texts)
    out = np.zeros((n, EMBEDDING_DIM), dtype=np.float32)
    for start in range(0, n, _PROGRESS_BATCH):
        chunk = texts[start:start + _PROGRESS_BATCH]
        vecs = await text_embedding(chunk, provider=provider, cache=True)
        for i, v in enumerate(vecs):
            if v is None:
                raise RuntimeError(
                    f"text_embedding returned None for row {start + i} "
                    f"(text={chunk[i]!r})"
                )
            out[start + i] = v
        done = start + len(chunk)
        log.info("  embedded %s / %s rows (%.1f%%)",
                 f"{done:,}", f"{n:,}", 100 * done / n)

    norms = np.linalg.norm(out, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    out /= norms
    return out


# ─── Phase 3: Resolve query → per-axis top-1 + LOINC code lookup ─────

# Cache per axes_dir so re-entrant batch calls don't re-read the 7 npy +
# TSV pairs. mmap-backed; process-lifetime singleton is fine.
_AXIS_VOCAB_CACHE: dict[str, dict[str, tuple[np.ndarray, list[str], np.ndarray]]] = {}
_CODE_INDEX_CACHE: dict[str, dict | None] = {}
_COMPONENT_RATIO_MASK_CACHE: dict[int, np.ndarray] = {}
_COMPONENT_NONLAB_MASK_CACHE: dict[int, np.ndarray] = {}


# Query carries an explicit "I want a ratio/index" signal. Mirrors the
# adapter-side ``_QUERY_WANTS_RATIO_RE`` in :mod:`..adapter` so the
# axes path applies a consistent rule. Used inversely here: when the
# query does NOT want a ratio, ratio-shaped COMPONENT vocab entries
# (``Calcium/Creatinine``, ``Fractional excretion of calcium``, ...)
# are masked out before argmax so the atomic analyte (``Calcium``) can
# win.
_QUERY_WANTS_RATIO_RE = re.compile(
    r"\bratio\b|\bindex\b"
    r"|比值|比率|指数|占比"
    r"|(?<![谢速频心吸])率",
    re.IGNORECASE,
)
_EXCLUDE_RATIO_METADATA_RE = re.compile(
    r"序号（指标）|Number\(index\)",
    re.IGNORECASE,
)

# COMPONENT part_name shapes that encode a ratio / fraction / clearance /
# index — broader than the legacy ``_RATIO_CODE_NAME_RE`` (which matches
# whole LongCommonNames) since COMPONENT atoms include semantic-ratio
# phrases like ``Fractional excretion of calcium`` and
# ``Calcium renal clearance`` that don't surface ``Ratio`` / ``/`` markers.
_COMPONENT_RATIO_RE = re.compile(
    r"\bratio\b"
    r"|\bindex\b"
    r"|(?<!\[)\b[A-Za-z][\w.]{1,30}/[A-Za-z][\w.]{1,30}\b"
    r"|\b(?:mass|number|pure number) fraction\b"
    r"|\bfractional excretion\b"
    r"|\b(?:renal|creatinine) clearance\b",
    re.IGNORECASE,
)

# COMPONENT part_name shapes that are NOT lab measurements:
# - ``X intake`` / ``X intake panel`` — dietary self-report instruments
#   (147 vocab rows: ``Vitamin B7 intake``, ``Calorie intake``, ...).
# - ``X goal`` — clinician-set treatment targets, not measurements
#   (29 vocab rows: ``Calcidiol+ercalcidiol goal``, ``LDL goal``, ...).
# Without this guard ``维生素B7`` anchors COMPONENT on ``Vitamin B7 intake``
# (only 81017-6 / 81018-4 in candidate pool — Biotin 34398-8 unreachable).
# Mirrors the runtime-only ``_HAND_DEMOTE_NAME_PATTERNS`` over LongCommonName
# in :mod:`..common`, applied here at the COMPONENT-anchor stage where it
# can actually prevent the wrong family lookup. Licensed by query tokens
# (``摄入``, ``目标``, ``intake``, ``dietary``, ``goal``, ``target``, ...).
_COMPONENT_NONLAB_RE = re.compile(
    r"\bintake\b|\bgoal$",
    re.IGNORECASE,
)
_QUERY_WANTS_NONLAB_RE = re.compile(
    r"\bintake\b|\bdietary\b|\bdiet\b|\bFFQ\b"
    r"|\bgoal\b|\btarget\b"
    r"|摄入|膳食|饮食|目标"
    r"|攝入|飲食"
    r"|摂取|食事|目標"
    r"|섭취|식이|목표"
    r"|ingesta|dieta|objetivo"
    r"|Aufnahme|Ernährung|Ziel"
    r"|apport|alimentaire|régime|objectif"
    r"|потребление|питание|цель",
    re.IGNORECASE,
)


# Recursive "peel" patterns that strip leading time / specimen / dose /
# time-of-day tokens from a Chinese medical query. Each successful
# strip produces a shorter span; ``_generate_axis_spans`` emits the
# full query plus each strip, and ``resolve_axes_many`` max-pools per-
# vocab cosines across spans. Lets the atomic analyte (``钙``) win on
# COMPONENT axis when the full query (``24小时尿钙``) gets dragged toward
# semantic compounds like ``Calcium/Creatinine`` by specimen + time
# tokens it carries.
_QUERY_PEEL_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Numeric time prefix: "24小时", "1.5H", "30 min", "10分钟".
    # ``\b`` doesn't fire between two Chinese characters (Python ``re``
    # treats CJK as ``\w``), so explicit lookahead ``(?![A-Za-z0-9])``
    # is used to require the next char to not extend the unit token —
    # which allows the next char to be Chinese, punctuation, or EOS.
    re.compile(
        r"^\d+(?:\.\d+)?\s*"
        r"(?:小时|分钟|秒钟|天|月|周|年|min|H|Hr|hour|hours|Mo|D|W|Y|sec|s)"
        r"(?![A-Za-z0-9])",
        re.IGNORECASE,
    ),
    # Chinese specimen prefix. ``尿`` is included (the residue is
    # typically a specific analyte: ``尿钙`` → ``钙`` = atomic Calcium,
    # ``尿蛋白`` → ``蛋白``). ``血`` is intentionally excluded — the
    # ``血X`` compound is often the canonical analyte name in Chinese
    # medical usage (``血糖`` = blood glucose, not "blood" + "sugar"),
    # and peeling leaves a too-generic residue (``糖`` brings ``Sugar``
    # 0.78 over ``Glucose`` 0.75). Multi-char variants (``血清`` /
    # ``血浆`` / ``血液``) are explicit body-fluid names and safe to
    # peel.
    re.compile(
        r"^(?:尿液|尿|血清|血浆|血液|唾液|脑脊液|淋巴液|腹水|胸水|"
        r"分泌物|乳汁|精液|羊水|胃液|关节液|胆汁|汗液|泪液)"
    ),
    # Numeric dose prefix: "75 g", "100mg", "5 mL"
    re.compile(
        r"^\d+(?:\.\d+)?\s*(?:g|mg|ug|µg|kg|mL|L)(?![A-Za-z0-9])",
        re.IGNORECASE,
    ),
    # Time-of-day Chinese: 空腹 / 餐前 / 餐后 / 晨起 / 睡前 / 夜间
    re.compile(r"^(?:空腹|餐前|餐后|晨起|睡前|夜间|晨间|凌晨)"),
)


# Hierarchy / field separators in benchmark-style indicator queries
# (``特殊实验室检查,B族维生素·叶酸``). The trailing segment after each
# separator is the most-specific child — pull it out as a span so the
# atomic analyte (``叶酸`` → Folate) can compete on COMPONENT axis even
# when the leading category prefix drags the full-query embedding away.
_QUERY_SPLIT_RE = re.compile(r"[,，|·・]")


# Time-phrase normalization. LOINC encodes time modifiers in COMPONENT
# (``^30M post dose glucose``, ``^1.5H post meal``) and in TIME_ASPCT
# (``30M``, ``1.5H``). The embedder maps ``30分钟 → 30M`` cleanly but
# stumbles on ``半小时``/``0.5小时``/``两小时`` (Chinese fraction or
# colloquial number) and ``0.5H``/``0.25H`` (English fractional hour
# that LOINC normalizes to minutes). We emit a normalized span
# alongside the original so the LOINC token shape always has a chance
# to align via embedding overlap. Normalization rules:
#   * ``X小时``/``XH``/``X hour(s)`` with X<1 → ``{X*60}M`` (LOINC uses
#     ``^30M`` not ``^0.5H``)
#   * ``X小时``/``XH``/... with X≥1 → ``{X}H`` (preserve fractional like 1.5H)
#   * ``X分钟``/``XM``/``X min(utes)`` → ``{X}M`` (no further conversion;
#     LOINC's ^15M / ^45M etc. directly)
#   * ``X秒钟``/``XS``/``X sec(onds)`` → ``{X}S``
# Spelled-out CJK numerals as time-modifier prefixes. Common clinical
# integers (1–24, plus 36/48/72 for multi-day specimen collections)
# plus the half/colloquial markers ``半``/``两``/``반``. Japanese reuses
# the Han characters (``一時間``/``二十四時間``) so the same Chinese
# numerals carry. Korean native counters (``한``/``두``/...) cover the
# ``한 시간`` style. Traditional/financial Chinese numerals
# (``壹貳叁``) are intentionally omitted — they're bank-check formal,
# never seen in clinical queries.
_CN_NUM_PREFIX: dict[str, float] = {
    # Half / colloquial
    "半": 0.5, "两": 2.0, "반": 0.5,
    # Simplified Chinese 1–10 (Japanese kanji shares these characters)
    "一": 1.0, "二": 2.0, "三": 3.0, "四": 4.0, "五": 5.0,
    "六": 6.0, "七": 7.0, "八": 8.0, "九": 9.0, "十": 10.0,
    # 11–19
    "十一": 11.0, "十二": 12.0, "十三": 13.0, "十四": 14.0, "十五": 15.0,
    "十六": 16.0, "十七": 17.0, "十八": 18.0, "十九": 19.0,
    # 20–29
    "二十": 20.0, "二十一": 21.0, "二十二": 22.0, "二十三": 23.0, "二十四": 24.0,
    "二十五": 25.0, "二十六": 26.0, "二十七": 27.0, "二十八": 28.0, "二十九": 29.0,
    # Common multi-day / multi-hour clinical values
    "三十": 30.0, "三十六": 36.0, "四十": 40.0, "四十五": 45.0, "四十八": 48.0,
    "五十": 50.0, "六十": 60.0, "七十": 70.0, "七十二": 72.0, "九十": 90.0,
    # Korean native counters (1–10, with ``한 시간`` style usage)
    "한": 1.0, "두": 2.0, "세": 3.0, "네": 4.0, "다섯": 5.0,
    "여섯": 6.0, "일곱": 7.0, "여덟": 8.0, "아홉": 9.0, "열": 10.0,
}
# Unit-word → canonical {H, M, S}. Keys are normalized lowercase.
# - Chinese: 小时 / 时 / 分钟 / 秒钟 / 秒 (分 same char as Japanese)
# - Japanese: 時間 / 時 (時間 is the full word, 時 alone fires only with
#   a numeric prefix so ``2時間`` and ``2時`` both match; ``午後3時``
#   without leading digit doesn't trigger). 分 / 秒 reuse the Chinese
#   character entries.
# - Korean: 시간 / 시 / 분 / 초 (all Hangul, no overlap with CJK).
# - Latin: hour(s) / hr(s) / h, minute(s) / min(s) / m, second(s) /
#   sec(s) / s. Case-insensitive via the regex flag.
_TIME_UNIT_CANON: dict[str, str] = {
    # zh
    "小时": "H", "时": "H", "分钟": "M", "秒钟": "S", "秒": "S", "分": "M",
    # ja additions (分 / 秒 covered by zh entries above; same chars)
    "時間": "H", "時": "H",
    # ko
    "시간": "H", "시": "H", "분": "M", "초": "S",
    # en
    "h": "H", "hr": "H", "hrs": "H", "hour": "H", "hours": "H",
    "m": "M", "min": "M", "mins": "M", "minute": "M", "minutes": "M",
    "s": "S", "sec": "S", "secs": "S", "second": "S", "seconds": "S",
}
# Number-prefix alternation: dict keys (multi-char CJK numerals) sorted
# longest-first so ``二十四小时`` matches ``二十四`` not ``二十``; ``\d+``
# tail handles Latin digits. Built at module-import time from
# ``_CN_NUM_PREFIX``.
_NUM_PAT = (
    "|".join(re.escape(k) for k in sorted(_CN_NUM_PREFIX, key=len, reverse=True))
    + r"|\d+(?:\.\d+)?"
)
# Lookahead/lookbehind keep us from matching mid-word — ``mmol``,
# ``Hb1Ac``, ``Mo`` (month) etc. ``(?![A-Za-z])`` only excludes Latin
# letters following the unit; CJK / Hangul are OK so ``30分钟尿`` still
# matches ``30分`` (Chinese/Japanese minutes — though 分钟 covers it
# first via the longer-alternation order below).
_TIME_NORM_RE = re.compile(
    r"(?<![A-Za-z0-9])"
    r"(" + _NUM_PAT + r")"
    r"\s*"
    # Longest unit forms first so the alternation grabs ``小时`` before
    # ``时``, ``時間`` before ``時``, ``시간`` before ``시``, ``hours``
    # before ``h``.
    r"(小时|分钟|秒钟|秒|时|分"
    r"|時間|時"
    r"|시간|시|분|초"
    r"|hours?|hrs?|h"
    r"|minutes?|mins?|m"
    r"|seconds?|secs?|s)"
    r"(?![A-Za-z])",
    re.IGNORECASE,
)


def _normalize_time_tokens(query: str) -> str:
    """Replace ``半小时`` / ``0.5小时`` / ``0.5H`` / ``X分钟`` etc. with
    LOINC-format tokens (``30M`` / ``30M`` / ``30M`` / ``XM``) so the
    query embedding aligns with the LOINC vocab's ``^X{TIME}`` modifier
    shapes. Operates token-wise; the original query stays intact and is
    also embedded — the normalized form is an ADDITIONAL span, not a
    replacement.
    """
    def _sub(m: re.Match[str]) -> str:
        num_str, unit_raw = m.group(1), m.group(2)
        if num_str in _CN_NUM_PREFIX:
            num = _CN_NUM_PREFIX[num_str]
        else:
            try:
                num = float(num_str)
            except ValueError:
                return m.group(0)
        canon = _TIME_UNIT_CANON.get(unit_raw.lower())
        if canon is None:
            return m.group(0)
        if canon == "H":
            if num < 1:
                minutes = int(round(num * 60))
                return f"{minutes}M"
            int_n = int(num)
            return f"{int_n}H" if num == int_n else f"{num}H"
        if canon == "M":
            # LOINC minute corpus has clean ``^XM`` for X ∈ {1, 2, 3,
            # ... 5, 10, 15, 20, 30, 40, 45, 50, 70, 75} — i.e. arbitrary
            # M values up to ~75 are legitimate. But ``^60M`` / ``^90M``
            # / ``^120M`` (clean half-hour-multiples ≥ 60) don't exist;
            # LOINC normalizes those to ``^1H`` / ``^1.5H`` / ``^2H``.
            # Convert only X minutes that are multiples of 30 AND ≥ 60.
            if num >= 60 and num % 30 == 0:
                hours = num / 60
                int_h = int(hours)
                return f"{int_h}H" if hours == int_h else f"{hours:g}H"
            int_n = int(num)
            return f"{int_n}M" if num == int_n else f"{num}M"
        # S: just normalize numeric formatting.
        int_n = int(num)
        if num == int_n:
            return f"{int_n}{canon}"
        return f"{num}{canon}"
    return _TIME_NORM_RE.sub(_sub, query)


def _generate_axis_spans(query: str) -> list[str]:
    """Produce progressively-stripped spans of *query* for max-pool embedding.

    Two strip layers compose:

      1. **Punctuation split** — iteratively peel the last segment after
         field (``,`` / ``，`` / ``|``) or hierarchy (``·`` / ``・``)
         separators. Each strip yields a shorter trailing chunk, e.g.
         ``特殊实验室检查,B族维生素·叶酸`` → ``[full, "B族维生素·叶酸", "叶酸"]``.

      2. **Token peel** — for each base span from layer 1, recursively
         strip leading time / specimen / dose / time-of-day tokens
         (e.g. ``24小时尿钙`` → ``尿钙`` → ``钙``).

    The original query is always included. Result is deduplicated; for
    non-peelable atomic queries (``葡萄糖``) the result is just ``[query]``.
    """
    query = query.strip()
    if not query:
        return []
    base_spans: list[str] = [query]
    seen = {query}

    # Layer 0: Chinese time normalization — emit a sibling span with
    # ``半小时`` / ``0.5小时`` / ``X分钟`` rewritten to LOINC-format
    # tokens (``30M`` / ``30M`` / ``XM``). The embedder maps these
    # directly to the ``^X{TIME}`` COMPONENT modifiers and to the
    # TIME_ASPCT vocab. Original query stays in the pool unchanged.
    normalized = _normalize_time_tokens(query)
    if normalized != query and normalized not in seen:
        base_spans.append(normalized)
        seen.add(normalized)

    # Layer 1: punctuation split — recurse on the trailing segment.
    s = query
    for _ in range(6):
        matches = list(_QUERY_SPLIT_RE.finditer(s))
        if not matches:
            break
        tail = s[matches[-1].end():].strip()
        if not tail or tail == s or tail in seen:
            break
        base_spans.append(tail)
        seen.add(tail)
        s = tail

    # Layer 2: token peel — apply to each base span independently.
    peeled: list[str] = []
    for base in base_spans:
        cur = base
        for _ in range(6):
            new_s = None
            for pat in _QUERY_PEEL_PATTERNS:
                m = pat.match(cur)
                if m and m.end() > 0:
                    new_s = cur[m.end():].lstrip()
                    break
            if not new_s or new_s == cur or new_s in seen:
                break
            peeled.append(new_s)
            seen.add(new_s)
            cur = new_s
    return base_spans + peeled


def _query_wants_ratio(q: str) -> bool:
    if not q:
        return False
    if _EXCLUDE_RATIO_METADATA_RE.search(q):
        return False
    return bool(_QUERY_WANTS_RATIO_RE.search(q))


def _component_ratio_mask(names: list[str]) -> np.ndarray:
    """Bool mask over COMPONENT vocab; True iff part_name is a ratio /
    fraction / clearance / index phrase. Cached on the names list's id
    (stable across batch calls since ``_AXIS_VOCAB_CACHE`` reuses it).
    """
    key = id(names)
    cached = _COMPONENT_RATIO_MASK_CACHE.get(key)
    if cached is not None:
        return cached
    mask = np.array(
        [bool(_COMPONENT_RATIO_RE.search(n)) for n in names], dtype=bool,
    )
    _COMPONENT_RATIO_MASK_CACHE[key] = mask
    log.info("COMPONENT ratio mask: %d / %d vocab entries flagged",
             int(mask.sum()), mask.size)
    return mask


def _component_nonlab_mask(names: list[str]) -> np.ndarray:
    """Bool mask over COMPONENT vocab; True iff part_name is a non-lab
    instrument (``X intake`` / ``X goal``). Cached on the names list's id.
    """
    key = id(names)
    cached = _COMPONENT_NONLAB_MASK_CACHE.get(key)
    if cached is not None:
        return cached
    mask = np.array(
        [bool(_COMPONENT_NONLAB_RE.search(n)) for n in names], dtype=bool,
    )
    _COMPONENT_NONLAB_MASK_CACHE[key] = mask
    log.info("COMPONENT nonlab mask: %d / %d vocab entries flagged",
             int(mask.sum()), mask.size)
    return mask


def _query_wants_nonlab(q: str) -> bool:
    if not q:
        return False
    return bool(_QUERY_WANTS_NONLAB_RE.search(q))


# Per-CLASS COMPONENT-vocab masks: COMPONENT vocab row is True iff at
# least one active LOINC code with the gated CLASS uses that COMPONENT.
# Built lazily on first ALLERGY / MICRO-gated query, cached on
# ``code_index`` so subsequent queries skip the loop. Skips when the
# legacy axis bundle (``fhir_loinc_axes_embeddings.npz``) is unavailable
# — the gate then silently no-ops, falling back to plain cosine.
def _build_class_component_mask(
    code_index: dict,
    rescore_cache: dict | None,
    class_name: str,
    n_component_vocab: int,
) -> np.ndarray | None:
    """Return bool[N_component_vocab] flagging COMPONENT vocab rows
    that any active LOINC code in ``CLASS == class_name`` uses.

    Returns ``None`` when:
      * the legacy axis bundle / ``rescore_cache`` is unavailable, OR
      * the requested ``class_name`` isn't in the LOINC CLASS vocab,
      * no LOINC code in that CLASS uses any vocab COMPONENT (mask all
        False — same effect as ``None``: caller treats as "no gate").

    Cached on the mutable ``code_index`` dict under
    ``_class_component_mask_<NAME>`` so subsequent ALLERGY-gated queries
    pay zero cost.
    """
    cache_key = f"_class_component_mask_{class_name}"
    if cache_key in code_index:
        return code_index[cache_key]
    code_index[cache_key] = None
    if rescore_cache is None:
        return None
    try:
        from ..resolve.axis import load_axis_centroids
        from ..common import code_to_fhir_id
    except Exception:
        return None
    axis_data = load_axis_centroids(rescore_cache)
    if axis_data is None:
        return None
    class_values = axis_data.get("values", {}).get("CLASS")
    class_ridx = axis_data.get("row_value_idx", {}).get("CLASS")
    row_by_id = rescore_cache.get("row_by_id")
    if class_values is None or class_ridx is None or row_by_id is None:
        return None
    try:
        target_idx = class_values.index(class_name)
    except ValueError:
        return None
    idx_comp = code_index.get("idx_COMPONENT")
    codes_arr = code_index.get("codes")
    if idx_comp is None or codes_arr is None:
        return None
    mask = np.zeros(n_component_vocab, dtype=bool)
    for i, code in enumerate(codes_arr):
        try:
            canonical = code_to_fhir_id("LOINC", str(code))
        except Exception:
            continue
        row = row_by_id.get(canonical)
        if row is None:
            continue
        if int(class_ridx[row]) != target_idx:
            continue
        ci = int(idx_comp[i])
        if 0 <= ci < n_component_vocab:
            mask[ci] = True
    if not mask.any():
        return None
    code_index[cache_key] = mask
    log.info(
        "COMPONENT class mask (%s): %d / %d vocab entries flagged",
        class_name, int(mask.sum()), mask.size,
    )
    return mask


def _read_axis_part_names(tsv_path: str) -> list[str]:
    names: list[str] = []
    with open(tsv_path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader, None)  # header
        for row in reader:
            if row:
                names.append(row[0])
    return names


def _read_axis_part_counts(tsv_path: str) -> list[int]:
    counts: list[int] = []
    with open(tsv_path, encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader, None)  # header
        for row in reader:
            if row:
                try:
                    counts.append(int(row[1]))
                except (IndexError, ValueError):
                    counts.append(0)
    return counts


def _get_axis_vocab(res_dir: str) -> dict[str, tuple[np.ndarray, list[str], np.ndarray]]:
    """Load per-axis ``(embs fp32, part_names, counts)`` from the bundled npz.

    *res_dir* is the parent of ``loinc_axes/`` (i.e. ``mirobody/res``).
    Each axis contributes three row-aligned npz members: ``<AXIS>`` for
    the fp16 ``(N_axis, 1024)`` embedding matrix, ``<AXIS>_names`` for
    the part-name strings, and ``<AXIS>_counts`` for how many ACTIVE
    LOINC codes use that part — a frequency prior used by the count-
    rerank tie-breaker (see :func:`resolve_axes_many`). TSVs are no
    longer touched at runtime. Cached per *res_dir*.
    """
    cached = _AXIS_VOCAB_CACHE.get(res_dir)
    if cached is not None:
        return cached

    bundle_path = os.path.join(res_dir, _BUNDLE_BASENAME)
    out: dict[str, tuple[np.ndarray, list[str], np.ndarray]] = {}
    if not os.path.isfile(bundle_path):
        log.warning("axis bundle missing at %s — run `loinc-axis-vocab` "
                    "+ `loinc-axis-emb`", bundle_path)
        _AXIS_VOCAB_CACHE[res_dir] = out
        return out

    with np.load(bundle_path, allow_pickle=True) as z:
        members = set(z.files)
        for axis in _RESOLVE_AXES:
            if axis not in members:
                log.warning("axis %s: embeddings not in %s", axis, bundle_path)
                continue
            names_key = f"{axis}_names"
            if names_key not in members:
                log.warning(
                    "axis %s: %s not in bundle — re-run `loinc-axis-emb` to "
                    "migrate from the pre-2026-05-21 layout", axis, names_key,
                )
                continue
            embs = z[axis].astype(np.float32, copy=False)
            names = [str(s) for s in z[names_key]]
            counts_key = f"{axis}_counts"
            counts = (
                z[counts_key].astype(np.int32, copy=False)
                if counts_key in members
                else np.zeros(len(names), dtype=np.int32)
            )
            if embs.shape[0] != len(names):
                log.error("axis %s: embeddings rows=%d, names=%d — skipping",
                          axis, embs.shape[0], len(names))
                continue
            if counts.shape[0] != len(names):
                log.warning("axis %s: counts rows=%d != names=%d — zeroing",
                            axis, counts.shape[0], len(names))
                counts = np.zeros(len(names), dtype=np.int32)
            out[axis] = (embs, names, counts)
    _AXIS_VOCAB_CACHE[res_dir] = out
    return out


def _load_code_index(res_dir: str) -> dict | None:
    """Lazy-load the LOINC code-index members of the unified bundle.

    Returns a dict with keys ``codes`` / ``code_longnames`` /
    ``idx_<axis>`` / ``viable_<axis>`` for each axis in
    :data:`_RESOLVE_AXES`. ``None`` when the bundle is missing or
    carries no ``codes`` member (e.g. you ran ``loinc-axis-emb``
    against a deployment without ever running ``loinc-axis-vocab``) —
    the LOINC-code lookup phase is silently skipped in that case
    (caller still gets the per-axis picks).
    """
    if res_dir in _CODE_INDEX_CACHE:
        return _CODE_INDEX_CACHE[res_dir]
    path = os.path.join(res_dir, _BUNDLE_BASENAME)
    if not os.path.isfile(path):
        log.warning("axis bundle missing at %s — LOINC lookup skipped", path)
        _CODE_INDEX_CACHE[res_dir] = None
        return None
    with np.load(path, allow_pickle=True) as z:
        if "codes" not in z.files:
            log.warning("axis bundle %s has no `codes` member — LOINC lookup "
                        "skipped (run `loinc-axis-vocab` to populate it)", path)
            _CODE_INDEX_CACHE[res_dir] = None
            return None
        out: dict = {
            "codes": z["codes"],
            "code_longnames": z["code_longnames"] if "code_longnames" in z.files
                              else np.array(["" for _ in z["codes"]], dtype=object),
            "common_test_rank": (
                z["common_test_rank"] if "common_test_rank" in z.files
                else np.zeros(len(z["codes"]), dtype=np.int32)
            ),
        }
        if "head_idx_COMPONENT" in z.files:
            out["head_idx_COMPONENT"] = z["head_idx_COMPONENT"]
        for axis in _RESOLVE_AXES:
            for prefix in ("idx_", "viable_"):
                key = f"{prefix}{axis}"
                if key in z.files:
                    out[key] = z[key]
    _CODE_INDEX_CACHE[res_dir] = out
    log.info("loaded code index from %s: %d LOINC codes",
             path, len(out["codes"]))
    return out


def _lookup_loinc_codes(
    top_idx: dict[str, int],
    scores: dict[str, float],
    code_index: dict,
    top_idx_alt: dict[str, int] | None = None,
    *,
    rescore_Q: np.ndarray | None = None,
    rescore_cache: dict | None = None,
) -> list[tuple[str, str, float]]:
    """COMPONENT-anchored, progressive-filter LOINC lookup with
    per-axis count-rerank fallback.

    When ``rescore_Q`` (the per-query embedding matrix, ``(S_q, D)``
    L2-normalized) and ``rescore_cache`` (the full FHIR bundle from
    :func:`.local.load`) are both supplied, each survivor gets a third
    element: the **cosine of the query against that LOINC code's full
    LongCommonName embedding** in ``fhir_embeddings.npy``. This score
    lives in the same space as the legacy ``resolve_many`` pipeline's
    score, so the two pipelines' outputs become directly comparable
    (downstream can pick whichever has higher rescore).
    Survivors that the FHIR bundle doesn't know get rescore ``0.0``.

    Algorithm:
      1. Anchor on COMPONENT: keep only LOINC codes whose COMPONENT row
         index equals the COMPONENT top-1 pick's index. Bail with ``[]``
         if the anchor yields zero candidates.
      2. Sort the remaining 5 axes by descending pick-score. For each
         axis: try the score-top filter first; if it empties the set,
         try the count-rerank alt (from ``top_idx_alt``) before giving
         up. Skip the axis entirely only when both empty.
      3. Return survivors as ``(code, long_common_name)`` pairs in the
         order they appear in ``code_index["codes"]``.

    The fallback fixes cases like ``24小时尿钙`` where the score-top
    SYSTEM pick (``Urine sed``) doesn't match any Calcium code but the
    count-alt (``Urine``) does — without the fallback the chain skipped
    SYSTEM entirely and returned 60+ Calcium codes across all systems.
    """
    component_idx = top_idx.get("COMPONENT", -1)
    if component_idx < 0:
        return []
    mask = code_index["idx_COMPONENT"] == component_idx
    if not mask.any():
        return []
    others = sorted(
        [a for a in _RESOLVE_AXES if a != "COMPONENT" and a in top_idx],
        key=lambda a: -scores.get(a, 0.0),
    )
    alt = top_idx_alt or {}
    for axis in others:
        key = f"idx_{axis}"
        if key not in code_index:
            continue
        new_mask = mask & (code_index[key] == top_idx[axis])
        if new_mask.any():
            mask = new_mask
            continue
        # Score-top would empty: try count-rerank alt if different.
        alt_idx = alt.get(axis)
        if alt_idx is not None and alt_idx != top_idx[axis]:
            alt_mask = mask & (code_index[key] == alt_idx)
            if alt_mask.any():
                mask = alt_mask
    surv = np.flatnonzero(mask)
    if surv.size == 0:
        return []
    codes_arr = code_index["codes"]
    longnames_arr = code_index["code_longnames"]
    # Sort survivors by LOINC's COMMON_TEST_RANK (1 = most-observed in
    # real labs, larger = rarer). Rank 0 means "unranked / never seen"
    # — push to the back. Tie-break by alphabetical LOINC code so the
    # order stays deterministic across runs.
    rank_arr = code_index.get("common_test_rank")
    if rank_arr is not None and rank_arr.shape[0] == codes_arr.shape[0]:
        surv_ranks = rank_arr[surv].astype(np.int64)
        sort_key = np.where(
            surv_ranks > 0, surv_ranks, np.iinfo(np.int64).max,
        )
        order = np.argsort(sort_key, kind="stable")
        surv = surv[order]

    # Rescore against full LOINC corpus (LongCommonName embedding). Lives
    # in the same space as the legacy ``resolve_many`` pipeline's score,
    # so downstream A/B comparison is direct: ``score`` here is
    # ``max_pool(cosine(query_spans, fhir_embeddings.npy[row]))`` and
    # legacy's score is the same quantity (plus its rerank bonuses).
    rescores = np.zeros(surv.size, dtype=np.float32)
    if rescore_Q is not None and rescore_cache is not None:
        from ..common import code_to_fhir_id
        full_embs = rescore_cache.get("embs")
        row_by_id = rescore_cache.get("row_by_id")
        if full_embs is not None and row_by_id is not None:
            for j, i in enumerate(surv):
                try:
                    canonical = code_to_fhir_id("LOINC", str(codes_arr[i]))
                except Exception:
                    continue
                row = row_by_id.get(canonical)
                if row is None:
                    continue
                full_emb = np.asarray(full_embs[row], dtype=np.float32)
                norm = float(np.linalg.norm(full_emb))
                if norm <= 0:
                    continue
                full_emb = full_emb / norm
                sims = rescore_Q @ full_emb
                rescores[j] = float(sims.max())
    return [
        (str(codes_arr[i]), str(longnames_arr[i]), float(rescores[j]))
        for j, i in enumerate(surv)
    ]


def _filter_axes_picks(
    picks: list[tuple[str, str, float]],
    query_text: str,
    local_cache: dict | None,
) -> list[tuple[str, str, float]]:
    """Apply the legacy ``_LOINC_FILTERS`` chain to axes-pipeline
    survivors. Maps each survivor's LOINC code to its FHIR-corpus row
    via ``code_to_fhir_id`` + ``row_by_id``, evaluates the composed
    keep mask, drops rows where ``keep[row] == False``.

    Empty result returns ``[]`` (axes contributes null for this query)
    so the legacy pipeline's answer wins the downstream win-by-max
    selection. Without this, axes survivors that the legacy pipeline
    would have explicitly nulled (vaginal WBC in Blood, pericardial
    ESR in Blood, lactate panel for single-analyte query, Bifidobac-
    terium in Milk for stool indicator, β-glucuronidase in CSF for
    stool indicator) still surface with higher cosine and override the
    correct legacy answer. Codes the FHIR bundle doesn't know (axes-
    only) pass through unfiltered — legacy can't have an opinion on
    those.
    """
    if not picks or local_cache is None:
        return picks
    from ..resolve.pipeline import _compose_loinc_keep
    keep = _compose_loinc_keep(query_text, local_cache)
    if keep is None:
        return picks
    from ..common import code_to_fhir_id
    row_by_id = local_cache.get("row_by_id")
    if row_by_id is None:
        return picks
    filtered: list[tuple[str, str, float]] = []
    for code, name, score in picks:
        try:
            canonical = code_to_fhir_id("LOINC", str(code))
        except Exception:
            filtered.append((code, name, score))
            continue
        row = row_by_id.get(canonical)
        if row is None:
            filtered.append((code, name, score))
            continue
        if 0 <= row < len(keep) and bool(keep[row]):
            filtered.append((code, name, score))
    return filtered


def _property_family_from_value(value: str | None) -> str | None:
    """``"350 mg/24h"`` → ``"MRat"``. Returns ``None`` when the string
    has no parseable unit or the unit doesn't pin a single PROPERTY family.

    Ambiguous units (``%``, ``mm[Hg]``) yield ``None`` — caller falls
    back to embedding for those. Pinning would need additional analyte
    context the resolver doesn't have at this layer.
    """
    if not value:
        return None
    from mirobody.units import parse_value_unit, unit_families
    parsed = parse_value_unit(value)
    if not parsed.unit:
        return None
    fams = unit_families(parsed.unit)
    if len(fams) != 1:
        return None
    return next(iter(fams))


async def resolve_axes_many(
    terms: list[str],
    *,
    values: list[str | None] | None = None,
    bundle_dir: str | None = None,
    provider: str | None = None,
) -> list[dict]:
    """For each query, return per-axis top-1 vocab picks + a LOINC code
    lookup derived from them.

    *values*: optional one-per-term observed value string (``"350 mg/24h"``).
    When the value carries a unit that pins a single LOINC PROPERTY
    family (see :mod:`..units`), the PROPERTY axis pick is overridden
    to that family — bypasses the embedder for the axis that's
    inherently unit-determined (``MCnc`` vs ``MRat`` etc.).

    Output per term::

        {
          "axes": {axis: AxisCode},          # 6 entries when all vocabs present
          "loinc": [(code, long_name), ...], # COMPONENT-anchored survivors
        }

    Blank queries yield ``{}``. The LOINC list is empty when
    ``code_index.npz`` is missing or the COMPONENT anchor matches no
    code (shouldn't happen given the vocab is built from PartLink).
    """
    from mirobody.utils.embedding import text_embedding
    from ...search import AxisCode

    res_dir = bundle_dir or RES_DIR
    vocab = _get_axis_vocab(res_dir)
    code_index = _load_code_index(res_dir)
    # Full LOINC corpus embedding bundle — used to rescore each picked
    # LOINC code by query × LongCommonName cosine. Lazy-loaded shared
    # singleton inside :mod:`.local`; ``None`` when the bundle is
    # missing (deployment without full FHIR data — picks just lack a
    # comparable-to-legacy score).
    from ..index import load as _load_local
    # ``load_meta=True`` materializes ``cache['names']`` so the legacy
    # row-level filter chain (specimen / panel / sleep / chemical-IRS
    # gates) can inspect LCN tokens. Without names, the LCN-based
    # filters silently return all-False masks for the axes pipeline.
    # Cost: ~50MB heap; one-time per process.
    local_cache = _load_local(load_meta=True, bundle_dir=bundle_dir)

    if values is not None and len(values) != len(terms):
        raise ValueError(
            f"values length ({len(values)}) must match terms length ({len(terms)})"
        )

    out: list[dict] = [{} for _ in terms]
    if not vocab:
        log.warning("resolve_axes_many: no axis vocab loaded from %s", res_dir)
        return out

    valid_idx = [i for i, t in enumerate(terms) if t and t.strip()]
    if not valid_idx:
        return out

    # Multi-span embedding: each query becomes 1-N spans (full query +
    # progressively-peeled shortenings). Per-axis cosine is max-pooled
    # across spans so atomic analytes (the ``钙`` tail of ``24小时尿钙``)
    # can outrank semantic compounds that the full string drags toward.
    spans_per_q: list[list[str]] = [
        _generate_axis_spans(terms[i]) for i in valid_idx
    ]
    flat_inputs: list[str] = []
    flat_to_local: list[int] = []  # local-index (position in valid_idx) per flat input
    for local_i, spans in enumerate(spans_per_q):
        for s in spans:
            flat_inputs.append(s)
            flat_to_local.append(local_i)
    vecs = await text_embedding(flat_inputs, provider=provider, cache=True)

    # Pre-index PROPERTY vocab by name for unit-family override lookup.
    property_name_to_idx: dict[str, int] = {}
    if "PROPERTY" in vocab:
        property_name_to_idx = {n: i for i, n in enumerate(vocab["PROPERTY"][1])}

    # Count-rerank candidate-pool size. Within the cosine top-K, pick
    # the vocab row with the highest LOINC-usage count — a frequency
    # tie-breaker that surfaces clinically-canonical analytes when
    # query/literal-translation lands on a generic peer (``尿糖`` →
    # ``Sugar`` 0.78 with count=14, while ``Glucose`` at #4 has count=
    # 2065). K=5 empirically covers the right analyte in `Glucose` /
    # `Calcium` / `Calcium/Creatinine` cases without diluting.
    _RERANK_K = 5

    # Reshape flat embeddings into per-query (S_q, D) blocks; normalize
    # each row defensively (Gemini returns near-unit but conversion to
    # fp32 can drift).
    flat_arr = np.asarray(vecs, dtype=np.float32)
    flat_norms = np.linalg.norm(flat_arr, axis=1, keepdims=True)
    flat_arr = np.divide(
        flat_arr, flat_norms, out=np.zeros_like(flat_arr),
        where=flat_norms > 0,
    )
    # Group flat rows by local query index — list of (S_q, D) matrices.
    q_blocks: list[np.ndarray] = []
    cursor = 0
    for spans in spans_per_q:
        s = len(spans)
        q_blocks.append(flat_arr[cursor:cursor + s])
        cursor += s

    for local_i, slot in enumerate(valid_idx):
        Q = q_blocks[local_i]               # (S_q, D)
        if Q.shape[0] == 0 or not np.any(np.linalg.norm(Q, axis=1) > 0):
            continue
        picks: dict[str, AxisCode] = {}
        picks_alt: dict[str, AxisCode] = {}
        top_idx: dict[str, int] = {}
        top_idx_alt: dict[str, int] = {}
        scores: dict[str, float] = {}
        q_wants_ratio = _query_wants_ratio(terms[slot])
        q_wants_nonlab = _query_wants_nonlab(terms[slot])
        for axis, (embs, names, counts) in vocab.items():
            # Per-axis cosine matrix (N_axis, S_q); max-pool across
            # spans gives each vocab row its best cosine over all query
            # framings — the "shortest meaningful framing" wins for
            # atomic-analyte vocab entries.
            sims_per_span = embs @ Q.T      # (N, S_q)
            sims = sims_per_span.max(axis=1)
            # Mask vocab rows that are never used as a full axis value
            # by any LOINC code (e.g. ``24H specimen`` in COMPONENT) —
            # they can't anchor anything in the downstream filter chain.
            if code_index is not None:
                viable = code_index.get(f"viable_{axis}")
                if viable is not None and viable.shape[0] == sims.shape[0] and viable.any():
                    sims = np.where(viable, sims, -np.inf)
            # Ratio guard (COMPONENT only): mirror the adapter-side
            # ratio rule but inverted — when the query doesn't ask for
            # a ratio/index/fraction, drop those COMPONENT shapes
            # (``Calcium/Creatinine``, ``Fractional excretion of X``,
            # ``X renal clearance``) so the atomic analyte can win.
            #
            # The symmetric direction (drop atomic COMPONENT when query
            # IS a ratio) is intentionally NOT applied here. Orientation
            # handling lives at the final-pick post-rerank step in the
            # v2 pipeline's ``_loinc_picks_topk._finalize``, which only
            # acts on overlay-bridgeable X/Y queries (``谷草/谷丙`` →
            # AST/ALT) and steps aside for the ambiguous "or"-style
            # slashes (``白色念珠菌/都柏林念珠菌``, ``卵巢/睾丸``).
            # Applying a symmetric drop here would force every X/Y
            # query into a ratio-shape COMPONENT pick, breaking
            # alternative-species panels (``小麦/麸质蛋白组反应性...``).
            if axis == "COMPONENT" and not q_wants_ratio:
                ratio_mask = _component_ratio_mask(names)
                if ratio_mask.shape[0] == sims.shape[0]:
                    sims = np.where(ratio_mask, -np.inf, sims)
            # Non-lab guard (COMPONENT only): drop ``X intake`` /
            # ``X goal`` parts when the query carries no intake/goal
            # license token. Without this, ``维生素B7`` → ``Vitamin B7
            # intake`` COMPONENT (cosine 0.84) → only 81017-6/81018-4
            # in pool, atomic ``Biotin`` 34398-8 unreachable.
            if axis == "COMPONENT" and not q_wants_nonlab:
                nonlab_mask = _component_nonlab_mask(names)
                if nonlab_mask.shape[0] == sims.shape[0]:
                    sims = np.where(nonlab_mask, -np.inf, sims)

            # CLASS-gate guard (COMPONENT only): when the query carries
            # an ALLERGY / MICRO keyword (``过敏 / 细菌 / 培养 / …``),
            # restrict COMPONENT vocab to entries used by ≥1 LOINC code
            # in the gated CLASS. Catches the ``过敏,左氧氟沙星`` case
            # where the generic ``levoFLOXacin`` COMPONENT (count=41,
            # used by Susceptibility/Toxicology codes in CLASS=ABXBACT
            # / DRUG/TOX) outscores ``Ofloxacin Ab.IgE`` /
            # ``Ciprofloxacin Ab.IgE`` (CLASS=ALLERGY) on bare drug-name
            # cosine. Mirrors the legacy pipeline's
            # ``apply_deterministic_class_filter`` in
            # :mod:`..resolve.axis`. Silent (no mask) when no gate
            # keyword fires or the mask would zero out every row (lets
            # cosine route freely instead of returning empty).
            if axis == "COMPONENT":
                try:
                    from ..resolve.axis import (
                        _class_gate_res, _earliest_gate_class,
                    )
                    gate_winner = _earliest_gate_class(
                        terms[slot], _class_gate_res(),
                    )
                except Exception:
                    gate_winner = None
                if gate_winner and code_index is not None:
                    cm = _build_class_component_mask(
                        code_index, local_cache, gate_winner, sims.shape[0],
                    )
                    if cm is not None and cm.shape[0] == sims.shape[0]:
                        masked = np.where(cm, sims, -np.inf)
                        if np.isfinite(masked).any():
                            sims = masked

            # COMPONENT two-stage selection. Stage 1 picks the top-K
            # ANALYTE FAMILIES (unique atomic heads) by head cosine —
            # ``^``-compounds inherit their atomic head's cosine via
            # ``head_idx_COMPONENT``, so the whole family scores
            # together and ranks alongside atomic analytes. Stage 2 is
            # the normal argmax on full cosine, restricted to vocab
            # rows whose head is in the top-K family pool.
            #
            # Effect: for ``C-肽(半小时)`` the ``C peptide`` family
            # (atomic + all ``^XH post Y`` compounds) is in the pool;
            # within the pool the highest-full-cosine compound wins.
            # For ``24小时尿钙`` the ``Calcium`` family is in the pool
            # but compounds (``^4H post X challenge``) don't match the
            # query's time tokens, so atomic ``Calcium`` keeps top-1.
            if axis == "COMPONENT" and code_index is not None:
                head_idx = code_index.get("head_idx_COMPONENT")
                if head_idx is not None and head_idx.shape[0] == sims.shape[0]:
                    unique_heads = np.unique(head_idx)
                    head_sims = sims[unique_heads]
                    valid = np.isfinite(head_sims)
                    if valid.any():
                        valid_heads = unique_heads[valid]
                        valid_head_sims = head_sims[valid]
                        k = min(_RERANK_K, valid_heads.size)
                        top_heads_part = np.argpartition(
                            -valid_head_sims, k - 1,
                        )[:k]
                        top_heads = valid_heads[top_heads_part]
                        pool_mask = np.isin(head_idx, top_heads)
                        sims = np.where(pool_mask, sims, -np.inf)
            top = int(np.argmax(sims))
            s = float(sims[top])
            picks[axis] = AxisCode(
                system="LOINC", code="", name=names[top], score=s,
            )
            top_idx[axis] = top
            scores[axis] = s

            # Count-rerank tie-breaker: within the cosine top-K, pick
            # the vocab row with highest LOINC-usage count. Surfaces
            # canonical analytes (``Glucose`` count=2065) when the
            # literal-translation winner is a generic peer (``Sugar``
            # count=14). Emit as ``picks_alt[axis]`` only when it
            # differs from the score-top — otherwise no value-add.
            if counts.size == sims.size:
                topk = np.argpartition(-sims, min(_RERANK_K, sims.size - 1))[:_RERANK_K]
                # Filter out -inf entries (e.g. masked vocab rows).
                topk = topk[np.isfinite(sims[topk])]
                if topk.size > 0:
                    alt = int(topk[np.argmax(counts[topk])])
                    if alt != top:
                        picks_alt[axis] = AxisCode(
                            system="LOINC", code="",
                            name=names[alt], score=float(sims[alt]),
                        )
                        top_idx_alt[axis] = alt

        # Unit-driven PROPERTY override — unit pins the LOINC PROPERTY
        # family exactly (mg/24h → MRat, mg/dL → MCnc, …), so when the
        # caller supplied a parseable value+unit, prefer that over the
        # embedder's PROPERTY pick. Single-family hits only — ambiguous
        # units (``%``, ``mm[Hg]``) fall back to the embedding pick.
        if values is not None and "PROPERTY" in vocab:
            family = _property_family_from_value(values[slot])
            if family and family in property_name_to_idx:
                idx = property_name_to_idx[family]
                # Show the embedder's actual cosine for the forced
                # family (max-pooled across spans, same scale as other
                # axis scores) — diagnostic only, not used by routing.
                fam_sims = (vocab["PROPERTY"][0] @ Q.T).max(axis=1)
                picks["PROPERTY"] = AxisCode(
                    system="LOINC", code="", name=family,
                    score=float(fam_sims[idx]),
                )
                top_idx["PROPERTY"] = idx
                # Score for the filter-chain ordering reflects the
                # confidence the unit-parser conveys — units that pin a
                # single family are deterministic, so promote PROPERTY
                # to filter-chain priority by giving it a max-cosine.
                scores["PROPERTY"] = 1.0

        loinc_picks: list[tuple[str, str, float]] = []
        loinc_picks_alt: list[tuple[str, str, float]] = []
        # PSG honest-null gate: when the query asks for a concept LOINC
        # doesn't enumerate (microarousal sub-cause / sleep
        # hypoventilation / flow-limitation / PLM / sleep posture),
        # skip the LOINC lookup entirely. Mirrors the legacy
        # ``_sleep_analyte_keep`` force-null branch so the axes pipeline
        # produces the same honest-null output for these queries; lazy
        # import to keep this module independent of the resolve subpkg
        # at load time.
        force_null = False
        if code_index is not None:
            from ..resolve.pipeline import should_force_null_loinc
            force_null = should_force_null_loinc(terms[slot])
        if code_index is not None and not force_null:
            # Primary chain: score-top picks with per-axis count-alt
            # fallback when score-top would empty the candidate set.
            loinc_picks = _lookup_loinc_codes(
                top_idx, scores, code_index, top_idx_alt,
                rescore_Q=Q, rescore_cache=local_cache,
            )
            # Alt chain: only fires when COMPONENT has a count-rerank
            # disagreement (different anchor → different candidate set).
            # Non-COMPONENT axes' alts already participate via the
            # primary chain's fallback.
            if "COMPONENT" in top_idx_alt:
                alt_top_idx = dict(top_idx)
                alt_top_idx["COMPONENT"] = top_idx_alt["COMPONENT"]
                loinc_picks_alt = _lookup_loinc_codes(
                    alt_top_idx, scores, code_index, top_idx_alt,
                    rescore_Q=Q, rescore_cache=local_cache,
                )
            # Legacy row-level filter chain (specimen routing / panel
            # demote / food-IgG / sleep_focus / chemical-IRS class
            # gates / ...) — the axes pipeline shares the same FHIR
            # bundle, so the same masks are computable. Drop survivors
            # whose FHIR row fails the strict mask; on empty fall back
            # to the unfiltered picks so analytes outside LOINC's
            # canonical coverage (rare specimens, niche food panels)
            # don't disappear entirely. See :func:`_filter_axes_picks`.
            loinc_picks = _filter_axes_picks(
                loinc_picks, terms[slot], local_cache,
            )
            if loinc_picks_alt:
                loinc_picks_alt = _filter_axes_picks(
                    loinc_picks_alt, terms[slot], local_cache,
                )

        rec: dict = {"axes": picks, "loinc": loinc_picks}
        if picks_alt:
            rec["axes_alt"] = picks_alt
        if loinc_picks_alt:
            rec["loinc_alt"] = loinc_picks_alt
        out[slot] = rec
    return out


async def cmd_loinc_axis_emb(args: Namespace) -> None:
    res_dir = args.res_dir or RES_DIR
    axes_dir = os.path.join(res_dir, "loinc_axes")
    bundle_path = os.path.join(res_dir, _BUNDLE_BASENAME)
    if not os.path.isdir(axes_dir):
        raise FileNotFoundError(
            f"{axes_dir} not found — run `loinc-axis-vocab` first."
        )

    if args.axis:
        tsv_paths = [os.path.join(axes_dir, f"{a}.tsv") for a in args.axis]
        for p in tsv_paths:
            if not os.path.isfile(p):
                raise FileNotFoundError(p)
    else:
        tsv_paths = sorted(
            os.path.join(axes_dir, f)
            for f in os.listdir(axes_dir)
            if f.endswith(".tsv")
        )

    # Carry over already-built members from any prior bundle so
    # ``--axis X`` re-embeds X without losing other 6 (and so the
    # code-index members written by ``loinc-axis-vocab`` survive).
    bundled: dict[str, np.ndarray] = {}
    if os.path.isfile(bundle_path):
        with np.load(bundle_path, allow_pickle=True) as z:
            bundled = {k: z[k] for k in z.files}

    for tsv_path in tsv_paths:
        axis = os.path.splitext(os.path.basename(tsv_path))[0]
        texts = _load_axis_inputs(tsv_path)
        names = _read_axis_part_names(tsv_path)
        counts = _read_axis_part_counts(tsv_path)
        log.info("[%s] %s rows from %s", axis, f"{len(texts):,}", tsv_path)
        if not texts:
            continue
        emb = await _embed_axis(texts, args.provider)
        bundled[axis] = emb.astype(np.float16)
        bundled[f"{axis}_names"] = np.asarray(names, dtype=object)
        bundled[f"{axis}_counts"] = np.asarray(counts, dtype=np.int32)
        log.info("[%s] embedded shape=%s dtype=float16", axis, emb.shape)

    np.savez(bundle_path, **bundled)
    embedded_axes = sorted(k for k in bundled if f"{k}_names" in bundled)
    other = sorted(k for k in bundled
                   if k not in embedded_axes and not k.endswith("_names"))
    log.info("wrote %s — %d embedded axes %s; other members: %s",
             bundle_path, len(embedded_axes), embedded_axes, other)

    # Sweep the legacy per-axis ``<AXIS>.npy`` files — the bundle is
    # authoritative now.
    legacy_axes = set(_RESOLVE_AXES) | {"CLASS"}
    for fname in os.listdir(axes_dir):
        if not fname.endswith(".npy"):
            continue
        if os.path.splitext(fname)[0] in legacy_axes:
            os.remove(os.path.join(axes_dir, fname))
            log.info("removed legacy %s", os.path.join(axes_dir, fname))
