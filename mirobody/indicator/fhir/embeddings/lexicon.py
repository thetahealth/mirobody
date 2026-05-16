"""Per-language CN-→canonical-EN lexicons inside ``fhir_loinc_bundle.tar.gz``.

The embedding model bridges common-language ↔ Latin well for common
medical vocabulary (creatinine, hemoglobin, insulin) but fails on
specialist Latin binomial names that rarely co-occur in pretraining
text — e.g. 出芽短梗霉 ↔ *Aureobasidium pullulans*, the species name
that LOINC writes in its English COMPONENT but no Qwen/Gemini bridge
recovers from the literal CJK calque. The query-side augmentation
appends the canonical Latin form to such queries before embedding;
this module owns the dict it appends from.

Bundle layout::

    aliases/zh.tsv     auto-derived from LOINC zhCN5LinguisticVariant.csv
    aliases/ja.tsv     auto-derived from jaJP3LinguisticVariant.csv (future)
    aliases/ko.tsv     etc.

Each TSV has two tab-separated columns, no header, UTF-8:

    src \t dst

…where ``src`` is a non-English clinical token (Chinese, Japanese, …)
and ``dst`` is the canonical LOINC English (often a Latin binomial, or
"<full name> <ACRONYM>" for lab abbreviations).

Why TSV and not JSON: per-line diffs in git survive added/removed
entries cleanly, and ``cat aliases/zh.tsv | grep 烟曲霉`` is the
fastest way to spot-check provenance during development.

Build (re)generates each file from the corresponding LOINC
LinguisticVariant CSV plus the per-language curated overlay in
:mod:`._curated_aliases`. The build path is:

  1. Read LOINC main ``Loinc.csv`` + ``{lang}LinguisticVariant.csv``.
  2. Filter to active rows in ``MICRO`` / ``ALLERGY`` / ``DRUG/TOX``
     classes — categories where Qwen/Gemini lack reliable bridges. CHEM
     / HEM / general clinical chemistry are well-covered by the
     embedding alone; auto-deriving them adds noise without precision
     wins.
  3. For each row, dot-segment both COMPONENT fields and strip a
     trailing-axis token (抗体 / DNA / IgG / 总计 / … on the CN side; the
     corresponding Ab / DNA / IgG / total on the EN side). The
     remainder is the species/analyte phrase.
  4. Keep only no-whitespace CN phrases of 2–8 CJK chars and English
     phrases of ≤ 5 tokens. Aggregate by CN phrase; keep when one EN
     covers ≥ 55% of the occurrences (or single attestation for 3+
     char CN — Latin binomials are reliable even at df=1).
  5. Derive 2-3 CJK-char genus suffixes from accumulated species
     entries (链球菌 → Streptococcus, 葡萄球菌 → Staphylococcus).
  6. Embed (CN, EN) pairs in batches, compute cosine, drop pairs with
     cosine ≥ 0.70 — those are already in the model's pretrained
     repertoire and add nothing.
  7. Filter LOINC-jargon noise (X多个未知种 = ``spp.`` calque, bare
     "X型" axis modifiers, 2-char genus-suffix fragments that are
     suffixes of longer species names) by pattern.
  8. Merge the per-language curated overlay (colloquial clinical
     names that LOINC zhCN's literal Latin transliteration doesn't
     emit — 绿脓杆菌 / 铜绿假单胞菌 — plus lab-test acronyms).

The build is async because step 6 hits the embedding API; expect ~3
minutes for one language. Reruns hit the embedding sqlite cache so
they're near-instant.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np

from .bundle import BUNDLE_BASENAME, BUNDLE_PATH, list_members, read_member, write_member
from .local import RES_DIR

log = logging.getLogger(__name__)


# ── Bundle member naming ──────────────────────────────────────────────


_MEMBER_PREFIX = "aliases/"
_MEMBER_SUFFIX = ".tsv"


def _member_name(lang: str) -> str:
    return f"{_MEMBER_PREFIX}{lang}{_MEMBER_SUFFIX}"


def _is_aliases_member(name: str) -> bool:
    return name.startswith(_MEMBER_PREFIX) and name.endswith(_MEMBER_SUFFIX)


_CURATED_SUFFIX = "_curated"


def _is_curated_member(name: str) -> bool:
    """True iff *name* is a hand-curated overlay (``aliases/{lang}_curated.tsv``).

    The curated overlay is the authoritative manual layer — it overrides
    the auto-derived ``aliases/{lang}.tsv`` for any duplicate key. See
    :func:`load_all_aliases` for the load-order policy that enforces
    this.
    """
    if not _is_aliases_member(name):
        return False
    stem = name[len(_MEMBER_PREFIX) : -len(_MEMBER_SUFFIX)]
    return stem.endswith(_CURATED_SUFFIX)


# ── (De)serialization ─────────────────────────────────────────────────


def _encode_tsv(d: dict[str, str]) -> bytes:
    """Two-column TSV, sorted longest-first so the bundle diff shows
    species/specific entries before short genus aliases — matches the
    runtime regex's leftmost-first / longest-match ordering.
    """
    items = sorted(d.items(), key=lambda kv: (-len(kv[0]), kv[0]))
    lines = [f"{src}\t{dst}" for src, dst in items]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _decode_tsv(raw: bytes) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in raw.decode("utf-8").splitlines():
        line = line.rstrip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t", 1)
        if len(parts) != 2:
            continue
        src, dst = parts
        if not src or not dst:
            continue
        out[src] = dst
    return out


# ── Loader (runtime) ──────────────────────────────────────────────────


def load_all_aliases(bundle_path: str | None = None) -> dict[str, str]:
    """Read every ``aliases/*.tsv`` member from the bundle, union into
    one dict. Empty when the bundle is absent or has no aliases members.

    Load order: **auto-derived members first, curated overlays last.**
    The auto-derive pipeline rebuilds ``aliases/{lang}.tsv`` from the
    LOINC LinguisticVariant CSV on every refresh — same key can flip to
    a different English canonical between releases. The hand-curated
    ``aliases/{lang}_curated.tsv`` is the authoritative manual layer
    (bypasses the cosine filter, covers CLASS-es the auto-derive skips,
    encodes domain judgment that LOINC doesn't represent). Loading
    curated last makes ``dict.update`` give it the final word on any
    duplicate key — without this ordering, an auto-derived entry that
    happens to share a key with a curated one silently overrides the
    human-audited canonical.

    Cross-language key collisions (one CJK term in zh.tsv vs ja.tsv,
    say) are vanishingly rare and resolve via tar-member iteration
    order — non-deterministic but inconsequential at the observed
    overlap rate.
    """
    path = bundle_path or BUNDLE_PATH
    members = [n for n in list_members(bundle_path=path) if _is_aliases_member(n)]
    # Non-curated first, curated last — see docstring.
    members.sort(key=_is_curated_member)
    out: dict[str, str] = {}
    for name in members:
        raw = read_member(name, bundle_path=path)
        if raw is None:
            continue
        out.update(_decode_tsv(raw))
    return out


def list_aliases_languages(bundle_path: str | None = None) -> list[str]:
    """Return sorted language codes for which ``aliases/{lang}.tsv``
    exists in the bundle. Diagnostic helper for callers that want to
    show coverage."""
    path = bundle_path or BUNDLE_PATH
    return sorted(
        name[len(_MEMBER_PREFIX) : -len(_MEMBER_SUFFIX)]
        for name in list_members(bundle_path=path)
        if _is_aliases_member(name)
    )


# ── Build pipeline ────────────────────────────────────────────────────


# LOINC CLASS prefixes to keep when deriving the lexicon. Other classes
# (CHEM / HEM / SURVEY / DOC / …) are skipped: the embedding already
# bridges general clinical chemistry well, and the rest is questionnaire
# / administrative vocabulary that doesn't belong here.
_KEEP_CLASSES = frozenset({"MICRO", "ALLERGY", "DRUG/TOX"})


# Axis suffixes to strip from each dot-segment, leaving the bare
# species / analyte phrase. The EN side is shared across all source
# languages (LOINC COMPONENT is always English); per-language source-
# side axes vary by language.
_EN_TAIL_AXES = frozenset({
    "Ab", "Ag", "Ag+Ab", "DNA", "RNA", "rRNA", "mRNA", "tRNA",
    "gene", "gene+protein",
    "panel", "set", "sp", "species", "group", "identified",
    "identified.PCR", "subgroup",
    "IgG", "IgM", "IgA", "IgE", "IgD", "IgG1", "IgG2", "IgG3", "IgG4",
    "total", "count", "presence", "concentration", "mass", "activity", "titer",
})

_TAIL_AXES_BY_LANG: dict[str, frozenset[str]] = {
    "zh": frozenset({
        "抗体", "抗原", "抗原与抗体", "DNA", "RNA", "rRNA", "mRNA", "tRNA",
        "基因", "基因+蛋白质",
        "组套", "组合", "属", "属单个未知种", "单个未知种", "种", "单个种",
        "分组", "群",
        "类", "检验项目", "培养", "可见性", "存在",
        "可用数量表示的", "可用滴度表示的",
        "IgG", "IgM", "IgA", "IgE", "IgD", "IgG1", "IgG2", "IgG3", "IgG4",
        "总计", "总数", "总量", "分数", "计数", "浓度", "质量", "活力", "活性", "滴度",
    }),
    "ko": frozenset({
        # Equivalents of Chinese / English axis tokens, derived by
        # frequency over koKR13LinguisticVariant.csv. Korean uses
        # transliterations for DNA/RNA (디옥시리보핵산 / 리보핵산).
        "항체", "항원", "항원과 항체",
        "DNA", "RNA", "디옥시리보핵산", "리보핵산", "rRNA", "mRNA", "tRNA",
        "유전자",
        "패널", "패키지", "그룹", "분류", "확인", "동정",
        "검체", "세포", "균",
        "관찰", "결정", "표현형", "분석", "분석용",
        "방사선알레르기흡착검사",
        "IgG", "IgM", "IgA", "IgE", "IgD", "IgG1", "IgG2", "IgG3", "IgG4",
        "총", "총량", "수", "농도", "질량", "활성도", "역가",
        "양성도", "양성률",
    }),
    "de": frozenset({
        # German LOINC tail tokens (deDE15). Ak = Antikörper (antibody),
        # Ag = Antigen. Klasse / Beobachtung / Stimulation / frei / gesamt
        # are axis-position modifiers — strip them.
        "Ak", "Ag", "DNA", "RNA", "rRNA", "mRNA", "tRNA",
        "IgG", "IgM", "IgA", "IgE", "IgD", "IgG1", "IgG2", "IgG3", "IgG4",
        "Klasse", "Probenmaterial", "Stimulation", "Zellen",
        "Beobachtung", "Erythrozyten", "Hämoglobin",
        "gesamt", "frei", "neutralisierend",
    }),
    "fr": frozenset({
        # French LOINC tail tokens (frFR18). Ac = Anticorps (antibody),
        # ADN = DNA, ARN = RNA.
        "Ac", "Ag", "ADN", "ARN", "DNA", "RNA", "rRNA",
        "IgG", "IgM", "IgA", "IgE", "IgD", "IgG1", "IgG2", "IgG3", "IgG4",
        "anticorps", "antigène", "panel", "épreuve", "RAST",
        "prélèvement", "cellules", "leucocytes",
        "trouvée", "identifié", "Observation",
        "total", "totale", "libre", "mutations",
    }),
    "es": frozenset({
        # Spanish LOINC tail tokens (esES12). Anticuerpos = antibodies,
        # Antígeno = antigen, Acs = abbrev for Anticuerpos.
        "Anticuerpos", "Acs", "Antígeno", "Ag", "ADN", "RNA", "DNA",
        "rRNA", "mRNA", "tRNA",
        "IgG", "IgM", "IgA", "IgE", "IgD", "IgG1", "IgG2", "IgG3", "IgG4",
        "panel", "especimen", "Observación",
        "total", "totales", "libre", "inducido",
        "especifica", "mutaciones", "fármacos",
    }),
    "ru": frozenset({
        # Russian LOINC tail tokens (ruRU20). Cyrillic Ат = Антитело
        # (antibody), Аг = Антиген, ДНК = DNA, РНК = RNA. Ig-tags are
        # written in Latin even in Russian LOINC.
        "Ат", "Аг", "ДНК", "РНК", "DNA", "RNA", "rRNA", "mRNA", "tRNA",
        "IgG", "IgM", "IgA", "IgE", "IgD", "IgG1", "IgG2", "IgG3", "IgG4",
        "пробы", "образец", "Клетки", "клетки", "панель", "ген",
        "класс", "общий", "свободный", "находки", "мутацию",
        "протокол", "порог",
    }),
}


# Per-language LinguisticVariant filename in LOINC's
# AccessoryFiles/LinguisticVariants/ directory. The two-letter key
# matches the file's ISO 639-1 language code; the value is the full
# filename for LOINC 2.82. Update the version-numbered suffix when LOINC
# ships a newer release (e.g. ``zhCN6LinguisticVariant.csv``). For
# region-specific variants (deAT24, esAR7, esMX28, frBE23, frCA8) — add
# under the same two-letter prefix on demand; left out by default to
# keep the language list to one canonical variant per language.
_LINGUISTIC_VARIANT_BY_LANG: dict[str, str] = {
    "ar": "arJO32LinguisticVariant.csv",
    "cs": "csCZ33LinguisticVariant.csv",
    "de": "deDE15LinguisticVariant.csv",
    "el": "elGR17LinguisticVariant.csv",
    "es": "esES12LinguisticVariant.csv",
    "et": "etEE10LinguisticVariant.csv",
    "fr": "frFR18LinguisticVariant.csv",
    "it": "itIT16LinguisticVariant.csv",
    "ko": "koKR13LinguisticVariant.csv",
    "nl": "nlNL22LinguisticVariant.csv",
    "pl": "plPL29LinguisticVariant.csv",
    "pt": "ptBR11LinguisticVariant.csv",
    "ru": "ruRU20LinguisticVariant.csv",
    "tr": "trTR19LinguisticVariant.csv",
    "uk": "ukUA30LinguisticVariant.csv",
    "zh": "zhCN5LinguisticVariant.csv",
}


def _find_linguistic_variant(loinc_dir: str, lang: str) -> str | None:
    filename = _LINGUISTIC_VARIANT_BY_LANG.get(lang)
    if not filename:
        return None
    path = os.path.join(loinc_dir, "AccessoryFiles", "LinguisticVariants", filename)
    return path if os.path.isfile(path) else None


# Per-language token-shape rule for the ``src`` side.
#
# - ``zh``: 2-8 CJK Unified Ideographs, no whitespace. Tighter upper
#   bound (8) because Chinese clinical terms above 8 chars are usually
#   multi-axis composite phrases, not atomic concepts.
# - ``ko``: 2-12 Hangul Syllables (가-힯), no whitespace. Korean
#   transliterates Latin species names syllable-by-syllable (each Latin
#   syllable ≈ one Hangul block), so the same biological concept that's
#   3-5 CJK chars in Chinese can be 6-10 Hangul blocks in Korean
#   (Aspergillus fumigatus → 아스페르길루스 푸미가투스).
# - ``de/fr/es``: Latin letters with Latin-1 / Latin-Ext-A accents
#   plus hyphens/apostrophes for compound terms (Sézary / l'enfant).
#   Length 3-20. The cosine filter handles the lexical-near-clones
#   between these languages and English (most non-clone entries
#   filter through because EN and the local script differ even when
#   both use Latin letters).
# - ``ru``: 3-20 Cyrillic letters (а-яё), no whitespace.
_SRC_PATTERN_BY_LANG: dict[str, re.Pattern] = {
    "zh": re.compile(r"^[一-鿿]{2,8}$"),
    "ko": re.compile(r"^[가-힯]{2,12}$"),
    "de": re.compile(r"^[A-Za-zÄÖÜäöüß'\-]{3,20}$"),
    "fr": re.compile(r"^[A-Za-zÀ-ÿ'\-]{3,20}$"),
    "es": re.compile(r"^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ'\-]{3,20}$"),
    "ru": re.compile(r"^[А-Яа-яЁё'\-]{3,20}$"),
}


def _passes_src_shape(s: str, lang: str) -> bool:
    p = _SRC_PATTERN_BY_LANG.get(lang)
    return p is not None and p.match(s) is not None


def _strip_tail(seg: str, tail: frozenset[str]) -> str:
    toks = seg.split()
    while toks and toks[-1] in tail:
        toks.pop()
    return " ".join(toks)


def _dot_segments(s: str) -> list[str]:
    return [seg.strip() for seg in s.split(".") if seg.strip()]


# Patterns for LOINC-jargon noise that survives axis-stripping. The
# user-typed clinical query never carries these calques — they're
# bookkeeping artifacts of LOINC's translation of ``spp.`` / ``sp.`` /
# axis modifiers.
_NOISE_PATTERNS = (
    re.compile(r"未知种$"),       # X单个未知种 / X多个未知种 — LOINC ``sp.`` / ``spp.``
    re.compile(r"^.{2,3}型$"),    # bare X型 axis modifier (口服型, 游离型, 还原型, …)
)


def _is_jargon(s: str) -> bool:
    return any(p.search(s) for p in _NOISE_PATTERNS)


_LATIN_GENUS_RE = re.compile(r"^[A-Z][a-z]+$")


def _is_short_fragment(src: str, dst: str, all_keys: set[str]) -> bool:
    """Drop 2-char CN entries that look like genus-suffix derivations
    from longer species names — e.g. ``团菌`` from ``军团菌`` mapping to
    ``Legionella``, ``岛素`` from ``胰岛素`` to ``Insulin``. Heuristic:
    2-char src + single Latin-word dst + the src is a suffix of some
    other longer src in the lexicon.
    """
    if len(src) != 2:
        return False
    if not _LATIN_GENUS_RE.match(dst):
        return False
    for other in all_keys:
        if other != src and len(other) > 2 and other.endswith(src):
            return True
    return False


# Hand-validated allowlist of 2-char clinical names that look LIKE
# fragments per the heuristic but are real Chinese clinical terms
# (allergen tree names, pesticide INNs, parasite genera commonly
# called by 2-char names in Chinese reports).
_TWO_CHAR_ALLOWLIST = frozenset({
    "乐果",   # Dimethoate (organophosphate pesticide)
    "刺柏",   # Juniper (tree allergen)
    "叶杨",   # Populus (tree allergen)
    "火蚁",   # Solenopsis (fire ant allergen)
    "线虫",   # Anisakis (nematode)
    "滨藜",   # Saltbush (plant allergen)
    "根霉",   # Rhizopus (fungus)
    "毛霉",   # Mucor (fungus)
})


def _derive_pairs_from_loinc(loinc_csv: str, ling_csv: str, lang: str) -> dict[str, str]:
    """Stage 1: aggregate (src, dst) pairs from LOINC main + per-
    language linguistic variant, axis-stripped and shape-filtered.
    """
    import polars as pl
    en = pl.read_csv(
        loinc_csv,
        columns=["LOINC_NUM", "COMPONENT", "CLASS", "STATUS"],
        infer_schema=False,
    ).filter(
        (pl.col("STATUS") == "ACTIVE")
        & (pl.col("CLASS").is_in(list(_KEEP_CLASSES)))
    )
    zh = pl.read_csv(
        ling_csv,
        columns=["LOINC_NUM", "COMPONENT"],
        infer_schema=False,
    ).rename({"COMPONENT": "_LING"})
    joined = en.join(zh, on="LOINC_NUM", how="inner").filter(
        pl.col("_LING").is_not_null() & pl.col("COMPONENT").is_not_null()
    )
    log.info("lexicon build [%s]: %d active rows in %s", lang, joined.height, _KEEP_CLASSES)

    src_tail = _TAIL_AXES_BY_LANG.get(lang, frozenset())
    pairs: list[tuple[str, str]] = []
    for row in joined.iter_rows(named=True):
        src_full = row["_LING"].strip()
        dst_full = row["COMPONENT"].strip()
        src_segs = _dot_segments(src_full)
        dst_segs = _dot_segments(dst_full)
        for i in range(min(len(src_segs), len(dst_segs))):
            s = _strip_tail(src_segs[i], src_tail)
            d = _strip_tail(dst_segs[i], _EN_TAIL_AXES)
            if not s or not d:
                continue
            if not _passes_src_shape(s, lang):
                continue
            if not re.search(r"[A-Za-z]{3,}", d):
                continue
            if len(d.split()) > 5:
                continue
            pairs.append((s, d))

    agg: dict[str, Counter] = defaultdict(Counter)
    for s, d in pairs:
        agg[s][d] += 1
    out: dict[str, str] = {}
    for s, ctr in agg.items():
        total = sum(ctr.values())
        top_d, top_n = ctr.most_common(1)[0]
        # ≥ 2 attestations require 55% majority; single attestation OK
        # for 3+ CJK char keys (Latin binomial alignment is reliable
        # at df=1 — only multi-attestation noise needs the majority
        # gate).
        if (total >= 2 and top_n / total >= 0.55) or (total == 1 and len(s) >= 3):
            out[s] = top_d
    return out


def _derive_genus_aliases(lex: dict[str, str]) -> dict[str, str]:
    """Stage 2: derive 2-3 char genus suffixes from species entries.

    ``烟曲霉 → Aspergillus fumigatus`` + ``黄曲霉 → Aspergillus flavus``
    +  ``黑曲霉 → Aspergillus niger`` lets us derive ``曲霉 → Aspergillus``
    by majority vote on the shared CJK suffix.
    """
    suf_genus: dict[str, Counter] = defaultdict(Counter)
    for src, dst in lex.items():
        if len(src) < 4:
            continue
        words = dst.split()
        if len(words) < 2 or not words[0][0].isupper():
            continue
        for suf_len in (2, 3):
            if len(src) > suf_len:
                suf_genus[src[-suf_len:]][words[0]] += 1
    derived: dict[str, str] = {}
    for suf, ctr in suf_genus.items():
        total = sum(ctr.values())
        top_g, top_n = ctr.most_common(1)[0]
        if total >= 3 and top_n / total >= 0.7:
            derived[suf] = top_g
    return derived


async def _filter_by_cosine(lex: dict[str, str], threshold: float = 0.70) -> dict[str, str]:
    """Stage 3: drop entries where the embedding model already bridges
    ``src ↔ dst`` (cosine ≥ *threshold*). The augmentation only helps
    when the bridge is genuinely weak; high-cosine pairs would just
    dilute the embedding with redundant English tokens.
    """
    from mirobody.utils.embedding import text_embedding
    items = list(lex.items())
    if not items:
        return {}
    srcs = [s for s, _ in items]
    dsts = [d for _, d in items]
    log.info("lexicon build: embedding %d src + %d dst pairs (cached)", len(srcs), len(dsts))
    src_emb = await text_embedding(srcs, provider="gemini", cache=True)
    dst_emb = await text_embedding(dsts, provider="gemini", cache=True)
    sa = np.asarray(src_emb, dtype=np.float32)
    da = np.asarray(dst_emb, dtype=np.float32)
    sa /= np.linalg.norm(sa, axis=1, keepdims=True) + 1e-9
    da /= np.linalg.norm(da, axis=1, keepdims=True) + 1e-9
    cos = (sa * da).sum(axis=1)
    kept = {srcs[i]: dsts[i] for i in range(len(items)) if cos[i] < threshold}
    log.info(
        "lexicon build: cosine<%.2f kept %d / %d (dropped %d the model already knows)",
        threshold, len(kept), len(items), len(items) - len(kept),
    )
    return kept


def _strip_noise(lex: dict[str, str]) -> dict[str, str]:
    """Stage 4: drop LOINC-jargon entries and likely genus-fragment
    artifacts. Hand-validated 2-char clinical names are preserved via
    :data:`_TWO_CHAR_ALLOWLIST`.
    """
    all_keys = set(lex.keys())
    out: dict[str, str] = {}
    n_jargon = 0
    n_frag = 0
    for src, dst in lex.items():
        if _is_jargon(src):
            n_jargon += 1
            continue
        if src not in _TWO_CHAR_ALLOWLIST and _is_short_fragment(src, dst, all_keys):
            n_frag += 1
            continue
        out[src] = dst
    log.info(
        "lexicon build: noise filter dropped %d jargon + %d fragments → %d kept",
        n_jargon, n_frag, len(out),
    )
    return out


# Languages that have no LOINC LinguisticVariant CSV and must be
# sourced from UMLS MRCONSO instead. The MRCONSO crosswalk runs MeSH-
# JPN ↔ MeSH-ENG and MedDRA-JPN ↔ MedDRA-EN pairings — far broader than
# LOINC's lab focus but covers the same gap (Latin / English-canonical
# clinical names whose Japanese equivalents the embedding doesn't
# bridge). Japan ships LOINC translations separately via JAMS, not via
# the LOINC International release, so UMLS is the practical source.
_UMLS_FALLBACK_LANGS: dict[str, tuple[str, ...]] = {
    "ja": ("MSHJPN", "MDRJPN"),
}


def _derive_pairs_from_umls(
    mrconso_path: str,
    lang: str,
    jpn_sources: tuple[str, ...],
) -> dict[str, str]:
    """Walk MRCONSO once, harvest (src-lang preferred-term, EN canonical)
    pairs sharing a CUI.

    *jpn_sources*: SABs whose ``LAT=JPN`` strings count as source-side
    canonical names. ``MSHJPN`` / ``MDRJPN`` are the only two sources
    UMLS 2025AB ships with Japanese strings; both translate concepts
    that LOINC / SNOMED already index, so the resulting (JPN, ENG) pair
    is generally LOINC-compatible.

    Source-side: takes the canonical preferred term per CUI (``TTY ==
    PEN`` for MeSH, ``TTY == PT`` for MedDRA) — synonyms / variants are
    skipped to keep the dict small and the regex efficient.

    Target-side: pairs JPN concept with its English from the SAME root
    vocabulary (``MSHJPN`` ↔ ``MSH``, ``MDRJPN`` ↔ ``MDR``). Cross-SAB
    walks via CUI would conflate concepts the source vocab considers
    distinct, so we keep the join tight.
    """
    from collections import defaultdict
    src_pref: dict[str, dict[str, str]] = {}     # cui → {sab_root: src_str}
    eng_pref: dict[str, dict[str, str]] = {}     # cui → {sab: eng_str}
    sab_root = {sab: sab.replace("JPN", "") or sab for sab in jpn_sources}
    eng_sabs = {root: sab for sab, root in sab_root.items()}
    eng_target_sabs = set(eng_sabs.keys()) | {"MSH", "MDR"}
    pref_ttys_src = {"PEN", "PT"}
    pref_ttys_eng = {"MH", "PT"}
    log.info("MRCONSO scan: jpn sources=%s, eng targets=%s",
             jpn_sources, sorted(eng_target_sabs))
    with open(mrconso_path, encoding="utf-8") as f:
        for line in f:
            cols = line.split("|")
            if len(cols) < 15:
                continue
            cui, lat, sab, tty, s = cols[0], cols[1], cols[11], cols[12], cols[14]
            if not s:
                continue
            if lat == "JPN" and sab in jpn_sources and tty in pref_ttys_src:
                root = sab_root[sab]
                src_pref.setdefault(cui, {})[root] = s
            elif lat == "ENG" and sab in eng_target_sabs and tty in pref_ttys_eng:
                eng_pref.setdefault(cui, {})[sab] = s

    out: dict[str, str] = {}
    for cui, src_by_root in src_pref.items():
        engs = eng_pref.get(cui)
        if not engs:
            continue
        # Prefer the SAME-root EN string (MSHJPN→MSH, MDRJPN→MDR);
        # fall back to whatever EN exists at this CUI.
        for root, src_str in src_by_root.items():
            en_str = engs.get(root)
            if en_str is None:
                en_str = next(iter(engs.values()))
            # Shape filter for the source string: at least one CJK char,
            # length 2-20 (Japanese clinical terms can run longer than
            # zh atomic concepts because kana adds syllables).
            if not (2 <= len(src_str) <= 20):
                continue
            if not re.search(r"[一-鿿぀-ヿ㐀-䶿]", src_str):
                continue
            # Shape filter for the EN side: must contain a 3+ letter
            # word; ≤ 5 tokens to keep canonical phrases short.
            if not re.search(r"[A-Za-z]{3,}", en_str):
                continue
            if len(en_str.split()) > 5:
                continue
            # UMLS-specific specificity filter. The LOINC LinguisticVariant
            # path is already filtered to MICRO / ALLERGY / DRUG/TOX so
            # the source pairs are inherently specialty terms. UMLS
            # MSHJPN / MDRJPN cover the FULL medical-concept space —
            # most pairs (高血圧 → Hypertension, 心筋梗塞 → Myocardial
            # infarction) are common conditions the embedding bridges
            # fluently. Keep only pairs whose EN side looks like a
            # rare-vocabulary specialty term (Latin binomial / Latin
            # genus / chemical suffix) — the categories where embedding
            # bridges genuinely fail.
            if not _looks_specialty(en_str):
                continue
            # Per-source greedy keep (shortest src wins for the same EN
            # canonical, in case the same concept has multiple JPN
            # preferred terms across MSHJPN/MDRJPN).
            existing = out.get(src_str)
            if existing is None or len(existing) > len(en_str):
                out[src_str] = en_str
    log.info("MRCONSO derivation [%s]: %d (jpn, eng) pairs after shape filters",
             lang, len(out))
    return out


# Latin-scientific specificity markers for the UMLS pre-filter. Tight
# patterns by design — keeping the lexicon focused on the rare-Latin
# species / drug-INN categories where the embedding bridge to JPN is
# genuinely weak. Common multi-word medical English (``Influenza A
# virus``, ``Myocardial infarction``) is intentionally NOT matched
# here: Qwen / Gemini bridge those fluently to JPN already, and
# including them would just inflate the embedding cost during build
# (17k → 5k pairs after this tightening) without runtime benefit.
_LATIN_BINOMIAL_RE = re.compile(r"^[A-Z][a-z]+ [a-z]{4,}(?: [a-z]+)?$")
_LATIN_GENUS_ALONE_RE = re.compile(
    r"^[A-Z][a-z]+("
    # Bacterial / fungal genus-suffix morphology — terms ending in
    # these morphemes are vanishingly rare in everyday English so the
    # JPN-EN bridge through pretraining text is unreliable.
    r"coccus|bacillus|monas|bacter|myces|spora|phyton|ella|phila"
    r"|spirillum|spirochaete|plasma|trichum|trichia"
    r")$"
)
# Drug INN suffixes — common monoclonal-antibody / kinase-inhibitor /
# protease-inhibitor / etc. naming endings standardized by WHO INN.
_DRUG_SUFFIX_RE = re.compile(
    r"^[A-Z][a-z]+("
    r"mab|tinib|navir|cycline|mycin|sartan|prazole|"
    r"dipine|gliptin|gliflozin|olol|caine"
    r")$"
)


def _looks_specialty(en: str) -> bool:
    """True when *en* looks like a specialty / Latin-scientific term —
    the categories where embedding bridges to JPN are weak. False for
    common medical English (Hypertension, Myocardial infarction, Fever,
    Influenza A virus) which Qwen / Gemini bridge fluently.
    """
    if _LATIN_BINOMIAL_RE.match(en):
        return True
    if _LATIN_GENUS_ALONE_RE.match(en):
        return True
    if _DRUG_SUFFIX_RE.match(en):
        return True
    return False


async def build_lexicon(
    loinc_dir: str,
    lang: str,
    cosine_threshold: float = 0.70,
    *,
    mrconso_path: str | None = None,
) -> dict[str, str]:
    """Full lexicon build for one language. See module docstring for
    the per-stage rationale.

    For languages in :data:`_UMLS_FALLBACK_LANGS` (currently ``ja``),
    sources from UMLS MRCONSO instead of a LOINC LinguisticVariant CSV.
    Pass *mrconso_path* explicitly or set the env var or pass via CLI.
    """
    if lang in _UMLS_FALLBACK_LANGS:
        if mrconso_path is None or not os.path.isfile(mrconso_path):
            raise FileNotFoundError(
                f"--mrconso required for lang={lang!r} (LOINC ships no "
                f"{lang}-LinguisticVariant; we derive from UMLS instead). "
                f"Got mrconso_path={mrconso_path!r}"
            )
        base = _derive_pairs_from_umls(
            mrconso_path, lang, _UMLS_FALLBACK_LANGS[lang],
        )
        log.info("lexicon build [%s]: %d raw pairs from UMLS", lang, len(base))
    else:
        loinc_csv = os.path.join(loinc_dir, "LoincTable", "Loinc.csv")
        if not os.path.isfile(loinc_csv):
            raise FileNotFoundError(loinc_csv)
        ling_csv = _find_linguistic_variant(loinc_dir, lang)
        if ling_csv is None:
            raise FileNotFoundError(
                f"no LinguisticVariant CSV for lang={lang!r} in {loinc_dir}"
            )
        base = _derive_pairs_from_loinc(loinc_csv, ling_csv, lang)
    # Merge derived genus aliases into the species lexicon BEFORE the
    # cosine filter — the genus derivation works off species-level
    # pairs, then we score everything together.
    for k, v in _derive_genus_aliases(base).items():
        base.setdefault(k, v)
    log.info("lexicon build [%s]: %d raw pairs (species + derived genus)", lang, len(base))

    filtered = await _filter_by_cosine(base, threshold=cosine_threshold)
    cleaned = _strip_noise(filtered)

    # Script-variant expansion: for zh, mirror every Simplified key to
    # its Traditional form so a query in either script hits the same EN
    # canonical. Cheap (~1ms via zhconv) and avoids maintaining a second
    # zh-Hant.tsv member that's 99% redundant with zh.tsv.
    if lang == "zh":
        cleaned = _expand_zh_traditional(cleaned)

    log.info(
        "lexicon build [%s]: final %d auto-derived entries (Trad-expanded if zh)",
        lang, len(cleaned),
    )
    # Curated layer is stored separately in ``aliases/{lang}_curated.tsv``
    # — a bundle member that this build never touches, so manual
    # additions survive every refresh. ``load_all_aliases`` unions all
    # ``aliases/*.tsv`` members at runtime, so callers see one merged
    # dict regardless of how many TSVs back it.
    return cleaned


def _expand_zh_traditional(lex: dict[str, str]) -> dict[str, str]:
    """Add Traditional Chinese variants for every Simplified key.

    Uses ``zhconv`` (lightweight, pure-Python, table-based) for the
    s2t conversion. Idempotent — if a key already matches its
    Traditional form (no characters that need conversion, e.g.
    ``白色念珠菌``), no duplicate is added. The Traditional variant
    points to the SAME English canonical, so the runtime regex picks
    up either form without a behavior split.
    """
    try:
        import zhconv  # noqa: PLC0415
    except ImportError:
        log.warning(
            "zhconv not installed; skipping zh Traditional expansion. "
            "Install via `pip install zhconv` and rerun for Trad coverage."
        )
        return lex
    out = dict(lex)
    added = 0
    for src, dst in list(lex.items()):
        trad = zhconv.convert(src, "zh-tw")
        if trad != src and trad not in out:
            out[trad] = dst
            added += 1
    log.info("zh Traditional expansion: +%d keys (from %d Simplified)",
             added, len(lex))
    return out


# ── CLI handler ───────────────────────────────────────────────────────


async def cmd_loinc_lexicon(args: argparse.Namespace) -> None:
    """Subcommand: ``loinc-lexicon`` — build & write ``aliases/{lang}.tsv``
    into ``fhir_loinc_bundle.tar.gz`` AND keep a loose copy at
    ``mirobody/res/aliases_src/{lang}.tsv`` for git-diff review.

    LOINC LinguisticVariant path for zh / ko / de / etc.; UMLS MRCONSO
    fallback for languages without a LOINC translation (currently ja).
    """
    res_dir = args.res_dir or RES_DIR
    bundle_path = os.path.join(res_dir, BUNDLE_BASENAME)
    needs_loinc = args.lang not in _UMLS_FALLBACK_LANGS
    if needs_loinc and (not args.loinc_dir or not os.path.isdir(args.loinc_dir)):
        raise SystemExit(
            f"--loinc-dir required for lang={args.lang!r} (got {args.loinc_dir!r}); "
            "needs LoincTable/Loinc.csv and AccessoryFiles/LinguisticVariants/"
        )

    lex = await build_lexicon(
        args.loinc_dir, args.lang, args.cosine_threshold,
        mrconso_path=args.mrconso,
    )
    payload = _encode_tsv(lex)

    # Loose copy on disk — same content as the bundle member, surfaces
    # cleanly in `git diff` so reviewers can see entry-level changes.
    src_dir = os.path.join(res_dir, "aliases_src")
    os.makedirs(src_dir, exist_ok=True)
    src_path = os.path.join(src_dir, f"{args.lang}.tsv")
    with open(src_path, "wb") as f:
        f.write(payload)
    log.info("wrote %s (%d entries, %.1f KiB)", src_path, len(lex), len(payload) / 1024)

    # Bundle member for production loading.
    write_member(_member_name(args.lang), payload, bundle_path=bundle_path)
