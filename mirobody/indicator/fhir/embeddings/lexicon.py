"""Per-language CN-→canonical-EN lexicons inside ``fhir_loinc_bundle.tar.gz``.

The embedding model bridges common-language ↔ Latin well for some of
medical vocabulary but fails unpredictably on specialist Latin binomial
names that rarely co-occur in pretraining text — e.g. 出芽短梗霉 ↔
*Aureobasidium pullulans*, the species name that LOINC writes in its
English COMPONENT but no Qwen/Gemini bridge recovers from the literal
CJK calque. Subtle sibling-cosine confusions (FEV1/FVC vs FEV1/FEV
total at ~0.002 cosine gap) further mean we can't trust the embedder
on bare CJK queries. The query-side augmentation appends the canonical
LOINC English form to such queries before embedding; this module owns
the dict it appends from.

Bundle layout — one TSV per language, no overlay carve-out::

    aliases/zh.tsv     deterministic union of LOINC zhCN5 + curated
    aliases/ja.tsv     UMLS MRCONSO MSHJPN/MDRJPN derivation + curated
    aliases/ko.tsv     LOINC koKR13 + curated
    aliases/de.tsv …   LOINC {lang}LinguisticVariant + curated

Each TSV has two tab-separated columns, no header, UTF-8:

    src \t dst

…where ``src`` is a non-English clinical token (Chinese, Japanese, …)
and ``dst`` is the canonical LOINC English (often a Latin binomial, or
"<full name> <ACRONYM>" for lab abbreviations). The augment direction
is strictly **foreign → English**: ASCII-only keys are rejected at
build time so the runtime regex never lifts EN-only queries.

Why TSV and not JSON: per-line diffs in git survive added/removed
entries cleanly, and ``cat aliases/zh.tsv | grep 烟曲霉`` is the
fastest way to spot-check provenance during development.

The build is **deterministic** — no embedding-cosine step. Two
versioned inputs feed the merge, both authoritative across embedding
providers / model versions:

  - ``{loinc_dir}/AccessoryFiles/LinguisticVariants/{lang}…CSV`` —
    LOINC's official per-language translation table.
  - ``mirobody/res/aliases_src/{lang}_curated.tsv`` — hand-edited
    colloquial / abbreviation entries that LOINC's literal translation
    doesn't emit (绿脓杆菌 vs LOINC's 铜绿假单胞菌; FEV1/FVC for 1秒率).

Build pipeline (per language, all stages pure-string-rule):

  1. Read LOINC main ``Loinc.csv`` + ``{lang}LinguisticVariant.csv``.
     **All active rows, all CLASSes** — embedding noise from auto-
     covered concepts has zero cost once the cosine filter is gone, and
     covering CHEM/HEM/PULM removes the per-class blind spots that the
     prior MICRO/ALLERGY/DRUG/TOX-only path produced.
  2. **COMPONENT pass** — for each row, dot-segment both COMPONENT
     fields and strip a trailing-axis token (抗体 / DNA / IgG / 总计 / …
     on the CN side; Ab / DNA / IgG / total on the EN side). The
     remainder is the species/analyte phrase. Shape filter requires
     pure-script keys (the per-language script regex), so mixed-
     script entries like ``Alpha 酮戊二酸`` are rejected here — keeping
     ``Alpha`` as an augment key would mis-bridge English queries.
  3. **RELATEDNAMES2 pass** (non-Latin-script langs only: zh/ko/ru) —
     recover the pure-script aliases LOINC ships in RELATEDNAMES2 for
     mixed-script COMPONENT segments. For ``Alpha 酮戊二酸`` segment,
     RN2 carries ``2-酮戊二酸; α-酮戊二酸; α酮戊二酸; α-酮戊二酸根; …``
     which all strip to the pure-CJK core ``酮戊二酸`` → emit pair
     against the EN segment ``Alpha ketoglutarate``. Matching is by
     core-substring containment (≥3 script chars), not by RN2 group
     position — RN2's whitespace-group boundaries aren't consistently
     axis-aligned across rows.
  4. Keep only no-whitespace CN phrases of 2–8 CJK chars and English
     phrases of ≤ 5 tokens. Aggregate by CN phrase; keep when one EN
     covers ≥ 55% of the occurrences (deterministic alphabetical tie-
     break on equal counts), or single attestation for 3+ char CN —
     Latin binomials are reliable even at df=1.
  5. Derive 2-3 CJK-char genus suffixes from accumulated species
     entries (链球菌 → Streptococcus, 葡萄球菌 → Staphylococcus).
  6. Filter LOINC-jargon noise (X多个未知种 = ``spp.`` calque, bare
     "X型" axis modifiers, 2-char genus-suffix fragments that are
     suffixes of longer species names) by pattern.
  7. Merge the per-language curated TSV — curated entries override
     auto on key collisions. Curated is also the only path for
     entries LOINC zhCN never carries (FEV1/FVC for 1秒率).
  8. zh-only: mirror every Simplified key to its Traditional form so
     a query in either script hits the same EN canonical.
  9. Direction check: drop identity pairs (src casefold == dst) — no
     embedding-augment signal there. For non-Latin-script langs
     (zh/ja/ko/ru/ar/el/uk), additionally reject ASCII-only keys to
     catch curated-input tagging mistakes. Latin-script langs allow
     ASCII-only keys (German ``Glukose`` → English ``Glucose``).
"""

from __future__ import annotations

import argparse
import logging
import os
import re
from collections import Counter, defaultdict

from mirobody._bundle import load_alias_sources

from .bundle import BUNDLE_BASENAME, BUNDLE_PATH, read_member
from .local import RES_DIR

log = logging.getLogger(__name__)



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


def load_all_aliases() -> dict[str, str]:
    """Union every ``res/aliases_src/*.tsv`` into one ``src -> canonical`` dict.

    Reads the same loose files the resolver reads, through the same precedence
    (curated ahead of machine-derived) — see
    :func:`mirobody._bundle.alias_source_files`. It used to read byte-identical
    copies stored as ``aliases/{lang}.tsv`` bundle members instead, and those
    had drifted: four rows added to ``zh_curated.tsv`` were live for the
    resolver and invisible here. One reader, one source.

    Keys are the surfaces as written — no fold — because the build passes need
    them that way. Cross-language key collisions (one CJK term in zh.tsv and
    ja.tsv) resolve by filename order and are vanishingly rare.
    """
    return load_alias_sources(include_overrides=False)




# ── Build pipeline ────────────────────────────────────────────────────


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


# Per-language "core script" regex for RELATEDNAMES2 mining (see
# :func:`_mine_rn2_pairs`). Only non-Latin-script langs are listed:
# de/fr/es/pt write COMPONENT in pure Latin, so there's no mixed-script
# alias-recovery opportunity in their RN2.
_CORE_SCRIPT_BY_LANG: dict[str, re.Pattern] = {
    "zh": re.compile(r"[一-鿿]+"),
    "ko": re.compile(r"[가-힯]+"),
    "ru": re.compile(r"[А-Яа-яЁё]+"),
}

# Minimum length of the COMPONENT-segment core script substring required
# to trust an RN2-mined alias. Two-char cores like ``抗体`` (antibody) or
# ``抗原`` (antigen) are too generic — they'd mis-bridge any 抗体 query
# to the specific compound the row encodes. Three-char minimum keeps
# Greek-prefix compounds (``酮戊二酸`` — 4 chars) and excludes the
# generic axis-modifier 2-char fragments.
_RN2_MIN_CORE = 3


# ── Positional-isomer mining (Greek prefix + optional digit) ──────────


# LOINC zhCN writes positional isomers as mixed-script COMPONENTs:
# ``Beta 丙氨酸`` (Beta alanine), ``Alpha 1 微球蛋白`` (Alpha-1 microglobulin),
# ``Alpha 酮戊二酸`` (Alpha ketoglutarate). The user-side colloquial form
# uses Greek letters (``β-丙氨酸``, ``α1-微球蛋白``). Without a position-
# aware bridge, naive bare-CJK extraction would emit ``丙氨酸 → Beta
# alanine`` (because the Beta row's RN2 attests ``丙氨酸`` many times,
# overpowering the un-prefixed ``Alanine`` row's single attestation in
# majority vote) — a silent bug that pushes plain-``丙氨酸`` queries to
# the wrong isomer. The positional pipeline (parallel in spirit to
# :mod:`.analyte_digit` which bridges ``Vit B1 → Thiamine``) emits
# explicit position-marked keys instead.
_GREEK_WORD_TO_LETTER: dict[str, str] = {
    "Alpha": "α",
    "Beta":  "β",
    "Gamma": "γ",
    "Delta": "δ",
}

# Matches ``<GreekWord>[\s|-]+[<digit>[\s|-]+]<rest>``. LOINC uses
# both styles inconsistently across its English COMPONENT column:
# ``Beta alanine`` (space), ``Beta-2-Microglobulin`` (dashes),
# ``Alpha 1 antitrypsin`` (mixed). The zhCN translations mirror these.
# Both sides (zh COMPONENT and en COMPONENT) must independently match
# with identical greek+digit before we trust the row as positional —
# guards against zhCN translation glitches where the prefix is on one
# side but not the other.
_POSITION_DETECT_RE = re.compile(
    r"^(Alpha|Beta|Gamma|Delta)[\s\-]+(?:(\d+)[\s\-]+)?(.+)$"
)


def _detect_position_row(
    zh_seg: str, en_seg: str, src_tail: frozenset,
) -> tuple[str, "str | None", str, str] | None:
    """Return ``(greek_word, digit_or_None, zh_base, en_canonical)`` if
    *zh_seg* + *en_seg* form a positional-isomer pair, else ``None``.

    Both sides must carry the same Greek word and same digit (or both
    no digit). The zh base must contain at least 2 contiguous CJK chars
    after tail-axis stripping (``抗原`` / ``抗体`` / etc.) — otherwise it's
    an empty-translation glitch or a generic-axis-only segment. The EN
    canonical is the full *en_seg* (preserves LOINC's separator style:
    ``Beta-2-Microglobulin`` stays dashed; ``Beta alanine`` stays
    spaced) after tail-axis stripping.
    """
    m_zh = _POSITION_DETECT_RE.match(zh_seg)
    m_en = _POSITION_DETECT_RE.match(en_seg)
    if not (m_zh and m_en):
        return None
    if m_zh.group(1) != m_en.group(1):
        return None
    if (m_zh.group(2) or "") != (m_en.group(2) or ""):
        return None
    zh_base = _strip_tail((m_zh.group(3) or "").strip(), src_tail)
    en_canonical = _strip_tail(en_seg.strip(), _EN_TAIL_AXES)
    if not zh_base or not en_canonical:
        return None
    if not re.search(r"[一-鿿]{2,}", zh_base):
        return None
    return (m_zh.group(1), m_zh.group(2), zh_base, en_canonical)


# Separator variants between prefix-and-digit, digit-and-base, or
# prefix-and-base. Empty / dash / space all appear in real user queries.
_POSITION_SEPS = ("", "-", " ")


def _position_marked_aliases(
    greek_word: str, digit: "str | None", zh_base: str,
) -> list[str]:
    """All position-marked alias keys for a row. Covers Greek-letter
    (lowercase) + ``Alpha``/``alpha`` word forms × separator variants.

    No-digit row (``greek_word=Beta``, ``zh_base=丙氨酸``):

        β丙氨酸, β-丙氨酸, β 丙氨酸,
        Beta丙氨酸, Beta-丙氨酸, Beta 丙氨酸,
        beta丙氨酸, beta-丙氨酸, beta 丙氨酸

    Digit row (``greek_word=Alpha``, ``digit=1``, ``zh_base=微球蛋白``):
    3 prefix forms × 3 sep1 × 3 sep2 = 27 variants (deduped).

    Uppercase Greek letters (``Α``, ``Β``) and single-Latin-letter
    shorthand (``A-``, ``B-``) deliberately omitted — they collide
    visually with Latin and would require NFKC normalization to be
    safe.
    """
    letter = _GREEK_WORD_TO_LETTER[greek_word]
    prefixes = (letter, greek_word, greek_word.lower())
    out: list[str] = []
    seen: set[str] = set()
    if digit:
        for p in prefixes:
            for s1 in _POSITION_SEPS:
                for s2 in _POSITION_SEPS:
                    k = f"{p}{s1}{digit}{s2}{zh_base}"
                    if k not in seen:
                        seen.add(k)
                        out.append(k)
    else:
        for p in prefixes:
            for s in _POSITION_SEPS:
                k = f"{p}{s}{zh_base}"
                if k not in seen:
                    seen.add(k)
                    out.append(k)
    return out


def _strip_tail(seg: str, tail: frozenset[str]) -> str:
    toks = seg.split()
    while toks and toks[-1] in tail:
        toks.pop()
    return " ".join(toks)


def _dot_segments(s: str) -> list[str]:
    """Split a LOINC COMPONENT into per-analyte segments.

    Splits on ``.`` (axis-modifier separator: ``X.subtype.spec``),
    ``/`` (ratio numerator/denominator: ``X/Y``), AND ``^``
    (LOINC's sub-modifier separator: ``Substance^Challenge^
    Adjustment^DivisorSubstance`` within the COMPONENT axis — e.g.
    ``Alpha-1-Fetoprotein^^adjusted for weight`` or
    ``Alpha cortolone^2D post dose dexamethasone``).

    Without splitting on ``^``, the merge would emit composite
    entries like ``Alpha 1胎儿球蛋白^^经过针对体重而调整的 →
    Alpha-1-Fetoprotein^^adjusted for weight`` that no clinical
    query ever uses (the ``^`` glyph doesn't appear in CN clinical
    text) AND would inflate the alias dict by pairing wrong sub-
    parts when the auto-mining heuristic falls back to substring
    matching (``型抗体`` ↔ ``HIV 1+2 Ab+HIV1 p24`` style noise).
    Splitting on all three separators yields per-axis-segment
    aliases that match real query text.
    """
    return [seg.strip() for seg in re.split(r"[./^]", s) if seg.strip()]


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


def _mine_rn2_pairs(
    joined,
    lang: str,
    src_tail: frozenset,
    pass1_keys: "set[str] | None" = None,
) -> list[tuple[str, str]]:
    """Mine analyte aliases from LOINC LinguisticVariant ``RELATEDNAMES2``
    for mixed-script COMPONENT segments. Two sub-paths depending on
    segment shape:

    **Positional rows** (zh only — Greek-prefix convention is specific
    to LOINC zhCN): COMPONENT like ``Beta 丙氨酸`` / ``Alpha 1 微球蛋白``
    where both sides start with ``Alpha|Beta|Gamma|Delta``. Emits
    position-MARKED variants (``β-丙氨酸``, ``Beta-丙氨酸``,
    ``α1-微球蛋白``, …) keyed against the EN canonical of THIS row.
    Bare-CJK base is NOT emitted here — it'd collide with the
    un-prefixed sibling LOINC row's canonical (``丙氨酸 → Alanine``
    from row 20636-7 vs ``丙氨酸 → Beta alanine`` from row 1932-3;
    naive bare-CJK mining picks the wrong one via majority vote since
    each positional row's RN2 carries many ``丙氨酸``-containing tokens).
    After all rows are processed, the bare CJK is emitted ONCE per base
    if and only if the base has exactly one positional canonical AND
    Pass 1 didn't already bridge it (``酮戊二酸`` is OK because LOINC
    has no un-prefixed canonical and only the Alpha form exists).

    **Non-positional mixed-script rows** (any lang with a
    :data:`_CORE_SCRIPT_BY_LANG` entry): COMPONENT like ``Anti-XX 抗体``
    where the mixed-script doesn't follow the positional pattern.
    Extracts the core-script run from each RN2 token and emits if it
    passes the per-language shape filter.

    Latin-script langs (de/fr/es/...) skip this entirely — their
    LOINC COMPONENTs are pure-Latin so there's no mixed-script
    recovery opportunity, and the core-script regex isn't defined
    for them. ``ja`` uses UMLS MRCONSO (different code path) and
    likewise doesn't invoke this.
    """
    core_re = _CORE_SCRIPT_BY_LANG.get(lang)
    if core_re is None:
        return []
    pass1_keys = pass1_keys or set()
    out: list[tuple[str, str]] = []
    # Track positional rows for the post-pass bare-CJK singleton
    # emission step.
    positional_base_canonicals: dict[str, set[str]] = defaultdict(set)

    for row in joined.iter_rows(named=True):
        rn2 = row.get("_RN2") or ""
        src_full = (row["_LING"] or "").strip()
        dst_full = (row["COMPONENT"] or "").strip()
        src_segs = _dot_segments(src_full)
        dst_segs = _dot_segments(dst_full)
        # RN2 uses both ``;`` and whitespace as token separators —
        # group boundaries (space) and intra-group separators (``;``)
        # both flatten to one token bag. We don't rely on group order
        # because it's inconsistent across rows; instead we filter by
        # core-substring containment.
        tokens = [t for t in re.split(r"[;\s]+", rn2) if t] if rn2 else []
        for i in range(min(len(src_segs), len(dst_segs))):
            src_seg = src_segs[i]
            dst_seg = dst_segs[i]

            # Positional-row path (zh only).
            if lang == "zh":
                pos = _detect_position_row(src_seg, dst_seg, src_tail)
                if pos is not None:
                    greek_word, digit, zh_base, en_canonical = pos
                    if len(en_canonical.split()) > 5:
                        continue
                    for alias in _position_marked_aliases(greek_word, digit, zh_base):
                        if 2 <= len(alias) <= 24:
                            out.append((alias, en_canonical))
                    positional_base_canonicals[zh_base].add(en_canonical)
                    continue  # Skip non-positional pass for this segment.

            # Non-positional mixed-script path.
            if not tokens:
                continue
            if not re.search(r"[A-Za-z0-9]", src_seg):
                continue  # Pure-script — main pass handles it.
            cores = core_re.findall(src_seg)
            if not cores:
                continue
            core = max(cores, key=len)
            if len(core) < _RN2_MIN_CORE:
                continue
            d = _strip_tail(dst_seg, _EN_TAIL_AXES)
            if not d or not re.search(r"[A-Za-z]{3,}", d) or len(d.split()) > 5:
                continue
            for tok in tokens:
                if core not in tok:
                    continue
                # Extract the script run containing core (handles
                # mixed-script tokens like ``Alpha 酮戊二酸根`` → ``酮戊二酸根``).
                cand = next(
                    (m for m in core_re.findall(tok) if core in m),
                    None,
                )
                if cand is None:
                    continue
                cand = _strip_tail(cand, src_tail)
                if _passes_src_shape(cand, lang):
                    out.append((cand, d))

    # Bare-CJK emission for positional singletons. Two guards: (1) Pass
    # 1 didn't already bridge this base (don't override the canonical
    # un-prefixed LOINC row); (2) only one EN canonical attests this
    # base across all positional rows (no Greek competition).
    n_singleton = 0
    for zh_base, canonicals in positional_base_canonicals.items():
        if zh_base in pass1_keys:
            continue
        if len(canonicals) != 1:
            continue
        out.append((zh_base, next(iter(canonicals))))
        n_singleton += 1
    if n_singleton:
        log.info(
            "lexicon build [%s]: %d positional bare-CJK singletons emitted",
            lang, n_singleton,
        )

    return out


def _derive_pairs_from_loinc(loinc_csv: str, ling_csv: str, lang: str) -> dict[str, str]:
    """Stage 1: aggregate (src, dst) pairs from LOINC main + per-
    language linguistic variant, axis-stripped and shape-filtered.

    All active LOINC rows are considered — no CLASS gate. The earlier
    MICRO/ALLERGY/DRUG/TOX-only filter assumed Gemini bridged CHEM /
    HEM / PULM reliably; in practice it doesn't (FEV1/FVC vs FEV1/FEV
    total siblings cosine within 0.002 even with a curated alias) so
    every class gets a chance to contribute auto-derived bridges.

    Two-pass mining:

      - Pass 1 walks dot-segmented COMPONENT and aligns segments to
        the EN COMPONENT — the existing path for pure-script entries.
      - Pass 2 (:func:`_mine_rn2_pairs`) recovers pure-script analyte
        aliases from RELATEDNAMES2 for mixed-script COMPONENT segments
        (``Alpha 酮戊二酸 → Alpha ketoglutarate`` via the ``α-酮戊二酸``
        alias in RN2). Skipped for Latin-script langs.
    """
    import polars as pl
    en = pl.read_csv(
        loinc_csv,
        columns=["LOINC_NUM", "COMPONENT", "STATUS"],
        infer_schema=False,
    ).filter(pl.col("STATUS") == "ACTIVE")
    zh = pl.read_csv(
        ling_csv,
        columns=["LOINC_NUM", "COMPONENT", "RELATEDNAMES2"],
        infer_schema=False,
    ).rename({"COMPONENT": "_LING", "RELATEDNAMES2": "_RN2"})
    joined = en.join(zh, on="LOINC_NUM", how="inner").filter(
        pl.col("_LING").is_not_null() & pl.col("COMPONENT").is_not_null()
    )
    log.info("lexicon build [%s]: %d active rows (all CLASSes)", lang, joined.height)

    src_tail = _TAIL_AXES_BY_LANG.get(lang, frozenset())
    pairs: list[tuple[str, str]] = []
    pass1_keys: set[str] = set()
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
            pass1_keys.add(s)
    n_main = len(pairs)

    # Pass 2: RELATEDNAMES2 mining for mixed-script COMPONENT segments.
    # ``pass1_keys`` lets the positional pipeline skip bare-CJK emission
    # for bases that Pass 1 already canonicalized — see the docstring on
    # :func:`_mine_rn2_pairs` for the ``丙氨酸`` example.
    rn2_pairs = _mine_rn2_pairs(joined, lang, src_tail, pass1_keys=pass1_keys)
    pairs.extend(rn2_pairs)
    log.info(
        "lexicon build [%s]: %d pairs from COMPONENT + %d from RELATEDNAMES2",
        lang, n_main, len(rn2_pairs),
    )

    agg: dict[str, Counter] = defaultdict(Counter)
    for s, d in pairs:
        agg[s][d] += 1
    out: dict[str, str] = {}
    for s, ctr in agg.items():
        total = sum(ctr.values())
        # Deterministic majority vote: sort by (-count, alphabetical
        # EN) so ties resolve the same way regardless of insertion
        # order. Counter.most_common preserves insertion order on ties,
        # which would make the output depend on LOINC row order.
        ranked = sorted(ctr.items(), key=lambda kv: (-kv[1], kv[0]))
        top_d, top_n = ranked[0]
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

    Skips entries whose ``dst`` starts with a Greek position prefix
    (``Alpha``/``Beta``/``Gamma``/``Delta``) — the first-word genus
    heuristic was designed for Latin binomials (``Genus species``) where
    ``dst.split()[0]`` is a meaningful taxon. For positional-isomer
    compounds (``Alpha-1-Acid glycoprotein``, ``Beta alanine``,
    ``Gamma aminobutyrate``) it returns the position prefix, which is
    semantically wrong as a genus name. Without this guard, ``Alpha-1-酸性糖蛋白``
    would seed ``糖蛋白 → Alpha-1-Acid`` (truncated, missing the actual
    analyte).
    """
    suf_genus: dict[str, Counter] = defaultdict(Counter)
    for src, dst in lex.items():
        if len(src) < 4:
            continue
        if _POSITION_DETECT_RE.match(dst):
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


def _load_curated_input(res_dir: str, lang: str) -> dict[str, str]:
    """Read ``aliases_src/{lang}_curated.tsv`` — the hand-edited source-
    of-truth for entries that supplement (or override) the auto-derive
    layer. Returns ``{}`` when the file is missing.

    Comments (``#`` prefix) and blank lines are stripped by
    :func:`_decode_tsv`. This is the only input besides the LOINC
    LinguisticVariant CSV — keeping it on disk under ``aliases_src/``
    means ``git diff`` surfaces every curated change for review.
    """
    path = os.path.join(res_dir, "aliases_src", f"{lang}_curated.tsv")
    if not os.path.isfile(path):
        return {}
    with open(path, "rb") as f:
        raw = f.read()
    out = _decode_tsv(raw)
    log.info("curated input [%s]: %d entries from %s", lang, len(out), path)
    return out


# Languages written in a non-Latin script — augment keys MUST contain a
# non-ASCII codepoint, because an ASCII-only key here means an EN-leak
# (e.g. ``EBV\tEpstein Barr virus`` mistakenly in ``zh_curated.tsv``).
# Latin-script langs (de/fr/es/pt/it/nl/cs/pl/tr/…) legitimately have
# ASCII-only keys for words like ``Calcium`` or ``Formule``; those get
# caught by the identity-pair check instead.
_NON_LATIN_SCRIPT_LANGS = frozenset({"ar", "el", "ja", "ko", "ru", "uk", "zh"})


def _enforce_direction(lex: dict[str, str], lang: str) -> dict[str, str]:
    """Direction filter — drop pairs that fail the foreign→EN contract.

    Two rules, applied universally:

    1. **Identity drop** — pairs where ``src.casefold() == dst.casefold()``
       contribute no signal at augment time (Gemini already bridges
       ``Calcium → Calcium`` without help). Without the old cosine
       filter these auto-derived no-ops would inflate the TSV;
       dropping them is a deterministic equivalent that doesn't depend
       on any embedding provider.

    2. **Script drop** (non-Latin-script langs only) — keys with no
       non-ASCII codepoint can't have come from a non-Latin source
       script and must be a tagging mistake in the curated input.
       Reject with a WARN so it surfaces in the build log.

    Latin-script langs skip rule (2) — German ``Glukose`` legitimately
    maps to English ``Glucose`` with no umlauts, and the per-language
    shape regex already requires the right Latin subset.
    """
    bad_identity = {k for k, v in lex.items() if k.casefold() == v.casefold()}
    bad_ascii: set[str] = set()
    if lang in _NON_LATIN_SCRIPT_LANGS:
        bad_ascii = {
            k for k in lex
            if k not in bad_identity and all(ord(c) < 128 for c in k)
        }
        if bad_ascii:
            log.warning(
                "[%s] dropping %d ASCII-only key(s) (non-Latin-script lang "
                "expects non-ASCII source): %s",
                lang, len(bad_ascii), sorted(bad_ascii)[:8],
            )
    drop = bad_identity | bad_ascii
    if bad_identity:
        log.info(
            "[%s] dropped %d identity pair(s) (src ≈ dst, augment no-op)",
            lang, len(bad_identity),
        )
    return {k: v for k, v in lex.items() if k not in drop}


def build_lexicon(
    loinc_dir: str,
    lang: str,
    res_dir: str,
    *,
    mrconso_path: str | None = None,
) -> dict[str, str]:
    """Full lexicon build for one language. See module docstring for
    the per-stage rationale. Deterministic — no embedding-API calls.

    For languages in :data:`_UMLS_FALLBACK_LANGS` (currently ``ja``),
    sources from UMLS MRCONSO instead of a LOINC LinguisticVariant CSV.
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
    # Genus derivation runs on auto pairs only (curated entries are
    # already authoritative endpoints; we don't want curated 2-3-char
    # additions accidentally minting derived genus rules).
    for k, v in _derive_genus_aliases(base).items():
        base.setdefault(k, v)
    log.info("lexicon build [%s]: %d raw pairs (species + derived genus)", lang, len(base))

    cleaned = _strip_noise(base)

    # Curated merge — curated wins on key collisions. ``dict.update``
    # gives the second arg the final word, so we update the AUTO dict
    # with curated entries.
    curated = _load_curated_input(res_dir, lang)
    overlap = len(set(cleaned) & set(curated))
    merged = dict(cleaned)
    merged.update(curated)
    log.info(
        "lexicon build [%s]: auto=%d + curated=%d (overlap=%d) → %d merged",
        lang, len(cleaned), len(curated), overlap, len(merged),
    )

    # Script-variant expansion: for zh, mirror every Simplified key
    # (auto + curated) to its Traditional form. Cheap (~1ms via zhconv)
    # and avoids maintaining a second zh-Hant.tsv member.
    if lang == "zh":
        merged = _expand_zh_traditional(merged)

    merged = _enforce_direction(merged, lang)
    log.info("lexicon build [%s]: final %d entries", lang, len(merged))
    return merged


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


def cmd_loinc_lexicon(args: argparse.Namespace) -> None:
    """Subcommand: ``loinc-lexicon`` — build & write ``aliases/{lang}.tsv``
    into ``fhir_loinc_bundle.tar.gz`` AND keep a loose copy at
    ``mirobody/res/aliases_src/{lang}.tsv`` for git-diff review.

    LOINC LinguisticVariant path for zh / ko / de / etc.; UMLS MRCONSO
    fallback for languages without a LOINC translation (currently ja).

    Synchronous: the build is pure string-rule, no embedding API calls.
    Purges any stale ``aliases/{lang}_curated.tsv`` bundle member after
    writing — curated entries now live in the merged main TSV.
    """
    res_dir = args.res_dir or RES_DIR
    bundle_path = os.path.join(res_dir, BUNDLE_BASENAME)
    needs_loinc = args.lang not in _UMLS_FALLBACK_LANGS
    if needs_loinc and (not args.loinc_dir or not os.path.isdir(args.loinc_dir)):
        raise SystemExit(
            f"--loinc-dir required for lang={args.lang!r} (got {args.loinc_dir!r}); "
            "needs LoincTable/Loinc.csv and AccessoryFiles/LinguisticVariants/"
        )

    lex = build_lexicon(
        args.loinc_dir, args.lang, res_dir,
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

    # No bundle member: `res/aliases_src/{lang}.tsv` above IS the artifact, and
    # writing it twice is what let the two copies drift. Everything that reads
    # aliases — the resolver and `load_all_aliases` — reads the loose files.
