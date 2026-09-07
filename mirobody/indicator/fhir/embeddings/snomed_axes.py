"""Build per-language SNOMED CT alias TSVs from UMLS MRCONSO.

Subcommand:

  ``snomed-axis-aliases``  reads ``$UMLS_DIR`` (default ``~/ref/umls-*``)
                           and writes one TSV per non-English language to
                           ``mirobody/res/snomed_axes/aliases/{lang}.tsv``
                           — header ``concept_id\\talias``, one row per
                           ``(SCTID, alias)`` pair. Languages match the
                           LOINC alias bundle: zh · ja · ko · fr · es ·
                           ru · de. Empty per-language tables are
                           skipped (no zero-row file written) so the
                           directory listing reflects real coverage.

English isn't dumped — SNOMED's own English FSN already lives in
``snomed_axes/<tag>.tsv`` and is what we embed via the canonical
description path.

Algorithm (two-pass over MRCONSO.RRF):

  Pass 1 — build the UMLS CUI ↔ SCTID bridge from SNOMEDCT_US/VET rows
  (``SUPPRESS`` ``O``/``E`` filtered out). Most CUIs map to exactly one
  SCTID; rare multi-SCTID CUIs (cross-edition duplicates) are kept as
  sets. National-extension SABs (SCTSPA, SCTGER_AE, …) aren't bridged
  here — their SCUIs ARE SCTIDs but always for concepts that already
  appear in the US/VET release via shared CUI. Pass 2 still picks up
  their translations.

  Pass 2 — for every non-English non-suppressed row whose CUI sits in
  the bridge, emit ``(SCTID, LANG, STR)``. Captures both direct
  national-edition extensions AND indirect translations via other
  vocabularies that share the CUI (LOINC linguistic variants, ICD-10
  national codings, MeSH translations, …).

Chinese coverage caveat:

  UMLS MRCONSO carries very few ``LAT=CHI`` rows whose CUIs map to
  SNOMED concepts — empirically the output is empty. SNOMED
  International ships a Chinese-language reference set under a separate
  affiliate license that this builder doesn't ingest. Until that source
  is wired in, ``zh.tsv`` is skipped at write time (rather than written
  as a misleading header-only file). Downstream callers that need
  SNOMED-zh aliases should fall back to the LOINC alias bundle's
  ``aliases/zh.tsv`` (zhCN5 + Traditional curated) for the lab-test
  subset where LOINC and SNOMED concepts overlap by CUI.

Filters:
  * ``SUPPRESS`` ``O`` (obsolete) and ``E`` (editorial removal) are
    dropped in BOTH passes; ``Y`` (low-precision synonym) is kept.
  * ``STR`` empty / all-whitespace is dropped.

Output is deduped per ``(lang, sctid, alias)`` triple. Within one
SCTID, aliases are sorted lexicographically for stable diffs.

Cost: ~2.2 GB ``MRCONSO.RRF`` read twice, ~1-2 min total on local SSD.
Memory: ~150 MB peak (CUI→SCTID bridge + per-lang alias maps).
"""
from __future__ import annotations

import logging
import os
from argparse import Namespace
from collections import defaultdict
from collections.abc import Iterator

from ..index import RES_DIR

log = logging.getLogger(__name__)


# MRCONSO ``LAT`` (3-letter ISO 639-2/B) → output slug, matching the
# LOINC alias bundle in ``fhir_loinc_bundle.tar.gz``.
_LANG_MAP: dict[str, str] = {
    "CHI": "zh",
    "JPN": "ja",
    "KOR": "ko",
    "FRE": "fr",
    "SPA": "es",
    "RUS": "ru",
    "GER": "de",
}

# SNOMED-rooted SABs whose ``SCUI`` is an SCTID. Used in pass 1 to build
# the CUI→SCTID bridge. National-extension SABs are intentionally NOT
# included — pass 2 picks up their translations via the shared CUI.
_SNOMED_SABS = frozenset({"SNOMEDCT_US", "SNOMEDCT_VET"})

# MRCONSO field offsets (0-indexed, per UMLS spec).
_F_CUI = 0
_F_LAT = 1
_F_SCUI = 9
_F_SAB = 11
_F_STR = 14
_F_SUPPRESS = 16
_MIN_FIELDS = _F_SUPPRESS + 1

_SUPPRESSED = frozenset({"O", "E"})


def _find_mrconso(umls_dir: str) -> str:
    direct = os.path.join(umls_dir, "META", "MRCONSO.RRF")
    if os.path.isfile(direct):
        return direct
    for root, _, files in os.walk(umls_dir):
        if "MRCONSO.RRF" in files:
            return os.path.join(root, "MRCONSO.RRF")
    raise FileNotFoundError(f"MRCONSO.RRF not found under {umls_dir!r}")


def _iter_mrconso_rows(mrconso: str) -> Iterator[list[str]]:
    """Yield non-suppressed MRCONSO rows as field lists.

    Centralizes the pipe split + bounds check + SUPPRESS filter so both
    passes share the same row gate. Rows shorter than ``_MIN_FIELDS``
    fields (corrupted / truncated) are skipped silently — MRCONSO is
    fixed-schema, this only protects against pathological input.
    """
    with open(mrconso, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("|")
            if len(parts) < _MIN_FIELDS:
                continue
            if parts[_F_SUPPRESS] in _SUPPRESSED:
                continue
            yield parts


def _pass1_cui_to_sctids(mrconso: str) -> dict[str, set[str]]:
    """Scan SNOMEDCT_US/VET rows → ``{CUI: {SCTID, ...}}``."""
    out: dict[str, set[str]] = defaultdict(set)
    n_rows = 0
    for parts in _iter_mrconso_rows(mrconso):
        n_rows += 1
        if parts[_F_SAB] not in _SNOMED_SABS:
            continue
        scui = parts[_F_SCUI]
        if scui:
            out[parts[_F_CUI]].add(scui)
    log.info("pass 1: scanned %s rows, %s CUIs bridged to SNOMED",
             f"{n_rows:,}", f"{len(out):,}")
    return dict(out)


def _pass2_collect_aliases(
    mrconso: str,
    cui_to_sctids: dict[str, set[str]],
) -> dict[str, dict[str, set[str]]]:
    """For each non-English row whose CUI is in *cui_to_sctids*, append
    its STR to ``out[lang_slug][sctid]``."""
    out: dict[str, dict[str, set[str]]] = {
        lang: defaultdict(set) for lang in _LANG_MAP.values()
    }
    n_rows = n_kept = 0
    for parts in _iter_mrconso_rows(mrconso):
        n_rows += 1
        slug = _LANG_MAP.get(parts[_F_LAT])
        if slug is None:
            continue
        sctids = cui_to_sctids.get(parts[_F_CUI])
        if not sctids:
            continue
        term = parts[_F_STR].strip()
        if not term:
            continue
        # MRCONSO STR is free text — defensively strip embedded tabs so
        # downstream TSV consumers don't see split-column corruption.
        term = term.replace("\t", " ")
        for sctid in sctids:
            out[slug][sctid].add(term)
        n_kept += 1
    log.info("pass 2: scanned %s rows, kept %s alias rows",
             f"{n_rows:,}", f"{n_kept:,}")
    return out


def _write_tsvs(out_dir: str, by_lang: dict[str, dict[str, set[str]]]) -> None:
    os.makedirs(out_dir, exist_ok=True)
    for lang in sorted(by_lang):
        by_sctid = by_lang[lang]
        path = os.path.join(out_dir, f"{lang}.tsv")
        if not by_sctid:
            # Skip empty languages so the directory listing reflects
            # real coverage. ``zh`` lands here under current UMLS.
            if os.path.isfile(path):
                os.remove(path)
                log.info("  %s.tsv: 0 rows — removed prior empty file", lang)
            else:
                log.info("  %s.tsv: 0 rows — skipped", lang)
            continue
        n_concepts = n_aliases = 0
        with open(path, "w", encoding="utf-8") as f:
            f.write("concept_id\talias\n")
            for sctid in sorted(by_sctid, key=int):
                terms = by_sctid[sctid]
                if not terms:
                    continue
                n_concepts += 1
                for t in sorted(terms):
                    f.write(f"{sctid}\t{t}\n")
                    n_aliases += 1
        log.info("  %s.tsv: %6d concepts · %7d alias rows",
                 lang, n_concepts, n_aliases)


def cmd_snomed_axis_aliases(args: Namespace) -> None:
    """Build per-language SNOMED alias TSVs from UMLS MRCONSO.

    Pure local computation — no DB or network. Synchronous; the central
    dispatcher in ``main.py`` calls it without an ``asyncio.run`` wrapper.
    """
    umls_dir = args.umls_dir
    if not umls_dir or not os.path.isdir(umls_dir):
        raise FileNotFoundError(
            f"UMLS release dir not found at {umls_dir!r}; "
            "pass --umls-dir or set MIROBODY_REF_DIR."
        )

    res_dir = args.res_dir or RES_DIR
    out_dir = os.path.join(res_dir, "snomed_axes", "aliases")

    mrconso = _find_mrconso(umls_dir)
    log.info("reading %s", mrconso)

    bridge = _pass1_cui_to_sctids(mrconso)
    by_lang = _pass2_collect_aliases(mrconso, bridge)

    log.info("writing to %s:", out_dir)
    _write_tsvs(out_dir, by_lang)
