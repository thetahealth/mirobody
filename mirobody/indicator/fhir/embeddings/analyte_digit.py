"""Chemical-name → numeric-subtype alias table inside ``fhir_loinc_bundle``.

When users query an analyte by its numeric shortname (``Vitamin B1`` /
``IGF-1`` / ``Vit K2``) but LOINC's canonical name uses the chemical /
biological form (``Thiamine`` / ``Insulin-like growth factor-I`` /
``Phytonadione``), the resolver's digit-aware family rerank in
:func:`mirobody.indicator.fhir.resolve.pipeline._loinc_picks_topk` can't
match the query digit against the row name. This module owns the table
that bridges the gap: each row maps a chemical name (regex match key)
to its numeric subtype. The pipeline applies the table inside
``_build_family_index`` so the row's ``row_digits`` and
``row_name_has_digit`` reflect the implicit subtype digit.

Two TSVs back the table, mirroring the existing ``aliases/*.tsv`` /
``aliases/{lang}_curated.tsv`` split:

  - ``analyte_digit.tsv`` — auto-derived. Built by ``cmd_analyte_digit``
    from LOINC ``Loinc.csv`` ``RELATEDNAMES2`` and SNOMED CT description
    snapshots (FSN + active synonyms, filtered to ``(substance)`` /
    ``(product)`` concepts). Regenerated on every build.

  - ``analyte_digit_curated.tsv`` — manually curated overlay. The build
    never touches this member, so hand additions / removals survive
    every refresh. Use it to:
      * fill misses both sources lack (``Folate ↔ B9`` — LOINC lists
        only ``Vit M``, SNOMED has no ``B9`` synonym for ``Folate``);
      * record subtypes with no LOINC concept (``Menaquinone ↔ K2`` so
        the query gets nulled rather than matching ``K1`` cosine-close);
      * record colloquial chemical names not in either source.

  - ``analyte_digit_blocklist.txt`` (in-bundle, optional) — auto-mined
    canonical names to drop from the auto layer. Used to suppress
    historical/deprecated mappings (``Adenine ↔ B4`` — B4 is no longer
    recognized as a vitamin; keeping it would map ``Vit B4`` queries to
    Adenine instead of returning null) and cross-reference noise
    (``Retinal nerve fiber layer ↔ A1`` — eye anatomy concept whose
    ``RELATEDNAMES2`` mentions ``Vit A1 aldehyde``). When absent, the
    builder uses :data:`_DEFAULT_BLOCKLIST`; when present, its lines
    REPLACE the default (each line one canonical name, ``#`` comments
    allowed). Curated overlay always overrides blocklist.

Build (``mirobody indicator analyte-digit --loinc-dir … --snomed-dir …``)
expects:
  - LOINC: ``{loinc-dir}/LoincTable/Loinc.csv`` (LOINC 2.82+ layout)
  - SNOMED: ``{snomed-dir}/Snapshot/Terminology/sct2_Description_*.txt``
    (International or Affiliate release)

Both flags are optional individually; the build uses whichever source
is present. Output is the merged table, written atomically to the
bundle, plus a loose copy at ``mirobody/res/analyte_digit_src/`` for
``git diff`` review.

Runtime loader (:func:`load_analyte_digit_aliases`) reads both TSVs
from the bundle and returns a merged ``{name: digit}`` dict. Curated
overlay wins on key collisions. Cached for the process lifetime.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
from collections import defaultdict
from functools import lru_cache

from .bundle import (
    BUNDLE_BASENAME,
    BUNDLE_PATH,
    read_member,
    write_member,
)
from ..index import RES_DIR

log = logging.getLogger(__name__)


# Bundle member names. Both live at the bundle root (not under
# ``aliases/``) so they're not picked up by :func:`load_all_aliases`,
# which scans ``aliases/*.tsv`` for the multilingual query-side
# augmentation lexicon — a different concept entirely.
_AUTO_MEMBER = "analyte_digit.tsv"
_CURATED_MEMBER = "analyte_digit_curated.tsv"
_BLOCKLIST_MEMBER = "analyte_digit_blocklist.txt"


# ── Build pipeline ────────────────────────────────────────────────────


# Mining patterns over LOINC ``RELATEDNAMES2`` and SNOMED CT synonyms.
# Two shapes:
#   - ``Vit X<digit>`` / ``Vit X-<digit>`` / ``Vit X <digit>``
#     where X is one Latin letter (A-K) for vitamin subtypes.
#   - ``IGF-<digit>`` / ``IGF<digit>`` / ``IGFBP-<digit>``
#     for insulin-like growth factor subfamilies.
# Both case-insensitive. Roman-numeral subtypes (IGF-I, IGF-II) come
# from canonical COMPONENT/FSN parsing, not these patterns.
_VITAMIN_RE = re.compile(
    r"\b(?:Vit|Vitamin)\s+([A-K])\s*-?\s*(\d{1,2})\b",
    re.IGNORECASE,
)
_IGF_RE = re.compile(
    r"\b(IGF|IGFBP)\s*-?\s*(\d{1,2})\b",
    re.IGNORECASE,
)


# Default blocklist of canonical names to drop from the auto layer.
# Replaced by ``analyte_digit_blocklist.txt`` in the bundle when present.
# Each entry is the lowercase canonical_name as it would appear in the
# auto TSV. Comments below each group explain the rejection reason; if
# you add new entries, follow the same format so the next maintainer
# can audit the rationale.
_DEFAULT_BLOCKLIST: frozenset[str] = frozenset({
    # Historical "Vit B4 = Adenine" naming. B4 (adenine) was removed
    # from the vitamin list decades ago, so a query ``Vit B4`` is
    # malformed clinically; mapping it to Adenine would silently mask
    # the error rather than null-out.
    "adenine",
    "adenine phosphoribosyltransferase",
    "flavin adenine dinucleotide",
    # "Vit K4 = Acetomenaphthone" — synthetic K analog dropped from
    # most pharmacopoeias; no current LOINC indicator uses it.
    "acetomenaphthone",
    # SNOMED's ``vitamin-K-epoxide reductase (warfarin-*)`` concepts
    # are GENE products, not vitamin K itself. Their FSN happens to
    # contain ``Vit K`` plus a 1/2 sensitivity marker.
    "vitamin-k-epoxide reductase (warfarin-insensitive)",
    "vitamin-k-epoxide reductase (warfarin-sensitive)",
    # Ophthalmology cross-references: ``Retinal nerve fiber layer``
    # etc. have ``Retinaldehyde; Vit A1 aldehyde`` in their LOINC
    # ``RELATEDNAMES2`` for indexing purposes. They are NOT Vitamin A
    # measurements.
    "retinal eye screening report",
    "retinal treatments",
    "retinoate",
    "retinoate esters",
    "retinene",
    # Adenine derivatives flagged via the B4 lineage — keep filtered
    # explicitly even though COMPONENT shape would let them through.
    "n6-methyl-adenine",
})


def _is_simple_component(comp: str) -> bool:
    """True iff *comp* is a single-analyte chemical name suitable for
    aliasing. Reject derived / panel / ratio / challenge concepts where
    the canonical name doesn't reduce to one analyte.
    """
    if not comp:
        return False
    if any(c in comp for c in "./&+^"):
        return False
    bare = re.sub(r"\([^)]*\)", "", comp).strip()
    if not bare or len(bare.split()) > 4:
        return False
    # Skip COMPONENTs that already carry a digit in their bare form —
    # the existing ``loinc_analyte_digits`` extractor handles those
    # without aliasing (``Hemoglobin A1c``, ``HPV 16``, ``CD4``).
    if re.search(r"\d", bare):
        return False
    return True


def _mine_loinc(loinc_csv_path: str) -> dict[str, dict[tuple[str, int], int]]:
    """Scan a LOINC ``Loinc.csv`` for ``(canonical, family, digit)``
    triples evidenced by ``RELATEDNAMES2`` patterns. Returns a nested
    dict: ``canonical → {(family, digit): row_count}``.

    Inactive LOINCs are skipped — deprecated rows still carry the
    aliases but emitting them locks downstream consumers to terminology
    LOINC has explicitly retired.
    """
    out: dict[str, dict[tuple[str, int], int]] = defaultdict(lambda: defaultdict(int))
    with open(loinc_csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("STATUS") != "ACTIVE":
                continue
            comp = (row.get("COMPONENT") or "").strip()
            related = row.get("RELATEDNAMES2") or ""
            if not related or not _is_simple_component(comp):
                continue
            canon = comp.lower()
            # Family letter (B/K/D/A/...) without "Vit " prefix —
            # compact TSV column.
            for m in _VITAMIN_RE.finditer(related):
                out[canon][(m.group(1).upper(), int(m.group(2)))] += 1
            for m in _IGF_RE.finditer(related):
                out[canon][(m.group(1).upper(), int(m.group(2)))] += 1
    return out


def _mine_snomed(snomed_desc_path: str) -> dict[str, dict[tuple[str, int], int]]:
    """Scan a SNOMED CT description snapshot for the same triples.

    Only concepts whose FSN ends in ``(substance)`` or ``(product)``
    qualify — those are the SNOMED hierarchies where chemical /
    biological analyte concepts live. Other axes
    (``(observable entity)`` / ``(procedure)`` / ``(disorder)``)
    produce noise (``Deficiency of vitamin K2`` matches but the
    canonical isn't a substance name).
    """
    concept_terms: dict[str, list[tuple[str, str]]] = defaultdict(list)
    with open(snomed_desc_path, encoding="utf-8") as f:
        next(f)  # header
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 9:
                continue
            # cols: id, effectiveTime, active, moduleId, conceptId,
            #       languageCode, typeId, term, caseSignificanceId
            active = parts[2]
            concept_id = parts[4]
            type_id = parts[6]
            term = parts[7]
            if active != "1":
                continue
            concept_terms[concept_id].append((term, type_id))

    FSN_TYPE_ID = "900000000000003001"

    out: dict[str, dict[tuple[str, int], int]] = defaultdict(lambda: defaultdict(int))
    for cid, terms in concept_terms.items():
        fsn = next((t for t, ty in terms if ty == FSN_TYPE_ID), None)
        if not fsn:
            continue
        if not (fsn.endswith("(substance)") or fsn.endswith("(product)")):
            continue
        canonical = re.sub(r"\s*\([^)]*\)\s*$", "", fsn).strip()
        if not _is_simple_component(canonical):
            continue
        canon = canonical.lower()
        for term, _ty in terms:
            for m in _VITAMIN_RE.finditer(term):
                out[canon][(m.group(1).upper(), int(m.group(2)))] += 1
            for m in _IGF_RE.finditer(term):
                out[canon][(m.group(1).upper(), int(m.group(2)))] += 1
    return out


def _merge_mined(
    loinc: dict[str, dict[tuple[str, int], int]],
    snomed: dict[str, dict[tuple[str, int], int]],
) -> dict[str, tuple[str, int, str]]:
    """Union two mining outputs, prefer the highest-evidence (family,
    digit) per canonical, record provenance.

    Returns: canonical → (family, digit, source) where source is one
    of ``loinc``, ``snomed``, ``loinc+snomed``.
    """
    all_canons = set(loinc) | set(snomed)
    merged: dict[str, tuple[str, int, str]] = {}
    for canon in all_canons:
        l_map = loinc.get(canon, {})
        s_map = snomed.get(canon, {})
        combined: dict[tuple[str, int], int] = defaultdict(int)
        for k, v in l_map.items():
            combined[k] += v
        for k, v in s_map.items():
            combined[k] += v
        if not combined:
            continue
        (fam, digit), _ = max(combined.items(), key=lambda kv: kv[1])
        in_l = (fam, digit) in l_map
        in_s = (fam, digit) in s_map
        src = "loinc+snomed" if (in_l and in_s) else ("loinc" if in_l else "snomed")
        merged[canon] = (fam, digit, src)
    return merged


def _load_blocklist(bundle_path: str | None = None) -> frozenset[str]:
    """Read ``analyte_digit_blocklist.txt`` from the bundle if present,
    falling back to :data:`_DEFAULT_BLOCKLIST` when missing.
    """
    raw = read_member(_BLOCKLIST_MEMBER, bundle_path=bundle_path)
    if raw is None:
        return _DEFAULT_BLOCKLIST
    entries: set[str] = set()
    for line in raw.decode("utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        entries.add(line.lower())
    return frozenset(entries)


def _decode_overlay_tsv(raw: bytes) -> dict[str, tuple[str, int, str]]:
    """Parse a curated TSV (one ``name\\tdigit\\tfamily`` row per line).
    Returns the same shape as the auto layer with ``source='curated'``.
    Ignores blank lines and ``#`` comment lines.
    """
    out: dict[str, tuple[str, int, str]] = {}
    for ln in raw.decode("utf-8").splitlines():
        ln = ln.rstrip("\r")
        if not ln or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        if len(parts) < 3:
            continue
        name, digit_s, fam = parts[0].strip().lower(), parts[1].strip(), parts[2].strip()
        if not name or not digit_s.isdigit() or not fam:
            continue
        out[name] = (fam.upper(), int(digit_s), "curated")
    return out


def _encode_tsv(entries: dict[str, tuple[str, int, str]]) -> bytes:
    """Stable, sortable TSV: family-letter then digit then canonical.
    Header row first so reviewers reading the bundle member can orient
    without referencing this docstring.
    """
    rows = sorted(
        entries.items(),
        key=lambda kv: (kv[1][0], kv[1][1], kv[0]),
    )
    buf = io.StringIO()
    buf.write("# name\tdigit\tfamily\tsource\n")
    for name, (fam, digit, src) in rows:
        buf.write(f"{name}\t{digit}\t{fam}\t{src}\n")
    return buf.getvalue().encode("utf-8")


def _decode_auto_tsv(raw: bytes) -> dict[str, tuple[str, int, str]]:
    """Inverse of :func:`_encode_tsv`. Skips comment / blank lines."""
    out: dict[str, tuple[str, int, str]] = {}
    for ln in raw.decode("utf-8").splitlines():
        ln = ln.rstrip("\r")
        if not ln or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        if len(parts) < 4:
            continue
        name, digit_s, fam, src = (p.strip() for p in parts[:4])
        if not name or not digit_s.isdigit() or not fam:
            continue
        out[name.lower()] = (fam.upper(), int(digit_s), src)
    return out


def build_analyte_digit_table(
    loinc_csv_path: str | None,
    snomed_desc_path: str | None,
    *,
    bundle_path: str | None = None,
) -> dict[str, tuple[str, int, str]]:
    """Build the auto-mined alias table from LOINC and/or SNOMED.

    Either *loinc_csv_path* or *snomed_desc_path* (or both) must be
    provided. Applies :data:`_DEFAULT_BLOCKLIST` (or the bundle's
    blocklist file when present) to suppress historical / noise
    entries before returning.
    """
    if not loinc_csv_path and not snomed_desc_path:
        raise ValueError("at least one of loinc_csv_path / snomed_desc_path required")
    loinc_mined = _mine_loinc(loinc_csv_path) if loinc_csv_path else {}
    snomed_mined = _mine_snomed(snomed_desc_path) if snomed_desc_path else {}
    merged = _merge_mined(loinc_mined, snomed_mined)
    blocklist = _load_blocklist(bundle_path=bundle_path)
    return {k: v for k, v in merged.items() if k not in blocklist}


# ── Runtime loader ────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def load_analyte_digit_aliases(
    bundle_path: str | None = None,
) -> dict[str, int]:
    """Return the merged auto + curated ``{name: digit}`` dict for the
    runtime resolver. Curated overlay wins on key collision.

    Cached for the process lifetime — the bundle is read-only at run
    time. Returns an empty dict if neither bundle member is present
    (runtime falls back to no aliasing, which is safe — the digit-null
    guard simply admits the cosine top-1 for families that lack
    digit-bearing members).
    """
    path = bundle_path or BUNDLE_PATH
    out: dict[str, int] = {}
    auto = read_member(_AUTO_MEMBER, bundle_path=path)
    if auto is not None:
        for name, (_fam, digit, _src) in _decode_auto_tsv(auto).items():
            out[name] = digit
    curated = read_member(_CURATED_MEMBER, bundle_path=path)
    if curated is not None:
        for name, (_fam, digit, _src) in _decode_overlay_tsv(curated).items():
            out[name] = digit
    return out


# ── CLI handler ───────────────────────────────────────────────────────


def cmd_analyte_digit(args: argparse.Namespace) -> None:
    """Subcommand: ``analyte-digit`` — build & write the analyte-digit
    alias TSVs into ``fhir_loinc_bundle.tar.gz``. Mines LOINC
    ``RELATEDNAMES2`` and SNOMED CT ``(substance|product)`` synonyms,
    merges with blocklist, writes ``analyte_digit.tsv`` to the bundle
    AND a loose copy at ``mirobody/res/analyte_digit_src/`` for
    ``git diff`` review.

    Curated overlay (``analyte_digit_curated.tsv``) is never written by
    this command — author it by hand and either ``tar`` it into the
    bundle directly or ship the loose copy as part of release tooling.
    """
    res_dir = args.res_dir or RES_DIR
    bundle_path = os.path.join(res_dir, BUNDLE_BASENAME)

    loinc_csv = None
    if args.loinc_dir:
        loinc_csv = os.path.join(args.loinc_dir, "LoincTable", "Loinc.csv")
        if not os.path.isfile(loinc_csv):
            raise SystemExit(f"--loinc-dir: Loinc.csv not found at {loinc_csv!r}")

    snomed_desc = None
    if args.snomed_dir:
        # SNOMED CT description Snapshot, US English. Match the
        # release date suffix dynamically — affiliate releases stamp
        # filenames with YYYYMMDD that the build shouldn't hard-code.
        desc_dir = os.path.join(args.snomed_dir, "Snapshot", "Terminology")
        if not os.path.isdir(desc_dir):
            raise SystemExit(
                f"--snomed-dir: Snapshot/Terminology not found at {desc_dir!r}"
            )
        candidates = sorted(
            f for f in os.listdir(desc_dir)
            if f.startswith("sct2_Description_Snapshot-en_") and f.endswith(".txt")
        )
        if not candidates:
            raise SystemExit(
                f"--snomed-dir: no sct2_Description_Snapshot-en_*.txt under {desc_dir!r}"
            )
        snomed_desc = os.path.join(desc_dir, candidates[-1])

    if not loinc_csv and not snomed_desc:
        raise SystemExit("must pass --loinc-dir, --snomed-dir, or both")

    log.info(
        "analyte-digit build: loinc=%s, snomed=%s",
        loinc_csv or "(skip)", snomed_desc or "(skip)",
    )
    table = build_analyte_digit_table(
        loinc_csv, snomed_desc, bundle_path=bundle_path,
    )
    payload = _encode_tsv(table)

    # Loose copy for git review.
    src_dir = os.path.join(res_dir, "analyte_digit_src")
    os.makedirs(src_dir, exist_ok=True)
    src_path = os.path.join(src_dir, "analyte_digit.tsv")
    with open(src_path, "wb") as f:
        f.write(payload)
    log.info("loose copy → %s", src_path)

    write_member(_AUTO_MEMBER, payload, bundle_path=bundle_path)
    log.info(
        "analyte-digit build complete: %d entries, %d bytes → %s/%s",
        len(table), len(payload), bundle_path, _AUTO_MEMBER,
    )

    # Sync the curated overlay from the loose copy when present. The
    # loose ``mirobody/res/analyte_digit_src/analyte_digit_curated.tsv``
    # is the source of truth for hand additions (so they show up in
    # ``git diff``); this step copies it verbatim into the bundle so
    # the runtime loader sees it. No-op when the loose file is absent —
    # the bundle's existing curated member (if any) is preserved.
    curated_loose = os.path.join(src_dir, _CURATED_MEMBER)
    if os.path.isfile(curated_loose):
        with open(curated_loose, "rb") as f:
            curated_bytes = f.read()
        write_member(_CURATED_MEMBER, curated_bytes, bundle_path=bundle_path)
        log.info(
            "curated overlay synced from %s (%d bytes)",
            curated_loose, len(curated_bytes),
        )
    else:
        log.info(
            "curated overlay loose copy not found at %s; bundle's "
            "existing %s member (if any) preserved",
            curated_loose, _CURATED_MEMBER,
        )

    # Print a short summary so the reviewer can sanity-check at the
    # console without opening the TSV.
    if args.verbose:
        for name, (fam, digit, src) in sorted(
            table.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[0])
        ):
            print(f"  {name:42s} → {fam}{digit:<3d}  ({src})")


__all__ = [
    "build_analyte_digit_table",
    "load_analyte_digit_aliases",
    "cmd_analyte_digit",
]
