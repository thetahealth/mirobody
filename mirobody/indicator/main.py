"""Entry point for the mirobody.indicator CLI.

Subcommands:
    siblings — Build sibling groups (_siblings_*.csv).
    bridge   — Build cross-vocabulary bridge files (_bridges_*.csv).
    merge    — Merge siblings + bridges into concepts.csv + fhir_concept_graph.bin.
    search   — Search concepts by keywords (requires DB).
    resolve  — Resolve free-text term to LOINC / RxNorm / SNOMED CT codes.
    normalize — Parse free-text 'value + unit' string into (comparator, value, UCUM unit).
    inspect  — Show concept-graph bridges/siblings for a (SYSTEM, CODE) node.
    loinc-skip — Build fhir_loinc_skip.npy mask (PHENX/SURVEY/DOC + DEPRECATED/DISCOURAGED excluded from resolve).
    loinc-rank — Build fhir_loinc_rank_bonus.npy soft-bonus (top-100 LOINCs +0.020, tail less; derived from COMMON_TEST_RANK).
    loinc-alias — Build fhir_alias_index.pkl multilingual lexical alias → row index (from LinguisticVariants + main RELATEDNAMES2).
    embed    — Batch-fill embedding_gemini for th_series_dim / fhir_indicators.

Build artifacts can be verified with:
    pytest tests/indicator/fhir/ --out-dir out/

Usage:
    python -m mirobody.indicator siblings -o out/ --loinc-dir ~/ref/Loinc_...
    python -m mirobody.indicator bridge   -o out/ --umls-dir  ~/ref/umls-...
    python -m mirobody.indicator merge    -o out/
    python -m mirobody.indicator search   -o out/ <user_id> <keywords...>
    python -m mirobody.indicator resolve  "blood glucose"
    python -m mirobody.indicator normalize "90次每分钟" "<5.6 mg/dL"
    python -m mirobody.indicator inspect  LOINC 65583-7

Required external data (default location: ~/ref/):
  UMLS Metathesaurus   — https://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html
                         Download "UMLS Metathesaurus Full Subset", extract to e.g. ~/ref/umls-2025AB/
                         Requires MRCONSO.RRF and MRREL.RRF under META/.
  SNOMED CT US Edition — https://www.nlm.nih.gov/healthit/snomedct/us_edition.html
                         Download "US Edition RF2 Release", extract to e.g.
                         ~/ref/SnomedCT_ManagedServiceUS_PRODUCTION_US1000124_YYYYMMDD/
  RxNorm Full Release  — https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html
                         Download "RxNorm Full Monthly Release", extract to e.g.
                         ~/ref/RxNorm_full_MMDDYYYY/
  NHSA drug catalog    — https://github.com/badman200/medicine
                         Download medicine_data.json to ~/ref/medicine_data.json

  Note: UMLS, SNOMED CT, and RxNorm require a free UMLS license from NLM.
"""

import asyncio
import glob
import logging
import os
from argparse import ArgumentParser

from .fhir.siblings import cmd_siblings
from .fhir.bridge import cmd_bridge
from .fhir.merge import cmd_merge
from .fhir.taxonomy import cmd_taxonomy
from .fhir.embeddings.db import cmd_embeddings_db, cmd_id_map
from .fhir.embeddings.dose import cmd_dose_index
from .fhir.embeddings.names import cmd_code_names
from .fhir.embeddings.ref import cmd_embeddings_ref
from .fhir.embeddings.alias import cmd_loinc_alias
from .fhir.embeddings.analyte_digit import cmd_analyte_digit
from .fhir.embeddings.axes import cmd_loinc_axis_emb, cmd_loinc_axis_vocab
from .fhir.embeddings.snomed_axes import cmd_snomed_axis_aliases
from .fhir.embeddings.lexicon import cmd_loinc_lexicon
from .fhir.embeddings.rank import cmd_loinc_rank
from .fhir.embeddings.skip import cmd_loinc_skip
from .fhir.common import SYSTEMS as _SYSTEMS
from .fhir.inspect import cmd_inspect
from .search import cmd_search
from .resolve import cmd_resolve
from .embed import cmd_embed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Default path helpers ────────────────────────────────────────────

_ref = os.environ.get("MIROBODY_REF_DIR", os.path.expanduser("~/ref"))


def _find_latest_dir(base: str, pattern: str) -> str:
    matches = sorted(p for p in glob.glob(os.path.join(base, pattern)) if os.path.isdir(p))
    return matches[-1] if matches else ""


# ─── CLI ─────────────────────────────────────────────────────────────

def cmd_normalize(args) -> None:
    """Parse 'value + unit' strings and emit JSON lines.

    No DB / config required — pure local computation against the
    :mod:`.fhir.units` token tables.
    """
    import json

    from .fhir.units import parse_value_unit, unit_family

    terms = list(args.terms)
    if args.input:
        with open(args.input, encoding="utf-8") as f:
            terms.extend(line.strip() for line in f if line.strip())
    if not terms:
        log.error("No terms provided. Pass positional terms or use --input.")
        return

    for t in terms:
        r = parse_value_unit(t)
        out = {
            "input": t,
            "comparator": r.comparator,
            "value": r.value,
            "unit": r.unit,
            "family": unit_family(r.unit) if r.unit else None,
        }
        print(json.dumps(out, ensure_ascii=False))


def _resolve_ref_defaults(args, ref_dir: str) -> None:
    """Fill in None-valued reference directory args from --ref-dir."""
    _patterns = {
        "snomed_dir": "SnomedCT_ManagedServiceUS_*",
        "loinc_dir":  "Loinc_*",
        "umls_dir":   "umls-*",
        "rxnorm_dir": "RxNorm_full_*",
        "dicom_dir":  "dicom",
    }
    for attr, pattern in _patterns.items():
        if hasattr(args, attr) and getattr(args, attr) is None:
            setattr(args, attr, _find_latest_dir(ref_dir, pattern))

    if hasattr(args, "nhsa_catalog") and getattr(args, "nhsa_catalog") is None:
        path = os.path.join(ref_dir, "medicine_data.json")
        setattr(args, "nhsa_catalog", path if os.path.isfile(path) else "")


def main() -> None:
    parser = ArgumentParser(
        description="mirobody.indicator: build LOINC/SNOMED sibling groups and bridge files"
    )
    parser.add_argument(
        "--ref-dir", default=_ref,
        help=f"Base directory for reference data (default: {_ref})",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    _default_output = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "..", "out"
    )

    # ── siblings ──────────────────────────────────────────────────────
    p_sib = sub.add_parser(
        "siblings",
        help="Build LOINC sibling groups and SNOMED IS-A sibling groups",
    )
    p_sib.add_argument(
        "-o", "--output",
        default=_default_output,
        help=f"Output directory (default: {_default_output})",
    )
    p_sib.add_argument("--loinc-dir", default=None, help="LOINC release dir")
    p_sib.add_argument("--snomed-dir", default=None, help="SNOMED CT release dir")
    p_sib.add_argument("--rxnorm-dir", default=None, help="RxNorm release dir")
    p_sib.add_argument("--nhsa-catalog", default=None, help="NHSA drug catalog JSON")
    p_sib.add_argument("--umls-dir", default=None, help="UMLS release dir for CUI-based enrichment")

    # ── bridge ────────────────────────────────────────────────────────
    p_bridge = sub.add_parser(
        "bridge",
        help="Build SNOMED↔LOINC bridge files (_bridges_icd.csv + _bridges_mrrel.csv) and merge into concepts.csv",
    )
    p_bridge.add_argument(
        "-o", "--output",
        default=_default_output,
        help=f"Output directory (default: {_default_output})",
    )
    p_bridge.add_argument("--umls-dir", default=None, help="UMLS release dir")
    p_bridge.add_argument("--snomed-dir", default=None, help="SNOMED CT release dir")
    p_bridge.add_argument("--loinc-dir", default=None, help="LOINC release dir for Jaccard bridge")

    # ── merge ─────────────────────────────────────────────────────────
    p_merge = sub.add_parser(
        "merge",
        help="Merge siblings + bridge files into concepts.csv + fhir_concept_graph.bin",
    )
    p_merge.add_argument(
        "-o", "--output",
        default=_default_output,
        help=f"Output directory (default: {_default_output})",
    )
    p_merge.add_argument("--loinc-dir", default=None, help="LOINC release dir for orphan injection")

    # ── search ────────────────────────────────────────────────────────
    p_search = sub.add_parser(
        "search",
        help="Search concepts by keywords (local, offline)",
    )
    p_search.add_argument(
        "-o", "--output",
        default=_default_output,
        help=f"Output directory (default: {_default_output})",
    )
    p_search.add_argument("user_id", help="User ID to search for")
    p_search.add_argument("keywords", nargs="+", help="Search keywords")
    p_search.add_argument("--start-time", default=None, help="Start date filter (YYYY-MM-DD)")
    p_search.add_argument("--end-time", default=None, help="End date filter (YYYY-MM-DD)")

    # ── resolve ───────────────────────────────────────────────────────
    p_resolve = sub.add_parser(
        "resolve",
        help="Resolve a free-text term to LOINC / RxNorm / SNOMED CT codes",
    )
    p_resolve.add_argument(
        "terms", nargs="*",
        help="Clinical term(s) to resolve (e.g. 'blood glucose' 'metformin'). "
             "Optional value via ``term=value`` syntax: ``glucose=150 mg/dL`` "
             "biases toward quantitative LOINC variants, ``glucose=++`` "
             "biases toward ordinal — disambiguates analytes with both. "
             "Use --input to read records from a file instead (same syntax) "
             "— avoids ARG_MAX limits for large batches.",
    )
    p_resolve.add_argument(
        "-i", "--input", metavar="FILE",
        help="Read records one-per-line from FILE. Each line is ``term`` or "
             "``term=value`` (same syntax as positional). Mutually exclusive "
             "with positional terms.",
    )
    p_resolve.add_argument(
        "-o", "--output", metavar="FILE",
        help="Append JSON Lines results to FILE. Enables resume: records whose "
             "(term, value) pair already appears in FILE are skipped on re-run "
             "— same term with different values runs as separate jobs. Without "
             "--output, results stream to stdout (no resume).",
    )
    p_resolve.add_argument(
        "-s", "--systems", nargs="+",
        help="Filter to specific systems "
             "(SNOMED_CT, LOINC, RXNORM, CVX, DCM, THETA). "
             "Default: all systems, top_k per system.",
    )
    p_resolve.add_argument(
        "-k", "--top-k", type=int, default=5,
        help="Number of results per code system (default: 5)",
    )
    p_resolve.add_argument(
        "-a", "--axes", action="store_true",
        help="Emit per-axis hybrid output on the LOINC top-1 pick — "
             "COMPONENT / PROPERTY / TIME_ASPCT / SYSTEM / SCALE_TYP / METHOD_TYP, "
             "each as ``(system, code, name)``. SYSTEM falls back to SNOMED "
             "``body structure`` when the LOINC pick's SYSTEM-axis cosine is "
             "below 0.55 (e.g. ``心包液检验·红细胞沉降率`` where LOINC has no "
             "Pericardial-fluid ESR code). Other axes stay on LOINC. See "
             "docs/health_indicator_resolving.md page 9.",
    )

    # ── normalize ─────────────────────────────────────────────────────
    p_norm = sub.add_parser(
        "normalize",
        help="Parse free-text 'value + unit' strings via "
             "mirobody.indicator.fhir.units. Emits one JSON line per input.",
    )
    p_norm.add_argument(
        "terms", nargs="*",
        help="Strings to parse, e.g. '90次每分钟', '<5.6 mg/dL', 'mmol/L'. "
             "Use --input for batch from file.",
    )
    p_norm.add_argument(
        "-i", "--input", metavar="FILE",
        help="Read one term per line from FILE (mutually exclusive with positional terms).",
    )

    # ── inspect ───────────────────────────────────────────────────────
    p_inspect = sub.add_parser(
        "inspect",
        help="Show concept-graph bridges/siblings for a (SYSTEM, CODE) node",
    )
    p_inspect.add_argument(
        "system",
        help=f"Code system: one of {', '.join(_SYSTEMS)}",
    )
    p_inspect.add_argument("code", help="Code string (LOINC may include the dash, e.g. 65583-7)")
    p_inspect.add_argument(
        "--bundle-dir", default=None,
        help="Override the FHIR bundle dir (default: pip-bundled mirobody/res)",
    )
    p_inspect.add_argument(
        "--loinc-dir", default=None,
        help="LOINC release dir; required to display the 6 LOINC axes "
             "(auto-resolved from --ref-dir if available)",
    )
    p_inspect.add_argument("--json", action="store_true", help="Emit JSON instead of pretty text")

    # ── embed ─────────────────────────────────────────────────────────
    p_embed = sub.add_parser(
        "embed",
        help="Batch-fill embedding_gemini for th_series_dim / fhir_indicators",
    )
    p_embed.add_argument(
        "target", choices=["series", "fhir", "all"], default="all", nargs="?",
        help="Which table to embed (default: all)",
    )

    # ── taxonomy ──────────────────────────────────────────────────────
    p_tax = sub.add_parser(
        "taxonomy",
        help="Build a taxonomy binary (currently: body systems → fhir_taxonomy.bin)",
    )
    p_tax.add_argument(
        "-o", "--output",
        default=_default_output,
        help=f"Output directory for CSV caches (default: {_default_output})",
    )
    p_tax.add_argument("--snomed-dir", default=None, help="SNOMED CT release dir")
    p_tax.add_argument("--umls-dir", default=None, help="UMLS release dir")
    p_tax.add_argument(
        "--bin-output", default=None,
        help="Output path for the .bin file (default: mirobody/res/fhir_taxonomy.bin)",
    )

    # ── embeddings ────────────────────────────────────────────────────
    p_emb_export = sub.add_parser(
        "embeddings",
        help="Export fhir_embeddings.npy + fhir_meta.csv.gz "
             "(+ fhir_id_map.npy in --from-db mode) to res/. Default source "
             "is the fhir_indicators DB table; use --from-ref to bootstrap "
             "from ~/ref (for fresh deployments).",
    )
    p_emb_export.add_argument(
        "-o", "--output", default=_default_output,
        help=f"Intermediate build-cache dir (partials, progress) (default: {_default_output})",
    )
    p_emb_export.add_argument(
        "--res-dir", default=None,
        help="Final output dir for fhir_embeddings.npy / fhir_meta.csv.gz / "
             "fhir_id_map.npy (default: mirobody/res)",
    )
    src_group = p_emb_export.add_mutually_exclusive_group()
    src_group.add_argument(
        "--from-db", action="store_true",
        help="Build from the fhir_indicators table (default)",
    )
    src_group.add_argument(
        "--from-ref", action="store_true",
        help="Build from ~/ref source files + Gemini embedding API",
    )
    p_emb_export.add_argument("--snomed-dir", default=None, help="SNOMED CT release dir (ref path)")
    p_emb_export.add_argument("--loinc-dir",  default=None, help="LOINC release dir (ref path)")
    p_emb_export.add_argument("--rxnorm-dir", default=None, help="RxNorm release dir (ref path)")
    p_emb_export.add_argument("--dicom-dir",  default=None, help="DICOM PS3.16 dir containing part16.xml (ref path)")

    # ── id-map ────────────────────────────────────────────────────────
    p_id_map = sub.add_parser(
        "id-map",
        help="Build fhir_id_map.npy (canonical ↔ fhir_indicators.id) "
             "without re-running the slow embedding export. Recovery "
             "path for users whose embeddings already exist but lack "
             "the sidecar.",
    )
    p_id_map.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res)",
    )

    # ── code-names ────────────────────────────────────────────────────
    p_names = sub.add_parser(
        "code-names",
        help="Recovery path: fill the `name` column of an existing "
             "fhir_meta.csv.gz from ~/ref LOINC/SNOMED/RxNorm/CVX "
             "source files. Both `embeddings` modes already do this "
             "inline when ~/ref is available; use this only to repair "
             "a bundle produced without ~/ref.",
    )
    p_names.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res)",
    )
    p_names.add_argument("--snomed-dir", default=None, help="SNOMED CT release dir")
    p_names.add_argument("--loinc-dir",  default=None, help="LOINC release dir")
    p_names.add_argument("--rxnorm-dir", default=None, help="RxNorm release dir")
    p_names.add_argument("--dicom-dir",  default=None, help="DICOM PS3.16 dir containing part16.xml")

    # ── loinc-skip ────────────────────────────────────────────────────
    p_skip = sub.add_parser(
        "loinc-skip",
        help="Build fhir_loinc_skip.npy: row-aligned bool mask marking "
             "LOINC codes that resolve should exclude — non-clinical classes "
             "(PHENX/SURVEY/DOC/admin) and superseded statuses "
             "(DEPRECATED/DISCOURAGED). Derived from fhir_meta + "
             "LoincTableCore.csv; no embedding rebuild needed.",
    )
    p_skip.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res)",
    )
    p_skip.add_argument("--loinc-dir", default=None, help="LOINC release dir")

    # ── loinc-rank ────────────────────────────────────────────────────
    p_rank = sub.add_parser(
        "loinc-rank",
        help="Build fhir_loinc_rank_bonus.npy: row-aligned float32 bonus "
             "applied to LOINC cosines during resolve, derived from the "
             "COMMON_TEST_RANK column in LoincTable/Loinc.csv. Acts as a "
             "soft tie-breaker — top-100 LOINCs get +0.020, longer tail "
             "gets less. Rebuild after each LOINC release.",
    )
    p_rank.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res)",
    )
    p_rank.add_argument("--loinc-dir", default=None, help="LOINC release dir")

    # ── dose-index ────────────────────────────────────────────────────
    p_dose = sub.add_parser(
        "dose-index",
        help="Build fhir_dose_index.npz: corpus-row → (value, UCUM unit) "
             "pairs scanned from each row's display name. Resolver "
             "applies a small cosine bonus when query dose set intersects "
             "row dose set — catches "
             "post-75g-glucose / post-100g-glucose / drug-strength variants "
             "that flat cosine can't distinguish. Rebuild after each meta "
             "refresh or any change to the units scanner's coverage.",
    )
    p_dose.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res)",
    )

    # ── loinc-alias ───────────────────────────────────────────────────
    p_alias = sub.add_parser(
        "loinc-alias",
        help="Build fhir_alias_index.pkl: multilingual lexical alias → "
             "corpus-row inverted index from LoincTable/Loinc.csv + "
             "AccessoryFiles/LinguisticVariants/*. Applied as a resolve-"
             "time cosine bonus when query substrings match an alias. "
             "Catches abbreviations and language synonyms the embedding "
             "underweights (Glu, 肌酐, HPV 11). Rebuild after each LOINC "
             "release.",
    )
    p_alias.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res)",
    )
    p_alias.add_argument("--loinc-dir", default=None, help="LOINC release dir")

    # ── loinc-lexicon ─────────────────────────────────────────────────
    p_lexicon = sub.add_parser(
        "loinc-lexicon",
        help="Build aliases/{lang}.tsv inside fhir_loinc_bundle.tar.gz: "
             "per-language src→canonical-EN mapping for the query-side "
             "augmentation that bridges multilingual embedding gaps "
             "(出芽短梗霉 → Aureobasidium pullulans; 1秒率 → FEV1/FVC). "
             "Deterministic: merges LOINC LinguisticVariant (all CLASSes) "
             "with mirobody/res/aliases_src/{lang}_curated.tsv (curated "
             "wins on key collisions). Direction strictly foreign→EN — "
             "ASCII-only keys are rejected. Also writes a loose "
             "mirobody/res/aliases_src/{lang}.tsv for git review.",
    )
    p_lexicon.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res). Also where the "
             "curated input ``aliases_src/{lang}_curated.tsv`` is read.",
    )
    p_lexicon.add_argument("--loinc-dir", default=None, help="LOINC release dir")
    p_lexicon.add_argument(
        "--lang", default="zh",
        help="Language code matching a LOINC LinguisticVariant file "
             "(zh / ja / ko / de / es / fr / pt / ru / ...). Default: zh.",
    )
    p_lexicon.add_argument(
        "--mrconso", default=None,
        help="Path to UMLS MRCONSO.RRF. Required for languages without "
             "a LOINC LinguisticVariant CSV (e.g. ja). Walks MeSH/MedDRA "
             "JPN↔ENG via CUI to derive (src, dst) pairs.",
    )

    # ── analyte-digit ─────────────────────────────────────────────────
    p_analyte = sub.add_parser(
        "analyte-digit",
        help="Build analyte_digit.tsv inside fhir_loinc_bundle.tar.gz: "
             "chemical-name → numeric-subtype alias table mined from "
             "LOINC RELATEDNAMES2 and SNOMED CT (substance|product) "
             "synonyms. Used by the resolver's digit-aware family "
             "rerank to align Vit B1 / IGF-1 / Vit K2 style queries "
             "against LOINC rows that carry the chemical form "
             "(Thiamine, Insulin-like growth factor-I, Phytonadione). "
             "Curated overlay analyte_digit_curated.tsv survives "
             "rebuilds. Also writes a loose mirobody/res/"
             "analyte_digit_src/analyte_digit.tsv for git review.",
    )
    p_analyte.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res)",
    )
    p_analyte.add_argument(
        "--loinc-dir", default=None,
        help="LOINC release dir (uses {dir}/LoincTable/Loinc.csv). "
             "Either --loinc-dir or --snomed-dir must be passed.",
    )
    p_analyte.add_argument(
        "--snomed-dir", default=None,
        help="SNOMED CT release dir (uses {dir}/Snapshot/Terminology/"
             "sct2_Description_Snapshot-en_*.txt). Either --loinc-dir "
             "or --snomed-dir must be passed.",
    )
    p_analyte.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print the final entries to stdout after the build.",
    )

    # ── loinc-axis-vocab ──────────────────────────────────────────────
    p_axis_vocab = sub.add_parser(
        "loinc-axis-vocab",
        help="Build per-axis LOINC Part vocabularies from LOINC source: "
             "writes mirobody/res/loinc_axes/<AXIS>.tsv (one TSV per "
             "axis with `part_name`/`count`/`translations` — comma-"
             "joined multilingual blob from all 22 LinguisticVariant "
             "locales) and re-generates mirobody/indicator/fhir/"
             "loinc_lookups.py (8 controlled-vocab dicts: SCALE / "
             "PROPERTY / TIME / RAD_*). Pure local computation, no DB "
             "or network. Feeds the `loinc-axis-emb` step.",
    )
    p_axis_vocab.add_argument(
        "--res-dir", default=None,
        help="Output dir for the TSVs (default: mirobody/res). "
             "loinc_lookups.py is always written next to the package.",
    )
    p_axis_vocab.add_argument(
        "--loinc-dir", default=None,
        help="LOINC release dir (auto-resolved from --ref-dir).",
    )

    # ── loinc-axis-emb ────────────────────────────────────────────────
    p_axis_emb = sub.add_parser(
        "loinc-axis-emb",
        help="Embed per-axis LOINC Part vocabularies: reads "
             "mirobody/res/loinc_axes/<AXIS>.tsv (produced by "
             "`loinc-axis-vocab`) and writes <AXIS>.npy (fp16 (N, 1024) "
             "L2-normalized, row-aligned to the TSV). Per-row input is "
             "`part_name,translations` so the embedder sees every "
             "LinguisticVariant locale captured upstream. Uses the "
             "SQLite embedding cache so re-runs after edits to the TSV "
             "only re-hit the API for changed rows.",
    )
    p_axis_emb.add_argument(
        "--res-dir", default=None,
        help="Output dir (default: mirobody/res). Reads <res-dir>/"
             "loinc_axes/*.tsv and writes the .npy siblings next to them.",
    )
    p_axis_emb.add_argument(
        "--axis", action="append", default=None,
        help="Limit to a specific axis name (e.g. SCALE_TYP). "
             "Repeatable. Default: all *.tsv files under loinc_axes/.",
    )
    p_axis_emb.add_argument(
        "--provider", default=None,
        help="Embedding provider override (gemini / qwen). Default: "
             "config key EMBEDDING_PROVIDER (falls back to gemini).",
    )

    # ── snomed-axis-aliases ───────────────────────────────────────────
    p_snomed_aliases = sub.add_parser(
        "snomed-axis-aliases",
        help="Build per-language SNOMED CT alias TSVs from UMLS MRCONSO: "
             "writes mirobody/res/snomed_axes/aliases/{lang}.tsv "
             "(header `concept_id\\talias`) for ja/ko/fr/es/ru/de. "
             "Two-pass scan over MRCONSO.RRF: pass 1 builds the CUI↔SCTID "
             "bridge from SNOMEDCT_US/VET (SUPPRESS-clean); pass 2 emits "
             "translations from any SAB whose row shares a bridged CUI. "
             "Empty per-language tables are skipped — zh.tsv is not "
             "written under current UMLS (LAT=CHI rows almost never "
             "share CUIs with SNOMED concepts).",
    )
    p_snomed_aliases.add_argument(
        "--res-dir", default=None,
        help="Output dir root (default: mirobody/res). Writes under "
             "<res-dir>/snomed_axes/aliases/.",
    )
    p_snomed_aliases.add_argument(
        "--umls-dir", default=None,
        help="UMLS release dir (auto-resolved from --ref-dir).",
    )

    args = parser.parse_args()
    _resolve_ref_defaults(args, args.ref_dir)

    async def _run_async(coro):
        from mirobody.utils import Config
        await Config.init()
        await coro

    if args.command == "siblings":
        cmd_siblings(args)
    elif args.command == "bridge":
        cmd_bridge(args)
    elif args.command == "merge":
        asyncio.run(_run_async(cmd_merge(args)))
    elif args.command == "search":
        asyncio.run(_run_async(cmd_search(args)))
    elif args.command == "resolve":
        asyncio.run(_run_async(cmd_resolve(args)))
    elif args.command == "normalize":
        cmd_normalize(args)
    elif args.command == "inspect":
        cmd_inspect(args)
    elif args.command == "embed":
        asyncio.run(_run_async(cmd_embed(args)))
    elif args.command == "taxonomy":
        asyncio.run(_run_async(cmd_taxonomy(args)))
    elif args.command == "embeddings":
        if args.from_ref:
            asyncio.run(_run_async(cmd_embeddings_ref(args)))
        else:
            asyncio.run(_run_async(cmd_embeddings_db(args)))
    elif args.command == "id-map":
        asyncio.run(_run_async(cmd_id_map(args)))
    elif args.command == "loinc-skip":
        asyncio.run(_run_async(cmd_loinc_skip(args)))
    elif args.command == "loinc-rank":
        asyncio.run(_run_async(cmd_loinc_rank(args)))
    elif args.command == "loinc-alias":
        asyncio.run(_run_async(cmd_loinc_alias(args)))
    elif args.command == "loinc-lexicon":
        cmd_loinc_lexicon(args)
    elif args.command == "analyte-digit":
        cmd_analyte_digit(args)
    elif args.command == "dose-index":
        asyncio.run(_run_async(cmd_dose_index(args)))
    elif args.command == "code-names":
        asyncio.run(_run_async(cmd_code_names(args)))
    elif args.command == "loinc-axis-vocab":
        cmd_loinc_axis_vocab(args)
    elif args.command == "loinc-axis-emb":
        asyncio.run(_run_async(cmd_loinc_axis_emb(args)))
    elif args.command == "snomed-axis-aliases":
        cmd_snomed_axis_aliases(args)


if __name__ == "__main__":
    main()
