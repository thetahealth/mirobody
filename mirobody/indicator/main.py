"""Entry point for the mirobody.indicator CLI.

Subcommands:
    siblings — Build sibling groups (_siblings_*.csv).
    bridge   — Build cross-vocabulary bridge files (_bridges_*.csv).
    merge    — Merge siblings + bridges into concepts.csv + concept_graph.bin.
    search   — Search concepts by keywords (requires DB).
    mapping  — Map free-text term to LOINC / RxNorm / SNOMED CT codes.
    embed    — Batch-fill embedding_gemini for th_series_dim / fhir_indicators.
    test     — Verify concepts.csv against known test cases.

Usage:
    python -m mirobody.indicator siblings -o out/ --loinc-dir ~/ref/Loinc_...
    python -m mirobody.indicator bridge   -o out/ --umls-dir  ~/ref/umls-...
    python -m mirobody.indicator merge    -o out/
    python -m mirobody.indicator search   -o out/ <user_id> <keywords...>
    python -m mirobody.indicator mapping  "blood glucose"

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

from .health.siblings import cmd_siblings
from .health.bridge import cmd_bridge
from .health.merge import cmd_merge
from .health.taxonomy import cmd_taxonomy
from .health.embeddings_db import cmd_embeddings_db
from .health.embeddings_ref import cmd_embeddings_ref
from .search import cmd_search
from .mapping import cmd_mapping
from .embed import cmd_embed
from .health.test import cmd_test

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Default path helpers ────────────────────────────────────────────

_ref = os.environ.get("MIROBODY_REF_DIR", os.path.expanduser("~/ref"))


def _find_latest_dir(base: str, pattern: str) -> str:
    matches = sorted(p for p in glob.glob(os.path.join(base, pattern)) if os.path.isdir(p))
    return matches[-1] if matches else ""


# ─── CLI ─────────────────────────────────────────────────────────────

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
        help="Merge siblings + bridge files into concepts.csv + concept_graph.bin",
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

    # ── mapping ───────────────────────────────────────────────────────
    p_map = sub.add_parser(
        "mapping",
        help="Map a free-text term to LOINC / RxNorm / SNOMED CT codes",
    )
    p_map.add_argument("term", help="Clinical term to map (e.g. 'blood glucose', 'metformin')")
    p_map.add_argument(
        "-s", "--systems", nargs="+",
        help="Filter to specific systems (LOINC, SNOMED_CT, RXNORM)",
    )
    p_map.add_argument(
        "-k", "--top-k", type=int, default=5,
        help="Number of results (default: 5)",
    )
    p_map.add_argument(
        "-t", "--threshold", type=float, default=0.0,
        help="Minimum similarity score 0-1 (default: 0.0)",
    )

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
        help="Build a taxonomy binary (currently: body systems → taxonomy.bin)",
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
        help="Output path for the .bin file (default: mirobody/res/taxonomy.bin)",
    )

    # ── embeddings ────────────────────────────────────────────────────
    p_emb_export = sub.add_parser(
        "embeddings",
        help="Export fhir embeddings/code-index to res/ (fp16, L2-normalised). "
             "Default source is the fhir_indicators DB table; use --from-ref "
             "to bootstrap from ~/ref (for empty-DB users).",
    )
    p_emb_export.add_argument(
        "-o", "--output", default=_default_output,
        help=f"Intermediate build-cache dir (partials, progress) (default: {_default_output})",
    )
    p_emb_export.add_argument(
        "--res-dir", default=None,
        help="Final output dir for fhir_code_index.csv.gz + fhir_embedding*.npy "
             "(default: mirobody/res)",
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

    # ── test ──────────────────────────────────────────────────────────
    p_test = sub.add_parser(
        "test",
        help="Verify concepts.csv against known test cases",
    )
    p_test.add_argument(
        "-o", "--output",
        default=_default_output,
        help=f"Output directory (default: {_default_output})",
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
    elif args.command == "mapping":
        asyncio.run(_run_async(cmd_mapping(args)))
    elif args.command == "embed":
        asyncio.run(_run_async(cmd_embed(args)))
    elif args.command == "taxonomy":
        asyncio.run(_run_async(cmd_taxonomy(args)))
    elif args.command == "embeddings":
        if args.from_ref:
            asyncio.run(_run_async(cmd_embeddings_ref(args)))
        else:
            asyncio.run(_run_async(cmd_embeddings_db(args)))
    elif args.command == "test":
        cmd_test(args)


if __name__ == "__main__":
    main()
