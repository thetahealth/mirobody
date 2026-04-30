"""Domain-agnostic indicator search engine.

The search command computes keyword embeddings, delegates vector recall and
graph expansion to a DomainAdapter, then merges and ranks results.
"""

from __future__ import annotations

import json
import logging
from argparse import Namespace
from dataclasses import asdict, dataclass

from mirobody.utils.embedding import text_embedding

log = logging.getLogger(__name__)


@dataclass
class ResolveResult:
    """One match from `resolve()` — free text → standard code."""
    system: str      # e.g. "LOINC", "SNOMED_CT", "RXNORM"
    code: str        # e.g. "2345-7", "73211009"
    name: str        # human-readable description (empty if meta absent)
    score: float     # cosine similarity (0-1, higher = better)


class DomainAdapter:

    domain: str = ""
    _registry: dict[str, type[DomainAdapter]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.domain:
            cls._registry[cls.domain] = cls

    @classmethod
    def get(cls, domain: str, **kwargs) -> DomainAdapter:
        if domain not in cls._registry:
            raise ValueError(f"unknown domain: {domain}")
        return cls._registry[domain](**kwargs)

    async def search(
        self,
        user_id: str,
        embeddings: list[list[float]],
        top_k: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> tuple[dict[int, float], list[dict]]:
        """Vector recall. Returns (primary_scores, secondary_indicators)."""
        ...

    async def fetch(
        self,
        user_id: str,
        id: list[int],
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict] | None:
        """Fetch indicators by IDs."""
        ...

    async def expand(self, top_ids: list[int]) -> list[int]:
        """Optional graph expansion of top-K vector hits before fetching.

        Default implementation is identity (no expansion). Domains with
        a concept graph (e.g. ``FhirAdapter``) override this to pull
        in bridge / sibling neighbors so a query for one code surfaces
        related codes the user may actually have data for.
        """
        return top_ids

    async def resolve(
        self,
        term: str,
        top_k: int,
        *,
        systems: list[str] | None = None,
    ) -> list[ResolveResult]:
        """Global text→canonical-code resolution.

        Unlike ``search`` (which is user-scoped and returns indicators
        the user owns), ``resolve`` runs against the full standard
        vocabulary and is independent of any user. Useful for ETL /
        terminology-mapping pipelines.

        ``top_k`` is per-system: results contain up to ``top_k`` matches
        from each code system. Final list is sorted by score descending,
        so the caller can compare across systems and judge by score.

        Not all domains implement this — domains without a canonical
        code system can leave it unimplemented.
        """
        ...


# ─── Search engine ─────────────────────────────────────────────────

async def _resolve_user_id(identifier: str) -> str:
    if "@" in identifier:
        from mirobody.utils import execute_query
        sql = "SELECT id FROM health_app_user WHERE email = :email AND is_del = FALSE"
        result = await execute_query(sql, {"email": identifier})
        if result:
            return str(result[0]["id"])
    return identifier


async def search(
    adapter: DomainAdapter,
    keywords: list[str],
    user_id: str,
    start_time: str | None = None,
    end_time: str | None = None,
    top_k: int = 10,
    non_fhir_min_score: float = 0.6,
) -> list[dict]:
    """Core search: keywords -> embedding -> vector recall -> graph expansion.

    This function is domain-agnostic; all domain-specific logic lives in the
    adapter.
    """
    user_id = await _resolve_user_id(user_id)

    # 1. Compute keyword embeddings
    queries = [" ".join(keywords)] + keywords if len(keywords) > 1 else keywords
    query_embeddings = await text_embedding(queries)

    # 2. Vector recall via adapter
    primary_scores, secondary_indicators = await adapter.search(
        user_id    = user_id,
        embeddings = query_embeddings,
        top_k      = top_k,
        start_time = start_time,
        end_time   = end_time,
    )

    ranked = sorted(primary_scores.items(), key=lambda x: x[1], reverse=True)

    # 3. Expand via adapter (domain-specific graph or identity), then fetch
    top_ids = [fid for fid, _ in ranked[:top_k]]
    fetch_ids = await adapter.expand(top_ids)

    indicators = await adapter.fetch(
        user_id    = user_id,
        id         = fetch_ids,
        start_time = start_time,
        end_time   = end_time,
    )

    # 4. Merge primary and secondary indicators
    fhir_scores = [
        s for ind in (indicators or [])
        if ind.get("id") and (s := primary_scores.get(ind["id"], 0)) > 0
    ]
    threshold = min(fhir_scores) if fhir_scores else non_fhir_min_score
    threshold = max(threshold, non_fhir_min_score)

    secondary_indicators = [
        ind for ind in secondary_indicators
        if ind.get("score", 0) >= threshold
    ]

    if indicators:
        indicators.extend(secondary_indicators)
    else:
        indicators = secondary_indicators

    if indicators:
        for ind in indicators:
            ind["score"] = round(ind.get("score") or primary_scores.get(ind.get("id"), 0), 4)
        indicators.sort(key=lambda x: (-x["score"], x.get("id", x.get("indicator", ""))))

    return indicators


async def cmd_search(args: Namespace) -> None:
    """Subcommand: search — search concepts by keywords."""
    from .fhir.search import FhirAdapter
    from mirobody.utils import safe_read_cfg

    bundle_dir = safe_read_cfg("FHIR_INDICATORS_DIR")
    adapter = FhirAdapter(bundle_dir=bundle_dir)
    results = await search(
        adapter    = adapter,
        user_id    = args.user_id,
        keywords   = args.keywords,
        start_time = args.start_time,
        end_time   = args.end_time,
    )
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))


async def cmd_resolve(args: Namespace) -> None:
    """Subcommand: resolve — map terms to standard medical codes.

    Single term, no --output  → pretty JSON list on stdout (legacy shape).
    Otherwise                 → JSON Lines (one ``{"term":..., "results":[...]}``
                                per line). With --output, results append to the
                                file and the run is resumable: terms already
                                present are skipped on re-run. tqdm prints to
                                stderr, with the bar's "completed" count seeded
                                from the existing output so overall progress
                                reflects the full job.
    """
    import sys
    from pathlib import Path
    from .fhir.search import FhirAdapter
    from mirobody.utils import safe_read_cfg

    # Resolve terms source: positional XOR --input.
    if args.input:
        if args.terms:
            log.error("cannot pass both positional terms and --input")
            sys.exit(2)
        with open(args.input, encoding="utf-8") as f:
            terms = [line.rstrip("\n") for line in f]
            terms = [t for t in terms if t.strip()]
    else:
        if not args.terms:
            log.error("must pass terms positionally or via --input")
            sys.exit(2)
        terms = list(args.terms)

    bundle_dir = safe_read_cfg("FHIR_INDICATORS_DIR")
    adapter = FhirAdapter(bundle_dir=bundle_dir)

    # Legacy shape preserved for ad-hoc single-term lookups.
    if len(terms) == 1 and not args.output:
        results = await adapter.resolve(
            term    = terms[0],
            top_k   = args.top_k,
            systems = args.systems,
        )
        print(json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2))
        return

    # Resume: scan existing output for already-completed terms.
    done: set[str] = set()
    if args.output and Path(args.output).exists():
        with open(args.output, encoding="utf-8") as f:
            for line in f:
                try:
                    done.add(json.loads(line)["term"])
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
        if done:
            log.info(f"resume: {len(done)} terms already in {args.output}")

    remaining = [t for t in terms if t not in done]
    if not remaining:
        log.info(f"all {len(terms)} terms already resolved, nothing to do")
        return

    out_fp = (
        open(args.output, "a", encoding="utf-8") if args.output else sys.stdout
    )

    from tqdm import tqdm
    chunk = 256
    pbar = tqdm(
        total=len(terms), initial=len(done), desc="resolve", unit="term",
    )
    try:
        for i in range(0, len(remaining), chunk):
            batch = remaining[i : i + chunk]
            batch_results = await adapter.resolve_many(
                batch, top_k=args.top_k, systems=args.systems,
            )
            for term, results in zip(batch, batch_results):
                out_fp.write(json.dumps(
                    {"term": term, "results": [asdict(r) for r in results]},
                    ensure_ascii=False,
                ) + "\n")
            out_fp.flush()
            pbar.update(len(batch))
    finally:
        pbar.close()
        if out_fp is not sys.stdout:
            out_fp.close()
