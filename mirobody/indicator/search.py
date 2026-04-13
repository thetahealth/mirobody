"""Domain-agnostic indicator search engine.

The search command computes keyword embeddings, delegates vector recall and
graph expansion to a DomainAdapter, then merges and ranks results.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from argparse import Namespace

from .concept_graph import ConceptGraph

log = logging.getLogger(__name__)


class DomainAdapter:

    domain: str = ""
    _registry: dict[str, type[DomainAdapter]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.domain:
            cls._registry[cls.domain] = cls

    @classmethod
    def get(cls, domain: str) -> DomainAdapter:
        if domain not in cls._registry:
            raise ValueError(f"unknown domain: {domain}")
        return cls._registry[domain]()

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


# ─── Embedding helpers ───────────────────────────────────────────────

_GEMINI_MAX_RETRIES = 3
_GEMINI_RETRY_BACKOFF = (1, 2, 4)  # seconds


async def gemini_embedding(texts: list[str]) -> list[list[float]]:
    """Call Gemini embedding API (1024-dim) with retry on transient errors."""
    import aiohttp

    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY not configured")

    model = "gemini-embedding-001"
    base_url = "https://generativelanguage.googleapis.com/v1beta"
    requests = [
        {"model": f"models/{model}", "content": {"parts": [{"text": t}]}, "output_dimensionality": 1024}
        for t in texts
    ]
    url = f"{base_url}/models/{model}:batchEmbedContents"
    headers = {"Content-Type": "application/json", "x-goog-api-key": api_key}

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for attempt in range(_GEMINI_MAX_RETRIES):
            async with session.post(url, headers=headers, json={"requests": requests}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return [item["values"] for item in data["embeddings"]]
                body = await resp.text()
                if resp.status in (429, 503) and attempt < _GEMINI_MAX_RETRIES - 1:
                    wait = _GEMINI_RETRY_BACKOFF[attempt]
                    log.warning(f"Gemini API {resp.status}, retry in {wait}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait)
                    continue
                raise RuntimeError(f"Gemini API error: {resp.status}, {body}")


# ─── Search engine ─────────────────────────────────────────────────

async def _resolve_user_id(identifier: str) -> str:
    if "@" in identifier:
        from mirobody.utils import execute_query
        sql = "SELECT id FROM health_app_user WHERE email = :email AND is_del = FALSE"
        result = await execute_query(sql, {"email": identifier})
        if result:
            return str(result[0]["id"])
    return identifier


async def _search(
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
    query_embeddings = await gemini_embedding(queries)

    # 2. Vector recall via adapter
    primary_scores, secondary_indicators = await adapter.search(
        user_id    = user_id,
        embeddings = query_embeddings,
        top_k      = top_k,
        start_time = start_time,
        end_time   = end_time,
    )

    ranked = sorted(primary_scores.items(), key=lambda x: x[1], reverse=True)

    # 3. Expand via graph, then fetch from DB
    top_ids = [fid for fid, _ in ranked[:top_k]]
    from mirobody.utils import safe_read_cfg
    from .concept_graph import GRAPH_BIN
    graph_dir = safe_read_cfg("FHIR_INDICATORS_DIR")
    graph = ConceptGraph.get(os.path.join(graph_dir, GRAPH_BIN)) if graph_dir else None

    if graph and top_ids:
        expanded: set[int] = set(top_ids)
        for fid in top_ids:
            expanded.update(graph.bridge_neighbors(fid) | graph.sibling_neighbors(fid))
        fetch_ids = list(expanded)
    else:
        fetch_ids = top_ids

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
    import mirobody.indicator.health.search  # noqa: F401 — register adapter

    adapter = DomainAdapter.get(getattr(args, "domain", "health"))
    results = await _search(
        adapter    = adapter,
        user_id    = args.user_id,
        keywords   = args.keywords,
        start_time = args.start_time,
        end_time   = args.end_time,
    )
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
