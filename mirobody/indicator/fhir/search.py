"""FHIR-vocabulary adapter: FHIR indicators, th_series_data, etc."""

from __future__ import annotations

import asyncio
import logging
import os

from typing import Any

import numpy as np

from mirobody.utils import execute_query
from mirobody.utils.embedding import text_embedding

from ..concept_graph import ConceptGraph
from ..search import DomainAdapter, ResolveResult
from .common import (
    SYSTEMS, SYSTEM_TO_CODE, _CODE_BITS, _CODE_MASK, int_to_code,
    resolve_fhir_embedding_column,
)
from .embeddings.local import RES_DIR as _RES_DIR, load as _load_local_fhir_cache
from .graph_builder import FHIR_GRAPH_BIN

log = logging.getLogger(__name__)

_VALID_SYSTEMS = set(SYSTEMS)
_SYS_MASK = 0x7  # 3-bit system enum, matches common._SYS_BITS

# Per-row systems array, computed once per cache instance (id-keyed).
_systems_cache: tuple[int, np.ndarray] | None = None


def _systems_array(cache: dict) -> np.ndarray:
    """Vectorized system-enum-index per row (cached)."""
    global _systems_cache
    cano = cache["canonical"]
    cache_id = id(cano)
    if _systems_cache is not None and _systems_cache[0] == cache_id:
        return _systems_cache[1]
    arr = ((np.asarray(cano) >> _CODE_BITS) & _SYS_MASK).astype(np.int8)
    _systems_cache = (cache_id, arr)
    return arr


class FhirAdapter(DomainAdapter):

    domain = "fhir"

    def __init__(self, bundle_dir: str | None = None) -> None:
        """Pin this adapter to a specific FHIR bundle directory.

        ``bundle_dir=None`` (default): use the pip-bundled ``RES_DIR``.
        ``bundle_dir=path``: pin to that directory; missing emb npy
        falls back to ``RES_DIR`` with a warning. Different paths get
        their own cache entries (~200 MB heap each on top of the shared
        mmap), so reuse the same adapter instance for the same path.

        Application config (e.g. ``FHIR_INDICATORS_DIR``) is the caller's
        responsibility — read it at the service / CLI boundary and pass
        the resolved path here. This adapter doesn't touch app config.
        """
        self._bundle_dir = bundle_dir

    def _graph(self) -> ConceptGraph:
        """Lazy-load the FHIR concept graph. Looked up under
        ``bundle_dir`` first (so external mounts can ship a custom
        graph alongside their embeddings), then under the pip-bundled
        ``mirobody/res/`` — the bin is small (~9 MB) and stays in the
        wheel by default, so the bundled fallback is the normal path.
        """
        candidates = []
        if self._bundle_dir:
            candidates.append(os.path.join(self._bundle_dir, FHIR_GRAPH_BIN))
        candidates.append(os.path.join(_RES_DIR, FHIR_GRAPH_BIN))
        for p in candidates:
            if os.path.isfile(p):
                return ConceptGraph.get(p)
        # Surface the most informative path so misconfigurations are obvious.
        raise FileNotFoundError(
            f"{FHIR_GRAPH_BIN} not found under bundle_dir or RES_DIR; "
            f"tried: {candidates}"
        )

    async def expand(self, top_ids: list[int]) -> list[int]:
        if not top_ids:
            return top_ids
        graph = self._graph()
        expanded: set[int] = set(top_ids)
        for fid in top_ids:
            expanded.update(
                graph.bridge_neighbors(fid) | graph.sibling_neighbors(fid)
            )
        return list(expanded)

    async def search(
        self,
        user_id: str,
        embeddings: list[list[float]],
        top_k: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> tuple[dict[int, float], list[dict]]:
        primary, secondary = await asyncio.gather(
            self._search_fhir(user_id, embeddings, top_k, start_time, end_time),
            self._search_non_fhir(user_id, embeddings, top_k, start_time, end_time),
        )
        return primary, secondary

    async def fetch(
        self,
        user_id: str,
        id: list[int],
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict] | None:
        if not id:
            return None

        time_clause, time_params = self._build_time_clause(start_time, end_time)
        params: dict[str, Any] = {"user_id": user_id, "id": id, **time_params}

        sql = f"""
        SELECT
            fi.id,
            fi.indicator_standard as system,
            fi.code,
            tsd.indicator,
            MAX(tsd.start_time) as last_time,
            MIN(tsd.start_time) as first_time,
            COUNT(*) as count
        FROM th_series_data tsd
        INNER JOIN fhir_indicators fi
        ON tsd.fhir_id = fi.id
        WHERE tsd.user_id = :user_id
            AND tsd.fhir_id = ANY(:id)
            AND tsd.deleted = 0
            {time_clause}
        GROUP BY tsd.indicator, fi.id
        """

        result = await execute_query(sql, params)
        if not result:
            return None

        return [
            {
                "id"        : row["id"],
                "system"    : row["system"] or "",
                "code"      : row["code"] or "",
                "indicator" : row["indicator"] or "",
                "start_time": str(row["first_time"]) if row["first_time"] is not None else "",
                "end_time"  : str(row["last_time"]) if row["last_time"] is not None else "",
                "count"     : row["count"],
            }
            for row in result
        ]

    # ── Private helpers ───────────────────────────────────────────────

    def _build_time_clause(self, start_time: str | None, end_time: str | None) -> tuple[str, dict[str, str]]:
        clause = ""
        params: dict[str, str] = {}
        if start_time:
            params["start_time"] = start_time
            clause += " AND tsd.start_time >= CAST(:start_time AS timestamp)"
        if end_time:
            params["end_time"] = end_time
            clause += " AND tsd.start_time <= CAST(:end_time AS timestamp)"
        return clause, params

    async def _search_fhir(
        self,
        user_id: str,
        embeddings: list[list[float]],
        top_k: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[int, float]:
        cache = _load_local_fhir_cache(load_meta=False, bundle_dir=self._bundle_dir)
        if cache is not None:
            return await self._search_fhir_local(
                cache, user_id, embeddings, top_k, start_time, end_time,
            )
        return await self._search_fhir_db(
            user_id, embeddings, top_k, start_time, end_time,
        )

    async def _search_fhir_db(
        self,
        user_id: str,
        embeddings: list[list[float]],
        top_k: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[int, float]:
        time_clause, time_params = self._build_time_clause(start_time, end_time)
        _, emb_col = resolve_fhir_embedding_column()

        async def _single_query(emb: list[float]) -> list[dict]:
            vector_str = "[" + ",".join(map(str, emb)) + "]"
            params: dict = {"user_id": user_id, "query_vector": vector_str, "top_k": top_k, **time_params}
            sql = f"""
            WITH user_fhir AS (
                SELECT DISTINCT tsd.fhir_id
                FROM th_series_data tsd
                WHERE tsd.user_id = :user_id
                AND tsd.fhir_id IS NOT NULL
                AND tsd.fhir_id > 0
                AND tsd.deleted = 0
                {time_clause}
            )
            SELECT
                fi.id,
                1 - (fi.{emb_col} <=> CAST(:query_vector AS vector)) as score
            FROM user_fhir uf
            INNER JOIN fhir_indicators fi ON fi.id = uf.fhir_id
            WHERE fi.{emb_col} IS NOT NULL
            ORDER BY score DESC
            LIMIT :top_k
            """
            return await execute_query(sql, params) or []

        all_hits = await asyncio.gather(*(_single_query(emb) for emb in embeddings))

        merged: dict[int, float] = {}
        for hits in all_hits:
            for hit in hits:
                fid = hit["id"]
                merged[fid] = max(merged.get(fid, 0), hit["score"])
        return merged

    async def _search_fhir_local(
        self,
        cache: dict,
        user_id: str,
        embeddings: list[list[float]],
        top_k: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[int, float]:
        """Same semantics as _search_fhir_db but runs cosine locally.

        Still queries th_series_data for the user's fhir_id set (that scope
        lives in DB), but skips the JOIN to fhir_indicators — similarity is
        computed against the in-memory fp16 matrix.

        The local cache is a point-in-time bundle produced by the
        `embeddings-db` (compat mode, with id_map sidecar), `embeddings-ref`
        (terminal mode, canonical-only), or `migrate` subcommand. Codes added
        after the last export are silently skipped here; refresh the bundle
        if that matters.
        """
        time_clause, time_params = self._build_time_clause(start_time, end_time)
        user_sql = f"""
            SELECT DISTINCT tsd.fhir_id
            FROM th_series_data tsd
            WHERE tsd.user_id = :user_id
              AND tsd.fhir_id IS NOT NULL
              AND tsd.fhir_id > 0
              AND tsd.deleted = 0
              {time_clause}
        """
        rows = await execute_query(user_sql, {"user_id": user_id, **time_params}) or []

        # tsd.fhir_id is DB pk in compat mode (sidecar present) or
        # canonical in terminal mode; row_by_id accepts either form.
        row_by_id = cache["row_by_id"]
        row_indices: list[int] = []
        for r in rows:
            idx = row_by_id.get(int(r["fhir_id"]))
            if idx is not None:
                row_indices.append(idx)
        if not row_indices:
            return {}

        # Subset copy to fp32 once; M is typically <10k per user so the
        # temp stays small (M × 4 KB).
        sub = np.asarray(cache["embs"][row_indices], dtype=np.float32)
        sub_canonical = cache["canonical"][row_indices]
        to_output_id = cache["to_output_id"]

        merged: dict[int, float] = {}
        for emb in embeddings:
            q = np.asarray(emb, dtype=np.float32)
            n = float(np.linalg.norm(q))
            if n:
                q /= n
            scores = sub @ q  # (M,) cosine, since rows are pre-normalised
            k = min(top_k, scores.shape[0])
            top_idx = np.argpartition(scores, -k)[-k:]
            for i in top_idx:
                fid = to_output_id(int(sub_canonical[i]))
                merged[fid] = max(merged.get(fid, 0.0), float(scores[i]))
        return merged

    async def resolve(
        self,
        term: str,
        top_k: int,
        *,
        systems: list[str] | None = None,
    ) -> list[ResolveResult]:
        """Resolve a free-text term to standard codes in fhir_indicators.

        Embedding is computed internally; callers don't deal with vectors.
        Local bundle preferred; falls back to pgvector when absent.

        ``top_k`` is per-system. Each code system contributes up to
        ``top_k`` of its best matches; the final list is sorted by
        score descending across systems. Lets callers compare candidates
        across vocabularies and judge by relative score.
        """
        results = await self.resolve_many([term], top_k, systems=systems)
        return results[0]

    async def resolve_many(
        self,
        terms: list[str],
        top_k: int,
        *,
        systems: list[str] | None = None,
    ) -> list[list[ResolveResult]]:
        """Batch version of :meth:`resolve`.

        All terms are embedded in a single ``text_embedding`` call (which
        chunks per provider batch limit internally), then matched against
        the local cache (or pgvector fallback) one-by-one. Output preserves
        positional order with the input; empty / invalid / un-embeddable
        terms map to an empty list.
        """
        if not terms:
            return []

        if systems:
            systems = [s.upper() for s in systems]
            invalid = set(systems) - _VALID_SYSTEMS
            if invalid:
                raise ValueError(
                    f"Invalid systems: {invalid}. Valid: {_VALID_SYSTEMS}"
                )

        # Provider must match the column we'll query (see _resolve_db /
        # _resolve_local_batch). text_embedding's default reads
        # EMBEDDING_PROVIDER, but the column is keyed off
        # DIM_EMBEDDING_PROVIDER — pass it explicitly so they can't drift.
        provider, _ = resolve_fhir_embedding_column()
        embeddings = await text_embedding(
            [t if isinstance(t, str) else "" for t in terms],
            provider=provider,
        )

        cache = _load_local_fhir_cache(bundle_dir=self._bundle_dir)
        if cache is not None:
            return self._resolve_local_batch(cache, embeddings, top_k, systems)

        # DB fallback: no batched pgvector path, fall back to per-emb queries.
        out: list[list[ResolveResult]] = []
        for emb in embeddings:
            if emb is None:
                out.append([])
            else:
                out.append(await self._resolve_db(emb, top_k, systems))
        return out

    def _resolve_local(
        self,
        cache: dict,
        emb: list[float],
        top_k: int,
        systems: list[str] | None,
    ) -> list[ResolveResult]:
        return self._resolve_local_batch(cache, [emb], top_k, systems)[0]

    def _resolve_local_batch(
        self,
        cache: dict,
        embs_in: list[list[float] | None],
        top_k: int,
        systems: list[str] | None,
    ) -> list[list[ResolveResult]]:
        """Batched cosine search: one chunked GEMM for the whole batch.

        For B queries against N rows × 1024 fp16 embeddings, this keeps peak
        fp32 working set at ~256 MB (one row chunk) plus the (B × N) score
        matrix, instead of allocating ~256 MB per query × B queries. The
        GEMM call (vs B independent GEMVs) also lifts BLAS throughput.

        Positions in ``embs_in`` that are ``None`` map to an empty result
        list at the same index.
        """
        out: list[list[ResolveResult]] = [[] for _ in embs_in]

        # Stack valid (non-None) queries into Q; remember their original positions.
        valid_idx: list[int] = []
        q_rows: list[np.ndarray] = []
        for i, emb in enumerate(embs_in):
            if emb is None:
                continue
            q = np.asarray(emb, dtype=np.float32)
            n = float(np.linalg.norm(q))
            if n:
                q = q / n
            valid_idx.append(i)
            q_rows.append(q)
        if not valid_idx:
            return out

        embs: np.ndarray = cache["embs"]
        canonical: np.ndarray = cache["canonical"]
        names: list[str] | None = cache["names"]
        code_strs: dict[int, str] | None = cache["code_strs"]
        sys_arr = _systems_array(cache)

        Q = np.stack(q_rows, axis=0)              # (B, 1024) fp32
        B = Q.shape[0]
        n_rows = embs.shape[0]
        scores_all = np.empty((B, n_rows), dtype=np.float32)

        # Chunked fp16→fp32 GEMM to cap peak RAM regardless of N (and B).
        chunk = 1 << 16
        for s in range(0, n_rows, chunk):
            e = min(s + chunk, n_rows)
            chunk_fp32 = embs[s:e].astype(np.float32)            # (chunk, 1024)
            np.matmul(Q, chunk_fp32.T, out=scores_all[:, s:e])   # (B, chunk)

        target_codes = (
            [SYSTEM_TO_CODE[s] for s in systems]
            if systems else list(range(len(SYSTEMS)))
        )

        # Per-query top-k pick (cheap once scores_all is computed).
        for b, qi in enumerate(valid_idx):
            scores = scores_all[b]

            picked_rows: list[int] = []
            for sys_int in target_codes:
                mask = sys_arr == sys_int
                n_in_sys = int(mask.sum())
                if n_in_sys == 0:
                    continue
                sys_scores = np.where(mask, scores, -np.inf)
                k = min(top_k, n_in_sys)
                top = np.argpartition(sys_scores, -k)[-k:]
                picked_rows.extend(int(r) for r in top)

            picked_rows.sort(key=lambda r: scores[r], reverse=True)

            results: list[ResolveResult] = []
            for r_int in picked_rows:
                sys_name = SYSTEMS[int(sys_arr[r_int])]
                if sys_name in ("DCM", "THETA"):
                    code = code_strs.get(r_int, "") if code_strs is not None else ""
                else:
                    code = int_to_code(int(canonical[r_int]) & _CODE_MASK, sys_name)
                name = names[r_int] if names is not None else ""
                results.append(ResolveResult(
                    system=sys_name,
                    code=code,
                    name=name,
                    score=round(float(scores[r_int]), 4),
                ))
            out[qi] = results
        return out

    async def _resolve_db(
        self,
        emb: list[float],
        top_k: int,
        systems: list[str] | None,
    ) -> list[ResolveResult]:
        vector_str = "[" + ",".join(map(str, emb)) + "]"
        system_clause = ""
        params: dict[str, Any] = {"query_vector": vector_str, "top_k": top_k}
        if systems:
            system_clause = "AND fi.indicator_standard = ANY(:systems)"
            params["systems"] = systems

        _, emb_col = resolve_fhir_embedding_column()
        # Per-system top_k via window function, then global score sort.
        sql = f"""
        WITH ranked AS (
            SELECT
                fi.indicator_standard AS system,
                fi.code,
                fi.full_name AS name,
                1 - (fi.{emb_col} <=> CAST(:query_vector AS vector)) AS score,
                ROW_NUMBER() OVER (
                    PARTITION BY fi.indicator_standard
                    ORDER BY fi.{emb_col} <=> CAST(:query_vector AS vector)
                ) AS rn
            FROM fhir_indicators fi
            WHERE fi.{emb_col} IS NOT NULL
              AND fi.code IS NOT NULL
              {system_clause}
        )
        SELECT system, code, name, score
        FROM ranked
        WHERE rn <= :top_k
        ORDER BY score DESC
        """
        rows = await execute_query(sql, params) or []
        return [
            ResolveResult(
                system=row["system"] or "",
                code=row["code"] or "",
                name=row["name"] or "",
                score=round(float(row["score"]), 4),
            )
            for row in rows
        ]

    async def _search_non_fhir(
        self,
        user_id: str,
        embeddings: list[list[float]],
        top_k: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict]:
        time_clause, time_params = self._build_time_clause(start_time, end_time)
        # th_series_dim follows the family-only convention (embedding_qwen,
        # embedding_gemini) — different from fhir_indicators' versioned
        # naming (embedding_qwen3). Resolve provider via the FHIR helper
        # purely for its config-key + whitelist plumbing, then build the
        # dim column name directly.
        provider, _ = resolve_fhir_embedding_column()
        dim_col = f"embedding_{provider}"

        async def _single_query(emb: list[float]) -> list[dict]:
            vector_str = "[" + ",".join(map(str, emb)) + "]"
            params: dict[str, Any] = {
                "user_id": user_id, "query_vector": vector_str, "top_k": top_k, **time_params,
            }
            sql = f"""
            SELECT
                agg.indicator,
                dim.standard_indicator as description,
                agg.last_time,
                agg.first_time,
                agg.count,
                1 - (dim.{dim_col} <=> CAST(:query_vector AS vector)) as score
            FROM (
                SELECT
                    tsd.indicator,
                    MAX(tsd.start_time) as last_time,
                    MIN(tsd.start_time) as first_time,
                    COUNT(*) as count
                FROM th_series_data tsd
                WHERE tsd.user_id = :user_id
                  AND (tsd.fhir_id IS NULL OR tsd.fhir_id = 0)
                  AND tsd.deleted = 0
                  {time_clause}
                GROUP BY tsd.indicator
            ) agg
            INNER JOIN th_series_dim dim ON agg.indicator = dim.original_indicator
            WHERE dim.{dim_col} IS NOT NULL
            ORDER BY score DESC
            LIMIT :top_k
            """
            return await execute_query(sql, params) or []

        all_hits = await asyncio.gather(*(_single_query(emb) for emb in embeddings))

        best: dict[str, dict] = {}
        for hits in all_hits:
            for row in hits:
                name = row["indicator"]
                score = float(row["score"]) if row.get("score") else 0.0
                if name not in best or score > best[name]["score"]:
                    best[name] = {
                        "indicator"  : name,
                        "description": row.get("description", ""),
                        "start_time" : str(row["first_time"]) if row.get("first_time") is not None else "",
                        "end_time"   : str(row["last_time"]) if row.get("last_time") is not None else "",
                        "count"      : row.get("count"),
                        "score"      : score,
                    }

        return sorted(best.values(), key=lambda x: x["score"], reverse=True)
