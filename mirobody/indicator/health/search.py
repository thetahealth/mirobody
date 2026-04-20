"""Health-domain adapter: FHIR indicators, th_series_data, etc."""

from __future__ import annotations

import asyncio
import logging
import os

from typing import Any

import numpy as np

from mirobody.utils import execute_query

from ..search import DomainAdapter

log = logging.getLogger(__name__)


# ── Local FHIR embedding cache ────────────────────────────────────────
# Artifacts produced by `embeddings-db` / `embeddings-ref`. When both
# files are present, _search_fhir bypasses the fhir_indicators table for
# the vector similarity step and computes cosine locally against the
# fp16 L2-normalised matrix. The th_series_data filter still runs on DB.
_RES_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "res")
_LOCAL_EMB_PATH = os.path.join(_RES_DIR, "fhir_embeddings.npy")
_LOCAL_IDS_PATH = os.path.join(_RES_DIR, "fhir_embedding_ids.npy")

_local_cache_loaded = False
_local_cache: dict | None = None


def _load_local_fhir_cache() -> dict | None:
    """Lazy-load the local fhir embedding + id arrays. Returns None if absent
    or if loading fails (caller then falls back to the DB path).
    """
    global _local_cache_loaded, _local_cache
    if _local_cache_loaded:
        return _local_cache
    _local_cache_loaded = True  # set before load so misses don't re-stat the FS
    if not (os.path.isfile(_LOCAL_EMB_PATH) and os.path.isfile(_LOCAL_IDS_PATH)):
        log.info("local fhir embeddings not found at %s; using DB path", _RES_DIR)
        return None
    try:
        embs = np.load(_LOCAL_EMB_PATH, mmap_mode="r")   # (N, 1024) fp16, paged by OS
        ids = np.load(_LOCAL_IDS_PATH)                   # (N,) int64, fully loaded
        _local_cache = {
            "embs": embs,
            "ids": ids,
            "id_to_row": {int(i): r for r, i in enumerate(ids)},
        }
    except Exception:
        log.exception("failed to load local fhir embeddings; falling back to DB")
        return None
    log.info("loaded local fhir embeddings: %s rows from %s",
             f"{len(ids):,}", _LOCAL_EMB_PATH)
    return _local_cache


class HealthAdapter(DomainAdapter):

    domain = "health"

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
            fi.indicator_standard as standard,
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
                "standard"  : row["standard"] or "",
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
        cache = _load_local_fhir_cache()
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
                1 - (fi.embedding_gemini <=> CAST(:query_vector AS vector)) as score
            FROM user_fhir uf
            INNER JOIN fhir_indicators fi ON fi.id = uf.fhir_id
            WHERE fi.embedding_gemini IS NOT NULL
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

        The local cache is a point-in-time snapshot of fhir_indicators
        produced by the `embeddings-db` command. fhir_ids added after the
        last export are silently skipped here; rerun `embeddings-db` to
        refresh the snapshot if that matters.
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

        id_to_row = cache["id_to_row"]
        row_indices: list[int] = []
        for r in rows:
            idx = id_to_row.get(int(r["fhir_id"]))
            if idx is not None:
                row_indices.append(idx)
        if not row_indices:
            return {}

        # Subset copy to fp32 once; M is typically <10k per user so the
        # temp stays small (M × 4 KB).
        sub = np.asarray(cache["embs"][row_indices], dtype=np.float32)
        sub_ids = cache["ids"][row_indices]  # numpy fancy-index into the id array

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
                fid = int(sub_ids[i])
                merged[fid] = max(merged.get(fid, 0.0), float(scores[i]))
        return merged

    async def _search_non_fhir(
        self,
        user_id: str,
        embeddings: list[list[float]],
        top_k: int,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict]:
        time_clause, time_params = self._build_time_clause(start_time, end_time)

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
                1 - (dim.embedding_gemini <=> CAST(:query_vector AS vector)) as score
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
            WHERE dim.embedding_gemini IS NOT NULL
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
