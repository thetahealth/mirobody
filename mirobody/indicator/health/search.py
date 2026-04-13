"""Health-domain adapter: FHIR indicators, th_series_data, etc."""

from __future__ import annotations

import asyncio

from typing import Any

from mirobody.utils import execute_query

from ..search import DomainAdapter


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
