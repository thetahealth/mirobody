"""FHIR-vocabulary adapter: FHIR indicators, th_series_data, etc."""

from __future__ import annotations

import asyncio
import logging
import os
import re

from typing import Any

import numpy as np

from mirobody.utils import execute_query
from mirobody.utils.embedding import text_embedding

from ..concept_graph import ConceptGraph
from ..search import DomainAdapter, ResolveResult
from .common import (
    FHIR_GRAPH_BIN,
    SYSTEMS, SYSTEM_TO_CODE, _CODE_BITS, _CODE_MASK, int_to_code,
    resolve_dim_embedding_column,
    resolve_fhir_embedding_column,
)
from .index import RES_DIR as _RES_DIR, load as _load_local_fhir_cache

log = logging.getLogger(__name__)

_VALID_SYSTEMS = set(SYSTEMS)
_SYS_MASK = 0x7  # 3-bit system enum, matches common._SYS_BITS

# Ratio guard configuration. See ``_resolve_local_batch`` for usage.
#
# Bonus magnitude calibrated to flip near-tie cases without overpowering
# substantively better cosine: typical "X/Y ratio" query has
# cosine(single-analyte X) ≈ 0.85 and cosine(X/Y ratio code) ≈ 0.80;
# a +0.06 bonus reliably elevates the ratio code on those near-ties.
# Compose-safe with alias bonus (max +0.08) and rank bonus (≤ +0.02):
# worst case +0.16 boost, still well inside the demote -2.0 band.
_RATIO_GUARD_BONUS = 0.06

# Query carries an explicit "I want a ratio/index" signal. CJK markers
# are bare strings (no \b — Chinese has no word boundaries the regex
# engine recognizes); English uses \b to avoid matching inside larger
# words ("indexed", "ratioed" — unlikely but cheap insurance).
#
# Bare ``率`` is included for compound terms like ``1秒率`` (FEV1/FVC)
# / ``阳性率`` / ``存活率`` / ``利用率`` where the multi-char ratio
# markers (比值/比率/指数) don't fire. The negative lookbehind blocks
# rate/velocity/frequency senses where ``率`` is the tail of a non-ratio
# compound: ``代谢率`` (代-谢-率, metabolic RATE not ratio — BMI's
# ``[Ratio]`` tag would otherwise steal the pick), ``速率`` (velocity),
# ``频率`` (frequency), ``心率`` / ``呼吸率`` (per-minute rates whose top
# candidates are anyway outside ``loinc_ratio_code_mask`` so neutral,
# but excluded for symmetry).
_QUERY_WANTS_RATIO_RE = re.compile(
    r"\bratio\b|\bindex\b"
    r"|比值|比率|指数|占比"
    r"|(?<![谢速频心吸])率",
    re.IGNORECASE,
)

# Sequence-metadata annotation patterns. These look like "Index" /
# "指标" requests but actually mean "give me the row's sequence number
# within the indicator column" — i.e. unrelated to LOINC Index codes.
# Real cases in the indicators_excel benchmark: ``氧饱和度·... ·序号
# （指标）`` / ``...Number(index)`` rows where the suffix is a
# row-position marker, not a measurement type. Excluding them keeps
# the ratio guard from boosting Index codes for SpO2-min / SpO2-avg
# style numeric measurements that just happen to carry the row-id
# tail.
_EXCLUDE_RATIO_METADATA_RE = re.compile(
    r"序号（指标）|Number\(index\)",
    re.IGNORECASE,
)

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

    def __init__(
        self,
        bundle_dir: str | None = None,
        *,
        loinc_table_csv: str | None = None,
    ) -> None:
        """Pin this adapter to a specific FHIR bundle directory.

        ``bundle_dir=None`` (default): use the pip-bundled ``RES_DIR``.
        ``bundle_dir=path``: pin to that directory; missing emb npy
        falls back to ``RES_DIR`` with a warning. Different paths get
        their own cache entries (~200 MB heap each on top of the shared
        mmap), so reuse the same adapter instance for the same path.

        Application config (e.g. ``FHIR_INDICATORS_DIR``) is the caller's
        responsibility — read it at the service / CLI boundary and pass
        the resolved path here. This adapter doesn't touch app config.

        ``loinc_table_csv`` overrides env var ``LOINC_TABLE_CSV`` for
        axis-centroid construction. Currently unused at the adapter
        level — axis data is loaded from the bundle's pre-built file
        — but kept for callers that opt into source-side rebuilds.
        """
        self._bundle_dir = bundle_dir
        self._loinc_table_csv = loinc_table_csv

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
        # Graph nodes are canonical fhir_ids (packed via code_to_fhir_id).
        # top_ids may arrive as canonical OR as DB pks (compat mode);
        # row_by_id accepts either form, canonical[row] yields canonical,
        # and to_output_id translates neighbor canonical back to the
        # caller's id form. Codes that aren't in the local corpus (e.g.
        # retired SNOMED IDs surfaced only through siblings) have no row
        # — we skip them on input but keep them on output, so callers
        # downstream can still match by canonical even when the row is
        # missing from the embeddings sidecar.
        cache = _load_local_fhir_cache(load_meta=False, bundle_dir=self._bundle_dir)
        canonical = cache["canonical"]
        row_by_id = cache["row_by_id"]
        to_output_id = cache["to_output_id"]
        expanded: set[int] = set(top_ids)
        for tid in top_ids:
            row = row_by_id.get(int(tid))
            cano = int(canonical[row]) if row is not None else int(tid)
            neighbors = graph.bridge_neighbors(cano) | graph.sibling_neighbors(cano)
            for n in neighbors:
                expanded.add(to_output_id(int(n)))
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
        cache = _load_local_fhir_cache(bundle_dir=self._bundle_dir)
        if cache is not None:
            return await self._fetch_local(cache, user_id, id, start_time, end_time)
        return await self._fetch_db(user_id, id, start_time, end_time)

    async def _fetch_db(
        self,
        user_id: str,
        id: list[int],
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict] | None:
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

    async def _fetch_local(
        self,
        cache: dict,
        user_id: str,
        id: list[int],
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict] | None:
        """Same semantics as _fetch_db but resolves system/code locally.

        Still queries th_series_data for the user's indicator aggregation
        (that scope lives in DB), but skips the JOIN to fhir_indicators —
        system + code come from the canonical packed in the local bundle.
        Rows whose tsd.fhir_id is not in the local bundle are dropped,
        matching the INNER JOIN behavior of _fetch_db.
        """
        time_clause, time_params = self._build_time_clause(start_time, end_time)
        params: dict[str, Any] = {"user_id": user_id, "id": id, **time_params}

        sql = f"""
        SELECT
            tsd.fhir_id,
            tsd.indicator,
            MAX(tsd.start_time) as last_time,
            MIN(tsd.start_time) as first_time,
            COUNT(*) as count
        FROM th_series_data tsd
        WHERE tsd.user_id = :user_id
            AND tsd.fhir_id = ANY(:id)
            AND tsd.deleted = 0
            {time_clause}
        GROUP BY tsd.indicator, tsd.fhir_id
        """

        result = await execute_query(sql, params)
        if not result:
            return None

        canonical = cache["canonical"]
        row_by_id = cache["row_by_id"]
        to_output_id = cache["to_output_id"]
        code_strs = cache.get("code_strs")

        out: list[dict] = []
        for row in result:
            r = row_by_id.get(int(row["fhir_id"]))
            if r is None:
                continue
            cano = int(canonical[r])
            sys_name = SYSTEMS[(cano >> _CODE_BITS) & _SYS_MASK]
            if sys_name in ("DCM", "THETA"):
                code = code_strs.get(r, "") if code_strs is not None else ""
            else:
                code = int_to_code(cano & _CODE_MASK, sys_name)
            out.append({
                "id"        : to_output_id(cano),
                "system"    : sys_name,
                "code"      : code,
                "indicator" : row["indicator"] or "",
                "start_time": str(row["first_time"]) if row["first_time"] is not None else "",
                "end_time"  : str(row["last_time"]) if row["last_time"] is not None else "",
                "count"     : row["count"],
            })
        return out or None

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
        query_texts: list[str] | None = None,
    ) -> list[list[ResolveResult]]:
        """Batch version of :meth:`resolve`.

        Runs the full 4-phase pipeline (see :mod:`.resolve` docstring):
        archetype routing → axis bonus → functional tag pool gates →
        cosine resolve → strategy-level output gating. Local bundle
        preferred; pgvector path is a degraded fallback that skips
        everything except raw cosine.

        ``terms`` drive Phase 1 archetype classification — pass the
        original user-facing string. ``query_texts`` overrides what
        actually gets embedded / fed to alias + ratio guards; default
        is ``terms`` itself. Use ``query_texts`` when the caller
        constructs a richer embedding query (e.g. benchmarks join
        ``source | indicator | abbrev``) but wants Phase 1 to classify
        on the bare term so source-column noise doesn't shift the
        archetype centroid.

        Output preserves positional order with ``terms``; empty /
        invalid / un-embeddable entries map to an empty list. Strategy
        gating drops vocabs the archetype excludes and drops top-1
        results below the archetype's per-vocab ``min_score`` when
        ``fallback="null"``.
        """
        if not terms:
            return []

        if query_texts is not None and len(query_texts) != len(terms):
            raise ValueError(
                f"query_texts length {len(query_texts)} != terms length {len(terms)}"
            )

        if systems:
            systems = [s.upper() for s in systems]
            invalid = set(systems) - _VALID_SYSTEMS
            if invalid:
                raise ValueError(
                    f"Invalid systems: {invalid}. Valid: {_VALID_SYSTEMS}"
                )

        # Provider must match the column we'll query (see _resolve_db /
        # _resolve_local_batch). Both the embedding API and the column
        # name are now keyed off the same ``UTILS_EMBEDDING_MODEL`` config
        # so they can't drift — pass through ``resolve_fhir_embedding_column``
        # so the column→provider mapping stays a single source of truth.
        provider, _ = resolve_fhir_embedding_column()
        # Query-side preprocessing — currently just sub-hour time-marker
        # augmentation (0.5h → "30 minutes 30 分钟"). See
        # :mod:`.embeddings.preprocess`. Applied before embedding AND
        # exposed as ``query_texts`` so downstream alias / ratio guards
        # see the same enriched text.
        from .embeddings.preprocess import (
            augment_time_markers,
            augment_zh_aliases,
            normalize_roman_numerals,
        )
        embed_inputs = query_texts if query_texts is not None else terms
        preprocessed = [
            augment_zh_aliases(
                augment_time_markers(normalize_roman_numerals(t))
            )
            if isinstance(t, str) else ""
            for t in embed_inputs
        ]
        # ``cache=True``: query-side strings repeat across reruns;
        # disk cache at ~/.cache/mirobody/text_embedding.sqlite turns
        # iteration on routing into a no-cost replay.
        embeddings = await text_embedding(preprocessed, provider=provider, cache=True)

        cache = _load_local_fhir_cache(bundle_dir=self._bundle_dir)
        if cache is None:
            # DB fallback: no batched pgvector path, fall back to
            # per-emb queries. Strategy gating is skipped — the pgvector
            # fallback exists for ops continuity, not for matching the
            # local-bundle's full pipeline.
            out: list[list[ResolveResult]] = []
            for emb in embeddings:
                if emb is None:
                    out.append([])
                else:
                    out.append(await self._resolve_db(emb, top_k, systems))
            return out

        # Phase 1: per-term archetype strategy. Phase 3 tag centroids
        # also warmed up here — Stage-2 gates inside _resolve_local_batch
        # need them whether or not any term ends up using their pool.
        from .resolve import (
            STRATEGY_SECTION_HEADER,
            _load_tag_centroids,
            section_header_pool_mask,
            strategies_for_terms,
        )
        strategies = await strategies_for_terms(terms, provider=provider)
        tag_centroids = await _load_tag_centroids(provider)

        # section_header strategy restricts the candidate pool to the
        # ~1.8k record-artifact / narrative-section rows; everything
        # else passes ``None`` so the full corpus competes.
        pool_masks: list[np.ndarray | None] | None = None
        if any(s.name == STRATEGY_SECTION_HEADER.name for s in strategies):
            sh_mask = section_header_pool_mask(cache)
            pool_masks = [
                sh_mask if s.name == STRATEGY_SECTION_HEADER.name else None
                for s in strategies
            ]

        raw = self._resolve_local_batch(
            cache, embeddings, top_k, systems,
            tag_centroids=tag_centroids,
            pool_masks=pool_masks,
            query_texts=preprocessed,
        )

        # Strategy gating: drop vocabs the archetype excludes; drop
        # results below the archetype's per-vocab min_score when the
        # archetype's fallback is "null". Iterating per-result (not
        # per top-1) generalises the gating from top_k=1 to top_k>1:
        # a per-system top score that misses threshold drops every
        # candidate in that system (lower-ranked candidates are by
        # definition weaker than the top-1 already deemed insufficient).
        out = []
        for results, strat in zip(raw, strategies, strict=False):
            gated: list[ResolveResult] = []
            for r in results:
                if r.system in strat.excluded:
                    continue
                if (
                    strat.fallback == "null"
                    and r.score < strat.min_score.get(r.system, 0.0)
                ):
                    continue
                gated.append(r)
            out.append(gated)
        return out

    def _resolve_local_batch(
        self,
        cache: dict,
        embs_in: list[list[float] | None],
        top_k: int,
        systems: list[str] | None,
        *,
        tag_centroids: dict[str, np.ndarray] | None = None,
        pool_masks: list[np.ndarray | None] | None = None,
        query_texts: list[str] | None = None,
    ) -> list[list[ResolveResult]]:
        """Batched cosine search with stage-1 axis rerank + stage-2 tag gates.

        For B queries against N rows × 1024 fp16 embeddings, the chunked
        GEMM keeps peak fp32 working set at ~256 MB (one row chunk) plus
        the (B × N) score matrix instead of allocating ~256 MB per query.

        Two rerank layers compose on top of flat cosine:

        1. **Stage 1 (axis centroids)** — per LOINC axis (other than
           COMPONENT), predict the query's top-1 axis value via cosine to
           per-axis-value centroids, then add a small bonus to candidate
           rows whose axis value matches. Confidence-gated: low-margin
           predictions skip that axis. See :mod:`axis`.

        2. **Stage 2 (functional tags)** — classify the query against
           the four orthogonal tag centroids in :mod:`functional`. Tags
           currently in use:

           - ``drug_allergy_panel``: restrict the LOINC candidate pool
             to rows whose display name contains ``IgE``. When no such
             row survives, the LOINC pick is empty (caller-visible
             null), which is the intended behavior for drug allergens
             without a LOINC IgE Ab variant.

           - ``vital_signs``, ``document_section``, ``microbiology_panel``:
             classified but no behavior wired yet; archetype routing in
             :mod:`category` handles document_section pool restriction,
             and the other two are placeholders.

        ``pool_masks`` is a per-query optional bool mask over corpus
        rows. AND'd into the per-system selector so the caller can pin
        a query to a specific subset (e.g. record-artifact rows for a
        section_header archetype).

        Positions in ``embs_in`` that are ``None`` map to an empty
        result list at the same index.
        """
        out: list[list[ResolveResult]] = [[] for _ in embs_in]

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
        skip_mask: np.ndarray | None = cache.get("loinc_skip_mask")
        demote_mask: np.ndarray | None = cache.get("loinc_demote_mask")
        rank_bonus: np.ndarray | None = cache.get("loinc_rank_bonus")
        ratio_code_mask: np.ndarray | None = cache.get("loinc_ratio_code_mask")
        dose_index: dict | None = cache.get("dose_index")

        Q = np.stack(q_rows, axis=0)              # (B, D) fp32
        B = Q.shape[0]
        n_rows = embs.shape[0]
        scores_all = np.empty((B, n_rows), dtype=np.float32)

        # Chunked fp16→fp32 GEMM to cap peak RAM regardless of N (and B).
        chunk = 1 << 16
        for s in range(0, n_rows, chunk):
            e = min(s + chunk, n_rows)
            chunk_fp32 = embs[s:e].astype(np.float32)
            np.matmul(Q, chunk_fp32.T, out=scores_all[:, s:e])

        # Stage 1: per-axis top-1 prediction; bonus per matching axis.
        # Per-axis bonus weights tuned to empirical accuracy of the
        # corresponding centroid family (see ``AXIS_WEIGHTS`` in
        # :mod:`.resolve.axis`). TIME/PROPERTY/SYSTEM at 0.04;
        # SCALE_TYP at 0.03; METHOD_TYP at 0.00 (39.7% accuracy was
        # net-negative). Sum across 4 contributing axes peaks at +0.15.
        from .resolve import AXIS_WEIGHTS, load_axis_centroids, predict_axis_top1
        from .resolve.axis import apply_deterministic_class_filter
        axis_data = load_axis_centroids(cache)
        if axis_data is not None and axis_data["axis_names"]:
            # CLASS is no longer a soft +0.04 axis bonus: it's a hard
            # deterministic filter applied to the raw cosine matrix
            # (see ``apply_deterministic_class_filter``). The other 5
            # axes (PROPERTY / TIME_ASPCT / SCALE_TYP / METHOD_TYP /
            # SYSTEM) keep their tie-breaker bonus role.
            apply_deterministic_class_filter(
                scores_all, axis_data, query_texts,
                sys_arr, SYSTEM_TO_CODE["LOINC"],
            )
            axis_pred = predict_axis_top1(Q, axis_data)
            for axis in axis_data["axis_names"]:
                if axis == "CLASS":
                    continue
                w = AXIS_WEIGHTS.get(axis, 0.04)
                if w == 0.0:
                    continue
                ridx = axis_data["row_value_idx"][axis]    # (N,) int32
                pred = axis_pred[axis]                     # (B,) int32
                valid = pred >= 0
                # match[b, n] iff pred[b] confident AND ridx[n] == pred[b]
                match = (pred[:, None] == ridx[None, :]) & valid[:, None]
                scores_all += match.astype(np.float32) * w

        # LOINC COMMON_TEST_RANK bonus. Tier-banded float32 array
        # (see :mod:`.embeddings.rank`) added to cosine before per-
        # system top-K. Tie-breaker scale (≤ 0.020) — when two
        # candidates' raw cosines are within ~0.02, the more common
        # one wins. Same shape/broadcast pattern as axis bonus.
        if rank_bonus is not None:
            scores_all += rank_bonus[None, :]

        # Lexical alias bonus. For each query that carries raw text,
        # look up candidate substrings (Latin n-grams + CJK char
        # n-grams) in the multilingual alias inverted index (see
        # :mod:`.embeddings.alias`). Rows that declare a matching
        # alias receive a per-match bonus (capped per row). Catches
        # abbreviations and language synonyms that the embedding
        # underweights — "Glu" → 葡萄糖 codes, "肌酐" → Creatinine
        # codes, "HPV 11" → HPV-type-11 codes. Per-row cap ≈ one
        # axis-bonus weight so the contribution stays in tie-breaker
        # territory even when many aliases collide.
        alias_index = cache.get("alias_index")
        if alias_index is not None and query_texts is not None:
            from .embeddings.alias import alias_bonus_row_vector
            for b, qi in enumerate(valid_idx):
                if qi >= len(query_texts):
                    continue
                qt = query_texts[qi]
                if not qt:
                    continue
                v = alias_bonus_row_vector(qt, alias_index, n_rows)
                if v is not None:
                    scores_all[b] += v

        # Dose value+unit match. When the query carries a dose
        # specifier (``75 g``, ``100 mg``, ``50 mL``) parseable by
        # :func:`mirobody.units.scan_value_units`, boost
        # every corpus row whose name carries the same (value, UCUM)
        # tuple. Disambiguates OGTT challenge variants (75 g vs 100 g
        # vs unspecified) and drug strengths (500 mg vs 1 g) — neither
        # of which falls into an axis the resolver already covers.
        # Bonus matches the axis-level magnitude; query without any
        # dose value leaves every row untouched.
        if dose_index is not None and query_texts is not None:
            from mirobody.units import scan_value_units as _scan_value_units
            from .resolve.challenge_time import context_implied_doses
            for b, qi in enumerate(valid_idx):
                if qi >= len(query_texts):
                    continue
                qt = query_texts[qi]
                if not qt:
                    continue
                # Explicit doses in query text PLUS context-implied
                # default doses (OGTT → 75g glucose when query carries
                # both ``糖尿病筛查``/``糖耐量`` etc. AND a time
                # interval). Union of the two sets feeds the same
                # dose_index lookup — keeps the adapter pass uniform.
                # ``scan_value_units`` returns list; cast for the set
                # operator.
                q_doses = set(_scan_value_units(qt)) | context_implied_doses(qt)
                if not q_doses:
                    continue
                # Union the matching row-index arrays across all of the
                # query's dose tuples. ``unique`` collapses overlaps so
                # the same row doesn't get bonus stacked from two
                # query-side doses pointing at the same row.
                hit_arrays = [
                    dose_index[k] for k in q_doses if k in dose_index
                ]
                if not hit_arrays:
                    continue
                matching = (
                    hit_arrays[0]
                    if len(hit_arrays) == 1
                    else np.unique(np.concatenate(hit_arrays))
                )
                scores_all[b, matching] += 0.04

        # Challenge-test time-interval match. Parallel mechanism to
        # the dose-index above but for the ``--N hour(s)/minute(s)
        # (post|pre)`` LOINC suffix. Queries like ``胰岛素(一小时)``
        # / ``(2 hours)`` extract the time interval and boost every
        # corpus row whose name encodes the same (value, unit) —
        # disambiguates the OGTT/IDDM/cortisol-challenge timing
        # variants that share a base analyte. See
        # :mod:`.resolve.challenge_time` for the corpus index and
        # query extraction. Bonus weight is in the same band as the
        # dose-index pass (+0.06 vs dose's +0.04) because the
        # cosine signal among ``--N hour post`` variants is
        # essentially zero — the entire disambiguation rides on
        # this match.
        if query_texts is not None and names is not None:
            from .resolve.challenge_time import (
                BONUS_WEIGHT as _CT_WEIGHT,
                CLOCK_BONUS_WEIGHT as _CLOCK_WEIGHT,
                _build_loinc_clock_index,
                _build_loinc_time_index,
                query_clock_times,
                query_time_intervals,
            )
            time_index = _build_loinc_time_index(cache)
            if time_index:
                for b, qi in enumerate(valid_idx):
                    if qi >= len(query_texts):
                        continue
                    qt = query_texts[qi]
                    if not qt:
                        continue
                    q_times = query_time_intervals(qt)
                    if not q_times:
                        continue
                    hit_arrays = [
                        time_index[k] for k in q_times if k in time_index
                    ]
                    if not hit_arrays:
                        continue
                    matching = (
                        hit_arrays[0]
                        if len(hit_arrays) == 1
                        else np.unique(np.concatenate(hit_arrays))
                    )
                    scores_all[b, matching] += _CT_WEIGHT

            # Circadian clock-time match (parallel to challenge_time):
            # query carries bracketed ``HH:MM`` (24-hour) and the
            # corpus has rows tagged ``--N AM/PM specimen``. Lower
            # weight than challenge_time because clock-time candidates
            # can span analytes (Cortisol --4 PM vs Corticotropin --4
            # PM), so the bonus must not overpower the analyte cosine
            # signal.
            clock_index = _build_loinc_clock_index(cache)
            if clock_index:
                for b, qi in enumerate(valid_idx):
                    if qi >= len(query_texts):
                        continue
                    qt = query_texts[qi]
                    if not qt:
                        continue
                    q_clocks = query_clock_times(qt)
                    if not q_clocks:
                        continue
                    hit_arrays = [
                        clock_index[k] for k in q_clocks if k in clock_index
                    ]
                    if not hit_arrays:
                        continue
                    matching = (
                        hit_arrays[0]
                        if len(hit_arrays) == 1
                        else np.unique(np.concatenate(hit_arrays))
                    )
                    scores_all[b, matching] += _CLOCK_WEIGHT

        # Ratio / index guard. When the query carries explicit
        # ratio/index markers (``ratio``, ``index``, ``比值``, ``比率``,
        # ``指数``, ``占比``) but NOT a sequence-metadata annotation
        # (``序号（指标）``, ``Number(index)``), boost every LOINC row
        # whose name encodes a ratio / index / fraction by a small
        # constant. Catches the recurring failure where a "X/Y ratio"
        # query lands on the single-analyte X or Y code because the
        # bare-analyte name embeds slightly closer than the
        # compound-name ratio code. The corpus-side mask is
        # pre-computed at meta load (see
        # ``index._compute_ratio_code_mask``).
        if ratio_code_mask is not None and query_texts is not None:
            for b, qi in enumerate(valid_idx):
                if qi >= len(query_texts):
                    continue
                qt = query_texts[qi]
                if not qt:
                    continue
                if _EXCLUDE_RATIO_METADATA_RE.search(qt):
                    continue
                if not _QUERY_WANTS_RATIO_RE.search(qt):
                    continue
                scores_all[b, ratio_code_mask] += _RATIO_GUARD_BONUS

        # Specificity-match demote. LOINC names carry inline qualifiers
        # (``--baseline``, ``--supine``, ``--2 hours post meal``,
        # ``intake 24 hour Estimated``) beyond the 6 named axes; a
        # query without any equivalent license token shouldn't prefer
        # a candidate that *adds* one. See :mod:`.resolve.specificity`
        # for the per-family table and the asymmetric rule.
        if query_texts is not None and names is not None:
            from .resolve.specificity import specificity_penalty
            penalty, _ = specificity_penalty(cache, query_texts, valid_idx)
            if penalty is not None:
                scores_all += penalty

        # Stage 2: per-query functional tag flags.
        tags: dict[str, np.ndarray] = {}
        if tag_centroids:
            from .resolve import classify_sync
            tags = classify_sync(Q, tag_centroids)

        # Pre-fetch each active tag's corpus-side pool mask. tag_pool_mask
        # is cache-keyed, so the first query of each tag pays the regex
        # scan (~150 ms over ~700K names); every subsequent query and
        # batch reuses the result.
        tag_pool_cache: dict[str, np.ndarray] = {}
        if tags:
            from .resolve import tag_pool_mask as _tag_pool_mask
            for tag, flag_arr in tags.items():
                if bool(flag_arr.any()):
                    m = _tag_pool_mask(tag, cache)
                    if m is not None:
                        tag_pool_cache[tag] = m

        target_codes = (
            [SYSTEM_TO_CODE[s] for s in systems]
            if systems else list(range(len(SYSTEMS)))
        )
        loinc_sys = SYSTEM_TO_CODE["LOINC"]

        for b, qi in enumerate(valid_idx):
            scores = scores_all[b]
            pool_mask = pool_masks[qi] if pool_masks is not None else None

            # Per-query LOINC tag mask: AND of every active tag's pool.
            # Empty intersection means no LOINC candidate competes for
            # this query → caller sees empty LOINC (the desired null
            # behavior when query semantics rule out the entire pool).
            tag_loinc_mask: np.ndarray | None = None
            for tag, flag_arr in tags.items():
                if not bool(flag_arr[b]):
                    continue
                m = tag_pool_cache.get(tag)
                if m is None:
                    continue
                tag_loinc_mask = m if tag_loinc_mask is None else (tag_loinc_mask & m)

            # Analyte-concept gate. Query whose LAST Ag/Ab marker is
            # ``抗原 / antigen / Ag`` drops LOINC rows whose name has Ab
            # but not Ag (Ab-only); mirror for ``抗体 / antibody / Ab``.
            # Rows with neither token, and combo Ag+Ab panels, survive.
            # Last-wins handles paths like ``血型单特异性抗体鉴定·A抗原``
            # where the parent class name carries the opposite concept.
            if query_texts is not None:
                from .resolve import (
                    analyte_concept_keep_mask as _concept_keep_mask,
                    query_analyte_concept as _q_concept,
                )
                concept = _q_concept(query_texts[qi])
                if concept is not None:
                    keep = _concept_keep_mask(cache, concept)
                    if keep is not None:
                        # Top-K probe fallback: if every LOINC row in the
                        # raw-cosine top-10 fails ``keep``, the species/
                        # intent likely lives in the masked-out region
                        # (e.g. species has only Ag-only rows in LOINC
                        # but query asks 抗体). Skip the mask — better
                        # cosine-best than wrong-species. Mirrors
                        # ``_compose_loinc_keep`` / CLASS routing.
                        loinc_rows = np.where(sys_arr == loinc_sys)[0]
                        if loinc_rows.size:
                            k = min(10, loinc_rows.size)
                            topk_idx = loinc_rows[
                                np.argpartition(-scores[loinc_rows], k - 1)[:k]
                            ]
                            if not keep[topk_idx].any():
                                keep = None
                    if keep is not None:
                        tag_loinc_mask = (
                            keep if tag_loinc_mask is None
                            else (tag_loinc_mask & keep)
                        )

            picked_rows: list[int] = []
            # Over-pick a buffer per system so the demote sort below
            # has non-demoted competitors. With raw top_k=1 (the
            # benchmark path), a demoted code that's the top-cosine
            # in its system was the SOLE pick and the demote -2.0
            # had no peer to lose to — the demote logic silently
            # no-op'd. The buffer guarantees a non-demoted candidate
            # is available whenever one exists within cosine 2.0.
            # Cost: tiny — argpartition is O(N + k log k) and k stays
            # ≤ 6 here against N ≈ 100K per system.
            internal_k_buffer = 5 if demote_mask is not None else 0
            for sys_int in target_codes:
                mask = sys_arr == sys_int
                if skip_mask is not None:
                    mask = mask & ~skip_mask
                if pool_mask is not None:
                    mask = mask & pool_mask
                if sys_int == loinc_sys and tag_loinc_mask is not None:
                    mask = mask & tag_loinc_mask
                n_in_sys = int(mask.sum())
                if n_in_sys == 0:
                    continue
                sys_scores = np.where(mask, scores, -np.inf)
                k = min(top_k + internal_k_buffer, n_in_sys)
                top = np.argpartition(sys_scores, -k)[-k:]
                picked_rows.extend(int(r) for r in top)

            # Deprecated / demote-masked rows fall behind any
            # non-demoted peer of comparable cosine. Symmetric -2.0
            # penalty keeps the sort key in a well-defined band.
            def _gsort(r: int) -> float:
                s = float(scores[r])
                if demote_mask is not None and demote_mask[r]:
                    s -= 2.0
                n = names[r] if names is not None else ""
                if n and n.lower().startswith("deprecated "):
                    s -= 2.0
                return s
            picked_rows.sort(key=_gsort, reverse=True)

            # The over-pick buffer was just to give demote a peer to
            # lose to. Trim back to top_k per system to keep the
            # documented contract — each system contributes at most
            # top_k results, sorted in the cross-system mix.
            if internal_k_buffer > 0:
                per_sys_count: dict[int, int] = {}
                trimmed: list[int] = []
                for r in picked_rows:
                    s_int = int(sys_arr[r])
                    if per_sys_count.get(s_int, 0) < top_k:
                        trimmed.append(r)
                        per_sys_count[s_int] = per_sys_count.get(s_int, 0) + 1
                picked_rows = trimmed

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
        # th_series_dim names the family (embedding_qwen) where
        # fhir_indicators names the model version (embedding_qwen3), so it has
        # its own map. This used to borrow the FHIR helper for its config +
        # whitelist plumbing and then build `f"embedding_{provider}"` by hand —
        # correct for both providers that existed, and a coincidence rather
        # than a convention.
        provider, dim_col = resolve_dim_embedding_column()

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
