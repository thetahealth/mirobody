"""Indicator dim-table sync task.

Producer (pulse): `await IndicatorSyncTask.enqueue("")` on ingest — payload
is a dirty flag, coalesced so at most one sweep is pending.

Consumer: five idempotent classmethods called from `consume`, arranged as a
funnel that progressively narrows the `th_series_data.fhir_id IS NULL` pool,
then materializes whatever remains into `th_series_dim` for embedding
search:

  1. `backfill_from_registry` — fill NULL from the THETA `fhir_indicators`
                                registry (deterministic: `code` /
                                `full_name` + unambiguous `short_name`).
                                Strongest signal; runs first.
  2. `backfill_from_history`  — fill NULL where an indicator's already-mapped
                                rows agree on a single `fhir_id`
                                (`COUNT(DISTINCT) = 1`). Catches free-text
                                indicators with stable medkg mapping.
  3. `backfill_from_dominant` — fill NULL using the ≥99% majority `fhir_id`
                                across an indicator's history — cleans
                                medkg drift noise. True multi-meaning
                                (e.g. "pain" spread across body sites)
                                never reaches 99% and is left for medkg.
  4. `insert`                 — INSERT placeholder dim rows for every
                                still-unmapped indicator (only
                                `original_indicator` is set).
  5. `embed`                  — compute and write text embeddings for dim
                                rows missing the configured provider's
                                embedding column.

Each method is independently idempotent (each filters only on its own
"missing" condition — no cross-step coupling), so they can be invoked in
isolation for tests / migrations / batch re-runs. The `consume` order is a
performance choice: earlier steps are cheaper and higher confidence, so
running them first shrinks the work later steps see.

This task is the canonical writer for `th_series_dim` (see memory:
project_dim_tables). It does NOT fill `standard_indicator` (medkg owns
description generation via `indicator_full_dim.llm_description`). It DOES
backfill `th_series_data.fhir_id`, but only for unambiguous cases —
truly ambiguous free-text indicators stay NULL for medkg to resolve.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .base import BaseRedisTask
from ..utils import execute_query
from ..utils.config import safe_read_cfg
from ..utils.embedding import EMBEDDING_PROVIDERS, text_embedding

#-----------------------------------------------------------------------------

class IndicatorSyncTask(BaseRedisTask):
    """Canonical writer for `th_series_dim`. Signal-driven, coalesced."""

    queue_key = "indicator_sync_queue"

    # Atomic check-and-push: LPUSH only if the queue is empty. Using a Lua
    # script avoids the TOCTOU race where two producers both see llen==0 and
    # both LPUSH, producing duplicate signals.
    _COALESCED_PUSH_LUA = (
        "if redis.call('llen', KEYS[1]) == 0 then "
        "  redis.call('lpush', KEYS[1], ARGV[1]); "
        "  return 1; "
        "else return 0; end"
    )

    @classmethod
    async def enqueue(cls, payload: Any = "") -> None:
        """Coalescing enqueue: skip LPUSH when a pending signal already exists.

        `consume` does a full sweep regardless of payload, so any in-flight
        signal covers subsequent producers. Requires producer to commit DB
        writes before calling enqueue.
        """
        del payload  # content ignored — queue is a dirty-flag only

        redis = await cls._get_producer_redis()
        try:
            pushed = await redis.eval(cls._COALESCED_PUSH_LUA, 1, cls.queue_key, "")
            if pushed:
                logging.info(f"{cls.__name__} enqueued (queue was empty)")
        except Exception as e:
            logging.error(f"{cls.__name__}.enqueue failed: {e}")

    async def consume(self, messages: list[str]) -> None:
        logging.info(f"indicator_sync starting: {len(messages)} signal(s)")
        await self.backfill_from_registry()
        await self.backfill_from_history()
        await self.backfill_from_dominant()
        await self.insert()
        await self.embed()
        logging.info("indicator_sync done")

    #-------------------------------------------------------------------------

    @classmethod
    async def backfill_from_registry(cls) -> None:
        """Backfill `th_series_data.fhir_id` from the THETA `fhir_indicators`
        registry. Deterministic lookup — the strongest and cheapest
        signal, so it runs first in the funnel.

        Match keys (OR'd into one `safe_dict` CTE):
        - `code` (verified 2026-04-23: equal to `full_name` for THETA rows):
          always unique per fhir_id via the `(indicator_standard, code)`
          UNIQUE constraint.
        - `short_name` — Chinese aliases. Only the globally-unique-within-
          THETA ones are used; ambiguous short_names (~10 collisions —
          multiple THETA codes sharing one alias such as "resting heart
          rate") and ~123 empty rows are filtered out by the
          `HAVING COUNT(DISTINCT fhir_id) = 1` guard.

        `split_part(indicator, '.', 1)` strips the source suffix
        (e.g. `dailyAvgHeartRates.apple_health` → `dailyAvgHeartRates`) to
        mirror `FhirMapping._strip_source_suffix` on the write hot path —
        so this step covers the residual NULL rows that the hot-path
        `FhirMapping` cache missed (cold start, config off, race, etc.).

        Covers mostly device-style English indicators; Chinese free-text
        falls through to later steps.
        """
        result = await execute_query("""
            WITH candidate AS (
                SELECT code AS key, id AS fhir_id
                FROM fhir_indicators
                WHERE indicator_standard = 'THETA' AND code IS NOT NULL
                UNION ALL
                SELECT short_name AS key, id AS fhir_id
                FROM fhir_indicators
                WHERE indicator_standard = 'THETA'
                  AND short_name IS NOT NULL AND short_name <> ''
            ),
            safe_dict AS (
                -- MIN(fhir_id) is aggregate-syntax noise, not selection logic:
                -- HAVING COUNT(DISTINCT fhir_id) = 1 already constrains the
                -- group to a single value, so MIN just picks it out.
                SELECT key, MIN(fhir_id) AS fhir_id
                FROM candidate
                GROUP BY key
                HAVING COUNT(DISTINCT fhir_id) = 1
            )
            UPDATE th_series_data sd
            SET fhir_id = d.fhir_id
            FROM safe_dict d
            WHERE sd.fhir_id IS NULL
              AND sd.deleted = 0
              AND split_part(sd.indicator, '.', 1) = d.key
        """)
        count = result.get("record_count", 0) if isinstance(result, dict) else 0
        logging.info(f"backfill_from_registry: {count} rows filled from THETA registry")

    @classmethod
    async def backfill_from_history(cls) -> None:
        """Backfill `th_series_data.fhir_id` using already-mapped rows as a
        self-referential dictionary — only for indicators whose entire
        non-NULL history agrees on a single `fhir_id`
        (`HAVING COUNT(DISTINCT fhir_id) = 1`).

        Catches free-text indicators (typically Chinese medical terms from
        report uploads) where medkg has already produced a stable mapping:
        we don't re-pay the LLM cost and don't expose the next rows to
        medkg's non-determinism. Conflict cases (same text → multiple
        fhir_ids — see memory: project_indicator_mapping_conflicts) are
        deliberately skipped and handled by `backfill_from_dominant` or
        medkg itself.

        Run order matters: runs AFTER `backfill_from_registry`, so any fhir_ids
        just filled by the dict step are already in the `fhir_id IS NOT
        NULL` pool and can seed remaining rows in the same sweep.
        """
        result = await execute_query("""
            WITH unique_map AS (
                SELECT indicator, MIN(fhir_id) AS fhir_id
                FROM th_series_data
                WHERE fhir_id IS NOT NULL AND deleted = 0
                GROUP BY indicator
                HAVING COUNT(DISTINCT fhir_id) = 1
            )
            UPDATE th_series_data sd
            SET fhir_id = m.fhir_id
            FROM unique_map m
            WHERE sd.fhir_id IS NULL
              AND sd.deleted = 0
              AND sd.indicator = m.indicator
        """)
        count = result.get("record_count", 0) if isinstance(result, dict) else 0
        logging.info(f"backfill_from_history: {count} rows filled from unique historical mapping")

    @classmethod
    async def backfill_from_dominant(cls, threshold: float = 0.99) -> None:
        """Backfill `th_series_data.fhir_id` using ≥`threshold` majority rule
        across an indicator's mapped history — cleans medkg drift noise
        without touching true multi-meaning cases.

        Rationale: a medical indicator is expected to have a single stable
        meaning. When the same text appears under multiple fhir_ids, the
        long-tailed minority is almost always medkg non-determinism /
        version drift (e.g. "fasting blood glucose" observed at 99.93% on
        one fhir_id). True multi-meaning — e.g. "pain" split 60/30/10
        across body sites — never reaches 99% dominance, so a strict
        threshold filters drift noise while leaving genuine ambiguity
        alone for medkg.

        Scope:
        - Only fills NULL rows. Does NOT overwrite existing fhir_ids —
          historical "collapse noisy minorities to dominant" is out of
          scope here (one-shot CLI maintenance if ever needed; see memory:
          project_indicator_mapping_conflicts).
        - `threshold` is a classmethod arg (default 0.99), not a config
          value: changing it affects safety semantics, so it should be an
          explicit per-call decision in tests/migrations.

        Run order: AFTER `backfill_from_history`, so indicators whose
        history already agrees on one fhir_id are handled by the cheaper
        step; this one only activates on multi-fhir_id indicators.
        """
        result = await execute_query("""
            WITH stats AS (
                SELECT
                    indicator,
                    fhir_id,
                    COUNT(*) AS cnt,
                    SUM(COUNT(*)) OVER (PARTITION BY indicator) AS total,
                    ROW_NUMBER() OVER (
                        PARTITION BY indicator ORDER BY COUNT(*) DESC
                    ) AS rnk
                FROM th_series_data
                WHERE fhir_id IS NOT NULL AND deleted = 0
                GROUP BY indicator, fhir_id
            ),
            dominant AS (
                SELECT indicator, fhir_id
                FROM stats
                -- Integer comparison avoids float-rounding ambiguity at the
                -- 0.99 boundary. Semantically: cnt/total >= threshold.
                WHERE rnk = 1 AND cnt * 100 >= :threshold_pct * total
            )
            UPDATE th_series_data sd
            SET fhir_id = d.fhir_id
            FROM dominant d
            WHERE sd.fhir_id IS NULL
              AND sd.deleted = 0
              AND sd.indicator = d.indicator
        """, {"threshold_pct": threshold * 100})
        count = result.get("record_count", 0) if isinstance(result, dict) else 0
        logging.info(
            f"backfill_from_dominant: {count} rows filled "
            f"(threshold={threshold:.2f})"
        )

    @classmethod
    async def insert(cls) -> None:
        """INSERT placeholder dim rows for every `th_series_data` indicator
        that is still unmapped (`fhir_id IS NULL`) and not yet in
        `th_series_dim`. Each backfill step first trims the NULL pool, so
        by the time `insert` runs only the genuinely unresolvable
        indicators — the ones that will rely on embedding-based search —
        get materialized as dim rows.

        `ON CONFLICT DO NOTHING` dedups concurrent workers at the DB. Only
        `original_indicator` is set; `embed` fills the embedding column
        in the same sweep.

        Filter matches the partial index in
        `93_th_series_data_unmapped_indicator_idx.sql`, so the scan is
        bounded to the NULL pool.
        """
        result = await execute_query("""
            INSERT INTO th_series_dim (original_indicator)
            SELECT DISTINCT sd.indicator
            FROM th_series_data sd
            WHERE sd.fhir_id IS NULL
              AND sd.deleted = 0
              AND sd.indicator IS NOT NULL
              AND sd.indicator <> ''
              AND sd.indicator <> 'nan'
              AND NOT EXISTS (
                  SELECT 1 FROM th_series_dim dim
                  WHERE dim.original_indicator = sd.indicator
              )
            ON CONFLICT (original_indicator) DO NOTHING
        """)
        count = result.get("record_count", 0) if isinstance(result, dict) else 0
        logging.info(f"insert: {count} placeholder dim rows created")

    @classmethod
    async def embed(cls, batch_size: int = 100, limit: int = 10_000) -> None:
        """Compute text embedding for dim rows with NULL `embedding_<provider>`
        and UPDATE. Text source is `standard_indicator` if set, else
        `original_indicator` — so legacy rows with an existing description
        still use the richer text, while new rows embed the raw name.

        Only embeds rows whose `original_indicator` still has unmapped
        th_series_data (fhir_id IS NULL) — mirrors `insert`'s filter and
        matches `_search_non_fhir`'s join scope. Once every series_data
        row for an indicator is mapped, its dim row is never hit by
        search and doesn't need an embedding.

        `limit` caps per-sweep work so one sweep doesn't load an unbounded
        candidate set into memory; the next sweep picks up the rest
        (`ORDER BY dim.id` makes progress deterministic).

        Provider selected by `DIM_EMBEDDING_PROVIDER`. Per-batch embedding
        errors are logged and skipped; the loop continues.
        """
        dim_provider = safe_read_cfg("DIM_EMBEDDING_PROVIDER", "gemini").lower()
        if dim_provider not in EMBEDDING_PROVIDERS:
            raise ValueError(
                f"DIM_EMBEDDING_PROVIDER invalid: {dim_provider!r} "
                f"(available: {sorted(EMBEDDING_PROVIDERS)})"
            )
        col_name = f"embedding_{dim_provider}"

        query = f"""
            SELECT dim.id, dim.original_indicator, dim.standard_indicator
            FROM th_series_dim dim
            WHERE dim.{col_name} IS NULL
              AND EXISTS (
                  SELECT 1 FROM th_series_data sd
                  WHERE sd.indicator = dim.original_indicator
                    AND sd.fhir_id IS NULL
                    AND sd.deleted = 0
              )
            ORDER BY dim.id
            LIMIT {int(limit)}
        """

        records = await execute_query(query)
        if not records:
            logging.info(f"embed: no rows need {col_name}")
            return

        logging.info(f"embed: {len(records)} rows missing {col_name}")

        update_query = f"""
            UPDATE th_series_dim
            SET {col_name} = :dim_emb,
                updated_at = :updated_at
            WHERE id = :record_id
        """

        # Per-row claim in Redis so concurrent workers across K8s replicas
        # don't pay for the same embedding call twice. TTL must cover one
        # batch's embed+UPDATE; on worker crash the claim expires and the
        # next sweep picks the row up. 2s/row upper bound, 60s floor, 10min cap.
        redis = await cls._get_producer_redis()
        claim_ttl = min(600, max(60, batch_size * 2))

        total_updated = 0
        total_failed = 0
        total_skipped = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            batch_num = i // batch_size + 1

            pipe = redis.pipeline()
            for r in batch:
                pipe.set(f"indicator_sync:embed:claim:{r['id']}", "1", nx=True, ex=claim_ttl)
            claim_results = await pipe.execute()
            batch = [r for r, ok in zip(batch, claim_results) if ok]
            skipped = len(claim_results) - len(batch)
            total_skipped += skipped
            if not batch:
                logging.info(f"embed: batch {batch_num} all {skipped} rows claimed by others")
                continue

            texts = [
                ((r["standard_indicator"] or r["original_indicator"]) or "").strip()
                for r in batch
            ]

            try:
                embeddings = await text_embedding(texts, provider=dim_provider)
            except Exception as e:
                logging.error(f"embed: batch {batch_num} failed: {e}")
                total_failed += len(batch)
                continue

            params = []
            for j, r in enumerate(batch):
                emb = embeddings[j] if j < len(embeddings) else None
                if not emb:
                    continue
                params.append({
                    "record_id": r["id"],
                    "dim_emb": "[" + ",".join(str(x) for x in emb) + "]",
                    "updated_at": datetime.now(timezone.utc).replace(tzinfo=None),
                })

            if params:
                await execute_query(update_query, params)
                total_updated += len(params)
                logging.info(f"embed: batch {batch_num} updated {len(params)}/{len(batch)}")
                # Release claims only for updated rows; rows with empty emb
                # keep their claim until TTL so we don't hot-retry.
                del_pipe = redis.pipeline()
                for p in params:
                    del_pipe.delete(f"indicator_sync:embed:claim:{p['record_id']}")
                await del_pipe.execute()

        logging.info(
            f"embed done: {total_updated} updated, {total_failed} failed, "
            f"{total_skipped} claimed by others, of {len(records)}"
        )

#-----------------------------------------------------------------------------
