#!/usr/bin/env python3
"""
Dimension Table Sync Tool
Fetches indicators from th_series_data table, inserts new records if they don't exist in th_series_dim dimension table.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg
from mirobody.utils.embedding import text_embedding
from mirobody.utils.llm import async_get_structured_output


def _format_vector(embedding: Optional[List[float]]) -> Optional[str]:
    """Format embedding as PostgreSQL vector literal: [v1,v2,v3,...]."""
    if not embedding:
        return None
    return "[" + ",".join(str(x) for x in embedding) + "]"


# ── Indicator description generation ─────────────────────────────────

_DESC_SYSTEM_PROMPT = (
    "You are a professional Chinese medical examination expert. "
    "For each medical indicator, write a 2-3 sentence Chinese description of what it "
    "measures, its clinical significance, and typical abnormal ranges. "
    "CRITICAL: keep the indicator names exactly as given — do not translate or modify them. "
    "DO NOT repeat the indicator name at the start of the description."
)

_DESC_USER_PROMPT = """Write a description for each of the following medical indicators:

Indicator List:
{indicators_text}

For each indicator, return:
- The indicator name, EXACTLY as provided (no translation, no modification)
- A 2-3 sentence Chinese description of what it measures, its clinical significance, and typical abnormal ranges.

IMPORTANT: Return all {indicator_count} indicators."""

_DESC_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "indicator_description",
        "schema": {
            "type": "object",
            "properties": {
                "results": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "indicator": {
                                "type": "string",
                                "description": "Indicator name exactly as provided in input.",
                            },
                            "indicator_description": {
                                "type": "string",
                                "description": "2-3 sentence Chinese description. DO NOT start with or repeat the indicator name.",
                            },
                        },
                        "required": ["indicator", "indicator_description"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["results"],
            "additionalProperties": False,
        },
    },
}


async def _generate_indicator_descriptions(
    indicators: List[str],
) -> List[Dict[str, str]]:
    """LLM-generate a Chinese description for each indicator.

    Returns: list of {"input_indicator": str, "indicator_description": str}.
    Indicators missing from the LLM response are dropped silently — caller
    compares sizes if strict accounting is needed.
    Raises on empty / malformed LLM response.
    """
    valid = [ind.strip() for ind in indicators if ind and ind.strip()]
    if not valid:
        return []

    indicators_text = "\n".join(f"{i + 1}. {ind}" for i, ind in enumerate(valid))
    user_prompt = _DESC_USER_PROMPT.format(
        indicators_text=indicators_text,
        indicator_count=len(valid),
    )
    messages = [
        {"role": "system", "content": _DESC_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt + "\n\nPlease return the result in JSON format."},
    ]

    content = await async_get_structured_output(
        messages=messages,
        response_format=_DESC_SCHEMA,
        temperature=0.1,
        max_tokens=8000,
    )
    if not content:
        raise RuntimeError("LLM returned empty content for indicator descriptions")

    # Normalize to list: different providers may wrap in {"results": [...]}.
    if isinstance(content, dict) and isinstance(content.get("results"), list):
        items = content["results"]
    elif isinstance(content, list):
        items = content
    else:
        raise RuntimeError(f"Unexpected LLM response shape: {type(content).__name__}")

    # Defensive key aliases in case a provider renames fields.
    _INDICATOR_KEYS = ["indicator", "Indicator", "Indicator Name", "name"]
    _DESC_KEYS = ["indicator_description", "Description", "description"]

    def _pick(item: dict, keys: List[str]) -> str:
        for k in keys:
            v = item.get(k)
            if v:
                return str(v).strip()
        return ""

    results = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ind = _pick(item, _INDICATOR_KEYS)
        desc = _pick(item, _DESC_KEYS)
        if ind and desc:
            results.append({"input_indicator": ind, "indicator_description": desc})

    return results


# ── Core implementation ──────────────────────────────────────────────

async def _get_missing_indicators(
    user_id: str = None,
    start_time: datetime = None,
    end_time: datetime = None,
    limit: int = None,
) -> Set[str]:
    """Return indicators present in th_series_data but missing from th_series_dim.

    Raises on query failure — callers must distinguish "no missing data"
    from "query errored out".
    """
    logging.info("🔍 Querying missing indicators...")

    base_query = """
    SELECT DISTINCT sd.indicator
    FROM th_series_data sd
    LEFT JOIN th_series_dim dim ON sd.indicator = dim.original_indicator
    WHERE dim.original_indicator IS NULL
    """

    params: Dict[str, Any] = {}
    conditions = []

    if user_id:
        conditions.append("sd.user_id = :user_id")
        params["user_id"] = user_id
    if start_time:
        conditions.append("sd.start_time >= :start_time")
        params["start_time"] = start_time
    if end_time:
        conditions.append("sd.end_time <= :end_time")
        params["end_time"] = end_time

    if conditions:
        base_query += " AND " + " AND ".join(conditions)

    if limit:
        base_query += f" ORDER BY sd.indicator LIMIT {limit}"

    results = await execute_query(base_query, params)
    missing_indicators = {row["indicator"] for row in results} if results else set()

    logging.info(f"📋 Found {len(missing_indicators)} missing indicators")
    if missing_indicators:
        logging.info(f"   Examples: {list(missing_indicators)[:5]}")

    return missing_indicators


async def _classify_and_embed(
    indicators: List[str],
    description_batch_size: int = 20,
) -> Dict[str, Dict[str, Any]]:
    """
    Generate descriptions + embeddings for a list of raw indicator strings.

    Returns: indicator -> {"description": {...}, "embedding": [...]}
    Indicators that fail either stage are dropped from the result; callers
    compare the result size to the input size to compute failures.
    """
    indicators = [ind for ind in indicators if ind]
    if not indicators:
        return {}

    # Step 1: generate descriptions in sub-batches (larger batches cause response truncation)
    descriptions: List[Dict[str, Any]] = []
    total_batches = (len(indicators) + description_batch_size - 1) // description_batch_size

    logging.info(
        f"📦 Describing {len(indicators)} indicators in {total_batches} batch(es) of ≤{description_batch_size}"
    )

    for i in range(0, len(indicators), description_batch_size):
        batch_indicators = indicators[i : i + description_batch_size]
        batch_num = i // description_batch_size + 1
        t0 = time.time()
        try:
            batch_descriptions = await _generate_indicator_descriptions(batch_indicators)
        except Exception as e:
            logging.error(f"❌ Description batch {batch_num} raised: {e}")
            continue

        if not batch_descriptions:
            logging.error(f"❌ Description batch {batch_num} returned empty")
            continue

        if len(batch_descriptions) != len(batch_indicators):
            logging.warning(
                f"⚠️ Description batch {batch_num} count mismatch: "
                f"expected {len(batch_indicators)}, got {len(batch_descriptions)}"
            )

        descriptions.extend(batch_descriptions)
        logging.info(
            f"✅ Batch {batch_num}/{total_batches} described {len(batch_descriptions)} indicators "
            f"in {time.time() - t0:.2f}s"
        )

    if not descriptions:
        raise RuntimeError("Description produced no results for any batch")

    indicator_to_description: Dict[str, Dict[str, Any]] = {
        d.get("input_indicator", "").strip(): d
        for d in descriptions
        if d.get("input_indicator", "").strip()
    }

    # Step 2: pick text per indicator (prefer standard description, fall back to name)
    # and embed — text_embedding handles dedup + batching internally
    embed_targets: List[tuple] = []  # [(indicator, text), ...]
    for indicator, d in indicator_to_description.items():
        standard = str(d.get("indicator_description", "")).strip()
        if standard and standard != "nan":
            embed_targets.append((indicator, standard))
        elif indicator != "nan":
            embed_targets.append((indicator, indicator))

    if not embed_targets:
        raise RuntimeError("No valid texts to embed")

    dim_provider = safe_read_cfg("DIM_EMBEDDING_PROVIDER", "gemini").lower()
    embeddings = await text_embedding([t for _, t in embed_targets], provider=dim_provider)

    # Step 3: combine into final result
    results: Dict[str, Dict[str, Any]] = {}
    for (indicator, _), emb in zip(embed_targets, embeddings):
        if not emb:
            logging.warning(f"⚠️ Indicator '{indicator}' missing embedding, dropped")
            continue
        results[indicator] = {
            "description": indicator_to_description[indicator],
            "embedding": emb,
        }

    logging.info(
        f"✅ Pipeline completed for {len(results)}/{len(indicators)} indicators "
        f"(described={len(indicator_to_description)})"
    )
    return results


async def _insert_missing_indicators(
    indicators: Set[str],
    batch_size: int = 50,
) -> Dict[str, int]:
    """Classify, embed, and batch-insert indicators into th_series_dim.

    Returns {"inserted": N, "failed": M} where N + M == len(indicators after filtering empties).
    Raises on unrecoverable pipeline errors.
    """
    indicators_list = [ind for ind in indicators if ind]
    if not indicators_list:
        logging.info("✅ No indicators to insert")
        return {"inserted": 0, "failed": 0}

    pipeline_results = await _classify_and_embed(indicators_list)

    logging.info(f"💾 Inserting {len(pipeline_results)}/{len(indicators_list)} indicators into dimension table")

    dim_col = f"embedding_{safe_read_cfg('DIM_EMBEDDING_PROVIDER', 'gemini').lower()}"
    insert_query = f"""
    INSERT INTO th_series_dim
    (original_indicator, standard_indicator, {dim_col}, updated_at)
    VALUES
    (:original_indicator, :indicator_description, :dim_emb, :updated_at)
    ON CONFLICT (original_indicator)
    DO NOTHING
    """

    processed = list(pipeline_results.keys())
    inserted = 0

    for i in range(0, len(processed), batch_size):
        batch = processed[i : i + batch_size]
        batch_params = []
        for indicator in batch:
            entry = pipeline_results[indicator]
            description = entry["description"]
            batch_params.append({
                "original_indicator": indicator,
                "indicator_description": description.get("indicator_description", ""),
                "dim_emb": _format_vector(entry["embedding"]),
                "updated_at": datetime.now(),
            })

        logging.info(f"🔄 Batch inserting {len(batch_params)} indicators")
        await execute_query(insert_query, batch_params)
        inserted += len(batch_params)

    failed = len(indicators_list) - inserted
    logging.info(f"✅ Insert phase complete: {inserted} submitted, {failed} dropped before insert")
    return {"inserted": inserted, "failed": failed}


async def _update_missing_medical_classifications(
    batch_size: int = 50, limit: int = None
) -> Dict[str, Any]:
    """
    Update records in dimension table with missing embeddings / standard_indicator.

    Checks records where the configured dim embedding column is null, runs the
    classifier + embedding pipeline, and refreshes standard_indicator + embedding.
    Raises on unrecoverable errors; caller wraps.
    """
    logging.info("🚀 Starting to update dimension table records missing embeddings")

    dim_provider = safe_read_cfg("DIM_EMBEDDING_PROVIDER", "gemini").lower()
    dim_col = f"embedding_{dim_provider}"
    query = f"""
        SELECT id, original_indicator
        FROM th_series_dim
        WHERE {dim_col} IS NULL
    """
    if limit:
        query += f" ORDER BY id desc LIMIT {limit}"

    records = await execute_query(query)

    if not records:
        logging.info("✅ No records need medical classification update")
        return {"success": True, "total_found": 0, "total_updated": 0, "failed": 0}

    logging.info(f"📋 Found {len(records)} records that need medical classification update")

    max_concurrent = 10
    total_batches = (len(records) + batch_size - 1) // batch_size
    semaphore = asyncio.Semaphore(max_concurrent)

    update_query = f"""
        UPDATE th_series_dim
        SET
            standard_indicator = :indicator_description,
            {dim_col} = :dim_emb,
            updated_at = :updated_at
        WHERE id = :record_id
    """

    logging.info(
        f"📦 Splitting {len(records)} records into {total_batches} batches, max concurrency {max_concurrent}"
    )

    async def process_batch(batch_records: list, batch_num: int) -> Dict[str, int]:
        async with semaphore:
            t_start = time.time()
            batch_indicators = [r["original_indicator"] for r in batch_records]
            logging.info(f"🔄 Batch {batch_num}/{total_batches} start: {len(batch_indicators)} indicators")

            try:
                pipeline_results = await _classify_and_embed(batch_indicators)
            except Exception as e:
                logging.error(f"❌ Batch {batch_num} classify/embed failed: {e}")
                return {"success": 0, "failed": len(batch_records)}

            batch_params = []
            for record in batch_records:
                entry = pipeline_results.get(record["original_indicator"])
                if not entry:
                    continue
                description = entry["description"]
                batch_params.append({
                    "record_id": record["id"],
                    "indicator_description": description.get("indicator_description", ""),
                    "dim_emb": _format_vector(entry["embedding"]),
                    "updated_at": datetime.now(),
                })

            if not batch_params:
                logging.warning(f"⚠️ Batch {batch_num}: no records survived pipeline")
                return {"success": 0, "failed": len(batch_records)}

            try:
                await execute_query(update_query, batch_params)
            except Exception as e:
                logging.error(f"❌ Batch {batch_num} DB update failed: {e}")
                return {"success": 0, "failed": len(batch_records)}

            success = len(batch_params)
            failed = len(batch_records) - success
            logging.info(
                f"🎉 Batch {batch_num} done: {success} updated, {failed} skipped, "
                f"total time {time.time() - t_start:.2f}s"
            )
            return {"success": success, "failed": failed}

    tasks = []
    for i in range(0, len(records), batch_size):
        batch_records = records[i : i + batch_size]
        batch_num = i // batch_size + 1
        tasks.append(process_batch(batch_records, batch_num))

    logging.info(f"🚀 Launching {len(tasks)} concurrent batch tasks")
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

    total_success = 0
    total_failed = 0
    successful_batches = 0
    failed_batches = 0

    for idx, result in enumerate(batch_results):
        if isinstance(result, Exception):
            batch_len = min(batch_size, len(records) - idx * batch_size)
            logging.error(f"❌ Batch {idx + 1} task raised: {result}")
            total_failed += batch_len
            failed_batches += 1
            continue
        total_success += result["success"]
        total_failed += result["failed"]
        if result["success"] > 0:
            successful_batches += 1
        else:
            failed_batches += 1

    logging.info(f"📊 Batch stats: {successful_batches} successful, {failed_batches} failed")
    logging.info(f"📊 Record stats: {total_success} updated, {total_failed} failed")

    if total_success:
        logging.info(
            f"🎉 Update completed — success rate: {total_success / len(records) * 100:.1f}% "
            f"({total_success}/{len(records)})"
        )
    else:
        logging.error("❌ All batches failed medical classification generation")

    return {
        "success": total_failed == 0,
        "total_found": len(records),
        "total_updated": total_success,
        "failed": total_failed,
    }


async def _sync_indicators_from_series_data(
    user_id: str = None,
    start_time: datetime = None,
    end_time: datetime = None,
    batch_size: int = 50,
    limit: int = None,
) -> Dict[str, Any]:
    """Discover missing indicators from th_series_data and insert them into th_series_dim."""
    logging.info("🚀 Starting to sync indicators from th_series_data to dimension table")
    logging.info(f"📊 Parameters: user_id={user_id}, time_range={start_time} to {end_time}")
    logging.info(f"🔧 Config: batch_size={batch_size}, limit={limit}")

    missing_indicators = await _get_missing_indicators(
        user_id=user_id, start_time=start_time, end_time=end_time, limit=limit
    )

    if not missing_indicators:
        logging.info("✅ No indicators need to be synced")
        return {"success": True, "total_found": 0, "total_inserted": 0, "failed": 0}

    insert_result = await _insert_missing_indicators(
        indicators=missing_indicators,
        batch_size=batch_size,
    )

    inserted = insert_result["inserted"]
    failed = insert_result["failed"]
    total_found = len(missing_indicators)

    if failed == 0:
        logging.info(f"🎉 Sync completed! Inserted {inserted}/{total_found} indicators")
    else:
        logging.warning(f"⚠️ Sync partial: inserted {inserted}/{total_found}, failed {failed}")

    return {
        "success": failed == 0,
        "total_found": total_found,
        "total_inserted": inserted,
        "failed": failed,
    }


# ── Public entry points ──────────────────────────────────────────────

async def sync_indicators_for_user(
    user_id: str,
    start_time: datetime,
    end_time: datetime,
    batch_size: int = 50,
    limit: int = None,
) -> Dict[str, Any]:
    """Sync indicators within a time range for a specific user."""
    try:
        return await _sync_indicators_from_series_data(
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            batch_size=batch_size,
            limit=limit,
        )
    except Exception as e:
        logging.error(f"❌ User {user_id} indicators sync failed: {str(e)}")
        return {
            "success": False,
            "total_found": 0,
            "total_inserted": 0,
            "failed": 0,
            "error": str(e),
        }


async def sync_all_missing_indicators(
    batch_size: int = 50, limit: int = None
) -> Dict[str, Any]:
    """Sync all missing indicators (no user or time restrictions)."""
    try:
        return await _sync_indicators_from_series_data(batch_size=batch_size, limit=limit)
    except Exception as e:
        logging.error(f"❌ Full indicators sync failed: {str(e)}", stack_info=True)
        return {
            "success": False,
            "total_found": 0,
            "total_inserted": 0,
            "failed": 0,
            "error": str(e),
        }


async def update_medical_classifications(
    batch_size: int = 50, limit: int = None
) -> Dict[str, Any]:
    """Update missing medical classification fields in dimension table."""
    try:
        return await _update_missing_medical_classifications(batch_size=batch_size, limit=limit)
    except Exception as e:
        logging.error(f"❌ Medical classification field update failed: {str(e)}", stack_info=True)
        return {
            "success": False,
            "total_found": 0,
            "total_updated": 0,
            "failed": 0,
            "error": str(e),
        }


async def backfill_dim_embeddings(
    batch_size: int = 100, limit: int = None
) -> Dict[str, Any]:
    """
    Backfill missing embedding_qwen / embedding_gemini for existing dim records.
    Reads DIM_EMBEDDING_PROVIDER config to determine which column to fill.
    """
    dim_provider = safe_read_cfg("DIM_EMBEDDING_PROVIDER", "gemini").lower()
    col_name = f"embedding_{dim_provider}"

    logging.info(f"🚀 Starting backfill for {col_name} (provider={dim_provider})")

    query = f"""
        SELECT id, original_indicator, standard_indicator
        FROM th_series_dim
        WHERE {col_name} IS NULL
    """
    if limit:
        query += f" ORDER BY id LIMIT {limit}"

    try:
        records = await execute_query(query)
        if not records:
            logging.info(f"✅ No records need {col_name} backfill")
            return {"success": True, "total_found": 0, "total_updated": 0, "failed": 0}

        logging.info(f"📋 Found {len(records)} records missing {col_name}")

        update_query = f"""
            UPDATE th_series_dim
            SET {col_name} = :dim_emb,
                updated_at = :updated_at
            WHERE id = :record_id
        """

        total_updated = 0
        total_failed = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            texts = [
                ((r["standard_indicator"] or r["original_indicator"]) or "").strip()
                for r in batch
            ]

            try:
                embeddings = await text_embedding(texts, provider=dim_provider)

                update_params = []
                for j, record in enumerate(batch):
                    if embeddings[j]:
                        update_params.append({
                            "record_id": record["id"],
                            "dim_emb": _format_vector(embeddings[j]),
                            "updated_at": datetime.now(),
                        })

                if update_params:
                    await execute_query(update_query, update_params)
                    total_updated += len(update_params)
                    logging.info(f"   ✅ Batch {i // batch_size + 1}: updated {len(update_params)} records")

            except Exception as e:
                logging.error(f"❌ Batch {i // batch_size + 1} failed: {str(e)}")
                total_failed += len(batch)

        logging.info(f"🎉 Backfill completed: {total_updated} updated, {total_failed} failed out of {len(records)}")
        return {
            "success": total_failed == 0,
            "total_found": len(records),
            "total_updated": total_updated,
            "failed": total_failed,
        }

    except Exception as e:
        logging.error(f"❌ Backfill {col_name} failed: {str(e)}", stack_info=True)
        return {"success": False, "total_found": 0, "total_updated": 0, "failed": 0, "error": str(e)}
