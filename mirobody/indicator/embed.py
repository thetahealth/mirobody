"""Batch-embed command for indicator tables."""

from __future__ import annotations

import logging
from argparse import Namespace

from mirobody.utils.embedding import EMBEDDING_PROVIDERS, text_embedding

from .fhir.common import FHIR_EMBEDDING_COLUMN

log = logging.getLogger(__name__)

# ─── Batch embed command ─────────────────────────────────────────────

BATCH_SIZE = 100


async def _embed_table(
    table: str,
    text_column: str,
    extra_where: str = "",
) -> int:
    """Embed rows missing the configured provider's embedding column.

    Provider comes from ``DIM_EMBEDDING_PROVIDER``. th_series_dim uses
    the family-only naming (``embedding_<provider>``); fhir_indicators
    uses model-version-specific names (e.g. ``embedding_qwen3``) via
    :data:`FHIR_EMBEDDING_COLUMN`.
    """
    from mirobody.utils import Config
    from mirobody.utils.config import safe_read_cfg

    provider = safe_read_cfg("DIM_EMBEDDING_PROVIDER", "gemini").lower()
    if provider not in EMBEDDING_PROVIDERS:
        raise ValueError(
            f"DIM_EMBEDDING_PROVIDER invalid: {provider!r} "
            f"(available: {sorted(EMBEDDING_PROVIDERS)})"
        )
    if table == "th_series_dim":
        embedding_column = f"embedding_{provider}"
    elif table == "fhir_indicators":
        embedding_column = FHIR_EMBEDDING_COLUMN[provider]
    else:
        raise ValueError(f"no embedding-column convention registered for table {table!r}")

    config = Config.get()
    conn = await config.get_postgresql().get_async_client(cursor_factory=None)

    cnt = 0
    async with conn:
        async with conn.cursor() as cur:
            while True:
                await cur.execute(
                    f"SELECT id, {text_column} FROM {table} "
                    f"WHERE {embedding_column} IS NULL {extra_where} "
                    f"LIMIT {BATCH_SIZE};"
                )
                rows = await cur.fetchall()
                if not rows:
                    break

                ids = [r[0] for r in rows]
                texts = [r[1] for r in rows]

                embeddings = await text_embedding(texts, provider=provider)

                for i, emb in enumerate(embeddings):
                    log.info("%d: %s  (%d dims)", cnt, texts[i], len(emb))
                    cnt += 1
                    await cur.execute(
                        f"UPDATE {table} SET {embedding_column} = %s WHERE id = %s;",
                        (emb, ids[i]),
                    )

                await conn.commit()

    log.info("embedded %d rows in %s.%s", cnt, table, embedding_column)
    return cnt


async def cmd_embed(args: Namespace) -> None:
    """Subcommand: embed — batch-fill the configured provider's embedding column.

    Provider is read from ``DIM_EMBEDDING_PROVIDER`` (default ``gemini``).
    """
    target = args.target

    if target in ("series", "all"):
        await _embed_table("th_series_dim", "original_indicator")

    if target in ("fhir", "all"):
        await _embed_table(
            "fhir_indicators", "llm_description",
            extra_where="AND llm_description IS NOT NULL",
        )
