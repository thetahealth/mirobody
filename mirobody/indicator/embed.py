"""Batch-embed command for indicator tables."""

from __future__ import annotations

import logging
from argparse import Namespace

from mirobody.utils.embedding import text_embedding

log = logging.getLogger(__name__)

# ─── Batch embed command ─────────────────────────────────────────────

BATCH_SIZE = 100


async def _embed_table(
    table: str,
    text_column: str,
    extra_where: str = "",
) -> int:
    """Embed all rows where embedding_gemini IS NULL."""
    from mirobody.utils import Config

    config = Config.get()
    conn = await config.get_postgresql().get_async_client(cursor_factory=None)

    cnt = 0
    async with conn:
        async with conn.cursor() as cur:
            while True:
                await cur.execute(
                    f"SELECT id, {text_column} FROM {table} "
                    f"WHERE embedding_gemini IS NULL {extra_where} "
                    f"LIMIT {BATCH_SIZE};"
                )
                rows = await cur.fetchall()
                if not rows:
                    break

                ids = [r[0] for r in rows]
                texts = [r[1] for r in rows]

                embeddings = await text_embedding(texts)

                for i, emb in enumerate(embeddings):
                    log.info("%d: %s  (%d dims)", cnt, texts[i], len(emb))
                    cnt += 1
                    await cur.execute(
                        f"UPDATE {table} SET embedding_gemini = %s WHERE id = %s;",
                        (emb, ids[i]),
                    )

                await conn.commit()

    log.info("embedded %d rows in %s", cnt, table)
    return cnt


async def cmd_embed(args: Namespace) -> None:
    """Subcommand: embed — batch-fill embedding_gemini for DB rows."""
    target = args.target

    if target in ("series", "all"):
        await _embed_table("th_series_dim", "original_indicator")

    if target in ("fhir", "all"):
        await _embed_table(
            "fhir_indicators", "llm_description",
            extra_where="AND llm_description IS NOT NULL",
        )
