"""Indicator-sync task.

Producer (pulse): `await indicator_sync.add_task("")` on ingest — payload is
just a dirty flag, so multiple signals during a batch collapse into one pass.
Consumer (holywell backend_job): runs global dim-table sync + embedding
backfill once per drained batch. Payload content is ignored.
"""

from __future__ import annotations

import logging

from .base import BaseRedisTask

#-----------------------------------------------------------------------------

class IndicatorSyncTask(BaseRedisTask):
    """Queue payload: ignored (any string acts as a trigger token)."""

    queue_key = "indicator_sync_queue"

    async def process_task(self, messages: list[str]) -> None:
        # Lazy imports break the mirobody.task ↔ mirobody.pulse cycle:
        #   mirobody.task.__init__
        #     → mirobody.task.indicator_sync
        #       → mirobody.pulse...utils_sync_dim_table
        #         → pulse package init
        #           → pulse.file_parser.services.database_services
        #             → mirobody.task  (partially initialized)
        from ..pulse.file_parser.tools.utils_sync_dim_table import (
            backfill_dim_embeddings,
            sync_all_missing_indicators,
        )

        logging.info(f"indicator_sync batch start: {len(messages)} signal(s)")
        try:
            await sync_all_missing_indicators()
            await backfill_dim_embeddings()
        except Exception as e:
            logging.error(f"indicator_sync failed: {e}", stack_info=True)
            return
        logging.info("indicator_sync batch done")

#-----------------------------------------------------------------------------
