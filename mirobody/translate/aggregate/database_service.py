"""The aggregation passes' write seam: summary rows into the observation model.

Every daily figure an aggregation pass produces (the SQL aggregator, the
derived rules, the Apple statistics upload) still arrives as a dict in the
shape the collect layer has always produced. `collect.observations` turns that
shape into drafts and writes them; a re-aggregation that changes a number
becomes an amendment of the row it replaces, never a second UPDATE.
"""

import logging
from typing import Any

from mirobody.collect import observations
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


class AggregateDatabaseService:
    """The aggregation passes' write of summary rows."""

    async def batch_save_summary_data(self, summary_records: list[dict[str, Any]]) -> bool:
        """Write summary rows as day-grained observations. A collision with an
        equal row is skipped; a changed value amends the row it replaces. The
        writer commits one transaction per (person, provenance) group. Returns
        whether the write completed."""
        if not summary_records:
            return True
        try:
            written = await observations.ingest_legacy_rows(summary_records, on_conflict=observations.ON_CONFLICT_AMEND)
        except Exception as e:
            logger.error("summary rows not written: records=%d error_type=%s", len(summary_records), type(e).__name__,
                         exc_info=not is_driver_exception(e))
            return False
        logger.info("summary rows written: records=%d inserted=%d", len(summary_records), written)
        return True
