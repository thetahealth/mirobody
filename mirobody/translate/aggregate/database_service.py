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

logger = logging.getLogger(__name__)


class AggregateDatabaseService:
    """Database service for aggregate indicator operations"""

    def __init__(self):
        """Initialize database service"""

    async def batch_save_summary_data(
            self,
            summary_records: list[dict[str, Any]],
            batch_size: int = 1000
    ) -> bool:
        """Write summary rows as day-grained observations. A collision with an
        equal row is skipped; a changed value amends the row it replaces.
        `batch_size` is accepted for the callers that pass it and unused: the
        writer commits one transaction per (person, provenance) group."""
        if not summary_records:
            logger.info("No summary records to save")
            return True

        try:
            written = await observations.ingest_legacy_rows(summary_records, on_conflict=observations.ON_CONFLICT_AMEND)
            logger.info(f"Successfully saved {written} summary observations")
            return True

        except Exception as e:
            logger.error(f"Error batch saving summary records: {e}")
            return False
