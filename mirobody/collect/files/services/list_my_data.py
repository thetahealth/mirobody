"""How much data a person has, for the Home data bar and the Files panel.

Two numbers: how many readings, across how many departments. The frontend
consumes only those, which is why `distribution` comes back empty rather than
carrying per-bucket rows nobody reads.
"""

import logging
from typing import Any

from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


async def get_user_data_distribution(user_id: str) -> dict[str, Any]:
    """Return aggregate counts of the user's processed health data.

    Frontend (web Home DataBar / Drive "Clean data" panel) consumes only
    ``total_records`` and ``total_categories``. The per-bucket
    ``distribution`` list has no consumer and is returned empty.
    """
    try:
        user_id = str(user_id)

        logger.info(f"Getting user data distribution: user_id={user_id}")

        # `v_observation` hides amended and retracted rows, so a count off it
        # is what the Indicators tab shows. The old query read th_series_data
        # and th_series_dim.department; 90_retire.sql renames both, and
        # nothing replaced `department`, so the category is the LOINC SYSTEM
        # axis (the specimen: Ser/Plas, Bld, Urine) for coded rows, 'Other'
        # for uncoded ones, and 'genetic' when the person has genotypes.
        query = """
        SELECT
            (
                SELECT COUNT(1) FROM v_observation
                WHERE user_id = :user_id
            ) + (
                SELECT COALESCE(SUM(n_rows), 0) FROM th_genotype_set
                WHERE user_id = :user_id AND status = 'active'
            ) AS total_records,
            (
                SELECT COUNT(DISTINCT cat) FROM (
                    SELECT o.loinc_system AS cat
                    FROM v_observation o
                    WHERE o.user_id = :user_id
                      AND o.loinc_system IS NOT NULL AND TRIM(o.loinc_system) <> ''
                    UNION
                    SELECT 'Other' WHERE EXISTS (
                        SELECT 1 FROM v_observation o
                        WHERE o.user_id = :user_id
                          AND (o.loinc_system IS NULL OR TRIM(o.loinc_system) = '')
                    )
                    UNION
                    SELECT 'genetic' WHERE EXISTS (
                        SELECT 1 FROM th_genotype_set
                        WHERE user_id = :user_id AND status = 'active' AND n_rows > 0
                    )
                ) cats
            ) AS total_categories
        """

        results = await execute_query(query=query, params={"user_id": user_id})

        if results:
            row = results[0] if isinstance(results[0], dict) else dict(results[0])
            total_records = row.get("total_records") or 0
            total_categories = row.get("total_categories") or 0
        else:
            total_records = 0
            total_categories = 0

        logger.info(
            f"Query completed: user={user_id}, total_categories={total_categories}, total_records={total_records}"
        )

        return {
            "user_id": user_id,
            "total_categories": total_categories,
            "total_records": total_records,
            "distribution": [],
        }

    except Exception as e:
        logger.error(f"Failed to query data distribution: {str(e)}", stack_info=True)
        raise Exception(f"Failed to query data distribution: {str(e)}")
