#!/usr/bin/env python3
"""
Genetic Service
Responsible for genetic data management and querying
"""

import logging

from typing import Any

from mirobody.utils.data import DataConverter
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


class GeneticService:
    """Genetic data service"""

    def __init__(self):
        self.name = "Genetic Service"
        self.version = "1.0.0"
        self.data_converter = DataConverter()

    # The tool signature used to also take chromosome / position / genotype /
    # offset. All four "narrow an already-matched set" — with rsid required
    # they had no realistic use, and every parameter is schema the model must
    # read on every call. Removed rather than documented better.
    async def get_genetic_data(
        self,
        rsid: str | list[str],
        user_info: dict[str, Any],
        limit: int = 100,
        include_nearby: bool = True,
        nearby_range: int = 1000000,  # Default search range: 1M base pairs before and after
    ) -> dict[str, Any]:
        """
        Look up this user's genotype at specific variants (rsIDs), optionally
        with nearby variants from the same region.

        Reads THEIR uploaded genotype file, not a reference database. Consumer
        arrays type a small fraction of the genome, so absent ≠ negative: an
        rsID missing from the result was not typed, it says nothing about the
        allele.

        Args:
            rsid: dbSNP identifiers, e.g. "rs4988235" or ["rs1801133", "rs429358"].
                A comma-separated string also works.
            limit: Max variants returned (default 100).
            include_nearby: Also return variants within `nearby_range` of each
                hit, capped at 20 per hit. Set false for exact lookups only.
            nearby_range: Half-window in base pairs (default 1,000,000).

        Returns:
            success: whether the lookup completed.
            data: matched variants — rsid, chromosome, position, genotype call.
            nearby: neighbours by POSITION only. Proximity is not linkage —
                do not present them as related to the queried variant's trait.

        Notes for LLMs:
            - Report genotypes; do not interpret risk. Genotype calls are not
              diagnoses; direct clinical questions to a genetic counsellor.
            - Genotypes are unphased: "AG" does not say which parent
              contributed which allele.
        """
        try:
            # Get user ID from user_info
            user_id = user_info.get("user_id")

            # Build basic query
            sql = """
            SELECT id, user_id, rsid, chromosome, position, genotype, 
                   create_time, update_time
            FROM th_series_data_genetic
            WHERE user_id = :user_id AND is_deleted = false
            """

            # Build parameter dictionary
            params = {"user_id": user_id}

            # Handle rsid parameter (supports list or comma-separated string)
            if isinstance(rsid, str) and "," in rsid:
                # Handle comma-separated string
                rsid_list = [r.strip() for r in rsid.split(",") if r.strip()]
                placeholders = [f":rsid_{i}" for i in range(len(rsid_list))]
                sql += f" AND rsid IN ({', '.join(placeholders)})"
                for i, r in enumerate(rsid_list):
                    params[f"rsid_{i}"] = r
            elif isinstance(rsid, list):
                if len(rsid) == 1:
                    # If only one element, use equals operator directly
                    sql += " AND rsid = :rsid"
                    params["rsid"] = rsid[0]
                else:
                    # Use IN operator to support multiple rsids
                    placeholders = [f":rsid_{i}" for i in range(len(rsid))]
                    sql += f" AND rsid IN ({', '.join(placeholders)})"
                    for i, r in enumerate(rsid):
                        params[f"rsid_{i}"] = r
            else:
                # Single rsid
                sql += " AND rsid = :rsid"
                params["rsid"] = rsid

            # Add sorting and pagination
            sql += " ORDER BY chromosome, position"
            sql += " LIMIT :limit"
            params["limit"] = limit

            # Execute query
            result = await execute_query(sql, params)

            # Debug logging
            logger.info(f"Query results type: {type(result)}, length: {len(result) if result else 0}")

            # Data conversion
            result = await self.data_converter.convert_list(result)

            # Convert to compact format
            compact_result = []
            for record in result:
                compact_record = {
                    "r": record.get("rsid"),  # rsid
                    "c": record.get("chromosome"),  # chromosome
                    "p": record.get("position"),  # position
                    "g": record.get("genotype"),  # genotype
                }
                # Keep only non-null values
                compact_record = {k: v for k, v in compact_record.items() if v is not None}
                compact_result.append(compact_record)

            # Collect queried variant information
            queried_positions = {}
            queried_rsids = set()
            for record in result:
                if record.get("chromosome") and record.get("position"):
                    chr_key = record["chromosome"]
                    if chr_key not in queried_positions:
                        queried_positions[chr_key] = []
                    queried_positions[chr_key].append(record["position"])
                    queried_rsids.add(record.get("rsid"))

            # If need to include nearby variants and have query results
            nearby_results = []
            if include_nearby and queried_positions:
                for chr_key, positions in queried_positions.items():
                    for pos in positions:
                        # Build SQL to query nearby variants
                        nearby_sql = """
                        SELECT id, user_id, rsid, chromosome, position, genotype, 
                               create_time, update_time
                        FROM th_series_data_genetic
                        WHERE user_id = :user_id 
                          AND is_deleted = false
                          AND chromosome = :chromosome
                          AND position BETWEEN :min_pos AND :max_pos
                          AND rsid NOT IN :exclude_rsids
                        ORDER BY ABS(position - :target_pos)
                        LIMIT :nearby_limit
                        """

                        nearby_params = {
                            "user_id": user_id,
                            "chromosome": chr_key,
                            "min_pos": pos - nearby_range,
                            "max_pos": pos + nearby_range,
                            "target_pos": pos,
                            "nearby_limit": min(20, limit),  # Return at most 20 nearby variants per variant
                        }

                        # One bind parameter per excluded rsid.
                        #
                        # This previously interpolated the rsids straight into the
                        # SQL text ("Use raw SQL to avoid parameterized IN clause
                        # issues"), which was a STORED SQL injection: rsids are
                        # parsed out of a user-uploaded genotype file in
                        # genetic_processor.py (`rsid, chromosome, position_str,
                        # genotype = parts[:4]` then `.strip()` — no format
                        # validation whatsoever), stored via a safe parameterized
                        # INSERT, and then spliced into a query string on read. A
                        # single quote inside an uploaded file was enough to break
                        # out of the literal. Expanding the IN clause into named
                        # parameters leaves quoting to the driver.
                        exclude_keys = []
                        for i, rsid_val in enumerate(queried_rsids):
                            key = f"excl_{i}"
                            nearby_params[key] = rsid_val
                            exclude_keys.append(f":{key}")
                        nearby_sql_final = nearby_sql.replace(
                            ":exclude_rsids",
                            f"({', '.join(exclude_keys)})" if exclude_keys else "('')",
                        )

                        nearby_data = await execute_query(nearby_sql_final, nearby_params)

                        if nearby_data:
                            nearby_converted = await self.data_converter.convert_list(nearby_data)
                            # Add distance information for nearby variants and simplify data structure
                            for nearby_record in nearby_converted:
                                distance = abs(nearby_record.get("position", 0) - pos)
                                # `next(..., None)` rather than `[...][0]`: `result` and the
                                # nearby rows come from two separate queries against a table a
                                # concurrent upload can extend, so the position/chromosome pair
                                # is not guaranteed to still be present. The bare index raised
                                # IndexError out of the tool and lost the whole response,
                                # including the variants that HAD resolved.
                                query_rsid = next(
                                    (
                                        r.get("rsid")
                                        for r in result
                                        if r.get("position") == pos and r.get("chromosome") == chr_key
                                    ),
                                    None,
                                )

                                # Create more compact record format
                                compact_record = {
                                    "r": nearby_record.get("rsid"),  # rsid
                                    "c": nearby_record.get("chromosome"),  # chromosome
                                    "p": nearby_record.get("position"),  # position
                                    "g": nearby_record.get("genotype"),  # genotype
                                    "d": distance,  # distance
                                    "q": query_rsid,  # query rsid
                                }
                                # Keep only non-null values
                                compact_record = {k: v for k, v in compact_record.items() if v is not None}
                                nearby_results.append(compact_record)

            logger.info(f"Query completed, returning {len(result)} genetic records, {len(nearby_results)} nearby variants")

            # Fallback strategy: if no genetic data
            if not result:
                logger.info("No genetic data found, returning structured no-data response")

                return {
                    "success": True,
                    "message": "No genetic data found. To access genetic analysis including SNPs, genotypes, chromosomes, and positions, please upload your genetic information first.",
                    "data": "No genetic data available for the requested variant(s). Please upload your genetic test results from services like 23andMe, AncestryDNA, or medical genetic testing to access personalized genetic insights.",
                    "limit": limit,
                    "redirect_to_upload": True,
                }

            # Apply data truncation with compact format
            response_data = {
                "success": True,
                "data": {
                    "q": compact_result,  # queried variants
                    "n": nearby_results if include_nearby else [],  # nearby variants
                    "s": {  # summary
                        "tq": len(result),  # total queried
                        "tn": len(nearby_results) if include_nearby else 0,  # total nearby
                        "chr": list(queried_positions.keys()),  # chromosomes
                        "range_kb": nearby_range // 1000 if include_nearby else 0,  # range in kb
                    },
                    "_legend": {
                        "r": "rsid",
                        "c": "chromosome",
                        "p": "position",
                        "g": "genotype",
                        "d": "distance_from_query",
                        "q": "query_rsid",
                        "tq": "total_queried",
                        "tn": "total_nearby",
                        "chr": "chromosomes",
                    },
                },
                "limit": limit,
            }
            return response_data

        except Exception as e:
            logger.error(str(e), exc_info=True)

            return {
                "success": False,
                "error": f"Failed to get genetic data: {str(e)}",
                "data": None,
                "redirect_to_upload": True,
            }
