#!/usr/bin/env python3
"""
Genetic Service
Responsible for genetic data management and querying
"""

import logging

from typing import Any, Dict, List, Optional, Union

from mirobody.utils.data import DataConverter
from mirobody.utils import execute_query


class GeneticService():
    """Genetic data service"""

    def __init__(self):
        self.name = "Genetic Service"
        self.version = "1.0.0"
        self.data_converter = DataConverter()

    async def get_genetic_data(
        self,
        rsid: Union[str, List[str]],
        user_info: Dict[str, Any],
        chromosome: Optional[str] = None,
        position: Optional[int] = None,
        genotype: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        # reason: Optional[str] = None,
        include_nearby: bool = True,
        nearby_range: int = 1000000,  # Default search range: 1M base pairs before and after
    ) -> Dict[str, Any]:
        """
        Retrieve variant information by rsid, with optional lookup of nearby related variants.

        Args:
            rsid: Variant identifier(s), supports single string or string list
            chromosome: Chromosome reference
            position: Genomic position
            genotype: Genotype information (if available)
            limit: Maximum number of records to return
            offset: Pagination offset
            include_nearby: Whether to include nearby variants
            nearby_range: Search range for nearby variants (in base pairs)

        Returns:
            Dictionary containing variant data for the requested rsid(s), optionally
            including related variants in the specified nearby range.
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

            if chromosome:
                sql += " AND chromosome = :chromosome"
                params["chromosome"] = chromosome

            if position:
                sql += " AND position = :position"
                params["position"] = position

            if genotype:
                sql += " AND genotype = :genotype"
                params["genotype"] = genotype

            # Add sorting and pagination
            sql += " ORDER BY chromosome, position"
            sql += " LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset

            # Execute query
            result = await execute_query(sql, params)

            # Debug logging
            logging.info(f"Query results type: {type(result)}, length: {len(result) if result else 0}")

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
                            "exclude_rsids": tuple(queried_rsids) if queried_rsids else ("",),
                            "nearby_limit": min(20, limit),  # Return at most 20 nearby variants per variant
                        }

                        # Use raw SQL to avoid parameterized IN clause issues
                        exclude_rsids_str = ", ".join([f"'{rsid}'" for rsid in queried_rsids])
                        nearby_sql_final = nearby_sql.replace(
                            ":exclude_rsids", f"({exclude_rsids_str if exclude_rsids_str else ''})"
                        )

                        nearby_data = await execute_query(
                            nearby_sql_final,
                            {k: v for k, v in nearby_params.items() if k != "exclude_rsids"},
                        )

                        if nearby_data:
                            nearby_converted = await self.data_converter.convert_list(nearby_data)
                            # Add distance information for nearby variants and simplify data structure
                            for nearby_record in nearby_converted:
                                distance = abs(nearby_record.get("position", 0) - pos)
                                query_rsid = [
                                    r.get("rsid")
                                    for r in result
                                    if r.get("position") == pos and r.get("chromosome") == chr_key
                                ][0]

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

            logging.info(f"Query completed, returning {len(result)} genetic records, {len(nearby_results)} nearby variants")

            # Fallback strategy: if no genetic data
            if not result:
                logging.info("No genetic data found, returning structured no-data response")

                return {
                    "success": True,
                    "message": "No genetic data found. To access genetic analysis including SNPs, genotypes, chromosomes, and positions, please upload your genetic information first.",
                    "data": "No genetic data available for the requested variant(s). Please upload your genetic test results from services like 23andMe, AncestryDNA, or medical genetic testing to access personalized genetic insights.",
                    "limit": limit,
                    "offset": offset,
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
                "offset": offset,
            }
            return response_data

        except Exception as e:
            logging.error(str(e), exc_info=True)

            return {
                "success": False,
                "error": f"Failed to get genetic data: {str(e)}",
                "data": None,
                "redirect_to_upload": True,
            }
