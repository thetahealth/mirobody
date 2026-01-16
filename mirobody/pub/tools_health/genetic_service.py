#!/usr/bin/env python3
"""
Genetic Service
Responsible for genetic data management and querying
"""

import logging

from typing import Any, Dict, List, Optional, Union

from mirobody.utils.data import DataConverter
from mirobody.utils.db import execute_query


class GeneticService():
    """Genetic data service"""

    # Constants
    MAX_NEARBY_VARIANTS_PER_QUERY = 20
    DEFAULT_NEARBY_RANGE = 1000000  # 1M base pairs

    def __init__(self):
        self.name = "Genetic Service"
        self.version = "1.0.0"
        self.data_converter = DataConverter()

    def _build_rsid_conditions(
        self, rsid: Union[str, List[str]], params: Dict[str, Any]
    ) -> str:
        """
        Build SQL conditions for rsid parameter.
        
        Args:
            rsid: Variant identifier(s), supports single string or string list
            params: Parameter dictionary to update
            
        Returns:
            SQL condition string
        """
        # Handle comma-separated string
        if isinstance(rsid, str) and "," in rsid:
            rsid_list = [r.strip() for r in rsid.split(",") if r.strip()]
            placeholders = [f":rsid_{i}" for i in range(len(rsid_list))]
            for i, r in enumerate(rsid_list):
                params[f"rsid_{i}"] = r
            return f" AND rsid IN ({', '.join(placeholders)})"
        
        # Handle list
        elif isinstance(rsid, list):
            if len(rsid) == 1:
                params["rsid"] = rsid[0]
                return " AND rsid = :rsid"
            else:
                placeholders = [f":rsid_{i}" for i in range(len(rsid))]
                for i, r in enumerate(rsid):
                    params[f"rsid_{i}"] = r
                return f" AND rsid IN ({', '.join(placeholders)})"
        
        # Single rsid
        else:
            params["rsid"] = rsid
            return " AND rsid = :rsid"

    def _format_compact_record(
        self, record: Dict[str, Any], distance: Optional[int] = None, query_rsid: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Format record into compact format.
        
        Args:
            record: Original record
            distance: Distance from query position (for nearby variants)
            query_rsid: Query rsid (for nearby variants)
            
        Returns:
            Compact formatted record
        """
        compact_record = {
            "r": record.get("rsid"),
            "c": record.get("chromosome"),
            "p": record.get("position"),
            "g": record.get("genotype"),
        }
        
        if distance is not None:
            compact_record["d"] = distance
        if query_rsid is not None:
            compact_record["q"] = query_rsid
            
        # Keep only non-null values
        return {k: v for k, v in compact_record.items() if v is not None}

    async def _query_nearby_variants(
        self,
        user_id: str,
        queried_positions: Dict[str, List[int]],
        queried_rsids: set,
        nearby_range: int,
        limit: int,
        result: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Query nearby variants for all queried positions in a single optimized query.
        
        Args:
            user_id: User identifier
            queried_positions: Dictionary of chromosome -> positions
            queried_rsids: Set of already queried rsids to exclude
            nearby_range: Search range in base pairs
            limit: Maximum results per variant
            result: Original query results
            
        Returns:
            List of nearby variants in compact format
        """
        if not queried_positions:
            return []
        
        nearby_results = []
        nearby_limit = min(self.MAX_NEARBY_VARIANTS_PER_QUERY, limit)
        
        # Build optimized query using UNION ALL for multiple positions
        union_queries = []
        union_params = {"user_id": user_id, "nearby_limit": nearby_limit}
        param_index = 0
        
        for chr_key, positions in queried_positions.items():
            for pos in positions:
                # Create unique parameter names for each position
                chr_param = f"chr_{param_index}"
                min_pos_param = f"min_pos_{param_index}"
                max_pos_param = f"max_pos_{param_index}"
                target_pos_param = f"target_pos_{param_index}"
                
                union_params[chr_param] = chr_key
                union_params[min_pos_param] = pos - nearby_range
                union_params[max_pos_param] = pos + nearby_range
                union_params[target_pos_param] = pos
                
                # Build exclude clause
                exclude_clause = ""
                if queried_rsids:
                    exclude_rsids_list = list(queried_rsids)
                    exclude_params = [f"exclude_{param_index}_{i}" for i in range(len(exclude_rsids_list))]
                    for i, rsid in enumerate(exclude_rsids_list):
                        union_params[f"exclude_{param_index}_{i}"] = rsid
                    exclude_clause = f"AND rsid NOT IN ({', '.join([':' + p for p in exclude_params])})"
                
                union_queries.append(f"""
                    SELECT 
                        rsid, chromosome, position, genotype,
                        ABS(position - :{target_pos_param}) as distance,
                        :{target_pos_param} as query_position,
                        :{chr_param} as query_chromosome
                    FROM theta_ai.th_series_data_genetic
                    WHERE user_id = :user_id 
                      AND is_deleted = false
                      AND chromosome = :{chr_param}
                      AND position BETWEEN :{min_pos_param} AND :{max_pos_param}
                      {exclude_clause}
                """)
                
                param_index += 1
        
        # Combine all queries with UNION ALL and apply global ordering and limit
        if union_queries:
            combined_sql = f"""
                SELECT * FROM (
                    {' UNION ALL '.join(union_queries)}
                ) AS combined
                ORDER BY query_chromosome, query_position, distance
                LIMIT :nearby_limit
            """
            
            nearby_data = await execute_query(
                combined_sql,
                union_params,
                mode="async",
                query_type="select",
            )
            
            if nearby_data:
                nearby_converted = await self.data_converter.convert_list(nearby_data)
                
                # Format results
                for nearby_record in nearby_converted:
                    # Find the corresponding query rsid
                    query_pos = nearby_record.get("query_position")
                    query_chr = nearby_record.get("query_chromosome")
                    query_rsid = next(
                        (r.get("rsid") for r in result 
                         if r.get("position") == query_pos and r.get("chromosome") == query_chr),
                        None
                    )
                    
                    compact_record = self._format_compact_record(
                        nearby_record,
                        distance=nearby_record.get("distance"),
                        query_rsid=query_rsid
                    )
                    nearby_results.append(compact_record)
        
        return nearby_results

    async def get_genetic_data(
        self,
        rsid: Union[str, List[str]],
        user_info: Dict[str, Any],
        chromosome: Optional[str] = None,
        position: Optional[int] = None,
        genotype: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        include_nearby: bool = True,
        nearby_range: Optional[int] = None,
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
            if not user_id:
                return {
                    "success": False,
                    "error": "User ID is required",
                    "data": None,
                }
            
            # Set default nearby range
            if nearby_range is None:
                nearby_range = self.DEFAULT_NEARBY_RANGE

            # Build basic query
            sql = """
            SELECT id, user_id, rsid, chromosome, position, genotype, 
                   create_time, update_time
            FROM theta_ai.th_series_data_genetic
            WHERE user_id = :user_id AND is_deleted = false
            """

            # Build parameter dictionary
            params = {"user_id": user_id}

            # Handle rsid parameter using helper method
            sql += self._build_rsid_conditions(rsid, params)

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
            result = await execute_query(sql, params, mode="async", query_type="select")

            # Debug logging
            logging.info(f"Query results type: {type(result)}, length: {len(result) if result else 0}")

            # Data conversion
            result = await self.data_converter.convert_list(result)

            # Convert to compact format using helper method
            compact_result = [self._format_compact_record(record) for record in result]

            # Collect queried variant information (optimized)
            queried_positions = {}
            queried_rsids = {record.get("rsid") for record in result if record.get("rsid")}
            
            for record in result:
                if record.get("chromosome") and record.get("position"):
                    chr_key = record["chromosome"]
                    queried_positions.setdefault(chr_key, []).append(record["position"])

            # Query nearby variants using optimized single-query approach
            nearby_results = []
            if include_nearby and queried_positions:
                nearby_results = await self._query_nearby_variants(
                    user_id=user_id,
                    queried_positions=queried_positions,
                    queried_rsids=queried_rsids,
                    nearby_range=nearby_range,
                    limit=limit,
                    result=result,
                )

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
                    "upload_suggestion": {
                        "message": "No genetic data available for analysis. To get personalized genetic insights and understand your genetic variations, please upload your genetic test results.",
                        "upload_url": "https://localhost:18080/drive",
                        "instructions": "Upload your genetic test results from services like 23andMe, AncestryDNA, or medical genetic testing to enable comprehensive genetic analysis and health risk assessment.",
                    },
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
            logging.error(str(e), stack_info=True)

            return {
                "success": False,
                "error": f"Failed to get genetic data: {str(e)}",
                "data": None,
                "redirect_to_upload": True,
            }
