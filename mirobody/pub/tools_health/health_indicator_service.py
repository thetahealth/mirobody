import json
import logging

from datetime import datetime, timedelta
from typing import Any

from mirobody.utils import execute_query

class HealthIndicatorService:
    def __init__(self):
        self.name = "Indicator Service"
        self.version = "3.0.0"

    #-------------------------------------------------------------------------

    def _build_time_clause(
        self,
        params: dict,
        start_time: str | None = None,
        end_time: str | None = None,
        time_column: str = "tsd.start_time",
    ) -> str:
        conditions = []
        try:
            if start_time:
                date_part = start_time[:10] if len(start_time) >= 10 else start_time
                params["start_time"] = datetime.strptime(date_part + " 00:00:00", "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
                conditions.append(f"AND {time_column} >= :start_time")
        except Exception as e:
            logging.warning(f"Failed to parse start_time '{start_time}': {e}")
        try:
            if end_time:
                date_part = end_time[:10] if len(end_time) >= 10 else end_time
                params["end_time"] = datetime.strptime(date_part + " 23:59:59", "%Y-%m-%d %H:%M:%S") + timedelta(days=1)
                conditions.append(f"AND {time_column} <= :end_time")
        except Exception as e:
            logging.warning(f"Failed to parse end_time '{end_time}': {e}")
        return " ".join(conditions)

    #-------------------------------------------------------------------------

    async def search_health_indicators(
        self,
        user_info: dict[str, Any],
        keywords: list[str],
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[str, Any]:
        """
        Search for relevant health indicators.

        Args:
            keywords: List of search keywords, provide 2 or more. For medical abbreviations (e.g. MCHC, HbA1c, BMI), always include BOTH the abbreviation AND the full name. e.g. ["MCHC", "Mean Corpuscular Hemoglobin Concentration"]
            start_time: Start date filter ("YYYY-MM-DD").
            end_time: End date filter ("YYYY-MM-DD").
        """
        try:
            user_id = user_info.get("user_id")
            if not user_id or not isinstance(user_id, str):
                return {"success": False, "error": "Authorization required."}

            # qwen and other LLMs may send keywords as None or a comma string;
            # normalize so iteration never crashes (qwen frequently passes None).
            if isinstance(keywords, str):
                keywords = [k.strip() for k in keywords.split(",") if k.strip()]
            keyword_list = list(dict.fromkeys(
                k.strip() for k in (keywords or []) if isinstance(k, str) and k.strip()
            ))

            if not keyword_list:
                return {"success": False, "error": "At least one keyword must be provided."}

            from mirobody.indicator.search import search
            from mirobody.indicator.fhir.adapter import FhirAdapter
            from mirobody.utils import safe_read_cfg

            bundle_dir = safe_read_cfg("FHIR_INDICATORS_DIR")
            adapter = FhirAdapter(bundle_dir=bundle_dir)
            indicators = await search(
                adapter    = adapter,
                user_id    = user_id,
                keywords   = keyword_list,
                start_time = start_time,
                end_time   = end_time,
            )

            if indicators:
                _drop = {"id", "score", "system", "code", "description"}
                indicators = [{k: v for k, v in ind.items() if k not in _drop} for ind in indicators]

            return {
                "success": True,
                "message": "Ok" if indicators else "No matching indicators found",
                "indicators": indicators if indicators else None,
            }

        except Exception as e:
            logging.error(f"[SearchIndicator] Error: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    #-------------------------------------------------------------------------

    async def _get_file_key_for_data_ids(self, data_ids: list[int]) -> dict[int, str]:
        """
        Get file_key for a list of th_series_data IDs for traceable data sources.
        The file_key can be used with fetch file tool to download the original source file.
        """
        if not data_ids:
            return {}

        file_key_map = {}

        try:
            # Get file_key from th_series_data.source_table_id (source_table='th_files').
            # The legacy EHR-file-records lookup was removed: ehr_file_records does
            # not exist in mirobody.
            remaining_ids = list(data_ids)

            if remaining_ids:
                th_files_query = """
                SELECT
                    id,
                    source_table_id
                FROM th_series_data
                WHERE id = ANY(:remaining_ids)
                  AND source_table = 'th_files'
                  AND source_table_id IS NOT NULL
                """

                th_files_result = await execute_query(th_files_query, {"remaining_ids": remaining_ids})

                if th_files_result:
                    for row in th_files_result:
                        data_id = row["id"]
                        source_table_id = row["source_table_id"]

                        if source_table_id and "_#_" in source_table_id:
                            file_key = source_table_id.split("_#_")[0]
                            file_key_map[data_id] = file_key

            return file_key_map

        except Exception as e:
            logging.error(f"Error getting file_keys: {str(e)}", exc_info=True)
            return {}

    async def _fetch_indicator_data_batch(
        self,
        user_id: str,
        indicator_names: list[str],
        start_time: str | None,
        end_time: str | None,
        limit: int,
    ) -> dict[str, list[dict]]:
        """Fetch data for all indicators in one query.

        Returns: {indicator_name: [record, ...]}
        """
        indicator_records: dict[str, list[dict]] = {n: [] for n in indicator_names}

        if not indicator_names:
            return indicator_records

        try:
            params: dict[str, Any] = {
                "user_id": user_id,
                "indicator_names": indicator_names,
                "limit": limit,
            }
            time_clause = self._build_time_clause(
                params, start_time, end_time,
                "tsd.start_time",
            )

            sql = f"""
            SELECT * FROM (
                SELECT
                    tsd.id,
                    tsd.indicator AS indicator,
                    tsd.value AS value,
                    tsd.fhir_mapping_info AS info,
                    tsd.start_time AS start_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY tsd.indicator
                        ORDER BY tsd.start_time DESC
                    ) AS rn
                FROM th_series_data tsd
                WHERE tsd.user_id = :user_id
                  AND tsd.indicator = ANY(:indicator_names)
                  AND tsd.deleted = 0
                  {time_clause}
            ) sub
            WHERE rn <= :limit
            ORDER BY indicator
            """

            result = await execute_query(sql, params)
            if not result:
                return indicator_records

            for row in result:
                record = {
                    "id"   : row["id"],
                    "time" : str(row["start_time"]) if row["start_time"] is not None else "",
                    "value": str(row["value"]) if row["value"] is not None else "",
                }
                if (row["info"] is not None) and ("unit" in row["info"]):
                    record["unit"] = row["info"]["unit"]

                indicator_records.setdefault(row["indicator"], []).append(record)

            return indicator_records

        except Exception as e:
            logging.error(f"[FetchIndicatorBatch] Error: {e}", exc_info=True)
            return indicator_records

    #-------------------------------------------------------------------------

    async def fetch_health_data(
        self,
        user_info: dict[str, Any],
        indicators: list[str],
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """
        Fetch detailed data for a specific indicator.
        Have to invoke search_health_indicators beforehand.

        Args:
            indicators: Exact indicator names (from search_health_indicators results)
            start_time: Start time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            end_time: End time filter (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
            limit: Maximum number of records to return (default: 100)
        """
        try:
            user_id = user_info.get("user_id")
            if not user_id or not isinstance(user_id, str):
                return {"success": False, "error": "Authorization required."}

            # LLMs (e.g. qwen) may send limit as None/0/str; the SQL `rn <= :limit`
            # then becomes `rn <= NULL` and drops every row. Coerce to a default.
            try:
                limit = int(limit)
            except (TypeError, ValueError):
                limit = 100
            if limit <= 0:
                limit = 100

            if isinstance(indicators, list):
                indicator_names = indicators
            elif isinstance(indicators, str):
                indicator_names = indicators.split(",")
            else:
                indicator_names = None
            
            if not indicator_names:
                return {"success": False, "error": "Indicator name cannot be empty"}
            
            logging.info(f"[FetchIndicator] user={user_id}, indicator={indicator_names}, time={start_time}~{end_time}, limit={limit}")
            
            # Clean up indicator names
            cleaned_names = []
            for name in indicator_names:
                if isinstance(name, str) and (name := name.strip()):
                    cleaned_names.append(name)
            if not cleaned_names:
                return {"success": False, "error": "Indicator name cannot be empty"}

            # Query 1: fetch data for all indicators
            indicator_records = await self._fetch_indicator_data_batch(
                user_id, cleaned_names, start_time, end_time, limit
            )

            # Query 2: get file_keys for all records
            all_data_ids = [
                r["id"] for records in indicator_records.values()
                for r in records if r.get("id")
            ]
            file_key_map = await self._get_file_key_for_data_ids(all_data_ids) if all_data_ids else {}

            # Assemble results
            result_indicators = {}
            for name in cleaned_names:
                records = indicator_records.get(name, [])

                for record in records:
                    data_id = record.pop("id", None)
                    if data_id and data_id in file_key_map:
                        record["file_key"] = file_key_map[data_id]

                result_indicator: dict[str, Any] = {"count": len(records)}
                if records:
                    result_indicator["data"] = records
                result_indicators[name] = result_indicator

            return {
                "success": True,
                "message": "Ok" if result_indicators else "No data found",
                "indicators": result_indicators if result_indicators else None
            }

        except Exception as e:
            logging.error(f"[FetchIndicator] Error: {e}", exc_info=True)
            return {"success": False, "error": str(e), "indicators": "No data found"}

#-----------------------------------------------------------------------------
