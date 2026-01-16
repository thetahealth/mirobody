"""
Standard Health data service
"""

import logging
import time

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from .base import BaseHealthService
from ..models.requests import StandardPulseData
from ..repositories.health_data import HealthDataRepository
from ...core.indicators_info import is_summary_indicator, is_series_indicator
from ...core.user import ThetaUserService
from ....utils import execute_query


class StandardHealthService(BaseHealthService):
    """Standard health data service"""

    # Vital Health type mapping
    TYPE_MAPPING = {
        "HEART_RATE": "heart_rate",
        "BLOOD_PRESSURE_SYSTOLIC": "blood_pressure_systolic",
        "BLOOD_PRESSURE_DIASTOLIC": "blood_pressure_diastolic",
        "TEMPERATURE": "body_temperature",
        "OXYGEN_SATURATION": "oxygen_saturation",
        "RESPIRATORY_RATE": "respiratory_rate",
    }

    def __init__(self, repository: HealthDataRepository = None):
        self.user_service = ThetaUserService()
        super().__init__(repository)

    def get_service_name(self) -> str:
        """Get service name"""
        return "standard_health"

    async def _get_user_timezone(self, user_id: str) -> str:
        """
        Get user's default timezone from database
        
        Args:
            user_id: User ID
            
        Returns:
            str: User timezone, defaults to UTC
        """
        try:
            user_info = await self.user_service.get_user_by_id(user_id)
            if user_info and user_info.get("tz"):
                user_timezone = user_info.get("tz").strip()
                if user_timezone:
                    logging.info(f"Retrieved user timezone from database: user_id={user_id}, timezone={user_timezone}")
                    return user_timezone

            logging.info(f"No timezone found for user {user_id}, using default UTC")
            return "UTC"

        except Exception as e:
            logging.warning(f"Failed to get user timezone for user {user_id}: {str(e)}, using default UTC")
            return "UTC"

    async def process_standard_data(self, standard_data: StandardPulseData, current_user: str) -> bool:
        """
        Directly process health data in StandardPulseData format

        Determine data storage location based on indicator type:
        - Summary indicators: Store to th_series_data table
        - Regular indicators: Store to series_data table

        Args:
            standard_data: Health data in StandardPulseData format
            current_user: Authenticated current user ID

        Returns:
            bool: Whether processing succeeded
        """
        try:
            user_id = current_user
            health_data = standard_data.healthData
            logging.info(f"Starting to process data, user_id: {user_id}, count: {len(health_data)}")
            t1 = time.time()

            # Classify and preprocess data
            summary_records, series_records = await self._classify_and_prepare_records(health_data, user_id)

            t2 = time.time()
            logging.info(f"Data classification and preparation: {(t2 - t1) * 1000}ms")

            # Batch process data
            summary_success, summary_count = await self._batch_save_summary_records(summary_records)
            series_success, series_count = await self._batch_save_series_records(series_records)

            t3 = time.time()

            # Collect statistics
            overall_success = summary_success and series_success
            total_processed = summary_count + series_count

            logging.info(f"Batch processing completed: {total_processed}/{len(health_data)} records for user {user_id} (summary: {summary_count}, series: {series_count}), timeCost={(t3 - t2) * 1e3}ms")

            return overall_success

        except Exception as e:
            logging.error(f"Error processing StandardPulseData: {str(e)}", stack_info=True)
            return False

    async def _classify_and_prepare_records(self, health_data: List, user_id: str) -> tuple[
        List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Classify and preprocess health data records
        
        Args:
            health_data: Raw health data list
            user_id: User ID
            
        Returns:
            tuple: (summary_records, series_records)
        """
        summary_records = []
        series_records = []

        user_timezone = await self._get_user_timezone(user_id)

        for record in health_data:
            try:
                record_dict = record.model_dump()
                indicator = record_dict.get("type")

                processed_record = await self._prepare_common_record_data(record_dict, user_id, user_timezone)
                if not processed_record:
                    continue

                if is_summary_indicator(indicator):
                    summary_record = self._prepare_summary_record(processed_record)
                    if summary_record:
                        summary_records.append(summary_record)
                if is_series_indicator(indicator):
                    series_record = self._prepare_series_record(processed_record)
                    if series_record:
                        series_records.append(series_record)

            except Exception as e:
                logging.warning(f"Failed to process single record: {str(record.model_dump())}, error: {e}")
                continue

        logging.info(f"Classified records: {len(summary_records)} summary, {len(series_records)} series")
        return summary_records, series_records

    async def _prepare_common_record_data(self, record: Dict[str, Any], user_id: str, user_timezone: str = None) -> Dict[str, Any]:
        try:
            source = record.get("source", "UNKNOWN")
            indicator = record.get("type")
            timestamp = record.get("timestamp")
            unit = record.get("unit", "")
            value = record.get("value", 0)
            timezone_info = record.get("timezone", "UTC")
            if timezone_info == "UTC":
                timezone_info = user_timezone if user_timezone else await self._get_user_timezone(user_id)

            source_id = record.get("source_id", "")
            task_id = record.get("task_id", "")
            custom_comment = record.get("comment", "")  # Extract custom comment from record

            start_time_ms = record.get("startTime")
            end_time_ms = record.get("endTime")

            try:
                _v = float(value)
                normalized_value, _ = self.normalize_health_data_unit(
                    indicator,
                    _v,
                    unit,
                    percentage_handling=True if source in ['apple_health'] else False
                )
            except:
                normalized_value = value

            record_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).replace(tzinfo=None)
            return {
                "user_id": user_id,
                "indicator": indicator,
                "source": source.lower(),
                "value": str(normalized_value),
                "timestamp": timestamp,
                "record_time": record_time,
                "unit": unit,
                "timezone": timezone_info,
                "source_id": source_id,
                "task_id": task_id,
                "custom_comment": custom_comment,  # Pass custom comment through

                "original_start_time_ms": start_time_ms,
                "original_end_time_ms": end_time_ms,
            }

        except Exception as e:
            logging.error(f"Error preparing common record data: {str(e)}", stack_info=True)
            return None

    def _prepare_summary_record(self, common_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare specific fields for Summary data
        
        Args:
            common_data: Common data
            
        Returns:
            Dict[str, Any]: Summary record data
        """
        try:
            # Calculate time range
            start_time, end_time = self._calculate_summary_time_range_from_common(common_data)

            if not start_time or not end_time:
                return None

            # Build system-generated comment
            system_comment = f"Source: {common_data['source']}, Unit: {common_data['unit']}，timezone: {common_data['timezone']}"
            
            # Merge with custom comment if provided (merge mode, not overwrite)
            custom_comment = common_data.get("custom_comment", "")
            if custom_comment:
                final_comment = f"{system_comment}, {custom_comment}"
            else:
                final_comment = system_comment

            return {
                **common_data,
                "start_time": start_time,
                "end_time": end_time,
                "source_table": "",  # Source table name
                "source_table_id": common_data.get("source_id", ""),
                "comment": final_comment,
                "indicator_id": f"",
            }

        except Exception as e:
            logging.error(f"Error preparing summary record: {str(e)}", stack_info=True)
            return None

    def _prepare_series_record(self, common_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare specific fields for Series data
        
        Args:
            common_data: Common data
            
        Returns:
            Dict[str, Any]: Series record data
        """
        try:
            return {
                "user_id": common_data["user_id"],
                "indicator": common_data["indicator"],
                "value": common_data["value"],
                "start_time": common_data["record_time"],  # Series uses timestamp as time point
                "end_time": common_data["record_time"],
                "source": common_data["source"],
                "timezone": common_data["timezone"],
                "record_type": "vital_health",
                "source_id": common_data["source_id"],
                "task_id": common_data["task_id"],
            }

        except Exception as e:
            logging.error(f"Error preparing series record: {str(e)}", stack_info=True)
            return None

    async def _batch_save_summary_records(self, summary_records: List[Dict[str, Any]]) -> tuple[bool, int]:
        """
        Batch save Summary records to th_series_data table
        
        Args:
            summary_records: Summary record list
            
        Returns:
            tuple: (success, processed_count)
        """
        if not summary_records:
            return True, 0

        try:
            query = """
            INSERT INTO theta_ai.th_series_data (
                user_id, indicator, value, start_time, end_time, source_table, 
                source_table_id, comment, indicator_id, source, task_id,
                create_time, update_time, deleted
            ) VALUES (
                :user_id, :indicator, :value, :start_time, :end_time, :source_table,
                :source_table_id, :comment, :indicator_id, :source, :task_id,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0
            )
            ON CONFLICT (user_id, indicator, start_time, end_time) 
            DO UPDATE SET
                value = EXCLUDED.value,
                source_table = EXCLUDED.source_table,
                source_table_id = EXCLUDED.source_table_id,
                comment = EXCLUDED.comment,
                source = EXCLUDED.source,
                task_id = EXCLUDED.task_id,
                update_time = CURRENT_TIMESTAMP
            """

            batch_size = 1000
            total_processed = 0

            logging.info(f"About to save {len(summary_records)} summary records to th_series_data")
            for record in summary_records[:2]:  # Log first 2 records for debugging
                logging.info(f"Sample record: {record}")

            for i in range(0, len(summary_records), batch_size):
                batch = summary_records[i:i + batch_size]
                logging.info(f"Executing batch {i // batch_size + 1} with {len(batch)} records")
                result = await execute_query(query=query, params=batch)
                logging.info(f"Batch execution result: {result}")
                total_processed += len(batch)
                logging.info(f"Processed summary batch {i // batch_size + 1}: {len(batch)} records")

            logging.info(f"Successfully batch saved {total_processed} summary records to th_series_data")
            return True, total_processed

        except Exception as e:
            logging.error(f"Error batch saving summary records: {str(e)}", stack_info=True)
            return False, 0

    async def _batch_save_series_records(self, series_records: List[Dict[str, Any]]) -> tuple[bool, int]:
        if not series_records:
            return True, 0

        try:
            success = await self.repository.save_health_records(series_records)
            processed_count = len(series_records) if success else 0
            return success, processed_count

        except Exception as e:
            logging.error(f"Error batch saving series records: {str(e)}", stack_info=True)
            return False, 0

    def _calculate_summary_time_range_from_common(self, common_data: Dict[str, Any]) -> tuple:
        try:
            indicator = common_data["indicator"]
            start_time_ms = common_data.get("original_start_time_ms")
            end_time_ms = common_data.get("original_end_time_ms")
            user_timezone = common_data["timezone"]
            if start_time_ms is not None and end_time_ms is not None:
                start_time_utc = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc)
                end_time_utc = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc)
                if user_timezone == "UTC":
                    start_time_local = start_time_utc.replace(tzinfo=None)
                    end_time_local = end_time_utc.replace(tzinfo=None)
                    logging.info(f"Using explicit time range for {indicator}: UTC timezone, keeping UTC time {start_time_local} to {end_time_local}")
                    return start_time_local, end_time_local

                # Convert to user's local timezone
                # th_series_data stores start_time/end_time as timestamp without time zone, representing user's local time
                try:
                    user_tz = ZoneInfo(user_timezone)
                    start_time = start_time_utc.astimezone(user_tz).replace(tzinfo=None)
                    end_time = end_time_utc.astimezone(user_tz).replace(tzinfo=None)
                    return start_time, end_time
                except Exception as e:
                    logging.warning(f"Failed to convert timezone {user_timezone}, using UTC: {str(e)}")
                    start_time = start_time_utc.replace(tzinfo=None)
                    end_time = end_time_utc.replace(tzinfo=None)
                    return start_time, end_time

            base_time = common_data["record_time"]
            indicator_lower = indicator.lower()

            if "daily" in indicator_lower:
                start_time = base_time.replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = base_time.replace(hour=23, minute=59, second=59, microsecond=999999)
                logging.info(f"Using daily fallback logic for {indicator}: {start_time} to {end_time}")
            elif "weekly" in indicator_lower:
                days_since_monday = base_time.weekday()
                start_time = (base_time - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0,
                                                                                     microsecond=0)
                end_time = (start_time + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999999)
                logging.info(f"Using weekly fallback logic for {indicator}: {start_time} to {end_time}")
            elif "hourly" in indicator_lower:
                start_time = base_time.replace(minute=0, second=0, microsecond=0)
                end_time = base_time.replace(minute=59, second=59, microsecond=999999)
                logging.info(f"Using hourly fallback logic for {indicator}: {start_time} to {end_time}")
            else:
                start_time = base_time
                end_time = base_time
                logging.info(f"Using timestamp as point time for {indicator}: {start_time}")

            return start_time, end_time

        except Exception as e:
            logging.error(f"Error calculating summary time range: {str(e)}", stack_info=True)
            return None, None

    def _prepare_record_for_batch(self, record: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        try:
            source = record.get("source", "UNKNOWN")
            record_type = record.get("type")
            timestamp = record.get("timestamp")
            unit = record.get("unit", '')
            value = str(record.get("value", 0))
            record_timezone = record.get("timezone", "UTC")
            source_id = record.get("source_id", "")
            task_id = record.get("task_id", "")

            indicator = record_type

            normalized_value, _ = self.normalize_health_data_unit(
                record_type,
                float(value),
                unit,
                percentage_handling=True if source in ['apple_health'] else False
            )

            record_time_with_tz = datetime.fromtimestamp(timestamp / 1000, tz=ZoneInfo("UTC"))
            record_time = record_time_with_tz.replace(tzinfo=None)

            return {
                "user_id": user_id,
                "indicator": indicator,
                "value": str(normalized_value),
                "start_time": record_time,
                "end_time": record_time,
                "source": source.lower(),
                "timezone": record_timezone,
                "record_type": "vital_health",
                "source_id": source_id,
                "task_id": task_id,
            }

        except Exception as e:
            logging.error(f"Error preparing record for batch: {str(e)}", stack_info=True)
            return None

    def _calculate_summary_time_range(self, record: Dict[str, Any], indicator: str, timestamp: int) -> tuple:
        try:
            start_time_ms = record.get("startTime")
            end_time_ms = record.get("endTime")

            if start_time_ms and end_time_ms:
                start_time = datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
                end_time = datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
                return start_time, end_time

            base_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).replace(tzinfo=None)

            indicator_lower = indicator.lower()

            if "daily" in indicator_lower:
                start_time = base_time.replace(hour=0, minute=0, second=0, microsecond=0)
                end_time = base_time.replace(hour=23, minute=59, second=59, microsecond=999999)

            elif "weekly" in indicator_lower:
                days_since_monday = base_time.weekday()
                start_time = (base_time - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0,
                                                                                     microsecond=0)
                end_time = (start_time + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999999)

            elif "hourly" in indicator_lower:
                start_time = base_time.replace(minute=0, second=0, microsecond=0)
                end_time = base_time.replace(minute=59, second=59, microsecond=999999)

            else:
                start_time = base_time
                end_time = base_time

            return start_time, end_time

        except Exception as e:
            logging.error(f"Error calculating summary time range: {str(e)}", stack_info=True)
            return None, None

    def _prepare_record_for_batch(self, record: Dict[str, Any], user_id: str) -> Dict[str, Any]:
        source = record.get("source", "UNKNOWN")
        record_type = record.get("type")
        timestamp = record.get("timestamp")
        unit = record.get("unit", '')
        value = str(record.get("value", 0))

        record_timezone = record.get("timezone", "UTC")

        logging.debug(f"Processing record: source={source}, type={record_type}, timezone={record_timezone}")

        # indicator = self.TYPE_MAPPING.get(record_type, record_type.lower())
        indicator = record_type

        _insert_value, _ = self.normalize_health_data_unit(
            record_type,
            float(value),
            unit,
            percentage_handling=True if source in ['apple_health'] else False,
        )

        record_time_with_tz = datetime.fromtimestamp(timestamp / 1000, tz=ZoneInfo("UTC"))
        record_time = record_time_with_tz.replace(tzinfo=None)

        prepared_record = {
            "user_id": user_id,
            "indicator": indicator,
            "value": str(_insert_value),
            "start_time": record_time,
            "end_time": record_time,
            "source": source.lower(),
            "timezone": record_timezone,
            "record_type": "vital_health",
            "source_id": record.get("source_id", ""),
            "task_id": record.get("task_id", ""),
        }

        return prepared_record
