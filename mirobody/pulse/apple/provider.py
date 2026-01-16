"""
Apple Health Provider implementations
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, Optional, Union
from zoneinfo import ZoneInfo

from .models import FLUTTER_TO_RECORD_TYPE_MAPPING, AppleHealthRecord, MetaInfo
from ..base import LinkRequest, Provider, ProviderInfo
from ..core import LinkType, ProviderStatus
from ..core.indicators_info import StandardIndicator
from ..data_upload.models.requests import (
    StandardPulseData,
    StandardPulseMetaInfo,
    StandardPulseRecord,
)


class AppleHealthProvider(Provider):

    _statistic_indicator_mapping = {
        StandardIndicator.STEPS.value.name: StandardIndicator.STEP_DURATION,
        StandardIndicator.FLOORS_CLIMBED.value.name: StandardIndicator.FLOORS_CLIMBED_DURATION,
        # StandardIndicator.ACTIVE_TIME.value.name: StandardIndicator.ACTIVE_TIME_DETAIL.value.name,
        StandardIndicator.DISTANCE.value.name: StandardIndicator.WALKING_RUNNING_DURATION,
        StandardIndicator.CYCLING_DISTANCE.value.name: StandardIndicator.CYCLING_DURATION,
        # StandardIndicator.DIETARY_WATER.value.name: StandardIndicator.DIETARY_WATER_DETAIL.value.name,
    }

    @property
    def info(self) -> ProviderInfo:
        """Get Provider information"""
        return ProviderInfo(
            slug="apple_health",
            name="Apple Health",
            description="Import health data from Apple Health export files",
            logo="https://static.thetahealth.ai/res/applehealth.png",
            supported=True,
            auth_type=LinkType.NONE,  # Apple Health does not require authentication
            status=ProviderStatus.CONNECTED,
        )

    async def link(self, request: LinkRequest) -> Dict[str, Any]:
        logging.info(f"Apple Health provider does not require linking for user {request.user_id}")
        return {
            "provider_slug": self.info.slug
        }

    async def unlink(self, user_id: str) -> Dict[str, Any]:
        logging.info(f"Apple Health provider does not require unlinking for user {user_id}")
        return {}

    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        try:
            t1 = time.time()

            user_id = raw_data.get("user_id")
            if not user_id:
                raise ValueError("Missing user_id in raw data")

            meta_info_data: MetaInfo = raw_data["meta_info"]
            default_timezone = meta_info_data.timezone

            timezone_cache = {
                "UTC": ZoneInfo("UTC"),
                default_timezone: ZoneInfo(default_timezone) if default_timezone != "UTC" else ZoneInfo("UTC"),
            }

            health_data = raw_data.get("health_data", [])
            total_count = len(health_data)

            logging.info(f"Starting to process {total_count} Apple Health records for user {user_id}")

            batch_size = 1000
            all_records = []

            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                current_batch = health_data[batch_start:batch_end]

                logging.info(f"Processing batch {batch_start // batch_size + 1}/{(total_count - 1) // batch_size + 1}, records: {batch_start}-{batch_end - 1}")

                batch_records = []
                batch_t1 = time.time()

                for record_data in current_batch:
                    if isinstance(record_data, AppleHealthRecord):
                        record = record_data
                    else:
                        try:
                            record = AppleHealthRecord(**record_data)
                        except Exception as e:
                            logging.error(f"Invalid record format: {str(e)}")
                            continue

                    processed_record = self._prepare_record_optimized(record, user_id, meta_info_data.taskId, timezone_cache, meta_info_data.directly_from_watch)
                    if processed_record:
                        batch_records.append(processed_record)

                        if processed_record.type in [
                            StandardIndicator.SLEEP_ASLEEP_DEEP.value.name, 
                            StandardIndicator.SLEEP_ASLEEP_CORE.value.name, 
                            StandardIndicator.SLEEP_ASLEEP_REM.value.name, 
                            StandardIndicator.SLEEP_UNSPECIFIED.value.name
                        ]:
                            total_sleep_record = StandardPulseRecord(
                                source=processed_record.source,
                                type=StandardIndicator.TOTAL_SLEEP.value.name,
                                timestamp=processed_record.timestamp,
                                unit=processed_record.unit,
                                value=processed_record.value,
                                timezone=processed_record.timezone,
                                startTime=processed_record.startTime,
                                endTime=processed_record.endTime,
                                source_id=processed_record.source_id,
                                task_id=processed_record.task_id,
                            )
                            batch_records.append(total_sleep_record)

                batch_t2 = time.time()
                logging.info(f"Batch {batch_start // batch_size + 1} processed: {len(batch_records)} records, "
                    f"time: {(batch_t2 - batch_t1) * 1000:.2f}ms")

                all_records.extend(batch_records)

            t2 = time.time()
            logging.info(f"Total processing time: {(t2 - t1) * 1000:.2f}ms for {len(all_records)} valid records")

            meta_info = StandardPulseMetaInfo(
                userId=user_id,
                requestId=raw_data.get("request_id"),
                timestamp=datetime.now().isoformat(),
                source="apple_health_watch" if meta_info_data.directly_from_watch else "apple_health",
                timezone=default_timezone,
                taskId=meta_info_data.taskId,
            )

            return StandardPulseData(metaInfo=meta_info, healthData=all_records)

        except Exception as e:
            logging.error(f"Error formatting Apple Health data: {str(e)}", stack_info=True)
            raise

    def _prepare_record_optimized(
            self, 
            record: AppleHealthRecord, 
            user_id: str, 
            task_id: str,
            timezone_cache: Optional[Dict[str, ZoneInfo]] = None,
            directly_from_watch: Optional[bool] = False
    ) -> Optional[StandardPulseRecord]:

        if timezone_cache is None:
            timezone_cache = {}

        try:
            record_type = record.type
            date_from = record.dateFrom
            date_to = record.dateTo
            value_data = record.value
            unit_symbol = record.unitSymbol
            source_id = record.sourceId or "unknown"
            timezone = record.timezone or "UTC"

            if len(timezone) > 20:
                timezone = "UTC"

            if record_type is None:
                return None

            flutter_type = record_type

            mapped_enum_value = FLUTTER_TO_RECORD_TYPE_MAPPING.get(flutter_type)
            if mapped_enum_value is None:
                logging.warning(f"UNMAPPED_HEALTH_TYPE: '{flutter_type}' not found in mapping. "
                    f"Record details - UUID: {record.uuid}, Value: {value_data}, Unit: {unit_symbol}, "
                    f"Source_Id: {source_id}, Time: {date_from}-{date_to}. "
                    f"This record will be DISCARDED. Please add mapping to FLUTTER_TO_RECORD_TYPE_MAPPING if needed.")
                return None

            type_value = mapped_enum_value

            if timezone not in timezone_cache:
                try:
                    timezone_cache[timezone] = ZoneInfo(timezone)
                except Exception:
                    timezone_cache[timezone] = ZoneInfo("UTC")
            tz_obj = timezone_cache[timezone]

            start_time = None
            end_time = None
            start_timestamp_ms = None
            end_timestamp_ms = None

            if date_from:
                if isinstance(date_from, int):
                    start_timestamp_ms = date_from
                    start_time = datetime.fromtimestamp(date_from / 1000, tz=tz_obj)
                elif isinstance(date_from, str):
                    start_time = datetime.fromisoformat(date_from).replace(microsecond=0, tzinfo=tz_obj)
                    start_timestamp_ms = int(start_time.timestamp() * 1000)

            if date_to:
                if isinstance(date_to, int):
                    end_timestamp_ms = date_to
                    end_time = datetime.fromtimestamp(date_to / 1000, tz=tz_obj)
                elif isinstance(date_to, str):
                    end_time = datetime.fromisoformat(date_to).replace(microsecond=0, tzinfo=tz_obj)
                    end_timestamp_ms = int(end_time.timestamp() * 1000)

            if not end_timestamp_ms:
                end_timestamp_ms = start_timestamp_ms
                end_time = start_time

            if not start_timestamp_ms:
                start_timestamp_ms = end_timestamp_ms
                start_time = end_time

            if not start_timestamp_ms and not end_timestamp_ms:
                return None

            main_timestamp_ms = start_timestamp_ms or end_timestamp_ms

            numeric_value = self._extract_value(value_data, type_value)

            if type_value in self._statistic_indicator_mapping and record.uuid:
                mapped_indicator = self._statistic_indicator_mapping[type_value]
                type_value = mapped_indicator.value.name
                unit_symbol = mapped_indicator.value.standard_unit
                numeric_value = end_timestamp_ms - start_timestamp_ms

            return StandardPulseRecord(
                source="apple_health_watch" if directly_from_watch else "apple_health",
                type=type_value,  # Standard indicator value, e.g., "heartRates"
                timestamp=main_timestamp_ms,
                unit=unit_symbol,
                value=numeric_value,
                timezone=timezone,
                startTime=start_timestamp_ms if start_timestamp_ms else None,
                endTime=end_timestamp_ms if end_timestamp_ms else None,
                source_id=source_id,
                task_id=task_id,
            )

        except Exception as e:
            logging.error(f"Error preparing record: {str(e)}", stack_info=True)
            return None

    def _extract_value(self, value_data: Any, record_type: str) -> Union[float, str]:
        if record_type == StandardIndicator.REPRODUCTIVE_CERVICAL_MUCUS_QUALITY.value.name:
            return {
                1: 'dry',
                2: 'sticky',
                3: 'creamy',
                4: 'water',
                5: 'eggWhite',
            }.get(value_data['numericValue'], 'unspecified')
        
        if record_type == StandardIndicator.REPRODUCTIVE_CONTRACEPTIVE.value.name:
            return {
                1: 'unspecified',
                2: 'implant',
                3: 'injection',
                4: 'intrauterineDevice',
                5: 'intravaginalRing',
                6: 'oral',
                7: 'patch',
            }.get(value_data['numericValue'], 'unspecified')
        
        if record_type == StandardIndicator.REPRODUCTIVE_MENSTRUATION_FLOW.value.name:
            return value_data['flow']
        
        if record_type == StandardIndicator.REPRODUCTIVE_OVULATION_TEST_RESULT.value.name:
            return {
                1: 'negative',
                2: 'positive',
                3: 'indeterminate',
                4: 'estrogenSurge',
            }.get(value_data['numericValue'], 'indeterminate')
        
        if record_type == StandardIndicator.REPRODUCTIVE_PREGNANCY_TEST_RESULT.value.name:
            return {
                1: 'negative',
                2: 'positive',
                3: 'indeterminate',
            }.get(value_data['numericValue'], 'indeterminate')
        
        if record_type == StandardIndicator.REPRODUCTIVE_PROGESTERONE_TEST_RESULT.value.name:
            return {
                1: 'negative',
                2: 'positive',
                3: 'indeterminate',
            }.get(value_data['numericValue'], 'indeterminate')
        
        if record_type == StandardIndicator.REPRODUCTIVE_SEXUAL_ACTIVITY.value.name:
            return 'True, With Protection' if value_data['isProtectionUsed'] else 'True, Without Protection'
        
        if record_type in [
            StandardIndicator.REPRODUCTIVE_INTERMENTSTRUAL_BLEEDING.value.name,
            StandardIndicator.REPRODUCTIVE_LACTATION.value.name,
            StandardIndicator.REPRODUCTIVE_PREGNANCY.value.name,
        ]:
            return 'True'
        
        if isinstance(value_data, dict):
            if "numericValue" in value_data:
                return float(value_data["numericValue"])

            return 1.0
        else:
            return 1.0  # Placeholder value


class CDAProvider(Provider):
    @property
    def info(self) -> ProviderInfo:
        """Get Provider information"""
        return ProviderInfo(
            slug="cda",
            name="CDA Documents",
            description="Import clinical data from CDA (Clinical Document Architecture) documents",
            logo="https://www.hl7.org/assets/images/hl7-logo.png",
            supported=True,
            auth_type=LinkType.NONE,  # CDA does not require authentication
            status=ProviderStatus.CONNECTED,
        )

    async def link(self, request: LinkRequest) -> Dict[str, Any]:
        logging.info(f"CDA provider does not require linking for user {request.user_id}")
        return {
            "provider_slug": self.info.slug
        }

    async def unlink(self, user_id: str) -> Dict[str, Any]:
        logging.info(f"CDA provider does not require unlinking for user {user_id}")
        return {}

    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        try:
            user_id = raw_data.get("user_id")
            if not user_id:
                raise ValueError("Missing user_id in raw data")

            cda_data = raw_data.get("cda_data", {})
            records = []

            if "vital_signs" in cda_data:
                vital_records = await self._format_vital_signs(cda_data["vital_signs"], user_id)
                records.extend(vital_records)

            if "lab_results" in cda_data:
                lab_records = await self._format_lab_results(cda_data["lab_results"], user_id)
                records.extend(lab_records)

            if "medications" in cda_data:
                med_records = await self._format_medications(cda_data["medications"], user_id)
                records.extend(med_records)

            meta_info = StandardPulseMetaInfo(
                userId=user_id,
                requestId=raw_data.get("request_id"),
                timestamp=datetime.now().isoformat(),
                source="apple.cda",
                timezone=raw_data.get("meta_info", {}).get("timezone", "UTC"),
                taskId=raw_data.get("meta_info", {}).get("taskId"),
            )

            return StandardPulseData(metaInfo=meta_info, healthData=records)

        except Exception as e:
            logging.error(f"Error formatting CDA data: {str(e)}", stack_info=True)
            raise

    async def _format_vital_signs(self, vital_signs_data: list, user_id: str) -> list:
        records = []
        return records

    async def _format_lab_results(self, lab_data: list, user_id: str) -> list:
        records = []
        return records

    async def _format_medications(self, med_data: list, user_id: str) -> list:
        records = []
        return records
