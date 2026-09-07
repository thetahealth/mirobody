"""
Apple Health Provider implementations
"""

import logging
import time
from dataclasses import replace
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from ...kernel import meds
from .models import FLUTTER_TO_RECORD_TYPE_MAPPING, AppleHealthRecord, MetaInfo
from ..base import LinkRequest, Provider, ProviderInfo
from ..core import LinkType, ProviderStatus
from ..standardize.indicators_info import StandardIndicator
from ..ingest.models.requests import (
    FormatDataInput,
    StandardPulseData,
    StandardPulseMetaInfo,
    StandardPulseRecord,
)

logger = logging.getLogger(__name__)


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

    async def link(self, request: LinkRequest) -> dict[str, Any]:
        logger.info(f"Apple Health provider does not require linking for user {request.user_id}")
        return {
            "provider_slug": self.info.slug
        }

    async def unlink(self, user_id: str) -> dict[str, Any]:
        logger.info(f"Apple Health provider does not require unlinking for user {user_id}")
        return {}

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        raw_data = fmt_input.payload
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

            logger.info(f"Starting to process {total_count} Apple Health records for user {user_id}")

            batch_size = 1000
            all_records = []

            for batch_start in range(0, total_count, batch_size):
                batch_end = min(batch_start + batch_size, total_count)
                current_batch = health_data[batch_start:batch_end]

                logger.info(f"Processing batch {batch_start // batch_size + 1}/{(total_count - 1) // batch_size + 1}, records: {batch_start}-{batch_end - 1}")

                batch_records = []
                batch_t1 = time.time()

                for record_data in current_batch:
                    if isinstance(record_data, AppleHealthRecord):
                        record = record_data
                    else:
                        try:
                            record = AppleHealthRecord(**record_data)
                        except Exception as e:
                            logger.error(f"Invalid record format: {str(e)}")
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
                logger.info(f"Batch {batch_start // batch_size + 1} processed: {len(batch_records)} records, "
                    f"time: {(batch_t2 - batch_t1) * 1000:.2f}ms")

                all_records.extend(batch_records)

            t2 = time.time()
            logger.info(f"Total processing time: {(t2 - t1) * 1000:.2f}ms for {len(all_records)} valid records")

            meta_info = StandardPulseMetaInfo(
                userId=user_id,
                requestId=raw_data.get("request_id"),
                timestamp=datetime.now().isoformat(),
                source="apple_health_watch" if meta_info_data.directly_from_watch else "apple_health",
                timezone=default_timezone,
                taskId=meta_info_data.taskId,
                windowFrom=meta_info_data.windowFrom,
                windowTo=meta_info_data.windowTo,
            )

            return StandardPulseData(metaInfo=meta_info, healthData=all_records)

        except Exception as e:
            logger.error(f"Error formatting Apple Health data: {str(e)}", stack_info=True)
            raise

    def _prepare_record_optimized(
            self, 
            record: AppleHealthRecord, 
            user_id: str, 
            task_id: str,
            timezone_cache: dict[str, ZoneInfo] | None = None,
            directly_from_watch: bool | None = False
    ) -> StandardPulseRecord | None:

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
                logger.warning(f"UNMAPPED_HEALTH_TYPE: '{flutter_type}' not found in mapping. "
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
            logger.error(f"Error preparing record: {str(e)}", stack_info=True)
            return None

    def _extract_value(self, value_data: Any, record_type: str) -> float | str:
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
        return 1.0  # Placeholder value


def _sections(cda_data: Any) -> dict[str, list]:
    """`cda_data` as `{section: entries}`, whether it arrived as that dict or
    as the list of documents the client actually sends."""
    if isinstance(cda_data, dict):
        return {k: (v if isinstance(v, list) else [v]) for k, v in cda_data.items()}
    out: dict[str, list] = {}
    for document in cda_data if isinstance(cda_data, list) else []:
        if not isinstance(document, dict):
            continue
        for key, value in document.items():
            out.setdefault(key, []).extend(value if isinstance(value, list) else [value])
    return out


def _plan_from_entry(entry: dict, *, subject_id: str, today: date) -> "meds.MedicationPlan | None":
    """One entry → a plan, from whichever of the two shapes it is."""
    resource_type = str(entry.get("resourceType") or "")
    if resource_type == "MedicationStatement":
        # The plan_id is derived, never taken from the resource's own `id`. A
        # FHIR resource id is unique within the server that issued it, and
        # `th_medication_plan.plan_id` is the primary key across every person:
        # two people importing documents from the same clinic would collide,
        # and one person's medication list would overwrite another's.
        concept = _concept_or_none(entry)
        if concept is None:
            return None
        record_id = str(entry.get("id") or "")
        return meds.from_fhir_medication_statement(
            entry,
            default_start=today,
            today=today,
            plan_id=meds.plan_id_for(subject_id, record_id, concept.concept_key),
            subject_id=subject_id,
        )
    if resource_type == "MedicationRequest":
        prescription = meds.from_fhir_medication_request(entry)
        return _plan_from_prescription(prescription, subject_id=subject_id, today=today)
    return _plan_from_flat(entry, subject_id=subject_id, today=today)


def _concept_or_none(entry: dict) -> "meds.MedicationConcept | None":
    """The entry's medication concept, or None when it names no drug at all.
    Read before importing so the derived plan_id can include the concept key."""
    try:
        return meds.MedicationConcept(
            text=str((entry.get("medicationCodeableConcept") or {}).get("text") or ""),
            codes=tuple(
                meds.Coding(str(c.get("system") or ""), str(c.get("code") or ""), str(c.get("display") or ""))
                for c in ((entry.get("medicationCodeableConcept") or {}).get("coding") or [])
                if isinstance(c, dict)
            ),
        )
    except ValueError:
        return None


def _plan_from_prescription(prescription, *, subject_id: str, today: date):
    """A clinician's ORDER is not yet a plan the person follows — but an order
    imported from the person's own health record is the only evidence there is
    that they were told to take it, so it lands as an unconfirmed plan for them
    to accept or delete."""
    if prescription is None:
        return None
    plan_id = meds.plan_id_for(subject_id, prescription.order_id, prescription.concept.concept_key)
    return meds.MedicationPlan(
        plan_id=plan_id,
        concept=prescription.concept,
        schedule=prescription.schedule or (meds.DoseInstruction(),),
        start=today,
        confirmed=False,
        order_id=prescription.order_id,
        source="apple:MedicationRequest",
        subject_id=subject_id,
    )


def _plan_from_flat(entry: dict, *, subject_id: str, today: date):
    """The flattened CDA shape: `{name, dose, unit, frequency, start, end}`."""
    name = str(entry.get("name") or entry.get("medication") or "").strip()
    if not name:
        return None
    concept = meds.MedicationConcept(text=name, strength=str(entry.get("strength") or ""))
    # The sig is the whole instruction when the exporter kept one; the separate
    # `dose`/`unit` fields fill in when it did not. `parse_dose_instruction`
    # returns None rather than half a regimen, and an unparsed instruction is
    # still a plan worth keeping — the text stays on the entry.
    schedule = meds.parse_dose_instruction(str(entry.get("frequency") or entry.get("sig") or ""))
    if schedule is None:
        schedule = (meds.DoseInstruction(),)
    dose = meds.dose_from_text(entry["dose"], str(entry.get("unit") or "")) if entry.get("dose") else None
    if dose is not None and schedule[0].dose is None:
        schedule = (replace(schedule[0], dose=dose), *schedule[1:])
    start = _entry_date(entry.get("start")) or today
    source_record_id = str(entry.get("id") or entry.get("source_record_id") or name)
    return meds.MedicationPlan(
        plan_id=meds.plan_id_for(subject_id, source_record_id, concept.concept_key),
        concept=concept,
        schedule=schedule,
        start=start,
        end=_entry_date(entry.get("end")),
        confirmed=False,          # imported, not entered by the person
        source="apple:cda",
        source_record_id=source_record_id,
        subject_id=subject_id,
    )


def _entry_date(value: Any) -> "date | None":
    if isinstance(value, date):
        return value
    text = str(value or "")[:10]
    try:
        return date.fromisoformat(text) if text else None
    except ValueError:
        return None


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

    async def link(self, request: LinkRequest) -> dict[str, Any]:
        logger.info(f"CDA provider does not require linking for user {request.user_id}")
        return {
            "provider_slug": self.info.slug
        }

    async def unlink(self, user_id: str) -> dict[str, Any]:
        logger.info(f"CDA provider does not require unlinking for user {user_id}")
        return {}

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        raw_data = fmt_input.payload
        try:
            user_id = raw_data.get("user_id")
            if not user_id:
                raise ValueError("Missing user_id in raw data")

            # `apple_router` forwards `cdaData` verbatim and the client sends a
            # LIST of documents, while this method was written against a dict of
            # sections. Both shapes arrive in the wild, so both are read here
            # rather than in one of the two places that happen to produce one.
            cda_data = _sections(raw_data.get("cda_data"))
            records = []

            if "vital_signs" in cda_data:
                vital_records = await self._format_vital_signs(cda_data["vital_signs"], user_id)
                records.extend(vital_records)

            if "lab_results" in cda_data:
                lab_records = await self._format_lab_results(cda_data["lab_results"], user_id)
                records.extend(lab_records)

            if "medications" in cda_data:
                # Medications are an ENTITY and are stored as one. Nothing is
                # added to `records`: a plan has a schedule and a lifecycle, and
                # filing it as a reading forces a choice between losing the
                # schedule and inventing a value.
                await self._format_medications(cda_data["medications"], user_id)

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
            logger.error(f"Error formatting CDA data: {str(e)}", stack_info=True)
            raise

    async def _format_vital_signs(self, vital_signs_data: list, user_id: str) -> list:
        records = []
        return records

    async def _format_lab_results(self, lab_data: list, user_id: str) -> list:
        records = []
        return records

    async def _format_medications(self, med_data: list, user_id: str, store: Any = None) -> int:
        """Medication entries from an Apple clinical record → medication plans.

        Apple's clinical records ARE FHIR: `HKClinicalRecord` carries the
        resource its provider published, so a `MedicationStatement` or a
        `MedicationRequest` arrives as itself and
        `meds.from_fhir_medication_statement` reads it. A CDA-flavoured dict
        (`{name, dose, unit, frequency, start, end}`) is accepted too, because
        an exporter that flattens the XML into that shape is the other thing
        seen in the wild.

        Returns how many plans were written. NOTHING goes to `th_series_data`:
        a plan is not a reading, and `plan_id` is derived from
        `(subject, source record, concept)` so re-importing the same document
        updates rather than duplicating.
        """
        entries = med_data if isinstance(med_data, list) else [med_data]
        if not entries:
            return 0
        if store is None:
            from ..meds import PostgresMedicationStore
            store = PostgresMedicationStore()

        today = date.today()
        written = 0
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                plan = _plan_from_entry(entry, subject_id=str(user_id), today=today)
            except (ValueError, KeyError, TypeError) as e:
                # One unreadable entry must not lose the rest of the document.
                # The reason code, never the entry: it holds a drug name.
                logger.warning("CDA medication entry skipped: error_type=%s", type(e).__name__)
                continue
            if plan is None:
                continue
            await store.put(plan)
            written += 1
        written_count = written
        logger.info("CDA import wrote %d medication plans", written_count)
        return written
