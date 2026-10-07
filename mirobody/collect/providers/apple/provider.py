"""
Apple Health Provider implementations
"""

import logging
import time
from dataclasses import replace
from datetime import date, datetime
from typing import Any

from mirobody.kernel import decoders, meds
from mirobody.kernel.ops import is_driver_exception
from .models import AppleHealthRecord, MetaInfo
from mirobody.collect.base import LinkRequest, Provider, ProviderInfo
from mirobody.collect.core import LinkType, ProviderStatus
from mirobody.collect.ingest import FormatDataInput, StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord
from mirobody.collect.providers._platform.normalize import records_from_facts

logger = logging.getLogger(__name__)


class AppleHealthProvider(Provider):

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
        return {"provider_slug": self.info.slug}

    async def unlink(self, user_id: str) -> dict[str, Any]:
        return {}

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """HealthKit records to standard records, via ``mirobody.kernel.decoders.apple``.

        Every ``type`` is a HealthKit identifier, the vocabulary Apple's own
        export uses, so this push endpoint and ``mirobody import apple`` read
        one table. The records arrive already validated as `AppleHealthRecord`
        (the route refuses a body with an invalid one). A record that decodes
        to nothing, of a type the table does not carry or without a parsable
        time, is dropped and counted, never guessed at.
        """
        raw = fmt_input.payload
        t1 = time.time()
        user_id = raw.get("user_id")
        if not user_id:
            raise ValueError("Missing user_id in raw data")
        meta: MetaInfo = raw["meta_info"]
        default_tz = meta.timezone
        source = "apple_health_watch" if meta.directly_from_watch else "apple_health"

        records: list[StandardPulseRecord] = []
        dropped: dict[str, int] = {}
        health_data: list[AppleHealthRecord] = raw.get("health_data", [])
        for record in health_data:
            tz = record.timezone if record.timezone and len(record.timezone) <= 20 else default_tz
            facts = decoders.decode("apple", record.type, record.sample(), tz)
            if not facts:
                dropped[record.type] = dropped.get(record.type, 0) + 1
                continue
            for r in records_from_facts(
                facts, slug=self.info.slug, tz=tz, source_id=record.sourceId or "unknown", source=source
            ):
                r.task_id = meta.taskId
                records.append(r)

        dropped_count = sum(dropped.values())
        duration_ms = (time.time() - t1) * 1000
        logger.info("Apple Health formatted: record_count=%d input_count=%d dropped_count=%d "
                    "dropped_type_count=%d duration_ms=%.0f", len(records), len(health_data), dropped_count,
                    len(dropped), duration_ms)

        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(
                userId=user_id,
                requestId=raw.get("request_id"),
                timestamp=datetime.now().isoformat(),
                source=source,
                timezone=default_tz,
                taskId=meta.taskId,
                windowFrom=meta.windowFrom,
                windowTo=meta.windowTo,
            ),
            healthData=records,
            processingInfo={"accepted": len(records), "dropped_types": sorted(dropped)},
        )


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
    """A clinician's ORDER is not yet a plan the person follows, but an order
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
    # still a plan worth keeping: the text stays on the entry.
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
        return {"provider_slug": self.info.slug}

    async def unlink(self, user_id: str) -> dict[str, Any]:
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
            logger.error("CDA format failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))
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
            from mirobody.collect.meds import PostgresMedicationStore
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
