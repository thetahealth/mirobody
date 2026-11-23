"""An Apple Watch ECG recording, written to the observation model.

The write side of issue #334: one `EcgReading` (`kernel.decoders.apple_ecg`,
read off the CSV header with no model) becomes one `Draft`, filed through
`collect.observations.ingest`, the single writer. Parallel to
`indicator_store.save_indicators_to_db`, which files a document's
model-extracted rows; this files the one reading a device format gives
directly, under `device-sensed`/`SOURCE_DEVICE` rather than `lab-report`/
`SOURCE_FILE`, so it is never filed as a lab finding and never shares a
series with a row some other document printed under the bare word
"Classification" (`coding_for` only consults the device catalogue for a
`SOURCE_DEVICE` batch, and the catalogue name is `ecgRhythmClassification`,
not the printed word).
"""

from __future__ import annotations

import logging

from mirobody.collect import observations
from mirobody.collect.files.services.report_date import file_source_ref
from mirobody.kernel.decoders.apple_ecg import EcgReading
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

#: The catalogue name an ECG classification is filed under
#: (`res/catalog/metrics.tsv`, member `ECG_RHYTHM_CLASSIFICATION`): confident
#: LOINC 8601-7, "EKG impression". That is the overall automated impression
#: of a single-lead recording, which is what Apple's "Sinus Rhythm" / "Atrial
#: Fibrillation" / "Inconclusive" / "Low or High Heart Rate" are (Apple's ECG
#: app Instructions for Use, "ECG Analysis"). The lexical index's other
#: candidates name something narrower or weaker: 8619-9 "Rhythm segment" is
#: one segment of a longer study, not a whole recording's result, and 8884-9
#: "Heart rate rhythm" is a bare TYPE with no Impression/interpretation
#: property, i.e. a rhythm's shape rather than a verdict on it.
METRIC_NAME = "ecgRhythmClassification"

#: `vendor` on the batch's `Provenance`. Distinct on purpose from the
#: live-sync pipeline's `apple_health` / `apple_health_watch`
#: (`collect/providers/apple/`, `standardize/aggregate/aggregators/
#: source_id_priority.py`), which this upload path never touches: a reader of
#: `stream_key` must not mistake a one-off CSV upload for a synced stream
#: those modules give Apple-specific election priority.
VENDOR = "apple_watch_ecg"


def draft_for(reading: EcgReading, *, row_ix: int = 0) -> observations.Draft:
    """The one `Draft` a recording gives: a finding (a categorical result,
    never a quantity), under the catalogue's own name rather than the printed
    word `Classification`. `reading` carries no sample and no name/date of
    birth (`apple_ecg.EcgReading` has no field for either), so there is
    nothing here that could turn either into a stored reading."""
    return observations.Draft(
        name_text=METRIC_NAME,
        observed_start=reading.recorded_at,
        value_text=reading.classification,
        method_text=reading.lead,
        note_text=reading.symptoms,
        kind=observations.KIND_FINDING,
        tz=_offset_text(reading),
        row_ix=row_ix,
    )


def _offset_text(reading: EcgReading) -> str:
    """`reading.recorded_at`'s own offset, as `standardize.resolve_tz` reads
    one (`"+08:00"`): the watch's clock, not the upload's connection or the
    person's saved zone. Left as `""` is read as "the source did not say"
    and falls back to the person's zone — the exact loss the first comment
    on #334 found in a real model's answer ("The time loses its offset")."""
    offset = reading.recorded_at.utcoffset()
    if offset is None:
        return ""
    minutes = int(offset.total_seconds() // 60)
    sign = "+" if minutes >= 0 else "-"
    hours, rem = divmod(abs(minutes), 60)
    return f"{sign}{hours:02d}:{rem:02d}"


async def save_ecg_reading(user_id: str, reading: EcgReading, file_key: str) -> observations.Report:
    """Write one recording's classification. A re-upload of the same file
    key is a replay (`observations.ingest`'s identity includes `source_ref`),
    as every other file-backed reading already is."""
    provenance = observations.Provenance(
        modality=observations.MODALITY_DEVICE,
        source_kind=observations.SOURCE_DEVICE,
        source_ref=file_source_ref(file_key),
        vendor=VENDOR,
    )
    tz = await observations.user_tz(str(user_id))
    report = await observations.ingest(str(user_id), [draft_for(reading)], provenance, user_tz=tz)
    if report.inserted:
        # The profile is a projection of these rows: a refresh that cannot be
        # queued now is queued by the next write, and must not fail this one
        # (the same guard `indicator_store.save_indicators_to_db` keeps).
        try:
            from mirobody.task import ProfileRefreshTask

            await ProfileRefreshTask.enqueue(str(user_id))
        except Exception as e:
            logger.warning("profile refresh not enqueued after an ECG reading: user_id=%s error_type=%s",
                           user_id, type(e).__name__, exc_info=not is_driver_exception(e))
    return report


__all__ = ["METRIC_NAME", "VENDOR", "draft_for", "save_ecg_reading"]
