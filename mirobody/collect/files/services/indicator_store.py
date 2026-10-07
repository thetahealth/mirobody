"""Readings extracted from a file, written to the observation model.

The write side of what ① Collect hands on: `save_indicators_to_db` takes what
the extractor found in one document and lands it through
`collect.observations.ingest`, the single writer.

Splitting a unit off the value used to happen here, on the string. It happens
in `translate.parse.parse_value` now, which reads the value column and the
unit column together and returns a typed value: the number, the comparator
("<2.5" is 2.5 and "<", not the text), and the unit in UCUM. The extraction
prompt asks for the two columns separately; this is what holds when a model
prints the unit twice anyway.
"""

import logging
from datetime import datetime
from typing import Any

from mirobody.collect import observations
from mirobody.collect.files.services.report_date import document_date, file_source_ref
from mirobody.collect.files.services.table_indicators import printed_flag, split_flag
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

#: The flags a reading is stored with: the report's high and low, in one
#: spelling (`printed_flag`). Stored as printed, `↑`, `H`, `偏高` and `high`
#: were four answers to one question.
STORED_FLAGS = ("high", "low")


def row_time(printed: Any, report_time: datetime) -> datetime:
    """The time a row's reading was taken: the date the row itself prints
    (a home log, a table by day), else the document's.

    A log of twelve morning weights used to be filed as twelve readings of one
    day, or one reading after the name-and-value dedup, because the extraction
    had one date per document. A printed row date that does not parse, or is
    more than a day ahead of now (a misread), is not used (`document_date`):
    the document's date is, as it was for every row before.
    """
    return document_date(printed) or report_time


def value_and_flag(indicator: dict[str, Any]) -> tuple[str, str]:
    """`(value, flag)` of a read row. A flag the report printed after the
    number (`6.49↑`, `5.6 H`) is moved out of the value the way the table
    rules move it (`split_flag`, the same `L`-is-litres guard); else the
    row's `status`. Either is kept only as `high` or `low`: a `normal` is a
    judgement against the range, not a flag the report printed. Left in,
    `6.49↑` was stored as a narrative with no number: all 16 value errors of
    GLM-OCR's end-to-end run on the OCR benchmark (benchmarks/local_ocr,
    2026-10-06)."""
    value, printed = split_flag(str(indicator.get("value") or ""), str(indicator.get("reference_range") or ""))
    flag = printed_flag(printed or str(indicator.get("status") or ""))
    return value, flag if flag in STORED_FLAGS else ""


async def save_indicators_to_db(
    user_id: str,
    indicators: list[dict[str, Any]],
    start_time: datetime,
    date_source: str,
    file_key: str,
    extractor: str = "",
) -> observations.Report:
    """Write a file's extracted indicators as observations; what the write
    did, row by row (`observations.Report`).

    `start_time` and `date_source` come from `resolve_report_date`; the
    caller resolves them so the same answer can be recorded on the file
    row. The extraction is frozen as it was read (`th_extraction`), every
    text field is stored as printed, and the coding sits beside each row.
    A re-upload of the same file is a replay: equal rows are skipped. A
    failed write raises: answered with 0, it read as a document with no
    readings in it.
    """
    drafts = []
    for ix, indicator in enumerate(indicators):
        original_indicator = indicator.get("original_indicator")
        if not original_indicator:
            continue
        method = str(indicator.get("detection_method") or "")
        kind = observations.KIND_FINDING if method in ("Imaging", "Pathological") else observations.KIND_MEASUREMENT
        value, flag = value_and_flag(indicator)
        drafts.append(observations.Draft(
            name_text=str(original_indicator),
            observed_start=row_time(indicator.get("date_time"), start_time),
            value_text=value,
            unit_text=str(indicator.get("unit") or ""),
            ref_text=str(indicator.get("reference_range") or ""),
            flag_text=flag,
            method_text=method,
            note_text=str(indicator.get("notes") or ""),
            kind=kind,
            row_ix=ix,
        ))

    provenance = observations.Provenance(
        modality=observations.MODALITY_LAB,
        source_kind=observations.SOURCE_FILE,
        source_ref=file_source_ref(file_key),
        extractor=extractor or "llm:file-parser@indicators-v1",
        report_date=start_time.date(),
        date_source=date_source,
    )
    tz = await observations.user_tz(str(user_id))
    report = await observations.ingest(str(user_id), drafts, provenance, user_tz=tz, payload=indicators)
    if report.inserted:
        # The profile is a projection of these rows: a refresh that cannot be
        # queued now is queued by the next write, and must not fail this one.
        try:
            from mirobody.task import ProfileRefreshTask
            await ProfileRefreshTask.enqueue(str(user_id))
        except Exception as e:
            logger.warning("profile refresh not enqueued after a file's readings: user_id=%s error_type=%s",
                           user_id, type(e).__name__, exc_info=not is_driver_exception(e))
    return report
