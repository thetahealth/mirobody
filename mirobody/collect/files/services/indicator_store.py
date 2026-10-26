"""Readings extracted from a file, written to the observation model.

The write side of what ① Collect hands on: `save_indicators_to_db` takes what
the extractor found in one document and lands it through
`collect.observations.ingest`, the single writer.

Splitting a unit off the value used to happen here, on the string. It happens
in `standardize.parse.parse_value` now, which reads the value column and the
unit column together and returns a typed value: the number, the comparator
("<2.5" is 2.5 and "<", not the text), and the unit in UCUM. The extraction
prompt asks for the two columns separately; this is what holds when a model
prints the unit twice anyway.
"""

import logging
import re
import unicodedata
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from mirobody.collect import observations
from mirobody.collect.files.services.report_date import document_date, file_source_ref
from mirobody.collect.files.services.table_indicators import printed_flag, split_flag
from mirobody.kernel.ops import is_driver_exception
from mirobody.standardize import RANGE_SEPARATOR

logger = logging.getLogger(__name__)

#: The flags a reading is stored with: the report's high and low, in one
#: spelling (`printed_flag`). Stored as printed, `↑`, `H`, `偏高` and `high`
#: were four answers to one question.
STORED_FLAGS = ("high", "low")


#: The extraction's `detection_method` values whose rows are findings, not measurements.
FINDING_METHODS = ("Imaging", "Pathological")

# A doubled dash is one range separator, as in `standardize.parse_range`:
# `125--350` ends at 350, while `-5---1` ends at -1.
_NUMERIC_TOKEN = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
_NUMBER = re.compile(rf"(?<![\d.])({_NUMERIC_TOKEN})(?:\s*(?:{RANGE_SEPARATOR}|to)\s*({_NUMERIC_TOKEN}))?")
_THOUSANDS = re.compile(r"(?<=\d),(?=\d{3}(?!\d))")
_SPACE = re.compile(r"\s+")
#: A non-numeric value longer than this is prose a model may rephrase; it is
#: not checked against the text.
_SHORT_WORD = 12


def text_bears_out(text: str) -> Callable[[str], bool | None]:
    """A check of one extracted value against the document's text: True when
    the text prints it, False when it does not, None when this cannot say.
    Numbers compare by value (`5.6` is the printed `5.60`); a short word such
    as `阴性` or `++` by its letters. A model can write a value no line of the
    page holds, and the text is the only witness left after extraction."""
    folded = _THOUSANDS.sub("", unicodedata.normalize("NFKC", text or "").replace("−", "-"))
    try:
        printed = {Decimal(n) for pair in _NUMBER.findall(folded) for n in pair if n}
    except InvalidOperation:
        # An unreadable exponent in one OCR token must not discard a report.
        printed = None
    letters = _SPACE.sub("", folded).casefold()

    def check(value: str) -> bool | None:
        v = _THOUSANDS.sub("", unicodedata.normalize("NFKC", value or "").replace("−", "-")).strip()
        if not v or not letters:
            return None
        numbers = [n for pair in _NUMBER.findall(v) for n in pair if n]
        if numbers:
            if printed is None:
                return None
            try:
                return all(Decimal(n) in printed for n in numbers)
            except InvalidOperation:
                return None
        if len(v) > _SHORT_WORD:
            return None
        return _SPACE.sub("", v).casefold() in letters

    return check


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
    rules move it (`split_flag`, the same `L`-is-litres guard). The rules
    retain a copied column or trailing flag in `_printed_flag`; else the
    row's `status`, and for a measurement only when the row prints a range
    to judge it by. Either is kept only as `high` or `low`: a `normal` is a
    judgement, not a flag. Left in, `6.49↑` was stored as a narrative with
    no number: all 16 value errors of GLM-OCR's end-to-end run
    (benchmarks/local_ocr, 2026-10-06). A measurement's `status` with no
    range was the model's own opinion: an outpatient note's 4.76 mmol/L
    glucose was stored `high` (2026-10-10). An imaging or pathology finding
    has no range, and its `high` means abnormal."""
    ref = str(indicator.get("reference_range") or "")
    value, printed = split_flag(str(indicator.get("value") or ""), ref)
    judged = ref.strip() or str(indicator.get("detection_method") or "") in FINDING_METHODS
    copied = str(indicator.get("_printed_flag") or "")
    flag = printed_flag(printed or copied or (str(indicator.get("status") or "") if judged else ""))
    return value, flag if flag in STORED_FLAGS else ""


async def save_indicators_to_db(
    user_id: str,
    indicators: list[dict[str, Any]],
    start_time: datetime,
    date_source: str,
    file_key: str,
    extractor: str = "",
    text: str = "",
) -> observations.Report:
    """Write a file's extracted indicators as observations; what the write
    did, row by row (`observations.Report`).

    With the document's `text`, a measurement whose value the text does not
    print is stored as `llm-unverified`: it stays in the record and the
    catalogue, marked, and the agent is told which ones they are.

    `start_time` and `date_source` come from `resolve_report_date`; the
    caller resolves them so the same answer can be recorded on the file
    row. The extraction is frozen as it was read (`th_extraction`), every
    text field is stored as printed, and the coding sits beside each row.
    A re-upload of the same file is a replay: equal rows are skipped. A
    failed write raises: answered with 0, it read as a document with no
    readings in it.
    """
    drafts = []
    bears_out = text_bears_out(text) if text else None
    unverified_count = 0
    for ix, indicator in enumerate(indicators):
        original_indicator = indicator.get("original_indicator")
        if not original_indicator:
            continue
        method = str(indicator.get("detection_method") or "")
        kind = observations.KIND_FINDING if method in FINDING_METHODS else observations.KIND_MEASUREMENT
        value, flag = value_and_flag(indicator)
        modality = ""
        if bears_out is not None and kind == observations.KIND_MEASUREMENT and bears_out(value) is False:
            modality = observations.MODALITY_UNVERIFIED
            unverified_count += 1
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
            modality=modality,
        ))
    if unverified_count:
        logger.warning("extracted values the text does not print: unverified_count=%d row_count=%d",
                       unverified_count, len(drafts))

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
