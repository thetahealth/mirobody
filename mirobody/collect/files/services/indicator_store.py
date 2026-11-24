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
from mirobody.collect.files.services.table_indicators import printed_flag, split_flag, split_pressure
from mirobody.collect.sentence import blood_pressure
from mirobody.kernel.ops import is_driver_exception
from mirobody.standardize import RANGE_SEPARATOR, parse_range, parse_value
from mirobody.units import normalize_unit, numerals, unit_family

logger = logging.getLogger(__name__)

#: The flags a reading is stored with: the report's high and low, in one
#: spelling (`printed_flag`). Stored as printed, `↑`, `H`, `偏高` and `high`
#: were four answers to one question.
STORED_FLAGS = ("high", "low")


#: The extraction's `detection_method` values whose rows are findings, not
#: measurements. `Examination` is a physical or dental examination read by
#: the clinician: `牙周`, `义齿`, a heart sound, a range of motion.
FINDING_METHODS = ("Imaging", "Pathological", "Examination")

# A doubled dash is one range separator, as in `standardize.parse_range`:
# `125--350` ends at 350, while `-5---1` ends at -1.
_NUMERIC_TOKEN = r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
_NUMBER = re.compile(rf"(?<![\d.])({_NUMERIC_TOKEN})(?:\s*(?:{RANGE_SEPARATOR}|to)\s*({_NUMERIC_TOKEN}))?")
# The comparator and number a value opens with; what follows is its unit or
# prose. Two-character comparators are the ones `standardize.parse_value`
# reads: with only `>` here, `>=97%` lost its head and an `Examination` SpO2
# printed that way was stored as a finding with no number.
_VALUE_HEAD = re.compile(rf"(?:[<>＜＞][=＝]?|[≤≥=＝])?\s*{_NUMERIC_TOKEN}")
# A systolic/diastolic pair and whatever the model printed after it (its
# unit, or nothing): "120/80", "111/65 mmHg". `standardize.parse_value`
# reads it as narrative (`units/normalize.py` tries the slash as a
# rate-unit prefix and refuses it), so a pair is split before it is stored.
_PAIR = re.compile(rf"^({_NUMERIC_TOKEN})\s*/\s*({_NUMERIC_TOKEN})\s*(.*)$")
#: Marks the two readings `_split_blood_pressure` made out of one pair.
_PRESSURE = "_pressure"
# A name that already says which half it is. `collect.sentence._BP_NAME`
# finds "blood pressure" inside `Systolic blood pressure` too, so without
# this two systolic readings in a row were renamed systolic and diastolic.
_HALF = re.compile(r"收缩|舒张|收縮|舒張|収縮|拡張|systolic|diastolic", re.IGNORECASE)
_SPACE = re.compile(r"\s+")
#: A non-numeric value longer than this is prose a model may rephrase; it is
#: not checked against the text.
_SHORT_WORD = 12


def text_bears_out(text: str) -> Callable[[str], bool | None]:
    """A check of one extracted value against the document's text: True when
    the text prints it, False when it does not, None when this cannot say.
    Numbers compare by value (`5.6` is the printed `5.60`); a short word such
    as `阴性` or `++` by its letters. A model can write a value no line of the
    page holds, and the text is the only witness left after extraction.

    Commas are read by `numerals`, the rule the stored number comes from, so
    the verdict is about the number stored: `250,000` was stored as 250 and
    found printed as 250000. A comma the rule reads as a decimal mark may
    also separate two numbers in running text or CSV (`2026-08-01,72`), so
    the text is credited with both readings; a grouping is one number."""
    nfkc = unicodedata.normalize("NFKC", text or "").replace("−", "-")
    folded = numerals.fold(nfkc)
    try:
        printed = {Decimal(n) for t in {folded, numerals.fold(nfkc, separators=True)}
                   for pair in _NUMBER.findall(t) for n in pair if n}
    except InvalidOperation:
        # An unreadable exponent in one OCR token must not discard a report.
        printed = None
    letters = _SPACE.sub("", folded).casefold()

    def check(value: str) -> bool | None:
        v = numerals.fold(unicodedata.normalize("NFKC", value or "").replace("−", "-")).strip()
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


def row_kind(indicator: dict[str, Any], value: str) -> str:
    """`finding` or `measurement` for a read row, given its stored `value`.
    A finding's record carries its text, never a number, so an `Examination`
    row whose value is a number, alone or with its unit printed inside it
    (`170`, `170cm`, `72次/分`, `>=97%`), stays a measurement: a `身高 170 cm`
    the model tagged that way keeps its 170 and its unit. `结石 3 mm` and
    `眼压 15/16 mmHg` are findings. A blood pressure's two readings
    (`_split_blood_pressure`) are measurements whatever method the model gave
    the row it came from. `Imaging` and `Pathological` are otherwise the
    model's explicit choice and keep their long-standing behaviour.
    """
    method = str(indicator.get("detection_method") or "")
    if method not in FINDING_METHODS or indicator.get(_PRESSURE):
        return observations.KIND_MEASUREMENT
    if method == "Examination" and _is_quantity(value):
        return observations.KIND_MEASUREMENT
    return observations.KIND_FINDING


def _is_quantity(value: str) -> bool:
    """Whether `standardize.parse_value` reads `value` as a quantity and what
    follows the number is nothing or a unit. The parser alone also reads
    `结石 3 mm` and `2 cm 结节` as quantities."""
    text = numerals.fold(value.strip())
    head = _VALUE_HEAD.match(text)
    if head is None or parse_value(text).value_num is None:
        return False
    tail = text[head.end():].strip()
    return not tail or normalize_unit(tail) is not None


def _split_blood_pressure(indicators: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every blood pressure among a document's rows as its systolic and
    diastolic readings, the two a table row or a journal entry already
    makes of one (`collect.sentence.blood_pressure` decides, `table_indicators.
    split_pressure` splits, neither a second rule).

    A model hands a blood pressure over in one of three shapes. Split into
    `收缩压`/`舒张压`, it passes through. Left as one row (`血压 120/80`),
    it was stored as one narrative value with no number in it, whatever the
    method: nothing to chart, compare or flag. Split into two rows both
    still named for the pair (`血压`, `血压`), the two shared one series
    (`observations.coding_for` keys a series by name and unit) and read
    back as one line alternating 120, 80, 120, 80. The last two are split
    or renamed here, first-seen as systolic, the printed reading order; two
    rows are one pair only when they share a printed time and neither name
    already says its half. A unit printed after the pair is kept when it is
    a pressure unit; any other tail (`120/80 左侧卧位`) leaves the row as
    it was.
    """
    out: list[dict[str, Any]] = []
    i = 0
    while i < len(indicators):
        row = indicators[i]
        name = str(row.get("original_indicator") or "")
        nxt = indicators[i + 1] if i + 1 < len(indicators) else None
        if (nxt is not None and name and str(nxt.get("original_indicator") or "") == name
                and not _HALF.search(name) and row.get("date_time") == nxt.get("date_time")):
            pair = blood_pressure(name, f"{_bare(row)}/{_bare(nxt)}")
            if pair is not None:
                out += [{**row, "original_indicator": pair[0][0], _PRESSURE: True},
                        {**nxt, "original_indicator": pair[1][0], _PRESSURE: True}]
                i += 2
                continue
        out.extend(_pressure_readings(row, name) or [row])
        i += 1
    return out


def _bare(row: dict[str, Any]) -> str:
    """A row's value without the flag printed after it (`150↑`)."""
    return split_flag(str(row.get("value") or ""), "")[0].strip()


def _pressure_readings(row: dict[str, Any], name: str) -> list[dict[str, Any]] | None:
    """One row holding a whole blood pressure, as its two readings; None
    when it does not hold one. A flag printed after the pair goes to both,
    as a table row's does: the pair is what the report marked."""
    value, printed = split_flag(str(row.get("value") or ""), "")
    m = _PAIR.match(value.strip())
    pair = blood_pressure(name, f"{m.group(1)}/{m.group(2)}") if m else None
    if pair is None:
        return None
    tail = m.group(3).strip()
    unit = normalize_unit(tail) if tail else None
    if tail and (unit is None or unit_family(unit) != "Pres"):
        return None
    flag = {"_printed_flag": printed} if printed else {}
    reading = {**row, "unit": str(row.get("unit") or "") or tail, **flag}
    return [{**r, _PRESSURE: True} for r in split_pressure(reading, pair)]


def value_and_flag(indicator: dict[str, Any]) -> tuple[str, str]:
    """`(value, flag)` of a read row. A flag the report printed after the
    number (`6.49↑`, `5.6 H`) is moved out of the value the way the table
    rules move it (`split_flag`, the same `L`-is-litres guard). The rules
    retain a copied column or trailing flag in `_printed_flag`; else the
    row's `status`, kept only where the read side cannot work the answer
    out for itself, because `status` is the extraction's opinion, not
    something printed a reader could check. Either is kept only as `high`
    or `low`: a `normal` is a judgement, not a flag.

    A quantity whose printed range also parses to numbers is NOT judged
    here any more: `collect/query.py::_flag_of` compares the stored
    `value_num` against `ref_low`/`ref_high` at read time, off the same two
    columns every other view already reads, so a reader never sees a
    judgement this function would have had to store to make. A measurement's
    `status` with no range, or a range too numeric to need a model's opinion
    on, was the model's own guess, not the report's: an outpatient note's
    4.76 mmol/L glucose with no printed range was stored `high` on the
    model's word alone (2026-10-10), and a 5.6 mmol/L glucose printed inside
    its own 3.9–6.1 range was still handed to a reader as `high` because this
    function kept whatever the model wrote once any range was present
    (2026-11-18). `status` is still kept for what the read side cannot judge: a
    qualitative result (阳性/阴性, no number to compare) and a finding (an
    imaging, pathology or examination row, per `row_kind`), whose `high`
    means abnormal, not above a range it does not print. Left in, `6.49↑`
    was stored as a narrative with no number: all 16 value errors of
    GLM-OCR's end-to-end run (benchmarks/local_ocr, 2026-10-06)."""
    ref = str(indicator.get("reference_range") or "")
    value, printed = split_flag(str(indicator.get("value") or ""), ref)
    is_finding = row_kind(indicator, value) == observations.KIND_FINDING
    numeric_judgment = (not is_finding and parse_value(value, str(indicator.get("unit") or "")).value_kind == "quantity"
                        and parse_range(ref) != (None, None))
    judged = is_finding or (bool(ref.strip()) and not numeric_judgment)
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
    indicators = _split_blood_pressure(indicators)
    drafts = []
    bears_out = text_bears_out(text) if text else None
    unverified_count = 0
    for ix, indicator in enumerate(indicators):
        original_indicator = indicator.get("original_indicator")
        if not original_indicator:
            continue
        method = str(indicator.get("detection_method") or "")
        value, flag = value_and_flag(indicator)
        kind = row_kind(indicator, value)
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
            panel_text=str(indicator.get("panel") or ""),
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
