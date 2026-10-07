"""A document's text read as readings, and the readings filed.

`IndicatorExtractor.read_indicators` reads the text: the table rules first,
then the model for what they left; `extract_indicators_from_text` files what
was read under the document's date (`resolve_report_date`, or the date the
person set) and writes it (`indicator_store.save_indicators_to_db`).
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from mirobody.collect.files.errors import UploadError
from mirobody.collect.files.services.indicator_store import save_indicators_to_db
from mirobody.collect.files.services.prompts.file_indicator_extract import (
    RESPONSE_SCHEMA_EXTRACT_INDICATORS,
    get_extract_indicators_prompt,
)
from mirobody.collect.files.services.report_date import manual_report_date, resolve_report_date
from mirobody.collect.files.services.table_indicators import (
    EXTRACTOR as TABLE_EXTRACTOR,
    is_unit,
    left_for_model,
    printed_flag,
    same_reading,
    split_flag,
    status_of,
    table_indicators,
    value_key,
    without_rows,
)
from mirobody.utils.coerce import parse_date
from mirobody.utils.req_ctx import request_language

logger = logging.getLogger(__name__)


#: What the date probe asks for. One field, one job: the full indicator
#: extraction takes 15-25 s on a lab page, and the Data page cannot ask "which
#: date?" until it knows there is no date, so the date is looked up first, on
#: its own, in a few seconds. Sample collection outranks receipt outranks report
#: date, the same priority the full extraction uses; empty when the document
#: shows none, never invented. Closed like every json_schema the product sends
#: (see `prompts/file_indicator_extract.py`).
_DATE_PROBE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "date_time": {
            "type": "string",
            "description": (
                "The examination date shown in the document, YYYY-MM-DD HH:MM:SS "
                "(HH:MM:SS may be 00:00:00). Priority: sample collection date > "
                "sample receipt date > report/review date. Empty string when the "
                "document shows no such date — never invent one."
            ),
        },
    },
    "required": ["date_time"],
}


#: Text longer than this that prints more than one page is read a page at a
#: time. MiniCPM5-2B given a 7-page check-up book (8,945 characters, 5,251
#: prompt tokens) in one request returned the header and `"indicators": []`;
#: page by page it found 77 of the 78 printed rows. Groups of pages up to
#: 3,000 and 4,500 characters found 72 and 67: a cover page in the same
#: request was enough for it to answer with nothing (benchmarks/local_models,
#: 2026-10-06). A one-page slip stays one request.
PAGE_READ_CHARS = 3000
#: How many pages are in flight at once: the local server's two slots.
PAGE_READ_CONCURRENCY = 2


#: Said before what the table rules left of a page. The rules already found
#: readings, so the document IS medical; asked to judge the leftover alone,
#: MiniCPM5-2B called it non-health and dropped it: six haematology rows on
#: one page, 26 on another where a generator's "SYNTHETIC SAMPLE" banner was
#: most of what remained (benchmarks/local_ocr, e2044e9, seeds 7, 8, 9).
REMAINDER_NOTE = (
    "This text is what is left of a medical report after its tables were read separately. "
    "It is health content: extract every measured value it still contains, and return no "
    "indicators only if none is left."
)


def answer_budget(text: str) -> int:
    """max_tokens for one extraction request: room for every row the text can
    hold (a row's JSON is ~3.5 tokens per printed character), not the whole
    context. At a flat 32,000 a looping MiniCPM5-2B wrote 30,067 tokens for
    13 minutes on one handwritten page (benchmarks/local_ocr, 2026-10-07)."""
    return min(32000, 2048 + 4 * len(text))


#: The page header `documents.extract` writes between pages.
_PAGE_MARK = re.compile(r"(?=^--- page \d+ ---$)", re.MULTILINE)


def _pages(text: str) -> list[str]:
    """The text's pages when it is long enough to read a page at a time,
    else `[text]`."""
    if len(text) <= PAGE_READ_CHARS:
        return [text]
    pages = [p for p in _PAGE_MARK.split(text) if p.strip()]
    return pages if len(pages) > 1 else [text]


def _merge_pages(answers: list[dict | None]) -> dict | None:
    """One answer from a document's per-page answers: every page's rows in
    page order; the document details from the first page that dates them (the
    cover usually carries them); medical if any page is, since a cover page
    alone reads as not health-related. None when no page was answered."""
    read = [a for a in answers if isinstance(a, dict)]
    if not read:
        return None
    medical = [a for a in read if a.get("content_type") == "medical_report"]
    merged = dict((medical or read)[0])
    dated = next((a for a in read if (a.get("content_info") or {}).get("date_time")), None)
    if dated is not None:
        merged["content_info"] = dated["content_info"]
    if medical:
        merged["content_type"] = "medical_report"
    merged["indicators"] = [i for a in read for i in (a.get("indicators") or [])]
    return merged


def _row_dates_in_a_log_only(indicators: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """`indicators` with each row's own `date_time` kept only when the rows
    print at least two different ones (a log, a table by day); otherwise every
    row is filed under the document's date. Measured on the 2026-10-07
    small-model eval: reading a check-up book a page at a time, the model put
    a page's `Printed: 2026-08-23` in two rows' `date_time`, and those two
    readings were filed a fortnight after the examination (2026-08-07)."""
    dated = {d for d in (parse_date(str(i.get("date_time") or "")) for i in indicators) if d is not None}
    if len(dated) >= 2:
        return indicators
    return [{**i, "date_time": ""} if i.get("date_time") else i for i in indicators]


def _latest_row_date(indicators: list[dict[str, Any]]) -> str:
    """The latest date the rows print, as the document's date when it names
    none of its own: a home log has no report date, and filed under the
    upload day it would ask "which date?", whose answer moves every row to it."""
    dated = [d for d in (parse_date(str(i.get("date_time") or "")) for i in indicators) if d is not None]
    return max(dated).strftime("%Y-%m-%d %H:%M:%S") if dated else ""


def _as_printed(row: dict[str, Any]) -> dict[str, Any]:
    """A model-read row whose `status` is kept only as a flag the report could
    have printed: high or low, and not one the value and the range printed
    beside it contradict. The prompt also lets the model judge a row against
    its range: on the demo check-up MiniCPM5-2B stored the printed `L` of
    `Resting Heart Rate 57 (60-100)` as high, and `normal` on 8 rows that
    printed no flag. The table rules' rows carry the printed flag itself."""
    status = printed_flag(str(row.get("status") or ""))
    value = split_flag(str(row.get("value") or ""), str(row.get("reference_range") or ""))[0]
    judged = status_of(value, str(row.get("reference_range") or ""))
    keep = status in ("high", "low") and judged in ("", status)
    return {**row, "status": status if keep else ""}


class NothingStored(UploadError):
    """A document's readings were read and none of them could be stored. The
    message is for the person who uploaded it: counts and rejection codes,
    never a reading."""


@dataclass
class Reading:
    """What one document's text was read as."""

    #: One row per printed reading (`_deduplicate_indicators`).
    rows: list[dict[str, Any]]
    #: The answer the rows came in, the table rules' merged with the model's;
    #: None when the model was asked and answered nothing.
    answer: dict[str, Any] | None
    #: `th_extraction.extractor`: which readers produced the rows.
    extractor: str
    #: The document's date as the answer gives it, else the latest a row prints.
    date: str


@dataclass
class Filed:
    """What one document's extraction left on the record."""

    indicators: list[dict[str, Any]]
    #: As `Reading.answer`: None tells "no model answered" from "none found".
    answer: dict[str, Any] | None
    #: Rows written, or already there from an earlier upload of the same file.
    stored: int
    #: `{"report_date", "date_source"}` the rows were filed under; None when
    #: there were none.
    report: dict[str, str] | None


async def _filing_date(user_id: int, file_key: str, document_date: str,
                       probed: tuple[datetime, str] | None) -> tuple[datetime, str]:
    """The date a document's readings are filed under, and where it came
    from: the date the person set on the file (they may have answered
    "which date?" while this ran), else the probe's date when it read one off
    the document, else this extraction's (`resolve_report_date`)."""
    manual = await manual_report_date(file_key)
    if manual is not None:
        return manual, "manual"
    if probed is not None and probed[1] == "extracted":
        return probed
    return await resolve_report_date(str(user_id), document_date)


class IndicatorExtractor:
    """Reads a document's text as readings, and files them."""

    @staticmethod
    async def probe_report_date(original_text: str) -> str:
        """The document's examination date alone, ahead of the full extraction.

        Returns the raw string the model gave (parsed by `resolve_report_date`),
        or "": for no date AND for any failure, so the caller falls through to
        the full extraction's own date field either way.
        """
        if not original_text or not original_text.strip():
            return ""
        from mirobody.utils.llm import async_get_structured_output

        answer = await async_get_structured_output(
            messages=[
                {"role": "system", "content": "You read medical documents and report ONE fact: the examination date."},
                {"role": "user", "content": f"Document:\n\n{original_text[:12000]}"},
            ],
            response_format={"type": "json_schema", "json_schema": {"name": "report_date", "schema": _DATE_PROBE_SCHEMA}},
            temperature=0,
            max_tokens=200,
        )
        return str((answer or {}).get("date_time") or "").strip()

    @staticmethod
    async def read_indicators(original_text: str) -> Reading:
        """The readings in a document's text. Tables are read by their columns
        first, whatever model reads the rest: a born-digital PDF's from its
        text layer, a scan's from the OCR tables pass, a CSV's and a sheet's
        as they are. The model reads what the rules left, and a rule's row
        outranks the model's for the same printed row. In every mode, a vendor
        key included: for three of four cloud references
        (benchmarks/local_models, ref-*-rules) readings on no printed row fell
        from about 48 to 27, and the text sent by 37%."""
        started = time.monotonic()
        rules, table_date, unread_count = table_indicators(original_text)
        rest = without_rows(original_text, rules) if rules else original_text
        if rules and not unread_count and not left_for_model(rest):
            answer: dict[str, Any] | None = {"indicators": rules, "content_info": {"date_time": table_date}}
            extractor = TABLE_EXTRACTOR
        else:
            answer = await IndicatorExtractor._llm_extract(rest, request_language(), remainder=bool(rules))
            if answer:
                answer = {**answer, "indicators": [_as_printed(i) for i in answer.get("indicators") or []]}
            extractor = ""
            if rules:
                extractor = f"{TABLE_EXTRACTOR}+llm:file-parser@indicators-v1"
                answer = IndicatorExtractor._merge_rule_rows(rules, table_date, answer)
        if not answer:
            logger.warning("indicator extraction: no model answered: rule_row_count=%d", len(rules))
            return Reading([], None, extractor, "")
        indicators = answer.get("indicators") or []
        date = (answer.get("content_info") or {}).get("date_time", "") or _latest_row_date(indicators)
        rows = IndicatorExtractor._deduplicate_indicators(_row_dates_in_a_log_only(indicators))
        logger.info("indicators read: row_count=%d rule_row_count=%d unread_row_count=%d duration_ms=%d",
                    len(rows), len(rules), unread_count, int((time.monotonic() - started) * 1000))
        return Reading(rows, answer, extractor, date)

    @staticmethod
    async def extract_indicators_from_text(
        original_text: str,
        user_id: int,
        file_key: str,
        report_date: tuple[datetime, str] | None = None,
    ) -> Filed:
        """Read a document's text (`read_indicators`) and file its readings
        under `file_key`. `report_date` is the date probe's `(datetime,
        date_source)`: an "extracted" one wins over this extraction's own
        date field; an "upload_time" one is only a fallback.

        Raises `NothingStored` when rows were read and none could be written:
        reported as a document with no readings, a rejected batch or a
        database that refused every row looked like a finished upload.
        """
        reading = await IndicatorExtractor.read_indicators(original_text)
        if not reading.rows:
            return Filed([], reading.answer, 0, None)
        start, source = await _filing_date(user_id, file_key, reading.date, report_date)
        report = await save_indicators_to_db(str(user_id), reading.rows, start, source, file_key, reading.extractor)
        stored = report.inserted + report.skipped
        if not stored:
            rejected = ", ".join(f"{reason}={n}" for reason, n in sorted(report.rejected.items())) or "none written"
            raise NothingStored(f"none of the {len(reading.rows)} readings could be stored ({rejected})")
        return Filed(reading.rows, reading.answer, stored,
                     {"report_date": start.strftime("%Y-%m-%d %H:%M:%S"), "date_source": source})

    @staticmethod
    def _merge_rule_rows(rows: list[dict], table_date: str, llm_ret: dict | None) -> dict:
        """The rule rows, then the model's rows for every printed row the rules
        did not read: not the same name, and not the same value under a name a
        misread character apart (the OCR's `y-` for the text layer's `γ-`) or
        a name the vocabulary resolves to the same analyte (`血红蛋白` beside
        the rule's `血红蛋白（HGB）`): `same_reading`."""
        result = dict(llm_ret or {})
        seen = {r["original_indicator"].strip().lower() for r in rows}
        extra = [i for i in result.get("indicators") or []
                 if str(i.get("original_indicator", "")).strip().lower() not in seen
                 and not any(same_reading(i, r, misread=True) for r in rows)]
        result["indicators"] = rows + extra
        info = dict(result.get("content_info") or {})
        info["date_time"] = table_date or info.get("date_time", "")
        result["content_info"] = info
        return result

    @staticmethod
    async def _llm_extract(original_text: str, language: str, *, remainder: bool = False) -> dict | None:
        """The model's reading of a document: `indicators` and `content_info`,
        or None. A long document is read a page at a time (`_pages`).
        `remainder`: the text is what the table rules left (`REMAINDER_NOTE`)."""
        pages = _pages(original_text)
        if len(pages) == 1:
            return await IndicatorExtractor._llm_extract_one(original_text, language, remainder=remainder)
        gate = asyncio.Semaphore(PAGE_READ_CONCURRENCY)

        async def read(page: str) -> dict | None:
            async with gate:
                return await IndicatorExtractor._llm_extract_one(page, language, remainder=remainder)

        answers = await asyncio.gather(*(read(p) for p in pages))
        failed = sum(1 for a in answers if not isinstance(a, dict))
        logger.info("indicator extraction read page by page: page_count=%d unanswered_count=%d", len(pages), failed)
        return _merge_pages(list(answers))

    @staticmethod
    async def _llm_extract_one(original_text: str, language: str, *, remainder: bool = False) -> dict | None:
        """One request: the whole text given, `indicators` and `content_info`, or None."""
        from mirobody.utils.llm import async_get_structured_output

        note = f"{REMAINDER_NOTE}\n\n" if remainder else ""
        messages = [
            {"role": "system", "content": get_extract_indicators_prompt(language=language)},
            {"role": "user", "content": f"{note}Please extract health indicators from the following document content:\n\n{original_text}"},
        ]
        return await async_get_structured_output(
            messages=messages,
            response_format={"type": "json_schema", "json_schema": {"name": "indicators_response", "schema": RESPONSE_SCHEMA_EXTRACT_INDICATORS}},
            temperature=0.1,
            max_tokens=answer_budget(original_text),
        )

    @staticmethod
    def _deduplicate_indicators(
        indicators: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """One stored row per printed reading: rows with the same value
        (`value_key`), the same analyte (`same_reading`) and the same date are
        one reading, from two pages, two passes or one page read twice; the
        row that also carries the printed range and unit is the one kept, so a
        summary page's `Apolipoprotein A1 1.69 g/L↑` gives way to the table's
        row. A log's same weight on two mornings is two readings: the dates
        differ. A row whose value is a unit and nothing else (`HGB | L`,
        `CREA | mmol/L`) is no reading: MiniCPM5-2B wrote 22 such rows for one
        check-up book on the 2026-10-07 eval, and each sat in the catalogue as
        a series of its own (`unit:conflict`)."""
        if not indicators:
            return []

        def fullness(row: dict[str, Any]) -> int:
            return bool(str(row.get("reference_range") or "").strip()) + bool(str(row.get("unit") or "").strip())

        groups: dict[tuple, list[int]] = {}
        unique_indicators: list[dict[str, Any]] = []
        for indicator in indicators:
            name = str(indicator.get("original_indicator") or "").strip()
            value = str(indicator.get("value") or "").strip()
            if not name or not value or is_unit(split_flag(value, "")[0].strip()):
                continue
            key = (value_key(value), str(parse_date(str(indicator.get("date_time") or "")) or ""))
            twins = groups.setdefault(key, [])
            twin = next((k for k in twins if same_reading(unique_indicators[k], indicator)), None)
            if twin is None:
                twins.append(len(unique_indicators))
                unique_indicators.append(indicator)
            elif fullness(indicator) > fullness(unique_indicators[twin]):
                unique_indicators[twin] = indicator

        return unique_indicators
