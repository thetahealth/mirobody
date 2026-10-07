"""
Indicator extraction service

Responsible for extracting health indicators from medical documents
"""

from mirobody.collect.files.services.indicator_store import save_indicators_to_db
from mirobody.collect.files.services.report_date import manual_report_date, resolve_report_date
import asyncio
import json
import re
import time
import logging
from typing import Any
from collections.abc import Callable

from mirobody.collect.files.services.table_indicators import (
    EXTRACTOR as TABLE_EXTRACTOR,
    _is_unit,
    _split_flag,
    left_for_model,
    same_reading,
    table_indicators,
    value_key,
    without_rows,
)
from mirobody.utils.coerce import parse_date
from mirobody.utils.config.llm import resolve_route
from mirobody.utils.i18n import localize
from mirobody.utils.req_ctx import request_language
from mirobody.collect.files.services.prompts.file_indicator_extract import (
    get_extract_indicators_prompt,
    RESPONSE_SCHEMA_EXTRACT_INDICATORS,
)

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


def _latest_row_date(indicators: list[dict[str, Any]]) -> str:
    """The latest date the rows print, as the document's date when it names
    none of its own: a home log has no report date, and filed under the
    upload day it would ask "which date?", whose answer moves every row to it."""
    dated = [d for d in (parse_date(str(i.get("date_time") or "")) for i in indicators) if d is not None]
    return max(dated).strftime("%Y-%m-%d %H:%M:%S") if dated else ""


class IndicatorExtractor:
    """Indicator extraction service class"""

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

        try:
            ret = await async_get_structured_output(
                messages=[
                    {"role": "system", "content": "You read medical documents and report ONE fact: the examination date."},
                    {"role": "user", "content": f"Document:\n\n{original_text[:12000]}"},
                ],
                response_format={"type": "json_schema", "json_schema": {"name": "report_date", "schema": _DATE_PROBE_SCHEMA}},
                temperature=0,
                max_tokens=200,
            )
        except Exception as e:
            logger.warning(f"[IndicatorExtractor] date probe failed: {e}")
            return ""
        if not ret:
            return ""
        result = ret if isinstance(ret, dict) else json.loads(ret)
        return str(result.get("date_time") or "").strip()

    @staticmethod
    async def extract_indicators_from_text(
        original_text: str,
        user_id: int,
        ocr_db_id: int = 0,
        source_table: str = "th_files",
        file_name: str = "",
        file_key: str = None,
        save_to_db: bool = True,
        progress_callback: Callable[[int, str], None] | None = None,
        report_date: tuple[Any, str] | None = None,
    ) -> tuple[list[dict[str, Any]], Any, dict[str, str] | None]:
        """
        Extract health indicators from pre-extracted original text.
        
        This method uses the already extracted text content instead of
        processing the original file, which is faster and avoids
        redundant file processing.

        Args:
            original_text: Pre-extracted text content from file
            user_id: User ID
            ocr_db_id: OCR record ID (optional)
            source_table: Source table name
            file_name: Original file name (for logging)
            file_key: File key from files array
            save_to_db: Whether to save indicators to database
            progress_callback: Progress callback function
            report_date: `(datetime, date_source)` already resolved by the
                caller (the date probe). An "extracted" one wins over this
                extraction's own date field; an "upload_time" one is only a
                fallback, so a date this extraction finds still upgrades it.

        Returns:
            (indicators, LLM response, report): `report` is
            `{"report_date", "date_source"}` as resolved by
            `resolve_report_date` when readings were
            saved, else None; the handler records it on the th_files row.

            The LLM response is ``None`` when the extraction call itself
            produced nothing (no provider, or every provider failed) and a
            dict (possibly with zero indicators) when a model answered. The
            two used to come back identical (``[], {}``), so "this deployment
            cannot extract" rendered exactly like "this document has no
            indicators" (#68).
        """
        indicators = []
        start_time = time.time()

        try:
            if not original_text or not original_text.strip():
                logger.warning(f"[IndicatorExtractor] Empty original text provided for: {file_name}")
                return [], {}, None

            language = request_language()
            
            if progress_callback:
                await progress_callback(65, localize("analyzing_medical_indicators", language, "indicator_extractor", filename=file_name))

            logger.info(f"[IndicatorExtractor] Extracting indicators from text - user_id: {user_id}, text_length: {len(original_text)}")

            # With a document-OCR model routed, its tables are read by their
            # columns. The model reads what the rules left (a row they could
            # not read, text outside any table), and a rule's row outranks the
            # model's for the same printed row: it is the value as printed.
            extractor = ""
            rows, table_date, unread_count = table_indicators(original_text) if resolve_route("ocr") else ([], "", 0)
            rest = without_rows(original_text, rows) if rows else original_text
            if rows and not unread_count and not left_for_model(rest):
                extractor = TABLE_EXTRACTOR
                llm_ret = {"indicators": rows, "content_info": {"date_time": table_date}}
                logger.info(f"[IndicatorExtractor] {len(rows)} indicators read off tables, no model - user_id: {user_id}")
            else:
                llm_ret = await IndicatorExtractor._llm_extract(rest, language, user_id, remainder=bool(rows))
                if rows:
                    extractor = f"{TABLE_EXTRACTOR}+llm:file-parser@indicators-v1"
                    llm_ret = IndicatorExtractor._merge_rule_rows(rows, table_date, llm_ret)
                    logger.info(f"[IndicatorExtractor] {len(rows)} indicators read off tables, {unread_count} rows and the text outside them left to the model - user_id: {user_id}")

            if not llm_ret:
                logger.warning(f"[IndicatorExtractor] LLM returned empty response for text extraction - user_id: {user_id}")
                return [], None, None

            if progress_callback:
                await progress_callback(75, localize("parsing_indicator_data", language, "indicator_extractor"))

            # Parse result (async_get_structured_output returns dict directly)
            result = llm_ret if isinstance(llm_ret, dict) else json.loads(llm_ret)
            indicators = result.get("indicators", [])
            exam_date = result.get("content_info", {}).get("date_time", "") or _latest_row_date(indicators)

            logger.info(f"[IndicatorExtractor] Parsed {len(indicators)} indicators from text - user_id: {user_id}")

            if not indicators:
                logger.info(f"[IndicatorExtractor] No indicators found in text - user_id: {user_id}, file_name: {file_name}")
                return [], result, None

            # Deduplicate indicators
            indicators = IndicatorExtractor._deduplicate_indicators(indicators)

            # Save to database if required
            report = None
            if save_to_db and indicators:
                if progress_callback:
                    await progress_callback(80, localize("saving_indicators_to_database", language, "indicator_extractor", count=len(indicators)))

                db_start_time = time.time()
                if report_date and report_date[1] == "extracted":
                    start_time_dt, date_source = report_date
                else:
                    start_time_dt, date_source = await resolve_report_date(str(user_id), exam_date)
                # The user may have answered "which date?" while this ran (the
                # Data page bar, or the agent's set_report_date): the file row
                # then already says `manual`, and that answer outranks anything
                # read off the document.
                manual = await manual_report_date(file_key) if file_key else None
                if manual is not None:
                    start_time_dt, date_source = manual, "manual"
                saved_count = await save_indicators_to_db(
                    str(user_id),
                    indicators,
                    start_time_dt,
                    date_source,
                    ocr_db_id,
                    "",
                    source_table=source_table,
                    file_key=file_key,
                    extractor=extractor,
                )
                report = {"report_date": start_time_dt.strftime("%Y-%m-%d %H:%M:%S"), "date_source": date_source}
                db_duration = time.time() - db_start_time
                logger.info(f"[IndicatorExtractor] Database save completed - user_id: {user_id}, duration: {db_duration:.2f}s, saved: {saved_count}")

                if progress_callback:
                    await progress_callback(85, localize("database_save_completed", language, "indicator_extractor", count=saved_count))

            if progress_callback:
                await progress_callback(90, localize("indicator_extraction_completed", language, "indicator_extractor", count=len(indicators)))

            total_duration = time.time() - start_time
            logger.info(f"[IndicatorExtractor] Text extraction completed: {file_name}, {len(indicators)} indicators, {total_duration:.2f}s")

            return indicators, result, report

        except json.JSONDecodeError as e:
            logger.error(f"[IndicatorExtractor] JSON parse failed for text extraction: {e}", exc_info=True)
            if progress_callback:
                language = request_language()
                await progress_callback(90, localize("json_parsing_failed", language, "indicator_extractor"))
            raise ValueError(f"JSON parsing failed: {str(e)}")
        except Exception as e:
            logger.error(f"[IndicatorExtractor] Text extraction failed: {e}", exc_info=True)
            if progress_callback:
                language = request_language()
                await progress_callback(90, localize("indicator_extraction_error", language, "indicator_extractor"))
            raise e

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
    async def _llm_extract(original_text: str, language: str, user_id: int, *, remainder: bool = False) -> dict | None:
        """The model's reading of a document: `indicators` and `content_info`,
        or None. A long document is read a page at a time (`_pages`).
        `remainder`: the text is what the table rules left (`REMAINDER_NOTE`)."""
        pages = _pages(original_text)
        if len(pages) == 1:
            return await IndicatorExtractor._llm_extract_one(original_text, language, user_id, remainder=remainder)
        gate = asyncio.Semaphore(PAGE_READ_CONCURRENCY)

        async def read(page: str) -> dict | None:
            async with gate:
                return await IndicatorExtractor._llm_extract_one(page, language, user_id, remainder=remainder)

        answers = await asyncio.gather(*(read(p) for p in pages))
        failed = sum(1 for a in answers if not isinstance(a, dict))
        logger.info(f"[IndicatorExtractor] read {len(pages)} pages, {failed} unanswered - user_id: {user_id}")
        return _merge_pages(list(answers))

    @staticmethod
    async def _llm_extract_one(
        original_text: str, language: str, user_id: int, *, remainder: bool = False
    ) -> dict | None:
        """One request: the whole text given, `indicators` and `content_info`, or None."""
        from mirobody.utils.llm import async_get_structured_output

        note = f"{REMAINDER_NOTE}\n\n" if remainder else ""
        messages = [
            {"role": "system", "content": get_extract_indicators_prompt(language=language)},
            {"role": "user", "content": f"{note}Please extract health indicators from the following document content:\n\n{original_text}"},
        ]
        api_start_time = time.time()
        llm_ret = await async_get_structured_output(
            messages=messages,
            response_format={"type": "json_schema", "json_schema": {"name": "indicators_response", "schema": RESPONSE_SCHEMA_EXTRACT_INDICATORS}},
            temperature=0.1,
            max_tokens=answer_budget(original_text),
        )
        api_duration = time.time() - api_start_time
        logger.info(f"[IndicatorExtractor] LLM text extraction completed - user_id: {user_id}, duration: {api_duration:.2f}s")
        return llm_ret

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
            if not name or not value or _is_unit(_split_flag(value, "")[0].strip()):
                continue
            key = (value_key(value), str(parse_date(str(indicator.get("date_time") or "")) or ""))
            twins = groups.setdefault(key, [])
            twin = next((k for k in twins if same_reading(unique_indicators[k], indicator)), None)
            if twin is None:
                twins.append(len(unique_indicators))
                unique_indicators.append(indicator)
            elif fullness(indicator) > fullness(unique_indicators[twin]):
                unique_indicators[twin] = indicator

        logger.info(f"Indicator deduplication completed: {len(indicators)} -> {len(unique_indicators)}")
        return unique_indicators
