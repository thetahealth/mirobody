"""
Indicator extraction service

Responsible for extracting health indicators from medical documents
"""

import json
import time
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx

from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.prompts.file_indicator_extract import (
    get_extract_indicators_prompt,
    RESPONSE_SCHEMA_EXTRACT_INDICATORS,
)


#: What the date probe asks for. One field, one job: the full indicator
#: extraction takes 15-25 s on a lab page, and the Data page cannot ask "which
#: date?" until it knows there is no date — so the date is looked up first, on
#: its own, in a few seconds. Sample collection outranks receipt outranks report
#: date, the same priority the full extraction uses; empty when the document
#: shows none, never invented.
_DATE_PROBE_SCHEMA = {
    "type": "object",
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


class IndicatorExtractor:
    """Indicator extraction service class"""

    @staticmethod
    async def probe_report_date(original_text: str) -> str:
        """The document's examination date alone, ahead of the full extraction.

        Returns the raw string the model gave (parsed by `resolve_report_date`),
        or "" — for no date AND for any failure, so the caller falls through to
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
            logging.warning(f"[IndicatorExtractor] date probe failed: {e}")
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
        progress_callback: Optional[Callable[[int, str], None]] = None,
        report_date: Optional[Tuple[Any, str]] = None,
    ) -> Tuple[List[Dict[str, Any]], Any, Optional[Dict[str, str]]]:
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
            (indicators, LLM response, report) — `report` is
            `{"report_date", "date_source"}` as resolved by
            `FileParserDatabaseService.resolve_report_date` when readings were
            saved, else None; the handler records it on the th_files row.
        """
        from mirobody.utils.llm import async_get_structured_output
        
        indicators = []
        start_time = time.time()

        try:
            if not original_text or not original_text.strip():
                logging.warning(f"[IndicatorExtractor] Empty original text provided for: {file_name}")
                return [], {}, None

            language = get_req_ctx("language", "en")
            
            if progress_callback:
                await progress_callback(65, t("analyzing_medical_indicators", language, "indicator_extractor", filename=file_name))

            logging.info(f"🔄 [IndicatorExtractor] Extracting indicators from text - user_id: {user_id}, text_length: {len(original_text)}")

            # Generate prompt dynamically based on user's language setting
            dynamic_prompt = get_extract_indicators_prompt(language=language)
            
            # Build messages for LLM
            messages = [
                {
                    "role": "system",
                    "content": dynamic_prompt
                },
                {
                    "role": "user",
                    "content": f"Please extract health indicators from the following document content:\n\n{original_text}"
                }
            ]

            # Use structured output with schema
            api_start_time = time.time()
            llm_ret = await async_get_structured_output(
                messages=messages,
                response_format={"type": "json_schema", "json_schema": {"name": "indicators_response", "schema": RESPONSE_SCHEMA_EXTRACT_INDICATORS}},
                temperature=0.1,
                max_tokens=32000
            )
            api_duration = time.time() - api_start_time
            logging.info(f"✅ [IndicatorExtractor] LLM text extraction completed - user_id: {user_id}, duration: {api_duration:.2f}s")

            if not llm_ret:
                logging.warning(f"[IndicatorExtractor] LLM returned empty response for text extraction - user_id: {user_id}")
                return [], {}, None

            if progress_callback:
                await progress_callback(75, t("parsing_indicator_data", language, "indicator_extractor"))

            # Parse result (async_get_structured_output returns dict directly)
            result = llm_ret if isinstance(llm_ret, dict) else json.loads(llm_ret)
            indicators = result.get("indicators", [])
            exam_date = result.get("content_info", {}).get("date_time", "")
            file_abstract = result.get("file_abstract", "")

            logging.info(f"[IndicatorExtractor] Parsed {len(indicators)} indicators from text - user_id: {user_id}")

            if not indicators:
                logging.info(f"[IndicatorExtractor] No indicators found in text - user_id: {user_id}, file_name: {file_name}")
                return [], result, None

            # Deduplicate indicators
            indicators = IndicatorExtractor._deduplicate_indicators(indicators)

            # Save to database if required
            report = None
            if save_to_db and indicators:
                if progress_callback:
                    await progress_callback(80, t("saving_indicators_to_database", language, "indicator_extractor", count=len(indicators)))

                db_start_time = time.time()
                if report_date and report_date[1] == "extracted":
                    start_time_dt, date_source = report_date
                else:
                    start_time_dt, date_source = await FileParserDatabaseService.resolve_report_date(str(user_id), exam_date)
                # The user may have answered "which date?" while this ran (the
                # Data page bar, or the agent's set_report_date): the file row
                # then already says `manual`, and that answer outranks anything
                # read off the document.
                manual = await FileParserDatabaseService.manual_report_date(file_key) if file_key else None
                if manual is not None:
                    start_time_dt, date_source = manual, "manual"
                saved_count = await FileParserDatabaseService.save_indicators_to_db(
                    str(user_id),
                    indicators,
                    start_time_dt,
                    date_source,
                    ocr_db_id,
                    "",
                    source_table=source_table,
                    file_key=file_key,
                )
                report = {"report_date": start_time_dt.strftime("%Y-%m-%d %H:%M:%S"), "date_source": date_source}
                db_duration = time.time() - db_start_time
                logging.info(f"[IndicatorExtractor] Database save completed - user_id: {user_id}, duration: {db_duration:.2f}s, saved: {saved_count}")

                if progress_callback:
                    await progress_callback(85, t("database_save_completed", language, "indicator_extractor", count=saved_count))

            if progress_callback:
                await progress_callback(90, t("indicator_extraction_completed", language, "indicator_extractor", count=len(indicators)))

            total_duration = time.time() - start_time
            logging.info(f"[IndicatorExtractor] Text extraction completed: {file_name}, {len(indicators)} indicators, {total_duration:.2f}s")

            return indicators, result, report

        except json.JSONDecodeError as e:
            logging.error(f"[IndicatorExtractor] JSON parse failed for text extraction: {e}", exc_info=True)
            if progress_callback:
                language = get_req_ctx("language", "en")
                await progress_callback(90, t("json_parsing_failed", language, "indicator_extractor"))
            raise ValueError(f"JSON parsing failed: {str(e)}")
        except Exception as e:
            logging.error(f"[IndicatorExtractor] Text extraction failed: {e}", exc_info=True)
            if progress_callback:
                language = get_req_ctx("language", "en")
                await progress_callback(90, t("indicator_extraction_error", language, "indicator_extractor"))
            raise e

    @staticmethod
    def _deduplicate_indicators(
        indicators: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate indicator data

        Args:
            indicators: Indicator list

        Returns:
            List[Dict[str, Any]]: Deduplicated indicator list
        """
        if not indicators:
            return []

        # Use indicator name and value as deduplication key
        seen = set()
        unique_indicators = []

        for indicator in indicators:
            # Create deduplication key
            name = indicator.get("original_indicator", "").strip().lower()
            value = indicator.get("value", "").strip()
            if not name or not value:
                continue

            dedup_key = f"{name}_{value}"

            if dedup_key not in seen:
                seen.add(dedup_key)
                unique_indicators.append(indicator)

        logging.info(f"Indicator deduplication completed: {len(indicators)} -> {len(unique_indicators)}")
        return unique_indicators

