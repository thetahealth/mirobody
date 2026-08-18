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


class IndicatorExtractor:
    """Indicator extraction service class"""

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
    ) -> Tuple[List[Dict[str, Any]], Any]:
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

        Returns:
            Tuple[List[Dict[str, Any]], Any]: (List of extracted indicators, LLM response)
        """
        from mirobody.utils.llm import async_get_structured_output
        
        indicators = []
        start_time = time.time()

        try:
            if not original_text or not original_text.strip():
                logging.warning(f"[IndicatorExtractor] Empty original text provided for: {file_name}")
                return [], {}

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
                return [], {}

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
                return [], result

            # Deduplicate indicators
            indicators = IndicatorExtractor._deduplicate_indicators(indicators)

            # Save to database if required
            if save_to_db and indicators:
                if progress_callback:
                    await progress_callback(80, t("saving_indicators_to_database", language, "indicator_extractor", count=len(indicators)))

                db_start_time = time.time()
                saved_count = await FileParserDatabaseService.save_indicators_to_db(
                    str(user_id),
                    indicators,
                    exam_date,
                    ocr_db_id,
                    "",
                    source_table=source_table,
                    file_key=file_key,
                )
                db_duration = time.time() - db_start_time
                logging.info(f"[IndicatorExtractor] Database save completed - user_id: {user_id}, duration: {db_duration:.2f}s, saved: {saved_count}")

                if progress_callback:
                    await progress_callback(85, t("database_save_completed", language, "indicator_extractor", count=saved_count))

            if progress_callback:
                await progress_callback(90, t("indicator_extraction_completed", language, "indicator_extractor", count=len(indicators)))

            total_duration = time.time() - start_time
            logging.info(f"[IndicatorExtractor] Text extraction completed: {file_name}, {len(indicators)} indicators, {total_duration:.2f}s")

            return indicators, result

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

