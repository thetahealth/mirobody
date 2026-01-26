"""
Indicator extraction service

Responsible for extracting health indicators from medical documents
"""

import asyncio
import json
import os
import time
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from mirobody.utils.i18n import t
from mirobody.utils.req_ctx import get_req_ctx
from mirobody.utils.config import safe_read_cfg

from google.genai import types

from mirobody.utils.llm import unified_file_extract


from mirobody.pulse.file_parser.services.content_formatter import ContentFormatter
from mirobody.pulse.file_parser.services.database_services import FileParserDatabaseService
from mirobody.pulse.file_parser.services.pdf_splitter import PDFSplitter
from mirobody.pulse.file_parser.services.temp_file_manager import TempFileManager

from mirobody.pulse.file_parser.services.prompts.file_indicator_extract import (
    get_extract_indicators_prompt,
    RESPONSE_SCHEMA_EXTRACT_INDICATORS,
)



class IndicatorExtractor:
    """Indicator extraction service class"""

    # Parallel processing configuration
    MAX_CONCURRENT_PAGES = 5  # Maximum concurrent pages for processing
    MIN_PAGES_FOR_PARALLEL = 2  # Minimum pages to trigger parallel processing

    @staticmethod
    async def extract_indicators_from_file(
        ocr_db_id: int,
        temp_file_path: str,
        content_type: str,
        file_name: str,
        user_id: int,
        source_table: str = "health_ocr",
        progress_callback: Optional[Callable[[int, str], None]] = None,
        file_key: str = None,
    ) -> Tuple[List[Dict[str, Any]], Any]:
        """
        Extract health indicators from file

        Args:
            ocr_db_id: OCR record ID
            file_key: File key from files array
            temp_file_path: Temporary file path
            content_type: File type
            file_name: File name
            user_id: User ID
            progress_callback: Progress callback function

        Returns:
            Tuple[List[Dict[str, Any]], Any]: (List of extracted indicators, LLM response which may contain file_abstract)
        """

        llm_ret = ""
        indicators = []
        start_time = time.time()

        try:
            # Check if indicator extraction is enabled
            enable_indicator_extraction = safe_read_cfg("ENABLE_INDICATOR_EXTRACTION") or os.environ.get("ENABLE_INDICATOR_EXTRACTION", 0)
            is_aliyun = safe_read_cfg("CLUSTER") == "ALIYUN"
            if not int(enable_indicator_extraction) and not is_aliyun:
                logging.info(f"Indicator extraction disabled, skipping: {file_name}")
                if progress_callback:
                    language = get_req_ctx("language", "en")
                    await progress_callback(90, t("indicator_extraction_skipped", language, "indicator_extractor"))
                return indicators, {}
            
            # Check if file type is supported
            if not (content_type.startswith("image/") or content_type == "application/pdf"):
                logging.warning(f"Unsupported file type: {file_name}")
                if progress_callback:
                    await progress_callback(90, "File type not supported, skipping indicator extraction")
                return indicators, {}

            if progress_callback:
                await progress_callback(65, f"Starting to analyze health indicators in {file_name}...")

            # If PDF file, use paginated parallel processing
            if content_type == "application/pdf":
                (
                    indicators,
                    llm_ret,
                ) = await IndicatorExtractor._extract_indicators_from_pdf_parallel(
                    temp_file_path,
                    file_name,
                    user_id,
                    ocr_db_id,
                    source_table,
                    progress_callback,
                    file_key,
                )
            else:
                # Image files use original processing method
                (
                    indicators,
                    llm_ret,
                ) = await IndicatorExtractor._extract_indicators_from_single_file(
                    temp_file_path,
                    content_type,
                    file_name,
                    user_id,
                    ocr_db_id,
                    source_table,
                    True,
                    progress_callback,
                    file_key,
                )

            # Ensure progress callback is called
            if progress_callback:
                language = get_req_ctx("language", "en")
                await progress_callback(88, t("organizing_indicator_data", language, "indicator_extractor", count=len(indicators)))
                await asyncio.sleep(0.1)
                await progress_callback(90, t("indicator_extraction_completed", language, "indicator_extractor", count=len(indicators)))

            logging.info(f"Indicator extraction completed: {file_name}, {len(indicators)} indicators, {time.time() - start_time:.2f}s")

            return indicators, llm_ret

        except json.JSONDecodeError as e:
            logging.error(f"JSON parse failed: {file_name}, error: {e}", exc_info=True)
            if progress_callback:
                language = get_req_ctx("language", "en")
                await progress_callback(88, t("json_parsing_error", language, "indicator_extractor"))
                await asyncio.sleep(0.1)
                await progress_callback(90, t("json_parsing_failed", language, "indicator_extractor"))
            raise ValueError(f"JSON parsing failed: {str(e)}")
        except Exception as e:
            logging.error(f"Indicator extraction failed: {file_name}, error: {e}", exc_info=True)
            if progress_callback:
                language = get_req_ctx("language", "en")
                await progress_callback(88, t("processing_issue_completing", language, "indicator_extractor"))
                await asyncio.sleep(0.1)
                await progress_callback(90, t("indicator_extraction_error", language, "indicator_extractor"))
            raise e
        finally:
            TempFileManager.cleanup_temp_file(temp_file_path)

        return indicators

    @staticmethod
    async def _extract_indicators_from_pdf_parallel(
        temp_file_path: str,
        file_name: str,
        user_id: int,
        ocr_db_id: int,
        source_table: str,
        progress_callback: Optional[Callable[[int, str], None]] = None,
        file_key: str = None,
    ) -> tuple[List[Dict[str, Any]], Any]:
        """
        Process PDF file using parallel page processing

        Args:
            temp_file_path: Temporary file path
            file_name: File name
            user_id: User ID
            ocr_db_id: OCR record ID
            source_table: Source table name
            progress_callback: Progress callback function

        Returns:
            tuple[List[Dict[str, Any]], Any]: (indicator list, merged LLM response)
        """
        pdf_start_time = time.time()

        # Get PDF page count
        page_count = PDFSplitter.get_pdf_page_count(temp_file_path)
        logging.info(f"PDF pages: {page_count}, file: {file_name}")

        if progress_callback:
            language = get_req_ctx("language", "en")
            await progress_callback(67, t("pdf_pages_analysis", language, "indicator_extractor", count=page_count))

        # Use single-file mode for few pages
        if page_count <= IndicatorExtractor.MIN_PAGES_FOR_PARALLEL:
            return await IndicatorExtractor._extract_indicators_from_single_file(
                temp_file_path,
                "application/pdf",
                file_name,
                user_id,
                ocr_db_id,
                source_table,
                True,
                progress_callback,
                file_key,
            )

        # Split PDF into pages
        page_files = PDFSplitter.split_pdf_to_pages(temp_file_path)
        if not page_files:
            logging.error(f"PDF split failed: {file_name}")
            return [], {}

        logging.info(f"PDF split: {len(page_files)} pages")

        if progress_callback:
            language = get_req_ctx("language", "en")
            
            await progress_callback(70, t("pdf_split_completed", language, "indicator_extractor", count=len(page_files)))

        try:
            # Process all pages in parallel
            if page_files:
                all_indicators, combined_llm_ret = await IndicatorExtractor._process_pages_parallel(
                    page_files, file_name, user_id, ocr_db_id, source_table, progress_callback, file_key,
                )
                logging.info(f"Parallel processing done: {len(all_indicators)} indicators, {time.time() - pdf_start_time:.2f}s")
                return all_indicators, combined_llm_ret
        except Exception as e:
            logging.error(f"PDF processing failed: {file_name}, error: {e}", exc_info=True)
            raise e
        finally:
            PDFSplitter.cleanup_page_files(page_files)

    @staticmethod
    async def _process_pages_parallel(
        page_files: List[str],
        file_name: str,
        user_id: int,
        ocr_db_id: int,
        source_table: str,
        progress_callback: Optional[Callable[[int, str], None]] = None,
        file_key: str = None,
    ) -> tuple[List[Dict[str, Any]], dict]:
        """
        Process multiple pages in parallel

        Args:
            page_files: List of page file paths
            file_name: Original file name
            user_id: User ID
            ocr_db_id: OCR record ID
            source_table: Source table name
            progress_callback: Progress callback function

        Returns:
            tuple[List[Dict[str, Any]], dict]: (merged indicator list, merged LLM response)
        """
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(IndicatorExtractor.MAX_CONCURRENT_PAGES)
        completed_pages = 0
        total_pages = len(page_files)

        # Create parallel tasks
        async def process_single_page(page_file: str, page_num: int):
            nonlocal completed_pages
            async with semaphore:
                page_name = f"{file_name}_page_{page_num}"
                try:
                    logging.info(f"Processing page {page_num}: {page_name}")

                    # Extract indicators without saving to database
                    (
                        indicators,
                        llm_response,
                    ) = await IndicatorExtractor._extract_indicators_from_single_file(
                        page_file,
                        "application/pdf",
                        page_name,
                        user_id,
                        ocr_db_id,
                        source_table,
                        save_to_db=False,
                        file_key=file_key,
                    )

                    completed_pages += 1
                    if progress_callback:
                        progress_percent = 70 + int((completed_pages / total_pages) * 20)  # 70-90% progress range
                        language = get_req_ctx("language", "en")
                        
                        await progress_callback(
                            progress_percent,
                            t("pages_processed", language, "indicator_extractor", completed=completed_pages, total=total_pages, count=len(indicators)),
                        )

                    logging.info(f"Page {page_num} completed, extracted {len(indicators)} indicators")
                    return {
                        "page_num": page_num,
                        "indicators": indicators,
                        "llm_response": llm_response,
                        "success": True,
                    }
                except Exception as e:
                    completed_pages += 1
                    logging.error(f"Page {page_num} processing failed: {str(e)}", exc_info=True)
                    return {
                        "page_num": page_num,
                        "indicators": [],
                        "llm_response": "",
                        "success": False,
                        "error": str(e),
                    }

        # Create parallel task list
        tasks = [process_single_page(page_file, idx + 1) for idx, page_file in enumerate(page_files)]

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge results
        all_indicators = []
        all_llm_responses = []
        successful_pages = 0
        page_results = []  # Store page results for formatting
        failed_pages = []  # Collect failed page information

        exam_date = ""
        file_abstract = ""  # Collect file_abstract from first page that has it
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Page processing task exception: {str(result)}")
                failed_pages.append({
                    "page_num": "unknown",
                    "error": str(result),
                    "type": type(result).__name__
                })
                continue

            if result.get("success", False):
                successful_pages += 1
                page_indicators = result.get("indicators", [])
                all_indicators.extend(page_indicators)

                # Store page result for formatting
                page_results.append(
                    {
                        "page_num": result.get("page_num", 0),
                        "indicators": page_indicators,
                        "llm_response": result.get("llm_response"),
                        "success": True,
                    }
                )

                if result.get("llm_response"):
                    # Add LLM response directly without Page prefix
                    all_llm_responses.append(result["llm_response"])

                    # Optimized exam_date and file_abstract processing logic
                    # If llm_response is a dict, get exam_date and file_abstract directly
                    if isinstance(result["llm_response"], dict):
                        current_exam_date = result["llm_response"].get("content_info", {}).get("date_time", "")
                        current_file_abstract = result["llm_response"].get("file_abstract", "")
                    else:
                        # If it's a string, try to parse JSON
                        try:
                            response_data = (
                                json.loads(result["llm_response"])
                                if isinstance(result["llm_response"], str)
                                else result["llm_response"]
                            )
                            current_exam_date = response_data.get("content_info", {}).get("date_time", "")
                            current_file_abstract = response_data.get("file_abstract", "")
                        except (json.JSONDecodeError, AttributeError):
                            current_exam_date = ""
                            current_file_abstract = ""

                    if current_exam_date:
                        # Update if exam_date is empty or a better date format is found
                        if not exam_date or IndicatorExtractor._is_better_date_format(exam_date, current_exam_date):
                            exam_date = current_exam_date
                    
                    # Collect file_abstract from the first page that has it
                    if current_file_abstract and not file_abstract:
                        file_abstract = current_file_abstract
            else:
                # Handle failed pages
                failed_pages.append({
                    "page_num": result.get("page_num", "unknown"),
                    "error": result.get("error", "Unknown error"),
                    "type": "ProcessingError"
                })

        # Check if any pages failed and raise exception
        if failed_pages:
            failed_count = len(failed_pages)
            total_count = len(page_files)
            error_details = "\n".join([
                f"  - Page {fp['page_num']}: {fp['error']} ({fp['type']})"
                for fp in failed_pages
            ])
            error_message = (
                f"PDF processing failed: {failed_count}/{total_count} pages failed to process.\n"
                f"Failed pages:\n{error_details}"
            )
            logging.error(error_message)
            raise Exception(error_message)

        logging.info(f"Parallel processing result: {successful_pages}/{len(page_files)} pages processed, "
            f"extracted {len(all_indicators)} indicators total"
        )

        # Deduplicate indicators
        unique_indicators = IndicatorExtractor._deduplicate_indicators(all_indicators)

        if progress_callback:
            language = get_req_ctx("language", "en")
            
            await progress_callback(90, t("merging_results", language, "indicator_extractor", count=len(unique_indicators)))

        # Save to database
        if unique_indicators:
            saved_count = await FileParserDatabaseService.save_indicators_to_db(
                str(user_id),
                unique_indicators,
                exam_date,
                ocr_db_id,
                "",
                source_table=source_table,
                file_key=file_key,
            )

            if progress_callback:
                language = get_req_ctx("language", "en")
                
                await progress_callback(95, t("saving_to_database", language, "indicator_extractor", count=saved_count))

            logging.info(f"Batch save completed: {saved_count} indicators")

        # Ensure PDF processing also calls 90% progress update (not 100%)
        if progress_callback:
            language = get_req_ctx("language", "en")
            
            await progress_callback(90, t("pdf_indicators_completed", language, "indicator_extractor", count=len(unique_indicators)))

        # 🆕 Use ContentFormatter to create formatted multi-page PDF content
        try:
            formatted_content = ContentFormatter.format_pdf_multi_page_content(
                page_results=page_results,
                all_indicators=unique_indicators,
                merged_llm_response={
                    "content_info": {
                        "date_time": exam_date,
                        "total_pages": len(page_files),
                        "successful_pages": successful_pages,
                    },
                    "indicators": unique_indicators,
                },
                original_filename=file_name,
            )
            logging.info(f"✅ PDF multi-page content formatted: {len(page_results)} pages, {len(unique_indicators)} indicators")
        except Exception as e:
            logging.warning(f"⚠️ PDF formatting failed, using fallback: {str(e)}")
            formatted_content = f"PDF Analysis Report\n\nFile: {file_name}\nPages: {len(page_files)}\nIndicators: {len(unique_indicators)}"

        # Build merged response structure
        combined_response = {
            "file_abstract": file_abstract,  # Add file abstract
            "content_info": {
                "date_time": exam_date,
                "total_pages": len(page_files),
                "successful_pages": successful_pages,
            },
            "indicators": unique_indicators,
            "pages_data": all_llm_responses,
            "formatted_content": formatted_content,  # 🆕 Add formatted content
        }

        return unique_indicators, combined_response

    @staticmethod
    async def _extract_indicators_from_single_file(
        temp_file_path: str,
        content_type: str,
        file_name: str,
        user_id: int,
        ocr_db_id: int,
        source_table: str,
        save_to_db: bool = True,
        progress_callback: Optional[Callable[[int, str], None]] = None,
        file_key: str = None,
    ) -> tuple[List[Dict[str, Any]], str]:
        """
        Extract indicators from a single file (original logic)

        Args:
            temp_file_path: Temporary file path
            content_type: File type
            file_name: File name
            user_id: User ID
            ocr_db_id: OCR record ID
            source_table: Source table name
            save_to_db: Whether to save to database
            progress_callback: Progress callback function

        Returns:
            tuple[List[Dict[str, Any]], str]: (indicator list, LLM response)
        """

        # Time point before API call
        api_start_time = time.time()
        # Get the language setting from request context, default to English
        language = get_req_ctx("language", "en")
        
        if progress_callback:
            await progress_callback(70, t("analyzing_medical_indicators", language, "indicator_extractor", filename=file_name))

        logging.info(f"🔄 Starting LLM API call - user_id: {user_id}, file_name: {file_name}")
        
        # Generate prompt dynamically based on user's language setting
        dynamic_prompt = get_extract_indicators_prompt(language=language)
        
        # Configure response_schema - all providers will use:
        # - Gemini: Native support for response_schema
        # - OpenRouter/Doubao: schema will be embedded in prompt to guide output format
        gemini_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=RESPONSE_SCHEMA_EXTRACT_INDICATORS,
            temperature=0.1,
        )
        
        # Use unified file extraction
        # Auto select: GOOGLE_API_KEY -> Gemini | OPENROUTER_API_KEY -> OpenRouter | VOLCENGINE_API_KEY -> Doubao
        llm_ret = await unified_file_extract(
            file_path=str(temp_file_path),
            prompt=dynamic_prompt,
            content_type=content_type,
            config=gemini_config,  # schema works for all providers
        )
        
        api_end_time = time.time()
        api_duration = api_end_time - api_start_time
        logging.info(f"✅ LLM API call completed - user_id: {user_id}, duration: {api_duration:.2f}s")

        # Check if API response is empty
        if not llm_ret:
            error_msg = f"LLM indicator extraction returned empty response - user_id: {user_id}, file_name: {file_name}"
            logging.error(f"❌ {error_msg}")
            # Raise exception for empty response which indicates API failure
            raise ValueError(error_msg)

        if progress_callback:
            await progress_callback(75, t("parsing_indicator_data", language, "indicator_extractor"))

        # JSON parsing time point
        parse_start_time = time.time()
        result = json.loads(llm_ret)
        indicators = result.get("indicators", [])
        exam_date = result.get("content_info", {}).get("date_time", "")
        file_abstract = result.get("file_abstract", "")  # Extract file abstract
        
        # Ensure file_abstract is in result for return
        if file_abstract and "file_abstract" not in result:
            result["file_abstract"] = file_abstract

        parse_duration = time.time() - parse_start_time
        logging.info(f"JSON parsing completed - user_id: {user_id}, duration: {parse_duration:.2f}s, indicators: {len(indicators)}, abstract_len: {len(file_abstract)}")


        if not indicators:
            logging.info(f"No indicators extracted, user_id: {user_id}, file_name: {file_name}")
            return [], result

        # Save to database if required
        if save_to_db:
            if progress_callback:
                await progress_callback(80, t("saving_indicators_to_database", language, "indicator_extractor", count=len(indicators)))

            # Database save time point
            db_start_time = time.time()
            logging.info(f"Saving indicators to database - user_id: {user_id}, count: {len(indicators)}")

            # Save indicators to database
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
            logging.info(f"Database save completed - user_id: {user_id}, duration: {db_duration:.2f}s, saved: {saved_count}")

            if progress_callback:
                await progress_callback(85, t("database_save_completed", language, "indicator_extractor", count=saved_count))

        if progress_callback:
            if indicators:
                await progress_callback(90, t("single_file_extraction_completed", language, "indicator_extractor", count=len(indicators)))
            else:
                await progress_callback(90, t("single_file_processing_completed", language, "indicator_extractor"))

        return indicators, result

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

    @staticmethod
    async def extract_indicators_background_task(
        ocr_db_id: int,
        temp_file_path: str,
        content_type: str,
        file_name: str,
        user_id: int,
        source_table: str = "health_ocr",
        progress_callback: Optional[Callable[[int, str], None]] = None,
        file_key: str = None,
    ):
        """
        Background task: Extract indicators

        Args:
            ocr_db_id: OCR record ID
            temp_file_path: Temporary file path
            content_type: File type
            file_name: File name
            user_id: User ID
            progress_callback: Progress callback function
        """
        try:
            await IndicatorExtractor.extract_indicators_from_file(
                ocr_db_id,
                temp_file_path,
                content_type,
                file_name,
                user_id,
                source_table,
                progress_callback,
                file_key=file_key,
            )
        except Exception as e:
            logging.error(f"Background indicator extraction task failed, user_id: {user_id}, file_name: {file_name}, error: {e}", exc_info=True)

    @staticmethod
    def start_indicator_extraction_task(
        ocr_db_id: int,
        temp_file_path: str,
        content_type: str,
        file_name: str,
        user_id: int,
        source_table: str = "health_ocr",
        progress_callback: Optional[Callable[[int, str], None]] = None,
        file_key: str = None,
    ):
        """
        Start indicator extraction background task

        Args:
            ocr_db_id: OCR record ID
            temp_file_path: Temporary file path
            content_type: File type
            file_name: File name
            user_id: User ID
            progress_callback: Progress callback function
        """
        # Record task creation time
        task_create_time = datetime.now()
        logging.info(f"Creating indicator extraction background task - user_id: {user_id}, file_name: {file_name}")

        task = asyncio.create_task(
            IndicatorExtractor.extract_indicators_background_task(
                ocr_db_id,
                temp_file_path,
                content_type,
                file_name,
                user_id,
                source_table,
                progress_callback,
                file_key=file_key,
            )
        )

        # Add task callback to monitor task completion
        def task_done_callback(task):
            completion_time = datetime.now()
            duration = (completion_time - task_create_time).total_seconds()
            if task.exception():
                logging.error(f"Indicator extraction background task failed - user_id: {user_id}, file_name: {file_name}, duration: {duration:.2f}s, error: {task.exception()}", exc_info=task.exception())
            else:
                logging.info(f"Indicator extraction background task completed - user_id: {user_id}, file_name: {file_name}, duration: {duration:.2f}s")


        task.add_done_callback(task_done_callback)

    @staticmethod
    def validate_indicators(indicators: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate and clean indicator data

        Args:
            indicators: Original indicator list

        Returns:
            List[Dict[str, Any]]: Validated indicator list
        """
        valid_indicators = []

        for indicator in indicators:
            # Check required fields
            if not indicator.get("name") or not indicator.get("value"):
                continue

            # Basic data cleaning
            cleaned_indicator = {
                "name": str(indicator.get("name", "")).strip(),
                "value": str(indicator.get("value", "")).strip(),
                "unit": str(indicator.get("unit", "")).strip(),
                "reference_range": str(indicator.get("reference_range", "")).strip(),
                "status": str(indicator.get("status", "")).strip(),
            }

            # Filter empty values
            cleaned_indicator = {k: v for k, v in cleaned_indicator.items() if v}

            if cleaned_indicator.get("name") and cleaned_indicator.get("value"):
                valid_indicators.append(cleaned_indicator)

        return valid_indicators

    @staticmethod
    async def extract_indicators_from_text(text_content: str, user_id: int, ocr_db_id: int) -> List[Dict[str, Any]]:
        """
        Extract indicators from text content (without file dependency)

        Args:
            text_content: Text content
            user_id: User ID
            ocr_db_id: OCR record ID

        Returns:
            List[Dict[str, Any]]: Extracted indicator list
        """
        # Text-based indicator extraction logic can be implemented here
        # Returns empty list for now, can be implemented as needed
        return []

    @staticmethod
    def _get_date_completeness_score(date_str: str) -> int:
        """
        Evaluate date format completeness score

        Args:
            date_str: Date string

        Returns:
            int: Completeness score, higher number means more complete format
        """
        if not date_str or not isinstance(date_str, str):
            return 0

        date_str = date_str.strip()
        if not date_str:
            return 0

        score = 0

        # Base score: has date content
        score += 1

        # Length score: longer dates are usually more complete
        score += min(len(date_str) // 2, 10)

        # Format score: check for date elements
        if any(char.isdigit() for char in date_str):
            score += 2

        # Year format (4 digits)
        if any(len(part) == 4 and part.isdigit() for part in date_str.split()):
            score += 5

        # Contains month
        if any(
            month in date_str.lower()
            for month in [
                "jan",
                "feb",
                "mar",
                "apr",
                "may",
                "jun",
                "jul",
                "aug",
                "sep",
                "oct",
                "nov",
                "dec",
                "month",
                "year",
                "day",
            ]
        ):
            score += 3

        # Contains separators (more standardized format)
        if any(sep in date_str for sep in ["-", "/", ":", " "]):
            score += 2

        # Contains time information
        if ":" in date_str:
            score += 4

        # Standard ISO format or common formats
        if any(pattern in date_str for pattern in ["T", "Z", "GMT", "UTC"]):
            score += 3

        return score

    @staticmethod
    def _is_better_date_format(current_date: str, new_date: str) -> bool:
        """
        Determine if new date format is better than current date format

        Args:
            current_date: Current date string
            new_date: New date string

        Returns:
            bool: True if new date format is better
        """
        current_score = IndicatorExtractor._get_date_completeness_score(current_date)
        new_score = IndicatorExtractor._get_date_completeness_score(new_date)

        return new_score > current_score
