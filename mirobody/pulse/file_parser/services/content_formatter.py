"""
Content formatter service for file processing results

This service formats raw content from PDF/image/Excel parsing results
for structured storage in theta_ai.th_messages.content.raw field.
Supports multi-page PDF aggregation and health indicator formatting.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List


class ContentFormatter:
    """Content formatter service for file processing results"""

    @staticmethod
    def format_parsed_content(
        file_results: List[Dict[str, Any]],
        file_names: List[str],
        llm_responses: List[Any] = None,
        indicators_list: List[List[Dict]] = None,
    ) -> str:
        """
        Format parsed content from multiple files (including multi-page PDFs)
        into a structured display format for storage in theta_ai.th_messages.content.raw field.

        Args:
            file_results: List of file processing results
            file_names: List of original file names
            llm_responses: List of LLM responses from processing
            indicators_list: List of indicator lists for each file

        Returns:
            str: Formatted content for display and storage
        """
        try:
            formatted_content = []

            # Add header
            formatted_content.append("# 📄 File Processing Report")
            formatted_content.append("")
            formatted_content.append(f"**Processing Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            formatted_content.append(f"**Total Files:** {len(file_results)}")
            formatted_content.append("")

            # Process each file
            for i, (file_result, file_name) in enumerate(zip(file_results, file_names)):
                file_type = file_result.get("type", "unknown")

                # Add file header
                formatted_content.append(f"## 📋 File {i + 1}: {file_name}")
                formatted_content.append("")
                formatted_content.append(f"**File Type:** {file_type.upper()}")

                # Get LLM response and indicators for this file
                llm_response = llm_responses[i] if llm_responses and i < len(llm_responses) else None
                indicators = indicators_list[i] if indicators_list and i < len(indicators_list) else []

                # Format based on content type
                if file_type in ["image", "pdf"] and (llm_response or indicators):
                    formatted_content.extend(ContentFormatter._format_health_report_content(llm_response, indicators))
                elif file_type == "excel":
                    formatted_content.extend(ContentFormatter._format_excel_content(file_result))
                else:
                    # Generic content formatting
                    raw_content = file_result.get("raw", "")
                    if raw_content:
                        formatted_content.append("**Raw Content:**")
                        formatted_content.append("```")
                        # 🔧 Keep all raw content complete, no truncation
                        formatted_content.append(str(raw_content))
                        formatted_content.append("```")

                formatted_content.append("")
                formatted_content.append("---")
                formatted_content.append("")

            # Add summary if multiple files
            if len(file_results) > 1:
                formatted_content.extend(ContentFormatter._format_multi_file_summary(file_results, indicators_list))

            return "\n".join(formatted_content)

        except Exception as e:
            logging.error(f"Content formatting failed: {str(e)}", stack_info=True)
            # Return fallback content
            return ContentFormatter._create_fallback_content(file_results, file_names)

    @staticmethod
    def format_pdf_multi_page_content(
        page_results: List[Dict],
        all_indicators: List[Dict],
        merged_llm_response: Any,
        original_filename: str,
    ) -> str:
        """
        Format multi-page PDF content with aggregated results

        Args:
            page_results: Results from each page
            all_indicators: Aggregated indicators from all pages
            merged_llm_response: Merged LLM response
            original_filename: Original PDF filename

        Returns:
            str: Formatted multi-page PDF content
        """
        try:
            formatted_content = []

            # PDF header
            formatted_content.append("# 📄 Multi-Page PDF Analysis Report")
            formatted_content.append("")
            formatted_content.append(f"**File:** {original_filename}")
            formatted_content.append(f"**Total Pages:** {len(page_results)}")
            formatted_content.append(f"**Analysis Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            formatted_content.append("")

            # Process aggregated health report
            if all_indicators or merged_llm_response:
                formatted_content.extend(
                    ContentFormatter._format_health_report_content(merged_llm_response, all_indicators)
                )

            # Add page-by-page summary
            if len(page_results) > 1:
                formatted_content.append("## 📑 Page-by-Page Summary")
                formatted_content.append("")

                for i, page_result in enumerate(page_results, 1):
                    page_indicators = page_result.get("indicators", [])
                    if page_indicators:
                        formatted_content.append(f"**Page {i}:** Found {len(page_indicators)} indicators")
                    else:
                        formatted_content.append(f"**Page {i}:** No indicators detected")

                formatted_content.append("")

            return "\n".join(formatted_content)

        except Exception as e:
            logging.error(f"Multi-page PDF formatting failed: {str(e)}", stack_info=True)
            return f"# PDF Analysis Report\n\nFile: {original_filename}\nPages: {len(page_results)}\n\nProcessing completed with {len(all_indicators)} total indicators found."

    @staticmethod
    def _format_health_report_content(llm_response: Any, indicators: List[Dict]) -> List[str]:
        """
        Format health report content (adapted from _format_llm_response_for_display)
        """
        content_lines = []

        try:
            # Parse content information from LLM response
            content_info = {}
            exam_date = ""

            if isinstance(llm_response, dict):
                content_info = llm_response.get("content_info", {})
                exam_date = content_info.get("date_time", "")
            elif isinstance(llm_response, str):
                try:
                    response_data = json.loads(llm_response)
                    if isinstance(response_data, dict) and "content_info" in response_data:
                        content_info = response_data["content_info"]
                        exam_date = content_info.get("date_time", "")
                except (json.JSONDecodeError, KeyError):
                    pass

            # Report basic information
            if content_info:
                content_lines.append("### 📊 Report Information")
                content_lines.append("")

                if exam_date:
                    content_lines.append(f"**Date/Time:** {exam_date}")

                # Content type details
                if content_info.get("content_type_detail"):
                    content_lines.append(f"**Content Type:** {content_info['content_type_detail']}")
                
                if content_info.get("content_category"):
                    content_lines.append(f"**Category:** {content_info['content_category']}")
                
                # Subject information
                subject_info = content_info.get("subject_info", {})
                if subject_info:
                    if subject_info.get("name"):
                        content_lines.append(f"**Subject Name:** {subject_info['name']}")
                    if subject_info.get("details"):
                        content_lines.append(f"**Subject Details:** {subject_info['details']}")
                
                # Source and reference
                if content_info.get("source"):
                    content_lines.append(f"**Source:** {content_info['source']}")
                
                if content_info.get("reference_number"):
                    content_lines.append(f"**Reference Number:** {content_info['reference_number']}")

                content_lines.append("")

            # Health indicators table
            if indicators and len(indicators) > 0:
                content_lines.append("### 🔬 Test Results")
                content_lines.append("")

                # Table header
                content_lines.append("| Indicator | Value | Reference Range | Unit | Status |")
                content_lines.append("|-----------|-------|-----------------|------|--------|")

                # Indicator data rows
                for indicator in indicators:
                    name = indicator.get("original_indicator", "").strip()
                    value = indicator.get("value", "").strip()
                    reference = indicator.get("reference_range", "").strip()
                    unit = indicator.get("unit", "").strip()

                    # Determine abnormal status
                    status = indicator.get("status", "").strip()

                    # Handle empty values
                    name = name if name else "-"
                    value = value if value else "-"
                    reference = reference if reference else "-"
                    unit = unit if unit else "-"

                    content_lines.append(f"| {name} | {value} | {reference} | {unit} | {status} |")

                content_lines.append("")

                # Statistics
                total_count = len(indicators)
                abnormal_count = sum(
                    1 for indicator in indicators if indicator.get("status", "").lower() in ["high", "low"]
                )
                normal_count = total_count - abnormal_count

                content_lines.append("### 📈 Statistics Overview")
                content_lines.append("")
                content_lines.append(f"- **Total Indicators:** {total_count}")
                content_lines.append(f"- **Normal Indicators:** {normal_count}")
                content_lines.append(f"- **Abnormal Indicators:** {abnormal_count}")

                if abnormal_count > 0:
                    content_lines.append("")
                    content_lines.append(
                        "⚠️ **Note:** Abnormal indicators detected. Please consult a healthcare professional for further evaluation."
                    )

            else:
                content_lines.append("### ❌ No Valid Indicators Detected")
                content_lines.append("")
                content_lines.append("No valid health indicator data was identified in this analysis.")

        except Exception as e:
            logging.warning(f"Health report formatting failed: {str(e)}")
            content_lines.append("### Processing Result")
            content_lines.append("")
            content_lines.append(f"Found {len(indicators)} indicators from health document analysis.")

        return content_lines

    @staticmethod
    def _format_excel_content(file_result: Dict) -> List[str]:
        """Format Excel file content"""
        content_lines = []

        content_lines.append("### 📊 Excel File Analysis")
        content_lines.append("")

        raw_content = file_result.get("raw", "")
        if raw_content:
            content_lines.append("**Data Summary:**")
            content_lines.append("```")
            # 🔧 Keep all content complete, no truncation
            content_lines.append(str(raw_content))
            content_lines.append("```")

        return content_lines

    @staticmethod
    def format_excel_detailed_content(
        parsed_data: Dict[str, List[Dict]],
        filename: str,
        saved_count: int = 0,
        file_metadata: Dict = None,
    ) -> str:
        """
        Format Excel data into detailed structured content
        Formats based on data content only, regardless of database save results

        Args:
            parsed_data: Parsed Excel data organized by data type
            filename: Original Excel filename
            saved_count: Number of records saved (ignored for formatting)
            file_metadata: Additional file metadata

        Returns:
            str: Detailed formatted Excel content
        """
        try:
            formatted_content = []

            # Excel header - focus on data content, not save results
            formatted_content.append("# 📊 Excel Health Report Analysis")
            formatted_content.append("")
            formatted_content.append(f"**File:** {filename}")
            formatted_content.append(f"**Analysis Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # 🔧 Calculate total records from parsed_data instead of using saved_count
            total_records = sum(len(records) for records in parsed_data.values()) if parsed_data else 0
            formatted_content.append(f"**Data Records Found:** {total_records}")
            formatted_content.append("")

            if not parsed_data or total_records == 0:
                formatted_content.append("## ❌ No Valid Data Found")
                formatted_content.append("")
                formatted_content.append("No valid health data could be extracted from this Excel file.")
                return "\n".join(formatted_content)

            # Data summary section
            formatted_content.append("## 📋 Data Summary")
            formatted_content.append("")

            total_indicators = 0
            data_types = []

            for data_type, records in parsed_data.items():
                if records:
                    data_types.append(data_type)
                    total_indicators += len(records)
                    formatted_content.append(f"- **{data_type}**: {len(records)} records")

            formatted_content.append("")
            formatted_content.append(f"**Total Data Types:** {len(data_types)}")
            formatted_content.append(f"**Total Records:** {total_indicators}")
            formatted_content.append("")

            # Detailed data sections
            for data_type, records in parsed_data.items():
                if not records:
                    continue

                formatted_content.extend(ContentFormatter._format_excel_data_type_section(data_type, records))

            # Processing status - focus on data analysis, not database operations
            formatted_content.append("## ✅ Processing Status")
            formatted_content.append("")
            formatted_content.append("- **File Processing:** Completed successfully")
            formatted_content.append(f"- **Data Extraction:** {len(data_types)} data types identified")
            formatted_content.append(f"- **Health Records:** {total_records} records extracted and analyzed")
            formatted_content.append("")
            formatted_content.append("All health data has been successfully analyzed and formatted for review.")

            return "\n".join(formatted_content)

        except Exception as e:
            logging.error(f"Excel detailed formatting failed: {str(e)}", stack_info=True)
            return ContentFormatter.create_excel_fallback_content(filename, parsed_data, saved_count)

    @staticmethod
    def _format_excel_data_type_section(data_type: str, records: List[Dict]) -> List[str]:
        """Format a specific data type section for Excel content"""
        content_lines = []
        content_lines.append(f"## 🔬 {data_type.title()} Data")
        content_lines.append("")

        if not records:
            content_lines.append("No records found for this data type.")
            content_lines.append("")
            return content_lines

        # Get unique indicators
        indicators = []
        facilities = set()
        time_range = []

        for record in records:
            # 🔧 Fix: Handle indicator field robustly
            indicator_raw = record.get("indicator", "")
            if isinstance(indicator_raw, dict):
                indicator = (
                    indicator_raw.get("name", str(indicator_raw)) if "name" in indicator_raw else str(indicator_raw)
                )
            else:
                indicator = str(indicator_raw) if indicator_raw else ""

            # For display, try to get a shorter name
            display_name = record.get(
                "indicator_name",
                record.get("third_category", record.get("second_category", indicator)),
            )
            if display_name and display_name not in indicators and len(str(display_name)) < len(indicator):
                indicators.append(str(display_name))
            elif indicator and indicator not in indicators:
                indicators.append(indicator)

            # 🔧 Fix: Handle facility field robustly
            facility_raw = record.get("facility", "")
            if isinstance(facility_raw, dict):
                facility = facility_raw.get("name", str(facility_raw)) if "name" in facility_raw else str(facility_raw)
            else:
                facility = str(facility_raw) if facility_raw else ""

            if facility:
                facilities.add(facility)

            # 🔧 Fix: Handle report_time field robustly
            report_time_raw = record.get("report_time", "")
            if isinstance(report_time_raw, dict):
                report_time = (
                    report_time_raw.get("time", str(report_time_raw))
                    if "time" in report_time_raw
                    else str(report_time_raw)
                )
            else:
                report_time = str(report_time_raw) if report_time_raw else ""

            if report_time:
                time_range.append(report_time)

        # Basic statistics
        content_lines.append(f"**Record Count:** {len(records)}")
        content_lines.append(f"**Unique Indicators:** {len(indicators)}")

        if facilities:
            # 🔧 Show all facilities, no truncation
            facilities_list = list(facilities)
            content_lines.append(f"**Medical Facilities:** {', '.join(facilities_list)}")

        if time_range:
            sorted_times = sorted([t for t in time_range if t])
            if sorted_times:
                content_lines.append(f"**Time Range:** {sorted_times[0]} to {sorted_times[-1]}")

        content_lines.append("")

        # Complete data table - show all records, no truncation
        if len(records) > 0:
            content_lines.append("### Complete Data")
            content_lines.append("")

            # Table header
            content_lines.append("| Indicator | Value | Reference Range | Unit | Status | Date |")
            content_lines.append("|-----------|-------|-----------------|------|--------|------|")

            # 🔧 Show ALL records - no truncation to maintain complete data integrity
            for i, record in enumerate(records):
                # 🔧 Fix: Handle different value field structures properly
                indicator = record.get("indicator", "-")

                # Handle value field - could be string, dict, or other types
                raw_value = record.get("value", "-")
                if isinstance(raw_value, dict):
                    # If value is a dict, try to extract the actual value
                    value = raw_value.get("value", str(raw_value)) if "value" in raw_value else str(raw_value)
                elif isinstance(raw_value, (list, tuple)):
                    value = str(raw_value)
                else:
                    value = str(raw_value) if raw_value is not None else "-"

                # Handle other fields with similar robust extraction
                reference_range = record.get("reference_range", "-")
                if isinstance(reference_range, dict):
                    reference_range = (
                        reference_range.get("range", str(reference_range))
                        if "range" in reference_range
                        else str(reference_range)
                    )
                else:
                    reference_range = str(reference_range) if reference_range is not None else "-"

                unit = record.get("unit", "-")
                if isinstance(unit, dict):
                    unit = unit.get("unit", str(unit)) if "unit" in unit else str(unit)
                else:
                    unit = str(unit) if unit is not None else "-"

                report_time = record.get("report_time", "-")
                if isinstance(report_time, dict):
                    report_time = (
                        report_time.get("time", str(report_time)) if "time" in report_time else str(report_time)
                    )
                else:
                    report_time = str(report_time) if report_time is not None else "-"

                # Determine status
                is_abnormal = record.get("is_abnormal", "")
                if isinstance(is_abnormal, dict):
                    is_abnormal = (
                        is_abnormal.get("abnormal", str(is_abnormal)) if "abnormal" in is_abnormal else str(is_abnormal)
                    )

                if is_abnormal and str(is_abnormal).lower() in [
                    "true",
                    "1",
                    "是",
                    "异常",
                ]:
                    status = "⚠️ Abnormal"
                elif is_abnormal and str(is_abnormal).lower() in [
                    "false",
                    "0",
                    "否",
                    "正常",
                ]:
                    status = "✅ Normal"
                else:
                    status = "-"

                # Format date - only truncate if it's clearly a timestamp (longer than reasonable)
                if report_time and len(str(report_time)) > 19:  # Only truncate if longer than "YYYY-MM-DD HH:MM:SS"
                    # Check if it looks like an ISO timestamp with microseconds
                    if "T" in str(report_time) and ("." in str(report_time) or "+" in str(report_time)):
                        report_time = str(report_time)[:19]  # Keep YYYY-MM-DDTHH:MM:SS part

                # 🔧 Keep all data complete - no truncation for value, reference_range, unit
                # Just ensure they are properly formatted strings

                # For display, try to get a shorter indicator name if available, but keep full data
                display_indicator = indicator
                short_name = record.get(
                    "indicator_name",
                    record.get("third_category", record.get("second_category", "")),
                )
                if short_name and len(short_name) < len(indicator) and len(short_name) > 0:
                    display_indicator = str(short_name)

                content_lines.append(
                    f"| {display_indicator} | {value} | {reference_range} | {unit} | {status} | {report_time} |"
                )

            # 🔧 No "more records" truncation message - all records are shown completely

        content_lines.append("")
        content_lines.append("---")
        content_lines.append("")

        return content_lines

    @staticmethod
    def _format_multi_file_summary(file_results: List[Dict], indicators_list: List[List[Dict]] = None) -> List[str]:
        """Format summary for multiple files"""
        content_lines = []

        content_lines.append("## 📋 Processing Summary")
        content_lines.append("")

        # File type statistics
        type_counts = {}
        total_indicators = 0

        for i, result in enumerate(file_results):
            file_type = result.get("type", "unknown")
            type_counts[file_type] = type_counts.get(file_type, 0) + 1

            if indicators_list and i < len(indicators_list):
                total_indicators += len(indicators_list[i])

        content_lines.append("**File Types:**")
        for file_type, count in type_counts.items():
            content_lines.append(f"- {file_type.upper()}: {count} files")

        if total_indicators > 0:
            content_lines.append("")
            content_lines.append(f"**Total Health Indicators Extracted:** {total_indicators}")

        content_lines.append("")
        content_lines.append("**Processing Status:** ✅ Completed")

        return content_lines

    @staticmethod
    def _create_fallback_content(file_results: List[Dict], file_names: List[str]) -> str:
        """Create fallback content when formatting fails"""
        try:
            content_lines = []
            content_lines.append("# File Processing Results")
            content_lines.append("")
            content_lines.append(f"Processed {len(file_results)} files:")

            for i, (result, name) in enumerate(zip(file_results, file_names)):
                content_lines.append(f"{i + 1}. {name} ({result.get('type', 'unknown')})")

            return "\n".join(content_lines)
        except Exception:
            return "File processing completed."

    @staticmethod
    def create_excel_fallback_content(filename: str, parsed_data: Dict = None, saved_count: int = 0) -> str:
        """Create fallback content for Excel when detailed formatting fails - focus on data analysis, not save results"""
        try:
            content_lines = []
            content_lines.append("# 📊 Excel Health Report")
            content_lines.append("")
            content_lines.append(f"**File:** {filename}")
            content_lines.append(f"**Processing Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            if parsed_data:
                content_lines.append(f"**Data Types Found:** {len(parsed_data)}")
                total_records = sum(len(records) for records in parsed_data.values())
                content_lines.append(f"**Health Records Extracted:** {total_records}")
            else:
                content_lines.append("**Status:** File processed but no structured health data found")

            content_lines.append("")
            content_lines.append("Excel file has been successfully analyzed and health data extracted.")

            return "\n".join(content_lines)
        except Exception:
            return f"Excel Health Report: {filename}\n\nFile processed successfully."
