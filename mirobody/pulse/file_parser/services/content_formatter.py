"""
Content formatter service for file processing results

This service formats raw content from PDF/image/Excel parsing results
for structured storage in th_messages.content.raw field.
Supports multi-page PDF aggregation and health indicator formatting.
"""

import json
import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class ContentFormatter:
    """Content formatter service for file processing results"""

    @staticmethod
    def format_parsed_content(
        file_results: list[dict[str, Any]],
        file_names: list[str],
        llm_responses: list[Any] = None,
        indicators_list: list[list[dict]] = None,
    ) -> str:
        """
        Format parsed content from multiple files (including multi-page PDFs)
        into a structured display format for storage in th_messages.content.raw field.

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
            for i, (file_result, file_name) in enumerate(zip(file_results, file_names, strict=False)):
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
            logger.error(f"Content formatting failed: {str(e)}", stack_info=True)
            # Return fallback content
            return ContentFormatter._create_fallback_content(file_results, file_names)

    @staticmethod
    def _format_health_report_content(llm_response: Any, indicators: list[dict]) -> list[str]:
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
            logger.warning(f"Health report formatting failed: {str(e)}")
            content_lines.append("### Processing Result")
            content_lines.append("")
            content_lines.append(f"Found {len(indicators)} indicators from health document analysis.")

        return content_lines

    @staticmethod
    def _format_excel_content(file_result: dict) -> list[str]:
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
    def _format_multi_file_summary(file_results: list[dict], indicators_list: list[list[dict]] = None) -> list[str]:
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
    def _create_fallback_content(file_results: list[dict], file_names: list[str]) -> str:
        """Create fallback content when formatting fails"""
        try:
            content_lines = []
            content_lines.append("# File Processing Results")
            content_lines.append("")
            content_lines.append(f"Processed {len(file_results)} files:")

            for i, (result, name) in enumerate(zip(file_results, file_names, strict=False)):
                content_lines.append(f"{i + 1}. {name} ({result.get('type', 'unknown')})")

            return "\n".join(content_lines)
        except Exception:
            return "File processing completed."

