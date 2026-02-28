"""
File Original Text Extraction Prompts

Unified prompt for extracting original text from PDF pages and images.
"""

FILE_ORIGINAL_TEXT_PROMPT = """Extract and return ALL text content from this document/image.

Requirements:
1. Extract all text completely, do not omit anything
2. For tables, use the following format:
   - Each row on a separate line
   - Columns separated by |
   - Keep table headers
3. Preserve the original paragraph structure
4. Keep all values, units, dates, measurements exactly as shown
5. Anonymize personal identifiable information (PII):
   - ID number (身份证号): replace with "***"
   - Phone number: replace with "***"
   - Address: keep only city/district level, replace detailed address with "***"
   - Patient ID / Medical record number: replace with "***"
   - Keep name, age, gender, and medical-related dates (examination date, report date) as is

Return ONLY the extracted text content. Do not add any explanations, summaries, or commentary.
If there is no text, return an empty response."""
