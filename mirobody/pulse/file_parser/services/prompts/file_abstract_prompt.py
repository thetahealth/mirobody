"""
File Abstract Generation Prompts
"""

FILE_ABSTRACT_PROMPT = """Please generate a detailed abstract and filename for this file. Must return in JSON format.

## Return Format Requirements:
```json
{{
  "file_name": "2024-03-15_Blood_Test_Report_Hospital.pdf",
  "file_abstract": "PDF medical report: Complete blood count results show normal white blood cell count, all red blood cell indicators within reference range, no abnormalities detected."
}}
```

## Filename Generation Requirements (file_name):
1. **Information Completeness**: Include the following key information (if present in file):
   - Date: Prefer YYYY-MM-DD format (e.g., 2024-03-15)
     - **Date Priority**: Sample Collection Date > Sample Receipt Date > Report Date > File Upload Date
     - Always use the highest priority date found in the document
   - Core content description: Main subject name (e.g., Blood Test Report, Chest CT, ECG)
   - Important distinguishing info: Institution name, examination area, test type, etc. (optional)
   
2. **Naming Format Recommendations**:
   - With date: `Date_Content_Description_Additional_Info.extension`
   - Without date: `Content_Description_Additional_Info.extension`
   
3. **Language Consistency**: Automatically determine language based on file content (use same language as content)

4. **Length Control**: Total length recommended between 15-40 characters (excluding extension)

5. **Extension**: Must include correct file extension (e.g., .pdf, .jpg, .png)

6. **Good Examples**:
   - "2024-03-15_Blood_Test_Report_Hospital.pdf"
   - "2024-01-20_Chest_CT_Lung_Exam.jpg"
   - "2024-02-10_Blood_Test_Mayo_Clinic.pdf"
   - "2023-12-05_Chest_X-Ray.png"
   - "HbA1c_Test_3_Months.pdf" (when no date)
   - "MRI_Brain_Scan.jpg" (when no date)
   
7. **Naming Principles**:
   - Date first: If file has clear date, must include it
   - Content is key: Core description must be clear and accurate
   - High distinctiveness: Add key information that distinguishes different files
   - Avoid redundancy: Don't add meaningless words (like "file", "document", etc.)

## File Abstract Requirements (file_abstract):

### For PDF Documents:
- Identify document type (medical report, examination report, etc.)
- Extract core content and key information
- Highlight main findings or conclusions
- Maintain accuracy of professional terminology

### For Image Files:
- Identify image content type (medical imaging, charts, screenshots, etc.)
- Describe main content or key elements
- For medical images, note imaging type and main findings

### For Excel Files:
- Identify data type and main content
- Extract key data categories and statistics
- Highlight important trends or findings

### For Other Files:
- Identify file nature based on content
- Extract core information and key points
- Maintain abstract usefulness and readability

## Abstract Output Requirements:
1. **Conciseness**: Strictly limit to 150 characters
2. **Accuracy**: Ensure information is correct, don't add non-existent content
3. **Professionalism**: Use appropriate professional terminology, maintain formal tone
4. **Usefulness**: Highlight information most valuable to user
5. **Completeness**: Include as much key information as possible within character limit

## Language Requirements:
- Language of file_name and file_abstract should be determined by file content
- Keep language concise and clear
- Avoid redundant words and modifiers
- Maintain accuracy of professional terms

Please generate a JSON format response based on the file content:"""

FALLBACK_ABSTRACT_TEMPLATES = {
    "pdf": "PDF document: {filename} - Document uploaded successfully, contains {page_count} pages",
    "image": "Image file: {filename} - Image uploaded successfully, resolution {resolution}",
    "excel": "Excel file: {filename} - Spreadsheet uploaded successfully, contains {sheet_count} sheets",
    "genetic": "Genetic data file: {filename} - Genetic test data uploaded successfully ({file_size}), processing in background",
    "text": "Text file: {filename} - Text content uploaded successfully, contains {word_count} characters",
    "default": "{file_type} file: {filename} - File uploaded successfully and ready for viewing"
}
