def get_extract_indicators_prompt(language: str = "zh-cn") -> str:
    """
    Generate the prompt for extracting health-related indicators from various content types.
    
    Args:
        language (str): User's preferred language code (e.g., 'zh-cn', 'en', 'ja', 'ko', 'es', etc.)
        
    Returns:
        str: The complete prompt for health indicator extraction
    """
    return f"""Analyze uploaded content and extract health-related indicators. **CRITICAL: First determine if content is health-related before any extraction.**

## STEP 1: Content Relevance Check (MANDATORY FIRST STEP)

**⚠️ STOP AND RETURN EMPTY RESULT if content matches ANY of these non-health categories:**

### Non-Health-Related Content (Return Empty Immediately):
- **Work/Business**: Documents, reports, presentations, contracts, invoices, financial statements, meeting notes
- **Technology**: Code, technical documentation, software screenshots, system logs, API docs
- **Education**: Study materials, textbooks, homework, lecture notes (except medical education)
- **Entertainment**: Games, movies, music, social media posts, memes, art
- **Travel/Scenery**: Landscape photos, travel photos, architecture (without health context)
- **Personal**: ID cards, certificates, tickets, receipts (non-medical)
- **Communication**: Chat messages, emails (non-medical), letters
- **Food/Nutrition**: Food images, nutrition labels, recipes, dietary records (NOT extracted)
- **Other**: Any content without explicit medical examination reports or medical device data

**For non-health content, immediately return:**
```json
{{
  "language": "{language}",
  "content_type": "non_health_related",
  "content_info": {{
    "content_type_detail": "[Brief description of actual content]",
    "content_category": "Non-health-related content"
  }},
  "indicators": []
}}
```

### Medical Content (Proceed with Extraction):
Only continue extraction if content is a **medical examination report**:
- Laboratory test reports: Complete blood count, biochemical tests, urinalysis, hormone tests, tumor markers, etc.
- Imaging reports: CT, MRI, X-ray, ultrasound, PET, endoscopy, etc.
- Pathology reports: Biopsy results, cytology, histopathology
- Physiological test reports: ECG, EEG, pulmonary function tests, etc.
- Medical device data: Blood glucose monitors, blood pressure monitors, wearable health devices

---

## STEP 2: If Health-Related, Proceed with Extraction

### User Language Settings:
Language code: {language}
- All adaptable fields must use language corresponding to `{language}`
- Keep `value` field in original text/objective description
- Fixed English values: `status` ["normal", "high", "low"], `detection_method` ["laboratory", "Imaging", "Physiological", "Pathological", "wearable"]

### Medical Report Types:

#### A. Numerical Reports (Laboratory Tests):
Complete blood count, biochemical panel, urinalysis, hormone tests, tumor markers, lipid panel, liver/kidney function, etc.

#### B. Descriptive Reports (Imaging/Pathology):
CT, MRI, X-ray, ultrasound, PET, endoscopy, biopsy, cytology, histopathology, etc.

#### C. Mixed Reports:
Comprehensive reports containing both numerical values and descriptive conclusions

#### D. Physiological Test Reports:
ECG, EEG, pulmonary function, audiometry, visual acuity, etc.

---

## STEP 3: Indicator Extraction Rules

### ⚠️ CRITICAL: Extract ALL Indicators - NO OMISSIONS

**For medical reports, you MUST extract EVERY single indicator present in the report. Do not skip or summarize.**

| Field | Description |
|-------|-------------|
| original_indicator | Indicator name in user's language ({language}), use standard medical terminology |
| value | Numerical with unit (e.g., "120 g/L") or descriptive text (keep original text exactly) |
| reference_range | Normal range in user's language (if provided in report) |
| unit | Extracted from value (e.g., "g/L", "mmol/L", "×10⁹/L") or empty string |
| detection_method | "laboratory" / "Imaging" / "Physiological" / "Pathological" / "wearable" |
| status | "normal" / "high" / "low" based on reference range comparison |
| notes | Clinical significance or abnormality explanation in user's language |

### Completeness Requirements:
1. **Extract EVERY indicator** listed in the report, including normal results
2. **Do NOT skip** any test items, even if results are within normal range
3. **For descriptive reports** (imaging/pathology): Extract each organ/region finding as separate indicator
4. **Preserve precision**: Keep exact numerical values and units as shown in report
5. **Include sub-items**: If a test has multiple components (e.g., lipid panel), extract each component separately

---

## General Rules:
1. **Completeness**: Extract ALL indicators from the report - do NOT omit any test results
2. **Status**: Always one of: "normal", "high", "low" (compare with reference range)
3. **Detection Method**: Always one of: "laboratory", "Imaging", "Physiological", "Pathological", "wearable"
4. **Units**: Extract exact unit from value field, empty string if none
5. **Language**: Adapt `original_indicator`, `notes`, `reference_range` to user's language ({language}); keep `value` in original text
6. **Precision**: Preserve exact numerical values as shown in the report

---

## Examples:

### Laboratory Report (zh-cn) - Extract ALL items:
```
Input: Complete Blood Count report with multiple items
Output: Extract EVERY indicator, for example:
- {{"original_indicator": "白细胞计数", "value": "15.5 ×10⁹/L", "reference_range": "4.0-10.0 ×10⁹/L", "unit": "×10⁹/L", "detection_method": "laboratory", "status": "high", "notes": "偏高"}}
- {{"original_indicator": "红细胞计数", "value": "4.5 ×10¹²/L", "reference_range": "4.0-5.5 ×10¹²/L", "unit": "×10¹²/L", "detection_method": "laboratory", "status": "normal", "notes": ""}}
- {{"original_indicator": "血红蛋白", "value": "140 g/L", "reference_range": "120-160 g/L", "unit": "g/L", "detection_method": "laboratory", "status": "normal", "notes": ""}}
... (extract ALL items from report)
```

### Imaging Report (zh-cn):
```
Input: CT chest report
Output: Extract each finding:
- {{"original_indicator": "肺部", "value": "双肺纹理清晰，未见明显实质性病变", "reference_range": "", "unit": "", "detection_method": "Imaging", "status": "normal", "notes": ""}}
- {{"original_indicator": "心脏", "value": "心影大小形态正常", "reference_range": "", "unit": "", "detection_method": "Imaging", "status": "normal", "notes": ""}}
```

### Non-Medical Content (Return Empty):
```
Input: Food image / Technical doc / Landscape photo / Business document
Output: {{"language": "{language}", "content_type": "non_health_related", "content_info": {{"content_type_detail": "Food image", "content_category": "Non-health-related content"}}, "indicators": []}}
```

---

## Required Response Fields:
- `language`: User's language code (`{language}`)
- `content_type`: "medical_report" or "non_health_related"
- `content_info`: Content metadata (report type, date, patient info, hospital, etc.)
- `indicators`: Array of ALL extracted indicators (empty array for non-medical content)
"""


# Legacy constant for backward compatibility
PROMPT_EXTRACT_INDICATORS = get_extract_indicators_prompt()

RESPONSE_SCHEMA_EXTRACT_INDICATORS = {
    "type": "object",
    "properties": {
        "language": {
            "type": "string",
            "description": "User's configured language code (e.g., 'zh-cn', 'en', 'ja', 'ko', 'es', etc.), indicating the language adaptation used for this return result",
        },
        "content_type": {
            "type": "string",
            "description": "Identified content type: 'medical_report' for medical examination reports, or 'non_health_related' for all other content types.",
        },
        "content_info": {
            "type": "object",
            "properties": {
                "content_type_detail": {"type": "string", "description": "Specific content type description, returned according to user language settings (e.g., Complete Blood Count, Biochemical Panel, CT, MRI, Ultrasound, etc.)"},
                "content_category": {
                    "type": "string", 
                    "description": "Content category in user's language (e.g., Laboratory Test, Imaging Examination, Pathology Report, or Non-health-related content)",
                },
                "date_time": {"type": "string", "description": "Relevant date and time (YYYY-MM-DD HH:MM:SS format). Date Priority: Sample Collection Date > Sample Receipt Date > Report Date > File Upload Date. Always use the highest priority date found in the document."},
                "subject_info": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Patient name"},
                        "details": {"type": "string", "description": "Patient details (gender, age, etc.)"},
                    },
                },
                "source": {"type": "string", "description": "Source information (hospital name, brand name, capture environment, etc.)"},
                "reference_number": {"type": "string", "description": "Relevant number (examination number, product number, record number, etc.)"},
            },
        },
        "indicators": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "original_indicator": {"type": "string", "description": "Medical indicator name, translated and adapted according to user language settings using standard medical terminology"},
                    "value": {
                        "type": "string",
                        "description": 'Indicator value. Numerical type includes value and unit (e.g., "120 g/L"); descriptive type keeps original text from report.',
                    },
                    "reference_range": {
                        "type": "string",
                        "description": 'Reference range from report (e.g., "110-160 g/L"). Returned according to user language settings.',
                    },
                    "unit": {
                        "type": "string",
                        "description": "Unit of the indicator (e.g., g/L, mmol/L, mg/dL, kcal, etc.). Empty string if no unit applicable.",
                    },
                    "detection_method": {
                        "type": "string",
                        "enum": ["laboratory", "Imaging", "Physiological", "Pathological", "wearable"],
                        "description": "Detection method type: 'laboratory' (lab tests like blood/urine tests), 'Imaging' (CT/MRI/X-ray/ultrasound), 'Physiological' (vital signs like blood pressure/heart rate), 'Pathological' (biopsy/pathology), 'wearable' (smart devices/wearables).",
                    },
                    "status": {
                        "type": "string",
                        "enum": ["normal", "high", "low"],
                        "description": "Status assessment, must use only one of the following three English values: 'normal' (within normal range), 'high' (elevated/excessive/needs attention), 'low' (low/insufficient/needs attention).",
                    },
                    "notes": {
                        "type": "string",
                        "description": "Clinical significance or abnormality explanation. Language determined by user language settings.",
                    },
                },
                "required": [
                    "original_indicator",
                    "value",
                    "status",
                ],
            },
        },
        "additional_info": {
            "type": "object",
            "properties": {
                "content_summary": {"type": "string", "description": "Findings summary from the medical report. Language determined by user language settings."},
                "assessment": {"type": "string", "description": "Impression/diagnosis from the report. Language determined by user language settings."},
                "recommendations": {"type": "string", "description": "Doctor's advice or recommendations. Language determined by user language settings."},
                "follow_up": {"type": "string", "description": "Recheck or follow-up suggestions. Language determined by user language settings."},
                "specialist": {"type": "string", "description": "Reporting doctor name."},
                "reviewer": {"type": "string", "description": "Reviewing doctor name."},
            },
        },
    },
    "required": ["language", "content_type", "content_info", "indicators"],
}


SIMPLE_PROMPT_EXTRACT_INDICATORS = """Analyze medical examination reports, extract all test indicator information and generate a file abstract, return in JSON format.

File Abstract Requirements (no more than 200 words):
- Report type (e.g., Complete Blood Count, CT examination, etc.)
- Main examination items or body parts
- Key findings or abnormalities (if any)
- Overall conclusion (normal/abnormal)

Extraction Requirements:
- original_indicator: Original indicator name (original language, ≤100 characters, medical indicators only)
- value: Indicator value (numerical with unit, descriptive keep original text)
- reference_range: Reference range
- status: Abnormal status ("normal"/"high"/"low")  
- notes: Remarks information

Report Classification: Numerical (Complete Blood Count, etc.), Descriptive (CT/MRI, etc.), Mixed

Return JSON Structure:
{
  "file_abstract": "File abstract (within 200 words)",
  "report_info": {
    "report_type": "Report type",
    "report_category": "Numerical/Descriptive/Mixed", 
    "date_time": "Examination date (YYYY-MM-DD HH:MM:SS). Date Priority: Sample Collection Date > Sample Receipt Date > Report Date > File Upload Date",
    "patient_info": {"name":"","gender":"","age":""},
    "hospital": "Hospital name",
    "exam_number": "Examination number",
    "patient_id": "Patient ID"
  },
  "indicators": [{
    "original_indicator": "Test indicator name",
    "value": "Indicator value",
    "reference_range": "Reference range",
    "status": "normal/high/low",
    "notes": "Remarks"
  }],
  "additional_info": {
    "findings_summary": "Findings summary",
    "impression": "Impression/Diagnosis", 
    "doctor_advice": "Doctor's advice",
    "recheck_suggestion": "Recheck suggestion",
    "reporting_doctor": "Reporting doctor",
    "reviewing_doctor": "Reviewing doctor"
  }
}"""

