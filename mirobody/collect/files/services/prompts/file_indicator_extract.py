def get_extract_indicators_prompt(language: str = "zh-cn") -> str:
    """
    Generate the prompt for extracting health-related indicators from various content types.

    `language` localizes what a person reads. It must never reach
    `original_indicator`, which is the key the resolver looks up: this prompt
    used to translate it into the UPLOADER's UI language, so a Chinese report
    read by an `en` account stored `Absolute Neutrophil Count` instead of the
    printed 中性粒细胞绝对值. Measured on a standard 25-item panel: printed
    names resolved 25/25, their translations 17/25, and the same analyte got
    two series identities depending on who uploaded it.

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
- **Other**: Any content with no health measurement, test result or clinical finding in it

A notice printed on a document (a watermark, "SAMPLE", "COPY", "仅供参考", a disclaimer) does not make it
non-health content: judge it by the measurements and findings it carries.

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
- Clinical notes: outpatient and inpatient records, discharge summaries, consultation notes. Extract every measured value they state (temperature, pulse, blood pressure, weight, a lab value quoted in the text), each as one indicator; the narrative itself is not an indicator
- Self-measurement logs: home blood pressure, glucose, weight or temperature records, as a table, a list or a photo of a screen or a notebook. Every value in every row is one indicator, with that row's own `date_time`

---

## STEP 2: If Health-Related, Proceed with Extraction

### User Language Settings:
Language code: {language}
- All adaptable fields must use language corresponding to `{language}`
- Keep `original_indicator` and `value` exactly as the report prints them, whatever language that is
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
| original_indicator | Indicator name EXACTLY as printed in the report: same language, same spelling, same abbreviation, any `#` or `%` suffix included. Never translate or normalize it |
| value | The result only: the number with its comparator if any ("5.62", "<0.5", "++", "阴性"). Do NOT put the unit here; descriptive results keep the original text exactly |
| unit | The unit EXACTLY as printed beside the value (e.g., "g/L", "mmol/L", "×10⁹/L"), or empty string if none |
| reference_range | The reference range EXACTLY as printed (e.g., "4.0-10.0", "<5.2"), empty string if none |
| detection_method | "laboratory" / "Imaging" / "Physiological" / "Pathological" / "wearable" |
| status | "normal" / "high" / "low" as the report flags it, or by comparing with the reference range |
| notes | Clinical significance or abnormality explanation in user's language |
| date_time | The date (and time) printed on THIS row, YYYY-MM-DD HH:MM:SS, when rows carry their own dates (a log, a table by day); empty string when the row has the document's date |

### Completeness Requirements:
1. **Extract EVERY indicator** listed in the report, including normal results
2. **Do NOT skip** any test items, even if results are within normal range
3. **For descriptive reports** (imaging/pathology): Extract each organ/region finding as separate indicator
4. **Preserve precision**: Keep exact numerical values and units as shown in report
5. **Include sub-items**: If a test has multiple components (e.g., lipid panel), extract each component separately
6. **Split pairs**: A value printed as a pair (blood pressure "120/80") is two indicators (systolic, diastolic), each with its own value
7. **Rows with their own dates**: In a log or a table by day, each row's values are separate indicators carrying that row's `date_time`; the same name on twelve days is twelve indicators

---

## General Rules:
1. **Completeness**: Extract ALL indicators from the report - do NOT omit any test results
2. **Status**: Always one of: "normal", "high", "low" (compare with reference range)
3. **Detection Method**: Always one of: "laboratory", "Imaging", "Physiological", "Pathological", "wearable"
4. **Units**: `unit` holds the unit; `value` never repeats it
5. **Language**: Only `notes` is written in the user's language ({language}). `original_indicator`, `value`, `unit` and `reference_range` are copied from the report verbatim, whatever its language, abbreviations and `#` / `%` suffixes included. The printed name is the key the standardization stage looks up — a translated one resolves to no code, or to a different measurement, and `中性粒细胞#` and `中性粒细胞%` are two different measurements
6. **Precision**: Preserve exact numerical values as shown in the report
7. **Privacy Protection**: Do NOT extract the following personal identifiable information (PII):
   - ID number (身份证号)
   - Phone number
   - Detailed address (only keep city/district level if needed)
   - Patient ID / Medical record number
   - Only extract: name, age, gender, and medical-related dates

---

## Examples:

### Complete Medical Report Example (MUST follow this structure):
```json
{{
  "language": "{language}",
  "content_type": "medical_report",
  "content_info": {{
    "content_type_detail": "Complete Blood Count",
    "content_category": "Laboratory Test",
    "date_time": "<the date this document prints, YYYY-MM-DD HH:MM:SS, or empty>",
    "subject_info": {{
      "name": "张三",
      "details": "Male, 31 years"
    }},
    "source": "Huashan Hospital Affiliated to Fudan University",
    "reference_number": ""
  }},
  "indicators": [
    {{"original_indicator": "白细胞计数", "value": "15.5", "reference_range": "4.0-10.0", "unit": "×10⁹/L", "detection_method": "laboratory", "status": "high", "notes": "偏高", "date_time": ""}},
    {{"original_indicator": "红细胞计数", "value": "4.5", "reference_range": "4.0-5.5", "unit": "×10¹²/L", "detection_method": "laboratory", "status": "normal", "notes": "", "date_time": ""}}
  ],
  "additional_info": {{
    "content_summary": "血常规检查",
    "assessment": "白细胞偏高，建议复查",
    "recommendations": "",
    "follow_up": "",
    "specialist": "",
    "reviewer": ""
  }}
}}
```

### Non-Medical Content (Return Empty):
```json
{{
  "language": "{language}",
  "content_type": "non_health_related",
  "content_info": {{
    "content_type_detail": "Food image",
    "content_category": "Non-health-related content"
  }},
  "indicators": []
}}
```

---

## Required Response Fields (MUST use exact field names):
- `language`: User's language code (`{language}`)
- `content_type`: "medical_report" or "non_health_related"
- `content_info`: Object with these EXACT fields:
  - `content_type_detail`: Report type (e.g., "Complete Blood Count", "CT", "MRI")
  - `content_category`: Category (e.g., "Laboratory Test", "Imaging Examination")
  - `date_time`: Date in YYYY-MM-DD HH:MM:SS format (Priority: Sample Collection > Sample Receipt > Report Date)
  - `subject_info`: Object with `name` (patient name) and `details` (gender, age combined as string like "Male, 31 years")
  - `source`: Hospital/facility name
  - `reference_number`: Examination/record number (if available)
- `indicators`: Array of ALL extracted indicators (empty array for non-medical content)
- `additional_info`: Optional object with `content_summary`, `assessment`, `recommendations`, `follow_up`, `specialist`, `reviewer`
"""


# Strict-compatible: every object closed with `additionalProperties: false`
# and every property in `required`. OpenAI's json_schema answers HTTP 400
# otherwise ("'additionalProperties' is required to be supplied and to be
# false"): GPT-6 Luna, GPT-6 Sol, GPT-6.1 Sol and GPT-5.6 Terra through
# OpenRouter, 2026-10-06, so no upload could be read with them. A field the
# document does not show is an empty string, which every reader already treats
# as absent (`content_formatter` tests each one for truth).
RESPONSE_SCHEMA_EXTRACT_INDICATORS = {
    "type": "object",
    "additionalProperties": False,
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
            "additionalProperties": False,
            "properties": {
                "content_type_detail": {"type": "string", "description": "Specific content type description, returned according to user language settings (e.g., Complete Blood Count, Biochemical Panel, CT, MRI, Ultrasound, etc.)"},
                "content_category": {
                    "type": "string", 
                    "description": "Content category in user's language (e.g., Laboratory Test, Imaging Examination, Pathology Report, or Non-health-related content)",
                },
                "date_time": {"type": "string", "description": "Relevant date and time (YYYY-MM-DD HH:MM:SS format). Date Priority: Sample Collection Date > Sample Receipt Date > Report Date. Always use the highest priority date found in the document; empty string when the document shows no date — never invent one."},
                "subject_info": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "name": {"type": "string", "description": "Patient name"},
                        "details": {"type": "string", "description": "Patient details (gender, age, etc.)"},
                    },
                    "required": ["name", "details"],
                },
                "source": {"type": "string", "description": "Source information (hospital name, brand name, capture environment, etc.)"},
                "reference_number": {"type": "string", "description": "Relevant number (examination number, product number, record number, etc.)"},
            },
            "required": [
                "content_type_detail",
                "content_category",
                "date_time",
                "subject_info",
                "source",
                "reference_number",
            ],
        },
        "indicators": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "original_indicator": {
                        "type": "string",
                        "description": "Medical indicator name exactly as the report prints it, in the report's own language, abbreviations and any '#' or '%' suffix included. Never translated: this string is the key the standardization stage resolves to a LOINC code, so a translated name resolves to nothing or to a different measurement.",
                    },
                    "value": {
                        "type": "string",
                        "description": 'The result only, without its unit: a number with its comparator ("5.62", "<0.5"), a grade ("++") or a label ("阴性"); descriptive results keep the original text.',
                    },
                    "reference_range": {
                        "type": "string",
                        "description": 'Reference range exactly as printed in the report (e.g., "110-160", "<5.2"). Empty string if none.',
                    },
                    "unit": {
                        "type": "string",
                        "description": "Unit exactly as printed beside the value (e.g., g/L, mmol/L, mg/dL, ×10⁹/L). Empty string if none.",
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
                    "date_time": {
                        "type": "string",
                        "description": "The date (and time) printed on THIS row, YYYY-MM-DD HH:MM:SS, when rows carry their own dates (a home log, a table by day). Empty string when the row has the document's date.",
                    },
                },
                # Every column the report prints is required, empty string
                # when the report shows none. Left optional, the model omitted
                # reference_range, unit and detection_method on a report that
                # printed all three, and the stored rows had no range at all.
                "required": [
                    "original_indicator",
                    "value",
                    "unit",
                    "reference_range",
                    "detection_method",
                    "status",
                    "notes",
                    "date_time",
                ],
            },
        },
        "additional_info": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "content_summary": {"type": "string", "description": "Findings summary from the medical report. Language determined by user language settings."},
                "assessment": {"type": "string", "description": "Impression/diagnosis from the report. Language determined by user language settings."},
                "recommendations": {"type": "string", "description": "Doctor's advice or recommendations. Language determined by user language settings."},
                "follow_up": {"type": "string", "description": "Recheck or follow-up suggestions. Language determined by user language settings."},
                "specialist": {"type": "string", "description": "Reporting doctor name."},
                "reviewer": {"type": "string", "description": "Reviewing doctor name."},
            },
            "required": [
                "content_summary",
                "assessment",
                "recommendations",
                "follow_up",
                "specialist",
                "reviewer",
            ],
        },
    },
    "required": ["language", "content_type", "content_info", "indicators", "additional_info"],
}

