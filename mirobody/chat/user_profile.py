import asyncio
import logging
import os
import re
from typing import List, Dict, Optional, Any
from datetime import datetime, date

from mirobody.utils.truncate import _split
from mirobody.utils import execute_query
from mirobody.utils.llm import async_get_text_completion
from mirobody.utils.config import safe_read_cfg

logger = logging.getLogger(__name__)

MAX_TOKENS = 10000
MAX_OUTPUT_TOKENS = 32000  # No limit on profile output length to avoid truncation
MAX_PREVIOUS_PROFILE_LENGTH = 15000  # Maximum character limit for previous profile version

#-----------------------------------------------------------------------------

tool_result_prompts = {
    "zh": "请根据上述工具调用结果，为用户提供完整的回答和分析。如果没有找到相关数据，请说明可能的原因并提供建议。",
    "en": "Please provide a complete answer and analysis for the user based on the above tool call results. If no relevant data is found, please explain possible reasons and provide suggestions.",
    "ja": "上記のツール呼び出し結果に基づいて、ユーザーに完全な回答と分析を提供してください。関連データが見つからない場合は、考えられる理由を説明し、提案を提供してください。",
    "fr": "Veuillez fournir une réponse complète et une analyse pour l'utilisateur basées sur les résultats de l'appel d'outil ci-dessus. Si aucune donnée pertinente n'est trouvée, veuillez expliquer les raisons possibles et fournir des suggestions.",
    "es": "Por favor, proporcione una respuesta completa y análisis para el usuario basado en los resultados de la llamada de herramienta anterior. Si no se encuentran datos relevantes, explique las posibles razones y proporcione sugerencias.",
}

language_instructions = {
    "zh": "请用中文回复。",
    "en": "Please reply in English.",
    "ja": "日本語で返信してください。",
    "fr": "Veuillez répondre en français.",
    "es": "Por favor, responda en español.",
}

#-----------------------------------------------------------------------------
# Shared Scenario Components (used by multiple prompts)
#-----------------------------------------------------------------------------

SCENARIO_CRITERIA_TABLE = """
| 场景 (Scenario) | 判断条件 (Criteria) |
|---|---|
| 隐匿性高血压人群早筛 | 年龄≥40岁；久坐少运动；长期熬夜或压力大；有高血压家族史 |
| 高血脂与冠心病高危人群管理 | 男≥35岁，女≥45岁；高脂饮食；血脂异常；冠心病家族史 |
| 冠心病复发风险人群二级预防 | 已确诊冠心病（心绞痛/支架/搭桥）；吸烟；合并三高 |
| 糖尿病早期预防与筛查 | 年龄≥40岁；BMI≥28；一级亲属糖尿病；腰围偏大 |
| 糖尿病患者并发症防控 | 病程≥5年；血糖波动大；合并高血压/高血脂；缺少眼底/肾筛查 |
| 绝经后女性骨质疏松防治 | 女性≥50岁或绝经；体型瘦；日晒少；家族骨折史 |
| 中老年膝关节炎管理 | 年龄≥50岁；长期负重/爬楼；BMI≥27；膝痛反复 |
| 结直肠癌高危人群早筛 | ≥45岁；息肉/肠炎史；一级亲属肠癌；高脂低纤饮食 |
| 乳腺癌高危女性早筛 | 女40–69岁；晚育/未育；家族史；良性乳腺病变 |
| 前列腺癌早期筛查 | 男≥50岁；家族史；排尿困难/夜尿多 |
| 轻度认知障碍（MCI）早识别 | ≥60岁；记忆下降；重复问事；痴呆家族史/卒中史 |
| 职场大脑健康维护 | 35–55岁；熬夜；高压；注意力下降 |
| 胃肠动力减慢与便秘管理 | ≥50岁；活动少；饮水少；纤维不足；慢性便秘 |
| 胃痛反酸的胃癌风险筛查 | ≥40岁；长期胃不适；幽门螺杆菌；家族史 |
| 慢性失眠睡眠重建 | ≥3个月失眠；白天乏力；依赖安眠药/酒精 |
| 睡眠呼吸暂停高危人群识别 | 打鼾憋气；白天嗜睡；中年男性；BMI≥28；颈围粗 |
| 职场焦虑与抑郁预防 | 25–55岁；高压；易怒/失眠/兴趣减退 |
| 空巢中老年情绪支持 | 独居；活动减少；孤独/情绪低落 |
| 胸痛人群急性心血管风险识别 | ≥45岁；三高；吸烟；胸痛放射；呼吸困难出汗；家族史 |
"""

SCENARIO_SELECTION_RULES_BASE = """
1. **Comprehensive Judgment**: Evaluate based on multiple dimensions including age, gender, BMI, medical history, family history, and lifestyle factors
2. **Criteria Matching**: Criteria separated by "；" are multiple conditions. User must meet **2 or more** major conditions to match a scenario
3. **Priority Rules**:
   - Management scenarios for confirmed diseases take priority over early screening scenarios
   - Acute risk scenarios take priority over chronic management scenarios
   - When multiple scenarios match, select the most urgent and relevant one
"""

#-----------------------------------------------------------------------------

GENERATE_USER_PROFILE_PROMPT = f"""
You are a professional health profile analyst. Generate a concise user health profile in Markdown format based on the provided data.

## Critical Instructions
1. **Output Format**: Generate the profile strictly in Markdown format. Output the Markdown content DIRECTLY without wrapping it in code blocks (no ```markdown``` or ``` wrappers)
2. **Language Consistency**: Use the same language as the user's preferred language throughout the output
3. **Incremental Update**: If a previous profile is provided, ADD new information to it. DO NOT modify or delete existing information
4. **Data Integrity**: Only include information that can be verified from the provided data
5. **No Diagnosis**: Do not make medical diagnoses or provide medical advice
6. **Concise Output**: Keep the output concise and to the point. Summarize and condense information where appropriate
7. **Skip Empty Sections**: If a section or subsection has no data, do NOT output it at all (no placeholder text like "暂无数据")
8. **No Code Blocks**: Do NOT wrap the output in ```markdown``` or any other code block format. Output plain Markdown text directly

## Output Structure (Markdown)
Only output sections that have actual data. Skip sections entirely if no data is available.

### 1. 用户基础信息 / Basic Information
Include: gender, age, race/ethnicity, language, blood type, and other basic demographics

### 2. 生活方式 / Lifestyle
Include: exercise habits, sleep patterns, diet, smoking status, alcohol consumption, and other lifestyle factors

### 3. 健康情况 / Health Status
Include the following subsections (only output subsections with actual data):
- 用药史 / Medication History: current and past medications, adverse drug reactions
- 既往史 / Past Medical History: previous diseases, surgeries, hospitalizations, allergies, trauma history
- 家族史 / Family History: hereditary diseases, immediate family health status
- 免疫接种 / Immunization History: vaccination records
- 月经周期 / Menstrual Cycle: (only for females, skip for males)

### 4. 近一周设备数据 / Recent Device Data (Past 7 Days)
Include: heart rate, steps, sleep data, and other wearable device metrics from the past week

## 场景 (Health Management Scenario)

Based on user health data, select the most appropriate health management scenario according to the **Scenario Criteria Table** below.

### Scenario Criteria Table
{SCENARIO_CRITERIA_TABLE}
### Scenario Selection Rules
{SCENARIO_SELECTION_RULES_BASE}
4. **Previous Scenario Continuity** (IMPORTANT):
   - If a Previous Scenario is provided, prioritize maintaining scenario continuity
   - Only change the scenario if:
     - User's health status has significantly changed (e.g., new disease diagnosis, major improvement/deterioration)
     - A more urgent health risk has emerged (e.g., acute cardiovascular risk)
     - Current health data no longer matches the previous scenario criteria
   - If user health data still matches the previous scenario criteria, KEEP the previous scenario
   - Scenario continuity helps maintain consistent health management tracking
5. **No Match Handling**: If user data is insufficient or does not match any scenario criteria, do NOT output the scenario section at all (including the heading)

## Important Notes
- **Skip Empty Sections**: Do NOT output any section or subsection that has no data. Do NOT write "暂无数据" or "No data available"
- **Concise**: Summarize and condense information. Avoid verbose descriptions
- **Data Completeness**: Ensure all available data is included in the appropriate sections
- **Scenario Selection**:
  - Only select a scenario when user health data clearly shows relevant risk factors or health conditions
  - Select only ONE most appropriate scenario, not multiple
  - If user does not match any scenario, do NOT output the scenario section at all (including the heading)
  - Do not force-match scenarios; only select when there is clear evidence in health data
  - **Scenario output format (MUST use Chinese scenario name only)**:

## 场景

隐匿性高血压人群早筛

  - **IMPORTANT**: Output the scenario name in Chinese ONLY. Do NOT include English translation or any additional description
"""

#-----------------------------------------------------------------------------
# Scenario Mapping Table
#-----------------------------------------------------------------------------

SCENARIO_MAPPING = {
    "隐匿性高血压人群早筛": {
        "scenario_en": "Screening for Masked Hypertension in High-Risk Populations",
        "scenario_image_url": ""
    },
    "高血脂与冠心病高危人群管理": {
        "scenario_en": "Management of Hyperlipidemia and High-Risk Populations for Coronary Heart Disease",
        "scenario_image_url": ""
    },
    "冠心病复发风险人群二级预防": {
        "scenario_en": "Secondary Prevention for Populations at Risk of Coronary Heart Disease Recurrence",
        "scenario_image_url": ""
    },
    "糖尿病早期预防与筛查": {
        "scenario_en": "Early Prevention and Screening for Diabetes Mellitus",
        "scenario_image_url": ""
    },
    "糖尿病患者并发症防控": {
        "scenario_en": "Prevention and Control of Complications in Patients with Diabetes Mellitus",
        "scenario_image_url": ""
    },
    "绝经后女性骨质疏松防治": {
        "scenario_en": "Prevention and Management of Osteoporosis in Postmenopausal Women",
        "scenario_image_url": ""
    },
    "中老年膝关节炎管理": {
        "scenario_en": "Management of Knee Osteoarthritis in Middle-Aged and Older Adults",
        "scenario_image_url": ""
    },
    "结直肠癌高危人群早筛": {
        "scenario_en": "Early Screening for High-Risk Populations of Colorectal Cancer",
        "scenario_image_url": ""
    },
    "乳腺癌高危女性早筛": {
        "scenario_en": "Early Screening for Women at High Risk of Breast Cancer",
        "scenario_image_url": ""
    },
    "前列腺癌早期筛查": {
        "scenario_en": "Early Screening for Prostate Cancer",
        "scenario_image_url": ""
    },
    "轻度认知障碍（MCI）早识别": {
        "scenario_en": "Early Identification of Mild Cognitive Impairment (MCI)",
        "scenario_image_url": ""
    },
    "职场大脑健康维护": {
        "scenario_en": "Brain Health Maintenance in the Workplace",
        "scenario_image_url": ""
    },
    "胃肠动力减慢与便秘管理": {
        "scenario_en": "Management of Gastrointestinal Hypomotility and Constipation",
        "scenario_image_url": ""
    },
    "胃痛反酸的胃癌风险筛查": {
        "scenario_en": "Gastric Cancer Risk Screening in Individuals with Epigastric Pain and Acid Reflux",
        "scenario_image_url": ""
    },
    "慢性失眠睡眠重建": {
        "scenario_en": "Sleep Reconstruction for Chronic Insomnia",
        "scenario_image_url": ""
    },
    "睡眠呼吸暂停高危人群识别": {
        "scenario_en": "Identification of High-Risk Populations for Obstructive Sleep Apnea",
        "scenario_image_url": ""
    },
    "职场焦虑与抑郁预防": {
        "scenario_en": "Prevention of Anxiety and Depression in the Workplace",
        "scenario_image_url": ""
    },
    "空巢中老年情绪支持": {
        "scenario_en": "Emotional Support for Empty-Nest Middle-Aged and Older Adults",
        "scenario_image_url": ""
    },
    "胸痛人群急性心血管风险识别": {
        "scenario_en": "Identification of Acute Cardiovascular Risk in Individuals Presenting with Chest Pain",
        "scenario_image_url": ""
    },
    "都市人群亚健康情况分析": {
        "scenario_en": "Analysis of Suboptimal Health Conditions Among Urban Populations",
        "scenario_image_url": ""
    }
}

#-----------------------------------------------------------------------------
# Default Scenario (Fallback)
#-----------------------------------------------------------------------------

DEFAULT_SCENARIO_ZH = "都市人群亚健康情况分析"


def _get_default_scenario_info() -> Dict[str, str]:
    """
    Get default scenario info for fallback when no scenario is matched
    
    Uses "都市人群亚健康情况分析" as the default scenario,
    with the image from "职场大脑健康维护" scenario.
    
    Returns:
        Dictionary containing scenario_zh, scenario_en, scenario_image_url
    """
    default_mapping = SCENARIO_MAPPING[DEFAULT_SCENARIO_ZH]
    # Use image from "职场大脑健康维护" scenario
    brain_health_scenario_en = SCENARIO_MAPPING["职场大脑健康维护"]["scenario_en"]
    scenario_image_url = _generate_scenario_image_url(brain_health_scenario_en)
    
    return {
        "scenario_zh": DEFAULT_SCENARIO_ZH,
        "scenario_en": default_mapping["scenario_en"],
        "scenario_image_url": scenario_image_url
    }

#-----------------------------------------------------------------------------

GENERATE_USER_PROFILE_USER_PROMPT = """
## User Basic Information
{basic_info}

## Health Indicator Data
{health_data}

## Device Data (Past 7 Days)
{device_data}

## Previous Profile (for incremental update)
{previous_profile}

## Previous Scenario (if exists)
{previous_scenario}

Please generate a comprehensive health profile in Markdown format following the required structure. Remember to only ADD new information to the previous profile, not modify or delete existing content. If a Previous Scenario exists, consider maintaining scenario continuity unless there are significant health changes.
"""

MERGE_USER_PROFILE_PROMPT = """
You are a professional health profile analyst. Merge the following profile chunks into a single concise Markdown profile.

## Merging Instructions
1. **Consolidate Information**: Combine related entries from different chunks
2. **Preserve All Data**: Ensure no important information is lost during merging
3. **Remove Duplicates**: Eliminate redundant information while preserving unique details
4. **Concise Output**: Keep the output concise and summarized
5. **Skip Empty Sections**: Do NOT output any section or subsection that has no data
6. **Language Consistency**: Use consistent language throughout the merged profile

## Required Output Structure
Only output sections that have actual data:
1. 用户基础信息 / Basic Information
2. 生活方式 / Lifestyle
3. 健康情况 / Health Status (subsections: 用药史, 既往史, 家族史, 免疫接种, 月经周期 - only include subsections with data)
4. 近一周设备数据 / Recent Device Data
5. ## 场景 (仅当有适用场景时输出，使用二级标题格式，直接列出中文场景名称。格式: ## 场景\n\n场景名称。场景名称必须使用中文，不要包含英文翻译)

Output ONLY the merged Markdown profile, no additional explanations. Skip sections with no data.
Do NOT wrap the output in ```markdown``` or any other code block format. Output plain Markdown text directly.
"""

MERGE_USER_PROFILE_USER_PROMPT = """
Here are the profile chunks to merge:

{profile_chunks}

Please merge all chunks into a single comprehensive Markdown profile following the required structure.
"""

#-----------------------------------------------------------------------------
# Scenario Only Prompt (for existing profiles without scenario)
#-----------------------------------------------------------------------------

SCENARIO_ONLY_PROMPT = f"""
You are a professional health profile analyst. Based on the provided user health profile, select the most appropriate health management scenario.

## Scenario Criteria Table
{SCENARIO_CRITERIA_TABLE}
## Selection Rules
{SCENARIO_SELECTION_RULES_BASE}
4. **No Match Handling**: If user data is insufficient or does not match any scenario criteria, output "NO_MATCH"

## Output Format

Output ONLY the Chinese scenario name (e.g., "隐匿性高血压人群早筛") or "NO_MATCH" if no scenario applies.
Do NOT include any explanation, translation, or additional text.
"""

SCENARIO_ONLY_USER_PROMPT = """
## User Health Profile

{profile_content}

Based on the above health profile, select the most appropriate health management scenario.
Output ONLY the Chinese scenario name or "NO_MATCH".
"""

#-----------------------------------------------------------------------------
# Remove Indicators from Profile Prompt
#-----------------------------------------------------------------------------

REMOVE_INDICATORS_PROMPT = """
You are a professional health profile editor. Your task is to remove specific health indicators and their related content from an existing user health profile.

## Critical Instructions (MUST FOLLOW STRICTLY)

1. **STRICT EXACT MATCHING**: Remove ONLY content that EXACTLY matches the deleted indicator names. Do NOT remove content with similar but different names.
2. **PRESERVE EVERYTHING ELSE**: Keep ALL other information completely intact, word for word. Do NOT modify, rephrase, or summarize any content that is not directly related to the deleted indicators.
3. **NO ADDITIONS**: Do NOT add any new information, commentary, or explanations.
4. **KEEP SCENARIO**: Do NOT modify or remove the scenario section (## 场景) if it exists.
5. **OUTPUT FORMAT**: Output the updated profile in Markdown format DIRECTLY without wrapping it in code blocks.
6. **LANGUAGE**: Keep the exact same language as the original profile.
7. **WHEN IN DOUBT, KEEP IT**: If you are uncertain whether content is related to a deleted indicator, DO NOT remove it. Only remove content you are 100% certain is related.
8. **IF NOTHING FOUND**: If the deleted indicators are not found in the profile, return the original profile EXACTLY as-is, unchanged.

## What to Remove (ONLY these, nothing else)

For each deleted indicator, remove ONLY:
- The exact indicator name and its value/data
- Sentences or bullet points that DIRECTLY contain that specific indicator
- Do NOT remove entire sections just because one indicator in them is deleted

## What to Keep (DO NOT touch these)

- ALL content not directly mentioning the deleted indicator names
- The original formatting, structure, and section headers
- The scenario section (## 场景) - NEVER modify this
- Basic user information (age, gender, blood type, etc.)
- Device data unrelated to deleted indicators
- Family history, medication history, lifestyle information - unless they explicitly mention the deleted indicator

## Example

If deleted indicator is "血红蛋白: 135 g/L":
- ✅ REMOVE: "血红蛋白: 135 g/L" and any sentence directly about this value
- ❌ DO NOT REMOVE: "白细胞计数: 6.5" (different indicator)
- ❌ DO NOT REMOVE: "贫血家族史" (related concept but NOT the indicator itself)
- ❌ DO NOT REMOVE: Any other health information
"""

REMOVE_INDICATORS_USER_PROMPT = """
## Current User Health Profile

{profile_content}

## Deleted Indicators (to be removed)

{deleted_indicators}

Please update the health profile by removing all content related to the deleted indicators listed above. Return the updated profile in Markdown format.
"""

#-----------------------------------------------------------------------------

def _clean_markdown_code_block(content: str) -> str:
    """
    Clean markdown code block format from LLM output
    
    If content is wrapped in ```markdown...``` or ```...```, extract the body content
    
    Args:
        content: Raw content from LLM output
        
    Returns:
        Cleaned body content
    """
    if not content:
        return content
    
    content = content.strip()
    
    # Handle ```markdown ... ``` format
    if content.startswith("```markdown") and content.endswith("```"):
        content = content[len("```markdown"):].strip()
        if content.endswith("```"):
            content = content[:-3].strip()
        return content
    
    # Handle ```md ... ``` format
    if content.startswith("```md") and content.endswith("```"):
        content = content[len("```md"):].strip()
        if content.endswith("```"):
            content = content[:-3].strip()
        return content
    
    # Handle generic ``` ... ``` format (first line may have language identifier)
    if content.startswith("```") and content.endswith("```"):
        # Remove leading ```
        content = content[3:]
        # If the first line is a language identifier (e.g., markdown, md), remove it
        first_newline = content.find("\n")
        if first_newline != -1:
            first_line = content[:first_newline].strip().lower()
            # Check if it's a language identifier (no spaces and short length)
            if first_line and len(first_line) < 20 and " " not in first_line:
                content = content[first_newline + 1:]
        # Remove trailing ```
        if content.endswith("```"):
            content = content[:-3]
        return content.strip()
    
    return content


def _generate_scenario_image_name(name: str) -> str:
    """
    Generate image filename from English scenario name
    
    Processing rules:
    1. Convert to lowercase
    2. Replace spaces with underscores
    3. Remove other special characters, keeping only letters, numbers, underscores, and hyphens
    
    Args:
        name: English scenario name
        
    Returns:
        Processed image filename (without extension)
        
    Examples:
        "Early Screening for Masked Hypertension" -> "early_screening_for_masked_hypertension"
        "Early Identification of Mild Cognitive Impairment (MCI)" -> "early_identification_of_mild_cognitive_impairment_mci"
    """
    # Convert to lowercase
    name = name.lower()
    
    # Replace spaces with underscores
    name = re.sub(r'\s+', '_', name)

    # Replace hyphens with underscores
    name = name.replace('-', '_')
    
    # Remove other special characters, keeping only letters, numbers, underscores, and hyphens
    name = re.sub(r'[^\w\-]', '', name)
    
    return name


def _generate_scenario_image_url(scenario_en: str) -> str:
    """
    Generate complete URL for scenario image
    
    URL format: {S3_CDN}/scenario_report/{image_name}.png
    
    Args:
        scenario_en: English scenario name
        
    Returns:
        Complete scenario image URL
        
    Example:
        Input: "Early Screening for Masked Hypertension"
        Output: "https://cdn.example.com/scenario_report/early_screening_for_masked_hypertension.png"
    """
    s3_cdn = safe_read_cfg("s3_cdn") or ""
    image_name = _generate_scenario_image_name(scenario_en)
    return f"{s3_cdn}scenario_report/{image_name}.png"


def _extract_scenario_from_profile(profile_markdown: str) -> tuple[str, str]:
    """
    Extract scenario section from profile, separating scenario and profile content
    
    Test cases:
    - Input: "### Health Status\nContent\n\n## 场景\n\n隐匿性高血压人群早筛"
      Output: ("### Health Status\nContent", "隐匿性高血压人群早筛")
    - Input: "### Health Status\nContent" (no scenario)
      Output: ("### Health Status\nContent", "")
    - Input: "## 场景\n\n糖尿病早期预防与筛查\n\nAdditional content"
      Output: ("Additional content", "糖尿病早期预防与筛查")
    
    Args:
        profile_markdown: Complete profile Markdown text
        
    Returns:
        (profile_without_scenario, scenario_zh): Separated profile content and Chinese scenario name
    """
    
    if not profile_markdown:
        return "", ""
    
    # Match content after ## 场景 (scenario name)
    # Format: ## 场景\n\n场景名称
    pattern = r'##\s*场景\s*\n\s*\n\s*([^\n]+)'
    match = re.search(pattern, profile_markdown)
    
    if match:
        scenario_zh = match.group(1).strip()
        # Remove entire scenario section (including title and content)
        # From ## 场景 (Chinese title) to end of scenario name
        scenario_pattern = r'\n*##\s*场景\s*\n\s*\n\s*[^\n]+\n*'
        profile_without_scenario = re.sub(scenario_pattern, '', profile_markdown).strip()
        return profile_without_scenario, scenario_zh
    else:
        # No scenario found
        return profile_markdown.strip(), ""


def _get_scenario_info(scenario_zh: str) -> Optional[Dict[str, str]]:
    """
    Look up mapping table by Chinese scenario name to get English scenario and image URL
    
    Image URL is dynamically generated in format: {S3_CDN}/scenario_report/{image_name}.png
    
    Args:
        scenario_zh: Chinese scenario name
        
    Returns:
        Dictionary containing scenario_zh, scenario_en, scenario_image_url, or None if not found
        
    Example output:
        {
            "scenario_zh": "隐匿性高血压人群早筛",
            "scenario_en": "Early Screening for Masked Hypertension",
            "scenario_image_url": "https://cdn.example.com/scenario_report/early_screening_for_masked_hypertension.png"
        }
    """
    if not scenario_zh:
        return None
    
    scenario_zh_clean = scenario_zh.strip()
    
    if scenario_zh_clean in SCENARIO_MAPPING:
        mapping = SCENARIO_MAPPING[scenario_zh_clean]
        scenario_en = mapping["scenario_en"]
        # Dynamically generate scenario image URL
        scenario_image_url = _generate_scenario_image_url(scenario_en)
        return {
            "scenario_zh": scenario_zh_clean,
            "scenario_en": scenario_en,
            "scenario_image_url": scenario_image_url
        }
    else:
        # Scenario not found
        logger.warning(f"Scenario not found in mapping: {scenario_zh_clean}")
        return None


class BasicInfoService:
    """User basic information service"""
    
    @staticmethod
    def _convert_gender_to_text(gender: Optional[int], lang: Optional[str]) -> Optional[str]:
        """
        Convert gender number to text based on user language
        
        Args:
            gender: Gender number (0-Unknown, 1-Male, 2-Female)
            lang: User language
            
        Returns:
            Converted gender text
        """
        if gender is None:
            return None
        
        is_chinese = lang and ('zh' in lang.lower() or 'cn' in lang.lower() or lang.lower() == 'chinese')
        
        if is_chinese:
            gender_map = {0: '未知', 1: '男', 2: '女'}
        else:
            gender_map = {0: 'Unknown', 1: 'Male', 2: 'Female'}
        
        return gender_map.get(gender, 'Unknown' if not is_chinese else '未知')
    
    @staticmethod
    def _calculate_age(birth: Optional[str]) -> Optional[int]:
        """
        Calculate age based on birth date
        
        Args:
            birth: Birth date string (format: YYYY-MM-DD or other common formats)
            
        Returns:
            Age
        """
        if not birth:
            return None
        
        try:
            if isinstance(birth, date):
                birth_date = birth
            else:
                birth_date = datetime.strptime(str(birth)[:10], "%Y-%m-%d").date()
            
            today = date.today()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            return age
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    async def get_user_basic_info(user_id: str) -> Dict[str, Any]:
        """
        Get user basic information from health_app_user table
        
        Args:
            user_id: User ID
            
        Returns:
            User basic information dictionary
        """
        try:
            user_id_int = int(user_id)
        except (ValueError, TypeError):
            logger.info(f"Invalid user_id format: {user_id}")
            return {}
        
        sql = """
        select blood, gender, birth, lang
        from theta_ai.health_app_user
        where id = :user_id
        and is_del = false
        limit 1
        """
        
        results = await execute_query(
            sql,
            params={"user_id": user_id_int},
        )
        
        if results:
            result = results[0]
            raw_gender = result.get('gender')
            lang = result.get('lang') or "English"
            birth = result.get('birth')
            
            return {
                "blood_type": result.get('blood'),
                "gender": BasicInfoService._convert_gender_to_text(raw_gender, lang),
                "age": BasicInfoService._calculate_age(birth),
                "language": lang
            }
        else:
            logger.info(f"No basic info found for user: {user_id}")
            return {}


class DeviceDataService:
    """Device data service"""
    
    @staticmethod
    async def get_device_data(user_id: str) -> str:
        """
        Get recent device data starting with rolling_7d from th_series_data table, return formatted string
        
        Args:
            user_id: User ID
            
        Returns:
            Formatted device data string
        """
        sql = """
        select distinct on (indicator) indicator, value, to_char(start_time, 'YYYY-MM-DD HH24:MI:SS') as start_time
        from theta_ai.th_series_data
        where user_id = :user_id
        and indicator like 'rolling_7d%'
        and deleted = 0
        order by indicator, start_time desc
        """
        
        results = await execute_query(
            sql,
            params={"user_id": user_id},
        )
        
        if not results:
            return "No device data available"
        
        device_lines = []
        for result in results:
            indicator = result['indicator'].replace('rolling_7d_', '')
            value = result['value']
            start_time = result['start_time']
            device_lines.append(f"- {indicator}: {value} (as of {start_time})")
        
        return "\n".join(device_lines)


class UserProfileGenerator:
    """User profile generation service"""
    
    @staticmethod
    async def _get_existing_profile(user_id: str) -> tuple[Optional[str], Optional[str]]:
        """
        Get user's existing health profile and scenario
        
        Args:
            user_id: User ID
            
        Returns:
            Tuple of (profile, scenario_zh): Existing health profile in Markdown format and previous scenario, or (None, None) if not found
        """
        sql = """
        select common_part, scenario_zh
        from theta_ai.health_user_profile_by_system
        where user_id = :user_id
        and is_deleted = false
        order by version desc
        limit 1
        """
        
        results = await execute_query(
            sql,
            params={"user_id": user_id},
        )
        
        if results:
            profile = results[0].get('common_part')
            scenario_zh = results[0].get('scenario_zh')
            
            # Limit previous_profile length to prevent unlimited growth
            if profile and len(profile) > MAX_PREVIOUS_PROFILE_LENGTH:
                profile = profile[:MAX_PREVIOUS_PROFILE_LENGTH] + "\n\n... (truncated due to length limit)"
            
            return profile, scenario_zh
        
        return None, None
    
    @staticmethod
    async def _generate_profile_chunk(
        basic_info: str,
        health_data: str,
        device_data: str,
        previous_profile: str,
        previous_scenario: Optional[str] = None,
        language: str = "English"
    ) -> str:
        """
        Generate a single chunk of user profile using LLM
        
        自动选择提供商: OPENAI_API_KEY -> OpenAI | OPENROUTER_API_KEY -> OpenRouter | GOOGLE_API_KEY -> Gemini | ...
        
        Args:
            basic_info: User basic information
            health_data: Health indicator data
            device_data: Device data
            previous_profile: Previous version profile
            previous_scenario: Previous scenario (if exists)
            language: Generation language
            
        Returns:
            Generated profile in Markdown format
        """
        language_instruction = f"\n\nPlease generate the profile in {language}." if language != "English" else ""
        
        system_prompt = GENERATE_USER_PROFILE_PROMPT + language_instruction
        
        user_prompt = GENERATE_USER_PROFILE_USER_PROMPT.format(
            basic_info=basic_info,
            health_data=health_data,
            device_data=device_data,
            previous_profile=previous_profile or "No previous profile available",
            previous_scenario=previous_scenario or "No previous scenario"
        )
        
        # Use unified text generation interface (auto-select provider)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        result = await async_get_text_completion(
            messages=messages,
            temperature=0,
            max_tokens=MAX_OUTPUT_TOKENS,
        )
        
        if not result:
            logger.error("Profile generation failed: empty response")
            return ""
        
        # Clean possible markdown code block format
        return _clean_markdown_code_block(result.strip())
    
    @staticmethod
    async def _merge_profile_results(results: List[str], language: str = "English") -> str:
        """
        Merge multiple profile chunk results
        
        自动选择提供商: OPENAI_API_KEY -> OpenAI | OPENROUTER_API_KEY -> OpenRouter | GOOGLE_API_KEY -> Gemini | ...
        
        Args:
            results: List of multiple profile results
            language: Generation language
            
        Returns:
            Merged profile in Markdown format
        """
        if len(results) == 1:
            return results[0]
        
        language_instruction = f"\n\nPlease merge the profile in {language}." if language != "English" else ""
        
        profile_chunks = "\n\n---\n\n".join([f"### Chunk {i+1}\n{r}" for i, r in enumerate(results)])
        
        system_prompt = MERGE_USER_PROFILE_PROMPT + language_instruction
        user_prompt = MERGE_USER_PROFILE_USER_PROMPT.format(profile_chunks=profile_chunks)
        
        # Use unified text generation interface (auto-select provider)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        result = await async_get_text_completion(
            messages=messages,
            temperature=0,
            max_tokens=MAX_OUTPUT_TOKENS,
        )
        
        if not result:
            logger.error("Profile merge failed: empty response")
            return ""
        
        # Clean possible markdown code block format
        return _clean_markdown_code_block(result.strip())
    
    @staticmethod
    async def _generate_scenario_only(profile_content: str) -> Optional[str]:
        """
        Generate scenario only based on existing profile content using LLM
        
        This method is used when a user already has a profile but no scenario assigned.
        It analyzes the existing profile and determines the most appropriate scenario.
        
        Args:
            profile_content: Existing user profile content in Markdown format
            
        Returns:
            Chinese scenario name if matched, None if no match or error
        """
        if not profile_content:
            logger.warning("Cannot generate scenario: empty profile content")
            return None
        
        try:
            messages = [
                {"role": "system", "content": SCENARIO_ONLY_PROMPT},
                {"role": "user", "content": SCENARIO_ONLY_USER_PROMPT.format(profile_content=profile_content)}
            ]
            
            result = await async_get_text_completion(
                messages=messages,
                temperature=0,
                max_tokens=100,  # Scenario name is short
            )
            
            if not result:
                logger.error("Scenario generation failed: empty response from LLM")
                return None
            
            scenario_zh = result.strip()
            
            # Check if LLM returned NO_MATCH
            if scenario_zh == "NO_MATCH" or scenario_zh.upper() == "NO_MATCH":
                logger.info("LLM determined no scenario matches the profile")
                return None
            
            # Validate scenario exists in mapping
            if scenario_zh in SCENARIO_MAPPING:
                logger.info(f"LLM generated scenario: {scenario_zh}")
                return scenario_zh
            else:
                logger.warning(f"LLM returned unknown scenario: {scenario_zh}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to generate scenario from profile: {e}")
            return None
    
    @staticmethod
    async def _remove_indicators_from_profile(profile_content: str, deleted_indicators: str) -> Optional[str]:
        """
        Remove specified indicators and related content from existing profile using LLM
        
        This method is used when user deletes uploaded documents and the corresponding
        indicators need to be removed from the profile.
        
        Args:
            profile_content: Existing user profile content in Markdown format
            deleted_indicators: String describing the deleted indicators
            
        Returns:
            Updated profile with deleted indicators removed, or None on failure
        """
        if not profile_content:
            logger.warning("[remove_indicators] Cannot process: empty profile content")
            return None
        
        if not deleted_indicators:
            logger.warning("[remove_indicators] No deleted indicators provided, returning original profile")
            return profile_content
        
        try:
            logger.info(f"[remove_indicators] Starting to remove indicators from profile, indicators: {deleted_indicators[:200]}...")
            
            messages = [
                {"role": "system", "content": REMOVE_INDICATORS_PROMPT},
                {"role": "user", "content": REMOVE_INDICATORS_USER_PROMPT.format(
                    profile_content=profile_content,
                    deleted_indicators=deleted_indicators
                )}
            ]
            
            result = await async_get_text_completion(
                messages=messages,
                temperature=0,
                max_tokens=MAX_OUTPUT_TOKENS,
            )
            
            if not result:
                logger.error("[remove_indicators] Failed: empty response from LLM")
                return None
            
            # Clean possible markdown code block format
            updated_profile = _clean_markdown_code_block(result.strip())
            logger.info("[remove_indicators] Successfully generated updated profile")
            return updated_profile
                
        except Exception as e:
            logger.error(f"[remove_indicators] Failed to remove indicators from profile: {e}")
            return None
    
    @classmethod
    async def generate_user_profile(
        cls,
        user_id: str,
        basic_info: Dict[str, Any],
        doc_list: List[Dict],
        device_data: str,
        language: str = "English"
    ) -> str:
        """
        Generate complete user profile (Markdown format)
        
        Args:
            user_id: User ID
            basic_info: User basic information
            doc_list: List of health indicator data
            device_data: Device data string
            language: Generation language
            
        Returns:
            User profile in Markdown format
        """
        if not doc_list and not basic_info:
            logger.info("No data provided for profile generation")
            return ""
        
        try:
            # Get existing profile and scenario
            previous_profile, previous_scenario = await cls._get_existing_profile(user_id)
            
            # Log previous scenario info
            if previous_scenario:
                logger.info(f"Found previous scenario for user {user_id}: {previous_scenario}")
            else:
                logger.info(f"No previous scenario found for user {user_id} (first time generation or no scenario matched before)")
            
            # Format basic information
            basic_info_str = "\n".join([f"- {k}: {v}" for k, v in basic_info.items() if v])
            if not basic_info_str:
                basic_info_str = "No basic information available"
            
            # Build health data text
            if doc_list:
                indicator_text_list = [
                    f"{r['original_indicator']}: {r['start_time']} {r['value']} {r['unit'] or ''}" 
                    for r in doc_list
                ]
                
                # Process in chunks
                context_list = _split(
                    [dict(content=f"{e}") for e in indicator_text_list], 
                    lambda _: "{content}", 
                    max_tokens=MAX_TOKENS
                )
                logger.info(f"context chunks length: {len(context_list)}")
            else:
                context_list = ["No health indicator data available"]
            
            # Process each chunk in parallel, max 50 chunks
            results = await asyncio.gather(*[
                cls._generate_profile_chunk(
                    basic_info=basic_info_str,
                    health_data=context,
                    device_data=device_data,
                    previous_profile=previous_profile,
                    previous_scenario=previous_scenario,
                    language=language
                )
                for context in context_list[:50]
            ])
            
            # Merge results
            merged_result = await cls._merge_profile_results(list(results), language)
            
            return merged_result
            
        except Exception as e:
            logger.info(f"Failed to generate user profile: {e}")
            return ""


class UserProfileService:
    """User profile main service"""
    
    @classmethod
    async def create_user_profile(cls, user_id: str) -> Dict[str, Any]:
        """
        Create complete user profile
        
        Args:
            user_id: User ID
            
        Returns:
            Creation result dictionary
        """
        logger.info(f"Starting profile creation for user: {user_id}")
        
        # 1. Get version control information
        version_info = await cls._get_version_info(user_id)
        current_version = version_info['version']
        last_execute_doc_id = version_info['last_execute_doc_id']
        
        # 2. Get basic information
        basic_info = await BasicInfoService.get_user_basic_info(user_id)
        language = basic_info.get('language') or "English"
        
        # 3. Get incremental data
        doc_list = await cls._get_incremental_data(user_id, last_execute_doc_id)
        
        if not doc_list:
            logger.info(f"No incremental data found for user: {user_id}, last_execute_doc_id: {last_execute_doc_id}. Skipping profile update.")
            return {
                "status": "no_incremental_data",
                "message": "No new data to process since last update",
                "current_version": current_version,
                "last_execute_doc_id": last_execute_doc_id
            }
        
        # 4. Get device data
        device_data = await DeviceDataService.get_device_data(user_id)
        
        # 5. Generate user profile (Markdown format)
        profile_markdown = await UserProfileGenerator.generate_user_profile(
            user_id=user_id,
            basic_info=basic_info,
            doc_list=doc_list,
            device_data=device_data,
            language=language
        )
        
        if not profile_markdown:
            logger.info(f"Failed to generate profile for user: {user_id}")
            return {
                "status": "error",
                "message": "Failed to generate user profile"
            }
        
        # 6. Extract scenario from profile
        profile_without_scenario, scenario_zh = _extract_scenario_from_profile(profile_markdown)
        
        # 7. Get scenario info (English translation and image URL)
        scenario_info = None
        if scenario_zh:
            scenario_info = _get_scenario_info(scenario_zh)
            if scenario_info:
                logger.info(f"Extracted scenario for user {user_id}: {scenario_zh} -> {scenario_info['scenario_en']}")
            else:
                logger.warning(f"Scenario extracted but not found in mapping for user {user_id}: {scenario_zh}")
        
        # 8. Fallback to default scenario if no scenario matched
        if not scenario_info:
            scenario_info = _get_default_scenario_info()
            logger.info(f"Using default fallback scenario for user {user_id}: {scenario_info['scenario_zh']}")
        
        # 9. Save profile
        new_version = current_version + 1
        new_last_execute_doc_id = max([doc['id'] for doc in doc_list]) if doc_list else last_execute_doc_id
        
        profile_id = await cls._save_profile(
            user_id=user_id,
            version=new_version,
            profile_markdown=profile_without_scenario,
            last_execute_doc_id=new_last_execute_doc_id,
            scenario_zh=scenario_info['scenario_zh'] if scenario_info else None,
            scenario_en=scenario_info['scenario_en'] if scenario_info else None,
            scenario_image_url=scenario_info['scenario_image_url'] if scenario_info else None,
            action_type="add"
        )
        
        logger.info(f"Successfully created profile {profile_id} version {new_version} for user: {user_id}, last_execute_doc_id: {new_last_execute_doc_id}, action_type: add")
        
        return {
            "status": "success",
            "profile_id": profile_id,
            "version": new_version,
            "last_execute_doc_id": new_last_execute_doc_id,
            "profile_data": profile_markdown
        }
    
    @classmethod
    async def ensure_scenario_exists(cls, user_id: str) -> Dict[str, Any]:
        """
        Ensure user has a profile with a valid scenario.
        
        This method implements smart scenario management:
        1. If user has no profile: create full profile using create_user_profile()
        2. If user has profile with scenario: no action needed, return existing info
        3. If user has profile without scenario: use LLM to determine scenario, fallback to default if no match
        
        In case 3, version is incremented while common_part and last_execute_doc_id remain unchanged.
        
        Args:
            user_id: User ID
            
        Returns:
            Result dictionary containing status, profile_id, version, scenario info, etc.
        """
        logger.info(f"[ensure_scenario_exists] Starting for user: {user_id}")
        
        # Step 1: Get latest profile info
        profile_info = await cls._get_latest_profile_info(user_id)
        
        # Case 1: No existing profile - create full profile
        if profile_info is None:
            logger.info(f"[ensure_scenario_exists] No existing profile found for user {user_id}, creating full profile")
            return await cls.create_user_profile(user_id)
        
        logger.info(f"[ensure_scenario_exists] Found existing profile for user {user_id}, version: {profile_info['version']}")
        
        # Extract current profile info
        current_version = profile_info['version']
        last_execute_doc_id = profile_info['last_execute_doc_id']
        common_part = profile_info['common_part']
        existing_scenario_zh = profile_info['scenario_zh']
        
        # Case 2: Profile exists with scenario - no action needed, return existing info
        if existing_scenario_zh:
            logger.info(f"[ensure_scenario_exists] User {user_id} already has scenario: {existing_scenario_zh}, no action needed")
            scenario_info = _get_scenario_info(existing_scenario_zh)
            
            # If existing scenario is not in mapping (edge case), still return existing data
            if not scenario_info:
                logger.warning(f"[ensure_scenario_exists] Existing scenario '{existing_scenario_zh}' not found in mapping, returning existing data as-is")
                return {
                    "status": "already_exists",
                    "version": current_version,
                    "last_execute_doc_id": last_execute_doc_id,
                    "scenario_zh": existing_scenario_zh,
                    "scenario_en": profile_info.get('scenario_en'),
                    "scenario_image_url": profile_info.get('scenario_image_url'),
                    "scenario_source": "existing",
                    "profile_data": common_part
                }
            
            return {
                "status": "already_exists",
                "version": current_version,
                "last_execute_doc_id": last_execute_doc_id,
                "scenario_zh": scenario_info['scenario_zh'],
                "scenario_en": scenario_info['scenario_en'],
                "scenario_image_url": scenario_info['scenario_image_url'],
                "scenario_source": "existing",
                "profile_data": common_part
            }
        
        # Case 3: Profile exists but no scenario - use LLM to determine
        logger.info(f"[ensure_scenario_exists] User {user_id} has no scenario, using LLM to determine")
        
        # Use LLM to generate scenario based on existing profile content
        llm_scenario_zh = await UserProfileGenerator._generate_scenario_only(common_part)
        
        if llm_scenario_zh:
            scenario_info = _get_scenario_info(llm_scenario_zh)
            if scenario_info:
                logger.info(f"[ensure_scenario_exists] LLM determined scenario for user {user_id}: {llm_scenario_zh}")
            else:
                # LLM returned a scenario not in mapping - use default
                logger.warning(f"[ensure_scenario_exists] LLM scenario '{llm_scenario_zh}' not in mapping, using default")
                scenario_info = _get_default_scenario_info()
        else:
            # LLM couldn't determine a scenario - use default
            logger.info(f"[ensure_scenario_exists] LLM found no matching scenario for user {user_id}, using default fallback")
            scenario_info = _get_default_scenario_info()
        
        # Save new version with scenario (common_part and last_execute_doc_id unchanged)
        new_version = current_version + 1
        
        profile_id = await cls._save_profile(
            user_id=user_id,
            version=new_version,
            profile_markdown=common_part,  # Keep unchanged
            last_execute_doc_id=last_execute_doc_id,  # Keep unchanged
            scenario_zh=scenario_info['scenario_zh'],
            scenario_en=scenario_info['scenario_en'],
            scenario_image_url=scenario_info['scenario_image_url'],
            action_type="keep"
        )
        
        scenario_source = "llm" if llm_scenario_zh else "default"
        logger.info(f"[ensure_scenario_exists] Successfully saved profile {profile_id} version {new_version} for user {user_id}, scenario: {scenario_info['scenario_zh']} (source: {scenario_source}), action_type: keep")
        
        return {
            "status": "success",
            "profile_id": profile_id,
            "version": new_version,
            "last_execute_doc_id": last_execute_doc_id,
            "scenario_zh": scenario_info['scenario_zh'],
            "scenario_en": scenario_info['scenario_en'],
            "scenario_image_url": scenario_info['scenario_image_url'],
            "scenario_source": scenario_source,  # "llm" or "default"
            "profile_data": common_part
        }
    
    @classmethod
    async def remove_indicators_from_profile(cls, user_id: str, deleted_indicators: str) -> Dict[str, Any]:
        """
        Remove specified indicators from user profile when documents are deleted.
        
        This method is called when user deletes uploaded documents. It removes the
        corresponding indicator content from the user profile while keeping all
        other information intact.
        
        Args:
            user_id: User ID
            deleted_indicators: String describing the deleted indicators (e.g., indicator names and values)
            
        Returns:
            Result dictionary containing status, profile_id, version, etc.
        """
        logger.info(f"[remove_indicators_from_profile] Starting for user: {user_id}")
        logger.info(f"[remove_indicators_from_profile] Deleted indicators: {deleted_indicators[:500] if deleted_indicators else 'None'}...")
        
        # Step 1: Get latest profile info
        profile_info = await cls._get_latest_profile_info(user_id)
        
        # Check if profile exists
        if profile_info is None:
            logger.warning(f"[remove_indicators_from_profile] No existing profile found for user {user_id}, nothing to update")
            return {
                "status": "no_profile",
                "message": "No existing profile found for user"
            }
        
        logger.info(f"[remove_indicators_from_profile] Found existing profile for user {user_id}, version: {profile_info['version']}")
        
        # Extract current profile info
        current_version = profile_info['version']
        last_execute_doc_id = profile_info['last_execute_doc_id']
        common_part = profile_info['common_part']
        scenario_zh = profile_info['scenario_zh']
        scenario_en = profile_info['scenario_en']
        scenario_image_url = profile_info['scenario_image_url']
        
        # Check if profile content exists
        if not common_part:
            logger.warning(f"[remove_indicators_from_profile] Profile content is empty for user {user_id}")
            return {
                "status": "empty_profile",
                "message": "Profile content is empty"
            }
        
        # Step 2: Use LLM to remove indicators from profile
        updated_profile = await UserProfileGenerator._remove_indicators_from_profile(
            profile_content=common_part,
            deleted_indicators=deleted_indicators
        )
        
        if not updated_profile:
            logger.error(f"[remove_indicators_from_profile] Failed to generate updated profile for user {user_id}")
            return {
                "status": "error",
                "message": "Failed to generate updated profile"
            }
        
        # Step 3: Save new version (only common_part changes, other fields remain unchanged)
        new_version = current_version + 1
        
        profile_id = await cls._save_profile(
            user_id=user_id,
            version=new_version,
            profile_markdown=updated_profile,  # Updated profile with indicators removed
            last_execute_doc_id=last_execute_doc_id,  # Keep unchanged
            scenario_zh=scenario_zh,  # Keep unchanged
            scenario_en=scenario_en,  # Keep unchanged
            scenario_image_url=scenario_image_url,  # Keep unchanged
            action_type="delete"
        )
        
        logger.info(f"[remove_indicators_from_profile] Successfully saved updated profile {profile_id} version {new_version} for user {user_id}, action_type: delete")
        
        return {
            "status": "success",
            "profile_id": profile_id,
            "version": new_version,
            "last_execute_doc_id": last_execute_doc_id,
            "scenario_zh": scenario_zh,
            "scenario_en": scenario_en,
            "scenario_image_url": scenario_image_url,
            "profile_data": updated_profile
        }
    
    @staticmethod
    async def _get_version_info(user_id: str) -> Dict[str, int]:
        """Get version control information"""
        sql = """
        select version, last_execute_doc_id
        from theta_ai.health_user_profile_by_system
        where user_id = :user_id
        and is_deleted = false
        order by version desc
        limit 1
        """
        
        results = await execute_query(
            sql,
            params={"user_id": user_id},
        )
        
        if results:
            return {
                "version": results[0]['version'],
                "last_execute_doc_id": results[0]['last_execute_doc_id']
            }
        else:
            return {
                "version": 0,
                "last_execute_doc_id": -1
            }
    
    @staticmethod
    async def _get_latest_profile_info(user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get user's latest profile complete information
        
        Args:
            user_id: User ID
            
        Returns:
            Dictionary containing version, last_execute_doc_id, common_part, 
            scenario_zh, scenario_en, scenario_image_url, or None if not found
        """
        sql = """
        select version, last_execute_doc_id, common_part, 
               scenario_zh, scenario_en, scenario_image_url
        from theta_ai.health_user_profile_by_system
        where user_id = :user_id
        and is_deleted = false
        order by version desc
        limit 1
        """
        
        results = await execute_query(
            sql,
            params={"user_id": user_id},
        )
        
        if results:
            return {
                "version": results[0]['version'],
                "last_execute_doc_id": results[0]['last_execute_doc_id'],
                "common_part": results[0]['common_part'],
                "scenario_zh": results[0]['scenario_zh'],
                "scenario_en": results[0]['scenario_en'],
                "scenario_image_url": results[0]['scenario_image_url']
            }
        else:
            return None
    
    @staticmethod
    async def _get_incremental_data(user_id: str, last_execute_doc_id: int) -> List[Dict]:
        """Get incremental data"""
        sql = """
        select
            data.id, data.value, data.start_time,
            dim.original_indicator, dim.unit
        from theta_ai.th_series_dim as dim
        join theta_ai.th_series_data as data
        on data.indicator = dim.original_indicator
        where data.user_id = :user_id
        and data.id > :last_execute_doc_id
        and data.source_table in ('chat', 'th_messages', 'theta_ai.th_messages', 'apple_health_cda', 'excel', 'health_data_epic', 'health_data_oracle')
        and data.deleted = 0
        order by data.id asc
        """
        
        results = await execute_query(
            sql,
            params={
                "user_id": user_id,
                "last_execute_doc_id": last_execute_doc_id
            },
        )
        
        return results or []
    
    @staticmethod
    async def _save_profile(
        user_id: str, 
        version: int, 
        profile_markdown: str, 
        last_execute_doc_id: int,
        scenario_zh: Optional[str] = None,
        scenario_en: Optional[str] = None,
        scenario_image_url: Optional[str] = None,
        action_type: str = "add"
    ) -> int:
        """
        Save user profile
        
        Args:
            user_id: User ID
            version: Profile version
            profile_markdown: Profile content in Markdown format
            last_execute_doc_id: Last executed document ID
            scenario_zh: Chinese scenario name
            scenario_en: English scenario name
            scenario_image_url: Scenario image URL
            action_type: Action type - "add" (new data), "delete" (deleted data), "keep" (scenario only, no profile update)
            
        Returns:
            Profile record ID
        """
        sql = """
        insert into theta_ai.health_user_profile_by_system
        (user_id, version, name, last_execute_doc_id, common_part, scenario_zh, scenario_en, scenario_image_url, action_type, is_deleted)
        values (:user_id, :version, :name, :last_execute_doc_id, :common_part, :scenario_zh, :scenario_en, :scenario_image_url, :action_type, :is_deleted)
        returning id
        """
        
        result = await execute_query(
            sql,
            params={
                "user_id": user_id,
                "version": version,
                "name": "physical_system",
                "last_execute_doc_id": last_execute_doc_id,
                "common_part": profile_markdown,
                "scenario_zh": scenario_zh,
                "scenario_en": scenario_en,
                "scenario_image_url": scenario_image_url,
                "action_type": action_type,
                "is_deleted": False
            },
        )
        
        return result.get("id")
