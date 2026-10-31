import asyncio
import logging
import re
from datetime import date, timedelta
from typing import Any


from mirobody.collect.observations import (
    KIND_CONDITION,
    KIND_FINDING,
    KIND_MEASUREMENT,
    KIND_NOTE,
    KIND_SYMPTOM,
    MODALITY_LAB,
    MODALITY_MANUAL,
    MODALITY_SELF,
    MODALITY_UNVERIFIED,
)
from mirobody.collect.query import PostgresHealthQuery
from mirobody.kernel.ops import is_driver_exception
from mirobody.standardize.parse import parse_range
from mirobody.utils import execute_query
from mirobody.utils.llm import async_get_text_completion
from mirobody.utils.config import safe_read_cfg

from .user import age_from_birth

logger = logging.getLogger(__name__)


def _extract_core_section(common_part: str, maxlen: int) -> str:
    """Pull the curated, durable '核心摘要 / Core Summary' section out of a profile.

    The profile generator emits this as the FIRST section (see
    `_core_summary`). We inject ONLY this section so the system
    prompt stays short and stable between profile refreshes (prompt-cache
    friendly): the volatile lab time-series lives in the detail, which the
    agent reads on demand from ``/memories/health_profile.md``.

    Returns the core section text (capped at ``maxlen``). Falls back to a
    plain truncation of the whole profile if the marker isn't found (older
    profiles generated before this change).
    """
    text = str(common_part or "").strip()
    if not text:
        return ""
    # Find the core-summary heading (bilingual; tolerant of '### 0.' numbering).
    lower = text.lower()
    start = -1
    for marker in ("核心摘要", "core summary"):
        idx = lower.find(marker.lower())
        if idx != -1:
            start = idx
            break
    if start == -1:
        return text[:maxlen]  # fallback: legacy profile without a core section
    # Begin at the heading line, end at the next markdown heading ('### ' / '## ').
    head_line_start = text.rfind("\n", 0, start) + 1
    body_start = text.find("\n", start)
    if body_start == -1:
        return text[head_line_start:][:maxlen]
    rest = text[body_start + 1:]
    end_rel = len(rest)
    for sep in ("\n## ", "\n### "):
        i = rest.find(sep)
        if i != -1:
            end_rel = min(end_rel, i)
    section = (text[head_line_start:body_start + 1] + rest[:end_rel]).strip()
    return section[:maxlen]


async def get_health_profile_core(user_id: str, maxlen: int = 2000) -> str | None:
    """Bounded, decrypted health-profile CORE for system-prompt injection.

    Returns the curated '核心摘要 / Core Summary' section of the latest profile
    (capped at ``maxlen``), or None when the user has no profile yet. The full
    detailed profile is mirrored to ``/memories/health_profile.md`` for the
    agent to read on demand: keeping the injected core short + stable so it
    stays prompt-cache friendly between profile refreshes.
    """
    if not user_id:
        return None
    try:
        # Query inlined from the deleted chat/user.py `fetch_system_user_profile`,
        # whose return dict named the decrypted text `medical_history` while this
        # caller read `common_part`, so the lookup ALWAYS came back empty and no
        # <health_profile> block ever reached the system prompt. Selecting the
        # column directly removes the renaming hop that hid the mismatch.
        rows = await execute_query(
            """
            select decrypt_content(common_part_encrypted) as common_part
            from health_user_profile_by_system
            where user_id = :user_id and is_deleted = false
            order by version desc limit 1
            """,
            params={"user_id": user_id},
        )
    except Exception as e:
        # This swallowed silently. Every agent turn calls it to put the
        # user's health context into the system prompt, so a failure here
        # degrades every answer the user gets, and left no trace explaining why.
        logger.error("health profile read failed: user_id=%s error_type=%s", user_id, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return None
    if not rows:
        return None
    core = _extract_core_section(rows[0].get("common_part") or "", maxlen)
    return core or None


#: Estimated tokens of the digest the scenario is picked from.
DIGEST_TOKENS = 10000
#: What the scenario answer may write: one line, "场景：<name>", which
#: MiniCPM5-2B wrote in 4-12 tokens for each of 17 refreshes (2026-10-31). The
#: rest is room for the entries that reason at `low`; a cut answer names no
#: scenario, and the previous one stays.
SCENARIO_MAX_TOKENS = 1024
#: How long the scenario answer may take before the previous scenario stays.
#: Inside the refresh's 600 s, a router whose slots were busy with chat turns
#: could hold the request until the refresh timed out and the record, which
#: needs no model, was not stored. MiniCPM5-2B answered in 15 s with the model
#: load on a fresh GPU router (2026-10-31), and the CPU preset runs 4.2 times
#: slower (2026-10-30); half the refresh leaves the rest a margin it needs
#: about a second of.
SCENARIO_TIMEOUT_SECONDS = 300
#: How far back the profile looks: the "past 3 years" the model was told to keep.
HISTORY_DAYS = 3 * 365
#: Values listed per indicator in the document, newest first; the rest are counted.
VALUES_SHOWN = 10
#: Items on one line of the core summary; the rest are counted.
CORE_ITEMS = 6


def _estimated_tokens(text: str) -> int:
    """A deliberate over-estimate: ~4 chars/token for Latin text, ~2 once the
    text contains CJK. It sizes the scenario digest against DIGEST_TOKENS,
    where an over-count leaves an in-range reading out and an under-count
    overruns a small local model's context. This was tiktoken behind a lazy import (`utils/truncate.py`); its
    BPE file downloads from an OpenAI CDN that is unreachable from exactly the
    networks a gateway deployment exists for, and the estimate does the same job.
    """
    if not text or text.isspace():
        return 0
    divisor = 2 if any(ord(ch) > 0x2E80 for ch in text) else 4
    return max(1, len(text) // divisor)


PROFILE_LOCK_TIMEOUT_SECONDS = 600  # 10 minutes lock timeout for profile generation
PROFILE_LOCK_WAIT_TIMEOUT_SECONDS = 120  # Maximum wait time to acquire lock (2 minutes)
PROFILE_LOCK_RETRY_INTERVAL_SECONDS = 2  # Retry interval when waiting for lock

#-----------------------------------------------------------------------------
# Profile generation must remain serialized across server instances.
# The provider lock manager holds a Postgres session lock until release;
# a crashed process closes that session and frees the lock.
from mirobody.utils.distributed_lock import PullTaskLockManager


class UserProfileLockManager:
    def __init__(self):
        self._locks = PullTaskLockManager()

    async def try_acquire_lock(
        self, user_id: str, timeout_seconds: int = PROFILE_LOCK_TIMEOUT_SECONDS
    ) -> str | None:
        del timeout_seconds
        return await self._locks.try_acquire_execution_lock(f"profile:{user_id}")

    async def acquire_lock_with_wait(
        self,
        user_id: str,
        timeout_seconds: int = PROFILE_LOCK_TIMEOUT_SECONDS,
        wait_timeout_seconds: int = PROFILE_LOCK_WAIT_TIMEOUT_SECONDS,
        retry_interval_seconds: int = PROFILE_LOCK_RETRY_INTERVAL_SECONDS,
    ) -> str | None:
        start = asyncio.get_running_loop().time()
        while True:
            lock_id = await self.try_acquire_lock(user_id, timeout_seconds)
            if lock_id:
                return lock_id
            if asyncio.get_running_loop().time() - start >= wait_timeout_seconds:
                logger.warning("profile lock wait expired: user_id=%s", user_id)
                return None
            await asyncio.sleep(retry_interval_seconds)

    async def release_lock(self, user_id: str, lock_id: str) -> bool:
        return await self._locks.release_execution_lock(f"profile:{user_id}", lock_id)


# Global lock manager instance
user_profile_lock_manager = UserProfileLockManager()

#-----------------------------------------------------------------------------



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

#: The model picks the scenario and writes nothing else that is stored. Asked
#: to rewrite the whole profile, MiniCPM5-2B on 2026-10-30 carried every
#: reading of three years through its own output: it filed a urine glucose
#: under 血糖, wrote "无记录" for panels the person never had and looped on them
#: to the output cap, and opened two profiles with the prompt's instructions.
#: Asked for the summary alone, it still filed symptoms as conditions and left
#: out sex and age (2026-10-31). What a record says, code writes.
SCENARIO_PROMPT = f"""
Pick the one health-management scenario from the table below that the person's data clearly supports, by the rules below it.

Answer with one line and nothing else: "场景：" followed by the scenario exactly as the table writes it, or "场景：无" when none fits.

## Scenario Criteria Table
{SCENARIO_CRITERIA_TABLE}
## Scenario Selection Rules
{SCENARIO_SELECTION_RULES_BASE}
4. **Previous Scenario Continuity**: if the data names a previous scenario and still meets its criteria, keep it. Change it only for a new diagnosis, a major improvement or deterioration, or a more urgent risk.
5. **No Match**: if the data is insufficient or matches no scenario, answer "场景：无".
"""

def _is_chinese(lang: str | None) -> bool:
    return bool(lang) and ("zh" in lang.lower() or "cn" in lang.lower() or lang.lower() == "chinese")

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


def _get_default_scenario_info() -> dict[str, str]:
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
    # No scenario found
    return profile_markdown.strip(), ""


def _scenario_in(answer: str) -> str | None:
    """The scenario a "场景：<name>" line names, "" when it names none, and
    None when the answer has no such line (the call failed or was cut).

    A line of its own, not the document's "## 场景" heading: asked for the
    heading, an empty line and the name, MiniCPM5-2B wrote the heading and
    stopped, for each of five people on 2026-10-31."""
    marks = list(re.finditer(r"^[#*\s]*场景\s*[:：]?", answer or "", re.M))
    if not marks:
        return None
    rest = answer[marks[-1].end():]
    name = next((line.strip(" *：:") for line in rest.splitlines() if line.strip()), "")
    return "" if name in ("", "无", "None", "none") else name


def _get_scenario_info(scenario_zh: str) -> dict[str, str] | None:
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
    # The scenario names a health condition, so the log says only that it missed.
    logger.warning("profile scenario not in the mapping")
    return None


class BasicInfoService:
    """User basic information service"""
    
    @staticmethod
    def _convert_gender_to_text(gender: int | None, lang: str | None) -> str | None:
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
        
        if _is_chinese(lang):
            gender_map = {0: '未知', 1: '男', 2: '女'}
        else:
            gender_map = {0: 'Unknown', 1: 'Male', 2: 'Female'}
        
        return gender_map.get(gender, '未知' if _is_chinese(lang) else 'Unknown')
    
    @staticmethod
    async def get_user_basic_info(user_id: str) -> dict[str, Any]:
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
        from health_app_user
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
                "age": age_from_birth(birth),
                "language": lang
            }
        logger.info(f"No basic info found for user: {user_id}")
        return {}


NO_DEVICE_DATA = "No device data available"


class DeviceDataService:
    """Device data service"""
    
    @staticmethod
    async def get_device_data(user_id: str) -> str:
        """
        Get recent device data starting with rolling_7d from the observation model, return formatted string
        
        Args:
            user_id: User ID
            
        Returns:
            Formatted device data string
        """
        sql = """
        select distinct on (name_text) name_text as indicator, value_text as value,
               to_char(observed_start, 'YYYY-MM-DD HH24:MI:SS') as start_time
        from v_observation
        where user_id = :user_id
        and name_text like 'rolling_7d%'
        order by name_text, observed_start desc
        """
        
        results = await execute_query(
            sql,
            params={"user_id": user_id},
        )
        
        if not results:
            return NO_DEVICE_DATA
        
        device_lines = []
        for result in results:
            indicator = result['indicator'].replace('rolling_7d_', '')
            value = result['value']
            start_time = result['start_time']
            device_lines.append(f"- {indicator}: {value} (as of {start_time})")
        
        return "\n".join(device_lines)


#-----------------------------------------------------------------------------
# What the record says, rendered by code, newest first.

_FINDINGS = frozenset({KIND_CONDITION, KIND_FINDING})
_REPORTED = frozenset({KIND_SYMPTOM, KIND_NOTE})


def _grouped(records: list[dict], kinds: frozenset[str]) -> list[list[dict]]:
    """Rows of `kinds` per series, newest first, in the order of each
    series' newest row (`records` is newest first). A note is grouped by its
    words: every note is filed in one series (`standardize.code_note`), and
    grouped by series a person's notes became one line, the newest note's
    words with every other note's dates."""
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in records:
        if row.get("kind") in kinds:
            key = (("note", row.get("name") or "") if row.get("kind") == KIND_NOTE
                   else ("series", row.get("series") or row.get("name") or ""))
            groups.setdefault(key, []).append(row)
    return list(groups.values())


def _reading(row: dict, *, flag: bool = True) -> str:
    """A value as stored, with the flag the report printed beside it."""
    text = " ".join(part for part in (row.get("value"), row.get("unit")) if part) or row.get("text") or ""
    if flag and row.get("flag"):
        text += f" ({row['flag']})"
    if row.get("modality") == MODALITY_UNVERIFIED:
        text += " (unverified)"
    return text


def _out_of_range(row: dict) -> str:
    """`high` or `low` as printed, else as the printed range reads the value, else empty."""
    if row.get("flag"):
        return row["flag"]
    low, high = parse_range(row.get("ref") or "")
    value = row.get("value_num")
    if value is None:
        return ""
    if high is not None and float(value) > high:
        return "high"
    if low is not None and float(value) < low:
        return "low"
    return ""


def _finding(row: dict) -> str:
    """A finding's name and what was found, once when the name already says it."""
    name, text = row.get("name") or "", _reading(row, flag=False)
    return name if text.casefold() in name.casefold() else f"{name}: {text}"


def _notable_findings(records: list[dict]) -> list[dict]:
    """The newest row of each condition, and of each finding the report marked
    abnormal. One marked normal says nothing a summary needs: of 21 findings
    on one 2026 check-up, two were not ("Mild fatty liver", "Renal cyst")."""
    return [rows[0] for rows in _grouped(records, _FINDINGS)
            if rows[0].get("kind") == KIND_CONDITION or rows[0].get("flag")]


def _finding_line(row: dict) -> str:
    flag = f" ({row['flag']})" if row.get("flag") else ""
    return f"- {_finding(row)}{flag} ({row.get('date')})"


def _series_lines(rows: list[dict]) -> list[str]:
    name, ref = rows[0].get("name") or "", rows[0].get("ref")
    head = f"- {name} (ref {ref})" if ref else f"- {name}"
    if len(rows) == 1:
        return [f"{head}: {_reading(rows[0])} ({rows[0].get('date')})"]
    lines = [f"{head}:"] + [f"  - {row.get('date')}: {_reading(row)}" for row in rows[:VALUES_SHOWN]]
    if len(rows) > VALUES_SHOWN:
        lines.append(f"  - {len(rows) - VALUES_SHOWN} earlier")
    return lines


def _reported_line(rows: list[dict]) -> str:
    dates = ", ".join(str(row.get("date")) for row in rows[:VALUES_SHOWN])
    more = f" and {len(rows) - VALUES_SHOWN} earlier" if len(rows) > VALUES_SHOWN else ""
    return f"- {rows[0].get('name')}: {dates}{more}"


def _render_records(basic_info: dict[str, Any], records: list[dict], device_data: str) -> str:
    """Every section of the profile after the core summary. The headings are
    the ones the model used to be told to write."""
    sections = []
    basics = [f"- {k}: {v}" for k, v in basic_info.items() if v]
    if basics:
        sections.append("### 1. 用户基础信息 / Basic Information\n" + "\n".join(basics))
    readings = [line for rows in _grouped(records, frozenset({KIND_MEASUREMENT})) for line in _series_lines(rows)]
    if readings:
        sections.append("### 2. 检验指标 / Lab Test Results\n" + "\n".join(readings))
    findings = [line for rows in _grouped(records, _FINDINGS)
                for line in (_series_lines(rows) if len(rows) > 1 else [_finding_line(rows[0])])]
    if findings:
        sections.append("### 3. 既往史与检查所见 / Conditions and Findings\n" + "\n".join(findings))
    reported = [_reported_line(rows) for rows in _grouped(records, _REPORTED)]
    if reported:
        sections.append("### 4. 自述症状 / Reported Symptoms and Notes\n" + "\n".join(reported))
    if device_data and device_data != NO_DEVICE_DATA:
        sections.append("### 5. 近一周设备数据 / Recent Device Data (Past 7 Days)\n" + device_data)
    return "\n\n".join(sections)


#: The core summary's words, in the person's language when it is Chinese.
_CORE_WORDS = {
    "zh": {"years": "{}岁", "blood": "血型{}", "findings": "疾病与异常检查所见", "range": "超出参考范围",
           "symptoms": "反复出现的症状", "high": "偏高", "low": "偏低", "more": "另有 {} 项见下文",
           "times": "{}次，最近{}", "sep": "；", "comma": "，"},
    "en": {"years": "{} years", "blood": "blood type {}", "findings": "Conditions and abnormal findings",
           "range": "Outside reference range", "symptoms": "Recurring symptoms", "high": "high", "low": "low",
           "more": "{} more below", "times": "{} times, last {}", "sep": "; ", "comma": ", "},
}


def _core_line(label: str, items: list[str], words: dict[str, str]) -> str:
    shown = items[:CORE_ITEMS]
    if len(items) > CORE_ITEMS:
        shown.append(words["more"].format(len(items) - CORE_ITEMS))
    return f"- {label}: " + words["sep"].join(shown)


def _core_summary(basic_info: dict[str, Any], records: list[dict], language: str) -> str:
    """The section `get_health_profile_core` puts in every system prompt: who
    the person is, then what is abnormal or recurring, newest first, each
    line cut at CORE_ITEMS. Written by code, so it changes only when the
    record does and never says a value is high that the report did not."""
    words = _CORE_WORDS["zh" if _is_chinese(language) else "en"]
    who = [str(basic_info["gender"])] if basic_info.get("gender") else []
    if basic_info.get("age") is not None:
        who.append(words["years"].format(basic_info["age"]))
    if basic_info.get("blood_type"):
        who.append(words["blood"].format(basic_info["blood_type"]))
    lines = [f"- {words['comma'].join(who)}"] if who else []
    findings = [f"{_finding(row)} ({row.get('date')})" for row in _notable_findings(records)]
    if findings:
        lines.append(_core_line(words["findings"], findings, words))
    out = []
    for rows in _grouped(records, frozenset({KIND_MEASUREMENT})):
        side = _out_of_range(rows[0])
        if side:
            out.append(f"{rows[0].get('name')} {_reading(rows[0], flag=False)} {words.get(side, side)}"
                       f" ({rows[0].get('date')})")
    if out:
        lines.append(_core_line(words["range"], out, words))
    recurring = sorted((rows for rows in _grouped(records, frozenset({KIND_SYMPTOM})) if len(rows) > 1),
                       key=len, reverse=True)
    if recurring:
        items = [f"{rows[0].get('name')} ({words['times'].format(len(rows), rows[0].get('date'))})"
                 for rows in recurring]
        lines.append(_core_line(words["symptoms"], items, words))
    return "\n".join(lines)


def _digest(basic_info: dict[str, Any], records: list[dict], device_data: str,
            previous_scenario: str | None) -> tuple[str, int]:
    """What the scenario is picked from: each series' newest value, those
    outside their range first, within DIGEST_TOKENS. Returns the digest and
    how many in-range readings it left out, which it also says."""
    basics = [f"- {k}: {v}" for k, v in basic_info.items() if v] or ["- none on record"]
    flagged, other = [], []
    for rows in _grouped(records, frozenset({KIND_MEASUREMENT})):
        row = rows[0]
        line = f"- {row.get('name')}: {_reading(row, flag=False)} ({row.get('date')})"
        side = _out_of_range(row)
        if side:
            flagged.append(f"{line}: {side}" + (f", range {row['ref']}" if row.get("ref") else ""))
        else:
            other.append(line)
    findings = [f"- {_finding(row)} ({row.get('date')})" for row in _notable_findings(records)]
    reported = [f"- {rows[0].get('name')}: {len(rows)} time(s), last {rows[0].get('date')}"
                for rows in _grouped(records, _REPORTED)]
    parts = [("Person", basics), ("Readings outside their reference range (newest value)", flagged),
             ("Conditions, and findings the report marked abnormal", findings),
             ("Symptoms and notes the person reported", reported),
             ("Device data, past 7 days", [device_data] if device_data and device_data != NO_DEVICE_DATA else []),
             ("Previous scenario", [previous_scenario] if previous_scenario else [])]
    head = "\n\n".join(f"## {title}\n" + "\n".join(lines) for title, lines in parts if lines)
    budget = DIGEST_TOKENS - _estimated_tokens(head)
    kept = []
    for line in other:
        budget -= _estimated_tokens(line) + 1
        if budget < 0:
            break
        kept.append(line)
    left_out = len(other) - len(kept)
    if kept:
        tail = "\n".join(kept) + (f"\n- and {left_out} more" if left_out else "")
        head += "\n\n## Readings within their reference range, or with none printed (newest value)\n" + tail
    return head, left_out


class UserProfileGenerator:
    """User profile generation service"""

    @staticmethod
    async def _get_previous_scenario(user_id: str) -> str | None:
        """The scenario of the current profile, so a refresh keeps it while the
        data still meets its criteria. The previous document itself is not read:
        a rewrite that reads its own output keeps its own errors
        (`kernel.memory`), and the 2026-10-30 echoes were carried that way."""
        rows = await execute_query(
            """
            select scenario_zh from health_user_profile_by_system
            where user_id = :user_id and is_deleted = false
            order by version desc limit 1
            """,
            params={"user_id": user_id},
        )
        return (rows[0].get("scenario_zh") or None) if rows else None

    @staticmethod
    async def _pick_scenario(digest: str) -> str | None:
        """The model's scenario for `digest`, as `_scenario_in` reads its answer,
        or None when none came within SCENARIO_TIMEOUT_SECONDS. A server that
        keeps writing after the call is dropped stops at SCENARIO_MAX_TOKENS."""
        try:
            async with asyncio.timeout(SCENARIO_TIMEOUT_SECONDS):
                answer = await async_get_text_completion(
                    messages=[{"role": "system", "content": SCENARIO_PROMPT}, {"role": "user", "content": digest}],
                    temperature=0,
                    max_tokens=SCENARIO_MAX_TOKENS,
                )
        except TimeoutError:
            seconds = SCENARIO_TIMEOUT_SECONDS
            logger.warning("profile scenario timed out: seconds=%d", seconds)
            return None
        return _scenario_in(answer or "")

    @classmethod
    async def generate_user_profile(
        cls,
        user_id: str,
        basic_info: dict[str, Any],
        records: list[dict],
        device_data: str,
        language: str = "English",
    ) -> str:
        """The profile document: the core summary and the record, written by
        code, and the scenario the model picks. When the model answers with
        no scenario, the previous one stays: the record is stored either way."""
        if not records and not basic_info:
            logger.info("No data provided for profile generation")
            return ""
        previous_scenario = scenario_zh = None
        try:
            previous_scenario = await cls._get_previous_scenario(user_id)
            digest, left_out_count = _digest(basic_info, records, device_data, previous_scenario)
            if left_out_count:
                logger.warning("profile digest over budget: user_id=%s readings_left_out=%d", user_id, left_out_count)
            scenario_zh = await cls._pick_scenario(digest)
        except Exception as e:
            logger.error("profile scenario failed: user_id=%s error_type=%s", user_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))
        if scenario_zh is None:
            logger.warning("profile scenario not picked, previous kept: user_id=%s", user_id)
            scenario_zh = previous_scenario
        parts = ["### 0. 核心摘要 / Core Summary\n" + _core_summary(basic_info, records, language),
                 _render_records(basic_info, records, device_data)]
        if scenario_zh:
            parts.append(f"## 场景\n\n{scenario_zh}")
        return "\n\n".join(part for part in parts if part)


class UserProfileService:
    """User profile main service"""
    
    @classmethod
    async def create_user_profile(cls, user_id: str) -> dict[str, Any]:
        """
        Create complete user profile
        
        This method uses distributed locking to ensure serialized execution
        when multiple documents are uploaded simultaneously for the same user.
        
        Args:
            user_id: User ID
            
        Returns:
            Creation result dictionary
        """
        logger.info(f"Starting profile creation for user: {user_id}")
        
        # Acquire distributed lock to prevent concurrent profile updates
        lock_id = await user_profile_lock_manager.acquire_lock_with_wait(user_id)
        if lock_id is None:
            logger.error(f"Failed to acquire lock for user {user_id} after waiting, aborting profile creation")
            return {
                "status": "error",
                "message": "Failed to acquire lock for profile update, please try again later"
            }
        
        try:
            # The lock is Postgres's session lock and has no expiry of its own,
            # and the task queue keeps this row's lease alive while it runs: a
            # model call that never returns would hold both for good. This is
            # the 600 s the lock's TTL used to give it.
            async with asyncio.timeout(PROFILE_LOCK_TIMEOUT_SECONDS):
                return await cls._create_user_profile_locked(user_id)
        except TimeoutError:
            # Not "error", so not retried: the model's part stops at
            # SCENARIO_TIMEOUT_SECONDS, so what ran past this is the rest of
            # the refresh, and an immediate retry would hold the person's lock
            # for another 600 s on the same work. The readings stay
            # unprocessed, and the next ingest's refresh includes them.
            seconds = PROFILE_LOCK_TIMEOUT_SECONDS
            logger.error("profile creation timed out: user_id=%s seconds=%d", user_id, seconds)
            return {"status": "timed_out", "message": "Profile generation took too long."}
        finally:
            # Always release lock
            await user_profile_lock_manager.release_lock(user_id, lock_id)

    @classmethod
    async def _create_user_profile_locked(cls, user_id: str) -> dict[str, Any]:
        """`create_user_profile`'s work, run while it holds the user's lock."""
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

        # 4. Get device data and everything on record, not only what is new
        device_data = await DeviceDataService.get_device_data(user_id)
        records = await cls._get_records(user_id)

        # 5. Generate user profile (Markdown format)
        profile_markdown = await UserProfileGenerator.generate_user_profile(
            user_id=user_id,
            basic_info=basic_info,
            records=records,
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
            if not scenario_info:
                logger.warning("profile scenario extracted but not in the mapping: user_id=%s", user_id)

        # 8. Fallback to default scenario if no scenario matched
        if not scenario_info:
            scenario_info = _get_default_scenario_info()
            logger.info("profile scenario defaulted: user_id=%s", user_id)

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
    
    @staticmethod
    async def _get_version_info(user_id: str) -> dict[str, int]:
        """Get version control information"""
        sql = """
        select version, last_execute_doc_id
        from health_user_profile_by_system
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
        return {
            "version": 0,
            "last_execute_doc_id": -1
        }
    
    @staticmethod
    async def _get_records(user_id: str) -> list[dict]:
        """Every visible entry a report, the person or an API wrote, newest
        first, through the read authority the tools and the Data page use. A
        device's stream is the device section."""
        page = await PostgresHealthQuery(reported=True).records(
            user_id, kind="all", modalities=[MODALITY_LAB, MODALITY_SELF, MODALITY_MANUAL, MODALITY_UNVERIFIED],
            limit=None,
        )
        # A diagnosis is history however old; a reading, a finding or a
        # symptom is kept for the window.
        since = (date.today() - timedelta(days=HISTORY_DAYS)).isoformat()
        return [row for row in page["rows"] if row.get("kind") == KIND_CONDITION or str(row.get("date")) >= since]

    @staticmethod
    async def _get_incremental_data(user_id: str, last_execute_doc_id: int) -> list[dict]:
        """Get incremental data"""
        sql = """
        select
            o.id, o.value_text as value, o.observed_start as start_time,
            o.name_text as original_indicator, o.unit_text as unit
        from v_observation o
        where o.user_id = :user_id
        and o.id > :last_execute_doc_id
        and o.source_kind in ('file', 'api', 'manual')
        order by o.id asc
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
        scenario_zh: str | None = None,
        scenario_en: str | None = None,
        scenario_image_url: str | None = None,
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
        insert into health_user_profile_by_system
        (user_id, version, name, last_execute_doc_id, common_part_encrypted, scenario_zh, scenario_en, scenario_image_url, action_type, is_deleted)
        values (:user_id, :version, :name, :last_execute_doc_id, encrypt_content(:common_part), :scenario_zh, :scenario_en, :scenario_image_url, :action_type, :is_deleted)
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
        
        return result[0]["id"] if result else None
