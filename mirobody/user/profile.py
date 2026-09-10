import asyncio
import logging
import re
import uuid
from typing import Any
from datetime import datetime, date

import redis.asyncio

from ..utils import execute_query
from ..utils.llm import async_get_text_completion
from ..utils.llm_output import strip_code_fence
from ..utils.config import safe_read_cfg, global_config

logger = logging.getLogger(__name__)


def _extract_core_section(common_part: str, maxlen: int) -> str:
    """Pull the curated, durable '核心摘要 / Core Summary' section out of a profile.

    The profile generator emits this as the FIRST section (see
    GENERATE_USER_PROFILE_PROMPT). We inject ONLY this section so the system
    prompt stays short and stable between profile refreshes (prompt-cache
    friendly) — the volatile lab time-series lives in the detail, which the
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
    agent to read on demand — keeping the injected core short + stable so it
    stays prompt-cache friendly between profile refreshes.
    """
    if not user_id:
        return None
    try:
        # Query inlined from the deleted chat/user.py `fetch_system_user_profile`,
        # whose return dict named the decrypted text `medical_history` while this
        # caller read `common_part` — so the lookup ALWAYS came back empty and no
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
        # degrades every answer the user gets — and left no trace explaining why.
        logger.warning("health profile unavailable for %s: %s", user_id, e, exc_info=True)
        return None
    if not rows:
        return None
    core = _extract_core_section(rows[0].get("common_part") or "", maxlen)
    return core or None


MAX_TOKENS = 10000
MAX_OUTPUT_TOKENS = 32000  # No limit on profile output length to avoid truncation
MAX_PREVIOUS_PROFILE_LENGTH = 15000  # Maximum character limit for previous profile version


def _estimated_tokens(text: str) -> int:
    """A deliberate over-estimate: ~4 chars/token for Latin text, ~2 once the
    text contains CJK. It only packs indicator lines into chunk budgets, where
    an over-count costs one more LLM call and an under-count overruns the
    context. This was tiktoken behind a lazy import (`utils/truncate.py`); its
    BPE file downloads from an OpenAI CDN that is unreachable from exactly the
    networks a gateway deployment exists for, and the estimate does the same job.
    """
    if not text or text.isspace():
        return 0
    divisor = 2 if any(ord(ch) > 0x2E80 for ch in text) else 4
    return max(1, len(text) // divisor)


def _chunk_by_budget(lines: list[str], budget: int) -> list[str]:
    """Greedily join lines into newline-separated chunks of at most `budget`
    estimated tokens. A single line over budget gets a chunk of its own; the
    previous packer silently dropped it."""
    chunks: list[list[str]] = []
    current: list[str] = []
    used = 0
    for line in lines:
        cost = _estimated_tokens(line) + 1
        if current and used + cost > budget:
            chunks.append(current)
            current, used = [], 0
        current.append(line)
        used += cost
    if current:
        chunks.append(current)
    return ["\n".join(chunk) for chunk in chunks]
PROFILE_LOCK_TIMEOUT_SECONDS = 600  # 10 minutes lock timeout for profile generation
PROFILE_LOCK_WAIT_TIMEOUT_SECONDS = 120  # Maximum wait time to acquire lock (2 minutes)
PROFILE_LOCK_RETRY_INTERVAL_SECONDS = 2  # Retry interval when waiting for lock

#-----------------------------------------------------------------------------
# Redis Client for Profile Lock
#-----------------------------------------------------------------------------

_profile_redis_client = None

async def _get_profile_redis_client() -> redis.asyncio.Redis | None:
    """
    Get Redis client for profile lock management
    
    Returns:
        Redis async client or None if not available
    """
    global _profile_redis_client
    
    if _profile_redis_client is None:
        try:
            _profile_redis_client = await global_config().get_redis().get_async_client()
        except Exception as e:
            logger.warning(f"[ProfileLock] Failed to get Redis client: {e}")
            return None
    
    return _profile_redis_client


class UserProfileLockManager:
    """
    User Profile distributed lock manager
    
    Ensures that profile generation/update for the same user is serialized,
    preventing race conditions when multiple documents are uploaded simultaneously.
    
    Features:
    - User-based distributed locking
    - Configurable lock timeout (default 10 minutes)
    - Automatic retry with wait
    - Graceful degradation when Redis is unavailable
    """
    
    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]
    
    def _get_lock_key(self, user_id: str) -> str:
        """Get Redis key for user profile lock"""
        return f"user_profile_lock:{user_id}"
    
    def _get_lock_value(self, lock_id: str) -> str:
        """Get lock value containing instance and timestamp info"""
        timestamp = datetime.now().isoformat()
        return f"{self.instance_id}:{timestamp}:{lock_id}"
    
    async def try_acquire_lock(
        self, 
        user_id: str, 
        timeout_seconds: int = PROFILE_LOCK_TIMEOUT_SECONDS
    ) -> str | None:
        """
        Try to acquire lock for user profile operation
        
        Args:
            user_id: User ID
            timeout_seconds: Lock timeout in seconds (default 10 minutes)
            
        Returns:
            lock_id if acquired, None if failed
        """
        lock_id = str(uuid.uuid4())
        lock_key = self._get_lock_key(user_id)
        
        redis_client = await _get_profile_redis_client()
        if redis_client is None:
            logger.warning(f"[ProfileLock] Redis not available for user {user_id}, proceeding without lock")
            return lock_id  # Return lock_id to allow operation without actual lock
        
        try:
            lock_value = self._get_lock_value(lock_id)
            
            # Try to acquire lock with NX (only set if not exists)
            acquired = await redis_client.set(
                lock_key, 
                lock_value, 
                ex=timeout_seconds, 
                nx=True
            )
            
            if acquired:
                logger.info(
                    f"[ProfileLock] Lock acquired for user {user_id} "
                    f"(instance: {self.instance_id}, lock_id: {lock_id}, timeout: {timeout_seconds}s)"
                )
                return lock_id
            # Lock already held by another process
            existing_lock = await redis_client.get(lock_key)
            if existing_lock:
                lock_info = existing_lock.decode() if isinstance(existing_lock, bytes) else existing_lock
                logger.info(f"[ProfileLock] Lock already held for user {user_id}: {lock_info}")
            return None
                
        except Exception as e:
            logger.error(f"[ProfileLock] Error acquiring lock for user {user_id}: {e}")
            return None
    
    async def acquire_lock_with_wait(
        self,
        user_id: str,
        timeout_seconds: int = PROFILE_LOCK_TIMEOUT_SECONDS,
        wait_timeout_seconds: int = PROFILE_LOCK_WAIT_TIMEOUT_SECONDS,
        retry_interval_seconds: int = PROFILE_LOCK_RETRY_INTERVAL_SECONDS
    ) -> str | None:
        """
        Acquire lock with waiting and retry
        
        Args:
            user_id: User ID
            timeout_seconds: Lock timeout
            wait_timeout_seconds: Maximum time to wait for lock
            retry_interval_seconds: Interval between retry attempts
            
        Returns:
            lock_id if acquired, None if timeout
        """
        start_time = asyncio.get_event_loop().time()
        
        while True:
            lock_id = await self.try_acquire_lock(user_id, timeout_seconds)
            if lock_id:
                return lock_id
            
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= wait_timeout_seconds:
                logger.warning(
                    f"[ProfileLock] Timeout waiting for lock for user {user_id} "
                    f"after {elapsed:.1f}s"
                )
                return None
            
            logger.info(
                f"[ProfileLock] Waiting for lock for user {user_id}, "
                f"elapsed: {elapsed:.1f}s, will retry in {retry_interval_seconds}s"
            )
            await asyncio.sleep(retry_interval_seconds)
    
    async def release_lock(self, user_id: str, lock_id: str) -> bool:
        """
        Release lock for user profile operation
        
        Args:
            user_id: User ID
            lock_id: Lock ID returned from acquire_lock
            
        Returns:
            True if released successfully
        """
        redis_client = await _get_profile_redis_client()
        if redis_client is None:
            logger.warning(f"[ProfileLock] Redis not available, skip lock release for user {user_id}")
            return True
        
        lock_key = self._get_lock_key(user_id)
        
        try:
            # Get current lock to verify ownership
            current_lock = await redis_client.get(lock_key)
            if current_lock is None:
                logger.warning(f"[ProfileLock] Lock already expired for user {user_id}")
                return True
            
            current_lock_str = current_lock.decode() if isinstance(current_lock, bytes) else current_lock
            
            # Verify we own this lock
            if lock_id in current_lock_str and self.instance_id in current_lock_str:
                await redis_client.delete(lock_key)
                logger.info(f"[ProfileLock] Lock released for user {user_id} (lock_id: {lock_id})")
                return True
            logger.warning(
                f"[ProfileLock] Lock ownership mismatch for user {user_id}, "
                f"expected lock_id: {lock_id}, current: {current_lock_str}"
            )
            return False
                
        except Exception as e:
            logger.error(f"[ProfileLock] Error releasing lock for user {user_id}: {e}")
            return False


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
9. **Time-Based Indicator Management** (CRITICAL):
   - For the SAME health indicator with multiple time points, keep ALL values from the **past 3 years**
   - Sort multiple values by date in **descending order** (most recent first)
   - Do NOT let older data overwrite newer data - if existing profile has a 2025 value, uploading a 2023 report should ADD the 2023 value, NOT replace the 2025 value
   - Remove data older than 3 years from today
   - Use the multi-time-point format shown below for indicators with multiple dates

## Output Structure (Markdown)
Only output sections that have actual data. Skip sections entirely if no data is available.

### 0. 核心摘要 / Core Summary
**ALWAYS output this section FIRST.** A short, durable, high-signal snapshot of the user —
this is the only part injected into the assistant's standing context, so keep it **tight and
stable** (it should change only when major facts change, not on every routine lab update).
Include ONLY: basic demographics (sex/age/language), chronic conditions & key past history,
current medications & allergies, and the **3–6 most clinically significant** current findings
(notably abnormal values). **Hard limit: ≤ 1500 characters.** Do NOT dump full lab panels or
time-series here — those belong in the detailed sections below.

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
- 检验指标 / Lab Test Results: use multi-time-point format for indicators with multiple dates:
  ```
  - 指标名称 / Indicator Name:
    - 2025-01-15: 值 单位
    - 2024-03-20: 值 单位
    - 2023-06-10: 值 单位
  ```
  For indicators with only one time point, use simple format: `- 指标名称: 值 单位 (日期)`

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
3. **Remove Duplicates**: Eliminate redundant information while preserving unique details (same indicator + same date + same value = duplicate)
4. **Concise Output**: Keep the output concise and summarized
5. **Skip Empty Sections**: Do NOT output any section or subsection that has no data
6. **Language Consistency**: Use consistent language throughout the merged profile
7. **Time-Based Indicator Merging** (CRITICAL):
   - For the SAME health indicator appearing in multiple chunks, merge ALL time points together
   - Sort by date in **descending order** (most recent first)
   - Only keep data from the **past 3 years**
   - Use multi-time-point format for indicators with multiple dates:
     ```
     - 指标名称:
       - 2025-01-15: 值 单位
       - 2024-03-20: 值 单位
     ```

## Required Output Structure
Only output sections that have actual data:
0. **### 0. 核心摘要 / Core Summary** — ALWAYS first. A tight, durable snapshot (≤ 1500 chars):
   demographics, chronic conditions & key history, current meds & allergies, and the 3–6 most
   clinically significant current findings. Do NOT put full lab panels/time-series here.
1. 用户基础信息 / Basic Information
2. 生活方式 / Lifestyle
3. 健康情况 / Health Status (subsections: 用药史, 既往史, 家族史, 免疫接种, 月经周期, 检验指标 - only include subsections with data)
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
    # Scenario not found
    logger.warning(f"Scenario not found in mapping: {scenario_zh_clean}")
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
        
        is_chinese = lang and ('zh' in lang.lower() or 'cn' in lang.lower() or lang.lower() == 'chinese')
        
        if is_chinese:
            gender_map = {0: '未知', 1: '男', 2: '女'}
        else:
            gender_map = {0: 'Unknown', 1: 'Male', 2: 'Female'}
        
        return gender_map.get(gender, 'Unknown' if not is_chinese else '未知')
    
    @staticmethod
    def _calculate_age(birth: str | None) -> int | None:
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
                "age": BasicInfoService._calculate_age(birth),
                "language": lang
            }
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
        from th_series_data
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
    async def _get_existing_profile(user_id: str) -> tuple[str | None, str | None]:
        """
        Get user's existing health profile and scenario
        
        Args:
            user_id: User ID
            
        Returns:
            Tuple of (profile, scenario_zh): Existing health profile in Markdown format and previous scenario, or (None, None) if not found
        """
        sql = """
        select decrypt_content(common_part_encrypted) as common_part, scenario_zh
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
        previous_scenario: str | None = None,
        language: str = "English"
    ) -> str:
        """
        Generate a single chunk of user profile using LLM
        
        自动选择提供商：按 mirobody/utils/config/llm.py 注册表的 text 面顺序，取第一个有 key 的 provider。
        
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
        
        return strip_code_fence(result)
    
    @staticmethod
    async def _merge_profile_results(results: list[str], language: str = "English") -> str:
        """
        Merge multiple profile chunk results
        
        自动选择提供商：按 mirobody/utils/config/llm.py 注册表的 text 面顺序，取第一个有 key 的 provider。
        
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
        
        return strip_code_fence(result)
    
    @classmethod
    async def generate_user_profile(
        cls,
        user_id: str,
        basic_info: dict[str, Any],
        doc_list: list[dict],
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
                context_list = _chunk_by_budget(indicator_text_list, MAX_TOKENS)
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

            # Mirror the FULL detailed profile into the agent's encrypted /memories/
            # so it can read specifics on demand; the bounded Core Summary is what
            # gets injected into the system prompt. Best-effort — never blocks save.

            return {
                "status": "success",
                "profile_id": profile_id,
                "version": new_version,
                "last_execute_doc_id": new_last_execute_doc_id,
                "profile_data": profile_markdown
            }
        finally:
            # Always release lock
            await user_profile_lock_manager.release_lock(user_id, lock_id)
    
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
    async def _get_incremental_data(user_id: str, last_execute_doc_id: int) -> list[dict]:
        """Get incremental data"""
        sql = """
        select
            data.id, data.value, data.start_time,
            dim.original_indicator, dim.unit
        from th_series_dim as dim
        join th_series_data as data
        on data.indicator = dim.original_indicator
        where data.user_id = :user_id
        and data.id > :last_execute_doc_id
        and data.source_table in ('chat', 'th_messages', 'th_files', 'apple_health_cda', 'excel', 'health_data_epic', 'health_data_oracle')
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
