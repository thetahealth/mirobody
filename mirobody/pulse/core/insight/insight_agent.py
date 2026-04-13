"""
Insight Agent (Layer 2 + Layer 3)

Uses LLM to:
1. Evaluate absolute health levels (is this value normal for this person?)
2. Generate hypotheses about possible causes
3. Generate user-facing touch message with compliance check

Uses async_get_structured_output for reliable JSON responses with auto provider fallback.

Spec:
    Input:
        - detection: Optional[InsightDetection] — Layer 1 results (can be None for absolute-value-only)
        - profile: UserProfile — baselines, tags, densities
        - daily_values: DailyValues — raw indicator time series
        - user_health_profile: Optional[str] — from health_user_profile_by_system
        - past_insights: Optional[List[PastInsight]] — confirmed/denied history

    Output:
        InsightAgentResult:
            - absolute_assessment: str
            - hypotheses: List[{cause, confidence, evidence}]
            - summary: str
            - severity: str
            - touch_message: str
            - touch_compliant: bool
            - recommend_action: str
"""

import logging
from typing import Any, Dict, List, Optional

from .models import DailyValues, InsightDetection, PastInsight, UserProfile


# JSON Schema for structured output
INSIGHT_RESPONSE_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "health_insight",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "absolute_assessment": {
                    "type": "string",
                    "description": "Assessment of whether current absolute values are within healthy range for this user"
                },
                "hypotheses": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "cause": {"type": "string"},
                            "confidence": {"type": "number"},
                            "evidence": {"type": "string"}
                        },
                        "required": ["cause", "confidence", "evidence"],
                        "additionalProperties": False
                    }
                },
                "summary": {
                    "type": "string",
                    "description": "One sentence comprehensive judgment in Chinese"
                },
                "severity": {
                    "type": "string",
                    "enum": ["mild", "moderate", "severe"]
                },
                "touch_message": {
                    "type": "string",
                    "description": "User-facing message in Chinese, warm tone, no diagnosis, suggest doctor for severe cases"
                },
                "touch_compliant": {
                    "type": "boolean",
                    "description": "True if touch_message passes compliance (no diagnostic conclusions, no prescriptions)"
                },
                "recommend_action": {
                    "type": "string",
                    "description": "Recommended action for the user in Chinese"
                }
            },
            "required": [
                "absolute_assessment", "hypotheses", "summary",
                "severity", "touch_message", "touch_compliant", "recommend_action"
            ],
            "additionalProperties": False
        }
    }
}

SYSTEM_PROMPT = """You are a health data analysis expert. You analyze wearable device data and user health profiles to provide health observations.

Rules:
- Never make diagnostic conclusions ("you have X disease")
- Use uncertain language: "may indicate", "suggest monitoring", "worth paying attention to"
- For severe anomalies, suggest consulting a doctor
- Consider the user's profile (age, conditions, medications) when interpreting data
- Evaluate both personal deviation AND absolute health levels
- Respond in Chinese (中文) for all user-facing text (summary, touch_message, recommend_action)
- For touch_message: warm tone, not alarming, actionable advice
- touch_compliant must be true only if the message contains NO diagnostic conclusions and NO specific medication recommendations

Hypothesis priority guidance:
- If deviation is extreme (>5σ) or multiple indicators worsen simultaneously: prioritize acute causes (illness, injury, infection, surgery recovery, acute pain) over lifestyle causes (training load, work stress, poor sleep)
- If deviation is moderate (2-4σ): consider both acute and lifestyle causes equally
- If deviation is mild (<2σ): lifestyle causes are more likely (sleep, stress, diet changes)
- Ask the user about recent health events rather than assuming the cause
- For body composition trends (weight, BMI, body fat): these change slowly, don't overreact to small weekly changes"""

USER_PROMPT_TEMPLATE = """## 用户画像
{user_profile}

## 用户标签（系统推断）
{user_tags}

## 已确认的用户习惯（正反馈）
{confirmed_insights}

## 已否认的猜测（排除项，30天内有效）
{denied_insights}

## 当前观察（Layer 1 规则检测结果）
{observations}

## 指标绝对值（近期数据）
{absolute_values}

## 任务

基于以上信息，完成以下分析：

1. **绝对值评估 (absolute_assessment)**：这些指标的当前绝对水平是否在健康范围内？结合用户年龄和已知疾病判断。

2. **原因假设 (hypotheses)**：如果有个人基线偏离或绝对值异常，给出1-3个可能原因，每个附带置信度(0-1)和证据。
   - 参考已确认的用户习惯做推理
   - 排除用户已否认的因素

3. **综合判断 (summary)**：一句话总结最重要的发现。

4. **严重度 (severity)**：mild/moderate/severe。

5. **触达消息 (touch_message)**：用温和、不吓人的语气告诉用户这个发现，并给出具体建议。如果是验证性洞察，可以用提问方式（"你是不是…？"）。

6. **合规检查 (touch_compliant)**：touch_message 是否通过合规检查（无诊断结论、无处方建议）。

7. **建议行动 (recommend_action)**：用户应该做什么。"""


class InsightAgent:
    """Layer 2+3: LLM-based health insight analysis.

    Usage:
        agent = InsightAgent()
        result = await agent.analyze(detection, profile, daily_values)
    """

    async def analyze(
        self,
        detection: Optional[InsightDetection],
        profile: UserProfile,
        daily_values: DailyValues,
        user_health_profile: Optional[str] = None,
        past_insights: Optional[List[PastInsight]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Generate insight analysis with structured output.

        Returns:
            Dict with absolute_assessment, hypotheses, summary, severity,
            touch_message, touch_compliant, recommend_action.
            Or None if LLM call fails.
        """
        prompt = self._build_prompt(detection, profile, daily_values, user_health_profile, past_insights)

        try:
            from mirobody.utils.llm.utils import async_get_structured_output

            result = await async_get_structured_output(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                response_format=INSIGHT_RESPONSE_SCHEMA,
                temperature=0.3,
                max_tokens=1500,
            )

            if result:
                logging.info(
                    f"[InsightAgent] Analysis complete for user={profile.user_id}: "
                    f"severity={result.get('severity')}, compliant={result.get('touch_compliant')}"
                )
            else:
                logging.warning(f"[InsightAgent] No result for user={profile.user_id}")

            return result

        except Exception as e:
            logging.error(f"[InsightAgent] Failed for user={profile.user_id}: {e}")
            return None

    def _build_prompt(
        self,
        detection: Optional[InsightDetection],
        profile: UserProfile,
        daily_values: DailyValues,
        user_health_profile: Optional[str],
        past_insights: Optional[List[PastInsight]],
    ) -> str:
        """Build structured prompt with all context."""

        # User profile
        profile_text = user_health_profile or "无用户画像信息"

        # Tags
        tags_text = ", ".join(profile.tags) if profile.tags else "无"

        # Past insights — confirmed
        confirmed_parts = []
        denied_parts = []
        if past_insights:
            for pi in past_insights:
                if pi.is_expired:
                    continue
                if pi.feedback_type.value == "confirmed":
                    confirmed_parts.append(f"- [{pi.target_date}] {pi.observation}")
                elif pi.feedback_type.value == "denied":
                    reason = f"（原因：{pi.feedback_reason}）" if pi.feedback_reason else ""
                    denied_parts.append(f"- [{pi.target_date}] {pi.hypothesis or pi.observation}{reason}")

        confirmed_text = "\n".join(confirmed_parts) if confirmed_parts else "无"
        denied_text = "\n".join(denied_parts) if denied_parts else "无"

        # Layer 1 observations
        if detection and detection.triggered:
            obs_parts = [detection.observation_text or ""]
            for d in detection.deviations:
                obs_parts.append(
                    f"- {d.category}: {d.direction} {d.sigma_deviation}σ "
                    f"(基线 {d.baseline_mean:.1f}, 当前 {d.current_value:.1f}, "
                    f"持续 {d.consecutive_days} 天)"
                )
            observations_text = "\n".join(obs_parts)
        else:
            observations_text = "Layer 1 未检测到明显偏离，请评估绝对值水平。"

        # Absolute values
        abs_parts = []
        for cat, bl in profile.baselines.items():
            vals = daily_values.get(bl.indicator_name, [])
            recent_vals = [v for _, v in vals[-7:]] if vals else []
            recent_avg = sum(recent_vals) / len(recent_vals) if recent_vals else bl.mean

            abs_parts.append(
                f"- {cat} ({bl.indicator_name}): "
                f"近7天均值={recent_avg:.1f}, "
                f"个人基线={bl.mean:.1f}±{bl.std:.1f}"
            )
        absolute_text = "\n".join(abs_parts) if abs_parts else "无数据"

        return USER_PROMPT_TEMPLATE.format(
            user_profile=profile_text,
            user_tags=tags_text,
            confirmed_insights=confirmed_text,
            denied_insights=denied_text,
            observations=observations_text,
            absolute_values=absolute_text,
        )
