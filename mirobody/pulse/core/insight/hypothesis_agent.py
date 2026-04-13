"""
Hypothesis Agent (Layer 2)

Uses LLM to:
1. Evaluate absolute health levels (not just personal deviation)
2. Generate hypotheses about possible causes
3. Consider user profile (demographics, medications, conditions)

Input:  InsightDetection (Layer 1) + UserProfile + user health profile
Output: hypothesis text + confidence score
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .models import InsightDetection, UserProfile


SYSTEM_PROMPT = """You are a health data analysis expert. You analyze wearable device data to provide health observations.

Rules:
- Never make diagnostic conclusions ("you have X disease")
- Use uncertain language: "may indicate", "suggest monitoring", "worth paying attention to"
- For severe anomalies, suggest consulting a doctor
- Consider the user's profile (age, conditions, medications) when interpreting data
- Evaluate both personal deviation AND absolute health levels
- Respond in Chinese (中文)"""

USER_PROMPT_TEMPLATE = """## 用户画像
{user_profile}

## 用户标签（系统推断）
{user_tags}

## 当前观察（Layer 1 检测结果）
{observations}

## 指标绝对值（近期均值）
{absolute_values}

## 任务

基于以上信息，请完成以下分析：

1. **绝对值评估**：这些指标的当前绝对水平是否在健康范围内？结合用户年龄和已知疾病判断。
2. **偏离分析**：如果有个人基线偏离，可能的原因是什么？给出 1-3 个可能性。
3. **综合判断**：给出一个综合性的健康观察，优先级排序。

请以 JSON 格式输出：
```json
{{
  "absolute_assessment": "对绝对值水平的评估",
  "hypotheses": [
    {{"cause": "可能原因1", "confidence": 0.7, "evidence": "支撑证据"}},
    {{"cause": "可能原因2", "confidence": 0.4, "evidence": "支撑证据"}}
  ],
  "summary": "一句话综合判断",
  "severity": "mild/moderate/severe",
  "recommend_action": "建议采取的行动"
}}
```"""


class HypothesisAgent:
    """Layer 2: LLM-based hypothesis generation.

    Usage:
        agent = HypothesisAgent()
        hypothesis, confidence = await agent.generate(detection, profile, daily_values, user_health_profile)
    """

    def __init__(self, model: str = "gpt-4o"):
        self.model = model

    async def generate(
        self,
        detection: Optional[InsightDetection],
        profile: UserProfile,
        daily_values: Dict[str, List[Tuple]],
        user_health_profile: Optional[str] = None,
    ) -> Tuple[Optional[str], float, Optional[Dict]]:
        """Generate hypothesis for an insight detection.

        Args:
            detection: Layer 1 detection result (can be None for absolute-value-only analysis)
            profile: UserProfile with baselines and tags
            daily_values: Raw daily values for context
            user_health_profile: Optional user health profile text (from health_user_profile_by_system)

        Returns:
            Tuple of (hypothesis_text, confidence, full_result_dict)
        """
        prompt = self._build_prompt(detection, profile, daily_values, user_health_profile)

        try:
            from mirobody.utils.llm.utils import get_openai_chat
            response = await get_openai_chat(
                model_name=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=1500,
            )

            if not response:
                logging.warning("[HypothesisAgent] Empty LLM response")
                return None, 0.0, None

            result = self._parse_response(response)
            if result:
                hypothesis = result.get("summary", "")
                confidence = max(
                    (h.get("confidence", 0) for h in result.get("hypotheses", [])),
                    default=0.5,
                )
                return hypothesis, confidence, result
            else:
                # Fallback: use raw response as hypothesis
                return response[:500], 0.3, None

        except Exception as e:
            logging.error(f"[HypothesisAgent] LLM call failed: {e}")
            return None, 0.0, None

    def _build_prompt(
        self,
        detection: Optional[InsightDetection],
        profile: UserProfile,
        daily_values: Dict[str, List[Tuple]],
        user_health_profile: Optional[str],
    ) -> str:
        """Build the user prompt with all context."""

        # User profile section
        profile_text = user_health_profile or "无用户画像信息"

        # Tags
        tags_text = ", ".join(profile.tags) if profile.tags else "无"

        # Observations from Layer 1
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

        # Absolute values from baselines
        abs_parts = []
        for cat, bl in profile.baselines.items():
            # Get the most recent value
            vals = daily_values.get(bl.indicator_name, [])
            recent_vals = [v for _, v in vals[-7:]] if vals else []
            recent_avg = sum(recent_vals) / len(recent_vals) if recent_vals else bl.mean

            abs_parts.append(
                f"- {cat} ({bl.indicator_name}): "
                f"近7天均值={recent_avg:.1f}, "
                f"30天基线={bl.mean:.1f}±{bl.std:.1f}, "
                f"数据天数={bl.data_days}"
            )
        absolute_text = "\n".join(abs_parts) if abs_parts else "无数据"

        return USER_PROMPT_TEMPLATE.format(
            user_profile=profile_text,
            user_tags=tags_text,
            observations=observations_text,
            absolute_values=absolute_text,
        )

    def _parse_response(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse LLM JSON response."""
        try:
            # Try to extract JSON from markdown code block
            if "```json" in response:
                start = response.index("```json") + 7
                end = response.index("```", start)
                json_str = response[start:end].strip()
            elif "```" in response:
                start = response.index("```") + 3
                end = response.index("```", start)
                json_str = response[start:end].strip()
            else:
                json_str = response.strip()

            return json.loads(json_str)
        except (json.JSONDecodeError, ValueError) as e:
            logging.warning(f"[HypothesisAgent] Failed to parse JSON: {e}")
            return None
