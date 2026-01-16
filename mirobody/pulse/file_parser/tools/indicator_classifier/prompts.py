# -*- coding: utf-8 -*-
"""
Medical Indicator Classifier Prompts
医学指标分类器提示词模板
"""

MEDICAL_INDICATOR_CLASSIFICATION_SYSTEM_PROMPT = """You are a professional Chinese medical examination expert. Please provide precise organ, physiological system, and disease classification based on the given medical indicator information. All classifications must use standard Chinese medical terminology. IMPORTANT: Always keep the indicator names exactly as provided in the input - never modify, translate, or change the indicator names. Make sure to provide complete classification results for all indicators."""

MEDICAL_INDICATOR_CLASSIFICATION_USER_PROMPT = """Please analyze the following list of medical indicators and provide precise classification:

Indicator List:
{indicators_text}

Please provide for each medical indicator:
1. Primary related organ (single most relevant organ)
2. Physiological system (single most relevant system)
3. Possibly related diseases (list 2-5 most relevant diseases, separated by commas)
4. Description of the indicator (use 2-3 sentences to describe what this indicator measures, its clinical significance, and abnormal ranges - DO NOT repeat the indicator name at the beginning)

Classification Requirements:
- Use standard Chinese medical terminology
- Organ examples: 心脏、肝脏、肾脏、肺部、大脑、胰腺等
- System Classification (please strictly use one of the following categories):
  1. 呼吸系统 - 肺功能、呼吸频率等
  2. 消化和排泄系统 - 肝功能、肾功能、胃肠道指标等
  3. 循环系统 - 血压、心率、血脂等
  4. 泌尿系统 - 尿常规、肾功能相关指标等
  5. 外皮系统 - 皮肤相关指标
  6. 骨骼系统 - 骨密度、钙磷代谢等
  7. 肌肉系统 - 肌酸激酶、肌红蛋白等
  8. 内分泌系统 - 血糖、甲状腺功能、激素水平等
  9. 淋巴系统 - 淋巴细胞计数等
  10. 神经系统 - 神经递质、脑电图等
  11. 生殖系统 - 性激素、生殖功能指标等
  12. 免疫系统 - 免疫球蛋白、白细胞等
  13. 其他 - 未分类或跨系统的指标
- Disease examples: 高血压、糖尿病、心律失常、肝炎等
- Description examples: 测量心脏每分钟跳动的次数。过快可能引起心慌、胸闷等症状，过慢可能引起头晕、乏力等症状，长期异常可能是心脏疾病的表现。

CRITICAL: The indicator names must be kept exactly as provided in the input list. Do not modify, translate, or change the indicator names in any way. Return the exact same indicator names as given.

IMPORTANT: Please ensure complete classification results are provided for all {indicator_count} indicators."""


# Complete response format configuration for medical indicator classification
MEDICAL_INDICATOR_CLASSIFICATION_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "medical_classification",
        "schema": {
            "type": "object",
            "properties": {
                "results": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "indicator": {
                                "type": "string",
                                "description": "Medical indicator name exactly as provided in input - DO NOT modify, translate, or change in any way",
                            },
                            "recommended_organ": {
                                "type": "string",
                                "description": "Primary related organ in Chinese medical terminology",
                            },
                            "recommended_system": {
                                "type": "string",
                                "description": "Physiological system in Chinese medical terminology",
                            },
                            "recommended_disease": {
                                "type": "string",
                                "description": "Related diseases separated by commas in Chinese medical terminology",
                            },
                            "indicator_description": {
                                "type": "string",
                                "description": "Description of what the indicator measures, its clinical significance, and abnormal ranges in Chinese - DO NOT start with or repeat the indicator name",
                            },
                        },
                        "required": [
                            "indicator",
                            "recommended_organ",
                            "recommended_system",
                            "recommended_disease",
                            "indicator_description",
                        ],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["results"],
            "additionalProperties": False,
        },
    },
}
