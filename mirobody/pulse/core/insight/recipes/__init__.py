"""
Insight Recipes

Each recipe is a standalone module with a detect() function.
All recipes are registered in register_all_recipes().
"""

from ..models import InsightRecipe, RecipeCategory
from ..recipe_registry import recipe_registry
from . import r01_multi_signal, r02_single_sustained, r03_trend, r04_recovery, r05_weekday_pattern, r06_glucose


def register_all_recipes():
    """Register all built-in recipes."""
    recipes = [
        InsightRecipe(
            name="multi_signal_deterioration",
            version="1.1.0",
            display_name="多指标同步恶化",
            category=RecipeCategory.ANOMALY,
            required_categories=["heartRate"],
            optional_categories=["sleepDeep", "hrv", "bloodGlucose", "stress", "spo2", "steps"],
            min_density_days=14,
            min_overlap_days=10,
            detect=r01_multi_signal.detect,
            observation_template="过去{days}天，观察到多项指标同时偏离基线：{details}",
            cooldown_days=3,  # multi-signal is high priority, report every 3 days
        ),
        InsightRecipe(
            name="single_sustained_anomaly",
            version="1.1.0",
            display_name="单指标持续异常",
            category=RecipeCategory.ANOMALY,
            required_categories=[],
            optional_categories=["heartRate", "sleepDeep", "hrv", "bloodGlucose", "steps", "stress", "bpSystolic"],
            min_density_days=14,
            min_overlap_days=0,
            detect=r02_single_sustained.detect,
            observation_template="你的{indicator}连续{days}天偏{direction}（基线{baseline}，当前{current}）",
            cooldown_days=7,  # single indicator, don't nag every day
        ),
        InsightRecipe(
            name="long_term_trend",
            version="1.1.0",
            display_name="长期趋势",
            category=RecipeCategory.TREND,
            required_categories=[],
            optional_categories=["heartRate", "sleepDeep", "bloodGlucose", "weight", "bmi", "bodyFat", "bpSystolic"],
            min_density_days=21,
            min_overlap_days=0,
            detect=r03_trend.detect,
            observation_template="过去4周，{indicator}呈持续{direction}趋势（{slope}/周）",
            cooldown_days=14,  # trends are slow-moving, report every 2 weeks
        ),
        InsightRecipe(
            name="recovery_trend",
            version="1.1.0",
            display_name="恢复趋势",
            category=RecipeCategory.RECOVERY,
            required_categories=["heartRate"],
            min_density_days=14,
            min_overlap_days=0,
            detect=r04_recovery.detect,
            observation_template="好消息：{indicator}从峰值{peak}降至{current}，正在恢复",
            cooldown_days=7,
        ),
        InsightRecipe(
            name="weekday_weekend_pattern",
            version="1.1.0",
            display_name="周末-工作日差异",
            category=RecipeCategory.PATTERN,
            required_categories=["steps"],
            min_density_days=21,
            min_overlap_days=0,
            detect=r05_weekday_pattern.detect,
            observation_template="你的工作日{indicator}均值{weekday_avg}，周末{weekend_avg}（差异{diff_pct}%）",
            cooldown_days=30,  # patterns are stable, report monthly
        ),
        InsightRecipe(
            name="glucose_control",
            version="1.1.0",
            display_name="血糖控制波动",
            category=RecipeCategory.ANOMALY,
            required_categories=["bloodGlucose"],
            optional_categories=["hba1c", "glucoseTAR", "glucoseTBR"],
            min_density_days=14,
            min_overlap_days=0,
            detect=r06_glucose.detect,
            observation_template="近期空腹血糖持续偏{direction}（基线{baseline}，近{days}天均值{current}）",
            cooldown_days=7,
        ),
    ]

    for recipe in recipes:
        recipe_registry.register(recipe)
