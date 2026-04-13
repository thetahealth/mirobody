"""
Insight Engine PullTask

The main scheduler entry point that orchestrates the insight pipeline:
1. Get user list
2. For each user: compute baseline → match recipes → run detection → save results
"""

import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from ..scheduler import PullTask, ScheduleType
from .baseline_engine import BaselineEngine
from .database_service import InsightDatabaseService
from .indicator_aliases import resolve_all
from .models import DailyValues, InsightDetection, InsightRecipe, UserProfile
from .recipe_registry import recipe_registry
from .insight_agent import InsightAgent
from .recipes import register_all_recipes


class InsightEnginePullTask(PullTask):
    """Runs insight detection for demo users."""

    def __init__(self):
        super().__init__(
            provider_slug="insight_engine",
            schedule_type=ScheduleType.INTERVAL,
            interval_minutes=360,        # every 6 hours
            execution_interval_hours=6.0,
            lock_duration_hours=2.0,
        )
        self.db = InsightDatabaseService()
        self.engine = BaselineEngine()
        self.insight_agent = InsightAgent()
        self._recipes_registered = False

    def _ensure_recipes(self):
        if not self._recipes_registered:
            register_all_recipes()
            self._recipes_registered = True

    async def execute(
        self,
        user_id: Optional[str] = None,
        target_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
        skip_llm: bool = False,
    ) -> bool:
        """Run insight detection pipeline.

        Args:
            user_id: If provided, only process this user. Otherwise all demo users.
            target_date_str: Start date (YYYY-MM-DD). Default: yesterday.
            end_date_str: End date for simulation range (YYYY-MM-DD). If provided,
                          slides day by day from target_date_str to end_date_str.
            skip_llm: If True, only run Layer 1 (no LLM calls). Faster for simulation.
        """
        self._ensure_recipes()

        try:
            # Parse dates
            if target_date_str:
                start_date = date.fromisoformat(target_date_str)
            else:
                start_date = date.today() - timedelta(days=1)

            if end_date_str:
                end_date = date.fromisoformat(end_date_str)
            else:
                end_date = start_date  # single day

            # Get users
            if user_id:
                user_ids = [user_id]
            else:
                user_ids = await self.db.get_demo_user_ids()

            total_days = (end_date - start_date).days + 1
            logging.info(
                f"[InsightEngine] Starting for {len(user_ids)} users, "
                f"dates={start_date}~{end_date} ({total_days} days), skip_llm={skip_llm}"
            )

            total_insights = 0
            total_users_with_insights = 0

            for uid in user_ids:
                user_insights = 0
                current_date = start_date
                while current_date <= end_date:
                    try:
                        if skip_llm:
                            n = await self._process_user_layer1_only(uid, current_date)
                        else:
                            n = await self._process_user(uid, current_date)
                        user_insights += n
                    except Exception as e:
                        logging.error(f"[InsightEngine] Error user={uid} date={current_date}: {e}")
                    current_date += timedelta(days=1)

                if user_insights > 0:
                    total_insights += user_insights
                    total_users_with_insights += 1

            logging.info(
                f"[InsightEngine] Done: {total_insights} insights for "
                f"{total_users_with_insights}/{len(user_ids)} users across {total_days} days"
            )

            stats = {
                "executed_at": datetime.now().isoformat(),
                "date_range": f"{start_date}~{end_date}",
                "total_days": total_days,
                "total_users": len(user_ids),
                "users_with_insights": total_users_with_insights,
                "total_insights": total_insights,
                "skip_llm": skip_llm,
            }
            await self.save_task_stats(stats)
            return True

        except Exception as e:
            logging.error(f"[InsightEngine] Execution error: {e}")
            return False

    async def _process_user(self, user_id: str, target_date: date) -> int:
        """Process a single user through the full pipeline.

        Returns: number of insights generated
        """
        # Step 1: Get user's indicator names
        user_indicators = await self.db.get_user_indicators(user_id)
        if not user_indicators:
            return 0

        # Step 2: Resolve categories
        category_map = resolve_all(user_indicators)
        if not category_map:
            return 0

        # Step 3: Fetch daily values for all resolved indicators
        indicator_names = list(category_map.values())
        daily_values = await self.db.get_daily_values(
            user_id, indicator_names, target_date, lookback_days=90
        )
        if not daily_values:
            return 0

        # Step 4: Compute baseline + profile
        profile = self.engine.compute(user_id, target_date, user_indicators, daily_values)
        if not profile.available_categories:
            return 0

        # Step 5: Match recipes
        matched_recipes = recipe_registry.match(profile)
        if not matched_recipes:
            return 0

        # Step 6: Run each recipe (Layer 1) with cooldown check
        insights_count = 0
        detections = []  # collect for Layer 2
        for recipe in matched_recipes:
            try:
                if recipe.cooldown_days > 1:
                    in_cooldown = await self.db.check_cooldown(
                        user_id, recipe.name, target_date, recipe.cooldown_days
                    )
                    if in_cooldown:
                        continue

                detection = recipe.detect(profile, daily_values)
                if detection and detection.triggered:
                    detections.append((recipe, detection))
            except Exception as e:
                logging.error(f"[InsightEngine] Recipe {recipe.name} failed for user {user_id}: {e}")
                continue

        # Step 7: Layer 2+3 — InsightAgent (runs once per user)
        agent_result = None
        try:
            primary_detection = None
            if detections:
                detections.sort(key=lambda x: {"severe": 3, "moderate": 2, "mild": 1}.get(
                    x[1].severity.value if x[1].severity else "", 0), reverse=True)
                primary_detection = detections[0][1]

            agent_result = await self.insight_agent.analyze(
                detection=primary_detection,
                profile=profile,
                daily_values=daily_values,
                user_health_profile=None,  # TODO: read from health_user_profile_by_system
                past_insights=None,  # TODO: read from DB
            )
        except Exception as e:
            logging.error(f"[InsightEngine] InsightAgent failed for user {user_id}: {e}")

        # Extract Layer 2+3 fields from agent result
        hypothesis = agent_result.get("summary") if agent_result else None
        hypothesis_confidence = max(
            (h.get("confidence", 0) for h in agent_result.get("hypotheses", [])), default=0.0
        ) if agent_result else 0.0
        touch_message = agent_result.get("touch_message") if agent_result else None
        touch_compliant = agent_result.get("touch_compliant") if agent_result else None

        # Step 8: Save all detections with Layer 2+3 results
        for recipe, detection in detections:
            try:
                await self._save_detection(
                    user_id, target_date, recipe, detection, profile,
                    hypothesis=hypothesis,
                    hypothesis_confidence=hypothesis_confidence,
                    touch_message=touch_message,
                    touch_compliant=touch_compliant,
                )
                insights_count += 1
            except Exception as e:
                logging.error(f"[InsightEngine] Save failed for {recipe.name}: {e}")

        # If no Layer 1 detections but Layer 2 has absolute-value insights, save as standalone
        if not detections and hypothesis:
            try:
                from .models import InsightDetection, Severity
                abs_detection = InsightDetection(
                    triggered=True,
                    severity=Severity(hypothesis_detail.get("severity", "mild")) if hypothesis_detail else Severity.MILD,
                    observation_text=hypothesis_detail.get("absolute_assessment", hypothesis) if hypothesis_detail else hypothesis,
                    deviations=[],
                )
                from .models import InsightRecipe, RecipeCategory
                abs_recipe = InsightRecipe(
                    name="absolute_value_assessment",
                    version="1.0.0",
                    display_name="绝对值健康评估",
                    category=RecipeCategory.ANOMALY,
                    required_categories=[],
                )
                await self._save_detection(
                    user_id, target_date, abs_recipe, abs_detection, profile,
                    hypothesis=hypothesis,
                    hypothesis_confidence=hypothesis_confidence,
                )
                insights_count += 1
            except Exception as e:
                logging.error(f"[InsightEngine] Absolute value save failed: {e}")

        return insights_count

    async def _process_user_layer1_only(self, user_id: str, target_date: date) -> int:
        """Process a single user with Layer 1 only (no LLM calls). Fast path for simulation."""
        self._ensure_recipes()

        user_indicators = await self.db.get_user_indicators(user_id)
        if not user_indicators:
            return 0

        category_map = resolve_all(user_indicators)
        if not category_map:
            return 0

        indicator_names = list(category_map.values())
        daily_values = await self.db.get_daily_values(
            user_id, indicator_names, target_date, lookback_days=90
        )
        if not daily_values:
            return 0

        profile = self.engine.compute(user_id, target_date, user_indicators, daily_values)
        if not profile.available_categories:
            return 0

        matched_recipes = recipe_registry.match(profile)
        if not matched_recipes:
            return 0

        insights_count = 0
        for recipe in matched_recipes:
            try:
                # Cooldown check
                if recipe.cooldown_days > 1:
                    in_cooldown = await self.db.check_cooldown(
                        user_id, recipe.name, target_date, recipe.cooldown_days
                    )
                    if in_cooldown:
                        continue

                detection = recipe.detect(profile, daily_values)
                if detection and detection.triggered:
                    await self._save_detection(
                        user_id, target_date, recipe, detection, profile
                    )
                    insights_count += 1
            except Exception as e:
                logging.error(f"[InsightEngine] Recipe {recipe.name} failed: {e}")
                continue

        return insights_count

    async def _save_detection(
        self,
        user_id: str,
        target_date: date,
        recipe: InsightRecipe,
        detection: InsightDetection,
        profile: UserProfile,
        hypothesis: Optional[str] = None,
        hypothesis_confidence: float = 0.0,
        touch_message: Optional[str] = None,
        touch_compliant: Optional[bool] = None,
    ) -> None:
        """Save a triggered detection to the database."""
        indicators_detail = [
            {
                "category": d.category,
                "indicator": d.indicator_name,
                "direction": d.direction,
                "current": d.current_value,
                "baseline_mean": d.baseline_mean,
                "baseline_std": d.baseline_std,
                "sigma": d.sigma_deviation,
                "consecutive_days": d.consecutive_days,
            }
            for d in detection.deviations
        ]

        baseline_snapshot = {
            cat: {"mean": b.mean, "std": b.std, "frozen": b.frozen, "data_days": b.data_days}
            for cat, b in profile.baselines.items()
        }

        await self.db.save_insight(
            user_id=user_id,
            target_date=target_date,
            recipe_name=recipe.name,
            recipe_version=recipe.version,
            severity=detection.severity.value if detection.severity else None,
            observation=detection.observation_text,
            indicators_detail=indicators_detail,
            baseline_snapshot=baseline_snapshot,
            user_tags=profile.tags,
            hypothesis=hypothesis,
            hypothesis_confidence=hypothesis_confidence,
            touch_message=touch_message,
            touch_compliant=touch_compliant,
        )

        logging.info(
            f"[InsightEngine] Insight: user={user_id} recipe={recipe.name} "
            f"severity={detection.severity} date={target_date} "
            f"hypothesis={'yes' if hypothesis else 'no'}"
        )

    async def get_task_info(self) -> Dict:
        full_status = await self.get_full_status()
        full_status.update({
            "task_name": "Insight Engine",
            "description": "Run insight detection recipes for demo users",
        })
        return full_status
