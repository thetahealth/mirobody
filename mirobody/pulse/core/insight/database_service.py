"""
Insight Database Service

Handles all DB operations for the insight engine:
- Read user list + daily values from th_series_data
- Write/read user_behavior_insight
- Read event.* for benchmark
"""

import json
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from ..database import execute_query

DB_CONFIG_CORE = None  # Uses default from execute_query


class InsightDatabaseService:
    """Database operations for insight engine."""

    # =========================================================================
    # User & Data Queries
    # =========================================================================

    async def get_demo_user_ids(self) -> List[str]:
        """Get all demo user IDs.

        Returns:
            List of user_id strings for demo users (email LIKE 'user%demo')
        """
        sql = """
            SELECT id::text as user_id
            FROM health_app_user
            WHERE email LIKE 'user%demo' AND is_del = FALSE
            ORDER BY id
        """
        rows = await execute_query(sql, query_type="select")
        return [r["user_id"] for r in rows]

    async def get_user_indicators(self, user_id: str) -> Set[str]:
        """Get all distinct indicator names for a user (excluding event.*).

        Args:
            user_id: User ID

        Returns:
            Set of indicator names present in th_series_data
        """
        sql = """
            SELECT DISTINCT indicator
            FROM th_series_data
            WHERE user_id = :user_id
              AND deleted = 0
              AND indicator NOT LIKE 'event.%'
        """
        rows = await execute_query(sql, {"user_id": user_id}, query_type="select")
        return {r["indicator"] for r in rows}

    async def get_daily_values(
        self,
        user_id: str,
        indicators: List[str],
        target_date: date,
        lookback_days: int = 60,
    ) -> Dict[str, List[Tuple[date, float]]]:
        """Get daily values for specified indicators.

        Args:
            user_id: User ID
            indicators: List of actual indicator names to fetch
            target_date: End date (inclusive)
            lookback_days: How many days back to look

        Returns:
            Dict of indicator_name -> [(date, value), ...] sorted by date
        """
        start_date = target_date - timedelta(days=lookback_days)
        indicator_list = ", ".join(f"'{ind}'" for ind in indicators)

        sql = f"""
            SELECT indicator, start_time::date as day, value::numeric as val
            FROM th_series_data
            WHERE user_id = :user_id
              AND deleted = 0
              AND indicator IN ({indicator_list})
              AND start_time::date >= :start_date
              AND start_time::date <= :target_date
              AND value ~ '^-?[0-9]+\\.?[0-9]*$'
            ORDER BY indicator, start_time::date
        """
        rows = await execute_query(
            sql,
            {"user_id": user_id, "start_date": str(start_date), "target_date": str(target_date)},
            query_type="select",
        )

        result: Dict[str, List[Tuple[date, float]]] = {}
        for row in rows:
            ind = row["indicator"]
            if ind not in result:
                result[ind] = []
            result[ind].append((row["day"], float(row["val"])))

        return result

    # =========================================================================
    # Cooldown Check
    # =========================================================================

    async def check_cooldown(
        self, user_id: str, recipe_name: str, target_date: date, cooldown_days: int
    ) -> bool:
        """Check if a recipe is in cooldown period for this user.

        Returns:
            True if in cooldown (should skip), False if OK to run.
        """
        if cooldown_days <= 1:
            return False  # per-day dedup handled by UNIQUE constraint

        cooldown_start = target_date - timedelta(days=cooldown_days)
        sql = """
            SELECT COUNT(*) as cnt
            FROM user_behavior_insight
            WHERE user_id = :user_id
              AND recipe_name = :recipe_name
              AND target_date > :cooldown_start
              AND target_date < :target_date
        """
        rows = await execute_query(sql, {
            "user_id": user_id,
            "recipe_name": recipe_name,
            "cooldown_start": str(cooldown_start),
            "target_date": str(target_date),
        }, query_type="select")
        return rows[0]["cnt"] > 0 if rows else False

    # =========================================================================
    # Insight Results CRUD
    # =========================================================================

    async def save_insight(
        self,
        user_id: str,
        target_date: date,
        recipe_name: str,
        recipe_version: str,
        severity: Optional[str],
        observation: Optional[str],
        indicators_detail: Optional[Dict],
        baseline_snapshot: Optional[Dict],
        user_tags: Optional[List[str]],
        hypothesis: Optional[str] = None,
        hypothesis_confidence: float = 0.0,
        touch_message: Optional[str] = None,
        touch_compliant: Optional[bool] = None,
    ) -> None:
        """Save an insight result (upsert by user_id + target_date + recipe_name)."""
        sql = """
            INSERT INTO user_behavior_insight (
                user_id, target_date, recipe_name, recipe_version,
                severity, observation, indicators_detail,
                hypothesis, hypothesis_confidence,
                touch_message, touch_compliance,
                baseline_snapshot, user_tags, created_at
            ) VALUES (
                :user_id, :target_date, :recipe_name, :recipe_version,
                :severity, :observation, :indicators_detail,
                :hypothesis, :hypothesis_confidence,
                :touch_message, :touch_compliance,
                :baseline_snapshot, :user_tags, NOW()
            )
            ON CONFLICT (user_id, target_date, recipe_name) DO UPDATE SET
                recipe_version = EXCLUDED.recipe_version,
                severity = EXCLUDED.severity,
                observation = EXCLUDED.observation,
                indicators_detail = EXCLUDED.indicators_detail,
                hypothesis = EXCLUDED.hypothesis,
                hypothesis_confidence = EXCLUDED.hypothesis_confidence,
                touch_message = EXCLUDED.touch_message,
                touch_compliance = EXCLUDED.touch_compliance,
                baseline_snapshot = EXCLUDED.baseline_snapshot,
                user_tags = EXCLUDED.user_tags,
                created_at = NOW()
        """
        params = {
            "user_id": user_id,
            "target_date": str(target_date),
            "recipe_name": recipe_name,
            "recipe_version": recipe_version,
            "severity": severity,
            "observation": observation,
            "indicators_detail": json.dumps(indicators_detail) if indicators_detail else None,
            "hypothesis": hypothesis,
            "hypothesis_confidence": hypothesis_confidence,
            "touch_message": touch_message,
            "touch_compliance": touch_compliant,
            "baseline_snapshot": json.dumps(baseline_snapshot) if baseline_snapshot else None,
            "user_tags": json.dumps(user_tags) if user_tags else None,
        }
        await execute_query(sql, params, query_type="dml")

    async def get_user_insights(
        self,
        user_id: str,
        limit: int = 20,
        offset: int = 0,
        severity: Optional[str] = None,
        recipe_name: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get insights for a user with pagination.

        Args:
            user_id: User ID
            limit: Max results per page
            offset: Pagination offset
            severity: Optional filter by severity
            recipe_name: Optional filter by recipe

        Returns:
            Tuple of (insight list, total count)
        """
        conditions = ["user_id = :user_id"]
        params: Dict[str, Any] = {"user_id": user_id, "limit": limit, "offset": offset}

        if severity:
            conditions.append("severity = :severity")
            params["severity"] = severity
        if recipe_name:
            conditions.append("recipe_name = :recipe_name")
            params["recipe_name"] = recipe_name

        where = " AND ".join(conditions)

        count_sql = f"SELECT COUNT(*) as cnt FROM user_behavior_insight WHERE {where}"
        count_rows = await execute_query(count_sql, params, query_type="select")
        total = count_rows[0]["cnt"] if count_rows else 0

        sql = f"""
            SELECT id, user_id, target_date, recipe_name, recipe_version,
                   severity, observation, hypothesis, touch_message,
                   indicators_detail, user_tags, created_at
            FROM user_behavior_insight
            WHERE {where}
            ORDER BY target_date DESC, created_at DESC
            LIMIT :limit OFFSET :offset
        """
        rows = await execute_query(sql, params, query_type="select")
        return rows, total

    async def get_unscored_insights(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get insights that haven't been benchmarked yet.

        Returns:
            List of user_behavior_insight rows where benchmark_score IS NULL
        """
        sql = """
            SELECT id, user_id, target_date, recipe_name, recipe_version,
                   severity, observation, indicators_detail
            FROM user_behavior_insight
            WHERE benchmark_score IS NULL
            ORDER BY created_at DESC
            LIMIT :limit
        """
        return await execute_query(sql, {"limit": limit}, query_type="select")

    async def update_benchmark_score(
        self,
        insight_id: int,
        benchmark_score: float,
        benchmark_detail: Optional[Dict],
    ) -> None:
        """Write benchmark evaluation result back to user_behavior_insight."""
        sql = """
            UPDATE user_behavior_insight
            SET benchmark_score = :score,
                benchmark_detail = :detail
            WHERE id = :id
        """
        params = {
            "id": insight_id,
            "score": benchmark_score,
            "detail": json.dumps(benchmark_detail) if benchmark_detail else None,
        }
        await execute_query(sql, params, query_type="dml")

    # =========================================================================
    # Event.* Ground Truth (for benchmark)
    # =========================================================================

    async def get_user_events(
        self,
        user_id: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[Dict[str, Any]]:
        """Get event.* indicators for a user (benchmark ground truth).

        Args:
            user_id: User ID
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            List of {date, event_type, event_name, value_json}
        """
        conditions = ["user_id = :user_id", "deleted = 0", "indicator LIKE 'event.%'"]
        params: Dict[str, Any] = {"user_id": user_id}

        if start_date:
            conditions.append("start_time::date >= :start_date")
            params["start_date"] = str(start_date)
        if end_date:
            conditions.append("start_time::date <= :end_date")
            params["end_date"] = str(end_date)

        where = " AND ".join(conditions)
        sql = f"""
            SELECT start_time::date as event_date,
                   split_part(indicator, '.', 2) as event_type,
                   split_part(indicator, '.', 3) as event_name,
                   value as value_json
            FROM th_series_data
            WHERE {where}
            ORDER BY start_time
        """
        return await execute_query(sql, params, query_type="select")

    # =========================================================================
    # User Health Profile (from health_user_profile_by_system)
    # =========================================================================

    async def get_user_health_profile(self, user_id: str) -> Optional[str]:
        """Get the latest user health profile (decrypted).

        Tries both column names: common_part (production) and
        common_part_encrypted with decrypt_content (test environment).

        Returns:
            Profile text, or None if not available.
        """
        # Try encrypted column with SQL-level decrypt_content()
        # This works because the DB connection has app.encryption_key set via connection options
        try:
            sql = """
                SELECT decrypt_content(common_part_encrypted) as profile
                FROM health_user_profile_by_system
                WHERE user_id = :user_id AND is_deleted = false
                ORDER BY version DESC LIMIT 1
            """
            rows = await execute_query(sql, {"user_id": user_id}, query_type="select")
            if rows and rows[0].get("profile"):
                profile = rows[0]["profile"]
                if not profile.startswith("gAAAA"):
                    logging.info(f"[InsightDB] Loaded profile for user {user_id}: {len(profile)} chars")
                    return profile
                else:
                    logging.warning(f"[InsightDB] decrypt_content returned ciphertext for user {user_id}")
            else:
                logging.warning(f"[InsightDB] No profile rows or empty profile for user {user_id}, rows={bool(rows)}")
        except Exception as e:
            logging.error(f"[InsightDB] Encrypted profile query failed for user {user_id}: {e}")

        # Try plain column (production)
        try:
            sql = """
                SELECT common_part as profile
                FROM health_user_profile_by_system
                WHERE user_id = :user_id AND is_deleted = false
                ORDER BY version DESC LIMIT 1
            """
            rows = await execute_query(sql, {"user_id": user_id}, query_type="select")
            if rows and rows[0].get("profile"):
                logging.info(f"[InsightDB] Loaded plain profile for user {user_id}")
                return rows[0]["profile"]
        except Exception:
            pass

        logging.warning(f"[InsightDB] No profile found for user {user_id}")
        return None

    # =========================================================================
    # Past Insights (feedback loop)
    # =========================================================================

    async def get_past_insights_with_feedback(
        self, user_id: str, limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get recent insights with user feedback (for Layer 2 context).

        Args:
            user_id: User ID
            limit: Max number of past insights

        Returns:
            List of insight rows with user_feedback IS NOT NULL
        """
        sql = """
            SELECT recipe_name, target_date, observation, hypothesis,
                   user_feedback, created_at
            FROM user_behavior_insight
            WHERE user_id = :user_id AND user_feedback IS NOT NULL
            ORDER BY target_date DESC
            LIMIT :limit
        """
        return await execute_query(sql, {"user_id": user_id, "limit": limit}, query_type="select")
