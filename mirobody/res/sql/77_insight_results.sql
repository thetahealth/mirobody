-- Phase 4 Insight Engine: user_behavior_insight table
-- Stores all insight detections with Layer 1/2/3 outputs and benchmark scores

CREATE TABLE IF NOT EXISTS user_behavior_insight (
    id                     SERIAL PRIMARY KEY,
    user_id                TEXT NOT NULL,
    target_date            DATE NOT NULL,
    recipe_name            TEXT NOT NULL,
    recipe_version         TEXT NOT NULL,

    -- Layer 1: Observation (pure facts)
    severity               TEXT,                    -- mild / moderate / severe
    observation            TEXT,                    -- human-readable observation text
    indicators_detail      JSONB,                  -- deviations: [{category, indicator, direction, sigma, ...}]

    -- Layer 2: Hypothesis (LLM reasoning, phase 2)
    hypothesis             TEXT,
    hypothesis_confidence  NUMERIC,

    -- Layer 3: Touch User (user-facing message, phase 2)
    touch_message          TEXT,
    touch_compliance       BOOLEAN,                -- passed compliance check

    -- Context snapshot
    baseline_snapshot      JSONB,                  -- baseline values at detection time
    user_tags              JSONB,                  -- tags at detection time, e.g. ["obesity", "pre_diabetes"]

    -- Benchmark (filled by insight_benchmark task)
    benchmark_score        NUMERIC,
    benchmark_detail       JSONB,                  -- matched event.* content
    benchmark_layer3_score NUMERIC,                -- Layer 3 text quality score

    -- User feedback (filled by product, phase 3)
    user_feedback          JSONB,                  -- {"confirmed": true} or {"denied": true, "reason": "..."}

    created_at             TIMESTAMPTZ DEFAULT NOW(),

    -- Per day-user-recipe dedup
    UNIQUE (user_id, target_date, recipe_name)
);

-- Query insights by user
CREATE INDEX IF NOT EXISTS idx_ubi_user_date
    ON user_behavior_insight (user_id, target_date DESC);

-- Find unscored insights for benchmark task
CREATE INDEX IF NOT EXISTS idx_ubi_unscored
    ON user_behavior_insight (benchmark_score)
    WHERE benchmark_score IS NULL;

-- Find insights with feedback for past-insight context
CREATE INDEX IF NOT EXISTS idx_ubi_feedback
    ON user_behavior_insight (user_id, created_at DESC)
    WHERE user_feedback IS NOT NULL;
