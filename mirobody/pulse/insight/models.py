"""
Insight Engine Data Models

All data structures for inter-module communication.
Each dataclass defines the contract (input/output spec) between modules.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# =============================================================================
# Enums
# =============================================================================

class Severity(str, Enum):
    MILD = "mild"
    MODERATE = "moderate"
    SEVERE = "severe"


class RecipeCategory(str, Enum):
    ANOMALY = "anomaly"
    TREND = "trend"
    PATTERN = "pattern"
    RECOVERY = "recovery"


class DensityLevel(str, Enum):
    CONTINUOUS = "continuous"      # >= 12/14 days
    INTERMITTENT = "intermittent"  # 7-11/14 days
    SPARSE = "sparse"             # < 7/14 days
    NONE = "none"                 # 0 days


class FeedbackType(str, Enum):
    CONFIRMED = "confirmed"
    DENIED = "denied"


# =============================================================================
# Baseline & Profile
# =============================================================================

@dataclass
class BaselineResult:
    """Baseline computation result for a single indicator category.

    Produced by: BaselineEngine.compute()
    Consumed by: Recipe.detect()
    """
    category: str              # indicator category key, e.g. "heartRate"
    indicator_name: str        # actual indicator name in DB, e.g. "RestingHeartRate-RHR"
    mean: float
    std: float
    frozen: bool = False       # True if baseline is frozen due to ongoing anomaly
    frozen_since: Optional[date] = None
    data_days: int = 0         # number of days with data in the lookback window


@dataclass
class IndicatorDensity:
    """Data density info for a single indicator category.

    Produced by: BaselineEngine.compute()
    Consumed by: RecipeMatcher.match()
    """
    category: str
    indicator_name: str
    days_with_data: int        # out of last 14 days
    level: DensityLevel
    last_data_date: Optional[date] = None


@dataclass
class UserProfile:
    """Complete user profile for insight engine.

    Produced by: BaselineEngine.compute()
    Consumed by: RecipeMatcher.match(), Recipe.detect()

    This is the central data structure that flows through the entire pipeline.
    """
    user_id: str
    target_date: date
    tags: List[str] = field(default_factory=list)
    baselines: Dict[str, BaselineResult] = field(default_factory=dict)
    densities: Dict[str, IndicatorDensity] = field(default_factory=dict)
    available_categories: List[str] = field(default_factory=list)


# =============================================================================
# Recipe & Detection
# =============================================================================

@dataclass
class IndicatorDeviation:
    """A single indicator's deviation from baseline.

    Part of InsightDetection.deviations.
    """
    category: str
    indicator_name: str
    direction: str             # "up" or "down"
    current_value: float
    baseline_mean: float
    baseline_std: float
    sigma_deviation: float     # how many σ away from baseline
    consecutive_days: int      # how many consecutive days this deviation persists


@dataclass
class InsightDetection:
    """Result of running a recipe's detect() function.

    Produced by: Recipe.detect()
    Consumed by: InsightEnginePullTask (to save insight_results)

    If triggered is False, no insight is generated.
    """
    triggered: bool
    severity: Optional[Severity] = None
    observation_text: Optional[str] = None
    deviations: List[IndicatorDeviation] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


# DailyValues type alias: category -> [(date, value), ...]
DailyValues = Dict[str, List[Tuple[date, float]]]


@dataclass
class InsightRecipe:
    """A recipe defines what to detect and how.

    Stored in: RecipeRegistry
    Consumed by: RecipeMatcher (for matching), InsightEnginePullTask (for execution)

    The detect field holds a callable with signature:
        (profile: UserProfile, daily_values: DailyValues) -> InsightDetection
    """
    name: str                           # "multi_signal_deterioration"
    version: str                        # "1.0.0"
    display_name: str                   # "多指标同步恶化"
    category: RecipeCategory

    # Data requirements (indicator categories)
    required_categories: List[str]      # ["heartRate", "sleep"]
    optional_categories: List[str] = field(default_factory=list)
    min_density_days: int = 14
    min_overlap_days: int = 10

    # Detection function
    detect: Optional[Callable] = None

    # Output templates
    observation_template: str = ""
    hypothesis_template: str = ""       # phase 2
    touch_template: str = ""            # phase 2

    # Severity thresholds (sigma)
    severity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "mild": 2.0,
        "moderate": 3.0,
        "severe": 4.0,
    })

    cooldown_days: int = 1


# =============================================================================
# Benchmark
# =============================================================================

@dataclass
class BenchmarkResult:
    """Result of matching an insight against event.* ground truth.

    Produced by: EventMatcher.match()
    Consumed by: BenchmarkAggregator.aggregate()
    """
    insight_id: int
    matched_event: Optional[Dict[str, Any]] = None
    time_delta_days: Optional[int] = None
    semantic_score: Optional[float] = None
    is_true_positive: bool = False
    is_false_positive: bool = False


@dataclass
class BenchmarkSummary:
    """Aggregated benchmark metrics.

    Produced by: BenchmarkAggregator.aggregate()
    """
    total_insights: int = 0
    total_events: int = 0
    true_positives: int = 0
    false_positives: int = 0
    recall: float = 0.0
    precision: float = 0.0
    f1: float = 0.0
    median_delay_days: Optional[float] = None
    by_recipe: Dict[str, Dict[str, float]] = field(default_factory=dict)
    by_event_type: Dict[str, Dict[str, float]] = field(default_factory=dict)


# =============================================================================
# Past Insights (feedback loop)
# =============================================================================

@dataclass
class PastInsight:
    """A historical insight with user feedback.

    Produced by: reading user_behavior_insight WHERE user_feedback IS NOT NULL
    Consumed by: HypothesisEngine (Layer 2) as context

    Expiry: negative feedback 30 days, positive feedback 90 days.
    """
    recipe_name: str
    target_date: date
    observation: str
    hypothesis: Optional[str]
    feedback_type: FeedbackType
    feedback_reason: Optional[str] = None
    created_at: Optional[datetime] = None

    POSITIVE_EXPIRY_DAYS = 90
    NEGATIVE_EXPIRY_DAYS = 30

    @property
    def is_expired(self) -> bool:
        if not self.created_at:
            return False
        days_ago = (datetime.utcnow() - self.created_at).days
        if self.feedback_type == FeedbackType.DENIED:
            return days_ago > self.NEGATIVE_EXPIRY_DAYS
        return days_ago > self.POSITIVE_EXPIRY_DAYS
