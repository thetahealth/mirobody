"""
Pulse Insight Engine (Phase 4)

Recipe-based health insight detection system.

Core modules:
- models: All data structures (dataclass) for inter-module contracts
- indicator_aliases: Indicator category -> actual DB indicator name mapping
- baseline_engine: EWMA baseline computation + freeze + tag inference
- recipe_registry: Recipe registration + matching
- recipes/: Individual recipe implementations
- database_service: user_behavior_insight table read/write
- engine_task: InsightEnginePullTask (scheduler entry point)
- benchmark_task: InsightBenchmarkPullTask (evaluation)
"""
