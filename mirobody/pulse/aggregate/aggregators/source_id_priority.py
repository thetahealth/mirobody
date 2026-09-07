"""Source-id-level priority resolution for SQLAggregator.

When a single source (e.g. apple_health) acts as an aggregator hub for
multiple devices/apps, the same physiological event can be recorded by
multiple source_ids simultaneously. Naively SUM-ing produces N-fold
inflation for cumulative metrics (sleep duration, steps, calories).

This module emits a SQL CASE expression that the aggregator uses to rank
source_ids per (user, indicator, source); only the highest-ranked
source_id's data is kept for aggregation.
"""


# (pattern, priority, match_mode)
# priority: smaller = more preferred
# match_mode: 'prefix' = LIKE pattern%; 'exact' = full equality
APPLE_SOURCE_ID_PRIORITY_RULES: list[tuple[str, int, str]] = [
    # Apple Watch native (UUID class - every device has a unique UUID)
    ("com.apple.health.",     10, "prefix"),
    # Trusted 3rd-party devices/apps
    ("com.ouraring.oura",     50, "exact"),
    ("com.whoop.iphone",      60, "exact"),
    ("com.neybox.Pillow",     70, "exact"),
    ("com.aliphcom.upopen",   80, "exact"),
    # Known low-quality sources
    ("com.apple.mobiletimer", 99, "exact"),  # alarm clock, not real sleep
]

# Only apply source_id-level resolution to these sources.
# Other sources (theta.*, vital.*) are not aggregator hubs.
APPLE_SOURCES: tuple[str, ...] = ("apple_health", "apple_health_watch")

# Default priority for unmapped source_ids. Falls between trusted apps and
# known low-quality sources, so an unknown-but-active device still wins
# over the alarm clock but loses to explicitly trusted devices.
DEFAULT_PRIORITY: int = 90


def _escape_sql_literal(s: str) -> str:
    """Escape single quotes for inclusion as a SQL string literal."""
    return s.replace("'", "''")


def build_apple_priority_case(source_col: str, source_id_col: str) -> str:
    """Build a SQL CASE expression that resolves source_id priority.

    For non-Apple sources, returns 0 (all source_ids equal -> no filtering).
    For Apple sources, matches rules in order via CASE WHEN; falls back to
    DEFAULT_PRIORITY for unmatched source_ids.

    The patterns come from a hard-coded constant in this module (no
    user input), so direct string interpolation into SQL is safe.

    Args:
        source_col: SQL column reference for the source column
                    (e.g. "s.source").
        source_id_col: SQL column reference for the source_id column
                       (e.g. "s.source_id").

    Returns:
        A SQL CASE ... END expression string. The caller is responsible
        for aliasing it (e.g. ``AS sid_priority``).
    """
    apple_in = ",".join(f"'{_escape_sql_literal(s)}'" for s in APPLE_SOURCES)
    when_clauses: list[str] = []
    for pattern, prio, mode in APPLE_SOURCE_ID_PRIORITY_RULES:
        escaped = _escape_sql_literal(pattern)
        if mode == "prefix":
            when_clauses.append(
                f"WHEN {source_id_col} LIKE '{escaped}%' THEN {prio}"
            )
        elif mode == "exact":
            when_clauses.append(
                f"WHEN {source_id_col} = '{escaped}' THEN {prio}"
            )
        else:
            raise ValueError(
                f"Unknown match_mode {mode!r} for pattern {pattern!r}"
            )
    when_str = " ".join(when_clauses)
    return (
        f"CASE "
        f"WHEN {source_col} NOT IN ({apple_in}) THEN 0 "
        f"{when_str} "
        f"ELSE {DEFAULT_PRIORITY} "
        f"END"
    )
