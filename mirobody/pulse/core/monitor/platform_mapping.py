"""
Platform mapping — resolve raw source strings to canonical platform names.

Rules are prefix-based so new sources are classified automatically.
"""


def resolve_platform(source: str) -> str:
    """
    Map a series_data source string to a canonical platform name.

    Args:
        source: Raw source value from series_data (e.g. "vital.garmin", "theta.renpho")

    Returns:
        Canonical platform: vital, theta, apple_health, app, ehr, or other
    """
    if not source:
        return "other"

    s = source.strip().lower()

    if s.startswith("vital."):
        return "vital"
    if s.startswith("theta."):
        return "theta"
    if s.startswith("apple_health"):
        return "apple_health"
    if s in ("chat", "journal"):
        return "app"
    if " " in source:
        return "ehr"

    return "other"
