"""The date-range task query: it must bind, and it must cover the last day.

`_get_tasks_for_user_date_range` had two defects that compounded into "the
recalculation silently did nothing":

1. `(:user_id IS NULL OR user_id = :user_id)` raises
   `psycopg.errors.AmbiguousParameter: could not determine data type of
   parameter $1` — Postgres cannot infer a type for a parameter whose only
   context is `IS NULL`. Reproduced against Postgres 15 through this project's
   own driver, and it failed for a real user_id as well as for None, so the
   function returned [] on *every* call. The blanket `except Exception` turned
   that into an empty task list, which `recalculate_date_range` reports as
   `{"status": "success", "summaries_created": 0}`.

   `repair_reconcile.py` already carried a workaround naming this bug
   ("the TH-424 AmbiguousParameter bug"), treating success-with-zero as a
   failure — a downstream patch for an upstream break.

2. `time <= :end_date` against a caller that passes a bare calendar date
   matched only rows at exactly 00:00:00 and dropped the rest of the last day.
   Measured against real Postgres: 1 row of 3.

These tests need no database. The SQL assertions guard the shape that the live
reproduction proved wrong; the boundary test covers the arithmetic.
"""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta

from .aggregators.sql_aggregator import SQLAggregator

_RAW = inspect.getsource(SQLAggregator._get_tasks_for_user_date_range)

# Comments in this function quote the broken predicate to explain why it was
# changed. Assert against the CODE, not the commentary, or the explanation
# trips the check it exists to justify.
SOURCE = "\n".join(
    line for line in _RAW.split("\n") if not line.strip().startswith("#")
)


def test_the_user_filter_is_cast():
    """A bare `:param IS NULL` cannot be type-inferred and raises at execute time."""
    assert ":user_id IS NULL" not in SOURCE, (
        "an uncast :user_id in an IS NULL predicate raises AmbiguousParameter — "
        "the query fails for every caller, not just user_id=None"
    )
    assert SOURCE.count("CAST(:user_id AS text)") == 4, (
        "expected the cast in both branches of the UNION, on both sides of the OR"
    )


def test_the_range_is_half_open():
    """Mixing `<=` and `<` across one file is how the last day goes missing."""
    assert "time <= :end_date" not in SOURCE
    assert SOURCE.count("time < :end_date") == 2, "both UNION branches"
    assert SOURCE.count("time >= :start_date") == 2


def test_the_end_bound_is_normalized_to_the_next_day():
    """The public contract is inclusive of end_date's day; the SQL is half-open."""
    assert "end_exclusive" in SOURCE, "the half-open bound must be derived, not passed through"
    assert '"end_date": end_exclusive' in SOURCE


def _end_exclusive(end_date: datetime) -> datetime:
    """The expression under test, mirrored so the arithmetic is checkable."""
    return (end_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def test_both_real_callers_get_the_whole_last_day():
    """The two callers pass different shapes; both must land on the same bound.

    `manage_router` sends `datetime.fromisoformat("2026-01-10")` (midnight);
    `repair_reconcile` pads to 23:59:59.999999. Under `<=` those behaved
    completely differently — 1 row versus 3, verified against Postgres.
    """
    bare = datetime(2026, 1, 10)
    padded = datetime(2026, 1, 10, 23, 59, 59, 999999)
    expected = datetime(2026, 1, 11)

    assert _end_exclusive(bare) == expected
    assert _end_exclusive(padded) == expected

    # A timestamp at the bound itself belongs to the next day and must be excluded.
    assert datetime(2026, 1, 10, 23, 59, 59) < expected
    assert not datetime(2026, 1, 11, 0, 0, 0) < expected


def test_month_end_and_leap_day_roll_over():
    assert _end_exclusive(datetime(2026, 1, 31)) == datetime(2026, 2, 1)
    assert _end_exclusive(datetime(2026, 12, 31)) == datetime(2027, 1, 1)
    assert _end_exclusive(datetime(2024, 2, 28)) == datetime(2024, 2, 29)   # leap year
    assert _end_exclusive(datetime(2026, 2, 28)) == datetime(2026, 3, 1)
