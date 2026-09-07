"""Which source a day publishes from — decided once, on the write side.

Two devices measure the same day. A watch reports 7,412 steps, a phone reports
7,890, and both are correct about themselves. Something has to choose, and the
question is where.

It used to be answered on the READ side, twice, in two places that could
disagree: the chat answer and the dashboard could show a person two different
numbers for the same Tuesday. So the choice happens here, once, after
aggregation, and every reader afterwards just filters on `elected`.

The ranking is `mirobody.kernel.series.elect` — the same pure function a consumer
with a different store uses, so what "prefer" means is one implementation and
what to prefer is this application's decision:

* a **measurer** beats a **profile echo**. A scale MEASURES a weight; a
  wearable's profile ECHOES the weight the person typed in months ago, on
  every sync, with today's date. Both look like "a weight reading today", and
  the echo is not one;
* then **coverage** — the source that watched more of the day;
* then **measurement freshness**, which is the measurement instant and never
  the row's update time: an echo is rewritten daily and looks fresh by update
  time while being stale;
* then a **static priority**, read from `th_data_source_priority` — the
  deployment's own list, not a rule of the framework.

A candidate is REJECTED, not merely ranked, when its numbers are impossible:
a total sleep time longer than the night it was measured in is arithmetic, not
opinion (`quality.overcount_suspect` through `series.coverage_bound`). When
every candidate fails, the day keeps whatever it published before and the
rejection reasons are logged — a silently empty day is worse than a stale one.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from ... import units
from ...kernel import metrics, series
from ...utils import execute_query

logger = logging.getLogger(__name__)

#: Ranking criteria, in order. Named here rather than taken from the kernel's
#: default so that changing what this application prefers is a visible diff.
RANKING: tuple[str, ...] = ("source_class", "coverage", "measurement_freshness", "static_priority")

#: How many (indicator, day) cells one pass elects over. A person's day is a
#: few hundred cells; a backfill of a year is a few tens of thousands.
CHUNK = 5_000

#: A duration may exceed its window by this much before it is rejected. Not
#: zero: a night's stages are recorded to the second and a sum of them can
#: round a few seconds past the span.
COVERAGE_TOLERANCE = 1.02


async def elect_day(user_id: str, indicator: str, day: date) -> series.Decision | None:
    """Elect one cell and mark it. `None` when the day holds nothing."""
    candidates, rows_by_source = await _candidates(user_id, indicator, day)
    if not candidates:
        return None
    decision = series.elect(
        candidates,
        ranking=RANKING,
        priorities=await source_priorities(),
        validators=_validators_for(indicator, day),
    )
    if decision.elected is None:
        # Every candidate was rejected, or none carried information. Keep what
        # the day already published; say why, with source ids and reason codes
        # only.
        if decision.blocked:
            # Counts and reason CODES only. The indicator name is not a value
            # but it is still the answer to "what was measured", and a log line
            # that carries `HIVViralLoad` has said something about the person.
            blocked_count = len(decision.blocked)
            blocked_reason = "|".join(sorted({r for rs in decision.blocked.values() for r in rs}))
            logger.warning(
                "no source elected for one day cell: blocked_count=%d reason=%s", blocked_count, blocked_reason
            )
        return decision
    await _mark(user_id, indicator, day, decision.elected.identity, rows_by_source)
    return decision


async def elect_range(user_id: str, start: date, end: date, *, indicators: list[str] | None = None) -> int:
    """Elect every cell of a window. Returns how many cells were decided.

    Idempotent: re-running over a day that has not changed re-elects the same
    source and rewrites the same flag.
    """
    cells = await _cells(user_id, start, end, indicators)
    decided_count = 0
    for indicator, day in cells:
        if await elect_day(user_id, indicator, day) is not None:
            decided_count += 1
    if decided_count:
        logger.info("elected %d day cells for user %s", decided_count, user_id)
    return decided_count


# --- the pieces --------------------------------------------------------------


async def source_priorities() -> list[str]:
    """The deployment's source order, best first. Empty when the table is not
    there — the framework ships no opinion about whose watch is better."""
    try:
        rows = await execute_query(
            "SELECT source FROM th_data_source_priority WHERE is_active ORDER BY priority", {}, log_sql=False
        ) or []
    except Exception as e:
        logger.warning("source priority table unavailable: error_type=%s", type(e).__name__)
        return []
    return [str(r["source"]) for r in rows]


async def _cells(user_id: str, start: date, end: date, indicators: list[str] | None) -> list[tuple[str, date]]:
    params: dict[str, object] = {"uid": str(user_id), "start": start, "end": end, "chunk": CHUNK}
    names = " AND indicator = ANY(:names)" if indicators else ""
    if indicators:
        params["names"] = indicators
    rows = await execute_query(
        f"""
        SELECT indicator, local_date
          FROM th_series_data
         WHERE user_id = :uid AND deleted = 0 AND local_date BETWEEN :start AND :end {names}
         GROUP BY indicator, local_date
        HAVING COUNT(DISTINCT COALESCE(source, '')) > 0
         ORDER BY local_date, indicator
         LIMIT :chunk
        """,
        params,
        log_sql=False,
    ) or []
    return [(str(r["indicator"]), r["local_date"]) for r in rows]


async def _candidates(user_id: str, indicator: str, day: date) -> tuple[list[series.Candidate], dict[str, list[int]]]:
    rows = await execute_query(
        """
        SELECT id, COALESCE(source, '') AS source, COALESCE(source_class, 'measurer') AS source_class,
               value, start_time, end_time
          FROM th_series_data
         WHERE user_id = :uid AND deleted = 0 AND indicator = :indicator AND local_date = :day
         ORDER BY start_time
        """,
        {"uid": str(user_id), "indicator": indicator, "day": day},
        log_sql=False,
    ) or []
    by_source: dict[str, list[dict]] = {}
    for r in rows:
        by_source.setdefault(str(r["source"]), []).append(dict(r))

    candidates: list[series.Candidate] = []
    row_ids: dict[str, list[int]] = {}
    for source, group in by_source.items():
        row_ids[source] = [int(r["id"]) for r in group]
        last = max(group, key=lambda r: r["start_time"])
        candidates.append(
            series.Candidate(
                identity=source,
                source_class=str(group[0]["source_class"] or series.SOURCE_MEASURER),
                values={"value": _number(last["value"])} if _number(last["value"]) is not None else {},
                measured_at_ms=_ms(last["start_time"]),
                coverage_ms=sum(_span_ms(r) for r in group),
            )
        )
    return candidates, row_ids


def _validators_for(indicator: str, day: date) -> list:
    """The checks a candidate must pass. Only impossibilities — "heart rate 190
    is high" is a reference range and this module maintains none.

    The one check that applies here is the coverage bound, and it applies to
    every metric whose VALUE is a duration, not only to the ones aggregated as
    a union of spans: `totalSleepTime` is an `instant`/`last` metric carrying a
    number of seconds, and a duplicate sync reporting forty hours of sleep in a
    twenty-four-hour night is the bug this whole rule exists for.
    """
    metric = metrics.METRICS.get(indicator) or metrics.METRICS.get(indicator.split(".", 1)[0])
    if metric is None:
        return []
    per_unit_ms = _duration_unit_ms(metric.standard_unit or metric.unit_ucum)
    if per_unit_ms is None:
        return []
    start_ms, end_ms = series.day_bounds_ms(day, "UTC", metric.window)
    return [
        series.coverage_bound("value", end_ms - start_ms, per_unit_ms=per_unit_ms, tolerance=COVERAGE_TOLERANCE)
    ]


def _duration_unit_ms(unit: str) -> float | None:
    """Milliseconds in one of `unit`, or `None` when it is not a duration.

    Through `mirobody.units`, so the catalogue's spellings (`seconds`,
    `minutes`, `hours`, `ms`) and any other UCUM time unit resolve the same
    way — a hand-written table here would be a second opinion about what a
    minute is.
    """
    normalized = units.normalize_unit(unit) or unit
    if not normalized or not units.convertible(normalized, "ms"):
        return None
    return units.convert_value(1.0, normalized, "ms")


async def _mark(user_id: str, indicator: str, day: date, winner: str, rows_by_source: dict[str, list[int]]) -> None:
    """One statement: the winning source's rows are the day's authority, every
    other row of that day is not. Written as an assignment rather than an
    "elect the winner" update, so a source that stops winning is un-elected in
    the same breath — a day with two elected rows is not a state this can reach.
    """
    winning_ids = rows_by_source.get(winner) or []
    await execute_query(
        """
        UPDATE th_series_data
           SET elected = (id = ANY(:winning)), update_time = update_time
         WHERE user_id = :uid AND deleted = 0 AND indicator = :indicator AND local_date = :day
           AND elected IS DISTINCT FROM (id = ANY(:winning))
        """,
        {"uid": str(user_id), "indicator": indicator, "day": day, "winning": winning_ids},
        log_sql=False,
    )


def _number(value: object) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _ms(value: object) -> int | None:
    return int(value.timestamp() * 1000) if isinstance(value, datetime) else None


def _span_ms(row: dict) -> float:
    start, end = row.get("start_time"), row.get("end_time")
    if isinstance(start, datetime) and isinstance(end, datetime) and end > start:
        return (end - start).total_seconds() * 1000
    return 0.0


__all__ = ["CHUNK", "COVERAGE_TOLERANCE", "RANKING", "elect_day", "elect_range", "source_priorities"]
