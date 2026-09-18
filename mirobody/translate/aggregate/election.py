"""Which stream a day publishes from: decided once, on the write side.

Two devices measure the same day. A watch reports 7,412 steps, a phone reports
7,890, and both are correct about themselves. Something has to choose, and the
question is where.

It used to be answered on the READ side, twice, in two places that could
disagree: the chat answer and the dashboard could show a person two different
numbers for the same Tuesday. So the choice happens here, once, after a
write, and lands in `th_day_authority`: one row per (person, series, local
day) naming the observation that is the day's published value. Every reader
joins that table; none re-decides.

The fact table is never touched. `th_series_data.elected` was an UPDATE on
the hot table after every aggregation pass, and that second write is where
the production deadlocks on it came from; an authority row in its own table
is an upsert nothing else contends for.

The ranking is `mirobody.kernel.series.elect`: the same pure function a
consumer with a different store uses, so what "prefer" means is one
implementation and what to prefer is this application's decision:

* a **measurer** beats a **profile echo**. A scale MEASURES a weight; a
  wearable's profile ECHOES the weight the person typed in months ago, on
  every sync, with today's date. Both look like "a weight reading today";
* then **coverage**: the stream that watched more of the day;
* then **measurement freshness**, which is the measurement instant and never
  the row's write time;
* then a **static priority**, read from `th_data_source_priority`: the
  deployment's own list, not a rule of the framework.

A candidate is REJECTED, not merely ranked, when its numbers are impossible:
a total sleep time longer than the night it was measured in is arithmetic,
not opinion. A rejection is a row in `th_check_result`, so an assistant can
see it; when every candidate fails, the day keeps whatever it published
before.
"""

from __future__ import annotations

import logging
from datetime import date, datetime

from mirobody import units
from mirobody.kernel import metrics, series
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)

#: Ranking criteria, in order. Named here rather than taken from the kernel's
#: default so that changing what this application prefers is a visible diff.
RANKING: tuple[str, ...] = ("source_class", "coverage", "measurement_freshness", "static_priority")
RULE = "|".join(RANKING)

#: How many (series, day) cells one pass elects over.
CHUNK = 5_000

#: A duration may exceed its window by this much before it is rejected. Not
#: zero: a night's stages are recorded to the second and a sum of them can
#: round a few seconds past the span.
COVERAGE_TOLERANCE = 1.02

CHECK_SCOPE = "day-cell"
CHECK_RULE = "coverage-bound"

_CELLS = """
SELECT series_id, local_date
  FROM v_observation
 WHERE user_id = :uid AND local_date BETWEEN :start AND :end AND value_num IS NOT NULL {names}
 GROUP BY series_id, local_date
 ORDER BY local_date, series_id
 LIMIT :chunk
"""

_CANDIDATES = """
SELECT id, stream_key, source_class, vendor, name_text, value_num, value_canonical, observed_start, observed_end
  FROM v_observation
 WHERE user_id = :uid AND series_id = :series_id AND local_date = :day AND value_num IS NOT NULL
 ORDER BY observed_start, id
"""

_MARK = """
INSERT INTO th_day_authority (user_id, series_id, local_date, observation_id, rule, candidates)
VALUES (:uid, :series_id, :day, :observation_id, :rule, :candidates)
ON CONFLICT (user_id, series_id, local_date) DO UPDATE
   SET observation_id = EXCLUDED.observation_id, rule = EXCLUDED.rule,
       candidates = EXCLUDED.candidates, decided_at = now()
 WHERE th_day_authority.observation_id IS DISTINCT FROM EXCLUDED.observation_id
    OR th_day_authority.candidates <> EXCLUDED.candidates
"""

_CHECK_FAIL = """
INSERT INTO th_check_result (user_id, scope, series_id, local_date, rule, verdict, detail)
VALUES (:uid, :scope, :series_id, :day, :rule, 'fail', CAST(:detail AS jsonb))
ON CONFLICT (user_id, scope, COALESCE(observation_id, 0), COALESCE(series_id, ''),
             COALESCE(local_date, DATE '0001-01-01'), rule)
DO UPDATE SET verdict = 'fail', detail = EXCLUDED.detail, checked_at = now()
"""

_CHECK_CLEAR = """
DELETE FROM th_check_result
 WHERE user_id = :uid AND scope = :scope AND series_id = :series_id AND local_date = :day AND rule = :rule
"""


async def elect_day(user_id: str, series_id: str, day: date) -> series.Decision | None:
    """Elect one cell and record it. `None` when the day holds nothing."""
    candidates, last_row = await _candidates(user_id, series_id, day)
    if not candidates:
        return None
    priorities = await source_priorities()
    ordered = [sk for vendor in priorities for sk in last_row if sk.endswith(f":{vendor}")]
    name = next(iter(last_row.values()))["name_text"] if last_row else ""
    decision = series.elect(
        candidates, ranking=RANKING, priorities=ordered, validators=_validators_for(name, day),
    )
    params = {"uid": str(user_id), "series_id": series_id, "day": day}
    if decision.elected is None:
        if decision.blocked:
            # Reason CODES and counts only: the series id names what was
            # measured, and that is already in the row this writes.
            await execute_query(_CHECK_FAIL, {
                **params, "scope": CHECK_SCOPE, "rule": CHECK_RULE,
                "detail": _json({"blocked": dict(decision.blocked)}),
            }, log_sql=False)
            logger.warning("no stream elected for one day cell: blocked_count=%d", len(decision.blocked))
        return decision
    await execute_query(_CHECK_CLEAR, {**params, "scope": CHECK_SCOPE, "rule": CHECK_RULE}, log_sql=False)
    await execute_query(_MARK, {
        **params,
        "observation_id": int(last_row[decision.elected.identity]["id"]),
        "rule": RULE,
        "candidates": len(candidates),
    }, log_sql=False)
    return decision


async def elect_range(user_id: str, start: date, end: date, *, series_ids: list[str] | None = None) -> int:
    """Elect every cell of a window. Returns how many cells were decided.

    Idempotent: re-running over a day that has not changed re-elects the same
    stream and the upsert changes nothing.
    """
    cells = await _cells(user_id, start, end, series_ids)
    decided = 0
    for series_id, day in cells:
        if await elect_day(user_id, series_id, day) is not None:
            decided += 1
    if decided:
        logger.info("elected %d day cells for one subject", decided)
    return decided


# --- the pieces --------------------------------------------------------------


async def source_priorities() -> list[str]:
    """The deployment's vendor order, best first. Empty when the table is not
    there: the framework ships no opinion about whose watch is better."""
    try:
        rows = await execute_query(
            "SELECT source FROM th_data_source_priority WHERE is_active ORDER BY priority", {}, log_sql=False
        ) or []
    except Exception as e:
        logger.warning("source priority table unavailable: error_type=%s", type(e).__name__)
        return []
    return [str(r["source"]) for r in rows]


async def _cells(user_id: str, start: date, end: date, series_ids: list[str] | None) -> list[tuple[str, date]]:
    params: dict[str, object] = {"uid": str(user_id), "start": start, "end": end, "chunk": CHUNK}
    names = ""
    if series_ids:
        names = " AND series_id = ANY(:names)"
        params["names"] = list(series_ids)
    rows = await execute_query(_CELLS.format(names=names), params, log_sql=False) or []
    return [(str(r["series_id"]), r["local_date"]) for r in rows]


async def _candidates(user_id: str, series_id: str, day: date) -> tuple[list[series.Candidate], dict[str, dict]]:
    rows = await execute_query(_CANDIDATES, {"uid": str(user_id), "series_id": series_id, "day": day}, log_sql=False) or []
    by_stream: dict[str, list[dict]] = {}
    for r in rows:
        by_stream.setdefault(str(r["stream_key"]), []).append(dict(r))
    candidates: list[series.Candidate] = []
    last_row: dict[str, dict] = {}
    for stream, group in by_stream.items():
        last = max(group, key=lambda r: (r["observed_start"], r["id"]))
        last_row[stream] = last
        value = last["value_canonical"] if last["value_canonical"] is not None else last["value_num"]
        candidates.append(series.Candidate(
            identity=stream,
            source_class=str(group[0]["source_class"] or series.SOURCE_MEASURER),
            values={"value": float(value)} if value is not None else {},
            measured_at_ms=_ms(last["observed_start"]),
            coverage_ms=sum(_span_ms(r) for r in group),
        ))
    return candidates, last_row


def _validators_for(name: str, day: date) -> list:
    """The checks a candidate must pass. Only impossibilities: "heart rate 190
    is high" is a reference range and this module maintains none.

    The coverage bound applies to every metric whose VALUE is a duration:
    a duplicate sync reporting forty hours of sleep in a twenty-four-hour
    night is the bug this rule exists for.
    """
    head = name.split(".", 1)[0]
    metric = metrics.METRICS.get(head)
    if metric is None:
        return []
    per_unit_ms = _duration_unit_ms(metric.standard_unit or metric.unit_ucum)
    if per_unit_ms is None:
        return []
    start_ms, end_ms = series.day_bounds_ms(day, "UTC", metric.window)
    return [series.coverage_bound("value", end_ms - start_ms, per_unit_ms=per_unit_ms, tolerance=COVERAGE_TOLERANCE)]


def _duration_unit_ms(unit: str) -> float | None:
    """Milliseconds in one of `unit`, or `None` when it is not a duration."""
    normalized = units.normalize_unit(unit) or unit
    if not normalized or not units.convertible(normalized, "ms"):
        return None
    return units.convert_value(1.0, normalized, "ms")


def _json(value: object) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, default=str)


def _ms(value: object) -> int | None:
    return int(value.timestamp() * 1000) if isinstance(value, datetime) else None


def _span_ms(row: dict) -> float:
    start, end = row.get("observed_start"), row.get("observed_end")
    if isinstance(start, datetime) and isinstance(end, datetime) and end > start:
        return (end - start).total_seconds() * 1000
    return 0.0


__all__ = ["CHUNK", "COVERAGE_TOLERANCE", "RANKING", "RULE", "elect_day", "elect_range", "source_priorities"]
