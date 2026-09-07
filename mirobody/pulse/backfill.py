"""Fill the day columns on rows written before `a4_series_data_day_authority.sql`.

`local_date` is derived at write time (`readings.derive_day_columns`), so
every row written from now on carries its day. The rows already in the table
do not, and a reader that meets a NULL there has to fall back to casting the
naive timestamp with a day of padding either side — the `date_padded_naive`
semantics the query layer reports. This pass removes that fallback for the
history.

Two things make it safe to run on every boot:

* it only ever touches `local_date IS NULL`, so a finished table costs one
  cheap index probe and nothing else;
* it groups by the metric's WINDOW rather than by indicator, and there are
  exactly two windows in the catalogue (`00:00` and `18:00`), so the whole
  history is two UPDATE statements — not one per name.

The time zone does not appear anywhere here on purpose: `th_series_data`
stores naive LOCAL wall clock, so the day is wall-clock arithmetic. A store
whose time column were UTC would need the subject's zone and would have to
say which zone it used — see `query.SEMANTICS_*`.
"""

from __future__ import annotations

import logging

from ..kernel import metrics
from .. import utils
logger = logging.getLogger(__name__)

#: How many rows one UPDATE claims. Large enough that the history goes in a
#: handful of statements, small enough that no single transaction locks the
#: table for a user who is reading it.
CHUNK = 20_000


def windows() -> dict[str, list[str]]:
    """window → the catalogue names that use it, plus the `name.source`
    spellings the aggregate writers produce. Only non-default windows are
    listed; everything else takes the plain calendar day."""
    out: dict[str, list[str]] = {}
    for metric in metrics.ROWS:
        if metric.window != "00:00":
            out.setdefault(metric.window, []).append(metric.name)
    return {w: sorted(set(names)) for w, names in out.items()}


_SQL_DEFAULT = """
UPDATE th_series_data SET
    local_date   = start_time::date,
    series_key   = indicator || '|' || COALESCE(source, ''),
    source_class = CASE
        WHEN task_id IN ('aggregate_indicator', 'derived_indicator') THEN 'aggregator'
        WHEN source_table = 'api' THEN 'manual'
        ELSE 'measurer' END
WHERE id IN (
    SELECT id FROM th_series_data
     WHERE local_date IS NULL AND start_time IS NOT NULL
       AND NOT (split_part(indicator, '.', 1) = ANY(:windowed))
     LIMIT :chunk
)
"""

# A windowed row is filed under the day its window OPENED: a sleep stage at
# 02:00 belongs to the night that started at 18:00 the day before. Rows the
# aggregator already anchored to local 00:00 are excluded — they ARE days.
_SQL_WINDOWED = """
UPDATE th_series_data SET
    local_date   = (start_time - (:offset)::interval)::date,
    series_key   = indicator || '|' || COALESCE(source, ''),
    source_class = CASE
        WHEN task_id IN ('aggregate_indicator', 'derived_indicator') THEN 'aggregator'
        WHEN source_table = 'api' THEN 'manual'
        ELSE 'measurer' END
WHERE id IN (
    SELECT id FROM th_series_data
     WHERE local_date IS NULL AND start_time IS NOT NULL
       AND split_part(indicator, '.', 1) = ANY(:windowed)
       AND NOT (task_id IN ('aggregate_indicator', 'derived_indicator'))
     LIMIT :chunk
)
"""

# The aggregate rows of a windowed metric: already anchored, plain date cast.
_SQL_WINDOWED_ANCHORED = """
UPDATE th_series_data SET
    local_date   = start_time::date,
    series_key   = indicator || '|' || COALESCE(source, ''),
    source_class = 'aggregator'
WHERE id IN (
    SELECT id FROM th_series_data
     WHERE local_date IS NULL AND start_time IS NOT NULL
       AND split_part(indicator, '.', 1) = ANY(:windowed)
       AND task_id IN ('aggregate_indicator', 'derived_indicator')
     LIMIT :chunk
)
"""


async def backfill_day_columns(*, chunk: int = CHUNK, max_chunks: int = 500) -> int:
    """Fill `local_date`/`series_key`/`source_class` where they are NULL.

    Returns how many rows were touched. `max_chunks` bounds one boot's work
    so a very large history is filled over several starts rather than
    delaying one indefinitely; the next start picks up where this stopped.
    """
    by_window = windows()
    windowed = sorted({n for names in by_window.values() for n in names})
    total = 0

    passes: list[tuple[str, dict[str, object]]] = [(_SQL_DEFAULT, {"windowed": windowed})]
    passes += [
        (_SQL_WINDOWED, {"windowed": sorted(names), "offset": f"{window}:00"})
        for window, names in by_window.items()
    ]
    passes.append((_SQL_WINDOWED_ANCHORED, {"windowed": windowed}))

    for sql, params in passes:
        for _ in range(max_chunks):
            result = await utils.execute_query(sql, {**params, "chunk": chunk}, log_sql=False)
            touched = _row_count(result)
            total += touched
            if touched < chunk:
                break

    if total:
        logger.info("day columns backfilled on %d rows", total)
    return total


def _row_count(result: object) -> int:
    """`execute_query` answers a DML with `{"record_count": n}`; a driver that
    answers something else counts as "nothing left to do" rather than looping."""
    if isinstance(result, dict):
        try:
            return int(result.get("record_count") or 0)
        except (TypeError, ValueError):
            return 0
    return 0
