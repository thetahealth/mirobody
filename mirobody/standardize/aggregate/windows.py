"""Which local day a reading belongs to, asked of the catalogue instead of the name.

The trigger queries used to split their input in two with
`LOWER(indicator) LIKE '%sleep%'`: everything matching was filed on an
18:00–18:00 day (so a night is one day), everything else on 00:00–24:00. The
predicate is a guess about a name, and the catalogue knows the answer:

* it MATCHES 58 `daily…Sleep…` metrics that are `provider_daily`: a vendor's
  own daily figure, already dated by the vendor. Re-anchoring those to an
  18:00 window moved every one of them a day;
* it MISSES `napDuration`, which is a real interval and does belong to the
  night that opened at 18:00.

`metrics.METRICS[name].window` is the one place that decision lives, and this
module turns it into the two things the SQL needs: the list of names each
window covers, and the expression that computes the day for it.

The SQL is generated rather than written out because the catalogue may
someday carry a third window. It refuses to generate more than
:data:`MAX_WINDOWS` branches: a UNION per window is fine at two and a
performance question at ten, and silently emitting ten is not this module's
call to make.
"""

from __future__ import annotations

from mirobody.kernel import metrics
#: The default window: a plain calendar day.
MIDNIGHT = "00:00"

#: How many distinct windows the generated SQL will branch over.
MAX_WINDOWS = 4


def windowed_names() -> dict[str, tuple[str, ...]]:
    """window → the catalogue names it covers, excluding the default one."""
    out: dict[str, set[str]] = {}
    for metric in metrics.ROWS:
        if metric.window != MIDNIGHT:
            out.setdefault(metric.window, set()).add(metric.name)
    return {w: tuple(sorted(names)) for w, names in sorted(out.items())}


#: A zone column holding a fixed offset, as `series.offset_name` spells one.
_OFFSET = "{tz} ~ '^UTC[+-]'"


def local_expression(utc: str, tz: str) -> str:
    """SQL for the wall clock of the naive UTC timestamp `utc` in zone `tz`.

    `tz` is an IANA name or `UTC±HH:MM`. Postgres reads a bare offset with the
    POSIX sign, west positive: `AT TIME ZONE 'UTC+08:00'` is eight hours BEHIND
    UTC (PostgreSQL 17), so an offset is added as an interval instead, as
    `collect/query.py` does for observations.
    """
    return (f"(CASE WHEN {_OFFSET.format(tz=tz)} THEN {utc} + substr({tz}, 4)::interval"
            f" ELSE ({utc} AT TIME ZONE 'UTC') AT TIME ZONE {tz} END)")


def utc_expression(local: str, tz: str) -> str:
    """SQL for the naive UTC timestamp of the wall clock `local` in zone
    `tz`: the inverse of :func:`local_expression`."""
    return (f"(CASE WHEN {_OFFSET.format(tz=tz)} THEN {local} - substr({tz}, 4)::interval"
            f" ELSE ({local} AT TIME ZONE {tz}) AT TIME ZONE 'UTC' END)")


def day_begin_expression(window: str, time_column: str = "time", tz_column: str = "timezone") -> str:
    """SQL for "the UTC instant this reading's local day began".

    `series_data.time` is a UTC timestamp and `timezone` the zone the reading
    was taken in, so the day is: read the instant in the zone, step back by
    the window, take the date, put the window's clock time on it, and convert
    back. Wall-clock arithmetic in the database, for the same reason
    `series.day_bounds_ms` does it on the wall clock in Python: on a
    daylight-saving day, adding 24 hours to the start gives the wrong end.
    """
    local = local_expression(time_column, tz_column)
    if window == MIDNIGHT:
        return utc_expression(f"({local}::date::text || ' 00:00:00')::timestamp", tz_column)
    hours, minutes = (int(p) for p in window.split(":"))
    shifted = f"({local} - INTERVAL '{hours} hours {minutes} minutes')"
    return utc_expression(f"({shifted}::date::text || ' {window}:00')::timestamp", tz_column)


def branches(indicator_column: str = "indicator") -> list[tuple[str, str, dict[str, object]]]:
    """One `(window, predicate, params)` per branch of the trigger UNION.

    The last branch is the default one and its predicate is the negation of
    every other, so the branches partition the input: a reading is counted
    once, whatever the catalogue says about it. A name the catalogue does not
    know lands in the default branch: an unknown metric is still a reading,
    and a plain calendar day is the honest guess for it.
    """
    by_window = windowed_names()
    if len(by_window) + 1 > MAX_WINDOWS:
        raise ValueError(
            f"the catalogue carries {len(by_window)} non-default day windows; "
            f"the trigger query branches over at most {MAX_WINDOWS}"
        )
    out: list[tuple[str, str, dict[str, object]]] = []
    covered: list[str] = []
    for i, (window, names) in enumerate(by_window.items()):
        key = f"window_names_{i}"
        out.append((window, f"{indicator_column} = ANY(:{key})", {key: list(names)}))
        covered.append(key)
    negation = " AND ".join(f"NOT ({indicator_column} = ANY(:{k}))" for k in covered) or "TRUE"
    out.append((MIDNIGHT, negation, {}))
    return out


def branch_params() -> dict[str, object]:
    """Every parameter :func:`branches` needs, in one dict to merge into the
    query's own."""
    params: dict[str, object] = {}
    for _window, _predicate, extra in branches():
        params.update(extra)
    return params


__all__ = [
    "MAX_WINDOWS",
    "MIDNIGHT",
    "branch_params",
    "branches",
    "day_begin_expression",
    "local_expression",
    "utc_expression",
    "windowed_names",
]
