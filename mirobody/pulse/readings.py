"""The one writer of `th_series_data` — every reading, whatever brought it in.

Five call sites used to carry their own INSERT for this table: the Apple
Health upload, the aggregation worker, the file parser, `POST /api/records`
and the demo seed. Same table, five column lists, four different
`ON CONFLICT` clauses, and one of the five forgot to `encrypt_content` the
comment. They differed in what they wanted a collision on the
`(user_id, indicator, start_time, end_time)` unique key to mean, and that is
the one thing a caller still says, through `on_conflict`:

- `"update"`         — the new reading replaces the old (device sync,
                       aggregation: the source re-sent the truth).
- `"update_revive"`  — replace AND un-delete (the demo seed: a replay must
                       bring the shared record back whatever the user did to it).
- `"revive_deleted"` — replace only a soft-DELETED copy; a live row is left
                       alone (the file parser: a report re-uploaded after its
                       file was deleted collided with its own deleted rows and
                       wrote nothing while the log said otherwise).
- `"nothing"`        — a collision is a retry, not a duplicate (the records
                       API: re-sending a batch after a timeout).

Columns a caller does not supply take the table's meaning of "unknown": empty
string for the text columns, NULL for `source`/`task_id`/`fhir_id`/
`fhir_mapping_info`. `comment` is always written through `encrypt_content`
— it is free-text health data, and the readers all `decrypt_content` it.
`fhir_id` and `fhir_mapping_info` COALESCE on update so a writer that does
not know the code cannot erase one that did.

## The day columns

`local_date`, `series_key`, `source_class` and `fingerprint` (schema
`a4_series_data_day_authority.sql`) are DERIVED here rather than asked of
every caller, because every caller would derive them differently and the
read side would then have to guess again. The derivation is
`mirobody.kernel.series` + `mirobody.kernel.metrics`, and it is pure — `day_key()` and
`series_key()` below are unit-tested without a database.

The one thing a caller must say is whether its rows are already anchored to
a day. A raw reading carries the instant it was measured, and its day comes
from the metric's window (a sleep stage at 02:00 belongs to the night that
opened at 18:00 the day before). An aggregate row already IS a day: the
aggregator writes it at local 00:00:00–23:59:59 of the day it summarises,
and pushing a sleep summary back through an 18:00 window would move it a day
earlier. `anchored=True` says "the timestamp is the day".

## The gate

Every row passes `mirobody.kernel.quality` before it is bound: `time_gate`
(no start, an end before its start, a span over 36 hours, a start more than a
day in the future) and `value_gate` (a non-finite number, a percentage outside
0–100). Only the physically impossible is rejected — a reference range is a
clinical asset this project does not maintain — and a rejection is a reason
CODE counted in the log, never a value. Rejected rows are dropped, not
quarantined: there is no quarantine table yet (`docs/roadmap.md`).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Literal

from ..kernel import metrics, quality, series, sink
# Resolved at call time through the package attribute, not bound at import:
# `demo/test_member_seed.py` substitutes `mirobody.utils.execute_query` to run
# the seed without a database, and a bound name would bypass the substitute.
from .. import utils
logger = logging.getLogger(__name__)

OnConflict = Literal["update", "update_revive", "revive_deleted", "nothing"]

#: Rows per executemany round trip. 1000 keeps the statement under a megabyte
#: for the Apple Health uploads, which arrive tens of thousands at a time.
BATCH_SIZE = 1000

#: The fields that carry meaning for `fingerprint`. Deliberately NOT
#: `update_time`, `task_id` or the row id: a re-sync that changed nothing must
#: produce the same hash so `sink.changed` can skip it.
FINGERPRINT_FIELDS = ("indicator", "value", "start_time", "end_time", "source", "comment")

#: `task_id` values the aggregation worker stamps on the rows it publishes.
#: Those rows are a computation over other rows, never a measurement.
_AGGREGATE_TASK_IDS = frozenset({"aggregate_indicator", "derived_indicator"})

_REQUIRED = {"user_id", "indicator", "value", "start_time", "end_time"}

_DEFAULTS: dict[str, Any] = {
    "source_table": "",
    "source_table_id": "",
    "comment": "",
    "indicator_id": "",
    "source": None,
    "task_id": None,
    "fhir_id": None,
    "fhir_mapping_info": None,
    "local_date": None,
    "series_key": None,
    "source_class": None,
    "fingerprint": None,
    "elected": False,
}

_INSERT = """
INSERT INTO th_series_data (
    user_id, indicator, value, start_time, end_time, source_table,
    source_table_id, comment, indicator_id, source, task_id,
    fhir_id, fhir_mapping_info, local_date, series_key, source_class,
    fingerprint, elected, create_time, update_time, deleted
) VALUES (
    :user_id, :indicator, :value, :start_time, :end_time, :source_table,
    :source_table_id, encrypt_content(:comment), :indicator_id, :source, :task_id,
    :fhir_id, :fhir_mapping_info, :local_date, :series_key, :source_class,
    :fingerprint, :elected, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0
)
ON CONFLICT (user_id, indicator, start_time, end_time)
"""

# `elected` is deliberately absent from the SET list: election is the
# aggregation pass's decision over the whole day, and a plain re-sync of one
# reading must not silently unpublish the day's authority.
_SET = """DO UPDATE SET
    value = EXCLUDED.value,
    source_table = EXCLUDED.source_table,
    source_table_id = EXCLUDED.source_table_id,
    comment = EXCLUDED.comment,
    source = EXCLUDED.source,
    task_id = EXCLUDED.task_id,
    fhir_id = COALESCE(EXCLUDED.fhir_id, th_series_data.fhir_id),
    fhir_mapping_info = COALESCE(EXCLUDED.fhir_mapping_info, th_series_data.fhir_mapping_info),
    local_date = COALESCE(EXCLUDED.local_date, th_series_data.local_date),
    series_key = COALESCE(EXCLUDED.series_key, th_series_data.series_key),
    source_class = COALESCE(EXCLUDED.source_class, th_series_data.source_class),
    fingerprint = EXCLUDED.fingerprint,
    update_time = CURRENT_TIMESTAMP"""

_ON_CONFLICT: dict[str, str] = {
    "update": _SET,
    "update_revive": _SET + ",\n    deleted = 0",
    "revive_deleted": _SET + ",\n    deleted = 0\nWHERE th_series_data.deleted = 1",
    "nothing": "DO NOTHING",
}


def upsert_sql(on_conflict: OnConflict) -> str:
    """The statement for one `on_conflict` policy. Exposed so a test can read
    it; callers go through `upsert_readings`."""
    return _INSERT + _ON_CONFLICT[on_conflict]


# --- the derived day columns (pure) -----------------------------------------


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00").replace(" ", "T", 1))
        except ValueError:
            return None
    return None


def window_for(indicator: str) -> str:
    """The local-day window of an indicator, `"00:00"` unless the catalogue
    says otherwise. The aggregate writers append the source to the name
    (`totalSleepTime.apple_health`), so the part before the first dot is
    tried too — this is what replaced four `LOWER(indicator) LIKE '%sleep%'`
    clauses, which matched `sleepScore` (a daily score, not a night) and
    missed `remDuration` (a night, not a score)."""
    metric = metrics.METRICS.get(indicator)
    if metric is None and "." in indicator:
        metric = metrics.METRICS.get(indicator.split(".", 1)[0])
    return metric.window if metric else "00:00"


def day_key(indicator: str, start_time: Any, *, anchored: bool = False) -> date | None:
    """The local day a reading belongs to, or `None` when its timestamp is
    unreadable (never a guess, and never `today`).

    `th_series_data.start_time` is naive LOCAL wall clock, so no time zone is
    needed: the window arithmetic is the whole of it. `anchored=True` means
    the row is already a day (the aggregator's 00:00–23:59 summary rows), and
    the window must not be applied a second time.
    """
    dt = _as_datetime(start_time)
    if dt is None:
        return None
    if anchored:
        return dt.date()
    window = window_for(indicator)
    if window == "00:00":
        return dt.date()
    hh, mm = (int(p) for p in window.split(":"))
    return (dt - timedelta(hours=hh, minutes=mm)).date()


def series_key(indicator: str, source: Any) -> str:
    """The physical stream a reading came from: `indicator|source`.

    The composition is pinned by test, not by convention: a reader that
    splits on the wrong separator silently merges two devices' curves. An
    unknown source is the empty right-hand side rather than a missing key, so
    the column is never NULL for a row this writer wrote.
    """
    return f"{indicator}|{source or ''}"


def source_class_for(row: dict[str, Any]) -> str:
    """Which of `series.SOURCE_*` a row is, from what the table already knows.

    A caller that knows better passes `source_class` explicitly — a wearable
    profile field that comes back on every sync is a `profile_echo`, and only
    the provider adapter can tell.
    """
    if str(row.get("task_id") or "") in _AGGREGATE_TASK_IDS:
        return series.SOURCE_AGGREGATOR
    if str(row.get("source_table") or "") == "api":
        return series.SOURCE_MANUAL
    return series.SOURCE_MEASURER


def derive_day_columns(row: dict[str, Any], *, anchored: bool = False) -> dict[str, Any]:
    """The four derived columns for one row, without overwriting anything the
    caller stated. Pure — no clock, no database."""
    out = dict(row)
    if out.get("local_date") is None:
        out["local_date"] = day_key(str(out.get("indicator") or ""), out.get("start_time"), anchored=anchored)
    if out.get("series_key") is None:
        out["series_key"] = series_key(str(out.get("indicator") or ""), out.get("source"))
    if out.get("source_class") is None:
        out["source_class"] = source_class_for(out)
    if out.get("fingerprint") is None:
        out["fingerprint"] = sink.fingerprint(out, FINGERPRINT_FIELDS)
    return out


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def gate(row: dict[str, Any], *, now: datetime | None = None) -> str:
    """The reason code that keeps `row` out of the table, or `""` to let it in.

    `start_time`/`end_time` are naive local wall clock, so `now` is compared
    the same way (`datetime.now()`, naive, local). The value column is text —
    a lab result can be "positive" — so `value_gate` sees only what parses as
    a number, against the catalogue's unit for the indicator.
    """
    start = _as_datetime(row.get("start_time"))
    if start is None:
        return quality.ERR_IMPOSSIBLE_TIME_RANGE
    end = _as_datetime(row.get("end_time"))
    try:
        value: float | None = float(str(row.get("value")).split()[0])
    except (TypeError, ValueError, IndexError):
        value = None
    indicator = str(row.get("indicator") or "")
    metric = metrics.METRICS.get(indicator) or (metrics.METRICS.get(indicator.split(".", 1)[0]) if "." in indicator else None)
    fact = series.Fact(
        metric_key=indicator,
        value_num=value,
        effective_start_ms=_ms(start),
        effective_end_ms=_ms(end) if end is not None else 0,
        unit=metric.unit_ucum if metric else "",
    )
    code = quality.time_gate(fact, _ms(now or datetime.now()))
    if code:
        return code
    return quality.value_gate(value, fact.unit)


async def upsert_readings(
    rows: list[dict[str, Any]],
    *,
    on_conflict: OnConflict,
    anchored: bool = False,
    batch_size: int = BATCH_SIZE,
) -> int:
    """Write `rows` to `th_series_data` in batches; return how many were written.

    Each row needs `user_id, indicator, value, start_time, end_time`; the
    other columns default as the module docstring says. Keys the table does
    not have are an error (SQLAlchemy would otherwise silently ignore them and
    a typo like `fhir_mappinginfo` would drop the unit on the floor). A row
    the quality gate rejects is counted in the log by reason code and not
    written; the return value is the rows that were.

    `anchored=True` for a writer whose rows already ARE days — see the module
    docstring.
    """
    if not rows:
        return 0
    unknown = set().union(*(r.keys() for r in rows)) - _REQUIRED - set(_DEFAULTS)
    if unknown:
        raise ValueError(f"th_series_data has no column(s) {sorted(unknown)}")

    admitted: list[dict[str, Any]] = []
    rejected: dict[str, int] = {}
    now = datetime.now()
    for r in rows:
        code = gate(r, now=now)
        if code:
            rejected[code] = rejected.get(code, 0) + 1
        else:
            admitted.append(r)
    if rejected:
        logger.warning("readings rejected by the quality gate: rejected_count=%d reason_counts=%s",
                       sum(rejected.values()), rejected)
    if not admitted:
        return 0

    sql = upsert_sql(on_conflict)
    params = [derive_day_columns({**_DEFAULTS, **r}, anchored=anchored) for r in admitted]
    for i in range(0, len(params), batch_size):
        await utils.execute_query(sql, params[i:i + batch_size], log_sql=False)
    logger.info(f"{len(params)} readings written to th_series_data ({on_conflict})")
    return len(params)
