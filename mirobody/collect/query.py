"""The read side of the observation model: one read authority, in SQL.

It lives beside `observations.py`, the one WRITER of the same tables,
because the tables are what the two share: a reader that guesses where a day
begins while the writer stores one is the whole class of bug this pair
exists to prevent. The agent layer holds the TOOL
(`agent/tools/health_indicators_service.py`) and this is the implementation
it is handed.

Every surface that shows a person their own readings goes through this
class: the chat agent's tool, an MCP client, the web client's Indicators
tab, a daily summary. Every query reads `v_observation`, which already hides
amended and retracted rows and carries the coding, and nothing else: no
reader re-derives a day, a unit or an identity.

## What an "indicator" is now

The unit of comparison is the SERIES (`translate.series`): every cholesterol
reading of this person, in mg/dL or mmol/L, from a file or typed in, is one
series with one `series_id`. The catalogue lists series; a selection by
`indicators` accepts a series id, a LOINC code, the series' display name or
a name as printed on a report, so a model can copy any of them back.

## Units

A series may hold readings in two units (5.6 mmol/L and 100 mg/dL are one
series). A statistic over such a series is computed over the canonical
values, in the canonical unit, and says so; over a single-unit series it is
computed in that unit. Raw rows always carry both the value as printed and
its canonical pair.

## Election

For `resolution=day` and coarser, the answer is the day's published authority:
the observation `th_day_authority` names for that (series, local day), or the
newest reading of the day where nothing has been elected, and `provenance`
says which. Election happens once, on the write side
(`translate/aggregate/election.py`), which is what keeps the chat answer and
the dashboard identical by construction rather than by two implementations
agreeing.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from mirobody import translate
from mirobody.kernel import query

logger = logging.getLogger(__name__)

#: Names listed for the MODEL: a token budget. The browser gets `REST_CATALOG_MAX`.
CATALOG_MAX = 200
REST_CATALOG_MAX = 2000

#: How many series one keyword search may expand to.
MAX_KEYWORD_NAMES = 12

_SUBDAY_TRUNC = {"minute": "minute", "hour": "hour"}
_DAY_TRUNC = {"day": "day", "week": "week", "month": "month"}

#: The local wall clock of a row. `tz` is an IANA name or `UTC±HH:MM`;
#: Postgres reads a bare `UTC+8` with the POSIX sign (west positive), so an
#: offset is applied as an interval instead of handed to AT TIME ZONE.
_LOCAL_TS = (
    "CASE WHEN o.tz ~ '^UTC[+-]' THEN (o.observed_start AT TIME ZONE 'UTC') + (substr(o.tz, 4))::interval"
    " ELSE o.observed_start AT TIME ZONE o.tz END"
)

#: The number a statistic runs over, per group: the printed values when the
#: group has one unit, the canonical values when it has more.
_STAT_VALUE = "CASE WHEN COUNT(DISTINCT unit_ucum) > 1 THEN {agg}(value_canonical) ELSE {agg}(value_num) END"
_STAT_UNIT = "CASE WHEN COUNT(DISTINCT unit_ucum) > 1 THEN MAX(unit_canonical) ELSE (ARRAY_AGG(unit_ucum ORDER BY at DESC))[1] END"

_FILE_KEY = "CASE WHEN o.source_kind = 'file' THEN substr(o.source_ref, 10) END"

#: The document a reading came from, by NAME. Handed only the key, the model
#: cited "web_uploads/17eaf4f6-…-edbee3267ea6.pdf" as the source of a value.
#: `file_name` is encrypted; the key may carry a legacy `_#_<page>` suffix.
_FILE_JOIN = """LEFT JOIN th_files f
                         ON o.source_kind = 'file'
                        AND f.file_key = split_part(substr(o.source_ref, 10), '_#_', 1)"""
_FILE_NAME = "decrypt_content(f.file_name) AS file_name"

#: The identity a group reports: that of its most recent row, so a series
#: holding two codes (mass and molar cholesterol) names one consistent pair.
_LATEST_IDENTITY = (
    "(ARRAY_AGG(display ORDER BY at DESC))[1] AS display,"
    " (ARRAY_AGG(code_system ORDER BY at DESC))[1] AS code_system,"
    " (ARRAY_AGG(code ORDER BY at DESC))[1] AS code"
)


class PostgresHealthQuery:
    """`query.HealthQuery` over `v_observation`.

    Async: the port is declared with plain `def` so an in-memory implementation
    stays possible, and `HealthIndicatorsService` awaits whatever it gets back.
    """

    # --- subject-level facts -------------------------------------------------

    async def tz(self, subject_id: str) -> str:
        """The subject's IANA zone, `"UTC"` when unset. Never the server's zone
        and never the session's: a window is the person's day."""
        from mirobody.collect.observations import user_tz
        return await user_tz(subject_id)

    async def on_read(self, subject_id: str) -> None:
        """The read-time refresh seam: a no-op here, because this deployment
        aggregates in a background worker. It stays on the port for a
        consumer that recomputes dirty windows before answering."""
        return None

    # --- the six leaves ------------------------------------------------------

    async def catalog(self, subject_id: str, window: query.Window | None, *, cap: int = CATALOG_MAX) -> list[dict]:
        """What this person actually has, one row per series.

        `total` rides on every row (a window function runs before LIMIT), so a
        truncated page can say how much it left out.
        """
        from mirobody.utils import execute_query

        params: dict[str, Any] = {"uid": str(subject_id), "cap": cap}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT o.series_id,
                   o.series_id NOT LIKE 'local:%' AS standard,
                   (ARRAY_AGG(o.code_system ORDER BY o.observed_start DESC))[1] AS code_system,
                   (ARRAY_AGG(o.code ORDER BY o.observed_start DESC))[1] AS code,
                   COALESCE((ARRAY_AGG(o.display ORDER BY o.observed_start DESC))[1],
                            (ARRAY_AGG(o.name_text ORDER BY o.observed_start DESC))[1]) AS display,
                   (ARRAY_AGG(o.name_text ORDER BY o.observed_start DESC))[1] AS name,
                   MAX(o.kind) AS kind,
                   COUNT(*) AS count,
                   COUNT(*) OVER () AS total,
                   to_char(MIN(o.local_date), 'YYYY-MM-DD') AS first_date,
                   to_char(MAX(o.local_date), 'YYYY-MM-DD') AS last_date,
                   (ARRAY_AGG(o.value_text ORDER BY o.observed_start DESC))[1] AS latest_value,
                   (ARRAY_AGG(o.unit_text ORDER BY o.observed_start DESC))[1] AS unit,
                   MAX(o.reason) AS reason
              FROM v_observation o
             WHERE o.user_id = :uid {where}
             GROUP BY o.series_id
             ORDER BY display
             LIMIT :cap
            """,
            params,
            log_sql=False,
        ) or []
        return [_catalog_row(r) for r in rows]

    async def readings(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, limit: int
    ) -> list[dict]:
        """Individual readings, newest first, capped per series. `total` per
        series comes from the same statement. `file_key` is the handle back
        to the ORIGINAL report."""
        from mirobody.utils import execute_query

        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {"uid": str(subject_id), "names": names, "limit": max(1, min(int(limit), query.MAX_LIMIT))}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT * FROM (
                SELECT o.id, o.series_id, o.display, o.name_text, o.value_text, o.unit_text,
                       o.value_num, o.value_canonical, o.unit_canonical, o.comparator,
                       to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI:SS') AS local_time,
                       o.tz, o.local_date, o.modality, o.code_system, o.code, o.elected, o.outcome,
                       {_FILE_KEY} AS file_key, {_FILE_NAME},
                       COUNT(*) OVER (PARTITION BY o.series_id) AS total,
                       ROW_NUMBER() OVER (PARTITION BY o.series_id ORDER BY o.observed_start DESC, o.id DESC) AS rn
                  FROM v_observation o
                {_FILE_JOIN}
                 WHERE o.user_id = :uid AND o.series_id = ANY(:names) {where}
            ) s
            WHERE rn <= :limit
            ORDER BY series_id, local_time DESC
            """,
            params,
            log_sql=False,
        ) or []
        return [_reading_row(r) for r in rows]

    async def buckets(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, resolution: str
    ) -> list[dict]:
        """One point per bucket. Day and coarser read the day authority."""
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        if resolution in _SUBDAY_TRUNC:
            rows = await self._subday_buckets(subject_id, names, window, resolution)
        else:
            rows = await self._day_buckets(subject_id, names, window, resolution)
        return [_bucket_row(r) for r in rows]

    async def stats(self, subject_id: str, sel: query.Selection, window: query.Window, *, basis: str) -> list[dict]:
        """count/min/max/avg/first/last/change per series over the WHOLE
        window, in SQL. `basis="daily"` averages the day authorities, which is
        what "my average resting heart rate this month" means."""
        from mirobody.utils import execute_query

        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {"uid": str(subject_id), "names": names}
        where = _window_clause(params, window)
        source = _DAY_AUTHORITY_CTE.format(where=where) if basis == "daily" else _READINGS_CTE.format(where=where)
        rows = await execute_query(
            f"""
            {source}
            SELECT series_id,
                   {_LATEST_IDENTITY},
                   COUNT(*) AS count,
                   COUNT(value_num) AS numeric_count,
                   {_STAT_VALUE.format(agg="MIN")} AS min,
                   {_STAT_VALUE.format(agg="MAX")} AS max,
                   ROUND(({_STAT_VALUE.format(agg="AVG")})::numeric, 4) AS avg,
                   (ARRAY_AGG(value_text ORDER BY at ASC))[1] AS first,
                   to_char(MIN(at), 'YYYY-MM-DD') AS first_date,
                   (ARRAY_AGG(value_text ORDER BY at DESC))[1] AS last,
                   to_char(MAX(at), 'YYYY-MM-DD') AS last_date,
                   {_STAT_UNIT} AS unit,
                   COUNT(DISTINCT unit_ucum) > 1 AS mixed_units,
                   (ARRAY_AGG(value_num ORDER BY at ASC))[1] AS first_num,
                   (ARRAY_AGG(value_num ORDER BY at DESC))[1] AS last_num
              FROM base
             GROUP BY series_id
             ORDER BY display
            """,
            params,
            log_sql=False,
        ) or []
        return [_stats_row(r, basis) for r in rows]

    async def latest(self, subject_id: str, sel: query.Selection, window: query.Window, *, basis: str) -> list[dict]:
        """The most recent value per series INSIDE the window."""
        from mirobody.utils import execute_query

        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {"uid": str(subject_id), "names": names}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT DISTINCT ON (o.series_id)
                   o.series_id, o.display, o.name_text, o.value_text, o.unit_text, o.value_num,
                   o.value_canonical, o.unit_canonical, o.code_system, o.code, o.local_date, o.elected, o.modality,
                   to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI:SS') AS local_time
              FROM v_observation o
             WHERE o.user_id = :uid AND o.series_id = ANY(:names) {where}
             ORDER BY o.series_id, o.elected DESC, o.observed_start DESC, o.id DESC
            """,
            params,
            log_sql=False,
        ) or []
        return [_latest_row(r, basis) for r in rows]

    # --- helpers -------------------------------------------------------------

    async def _resolve(self, subject_id: str, sel: query.Selection, window: query.Window | None) -> list[str]:
        """A `Selection` to the series ids this person HAS. Never the global
        vocabulary: "do I have HbA1c" is about this person's record."""
        if sel.indicators:
            return await self._by_names(subject_id, list(sel.indicators))
        if sel.keywords:
            return await self._by_keywords(subject_id, sel.keywords, window)
        return []

    async def _by_names(self, subject_id: str, names: list[str]) -> list[str]:
        """Exact selectors: a series id, a code, a display name or a printed name."""
        from mirobody.utils import execute_query

        rows = await execute_query(
            """
            SELECT DISTINCT o.series_id
              FROM v_observation o
             WHERE o.user_id = :uid
               AND (o.series_id = ANY(:names) OR o.code = ANY(:names)
                    OR lower(o.display) = ANY(:lower) OR o.name_key = ANY(:keys))
            """,
            {
                "uid": str(subject_id),
                "names": names,
                "lower": [n.lower() for n in names],
                "keys": [translate.name_key(n) for n in names],
            },
            log_sql=False,
        ) or []
        return [str(r["series_id"]) for r in rows]

    async def _labels(self, subject_id: str, window: query.Window | None) -> list[tuple[str, str, str]]:
        """`(label, series_id, code)` for every display and printed name."""
        from mirobody.utils import execute_query

        params: dict[str, Any] = {"uid": str(subject_id)}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"SELECT o.series_id, o.name_text, o.display, o.code FROM v_observation o"
            f" WHERE o.user_id = :uid {where} GROUP BY o.series_id, o.name_text, o.display, o.code",
            params,
            log_sql=False,
        ) or []
        out: list[tuple[str, str, str]] = []
        for r in rows:
            sid, code = str(r["series_id"]), str(r["code"] or "")
            out.append((str(r["name_text"]), sid, code))
            if r["display"]:
                out.append((str(r["display"]), sid, code))
        return out

    async def _by_keywords(self, subject_id: str, keywords: tuple[str, ...], window: query.Window | None) -> list[str]:
        """Free text to the person's own series, in two tiers: a lexical rank
        over their printed and display names (free, deterministic, scoped to
        what they have), then the offline resolver's code matched against
        their codes, which reaches the same place with no key at all."""
        kws = [k.strip() for k in keywords if k and k.strip()]
        if not kws:
            return []
        labels = await self._labels(subject_id, window)
        by_label: dict[str, str] = {}
        for label, sid, _code in labels:
            by_label.setdefault(label, sid)
        ranked: list[str] = []
        for kw in kws:
            for label in query.rank_catalog(kw, list(by_label), limit=MAX_KEYWORD_NAMES):
                ranked.append(by_label[label])
        if ranked:
            return list(dict.fromkeys(ranked))[:MAX_KEYWORD_NAMES]
        codes: set[str] = set()
        try:
            from mirobody.engine import resolve
            for kw in kws:
                hit = resolve(kw)
                if hit.resolved and hit.loinc and hit.method == "lexical":
                    codes.add(hit.loinc)
        except Exception as e:
            logger.warning("offline resolver unavailable in keyword recall: error_type=%s", type(e).__name__)
        if not codes:
            return []
        return list(dict.fromkeys(sid for _label, sid, code in labels if code in codes))[:MAX_KEYWORD_NAMES]

    async def _subday_buckets(self, subject_id: str, names: list[str], window: query.Window, resolution: str) -> list[dict]:
        from mirobody.utils import execute_query

        params: dict[str, Any] = {"uid": str(subject_id), "names": names}
        where = _window_clause(params, window)
        trunc = _SUBDAY_TRUNC[resolution]
        return await execute_query(
            f"""
            WITH base AS (
                SELECT o.series_id, o.display, o.code_system, o.code, o.value_num, o.value_canonical,
                       o.unit_ucum, o.unit_canonical, date_trunc('{trunc}', {_LOCAL_TS}) AS at
                  FROM v_observation o
                 WHERE o.user_id = :uid AND o.series_id = ANY(:names) AND o.value_num IS NOT NULL {where}
            )
            SELECT series_id, {_LATEST_IDENTITY},
                   to_char(at, 'YYYY-MM-DD HH24:MI') AS period,
                   COUNT(*) AS n,
                   ROUND(({_STAT_VALUE.format(agg="AVG")})::numeric, 4) AS avg,
                   {_STAT_VALUE.format(agg="MIN")} AS min,
                   {_STAT_VALUE.format(agg="MAX")} AS max,
                   {_STAT_UNIT} AS unit,
                   false AS elected
              FROM base
             GROUP BY series_id, at
             ORDER BY series_id, period
            """,
            params,
            log_sql=False,
        ) or []

    async def _day_buckets(self, subject_id: str, names: list[str], window: query.Window, resolution: str) -> list[dict]:
        """Day and coarser: one row per (series, bucket), from the day
        authority where one has been elected and the newest reading of the
        day otherwise, which `provenance` reports."""
        from mirobody.utils import execute_query

        params: dict[str, Any] = {"uid": str(subject_id), "names": names}
        where = _window_clause(params, window)
        trunc = _DAY_TRUNC[resolution]
        return await execute_query(
            f"""
            {_DAY_AUTHORITY_CTE.format(where=where)}
            SELECT series_id, {_LATEST_IDENTITY},
                   to_char(date_trunc('{trunc}', at::timestamp), 'YYYY-MM-DD') AS period,
                   COUNT(*) AS n,
                   ROUND(({_STAT_VALUE.format(agg="AVG")})::numeric, 4) AS avg,
                   {_STAT_VALUE.format(agg="MIN")} AS min,
                   {_STAT_VALUE.format(agg="MAX")} AS max,
                   {_STAT_UNIT} AS unit,
                   bool_or(elected) AS elected
              FROM base
             GROUP BY series_id, date_trunc('{trunc}', at::timestamp)
             ORDER BY series_id, period
            """,
            params,
            log_sql=False,
        ) or []


#: Every reading in the window, as the columns the statistics read.
_READINGS_CTE = """
WITH base AS (
    SELECT o.series_id, o.display, o.code_system, o.code, o.observed_start AS at, o.value_text, o.value_num,
           o.value_canonical, o.unit_ucum, o.unit_canonical, o.elected
      FROM v_observation o
     WHERE o.user_id = :uid AND o.series_id = ANY(:names) {where}
)"""

#: One row per (series, local day): the elected authority where one exists,
#: the newest reading of that day otherwise.
_DAY_AUTHORITY_CTE = """
WITH base AS (
    SELECT DISTINCT ON (o.series_id, o.local_date)
           o.series_id, o.display, o.code_system, o.code, o.local_date::timestamp AS at, o.value_text, o.value_num,
           o.value_canonical, o.unit_ucum, o.unit_canonical, o.elected
      FROM v_observation o
     WHERE o.user_id = :uid AND o.series_id = ANY(:names) {where}
     ORDER BY o.series_id, o.local_date, o.elected DESC, o.observed_start DESC, o.id DESC
)"""


# --- row shapers (pure) ------------------------------------------------------


def _window_clause(params: dict[str, Any], window: query.Window | None) -> str:
    """The window as SQL: an equality on the stored local day, inclusive at
    both ends. Every row has one, so there is only `tz_exact`."""
    if window is None or not (window.start or window.end):
        return ""
    params["day_from"] = _date_of(window.start_ms, window.tz)
    params["day_to"] = _date_of(window.end_ms - 1, window.tz)
    return " AND o.local_date BETWEEN :day_from AND :day_to"


def _date_of(ms: int, tz: str) -> date:
    return datetime.fromtimestamp(ms / 1000, translate.zone_for(tz)).date()


def _identity(r: dict) -> dict:
    return {
        "indicator": r.get("display") or r.get("name") or r.get("name_text") or "",
        "series": r.get("series_id") or "",
        "system": r.get("code_system") or "",
        "code": r.get("code") or "",
    }


def _catalog_row(r: dict) -> dict:
    return {
        **_identity(r),
        "standard": bool(r.get("standard")),
        "name": r.get("name") or "",
        "kind": r.get("kind") or "",
        "count": int(r.get("count") or 0),
        "unit": r.get("unit") or "",
        "latest_value": _text(r.get("latest_value")),
        "first_date": r.get("first_date") or "",
        "last_date": r.get("last_date") or "",
        "total": int(r.get("total") or 0),
        "reason": r.get("reason") or "",
        "day_known": True,
    }


def _reading_row(r: dict) -> dict:
    return {
        **_identity(r),
        "name": r.get("name_text") or "",
        "time": _text(r.get("local_time")),
        "date": _text(r.get("local_date")),
        "value": _text(r.get("value_text")),
        "unit": r.get("unit_text") or "",
        "value_canonical": _number(r.get("value_canonical")),
        "unit_canonical": r.get("unit_canonical") or "",
        # `file_key` opens the document; `file` is what a person calls it. The
        # model used to be handed only the key and cited
        # "web_uploads/17eaf4f6-…-edbee3267ea6.pdf" as the source of a value.
        "file": _text(r.get("file_name")) or (r.get("file_key") or ""),
        "file_key": r.get("file_key") or "",
        "row_id": r.get("id"),
        "modality": r.get("modality") or "",
        "total": int(r.get("total") or 0),
        "day_known": True,
        "provenance": "elected:day_authority" if r.get("elected") else "measured",
    }


def _bucket_row(r: dict) -> dict:
    return {
        **_identity(r),
        "period": r.get("period") or "",
        "avg": _number(r.get("avg")),
        "min": _number(r.get("min")),
        "max": _number(r.get("max")),
        "n": int(r.get("n") or 0),
        "unit": r.get("unit") or "",
        "day_known": True,
        "provenance": "elected:day_authority" if r.get("elected") else "measured",
    }


def _stats_row(r: dict, basis: str) -> dict:
    out = {
        **_identity(r),
        "count": int(r.get("count") or 0),
        "numeric_count": int(r.get("numeric_count") or 0),
        "min": _number(r.get("min")),
        "max": _number(r.get("max")),
        "avg": _number(r.get("avg")),
        "first": _text(r.get("first")),
        "first_date": r.get("first_date") or "",
        "last": _text(r.get("last")),
        "last_date": r.get("last_date") or "",
        "unit": r.get("unit") or "",
        "mixed_units": bool(r.get("mixed_units")),
        "basis": basis,
        "day_known": True,
        "provenance": "computed",
    }
    # `change` only when both ends are numbers in one unit: a delta across
    # mg/dL and mmol/L, or across "Positive" and "Negative", is not a delta.
    first, last = _number(r.get("first_num")), _number(r.get("last_num"))
    if first is not None and last is not None and not out["mixed_units"]:
        out["change"] = round(last - first, 4)
    return out


def _latest_row(r: dict, basis: str) -> dict:
    return {
        **_identity(r),
        "name": r.get("name_text") or "",
        "time": _text(r.get("local_time")),
        "date": _text(r.get("local_date")),
        "value": _text(r.get("value_text")),
        "unit": r.get("unit_text") or "",
        "value_canonical": _number(r.get("value_canonical")),
        "unit_canonical": r.get("unit_canonical") or "",
        "modality": r.get("modality") or "",
        "basis": basis,
        "day_known": True,
        "provenance": "elected:day_authority" if r.get("elected") else "measured",
    }


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _number(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


__all__ = ["CATALOG_MAX", "MAX_KEYWORD_NAMES", "PostgresHealthQuery", "REST_CATALOG_MAX"]
