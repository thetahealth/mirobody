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

The unit of comparison is the SERIES (`standardize.series`): every cholesterol
reading of this person, in mg/dL or mmol/L, from a file or typed in, is one
series with one `series_id`. The catalogue lists series; a selection by
`indicators` accepts a series id, a LOINC code, the series' display name or
a name as printed on a report, so a model can copy any of them back.

## Units

A series may hold readings printed in two units (5.6 mmol/L and 100 mg/dL
are one series), and every view answers it in ONE: the unit this person's
reports print most often for it, the newest on a tie
(`units.pick_display_unit`, over the whole record, so every view and window
agree), labelled with that unit's newest printed spelling (`bpm`, not
`/min`). A reading printed in another unit is placed by its canonical pair,
divided by the series unit's own factor (`units.canonicalize`, the function
that wrote the pair, so the conversion is exact); its range is converted with
it. A raw row keeps what the report printed in `printed`, for the places that
show a source; the model's columns never carry it. A reading whose unit does
not convert (albumin in g/L and umol/L, with no molar mass) keeps its own
unit, is marked `unit_differs`, and is left out of every statistic.

Before this, the web chart plotted 212 mg/dL and 4.90 mmol/L of one
cholesterol as a fall from 212 to 4.9, and a stats row answered in mol/L,
which a small model read as "these cannot be compared" (#271).

## Election

For `view=day` and coarser, the answer is the day's published authority:
the observation `th_day_authority` names for that (series, local day), or the
newest reading of the day where nothing has been elected, and `provenance`
says which. Election happens once, on the write side
(`standardize/aggregate/election.py`), which is what keeps the chat answer and
the dashboard identical by construction rather than by two implementations
agreeing.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import date, datetime
from typing import Any, NamedTuple

from mirobody import standardize, units
from mirobody.kernel import query
from mirobody.kernel.ops import is_driver_exception
from mirobody.kernel.tools import PROVENANCE_REPORTED
from mirobody.collect.observations import (
    KIND_CONDITION,
    KIND_MEASUREMENT,
    KIND_NOTE,
    KIND_SYMPTOM,
    MODALITY_UNVERIFIED,
    contains_pattern,
    user_tz,
)
from mirobody.utils import execute_query
from mirobody.utils.req_ctx import request_language

logger = logging.getLogger(__name__)

#: Names listed for the MODEL: a token budget. The browser gets `REST_CATALOG_MAX`.
CATALOG_MAX = 200
REST_CATALOG_MAX = 2000
#: Raw rows per indicator for the browser's reading list, the 200 the web
#: client pages through. The model gets `query.ROW_CAP`.
REST_ROW_MAX = 200

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

# Whole-second formatting collapsed distinct device samples onto one displayed
# instant. Keep PostgreSQL's microseconds without padding whole-second rows.
_LOCAL_TIME = f"rtrim(rtrim(to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI:SS.US'), '0'), '.')"

#: Each series' unit (`SeriesUnit`), joined to its readings: four parallel
#: arrays from `_unit_params`, so one statement reads every series in its own
#: unit.
_SERIES_UNIT_JOIN = (
    " LEFT JOIN unnest(CAST(:su_ids AS text[]), CAST(:su_ucum AS text[]), CAST(:su_base AS text[]),"
    " CAST(:su_factor AS double precision[])) AS su(series_id, ucum, base, factor) ON su.series_id = o.series_id"
)

#: A reading's number in its series' unit, the one number every statistic,
#: bucket and band reads: as printed in that unit (or in none), else its
#: canonical value over the unit's factor, else NULL. Over canonical values
#: stats answered in mol/L, which a small model called incomparable (#271);
#: with no molar mass, 40 g/L and 600 umol/L of albumin averaged to 20.0003
#: "mol/L". Full precision: four decimals once rounded a 1e-5 g/L daily mean
#: to zero before the trend code saw it.
_VALUE_STD = (
    "CASE WHEN o.value_num IS NULL THEN NULL"
    " WHEN su.ucum IS NULL OR COALESCE(o.unit_ucum, '') IN ('', su.ucum) THEN o.value_num::double precision"
    " WHEN su.factor IS NOT NULL AND o.unit_canonical = su.base THEN o.value_canonical::double precision / su.factor"
    " END AS value_std"
)

#: A count over the readings inside `:vmin`..`:vmax`, either end open, in the
#: series' unit, which is the unit the row reports. NULL when no band was
#: asked for. Compared on printed values, 80 umol/L counted and 0.9 mg/dL did
#: not, the same creatinine.
_STAT_BAND = (
    "CASE WHEN CAST(:vmin AS DOUBLE PRECISION) IS NULL AND CAST(:vmax AS DOUBLE PRECISION) IS NULL THEN NULL"
    " ELSE {agg} FILTER (WHERE value_std IS NOT NULL"
    " AND (CAST(:vmin AS DOUBLE PRECISION) IS NULL OR value_std >= CAST(:vmin AS DOUBLE PRECISION))"
    " AND (CAST(:vmax AS DOUBLE PRECISION) IS NULL OR value_std <= CAST(:vmax AS DOUBLE PRECISION))) END"
)

#: Spread and centre, over `value_std` like every other statistic.
_STAT_MEDIAN = "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value_std)"
_STAT_STDDEV = "STDDEV_SAMP(value_std)"

# Propagate the evidence status of the rows actually used by each result:
# an out-of-window or unelected reading must not taint an unrelated answer.
_UNVERIFIED_ROW = f"o.modality = '{MODALITY_UNVERIFIED}' AS unverified"

#: What a person reports about themselves: a symptom felt, a diagnosis given,
#: or a note on their day, in their own words, with no value and no unit. The
#: same table as a reading, so the model's tool reads both, and each row says
#: which it is (`provenance="reported"`). The web client's Indicators tab lists
#: readings only (`reported=False`); the journal is its own tab there.
REPORTED_KINDS = [KIND_SYMPTOM, KIND_CONDITION, KIND_NOTE]

#: What a reported row carries beyond a reading's columns: its kind, why it
#: is uncoded, and the note the person added (encrypted at rest).
_REPORTED_COLUMNS = (
    "o.kind = ANY(:reported) AS reported, o.kind, o.reason,"
    " CASE WHEN o.kind = ANY(:reported) THEN decrypt_content(o.note_text) END AS note"
)

_FILE_KEY = "CASE WHEN o.source_kind = 'file' THEN substr(o.source_ref, 10) END"

#: The document a reading came from, by NAME. Handed only the key, the model
#: cited "web_uploads/17eaf4f6-…-edbee3267ea6.pdf" as the source of a value.
#: `file_name` is encrypted; the key may carry a legacy `_#_<page>` suffix.
_FILE_JOIN = """LEFT JOIN th_files f
                         ON o.source_kind = 'file'
                        AND f.file_key = split_part(substr(o.source_ref, 10), '_#_', 1)"""
_FILE_NAME = "decrypt_content(f.file_name) AS file_name"

def _periods_cte(anchor: str) -> str:
    """`chain`, `cut` and `visible_periods`: when each anchored visible entry
    began its current period. `anchor` is the FROM/WHERE naming the visible
    rows (`v`) to start from; the caller writes `WITH RECURSIVE`.

    `v_observation` shows only the end of an amendment chain, so this walks the
    chain back through `amends`. A correction keeps its entry's period; a
    retraction ends one, and the row that reasserts the entry after it starts
    the next. So the period starts at the oldest row newer than the latest
    retraction in the chain, or at the chain's first row when nothing was ever
    retracted. Taking the visible row's own time instead listed an old reading
    corrected yesterday as new; taking the root's missed a reassertion; and
    taking the visible row whenever any retraction was present made a reading
    corrected after its reassertion look newer than it is.

    The walk's step reads `th_observation`, not the view: the rows behind a
    visible one (retracted, amended) are exactly what the view hides. Every
    anchor and every reader of the result reads the view.

    The anchor must narrow the walk itself. A recursive CTE is an optimisation
    fence, so the outer query's filters never reach inside it: unanchored, it
    walked every visible row in the database on each page (0.69 s against
    0.22 s at 62k rows, 21k of them the reader's). Two anchors are exact:
    one page's ids, and rows written after a cutoff, because a period never
    starts after its visible row was written.
    """
    return f"""
chain AS (
    SELECT v.id AS visible_id, v.id AS chain_id, v.amends, v.status, v.created_at, 0 AS depth
      {anchor}
    UNION ALL
    SELECT p.visible_id, o.id, o.amends, o.status, o.created_at, p.depth + 1
      FROM chain p
      JOIN th_observation o ON o.id = p.amends
), cut AS (
    SELECT visible_id, MIN(depth) FILTER (WHERE status = 'entered-in-error') AS depth
      FROM chain
     GROUP BY visible_id
), visible_periods AS (
    SELECT c.visible_id, MIN(c.created_at) AS period_start
      FROM chain c
      JOIN cut ON cut.visible_id = c.visible_id
     WHERE cut.depth IS NULL OR c.depth < cut.depth
     GROUP BY c.visible_id
)
"""


#: Visible rows of `:uid` written after `:since`: the only rows whose period
#: can start after it.
_WRITTEN_SINCE = "FROM v_observation v WHERE v.user_id = :uid AND v.created_at > :since"

#: What the web's records table may ask for in one page.
RECORDS_PAGE_MAX = 200

#: The columns an export of records carries, in order. A whitelist, not
#: "whatever the row has": a field added to the row later does not leave in a
#: download until it is named here.
RECORD_EXPORT_COLUMNS = (
    "row_id", "kind", "indicator", "name", "code", "system", "series", "time", "date",
    "value", "unit", "ref", "printed_value", "printed_unit", "printed_ref", "value_canonical", "unit_canonical",
    "modality", "source_kind", "file", "file_key", "created_at", "provenance", "text",
)


def export_row(row: dict) -> dict:
    """One records row as `RECORD_EXPORT_COLUMNS`: the reading in its series'
    unit, and beside it what the report printed, flattened for a CSV."""
    printed = row.get("printed") or {}
    flat = {**row, "printed_value": printed.get("value", ""), "printed_unit": printed.get("unit", ""),
            "printed_ref": printed.get("ref", "")}
    return {k: flat.get(k) for k in RECORD_EXPORT_COLUMNS}


#: The identity a group reports: that of its most recent row, so a series
#: holding two codes (mass and molar cholesterol) names one consistent pair.
#: The name falls back to the printed one, as the catalogue's does: `display`
#: is the LOINC catalogue's, which a device series and an uncoded `local:`
#: reading lack, so their aggregate rows came back nameless (22 of 29 series
#: in one evaluation database, 2026-11-09). Each `base` selects `name_text`.
_LATEST_IDENTITY = (
    "COALESCE((ARRAY_AGG(display ORDER BY at DESC))[1],"
    " (ARRAY_AGG(name_text ORDER BY at DESC))[1]) AS display,"
    " (ARRAY_AGG(code_system ORDER BY at DESC))[1] AS code_system,"
    " (ARRAY_AGG(code ORDER BY at DESC))[1] AS code"
)


class PostgresHealthQuery:
    """`query.HealthQuery` over `v_observation`.

    Async: the port is declared with plain `def` so an in-memory implementation
    stays possible, and `HealthIndicatorsService` awaits whatever it gets back.

    `reported=False` leaves out what the person reported. The kind filter sits
    where a series is FOUND (the catalogue, `_by_names`, `_labels`), so no
    later statement can receive one.
    """

    def __init__(self, *, reported: bool = True) -> None:
        self._reported = reported

    async def records(
        self,
        subject_id: str,
        *,
        kind: str = KIND_MEASUREMENT,
        modalities: list[str] | None = None,
        start_time: date | None = None,
        end_time: date | None = None,
        created_since: datetime | None = None,
        keywords: str | None = None,
        limit: int | None = 50,
        offset: int = 0,
        notes: bool = False,
    ) -> dict[str, Any]:
        """Visible entries across every series, newest observed first, one page.

        `created_since` compares the start of each entry's current period
        (`_periods_cte`), the same instant `delta` counts, so the rows
        behind a "3 new" badge are exactly these. `start_time` / `end_time` are
        days observed, inclusive. `keywords` is matched literally against the
        printed and the display name. `limit=None` is every row, for an export.
        `notes=True` adds each row's decrypted note as `comment`, a reading's
        too; without it only a reported entry's note is decrypted.
        """
        params: dict[str, Any] = {
            "uid": str(subject_id),
            "offset": max(0, int(offset)),
            "kind": kind,
            "modalities": modalities or [],
            "reported": REPORTED_KINDS,
        }
        conditions = ["o.user_id = :uid", "(:kind = 'all' OR o.kind = :kind)"]
        if modalities:
            conditions.append("o.modality = ANY(:modalities)")
        if start_time is not None:
            params["start_time"] = start_time
            conditions.append("o.local_date >= :start_time")
        if end_time is not None:
            params["end_time"] = end_time
            conditions.append("o.local_date <= :end_time")
        if keywords and keywords.strip():
            params["keywords"] = contains_pattern(keywords.strip())
            conditions.append("(o.name_text ILIKE :keywords OR COALESCE(o.display, '') ILIKE :keywords)")
        if not self._reported:
            conditions.append("o.kind <> ALL(:reported)")
        page = ""
        if limit is not None:
            params["limit"] = max(1, int(limit))
            page = "LIMIT :limit OFFSET :offset"
        elif params["offset"]:
            page = "OFFSET :offset"
        comment = ", decrypt_content(o.note_text) AS comment" if notes else ""
        columns = f"""o.id, o.series_id, o.display, o.name_text, o.value_text, o.unit_text,
                   o.ref_text, o.ref_low, o.ref_high, o.flag_text, o.value_num, o.comparator, o.unit_ucum,
                   o.value_canonical, o.unit_canonical,
                   {_LOCAL_TIME} AS local_time,
                   o.observed_start, o.observed_end,
                   o.local_date, o.modality, o.code_system, o.code, o.elected, o.outcome,
                   o.source_kind, p.period_start,
                   {_REPORTED_COLUMNS},
                   {_FILE_KEY} AS file_key, {_FILE_NAME}{comment}"""
        where = " AND ".join(conditions)
        if created_since is None:
            # Browsing: the page first, then the chains of its rows only.
            sql = f"""
            WITH RECURSIVE page AS (
                SELECT o.id, COUNT(*) OVER () AS total
                  FROM v_observation o
                 WHERE {where}
                 ORDER BY o.observed_start DESC, o.id DESC
                 {page}
            ), {_periods_cte("FROM v_observation v JOIN page ON page.id = v.id")}
            SELECT {columns}, page.total
              FROM page
              JOIN v_observation o ON o.id = page.id
              JOIN visible_periods p ON p.visible_id = o.id
            {_FILE_JOIN}
             ORDER BY o.observed_start DESC, o.id DESC
            """
        else:
            params["since"] = created_since
            sql = f"""
            WITH RECURSIVE {_periods_cte(_WRITTEN_SINCE)}
            SELECT {columns}, COUNT(*) OVER () AS total
              FROM v_observation o
              JOIN visible_periods p ON p.visible_id = o.id
            {_FILE_JOIN}
             WHERE {where} AND p.period_start > :since
             ORDER BY o.observed_start DESC, o.id DESC
             {page}
            """
        rows = await execute_query(sql, params, log_sql=False) or []
        total = int(rows[0]["total"]) if rows else 0
        if not rows and params["offset"]:
            # Past the end: the window count went with the rows, so ask once.
            total = await self._records_total(where, params, since=created_since is not None)
        unit_of = await self.series_units(subject_id, [r["series_id"] for r in rows])
        return {
            "rows": [_record_row(r, unit_of.get(r["series_id"])) for r in rows],
            "total": total,
            "has_more": params["offset"] + len(rows) < total,
        }

    async def _records_total(self, where: str, params: dict[str, Any], *, since: bool) -> int:
        if since:
            sql = f"""
            WITH RECURSIVE {_periods_cte(_WRITTEN_SINCE)}
            SELECT COUNT(*) AS total
              FROM v_observation o
              JOIN visible_periods p ON p.visible_id = o.id
             WHERE {where} AND p.period_start > :since
            """
        else:
            sql = f"SELECT COUNT(*) AS total FROM v_observation o WHERE {where}"
        rows = await execute_query(sql, params, log_sql=False) or []
        return int(rows[0]["total"]) if rows else 0

    async def delta(
        self,
        subject_id: str,
        since: datetime,
        *,
        target_kind: str = "all",
    ) -> dict[str, Any]:
        """Visible entries whose current period began after `since`, by source.
        The same rule `records(created_since=)` filters on, so the count and
        the rows behind it cannot disagree."""
        params: dict[str, Any] = {
            "uid": str(subject_id),
            "since": since,
            "kind": target_kind,
            "reported": REPORTED_KINDS,
        }
        kind_clause = "(:kind = 'all' OR o.kind = :kind)"
        if not self._reported:
            kind_clause += " AND o.kind <> ALL(:reported)"
        rows = await execute_query(
            f"""
            WITH RECURSIVE {_periods_cte(_WRITTEN_SINCE)}
            SELECT o.source_kind, COUNT(*) AS count
              FROM visible_periods p
              JOIN v_observation o ON o.id = p.visible_id
             WHERE p.period_start > :since AND {kind_clause}
             GROUP BY o.source_kind
             ORDER BY o.source_kind
            """,
            params,
            log_sql=False,
        ) or []
        buckets = [{"name": str(r["source_kind"]), "count": int(r["count"])} for r in rows]
        return {
            "since": since.isoformat(),
            "total_new": sum(r["count"] for r in buckets),
            "by_source": buckets,
        }

    def _kinds(self, params: dict[str, Any]) -> str:
        if self._reported:
            return ""
        params["reported_kinds"] = REPORTED_KINDS
        return " AND o.kind <> ALL(:reported_kinds)"

    # --- subject-level facts -------------------------------------------------

    async def tz(self, subject_id: str) -> str:
        """The subject's IANA zone, `"UTC"` when unset. Never the server's zone
        and never the session's: a window is the person's day."""
        return await user_tz(subject_id)

    async def series_units(self, subject_id: str, series_ids: list[str]) -> dict[str, SeriesUnit]:
        """The one unit each series is read in (module docstring, "Units"),
        from every numeric reading the person has in it: never the window's,
        so a series reads in one unit in every view and every window. A series
        with no unit at all is absent."""
        wanted = sorted({s for s in series_ids if s})
        if not wanted:
            return {}
        rows = await execute_query(
            """
            SELECT series_id, unit_ucum, MAX(unit_canonical) AS base, COUNT(*) AS n,
                   MAX(observed_start) AS latest,
                   (ARRAY_AGG(unit_text ORDER BY observed_start DESC, id DESC))[1] AS label,
                   (ARRAY_AGG(code_system ORDER BY observed_start DESC, id DESC))[1] AS code_system,
                   (ARRAY_AGG(code ORDER BY observed_start DESC, id DESC))[1] AS code,
                   (ARRAY_AGG(display ORDER BY observed_start DESC, id DESC))[1] AS display,
                   (ARRAY_AGG(code ORDER BY observed_start DESC, id DESC)
                        FILTER (WHERE code_system = :loinc))[1] AS loinc
              FROM v_observation
             WHERE user_id = :uid AND series_id = ANY(:sids)
               AND value_num IS NOT NULL AND COALESCE(unit_ucum, '') <> ''
             GROUP BY series_id, unit_ucum
            """,
            {"uid": str(subject_id), "sids": wanted, "loinc": standardize.LOINC_SYSTEM},
            log_sql=False,
        ) or []
        groups: dict[str, list[dict]] = {}
        for r in rows:
            groups.setdefault(r["series_id"], []).append(r)
        return {sid: _series_unit(found) for sid, found in groups.items()}

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
        params: dict[str, Any] = {"uid": str(subject_id), "cap": cap, "reported": REPORTED_KINDS}
        where = self._kinds(params) + _window_clause(params, window)
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
                   (ARRAY_AGG(o.unit_ucum ORDER BY o.observed_start DESC))[1] AS latest_ucum,
                   (ARRAY_AGG(o.value_num ORDER BY o.observed_start DESC))[1] AS latest_num,
                   (ARRAY_AGG(o.comparator ORDER BY o.observed_start DESC))[1] AS latest_comparator,
                   (ARRAY_AGG(o.value_canonical ORDER BY o.observed_start DESC))[1] AS latest_canonical,
                   (ARRAY_AGG(o.unit_canonical ORDER BY o.observed_start DESC))[1] AS latest_base,
                   MAX(o.reason) AS reason,
                   bool_or(o.modality = '{MODALITY_UNVERIFIED}') AS unverified,
                   bool_or(o.kind = ANY(:reported)) AS reported
              FROM v_observation o
             WHERE o.user_id = :uid {where}
             GROUP BY o.series_id
             ORDER BY reported DESC, display
             LIMIT :cap
            """,
            params,
            log_sql=False,
        ) or []
        unit_of = await self.series_units(subject_id, [r["series_id"] for r in rows])
        return [_catalog_row(r, unit_of.get(r["series_id"])) for r in rows]

    async def readings(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, limit: int
    ) -> list[dict]:
        """Individual readings, newest first, capped per series. `total` per
        series comes from the same statement. `file_key` is the handle back
        to the ORIGINAL report."""
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {
            "uid": str(subject_id), "names": names, "limit": max(1, int(limit)),
            "reported": REPORTED_KINDS,
        }
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT * FROM (
                SELECT o.id, o.series_id, o.display, o.name_text, o.value_text, o.unit_text,
                       o.ref_text, o.ref_low, o.ref_high, o.flag_text, o.value_num, o.unit_ucum,
                       o.value_canonical, o.unit_canonical, o.comparator,
                       {_LOCAL_TIME} AS local_time,
                       o.tz, o.local_date, o.modality, o.code_system, o.code, o.elected, o.outcome,
                       {_REPORTED_COLUMNS},
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
        unit_of = await self.series_units(subject_id, names)
        return [_reading_row(r, unit_of.get(r["series_id"])) for r in rows]

    async def buckets(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, resolution: str, limit: int
    ) -> list[dict]:
        """One point per bucket, the newest `limit` per series, oldest first.
        Day and coarser read the day authority. `total` per series comes from
        the same statement, as for `readings`."""
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        unit_of = await self.series_units(subject_id, names)
        if resolution in _SUBDAY_TRUNC:
            rows = await self._subday_buckets(subject_id, names, window, resolution, limit, unit_of)
        else:
            rows = await self._day_buckets(subject_id, names, window, resolution, limit, unit_of)
        return [_bucket_row(r, unit_of.get(r["series_id"])) for r in rows]

    async def stats(self, subject_id: str, sel: query.Selection, window: query.Window, *,
                    value_min: float | None = None, value_max: float | None = None) -> list[dict]:
        """count/min/max/avg/median/stddev/sum/first/last/change over the WHOLE
        window, in SQL, over `_STATS_CTE`: a day with an elected authority
        counts once, as that value; any other day counts every reading.

        `sum` is over the same values as `avg`, in the same unit. Without it a
        total over more than `query.ROW_CAP` readings (a year of exercise logs)
        could only be rebuilt as avg x count, which a model rarely did.

        `first_date`/`last_date` are the readings' stored local days, as the
        catalogue's are. They were the UTC date of the instant, so a report
        filed at local midnight in Asia/Shanghai showed the day before: a
        ferritin of 2026-03-05 came back as 2026-03-04 and a model repeated it
        (benchmarks/local_models, qa3).

        Which of the two a day is was already decided on the write side, so
        the caller is not asked. It used to be: `resolution=raw` counted every
        reading, which averages a watch's and a phone's step totals for the
        same Tuesday together, and `resolution=day` kept the newest reading of
        an unelected day, which drops a morning blood pressure when an evening
        one follows. Neither is a choice a model can make from the question.
        """
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        unit_of = await self.series_units(subject_id, names)
        params: dict[str, Any] = {"uid": str(subject_id), "names": names, "reported": REPORTED_KINDS,
                                  "vmin": value_min, "vmax": value_max, **_unit_params(unit_of)}
        where = _window_clause(params, window)
        source = _STATS_CTE.format(where=where)
        rows = await execute_query(
            f"""
            {source}
            SELECT series_id,
                   {_LATEST_IDENTITY},
                   COUNT(*) AS count,
                   COUNT(value_num) AS numeric_count,
                   COUNT(value_num) - COUNT(value_std) AS unconverted,
                   {_STAT_BAND.format(agg="COUNT(*)")} AS count_in_range,
                   COUNT(DISTINCT local_date) AS days,
                   {_STAT_BAND.format(agg="COUNT(DISTINCT local_date)")} AS days_in_range,
                   MIN(value_std) AS min,
                   MAX(value_std) AS max,
                   AVG(value_std) AS avg,
                   {_STAT_MEDIAN} AS median,
                   {_STAT_STDDEV} AS stddev,
                   SUM(value_std) AS sum,
                   (ARRAY_AGG(value_text ORDER BY at ASC))[1] AS first,
                   (ARRAY_AGG(value_std ORDER BY at ASC))[1] AS first_std,
                   (ARRAY_AGG(unit_ucum ORDER BY at ASC))[1] AS first_ucum,
                   to_char((ARRAY_AGG(local_date ORDER BY at ASC))[1], 'YYYY-MM-DD') AS first_date,
                   (ARRAY_AGG(value_text ORDER BY at DESC))[1] AS last,
                   (ARRAY_AGG(value_std ORDER BY at DESC))[1] AS last_std,
                   (ARRAY_AGG(unit_ucum ORDER BY at DESC))[1] AS last_ucum,
                   to_char((ARRAY_AGG(local_date ORDER BY at DESC))[1], 'YYYY-MM-DD') AS last_date,
                   COUNT(DISTINCT unit_ucum) > 1 AS mixed_units,
                   bool_or(unverified) AS unverified,
                   bool_or(reported) AS reported
              FROM base
             GROUP BY series_id
             ORDER BY display
            """,
            params,
            log_sql=False,
        ) or []
        return [_stats_row(r, unit_of.get(r["series_id"])) for r in rows]

    async def latest(self, subject_id: str, sel: query.Selection, window: query.Window) -> list[dict]:
        """The most recent value per series INSIDE the window: the newest
        local day first, and that day's elected reading where it has one.
        Election only ranks readings of the same day; ranked first across
        days, an elected value from last month beat an unelected one from
        today, which `stats()` called the last."""
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {"uid": str(subject_id), "names": names, "reported": REPORTED_KINDS}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT DISTINCT ON (o.series_id)
                   o.id, o.series_id, o.display, o.name_text, o.value_text, o.unit_text, o.ref_text, o.ref_low,
                   o.ref_high, o.flag_text, o.value_num, o.comparator, o.unit_ucum,
                   o.value_canonical, o.unit_canonical, o.code_system, o.code, o.local_date, o.elected, o.modality,
                   o.outcome, {_REPORTED_COLUMNS},
                   {_FILE_KEY} AS file_key, {_FILE_NAME},
                   {_LOCAL_TIME} AS local_time
              FROM v_observation o
            {_FILE_JOIN}
             WHERE o.user_id = :uid AND o.series_id = ANY(:names) {where}
             ORDER BY o.series_id, o.local_date DESC, o.elected DESC, o.observed_start DESC, o.id DESC
            """,
            params,
            log_sql=False,
        ) or []
        unit_of = await self.series_units(subject_id, names)
        return [_latest_row(r, unit_of.get(r["series_id"])) for r in rows]

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
        """Exact selectors: a series id, a code, a display name, a printed name,
        or the name a reader is shown (`label`, in the request's language or in
        English). The model reads the label and asks again by it, so a label
        that selected nothing would make the answer it just read unreachable."""
        params: dict[str, Any] = {
            "uid": str(subject_id),
            "names": names,
            "lower": [n.lower() for n in names],
            "keys": [standardize.name_key(n) for n in names],
        }
        kinds = self._kinds(params)
        rows = await execute_query(
            f"""
            SELECT DISTINCT o.series_id, o.code, lower(o.display) AS display, o.name_key
              FROM v_observation o
             WHERE o.user_id = :uid{kinds}
               AND (o.series_id = ANY(:names) OR o.code = ANY(:names)
                    OR lower(o.display) = ANY(:lower) OR o.name_key = ANY(:keys))
            """,
            params,
            log_sql=False,
        ) or []
        found = [str(r["series_id"]) for r in rows]
        answered = {str(r[k]) for r in rows for k in ("series_id", "code", "display", "name_key") if r[k]}
        rest = {n.lower() for n in names
                if not {n, n.lower(), standardize.name_key(n)} & answered}
        if rest:
            found += [sid for label, sid in await self._labels(subject_id, None) if label.lower() in rest]
        return list(dict.fromkeys(found))

    async def _labels(self, subject_id: str, window: query.Window | None) -> list[tuple[str, str]]:
        """`(label, series_id)` for every name a series answers to: the printed
        ones, the catalogue's display, and the label a reader is shown, in the
        request's language and in English (`standardize.label_for`)."""
        params: dict[str, Any] = {"uid": str(subject_id)}
        where = self._kinds(params) + _window_clause(params, window)
        rows = await execute_query(
            f"SELECT o.series_id, o.name_text, o.display, o.code_system, o.code FROM v_observation o"
            f" WHERE o.user_id = :uid {where}"
            f" GROUP BY o.series_id, o.name_text, o.display, o.code_system, o.code",
            params,
            log_sql=False,
        ) or []
        languages = dict.fromkeys((request_language(), "en"))
        out: list[tuple[str, str]] = []
        for r in rows:
            sid = str(r["series_id"])
            names = [r["name_text"], r["display"],
                     *(standardize.label_for(r["code_system"] or "", r["code"] or "", lang) for lang in languages)]
            out += [(str(n), sid) for n in dict.fromkeys(names) if n]
        return out

    async def _by_keywords(self, subject_id: str, keywords: tuple[str, ...], window: query.Window | None) -> list[str]:
        """Free text to the person's own series, in two tiers per keyword: a
        lexical rank over their printed and display names (free, deterministic,
        scoped to what they have), then the series the offline resolvers' codes
        name, matched against their series, which reaches the same place with
        no key at all.

        The tiers are per keyword: in ["血压", "头痛"] a lexical hit on the
        first must not stop the second from reaching an entry written 头疼,
        which only the code (NS01) connects.

        A name can belong to several series: a lab's fasting glucose and a
        CGM's, a clinic's weight and a scale's. A name that matches brings
        every series printed or displayed under it, so the model sees them all
        and tells them apart by their rows (series, file, modality)."""
        kws = [k.strip() for k in keywords if k and k.strip()]
        if not kws:
            return []
        labels = await self._labels(subject_id, window)
        by_label: dict[str, list[str]] = {}
        for label, sid in labels:
            by_label.setdefault(label, []).append(sid)
        found: list[str] = []
        for kw in kws:
            ranked = [
                sid
                for label in query.rank_catalog(kw, list(by_label), limit=MAX_KEYWORD_NAMES)
                for sid in by_label[label]
            ]
            if not ranked:
                named = self._series_for(kw)
                ranked = [sid for _label, sid in labels if sid in named]
            found.extend(ranked)
        return list(dict.fromkeys(found))[:MAX_KEYWORD_NAMES]

    def _series_for(self, keyword: str) -> set[str]:
        """The series one keyword names: LOINC always, and the two ICPC-3 axes
        when reported entries are in scope. Each resolver abstains on what is
        not its own, so 头痛 yields only NS01 and 血压 only a LOINC series.

        A series, not a code. The resolver answers a NAME with one code, and
        a reading is coded by its unit too: "triglycerides" resolves to 2571-8
        (mass) and a reading printed in mmol/L is 14927-8 (moles), so matching
        codes missed it (1.5.4 local-model evaluation). The series is where the
        writer files both (`standardize.series_of`), mass and moles, with or
        without a method."""
        named: set[str] = set()
        try:
            from mirobody.engine import resolve
            hit = resolve(keyword)
            if hit.resolved and hit.loinc and hit.method == "lexical":
                series = standardize.series_of(hit.loinc)
                if series:
                    named.add(series)
        except Exception as e:
            logger.warning("offline resolver unavailable in keyword recall: error_type=%s", type(e).__name__,
                           exc_info=not is_driver_exception(e))
        if self._reported:
            for coding in (standardize.resolve_symptom(keyword), standardize.resolve_condition(keyword)):
                if coding.outcome == "coded" and coding.code:
                    named.add(coding.series_id)
        return named

    async def _subday_buckets(
        self, subject_id: str, names: list[str], window: query.Window, resolution: str, limit: int,
        unit_of: dict[str, SeriesUnit],
    ) -> list[dict]:
        params: dict[str, Any] = {"uid": str(subject_id), "names": names, "limit": max(1, int(limit)),
                                  **_unit_params(unit_of)}
        where = _window_clause(params, window)
        trunc = _SUBDAY_TRUNC[resolution]
        return await execute_query(
            f"""
            WITH base AS (
                SELECT o.series_id, o.display, o.name_text, o.code_system, o.code, {_VALUE_STD}, {_UNVERIFIED_ROW},
                       date_trunc('{trunc}', {_LOCAL_TS}) AS at
                  FROM v_observation o{_SERIES_UNIT_JOIN}
                 WHERE o.user_id = :uid AND o.series_id = ANY(:names) AND o.value_num IS NOT NULL {where}
            ), grouped AS (
                SELECT series_id, {_LATEST_IDENTITY},
                       to_char(at, 'YYYY-MM-DD HH24:MI') AS period,
                       COUNT(*) AS n,
                       AVG(value_std) AS avg,
                       MIN(value_std) AS min,
                       MAX(value_std) AS max,
                       false AS elected,
                       bool_or(unverified) AS unverified,
                       {_BUCKET_BUDGET.format(bucket="at")}
                  FROM base
                 GROUP BY series_id, at
            )
            SELECT * FROM grouped WHERE rn <= :limit ORDER BY series_id, period
            """,
            params,
            log_sql=False,
        ) or []

    async def _day_buckets(
        self, subject_id: str, names: list[str], window: query.Window, resolution: str, limit: int,
        unit_of: dict[str, SeriesUnit],
    ) -> list[dict]:
        """Day and coarser: one row per (series, bucket), from the day
        authority where one has been elected and the newest reading of the
        day otherwise, which `provenance` reports."""
        params: dict[str, Any] = {
            "uid": str(subject_id), "names": names, "reported": REPORTED_KINDS, "limit": max(1, int(limit)),
            **_unit_params(unit_of),
        }
        where = _window_clause(params, window)
        trunc = _DAY_TRUNC[resolution]
        bucket = f"date_trunc('{trunc}', at::timestamp)"
        return await execute_query(
            f"""
            {_DAY_AUTHORITY_CTE.format(where=where)}, std AS (
                SELECT o.*, {_VALUE_STD} FROM base o{_SERIES_UNIT_JOIN}
            ), grouped AS (
                SELECT series_id, {_LATEST_IDENTITY},
                       to_char({bucket}, 'YYYY-MM-DD') AS period,
                       COUNT(*) AS n,
                       AVG(value_std) AS avg,
                       MIN(value_std) AS min,
                       MAX(value_std) AS max,
                       bool_or(elected) AS elected,
                       bool_or(unverified) AS unverified,
                       bool_or(reported) AS reported,
                       {_BUCKET_BUDGET.format(bucket=bucket)}
                  FROM std
                 GROUP BY series_id, {bucket}
            )
            SELECT * FROM grouped WHERE rn <= :limit ORDER BY series_id, period
            """,
            params,
            log_sql=False,
        ) or []


#: Every reading in the window, as the columns the statistics read.
#: What a statistic counts: per (series, local day), the elected authority
#: alone where the write side elected one, every reading otherwise.
_STATS_CTE = f"""
WITH day_rows AS (
    SELECT o.series_id, o.display, o.name_text, o.code_system, o.code, o.observed_start AS at, o.local_date,
           o.value_text, o.value_num, o.unit_ucum, {_VALUE_STD}, o.elected, o.kind = ANY(:reported) AS reported,
           {_UNVERIFIED_ROW},
           bool_or(o.elected) OVER (PARTITION BY o.series_id, o.local_date) AS day_elected
      FROM v_observation o{_SERIES_UNIT_JOIN}
     WHERE o.user_id = :uid AND o.series_id = ANY(:names) {{where}}
), base AS (
    SELECT * FROM day_rows WHERE elected OR NOT day_elected
)"""

#: The two window columns a bucket statement adds over its grouped rows:
#: how many buckets each series has (window functions run after GROUP BY, so
#: this counts buckets, not readings) and each bucket's rank from the newest.
#: `{bucket}` is the GROUP BY expression, since a window's ORDER BY may not
#: name an output alias.
_BUCKET_BUDGET = (
    "COUNT(*) OVER (PARTITION BY series_id) AS total,"
    " ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY {bucket} DESC) AS rn"
)

#: One row per (series, local day): the elected authority where one exists,
#: the newest reading of that day otherwise.
_DAY_AUTHORITY_CTE = f"""
WITH base AS (
    SELECT DISTINCT ON (o.series_id, o.local_date)
           o.id, o.series_id, o.display, o.name_text, o.code_system, o.code, o.local_date::timestamp AS at,
           o.value_text, o.value_num, o.value_canonical, o.unit_ucum, o.unit_canonical, o.elected,
           o.kind = ANY(:reported) AS reported,
           {_UNVERIFIED_ROW}
      FROM v_observation o
     WHERE o.user_id = :uid AND o.series_id = ANY(:names) {{where}}
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
    return datetime.fromtimestamp(ms / 1000, standardize.zone_for(tz)).date()


def _identity(r: dict, facets: bool = False) -> dict:
    """`indicator` is LOINC's long name and stays what callers select by: two
    series can share one plain name (serum and whole-blood glucose are both
    葡萄糖). A coded row also gets, in the request's language, `label`, the name
    a person reads (`standardize.label_for`: 血红蛋白, `Hemoglobin, Blood`, and for
    a device metric 深睡比例 rather than `dailyDeepSleepPercentage`). With
    `facets`, a LOINC row says what the code measures, axis by axis (specimen
    全血, property 质量浓度): the long name's content without its brackets. Only a
    catalogue row asks for those: they describe the code, so on every one of 200
    readings they would be the same dict 200 times."""
    out = {
        "indicator": r.get("display") or r.get("name") or r.get("name_text") or "",
        "series": r.get("series_id") or "",
        "system": r.get("code_system") or "",
        "code": r.get("code") or "",
    }
    label = standardize.label_for(out["system"], out["code"], request_language())
    if label:
        out["label"] = label
    if facets and out["system"] == standardize.LOINC_SYSTEM and out["code"]:
        names = standardize.names_for(out["code"], request_language())
        if names is not None and names.facets:
            out["facets"] = dict(names.facets)
    return out


def _reported(r: dict) -> dict:
    """The columns only a reported row has. Empty for a reading, so a table of
    readings renders exactly as it did before reported rows existed."""
    if not r.get("reported"):
        return {}
    coded = r.get("outcome", "coded") == "coded"
    return {
        "name": r.get("name_text") or "",
        "kind": r.get("kind") or "",
        "reason": "" if coded else (r.get("reason") or ""),
        "note": _text(r.get("note")),
        "provenance": PROVENANCE_REPORTED,
    }


def _catalog_row(r: dict, su: SeriesUnit | None = None) -> dict:
    latest = _in_series_unit({
        "value_text": r.get("latest_value"), "unit_text": r.get("unit"), "unit_ucum": r.get("latest_ucum"),
        "value_num": r.get("latest_num"), "comparator": r.get("latest_comparator"),
        "value_canonical": r.get("latest_canonical"), "unit_canonical": r.get("latest_base"),
        "code": r.get("code"), "code_system": r.get("code_system"),
    }, su)
    return {
        **_identity(r if latest.get("unit_differs") else _as_series(r, su), facets=True),
        "standard": bool(r.get("standard")),
        "name": r.get("name") or "",
        "kind": r.get("kind") or "",
        "count": int(r.get("count") or 0),
        "unit": latest["unit"],
        "latest_value": latest["value"],
        "first_date": r.get("first_date") or "",
        "last_date": r.get("last_date") or "",
        "total": int(r.get("total") or 0),
        "reason": r.get("reason") or "",
        "day_known": True,
        "unverified": bool(r.get("unverified")),
        **({"provenance": PROVENANCE_REPORTED} if r.get("reported") else {}),
    }


def _reading_row(r: dict, su: SeriesUnit | None = None) -> dict:
    shown = _in_series_unit(r, su)
    return {
        # Every reading of a series names it one way, the way its other views
        # do: a sibling code (another method, the mass twin) is the series'
        # here and the reading's own in `printed`. Only a reading whose unit
        # does not convert keeps its code, which says what that unit measures.
        **_identity(r if shown.get("unit_differs") else _as_series(r, su)),
        "time": _text(r.get("local_time")),
        "date": _text(r.get("local_date")),
        # `value`, `unit` and `ref` are in the series' unit; what the report
        # printed is `printed` (`_in_series_unit`). The range is there because
        # without one a model judged against a range it remembered (1.5.4
        # local runs). The flag is high or low as the report printed it, else
        # empty (`collect/files/services/indicator_store.value_and_flag`).
        **shown,
        "flag": r.get("flag_text") or "",
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
        **_reported(r),
    }


def _bucket_row(r: dict, su: SeriesUnit | None = None) -> dict:
    return {
        **_identity(_as_series(r, su)),
        "period": r.get("period") or "",
        "avg": _number(r.get("avg")),
        "min": _number(r.get("min")),
        "max": _number(r.get("max")),
        "n": int(r.get("n") or 0),
        "unit": su.label if su else (r.get("unit") or ""),
        "total": int(r.get("total") or 0),
        "day_known": True,
        "provenance": "elected:day_authority" if r.get("elected") else "measured",
        "unverified": bool(r.get("unverified")),
        **({"provenance": PROVENANCE_REPORTED} if r.get("reported") else {}),
    }


def _count(value: Any) -> int | None:
    return None if value is None else int(value)


def _end(r: dict, end: str, su: SeriesUnit | None) -> str:
    """A stats row's `first` or `last`, in the row's unit: as printed when it
    was printed in that unit, else converted. They were the printed values
    beside min..max in mol/L: `first=212, last=4.90, unit=mol/L` was the fall
    from 212 to 4.9 the web chart drew as well (#271). A text result
    ("Negative") stays as printed."""
    text, std, ucum = r.get(end), _number(r.get(f"{end}_std")), r.get(f"{end}_ucum") or ""
    if su is None or std is None or not ucum or ucum == su.ucum:
        return _shown(text, su.label if su else r.get("unit"))
    return _like(std, _text(text))


def _stats_row(r: dict, su: SeriesUnit | None = None) -> dict:
    out = {
        **_identity(_as_series(r, su)),
        "count": int(r.get("count") or 0),
        "numeric_count": int(r.get("numeric_count") or 0),
        # Readings whose unit does not convert to the series' and that no
        # statistic below counted (`unit_differs` on a raw row).
        "unconverted": int(r.get("unconverted") or 0),
        # A day count answers "how many days" exactly, which `count` does not:
        # a heart-rate series has many readings a day, and 392 of PHIA's 4,000
        # objective questions ask how many days something happened.
        "days": int(r.get("days") or 0),
        # None without a band, so a table without one does not carry a band
        # count that is only the whole count again.
        "count_in_range": _count(r.get("count_in_range")),
        "days_in_range": _count(r.get("days_in_range")),
        "min": _number(r.get("min")),
        "max": _number(r.get("max")),
        "avg": _number(r.get("avg")),
        "median": _number(r.get("median")),
        "stddev": _number(r.get("stddev")),
        "sum": _number(r.get("sum")),
        "first": _end(r, "first", su),
        "first_date": r.get("first_date") or "",
        "last": _end(r, "last", su),
        "last_date": r.get("last_date") or "",
        "unit": su.label if su else (r.get("unit") or ""),
        # Printed in more than one unit; every number above is in `unit`.
        "mixed_units": bool(r.get("mixed_units")),
        "day_known": True,
        "unverified": bool(r.get("unverified")),
        "provenance": PROVENANCE_REPORTED if r.get("reported") else "computed",
    }
    # `change` only when both ends are numbers: across "Positive" and
    # "Negative" there is no delta. Both are in the series' unit.
    first, last = _number(r.get("first_std")), _number(r.get("last_std"))
    if first is not None and last is not None:
        out["change"] = round(last - first, 4)
    return out


def _latest_row(r: dict, su: SeriesUnit | None = None) -> dict:
    """A reading row without the paging total: the latest value is the answer
    asked for most, and it carries the same range, flag and document. It keeps
    `row_id`, which an answer citing it resolves to (`collect/citations.py`)."""
    row = _reading_row(r, su)
    del row["total"]
    return row


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _shown(value: object, unit: object) -> str:
    """The value without a copy of its unit. A value cell that printed its
    unit ("170 ms") is stored as printed beside the unit column ("ms"), and a
    page that sets the two side by side showed "170 ms ms"."""
    text, unit = _text(value).strip(), _text(unit).strip()
    if unit and text.casefold().endswith(unit.casefold()):
        # UCUM's dimensionless "1" is also a digit: "1.021" is a value, not
        # "1.02 1". A unit that starts with a digit needs a printed space.
        boundary = len(text) - len(unit)
        if unit[0].isdigit() and (boundary == 0 or not text[boundary - 1].isspace()):
            return _text(value)
        head = text[: -len(unit)].rstrip()
        if head[-1:].isdigit():
            return head
    return _text(value)


def _number(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


class SeriesUnit(NamedTuple):
    """The one unit a series is read in (module docstring, "Units"). `ucum`
    chooses and compares; `label` is what a reader sees, the newest printed
    spelling of that unit, because UCUM spells a heart rate `/min`, a pressure
    `mm[Hg]` and a count `{#}`. A reading in another unit is its canonical
    value over `factor`, when its canonical unit is `base`.

    `code_system`, `code` and `display` are the identity of the newest reading
    in that unit. A value in mmol/L is a [Moles/volume] measurement: a
    converted reading, and a statistic over the series, is named by it, not by
    the [Mass/volume] code its report was coded with, the mismatch that once
    had a model give 1558-6 beside 5.4 mmol/L (`agent/tools/_render.py`)."""

    ucum: str
    label: str
    base: str
    factor: float | None
    code_system: str = ""
    code: str = ""
    display: str = ""


def _series_unit(groups: list[dict]) -> SeriesUnit:
    """One series' `SeriesUnit` from its (unit, count, newest) groups. The
    factor comes from `units.canonicalize`, the function that wrote every
    stored canonical pair, with the code a reading in that unit was coded
    with, so a value converted through it is exact."""
    ucum = units.pick_display_unit((g["unit_ucum"], int(g["n"]), g["latest"]) for g in groups)
    chosen = next(g for g in groups if g["unit_ucum"] == ucum)
    base = chosen.get("base") or ""
    folded = units.canonicalize(1.0, ucum, loinc_code=chosen.get("loinc") or "")
    factor = folded.value if base and folded.unit == base and folded.value else None
    return SeriesUnit(ucum, chosen.get("label") or ucum, base, factor,
                      chosen.get("code_system") or "", chosen.get("code") or "", chosen.get("display") or "")


def _as_series(r: dict, su: SeriesUnit | None) -> dict:
    """`r` named by its series' unit (`SeriesUnit`), when that unit has a code."""
    if su is None or not su.code:
        return r
    return {**r, "code_system": su.code_system, "code": su.code, "display": su.display or r.get("display")}


def _unit_params(found: dict[str, SeriesUnit]) -> dict[str, list]:
    """`found` as the four arrays `_SERIES_UNIT_JOIN` unnests."""
    ids = list(found)
    return {"su_ids": ids, "su_ucum": [found[i].ucum for i in ids], "su_base": [found[i].base for i in ids],
            "su_factor": [found[i].factor for i in ids]}


def _in_series_unit(r: dict, su: SeriesUnit | None) -> dict:
    """`value`, `unit` and `ref` of one reading in its series' unit, and
    `printed`, the name and those three as the report printed them and the
    code they were coded with: what a source view shows and the model never
    reads.

    A reading printed in the series' unit, or with no unit, keeps its printed
    value ("4.90" stays "4.90"). One in another unit is converted, its range
    with it (`ref_low`/`ref_high`, parsed when it was written), and marked
    `converted`: "212 mg/dL, <200" of a series read in mmol/L is "5.48",
    "<5.17". One that does not convert keeps its own unit and range and is
    marked `unit_differs`; no statistic counts it."""
    printed = {"name": r.get("name_text") or "", "value": _shown(r.get("value_text"), r.get("unit_text")),
               "unit": r.get("unit_text") or "", "ref": r.get("ref_text") or "", "code": r.get("code") or ""}
    ucum = r.get("unit_ucum") or ""
    if su is None or ucum == su.ucum or not (ucum or printed["unit"]):
        return {"value": printed["value"], "unit": su.label if su else printed["unit"], "ref": printed["ref"],
                "printed": printed}
    ratio = _ratio(r, ucum, su)
    value = _number(r.get("value_num"))
    if ratio is None or value is None:
        return {"value": printed["value"], "unit": printed["unit"], "ref": printed["ref"], "printed": printed,
                "unit_differs": True}
    return {"value": _text(r.get("comparator")) + _like(value * ratio, printed["value"]), "unit": su.label,
            "ref": _range_in(r, ratio), "printed": printed, "converted": True}


def _ratio(r: dict, ucum: str, su: SeriesUnit) -> float | None:
    """What one of `ucum` is in the series' unit: through the canonical unit
    both fold to, with the reading's own code for its molar mass. None when
    they do not fold to one."""
    if not su.factor or r.get("unit_canonical") != su.base:
        return None
    code = r.get("code") if r.get("code_system") == standardize.LOINC_SYSTEM else ""
    folded = units.canonicalize(1.0, ucum, loinc_code=code or "")
    return folded.value / su.factor if folded.unit == su.base else None


def _range_in(r: dict, ratio: float) -> str:
    """The printed range, its bounds times `ratio`, with the bound sign the
    report used. A range that is words ("阴性") stays as printed."""
    text = r.get("ref_text") or ""
    low, high = _number(r.get("ref_low")), _number(r.get("ref_high"))
    lo = _like(low * ratio, text) if low is not None else ""
    hi = _like(high * ratio, text) if high is not None else ""
    if lo and hi:
        return f"{lo}-{hi}"
    if hi:
        return ("≤" if re.search(r"≤|<=|＜=", text) else "<") + hi
    if lo:
        return ("≥" if re.search(r"≥|>=|＞=", text) else ">") + lo
    return text


_FIRST_NUMBER = re.compile(r"\d+(?:\.\d+)?")


def _like(value: float, printed: str) -> str:
    """`value` written with as many significant figures as the first number in
    `printed`, three at least, and never fewer whole digits than it has:
    212 mg/dL is 5.48 mmol/L and 4.90 mmol/L is 189 mg/dL."""
    found = _FIRST_NUMBER.search(printed or "")
    digits = max(3, len(found.group().replace(".", "").lstrip("0")) if found else 0)
    if value == 0 or not math.isfinite(value):
        return f"{value:g}"
    decimals = max(0, digits - 1 - math.floor(math.log10(abs(value))))
    return f"{value:.{min(decimals, 12)}f}"


def _record_row(r: dict, su: SeriesUnit | None = None) -> dict:
    """One row of the records list: a reading row's fields (the same names the
    Indicators table and the tools read), plus what kind of entry it is, where
    it came from, `created_at`, the start of its current visible period, and
    the stored instants and parsed number that the developer API's
    `GET /api/data` answers with.

    An entry that is not a measurement carries no value and no unit, never a
    made-up one; what the person wrote is `text`."""
    row = _reading_row(r, su)
    row.pop("total", None)
    kind = r.get("kind") or KIND_MEASUREMENT
    # A record is the read-back of what was written: it keeps the code it was
    # coded with and the name it was printed under, where a query names the
    # series one way.
    row.pop("label", None)
    row.update(_identity(_as_series(r, su) if row.get("converted") else r))
    row.update({
        "name": r.get("name_text") or "",
        "kind": kind,
        "source_kind": r.get("source_kind") or "",
        "created_at": _iso(r.get("period_start")),
        "observed_start": _iso(r.get("observed_start")),
        "observed_end": _iso(r.get("observed_end")),
        "value_num": r.get("value_num"),
        "unit_ucum": r.get("unit_ucum") or "",
    })
    if "comment" in r:
        row["comment"] = _text(r["comment"])
    if kind != KIND_MEASUREMENT:
        row.update({"value": "", "unit": "", "ref": "", "value_num": None, "unit_ucum": "", "value_canonical": None,
                    "unit_canonical": ""})
        for key in ("printed", "converted", "unit_differs"):
            row.pop(key, None)
        row["text"] = _text(r.get("note")) or _text(r.get("value_text"))
    return row


def _iso(value: object) -> str:
    return value.isoformat() if isinstance(value, datetime) else _text(value)


__all__ = [
    "CATALOG_MAX",
    "MAX_KEYWORD_NAMES",
    "PostgresHealthQuery",
    "RECORDS_PAGE_MAX",
    "RECORD_EXPORT_COLUMNS",
    "SeriesUnit",
    "export_row",
    "REPORTED_KINDS",
    "REST_CATALOG_MAX",
    "REST_ROW_MAX",
]
