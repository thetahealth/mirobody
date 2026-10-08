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

For `view=day` and coarser, the answer is the day's published authority:
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
from mirobody.kernel.ops import is_driver_exception
from mirobody.kernel.tools import PROVENANCE_REPORTED
from mirobody.collect.observations import (
    KIND_CONDITION,
    KIND_MEASUREMENT,
    KIND_NOTE,
    KIND_SYMPTOM,
    contains_pattern,
    user_tz,
)
from mirobody.utils import execute_query

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

#: The number a statistic runs over, per group: the printed values when the
#: group has one unit, the canonical values when it has more.
_STAT_VALUE = "CASE WHEN COUNT(DISTINCT unit_ucum) > 1 THEN {agg}(value_canonical) ELSE {agg}(value_num) END"
_STAT_UNIT = "CASE WHEN COUNT(DISTINCT unit_ucum) > 1 THEN MAX(unit_canonical) ELSE (ARRAY_AGG(unit_ucum ORDER BY at DESC))[1] END"

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
    "value", "unit", "value_canonical", "unit_canonical", "modality", "source_kind",
    "file", "file_key", "created_at", "provenance", "text",
)


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
                   o.ref_text, o.flag_text, o.value_num, o.unit_ucum, o.value_canonical, o.unit_canonical,
                   to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI:SS') AS local_time,
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
        return {
            "rows": [_record_row(r) for r in rows],
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
                   MAX(o.reason) AS reason,
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
        return [_catalog_row(r) for r in rows]

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
                       o.ref_text, o.flag_text, o.value_num, o.value_canonical, o.unit_canonical, o.comparator,
                       to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI:SS') AS local_time,
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
        return [_reading_row(r) for r in rows]

    async def buckets(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, resolution: str, limit: int
    ) -> list[dict]:
        """One point per bucket, the newest `limit` per series, oldest first.
        Day and coarser read the day authority. `total` per series comes from
        the same statement, as for `readings`."""
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        if resolution in _SUBDAY_TRUNC:
            rows = await self._subday_buckets(subject_id, names, window, resolution, limit)
        else:
            rows = await self._day_buckets(subject_id, names, window, resolution, limit)
        return [_bucket_row(r) for r in rows]

    async def stats(self, subject_id: str, sel: query.Selection, window: query.Window) -> list[dict]:
        """count/min/max/avg/first/last/change per series over the WHOLE
        window, in SQL, over `_STATS_CTE`: a day with an elected authority
        counts once, as that value; any other day counts every reading.

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
        params: dict[str, Any] = {"uid": str(subject_id), "names": names, "reported": REPORTED_KINDS}
        where = _window_clause(params, window)
        source = _STATS_CTE.format(where=where)
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
                   to_char((ARRAY_AGG(local_date ORDER BY at ASC))[1], 'YYYY-MM-DD') AS first_date,
                   (ARRAY_AGG(value_text ORDER BY at DESC))[1] AS last,
                   to_char((ARRAY_AGG(local_date ORDER BY at DESC))[1], 'YYYY-MM-DD') AS last_date,
                   {_STAT_UNIT} AS unit,
                   COUNT(DISTINCT unit_ucum) > 1 AS mixed_units,
                   (ARRAY_AGG(value_num ORDER BY at ASC))[1] AS first_num,
                   (ARRAY_AGG(value_num ORDER BY at DESC))[1] AS last_num,
                   bool_or(reported) AS reported
              FROM base
             GROUP BY series_id
             ORDER BY display
            """,
            params,
            log_sql=False,
        ) or []
        return [_stats_row(r) for r in rows]

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
                   o.series_id, o.display, o.name_text, o.value_text, o.unit_text, o.ref_text, o.flag_text, o.value_num,
                   o.value_canonical, o.unit_canonical, o.code_system, o.code, o.local_date, o.elected, o.modality,
                   o.outcome, {_REPORTED_COLUMNS},
                   {_FILE_KEY} AS file_key, {_FILE_NAME},
                   to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI:SS') AS local_time
              FROM v_observation o
            {_FILE_JOIN}
             WHERE o.user_id = :uid AND o.series_id = ANY(:names) {where}
             ORDER BY o.series_id, o.local_date DESC, o.elected DESC, o.observed_start DESC, o.id DESC
            """,
            params,
            log_sql=False,
        ) or []
        return [_latest_row(r) for r in rows]

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
        params: dict[str, Any] = {
            "uid": str(subject_id),
            "names": names,
            "lower": [n.lower() for n in names],
            "keys": [translate.name_key(n) for n in names],
        }
        kinds = self._kinds(params)
        rows = await execute_query(
            f"""
            SELECT DISTINCT o.series_id
              FROM v_observation o
             WHERE o.user_id = :uid{kinds}
               AND (o.series_id = ANY(:names) OR o.code = ANY(:names)
                    OR lower(o.display) = ANY(:lower) OR o.name_key = ANY(:keys))
            """,
            params,
            log_sql=False,
        ) or []
        return [str(r["series_id"]) for r in rows]

    async def _labels(self, subject_id: str, window: query.Window | None) -> list[tuple[str, str]]:
        """`(label, series_id)` for every display and printed name."""
        params: dict[str, Any] = {"uid": str(subject_id)}
        where = self._kinds(params) + _window_clause(params, window)
        rows = await execute_query(
            f"SELECT o.series_id, o.name_text, o.display FROM v_observation o"
            f" WHERE o.user_id = :uid {where}"
            f" GROUP BY o.series_id, o.name_text, o.display",
            params,
            log_sql=False,
        ) or []
        out: list[tuple[str, str]] = []
        for r in rows:
            sid = str(r["series_id"])
            out.append((str(r["name_text"]), sid))
            if r["display"]:
                out.append((str(r["display"]), sid))
        return out

    async def _by_keywords(self, subject_id: str, keywords: tuple[str, ...], window: query.Window | None) -> list[str]:
        """Free text to the person's own series, in two tiers per keyword: a
        lexical rank over their printed and display names (free, deterministic,
        scoped to what they have), then the series the offline resolvers' codes
        name, matched against their series, which reaches the same place with
        no key at all.

        The tiers are per keyword: in ["血压", "头痛"] a lexical hit on the
        first must not stop the second from reaching an entry written 头疼,
        which only the code (NS01) connects."""
        kws = [k.strip() for k in keywords if k and k.strip()]
        if not kws:
            return []
        labels = await self._labels(subject_id, window)
        by_label: dict[str, str] = {}
        for label, sid in labels:
            by_label.setdefault(label, sid)
        found: list[str] = []
        for kw in kws:
            ranked = [by_label[label] for label in query.rank_catalog(kw, list(by_label), limit=MAX_KEYWORD_NAMES)]
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
        writer files both (`translate.series_of`), mass and moles, with or
        without a method."""
        named: set[str] = set()
        try:
            from mirobody.engine import resolve
            hit = resolve(keyword)
            if hit.resolved and hit.loinc and hit.method == "lexical":
                series = translate.series_of(hit.loinc)
                if series:
                    named.add(series)
        except Exception as e:
            logger.warning("offline resolver unavailable in keyword recall: error_type=%s", type(e).__name__,
                           exc_info=not is_driver_exception(e))
        if self._reported:
            for coding in (translate.resolve_symptom(keyword), translate.resolve_condition(keyword)):
                if coding.outcome == "coded" and coding.code:
                    named.add(coding.series_id)
        return named

    async def _subday_buckets(
        self, subject_id: str, names: list[str], window: query.Window, resolution: str, limit: int
    ) -> list[dict]:
        params: dict[str, Any] = {"uid": str(subject_id), "names": names, "limit": max(1, int(limit))}
        where = _window_clause(params, window)
        trunc = _SUBDAY_TRUNC[resolution]
        return await execute_query(
            f"""
            WITH base AS (
                SELECT o.series_id, o.display, o.code_system, o.code, o.value_num, o.value_canonical,
                       o.unit_ucum, o.unit_canonical, date_trunc('{trunc}', {_LOCAL_TS}) AS at
                  FROM v_observation o
                 WHERE o.user_id = :uid AND o.series_id = ANY(:names) AND o.value_num IS NOT NULL {where}
            ), grouped AS (
                SELECT series_id, {_LATEST_IDENTITY},
                       to_char(at, 'YYYY-MM-DD HH24:MI') AS period,
                       COUNT(*) AS n,
                       ROUND(({_STAT_VALUE.format(agg="AVG")})::numeric, 4) AS avg,
                       {_STAT_VALUE.format(agg="MIN")} AS min,
                       {_STAT_VALUE.format(agg="MAX")} AS max,
                       {_STAT_UNIT} AS unit,
                       false AS elected,
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
        self, subject_id: str, names: list[str], window: query.Window, resolution: str, limit: int
    ) -> list[dict]:
        """Day and coarser: one row per (series, bucket), from the day
        authority where one has been elected and the newest reading of the
        day otherwise, which `provenance` reports."""
        params: dict[str, Any] = {
            "uid": str(subject_id), "names": names, "reported": REPORTED_KINDS, "limit": max(1, int(limit)),
        }
        where = _window_clause(params, window)
        trunc = _DAY_TRUNC[resolution]
        bucket = f"date_trunc('{trunc}', at::timestamp)"
        return await execute_query(
            f"""
            {_DAY_AUTHORITY_CTE.format(where=where)}, grouped AS (
                SELECT series_id, {_LATEST_IDENTITY},
                       to_char({bucket}, 'YYYY-MM-DD') AS period,
                       COUNT(*) AS n,
                       ROUND(({_STAT_VALUE.format(agg="AVG")})::numeric, 4) AS avg,
                       {_STAT_VALUE.format(agg="MIN")} AS min,
                       {_STAT_VALUE.format(agg="MAX")} AS max,
                       {_STAT_UNIT} AS unit,
                       bool_or(elected) AS elected,
                       bool_or(reported) AS reported,
                       {_BUCKET_BUDGET.format(bucket=bucket)}
                  FROM base
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
_STATS_CTE = """
WITH day_rows AS (
    SELECT o.series_id, o.display, o.code_system, o.code, o.observed_start AS at, o.local_date, o.value_text,
           o.value_num, o.value_canonical, o.unit_ucum, o.unit_canonical, o.elected, o.kind = ANY(:reported) AS reported,
           bool_or(o.elected) OVER (PARTITION BY o.series_id, o.local_date) AS day_elected
      FROM v_observation o
     WHERE o.user_id = :uid AND o.series_id = ANY(:names) {where}
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
_DAY_AUTHORITY_CTE = """
WITH base AS (
    SELECT DISTINCT ON (o.series_id, o.local_date)
           o.series_id, o.display, o.code_system, o.code, o.local_date::timestamp AS at, o.value_text, o.value_num,
           o.value_canonical, o.unit_ucum, o.unit_canonical, o.elected, o.kind = ANY(:reported) AS reported
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


def _reported(r: dict) -> dict:
    """The columns only a reported row has. Empty for a reading, so a table of
    readings renders exactly as it did before reported rows existed."""
    if not r.get("reported"):
        return {}
    coded = r.get("outcome", "coded") == "coded"
    return {
        "kind": r.get("kind") or "",
        "reason": "" if coded else (r.get("reason") or ""),
        "note": _text(r.get("note")),
        "provenance": PROVENANCE_REPORTED,
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
        **({"provenance": PROVENANCE_REPORTED} if r.get("reported") else {}),
    }


def _reading_row(r: dict) -> dict:
    return {
        **_identity(r),
        "name": r.get("name_text") or "",
        "time": _text(r.get("local_time")),
        "date": _text(r.get("local_date")),
        "value": _text(r.get("value_text")),
        "unit": r.get("unit_text") or "",
        # The range as printed, empty when the report printed none: without
        # it a model judged against a range it remembered (1.5.4 local runs).
        # The flag is high or low as the report printed it, else empty
        # (`collect/files/services/indicator_store.value_and_flag`).
        "ref": r.get("ref_text") or "",
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


def _bucket_row(r: dict) -> dict:
    return {
        **_identity(r),
        "period": r.get("period") or "",
        "avg": _number(r.get("avg")),
        "min": _number(r.get("min")),
        "max": _number(r.get("max")),
        "n": int(r.get("n") or 0),
        "unit": r.get("unit") or "",
        "total": int(r.get("total") or 0),
        "day_known": True,
        "provenance": "elected:day_authority" if r.get("elected") else "measured",
        **({"provenance": PROVENANCE_REPORTED} if r.get("reported") else {}),
    }


def _stats_row(r: dict) -> dict:
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
        "day_known": True,
        "provenance": PROVENANCE_REPORTED if r.get("reported") else "computed",
    }
    # `change` only when both ends are numbers in one unit: a delta across
    # mg/dL and mmol/L, or across "Positive" and "Negative", is not a delta.
    first, last = _number(r.get("first_num")), _number(r.get("last_num"))
    if first is not None and last is not None and not out["mixed_units"]:
        out["change"] = round(last - first, 4)
    return out


def _latest_row(r: dict) -> dict:
    """A reading row without the paging fields: the latest value is the answer
    asked for most, and it carries the same range, flag and document."""
    row = _reading_row(r)
    del row["row_id"], row["total"]
    return row


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _number(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _record_row(r: dict) -> dict:
    """One row of the records list: a reading row's fields (the same names the
    Indicators table and the tools read), plus what kind of entry it is, where
    it came from, `created_at`, the start of its current visible period, and
    the stored instants and parsed number that the developer API's
    `GET /api/data` answers with.

    An entry that is not a measurement carries no value and no unit, never a
    made-up one; what the person wrote is `text`."""
    row = _reading_row(r)
    row.pop("total", None)
    kind = r.get("kind") or KIND_MEASUREMENT
    row.update({
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
        row.update({"value": "", "unit": "", "value_num": None, "unit_ucum": "", "value_canonical": None,
                    "unit_canonical": ""})
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
    "REPORTED_KINDS",
    "REST_CATALOG_MAX",
    "REST_ROW_MAX",
]
