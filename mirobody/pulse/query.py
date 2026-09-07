"""The read side of `th_series_data` — one read authority, in SQL.

It lives beside `readings.py`, the one WRITER of the same table, because the
table is what the two share: a reader that guesses where a day begins while
the writer stores one is the whole class of bug this pair exists to prevent.
(`meds/store.py` is the same arrangement for the medication tables.) The agent
layer holds the TOOL — `agent/tools/health_indicators_service.py` — and this
is the implementation it is handed.

Every surface that shows a person their own readings goes through this class:
the chat agent's tool, an MCP client, the web client's Indicators tab, a daily
summary. That is the point of the port. Before it, the model's tool and the
browser's endpoint shared a service but each aggregation surface answered "what
is my resting heart rate on the 3rd" with its own SQL and its own idea of where
a day begins, and the two could disagree on screen.

## The two window semantics

`th_series_data.start_time` is a naive LOCAL wall clock. Rows written since
`a4_series_data_day_authority.sql` carry `local_date`, computed at write time
through the metric's own window (`"18:00"` for the sleep family), so a day
query is an equality on a date column: `tz_exact`. Rows older than that
migration have `local_date IS NULL` until the backfill reaches them, and those
are found by casting the timestamp with a day of padding either side —
`date_padded_naive`, which is why a June window used to return a May 31 bucket.
Which one answered is reported in `query.Meta.window_semantics`; it is not a
detail to hide, because it is the difference between "on the 3rd" and "around
the 3rd".

## Election

For `resolution=day` and coarser, the answer is the day's published authority:
the `elected` row for that `(indicator, local_date)`. Election happens once, on
the write side (`pulse/aggregate`), which is what keeps the chat answer and the
dashboard identical by construction rather than by two implementations
agreeing. Where nothing has been elected yet the query falls back to the rows
themselves and says so in `provenance` — `elected:<rule>` when a row was
elected, `measured` when it was not.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from ..kernel import metrics, query, series
from ..utils import execute_query

logger = logging.getLogger(__name__)

#: `value` is TEXT: it also holds "Positive", "120/80" and free text. Cast only
#: what is unambiguously numeric, so AVG/MIN/MAX skip the rest instead of
#: erroring the whole query.
_NUMERIC = r"CASE WHEN tsd.value ~ '^-?[0-9]+(\.[0-9]+)?$' THEN tsd.value::numeric END"
#: The same cast against a projected `value` column.
_NUMERIC_V = r"CASE WHEN value ~ '^-?[0-9]+(\.[0-9]+)?$' THEN value::numeric END"

#: Every reading in the window, as `(indicator, at, value, unit, day_known)`.
_READINGS_CTE = """
WITH base AS (
    SELECT tsd.indicator, tsd.start_time AS at, tsd.value,
           tsd.fhir_mapping_info ->> 'unit' AS unit,
           tsd.local_date IS NOT NULL AS day_known
      FROM th_series_data tsd
     WHERE tsd.user_id = :uid AND tsd.deleted = 0
       AND tsd.indicator = ANY(:names) {where}
)"""

#: One row per (indicator, local day): the elected authority where one exists,
#: the newest reading of that day otherwise. `DISTINCT ON` does both in one
#: pass, so a store that has never been through an election still answers.
_DAY_AUTHORITY_CTE = """
WITH base AS (
    SELECT DISTINCT ON (tsd.indicator, COALESCE(tsd.local_date, tsd.start_time::date))
           tsd.indicator,
           COALESCE(tsd.local_date, tsd.start_time::date)::timestamp AS at,
           tsd.value,
           tsd.fhir_mapping_info ->> 'unit' AS unit,
           tsd.local_date IS NOT NULL AS day_known
      FROM th_series_data tsd
     WHERE tsd.user_id = :uid AND tsd.deleted = 0
       AND tsd.indicator = ANY(:names) {where}
     ORDER BY tsd.indicator, COALESCE(tsd.local_date, tsd.start_time::date),
              tsd.elected DESC, tsd.start_time DESC
)"""

#: Names listed for the MODEL: a token budget. The browser gets `_REST_CATALOG`
#: — capping its catalogue at a model's context window hid 44 of the demo
#: user's 244 indicators AND reported `count: 200` as if that were the total.
CATALOG_MAX = 200
REST_CATALOG_MAX = 2000

#: How many indicator names one keyword search may expand to. Discovery has a
#: budget: twenty names at fifty rows each is a thousand rows for a question the
#: model has not asked yet; the catalogue is the documented way to widen it.
MAX_KEYWORD_NAMES = 12

#: `resolution` → the Postgres `date_trunc` field for a sub-day bucket.
_SUBDAY_TRUNC = {"minute": "minute", "hour": "hour"}
#: `resolution` → how a run of local days is grouped for a day-or-coarser bucket.
_DAY_TRUNC = {"day": "day", "week": "week", "month": "month"}


class PostgresHealthQuery:
    """`query.HealthQuery` over `th_series_data`.

    Async: the port is declared with plain `def` so an in-memory implementation
    stays possible, and `HealthIndicatorsService` awaits whatever it gets back.
    """

    # --- subject-level facts -------------------------------------------------

    async def tz(self, subject_id: str) -> str:
        """The subject's IANA zone, `"UTC"` when unset. Never the server's zone
        and never the session's: a window is the person's day.

        Through `user.get_user` rather than its own SELECT, because that
        function is where `is_del = false` lives — a hand-rolled lookup answers
        for deleted accounts, time zone and all.
        """
        from ..user.user import get_user
        row = await get_user(user_id=subject_id)
        return ((row or {}).get("tz") or "").strip() or "UTC"

    async def on_read(self, subject_id: str) -> None:
        """The read-time refresh seam.

        This deployment aggregates in a background worker, so a read refreshes
        nothing and this is a no-op. It stays on the port because the other
        shape — recompute the dirty windows before answering — is common, and a
        consumer that needs it must have somewhere to put it that every surface
        already calls.
        """
        return None

    # --- the six leaves ------------------------------------------------------

    async def catalog(self, subject_id: str, window: query.Window | None, *, cap: int = CATALOG_MAX) -> list[dict]:
        """What this person actually has — the honest answer to a miss.

        `total` rides on every row (a window function runs before LIMIT), so a
        truncated page can say how much it left out. Reporting `count: 200` as
        though it were the total is how 44 indicators went missing in silence.
        """
        params: dict[str, Any] = {"uid": str(subject_id), "cap": cap}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT tsd.indicator,
                   COUNT(*) AS count,
                   COUNT(*) OVER () AS total,
                   MAX(fi.indicator_standard) AS system,
                   MAX(fi.code) AS code,
                   to_char(MIN(tsd.start_time), 'YYYY-MM-DD') AS first_date,
                   to_char(MAX(tsd.start_time), 'YYYY-MM-DD') AS last_date,
                   (ARRAY_AGG(tsd.value ORDER BY tsd.start_time DESC))[1] AS latest_value,
                   (ARRAY_AGG(tsd.fhir_mapping_info ->> 'unit' ORDER BY tsd.start_time DESC))[1] AS unit,
                   bool_and(tsd.local_date IS NOT NULL) AS day_known
              FROM th_series_data tsd
              LEFT JOIN fhir_indicators fi ON tsd.fhir_id = fi.id
             WHERE tsd.user_id = :uid AND tsd.deleted = 0 {where}
             GROUP BY tsd.indicator
             ORDER BY tsd.indicator
             LIMIT :cap
            """,
            params,
            log_sql=False,
        ) or []
        return [_catalog_row(r) for r in rows]

    async def readings(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, limit: int
    ) -> list[dict]:
        """Individual readings, newest first, capped per indicator. A baseline
        is `stats` (first/first_date), not a reversed page.

        `total` per indicator comes from the same statement rather than a
        second round trip, so "5 of 667 shown" is one query. `file_key` is the
        handle back to the ORIGINAL lab report — the whole reason the engine
        keeps the source document.
        """
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {"uid": str(subject_id), "names": names, "limit": max(1, min(int(limit), query.MAX_LIMIT))}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT id, indicator, start_time, value, info, source_table_id, total, day_known FROM (
                SELECT tsd.id, tsd.indicator, tsd.start_time, tsd.value,
                       tsd.fhir_mapping_info AS info,
                       tsd.local_date IS NOT NULL AS day_known,
                       CASE WHEN tsd.source_table = 'th_files' THEN tsd.source_table_id END AS source_table_id,
                       COUNT(*) OVER (PARTITION BY tsd.indicator) AS total,
                       ROW_NUMBER() OVER (PARTITION BY tsd.indicator ORDER BY tsd.start_time DESC) AS rn
                  FROM th_series_data tsd
                 WHERE tsd.user_id = :uid AND tsd.deleted = 0
                   AND tsd.indicator = ANY(:names) {where}
            ) s
            WHERE rn <= :limit
            ORDER BY indicator, start_time DESC
            """,
            params,
            log_sql=False,
        ) or []
        coding = await self._coding_for(subject_id, names)
        return [_reading_row(r, coding) for r in rows]

    async def buckets(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, resolution: str
    ) -> list[dict]:
        """One point per bucket. Day and coarser read the day authority."""
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        coding = await self._coding_for(subject_id, names)
        if resolution in _SUBDAY_TRUNC:
            rows = await self._subday_buckets(subject_id, sel, window, resolution)
        else:
            rows = await self._day_buckets(subject_id, sel, window, resolution, names)
        return [_bucket_row(r, coding) for r in rows]

    async def stats(self, subject_id: str, sel: query.Selection, window: query.Window, *, basis: str) -> list[dict]:
        """count/min/max/avg/first/last/change per indicator over the WHOLE
        window — computed in SQL, never over a fetched page. "How did my LDL
        change this year" must not require pulling every reading.

        `basis` decides WHAT is averaged, and it is not decoration. At
        `resolution=day` the basis is `daily`: the mean of the day authorities,
        which is what "my average resting heart rate this month" means. Over
        raw readings the same month would weight a day with 40 intraday samples
        forty times against a day with one.
        """
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {"uid": str(subject_id), "names": names}
        where = _window_clause(params, window)
        source = _DAY_AUTHORITY_CTE.format(where=where) if basis == "daily" else _READINGS_CTE.format(where=where)
        rows = await execute_query(
            f"""
            {source}
            SELECT indicator,
                   COUNT(*) AS count,
                   COUNT({_NUMERIC_V}) AS numeric_count,
                   MIN({_NUMERIC_V}) AS min,
                   MAX({_NUMERIC_V}) AS max,
                   ROUND(AVG({_NUMERIC_V})::numeric, 4) AS avg,
                   (ARRAY_AGG(value ORDER BY at ASC))[1] AS first,
                   to_char(MIN(at), 'YYYY-MM-DD') AS first_date,
                   (ARRAY_AGG(value ORDER BY at DESC))[1] AS last,
                   to_char(MAX(at), 'YYYY-MM-DD') AS last_date,
                   (ARRAY_AGG(unit ORDER BY at DESC))[1] AS unit,
                   bool_and(day_known) AS day_known
              FROM base
             GROUP BY indicator
             ORDER BY indicator
            """,
            params,
            log_sql=False,
        ) or []
        coding = await self._coding_for(subject_id, names)
        return [_stats_row(r, coding, basis) for r in rows]

    async def latest(self, subject_id: str, sel: query.Selection, window: query.Window, *, basis: str) -> list[dict]:
        """The most recent value per indicator INSIDE the window. A `latest`
        that ignored the window would answer "your last reading" to "what was
        it in March", which is a different question with the same shape."""
        names = await self._resolve(subject_id, sel, window)
        if not names:
            return []
        params: dict[str, Any] = {"uid": str(subject_id), "names": names}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"""
            SELECT DISTINCT ON (tsd.indicator)
                   tsd.indicator, tsd.start_time, tsd.value,
                   tsd.fhir_mapping_info ->> 'unit' AS unit, tsd.local_date, tsd.elected,
                   tsd.local_date IS NOT NULL AS day_known
              FROM th_series_data tsd
             WHERE tsd.user_id = :uid AND tsd.deleted = 0
               AND tsd.indicator = ANY(:names) {where}
             ORDER BY tsd.indicator, tsd.elected DESC, tsd.start_time DESC
            """,
            params,
            log_sql=False,
        ) or []
        coding = await self._coding_for(subject_id, names)
        return [_latest_row(r, coding, basis) for r in rows]

    # --- helpers -------------------------------------------------------------

    async def _resolve(self, subject_id: str, sel: query.Selection, window: query.Window | None) -> list[str]:
        """A `Selection` → the exact indicator names this person HAS.

        Never the global vocabulary: the answer to "do I have HbA1c" is about
        this person's record, and offering a name they have no reading for
        sends the model into a second empty call.
        """
        if sel.indicators:
            names = list(sel.indicators)
        elif sel.keywords:
            names = await self._by_keywords(subject_id, sel.keywords, window)
        else:
            names = []
        return names

    async def _names_for(self, subject_id: str, window: query.Window | None) -> list[str]:
        params: dict[str, Any] = {"uid": str(subject_id)}
        where = _window_clause(params, window)
        rows = await execute_query(
            f"SELECT DISTINCT tsd.indicator FROM th_series_data tsd"
            f" WHERE tsd.user_id = :uid AND tsd.deleted = 0 {where} ORDER BY 1",
            params,
            log_sql=False,
        ) or []
        return [r["indicator"] for r in rows]

    async def _by_keywords(self, subject_id: str, keywords: tuple[str, ...], window: query.Window | None) -> list[str]:
        """Free text → the person's own indicator names, in three tiers.

        1. `query.rank_catalog` — a zero-API lexical rank over THIS PERSON'S
           catalogue, with the shipped zh↔en synonym seed;
        2. semantic recall, for a surface that shares no characters with the
           stored name (another language, a brand for a generic);
        3. the offline resolver's LOINC code matched against the catalogue's
           codes, which reaches the same place with no key at all.

        The lexical tier is FIRST, and that is a change of order with a
        reason. It is free, deterministic and scoped to what the person has,
        whereas a vector search returns its top-k for any input at all: asked
        for `definitely-not-zzz` it confidently offered ten sleep metrics,
        which the model then had to be told to disbelieve. Semantics still
        answer what lexical recall cannot — `血红蛋白` finds hemoglobin — and
        now cost an API call only in that case.
        """
        kws = [k.strip() for k in keywords if k and k.strip()]
        if not kws:
            return []

        catalog = await self._names_for(subject_id, window)
        ranked: list[str] = []
        for kw in kws:
            ranked.extend(query.rank_catalog(kw, catalog, limit=MAX_KEYWORD_NAMES))
        if ranked:
            return list(dict.fromkeys(ranked))[:MAX_KEYWORD_NAMES]

        semantic = await self._by_semantic_search(subject_id, kws, window)
        if semantic:
            return semantic[:MAX_KEYWORD_NAMES]
        return (await self._by_resolved_code(subject_id, kws, catalog))[:MAX_KEYWORD_NAMES]

    async def _by_semantic_search(
        self, subject_id: str, kws: list[str], window: query.Window | None
    ) -> list[str]:
        """The embedding tier. Unavailable without a key, and that used to fail
        SILENTLY: the search answered "no indicator matched" for `HbA1c` while
        the person's own `GlycatedHemoglobin-HbA1c` sat in the table."""
        try:
            from ..indicator.fhir.adapter import FhirAdapter
            from ..indicator.search import search
            found = await search(
                adapter=FhirAdapter(bundle_dir=None), user_id=str(subject_id),
                keywords=kws, start_time=(window.start if window else None),
                end_time=(window.end if window else None),
            ) or []
        except Exception as e:
            logger.warning("semantic indicator search unavailable: error_type=%s", type(e).__name__)
            return []
        return list(dict.fromkeys(n for n in (i.get("indicator") for i in found) if n))

    async def _by_resolved_code(self, subject_id: str, kws: list[str], catalog: list[str]) -> list[str]:
        codes: set[str] = set()
        try:
            from ..engine import resolve
            for kw in kws:
                result = resolve(kw)
                if getattr(result, "resolved", False) and getattr(result, "loinc", ""):
                    codes.add(result.loinc)
        except Exception as e:
            # No resolver data (the LFS bundle absent, say) — the lexical tier
            # above already ran; this one simply contributes nothing.
            logger.warning("offline resolver unavailable in keyword recall: error_type=%s", type(e).__name__)
        if not codes:
            return []
        coding = await self._coding_for(subject_id, catalog)
        return [n for n in catalog if (coding.get(n) or {}).get("code") in codes]

    async def _coding_for(self, subject_id: str, names: list[str]) -> dict[str, dict]:
        """indicator → `{system, code}`: the terminology identity, which is
        what makes two differently-named results comparable. It used to be
        computed by the search pipeline and dropped before the model saw it."""
        if not names:
            return {}
        rows = await execute_query(
            "SELECT DISTINCT tsd.indicator, fi.indicator_standard AS system, fi.code"
            "  FROM th_series_data tsd JOIN fhir_indicators fi ON tsd.fhir_id = fi.id"
            " WHERE tsd.user_id = :uid AND tsd.deleted = 0 AND tsd.indicator = ANY(:names)",
            {"uid": str(subject_id), "names": names},
            log_sql=False,
        ) or []
        out = {r["indicator"]: {"system": r["system"] or "", "code": r["code"] or ""} for r in rows}
        # The catalogue answers for anything the store could not: a metric with
        # a public LOINC gets it, everything else gets this project's own
        # namespace rather than an empty identity.
        for name in names:
            if not (out.get(name) or {}).get("code"):
                system, code = metrics.canonical(name)
                out[name] = {"system": system, "code": code}
        return out

    async def _subday_buckets(
        self, subject_id: str, sel: query.Selection, window: query.Window, resolution: str
    ) -> list[dict]:
        params: dict[str, Any] = {"uid": str(subject_id), "names": await self._resolve(subject_id, sel, window)}
        where = _window_clause(params, window)
        trunc = _SUBDAY_TRUNC[resolution]
        return await execute_query(
            f"""
            SELECT tsd.indicator,
                   to_char(date_trunc('{trunc}', tsd.start_time), 'YYYY-MM-DD HH24:MI') AS period,
                   COUNT(*) AS n,
                   ROUND(AVG({_NUMERIC})::numeric, 4) AS avg,
                   MIN({_NUMERIC}) AS min,
                   MAX({_NUMERIC}) AS max,
                   (ARRAY_AGG(tsd.fhir_mapping_info ->> 'unit'))[1] AS unit,
                   false AS elected,
                   bool_and(tsd.local_date IS NOT NULL) AS day_known
              FROM th_series_data tsd
             WHERE tsd.user_id = :uid AND tsd.deleted = 0
               AND tsd.indicator = ANY(:names) {where}
             GROUP BY tsd.indicator, date_trunc('{trunc}', tsd.start_time)
             ORDER BY tsd.indicator, period
            """,
            params,
            log_sql=False,
        ) or []

    async def _day_buckets(
        self, subject_id: str, sel: query.Selection, window: query.Window, resolution: str, names: list[str]
    ) -> list[dict]:
        """Day and coarser: one row per (indicator, bucket), from the day
        authority where one has been elected.

        The inner `DISTINCT ON` picks the elected row of each local day when
        there is one and the newest otherwise, in a single pass — so a store
        that has not been through an election yet still answers, and says
        which it was.
        """
        params: dict[str, Any] = {"uid": str(subject_id), "names": names}
        where = _window_clause(params, window)
        trunc = _DAY_TRUNC[resolution]
        day_expr = _day_expression()
        return await execute_query(
            f"""
            WITH authority AS (
                SELECT DISTINCT ON (tsd.indicator, {day_expr})
                       tsd.indicator,
                       {day_expr} AS day,
                       tsd.value,
                       tsd.elected,
                       tsd.local_date IS NOT NULL AS day_known,
                       tsd.fhir_mapping_info ->> 'unit' AS unit
                  FROM th_series_data tsd
                 WHERE tsd.user_id = :uid AND tsd.deleted = 0
                   AND tsd.indicator = ANY(:names) {where}
                 ORDER BY tsd.indicator, {day_expr}, tsd.elected DESC, tsd.start_time DESC
            )
            SELECT indicator,
                   to_char(date_trunc('{trunc}', day::timestamp), 'YYYY-MM-DD') AS period,
                   COUNT(*) AS n,
                   ROUND(AVG(CASE WHEN value ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN value::numeric END)::numeric, 4) AS avg,
                   MIN(CASE WHEN value ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN value::numeric END) AS min,
                   MAX(CASE WHEN value ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN value::numeric END) AS max,
                   (ARRAY_AGG(unit))[1] AS unit,
                   bool_or(elected) AS elected,
                   bool_and(day_known) AS day_known
              FROM authority
             GROUP BY indicator, date_trunc('{trunc}', day::timestamp)
             ORDER BY indicator, period
            """,
            params,
            log_sql=False,
        ) or []


# --- row shapers (pure) ------------------------------------------------------


def _day_expression() -> str:
    """The local day of a row: the stored column when it is there, the
    timestamp cast when it is not. One expression, so `tz_exact` and
    `date_padded_naive` rows can sit in the same result."""
    return "COALESCE(tsd.local_date, tsd.start_time::date)"


def _window_clause(params: dict[str, Any], window: query.Window | None) -> str:
    """The window as SQL, in whichever semantics the rows support.

    A row with `local_date` is compared on the date, inclusive at both ends —
    that is `tz_exact`. A row without one falls back to the naive timestamp
    padded a day each way, because the timestamp's own zone is unknown; the
    padding is why the answer says `date_padded_naive` rather than pretending.
    """
    if window is None or not (window.start or window.end):
        return ""
    start = _date_of(window.start_ms, window.tz)
    end = _date_of(window.end_ms - 1, window.tz)
    params["day_from"], params["day_to"] = start, end
    params["ts_from"] = datetime.combine(start, datetime.min.time()) - timedelta(days=1)
    params["ts_to"] = datetime.combine(end, datetime.max.time()) + timedelta(days=1)
    return (
        " AND (CASE WHEN tsd.local_date IS NOT NULL"
        "           THEN tsd.local_date BETWEEN :day_from AND :day_to"
        "           ELSE tsd.start_time BETWEEN :ts_from AND :ts_to END)"
    )


def _date_of(ms: int, tz: str):
    return datetime.fromtimestamp(ms / 1000, series.zone(tz)).date()


def _catalog_row(r: dict) -> dict:
    return {
        "indicator": r.get("indicator") or "",
        "system": r.get("system") or "",
        "code": r.get("code") or "",
        "count": int(r.get("count") or 0),
        "unit": r.get("unit") or "",
        "latest_value": _text(r.get("latest_value")),
        "first_date": r.get("first_date") or "",
        "last_date": r.get("last_date") or "",
        "total": int(r.get("total") or 0),
        "day_known": bool(r.get("day_known")),
    }


def _reading_row(r: dict, coding: dict[str, dict]) -> dict:
    info = r.get("info") or {}
    code = coding.get(r.get("indicator") or "") or {}
    # A th_files row's `source_table_id` IS the file key; legacy rows carried a
    # "<file_key>_#_<row>" suffix, and requiring the suffix cost every current
    # reading its link back to the report it came from.
    file_key = (r.get("source_table_id") or "").split("_#_")[0]
    return {
        "indicator": r.get("indicator") or "",
        "time": _text(r.get("start_time")),
        "value": _text(r.get("value")),
        "unit": (info.get("unit") if isinstance(info, dict) else "") or "",
        "file_key": file_key,
        "row_id": r.get("id"),
        "system": code.get("system", ""),
        "code": code.get("code", ""),
        "total": int(r.get("total") or 0),
        "day_known": bool(r.get("day_known")),
        "provenance": "measured",
    }


def _bucket_row(r: dict, coding: dict[str, dict]) -> dict:
    code = coding.get(r.get("indicator") or "") or {}
    return {
        "indicator": r.get("indicator") or "",
        "period": r.get("period") or "",
        "avg": _number(r.get("avg")),
        "min": _number(r.get("min")),
        "max": _number(r.get("max")),
        "n": int(r.get("n") or 0),
        "unit": r.get("unit") or "",
        "system": code.get("system", ""),
        "code": code.get("code", ""),
        "day_known": bool(r.get("day_known")),
        "provenance": "elected:day_authority" if r.get("elected") else "measured",
    }


def _stats_row(r: dict, coding: dict[str, dict], basis: str) -> dict:
    code = coding.get(r.get("indicator") or "") or {}
    first, last = _text(r.get("first")), _text(r.get("last"))
    out = {
        "indicator": r.get("indicator") or "",
        "count": int(r.get("count") or 0),
        "numeric_count": int(r.get("numeric_count") or 0),
        "min": _number(r.get("min")),
        "max": _number(r.get("max")),
        "avg": _number(r.get("avg")),
        "first": first,
        "first_date": r.get("first_date") or "",
        "last": last,
        "last_date": r.get("last_date") or "",
        "unit": r.get("unit") or "",
        "system": code.get("system", ""),
        "code": code.get("code", ""),
        "basis": basis,
        "day_known": bool(r.get("day_known")),
        "provenance": "computed",
    }
    # `change` only when BOTH ends are numeric: "Positive" minus "Negative" is
    # not a delta, and a model handed one will narrate it anyway.
    try:
        out["change"] = round(float(last) - float(first), 4)
    except (TypeError, ValueError):
        pass
    return out


def _latest_row(r: dict, coding: dict[str, dict], basis: str) -> dict:
    code = coding.get(r.get("indicator") or "") or {}
    return {
        "indicator": r.get("indicator") or "",
        "time": _text(r.get("start_time")),
        "date": _text(r.get("local_date")),
        "value": _text(r.get("value")),
        "unit": r.get("unit") or "",
        "system": code.get("system", ""),
        "code": code.get("code", ""),
        "basis": basis,
        "day_known": bool(r.get("day_known")),
        "provenance": "elected:day_authority" if r.get("elected") else "measured",
    }


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _number(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


__all__ = ["CATALOG_MAX", "PostgresHealthQuery", "REST_CATALOG_MAX"]
