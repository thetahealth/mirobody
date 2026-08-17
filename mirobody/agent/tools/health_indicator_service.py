import json
import logging

from pydantic import Field

from datetime import datetime, timedelta
from typing import Annotated, Any, Literal

from mirobody.utils import execute_query

class HealthIndicatorService:
    def __init__(self):
        self.name = "Indicator Service"
        self.version = "3.0.0"

    #-------------------------------------------------------------------------

    def _build_time_clause(
        self,
        params: dict,
        start_time: str | None = None,
        end_time: str | None = None,
        time_column: str = "tsd.start_time",
    ) -> str:
        conditions = []
        try:
            if start_time:
                date_part = start_time[:10] if len(start_time) >= 10 else start_time
                params["start_time"] = datetime.strptime(date_part + " 00:00:00", "%Y-%m-%d %H:%M:%S") - timedelta(days=1)
                conditions.append(f"AND {time_column} >= :start_time")
        except Exception as e:
            logging.warning(f"Failed to parse start_time '{start_time}': {e}")
        try:
            if end_time:
                date_part = end_time[:10] if len(end_time) >= 10 else end_time
                params["end_time"] = datetime.strptime(date_part + " 23:59:59", "%Y-%m-%d %H:%M:%S") + timedelta(days=1)
                conditions.append(f"AND {time_column} <= :end_time")
        except Exception as e:
            logging.warning(f"Failed to parse end_time '{end_time}': {e}")
        return " ".join(conditions)

    #-------------------------------------------------------------------------

    #-------------------------------------------------------------------------
    # The one health-data tool.

    # `value` is TEXT in th_series_data (it also holds "positive", "120/80",
    # free text). Cast only what is unambiguously numeric so AVG/MIN/MAX skip
    # the rest instead of erroring the whole query.
    _NUMERIC = r"CASE WHEN tsd.value ~ '^-?[0-9]+(\.[0-9]+)?$' THEN tsd.value::numeric END"

    _MAX_LIMIT = 500          # hard server-side ceiling, whatever the caller asks
    _CATALOG_MAX = 200        # names returned when listing what the user has

    async def query_health_indicators(
        self,
        user_info: dict[str, Any],
        keywords: list[str] | None = None,
        indicators: list[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        # Declared as Literal / Annotated so the SDK's schema generator emits a
        # real `enum` and real min/max bounds. A model that sends
        # aggregate="hourly" or limit=1000000 is now rejected by the schema
        # itself, before any of our code runs.
        aggregate: Literal["none", "stats", "day", "week", "month"] = "none",
        limit: Annotated[int, Field(ge=1, le=500)] = 50,
    ) -> dict[str, Any]:
        """
        Read the user's health indicator data — search AND fetch in ONE call.

        Args:
            keywords: Fuzzy search terms for indicators whose exact name you do not know.
                Include BOTH the abbreviation and the full name for medical shorthand,
                e.g. ["MCHC", "Mean Corpuscular Hemoglobin Concentration"]. Any language.
            indicators: EXACT indicator names, as returned by a previous call. Use this
                instead of `keywords` when you already know the names.
            start_time: Inclusive start date, "YYYY-MM-DD".
            end_time: Inclusive end date, "YYYY-MM-DD".
            aggregate: Shape of the answer. "none" returns individual readings.
                "stats" returns count/min/max/avg/first/last/change per indicator over
                the whole window. "day", "week" or "month" return one bucketed point per
                period. Use "stats" or a bucket for any trend question — never pull raw
                readings just to compute an average yourself.
            limit: Max readings per indicator for aggregate="none" (capped at 500).

        Returns:
            indicators: one entry per matched indicator, each with
                - indicator: its exact name (pass this back as `indicators` later)
                - system / code: the canonical terminology identity, e.g. LOINC 718-7.
                  Two indicators sharing a code are the same test and ARE comparable
                  even when their names differ; different codes are not.
                - count: how many readings matched
                - rows: a compact pipe-delimited table. The first line is the header;
                  a leading "(constants: k=v)" line lists columns identical on every
                  row (typically the unit), so they are not repeated per row.
            catalog: returned INSTEAD of `indicators` when you pass neither `keywords`
                nor `indicators` — the full list of indicators this user has, with codes
                and date ranges. Start here when you don't know what exists.
            truncated: present when a series was cut off by `limit`; maps indicator →
                total available. Narrow the window or use an aggregate instead of
                raising `limit`.

        Notes for LLMs:
            - ONE call is usually enough. Do not call this to find a name and then call
              it again to read the values — pass `keywords` and an `aggregate` together.
            - No match returns `catalog` so you can see what the user actually has;
              re-guessing keywords blindly is wasted effort.
            - Absence of data is not absence of the condition — the user may simply
              never have recorded it. Say so rather than concluding they are healthy.
        """
        user_id = user_info.get("user_id")
        if not user_id or not isinstance(user_id, str):
            return {"success": False, "error": "Authorization required."}

        mode = (aggregate or "none").strip().lower()
        if mode not in ("none", "stats", "day", "week", "month"):
            return {"success": False, "error": f"aggregate must be one of none|stats|day|week|month, got {mode!r}."}

        try:
            names = self._normalize_indicator_arg(indicators)

            # No exact names given but keywords are: resolve them first. This is the
            # step that used to be a separate `search_health_indicators` call.
            if not names and keywords:
                names, coding = await self._resolve_keywords(user_id, keywords, start_time, end_time)
            else:
                coding = await self._coding_for(user_id, names) if names else {}

            # Neither → the user asked "what do I have?".
            if not names:
                return await self._catalog(user_id, start_time, end_time, searched=bool(keywords))

            if mode == "none":
                payload, truncated = await self._rows(user_id, names, start_time, end_time, limit)
            else:
                payload, truncated = await self._aggregate(user_id, names, start_time, end_time, mode)

            for name, entry in payload.items():
                code = coding.get(name) or {}
                entry["system"] = code.get("system", "")
                entry["code"] = code.get("code", "")

            out: dict[str, Any] = {
                "success": True,
                "message": "Ok" if payload else "No data found",
                "indicators": payload or None,
            }
            if truncated:
                out["truncated"] = truncated
            return out

        except Exception as e:
            # Never hand the raw exception to the model — it leaks query shape and
            # parameter values to whatever MCP client is on the other end.
            logging.error(f"[query_health_indicators] {e}", exc_info=True)
            return {
                "success": False,
                "error": "This lookup could not complete.",
                "hint": "Do not repeat the identical call. Narrow the window, ask for "
                        "fewer indicators, or tell the user it is temporarily unavailable.",
            }

    #-------------------------------------------------------------------------
    # Helpers for query_health_indicators.

    @staticmethod
    def _table(rows: list[dict], columns: list[str]) -> str:
        """Rows → a pipe-delimited table, with constant columns factored out.

        Repeating `"unit": "mmol/L"` on every one of 200 rows is pure token cost
        for a third-party client that pays for its own context. Columns whose
        value never varies become a single `(constants: …)` line; empty columns
        disappear entirely.
        """
        if not rows:
            return ""
        present = [c for c in columns if any(str(r.get(c, "")) for r in rows)]
        constants = {
            c: str(rows[0].get(c, ""))
            for c in present
            if len({str(r.get(c, "")) for r in rows}) == 1
        }
        varying = [c for c in present if c not in constants]
        out: list[str] = []
        if constants:
            out.append("(constants: " + ", ".join(f"{k}={v}" for k, v in constants.items()) + ")")
        if not varying:                       # every column constant — header alone says it
            return "\n".join(out)
        out.append("|".join(varying))
        out.extend("|".join(str(r.get(c, "")) for c in varying) for r in rows)
        return "\n".join(out)

    @staticmethod
    def _normalize_indicator_arg(indicators: Any) -> list[str]:
        """Flatten whatever the model actually sent into a list of clean names.

        Models do not reliably send `["Heart Rate"]`. Observed in the wild: a
        JSON-stringified list (`'["Heart Rate"]'`), a list whose elements are
        themselves JSON strings, and a comma-separated string. Untreated, the
        SQL compares `indicator = ANY(...)` against the literal text
        `["Heart Rate"]` and matches nothing — a silent empty result, not an
        error. This surface is public, so tolerate the input.
        """
        def expand(item: Any) -> list[str]:
            if not isinstance(item, str) or not item.strip():
                return []
            s = item.strip()
            if s[0] == "[" and s[-1] == "]":
                try:
                    parsed = json.loads(s)
                except (json.JSONDecodeError, ValueError):
                    return [s]
                if isinstance(parsed, list):
                    return [n for el in parsed for n in expand(el)]
                return expand(parsed) if isinstance(parsed, str) else []
            return [part.strip() for part in s.split(",") if part.strip()]

        items = [indicators] if isinstance(indicators, str) else indicators
        if not isinstance(items, list):
            return []
        seen, out = set(), []
        for name in (n for item in items for n in expand(item)):
            if name not in seen:
                seen.add(name)
                out.append(name)
        return out

    async def _coding_for(self, user_id: str, names: list[str]) -> dict[str, dict]:
        """indicator name → {system, code} for the terminology identity.

        This is ② Sort's answer and the reason the engine exists; it used to be
        computed by the search pipeline and then dropped before the model saw it.
        """
        if not names:
            return {}
        sql = """
        SELECT DISTINCT tsd.indicator, fi.indicator_standard AS system, fi.code
          FROM th_series_data tsd
          JOIN fhir_indicators fi ON tsd.fhir_id = fi.id
         WHERE tsd.user_id = :user_id AND tsd.deleted = 0
           AND tsd.indicator = ANY(:names)
        """
        rows = await execute_query(sql, {"user_id": user_id, "names": names}) or []
        return {r["indicator"]: {"system": r["system"] or "", "code": r["code"] or ""} for r in rows}

    async def _resolve_keywords(
        self, user_id: str, keywords: list[str], start_time: str | None, end_time: str | None,
    ) -> tuple[list[str], dict[str, dict]]:
        """Fuzzy keywords → the exact indicator names this user actually has."""
        from mirobody.indicator.search import search
        from mirobody.indicator.fhir.adapter import FhirAdapter

        kws = [k.strip() for k in (keywords or []) if isinstance(k, str) and k.strip()]
        if not kws:
            return [], {}

        found = await search(
            adapter=FhirAdapter(bundle_dir=None), user_id=user_id,
            keywords=kws, start_time=start_time, end_time=end_time,
        ) or []
        names, coding = [], {}
        for ind in found:
            name = ind.get("indicator")
            if not name or name in coding:
                continue
            names.append(name)
            coding[name] = {"system": ind.get("system") or "", "code": ind.get("code") or ""}
        return names, coding

    async def _catalog(
        self, user_id: str, start_time: str | None, end_time: str | None, *, searched: bool,
    ) -> dict[str, Any]:
        """What this user actually has — the honest answer to a miss."""
        params: dict[str, Any] = {"user_id": user_id, "cap": self._CATALOG_MAX}
        time_clause = self._build_time_clause(params, start_time, end_time, "tsd.start_time")
        sql = f"""
        SELECT tsd.indicator,
               COUNT(*) AS count,
               MAX(fi.indicator_standard) AS system,
               MAX(fi.code) AS code,
               to_char(MIN(tsd.start_time), 'YYYY-MM-DD') AS first_date,
               to_char(MAX(tsd.start_time), 'YYYY-MM-DD') AS last_date
          FROM th_series_data tsd
          LEFT JOIN fhir_indicators fi ON tsd.fhir_id = fi.id
         WHERE tsd.user_id = :user_id AND tsd.deleted = 0 {time_clause}
         GROUP BY tsd.indicator
         ORDER BY tsd.indicator
         LIMIT :cap
        """
        rows = await execute_query(sql, params) or []
        return {
            "success": True,
            "message": (
                "No indicator matched those keywords. These are the indicators this user has — "
                "pick from them via `indicators`."
                if searched else
                "The indicators this user has. Call again with `indicators` (or `keywords`) to read values."
            ),
            "catalog": self._table(
                [dict(r) for r in rows],
                ["indicator", "system", "code", "count", "first_date", "last_date"],
            ),
            "count": len(rows),
        }

    async def _rows(
        self, user_id: str, names: list[str], start_time: str | None,
        end_time: str | None, limit: int,
    ) -> tuple[dict[str, Any], dict[str, int]]:
        """Individual readings, compacted, with a hard row ceiling.

        `limit` is clamped server-side: it used to be int-coerced with no
        ceiling, so any caller — including an anonymous third-party MCP client —
        could ask for a million rows per indicator.

        `file_key` is carried through when a reading came from an uploaded
        document: it is the handle that takes the model back to the ORIGINAL
        lab report rather than this extracted number, which is the whole point
        of the engine keeping the source file.
        """
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 50
        limit = max(1, min(limit, self._MAX_LIMIT))

        params: dict[str, Any] = {"user_id": user_id, "names": names, "limit": limit}
        time_clause = self._build_time_clause(params, start_time, end_time, "tsd.start_time")
        sql = f"""
        SELECT indicator, start_time, value, info, source_table_id FROM (
            SELECT tsd.indicator, tsd.start_time, tsd.value,
                   tsd.fhir_mapping_info AS info,
                   CASE WHEN tsd.source_table = 'th_files' THEN tsd.source_table_id END AS source_table_id,
                   ROW_NUMBER() OVER (PARTITION BY tsd.indicator ORDER BY tsd.start_time DESC) AS rn
              FROM th_series_data tsd
             WHERE tsd.user_id = :user_id AND tsd.deleted = 0
               AND tsd.indicator = ANY(:names) {time_clause}
        ) s
        WHERE rn <= :limit
        ORDER BY indicator, start_time DESC
        """
        rows = await execute_query(sql, params) or []

        grouped: dict[str, list[dict]] = {}
        for r in rows:
            rec = {
                "time": str(r["start_time"]) if r["start_time"] is not None else "",
                "value": str(r["value"]) if r["value"] is not None else "",
            }
            info = r["info"]
            if info and "unit" in info:
                rec["unit"] = info["unit"]
            # source_table_id is "<file_key>_#_<row>" for document-derived readings.
            stid = r["source_table_id"]
            if stid and "_#_" in stid:
                rec["file_key"] = stid.split("_#_")[0]
            grouped.setdefault(r["indicator"], []).append(rec)

        totals = await self._totals(user_id, names, start_time, end_time)
        payload, truncated = {}, {}
        for name, rs in grouped.items():
            payload[name] = {
                "count": len(rs),
                "rows": self._table(rs, ["time", "value", "unit", "file_key"]),
            }
            total = totals.get(name, 0)
            if total > len(rs):
                truncated[name] = total
        return payload, truncated

    async def _totals(
        self, user_id: str, names: list[str], start_time: str | None, end_time: str | None,
    ) -> dict[str, int]:
        params: dict[str, Any] = {"user_id": user_id, "names": names}
        time_clause = self._build_time_clause(params, start_time, end_time, "tsd.start_time")
        sql = f"""
        SELECT tsd.indicator, COUNT(*) AS total
          FROM th_series_data tsd
         WHERE tsd.user_id = :user_id AND tsd.deleted = 0
           AND tsd.indicator = ANY(:names) {time_clause}
         GROUP BY tsd.indicator
        """
        rows = await execute_query(sql, params) or []
        return {r["indicator"]: int(r["total"]) for r in rows}

    async def _aggregate(
        self, user_id: str, names: list[str], start_time: str | None,
        end_time: str | None, mode: str,
    ) -> tuple[dict[str, Any], dict[str, int]]:
        """Server-side stats / bucketing.

        Computed in SQL over the WHOLE window, not over a fetched page — a
        client asking "how did my LDL change this year" must not have to pull
        every reading and diff them itself.
        """
        params: dict[str, Any] = {"user_id": user_id, "names": names}
        time_clause = self._build_time_clause(params, start_time, end_time, "tsd.start_time")
        num = self._NUMERIC

        if mode == "stats":
            sql = f"""
            SELECT tsd.indicator,
                   COUNT(*) AS count,
                   COUNT({num}) AS numeric_count,
                   MIN({num}) AS min,
                   MAX({num}) AS max,
                   ROUND(AVG({num})::numeric, 4) AS avg,
                   (ARRAY_AGG(tsd.value ORDER BY tsd.start_time ASC))[1] AS first,
                   to_char(MIN(tsd.start_time), 'YYYY-MM-DD') AS first_date,
                   (ARRAY_AGG(tsd.value ORDER BY tsd.start_time DESC))[1] AS last,
                   to_char(MAX(tsd.start_time), 'YYYY-MM-DD') AS last_date
              FROM th_series_data tsd
             WHERE tsd.user_id = :user_id AND tsd.deleted = 0
               AND tsd.indicator = ANY(:names) {time_clause}
             GROUP BY tsd.indicator
            """
            rows = await execute_query(sql, params) or []
            payload = {}
            for r in rows:
                d = dict(r)
                name = d.pop("indicator")
                # `change` only when both ends are numeric — a text result has no delta.
                try:
                    d["change"] = round(float(d["last"]) - float(d["first"]), 4)
                except (TypeError, ValueError):
                    pass
                payload[name] = {"count": int(d.get("count") or 0), "stats": d}
            return payload, {}

        sql = f"""
        SELECT tsd.indicator,
               to_char(date_trunc('{mode}', tsd.start_time), 'YYYY-MM-DD') AS period,
               COUNT(*) AS n,
               ROUND(AVG({num})::numeric, 4) AS avg,
               MIN({num}) AS min,
               MAX({num}) AS max
          FROM th_series_data tsd
         WHERE tsd.user_id = :user_id AND tsd.deleted = 0
           AND tsd.indicator = ANY(:names) {time_clause}
         GROUP BY tsd.indicator, date_trunc('{mode}', tsd.start_time)
         ORDER BY tsd.indicator, period
        """
        rows = await execute_query(sql, params) or []
        grouped: dict[str, list[dict]] = {}
        for r in rows:
            d = dict(r)
            grouped.setdefault(d.pop("indicator"), []).append(d)
        payload = {
            name: {"count": sum(int(p["n"]) for p in pts),
                   "rows": self._table(pts, ["period", "avg", "min", "max", "n"])}
            for name, pts in grouped.items()
        }
        return payload, {}


#-----------------------------------------------------------------------------
