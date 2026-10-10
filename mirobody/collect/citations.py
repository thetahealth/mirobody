"""The short ids (`r1`, `r2`, ...) a conversation's tool rows carry, kept per
conversation in `th_chat_citation`, keyed like the agent's checkpoint: the
session's owner and id (`agent.checkpointer.thread_for`).

A model cites a row by its rid (`kernel.citations`). The number is minted the
first time a session is shown that row and stored, so it names the same row
after a restart, on any worker, and when the conversation is opened again. A
reading's identity is its observation id; an aggregate's (a stats line, a day
or month bucket) is its definition: series, window, view and period.

Only identities are stored, never a value: `resolve_rids` reads the rows live, so a
reading the person has erased resolves to "gone", not to its old value.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from collections.abc import Mapping, Sequence
from typing import Any

from mirobody.utils import execute_query

from .query import _FILE_JOIN, _FILE_KEY, _FILE_NAME, _LOCAL_TS

#: How many of an aggregate's readings `resolve_rids` lists, newest first.
MEMBERS_SHOWN = 50

_MINT = """
WITH input AS (
    SELECT DISTINCT ON (row_key) row_key, detail, ord
      FROM unnest(CAST(:keys AS text[]), CAST(:details AS text[])) WITH ORDINALITY AS t(row_key, detail, ord)
     ORDER BY row_key, ord
),
fresh AS (
    SELECT i.row_key, i.detail, i.ord FROM input i
     WHERE NOT EXISTS (SELECT 1 FROM th_chat_citation c WHERE c.session_id = :sid AND c.row_key = i.row_key)
),
seq AS (
    INSERT INTO th_chat_citation_seq AS s (session_id, n) VALUES (:sid, (SELECT count(*) FROM fresh))
    ON CONFLICT (session_id) DO UPDATE SET n = s.n + EXCLUDED.n
    RETURNING n
),
numbered AS (
    SELECT f.row_key, f.detail,
           'r' || (seq.n - (SELECT count(*) FROM fresh) + ROW_NUMBER() OVER (ORDER BY f.ord)) AS rid
      FROM fresh f, seq
),
inserted AS (
    INSERT INTO th_chat_citation (session_id, rid, row_key, subject_id, detail)
    SELECT :sid, rid, row_key, :subject, CAST(NULLIF(detail, '') AS jsonb) FROM numbered
    ON CONFLICT (session_id, row_key) DO NOTHING
    RETURNING row_key, rid
)
SELECT row_key, rid FROM inserted
UNION ALL
SELECT row_key, rid FROM th_chat_citation WHERE session_id = :sid AND row_key = ANY(CAST(:keys AS text[]))
"""

_KNOWN = "SELECT row_key, rid FROM th_chat_citation WHERE session_id = :sid AND row_key = ANY(CAST(:keys AS text[]))"


def reading_key(observation_id: Any) -> str:
    return f"o:{observation_id}"


def aggregate_key(definition: Mapping[str, Any]) -> str:
    text = json.dumps(dict(definition), sort_keys=True, ensure_ascii=False)
    return "a:" + hashlib.sha1(text.encode()).hexdigest()[:16]


async def mint_rids(session_id: str, subject_id: str, rows: Sequence[tuple[str, Mapping[str, Any] | None]]) -> dict[str, str]:
    """The rid of each `(row_key, aggregate definition or None)`, minting the
    ones this session has not shown yet. Two calls minting at once share no
    number; when they race on the same row, the loser reads the winner's rid."""
    if not rows:
        return {}
    keys = [key for key, _ in rows]
    details = [json.dumps(dict(detail), sort_keys=True, ensure_ascii=False) if detail else "" for _, detail in rows]
    params = {"sid": session_id, "subject": str(subject_id), "keys": keys, "details": details}
    found = {r["row_key"]: r["rid"] for r in (await execute_query(_MINT, params, log_sql=False) or [])}
    missing = [key for key in keys if key not in found]
    if missing:
        rows_back = await execute_query(_KNOWN, {"sid": session_id, "keys": missing}, log_sql=False) or []
        found.update({r["row_key"]: r["rid"] for r in rows_back})
    return found


async def forget_rids(session_id: str) -> None:
    """Drop a conversation's citations, with the conversation."""
    for table in ("th_chat_citation", "th_chat_citation_seq"):
        await execute_query(f"DELETE FROM {table} WHERE session_id = :sid", {"sid": session_id}, log_sql=False)


async def resolve_rids(session_id: str, subject_id: str, rids: Sequence[str]) -> list[dict[str, Any]]:
    """What each rid stands for now, for a caller already authorized to read
    `subject_id`'s record. A rid minted on another record, or never minted,
    resolves to `unknown`; a reading since erased to `gone`."""
    wanted = [rid for rid in dict.fromkeys(rids) if isinstance(rid, str)]
    if not wanted:
        return []
    stored = await execute_query(
        "SELECT rid, row_key, subject_id, detail FROM th_chat_citation WHERE session_id = :sid AND rid = ANY(:rids)",
        {"sid": session_id, "rids": wanted}, log_sql=False) or []
    by_rid = {r["rid"]: r for r in stored if str(r["subject_id"]) == str(subject_id)}
    readings = await _readings(subject_id, [r["row_key"][2:] for r in by_rid.values() if r["row_key"].startswith("o:")])
    out = []
    for rid in wanted:
        row = by_rid.get(rid)
        if row is None:
            out.append({"rid": rid, "status": "unknown"})
        elif row["row_key"].startswith("o:"):
            reading = readings.get(row["row_key"][2:])
            out.append({"rid": rid, "status": "ok", "kind": "reading", **reading} if reading
                       else {"rid": rid, "status": "gone"})
        else:
            detail = row["detail"] if isinstance(row["detail"], dict) else json.loads(row["detail"] or "{}")
            out.append({"rid": rid, "status": "ok", "kind": "aggregate", **detail,
                        **await _members(subject_id, detail)})
    return out


_READING_COLUMNS = f"""o.id, o.display AS indicator, o.name_text AS name, o.value_text AS value,
       o.unit_text AS unit, o.ref_text AS ref, o.flag_text AS flag, o.kind AS record_kind, o.modality, o.source_kind,
       to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI:SS') AS time, {_FILE_KEY} AS file_key, {_FILE_NAME}"""


async def _readings(subject_id: str, ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    numeric = [int(i) for i in ids if str(i).isdigit()]
    if not numeric:
        return {}
    rows = await execute_query(
        f"SELECT {_READING_COLUMNS} FROM v_observation o {_FILE_JOIN} WHERE o.user_id = :uid AND o.id = ANY(:ids)",
        {"uid": str(subject_id), "ids": numeric}, log_sql=False) or []
    return {str(r["id"]): _public(r) for r in rows}


async def _members(subject_id: str, detail: Mapping[str, Any]) -> dict[str, Any]:
    """The readings an aggregate was computed over: how many, the first and
    last day, and the newest `MEMBERS_SHOWN` of them."""
    series = detail.get("series")
    if not series:
        return {"total": 0, "readings": []}
    params: dict[str, Any] = {"uid": str(subject_id), "series": series, "limit": MEMBERS_SHOWN}
    where = ""
    if detail.get("from_time") and detail.get("to_time"):
        params.update(lo=detail["from_time"], hi=detail["to_time"])
        where = f" AND to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI') >= :lo AND to_char({_LOCAL_TS}, 'YYYY-MM-DD HH24:MI') < :hi"
    else:
        if detail.get("from"):
            params["lo"], where = detail["from"], where + " AND o.local_date >= CAST(:lo AS date)"
        if detail.get("to"):
            params["hi"], where = detail["to"], where + " AND o.local_date <= CAST(:hi AS date)"
    rows = await execute_query(
        f"""SELECT {_READING_COLUMNS}, COUNT(*) OVER () AS total,
                   to_char(MIN(o.local_date) OVER (), 'YYYY-MM-DD') AS first_day,
                   to_char(MAX(o.local_date) OVER (), 'YYYY-MM-DD') AS last_day
              FROM v_observation o {_FILE_JOIN}
             WHERE o.user_id = :uid AND o.series_id = :series {where}
             ORDER BY o.observed_start DESC, o.id DESC LIMIT :limit""",
        params, log_sql=False) or []
    if not rows:
        return {"total": 0, "readings": []}
    head = rows[0]
    readings = [_public({k: v for k, v in r.items() if k not in ("total", "first_day", "last_day")}) for r in rows]
    return {"total": int(head["total"]), "first_day": head["first_day"], "last_day": head["last_day"],
            "readings": readings}


#: The length of a sub-day bucket, by view.
_SUBDAY = {"minute": timedelta(minutes=1), "hour": timedelta(hours=1)}


def aggregate_definition(method: str, view: str, row: Mapping[str, Any], start: str = "", end: str = "") -> dict[str, Any]:
    """What an aggregate row was computed over: its series and the local days
    (or, below a day, the local minutes) it covers. `start`/`end` are the
    query window's local dates, inclusive; a bucket narrows them to its period."""
    detail: dict[str, Any] = {"view": view if method == "buckets" else "stats", "series": row.get("series") or "",
                              "indicator": row.get("indicator") or ""}
    period = str(row.get("period") or "")
    if method != "buckets" or not period:
        return {**detail, "from": start, "to": end}
    detail["period"] = period
    if view in _SUBDAY:
        first = datetime.strptime(period, "%Y-%m-%d %H:%M")
        return {**detail, "from_time": period, "to_time": (first + _SUBDAY[view]).strftime("%Y-%m-%d %H:%M")}
    first = date.fromisoformat(period[:10])
    if view == "week":
        last = first + timedelta(days=6)
    elif view == "month":
        last = (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    else:
        last = first
    return {**detail, "from": first.isoformat(), "to": last.isoformat()}


def _public(row: Mapping[str, Any]) -> dict[str, Any]:
    out = {k: ("" if v is None else v) for k, v in row.items() if k != "id"}
    out["file"] = out.pop("file_name", "") or ""
    return out


__all__ = ["MEMBERS_SHOWN", "aggregate_definition", "aggregate_key", "forget_rids", "mint_rids", "reading_key", "resolve_rids"]
