"""Structured records and standardization over HTTP, shaped like the platform.

**Why this exists next to `indicator_router.py`.** That router serves the
bundled web client: house envelope (`{code, msg, data}`), rows grouped by
indicator name, a `target_user_id` for care-circle reads. It is the browser's
API and it should stay that way.

This one is for a *developer* pointing their own code at a self-hosted
deployment. Someone who has read [docs.mirobody.ai](https://docs.mirobody.ai/)
knows `POST /data` with a `records[]` array, `GET /data` returning
`{"object": "list", "data": [...], "has_more": …}`, and
`POST /standardize` returning `{"object": "extraction", "data": [...]}`. Making
them learn a second, differently-shaped API to run the same engine on their own
machine is a tax with nothing on the other side of it, so the wire shapes here
are the hosted ones.

**What is deliberately NOT copied**, because it is operator machinery rather
than engine behaviour, and self-hosting has no operator but you:

* `retention` / `session_id` — the hosted plane expires rows on a schedule.
  Here a row lives until something deletes it. Sending `retention` is not an
  error; it is ignored, and saying so beats a 400 for a field the platform docs
  told the caller to send.
* the `user` Subject key — one deployment, real accounts, JWT. There are no
  Subjects to isolate; the caller's own token says who they are.
* `mb_live_*` keys, quota and rate metering — the hosted plane's billing edge.

The path prefix is `/api`, not `/v1`: this is not the hosted contract and
should not claim to be versioned alongside it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ...engine import resolve_reading as resolve_indicator_name
from mirobody.units.normalize import normalize_unit, parse_value_unit
from ...utils import execute_query
from ..auth import verify_token

router = APIRouter(prefix="/api", tags=["records"])

# Rows written through this router. `source` is the provenance column the read
# side echoes, and it is what tells a later reader that a value arrived over the
# API rather than out of a device pull or a parsed report.
_SOURCE_API = "api"
_SOURCE_STANDARDIZE = "standardize"
_SOURCE_TABLE = "api"

MAX_RECORDS_PER_REQUEST = 500


def _error(status: int, message: str, code: str, param: str | None = None) -> JSONResponse:
    """The hosted plane's error envelope, so one client can read both.

    `{"error": {message, type, code, param}}` — not the house `{code, msg}`,
    which every other router here answers with. The two envelopes coexist on
    purpose: the web client's endpoints keep theirs, this surface speaks the
    one a developer's OpenAI-shaped error handling already understands.
    """
    return JSONResponse(
        status_code=status,
        content={
            "error": {
                "message": message,
                "type": "invalid_request_error" if status < 500 else "server_error",
                "code": code,
                "param": param,
            }
        },
    )


# ── standardization ──────────────────────────────────────────────────────────

def _standardize(name: str, value: str | None, unit: str | None) -> dict[str, Any]:
    """One reading -> its canonical identity. Offline, no database, no model.

    This is stage ② in one function: name -> LOINC via the offline resolver,
    value+unit -> a parsed number and a UCUM unit. `None` for the code is the
    honest answer for a term that did not resolve — never a guessed one.

    `resolve_reading`, not `resolve`: the code depends on the unit. Total
    cholesterol is 2093-3 in mg/dL and 14647-2 in mmol/L, and a route that has
    the unit in its hand has no excuse for filing one under the other.
    """
    resolution = resolve_indicator_name(name or "", value, unit)
    parsed = parse_value_unit(f"{value or ''} {unit or ''}".strip())
    ucum = normalize_unit(unit) if unit else (parsed.unit or None)
    return {
        "loinc_code": resolution.loinc or None,
        "canonical_name": resolution.canonical or None,
        "parsed_value": None if parsed.value is None else str(parsed.value),
        "unit_ucum": ucum or None,
    }


class StandardizeRequest(BaseModel):
    """`text` is the only source this endpoint takes.

    The hosted endpoint also accepts a `file`/`file_key`, because it owns the
    upload plane. Here uploads already have a home — `POST /files/upload` and
    the Data tab — and duplicating that intake would mean a second copy of the
    parser wiring. A file's readings reach the same place by that route.
    """

    text: str = Field(min_length=1, description="Report text to standardize")
    store: bool = Field(False, description="Also write the readings as records")
    # Accepted and ignored; see the module docstring.
    retention: Optional[str] = None
    session_id: Optional[str] = None


@router.post("/standardize")
async def standardize(body: StandardizeRequest, user_id: str = Depends(verify_token)):
    """Report text in, standardized readings out. Dry-run unless `store=true`.

    Extraction is one LLM call (`engine.parse_text`); resolution and unit
    normalization are offline. A deployment with no LLM key configured gets a
    plain 400 saying so rather than a stack trace.
    """
    from ...engine import parse_text

    try:
        readings = await parse_text(body.text)
    except RuntimeError as e:
        # parse_text raises RuntimeError with a readable message for "no
        # provider key" and for non-JSON model output. Both are the caller's
        # problem to act on, and neither is a server fault.
        return _error(400, str(e), "extraction_failed", "text")
    except Exception as e:
        logging.error(f"[standardize] {e}", exc_info=True)
        return _error(500, "This extraction could not complete.", "internal_error")

    data: list[dict[str, Any]] = []
    for reading in readings:
        std = _standardize(reading.name, reading.value, reading.unit)
        data.append(
            {
                "indicator_raw": reading.name,
                "canonical_name": std["canonical_name"],
                "loinc_code": std["loinc_code"],
                "value_raw": reading.value,
                "parsed_value": std["parsed_value"],
                "unit_raw": reading.unit or None,
                "unit_ucum": std["unit_ucum"],
                "reference_range": reading.reference_range or None,
                "measured_at": None,
            }
        )

    stored_count = 0
    if body.store and data:
        stored_count, _ = await _insert_records(
            user_id,
            [
                {
                    "indicator": row["indicator_raw"],
                    "value": row["value_raw"],
                    "unit": row["unit_raw"],
                    "time": None,
                    "end_time": None,
                    "source": None,
                }
                for row in data
            ],
            source=_SOURCE_STANDARDIZE,
        )

    out: dict[str, Any] = {
        "object": "extraction",
        "data": data,
        "stored": bool(body.store),
        "stored_count": stored_count,
    }
    if not data:
        # An empty result is a documented boundary, not an error: narrative text
        # ("dizzy all afternoon") has no quantifiable reading in it.
        out["note"] = "No quantifiable readings were found in this text."
    return out


# ── records ──────────────────────────────────────────────────────────────────

class Record(BaseModel):
    indicator: str = Field(min_length=1, max_length=200)
    value: Any = Field(description="Reading value; stored as written")
    unit: Optional[str] = Field(None, max_length=64)
    # `measured_at` / `start_time` are accepted as aliases so a row handed back
    # from POST /standardize (which names the field `measured_at`) can be
    # forwarded here unchanged.
    time: Optional[str] = None
    measured_at: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = Field(None, description="Closes an episode that starts at `time`")
    source: Optional[str] = Field(None, max_length=128, description="Where the reading came from")

    def when(self) -> str | None:
        return self.time or self.measured_at or self.start_time


class WriteRequest(BaseModel):
    records: list[Record] = Field(min_length=1, max_length=MAX_RECORDS_PER_REQUEST)
    retention: Optional[str] = None      # accepted and ignored
    session_id: Optional[str] = None     # accepted and ignored


def _parse_time(raw: str | None) -> datetime:
    if not raw:
        return datetime.now(timezone.utc)
    text = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


async def _insert_records(user_id: str, records: list[dict[str, Any]], *, source: str) -> tuple[int, int]:
    """Write readings, standardizing each on the way in.

    Returns `(written, standardized)`. The caller used to compute the second
    number with its own pass of `_standardize` over the same records, which
    resolved every indicator name twice per request.

    `ON CONFLICT DO NOTHING` against the `(user_id, indicator, start_time,
    end_time)` unique key: re-sending a batch after a timeout is a retry, not a
    request for a duplicate row.
    """
    params = []
    coded = 0
    for record in records:
        when = _parse_time(record.get("time"))
        end = _parse_time(record["end_time"]) if record.get("end_time") else when
        value = record.get("value")
        unit = record.get("unit")
        # The unit rides with the value in `value` because that is the column's
        # existing convention — `th_series_data.value` already holds "3.9
        # mmol/L" for rows written by the file parser, and a reader that
        # splits it expects to find it there.
        text = f"{value} {unit}".strip() if unit else f"{value}"
        std = _standardize(record["indicator"], str(value), unit)
        if std["loinc_code"]:
            coded += 1
        params.append(
            {
                "user_id": str(user_id),
                "indicator": record["indicator"],
                "value": text,
                "start_time": when.replace(tzinfo=None),
                "end_time": end.replace(tzinfo=None),
                "source_table": _SOURCE_TABLE,
                "source_table_id": "",
                "comment": record.get("source") or "",
                "indicator_id": std["loinc_code"] or "",
                "source": source,
            }
        )

    await execute_query(
        query="""
        INSERT INTO th_series_data (
            user_id, indicator, value, start_time, end_time, source_table,
            source_table_id, comment, indicator_id, source
        ) VALUES (
            :user_id, :indicator, :value, :start_time, :end_time, :source_table,
            :source_table_id, encrypt_content(:comment), :indicator_id, :source
        )
        ON CONFLICT (user_id, indicator, start_time, end_time) DO NOTHING
        """,
        params=params,
    )
    return len(params), coded


@router.post("/data")
async def write_records(body: WriteRequest, user_id: str = Depends(verify_token)):
    """Write structured records. Every write is standardized, not just stored."""
    records = [
        {
            "indicator": r.indicator,
            "value": r.value,
            "unit": r.unit,
            "time": r.when(),
            "end_time": r.end_time,
            "source": r.source,
        }
        for r in body.records
    ]
    try:
        written, coded = await _insert_records(user_id, records, source=_SOURCE_API)
    except Exception as e:
        logging.error(f"[write_records] {e}", exc_info=True)
        return _error(500, "These records could not be written.", "internal_error")
    return {"status": "ok", "ingested": written, "standardized": coded}


@router.get("/data")
async def read_records(
    indicator: Optional[str] = Query(None, description="Filter by name (substring match)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user_id: str = Depends(verify_token),
):
    """The caller's records, newest first — one object per reading.

    Row-level, not grouped by indicator like the web client's endpoint: `id` is
    what `DELETE /api/data?id=` takes, and a developer paging a series wants the
    readings in order, not a name-keyed map to flatten first.
    """
    # `value` is stored in the clear and every other reader selects it that way;
    # `comment` is the encrypted one — all three writers wrap it in
    # `encrypt_content`, so reading it raw would hand back ciphertext.
    sql = """
    SELECT id, indicator, value, start_time, end_time,
           source, decrypt_content(comment) AS comment, indicator_id
      FROM th_series_data
     WHERE user_id = :uid AND deleted = 0
       AND (:indicator IS NULL OR indicator ILIKE :pattern)
     ORDER BY start_time DESC, id DESC
     LIMIT :limit OFFSET :offset
    """
    try:
        rows = await execute_query(
            sql,
            {
                "uid": str(user_id),
                "indicator": indicator,
                "pattern": f"%{indicator}%" if indicator else None,
                "limit": limit + 1,          # one extra row answers `has_more`
                "offset": offset,
            },
        ) or []
    except Exception as e:
        logging.error(f"[read_records] {e}", exc_info=True)
        return _error(500, "This lookup could not complete.", "internal_error")

    has_more = len(rows) > limit
    data = []
    for row in rows[:limit]:
        value = row.get("value")
        parsed = parse_value_unit(value)
        data.append(
            {
                "id": row.get("id"),
                "indicator": row.get("indicator"),
                "value": value,
                "parsed_value": None if parsed.value is None else str(parsed.value),
                "parsed_unit": parsed.unit or None,
                "loinc_code": row.get("indicator_id") or None,
                "time": row.get("start_time").isoformat() if row.get("start_time") else None,
                "end_time": row.get("end_time").isoformat() if row.get("end_time") else None,
                "source": row.get("source") or None,
                "comment": row.get("comment") or "",
            }
        )
    return {"object": "list", "data": data, "has_more": has_more}


@router.delete("/data")
async def erase_records(
    request: Request,
    id: Optional[int] = Query(None, description="One row, from GET /api/data"),
    indicator: Optional[str] = Query(None, description="One indicator (substring match)"),
    user_id: str = Depends(verify_token),
):
    """Erase the caller's records — one row, one indicator, or all of them.

    `all=true` is required for the widest scope. The hosted endpoint treats "no
    filter" as "everything", which is defensible behind an API key an operator
    minted on purpose; on a deployment where a mistyped curl is one keystroke
    from a person's whole record, an explicit flag is the better trade.
    """
    delete_all = str(request.query_params.get("all", "")).lower() in ("1", "true", "yes")
    if id is not None:
        where, params = "id = :id", {"uid": str(user_id), "id": id}
    elif indicator:
        where, params = "indicator ILIKE :pattern", {"uid": str(user_id), "pattern": f"%{indicator}%"}
    elif delete_all:
        where, params = "TRUE", {"uid": str(user_id)}
    else:
        return _error(
            400,
            "Pass `id`, `indicator`, or `all=true` to erase every record.",
            "missing_scope",
            "id",
        )

    # `user_id = :uid` IS the authorization: an id belonging to someone else
    # matches zero rows and reports 0 deleted, indistinguishable from an id that
    # was never there.
    try:
        rows = await execute_query(
            f"DELETE FROM th_series_data WHERE user_id = :uid AND {where} RETURNING id",
            params,
        ) or []
    except Exception as e:
        logging.error(f"[erase_records] {e}", exc_info=True)
        return _error(500, "This deletion could not complete.", "internal_error")
    return {"status": "ok", "deleted": len(rows)}
