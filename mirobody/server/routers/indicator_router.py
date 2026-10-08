"""Health-indicator lookup over REST.

The web client's Indicators tab needs the same answer the agent gets from the
`query_health_indicators` tool: what indicators does this person have, and what are
the readings. Both go through the ONE read authority (`query.HealthQuery`,
implemented by `PostgresHealthQuery`) so this router is a serialization
boundary and nothing more. It must never grow a second copy of the query; when
it did, the chat answer and the dashboard could disagree on screen about the
same day.

**Why a separate route at all, rather than pointing the browser at the tool.**
The tool answers a model, and its output is shaped for one: pipe-delimited
tables with constant columns hoisted into a `(constants: unit=mmol/L)` line,
because repeating the unit on 200 rows is token cost a third-party MCP client
pays for, plus a methodology line written at the model. A browser wants arrays
of objects it can sort and paginate, and parsing that table back into the dicts
it came from would be a strange thing to ask JavaScript to do. `render_rest`
and `render_compact` are the two serializations of one envelope; everything
upstream of them is shared.

Until this existed the Indicators tab showed "No indicators yet" while the tab
badge (fed by a different endpoint that counts rows directly) said 21. The
list call was 404ing and the client's fallback path was 404ing too; a
contradiction on screen was the only symptom.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from mirobody.collect import RECORD_EXPORT_COLUMNS, RECORDS_PAGE_MAX, REST_CATALOG_MAX, REST_ROW_MAX, PostgresHealthQuery
from mirobody.agent.tools._render import render_rest
from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService
from mirobody.server.auth import subject_for, verify_token
from mirobody.kernel.ops import is_driver_exception
from mirobody.server.envelope import ErrorResponse, StandardResponse, failed

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["indicators"])

# The browser reads through the same authority the model does, with a larger
# catalogue cap: a table the user scrolls is not a model's context window, and
# capping it at one hid 44 of the demo user's 244 indicators while reporting
# `count: 200` as though that were the total. Readings only: what the person
# reported has its own tab (`/api/v1/journal`). No outside-window note:
# `render_rest` shows no notes, and the note's whole-record catalogue took
# 1.8 s over a million readings (synthetic, 2026-10-08) on every dated call.
_service = HealthIndicatorsService(PostgresHealthQuery(reported=False), catalog_cap=REST_CATALOG_MAX,
                                   row_cap=REST_ROW_MAX, outside_note=False)
# Every visible entry, reported ones included: the records table and the delta
# count cover what a person logged as well as what was measured.
_records = PostgresHealthQuery(reported=True)

#: The most rows one export carries. Past it the export says it was cut short
#: rather than hold an unbounded account in memory.
EXPORT_MAX = 100_000



def _split(value: str | None) -> list[str] | None:
    """`?keywords=a,b` or repeated `?keywords=a&keywords=b`, both flattened."""
    if not value:
        return None
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return parts or None


def _date(value: str | None, name: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD") from exc


def _instant(value: str, name: str = "since") -> datetime:
    """An ISO 8601 instant that says its offset. A naive one is refused: read
    as UTC, "since 09:00" from a browser in UTC+8 would count eight hours of
    entries the person has already seen."""
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone offset")
    return parsed


def _denied() -> ErrorResponse:
    return ErrorResponse(code=403, msg="Not permitted to read this member's health data.")


def _record_filters(kind: str, modality: str | None, start_time: str | None, end_time: str | None,
                    created_since: str | None, keywords: str | None) -> dict:
    """The records filters, parsed. Raises ValueError with a message naming
    the parameter at fault."""
    if kind not in {"measurement", "all"}:
        raise ValueError("kind must be measurement or all.")
    return {
        "kind": kind,
        "modalities": _split(modality),
        "start_time": _date(start_time, "start_time"),
        "end_time": _date(end_time, "end_time"),
        "created_since": _instant(created_since, "created_since") if created_since else None,
        "keywords": keywords,
    }


@router.get("/health-indicators")
async def health_indicators(
    keywords: str | None = Query(None, description="Fuzzy terms; omit for the catalog"),
    indicators: str | None = Query(None, description="Exact names from a previous call"),
    start_time: str | None = Query(None, description='Inclusive "YYYY-MM-DD"'),
    end_time: str | None = Query(None, description='Inclusive "YYYY-MM-DD"'),
    view: str = Query("raw", description="raw | minute | hour | day | week | month | stats | latest"),
    target_user_id: str | None = Query(None, description="Care-circle member to read"),
    user_id: str = Depends(verify_token),
):
    """Catalog when neither `keywords` nor `indicators` is given; readings otherwise.

    Answers in the house envelope (`{code, msg, data}`) because the web client's
    response interceptor returns `response.data.data` on success: a bare
    payload arrives at the component as `undefined`, which is exactly how this
    endpoint first shipped: 200, correct rows on the wire, and an empty list on
    screen.
    """
    # Reading someone else's record goes through the care-circle check, not a
    # trusted query parameter: the same rule the agent's tools follow.
    owner_id = await subject_for(user_id, target_user_id)
    if owner_id is None:
        return _denied()

    args = {
        "keywords": _split(keywords),
        "indicators": _split(indicators),
        "start": start_time,
        "end": end_time,
        "view": view,
    }
    envelope = await _service.envelope({"user_id": owner_id}, **{k: v for k, v in args.items() if v})

    if envelope.status == "error":
        code = 400 if envelope.error_class == "recoverable" else 500
        return ErrorResponse(code=code, msg="; ".join(envelope.assumptions) or "This lookup could not complete.")

    return StandardResponse(data=render_rest(envelope))


@router.get("/health-indicators/records")
async def health_indicator_records(
    target_user_id: str | None = Query(None),
    kind: str = Query("measurement", description='"measurement", or "all" to include logged entries'),
    modality: str | None = Query(None, description="Comma-separated observation modalities"),
    start_time: str | None = Query(None, description="First day observed, YYYY-MM-DD"),
    end_time: str | None = Query(None, description="Last day observed, YYYY-MM-DD"),
    created_since: str | None = Query(None, description="ISO 8601 with offset; entries new after it"),
    keywords: str | None = Query(None, description="Substring of the printed or standard name"),
    limit: int = Query(50, ge=1, le=RECORDS_PAGE_MAX),
    offset: int = Query(0, ge=0),
    user_id: str = Depends(verify_token),
):
    """One page of visible entries across every indicator, newest observed first."""
    owner = await subject_for(user_id, target_user_id)
    if owner is None:
        return _denied()
    try:
        filters = _record_filters(kind, modality, start_time, end_time, created_since, keywords)
        data = await _records.records(owner, **filters, limit=limit, offset=offset)
    except ValueError as exc:
        return ErrorResponse(code=400, msg=str(exc))
    except Exception as exc:
        return failed("health records", exc, "This query could not complete.")
    return StandardResponse(data=data)


@router.get("/health-indicators/export")
async def export_health_indicators(
    target_user_id: str | None = Query(None),
    kind: str = Query("measurement"),
    modality: str | None = Query(None),
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    created_since: str | None = Query(None),
    keywords: str | None = Query(None),
    format: str = Query("csv", description='"csv" (a download) or "json"'),
    user_id: str = Depends(verify_token),
):
    """Every visible entry the same filters select, standardized value and unit
    included, from the same query as the table in one statement. Paging through
    it instead re-walked each chain once per page.

    The caller's own record only, as `/api/user/data-export`: a care-circle
    read grant shows a member's rows a page at a time, and does not hand over
    a copy of the whole record. `target_user_id` naming anyone else is 403."""
    if target_user_id and target_user_id != user_id:
        return ErrorResponse(code=403, msg="Only the record owner can export it.")
    owner = user_id
    if format not in {"csv", "json"}:
        return ErrorResponse(code=400, msg="format must be csv or json.")
    try:
        filters = _record_filters(kind, modality, start_time, end_time, created_since, keywords)
        page = await _records.records(owner, **filters, limit=EXPORT_MAX + 1)
    except ValueError as exc:
        return ErrorResponse(code=400, msg=str(exc))
    except Exception as exc:
        return failed("health export", exc, "This export could not complete.")
    rows = [{k: r.get(k) for k in RECORD_EXPORT_COLUMNS} for r in page["rows"][:EXPORT_MAX]]
    truncated = len(page["rows"]) > EXPORT_MAX
    if format == "json":
        return StandardResponse(data={
            "columns": list(RECORD_EXPORT_COLUMNS), "rows": rows, "total": page["total"], "truncated": truncated,
        })
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=RECORD_EXPORT_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    headers = {"Content-Disposition": 'attachment; filename="mirobody-indicators.csv"'}
    if truncated:
        headers["X-Export-Truncated"] = "true"
    return Response(content=output.getvalue(), media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/data/data-delta")
async def data_delta(
    since: str = Query(..., description="ISO 8601 with offset: the browser's last visit"),
    target_user_id: str | None = Query(None),
    kind: str = Query("all"),
    user_id: str = Depends(verify_token),
):
    """How many visible entries are new since `since`, by source. The cursor is
    the browser's: the server keeps no "last visit" for anyone."""
    owner = await subject_for(user_id, target_user_id)
    if owner is None:
        return _denied()
    if kind not in {"measurement", "all"}:
        return ErrorResponse(code=400, msg="kind must be measurement or all.")
    try:
        data = await _records.delta(owner, _instant(since), target_kind=kind)
    except ValueError as exc:
        return ErrorResponse(code=400, msg=str(exc))
    except Exception as exc:
        return failed("health delta", exc, "This query could not complete.")
    return StandardResponse(data=data)


class ReadingPatch(BaseModel):
    """One user-owned reading, corrected or removed from the web UI.

    Extraction is an LLM reading a lab report: it mis-reads a value now and
    then, and until this endpoint existed the only fix was deleting and
    re-uploading the whole file. Owner-only on purpose: care-circle "health"
    permission grants reading, not rewriting someone else's record.
    """

    id: int = Field(gt=0, description="observation row id, from the readings payload")
    value: str | None = Field(None, max_length=200, description="Corrected value")
    delete: bool = Field(False, description="Soft-delete this reading instead")


@router.post("/health-indicators/reading")
async def patch_reading(patch: ReadingPatch, user_id: str = Depends(verify_token)):
    """Correct or soft-delete a single reading the caller owns.

    The `user_id = :uid` predicate IS the authorization: a row id belonging to
    someone else matches zero rows and reports not-found, indistinguishable
    from a genuinely absent row.
    """
    if not patch.delete and (patch.value is None or not patch.value.strip()):
        return ErrorResponse(code=400, msg="Provide a value, or set delete.")

    # Neither path touches the stored row: a removal is a retraction row
    # pointing at it, a correction is an amended row pointing at it, and the
    # read view hides the original in both cases.
    from mirobody.collect import observations

    try:
        if patch.delete:
            done = await observations.retract(str(user_id), [patch.id]) > 0
        else:
            tz = await observations.user_tz(str(user_id))
            done = await observations.amend(str(user_id), patch.id, value_text=patch.value.strip(), user_tz=tz) is not None
    except Exception as e:
        logger.error("reading correction failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return ErrorResponse(code=500, msg="This update could not complete.")

    if not done:
        return ErrorResponse(code=404, msg="No such reading.")

    return StandardResponse(data={"id": patch.id, "deleted": patch.delete})


class FileDatePatch(BaseModel):
    """Re-file every reading extracted from one uploaded file under a date.

    A report photographed as several screenshots shows its date on the first
    page only, so the other pages' readings were filed under the upload day
    and the report's timeline split in two (#53). Extraction runs one file at
    a time and cannot tell "page 2 of the same report" from "a second report
    whose date did not come out", so it must not inherit a date on its own,
    it labels the guess (`date_source: upload_time`, see
    `report_date.resolve_report_date`) and the Data page asks.
    The three answers (a sibling file's extracted date, a typed date, or
    "keep the upload time") all land here.
    """

    file_key: str = Field(min_length=1, max_length=255)
    report_date: str | None = Field(
        None,
        description='"YYYY-MM-DD" (a time may follow). Omit to keep the upload time and stop asking.',
    )


@router.post("/health-indicators/file-date")
async def patch_file_date(patch: FileDatePatch, user_id: str = Depends(verify_token)):
    """Move a file's readings to `report_date`, or confirm the upload time.

    The readings belong to the record the file was uploaded INTO
    (`query_user_id` on a proxy upload), which is the `user_id` every
    observation from that file carries; rewriting someone else's record
    needs the care-circle write grant, the same rule the upload itself enforces.
    What "set the date" means (including a reading whose indicator already
    has a row on the target date staying put and being counted as `skipped`) 
    is `services.report_date.set_file_report_date`, shared with the agent tool.
    """
    from mirobody.utils.coerce import parse_date
    from mirobody.collect import FileDbService
    from mirobody.collect import set_file_report_date

    row = await FileDbService.get_file_by_key(patch.file_key)
    if not row:
        return ErrorResponse(code=404, msg="No such file.")
    owner = await subject_for(user_id, str(row.get("query_user_id") or row.get("user_id")), write=True)
    if owner is None:
        # Same answer as an absent key: file keys are second-resolution
        # timestamps plus 8 hex, enumerable enough that "forbidden" would
        # confirm one exists.
        return ErrorResponse(code=404, msg="No such file.")

    when = None
    if patch.report_date is not None:
        when = parse_date(patch.report_date)
        if when is None:
            return ErrorResponse(code=400, msg="report_date must be YYYY-MM-DD.")

    try:
        data = await set_file_report_date(owner, patch.file_key, when)
    except Exception as e:
        logger.error("file date change failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return ErrorResponse(code=500, msg="This update could not complete.")
    return StandardResponse(data=data)
