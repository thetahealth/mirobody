"""Health-indicator lookup over REST.

The web client's Indicators tab needs the same answer the agent gets from the
`query_health_indicators` tool: what indicators does this person have, and what are
the readings. Both go through the ONE read authority — `query.HealthQuery`,
implemented by `PostgresHealthQuery` — so this router is a serialization
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
badge — fed by a different endpoint that counts rows directly — said 21. The
list call was 404ing and the client's fallback path was 404ing too; a
contradiction on screen was the only symptom.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from ...pulse.query import REST_CATALOG_MAX, PostgresHealthQuery
from ...agent.tools.health_indicators_service import HealthIndicatorsService, render_rest
from ...utils import execute_query
from ...user.care_circle import CareCircleDenied, resolve_subject
from ..auth import verify_token
from ..envelope import ErrorResponse, StandardResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["indicators"])

# The browser reads through the same authority the model does, with a larger
# catalogue cap: a table the user scrolls is not a model's context window, and
# capping it at one hid 44 of the demo user's 244 indicators while reporting
# `count: 200` as though that were the total.
_service = HealthIndicatorsService(PostgresHealthQuery(), catalog_cap=REST_CATALOG_MAX)


def _split(value: str | None) -> list[str] | None:
    """`?keywords=a,b` or repeated `?keywords=a&keywords=b`, both flattened."""
    if not value:
        return None
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return parts or None


@router.get("/health-indicators")
async def health_indicators(
    keywords: str | None = Query(None, description="Fuzzy terms; omit for the catalog"),
    indicators: str | None = Query(None, description="Exact names from a previous call"),
    start_time: str | None = Query(None, description='Inclusive "YYYY-MM-DD"'),
    end_time: str | None = Query(None, description='Inclusive "YYYY-MM-DD"'),
    resolution: str = Query("raw", description="raw | minute | hour | day | week | month"),
    aggregate: str = Query("none", description="none | stats | latest"),
    limit: int = Query(50, ge=1, le=500),
    target_user_id: str | None = Query(None, description="Care-circle member to read"),
    user_id: str = Depends(verify_token),
):
    """Catalog when neither `keywords` nor `indicators` is given; readings otherwise.

    Answers in the house envelope (`{code, msg, data}`) because the web client's
    response interceptor returns `response.data.data` on success — a bare
    payload arrives at the component as `undefined`, which is exactly how this
    endpoint first shipped: 200, correct rows on the wire, and an empty list on
    screen.
    """
    owner_id = user_id

    # Reading someone else's record goes through the care-circle check, not a
    # trusted query parameter — the same rule the agent's tools follow.
    if target_user_id and target_user_id != user_id:
        try:
            await resolve_subject(user_id, target_user_id)
        except CareCircleDenied:
            return ErrorResponse(code=403, msg="Not permitted to read this member's health data.")
        owner_id = target_user_id

    args = {
        "keywords": _split(keywords),
        "indicators": _split(indicators),
        "start": start_time,
        "end": end_time,
        "resolution": resolution,
        "aggregate": aggregate,
    }
    # `limit` only applies to raw rows without aggregation — the same rule the
    # model is held to, so the two surfaces cannot answer differently for the
    # same arguments.
    if (resolution, aggregate) == ("raw", "none"):
        args["limit"] = limit
    envelope = await _service.envelope({"user_id": owner_id}, **{k: v for k, v in args.items() if v})

    if envelope.status == "error":
        code = 400 if envelope.error_class == "recoverable" else 500
        return ErrorResponse(code=code, msg="; ".join(envelope.assumptions) or "This lookup could not complete.")

    return StandardResponse(data=render_rest(envelope))


class ReadingPatch(BaseModel):
    """One user-owned reading, corrected or removed from the web UI.

    Extraction is an LLM reading a lab report: it mis-reads a value now and
    then, and until this endpoint existed the only fix was deleting and
    re-uploading the whole file. Owner-only on purpose — care-circle "health"
    permission grants reading, not rewriting someone else's record.
    """

    id: int = Field(gt=0, description="th_series_data row id, from the readings payload")
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

    if patch.delete:
        sql = """
        UPDATE th_series_data SET deleted = 1, update_time = CURRENT_TIMESTAMP
         WHERE id = :id AND user_id = :uid AND deleted = 0
        RETURNING id
        """
        params = {"id": patch.id, "uid": str(user_id)}
    else:
        sql = """
        UPDATE th_series_data SET value = :value, update_time = CURRENT_TIMESTAMP
         WHERE id = :id AND user_id = :uid AND deleted = 0
        RETURNING id
        """
        params = {"id": patch.id, "uid": str(user_id), "value": patch.value.strip()}

    try:
        rows = await execute_query(sql, params)
    except Exception as e:
        logger.error(f"[patch_reading] {e}", exc_info=True)
        return ErrorResponse(code=500, msg="This update could not complete.")

    if not rows:
        return ErrorResponse(code=404, msg="No such reading.")

    return StandardResponse(data={"id": patch.id, "deleted": patch.delete})


class FileDatePatch(BaseModel):
    """Re-file every reading extracted from one uploaded file under a date.

    A report photographed as several screenshots shows its date on the first
    page only, so the other pages' readings were filed under the upload day
    and the report's timeline split in two (#53). Extraction runs one file at
    a time and cannot tell "page 2 of the same report" from "a second report
    whose date did not come out", so it must not inherit a date on its own —
    it labels the guess (`date_source: upload_time`, see
    `FileParserDatabaseService.resolve_report_date`) and the Data page asks.
    The three answers — a sibling file's extracted date, a typed date, or
    "keep the upload time" — all land here.
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
    th_series_data row from that file carries; rewriting someone else's record
    needs the care-circle write grant, the same rule the upload itself enforces.
    What "set the date" means — including a reading whose indicator already
    has a row on the target date staying put and being counted as `skipped` —
    is `services.report_date.set_file_report_date`, shared with the agent tool.
    """
    from ...pulse.file_parser.services.db_utils import parse_date
    from ...pulse.file_parser.services.file_db_service import FileDbService
    from ...pulse.file_parser.services.report_date import set_file_report_date

    row = await FileDbService.get_file_by_key(patch.file_key)
    if not row:
        return ErrorResponse(code=404, msg="No such file.")
    owner = str(row.get("query_user_id") or row.get("user_id"))
    if owner != str(user_id):
        try:
            await resolve_subject(user_id, owner, require_write=True)
        except CareCircleDenied:
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
        logger.error(f"[patch_file_date] {e}", exc_info=True)
        return ErrorResponse(code=500, msg="This update could not complete.")
    return StandardResponse(data=data)
