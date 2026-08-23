"""Health-indicator lookup over REST.

The web client's Indicators tab needs the same answer the agent gets from the
`query_health_indicators` MCP tool: what indicators does this user have, and
what are the readings. That logic — the SQL, the terminology join, the
keyword resolution, the server-side `limit` ceiling — already exists in
`HealthIndicatorService`, so this router is a serialization boundary and
nothing more. It must never grow a second copy of the query.

**Why a separate route at all, rather than pointing the browser at the MCP
tool.** The tool answers a model, and its output is shaped for one: readings
come back as a pipe-delimited table with constant columns hoisted into a
`(constants: unit=mmol/L)` line, because repeating the unit on 200 rows is
token cost a third-party MCP client pays for. It also carries prose written at
the model ("pick from them via `indicators`"). A browser wants arrays of
objects it can sort and paginate, and parsing that table back into the dicts it
came from would be a strange thing to ask JavaScript to do. `service.query(...,
compact=False)` stops before the compaction step; everything upstream is shared.

Until this existed the Indicators tab showed "No indicators yet" while the tab
badge — fed by a different endpoint that counts rows directly — said 21. The
list call was 404ing and the client's fallback path was 404ing too; a
contradiction on screen was the only symptom.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from ...agent.tools.health_indicator_service import HealthIndicatorService
from ...utils import execute_query
from ...user.care_circle import CareCircleDenied, resolve_subject
from ..auth import verify_token
from .public_router import ErrorResponse, StandardResponse

router = APIRouter(prefix="/api/v1", tags=["indicators"])

_service = HealthIndicatorService()


def _split(value: Optional[str]) -> list[str] | None:
    """`?keywords=a,b` or repeated `?keywords=a&keywords=b`, both flattened."""
    if not value:
        return None
    parts = [p.strip() for p in value.split(",") if p.strip()]
    return parts or None


@router.get("/health-indicators")
async def health_indicators(
    keywords: Optional[str] = Query(None, description="Fuzzy terms; omit for the catalog"),
    indicators: Optional[str] = Query(None, description="Exact names from a previous call"),
    start_time: Optional[str] = Query(None, description='Inclusive "YYYY-MM-DD"'),
    end_time: Optional[str] = Query(None, description='Inclusive "YYYY-MM-DD"'),
    aggregate: str = Query("none", description="none | stats | day | week | month"),
    limit: int = Query(50, ge=1, le=500),
    target_user_id: Optional[str] = Query(None, description="Care-circle member to read"),
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

    try:
        result = await _service._query(
            owner_id,
            keywords=_split(keywords),
            indicators=_split(indicators),
            start_time=start_time,
            end_time=end_time,
            aggregate=aggregate,
            limit=limit,
            compact=False,
        )
    except Exception as e:
        logging.error(f"[health_indicators] {e}", exc_info=True)
        return ErrorResponse(code=500, msg="This lookup could not complete.")

    if not result.get("success"):
        return ErrorResponse(code=400, msg=result.get("error") or "This lookup could not complete.")

    return StandardResponse(data={k: v for k, v in result.items() if k != "success"})


class ReadingPatch(BaseModel):
    """One user-owned reading, corrected or removed from the web UI.

    Extraction is an LLM reading a lab report: it mis-reads a value now and
    then, and until this endpoint existed the only fix was deleting and
    re-uploading the whole file. Owner-only on purpose — care-circle "health"
    permission grants reading, not rewriting someone else's record.
    """

    id: int = Field(gt=0, description="th_series_data row id, from the readings payload")
    value: Optional[str] = Field(None, max_length=200, description="Corrected value")
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
        logging.error(f"[patch_reading] {e}", exc_info=True)
        return ErrorResponse(code=500, msg="This update could not complete.")

    if not rows:
        return ErrorResponse(code=404, msg="No such reading.")

    return StandardResponse(data={"id": patch.id, "deleted": patch.delete})
