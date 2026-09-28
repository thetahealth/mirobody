"""Self-describing personal data export, shaped after the Mirovital contract.

Mirobody's exportable health dataset is the visible standardized observation
set. The endpoint is deliberately caller-only: a care-circle read grant does
not grant a bulk copy of another person's record. JSON carries a manifest and
data object; NDJSON adds a header and a completion footer so a partial stream
cannot be mistaken for a finished export.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, UTC

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse

from mirobody.collect import PostgresHealthQuery
from mirobody.kernel.ops import is_driver_exception
from mirobody.server.auth import verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["data-export"])
NDJSON = "application/x-ndjson"
PAGE_SIZE = 200
MAX_PAGE_SIZE = 20_000

_records = PostgresHealthQuery(reported=True)
_COLUMNS = (
    "row_id", "kind", "indicator", "name", "code", "system", "series", "time",
    "date", "value", "unit", "modality", "source_kind", "file", "file_key",
    "created_at", "provenance", "text",
)


def _line(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def _manifest(total: int, *, offset: int = 0, truncated: bool = False, next_offset: int | None = None) -> dict:
    entry = {"dataset": "health_records", "columns": list(_COLUMNS), "rows": total}
    if offset:
        entry["offset"] = offset
    if truncated:
        entry.update({"truncated": True, "next_offset": next_offset})
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "manifest": [entry],
    }


async def _stream(subject_id: str) -> AsyncIterator[bytes]:
    total = 0
    offset = 0
    yield _line({
        "type": "header", "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "datasets": [{"dataset": "health_records", "columns": list(_COLUMNS)}],
    })
    try:
        while True:
            page = await _records.records(subject_id, kind="all", limit=PAGE_SIZE, offset=offset)
            rows = page["rows"]
            for row in rows:
                total += 1
                yield _line({"type": "row", "dataset": "health_records", "data": row})
            if not page["has_more"]:
                break
            offset += len(rows)
        yield _line({"type": "footer", "complete": True, "rows": {"health_records": total}, "errors": {}})
    except Exception as exc:
        logger.error(
            "data export stream failed: error_type=%s",
            type(exc).__name__,
            exc_info=not is_driver_exception(exc),
        )
        yield _line({"type": "footer", "complete": False, "rows": {"health_records": total}, "errors": {"health_records": "unavailable"}})


@router.get("/user/data-export")
async def data_export(
    request: Request,
    dataset: str = Query(""),
    offset: int = Query(0, ge=0),
    page_size: int | None = Query(None, ge=1),
    user_id: str = Depends(verify_token),
):
    """Export the caller's standardized health records as JSON or NDJSON."""
    if dataset not in {"", "health_records"}:
        return ErrorResponse(code=-1, msg=f"unknown dataset: {dataset}")
    wants_stream = NDJSON in (request.headers.get("accept") or "") and not dataset
    if wants_stream:
        return StreamingResponse(
            _stream(str(user_id)), media_type=NDJSON,
            headers={
                "Content-Disposition": f'attachment; filename="mirobody-{datetime.now(UTC):%Y-%m-%d}.ndjson"',
                "X-Accel-Buffering": "no",
            },
        )
    size = min(page_size or PAGE_SIZE, MAX_PAGE_SIZE)
    page = await _records.records(str(user_id), kind="all", limit=size, offset=offset)
    meta = _manifest(len(page["rows"]), offset=offset, truncated=page["has_more"], next_offset=offset + len(page["rows"]))
    meta["data"] = {"health_records": page["rows"]}
    return StandardResponse(data=meta)


__all__ = ["router"]
