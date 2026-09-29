"""`GET /api/user/data-export`: the caller's records, saying what they contain.

The path and shape are the hosted service's export contract, so one client can
take a person's data out of either deployment. What this deployment exports is
the visible observation set (`health_records`), through the same query as the
records table; files, medications and the rest are not in it yet, and the
manifest says so by naming only what is.

Caller only. A care-circle read grant shows a member's rows one page at a time
on the records table; it does not hand over a bulk copy.

JSON is one page with a manifest (`truncated` and `next_offset` when there is
more). NDJSON (`Accept: application/x-ndjson`) streams everything between a
header and a footer, and the footer's `complete` is the only proof it finished:
a stream cut off anywhere else has no footer, and a failed one says `false`.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse

from mirobody.collect import RECORD_EXPORT_COLUMNS, PostgresHealthQuery
from mirobody.kernel.ops import is_driver_exception
from mirobody.server.auth import verify_token
from mirobody.server.envelope import ErrorResponse, StandardResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["data-export"])

NDJSON = "application/x-ndjson"
DATASET = "health_records"
PAGE_SIZE = 200
MAX_PAGE_SIZE = 20_000
#: Rows per statement while streaming. Each statement re-walks the person's
#: amendment chains, so a small page made a long export quadratic.
STREAM_PAGE = 5_000

_records = PostgresHealthQuery(reported=True)


def _line(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def _row(row: dict) -> dict:
    return {k: row.get(k) for k in RECORD_EXPORT_COLUMNS}


def _manifest(rows: int, *, offset: int, truncated: bool, next_offset: int) -> dict:
    entry: dict = {"dataset": DATASET, "columns": list(RECORD_EXPORT_COLUMNS), "rows": rows}
    if offset:
        entry["offset"] = offset
    if truncated:
        entry.update({"truncated": True, "next_offset": next_offset})
    return {"generated_at": datetime.now(UTC).isoformat(), "manifest": [entry]}


def _failed(exc: Exception) -> None:
    # A type name only: a driver exception quotes the SQL with its parameters.
    logger.error("data export failed: error_type=%s", type(exc).__name__,
                 exc_info=not is_driver_exception(exc))


async def _stream(subject_id: str) -> AsyncIterator[bytes]:
    total = 0
    offset = 0
    yield _line({
        "type": "header", "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "datasets": [{"dataset": DATASET, "columns": list(RECORD_EXPORT_COLUMNS)}],
    })
    try:
        while True:
            page = await _records.records(subject_id, kind="all", limit=STREAM_PAGE, offset=offset)
            for row in page["rows"]:
                total += 1
                yield _line({"type": "row", "dataset": DATASET, "data": _row(row)})
            if not page["has_more"]:
                break
            offset += len(page["rows"])
    except Exception as exc:
        _failed(exc)
        yield _line({"type": "footer", "complete": False, "rows": {DATASET: total},
                     "errors": {DATASET: "unavailable"}})
        return
    yield _line({"type": "footer", "complete": True, "rows": {DATASET: total}, "errors": {}})


@router.get("/user/data-export")
async def data_export(
    request: Request,
    dataset: str = Query("", description=f'"" for everything, or "{DATASET}"'),
    offset: int = Query(0, ge=0),
    page_size: int | None = Query(None, ge=1, le=MAX_PAGE_SIZE),
    user_id: str = Depends(verify_token),
):
    """The caller's standardized records, as a JSON page or an NDJSON stream."""
    if dataset not in {"", DATASET}:
        return ErrorResponse(code=400, msg=f"dataset must be empty or {DATASET}.")
    if NDJSON in (request.headers.get("accept") or "") and not dataset:
        return StreamingResponse(
            _stream(str(user_id)), media_type=NDJSON,
            headers={
                "Content-Disposition": f'attachment; filename="mirobody-{datetime.now(UTC):%Y-%m-%d}.ndjson"',
                "X-Accel-Buffering": "no",
            },
        )
    size = page_size or PAGE_SIZE
    try:
        page = await _records.records(str(user_id), kind="all", limit=size, offset=offset)
    except Exception as exc:
        _failed(exc)
        return ErrorResponse(code=500, msg="This export could not complete.")
    rows = [_row(r) for r in page["rows"]]
    meta = _manifest(len(rows), offset=offset, truncated=page["has_more"], next_offset=offset + len(rows))
    meta["data"] = {DATASET: rows}
    return StandardResponse(data=meta)


__all__ = ["router"]
