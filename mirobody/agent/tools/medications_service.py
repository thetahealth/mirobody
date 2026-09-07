"""`query_medications` — the one tool for a person's medications.

Medications are an entity, not a series: a plan has a lifecycle, a dose has a
day, a course has a reason it closed. That grammar has nothing in common with
`resolution` and `aggregate`, so it is its own tool with five parameters
(`meds.TOOL_SCHEMA`) rather than a mode inside the readings tool — a mode
under which most of the readings parameters would have to be refused.

The tool body is thin on purpose: authorization, the two stores, a window in
the subject's zone, and an envelope. Everything that decides what a row says
— which plans are in effect, what state each of today's slots is in, how a
schedule reads — is pure and lives in `mirobody.kernel.meds`, so a consumer
with its own storage renders the same rows.

It never raises, for the same reason as the readings tool: a PTC call has
nothing above it to contain a fault.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ...kernel import meds, query, series, tools
from ...kernel.ops import is_driver_exception
from ._authz import caller_of, denied, refused, subject_for
from .health_indicators_service import awaited, envelope_meta, render_compact

logger = logging.getLogger(__name__)


class MedicationsService:
    """The tool body. `__tools__` is the whole published surface; `envelope`
    is API for the chat adapter, not a tool."""

    __tools__ = (meds.TOOL_NAME,)
    input_schema = meds.TOOL_SCHEMA

    def __init__(self, store: Any = None, dose_log: Any = None, *, tz: Any = None, now: Any = None) -> None:
        # Injected so a test can hand in fakes and a deployment that keeps
        # medications elsewhere can say so; the reference stores otherwise.
        self._store = store
        self._dose_log = dose_log
        self._tz = tz  # subject_id -> IANA zone; the reference reads the user row
        self._now = now

    async def query_medications(self, user_info: dict[str, Any], **args: Any) -> dict[str, Any]:
        """
        Read this person's medications: the list they keep (plan), the doses
        they recorded (log), or the courses over time (history).

        USE IT for anything about what they take, took or stopped — "what am
        I on", "when did I start metformin", "did I take my evening dose",
        "what was I taking in March". `view="history"` answers "when did I
        switch", which is how a question like "how did my blood pressure move
        after the change" gets its date for `query_health_indicators`.

        DO NOT use it for readings (`query_health_indicators`), for drug
        information or interactions, or for a person outside the caller's
        care circle. Read-only: it cannot add, change or stop a medication.

        The parameters are documented in the schema (`input_schema` IS
        `meds.TOOL_SCHEMA`, published verbatim).

        Returns:
            A compact table plus a `meta` block. Absence means "not on file
            here", never "not taken".

        Notes for LLMs:
            - A plan is what the person intends to take. Never answer an
              adherence question ("did I take it", "how many did I miss") from
              the plan — use `view="log"`.
            - A dose missing from the log is not evidence it was not taken.
        """
        envelope = await self.envelope(user_info, **args)
        view = str(args.get("view") or meds.VIEW_PLAN)
        return {"result": render_compact(envelope, meds.VIEW_COLUMNS.get(view)), **envelope_meta(envelope)}

    async def envelope(self, user_info: Mapping[str, Any], **args: Any) -> tools.Envelope:
        caller_id = caller_of(user_info)
        if not caller_id:
            return denied("authorization required")
        try:
            return await self._run(caller_id, args)
        except query.Denied:
            return denied("you may not read this person's data")
        except Exception as e:
            tool_name = meds.TOOL_NAME
            logger.error("[%s] error_type=%s", tool_name, type(e).__name__, exc_info=not is_driver_exception(e))
            return tools.fault_envelope(e)

    # --- the run ------------------------------------------------------------

    async def _run(self, caller_id: str, args: Mapping[str, Any]) -> tools.Envelope:
        problems = meds.validate_query(args)
        if problems:
            return refused(problems)
        request = meds.parse_query(args)
        subject_id = await subject_for(caller_id, request.member)
        store, log = self._stores()
        tz = await self._zone_of(subject_id)
        now = self._clock().astimezone(series.zone(tz))
        today = now.date()
        window = _window(request, today, default_days=meds.LOG_DEFAULT_DAYS if request.view == meds.VIEW_LOG else None)

        plans = list(await awaited(store.list(subject_id)))
        if request.view == meds.VIEW_LOG:
            events = list(await awaited(log.list(subject_id, window)))  # type: ignore[arg-type]
            rows = meds.log_rows(events, {p.plan_id: p for p in plans}, keywords=request.keywords)
        elif request.view == meds.VIEW_HISTORY:
            by_plan = {p.plan_id: list(await awaited(store.courses(p.plan_id))) for p in plans}
            rows = meds.history_rows(plans, by_plan, keywords=request.keywords, window=window)
        else:
            todays = list(await awaited(log.list(subject_id, (today, today))))
            rows = meds.plan_rows(
                plans, todays, keywords=request.keywords, window=window, today=today,
                now_ms=int(now.timestamp() * 1000), tz=tz,
            )
        return _envelope_for(request, window, tz, rows)

    def _stores(self) -> tuple[Any, Any]:
        if self._store is None or self._dose_log is None:
            from ...pulse.meds import PostgresDoseLogStore, PostgresMedicationStore
            self._store = self._store or PostgresMedicationStore()
            self._dose_log = self._dose_log or PostgresDoseLogStore()
        return self._store, self._dose_log

    async def _zone_of(self, subject_id: str) -> str:
        if self._tz is not None:
            return await awaited(self._tz(subject_id)) or "UTC"
        from ...user.user import get_user
        row = await get_user(user_id=subject_id)
        return ((row or {}).get("tz") or "").strip() or "UTC"

    def _clock(self) -> datetime:
        return self._now() if callable(self._now) else datetime.now(UTC)


# --- pure --------------------------------------------------------------------


def _window(request: meds.MedicationsRequest, today: date, *, default_days: int | None) -> tuple[date, date] | None:
    """The caller's dates as local days, inclusive. Neither date: `None`
    (everything) unless the view has a default span (the dose log)."""
    if not request.start and not request.end:
        return (today - timedelta(days=default_days), today) if default_days else None
    end = date.fromisoformat(request.end) if request.end else today
    start = date.fromisoformat(request.start) if request.start else end - timedelta(days=default_days or 30)
    if end < start:
        start, end = end, start
    return (start, end)


def _envelope_for(
    request: meds.MedicationsRequest, window: tuple[date, date] | None, tz: str, rows: list[dict]
) -> tools.Envelope:
    dated = bool(request.start or request.end) or (request.view == meds.VIEW_LOG)
    meta = tools.Meta(
        window=(window[0].isoformat(), window[1].isoformat()) if (window and dated) else ("", ""),
        tz=tz,
        row_count=len(rows),
        truncated=len(rows) >= meds.MAX_ROWS,
    )
    notes = (meds.PLAN_NOTE,) if request.view == meds.VIEW_PLAN else (meds.LOG_NOTE,) if request.view == meds.VIEW_LOG else ()
    if not rows:
        notes = (*notes, "nothing on file for this view; absence means not recorded here, never not taken")
    return tools.Envelope(
        tools.STATUS_PARTIAL if meta.truncated else tools.STATUS_OK,
        data=[{k: v for k, v in r.items() if k != "provenance"} for r in rows],
        meta=meta,
        provenance={str(r.get("plan_id") or ""): str(r.get("provenance") or "measured") for r in rows},
        assumptions=notes,
        next_steps=(tools.NEXT_NARROW_WINDOW,) if meta.truncated else (),
    )


__tools__: tuple[str, ...] = ()

__all__ = ["MedicationsService"]
