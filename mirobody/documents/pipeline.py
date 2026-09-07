"""The two-phase upload: store now, read later — with every product decision a port.

An upload has one shape wherever health documents are accepted. Phase one, in
the request: store the originals and answer, so a slow scan never holds the
client. Phase two, in the background: turn each stored file into text
(`extract`), keep that text beside the file, hand the text to whatever mines
it (indicators, a summary), and leave a STATUS the user can see — ``pending``
while it runs, ``ok`` when done, ``empty_text`` for a blank scan, ``partial``
when some pages failed, ``failed`` with the stage and the reason. A silent
failure is the failure this exists to prevent: an upload that produced no
readings must be distinguishable from a report that had none.

What differs between deployments is where things live and what "mine" means,
so those are `Ports`: the object store, the text sidecar, the status table, the
ingest. What is the same is here: per-file isolation (one bad file never fails
the batch), the status transitions, the skip rules — a file whose text already
exists is not OCR'd again, a file already ingested (or being ingested right
now) is not mined again, except when the caller says ``force`` or scopes the
result to a session that must get its own rows.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

logger = logging.getLogger(__name__)

PENDING = "pending"
OK = "ok"
EMPTY_TEXT = "empty_text"
PARTIAL = "partial"
FAILED = "failed"

#: How long a ``pending`` row counts as "in flight". Extracting a large PDF can
#: run for minutes, but not forever: every failure path writes ``failed``, so a
#: ``pending`` that never moves means the process died mid-way — after this
#: window it is retried, or the file is never extracted at all.
IN_FLIGHT_MS = 10 * 60 * 1000

#: ``(filename, content_type, data)`` — one uploaded part.
Part = tuple[str, str, bytes]
#: ``extract(filename, content_type, data, key) -> (text, error)``; ``error`` is
#: non-empty when the extractor raised (a blank scan is ``("", "")``).
Extract = Callable[[str, str, bytes, str | None], Awaitable[tuple[str, str]]]


@dataclass(frozen=True)
class IngestResult:
    """What mining one file's text produced. ``failed_pages`` non-empty means
    the result is partial and the file is worth retrying."""

    stored: int = 0
    readings: int = 0
    failed_pages: tuple[Any, ...] = ()


class Ports(Protocol):
    """The deployment's half of the pipeline."""

    async def store(self, user_id: int, name: str, content_type: str, data: bytes) -> str | None:
        """Persist one original; its key, or ``None`` when it could not be stored."""

    async def has_text(self, user_id: int, key: str) -> bool:
        """Whether extracted text already exists for this key (content-addressed
        stores make a re-upload the same key — and the same text)."""

    async def store_text(self, user_id: int, key: str, text: str) -> None:
        """Keep the extracted text beside the original."""

    async def status(self, key: str, user_id: int) -> dict | None:
        """The file's prior status row (``status``, ``indicators``, ``updated_at`` ms), or ``None``."""

    async def record(self, key: str, user_id: int, status: str, *, stage: str = "", error: str = "",
                     pages_failed: int = 0, indicators: int = 0) -> None:
        """Write the file's status. Best effort: never raises into the pipeline."""

    async def ingest(self, user_id: int, key: str, name: str, text: str) -> IngestResult | None:
        """Mine the text; ``None`` when there is nothing to mine (recorded as
        ``ok`` with zero indicators, never as a failure)."""


@dataclass
class Plan:
    """Which parts take which path — decided once, before any I/O."""

    convertible: list[int] = field(default_factory=list)  # a parser or OCR produces text
    textual: list[int] = field(default_factory=list)  # decodable as is; no sidecar

    def wants_text(self, i: int) -> bool:
        return i in self.convertible or i in self.textual


def plan(parts: Iterable[Part], *, is_convertible: Callable[[str, str], bool], is_text: Callable[[str, str], bool]) -> Plan:
    out = Plan()
    for i, (name, content_type, _data) in enumerate(parts):
        if is_convertible(name, content_type):
            out.convertible.append(i)
        elif is_text(name, content_type):
            out.textual.append(i)
    return out


def already_ingesting(prior: dict | None, *, in_flight_ms: int = IN_FLIGHT_MS, now_ms: int | None = None) -> str:
    """Why this file's text must NOT be mined again — ``""`` when it may be.

    ``ok`` means it was: mining the same text twice makes duplicate rows (a
    model does not always find the collection date twice, and a natural key
    with a calendar day in it cannot dedupe a row dated today against one dated
    correctly). A fresh ``pending`` means someone is mining it now — ``ok`` is
    written AFTER the ingest, so a client re-uploading in that window would
    otherwise slip past an ok-only check (observed). An unreadable timestamp
    skips conservatively: a duplicate is harder to repair than a delay.
    """
    if not prior:
        return ""
    status = str(prior.get("status") or "").strip()
    if status == OK:
        return f"already ingested (indicators={prior.get('indicators')})"
    if status == PENDING:
        try:
            age = (now_ms if now_ms is not None else int(time.time() * 1000)) - int(prior.get("updated_at"))
        except (TypeError, ValueError):
            return "extraction in flight"
        if age < in_flight_ms:
            return f"extraction in flight ({age}ms ago)"
    return ""


async def store_parts(user_id: int, parts: list[Part], ports: Ports) -> list[str | None]:
    """Phase one: every original, one at a time; a failure is a ``None`` in its
    slot, never a failed batch."""
    keys: list[str | None] = []
    for name, content_type, data in parts:
        try:
            keys.append(await ports.store(user_id, name, content_type, data))
        except Exception as exc:
            logger.error("upload: store failed for one part: user_id=%s error_type=%s", user_id, type(exc).__name__)
            keys.append(None)
    return keys


async def snapshot_done(subject: int, keys: Iterable[str | None], ports: Ports) -> frozenset[str]:
    """The keys whose text is already mined or being mined — taken BEFORE this
    run writes its own ``pending``, or the run would read itself and skip its
    own first extraction (observed: an upload with zero indicators)."""
    done: set[str] = set()
    for key in keys:
        if key and already_ingesting(await ports.status(key, subject)):
            done.add(key)
    return frozenset(done)


async def extract_phase(
    user_id: int,
    parts: list[Part],
    keys: list[str | None],
    *,
    ports: Ports,
    extract: Extract,
    the_plan: Plan,
    subject: int | None,
    skip_keys: frozenset[str] = frozenset(),
    force: bool = False,
) -> list[tuple[int, str]]:
    """Phase two, first half: text for every part that can yield one, the
    sidecar for the convertible ones, statuses for all of them. Returns
    ``(index, text)`` for the ingest half. Parts already extracted (text on
    file, unless ``force``) and parts already mined (``skip_keys``) are left as
    they are — a re-run must never overwrite an ``ok`` with an ``empty_text``.
    """
    started = time.monotonic()
    already_extracted: set[int] = set()
    if not force:
        for i in the_plan.convertible:
            key = keys[i]
            if key and await ports.has_text(user_id, key):
                already_extracted.add(i)
    if already_extracted:
        logger.info("upload: skipping already-extracted parts: part_count=%d", len(already_extracted))

    if subject is not None:
        for i in the_plan.convertible + the_plan.textual:
            key = keys[i]
            if key and key not in skip_keys and i not in already_extracted:
                await ports.record(key, subject, PENDING, stage="extract")

    async def one(i: int) -> tuple[int, str, str]:
        name, content_type, data = parts[i]
        if i in already_extracted:
            return i, "", ""
        if i in the_plan.convertible:
            text, error = await extract(name, content_type, data, keys[i])
            return i, text, error
        if i in the_plan.textual:
            return i, data.decode("utf-8", errors="replace"), ""
        return i, "", ""

    extracted = await asyncio.gather(*(one(i) for i in range(len(parts))))
    logger.info("upload: extracted parts: part_count=%d time_cost=%.1fs", len(parts), time.monotonic() - started)

    if subject is not None:
        for i, text, error in extracted:
            key = keys[i]
            if not key or i in already_extracted or not the_plan.wants_text(i):
                continue
            if error:
                await ports.record(key, subject, FAILED, stage="extract", error=error)
            elif not text.strip():
                await ports.record(key, subject, EMPTY_TEXT, stage="extract",
                                   error="no readable text was extracted from this document")

    wired = 0
    for i, text, _error in extracted:
        key = keys[i]
        if not (key and text.strip() and i in the_plan.convertible):
            continue  # a textual part IS its text; nothing to keep beside it
        try:
            await ports.store_text(user_id, key, text)
            wired += 1
        except Exception as exc:
            logger.error("upload: sidecar write failed: error_type=%s", type(exc).__name__)
            if subject is not None:
                await ports.record(key, subject, FAILED, stage="sidecar", error=f"{type(exc).__name__}: {str(exc)[:300]}")
    if wired:
        logger.info("upload: sidecars written: part_count=%d", wired)
    return [(i, text) for i, text, _error in extracted]


async def ingest_phase(
    subject: int,
    parts: list[Part],
    keys: list[str | None],
    results: list[tuple[int, str]],
    *,
    ports: Ports,
    the_plan: Plan,
    skip_keys: frozenset[str] = frozenset(),
) -> None:
    """Phase two, second half: mine each text, record ``ok`` / ``partial`` /
    ``failed``. ``skip_keys`` are files whose text was mined already (see
    `snapshot_done`); the caller decides whether a scoped run may skip."""
    for i, text in results:
        key = keys[i]
        name = parts[i][0]
        if not (key and text and text.strip() and the_plan.wants_text(i)):
            continue  # the extract phase recorded these
        if key in skip_keys:
            logger.info("upload: text already mined or in flight; skipping ingest for one part")
            continue
        started = time.monotonic()
        try:
            result = await ports.ingest(subject, key, name, text)
            if result is None:
                # Text, but nothing to mine (a photo, a letter): that is `ok`
                # with zero indicators, never a failure.
                await ports.record(key, subject, OK, stage="ingest", indicators=0)
                continue
            if result.failed_pages:
                logger.error("upload: ingest partial: pages_failed=%d stored_count=%d time_cost=%.1fs",
                             len(result.failed_pages), result.stored, time.monotonic() - started)
                await ports.record(key, subject, PARTIAL, stage="ingest",
                                   error=f"{len(result.failed_pages)} page(s) failed extraction: {list(result.failed_pages)}",
                                   pages_failed=len(result.failed_pages), indicators=result.stored)
            else:
                await ports.record(key, subject, OK, stage="ingest", indicators=result.stored)
        except Exception as exc:
            logger.error("upload: ingest failed: error_type=%s time_cost=%.1fs", type(exc).__name__, time.monotonic() - started)
            await ports.record(key, subject, FAILED, stage="ingest", error=f"{type(exc).__name__}: {str(exc)[:300]}")


async def extract_and_wire(
    user_id: int,
    parts: list[Part],
    keys: list[str | None],
    *,
    ports: Ports,
    extract: Extract,
    is_convertible: Callable[[str, str], bool],
    is_text: Callable[[str, str], bool],
    subject: int | None = None,
    force: bool = False,
    snapshot: bool = True,
) -> None:
    """Phase two, whole: extract, sidecar, statuses, ingest. ``subject`` is who
    the mined rows belong to (``None`` = extract only). ``snapshot=False`` mines
    even files mined before — for a run scoped to a session that must get its
    own rows. A failure of the whole task still leaves every affected file a
    ``failed`` status rather than a ``pending`` forever."""
    the_plan = plan(parts, is_convertible=is_convertible, is_text=is_text)
    try:
        skip = await snapshot_done(subject, keys, ports) if (subject is not None and snapshot and not force) else frozenset()
        results = await extract_phase(user_id, parts, keys, ports=ports, extract=extract, the_plan=the_plan,
                                      subject=subject, skip_keys=skip, force=force)
        if subject is not None:
            await ingest_phase(subject, parts, keys, results, ports=ports, the_plan=the_plan, skip_keys=skip)
    except Exception as exc:
        logger.error("upload: background extraction failed: part_count=%d error_type=%s", len(parts), type(exc).__name__)
        if subject is not None:
            for i in the_plan.convertible + the_plan.textual:
                if keys[i]:
                    await ports.record(keys[i], subject, FAILED, stage="background", error=f"{type(exc).__name__}: {str(exc)[:300]}")


_background: set[asyncio.Task] = set()


def spawn(coro: Awaitable[None]) -> asyncio.Task:
    """Run phase two without awaiting it, holding a reference so the loop does
    not collect the task mid-flight."""
    task = asyncio.ensure_future(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)
    return task
