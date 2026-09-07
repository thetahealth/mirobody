"""Text the harness writes for the model: the system prompt, and the note
that announces this turn's attachments.

`build_system_prompt` renders the configured template (`PROMPTS`) with the
tool descriptions, the time in the user's zone and the user context.
Rendering is strict: a variable the template names and the harness does not
supply raises, and `MirobodyAgent._build_system_prompt` turns that into the
`AgentError` the client sees. The alternative — falling back to the raw
template — handed the model `{{ tools_description }}` as literal text and
called it a warning.

`attachment_reminder` names the files a turn attached and where the virtual
filesystem serves them, so the model reads them without an `ls /uploads/`
round trip and never silently misses one. The paths come from the MOUNT, not
from the request — see the function for why that distinction has bitten.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from ..utils import prompts

logger = logging.getLogger(__name__)


async def build_system_prompt(
    base_prompt: str,
    language: str,
    user_id: str,
    langchain_tools: list,
    agent_name: str,
    user_name: str,
    timezone: str = "UTC",
    health_profile: str | None = None,
    tool_round_limit: int = 15,
) -> str:
    """Render `base_prompt` with tool descriptions, the current time in
    `timezone`, and the user context the template may reference."""
    tool_prompts = [
        f"**{tool.name}**: {tool.description}"
        for tool in langchain_tools
        if getattr(tool, "description", None)
    ]
    tools_description = "\n\n---\n\n".join(tool_prompts)
    if tools_description:
        tools_description += "\n\n---\n\n"

    current_time = datetime.now(ZoneInfo(timezone)).strftime("%A, %B %d, %Y, at %I:00 %p %Z (UTC%z)")

    template = prompts.environment(enable_async=True).from_string(base_prompt)
    return await template.render_async(
        agent_name=agent_name,
        user_name=user_name,
        current_time=current_time,
        language=language if language else "en",
        tools_description=tools_description,
        user_info={"user_id": user_id},
        health_profile=health_profile,
        tool_round_limit=tool_round_limit,
    )

async def report_date_status(attached: list[dict[str, Any]]) -> str:
    """One line per attachment: its file_key and whether a report date was
    found — what the prompt's "Report date of an attachment" rule keys on.

    Extraction starts when the chat request lands and the date probe answers
    within seconds, but this note is built at the very start of the turn, so
    the row may not know yet; the model then reads the document itself and
    asks only if the text shows no examination date."""
    from ..pulse.file_parser.services.file_db_service import FileDbService

    lines = []
    for f in attached:
        key = f.get("file_key")
        try:
            row = await FileDbService.get_file_by_key(key)
        except Exception:
            row = None
        content = (row or {}).get("file_content") or {}
        source = content.get("date_source")
        day = str(content.get("report_date") or "")[:10]
        if source == "extracted":
            status = f"report date {day} (found on the document)"
        elif source == "manual":
            status = f"report date {day} (set by the user)"
        elif source == "upload_time":
            status = "no date found — readings are on the upload day until you set one"
        else:
            status = "date not determined yet — read the document; if it shows no examination date, ask"
        lines.append(f"- file_key={key}: {status}")
    return "Report dates:\n" + "\n".join(lines)


async def attachment_reminder(backend: Any,
                              file_list: list[dict[str, Any]] | None) -> str | None:
    """A short note naming this turn's attachments and their `/uploads/` paths,
    so the model reads them without first having to ``ls /uploads/`` (and never
    silently misses one). Returns None when nothing readable is attached.

    **The paths come from the MOUNT, never from the request.** Deriving them
    from the request payload is what issue #40 was: two independent name
    computations that agreed only for as long as nothing renamed the file
    mid-turn. Pinning `/uploads/` to the request's names (PR #42) made them
    agree in the common case, but a request-derived listing can still name a
    path the mount does not serve, in at least three ways:

    * two attachments share a name — the mount serves the second under a
      ``__thf_`` suffix the request cannot predict, so a request-derived note
      announces one path twice and every read lands on the first file;
    * the row is gone or was never the caller's (deleted, another user's
      `file_key`) — the projection filters on `user_id` and `is_del`, the
      request does not, so the note promises a file the mount will refuse;
    * more than `_MAX_SESSION_FILES` attachments — the mount truncates, and a
      request-derived note lists files that are not there.

    Every one of those ends the same way for the user: the agent says the
    upload is unreadable and asks them to send it again. Asking the projection
    removes the class rather than the instances, and keeps this note correct
    through any future change to how the mount names a file.

    The listing is a snapshot taken as the turn starts, while the mount
    re-queries on every tool call. That is the right way round: an upload
    INSERTs its `th_files` row before the chat request carries its `file_key`,
    so what lands mid-turn is an UPDATE (the rename), and anything the snapshot
    somehow missed is still reachable with `ls`.

    Injected as a transient message (NOT the system prompt — that is cached and
    must stay stable across turns). Ephemeral: persistence saves the user
    question + assistant reply separately, not this note.
    """
    attached = [f for f in (file_list or []) if isinstance(f, dict) and f.get("file_key")]
    if not attached:
        return None

    # Anonymous sessions get a bare StateBackend with no /uploads/ route, and
    # nothing to announce; `routes` is what tells the two apart.
    uploads = getattr(backend, "routes", {}).get("/uploads/")
    if uploads is None:
        return None

    # The public listing seam, so this note says exactly what the model's own
    # `ls /uploads/` would say. A query failure returns no entries (`_files`
    # logs and degrades); the model then falls back to `ls`, as it did before
    # this note existed.
    listed = await uploads.als("/")
    paths = [f"/uploads{e['path']}" for e in (listed.entries or [])
             if e.get("path") and not e.get("is_dir")]
    if not paths:
        return None

    listing = "\n".join(f"- {p}" for p in paths)
    note = (
        "[System note: the user attached file(s) to THIS message. "
        "Read the relevant one(s) with read_file before answering:\n"
        f"{listing}\n"
        f"{await report_date_status(attached)}"
    )
    # Say so rather than quietly listing fewer than were sent: a model that
    # believes it has seen everything answers about everything.
    if len(paths) < len(attached):
        note += (
            f"\n({len(paths)} of {len(attached)} attached file(s) are readable; "
            "the rest are not in the user's file store.)"
        )
    return note + "]"
