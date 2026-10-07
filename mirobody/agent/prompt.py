"""Text the harness writes for the model: the system prompt, and the note
that announces this turn's attachments.

`build_system_prompt` renders the configured template (`PROMPTS`) with the
tool descriptions, the time in the user's zone and the user context.
Rendering is strict: a variable the template names and the harness does not
supply raises, and `MirobodyAgent._build_system_prompt` turns that into the
`AgentError` the client sees, naming the exception's type. The alternative,
falling back to the raw template, handed the model `{{ tools_description }}`
as literal text and called it a warning.

`attachment_reminder` names the files a turn attached and where the virtual
filesystem serves them, so the model reads them without an `ls /uploads/`
round trip and never silently misses one. The paths come from the MOUNT, not
from the request: see the function for why that distinction has bitten.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any
from zoneinfo import ZoneInfo

from mirobody.utils import prompts

logger = logging.getLogger(__name__)


#: Word-sized units per script: a Latin or Cyrillic word, two Han characters
#: (most Chinese words), two kana, one Hangul block (a syllable of a word).
_SCRIPTS = (
    ("latin", re.compile(r"[A-Za-z\u00c0-\u024f]+"), 1.0),
    ("cyrillic", re.compile(r"[\u0400-\u04ff]+"), 1.0),
    ("kana", re.compile(r"[\u3040-\u30ff]"), 0.5),
    ("han", re.compile(r"[\u4e00-\u9fff]"), 0.5),
    ("hangul", re.compile(r"[\uac00-\ud7af]"), 0.5),
)
#: Letters only Ukrainian, Belarusian or Serbian use: Cyrillic is not Russian then.
_NOT_RUSSIAN = re.compile(r"[іїєґўђјљњћџ]", re.I)


def question_language(text: str) -> str:
    """The language a question is written in, read off its script, for the
    languages a model has been seen to answer in English; "" when the
    question's own words leave it to the model (English, a mix, too short).

    The prompt already says the latest question decides; Bonsai-27B still
    answered a Chinese trend question in English, twice in two, after 78
    rows of English tool output. Naming the language turns inference into
    an instruction, so it is named only when the question is mostly in it:
    one Chinese lab term in an English question ("What does 甘油三酯 mean?")
    named Chinese and ordered a Chinese answer. Kana marks Japanese, which
    shares Han with Chinese; fewer than three Han characters alone (血圧) could
    be either, and are left to the model.
    """
    # A term (LDL, HbA1c, VO2) is the same in every language and says nothing
    # about this one: a Chinese question names its tests in Latin letters.
    words = re.sub(r"(?<![A-Za-z0-9])[A-Za-z0-9]*(?:[A-Z]{2,}|\d)[A-Za-z0-9]*(?![A-Za-z0-9])", " ", text)
    units = {name: len(pattern.findall(words)) * weight for name, pattern, weight in _SCRIPTS}
    total = sum(units.values())
    if not total:
        return ""
    if units["kana"]:
        cjk = units["kana"] + units["han"]
        return "Japanese (日本語)" if cjk / total >= 0.5 else ""
    script = max(units, key=units.get)
    if units[script] / total < 0.6:
        return ""
    if script == "han":
        if units["han"] < 1.5:  # fewer than three characters
            return ""
        from mirobody.zh_fold import fold_to_hans

        return "Traditional Chinese (繁體中文)" if fold_to_hans(text) != text else "Chinese (中文)"
    if script == "hangul":
        return "Korean (한국어)"
    if script == "cyrillic" and not _NOT_RUSSIAN.search(text):
        return "Russian (русский)"
    return ""


async def build_system_prompt(
    base_prompt: str,
    langchain_tools: list,
    agent_name: str,
    record_owner: str = "",
    timezone: str = "UTC",
    health_profile: str | None = None,
    tool_round_limit: int = 15,
    answer_language: str = "",
) -> str:
    """Render `base_prompt` with tool descriptions, the current time in
    `timezone`, and the user context the template may reference.
    `record_owner` names whose record it is when that is not the asker's;
    `answer_language` the latest question's language (`question_language`)."""
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
        record_owner=record_owner,
        current_time=current_time,
        tools_description=tools_description,
        health_profile=health_profile,
        tool_round_limit=tool_round_limit,
        answer_language=answer_language,
    )


async def report_date_status(user_id: str, file_keys: list[str]) -> str:
    """One line per attachment of `user_id`'s record: its file_key and whether
    a report date was found: what the prompt's "Report date of an attachment"
    rule keys on.

    Only live files of `user_id`'s record are read, the filter the `/uploads/`
    mount applies, in one query. A key from the request was read by key alone,
    once per file, so another account's file put its report date and source
    into this conversation.

    Extraction starts when the chat request lands and the date probe answers
    within seconds, but this note is built at the very start of the turn, so
    the row may not know yet; the model then reads the document itself and
    asks only if the text shows no examination date."""
    from mirobody.utils import execute_query

    try:
        rows = await execute_query(
            "SELECT file_key, decrypt_content(file_content) AS file_content FROM th_files"
            " WHERE COALESCE(query_user_id, user_id) = :user_id AND file_key = ANY(:keys)"
            " AND is_del = false",
            params={"user_id": str(user_id), "keys": list(file_keys)},
        )
        found = {str(r.get("file_key")): _json_object(r.get("file_content")) for r in rows or []}
    except Exception as e:
        logger.warning("report dates unavailable for the attachment note: error_type=%s", type(e).__name__)
        found = {key: {} for key in file_keys}
    lines = [f"- file_key={key}: {_date_status(found[key])}" for key in file_keys if key in found]
    return "Report dates:\n" + "\n".join(lines) if lines else ""


def _date_status(content: dict[str, Any]) -> str:
    source = content.get("date_source")
    day = str(content.get("report_date") or "")[:10]
    if source == "extracted":
        return f"report date {day} (found on the document)"
    if source == "manual":
        return f"report date {day} (set by the user)"
    if source == "upload_time":
        return "no date found — readings are on the upload day until you set one"
    return "date not determined yet — read the document; if it shows no examination date, ask"


def _json_object(text: Any) -> dict[str, Any]:
    try:
        value = json.loads(text) if isinstance(text, str) else text
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


_IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif"})


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

    * two attachments share a name: the mount serves the second under a name
      tagged with its key (`naming.disambiguate`), which the request cannot
      predict, so a request-derived note announces one path twice and every
      read lands on the first file;
    * the row is gone or was never the caller's (deleted, another user's
      `file_key`): the projection filters on `user_id` and `is_del`, the
      request does not, so the note promises a file the mount will refuse;
    * more than `_MAX_SESSION_FILES` attachments: the mount truncates, and a
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

    Sent as a message of this turn, NOT in the system prompt, which is cached
    and must stay stable across turns. The checkpointer keeps it in the
    thread with the question it came with; `th_messages` stores only the
    question and the answer.
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
        f"{listing}"
    )
    dates = await report_date_status(getattr(uploads, "user_id", ""), [str(f["file_key"]) for f in attached])
    if dates:
        note += f"\n{dates}"
    # Before it reads anything: a text-only model is handed an image's OCR
    # text, and must not answer as if it had seen the picture.
    if not getattr(uploads, "supports_image", True) and any(
        PurePosixPath(p).suffix.lower() in _IMAGE_SUFFIXES for p in paths
    ):
        note += (
            "\nYou cannot see images: an attached image reaches you as the text an OCR "
            "model read from it (printed text and tables only). For what a photo shows, "
            "such as a meal, a rash or a scene, say you cannot see it and ask the user to "
            "describe it."
        )
    # Say so rather than quietly listing fewer than were sent: a model that
    # believes it has seen everything answers about everything.
    if len(paths) < len(attached):
        note += (
            f"\n({len(paths)} of {len(attached)} attached file(s) are readable; "
            "the rest are not in the user's file store.)"
        )
    return note + "]"
