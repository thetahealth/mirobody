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

import ipaddress
import json
import logging
import re
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

from mirobody.agent.clock import now as _now
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


#: The note in the question's own language. Written in English, it lost to
#: 19k characters of English tool output: MiniCPM5-2B answered a Chinese chart
#: question in English on two runs of three (2026-10-12). It names the thinking,
#: which the web client streams, and tells Japanese from Chinese. DeepSeek V4
#: Flash thought in English for every Chinese and Japanese question and wrote
#: Chinese for two Japanese ones of four; with it, ten Chinese runs of thirteen
#: thought in Chinese and nine Japanese runs of nine answered in Japanese
#: (2026-10-31).
_TURN_NOTES = {
    "Chinese (中文)": "[系统提示：现在是 {now}。最新的问题是中文：思考过程和整个回答都用中文写，"
                      "图表标题和坐标轴也用中文，即使工具返回的内容是英文。]",
    "Traditional Chinese (繁體中文)": "[系統提示：現在是 {now}。最新的問題是繁體中文：思考過程和整個回答都用繁體中文寫，"
                                     "圖表標題和座標軸也用繁體中文，即使工具回傳的內容是英文。]",
    "Japanese (日本語)": "[システム通知：現在は {now} です。最新の質問は日本語です（中国語ではありません）。ツールの出力が英語でも、"
                        "考える過程も、グラフのタイトルや軸を含む回答も、すべて日本語で書いてください。]",
    "Korean (한국어)": "[시스템 안내: 지금은 {now}입니다. 최신 질문은 한국어입니다. 도구 출력이 영어여도 "
                      "생각하는 과정과, 차트 제목과 축을 포함한 답변 전체를 한국어로 작성하세요.]",
    "Russian (русский)": "[Системная заметка: сейчас {now}. Последний вопрос на русском: рассуждения и весь ответ, "
                         "включая заголовки и оси графиков, пишите по-русски, даже если инструменты вернули текст "
                         "на английском.]",
}


def turn_note(timezone: str, question: str) -> str:
    """The current time and the language to answer in, as a note sent with the
    question. In the system prompt both changed per hour or per question, and
    the tool definitions after them were read again: 5.2k tokens for
    MiniCPM5-2B on llama.cpp each time, 47 with this note (2026-10-12)."""
    moment = _now(timezone)
    template = _TURN_NOTES.get(question_language(question))
    if template:
        return template.format(now=moment.strftime("%Y-%m-%d %H:00 %Z (UTC%z)"))
    return f"[System note: it is now {moment.strftime('%A, %B %d, %Y, at %I:00 %p %Z (UTC%z)')}.]"


async def build_system_prompt(
    base_prompt: str,
    langchain_tools: list,
    agent_name: str,
    record_owner: str = "",
    timezone: str = "UTC",
    health_profile: str | None = None,
    tool_round_limit: int = 15,
    answer_language: str = "",
    knowledge: Mapping[str, str] | None = None,
    deployment_facts: str = "",
) -> str:
    """Render `base_prompt` with tool descriptions, the current time in
    `timezone`, and the user context the template may reference.
    `record_owner` names whose record it is when that is not the asker's;
    `answer_language` the latest question's language (`question_language`);
    `knowledge` the medical-knowledge scopes this deployment has, by name;
    `deployment_facts` where this deployment's models run (`deployment_facts`)."""
    tool_prompts = [
        f"**{tool.name}**: {tool.description}"
        for tool in langchain_tools
        if getattr(tool, "description", None)
    ]
    tools_description = "\n\n---\n\n".join(tool_prompts)
    if tools_description:
        tools_description += "\n\n---\n\n"

    current_time = _now(timezone).strftime("%A, %B %d, %Y, at %I:00 %p %Z (UTC%z)")

    template = prompts.environment(enable_async=True).from_string(base_prompt)
    return await template.render_async(
        agent_name=agent_name,
        record_owner=record_owner,
        current_time=current_time,
        tools_description=tools_description,
        health_profile=health_profile,
        tool_round_limit=tool_round_limit,
        answer_language=answer_language,
        knowledge=dict(knowledge or {}),
        deployment_facts=deployment_facts,
        code_language=next((str((getattr(tool, "metadata", None) or {}).get("language", ""))
                            for tool in langchain_tools if tool.name == "code_exec"), ""),
    )


#: Hosts that are this machine: loopback, Docker's name for its host, and the
#: compose services that serve the local models.
_THIS_MACHINE = frozenset({"localhost", "host.docker.internal", "llama", "llama_cpu"})

#: Where an entry with no `base_url` sends requests: its family's API.
_FAMILY_HOST = {
    "openai": "api.openai.com",
    "anthropic": "api.anthropic.com",
    "google_genai": "generativelanguage.googleapis.com",
    "google_vertexai": "aiplatform.googleapis.com",
    "google_anthropic_vertex": "aiplatform.googleapis.com",
}


def place(base_url: str, llm_type: str = "openai") -> str:
    """Where a model at `base_url` runs, as the prompt states it: on this
    machine, on the person's own network, or the internet host it is sent
    to. "" when that cannot be told (an endpoint name left unset)."""
    if base_url:
        host = (urlsplit(base_url).hostname or "") if "://" in base_url else ""
    else:
        host = _FAMILY_HOST.get(llm_type.strip().lower().replace("-", "_"), "")
    host = host.lower()
    if not host:
        return ""
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if host in _THIS_MACHINE or (address is not None and address.is_loopback):
        return "on this machine"
    if (address is not None and address.is_private) or "." not in host or host.endswith((".local", ".lan")):
        return f"on your own network ({host}), not a cloud service"
    return f"sent over the internet to {host}"


def _as_route(spec: Any) -> tuple[str, str, str] | None:
    return (spec.model, spec.base_url, spec.llm_type) if spec is not None else None


def _chat_route(alias: str) -> tuple[str, str, str] | None:
    """(model, base_url, llm_type) of a chat entry. Read from the entry itself:
    `resolve_named` serves only the utility families, so it has no answer for
    a Gemini or Vertex chat entry."""
    from mirobody.utils.config.llm import base_url_override, chat_entries, endpoint_value, is_endpoint_name, resolve_named

    entry = chat_entries().get(alias) if alias else None
    if not entry:
        return None
    spec = resolve_named(alias)
    if spec is not None:
        return _as_route(spec)
    base_url = str(entry.get("base_url") or "").strip()
    if is_endpoint_name(base_url):
        base_url = endpoint_value(base_url)
    base_url = base_url_override(str(entry.get("api_key") or "").strip()) or base_url
    return str(entry.get("model") or alias), base_url, str(entry.get("llm_type") or "openai")


def runs_locally(chat_alias: str) -> bool:
    """Whether chat entry `chat_alias` runs on this machine or the person's
    own network (`place`). False for a cloud service and for an entry whose
    endpoint cannot be told."""
    route = _chat_route(chat_alias)
    return route is not None and place(route[1], route[2]).startswith(("on this machine", "on your own network"))


def deployment_facts(chat_alias: str) -> str:
    """Which model answers and which ones read documents, and where each runs,
    read from the routes in effect. The model answers "is my data uploaded"
    and "which model are you" from this, not from what it believes it is."""
    from mirobody.utils.config.llm import resolve_route

    routes = [("the answering model", _chat_route(chat_alias)),
              ("report photos and scanned pages are read by", _as_route(resolve_route("ocr") or resolve_route("vision"))),
              ("text extraction, titles and summaries use", _as_route(resolve_route("text")))]
    parts = []
    for label, route in routes:
        if route is None:
            continue
        model, base_url, llm_type = route
        parts.append(f"{label} {model} ({place(base_url, llm_type) or 'at its configured provider'})")
    if not parts:
        return ""
    facts = "In this deployment, " + "; ".join(parts) + "."
    if all("internet" not in p and "configured provider" not in p for p in parts):
        facts += " None of these is a cloud service."
    return facts[0].upper() + facts[1:]


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
        "[System note: the user attached file(s) to the message below. "
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
