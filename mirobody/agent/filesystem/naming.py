"""Naming and typing inside the agent's virtual filesystem.

deepagents' ``ls`` returns paths and nothing else — no size, no date — so the
NAME is the only index the model gets, and a backend that projects stored
records into files has to answer the same three questions every time: how to
keep a name to one path segment (`safe_basename`), how two records with the same
name both stay visible (`resolve_names`: the later one is renamed, never
dropped — a listing that silently loses a file is a listing the model trusts
and is wrong), and — for a backend that serves EXTRACTED TEXT for binaries —
how to keep deepagents from wrapping that text as a file/image block
(`display_alias`: a ``.txt`` suffix, because the tool-result block type is
chosen from the requested path's suffix, and Anthropic rejects a file block in
a tool result with a 400 that ends the turn).

It also answers what KIND a file is — `guess_mime` (re-exported from the
engine layer) and `MULTIMODAL_EXTS` / `is_multimodal`, the extensions
deepagents serves as content blocks rather than as text.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath

from deepagents.backends.utils import _EXTENSION_TO_FILE_TYPE, _VIDEO_EXTRA_EXTENSIONS

# `guess_mime` lives in the engine layer, because object storage needs the
# same answers and cannot import the agent layer.
from ...utils.file_types import guess_mime as guess_mime

#: The suffixes deepagents treats as binary — derived from its own table, not
#: copied (a copy once missed ``.mkv``).
BINARY_SUFFIXES: tuple[str, ...] = tuple(sorted({*_EXTENSION_TO_FILE_TYPE, *_VIDEO_EXTRA_EXTENSIONS}))

_ALIAS_RE = re.compile(r"(" + "|".join(re.escape(s) for s in BINARY_SUFFIXES) + r")\.txt\b", re.IGNORECASE)


def safe_basename(name: str) -> str:
    """One path segment, no traversal: ``/`` and ``\\`` collapse to the last part."""
    base = PurePosixPath(str(name or "").replace("\\", "/")).name.strip()
    return base if base and base not in (".", "..") else ""


def display_alias(name: str) -> str:
    """The agent-facing name of a stored file whose CONTENT is served as text:
    binary-suffixed names get ``.txt`` appended, unconditionally by suffix, so a
    not-yet-extracted file reads back as an honest error rather than a raw-named
    read that would 400 the whole turn."""
    return name + ".txt" if name.lower().endswith(BINARY_SUFFIXES) else name


def strip_display_alias(text: str) -> str:
    """Undo `display_alias` wherever it appears in USER-FACING text (tool-call
    arguments, listings shown in a chat): ``report.pdf.txt`` → ``report.pdf``.
    Only a ``.txt`` directly after a known binary suffix is removed, so a real
    ``notes.txt`` upload is untouched."""
    return _ALIAS_RE.sub(r"\1", text or "")


def disambiguate(name: str, key: str) -> str:
    """The second name for a duplicate: ``report.pdf`` → ``report-1a2b3c4d.pdf``,
    tagged with the tail of the record's key so it is stable across listings."""
    tag = (str(key or "").rsplit("/", 1)[-1].rsplit(".", 1)[0] or "dup")[-8:]
    stem, dot, ext = name.rpartition(".")
    return f"{stem}-{tag}{dot}{ext}" if dot else f"{name}-{tag}"


def resolve_names(rows: list[dict], *, pinned: dict[str, str] | None = None, key: str = "file_key", name: str = "filename") -> list[dict]:
    """Fix each row's agent-facing filename; returns copies with ``name`` rewritten.

    ``pinned`` (key → name) wins over the stored name — the mount must answer to
    the paths the model was already told, and the stored column may be rewritten
    mid-turn. A repeated name is DISAMBIGUATED, not dropped: this view is
    addressed by name, so names must be unique, but dropping the later record
    made every same-named file but the newest invisible to the agent.
    """
    seen: set[str] = set()
    seen_keys: set[str] = set()
    out: list[dict] = []
    for row in rows:  # newest first
        record_key = str(row.get(key) or "")
        if record_key and record_key in seen_keys:
            continue  # the same record twice is one record
        filename = (pinned or {}).get(record_key) or row.get(name) or record_key
        if not filename:
            continue
        if filename in seen:
            filename = disambiguate(filename, record_key)
            if filename in seen:
                continue
        seen.add(filename)
        seen_keys.add(record_key)
        out.append({**row, name: filename})
    return out


#: Extensions deepagents surfaces as multimodal content blocks (stored as raw
#: bytes / object-storage offload, served as base64 — never as inline text).
#: Mirrors the harness virtual-filesystem docs:
#: https://docs.langchain.com/oss/python/deepagents/harness#virtual-filesystem-access
#: One copy: it used to live in both `parser.py` (upload-time classification)
#: and `backend.py` (read-time serving), held together by a "kept in sync"
#: comment, so an edit to one made storage disagree with serving.
MULTIMODAL_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".heic", ".heif",          # image
    ".mp4", ".mpeg", ".mov", ".avi", ".flv", ".mpg", ".webm", ".wmv", ".3gpp",  # video
    ".wav", ".mp3", ".aiff", ".aac", ".ogg", ".flac",                    # audio
    ".pdf", ".ppt", ".pptx",                                             # documents
}


def is_multimodal(filename: str) -> bool:
    return PurePosixPath(filename or "").suffix.lower() in MULTIMODAL_EXTS
