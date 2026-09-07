"""Long-term memory as a document the agent reads, never one it writes.

The shape production converged on: a background pass *extracts* facts from
what the person says and uploads; a slower pass *rewrites* the profile
document from those facts; the agent sees the rewritten document plus the
facts learned since the last rewrite, injected raw — knowing something must
not wait on tidying it. The person's own edits live in a second document the
rewrite never touches.

Two invariants are enforced by signature rather than by review:

* :func:`rewrite_input` builds the material for a rewrite from the fact
  stream and the person's edits — it has **no parameter for the previous
  system document**, so a rewrite cannot feed its own output back to itself
  and drift (the bootstrapping failure).
* :func:`render_profile` bounds the raw fresh facts; hitting the bound is a
  signal that the rewrite job is behind, reported as a flag, not silently
  truncated.

Pure; stdlib only. Storage and the file-system projection an agent harness
mounts are the consumer's (the reference implementation lives in
``mirobody.agent``).
"""

from __future__ import annotations

import fnmatch
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

#: Stamped into the system document as an HTML comment, so it never reaches
#: the model's eyes but tells the delta query where "since" starts. The token
#: is `mirobody:rewritten-at=` — the spelling the first production consumer's
#: stored documents already carry; a reader accepts the underscore variant an
#: earlier draft of this module wrote, so no document is ever "never rewritten"
#: because of a hyphen.
MARKER_TOKEN = "mirobody:rewritten-at="
MARKER_PREFIX = "<!-- " + MARKER_TOKEN
_MARKER_RE = re.compile(r"<!--\s*mirobody:rewritten[-_]at=([0-9T:.+\-Z]+)\s*-->")
#: The token without its comment — what is left when a model echoed the
#: watermark and a cleaner already removed the ``<!-- -->`` delimiters.
_BARE_MARKER_RE = re.compile(r"mirobody:rewritten[-_]at=[0-9T:.+\-Z]*")


@dataclass(frozen=True)
class ExtractedFact:
    """One thing learned, as a sentence, with where and when it came from."""

    text: str
    observed_at_ms: int
    source_record_id: str = ""
    kind: str = ""  # the consumer's own taxonomy (condition, allergy, preference, …)


@dataclass(frozen=True)
class ProfileDoc:
    """The two documents: what the rewrite produced and what the person wrote."""

    system: str = ""
    edit: str = ""


def strip_watermark(text: str) -> str:
    """``text`` without any watermark: the full comment, and a bare token a
    model echoed into its own answer. The watermark is the store's to write —
    one a model wrote would corrupt the schedule of every later rewrite."""
    return _BARE_MARKER_RE.sub("", _MARKER_RE.sub("", text or ""))


def stamp(doc: str, at: datetime) -> str:
    """``doc`` with the rewrite watermark replaced or appended."""
    iso = at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    marker = f"{MARKER_PREFIX}{iso} -->"
    stripped = strip_watermark(doc).rstrip()
    return f"{stripped}\n\n{marker}\n" if stripped else f"{marker}\n"


def rewritten_at(doc: str) -> datetime | None:
    """When the system document was last regenerated, or ``None`` (never —
    which means every stored fact is still un-reconciled)."""
    m = _MARKER_RE.search(doc or "")
    if not m:
        return None
    text = m.group(1).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def fresh_facts(facts: Sequence[ExtractedFact], since: datetime | None) -> tuple[ExtractedFact, ...]:
    """Facts observed after the last rewrite, newest first."""
    cutoff = int(since.timestamp() * 1000) if since else -1
    return tuple(sorted((f for f in facts if f.observed_at_ms > cutoff), key=lambda f: -f.observed_at_ms))


@dataclass(frozen=True)
class Rendered:
    text: str
    fresh_count: int
    rewrite_behind: bool  # the fresh-fact bound was hit: the rewrite job is falling behind


def render_profile(doc: ProfileDoc, fresh: Sequence[ExtractedFact], *, max_fresh: int, basics: str = "") -> Rendered:
    """The one rendering of a profile — for the system prompt and for the
    agent's own ``read_file`` alike, so the two can never disagree.
    ``basics`` is an already-safe demographics line (an age, never a birth
    date). Empty everything renders an empty string, which a harness treats
    as "nothing to mount"."""
    if max_fresh < 1:
        raise ValueError("max_fresh must be >= 1")
    system = strip_watermark(doc.system or "").strip()
    edit = (doc.edit or "").strip()
    shown = list(fresh[:max_fresh])
    if not system and not edit and not shown and not basics:
        return Rendered("", 0, False)
    parts: list[str] = []
    if basics:
        parts.append(basics.strip())
    if system:
        parts.append(system)
    if edit:
        parts.append("## Notes written by the user\n\n" + edit)
    if shown:
        bullets = "\n".join(f"- {f.text.strip()}" for f in shown if f.text.strip())
        parts.append("## Learned recently (not yet folded into the profile)\n\n" + bullets)
    return Rendered("\n\n".join(parts).strip() + "\n", len(shown), len(fresh) > max_fresh)


@dataclass(frozen=True)
class RewriteInput:
    """Everything a rewrite may look at. There is no field for the previous
    system document on purpose."""

    facts: tuple[ExtractedFact, ...]
    edit: str
    as_of: datetime


def rewrite_input(facts: Sequence[ExtractedFact], *, edit: str, as_of: datetime) -> RewriteInput:
    """The material for a full regeneration: every stored fact (oldest
    first, de-duplicated by text) and the person's own notes. The previous
    generated document is not an input — a rewrite that reads its own output
    accumulates its own errors."""
    seen: set[str] = set()
    kept: list[ExtractedFact] = []
    for f in sorted(facts, key=lambda f: f.observed_at_ms):
        key = " ".join(f.text.split()).casefold()
        if key and key not in seen:
            seen.add(key)
            kept.append(f)
    return RewriteInput(tuple(kept), (edit or "").strip(), as_of)


# --- projection helpers for a read-only file view ------------------------------------


def slice_lines(text: str, offset: int, limit: int) -> str:
    """Lines ``offset..offset+limit`` of ``text`` — the ``read_file`` window
    over a projected document."""
    lines = text.splitlines()
    if offset >= len(lines):
        return f"(end of document — {len(lines)} line(s) total.)"
    return "\n".join(lines[offset : offset + max(1, limit)])


def grep_lines(text: str, pattern: str, *, max_line: int = 500) -> tuple[tuple[int, str], ...]:
    """``(line_number, line)`` for every line containing ``pattern``
    (literal, case-sensitive) — the ``grep`` over a projected document."""
    return tuple((i, line[:max_line]) for i, line in enumerate(text.splitlines(), start=1) if pattern in line)


def glob_match(name: str, pattern: str) -> bool:
    """Whether a projected file ``name`` matches a glob given with or without
    a leading slash."""
    pat = (pattern or "").lstrip("/")
    return fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(f"/{name}", pattern or "")


__all__ = [
    "MARKER_PREFIX",
    "ExtractedFact",
    "ProfileDoc",
    "Rendered",
    "RewriteInput",
    "fresh_facts",
    "glob_match",
    "grep_lines",
    "render_profile",
    "rewrite_input",
    "rewritten_at",
    "slice_lines",
    "stamp",
]
