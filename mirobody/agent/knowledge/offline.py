"""The offline tier: a SQLite FTS5 index of MedlinePlus health topics and lab
test pages and FDA label sections, searched with no network at answer time.

`build.build_index` (`mirobody fetch knowledge`) writes it to `KNOWLEDGE_INDEX`,
default `~/.mirobody/knowledge/medref.sqlite3`. It is never in git or the
wheel; without it the tier is off and its tool is not offered.
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

DEFAULT_INDEX = Path.home() / ".mirobody" / "knowledge" / "medref.sqlite3"

#: How much of a passage a search shows; a read shows all of it.
EXCERPT_CHARS = 700

SCHEMA = """
CREATE VIRTUAL TABLE passages USING fts5(
    ref UNINDEXED, source UNINDEXED, url UNINDEXED, title, also, body,
    tokenize = 'porter unicode61'
);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""

#: bm25 weights in column order (ref, source, url, title, also, body): a word
#: in the title or an alternative name says more than a mention in the text.
_RANK = "bm25(passages, 0, 0, 0, 8.0, 4.0, 1.0)"
_WORD = re.compile(r"[A-Za-z0-9]+")
#: The words of a question that are not its subject ("what does a high ALT mean").
_STOP = frozenset(
    "a about an and are be can could did do does for how i in is it its me mean means meaning my of on or "
    "should that the this to was what whats when which who why will with would you your".split())


def index_path() -> Path:
    from mirobody.utils.config import safe_read_cfg

    configured = str(safe_read_cfg("KNOWLEDGE_INDEX") or "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_INDEX


def available(path: Path | None = None) -> bool:
    return (path or index_path()).is_file()


def _connect(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def match_terms(query: str) -> list[str]:
    """The query's subject words as FTS5 terms, prefix-matched from four
    letters ("statin" finds "statins"). Only words reach the parser, so FTS5
    syntax a model types (AND, quotes, colons) cannot break the query."""
    words = list(dict.fromkeys(w.lower() for w in _WORD.findall(query or "")))
    kept = [w for w in words if w not in _STOP] or words
    return [f'"{w}"*' if len(w) >= 4 else f'"{w}"' for w in kept]


def search(query: str, *, limit: int = 5, path: Path | None = None) -> list[dict[str, str]]:
    """The best passages for `query`, each with an excerpt of its text:
    those holding every term first, then those holding any."""
    terms = match_terms(query)
    if not terms:
        return []
    con = _connect(path or index_path())
    found: dict[str, tuple] = {}
    try:
        for op in ("AND", "OR")[: 2 if len(terms) > 1 else 1]:
            for row in con.execute(
                    f"SELECT ref, source, title, url, body FROM passages WHERE passages MATCH ? ORDER BY {_RANK} LIMIT ?",
                    (f" {op} ".join(terms), int(limit))):
                found.setdefault(row[0], row)
            if len(found) >= limit:
                break
    finally:
        con.close()
    rows = list(found.values())[:limit]
    return [{"ref": r[0], "source": r[1], "title": r[2], "url": r[3], "text": excerpt(r[4])} for r in rows]


def read(ref: str, *, path: Path | None = None) -> dict[str, str] | None:
    """One passage in full, or None when the index does not hold `ref`."""
    path = path or index_path()
    if not path.is_file():
        return None
    con = _connect(path)
    try:
        row = con.execute("SELECT ref, source, title, url, body FROM passages WHERE ref = ?", (ref,)).fetchone()
    finally:
        con.close()
    return {"ref": row[0], "source": row[1], "title": row[2], "url": row[3], "text": row[4]} if row else None


def meta(path: Path | None = None) -> dict[str, str]:
    path = path or index_path()
    if not path.is_file():
        return {}
    con = _connect(path)
    try:
        return dict(con.execute("SELECT key, value FROM meta").fetchall())
    finally:
        con.close()


def excerpt(body: str, limit: int = EXCERPT_CHARS) -> str:
    body = body or ""
    if len(body) <= limit:
        return body
    cut = body.rfind(". ", 0, limit)
    return body[: cut + 1 if cut > limit // 2 else limit] + " ..."
