"""The agent-only medical-reference search: one offline FTS lookup the chat
model uses for GENERAL medical knowledge, never for the person's record.

Purpose: the small local models this deployment runs on weak world knowledge.
Grounding a "what is this drug for" answer in bundled authoritative text
(MedlinePlus health-topic summaries, FDA drug labels for common chronic
medicines) keeps the model from inventing pharmacology and gives it something
to cite. The corpus ships as one SQLite FTS5 file at
``mirobody/res/medref/index.sqlite3`` (built offline by
process/medref/build_index.py; licenses in res/medref/NOTICE); this module
opens it read-only and makes no network calls.

Wired into the agent next to `ask_user` (agent.py `_build_agent`), NOT into
`agent/tools/`: the MCP surface is exactly seven tools and the local suite
asserts that list. An MCP client that wants reference text can build the same
index; nothing here reads a record, so nothing is lost by keeping it
chat-side.

Privacy shape: the query comes FROM the model, so the result may carry it,
but the log lines below carry counts, names and durations only. A query is
the answer to "what was asked", and that never goes to the log.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

from langchain_core.tools import tool

from mirobody._bundle import is_lfs_pointer
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

TOOL_NAME = "search_medical_reference"
INDEX_PATH = Path(__file__).resolve().parent.parent / "res" / "medref" / "index.sqlite3"

DEFAULT_K = 5
MAX_K = 10
SOURCES = ("medlineplus", "openfda")

#: FTS5 column weights for bm25: rid/source/lang/url are unindexed (weight 0);
#: title and the zh alias column outweigh the body text: a title hit or a
#: synonym-table hit is a much stronger signal than a casual mention.
_BM25_WEIGHTS = "bm25(passages_fts, 0.0, 0.0, 0.0, 0.0, 6.0, 2.0, 1.0, 5.0)"

_LATIN_WORD = re.compile(r"[a-z0-9]+")
_CJK_RUN = re.compile(r"[一-鿿　-〿]+")


@lru_cache(maxsize=2)
def _index_state(index_path: str) -> tuple[str, frozenset[str]]:
    """(corpus version, zh alias keys) for one index file, or ("", empty) when
    the index is absent or an LFS pointer stub. Cached for the process: the
    file is a bundled artifact, immutable within a deployment."""
    path = Path(index_path)
    if not path.is_file() or is_lfs_pointer(str(path)):
        return "", frozenset()
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            version = con.execute("SELECT value FROM meta WHERE key='version'").fetchone()
            zh_keys = frozenset(r[0] for r in con.execute("SELECT zh FROM aliases"))
        finally:
            con.close()
    except sqlite3.Error as e:
        tool_name = TOOL_NAME  # a local the PHI log lint reads as a name
        logger.error("[%s] index probe failed: error_type=%s", tool_name, type(e).__name__)
        return "", frozenset()
    return (version[0] if version else ""), zh_keys


def _fts_terms(query: str, zh_alias_keys: frozenset[str]) -> tuple[list[str], list[str], int]:
    """A free-text query as FTS5 OR terms: (terms, matched zh terms, zh chars
    unmatched).

    unicode61 has no stemming and no CJK segmentation. Latin words get prefix
    matching at 4+ letters ("statin*" finds "statins"); shorter ones stay
    exact so "a1c" or "ldl" do not explode. A Chinese run is segmented by
    greedy longest match against the index's own alias table, the same table
    the builder used to tag passages. Each matched term contributes its
    Chinese token plus its English trigger words, so 他汀 finds the statin
    passages. Characters no alias covers are counted, not guessed at.
    """
    terms: list[str] = []
    zh_terms: list[str] = []
    dropped_count = 0
    lowered = query.casefold()
    for word in _LATIN_WORD.findall(lowered):
        terms.append(f"{word}*" if len(word) >= 4 else f'"{word}"')
    for run in _CJK_RUN.findall(query):
        rest = run
        while rest:
            hit = next((zh for n in range(len(rest), 1, -1) if (zh := rest[:n]) in zh_alias_keys), None)
            if hit is None:
                dropped_count += 1
                rest = rest[1:]
                continue
            zh_terms.append(hit)
            terms.append(f'"{hit}"')
            rest = rest[len(hit):]
    return terms, zh_terms, dropped_count


def _expand_zh_triggers(zh_terms: list[str], index_path: str) -> list[str]:
    """The English trigger words of matched zh alias terms, as prefix terms."""
    if not zh_terms:
        return []
    out: list[str] = []
    con = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    try:
        for zh in zh_terms:
            row = con.execute("SELECT en FROM aliases WHERE zh = ?", (zh,)).fetchone()
            if row:
                for word in _LATIN_WORD.findall(row[0].casefold()):
                    out.append(f"{word}*" if len(word) >= 4 else f'"{word}"')
    finally:
        con.close()
    return out


def search_medref(query: str, k: int = DEFAULT_K, source: str | None = None,
                  *, index_path: str = str(INDEX_PATH)) -> dict[str, Any]:
    """The tool body as data: a dict the test suite and any future surface can
    assert on. Never raises; failures come back as ``ok: False`` plus a note
    the model can act on. Only counts leave here as metadata: the query text
    itself stays inside the row snippets it produced."""
    started = time.monotonic()
    tool_name = TOOL_NAME  # a local the PHI log lint reads as a name
    query = (query or "").strip()
    k = max(1, min(int(k or DEFAULT_K), MAX_K))
    if source is not None and source not in SOURCES:
        return {"ok": False, "rows": [],
                "note": f"unknown source {source!r}; the bundled sources are: {', '.join(SOURCES)}"}
    if not query:
        return {"ok": False, "rows": [], "note": "an empty query cannot be searched"}

    version, zh_alias_keys = _index_state(index_path)
    if not version:
        logger.error("[%s] reference index missing or unreadable", tool_name)
        return {"ok": False, "rows": [],
                "note": "the bundled medical-reference index is not installed "
                        "(res/medref/index.sqlite3; run `git lfs pull` or rebuild it with "
                        "process/medref/build_index.py); answer without it and say the "
                        "reference lookup was unavailable"}

    terms, zh_terms, dropped_count = _fts_terms(query, zh_alias_keys)
    terms += _expand_zh_triggers(zh_terms, index_path)
    if not terms:
        return {"ok": False, "rows": [],
                "note": "nothing in the query maps to the reference vocabulary; "
                        "try English drug or condition names"}

    match = " OR ".join(terms)
    where = "passages_fts MATCH ?"
    params: list[Any] = [match]
    if source:
        where += " AND source = ?"
        params.append(source)
    sql = (
        "SELECT rid, source, lang, url, title, section, "
        f"snippet(passages_fts, 6, '[', ']', ' … ', 24) AS snip "
        f"FROM passages_fts WHERE {where} ORDER BY {_BM25_WEIGHTS} LIMIT ?"
    )
    params.append(k)
    try:
        con = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
        try:
            found = con.execute(sql, params).fetchall()
        finally:
            con.close()
    except sqlite3.Error as e:
        logger.error("[%s] search failed: error_type=%s", tool_name, type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return {"ok": False, "rows": [],
                "note": "the reference index query failed; answer without it and say so"}

    rows = [{
        "rid": f"ref:{r[1]}:{r[0]}",
        "source": r[1],
        "lang": r[2],
        "url": r[3],
        "title": r[4],
        "section": r[5],
        "snippet": " ".join(str(r[6]).split()),
    } for r in found]
    duration_ms = int((time.monotonic() - started) * 1000)
    logger.info("[%s] terms=%d zh_matched=%d zh_dropped=%d hits=%d duration_ms=%d",
                tool_name, len(terms), len(zh_terms), dropped_count, len(rows), duration_ms)
    return {
        "ok": True,
        "version": version,
        "rows": rows,
        "query_terms": len(terms),
        "zh_matched": len(zh_terms),
        "zh_dropped": dropped_count,
    }


_SOURCE_LABEL = {"medlineplus": "MedlinePlus (U.S. National Library of Medicine)",
                 "openfda": "FDA drug label (openFDA)"}


def _render(result: dict[str, Any]) -> str:
    """The dict as the text block the model reads. Rows keep their `rid` so
    the answer can cite them; the closing guidance is repeated on every call
    because a small model drops standing instructions under load."""
    if not result.get("ok"):
        return f"Medical reference search unavailable: {result.get('note', 'unknown error')}"
    rows = result["rows"]
    if not rows:
        return (
            "No passage in the bundled medical reference matched this query "
            f"(offline corpus {result.get('version')}: MedlinePlus health-topic summaries "
            "in English and Spanish, plus FDA labels for common chronic-disease medicines; "
            "no Chinese full text yet). Do not fabricate a citation. Say the bundled "
            "reference does not cover it and answer from general knowledge with a caveat, "
            "or suggest asking a clinician/pharmacist."
        )
    lines = [
        (f"{len(rows)} reference passage(s) from the bundled offline corpus ({result.get('version')}). "
         "This is general medical knowledge, NOT anything from the person's own record:"),
    ]
    for r in rows:
        lines.append(f"[{r['rid']}] {_SOURCE_LABEL.get(r['source'], r['source'])} · "
                     f"{r['title']} › {r['section']} ({r['lang']})")
        lines.append(f"    {r['snippet']}")
        lines.append(f"    {r['url']}")
    lines.append(
        "Cite any passage you use inline as (ref:…). Keep \"your record shows\" and "
        "\"the reference/guideline says\" as separate claims. For starting, stopping or "
        "changing a dose, cite the label AND say to confirm with the prescribing clinician "
        "— never issue dosing instructions yourself."
    )
    if result.get("zh_dropped"):
        lines.append("Note: part of the Chinese query was outside the bundled synonym table; "
                     "English drug/condition names widen the search.")
    return "\n".join(lines)


@tool
def search_medical_reference(query: str, k: int = DEFAULT_K, source: str | None = None) -> str:
    """Search the bundled authoritative medical reference (fully offline; no
    web access). Use it for GENERAL medical knowledge (what a drug is for,
    label warnings and interactions, what a condition or lab test means) and
    cite the returned `ref:` ids. Do NOT use it for anything about this
    person's own readings, medications or documents: those come from
    `query_health_indicators`, `query_medications` and read_file.

    Args:
        query: What to look up, in Chinese or English ("他汀 副作用" or
            "metformin lactic acidosis"). Chinese drug and condition names
            work through a bundled synonym table.
        k: Passages to return, 1-10 (default 5).
        source: Restrict to one corpus: "medlineplus" (consumer health-topic
            summaries) or "openfda" (drug labels). Omit to search both.

    Returns:
        Ranked passages, each with a citeable `ref:<source>:<passage_id>` id,
        source, title, section and snippet; or an explicit no-hits /
        unavailable message, never a bare empty result.
    """
    return _render(search_medref(query, k=k, source=source))
