"""The two medical-knowledge tools, as text an agent reads.

`search` and `read` back `search_medical_knowledge` and `read_medical_source`
(`agent/tools/knowledge_service.py`), which the chat agent and any signed-in
MCP client call alike. `scopes()` is what this deployment has: the offline
reference when its index is installed, the literature and trials when
`KNOWLEDGE_ONLINE` is on. With neither, the tools are not listed.
"""

from __future__ import annotations

import asyncio
import logging

from . import offline, online, refs

logger = logging.getLogger(__name__)

SEARCH = "search_medical_knowledge"
READ = "read_medical_source"
#: Calls per chat turn (`harness.standard_middleware`'s per-tool caps).
CALL_LIMITS = {SEARCH: 3, READ: 2}

SCOPES = {
    "reference": "MedlinePlus health topics and lab test pages, and FDA drug labels (on this server)",
    "literature": "PubMed reviews, meta-analyses, guidelines and trials, with abstracts (online, Europe PMC)",
    "trials": "registered clinical trials (online, ClinicalTrials.gov)",
}
_OFFLINE_SOURCES = ("medlineplus", "medlineplus_test", "openfda")
_ATTRIBUTION = {"medlineplus": "MedlinePlus, National Library of Medicine", "openfda": "FDA label via openFDA"}
_RESULTS = 5


def scopes() -> list[str]:
    available = ["reference"] if offline.available() else []
    return available + (["literature", "trials"] if online.enabled() else [])


async def search(query: str, scope: str = "") -> str:
    scope = scope or (scopes() or ["reference"])[0]
    try:
        if scope == "reference":
            if not offline.available():
                return "The offline medical reference is not installed on this server; say so."
            if not offline.match_terms(query):
                return "This reference is in English: search again with English medical terms, e.g. 'high ALT'."
            rows = await asyncio.to_thread(offline.search, query, limit=_RESULTS)
        elif scope in ("literature", "trials") and online.enabled():
            find = online.search_literature if scope == "literature" else online.search_trials
            rows = await find(query, _RESULTS)
        else:
            return f"The scope {scope!r} is not available on this server. Use one of: {', '.join(scopes())}."
    except online.Unavailable:
        logger.warning("knowledge source unreachable: scope=%s", scope)  # phi: ok one of three scope names
        return (f"The {scope} source could not be reached. This is an outage, not an absence of evidence: say "
                "so, and do not answer from memory.")
    if not rows:
        return (f"Nothing in {scope} matches. Retry once with fewer, broader English terms; if that is empty "
                "too, say the reference has nothing on it. Do not answer from memory.")
    blocks = [_passage(row) for row in rows]
    return (f"{len(rows)} passages ({SCOPES[scope]}).\n\n" + "\n\n".join(blocks) +
            f"\n\nUse only what these passages say, and give each fact its source by ref and link. "
            f"{READ}(ref) shows one passage in full.")


async def read(ref: str) -> str:
    parsed = refs.parse(ref)
    if parsed is None:
        return "That is not a ref a search returned. Copy one exactly, e.g. ref:medlineplus:6308."
    source, ident = parsed
    try:
        if source in _OFFLINE_SOURCES:
            row = await asyncio.to_thread(offline.read, ref.strip())
        elif online.enabled():
            row = await online.read(source, ident)
        else:
            return "Online sources are off on this server, so this ref cannot be read."
    except online.Unavailable:
        logger.warning("knowledge source unreachable: source=%s", source)  # phi: ok a refs.SOURCES key
        return "The source could not be reached. Cite the search excerpt instead, or say it is unavailable."
    if row is None:
        return f"{ref} is not in the sources. Search again and copy a ref from the results."
    return _passage(row)


def _passage(row: dict[str, str]) -> str:
    credit = _ATTRIBUTION.get(row["source"], refs.label(row["source"]) if row["source"] in refs.SOURCES else "")
    head = f"[{row['ref']}] {row['title']}"
    about = "; ".join(x for x in (row.get("about", ""), credit, row.get("url", "")) if x)
    return f"{head}\n({about})\n{row['text']}"
