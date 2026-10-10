"""The two agent-only tools of the medical-knowledge capability.

`knowledge_tools()` builds them for the tiers this deployment has: the
offline reference when its index is installed, the literature and trials when
`KNOWLEDGE_ONLINE` is on. With neither there are no tools, and the prompt's
"Medical knowledge" section is not rendered.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Literal

from langchain_core.tools import BaseTool, StructuredTool
from pydantic import BaseModel, Field, create_model

from . import offline, online, refs

logger = logging.getLogger(__name__)

SEARCH = "search_medical_knowledge"
READ = "read_medical_source"
#: Calls per turn (`harness.standard_middleware`'s per-tool caps).
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


def knowledge_tools() -> list[BaseTool]:
    available = scopes()
    if not available:
        return []
    fields: dict = {"query": (str, Field(description="English medical terms, e.g. 'metformin side effects', "
                                                     "'high ALT'. Translate the question; never include the "
                                                     "person's name or values."))}
    if len(available) > 1:
        # One scope is no choice, and a one-value enum is a JSON-schema
        # `const` some providers' tool schemas reject.
        listed = "; ".join(f'"{s}": {SCOPES[s]}' for s in available)
        fields["scope"] = (Literal[tuple(available)], Field(default=available[0], description=listed))
    search_args = create_model("SearchMedicalKnowledge", **fields)
    search = StructuredTool.from_function(
        coroutine=_search, name=SEARCH, args_schema=search_args,
        description=("Search general medical knowledge: what a test measures and what its results mean, what a "
                     "condition is, what a medicine is for, its side effects and interactions. It never reads "
                     "the person's record. Returns passages, each with a ref to cite."),
    )
    read = StructuredTool.from_function(
        coroutine=_read, name=READ, args_schema=_ReadArgs,
        description="Read one passage from search_medical_knowledge in full. Pass its ref exactly as shown.",
    )
    return [search, read]


class _ReadArgs(BaseModel):
    ref: str = Field(description="A ref from a search result, e.g. ref:medlineplus_test:alt-blood-test")


async def _search(query: str, scope: str | None = None) -> str:
    # A tool is called with the arguments the model sent, not the schema's
    # defaults, so an omitted scope is chosen here.
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
        logger.warning("knowledge source unreachable: scope=%s", scope)
        return (f"The {scope} source could not be reached. This is an outage, not an absence of evidence: say "
                "so, and do not answer from memory.")
    if not rows:
        return (f"Nothing in {scope} matches. Retry once with fewer, broader English terms; if that is empty "
                "too, say the reference has nothing on it. Do not answer from memory.")
    blocks = [_passage(row) for row in rows]
    return (f"{len(rows)} passages ({SCOPES[scope]}).\n\n" + "\n\n".join(blocks) +
            f"\n\nUse only what these passages say, citing each fact by its ref: <cite>[{rows[0]['ref']}]</cite>. "
            f"{READ}(ref) shows one passage in full.")


async def _read(ref: str) -> str:
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
        logger.warning("knowledge source unreachable: source=%s", source)
        return "The source could not be reached. Cite the search excerpt instead, or say it is unavailable."
    if row is None:
        return f"{ref} is not in the sources. Search again and copy a ref from the results."
    return _passage(row)


def _passage(row: dict[str, str]) -> str:
    credit = _ATTRIBUTION.get(row["source"], refs.label(row["source"]) if row["source"] in refs.SOURCES else "")
    head = f"[{row['ref']}] {row['title']}"
    about = "; ".join(x for x in (row.get("about", ""), credit, row.get("url", "")) if x)
    return f"{head}\n({about})\n{row['text']}"
