"""The ids a knowledge passage is cited by, and the page each one opens.

A ref is `ref:<source>:<id>`, the reference kind of `kernel.citations`. Each
source has one id grammar; a ref outside it is refused before any lookup or
connection, so a model cannot steer a read to a URL of its choosing.
"""

from __future__ import annotations

import re
from urllib.parse import quote

#: source -> (id grammar, page URL template, label a reader sees).
SOURCES: dict[str, tuple[re.Pattern[str], str, str]] = {
    "medlineplus": (re.compile(r"\d{1,7}"), "", "MedlinePlus"),
    "medlineplus_test": (re.compile(r"[a-z0-9]+(-[a-z0-9]+)*"), "https://medlineplus.gov/lab-tests/{id}/", "MedlinePlus"),
    "openfda": (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(#[a-z_]+)?"),
                "https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid={id}", "FDA label"),
    "pmid": (re.compile(r"\d{1,9}"), "https://pubmed.ncbi.nlm.nih.gov/{id}/", "PubMed"),
    "pmc": (re.compile(r"PMC\d{3,9}"), "https://pmc.ncbi.nlm.nih.gov/articles/{id}/", "PubMed Central"),
    "doi": (re.compile(r"10\.\d{4,9}/[^\s\]]+"), "https://doi.org/{id}", "DOI"),
    "nct": (re.compile(r"NCT\d{8}"), "https://clinicaltrials.gov/study/{id}", "ClinicalTrials.gov"),
}

#: Where the online tier connects. Nothing else is ever fetched at answer time.
ONLINE_HOSTS = ("www.ebi.ac.uk", "clinicaltrials.gov")


def make(source: str, ident: str) -> str:
    return f"ref:{source}:{ident}"


def parse(ref: str) -> tuple[str, str] | None:
    """`(source, id)` of a well-formed ref, else None."""
    if not isinstance(ref, str):
        return None
    parts = ref.strip().split(":", 2)
    if len(parts) != 3 or parts[0] != "ref" or parts[1] not in SOURCES:
        return None
    source, ident = parts[1], parts[2]
    return (source, ident) if SOURCES[source][0].fullmatch(ident) else None


def page_url(source: str, ident: str) -> str:
    """The public page of a ref, or "" when it needs the index (MedlinePlus
    topic pages are named by slug, not by id)."""
    template = SOURCES[source][1]
    if not template:
        return ""
    return template.format(id=quote(ident.split("#", 1)[0], safe="/.:"))


def label(source: str) -> str:
    return SOURCES[source][2]


def resolve(ref: str, passage: dict[str, str] | None = None) -> dict[str, str]:
    """What a cited ref is, for a reader: its source, title and page. No
    network: the title comes from the offline index (`passage`) when the ref
    is there, and an online ref shows its source and id."""
    parsed = parse(ref)
    if parsed is None:
        return {"rid": ref, "status": "unknown"}
    source, ident = parsed
    url = (passage or {}).get("url") or page_url(source, ident)
    if not url:
        return {"rid": ref, "status": "unknown"}
    title = (passage or {}).get("title") or f"{label(source)} {ident.split('#', 1)[0]}"
    return {"rid": ref, "status": "ok", "kind": "reference", "source": label(source), "title": title, "url": url}
