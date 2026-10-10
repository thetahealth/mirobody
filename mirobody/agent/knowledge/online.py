"""The online tier: PubMed records through Europe PMC, and ClinicalTrials.gov.

Off unless `KNOWLEDGE_ONLINE` is true. A search sends its words to one of
`refs.ONLINE_HOSTS` and nothing else; the record never leaves the server.
A source that cannot be reached raises `Unavailable`, which the tool reports
as an outage: an empty list would read as "there is no evidence".
"""

from __future__ import annotations

import html
import json
import re
from typing import Any
from urllib.parse import urlencode, urlsplit

from . import refs
from .offline import excerpt

EUROPE_PMC = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
TRIALS = "https://clinicaltrials.gov/api/v2/studies"

#: Literature search keeps to PubMed records with an abstract, of the kinds
#: that summarize evidence. Unfiltered, editorials ranked first.
_EVIDENCE = ('SRC:MED AND HAS_ABSTRACT:y AND (PUB_TYPE:"review" OR PUB_TYPE:"systematic-review" '
             'OR PUB_TYPE:"meta-analysis" OR PUB_TYPE:"practice guideline" OR PUB_TYPE:"guideline" '
             'OR PUB_TYPE:"randomized controlled trial")')
_QUERY_CHARS = re.compile(r"[^\w\s\-'.]")
#: Europe PMC marks abstracts up and escapes some titles twice
#: ("D&lt;sub&gt;3"), so the markup is removed on both sides of unescaping,
#: by tag name: a bare "<" is a value ("P<0.001").
_TAG = re.compile(r"</?(?:sub|sup|i|b|u|em|strong|h[1-6]|p|br|span|div|inf)\b[^>]*>", re.I)
#: A read shows this much of an abstract or a trial record.
READ_CHARS = 6000


class Unavailable(Exception):
    """The source did not answer."""


def enabled() -> bool:
    from mirobody.utils.config import safe_read_cfg

    return safe_read_cfg("KNOWLEDGE_ONLINE").upper() in ("TRUE", "1", "YES", "ON")


def _clean_query(query: str) -> str:
    return " ".join(_QUERY_CHARS.sub(" ", query or "").split())[:200]


def _text(value: Any) -> str:
    return " ".join(_TAG.sub("", html.unescape(_TAG.sub(" ", str(value or "")))).split())


async def _get_json(url: str) -> dict[str, Any] | None:
    """The JSON at `url`, None on a 404 (no such record)."""
    from mirobody.utils.net import fetch_bounded

    if urlsplit(url).hostname not in refs.ONLINE_HOSTS:
        raise Unavailable(f"not an online knowledge host: {urlsplit(url).hostname}")
    try:
        body, _ = await fetch_bounded(url, max_bytes=4_000_000, timeout_s=20)
        return json.loads(body)
    except Exception as e:
        if getattr(e, "status", None) == 404:
            return None
        raise Unavailable(type(e).__name__) from e


async def _europe_pmc(query: str, page_size: int) -> list[dict[str, Any]]:
    params = {"query": query, "format": "json", "pageSize": page_size, "resultType": "core"}
    found = await _get_json(f"{EUROPE_PMC}?{urlencode(params)}") or {}
    return (found.get("resultList") or {}).get("result") or []


def _article(record: dict[str, Any], chars: int) -> dict[str, str] | None:
    pmid, pmcid, doi = (str(record.get(k) or "").strip() for k in ("pmid", "pmcid", "doi"))
    source, ident = ("pmid", pmid) if pmid else ("pmc", pmcid) if pmcid else ("doi", doi)
    if not ident or not refs.SOURCES[source][0].fullmatch(ident):
        return None
    journal = ((record.get("journalInfo") or {}).get("journal") or {}).get("title") or ""
    kinds = [k for k in (record.get("pubTypeList") or {}).get("pubType") or [] if k[:1].isupper()]
    about = ", ".join(x for x in (journal, str(record.get("pubYear") or ""), "; ".join(kinds[:3])) if x)
    return {"ref": refs.make(source, ident), "source": source, "title": _text(record.get("title")),
            "url": refs.page_url(source, ident), "about": about,
            "text": excerpt(_text(record.get("abstractText")), chars)}


async def search_literature(query: str, limit: int = 5) -> list[dict[str, str]]:
    words = _clean_query(query)
    if not words:
        return []
    records = await _europe_pmc(f"({words}) AND {_EVIDENCE}", limit)
    return [a for a in (_article(r, 700) for r in records) if a]


def _trial(study: dict[str, Any], full: bool) -> dict[str, str] | None:
    section = study.get("protocolSection") or {}
    nct = (section.get("identificationModule") or {}).get("nctId") or ""
    if not refs.SOURCES["nct"][0].fullmatch(nct):
        return None
    status = section.get("statusModule") or {}
    phases = ", ".join((section.get("designModule") or {}).get("phases") or [])
    conditions = ", ".join((section.get("conditionsModule") or {}).get("conditions") or [])
    about = "; ".join(x for x in (status.get("overallStatus") or "", phases, conditions) if x)
    summary = _text((section.get("descriptionModule") or {}).get("briefSummary"))
    text = summary
    if full:
        arms = (section.get("armsInterventionsModule") or {}).get("interventions") or []
        eligibility = (section.get("eligibilityModule") or {}).get("eligibilityCriteria") or ""
        lines = [("Interventions", ", ".join(a.get("name", "") for a in arms if a.get("name"))),
                 ("Started", (status.get("startDateStruct") or {}).get("date") or ""),
                 ("Summary", summary), ("Eligibility", " ".join(eligibility.split()))]
        text = "\n".join(f"{k}: {v}" for k, v in lines if v)
    return {"ref": refs.make("nct", nct), "source": "nct", "title": _text(section["identificationModule"].get("briefTitle")),
            "url": refs.page_url("nct", nct), "about": about, "text": excerpt(text, READ_CHARS if full else 700)}


async def search_trials(query: str, limit: int = 5) -> list[dict[str, str]]:
    words = _clean_query(query)
    if not words:
        return []
    found = await _get_json(f"{TRIALS}?{urlencode({'query.term': words, 'pageSize': limit, 'format': 'json'})}") or {}
    return [t for t in (_trial(s, full=False) for s in found.get("studies") or []) if t]


async def read(source: str, ident: str) -> dict[str, str] | None:
    """One online record in full, or None when the source has no such record."""
    if source == "nct":
        study = await _get_json(f"{TRIALS}/{ident}?format=json")
        return _trial(study, full=True) if study else None
    field = {"pmid": "EXT_ID:{} AND SRC:MED", "pmc": "PMCID:{}", "doi": 'DOI:"{}"'}[source]
    records = await _europe_pmc(field.format(ident), 1)
    return _article(records[0], READ_CHARS) if records else None
