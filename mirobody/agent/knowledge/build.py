"""`mirobody fetch knowledge`: the offline index, from public sources.

- MedlinePlus health topics (NLM, public domain): the newest daily XML, one
  passage per English topic, its "also called" names searchable beside the title.
- MedlinePlus lab test pages (public domain): what a test measures, what it
  is for and what its results mean, one passage per test, "other names"
  searchable. The topics have no page for most single tests (ALT, ferritin).
- FDA labels (openFDA, CC0): the most-labelled prescription generics, ranked
  by openFDA itself rather than by a list kept here, one passage per section
  of each generic's newest label, brand names searchable.

The build is the only step that touches the network. It writes a new file
and swaps it in, so a failed build leaves the previous index in place.
"""

from __future__ import annotations

import asyncio
import html
import io
import json
import os
import re
import sqlite3
import zipfile
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import quote
from xml.etree import ElementTree

from . import refs
from .offline import SCHEMA

MEDLINEPLUS_LISTING = "https://medlineplus.gov/xml.html"
MEDLINEPLUS_XML = "https://medlineplus.gov/xml/{name}"
LAB_TESTS = "https://medlineplus.gov/lab-tests/"
OPENFDA_LABEL = "https://api.fda.gov/drug/label.json"

#: The label sections indexed, with the words a person would search them by.
LABEL_SECTIONS = {
    "boxed_warning": "boxed warning",
    "indications_and_usage": "uses",
    "dosage_and_administration": "dosage and administration",
    "contraindications": "contraindications, who should not take it",
    "warnings_and_cautions": "warnings and precautions",
    "adverse_reactions": "side effects",
    "drug_interactions": "drug interactions",
}
#: Older labels file their warnings under this key instead.
_LEGACY_WARNINGS = "warnings"
#: A section is kept to this many characters; the rest is on DailyMed.
SECTION_CHARS = 6000
#: openFDA allows 240 requests a minute without a key; MedlinePlus asks for
#: no more than 85 a minute.
_OPENFDA_PACE_S = 0.3
_MEDLINEPLUS_PACE_S = 0.75
#: The questions of a lab test page that are indexed; the rest are about the
#: blood draw itself.
_TEST_SECTIONS = ("What is", "What are", "What is it used for", "What do the results mean", "Is there anything else")
_TEST_SECTION = re.compile(r'<section><div class="mp-content"><h2>(.*?)</h2>(.*?)</div>\s*</section>', re.S)
_TEST_LINK = re.compile(r'href="https://medlineplus\.gov/lab-tests/([a-z0-9]+(?:-[a-z0-9]+)*)/"')
_OTHER_NAMES = re.compile(r"^Other names?:\s*(.+)$", re.M)


class _Text(HTMLParser):
    """MedlinePlus summaries are HTML: paragraphs and bulleted lists."""

    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag == "li":
            self.parts.append("\n- ")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("p", "ul", "ol"):
            self.parts.append("\n\n")

    def handle_data(self, data: str) -> None:
        # A newline between two tags is the source's layout, not text.
        if data.strip() or "\n" not in data:
            self.parts.append(data)


def html_text(fragment: str) -> str:
    parser = _Text()
    parser.feed(fragment or "")
    text = html.unescape("".join(parser.parts))
    text = re.sub(r"[ \t]+", " ", text)
    return re.sub(r"\n\s*\n\s*", "\n\n", re.sub(r" *\n *", "\n", text)).strip()


def medlineplus_passages(xml: bytes) -> Iterator[dict[str, str]]:
    """One passage per English health topic."""
    for _, element in ElementTree.iterparse(io.BytesIO(xml), events=("end",)):
        if element.tag != "health-topic":
            continue
        if element.get("language") == "English":
            body = html_text(element.findtext("full-summary") or "")
            if body:
                also = [e.text.strip() for e in element.findall("also-called") if e.text]
                yield {"ref": refs.make("medlineplus", element.get("id", "")), "source": "medlineplus",
                       "url": element.get("url", ""), "title": element.get("title", ""),
                       "also": "; ".join(also), "body": body}
        element.clear()


def lab_test_passage(slug: str, page: str) -> dict[str, str] | None:
    """The indexed sections of one lab test page, or None when it has none."""
    title = html_text((re.search(r"<h1>(.*?)</h1>", page, re.S) or [None, ""])[1])
    parts = []
    for heading, content in _TEST_SECTION.findall(page):
        heading = html_text(heading)
        if heading.startswith(_TEST_SECTIONS):
            parts.append(f"{heading}\n{html_text(content)}")
    body = "\n\n".join(parts)
    if not title or not body:
        return None
    also = _OTHER_NAMES.search(body)
    body = _OTHER_NAMES.sub("", body).strip()
    return {"ref": refs.make("medlineplus_test", slug), "source": "medlineplus",
            "url": refs.page_url("medlineplus_test", slug), "title": title,
            "also": also.group(1).replace(", ", "; ") if also else "", "body": re.sub(r"\n{3,}", "\n\n", body)}


def label_passages(label: dict[str, Any]) -> Iterator[dict[str, str]]:
    """One passage per indexed section of an openFDA label record."""
    openfda = label.get("openfda") or {}
    set_id = str(label.get("set_id") or "")
    generic = ", ".join(openfda.get("generic_name") or []).strip()
    if not refs.SOURCES["openfda"][0].fullmatch(set_id) or not generic:
        return
    name = generic.capitalize()
    brands = "; ".join(dict.fromkeys(b.strip() for b in openfda.get("brand_name") or [] if b.strip()))
    url = refs.page_url("openfda", set_id)
    for key, words in LABEL_SECTIONS.items():
        text = label.get(key) or (label.get(_LEGACY_WARNINGS) if key == "warnings_and_cautions" else None)
        body = _clip(" ".join(text or []))
        if body:
            yield {"ref": refs.make("openfda", f"{set_id}#{key}"), "source": "openfda", "url": url,
                   "title": f"{name}: {words} (FDA label)", "also": brands, "body": body}


def _clip(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= SECTION_CHARS:
        return text
    cut = text.rfind(". ", 0, SECTION_CHARS)
    return text[: cut + 1 if cut > SECTION_CHARS // 2 else SECTION_CHARS] + " (continues on the full label)"


def write_index(path: Path, passages: Iterable[dict[str, str]], meta: dict[str, Any]) -> int:
    """Write the index to `path` atomically; returns how many passages."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    partial.unlink(missing_ok=True)
    con = sqlite3.connect(partial)
    try:
        try:
            con.executescript(SCHEMA)
        except sqlite3.OperationalError as e:
            raise RuntimeError(f"this Python's SQLite has no FTS5 ({e})") from e
        seen: set[str] = set()
        rows = []
        for p in passages:
            if p["ref"] not in seen:
                seen.add(p["ref"])
                rows.append((p["ref"], p["source"], p["url"], p["title"], p["also"], p["body"]))
        con.executemany("INSERT INTO passages (ref, source, url, title, also, body) VALUES (?, ?, ?, ?, ?, ?)", rows)
        con.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", [(k, str(v)) for k, v in meta.items()])
        con.execute("INSERT INTO passages(passages) VALUES ('optimize')")
        con.commit()
    finally:
        con.close()
    os.replace(partial, path)
    return len(rows)


async def _get(url: str, max_bytes: int) -> bytes:
    from mirobody.utils.net import fetch_bounded

    body, _ = await fetch_bounded(url, max_bytes=max_bytes, timeout_s=120)
    return body


async def fetch_medlineplus() -> tuple[str, bytes]:
    """The newest health-topics XML: its file name and bytes."""
    listing = (await _get(MEDLINEPLUS_LISTING, 2_000_000)).decode("utf-8", "replace")
    names = sorted(set(re.findall(r"mplus_topics_compressed_\d{4}-\d{2}-\d{2}\.zip", listing)))
    if not names:
        raise RuntimeError("the MedlinePlus XML page lists no health-topics file")
    archive = zipfile.ZipFile(io.BytesIO(await _get(MEDLINEPLUS_XML.format(name=names[-1]), 64_000_000)))
    member = next(n for n in archive.namelist() if n.endswith(".xml"))
    return member, archive.read(member)


async def fetch_lab_tests(progress: Callable[[str], None]) -> list[dict[str, str]]:
    index = (await _get(LAB_TESTS, 5_000_000)).decode("utf-8", "replace")
    slugs = list(dict.fromkeys(_TEST_LINK.findall(index)))
    passages = []
    for i, slug in enumerate(slugs, 1):
        try:
            page = (await _get(refs.page_url("medlineplus_test", slug), 2_000_000)).decode("utf-8", "replace")
            passage = lab_test_passage(slug, page)
            if passage:
                passages.append(passage)
        except Exception as e:
            progress(f"  skipped {slug}: {type(e).__name__}")
        if i % 50 == 0:
            progress(f"  {i}/{len(slugs)} pages")
        await asyncio.sleep(_MEDLINEPLUS_PACE_S)
    return passages


async def fetch_labels(count: int, progress: Callable[[str], None]) -> list[dict[str, Any]]:
    """The newest label of each of the `count` most-labelled prescription generics."""
    prescription = quote('openfda.product_type:"HUMAN PRESCRIPTION DRUG"')
    ranked = json.loads(await _get(
        f"{OPENFDA_LABEL}?search={prescription}&count=openfda.generic_name.exact&limit={int(count)}", 5_000_000))
    generics = [r["term"] for r in ranked.get("results", [])]
    labels: list[dict[str, Any]] = []
    for i, generic in enumerate(generics, 1):
        search = quote(f'openfda.generic_name.exact:"{generic}"')
        try:
            found = json.loads(await _get(f"{OPENFDA_LABEL}?search={search}&sort=effective_time:desc&limit=1",
                                          8_000_000))
            labels.extend(found.get("results", [])[:1])
        except Exception as e:
            progress(f"  skipped {generic}: {type(e).__name__}")
        if i % 50 == 0:
            progress(f"  {i}/{len(generics)} labels")
        await asyncio.sleep(_OPENFDA_PACE_S)
    return labels


async def _optional(fetch: Any, progress: Callable[[str], None]) -> list:
    """A source past the health topics: when it fails, the index is built
    without it rather than not at all."""
    try:
        return await fetch
    except Exception as e:
        progress(f"  unavailable ({type(e).__name__}); built without it")
        return []


async def build_index(path: Path, *, medicines: int = 200, progress: Callable[[str], None] = print) -> dict[str, Any]:
    progress("MedlinePlus health topics ...")
    member, xml = await fetch_medlineplus()
    topics = list(medlineplus_passages(xml))
    progress(f"  {len(topics)} English topics from {member}")
    progress("MedlinePlus lab test pages ...")
    tests = await _optional(fetch_lab_tests(progress), progress)
    progress(f"  {len(tests)} lab tests")
    progress(f"FDA labels of the {medicines} most-labelled prescription generics ...")
    labels = await _optional(fetch_labels(medicines, progress), progress) if medicines > 0 else []
    sections = [p for label in labels for p in label_passages(label)]
    progress(f"  {len(sections)} sections from {len(labels)} labels")
    meta = {"built_at": datetime.now(UTC).isoformat(timespec="seconds"), "medlineplus_file": member,
            "medlineplus_topics": len(topics), "medlineplus_tests": len(tests), "openfda_labels": len(labels)}
    return {**meta, "passages": write_index(path, [*topics, *tests, *sections], meta)}
