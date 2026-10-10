#!/usr/bin/env python3
"""Build the offline medical-reference FTS index consumed by the agent's
`search_medical_reference` tool (mirobody/agent/medref.py).

Two bundled corpora, both verified redistributable (see README.md here):

- MedlinePlus health-topic XML dump (NLM/NIH): the `full-summary` text of
  every English and Spanish health topic. Public domain with an attribution
  requirement — the NOTICE that ships beside the index carries it. The A.D.A.M.
  encyclopedia articles and ASHP drug monographs on the MedlinePlus site are
  copyrighted and are NOT in this XML (the dump holds summaries only); do not
  scrape them in.
- openFDA drug labels for the curated generics of fetch_openfda.py
  (CC0 1.0). One label per generic: chronic-disease primary care is the
  retrieval workload, and the full bulk dump would dwarf the repository.

Output: one SQLite file with a single FTS5 table (no external service, no
triggers, nothing running but sqlite itself):

    meta(key TEXT PRIMARY KEY, value TEXT)       -- corpus version + counts
    aliases(zh TEXT PRIMARY KEY, en TEXT)        -- zh_aliases.tsv, verbatim;
                                                  -- the runtime reads query
                                                  -- expansion from here so the
                                                  -- two sides cannot drift
    passages_fts USING fts5(
        rid UNINDEXED,       -- stable id: "<doc>:<section>:<n>"
        source UNINDEXED,    -- medlineplus | openfda
        lang UNINDEXED,      -- en | es (MedlinePlus ships no Chinese)
        url UNINDEXED,       -- citation link for the model
        title, section, text, zh,
        tokenize='unicode61 remove_diacritics 2')

The output path (mirobody/res/medref/index.sqlite3) and the raw downloads
under scripts/medref/raw/ are gitignored build products: a deployment builds
the index on site rather than cloning it.

unicode61 because it is sqlite's built-in: no extension to load on any
runtime. It does no CJK segmentation — a Chinese sentence is one token — so
Chinese retrieval goes through the `zh` column: at build time each passage
that mentions a trigger term of zh_aliases.tsv gets the matching Chinese
terms added there; at query time a Chinese query is segmented against that
same table and expanded with its English triggers. Exact zh term tokens then
match on both sides.

The read-side column layout is asserted by mirobody/tests/test_medref_reference.py.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from pathlib import Path

logger = logging.getLogger("build_index")

HERE = Path(__file__).resolve().parent
DEFAULT_RAW = HERE / "raw"
DEFAULT_OUT = HERE.parents[1] / "mirobody" / "res" / "medref" / "index.sqlite3"
ALIASES_TSV = HERE / "zh_aliases.tsv"

DDL_META = "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)"
DDL_ALIASES = "CREATE TABLE aliases(zh TEXT PRIMARY KEY, en TEXT)"
DDL_FTS = """CREATE VIRTUAL TABLE passages_fts USING fts5(
    rid UNINDEXED, source UNINDEXED, lang UNINDEXED, url UNINDEXED,
    title, section, text, zh,
    tokenize='unicode61 remove_diacritics 2')"""

#: A paragraph below this length is navigational boilerplate ("What is X?"),
#: not a passage; above the cap a block is split at sentence ends.
MIN_PASSAGE = 30
MAX_PASSAGE = 1400
MAX_PER_LABEL = 40
MAX_ZH_PER_PASSAGE = 12

_TAG = re.compile(r"<[^>]+>")
_SUMMARY_BLOCK = re.compile(r"<(?:p|li)[^>]*>(.*?)</(?:p|li)>", re.S)
_SENTENCE_END = re.compile(r"(?<=[.;:!?])\s+")


def load_aliases(path: Path) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        zh, _, en = line.partition("\t")
        zh, en = zh.strip(), en.strip()
        if zh and en:
            out[zh] = [t.strip() for t in en.split("|") if t.strip()]
    return out


def zh_terms_for(haystack: str, aliases: dict[str, list[str]]) -> str:
    """The Chinese alias terms whose English triggers appear in `haystack`,
    space-joined (the `zh` FTS column of one passage)."""
    low = haystack.casefold()
    hits = [zh for zh, triggers in aliases.items()
            if any(t.casefold() in low for t in triggers)]
    return " ".join(sorted(hits)[:MAX_ZH_PER_PASSAGE])


def _strip_tags(fragment: str) -> str:
    text = _TAG.sub(" ", fragment)
    return " ".join(html.unescape(text).split())


def _chunks(text: str) -> list[str]:
    """Text to passages: each ≤ MAX_PASSAGE, split at sentence ends; pieces
    shorter than MIN_PASSAGE join their predecessor instead of standing alone
    as a title-sized fragment."""
    out: list[str] = []
    for piece in _SENTENCE_END.split(text):
        if out and len(out[-1]) + len(piece) < MAX_PASSAGE and len(out[-1]) < MIN_PASSAGE * 4:
            out[-1] = out[-1] + " " + piece
        elif len(piece) > MAX_PASSAGE:
            for i in range(0, len(piece), MAX_PASSAGE):
                out.append(piece[i:i + MAX_PASSAGE])
        else:
            out.append(piece)
    return [p for p in out if len(p) >= MIN_PASSAGE]


def medlineplus_passages(xml_path: Path, aliases: dict[str, list[str]]):
    """Yield (rid, source, lang, url, title, section, text, zh) rows from the
    health-topic dump. `full-summary` is the public-domain part (see module
    docstring); `also-called` and the MeSH descriptor only feed search recall
    via the title column and the alias haystack, never the passage text."""
    date_created = ""
    n_topics = 0
    for event, elem in ET.iterparse(xml_path, events=("end",)):
        if elem.tag != "health-topic":
            continue
        topic_id = elem.get("id", "")
        lang = {"english": "en", "spanish": "es"}.get((elem.get("language") or "").lower(),
                                                      (elem.get("language") or "en")[:2].lower())
        title = elem.get("title", "")
        url = elem.get("url", "")
        also_called = [el.text or "" for el in elem.findall("also-called")]
        summary_html = ""
        for el in elem.findall("full-summary"):
            summary_html += (el.text or "")
        blocks = [_strip_tags(b) for b in _SUMMARY_BLOCK.findall(html.unescape(summary_html))]
        blocks = [b for b in blocks if b]
        text_all = " ".join(blocks)
        # Alias matching is per TOPIC: every passage of a topic that concerns
        # 高血压 carries the term, so a zh query can land on any of them.
        haystack = " ".join([title, *also_called, text_all])
        zh = zh_terms_for(haystack, aliases)
        n = 0
        for block in blocks:
            for chunk in _chunks(block):
                yield (f"{topic_id}:summary:{n}", "medlineplus", lang, url, title, "Summary", chunk, zh)
                n += 1
        n_topics += 1
        if not date_created:
            date_created = elem.get("date-created", "")
        elem.clear()
    logger.info("medlineplus: %d topics", n_topics)


OPENFDA_SECTIONS: tuple[tuple[str, str], ...] = (
    ("boxed_warning", "Boxed warning"),
    ("indications_and_usage", "Indications and usage"),
    ("dosage_and_administration", "Dosage and administration"),
    ("warnings_and_cautions", "Warnings and precautions"),
    ("warnings", "Warnings"),
    ("contraindications", "Contraindications"),
    ("adverse_reactions", "Adverse reactions"),
    ("drug_interactions", "Drug interactions"),
    ("use_in_specific_populations", "Use in specific populations"),
    ("pregnancy", "Pregnancy"),
    ("geriatric_use", "Geriatric use"),
    ("pediatric_use", "Pediatric use"),
    ("overdosage", "Overdosage"),
    ("information_for_patients", "Patient information"),
)


def openfda_passages(openfda_dir: Path, aliases: dict[str, list[str]]):
    """Yield rows from the fetched label JSONs; one label record per generic
    file (fetch_openfda.py --limit 1). Both `warnings` spellings exist in the
    wild (boxed_warning is separate); the first that is present wins."""
    n_labels = 0
    seen_set_ids: set[str] = set()
    for path in sorted(openfda_dir.glob("*.json")):
        if path.name.startswith("_"):
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        results = payload.get("results") or []
        if not results:
            continue
        label = results[0]
        # Two queries can land the same SPL set ("metformin" and "sitagliptin"
        # both once fetched the combination label): passage rids key on it, so
        # the second copy would double every row.
        if label.get("set_id") in seen_set_ids:
            continue
        seen_set_ids.add(label.get("set_id"))
        queried = (payload.get("_mirobody_query") or {}).get("generic", path.stem)
        of = label.get("openfda") or {}
        generic = (of.get("generic_name") or [queried])[0]
        brands = ", ".join(of.get("brand_name") or [])
        title = f"{generic} ({brands})" if brands else generic
        set_id = label.get("set_id", path.stem)
        url = f"https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid={set_id}"
        zh = zh_terms_for(" ".join([title, queried]), aliases)
        made = 0
        for key, section in OPENFDA_SECTIONS:
            if made >= MAX_PER_LABEL:
                break
            values = label.get(key) or []
            text = _strip_tags(" ".join(str(v) for v in values))
            n = 0
            for chunk in _chunks(text):
                if made >= MAX_PER_LABEL:
                    break
                yield (f"{set_id}:{key}:{n}", "openfda", "en", url, title, section, chunk, zh)
                n += 1
                made += 1
        n_labels += 1
    logger.info("openfda: %d labels", n_labels)


def build_index(medlineplus_xml: Path | None, openfda_dir: Path | None, out_path: Path,
                aliases_path: Path = ALIASES_TSV) -> dict[str, int | str]:
    started = time.monotonic()
    aliases = load_aliases(aliases_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()
    con = sqlite3.connect(out_path)
    con.execute(DDL_META)
    con.execute(DDL_ALIASES)
    con.execute(DDL_FTS)
    con.executemany("INSERT INTO aliases(zh, en) VALUES (?, ?)",
                    [(zh, "|".join(en)) for zh, en in sorted(aliases.items())])

    counts = {"medlineplus": 0, "openfda": 0}
    insert = "INSERT INTO passages_fts(rid, source, lang, url, title, section, text, zh) VALUES (?,?,?,?,?,?,?,?)"
    batch: list[tuple] = []

    def flush() -> None:
        nonlocal batch
        if batch:
            con.executemany(insert, batch)
            batch = []

    sources = []
    if medlineplus_xml:
        sources.append(medlineplus_passages(medlineplus_xml, aliases))
    if openfda_dir:
        sources.append(openfda_passages(openfda_dir, aliases))
    digest = hashlib.sha256()
    for stream in sources:
        for row in stream:
            counts[row[1]] += 1
            digest.update(row[0].encode())
            batch.append(row)
            if len(batch) >= 2000:
                flush()
    flush()
    con.execute("INSERT INTO passages_fts(passages_fts) VALUES('optimize')")

    today = time.strftime("%Y.%m.%d")
    version = f"medref+{today}-{digest.hexdigest()[:10]}"
    for key, value in {
        "version": version,
        "built": today,
        "passages": str(sum(counts.values())),
        "passages_medlineplus": str(counts["medlineplus"]),
        "passages_openfda": str(counts["openfda"]),
        "aliases": str(len(aliases)),
        "tokenizer": "unicode61 remove_diacritics 2",
    }.items():
        con.execute("INSERT INTO meta(key, value) VALUES (?, ?)", (key, value))
    con.commit()
    con.close()
    logger.info("index: %d passages (%d medlineplus, %d openfda), %d aliases, %s, %.1fs",
                sum(counts.values()), counts["medlineplus"], counts["openfda"],
                len(aliases), version, time.monotonic() - started)
    return {**counts, "aliases": len(aliases), "version": version}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--medlineplus", type=Path,
                    default=next(iter(sorted(DEFAULT_RAW.glob("mplus_topics_2*.xml"))), None))
    ap.add_argument("--openfda", type=Path, default=DEFAULT_RAW / "openfda")
    ap.add_argument("--aliases", type=Path, default=ALIASES_TSV)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    build_index(args.medlineplus, args.openfda, args.out, args.aliases)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
