"""Read an Apple Health export into the items `apple.decode` expects.

The decode table next door is a pure function of one record. This module is
the other half for the file front door: it opens the archive and streams it.
It lives here rather than in the IO layer because the whole job is `zipfile`
and `xml.etree`, so a bare `pip install mirobody` can read an export with no
database, no server and no extra.

Streaming is not an optimisation. A measured export is 5.5 MB zipped, 109 MB
open, about 446,670 `Record` elements; a watch worn for years reaches
hundreds of megabytes. Records are yielded one at a time, and each element is
cleared and dropped from the root, so memory does not follow the file.
"""

from __future__ import annotations

import zipfile
from collections.abc import Iterator
from pathlib import Path
from typing import IO
from xml.etree import ElementTree as ET

from .apple import SLEEP_TYPE

#: The path Apple writes inside the archive on an English phone. Apple
#: documents neither this nor the `export.zip` filename, and a phone set to
#: another language localizes it (Chinese writes `导出.xml`), so this name is
#: only tried first: the export is the XML document whose root is `HealthData`.
_MEMBER = "apple_health_export/export.xml"
_ROOT = "HealthData"

#: Attributes copied from a `Record` element. The DTD makes `value` and
#: `unit` optional, so a record can legitimately carry neither.
_FIELDS = ("value", "unit", "startDate", "endDate", "sourceName", "device")


class Counts:
    """What a run saw, for the caller to report. Which types decoded to
    nothing is the caller's to count, since only `apple.decode` knows."""

    def __init__(self) -> None:
        self.records = 0
        self.correlations = 0
        self.clinical_files = 0


def _item(elem: ET.Element) -> dict:
    return {k: elem.get(k) for k in _FIELDS if elem.get(k) is not None}


def _is_candidate(name: str) -> bool:
    # The clinical records sit next to the export as `*_cda.xml`.
    return name.lower().endswith(".xml") and not name.lower().endswith("_cda.xml")


def _has_health_root(handle: IO[bytes]) -> bool:
    with handle:
        try:
            for _event, elem in ET.iterparse(handle, events=("start",)):
                return elem.tag == _ROOT
        except ET.ParseError:
            return False
    return False


def open_export(path: str | Path) -> tuple[IO[bytes], zipfile.ZipFile | None]:
    """A binary handle on the export document, whether `path` is the zip, the
    unpacked directory, or the file itself. The second element is the archive
    to close when the caller is done, or None."""
    p = Path(path)
    if p.is_dir():
        named = [p / _MEMBER, p / "export.xml"]
        others = sorted(
            f
            for d in (p / "apple_health_export", p)
            if d.is_dir()
            for f in d.iterdir()
            if f.is_file() and _is_candidate(f.name)
        )
        for found in dict.fromkeys(f for f in named + others if f.is_file()):
            if _has_health_root(found.open("rb")):
                return found.open("rb"), None
        raise FileNotFoundError(f"{p}: no Apple Health export inside the directory")
    if zipfile.is_zipfile(p):
        zf = zipfile.ZipFile(p)
        names = zf.namelist()
        named = [n for n in names if n.endswith((_MEMBER, "/export.xml")) or n == "export.xml"]
        # Only the export folder itself: the routes and ECGs live in subfolders.
        others = [n for n in names if n.count("/") <= 1 and _is_candidate(n) and n not in named]
        for name in named + others:
            if _has_health_root(zf.open(name)):
                return zf.open(name), zf
        zf.close()
        raise FileNotFoundError(f"{p}: no Apple Health export inside the archive")
    return p.open("rb"), None


def iter_items(path: str | Path, counts: Counts | None = None) -> Iterator[tuple[str, dict]]:
    """`(data_type, item)` pairs ready for `apple.decode`.

    A blood pressure is a `Correlation` wrapping its two numbers, and Apple's
    own DTD comment says those children "also appear as top-level records in
    this document". The top-level copies are therefore the complete set, and
    everything inside a Correlation is skipped: emitting both would file one
    reading twice. The pair is regrouped by `apple.decode`, which panels a
    systolic and a diastolic that share a start.
    """
    seen = counts if counts is not None else Counts()
    handle, archive = open_export(path)
    try:
        depth = 0
        root: ET.Element | None = None
        for event, elem in ET.iterparse(handle, events=("start", "end")):
            if event == "start":
                if root is None:
                    root = elem
                if elem.tag == "Correlation":
                    depth += 1
                continue
            if elem.tag == "Correlation":
                depth -= 1
                seen.correlations += 1
            elif elem.tag == "ClinicalRecord":
                seen.clinical_files += 1
            elif elem.tag == "Record" and not depth:
                kind = elem.get("type") or ""
                item = _item(elem)
                if kind == SLEEP_TYPE:
                    item["value"] = elem.get("value")
                seen.records += 1
                yield kind, item
            elif elem.tag != "Record":
                continue
            elem.clear()
            if root is not None and not depth:
                # A cleared element is still the root's child: 400,000 empty
                # Record shells held 32.7 MB until the file ended.
                root.clear()
    finally:
        handle.close()
        if archive is not None:
            archive.close()
