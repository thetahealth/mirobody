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

#: The path Apple writes inside the archive. Apple documents neither this nor
#: the `export.zip` filename, so the archive is searched by content: any
#: member whose name ends this way is the export.
_MEMBER = "apple_health_export/export.xml"

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


def open_export(path: str | Path) -> tuple[IO[bytes], zipfile.ZipFile | None]:
    """A binary handle on `export.xml`, whether `path` is the zip, the
    unpacked directory, or the file itself. The second element is the archive
    to close when the caller is done, or None."""
    p = Path(path)
    if p.is_dir():
        found = p / _MEMBER
        if not found.exists():
            found = p / "export.xml"
        return found.open("rb"), None
    if zipfile.is_zipfile(p):
        zf = zipfile.ZipFile(p)
        names = [n for n in zf.namelist() if n.endswith((_MEMBER, "/export.xml"))]
        if not names:
            zf.close()
            raise FileNotFoundError(f"{p}: no export.xml inside the archive")
        return zf.open(names[0]), zf
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
