"""Read a versioned, public dbSNP-derived site catalog without loading it in RAM."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_PATH = Path(__file__).resolve().parents[1] / "res" / "genomics" / "genotype_sites.sqlite3"


class SiteCatalog:
    """A per-import read-only connection to the shipped site index."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path or os.environ.get("GENOTYPE_SITE_CATALOG") or DEFAULT_PATH)
        self._conn: sqlite3.Connection | None = None
        self.version = "missing"
        if self.path.is_file():
            self._conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
            self._conn.row_factory = sqlite3.Row
            row = self._conn.execute("SELECT value FROM metadata WHERE key = 'version'").fetchone()
            self.version = str(row[0]) if row else "unversioned"

    def lookup(self, rsid: str, chrom: str, position: int) -> dict[str, Any] | None:
        if self._conn is None:
            return None
        current = rsid
        if rsid != ".":
            merged = self._conn.execute("SELECT rsid FROM merged WHERE old_rsid = ?", (rsid,)).fetchone()
            if merged:
                current = str(merged[0])
            row = self._conn.execute(
                "SELECT rsid, chrom, pos37, pos38, ref, alt, gene FROM sites WHERE rsid = ?", (current,),
            ).fetchone()
            return dict(row) if row else None
        # An unannotated VCF can be resolved by coordinate only if exactly one
        # reference site matches. Returning the first of two would invent an ID.
        rows = self._conn.execute(
            "SELECT rsid, chrom, pos37, pos38, ref, alt, gene FROM sites "
            "WHERE chrom = ? AND (pos37 = ? OR pos38 = ?) LIMIT 2",
            (chrom, position, position),
        ).fetchall()
        return dict(rows[0]) if len(rows) == 1 else None

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> SiteCatalog:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
