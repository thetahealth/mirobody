"""Integer-ID taxonomy: build, serialise, load, and query.

Domain-specific subclasses override ``load_labels()`` and
``load_assignments()`` on ``TaxonomyBuilder``. The binary format,
serialisation, and query API live here.

Each fhir_id is assigned to at most one label; labels form a shallow
tree via ``parent_id``. Labels use local uint8 IDs (1..255), with an
external integer code (e.g. SNOMED SCTID) carried alongside for API
exposure.
"""

from __future__ import annotations

import logging
import os
import struct
import zlib
from array import array
from typing import NamedTuple

log = logging.getLogger(__name__)

TAXONOMY_MAGIC = b"TXNM"
TAXONOMY_VERSION = 1

# Label record: label_id(B) + parent_id(B) + _pad(H) + external_code(Q)
#             + name_off(H) + name_len(H)
#             + fhir_off(I) + fhir_count(I)
_LABEL_STRUCT = struct.Struct("<BBHQHHII")
assert _LABEL_STRUCT.size == 24


class Label(NamedTuple):
    id: int              # local 1..255
    parent_id: int       # 0 = root
    external_code: int   # e.g. SNOMED SCTID (0 = UI-only container)
    name: str            # English; UI localisation happens in frontend


# ── Builder ──────────────────────────────────────────────────────────


class TaxonomyBuilder:
    """Build-time: populate labels + fhir_id → label_id, serialise to binary.

    Subclass and override ``load_labels()`` / ``load_assignments()``,
    then call ``build(src_dir, dest_path)`` which handles serialisation.
    """

    def __init__(self) -> None:
        self.labels: dict[int, Label] = {}
        self.assignments: dict[int, int] = {}

    def load_labels(self, src_dir: str) -> dict[int, Label]:
        raise NotImplementedError

    def load_assignments(self, src_dir: str) -> dict[int, int]:
        raise NotImplementedError

    def build(self, src_dir: str, dest_path: str) -> None:
        self.labels = self.load_labels(src_dir)
        self.assignments = self.load_assignments(src_dir)
        self._save(dest_path)

    def _save(self, file_path: str) -> None:
        """Serialise ``self.labels`` / ``self.assignments`` to binary.

        Format (v1), all little-endian, zlib-compressed:
          header:     magic(4)='TXNM' + version(H) + n_labels(H)
          labels:     n_labels × 24 bytes ``_LABEL_STRUCT``
          name pool:  pool_len(I) + utf-8 bytes
          fhir pool:  pool_count(I) + uint32 × pool_count
        """
        if not self.labels:
            raise ValueError("No labels to serialise")

        by_label: dict[int, list[int]] = {lid: [] for lid in self.labels}
        for fhir_id, label_id in self.assignments.items():
            if label_id in by_label:
                by_label[label_id].append(fhir_id)
        for lid in by_label:
            by_label[lid].sort()

        sorted_lids = sorted(self.labels)

        name_pool = bytearray()
        name_offs: dict[int, tuple[int, int]] = {}
        for lid in sorted_lids:
            nb = self.labels[lid].name.encode("utf-8")
            name_offs[lid] = (len(name_pool), len(nb))
            name_pool.extend(nb)
        if len(name_pool) > 0xFFFF:
            raise ValueError(f"Name pool exceeds 64KB: {len(name_pool)}")

        fhir_pool: list[int] = []
        fhir_offs: dict[int, tuple[int, int]] = {}
        for lid in sorted_lids:
            ids = by_label.get(lid, [])
            fhir_offs[lid] = (len(fhir_pool), len(ids))
            fhir_pool.extend(ids)

        buf = bytearray()
        buf.extend(struct.pack("<4sHH", TAXONOMY_MAGIC, TAXONOMY_VERSION, len(sorted_lids)))
        for lid in sorted_lids:
            lab = self.labels[lid]
            noff, nlen = name_offs[lid]
            foff, fcnt = fhir_offs[lid]
            buf.extend(_LABEL_STRUCT.pack(
                lab.id, lab.parent_id, 0, lab.external_code,
                noff, nlen, foff, fcnt,
            ))
        buf.extend(struct.pack("<I", len(name_pool)))
        buf.extend(name_pool)
        buf.extend(struct.pack("<I", len(fhir_pool)))
        if fhir_pool:
            buf.extend(array("I", fhir_pool).tobytes())

        compressed = zlib.compress(bytes(buf), level=6)
        with open(file_path, "wb") as f:
            f.write(compressed)
        log.info("Taxonomy binary saved: %d labels, %d assignments, "
                 "%s raw → %s compressed → %s",
                 len(sorted_lids), len(self.assignments),
                 f"{len(buf):,}", f"{len(compressed):,}", file_path)


# ── Query ────────────────────────────────────────────────────────────


class Taxonomy:
    """Query-time taxonomy loaded from a pre-built binary."""

    _cache: dict[str, "Taxonomy"] = {}

    def __init__(self) -> None:
        self._labels: dict[int, Label] = {}
        self._code_to_id: dict[int, int] = {}
        self._children: dict[int, list[int]] = {}
        self._fhir_pool: array = array("I")
        self._fhir_off: dict[int, tuple[int, int]] = {}

    @classmethod
    def get(cls, file_path: str) -> "Taxonomy":
        """Load and cache the taxonomy at *file_path*. Same path returns
        the same instance; missing file is a config error, not silently
        recoverable. Caller (the domain that owns this binary) decides
        the path — the framework has no implicit "bundled default"."""
        file_path = os.path.realpath(file_path)
        if file_path not in cls._cache:
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"Taxonomy binary not found: {file_path}")
            tax = cls()
            tax._load_bin(file_path)
            cls._cache[file_path] = tax
            log.info("Taxonomy loaded: %d labels, %d assignments",
                     len(tax._labels), len(tax._fhir_pool))
        return cls._cache[file_path]

    def _load_bin(self, file_path: str) -> None:
        with open(file_path, "rb") as f:
            raw = f.read()
        if raw.startswith(b"version https://git-lfs"):
            raise RuntimeError(
                f"{file_path} is a Git LFS pointer. Run 'git lfs pull' to fetch.")
        data = zlib.decompress(raw)

        magic, version, n_labels = struct.unpack_from("<4sHH", data, 0)
        if magic != TAXONOMY_MAGIC:
            raise ValueError(f"Invalid taxonomy binary: bad magic {magic!r}")
        if version != TAXONOMY_VERSION:
            raise ValueError(f"Unsupported taxonomy version {version}")

        pos = 8
        records = []
        for _ in range(n_labels):
            records.append(_LABEL_STRUCT.unpack_from(data, pos))
            pos += _LABEL_STRUCT.size

        (pool_len,) = struct.unpack_from("<I", data, pos)
        pos += 4
        name_pool = data[pos:pos + pool_len]
        pos += pool_len

        (fhir_count,) = struct.unpack_from("<I", data, pos)
        pos += 4
        if fhir_count:
            self._fhir_pool = array("I")
            self._fhir_pool.frombytes(data[pos:pos + fhir_count * 4])

        for lid, pid, _pad, ecode, noff, nlen, foff, fcnt in records:
            name = bytes(name_pool[noff:noff + nlen]).decode("utf-8")
            self._labels[lid] = Label(lid, pid, ecode, name)
            if ecode:  # skip UI-only containers (external_code == 0)
                self._code_to_id[ecode] = lid
            self._fhir_off[lid] = (foff, fcnt)
            self._children.setdefault(pid, []).append(lid)

    # ── Query API ───────────────────────────────────────────────────

    def all_labels(self) -> list[Label]:
        return [self._labels[lid] for lid in sorted(self._labels)]

    def label(self, label_id: int) -> Label | None:
        return self._labels.get(label_id)

    def code_to_label(self, external_code: int) -> Label | None:
        lid = self._code_to_id.get(external_code)
        return self._labels.get(lid) if lid is not None else None

    def children(self, label_id: int) -> list[int]:
        return list(self._children.get(label_id, ()))

    def ancestors(self, label_id: int) -> list[int]:
        result: list[int] = []
        cur = label_id
        while cur in self._labels:
            parent = self._labels[cur].parent_id
            if parent == 0 or parent not in self._labels:
                break
            result.append(parent)
            cur = parent
        return result

    def descendants(self, label_id: int) -> set[int]:
        result: set[int] = set()
        stack = list(self._children.get(label_id, ()))
        while stack:
            cur = stack.pop()
            if cur in result:
                continue
            result.add(cur)
            stack.extend(self._children.get(cur, ()))
        return result

    def indicators_of(self, label_id: int, recursive: bool = False) -> list[int]:
        """Return fhir_ids assigned under this label.

        When ``recursive`` is True, also include fhir_ids of all descendant
        labels. Order is unspecified across labels but sorted within each.
        """
        if label_id not in self._labels:
            return []
        target_ids = {label_id}
        if recursive:
            target_ids |= self.descendants(label_id)
        result: list[int] = []
        for lid in target_ids:
            off, cnt = self._fhir_off.get(lid, (0, 0))
            if cnt:
                result.extend(self._fhir_pool[off:off + cnt])
        return result

    def stats(self) -> dict:
        return {
            "labels": len(self._labels),
            "assignments": len(self._fhir_pool),
        }
