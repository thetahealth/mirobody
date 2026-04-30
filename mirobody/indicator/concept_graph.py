"""Integer-ID concept graph: build, serialise, load, and query.

Domain-specific subclasses override ``load_bridges()`` and
``load_siblings()`` on ``ConceptGraphBuilder``.  The binary format,
serialisation, and query API live here.
"""

from __future__ import annotations

import logging
import os
import struct
import zlib

from array import array
from bisect import bisect_left
from collections import defaultdict

log = logging.getLogger(__name__)

GRAPH_MAGIC = b"CGPH"
GRAPH_VERSION = 1


# ── Builder ──────────────────────────────────────────────────────────


class ConceptGraphBuilder:
    """Build-time graph: populate data then serialise to binary.

    Subclass and override ``load_bridges()`` / ``load_siblings()``,
    then call ``build(out_dir)`` which handles the rest. Subclasses set
    ``DEFAULT_BIN_NAME`` so callers can ``build(out_dir)`` without
    naming the output file every time — the binary's name is a
    domain-specific concern (e.g. ``fhir_concept_graph.bin``), not a
    framework default.
    """

    # Subclass must override; framework has no opinion on filename.
    DEFAULT_BIN_NAME: str = ""

    def __init__(self) -> None:
        self.bridges: dict[int, set[int]] = {}
        self.siblings: list[list[int]] = []

    # ── Interface (override in subclass) ─────────────────────────────

    def load_bridges(self, src_dir: str) -> dict[int, set[int]]:
        """Load cross-system bridge edges from domain-specific sources.

        Args:
            src_dir: Directory containing domain data files.

        Returns:
            ``{node_id: {neighbor_ids}}`` — symmetric edges.
            If A→B exists, B→A must also exist.
        """
        raise NotImplementedError

    def load_siblings(self, src_dir: str) -> list[list[int]]:
        """Load same-system sibling groups from domain-specific sources.

        Args:
            src_dir: Directory containing domain data files.

        Returns:
            ``[[id, id, ...], ...]`` — each inner list is a cluster of
            related nodes.  Groups of size < 2 are ignored at query time.
        """
        raise NotImplementedError

    # ── Template method ──────────────────────────────────────────────

    def build(self, src_dir: str, dest_path: str | None = None) -> None:
        """Build the graph and save to binary.

        Args:
            src_dir: Directory containing domain source data (passed to
                     ``load_bridges`` and ``load_siblings``).
            dest_path: Output binary path.  Defaults to
                       ``src_dir/<DEFAULT_BIN_NAME>`` if the subclass
                       sets that class attribute.
        """
        if not os.path.isdir(src_dir):
            raise FileNotFoundError(f"Source data directory does not exist: {src_dir}")
        if dest_path is None:
            if not self.DEFAULT_BIN_NAME:
                raise ValueError(
                    f"{type(self).__name__} has no DEFAULT_BIN_NAME; "
                    f"pass dest_path explicitly"
                )
            dest_path = os.path.join(src_dir, self.DEFAULT_BIN_NAME)

        self.bridges = self.load_bridges(src_dir)
        self.siblings = self.load_siblings(src_dir)
        self._save(dest_path)

    def _save(self, file_path: str) -> None:
        """Serialize ``self.bridges`` / ``self.siblings`` to binary.

        Format (v2):
          header:  magic(4) + version(H) + n_bridge(I) + n_siblings(I)
          bridges: per entry: src(I) + n_dst(I) + dst_ids(I*n)
                   Only stores edges where src < dst to halve size.
          siblings: per group: n_ids(H) + ids(I*n)
        """
        buf = bytearray()
        header_fmt = "<4sHII"
        header_size = struct.calcsize(header_fmt)
        buf.extend(b"\x00" * header_size)

        n_bridge_entries = 0
        for src in sorted(self.bridges):
            dst_ids = sorted(n for n in self.bridges[src] if n > src)
            if dst_ids:
                buf.extend(struct.pack(f"<II{len(dst_ids)}I", src, len(dst_ids), *dst_ids))
                n_bridge_entries += 1

        for group in self.siblings:
            buf.extend(struct.pack(f"<H{len(group)}I", len(group), *group))

        struct.pack_into(header_fmt, buf, 0,
                         GRAPH_MAGIC, GRAPH_VERSION,
                         n_bridge_entries, len(self.siblings))

        compressed = zlib.compress(bytes(buf), level=6)
        with open(file_path, "wb") as f:
            f.write(compressed)
        log.info("Graph binary saved: %s bridge entries (half-edge), "
                 "%s sibling groups, %s raw → %s compressed → %s",
                 f"{n_bridge_entries:,}", f"{len(self.siblings):,}",
                 f"{len(buf):,}", f"{len(compressed):,}", file_path)


# ── Query ────────────────────────────────────────────────────────────


class ConceptGraph:
    """Query-time graph loaded from a pre-built binary."""

    _cache: dict[str, ConceptGraph] = {}

    def __init__(self) -> None:
        self._bridge_keys: array = array('I')
        self._bridge_off: array = array('I')
        self._bridge_data: array = array('I')
        
        self._sib_data: array = array('I')
        self._sib_off: array = array('I')
        self._sib_idx_keys: array = array('I')
        self._sib_idx_off: array = array('I')
        self._sib_idx_data: array = array('I')
        self._sib_grp_size: array = array('I')

    @classmethod
    def get(cls, file_path: str) -> ConceptGraph:
        """Load and cache the graph at *file_path*.

        Path-keyed cache: same path returns the same instance, different
        paths get separate ones (so e.g. fhir and finance graphs can
        coexist). The framework has no implicit "bundled default" —
        each domain owns its own binary; the missing file is a config
        error, not something to silently paper over.
        """
        file_path = os.path.realpath(file_path)
        if file_path not in cls._cache:
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"Graph binary not found: {file_path}")
            graph = cls()
            graph._load_bin(file_path)
            cls._cache[file_path] = graph
            log.info("Graph loaded: %s", graph.stats())
        return cls._cache[file_path]

    # ── Binary loading ─────────────────────────────────────────────

    def _load_bin(self, file_path: str) -> None:
        with open(file_path, "rb") as f:
            raw = f.read()
        if raw.startswith(b"version https://git-lfs"):
            raise RuntimeError(
                f"{file_path} is a Git LFS pointer, not the actual binary. "
                f"Run 'git lfs pull' to download the real file.")
        data = zlib.decompress(raw)

        header_fmt = "<4sHII"
        header_size = struct.calcsize(header_fmt)
        magic, version, n_bridge, n_siblings = struct.unpack_from(header_fmt, data, 0)
        if magic != GRAPH_MAGIC:
            raise ValueError(f"Invalid graph binary: bad magic {magic!r}")
        if version != GRAPH_VERSION:
            raise ValueError(f"Unsupported graph version {version}, expected {GRAPH_VERSION}")

        sib_start = self._load_bridges(data, header_size, n_bridge)
        self._load_siblings(data, sib_start, n_siblings)

        log.info("Graph loaded: %s bridge nodes, %s sibling groups, %s sibling index entries",
                 f"{len(self._bridge_keys):,}", f"{n_siblings:,}",
                 f"{len(self._sib_idx_keys):,}")

    def _load_bridges(self, data: bytes, pos: int, n_bridge: int) -> int:
        degree: dict[int, int] = defaultdict(int)
        scan_pos = pos
        for _ in range(n_bridge):
            src, n_dst = struct.unpack_from("<II", data, pos)
            pos += 8
            dst_ids = struct.unpack_from(f"<{n_dst}I", data, pos)
            pos += n_dst * 4
            for dst in dst_ids:
                degree[src] += 1
                degree[dst] += 1

        sib_start = pos

        sorted_nodes = sorted(degree)
        bridge_keys = array('I', sorted_nodes)
        bridge_off = array('I')
        node_pos_map: dict[int, int] = {}
        total = 0
        for i, nid in enumerate(sorted_nodes):
            node_pos_map[nid] = i
            bridge_off.append(total)
            total += degree[nid]
        bridge_off.append(total)
        bridge_data = array('I', bytes(total * 4))
        cursor = array('I', bridge_off[:-1])

        pos = scan_pos
        for _ in range(n_bridge):
            src, n_dst = struct.unpack_from("<II", data, pos)
            pos += 8
            dst_ids = struct.unpack_from(f"<{n_dst}I", data, pos)
            pos += n_dst * 4
            src_idx = node_pos_map[src]
            for dst in dst_ids:
                dst_idx = node_pos_map[dst]
                bridge_data[cursor[src_idx]] = dst
                cursor[src_idx] += 1
                bridge_data[cursor[dst_idx]] = src
                cursor[dst_idx] += 1

        self._bridge_keys = bridge_keys
        self._bridge_off = bridge_off
        self._bridge_data = bridge_data
        return sib_start

    def _load_siblings(self, data: bytes, pos: int, n_siblings: int) -> None:
        sib_data = array('I')
        sib_off = array('I')
        sib_member_count: dict[int, int] = defaultdict(int)
        for gi in range(n_siblings):
            (n_ids,) = struct.unpack_from("<H", data, pos)
            pos += 2
            ids = struct.unpack_from(f"<{n_ids}I", data, pos)
            pos += n_ids * 4
            sib_off.append(len(sib_data))
            sib_data.extend(ids)
            for nid in ids:
                sib_member_count[nid] += 1
        sib_off.append(len(sib_data))
        self._sib_data = sib_data
        self._sib_off = sib_off

        sorted_sib_ids = sorted(sib_member_count)
        sib_idx_keys = array('I', sorted_sib_ids)
        sib_idx_off = array('I')
        sib_id_pos: dict[int, int] = {}
        total = 0
        for i, nid in enumerate(sorted_sib_ids):
            sib_id_pos[nid] = i
            sib_idx_off.append(total)
            total += sib_member_count[nid]
        sib_idx_off.append(total)
        sib_idx_data = array('I', bytes(total * 4))
        sib_cursor = array('I', sib_idx_off[:-1])

        for gi in range(n_siblings):
            start = sib_off[gi]
            end = sib_off[gi + 1]
            for j in range(start, end):
                nid = sib_data[j]
                fi = sib_id_pos[nid]
                sib_idx_data[sib_cursor[fi]] = gi
                sib_cursor[fi] += 1

        self._sib_idx_keys = sib_idx_keys
        self._sib_idx_off = sib_idx_off
        self._sib_idx_data = sib_idx_data
        self._sib_grp_size = array('I', (
            sib_off[g + 1] - sib_off[g] for g in range(n_siblings)
        ))

    # ── Query API ───────────────────────────────────────────────────

    @staticmethod
    def _bisect_lookup(keys, key: int) -> int:
        i = bisect_left(keys, key)
        if i < len(keys) and keys[i] == key:
            return i
        return -1

    def bridge_neighbors(self, id: int) -> set[int]:
        """Cross-system mapped IDs."""
        i = self._bisect_lookup(self._bridge_keys, id)
        if i < 0:
            return set()
        return set(self._bridge_data[self._bridge_off[i]:self._bridge_off[i + 1]])

    def sibling_neighbors(self, id: int, max_per_id: int = 50) -> set[int]:
        """Same-system related IDs, preferring smaller groups."""
        i = self._bisect_lookup(self._sib_idx_keys, id)
        if i < 0:
            return set()
        grp_indices = self._sib_idx_data[self._sib_idx_off[i]:self._sib_idx_off[i + 1]]
        result: set[int] = set()
        for gi in sorted(grp_indices, key=self._sib_grp_size.__getitem__):
            result.update(self._sib_data[self._sib_off[gi]:self._sib_off[gi + 1]])
            if len(result) >= max_per_id:
                break
        result.discard(id)
        return result

    def neighbors(self, id: int, max_siblings: int = 50) -> set[int]:
        """All neighbors: bridges + siblings."""
        return self.bridge_neighbors(id) | self.sibling_neighbors(id, max_siblings)

    def stats(self) -> dict:
        return {
            "bridge_nodes": len(self._bridge_keys),
            "bridge_edges": len(self._bridge_data),
            "sibling_groups": len(self._sib_off) - 1 if self._sib_off else 0,
            "sibling_ids": len(self._sib_idx_keys),
        }
