#!/usr/bin/env python3
"""Fail when the files that name Mirobody's release disagree.

A release is cut by bumping its version in four files, and nothing tied them
together. A cut that bumps only some of them ships a release whose
``./deploy.sh`` still pulls the previous image, or whose MCP registry entry
still installs the previous wheel. This is the guard for that cut. It reads:

- ``compose.yaml``: the app image's tag, in every service that runs it;
- ``mirobody/__init__.py``: the fallback ``__version__`` of a source tree;
- ``server.json``: the registry entry's ``version`` and its package's;
- ``Dockerfile``: the ``MIROBODY_VERSION`` default, ``X.Y.Z.dev0`` for a
  local build, of which only ``X.Y.Z`` counts.

    python scripts/check_versions.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _found(root: Path, path: str, pattern: str) -> list[str]:
    return re.findall(pattern, (root / path).read_text(encoding="utf-8"), flags=re.MULTILINE)


def versions(root: Path = ROOT) -> dict[str, list[str]]:
    """Every version each file names, keyed by where it was read."""
    server = json.loads((root / "server.json").read_text(encoding="utf-8"))
    return {
        "compose.yaml, the app image's tag": _found(
            root, "compose.yaml", r"thetahealth4mirobody/mirobody:([^\s}\"']+)"),
        # pypi-release.yml reads the tree's version with this same pattern, so
        # the tag it accepts and the version checked here cannot drift apart.
        "mirobody/__init__.py, the fallback __version__": _found(
            root, "mirobody/__init__.py", r'or "(\d+\.\d+\.\d+)"'),
        "server.json, version": [server["version"]] if "version" in server else [],
        "server.json, the packages' version": [p["version"] for p in server.get("packages", []) if "version" in p],
        "Dockerfile, MIROBODY_VERSION's X.Y.Z": _found(
            root, "Dockerfile", r"^ARG MIROBODY_VERSION=(\d+\.\d+\.\d+)"),
    }


def main(root: Path = ROOT) -> int:
    found = versions(root)
    named = {version for values in found.values() for version in values}
    if len(named) == 1 and all(found.values()):
        print(f"check_versions: every file names {named.pop()}")
        return 0
    for where, values in found.items():
        print(f"  {where}: {', '.join(values) or 'not found'}", file=sys.stderr)
    print("check_versions: these name one release and are bumped together.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
