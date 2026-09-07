"""Snapshot files with an explicit update switch.

``assert_golden(path, actual)`` compares ``actual`` with the file at
``path`` and raises with a unified diff on mismatch. Setting
``MIROBODY_UPDATE_GOLDEN=1`` rewrites the file instead — the only way a
snapshot changes, so a behaviour change is a visible diff in review, never
a silent re-baseline. Pure; stdlib only.
"""

from __future__ import annotations

import difflib
import json
import os
from pathlib import Path

UPDATE_ENV = "MIROBODY_UPDATE_GOLDEN"


def updating() -> bool:
    return os.environ.get(UPDATE_ENV, "") not in ("", "0", "false", "no")


def canonical_json(value: object) -> str:
    """Stable JSON: sorted keys, two-space indent, trailing newline."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"


def assert_golden(path: Path | str, actual: str) -> None:
    path = Path(path)
    if updating() or not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(actual, encoding="utf-8")
        if not updating():
            raise AssertionError(f"golden file {path} did not exist; it has been written — review and commit it")
        return
    expected = path.read_text(encoding="utf-8")
    if expected != actual:
        diff = "".join(difflib.unified_diff(expected.splitlines(True), actual.splitlines(True), str(path), "actual"))
        raise AssertionError(f"golden mismatch for {path} (set {UPDATE_ENV}=1 to accept):\n{diff}")


def assert_golden_json(path: Path | str, value: object) -> None:
    assert_golden(path, canonical_json(value))


__all__ = ["UPDATE_ENV", "assert_golden", "assert_golden_json", "canonical_json", "updating"]
