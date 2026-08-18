"""Every example must actually run.

`examples/README.md` tells the reader that if one of these fails, it is a bug
rather than a missing step. That is only true if something checks — an example
that no longer runs is a claim nobody verified, and it is the first thing a new
reader tries.

Each runs in its own subprocess, from the repository root, with no arguments and
no configuration. They are written to degrade to their offline half when a key,
a database or a file is absent, so exit 0 is the correct expectation on a bare
machine — including in CI.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
EXAMPLES = sorted((REPO / "examples").glob("[0-9]*.py"))


def test_there_are_examples_to_check():
    """Guard against this file silently passing because the glob broke."""
    assert len(EXAMPLES) >= 5, f"expected the examples/ scripts, found {EXAMPLES}"


@pytest.mark.parametrize("script", EXAMPLES, ids=lambda p: p.stem)
def test_example_runs_clean(script: Path):
    proc = subprocess.run(
        [sys.executable, str(script)],
        cwd=REPO, capture_output=True, text=True, timeout=300,
    )
    assert proc.returncode == 0, (
        f"{script.name} exited {proc.returncode}\n"
        f"--- stdout ---\n{proc.stdout[-2000:]}\n"
        f"--- stderr ---\n{proc.stderr[-2000:]}"
    )
    assert proc.stdout.strip(), f"{script.name} printed nothing — an example must show something"


@pytest.mark.parametrize("script", EXAMPLES, ids=lambda p: p.stem)
def test_example_is_documented(script: Path):
    """The index in examples/README.md is how anyone finds these."""
    readme = (REPO / "examples" / "README.md").read_text(encoding="utf-8")
    assert script.name in readme, f"{script.name} is not listed in examples/README.md"
