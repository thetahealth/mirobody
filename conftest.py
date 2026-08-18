"""Let `pip install -e '.[test]'` run the tests it can.

The suite spans two install sizes. Most of it exercises the ENGINE, which by
design needs no agent framework. A handful of modules exercise the AGENT layer
and therefore need the `[agents]` extra.

Without this file, a contributor who ran the command CONTRIBUTING documents —
`pip install -e '.[test]'` — got a hard collection error, not a skip:

    ImportError while importing test module '.../test_adapters.py'
    E   ModuleNotFoundError: No module named 'langchain_core'

and the whole run aborted, including the ~190 tests that had everything they
needed. A module-level `pytest.importorskip` cannot fix that: importing the
test module imports its parent package first, and that is what pulls in
langchain. Deciding at COLLECTION time is the only thing early enough.
"""

from __future__ import annotations

import importlib.util

_HAS_AGENTS = importlib.util.find_spec("langchain_core") is not None

# Tests that import the agent layer, directly or through their parent package.
_AGENT_ONLY = [
    "mirobody/agent/*",
    "mirobody/agent/**/*",
    "mirobody/server/test_htdoc.py",
]

collect_ignore_glob: list[str] = [] if _HAS_AGENTS else list(_AGENT_ONLY)


def pytest_report_header(config) -> str:
    """Say which half is running, so a short run is never mistaken for a pass."""
    if _HAS_AGENTS:
        return "mirobody: engine + agent tests (the [agents] extra is installed)"
    return (
        "mirobody: ENGINE TESTS ONLY — the [agents] extra is not installed, so "
        "agent-layer tests are skipped. Install with `pip install -e '.[agents,test]'` "
        "to run everything."
    )
