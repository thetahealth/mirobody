"""Let `pip install -e '.[test]'` run the tests it can.

The suite spans three install sizes. Most of it exercises the LIBRARY, which by
design needs numpy and nothing else. Some modules exercise the SERVER layer
(fastapi, psycopg_pool, mandrill) and some the AGENT layer (langchain); both
arrive with the `[app]` extra, but they are detected separately because a
langchain-free server install is a real configuration the release workflow
builds on purpose.

Without this file, a contributor who ran the command CONTRIBUTING documents —
`pip install -e '.[test]'` — got a hard collection error, not a skip:

    ImportError while importing test module '.../test_adapters.py'
    E   ModuleNotFoundError: No module named 'langchain_core'

and the whole run aborted, including the tests that had everything they needed.
A module-level `pytest.importorskip` cannot fix that: importing the test module
imports its parent package first, and that is what pulls in langchain. Deciding
at COLLECTION time is the only thing early enough.

The server half of this was missed when the file was written, and an
external reviewer found it the only way it can be found — in a CLEAN
environment, where `pip install -e '.[test]'` still aborted with

    ERROR mirobody/mcp/test_protocol.py
    E   ModuleNotFoundError: No module named 'psycopg_pool'
    ERROR mirobody/user/test_oauth_code.py
    E   ModuleNotFoundError: No module named 'mandrill'

Same mechanism, different layer: `mirobody/server/__init__` imports the pool
and `mirobody/user/__init__` imports mandrill, so importing ANY test module
under those packages needs the server stack whether that test touches it or not.
CI never caught it because CI never ran pytest at all.
"""

from __future__ import annotations

import importlib.util

def _installed(*modules: str) -> bool:
    """All of `modules` importable? `find_spec` raises for a missing PARENT
    package, which is exactly the case being probed, so it is caught."""
    for name in modules:
        try:
            if importlib.util.find_spec(name) is None:
                return False
        except (ImportError, ValueError):
            return False
    return True


_HAS_AGENTS = _installed("langchain_core")
_HAS_SERVER = _installed("fastapi", "psycopg_pool", "mandrill")
# The [parse] layer: document extraction and the model-client factory. Probed
# on `dotenv`/`ruamel` rather than on pdfplumber because `mirobody/utils/config`
# is what most of these reach through, and it is the first thing to be missing.
_HAS_PARSE = _installed("dotenv", "ruamel.yaml", "pdfplumber")

# Tests that import the agent layer, directly or through their parent package.
_AGENT_ONLY = [
    "mirobody/agent/*",
    "mirobody/agent/**/*",
    "mirobody/server/test_htdoc.py",
]

# Tests that import the server layer, directly or through their parent package.
# `mirobody/user/*` is here in full because `mirobody/user/__init__` imports
# `.email`, which imports mandrill at module scope — a test of pure token logic
# still needs the extra.
_SERVER_ONLY = [
    "mirobody/server/*",
    "mirobody/server/**/*",
    "mirobody/user/*",
    "mirobody/user/**/*",
    "mirobody/mcp/test_protocol.py",
]

# Tests that import the [parse] layer, directly or through their parent
# package. This third bucket arrived with 1.3.0: `dotenv`, `ruamel.yaml` and
# the extraction stack left base, so on a `pip install -e '.[test]'` the five
# modules below now abort collection the same way the server ones used to.
# Found the only way this class is ever found — in a clean clone, not in a
# long-lived venv that has everything.
_PARSE_ONLY = [
    "mirobody/pulse/file_parser/*",
    "mirobody/pulse/file_parser/**/*",
    "mirobody/utils/test_content_type.py",
    "mirobody/test_one_key_defaults.py",
    "mirobody/test_readme_numbers.py",
]

collect_ignore_glob: list[str] = []
if not _HAS_AGENTS:
    collect_ignore_glob += _AGENT_ONLY
if not _HAS_SERVER:
    collect_ignore_glob += _SERVER_ONLY
if not _HAS_PARSE:
    collect_ignore_glob += _PARSE_ONLY


def pytest_report_header(config) -> str:
    """Say which layers are running, so a short run is never mistaken for a
    full pass — the failure mode this whole file exists to prevent."""
    if _HAS_AGENTS and _HAS_SERVER and _HAS_PARSE:
        return "mirobody: library + parse + server + agent tests (all extras installed)"

    missing = [
        name for name, present in (("the parse stack", _HAS_PARSE),
                                   ("the server stack", _HAS_SERVER),
                                   ("the agent stack", _HAS_AGENTS))
        if not present
    ]
    return (
        f"mirobody: PARTIAL RUN — {' and '.join(missing)} not installed, so those "
        "tests are not collected. `pip install -e '.[app,test]'` runs everything."
    )
