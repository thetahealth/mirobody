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
import pathlib

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
# on `dotenv`/`ruamel` as well as the PDF library because `mirobody/utils/config`
# is what most of these reach through, and it is the first thing to be missing.
_HAS_PARSE = _installed("dotenv", "ruamel.yaml", "pypdfium2")

# Tests that import the agent layer, directly or through their parent package.
#
# TWO ROOTS, and both must be listed. The suite moved from `mirobody/` to
# `tests/` and these globs did not follow: on `pip install -e '.[test]'` —
# the command CONTRIBUTING documents and CI's minimal job runs — collection
# aborted with sixteen errors and zero tests, the exact failure this file
# exists to prevent. A glob that matches nothing fails silently, so when a
# test moves, grep this list.
_AGENT_ONLY = [
    "tests/agent/*",
    "tests/agent/**/*",
    "tests/test_plugin_entry_points.py",
    "tests/server/test_htdoc.py",
]

# Tests that import the server layer, directly or through their parent package.
# `*/user/*` is here in full because `mirobody/user/__init__` imports `.auth`,
# which imports mandrill at module scope — a test of pure token logic still
# needs the extra. `utils/test_db.py` needs sqlalchemy, which arrives with the
# same extra.
_SERVER_ONLY = [
    "mirobody/server/*",
    "mirobody/server/**/*",
    "mirobody/user/*",
    "mirobody/user/**/*",
    "tests/server/*",
    "tests/server/**/*",
    "tests/user/*",
    "tests/user/**/*",
    "tests/mcp/*",
    "tests/mcp/**/*",
    "tests/utils/test_db.py",
]

# Tests that import the [parse] layer, directly or through their parent
# package. This third bucket arrived with 1.3.0: `dotenv`, `ruamel.yaml` and
# the extraction stack left base, so on a `pip install -e '.[test]'` the
# modules below abort collection the same way the server ones used to.
# Found the only way this class is ever found — in a clean clone, not in a
# long-lived venv that has everything.
_PARSE_ONLY = [
    "mirobody/pulse/file_parser/*",
    "mirobody/pulse/file_parser/**/*",
    "mirobody/utils/test_content_type.py",
    "mirobody/test_one_key_defaults.py",
    "mirobody/test_readme_numbers.py",
    "tests/pulse/file_parser/*",
    "tests/pulse/file_parser/**/*",
    "tests/pulse/aggregate/*",
    "tests/documents/*",
    "tests/documents/**/*",
    "tests/utils/test_prompts.py",
    "tests/utils/test_user_tag.py",
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
    full pass — the failure mode this whole file exists to prevent.

    It also prints how many test ROOTS exist, because the other way to get a
    short run is a clone: `tests/` is gitignored, so a contributor's checkout
    has only the evidence tests inside `mirobody/`."""
    roots = "two roots" if pathlib.Path(config.rootpath, "tests").is_dir() else "evidence tests only (no local tests/)"
    if _HAS_AGENTS and _HAS_SERVER and _HAS_PARSE:
        return f"mirobody: library + parse + server + agent tests, {roots}"

    missing = [
        name for name, present in (("the parse stack", _HAS_PARSE),
                                   ("the server stack", _HAS_SERVER),
                                   ("the agent stack", _HAS_AGENTS))
        if not present
    ]
    return (
        f"mirobody: PARTIAL RUN ({roots}) — {' and '.join(missing)} not installed, "
        "so those tests are not collected. `pip install -e '.[app,test]'` runs everything."
    )
