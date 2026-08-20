"""Let `pip install -e '.[test]'` run the tests it can.

The suite spans three install sizes. Most of it exercises the ENGINE, which by
design needs no web framework and no agent framework. Some modules exercise the
SERVER layer (`[server]`: fastapi, psycopg_pool, mandrill), and some the AGENT
layer (`[agents]`: langchain).

Without this file, a contributor who ran the command CONTRIBUTING documents —
`pip install -e '.[test]'` — got a hard collection error, not a skip:

    ImportError while importing test module '.../test_adapters.py'
    E   ModuleNotFoundError: No module named 'langchain_core'

and the whole run aborted, including the tests that had everything they needed.
A module-level `pytest.importorskip` cannot fix that: importing the test module
imports its parent package first, and that is what pulls in langchain. Deciding
at COLLECTION time is the only thing early enough.

The `[server]` half of this was missed when the file was written, and an
external reviewer found it the only way it can be found — in a CLEAN
environment, where `pip install -e '.[test]'` still aborted with

    ERROR mirobody/mcp/test_protocol.py
    E   ModuleNotFoundError: No module named 'psycopg_pool'
    ERROR mirobody/user/test_oauth_code.py
    E   ModuleNotFoundError: No module named 'mandrill'

Same mechanism, different extra: `mirobody/server/__init__` imports the pool
and `mirobody/user/__init__` imports mandrill, so importing ANY test module
under those packages needs the server extra whether that test touches it or not.
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
    # Not a layering slip: these exercise the provider PLATFORM, which manages
    # user accounts (`pulse/core/user.py` -> `mirobody.user.UserService` ->
    # psycopg_pool) and loads providers whose own SDKs are server-extra
    # dependencies (`requests_oauthlib` for Garmin, `psycopg` for pgsql). A
    # provider that links accounts to a database needs the database.
    "mirobody/pulse/gate_tests/test_format_data.py",
    "mirobody/pulse/providers/test_provider_loading.py",
]

collect_ignore_glob: list[str] = []
if not _HAS_AGENTS:
    collect_ignore_glob += _AGENT_ONLY
if not _HAS_SERVER:
    collect_ignore_glob += _SERVER_ONLY


def pytest_report_header(config) -> str:
    """Say which layers are running, so a short run is never mistaken for a
    full pass — the failure mode this whole file exists to prevent."""
    if _HAS_AGENTS and _HAS_SERVER:
        return "mirobody: engine + server + agent tests (all extras installed)"

    missing = [
        name for name, present in (("[server]", _HAS_SERVER), ("[agents]", _HAS_AGENTS))
        if not present
    ]
    return (
        f"mirobody: PARTIAL RUN — {' and '.join(missing)} not installed, so those "
        "tests are not collected. `pip install -e '.[server,agents,test]'` runs "
        "everything."
    )
