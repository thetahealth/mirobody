"""The library contract the READMEs sell, checked rather than asserted.

Four claims, each of which has been false at some point in this project's
history and none of which any other test covers:

1. ``pip install mirobody`` is the vocabulary layer on numpy alone. The gate is
   not the dependency list — that is easy to keep tidy and still wrong — but
   what an import actually reaches for.
2. ``from mirobody import resolve`` works, and every name the package
   advertises resolves. (``test_public_surface.py`` checks ``__all__`` against
   ``hasattr`` repo-wide; this checks the lazy root package specifically,
   because a PEP 562 ``__getattr__`` can advertise a name and still raise.)
3. Importing the package is cheap. The lazy re-exports exist so that
   ``import mirobody`` does not load numpy or open the 15 MB bundle, and a
   single eager import at the top of ``__init__`` would undo that silently.
4. ``py.typed`` ships. Without it PEP 561 says every type checker ignores the
   annotations in this package, which is the state it was in until 1.3.0.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import mirobody

_ROOT = pathlib.Path(__file__).resolve().parents[1] / "mirobody"


def _in_subprocess(code: str) -> str:
    """Run *code* in a fresh interpreter and return its stdout.

    A fresh one, because pytest has already imported numpy and half of PyPI
    into this process — the question is what a USER's first import pulls in.
    """
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


# The DELTA an import causes, not the absolute set: a venv can start with
# third-party namespace packages already in `sys.modules` (a `.pth` from any
# `google-cloud-*` install puts `google` there), and blaming those on mirobody
# would make this fail for a reason that has nothing to do with the package.
_DELTA = """
import sys


def tops():
    return {{
        m.split(".")[0] for m in sys.modules
        if m.split(".")[0] not in sys.stdlib_module_names
        and not m.startswith("_") and m.split(".")[0] != "mirobody"
    }}


before = tops()
{imports}
print(",".join(sorted(tops() - before)))
"""


def test_importing_the_package_reaches_for_nothing():
    """`import mirobody` must not pull a single third-party package.

    Not even numpy: it is a base dependency because `OfflineResolver` needs it,
    and it is imported inside `__init__` of that class so that merely importing
    the library costs nothing.
    """
    got = [p for p in _in_subprocess(_DELTA.format(imports="import mirobody")).split(",") if p]
    assert got == [], (
        f"import mirobody now pulls in {got}. Something at the top of a module "
        "on the import path stopped being lazy."
    )


def test_the_vocabulary_modules_reach_for_nothing():
    """units / lexical / value_scale / zh_fold are pure Python and stay so.

    This is the half of the library a consumer imports directly, and it is the
    reason base can be one line. A dataframe import sneaking into a unit table
    would put pandas back in front of everyone who wants `unit_family`.
    """
    imports = (
        "import mirobody.units, mirobody.lexical, "
        "mirobody.value_scale, mirobody.zh_fold"
    )
    got = [p for p in _in_subprocess(_DELTA.format(imports=imports)).split(",") if p]
    assert got == [], f"the vocabulary layer now pulls in {got}"


def test_every_advertised_root_name_actually_resolves():
    """`__all__` on a PEP 562 package is a promise `__getattr__` must keep."""
    missing = []
    for name in mirobody.__all__:
        try:
            getattr(mirobody, name)
        except AttributeError:
            missing.append(name)
    assert not missing, f"mirobody.__all__ advertises {missing}, which __getattr__ does not serve"


def test_the_headline_import_works_offline():
    """The one line the README leads with, in a fresh interpreter."""
    out = _in_subprocess(
        "from mirobody import resolve; r = resolve('血红蛋白'); print(r.loinc, r.method)"
    )
    assert out == "718-7 lexical", out


def test_the_shipped_bundle_says_which_corpus_it_is():
    """A corpus version that can be asserted against, distinct from the package
    version. Consumers generating build-time artifacts from the bundle and
    pinning the package at runtime have no other way to check the two agree."""
    version = mirobody.BUNDLE_VERSION
    assert version, "the shipped bundle carries no VERSION member"
    # loinc-<release>+<YYYY.MM.DD>-<12 hex>. The LOINC release is the half that
    # answers "which vocabulary am I resolving with", and the LOINC license
    # (§9) requires every copy to carry it, so it is not decoration.
    release, plus, rest = version.partition("+")
    assert release.startswith("loinc-") and plus, f"no LOINC release in {version!r}"
    date, _, digest = rest.rpartition("-")
    assert len(date.split(".")) == 3 and len(digest) == 12, (
        f"expected loinc-<release>+YYYY.MM.DD-<12 hex>, got {version!r}"
    )


def test_py_typed_ships():
    """PEP 561. Without the marker file the annotations are invisible to every
    type checker, however carefully they are written."""
    assert (_ROOT / "py.typed").is_file(), (
        "mirobody/py.typed is missing — consumers get `Any` for every symbol"
    )


def test_the_default_install_is_one_dependency():
    """The README says `pip install mirobody` is 2 packages. That is a promise
    about THIS FILE, not about a venv someone measured once, so it is checked
    here — the measurement drifts with upstream, the declaration does not.
    """
    import tomllib

    with (_ROOT.parent / "pyproject.toml").open("rb") as fh:
        project = tomllib.load(fh)["project"]
    assert project["dependencies"] == ["numpy"], (
        f"base dependencies are {project['dependencies']}; anything beyond numpy "
        "is a package every consumer of the vocabulary layer has to download"
    )


def test_the_extras_are_parse_agent_app_and_two_development_ones():
    """`[server]`, `[agents]` and `[cn]` were removed in 1.3.0 — the first two
    merged into `[app]`, and `[cn]` was not an axis at all. 1.4.0 added one
    axis back: `[agent]` is the agent layer as a library, for a consumer that
    runs its own deepagents agent and never starts this server; `[app]` is
    `[parse]` + `[agent]` + the server. A further extra is a further decision
    for everyone installing this, so the set is a gate."""
    import tomllib

    with (_ROOT.parent / "pyproject.toml").open("rb") as fh:
        extras = set(tomllib.load(fh)["project"]["optional-dependencies"])
    assert extras == {"parse", "agent", "app", "test", "indicator-build"}, (
        f"extras are {sorted(extras)}; expected parse/agent/app for users and "
        "test/indicator-build for development"
    )


# ── mirobody.bundle: the build-time surface ─────────────────────────────────
def test_bundle_module_exports_what_it_promises():
    """`_bundle` is internal; this module is the stable name over it.

    Raised by a downstream consumer: generating a seed from LOINC needed the
    axis table, and the only route was a module whose underscore says it moves
    without notice.
    """
    from mirobody import bundle
    assert set(bundle.__all__) == {
        "ALIAS_SRC_DIR", "BUNDLE_PATH", "RES_DIR", "alias_source_files",
        "bundle_version", "is_lfs_pointer", "list_members",
        "load_alias_sources", "load_axis", "read_member",
    }
    for name in bundle.__all__:
        assert getattr(bundle, name) is not None, name


def test_bundle_is_not_in_the_runtime_surface():
    """It must not join `mirobody.__all__` — `import mirobody` stays lazy."""
    import mirobody

    assert "bundle" not in mirobody.__all__


def test_bundle_axis_reads_the_runtime_members_not_the_csv():
    """`load_axis` must work from a wheel, where `loinc_axis.csv` is gated out.

    The reader parses `axis_fields.bin` / `axis_index.npz`, which are runtime
    members and ship. Documented in `mirobody/bundle.py`; asserted here because
    the docstring is only as good as the member list it names.
    """
    from mirobody._bundle import AXIS_BLOB_MEMBER, AXIS_INDEX_MEMBER
    from mirobody.bundle import load_axis

    # Read from the wheel GATE, not from `build_backend` — that module imports
    # setuptools at import time, which pip's build isolation need not leave in
    # the runtime venv the minimal-install CI job uses.
    from scripts.check_wheel_data import BUNDLE_FORBIDDEN, BUNDLE_REQUIRED

    assert AXIS_BLOB_MEMBER in BUNDLE_REQUIRED
    assert AXIS_INDEX_MEMBER in BUNDLE_REQUIRED
    assert "loinc_axis.csv" in BUNDLE_FORBIDDEN

    axis, by_code, by_name = load_axis()
    assert len(by_code) > 90_000
