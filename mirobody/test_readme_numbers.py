"""Every exact number the root READMEs quote, checked against what produces it.

``test_readme_links.py`` guards the links and the per-language diagrams; this
file gates the figures — alias counts, graph sizes, the resolver score. They
are quoted in four files and derived in none, so nothing short of a gate keeps
them honest.

The check is presence of today's value, not parsing of the prose: for each
claim the README must contain the number the code currently produces, written
the way the READMEs write numbers (thousands separated). A bundle rebuild that
moves an alias count therefore turns these red, which is the point — those
counts are quoted in four files and derived in none.

Two claims cannot be checked by presence alone and get a regex instead: the
pulse-indicator count, because a bare ``300`` also matches inside ``4,300+``;
and the resolver score, which is a ratio.

Deliberately not checked: the approximations ("~310 UCUM families", "4,000+",
"two years", "50 pages"). They are round by intent, and pinning them would
turn every rebuild into a README edit for no gain in truth.

Also not checked, for a different reason: the install footprint (2 packages /
49 MB for the library). The package COUNT is a property of this project's own
metadata and would be worth gating, but the megabytes move with every upstream
release and differ per platform and installer, so a gate on the pair would fail
for reasons that have nothing to do with this repo.
"""

from __future__ import annotations

import gzip
import json
import pathlib
import re

import pytest

from ruamel.yaml import YAML

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_READMES = ["README.md", "README.zh-CN.md", "README.zh-TW.md", "README.ja.md"]


def _text(name: str) -> str:
    return (_ROOT / name).read_text(encoding="utf-8")


def _fixture() -> dict:
    path = _ROOT / "mirobody" / "demo" / "care_circle_demo.json.gz"
    with gzip.open(path) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def live() -> dict[str, int]:
    """The figures, read from the artifacts and code that define them.

    Module-scoped because the concept graph is 22 MB and takes a few seconds
    to mmap; every claim that depends on it shares the one load.
    """
    from mirobody.indicator.concept_graph import ConceptGraph
    from mirobody.zh_fold import _TABLE
    from mirobody.pulse.standardize import StandardIndicator
    from mirobody.test_engine_coverage import CASES, MUST_NOT_RESOLVE

    graph = ConceptGraph.get(str(_ROOT / "mirobody" / "res" / "fhir_concept_graph.bin")).stats()

    # `res/aliases_src/{lang}.tsv`, not the bundle: the byte-identical
    # `aliases/{lang}.tsv` members are gone (they had drifted from the curated
    # files by four rows), and these loose ones are what the resolver reads.
    # `{lang}_curated.tsv` is excluded — its rows are merged into `{lang}.tsv`
    # by the lexicon build, so counting both double-counts.
    alias_dir = _ROOT / "mirobody" / "res" / "aliases_src"
    aliases = {
        p.stem: sum(1 for _ in p.open("rb"))
        for p in alias_dir.glob("*.tsv")
        if not p.stem.endswith("_curated")
    }

    series = _fixture()["series"]

    return {
        "concept-graph nodes": graph["bridge_nodes"],
        "cross-vocabulary edges": graph["bridge_edges"],
        "source ids": graph["sibling_ids"],
        "multilingual aliases": sum(aliases.values()),
        "Chinese aliases": aliases["zh"],
        "Japanese aliases": aliases["ja"],
        "zh-Hant fold table": len(_TABLE),
        "demo indicators": len({r["indicator"] for r in series}),
        # Not presence-checkable (substring of 4,300+); see test_pulse_count.
        "_pulse indicators": len(StandardIndicator),
        # The benchmark's own denominator, not a re-derivation of it.
        "_resolver cases": len(CASES) + len(MUST_NOT_RESOLVE),
    }


@pytest.mark.parametrize("name", _READMES)
def test_every_exact_figure_is_the_current_one(name: str, live: dict[str, int]):
    """Report every stale figure in this language at once, not one per test.

    When a bundle rebuild shifts things the useful output is the whole list of
    what to edit, in one place.
    """
    text = _text(name)
    stale = [
        f"{what}: code says {value:,}, {name} does not contain that number"
        for what, value in live.items()
        if not what.startswith("_") and f"{value:,}" not in text
    ]
    assert not stale, "\n".join(stale)


# A number attached to the word "pulse" on either side of it — "300 standard
# pulse indicators" in English, "標準pulse指標300種" in Japanese.
_NEAR_PULSE = re.compile(r"(\d[\d,]*)[^\n\d]{0,16}pulse|pulse[^\n\d]{0,16}(\d[\d,]*)", re.I)


@pytest.mark.parametrize("name", _READMES)
def test_pulse_count(name: str, live: dict[str, int]):
    found = {
        int(a.replace(",", "") or b.replace(",", ""))
        for a, b in _NEAR_PULSE.findall(_text(name))
    }
    assert found, f"{name} no longer states how many standard pulse indicators there are"
    assert found == {live["_pulse indicators"]}, (
        f"{name} says {sorted(found)} standard pulse indicators; "
        f"StandardIndicator has {live['_pulse indicators']}"
    )


# Only perfect scores: "32/94" is the honest historical baseline and stays.
# Digit lookaround rather than \b: Japanese writes "今日は197/197" and CJK
# counts as \w, so there is no word boundary before the number to anchor to.
_RATIO = re.compile(r"(?<!\d)(\d[\d,]*)/(\d[\d,]*)(?!\d)")


@pytest.mark.parametrize("name", _READMES)
def test_resolver_score(name: str, live: dict[str, int]):
    perfect = {
        int(a.replace(",", ""))
        for a, b in _RATIO.findall(_text(name))
        if a == b
    }
    assert perfect, f"{name} no longer quotes the resolver coverage score"
    assert perfect == {live["_resolver cases"]}, (
        f"{name} claims a perfect {sorted(perfect)}; the benchmark scores "
        f"{live['_resolver cases']} cases"
    )


_EMAIL = re.compile(r"[\w.+-]+@mirobody\.ai")


# The canonical source-tree version — read from the FILE, not imported:
# an editable install's metadata freezes whatever was current at install
# time, so `mirobody.__version__` can lag the checkout it sits in.
_VERSION_SENTINEL = re.compile(r'or "(\d+\.\d+\.\d+)"')


def test_the_changelog_names_the_version_the_tree_calls_itself():
    """The CHANGELOG's top entry names the release being prepared and
    `mirobody/__init__.py` carries the version the tree calls itself; they
    must agree, or the release notes describe a number nothing will ship
    under.

    This used to check a third place: the READMEs told readers to run from a
    source checkout "until 1.2.1 reaches PyPI", because the published 1.0.62
    wheel was an empty shell. 1.2.1 published on 2026-08-23, the four READMEs
    dropped the warning, and this half went with it — as the previous version
    of this docstring said it should."""
    src = (_ROOT / "mirobody" / "__init__.py").read_text(encoding="utf-8")
    version = _VERSION_SENTINEL.search(src).group(1)

    changelog = (_ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    top = re.search(r"^## (\d+\.\d+\.\d+)", changelog, re.M).group(1)
    assert top == version, (
        f"CHANGELOG's top entry is {top}; mirobody/__init__.py says {version}"
    )


@pytest.mark.parametrize("name", _READMES)
def test_the_demo_credential_is_one_the_server_accepts(name: str):
    """A wrong account here breaks the first thing a reader does."""
    # ruamel, not PyYAML: ruamel.yaml is a declared base dependency and the
    # parser utils/config/config.py itself uses, so this reads config.yaml the
    # way the server does. An undeclared `import yaml` would pass in a dev venv
    # (where PyYAML sits by transitive accident) and die in a minimal `.[test]`
    # install.
    codes = YAML(typ="safe").load(_ROOT / "config.yaml")["EMAIL_PREDEFINE_CODES"]
    text = _text(name)
    for email in set(_EMAIL.findall(text)):
        assert email in codes, (
            f"{name} tells the reader to sign in as {email}, which is not in "
            f"config.yaml's EMAIL_PREDEFINE_CODES ({sorted(codes)})"
        )
        assert codes[email] in text, (
            f"{name} names {email} but not its code {codes[email]!r}"
        )


_DEMO_FILE = re.compile(r"mirobody/demo/([\w.-]+\.(?:pdf|json\.gz))")


@pytest.mark.parametrize("name", _READMES)
def test_the_demo_file_the_readme_hands_you_is_shipped(name: str):
    """The upload step names a path; the wheel has to actually carry it."""
    referenced = set(_DEMO_FILE.findall(_text(name)))
    assert referenced, f"{name} no longer names the demo lab report"
    held_out = _fixture()["held_out_exam"]
    for filename in referenced:
        assert (_ROOT / "mirobody" / "demo" / filename).exists(), (
            f"{name} points at mirobody/demo/{filename}, which is not in the tree"
        )
        if filename.endswith(".pdf"):
            assert held_out in filename, (
                f"{name} hands the reader {filename}, but the panel held out of the "
                f"seeded history is dated {held_out} — the demo arc only works if "
                "the upload is the exam the database is missing"
            )
