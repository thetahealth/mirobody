"""Every relative link in every README points at something that exists.

The four READMEs carry ~50 relative targets each — module paths, docs, images,
each other. CONTRIBUTING says "if you rename a module, grep the `.md` files",
which is a rule that depends on someone remembering. This is the same rule,
enforced.

It is not hypothetical rot: this repo has shipped a README documenting a CLI
subcommand that exits with `invalid choice`, and a `pytest tests/ -m mcp`
command for a `tests/` directory that never existed. A dead link is the same
failure with less noise.

Skipped when the repo root is not present, because the READMEs are not in the
wheel and an installed package has nothing to check.
"""

from __future__ import annotations

import pathlib
import re

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_READMES = ["README.md", "README.zh-CN.md", "README.zh-TW.md", "README.ja.md"]

pytestmark = pytest.mark.skipif(
    not (_ROOT / "README.md").is_file(),
    reason="repo root not present (installed wheel ships no README)",
)

# `](target)` for markdown links and `src="target"` for the HTML <img> tags the
# hero diagrams use. Anchors and absolute URLs are somebody else's problem.
_MD_LINK = re.compile(r"\]\((?!https?:|mailto:|#)([^)\s]+)")
_HTML_SRC = re.compile(r'src="((?!https?:)[^"]+)"')


def _targets(text: str) -> set[str]:
    found = set(_MD_LINK.findall(text)) | set(_HTML_SRC.findall(text))
    return {t.split("#")[0] for t in found if t.split("#")[0]}


@pytest.mark.parametrize("name", _READMES)
def test_every_relative_link_resolves(name):
    path = _ROOT / name
    if not path.is_file():
        pytest.skip(f"{name} not present")
    missing = sorted(t for t in _targets(path.read_text(encoding="utf-8"))
                     if not (_ROOT / t).exists())
    assert not missing, f"{name} links to nonexistent paths: {missing}"


def test_all_four_languages_exist_and_cross_link():
    """The switcher must offer four working links from every language.

    A translated README that links a sibling which was never written is worse
    than not offering the switcher: it looks finished and 404s.
    """
    present = [n for n in _READMES if (_ROOT / n).is_file()]
    assert present == _READMES, f"missing translations: {set(_READMES) - set(present)}"

    for name in _READMES:
        text = (_ROOT / name).read_text(encoding="utf-8")
        others = [o for o in _READMES if o != name]
        for other in others:
            assert f"]({other})" in text, f"{name} does not link to {other}"
        # and the current language is plain text, not a link to itself
        assert f"]({name})" not in text, f"{name} links to itself in the switcher"


@pytest.mark.parametrize("name", _READMES)
def test_a_translated_readme_uses_its_own_diagrams(name):
    """An English diagram in a localized README is the first thing a reader
    sees and the one part they may not be able to read."""
    path = _ROOT / name
    if not path.is_file():
        pytest.skip(f"{name} not present")
    text = path.read_text(encoding="utf-8")
    lang = "" if name == "README.md" else "." + name.removeprefix("README.").removesuffix(".md")
    for stem in ("where-your-data-comes-from", "your-care-circle"):
        expected = f"docs/images/{stem}{lang}.svg"
        assert expected in text, f"{name} should embed {expected}"
