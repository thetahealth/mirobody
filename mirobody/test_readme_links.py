"""Every relative link in every README points at something that exists.

The two live READMEs carry ~50 relative targets each — module paths, docs, images,
each other. CONTRIBUTING says "if you rename a module, grep the `.md` files",
which is a rule that depends on someone remembering. This is the same rule,
enforced.

Skipped when the repo root is not present, because the READMEs are not in the
wheel and an installed package has nothing to check.
"""

from __future__ import annotations

import pathlib
import re

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
# The two LIVE editions. 繁體中文 and 日本語 are frozen at 1.4.0 under `archived/`
# (each drew under 9 unique visitors in the 14 days before the freeze, against
# 263 for zh-CN) and are not gated; `archived/README.md` says how to revive one.
_READMES = ["README.md", "README.zh-CN.md"]

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


def test_both_live_editions_exist_and_cross_link():
    """The switcher must offer a working link to the other live edition, and
    the frozen editions must still be on disk under `archived/` (unadvertised —
    the archive is deliberately not linked from the live pages).

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
    for frozen in ("README.zh-TW.md", "README.ja.md"):
        assert (_ROOT / "archived" / frozen).is_file(), f"archived/{frozen} is missing"


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
    # The demo GIF's terminal content is language-neutral by construction (real
    # CLI output), but its window label and closing caption are prose, and prose
    # in the wrong language is the same defect as an English diagram.
    expected = f"docs/images/resolve-demo{lang}.gif"
    assert expected in text, f"{name} should embed {expected}"
    # The web walkthroughs are screenshots of the running app in a chosen UI
    # language, so they localize like everything else — an English screenshot
    # in a translated README is the same defect as an English diagram.
    #
    # They are recorded one at a time, though: each is a person signing in and
    # clicking through a running stack, and the chat ones need a real
    # conversation in that language. So the rule is "use yours if it exists":
    # a localized recording that is present MUST be the one embedded, and a
    # language that has not been recorded yet still passes on the English file.
    # Adding `docs/images/<stem>.<lang>.gif` is therefore what makes this test
    # start demanding it.
    # The README shows three of the four walkthrough scenes since 1.4.1 — the
    # care circle you arrive in, ① Collect + ② Translate on an upload, ③ Answer
    # on the shared record — and links docs/walkthrough.md for the fourth. A
    # scene absent from a translation is still a silently shorter narrative, so
    # both live editions must carry the same three.
    for stem in ("care-circle-demo", "upload-demo", "ask-circle-demo"):
        localized = f"docs/images/{stem}{lang}.gif"
        if lang and (_ROOT / localized).is_file():
            assert localized in text, (
                f"{name} should embed {localized} — it exists, and an English "
                f"screenshot in a translated README is what this checks for"
            )
        else:
            assert f"docs/images/{stem}.gif" in text, f"{name} should embed {stem}.gif"


# The docs site carries en and zh only, matching the two live editions.
_DOCS_LOCALE = {"README.md": "en", "README.zh-CN.md": "zh"}
_DOCS_LINK = re.compile(r"https://docs\.mirobody\.ai/([a-z-]+)/")


@pytest.mark.parametrize("name", _READMES)
def test_docs_links_use_the_locale_of_the_readme_they_are_in(name):
    """A reader following a link out of the Chinese README should not land in
    English when a Chinese page exists."""
    text = (_ROOT / name).read_text(encoding="utf-8")
    want = _DOCS_LOCALE[name]
    found = set(_DOCS_LINK.findall(text))
    assert found, f"{name} links to no documentation pages at all"
    assert found == {want}, (
        f"{name} should link to docs.mirobody.ai/{want}/; found {sorted(found)}"
    )
