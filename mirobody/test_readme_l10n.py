"""Every live README edition must show the same demo, in its own language
where a localized asset exists, and name only modules that import.

The other README gates (links, numbers) are single-language: neither notices a
translation whose demo section quietly fell a version behind the English
walkthrough while an already-produced localized GIF sits unreferenced. The
failure is silent; the check is cheap.

Until 1.4.1 this file also checked that README.zh-TW.md contained no Simplified
characters. That edition is frozen under `archived/` and no longer gated, so the
check went with it; `archived/README.md` says what brings it back.
"""

from __future__ import annotations

import pathlib
import re

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_READMES = {
    "README.md": None,
    "README.zh-CN.md": "zh-CN",
}

pytestmark = pytest.mark.skipif(
    not (_ROOT / "README.md").is_file(),
    reason="repo root not present (installed wheel ships no README)",
)

# ---------------------------------------------------------------------------
# 1. Every language shows the same demo, in its own language where possible.
# ---------------------------------------------------------------------------

_IMG = re.compile(r'(?:src="|\]\()docs/images/([^")\s]+)')
_LANG_SUFFIX = re.compile(r"^(?P<stem>.+?)(?:\.(?P<lang>zh-CN|zh-TW|ja))?\.(?P<ext>gif|svg|png)$")


def _images(name: str) -> list[re.Match]:
    text = (_ROOT / name).read_text(encoding="utf-8")
    matches = []
    for ref in _IMG.findall(text):
        m = _LANG_SUFFIX.match(ref)
        assert m, f"{name}: unparseable image reference {ref!r}"
        matches.append(m)
    return matches


def test_all_languages_reference_the_same_set_of_images():
    """A translation missing a GIF the English README shows is a silently
    older demo — exactly the drift CONTRIBUTING says must be visible."""
    stem_sets = {
        name: {(m["stem"], m["ext"]) for m in _images(name)}
        for name in _READMES
    }
    reference = stem_sets["README.md"]
    for name, stems in stem_sets.items():
        missing = reference - stems
        extra = stems - reference
        assert not missing and not extra, (
            f"{name} image set differs from README.md — "
            f"missing: {sorted(missing)}, extra: {sorted(extra)}"
        )


@pytest.mark.parametrize("name,lang", [(n, l) for n, l in _READMES.items() if l])
def test_translations_use_localized_assets_where_they_exist(name, lang):
    """If a localized variant of an asset exists on disk, the matching
    translation must use it instead of the English one — a produced-but-
    unreferenced localized GIF is finished work no reader ever sees."""
    wrong = []
    for m in _images(name):
        if m["lang"] == lang:
            continue  # already localized
        localized = f"{m['stem']}.{lang}.{m['ext']}"
        if (_ROOT / "docs" / "images" / localized).is_file():
            wrong.append(f"{m.group(0)} → should be {localized}")
    assert not wrong, f"{name} ignores existing localized assets:\n" + "\n".join(wrong)


# ---------------------------------------------------------------------------
# 2. Every language names the same modules, and every module it names exists.
# ---------------------------------------------------------------------------

_DOTTED = re.compile(r"`(mirobody(?:\.[a-z_][a-z0-9_]*)+)`")

#: Dotted `mirobody.*` names that are NOT import paths, so `find_spec` is the
#: wrong question to ask about them. All three are entry-point GROUP names —
#: what a plugin distribution declares under `[project.entry-points]`.
_ENTRY_POINT_GROUPS = frozenset({
    "mirobody.providers",
    "mirobody.tools",
    "mirobody.agents",
})


def _dotted_names(name: str) -> set[str]:
    return set(_DOTTED.findall((_ROOT / name).read_text(encoding="utf-8")))


def test_no_readme_names_a_module_that_does_not_exist():
    """A translation is documentation a reader types into a REPL.

    1.4.0 moved the pure modules into `mirobody.kernel`, the English README
    followed, and the three translations kept `mirobody.series` /
    `mirobody.quality` — names that raise `ModuleNotFoundError`. The other
    l10n gates could not see it: one reads characters, the other images.
    """
    import importlib.util

    missing = []
    for name in _READMES:
        for dotted in sorted(_dotted_names(name)):
            if dotted in _ENTRY_POINT_GROUPS:
                continue
            try:
                found = importlib.util.find_spec(dotted) is not None
            except Exception:
                found = False
            if not found:
                missing.append(f"{name}: `{dotted}`")
    assert not missing, (
        "README names a module that cannot be imported:\n  "
        + "\n  ".join(missing)
        + "\nIf it is a new entry-point group rather than a module, add it to "
          "_ENTRY_POINT_GROUPS."
    )


@pytest.mark.parametrize("name", [n for n in _READMES if n != "README.md"])
def test_translations_name_no_module_the_english_readme_does_not(name):
    """The rename that produced the bug above was applied to one file of four.
    Divergence in either direction is the signal: a translation citing a name
    the English README does not is either stale or ahead of it."""
    extra = sorted(_dotted_names(name) - _dotted_names("README.md"))
    assert not extra, (
        f"{name} names modules README.md does not: {extra} — "
        "the translation is out of step with the English original"
    )
