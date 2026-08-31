"""The Traditional Chinese README must be Traditional, and every language must
show the same demo.

The other README gates (links, numbers) are script-blind and single-language:
neither notices a Simplified character in the zh-TW narrative, nor a
translation whose demo section quietly fell a version behind the English
walkthrough while an already-produced localized GIF sits unreferenced. Both
failures are silent; both checks are cheap.
"""

from __future__ import annotations

import pathlib
import re

import pytest

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_READMES = {
    "README.md": None,
    "README.zh-CN.md": "zh-CN",
    "README.zh-TW.md": "zh-TW",
    "README.ja.md": "ja",
}

pytestmark = pytest.mark.skipif(
    not (_ROOT / "README.md").is_file(),
    reason="repo root not present (installed wheel ships no README)",
)

# ---------------------------------------------------------------------------
# 1. No simplified characters in the zh-TW README.
# ---------------------------------------------------------------------------

# Simplified-only characters whose traditional form differs. Curated, not
# exhaustive (no opencc dependency): every entry is unambiguous — a character
# that standard Traditional Chinese text has no reason to contain. Characters
# valid in BOTH scripts (你, 作, 依, 份, 中…) are deliberately absent.
# Hand-typed, and `test_the_simplified_list_is_actually_simplified` keeps it
# honest — `骨` used to be in here, swept in beside 驱驶 while someone typed the
# 马 radical group, and it is the SAME character in Traditional (骨骼, 骨質).
# The false positive only surfaced when a README first mentioned 骨量.
_SIMPLIFIED_ONLY = set(
    "标图谱个总览词数据记录变设红蓝绿级别题细谁说话语请谢电见觉观频账历"
    "经过还进运银钱铁钟们时间东车书学习门问闻队阶阴阳阵际陆隐离难验预顾"
    "风飞饭馆马驱驶简体汉无张归当岁币师应库废开异弃发汇圆单双报划义乐"
    "买卖万与专业丛两严亲亿从众优传伤础纸纯线组织终结给绝统继绩续维绿网"
    "罗节芦苏药虑虽装见规视览觉计订认讨让训议讯记讲许论设访证评识诊译试"
)

# Simplified strings the zh-TW README carries ON PURPOSE, verbatim:
#   - the language switcher's own name for the zh-CN edition;
#   - the multilingual resolve demo, whose whole point is that a
#     Simplified-Chinese term lands on the same LOINC code;
#   - prose that names the Simplified script itself.
# `中性粒细胞` joins `血红蛋白` for the same reason: the code block demonstrates
# that a Simplified input resolves, and rewriting the demo into Traditional
# would delete the thing being shown. The four READMEs run identical calls
# by design — `test_readme_examples` pins that — so the input stays Hans in
# all of them and only the comment beside it is translated.
_DELIBERATE_SIMPLIFIED = (
    "简体中文", "血红蛋白", "中性粒细胞", "简体", "简→繁", "繁→简",
)


def test_the_simplified_list_is_actually_simplified():
    """Every char in `_SIMPLIFIED_ONLY` must be the Hans side of a real fold
    pair, checked against the DERIVED table in `mirobody/zh_fold.py`.

    The list is typed by hand and the gate that uses it fails a README, so a
    character that is shared between the scripts blocks correct Traditional
    prose until someone argues with the test. One did: `骨`.
    """
    from mirobody.zh_fold import _TABLE

    hans = {chr(v) for v in _TABLE.values()}
    shared = sorted(c for c in _SIMPLIFIED_ONLY if c not in hans)
    assert not shared, (
        f"{''.join(shared)!r} in _SIMPLIFIED_ONLY, but nothing folds TO them — "
        "they are the same character in both scripts, so flagging them rejects "
        "correct zh-TW text"
    )


def test_zh_tw_readme_contains_no_stray_simplified_characters():
    text = (_ROOT / "README.zh-TW.md").read_text(encoding="utf-8")
    for allowed in _DELIBERATE_SIMPLIFIED:
        text = text.replace(allowed, "")

    offenders: list[str] = []
    for lineno, line in enumerate(text.splitlines(), 1):
        hits = sorted({ch for ch in line if ch in _SIMPLIFIED_ONLY})
        if hits:
            offenders.append(f"line {lineno}: {''.join(hits)!r} in {line.strip()[:60]!r}")

    assert not offenders, (
        "Simplified characters in README.zh-TW.md (outside the deliberate "
        "demo terms):\n" + "\n".join(offenders)
        + "\nIf one is intentional, add the full phrase to "
        "_DELIBERATE_SIMPLIFIED with a reason."
    )


# ---------------------------------------------------------------------------
# 2. Every language shows the same demo, in its own language where possible.
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
