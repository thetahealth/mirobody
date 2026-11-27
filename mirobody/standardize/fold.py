"""The one fold from a printed name to its identity key.

`name_key` is what two readings are the same reading BY. It is stored on
every observation, it is half of the local series key of an uncoded reading,
and it is the identity a coding decision is shared under. So it is written
once, here, and every writer and reader uses this function: a second fold
that differed by one rule (a full-width bracket, a space inside a Chinese
name) would silently split one series into two.

The fold is deliberately shallow. It removes what printing adds and nothing
else: width, case, dash shape, superscripts, whitespace around CJK, trailing
punctuation. It does not translate, does not strip specimen words, does not
expand abbreviations: those are coding decisions, recorded as such.
"""

from __future__ import annotations

import re
import unicodedata

from mirobody.lexical import normalize

_CJK = "぀-ヿ㐀-䶿一-鿿豈-﫿ｦ-ﾟ가-힯"
_CJK_SPACE = re.compile(rf"(?<=[{_CJK}])\s+|\s+(?=[{_CJK}])")
_TRAILING = re.compile(r"[\s:：;；,，.。、]+$")

# Chinese medical report suffixes that should be stripped before folding.
# These appear in printed report names but are not part of the analyte identity.
# Examples: 尿酸(UA)测定 -> 尿酸, γ-谷氨酰基转移酶(GGT)测定 -> γ-谷氨酰基转移酶
_CHINESE_SUFFIXES = (
    "测定",   # measurement/test
    "检测",   # detection/test
    "定量",   # quantitative
    "定性",   # qualitative
    "测试",   # test
)

# Pattern for parenthesized abbreviations like (UA), (ALT), (GGT), etc.
_PAREN_ABBREV = re.compile(r"\s*\([A-Za-z0-9+\-]+\)\s*$")

# Pattern for trailing asterisks (sometimes used as footnote markers)
_TRAILING_ASTERISK = re.compile(r"\*+$")


def name_key(text: str) -> str:
    """NFKC, casefold, one space between Latin words, no space next to a CJK
    character, no trailing punctuation. Strips common Chinese medical report
    suffixes and parenthesized abbreviations.

        "白细胞 计数"             -> "白细胞计数"
        "Cholesterol, Total:"   -> "cholesterol, total"
        "ＨＤＬ－Ｃ"              -> "hdl-c"
        "尿酸(UA)测定"           -> "尿酸"
        "γ-谷氨酰基转移酶(GGT)测定" -> "γ-谷氨酰基转移酶"
        "甘油三酯(TG)测定*"       -> "甘油三酯"
    """
    if not text:
        return ""
    folded = normalize(unicodedata.normalize("NFKC", text)).casefold()

    # Strip Chinese medical report suffixes (测定, 检测, 定量, etc.)
    for suffix in _CHINESE_SUFFIXES:
        if folded.endswith(suffix):
            folded = folded[: -len(suffix)]
            break  # Only strip one suffix

    # Strip parenthesized abbreviations like (UA), (ALT), (GGT)
    folded = _PAREN_ABBREV.sub("", folded)

    # Strip trailing asterisks
    folded = _TRAILING_ASTERISK.sub("", folded)

    folded = _CJK_SPACE.sub("", folded)
    return _TRAILING.sub("", folded).strip()


def unit_key(text: str) -> str:
    """The fold for a unit that did not normalize to UCUM: the same rules as
    a name, so `local_key` is built from one vocabulary of folds."""
    return name_key(text)


__all__ = ["name_key", "unit_key"]