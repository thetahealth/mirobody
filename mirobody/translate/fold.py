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


def name_key(text: str) -> str:
    """NFKC, casefold, one space between Latin words, no space next to a CJK
    character, no trailing punctuation.

        "白细胞 计数"          -> "白细胞计数"
        "Cholesterol, Total:" -> "cholesterol, total"
        "ＨＤＬ－Ｃ"            -> "hdl-c"
    """
    if not text:
        return ""
    folded = normalize(unicodedata.normalize("NFKC", text)).casefold()
    folded = _CJK_SPACE.sub("", folded)
    return _TRAILING.sub("", folded).strip()


def unit_key(text: str) -> str:
    """The fold for a unit that did not normalize to UCUM: the same rules as
    a name, so `local_key` is built from one vocabulary of folds."""
    return name_key(text)


__all__ = ["name_key", "unit_key"]
