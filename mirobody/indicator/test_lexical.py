"""Golden-lock for the surface algebra.

No bundle, no data files, no network — these run even on a checkout without
`git lfs pull`. Every case here is a shape that actually appeared on a report or
in an API call and cost a resolution before this module existed.
"""

from __future__ import annotations

import pytest

from mirobody.indicator.lexical import (
    normalize,
    split_trailing_parenthetical,
    surface_variants,
    word_tokens,
)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("  Fasting  Glucose  ", "fasting glucose"),  # trim + collapse + casefold
        ("ＦＢＧ", "fbg"),  # full-width ASCII
        ("HbA1c", "hba1c"),
        ("mg/m²", "mg/m2"),  # superscript digit
        ("LDL–C", "ldl-c"),  # en-dash
        ("LDL−C", "ldl-c"),  # minus sign
        ("25‐OH", "25-oh"),  # U+2010 hyphen, what IUPAC names carry
        ("空腹　血糖", "空腹 血糖"),  # ideographic space
        ("β2-微球蛋白", "β2-微球蛋白"),  # Greek is NOT casefolded away
        ("", ""),
        (None, ""),
    ],
)
def test_normalize(raw, expected):
    assert normalize(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Fasting glucose", ["fasting", "glucose"]),
        ("fasting_glucose", ["fasting", "glucose"]),  # the API's own spelling
        ("Glucose^post CFst", ["glucose", "post", "cfst"]),
        ("HPV 16", ["hpv", "16"]),
        ("LDL-C", ["ldl", "c"]),
        ("Cholesterol, total", ["cholesterol", "total"]),
        ("空腹血糖", ["空腹血糖"]),  # a CJK run is ONE token
        ("FBG 空腹血糖", ["fbg", "空腹血糖"]),
        ("空腹血糖(GLU)", ["空腹血糖", "glu"]),
        ("", []),
    ],
)
def test_word_tokens(raw, expected):
    assert word_tokens(raw) == expected


def test_cjk_run_stays_one_token():
    """血糖 must not be a token of 空腹血糖, or the two collapse into one series."""
    assert word_tokens("空腹血糖") == ["空腹血糖"]
    assert word_tokens("血糖") == ["血糖"]


@pytest.mark.parametrize(
    "raw, expected",
    [
        # The term as written is always first, so a caller that stops at the
        # first hit keeps today's answer for today's inputs.
        ("fasting glucose", ["fasting glucose"]),
        ("fasting_glucose", ["fasting_glucose", "fasting glucose"]),
        ("ＦＢＧ", ["ＦＢＧ", "fbg"]),
        ("LDL–C", ["LDL–C", "ldl-c", "ldl c"]),
    ],
)
def test_surface_variants(raw, expected):
    assert surface_variants(raw) == expected


@pytest.mark.parametrize(
    "raw, stem, inside",
    [
        ("空腹血糖(GLU)", "空腹血糖", "GLU"),
        ("血小板计数（PLT）", "血小板计数", "PLT"),  # full-width brackets
        ("总胆固醇【TC】", "总胆固醇", "TC"),
        ("低密度脂蛋白胆固醇(LDL-C)", "低密度脂蛋白胆固醇", "LDL-C"),
        ("Glucose (fasting)", "Glucose", "fasting"),
        ("lipoprotein(a)", "lipoprotein", "a"),
        ("fasting glucose", "", ""),  # nothing to split
        ("(orphan)", "", "orphan"),  # no stem is a legal outcome
        # A parenthetical with no letter is a UNIT, not a name. `中性粒细胞(%)`
        # is a differential percentage; its stem answers the absolute-count
        # code while the value is a fraction, so stripping would turn an honest
        # miss into a confidently wrong answer.
        ("中性粒细胞(%)", "", ""),
        ("淋巴细胞(%)", "", ""),
        ("尿蛋白(+)", "", ""),
        # A unit that does contain letters still splits: the abbreviation and
        # the unit are not distinguishable here, and the resolver's own
        # disagreement rule decides what to do with it.
        ("中性粒细胞(10*9/L)", "中性粒细胞", "10*9/L"),
    ],
)
def test_split_trailing_parenthetical(raw, stem, inside):
    assert split_trailing_parenthetical(raw) == (stem, inside)


def test_split_only_takes_the_trailing_group():
    """A parenthetical in the middle is not a suffix and must be left alone."""
    assert split_trailing_parenthetical("Glucose (fasting) in serum") == ("", "")
