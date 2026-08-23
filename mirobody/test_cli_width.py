"""The resolve table lines up when the terms are not all ASCII.

`mirobody resolve` exists to show four languages landing on one code, printed
as a table. Alignment must count DISPLAY width, not characters: each CJK
character occupies two terminal columns, so character-count padding
(`str.ljust`, `f"{term:<{n}}"`) misaligns every non-ASCII row — in the one
command whose entire point is the multilingual table.
"""

from mirobody.cli import _pad, _width


def test_ascii_is_one_column_per_character():
    assert _width("LDL cholesterol") == 15


def test_cjk_is_two_columns_per_character():
    assert _width("血红蛋白") == 8
    assert _width("ヘモグロビン") == 12


def test_mixed_width_counts_each_half_correctly():
    # Full-width parentheses would count 2; these are ASCII, so 4*2 + 5 = 13.
    assert _width("空腹血糖(GLU)") == 13


def test_padding_makes_columns_equal_in_display_width():
    terms = ["LDL cholesterol", "血红蛋白", "ヘモグロビン", "空腹血糖(GLU)"]
    n = max(_width(t) for t in terms)
    assert {_width(_pad(t, n)) for t in terms} == {n}


def test_padding_never_truncates_an_over_wide_term():
    assert _pad("ヘモグロビン", 4) == "ヘモグロビン"
