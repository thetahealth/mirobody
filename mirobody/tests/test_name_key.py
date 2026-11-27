"""`name_key` is identity, not a lookup fold.

It is half of the key `collect/observations.py` deduplicates readings on, the
first half of an uncoded reading's local series, and the key a confirmed alias
is filed under. #291 and #370 each taught it to strip what a Chinese report
prints after a name (`(ABO)`, `定性`, `测定`), and every resolver test stayed
green: those tests resolve names, and nothing pinned the key itself. These
pairs are different tests a report can print on one page, so a fold that
merges any of them drops a reading at insert time.

What a report prints after a name is a CODING decision, and it lives in the
resolver (`lexical.strip_report_marks`, `OfflineResolver._resolve_seam`).
"""

from __future__ import annotations

import pytest

from mirobody.standardize import name_key, unit_key

#: Two names that must keep two keys, and what tells them apart.
APART = [
    ("血型(ABO)检测", "血型(Rh)检测"),              # two typings
    ("尿蛋白定性", "尿蛋白定量"),                    # a dipstick and a quantitative assay
    ("HBsAg定性", "HBsAg定量"),
    ("乙肝表面抗原(定性)测定", "乙肝表面抗原(定量)测定"),
    ("GLU", "GLU(U)"),                              # blood and urine glucose on one checkup page
    ("胆固醇", "胆固醇(HDL-C)"),                     # total and HDL cholesterol
    ("尿酸(UA)测定", "尿酸"),                        # one analyte, but joined by its CODE, not its key
]


@pytest.mark.parametrize("a, b", APART)
def test_distinct_printed_names_keep_distinct_keys(a, b):
    assert name_key(a) != name_key(b)


@pytest.mark.parametrize("printed, folded", [
    ("白细胞 计数", "白细胞计数"),
    ("Cholesterol, Total:", "cholesterol, total"),
    ("ＨＤＬ－Ｃ", "hdl-c"),
    ("尿酸（UA）测定", "尿酸(ua)测定"),
])
def test_the_fold_removes_only_what_printing_adds(printed, folded):
    assert name_key(printed) == folded


@pytest.mark.parametrize("unit", ["(mmHg)", "(fL)", "(U/L)"])
def test_a_bracketed_unit_is_not_folded_away(unit):
    """`unit_key` is this fold, and an empty unit key reads as "no unit" to
    the coding path, which tells the two apart on purpose."""
    assert unit_key(unit)
