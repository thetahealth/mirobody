"""Readings straight off a report's tables: no model between the OCR and ② Translate.

GLM-OCR's table mode returns HTML tables whose columns are already split; a
spreadsheet arrives as a markdown table, a CSV as delimited lines. When a table
carries a name and a result column, its rows are read here by their header text
and handed to the store in the shape the LLM extractor produces. Measured on one
real 11-page checkup: one HTML table held several panels, each opened by a
single-cell section row and a repeated header, the reference column BEFORE the
unit column, and a page's first row continuing the previous page's panel with no
header of its own. So columns are mapped per header, never by position; a
name or value column is kept only when the cells under it agree; a column
whose header is blank, or a flag word over printed ranges, is typed by its
cells; a header that names the name column twice is two panels side by side
(双栏); and a table with no header of its own borrows the last one only for
rows that look like readings under it. Headers are compared with Traditional
folded to Simplified (`zh_fold`), so `檢驗項目 | 結果` reads as `检验项目 | 结果`.

An OCR model's grid is not the printed one. Measured on the OCR benchmark
(benchmarks/local_ocr, 2026-10-07), PaddleOCR-VL-1.6 returns a whole page as
one grid: every panel's title and header inside it, a header split over two
rows or several header words in one cell, a page's opening rows above the
next panel's header, and a row's empty cells moved (`ALT | 23 | | U/L | 7~40 |
02 |` under `… | Methodology | Status | Unit | Normal Range | Lab`). So spans
keep their columns (`_Tables`), a split header is joined (`_rejoined`,
`_split_cells`), rows above a table's first header borrow it, a row whose
cells contradict their columns is laid again by content (`_seat`), and a
table with no header at all is typed by its cells when nothing about it is
ambiguous (`_by_content`). GLM-OCR and MinerU2.5 make the same moves less
often; each is read the same way.

A row is a reading only when it looks like one: a result that is a number, an
ordinal (`1+`) or a nominal word (阴性, negative), beside a unit the unit engine
knows and a range (or nothing there). A blood pressure printed as a pair
(`Blood Pressure | 123/78`) is two readings, split and named as the journal
splits one. A row of patient details (姓名, 年龄, 送检医生, or `审核者：王五` in
one cell) is skipped; any other row of two or more cells is left to the model.
Measured on 2026-10-06, before these checks: a footer `检验者|李四|审核者|王五`
and a page-2 `姓名|张三|性别|男` were stored as readings, a medication table was
read under the lab table's header, and the right half of a side-by-side panel
was dropped with nothing left for the model.

What is not done here: a value is kept as printed, less a trailing flag
(`7.2↑`, which goes to the flag); the status is only the printed flag, and the
range is stored beside it for a reader to compare; a table whose rows carry
different dates is a series, not one report, and is left to the model.
"""

from __future__ import annotations

import bisect
import csv
import difflib
import functools
import html
import itertools
import re
import unicodedata
from dataclasses import dataclass, field
from html.parser import HTMLParser

from mirobody import translate
from mirobody.collect.sentence import blood_pressure
from mirobody.translate.parse import KIND_ABSENT
from mirobody.units import normalize_unit
from mirobody.zh_fold import fold_to_hans

EXTRACTOR = "rules:table@v1"

#: Header words per column, compared after `_key` (lowercased, Traditional
#: folded to Simplified, no spaces, colons or brackets), so a bilingual
#: `单位(Unit)` or `单位 Unit` is `单位unit` and a Traditional `檢驗項目` is
#: `检验项目`. Measured on the 2026-10-06 local-model corpus: with `单位(Unit)`,
#: `正常范围值` and a slip's bare `参考` missing, every reading of those tables
#: was stored with no unit or no range (hemoglobin 140 then cannot be coded);
#: with `检测结果`, `化验结果`, `本次结果`, `数值`, `检查名称`, `Test Item`,
#: `Measured`, `REF.RANGE` and the Traditional headers missing, 49 of the
#: corpus' 70 documents went to the model, which reads them less exactly.
_HEADERS: dict[str, tuple[str, ...]] = {
    "name": ("项目名称", "检验项目", "检查项目", "检测项目", "化验项目", "测定项目", "项目", "项目名", "中文名称",
             "名称", "检查名称", "检查项目名称", "检验项目名称", "指标", "指标名称", "analyte", "test", "tests",
             "testname", "testitem", "item", "items", "parameter", "name", "indicator", "component",
             "observation", "metric"),
    "value": ("结果", "检验结果", "检查结果", "检测结果", "化验结果", "报告结果", "本次结果", "测定结果", "测定值",
              "测量值", "检测值", "数值", "result", "results", "value", "measured", "measuredvalue", "inrange"),
    # A second result column, read when the first is empty: `Items | In Range |
    # Out of Range | Reference Interval`, the layout of a US lab's report, puts
    # each result under one of the two. None of the three OCR models' tables
    # of it was read before this (OCR benchmark, 2026-10-07).
    "out": ("outofrange",),
    "unit": ("单位", "单位unit", "unit", "units"),
    "ref": ("参考值", "参考范围", "参考区间", "参考", "参考值范围", "正常值", "正常范围", "正常范围值", "正常参考值",
            "生物参考区间", "reference", "referencerange", "referenceinterval", "range", "ref", "refrange",
            "normalrange"),
    "flag": ("标志", "标记", "提示", "提示信息", "异常", "结果提示", "判断", "flag", "状态", "status", "abnormal"),
    "date": ("采样时间", "采集时间", "检验时间", "日期", "collected", "collectiondate", "date"),
}
#: Header words that name the name column in one report and the value column
#: in another: `Test Item | Measurement | Unit` prints results under it, a
#: device export's `Date | Measurement | Result | Unit` prints names. It is the
#: value column beside a name word and the name column without one, and the
#: cells under it must agree (`_by_cells`).
_EITHER = ("measurement", "measure")
_FLAG_HIGH = {"↑", "h", "高", "偏高", "high", "hh", "↑↑"}
_FLAG_LOW = {"↓", "l", "低", "偏低", "low", "ll", "↓↓"}
_FLAG_NORMAL = {"n", "正常", "normal"}

#: Row labels that name a part of a report, not a measurement: the model reads
#: those rows with their section around them. `是否异常 | 是` closes an ECG
#: panel: it says the section is abnormal, and was stored as a reading.
_LABELS = {"检查描述", "检查结论", "描述", "结论", "所见", "检查所见", "诊断", "小结", "建议", "意见", "是否异常",
           "description", "conclusion", "impression", "findings", "comment", "remarks"}

#: Row labels of the patient and the paperwork: skipped, and nothing left for a
#: model to read (a date among them is still the report's date).
_ADMIN = {
    "姓名", "性别", "年龄", "出生日期", "科室", "科别", "病区", "床号", "病历号", "住院号", "门诊号", "就诊号",
    "登记号", "样本号", "标本号", "标本类型", "样本类型", "标本", "条码号", "申请医生", "送检医生", "开单医生",
    "申请科室", "送检科室", "检查者", "检验者", "检验医师", "检验人", "审核者", "审核人", "审核医师", "报告者", "报告人",
    "报告医生", "操作者", "采样时间", "采集时间", "接收时间", "检验时间", "报告时间", "报告日期", "送检日期",
    # The date labels `_DATE_LABELS` reads were not all here: a slip's
    # `采样日期：20221205 | 报告日期：20221205` row, which PaddleOCR-VL-1.6 put
    # in the result table, was left to the model as an unread row (OCR
    # benchmark, 2026-10-07).
    "采样日期", "采集日期", "检查日期", "检验日期", "检查时间",
    "临床诊断", "备注", "电话", "地址", "身份证号", "医院", "页码",
    # The report's own tally of its abnormal rows (`异常项目数 | 7`), printed
    # inside the table: four corpus documents had it stored as a reading.
    "异常项目数",
    "patient", "patientname", "subject", "sex", "gender", "age", "dob", "dateofbirth", "birthdate", "birthday",
    "department", "ward", "bed", "mrn", "patientid", "sampleid", "specimenid", "specimen", "sampletype",
    "barcode", "physician", "doctor", "orderedby", "orderingphysician", "requestedby", "received", "reported",
    "reportdate", "verifiedby", "approvedby", "technician", "phone", "tel", "fax", "email", "address",
}

#: Date labels, most specific first: the day blood was drawn is the reading's
#: day, a report's printing day is the fallback.
_DATE_LABELS = (
    ("采样时间", "采集时间", "采样日期", "采集日期", "collected", "collection", "collectiondate"),
    ("检验时间", "检查日期", "检验日期", "检查时间", "testdate", "examdate"),
    ("报告日期", "报告时间", "reportdate", "reported"),
    ("日期", "date"),
)
_DATE_VALUE = r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?"
_LABELLED_DATE = re.compile(
    r"(?P<label>" + "|".join(sorted({w for group in _DATE_LABELS for w in group}, key=len, reverse=True))
    + r")\s*[:：]?\s*" + _DATE_VALUE, re.I)
#: A date this close after one of these words is a birthday, not the report's.
_BIRTH = re.compile(r"(?:出生|birth|dob|born)\W*$", re.I)

_NUMBER = re.compile(r"^[<>≤≥]?\s*[-+]?\d+(?:\.\d+)?$")
_RANGE = re.compile(r"^\s*([-+]?\d+(?:\.\d+)?)\s*(?:-{1,2}|~|–|—|至)\s*([-+]?\d+(?:\.\d+)?)\s*$")
_BOUND = re.compile(r"^\s*([<>≤≥]|<=|>=)\s*([-+]?\d+(?:\.\d+)?)\s*$")
#: A flag printed after the value in its own cell, when the table has no flag
#: column: right after the number (`7.2↑`, `3.1 L`); after a unit, glued to the
#: number or not (`7.49mmol/L偏高`, `1.69 g/L↑`, `50.5% H`); or after a result
#: word (`阳性 偏高`, `Positive H`). A letter needs a space before it, so the
#: `L` of `mmol/L` is not read as low. Measured on the 2026-10-07 small-model
#: eval: with a digit required right before the arrow, a check-up book's
#: summary rows `1.69 g/L↑` and `0.58 g/L↓` were stored as narratives, unit and
#: arrow in the value; on the corpus's text-layer books, `阳性 偏高`, `Positive
#: H` and `++ H` kept their flags in the value the same way.
_TRAILING_FLAG = re.compile(r"^(.*?\d)\s*(↑↑|↓↓|↑|↓|偏高|偏低|高|低|HH|LL|H|L)$"
                            r"|^(.*?\S)\s*(↑↑|↓↓|↑|↓|偏高|偏低)$|^(.*?\S)\s+(HH|LL|H|L)$")
#: A range or bound, then a unit after it in the same cell: `0-5.0 ng/mL`, or
#: one that starts with a digit after a space (`4.0-10.0 10^9/L`), never glued
#: digits (`3.5-5.51` is a range).
_REF_UNIT = re.compile(
    r"^\s*(\(?[<>≤≥]?=?\s*[-+]?\d+(?:\.\d+)?(?:\s*(?:-{1,2}|~|–|—|至)\s*[-+]?\d+(?:\.\d+)?)?\)?)"
    r"(?:\s*([^\d\s&].*?)|\s+(\d[\d.]*[^\d\s.].*?))\s*$")
#: A result cell that says the test was not done (`尿葡萄糖 | 未做`): no result.
_NOT_DONE = {"未做"}
_RESULT_KINDS = ("quantity", "ordinal", "nominal")
#: The longest text result read without a model ("淡黄色", "微浑"); a longer
#: one is a sentence, and a sentence is the model's to read.
_SHORT_TEXT = 8


def _key(cell: str) -> str:
    return re.sub(r"[\s:：()（）._\-/]", "", fold_to_hans(cell)).lower()


class _Tables(HTMLParser):
    """Every <table> as a list of rows, a row as its cell texts laid on the
    table's grid: a cell spanning columns is followed by empty cells, and a
    row under a cell spanning rows has an empty cell in its place, so every
    cell keeps its column. Measured on the OCR benchmark (2026-10-07): a
    header of PaddleOCR-VL's whose `Lab` cell spanned two columns read one
    column short of its rows, and every cell after it under the wrong word."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._span = (1, 1)
        self._above: dict[int, int] = {}  # column -> rows a cell above still covers, this one included
        self._new: dict[int, int] = {}

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.tables.append([])
            self._above = {}
        elif tag == "tr" and self.tables:
            self._row, self._new = [], {}
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []
            a = dict(attrs)
            self._span = (_span(a.get("colspan")), _span(a.get("rowspan")))

    def _covered(self):
        while self._row is not None and len(self._row) in self._above:
            self._row.append("")

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._row is not None and self._cell is not None:
            self._covered()
            columns, rows = self._span
            start = len(self._row)
            self._row.append(" ".join("".join(self._cell).split()))
            self._row.extend([""] * (columns - 1))
            if rows > 1:
                self._new.update(dict.fromkeys(range(start, start + columns), rows - 1))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self._covered()
            if any(self._row):
                self.tables[-1].append(self._row)
            self._above = {c: n - 1 for c, n in self._above.items() if n > 1} | self._new
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def _span(value: str | None) -> int:
    try:
        return min(max(int(value or 1), 1), 100)
    except ValueError:
        return 1


def _html_tables(text: str) -> list[list[list[str]]]:
    parser = _Tables()
    parser.feed(text)
    return [t for t in parser.tables if t]


def _markdown_row(line: str) -> list[str] | None:
    line = line.strip()
    if not (line.startswith("|") and line.count("|") >= 3):
        return None
    return [html.unescape(c.strip()) for c in line.strip("|").split("|")]


def _markdown_tables(text: str) -> list[list[list[str]]]:
    tables, current = [], []
    for line in text.splitlines():
        cells = _markdown_row(line)
        if cells is not None:
            if not all(re.fullmatch(r":?-{3,}:?", c) for c in cells if c):
                current.append(cells)
        elif current:
            tables.append(current)
            current = []
    if current:
        tables.append(current)
    return tables


def _delimited_tables(text: str) -> list[list[list[str]]]:
    """A CSV or TSV document as one table, when every line splits into the same
    number of fields and the first line is a header naming a name and a result."""
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return []
    for delimiter in (",", "\t", ";"):
        rows = [[c.strip() for c in row] for row in csv.reader(lines, delimiter=delimiter)]
        if len(rows[0]) >= 2 and len({len(r) for r in rows}) == 1 and _header(rows[0]):
            return [rows]
    return []


def _column(cell: str) -> str | None:
    key = _key(cell)
    if key in _EITHER:
        return "either"
    for column, words in _HEADERS.items():
        if key in words:
            return column
    return None


def _roles(row: list[str]) -> list[str | None]:
    """Each header cell's column, a `_EITHER` word decided by the row: the
    value column when another cell names the name column, else the name."""
    roles = [_column(cell) for cell in row]
    either = "value" if "name" in roles else "name"
    return [either if role == "either" else role for role in roles]


def _header(row: list[str]) -> list[dict[str, int]] | None:
    """The column groups of a header row, one `{column: index}` per panel, or
    None when `row` names no name-and-result pair. A name column named again
    opens a second panel beside the first; a column before the first name
    (a CSV's leading `date`) belongs to the first panel. A `_EITHER` word
    takes its column only when no other word in its panel names that one."""
    roles = _roles(row)
    starts = [i for i, column in enumerate(roles) if column == "name"]
    if not starts:
        return None
    groups: list[dict[str, int]] = [{} for _ in starts]
    either = {i for i, cell in enumerate(row) if _column(cell) == "either"}
    for i in sorted(range(len(row)), key=lambda i: i in either):
        if roles[i] is None:
            continue
        g = groups[max(0, bisect.bisect_right(starts, i) - 1)]
        g.setdefault(roles[i], i)
    groups = [g for g in groups if {"name", "value"} <= g.keys()]
    return groups or None


def _split_cells(row: list[str]) -> list[str]:
    """A header row with each cell that names several columns split into one
    cell per column, or `row` itself when no cell does. Measured on the OCR
    benchmark (2026-10-07): PaddleOCR-VL-1.6 wrote a check-up book's
    `Measurement | Methodology | Status | Unit` as one cell over its two
    columns of results and units. A cell is split only when two or more of
    its words are column words, so `Test Item` or `Normal Range` stays whole."""
    out: list[str] = []
    for cell in row:
        words = cell.split()
        if len(words) < 2 or _column(cell):
            out.append(cell)
            continue
        parts, i = [], 0
        while i < len(words):
            n = next(n for n in (3, 2, 1) if i + n <= len(words) and (n == 1 or _column(" ".join(words[i:i + n]))))
            parts.append(" ".join(words[i:i + n]))
            i += n
        out.extend(parts if sum(1 for part in parts if _column(part)) >= 2 else [cell])
    return out if len(out) > len(row) else row


def _joined(rest: list[str], name_row: list[str]) -> list[str] | None:
    """`rest` with the one cell of `name_row`, a name column word, in its
    column, when that makes a header of `rest`: a header the OCR split over
    two rows. Measured on the OCR benchmark (2026-10-07): PaddleOCR-VL-1.6 put
    a panel's title in the header row, `血常规 | 英文名称 | 化验结果 | 参考值`,
    and `检查项目` alone on the row under it, and the 22 rows of the panel went
    unread for want of a name column."""
    filled = [(i, c) for i, c in enumerate(name_row) if c.strip()]
    if len(filled) != 1 or _column(filled[0][1]) != "name" or filled[0][0] >= len(rest):
        return None
    i, word = filled[0]
    if _column(rest[i]) or _header(rest):
        return None
    joined = [*rest[:i], word, *rest[i + 1:]]
    return joined if _header(joined) else None


def _rejoined(table: list[list[str]]) -> list[list[str]]:
    """`table` with every header split over two rows (`_joined`) as one row."""
    out: list[list[str]] = []
    i = 0
    while i < len(table):
        after = table[i + 1] if i + 1 < len(table) else None
        joined = after is not None and (_joined(table[i], after) or _joined(after, table[i]))
        out.append(joined or table[i])
        i += 2 if joined else 1
    return out


def _by_content(table: list[list[str]]) -> list[dict[str, int]] | None:
    """The one panel of a table with no header, typed by its cells alone, or
    None unless every column a reading needs is unambiguous: one column of
    numeric results, one of ranges (a lab table prints them; a list of doses
    or prices does not), names to the left of the results, at most one unit
    column, and results on the scale of their ranges in four rows of five.
    A row number, a code repeated on every row and a mostly empty column are
    no column. Two result columns (this result and the last) are the model's.
    Measured on the OCR benchmark (2026-10-07): a check-up book's second page
    continues the first page's panel with no header of its own, and a photo
    of that page alone held 20 readings no rule could read."""
    body = [r for r in table if sum(1 for c in r if c.strip()) >= 3 and not _admin(next(c for c in r if c.strip()))]
    if len(body) < 3:
        return None
    columns: dict[str, list[int]] = {"name": [], "value": [], "ref": [], "unit": []}
    for i in range(max(len(r) for r in body)):
        cells = [r[i].strip() for r in body if i < len(r) and r[i].strip()]
        if 5 * len(cells) < 4 * len(body) or len(set(cells)) == 1:
            continue
        if all(c.isdigit() for c in cells) and all(int(b) == int(a) + 1 for a, b in itertools.pairwise(cells)):
            continue
        kinds = [_kinds(c) for c in cells]
        if all("ref" in k and _RANGE_IN.search(c) for k, c in zip(kinds, cells, strict=True)):
            columns["ref"].append(i)
        elif all("value" in k and re.search(r"\d", c) for k, c in zip(kinds, cells, strict=True)):
            columns["value"].append(i)
        elif _unit_cells(cells):
            columns["unit"].append(i)
        elif all("text" in k for k in kinds):
            columns["name"].append(i)
    if len(columns["value"]) != 1 or len(columns["ref"]) != 1 or len(columns["unit"]) > 1:
        return None
    value = columns["value"][0]
    names = [i for i in columns["name"] if i < value]
    if not names:
        return None
    panel = {"name": names[-1], "value": value, "ref": columns["ref"][0]}
    if columns["unit"]:
        panel["unit"] = columns["unit"][0]
    near = [n for r in body if max(value, panel["ref"]) < len(r) and (n := _near(r[value], r[panel["ref"]])) is not None]
    return [panel] if near and 5 * sum(near) >= 4 * len(near) else None


def _near(value: str, ref: str) -> bool | None:
    """Whether a result is within ten times its range (or bound), None when
    either is not a number: how a lab result sits beside its range."""
    number = re.match(r"\s*[<>≤≥]?\s*([-+]?\d+(?:\.\d+)?)", value)
    bounds = [float(b) for b in re.findall(r"\d+(?:\.\d+)?", (_ref_unit(ref) or (ref,))[0])]
    if not number or not bounds:
        return None
    v = float(number.group(1))
    return min(bounds) / 10 <= v <= max(bounds) * 10 if max(bounds) > 0 else v <= 10


def _header_at(table: list[list[str]], i: int) -> tuple[list[dict[str, int]] | None, int, bool] | None:
    """When row `i` is a header: (its panels as the cells under it confirm
    them, or None when they contradict every one; its width; whether its
    cells were split, so every row under it is laid by content). None when
    row `i` is no header."""
    row = _split_cells(table[i])
    header = _header(row)
    if not header:
        return None
    split = row is not table[i]
    body = table[i + 1:]
    if split:
        body = [_seat(r, header, len(row)) or [] for r in itertools.takewhile(lambda r: not _header(r), body)]
    return _by_cells(row, header, body) or None, len(row), split


def _admin(cell: str) -> bool:
    """Whether `cell` is a label of the patient or the paperwork, printed alone
    (`审核者`) or with its value after a colon (`审核者：王五`). Measured on the
    2026-10-06 local-model corpus: a photo's OCR put the signer line
    `检查者：武娟琳 | 检验者：段松洋` inside the result table, and the whole-cell
    comparison let it through as a reading."""
    return _key(re.split(r"[:：]", cell, maxsplit=1)[0]) in _ADMIN


#: A printed range or bound inside a reference cell.
_RANGE_IN = re.compile(r"[-+]?\d+(?:\.\d+)?\s*(?:-{1,2}|~|–|—|至)\s*[-+]?\d+(?:\.\d+)?|[<>≤≥]=?\s*[-+]?\d+(?:\.\d+)?")


def _range_cell(cell: str) -> bool:
    """A reference cell and nothing else: ranges or bounds with no other number
    beside them (`(<3.00)`, `90-139/60-89`, `男:4.30--5.80 女:3.80--5.10`), or
    an expected word (`阴性`). A result, a date or a flag is not one."""
    if _RANGE_IN.search(cell):
        return not re.search(r"\d", _RANGE_IN.sub("", cell))
    return not _printed_flag(cell) and translate.parse_value(cell, "").value_kind in ("nominal", "ordinal")


def _unit_cells(cells: list[str]) -> bool:
    """Whether a column of cells is a unit column: none is a flag, a number,
    a range or a result word, and most are units the engine reads. Most, not
    all: a coagulation panel's `mg/L FEU` is a unit the engine does not know,
    beside `s` and `g/L` that it does."""
    if any(_printed_flag(c) or _NUMBER.match(c) or _RANGE_IN.search(c)
           or translate.parse_value(c, "").value_kind in _RESULT_KINDS for c in cells):
        return False
    return 2 * sum(1 for c in cells if normalize_unit(c) is not None) > len(cells)


def _results(cells: list[str]) -> int:
    """How many cells read as a result: a number, an ordinal or a nominal word,
    less a trailing flag."""
    return sum(1 for c in cells if translate.parse_value(_split_flag(c, "")[0], "").value_kind in _RESULT_KINDS
               or _value_parts(c))


def _by_cells(row: list[str], groups: list[dict[str, int]], body: list[list[str]]) -> list[dict[str, int]]:
    """`groups` as the cells under `row` confirm them, up to the next header.

    A panel's name and value columns are kept only when their cells agree: at
    least half of the value column reads as results, and no more than half of
    the name column is numbers or ranges. A header word says what a column
    usually holds, not what this report put there: `Measurement` heads names
    in one export and results in another, and `数值` or `Items` could head
    anything. A panel the cells contradict is dropped and its rows go to the
    model.

    A panel's missing range or unit column is found by its cells, when its
    header does not name it: under a blank header, cells that are all units
    are the unit column and cells that are all ranges the range column; under
    a flag word (`提示`, `结果提示`), cells that are all ranges are the range
    column, not flags. Measured on the 2026-10-06 local-model corpus: a book
    printed `检测项目 | 测定值 | (blank) | 单位(Unit)`, another put its ranges under
    `结果提示`, and the rules stored those ranges as nothing."""
    body = list(itertools.takewhile(lambda r: not _header(r), body))
    body = [r for r in body if sum(1 for c in r if c) > 1 and not _admin(first := next(c for c in r if c))
            and _key(first) not in _LABELS]
    starts = [i for i, role in enumerate(_roles(row)) if role == "name"]

    def column(i: int) -> list[str]:
        return [r[i].strip() for r in body if i < len(r) and r[i].strip()]

    typed = []
    for g in groups:
        g = dict(g)
        names, values = column(g["name"]), column(g["value"])
        if 2 * _results(values) < len(values):
            continue
        if 2 * sum(1 for c in names if translate.parse_value(c, "").value_kind == "quantity"
                   or _RANGE_IN.fullmatch(c.strip())) > len(names):
            continue
        for i, head in enumerate(row):
            owner = starts[max(0, bisect.bisect_right(starts, i) - 1)]
            blank, flag = not _key(head), g.get("flag") == i
            if owner != g["name"] or not (blank or flag):
                continue
            cells = [r[i].strip() for r in body if i < len(r) and r[i].strip()]
            if not cells:
                continue
            if "ref" not in g and all(_range_cell(c) for c in cells) and any(_RANGE_IN.search(c) for c in cells):
                g["ref"] = i
                if flag:
                    del g["flag"]
            elif blank and "unit" not in g and _unit_cells(cells):
                g["unit"] = i
        typed.append(g)
    return typed


def _is_unit(cell: str) -> bool:
    """A unit and nothing else: not `<5.18 mmol/L` or `10.0/L`, which the unit
    engine reads past their numbers, and not `1`, which it reads as unity."""
    return (normalize_unit(cell) is not None and not _NUMBER.match(cell)
            and translate.parse_value(cell, "").value_kind != "quantity")


def _unit_like(unit: str) -> bool:
    return not unit or normalize_unit(unit) is not None


def _ref_like(ref: str) -> bool:
    """A printed range, bound or expected word ("阴性"); not a name or a ward."""
    if not ref or re.search(r"\d", ref):
        return True
    return translate.parse_value(ref, "").value_kind in _RESULT_KINDS


def _printed_flag(flag: str) -> str:
    f = flag.strip().lower()
    if f in _FLAG_HIGH:
        return "high"
    if f in _FLAG_LOW:
        return "low"
    return "normal" if f in _FLAG_NORMAL else ""


def status_of(value: str, ref: str, flag: str = "") -> str:
    """high / low / normal from a printed flag, else by comparing `value` with
    the range printed beside it; "" when neither says. For a READER (a file
    summary): the store keeps only the printed flag."""
    printed = _printed_flag(flag)
    if printed:
        return printed
    number = re.fullmatch(r"\s*([-+]?\d+(?:\.\d+)?)\s*", value)
    if not number:
        return ""
    v = float(number.group(1))
    if m := _RANGE.match(ref):
        low, high = float(m.group(1)), float(m.group(2))
        return "high" if v > high else "low" if v < low else "normal"
    if m := _BOUND.match(ref):
        op, bound = m.group(1), float(m.group(2))
        if op in ("<", "≤", "<="):
            return "high" if (v >= bound if op == "<" else v > bound) else "normal"
        return "low" if (v <= bound if op == ">" else v < bound) else "normal"
    return ""


def _split_flag(value: str, ref: str) -> tuple[str, str]:
    """`(value, flag)` with a flag printed after the value moved out. A bare
    `L` is a flag only when the printed range says the value is low: `1.5 L`
    of urine is litres. A bare `H` after a word is a flag only after a result
    word (`Positive H`), never after any other (`Vitamin H`)."""
    m = _TRAILING_FLAG.match(value.strip())
    if not m:
        return value, ""
    number, flag = next((m.group(i).strip(), m.group(i + 1)) for i in (1, 3, 5) if m.group(i) is not None)
    lead = re.match(r"\s*[-+]?\d+(?:\.\d+)?", number)
    if flag in ("L", "LL") and status_of(lead.group(0) if lead else number, ref) != "low":
        return value, ""
    if flag in ("H", "HH") and not lead and translate.parse_value(number, "").value_kind not in ("ordinal", "nominal"):
        return value, ""
    return number, flag


def _ref_unit(ref: str) -> tuple[str, str] | None:
    """`(range, unit)` when a reference cell prints the unit after the range: a
    slip with no unit column does (`<7.00&ng/mL`, `125-350&10^9/L`, `0-5.0
    ng/mL`). After an `&` the range may be cut short (`<8.…&mg/L`). A `±`
    between a range and a unit is that `&` misread: PaddleOCR-VL-1.6 read
    every `&` of a slip as `\\pm` (OCR benchmark, 2026-10-07), and a tolerance
    is a number, never a unit."""
    for separator in ("&", "±"):
        head, found, tail = ref.rpartition(separator)
        if found and re.search(r"\d", head) and _is_unit(tail.strip()):
            return head.strip(), tail.strip()
    m = _REF_UNIT.match(ref)
    unit = m.group(2) or m.group(3) if m else None
    return (m.group(1).strip(), unit.strip()) if unit and normalize_unit(unit) is not None else None


_VALUE_HEAD = re.compile(r"^\s*([<>≤≥]?\s*[-+]?\d+(?:\.\d+)?\s*(?:↑↑|↓↓|↑|↓)?)\s+(\S.*?)\s*$")


def _value_parts(cell: str) -> tuple[str, str, str] | None:
    """`(value, range, unit)` of a result cell that also holds its row's range,
    and maybe its unit, after the number: PaddleOCR-VL-1.6 read a slip's
    `报告结果 | 正常值` columns, printed `2.873 (0.270 - 4.200)&mIU/L`, as one
    cell (OCR benchmark, 2026-10-07)."""
    m = _VALUE_HEAD.match(cell)
    if not m:
        return None
    value, rest = m.group(1).replace(" ", ""), m.group(2)
    if not re.match(r"[(（]?\s*[<>≤≥]?=?\s*[-+]?\d", rest):
        return None  # `5.4 mmol/L 3.9-6.1`: a unit first is no range tail
    if split := _ref_unit(rest):
        return value, split[0], split[1]
    if _RANGE_IN.search(rest) and _range_cell(rest):
        return value, rest, ""
    return None


# --- a row the OCR laid out of line with its header -------------------------------------
#
# Measured on the OCR benchmark (benchmarks/local_ocr, 2026-10-07): an OCR model
# puts a row's empty cells where it likes. Under a check-up book's `Test Item |
# Measurement | Methodology | Status | Unit | Normal Range | Lab`, PaddleOCR-VL-1.6
# wrote `ALT | 23 | | U/L | 7~40 | 02 |` (the two empty cells as one, the
# spare one at the end) and GLM-OCR `Urea | 4.57 | | mmol/L | 2.60--7.50 | | 02`.
# Read by position, the unit sat under `Status`, the range under `Unit`, and
# the lab code `02` was stored as Urea's reference range. The filled cells are
# still in their printed order; only the gaps moved. So a row whose cells
# contradict the columns they are under is laid again, its filled cells in
# order under the columns their content fits, and read only when exactly one
# layout fits best.

_TYPED = frozenset({"unit", "ref", "flag"})


def _kinds(cell: str) -> frozenset[str]:
    """What a cell's content says it is: a "unit", a "ref" (a range, a bound
    or an expected word), a "flag", a "value" (a result), or "text" (a name, a
    code, a unit the engine does not know) when it says none of these."""
    kinds = set()
    if _printed_flag(cell):
        kinds.add("flag")
    if _is_unit(cell):
        kinds.add("unit")
    else:
        if _range_cell(cell) or _ref_unit(cell):
            kinds.add("ref")
        if translate.parse_value(_split_flag(cell, "")[0], "").value_kind in _RESULT_KINDS or _value_parts(cell):
            kinds.add("value")
    if re.fullmatch(_DATE_VALUE, cell):
        kinds = {"date"}
    return frozenset(kinds or {"text"})


def _layout(width: int, groups: list[dict[str, int]]) -> list[tuple[int, str | None]]:
    """Each column's panel and the column it is in that panel (None for a
    column no header word names). A column before a panel's name column
    belongs to the panel on its left, the first panel for the first ones."""
    starts = [g["name"] for g in groups]
    out: list[tuple[int, str | None]] = [(max(0, bisect.bisect_right(starts, i) - 1), None) for i in range(width)]
    for p, g in enumerate(groups):
        for column, i in g.items():
            if i < width:
                out[i] = (p, column)
    return out


def _fit(kinds: frozenset[str], own: set[str], column: str | None) -> int | None:
    """How a cell of `kinds` sits under `column`: 1 as its own kind, 0 as
    something it may be (a name that reads as a unit, `K`; a unit the engine
    does not know), None when it cannot. A unit, range or flag never sits in a
    column no header names while its panel has a column of its own for it."""
    if column is None:
        return None if kinds & own & _TYPED else 0
    if column == "out":
        column = "value"
    if column == "name":
        if "text" in kinds:
            return 1
        return 0 if kinds & {"value", "unit"} else None
    if column in kinds:
        return 1
    return 0 if "text" in kinds and column != "date" else None


def _misplaced(row: list[str], groups: list[dict[str, int]]) -> bool:
    """Whether a cell contradicts the column it is under: a cell that is
    plainly another column's (a range under the unit header, a unit under the
    range or flag header, a number under any of the three), or a unit, range
    or flag in a column no header names while its panel's own column for it
    is empty."""
    layout = _layout(len(row), groups)
    for i, cell in enumerate(row):
        cell = cell.strip()
        if not cell:
            continue
        panel, column = layout[i]
        kinds = _kinds(cell)
        if column in _TYPED and column not in kinds and kinds & (_TYPED | {"value"}):
            return True
        if column is None and any(not _cell(row, groups[panel], k) for k in kinds & _TYPED & set(groups[panel])):
            return True
    return False


def _seat(row: list[str], groups: list[dict[str, int]], width: int) -> list[str] | None:
    """`row`'s filled cells, in their order, under the header columns their
    content fits best, or None when no layout fits or two fit equally well
    and disagree on what a column holds. Cells under unnamed columns are
    dropped: nothing reads them."""
    cells = [c.strip() for c in row if c.strip()]
    layout = _layout(max(width, len(row)), groups)
    own = [set(g) for g in groups]
    kinds = [_kinds(c) for c in cells]
    n, m = len(cells), len(layout)

    @functools.cache
    def best(k: int, j: int) -> tuple[int, frozenset] | None:
        # The best score of cells k.. under columns j.., and the layouts (as
        # (column, cell) pairs of the named columns) that reach it, two at most.
        if k == n:
            return 0, frozenset({()})
        if m - j < n - k:
            return None
        options = [found] if (found := best(k, j + 1)) else []
        panel, column = layout[j]
        fit = _fit(kinds[k], own[panel], column)
        if fit is not None and (rest := best(k + 1, j + 1)):
            here = ((j, k),) if column is not None else ()
            options.append((fit + rest[0], frozenset(here + r for r in rest[1])))
        if not options:
            return None
        top = max(score for score, _ in options)
        layouts = frozenset().union(*(found for score, found in options if score == top))
        return top, frozenset(itertools.islice(layouts, 2))

    found = best(0, 0)
    if not found or len(found[1]) != 1:
        return None
    seated = [""] * m
    for j, k in next(iter(found[1])):
        seated[j] = cells[k]
    return seated


@dataclass
class _Row:
    """What one table row came to."""

    readings: list[dict[str, str]] = field(default_factory=list)
    unread: bool = False


def _cell(row: list[str], columns: dict[str, int], column: str) -> str:
    i = columns.get(column)
    return row[i].strip() if i is not None and i < len(row) else ""


def _reading(row: list[str], columns: dict[str, int], *, borrowed: bool) -> list[dict[str, str]] | str | None:
    """One panel of one row: its readings (two for a blood pressure printed as
    a pair), `"admin"` for a patient-details row, `"empty"` for a panel with
    nothing in it, or None for a row a model should read."""
    name, value, out = _cell(row, columns, "name"), _cell(row, columns, "value"), _cell(row, columns, "out")
    if value and out:
        return None  # a result in both columns: which one is printed is the model's to read
    value = value or out
    if not name and not value:
        return "empty"
    if _admin(name) or _column(name) == "name":
        # `Name | Nora Ho`: the patient's name printed inside the table, under
        # the word a header uses for its name column.
        return "admin"
    if re.fullmatch(r"(?i)rs\d+|i\d{4,}", name):
        return "admin"  # a genotype call: the genomics upload reads those
    if not name or not value or _NUMBER.match(name) or _header(row) or re.search(_DATE_VALUE, value):
        # A date is never a result: a running footer laid under a table's
        # columns (`Page 2 of 11 | Printed 2026-02-14 11:37:08`) was read as a
        # value of 2026 (corpus p004_2026-02-14_e10a, text-layer tables).
        return None
    if _key(name) in _LABELS or _column(value):
        return None
    if _key(value) in _NOT_DONE:
        return "empty"
    ref, unit, flag = _cell(row, columns, "ref"), _cell(row, columns, "unit"), _cell(row, columns, "flag")
    if not ref and (parts := _value_parts(value)):
        value, ref, unit = parts[0], parts[1], unit or parts[2]
    if not unit and (split := _ref_unit(ref)):
        ref, unit = split
    if unit and normalize_unit(unit) is None and _RANGE_IN.search(unit):
        # A range under the unit header that `_seat` left there: the row is
        # out of line with its header in a way no one layout explains.
        return None
    if not flag:
        value, flag = _split_flag(value, ref)
        if not unit and re.search(r"\d\s*L{1,2}$", value):
            return None  # low, or litres: the range does not say, so a model reads it
    parsed = translate.parse_value(value, unit)
    if parsed.value_kind == "quantity" and not unit and parsed.unit_ucum:
        # GLM-OCR puts a vital's unit in the value cell ("72.0kg") when the
        # table has no unit column; the number has to stand alone to be charted.
        m = re.match(r"^([<>≤≥]?\s*[-+]?\d+(?:\.\d+)?)\s*(\S.*)$", value)
        if m and normalize_unit(m.group(2)) is not None:
            value, unit = m.group(1).replace(" ", ""), m.group(2).strip()
    if not _ref_like(ref):
        return None
    pair = blood_pressure(_key(name), value)
    if parsed.value_kind in _RESULT_KINDS or pair:
        if borrowed and not _unit_like(unit):
            return None
    elif (borrowed or len(value) > _SHORT_TEXT or re.search(r"\d|[:：]", value) or not _unit_like(unit)
          or normalize_unit(value) is not None):
        # A word under the table's own header ("淡黄色") is a result; under a
        # borrowed one, or beside a cell that is not a unit, it is a name. A
        # word with a colon is a label and its value (`检验者：段松洋`), and a
        # unit alone (`MCV | fl`) is a result whose number was not printed.
        return None
    reading = {
        "original_indicator": name,
        "value": value,
        "unit": unit,
        "reference_range": ref,
        "detection_method": "laboratory",
        "status": _printed_flag(flag),
        "notes": "",
        "_date": _cell(row, columns, "date"),
    }
    if pair:
        # `parse_value` leaves a pair narrative, so the row went to the model,
        # which missed it: 3 of 4 cloud models (DeepSeek V4.1 Flash, Claude
        # Sonnet 5.5, GPT-6 Luna) dropped a check-up book's `Blood Pressure |
        # 123/78 | mmHg | 90-139/60-89` (benchmarks/local_models, 2026-10-07).
        return [{**reading, "original_indicator": n, "value": v, "unit": unit or "mmHg", "reference_range": r}
                for (n, v), r in zip(pair, _pair_range(ref), strict=True)]
    return [reading]


def _pair_range(ref: str) -> tuple[str, str]:
    """A blood pressure's printed range for each of its readings: a paired
    range (`90-139/60-89`) split, any other given to both as printed."""
    halves = [h.strip() for h in ref.split("/")]
    if len(halves) == 2 and all(_RANGE_IN.fullmatch(h) for h in halves):
        return halves[0], halves[1]
    return ref, ref


def _read_row(row: list[str], groups: list[dict[str, int]], *, borrowed: bool) -> _Row:
    out = _Row()
    for columns in groups:
        got = _reading(row, columns, borrowed=borrowed)
        if isinstance(got, list):
            out.readings.extend(got)
        elif got is None:
            out.unread = True
    if borrowed and out.unread:
        # A borrowed header is a guess about this table; one panel it does not
        # fit says it is a different table, and the model reads all of it.
        return _Row(unread=True)
    return out


def _dates(text: str) -> list[tuple[int, str]]:
    """`(rank, "YYYY-MM-DD[ HH:MM[:SS]]")` for every labelled date in `text`, rank 0 the most specific.
    Traditional labels (`報告時間`) are read as their Simplified form: the fold
    maps one character to one, so positions in `text` hold."""
    text = fold_to_hans(text)
    out = []
    for m in _LABELLED_DATE.finditer(text):
        if _BIRTH.search(text[max(0, m.start() - 12):m.start()]):
            continue
        label = _key(m.group("label"))
        rank = next((r for r, group in enumerate(_DATE_LABELS) if label in group), len(_DATE_LABELS))
        y, mo, d, hh, mm, ss = m.group(2, 3, 4, 5, 6, 7)
        day = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
        out.append((rank, f"{day} {hh}:{mm}:{ss or '00'}" if hh else day))
    return out


def _pages(text: str) -> list[str]:
    parts = re.split(r"^--- page \d+ ---$", text, flags=re.M)
    return [p for p in parts if p.strip()] or [text]


_TABLE_BLOCK = re.compile(r"<table\b.*?</table>", re.S | re.I)


def _confirmed(reading: dict[str, str], elsewhere: str) -> bool:
    """Whether the page says this value outside the OCR's table: in the text
    layer of a born-digital page, or in the OCR's own text pass. A digit the
    table pass misread is then caught by the other copy."""
    if not elsewhere.strip():
        return True
    return re.search(r"(?<![\d.])" + re.escape(reading["value"]) + r"(?![\d.])", elsewhere) is not None


def table_indicators(text: str) -> tuple[list[dict[str, str]], str, int]:
    """(readings, examination date or "", unread) from every table in `text`
    that has a name and a result column. `unread` counts the table rows of two
    or more cells that no rule read (a row that does not look like a reading,
    a table with no usable header, a value the page does not confirm): the
    caller's cue that a model should read the document too. Patient-details
    rows are not counted. ([], "", 0) when there is no table at all."""
    readings: list[dict[str, str]] = []
    unread = 0
    groups: list[dict[str, int]] | None = None
    width, split = 0, False
    pages = _pages(text)
    markup = any(_html_tables(p) or _markdown_tables(p) for p in pages)
    for page in pages:
        elsewhere = _TABLE_BLOCK.sub(" ", page)
        sources = [(t, True) for t in _html_tables(page)] + [(t, False) for t in _markdown_tables(page)]
        if not markup:
            sources += [(t, False) for t in _delimited_tables(page)]
        for table, from_ocr in sources:
            table_readings: list[dict[str, str]] = []
            table_unread = 0
            table = _rejoined(table)
            borrowed = groups is not None
            if groups is None:
                # Rows above a table's first header, with no header before
                # it: a page that opens mid-panel, the panel's header on the
                # page before. The OCR laid them on that header's grid, so
                # they borrow it, as a later table borrows an earlier one.
                ahead = next((h for k in range(len(table)) if (h := _header_at(table, k))), None)
                if ahead and ahead[0]:
                    groups, width, split = ahead
                    borrowed = True
                elif not ahead and (typed := _by_content(table)):
                    groups, width, split, borrowed = typed, max(len(r) for r in table), False, True
            for i, row in enumerate(table):
                cells = [c for c in row if c]
                if len(cells) == 1 or _admin(cells[0]):
                    continue
                if found := _header_at(table, i):
                    # A header whose every panel its cells contradict reads
                    # nothing: its rows, and a table that would borrow it,
                    # are the model's.
                    groups, width, split = found
                    borrowed = False
                    continue
                if groups is None or (borrowed and len(row) > width):
                    table_unread += 1
                    continue
                if split or _misplaced(row, groups):
                    row = _seat(row, groups, width)
                    if row is None:
                        table_unread += 1
                        continue
                got = _read_row(row, groups, borrowed=borrowed)
                confirmed = [r for r in got.readings if not from_ocr or _confirmed(r, elsewhere)]
                table_readings.extend(confirmed)
                table_unread += int(got.unread or len(confirmed) < len(got.readings))
            days = {r["_date"][:10] for r in table_readings if r["_date"]}
            if len(days) > 1:
                # One row per day is a series (a home log, a device export):
                # one report date would put every row on the earliest day.
                table_unread += len(table_readings)
                table_readings = []
            readings.extend(table_readings)
            unread += table_unread
    dated = _dates(text) + [(0, r["_date"]) for r in readings if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", r["_date"])]
    for r in readings:
        del r["_date"]
    date = ""
    if dated:
        best = min(rank for rank, _ in dated)
        date = min(d for rank, d in dated if rank == best).replace("T", " ")
    return readings, (date if len(date) > 10 else f"{date} 00:00:00" if date else ""), unread


_TR = re.compile(r"<tr\b[^>]*>.*?</tr>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")
_TD = re.compile(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", re.S | re.I)
#: Numbers that are not results: the two ends of a printed range, and the
#: exponent of a count unit (10^9/L).
_NOT_RESULT = re.compile(r"[-+]?\d+(?:\.\d+)?\s*(?:-{1,2}|~|–|—|至)\s*[-+]?\d+(?:\.\d+)?|10\s*[\^*]\s*\d+|×\s*10\S*")
_NUMBER_TOKEN = re.compile(r"(?<![\w.])[<>≤≥]?[-+]?\d+(?:\.\d+)?(?![\w.])")


def _row_read(cells: list[str], pairs: set[tuple[str, str]], codes: frozenset[int] = frozenset()) -> bool:
    """Whether every result in a table row is a read reading named in that row.
    A cell in one of `codes` (a lab's code, a row number, a previous result)
    is no result of this report; a result may carry its unit (`125g/L`),
    its flag or its range (`2.873 (0.270 - 4.200)&mIU/L`)."""
    names = {c for c in cells if c}
    # A blood pressure the rules split is read as printed: neither of its two
    # readings carries the printed name or the pair (`Blood Pressure |
    # 123/78`), and that row left in the text sends the document to the model.
    pressures = {(n, v) for v in names if "/" in v for n in names
                 if (split := blood_pressure(_key(n), _split_flag(v, "")[0])) and set(split) <= pairs}
    pairs = pairs | pressures
    results = [c for i, c in enumerate(cells) if c and i not in codes and not _is_unit(c)
               and not (_RANGE_IN.search(c) and _range_cell(c))
               and (translate.parse_value(_split_flag(c, "")[0], "").value_kind == "quantity" or _value_parts(c)
                    or any(c == v for _, v in pressures))]
    if not results:
        # A row of word results (`Urine protein(PRO) | Negative | 阴性 | 02`) is
        # read when a read reading names it with that word and no other name
        # in it went unread (the other half of a side-by-side panel). Left in,
        # every urinalysis and serology row of a text-layer book went to the
        # model a second time (corpus p002_2026-08-07_e04a).
        named = {name for name, value in pairs if name in names and value in cells}
        others = {c for i, c in enumerate(cells) if c and i not in codes and not _is_unit(c) and not _range_cell(c)
                  and not _printed_flag(c)
                  and translate.parse_value(_split_flag(c, "")[0], "").value_kind not in _RESULT_KINDS}
        return bool(named) and others <= named

    def printed(c: str) -> set[str]:
        head = re.match(r"\s*([<>≤≥]?\s*[-+]?\d+(?:\.\d+)?)", c)
        return {c, _split_flag(c, "")[0], (_value_parts(c) or ("",))[0], head.group(1).replace(" ", "") if head else c}

    return all(any(value in printed(c) and name in names for name, value in pairs) for c in results)


#: Header words of a column that repeats an earlier report's result beside
#: this one's: what it holds was read from that report, on that report's day.
_PREVIOUS = {"上次结果", "前次结果", "上次", "前次", "历史结果", "previous", "previousresult", "lastresult",
             "priorresult", "prior"}
#: Header words of a column of row numbers (`# | Tests | Measured`): with one
#: or two rows under the header, the numbers alone do not show it is one.
_ROW_NUMBERS = {"#", "序号", "no", "编号"}


def _code_columns(rows: list[list[str]]) -> frozenset[int]:
    """The columns of a table that hold no result of this report: one number
    on every row (a lab's code), the row numbers, or a previous result.
    Measured on the OCR benchmark (2026-10-07): a check-up book prints `Lab |
    02` on every row, and with `02` taken for an unread result, no row it was
    on, and no line of the text pass that repeated one, left the model's text;
    a slip's `上次结果` column left its six rows to the model too."""
    out = set()
    for i in range(max((len(r) for r in rows), default=0)):
        cells = [r[i].strip() for r in rows if i < len(r) and r[i].strip()]
        if any(_key(c) in _PREVIOUS or _key(c) in _ROW_NUMBERS for c in cells):
            out.add(i)
            continue
        numbers = [c for c in cells if c.isdigit()]
        if numbers and all(len(c) > 1 and c.startswith("0") for c in numbers):
            # Zero-padded (`02`): a code, never a count, however few rows
            # print it; a book's two-row glucose table kept its `Lab | 02`
            # as an unread result, and both rows went to the model again.
            out.add(i)
            continue
        if len(numbers) < 3 or any(_column(c) in ("value", "out", "either") for c in cells):
            continue
        counts = [int(c) for c in numbers]
        if len(set(numbers)) == 1 or all(b in (a + 1, 1) for a, b in itertools.pairwise(counts)):
            out.add(i)
    return frozenset(out)


def _line_read(line: str, pairs: set[tuple[str, str]]) -> bool:
    """Whether a plain line (a text layer's copy of a row) holds only read readings."""
    found = [(n, v) for n, v in pairs
             if re.search(r"(?<![A-Za-z0-9])" + re.escape(n) + r"(?![A-Za-z0-9])", line)
             and re.search(r"(?<![\d.])" + re.escape(v) + r"(?![\d.])", line)]
    if not found:
        return False
    values = {v for _, v in found}
    return all(t in values for t in _NUMBER_TOKEN.findall(_NOT_RESULT.sub(" ", line)))


def _fold(text: str) -> str:
    """`text` as two passes of one OCR model print it differently: width,
    script (`總` / `总`), case, spacing, the `&` before a unit, and the dash or
    tilde of a range all set aside."""
    text = fold_to_hans(unicodedata.normalize("NFKC", text)).lower().replace("&", "")
    return re.sub(r"--|[~～—–－一]", "-", "".join(text.split()))


def _copied(line: str, rows: list[list[str]]) -> bool:
    """Whether a plain line with a number in it is a copy of one table row the
    rules read, whole or in part: every word of it is in that row's cells. An
    OCR's text pass prints each row of the page again, with the code, the row
    number or the abbreviation the reading's name is not (`WBC 6.27 3.50~9.50`
    for the row `白细胞计数 | WBC | 6.27 | 3.50~9.50`), or a column at a time
    (`5.73↑` on a line of its own). Measured on the OCR benchmark (2026-10-07):
    those copies were most of what the model was handed on pages whose every
    row the rules had read, so it read them all again."""
    if not re.search(r"\d", line):
        return False
    words = [_fold(w) for w in line.split()]
    return any(all(w in joined for w in words) for joined in ("\x00".join(_fold(c) for c in r) for r in rows))


def without_rows(text: str, readings: list[dict[str, str]]) -> str:
    """`text` less every table row and line whose results the rules read: what
    a model still has to read. A row with another result in it (the unread
    half of a side-by-side panel) stays whole, and a name is matched as a
    word, so `K 4.1` does not take `Creatinine 64.1` with it. A line that only
    repeats a row the rules read (`_copied`) goes with the row."""
    pairs = {(r["original_indicator"], r["value"]) for r in readings}
    copied: list[list[str]] = []
    headings: set[str] = set()

    def cells_of(row: str) -> list[str]:
        return [" ".join(html.unescape(_TAG.sub(" ", c)).split()) for c in _TD.findall(row)]

    def table(block: re.Match) -> str:
        codes = _code_columns([cells_of(row) for row in _TR.findall(block.group(0))])

        def tr(m: re.Match) -> str:
            cells = cells_of(m.group(0))
            if _header(cells):
                # Each header word, and the whole header as a text layer
                # prints it on one line (`Test Item Measured … Lab`).
                headings.update(_fold(c) for c in cells if c)
                headings.add(_fold("".join(cells)))
            if _row_read(cells, pairs, codes) or _admin(next((c for c in cells if c), "")):
                # A patient-details row holds nothing for a model either, and
                # neither does the text pass's copy of it (`68岁`).
                copied.append(cells)
                return ""
            return m.group(0)

        rest = _TR.sub(tr, block.group(0))
        # A table left with its header and rows that hold nothing for a model
        # (a page mark), each judged as `left_for_model` judges a line: a
        # row of findings (`心电图诊断：…`) keeps the table.
        left = [cells_of(row) for row in _TR.findall(rest)]
        return "" if all(_header(c) or not left_for_model(" ".join(c)) for c in left) else rest

    text = _TABLE_BLOCK.sub(table, text)
    # Rows outside a closed table: an answer cut off at the token cap.
    text = _TR.sub(lambda m: "" if _row_read(cells_of(m.group(0)), pairs) else m.group(0), text)
    lines = [(line, _markdown_row(line) or _delimited_row(line)) for line in text.splitlines()]
    copied += [cells for _, cells in lines if cells is not None and _row_read(cells, pairs)]
    kept = []
    for line, cells in lines:
        if cells is not None and _row_read(cells, pairs):
            continue
        if cells is None and (_line_read(line, pairs) or _copied(line, copied) or _fold(line) in headings):
            # `_fold(line) in headings`: a column's header word on a line of
            # its own, a text pass reading the table a column at a time.
            continue
        kept.append(line)
    return "\n".join(kept)


def _delimited_row(line: str) -> list[str] | None:
    """A CSV or TSV line's fields, or None for a line that is not one."""
    for delimiter in ("\t", ","):
        if delimiter in line:
            cells = [c.strip() for c in next(csv.reader([line], delimiter=delimiter))]
            if len(cells) >= 3:
                return cells
    return None


_PAGE_LINE = re.compile(r"^\s*(?:--- page \d+ ---|第\s*\d+\s*页.*|page \d+( of \d+)?|\d+\s*/\s*\d+)\s*$", re.I)


def left_for_model(text: str) -> str:
    """What of `without_rows`' answer a model should still read, or "": the
    table rules leave a document's headers, patient details and page marks
    behind, and none of that holds a reading. A line with a digit outside a
    date, or a section a report writes findings under (结论, impression),
    does."""
    keep = []
    for raw in _TABLE_BLOCK.sub(lambda m: _TAG.sub(" ", m.group(0)), text).splitlines():
        line = " ".join(html.unescape(_TAG.sub(" ", raw)).split())
        if not line or _PAGE_LINE.match(line):
            continue
        cells = _markdown_row(raw)
        if _admin(cells[0] if cells else re.split(r"\s", line, maxsplit=1)[0]) or _header(cells or [line]):
            continue
        if not re.search(r"\d", line) and _header(line.split()):
            # A text pass's copy of a header (`检查项目名称 化验结果 医生建议
            # 参考值(范围)`), kept for its `建议` otherwise. Only with no number
            # in it: `HbA1c Test Result: 6.1%` names a column twice and is a reading.
            continue
        undated = _LABELLED_DATE.sub(" ", re.sub(_DATE_VALUE, " ", line))
        # A number, not a digit inside a name (HbA1c) or a date.
        if re.search(r"(?<![A-Za-z\d.])\d+(?:[.,]\d+)?", undated) or any(label in _key(line) for label in _LABELS):
            keep.append(raw)
    return "\n".join(keep)


def same_reading(a: dict, b: dict, *, misread: bool = False) -> bool:
    """Whether two extractions name the same printed reading: the same value
    (`value_key`: one number less its flag, unit and copied range, `6.49↑` /
    `6.49` / `1.69 g/L↑` / `1.69`), and the same analyte: one name, one name
    less its bracketed abbreviation or that abbreviation alone
    (`Apolipoprotein B(ApoB)` / `Apolipoprotein B` / `ApoB`), or names the
    vocabulary files under one series (`血红蛋白（HGB）` / `HGB` /
    `Hemoglobin`). With `misread`, names a character apart too (`γ-谷氨酰转移酶`
    / `y-谷氨酰转移酶`): two OCR passes of one page, the tables pass's row
    against the text pass's, never two rows of one text (`HBsAg` / `HBeAg`,
    both `Negative`, are 0.85 alike).

    Measured on the 2026-10-07 small-model eval: the rule row `血红蛋白（HGB）
    153` and the model's `血红蛋白 153` were both stored, and a check-up book's
    summary page stored `Apolipoprotein A1 1.69 g/L↑` and `Apolipoprotein B
    0.58 g/L↓` beside the table's rows. Two analytes with one value (`EO%` and
    `EO#`, both 0.6) stay two readings."""
    if value_key(str(a.get("value", ""))) != value_key(str(b.get("value", ""))):
        return False
    return _same_analyte(str(a.get("original_indicator", "")).strip(), str(b.get("original_indicator", "")).strip(),
                         misread=misread)


#: A name's trailing bracketed part: an abbreviation (`(HGB)`, `（ApoB）`).
_BRACKETED = re.compile(r"^(.*?)\s*[(（]([^()（）]+)[)）]\s*$")


def _base_and_bracket(name: str) -> tuple[str, str]:
    """(the name less a trailing bracketed part, that part), keyed."""
    m = _BRACKETED.match(name)
    return (_key(m.group(1)), _key(m.group(2))) if m else (_key(name), "")


def _same_analyte(x: str, y: str, *, misread: bool) -> bool:
    kx, ky = _key(x), _key(y)
    if not kx or not ky:
        return False
    (bx, ax), (by, ay) = _base_and_bracket(x), _base_and_bracket(y)
    # One name, the same name less its bracket, or the bracket alone; never
    # two brackets alike under two names (`Glucose(GLU)`, `Urine glucose(GLU)`).
    if kx == ky or (bx and bx == by) or (ax and ax == ky) or (ay and ay == kx):
        return True
    if misread and difflib.SequenceMatcher(None, kx, ky).ratio() >= 0.8:
        return True
    series = _series_of_name(x)
    return bool(series) and series == _series_of_name(y)


def value_key(value: str) -> tuple:
    """What two printings of one value share: the number and its comparator,
    less a trailing flag, a unit and a range a model copied with it (on the
    OCR benchmark, 2026-10-07, MiniCPM5-2B returned a slip's whole cell,
    `2.873 (0.270 - 4.200)&mIU/L`, beside the rule's `2.873`); else the text
    less its flag, case set aside (`Positive H` / `positive`)."""
    v = value.strip()
    head = _split_flag((_value_parts(v) or (v,))[0], "")[0].strip()
    parsed = translate.parse_value(head, "")
    if parsed.value_kind == "quantity" and parsed.value_num is not None:
        return ("number", parsed.value_num, parsed.comparator)
    return ("text", head.casefold())


@functools.lru_cache(maxsize=4096)
def _series_of_name(name: str) -> str:
    """The series the vocabulary files a printed name under, by the name
    alone (`translate.code`, the coder `observations.coding_for` stores every
    reading through, with no value and no unit, so a model row that left the
    unit off still resolves), or "" when it codes to none."""
    key = translate.name_key(name)
    coding = translate.code(name, name_key=key, local_key=key, value_kind=KIND_ABSENT)
    return coding.series_id if coding.coded else ""


__all__ = ["EXTRACTOR", "left_for_model", "same_reading", "status_of", "table_indicators", "value_key", "without_rows"]
