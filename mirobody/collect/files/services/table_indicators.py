"""Readings straight off a report's tables: no model between the OCR and ② Translate.

GLM-OCR's table mode returns HTML tables whose columns are already split; a
spreadsheet arrives as a markdown table, a CSV as delimited lines. When a table
carries a name and a result column, its rows are read here by their header text
and handed to the store in the shape the LLM extractor produces. Measured on one
real 11-page checkup: one HTML table held several panels, each opened by a
single-cell section row and a repeated header, the reference column BEFORE the
unit column, and a page's first row continuing the previous page's panel with no
header of its own. So columns are mapped per header, never by position; a
column whose header is blank, or a flag word over printed ranges, is typed by
its cells; a header that names the name column twice is two panels side by
side (双栏); and a table with no header of its own borrows the last one only
for rows that look like readings under it.

A row is a reading only when it looks like one: a result that is a number, an
ordinal (`1+`) or a nominal word (阴性, negative), beside a unit the unit engine
knows and a range (or nothing there). A row of patient details (姓名, 年龄,
送检医生, or `审核者：王五` in one cell) is skipped; any other row of two or
more cells is left to the model.
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
import html
import itertools
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser

from mirobody import translate
from mirobody.units import normalize_unit

EXTRACTOR = "rules:table@v1"

#: Header words per column, compared after `_key` (lowercased, no spaces,
#: colons or brackets), so a bilingual `单位(Unit)` or `单位 Unit` is `单位unit`.
#: Measured on the 2026-10-06 local-model corpus: `单位(Unit)`, `正常范围值` and a
#: slip's bare `参考` were not here, and every reading of those tables was
#: stored with no unit or no range (hemoglobin 140 then cannot be coded).
_HEADERS: dict[str, tuple[str, ...]] = {
    "name": ("项目名称", "检验项目", "检查项目", "检测项目", "化验项目", "项目", "项目名", "中文名称", "名称",
             "指标", "指标名称", "analyte", "test", "testname", "item", "parameter", "name", "indicator",
             "measurement", "measure", "component", "observation", "metric"),
    "value": ("结果", "检验结果", "检查结果", "测定结果", "测定值", "检测值", "result", "results", "value"),
    "unit": ("单位", "单位unit", "unit", "units"),
    "ref": ("参考值", "参考范围", "参考区间", "参考", "正常值", "正常范围", "正常范围值", "生物参考区间",
            "reference", "referencerange", "range", "ref", "normalrange"),
    "flag": ("标志", "提示", "异常", "结果提示", "flag", "状态"),
    "date": ("采样时间", "采集时间", "检验时间", "日期", "collected", "collectiondate", "date"),
}
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
    "临床诊断", "备注", "电话", "地址", "身份证号", "医院", "页码",
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
#: A flag printed after the number in the value cell, when the table has no flag column.
_TRAILING_FLAG = re.compile(r"^(.*?\d)\s*(↑↑|↓↓|↑|↓|偏高|偏低|高|低|HH|LL|H|L)$")
#: A range or bound, then what follows it in the same cell (`<7.00&ng/mL`).
_REF_UNIT = re.compile(
    r"^\s*(\(?[<>≤≥]?=?\s*[-+]?\d+(?:\.\d+)?(?:\s*(?:-{1,2}|~|–|—|至)\s*[-+]?\d+(?:\.\d+)?)?\)?)\s*&?\s*([^\d\s&].*?)\s*$")
_RESULT_KINDS = ("quantity", "ordinal", "nominal")
#: The longest text result read without a model ("淡黄色", "微浑"); a longer
#: one is a sentence, and a sentence is the model's to read.
_SHORT_TEXT = 8


def _key(cell: str) -> str:
    return re.sub(r"[\s:：()（）._\-/]", "", cell).lower()


class _Tables(HTMLParser):
    """Every <table> as a list of rows, a row as its cell texts."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables: list[list[list[str]]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.tables.append([])
        elif tag == "tr" and self.tables:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._row is not None and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if any(self._row):
                self.tables[-1].append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


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
    for column, words in _HEADERS.items():
        if key in words:
            return column
    return None


def _header(row: list[str]) -> list[dict[str, int]] | None:
    """The column groups of a header row, one `{column: index}` per panel, or
    None when `row` names no name-and-result pair. A name column named again
    opens a second panel beside the first; a column before the first name
    (a CSV's leading `date`) belongs to the first panel."""
    columns = [(i, _column(cell)) for i, cell in enumerate(row)]
    starts = [i for i, column in columns if column == "name"]
    if not starts:
        return None
    groups: list[dict[str, int]] = [{} for _ in starts]
    for i, column in columns:
        if column is None:
            continue
        g = groups[max(0, sum(1 for s in starts if s <= i) - 1)]
        g.setdefault(column, i)
    groups = [g for g in groups if {"name", "value"} <= g.keys()]
    return groups or None


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


def _by_cells(row: list[str], groups: list[dict[str, int]], body: list[list[str]]) -> list[dict[str, int]]:
    """`groups` with a panel's missing range or unit column found by its cells,
    when its header does not name it: under a blank header, cells that are all
    units are the unit column and cells that are all ranges the range column;
    under a flag word (`提示`, `结果提示`), cells that are all ranges are the
    range column, not flags. `body` is the table below `row`, read up to the
    next header. Measured on the 2026-10-06 local-model corpus: a book printed
    `检测项目 | 测定值 | (blank) | 单位(Unit)`, another put its ranges under
    `结果提示`, and the rules stored those ranges as nothing."""
    body = list(itertools.takewhile(lambda r: not _header(r), body))
    body = [r for r in body if sum(1 for c in r if c) > 1 and not _admin(next(c for c in r if c))]
    starts = [i for i, cell in enumerate(row) if _column(cell) == "name"]
    typed = []
    for g in groups:
        g = dict(g)
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
    """`(value, flag)` with a flag printed after the number moved out. A bare
    `L` is a flag only when the printed range says the value is low: `1.5 L`
    of urine is litres."""
    m = _TRAILING_FLAG.match(value.strip())
    if not m:
        return value, ""
    number, flag = m.group(1).strip(), m.group(2)
    if flag in ("L", "LL") and status_of(number, ref) != "low":
        return value, ""
    return number, flag


@dataclass
class _Row:
    """What one table row came to."""

    readings: list[dict[str, str]] = field(default_factory=list)
    unread: bool = False


def _cell(row: list[str], columns: dict[str, int], column: str) -> str:
    i = columns.get(column)
    return row[i].strip() if i is not None and i < len(row) else ""


def _reading(row: list[str], columns: dict[str, int], *, borrowed: bool) -> dict[str, str] | str | None:
    """One panel of one row: a reading, `"admin"` for a patient-details row,
    `"empty"` for a panel with nothing in it, or None for a row a model should read."""
    name, value = _cell(row, columns, "name"), _cell(row, columns, "value")
    if not name and not value:
        return "empty"
    if _admin(name):
        return "admin"
    if re.fullmatch(r"(?i)rs\d+|i\d{4,}", name):
        return "admin"  # a genotype call: the genomics upload reads those
    if not name or not value or _NUMBER.match(name) or _header(row):
        return None
    if _key(name) in _LABELS or _column(value):
        return None
    ref, unit, flag = _cell(row, columns, "ref"), _cell(row, columns, "unit"), _cell(row, columns, "flag")
    if not unit and (m := _REF_UNIT.match(ref)) and normalize_unit(m.group(2)) is not None:
        # A slip with no unit column prints the unit after the range
        # (`<7.00&ng/mL`): it is the reading's unit, and the range is the rest.
        ref, unit = m.group(1).strip(), m.group(2).strip()
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
    if parsed.value_kind in _RESULT_KINDS:
        if borrowed and not _unit_like(unit):
            return None
    elif (borrowed or len(value) > _SHORT_TEXT or re.search(r"\d|[:：]", value) or not _unit_like(unit)
          or normalize_unit(value) is not None):
        # A word under the table's own header ("淡黄色") is a result; under a
        # borrowed one, or beside a cell that is not a unit, it is a name. A
        # word with a colon is a label and its value (`检验者：段松洋`), and a
        # unit alone (`MCV | fl`) is a result whose number was not printed.
        return None
    return {
        "original_indicator": name,
        "value": value,
        "unit": unit,
        "reference_range": ref,
        "detection_method": "laboratory",
        "status": _printed_flag(flag),
        "notes": "",
        "_date": _cell(row, columns, "date"),
    }


def _read_row(row: list[str], groups: list[dict[str, int]], *, borrowed: bool) -> _Row:
    out = _Row()
    for columns in groups:
        got = _reading(row, columns, borrowed=borrowed)
        if isinstance(got, dict):
            out.readings.append(got)
        elif got is None:
            out.unread = True
    if borrowed and out.unread:
        # A borrowed header is a guess about this table; one panel it does not
        # fit says it is a different table, and the model reads all of it.
        return _Row(unread=True)
    return out


def _dates(text: str) -> list[tuple[int, str]]:
    """`(rank, "YYYY-MM-DD[ HH:MM[:SS]]")` for every labelled date in `text`, rank 0 the most specific."""
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
    width = 0
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
            borrowed = groups is not None
            for i, row in enumerate(table):
                cells = [c for c in row if c]
                if len(cells) == 1 or _admin(cells[0]):
                    continue
                if header := _header(row):
                    groups, width, borrowed = _by_cells(row, header, table[i + 1:]), len(row), False
                    continue
                if groups is None or (borrowed and len(row) > width):
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


def _row_read(cells: list[str], pairs: set[tuple[str, str]]) -> bool:
    """Whether every result in a table row is a read reading named in that row."""
    names = {c for c in cells if c}
    results = [c for c in cells if c and translate.parse_value(_split_flag(c, "")[0], "").value_kind == "quantity"
               and not _RANGE.match(c) and not _BOUND.match(c) and normalize_unit(c) is None]
    if not results:
        return False
    return all(any(value in (c, _split_flag(c, "")[0]) and name in names for name, value in pairs) for c in results)


def _line_read(line: str, pairs: set[tuple[str, str]]) -> bool:
    """Whether a plain line (a text layer's copy of a row) holds only read readings."""
    found = [(n, v) for n, v in pairs
             if re.search(r"(?<![A-Za-z0-9])" + re.escape(n) + r"(?![A-Za-z0-9])", line)
             and re.search(r"(?<![\d.])" + re.escape(v) + r"(?![\d.])", line)]
    if not found:
        return False
    values = {v for _, v in found}
    return all(t in values for t in _NUMBER_TOKEN.findall(_NOT_RESULT.sub(" ", line)))


def without_rows(text: str, readings: list[dict[str, str]]) -> str:
    """`text` less every table row and line whose results the rules read: what
    a model still has to read. A row with another result in it (the unread
    half of a side-by-side panel) stays whole, and a name is matched as a
    word, so `K 4.1` does not take `Creatinine 64.1` with it."""
    pairs = {(r["original_indicator"], r["value"]) for r in readings}

    def tr(m: re.Match) -> str:
        cells = [html.unescape(_TAG.sub(" ", c)).strip() for c in _TD.findall(m.group(0))]
        return "" if _row_read([" ".join(c.split()) for c in cells], pairs) else m.group(0)

    text = _TR.sub(tr, text)
    kept = []
    for line in text.splitlines():
        cells = _markdown_row(line) or _delimited_row(line)
        if cells is not None and _row_read(cells, pairs):
            continue
        if cells is None and _line_read(line, pairs):
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
        undated = _LABELLED_DATE.sub(" ", re.sub(_DATE_VALUE, " ", line))
        # A number, not a digit inside a name (HbA1c) or a date.
        if re.search(r"(?<![A-Za-z\d.])\d+(?:[.,]\d+)?", undated) or any(label in _key(line) for label in _LABELS):
            keep.append(raw)
    return "\n".join(keep)


def same_reading(a: dict, b: dict) -> bool:
    """Whether two extractions name the same printed row: the same value, and
    names a misread character apart (`γ-谷氨酰转移酶` / `y-谷氨酰转移酶`)."""
    if str(a.get("value", "")).strip() != str(b.get("value", "")).strip():
        return False
    x, y = (_key(str(r.get("original_indicator", ""))) for r in (a, b))
    return bool(x) and bool(y) and (x == y or difflib.SequenceMatcher(None, x, y).ratio() >= 0.8)


__all__ = ["EXTRACTOR", "left_for_model", "same_reading", "status_of", "table_indicators", "without_rows"]
