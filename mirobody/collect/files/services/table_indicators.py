"""Readings straight off a report's tables: no model between the OCR and ② Translate.

GLM-OCR's table mode returns HTML tables whose columns are already split; a
spreadsheet arrives as a markdown table. When a document's tables carry a name
and a result column, the rows are read here by their header text and handed to
the store in the shape the LLM extractor produces. Measured on one real 11-page
checkup: one HTML table held several panels, each opened by a single-cell
section row and a repeated header, the reference column BEFORE the unit column,
and a page's first row continuing the previous page's panel with no header of
its own. So columns are mapped per header, never by position, and a row before
any header borrows the last header seen.

What is not read here: a value is kept exactly as printed, a status comes only
from a printed flag or a printed range, and a document with no such table
returns nothing so the caller can fall back to a model.
"""

from __future__ import annotations

import csv
import html
import re
from html.parser import HTMLParser

EXTRACTOR = "rules:table@v1"

#: Header words per column, compared after lowercasing and dropping spaces.
_HEADERS: dict[str, tuple[str, ...]] = {
    "name": ("项目名称", "检验项目", "检查项目", "检测项目", "化验项目", "项目", "项目名", "中文名称", "名称",
             "指标", "指标名称", "analyte", "test", "testname", "item", "parameter", "name", "indicator",
             "measurement", "measure", "component", "observation", "metric"),
    "value": ("结果", "检验结果", "检查结果", "测定结果", "测定值", "检测值", "result", "results", "value"),
    "unit": ("单位", "unit", "units"),
    "ref": ("参考值", "参考范围", "参考区间", "正常值", "正常范围", "生物参考区间",
            "reference", "referencerange", "range", "ref", "normalrange"),
    "flag": ("标志", "提示", "异常", "结果提示", "flag", "状态"),
    "date": ("采样时间", "采集时间", "检验时间", "日期", "collected", "collectiondate", "date"),
}
_FLAG_HIGH = {"↑", "h", "高", "偏高", "high", "hh", "↑↑"}
_FLAG_LOW = {"↓", "l", "低", "偏低", "low", "ll", "↓↓"}
_DATE = re.compile(r"(?:检验时间|采样时间|采集时间|采样日期|检查日期|报告日期|collected|collection|date)\s*[:：]?\s*(\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)", re.I)
_NUMBER = re.compile(r"^[<>≤≥]?\s*[-+]?\d+(?:\.\d+)?$")
_RANGE = re.compile(r"^\s*([-+]?\d+(?:\.\d+)?)\s*(?:-{1,2}|~|–|—|至)\s*([-+]?\d+(?:\.\d+)?)\s*$")
_BOUND = re.compile(r"^\s*([<>≤≥]|<=|>=)\s*([-+]?\d+(?:\.\d+)?)\s*$")


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


def _markdown_tables(text: str) -> list[list[list[str]]]:
    tables, current = [], []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("|") and line.count("|") >= 3:
            cells = [html.unescape(c.strip()) for c in line.strip("|").split("|")]
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
    key = re.sub(r"[\s:：()（）]", "", cell).lower()
    for column, words in _HEADERS.items():
        if key in words:
            return column
    return None


def _header(row: list[str]) -> dict[str, int] | None:
    """{column: index} when `row` is a header naming at least a name and a result."""
    found: dict[str, int] = {}
    for i, cell in enumerate(row):
        column = _column(cell)
        if column and column not in found:
            found[column] = i
    return found if {"name", "value"} <= found.keys() else None


def status_of(value: str, ref: str, flag: str) -> str:
    """high / low / normal from a printed flag, else from a printed range; "" when neither says."""
    f = flag.strip().lower()
    if f in _FLAG_HIGH:
        return "high"
    if f in _FLAG_LOW:
        return "low"
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


#: Row labels that name a part of a report, not a measurement: the model reads
#: those rows with their section around them.
_LABELS = {"检查描述", "检查结论", "描述", "结论", "所见", "检查所见", "诊断", "小结", "建议", "意见",
           "description", "conclusion", "impression", "findings", "comment", "remarks"}
_VALUE_WITH_UNIT = re.compile(r"^([<>≤≥]?\s*[-+]?\d+(?:\.\d+)?)\s*([^\d\s.,;:：，].*)$")

_UNIT = re.compile(r"^(?:%|‰|[a-zA-Zμµ×][\w/^·.*×μµ]*(?:/[\w^.]+)?|\d*\^?\d+/[a-zA-Z]+)$")


def _inferred(row: list[str]) -> dict[str, int]:
    """Columns guessed from content, for a row whose table carried no header of
    its own width: the first cell is the name, a range is the reference, a unit
    is the unit, an arrow or H/L the flag, and the first other cell the result."""
    columns = {"name": 0}
    for i, cell in enumerate(row[1:], start=1):
        c = cell.strip()
        if not c:
            continue
        if "ref" not in columns and (_RANGE.match(c) or _BOUND.match(c)) and "value" in columns:
            columns["ref"] = i
        elif "flag" not in columns and c.lower() in _FLAG_HIGH | _FLAG_LOW and "value" in columns:
            columns["flag"] = i
        elif "unit" not in columns and _UNIT.match(c) and "value" in columns and not _NUMBER.match(c):
            columns["unit"] = i
        elif "value" not in columns:
            columns["value"] = i
    return columns


def _reading(row: list[str], columns: dict[str, int]) -> dict[str, str] | None:
    def cell(column: str) -> str:
        i = columns.get(column)
        return row[i].strip() if i is not None and i < len(row) else ""

    name, value = cell("name"), cell("value")
    if not name or not value or _NUMBER.match(name) or _header(row):
        return None
    if name.lower() in _LABELS or _column(value):
        return None
    ref, flag, unit = cell("ref"), cell("flag"), cell("unit")
    # GLM-OCR puts a vital's unit in the value cell ("72.0kg") when the table
    # has no unit column; the number has to stand alone to be charted.
    if not unit and (m := _VALUE_WITH_UNIT.match(value)) and not _RANGE.match(value):
        value, unit = m.group(1).replace(" ", ""), m.group(2).strip()
    return {
        "original_indicator": name,
        "value": value,
        "unit": unit,
        "reference_range": ref,
        "detection_method": "laboratory",
        "status": status_of(value, ref, flag),
        "notes": "",
    }


def table_indicators(text: str) -> tuple[list[dict[str, str]], str, int]:
    """(readings, examination date or "", unread) from every table in `text`
    that has a name and a result column. `unread` counts the table rows with
    two or more cells that no rule read (patient details, narrative findings,
    a table with no usable header): the caller's cue that a model should read
    the document too. ([], "", 0) when there is no table at all."""
    readings: list[dict[str, str]] = []
    dates: list[str] = []
    unread = 0
    columns: dict[str, int] | None = None
    width = 0
    for table in _html_tables(text) + _markdown_tables(text) + _delimited_tables(text):
        for row in table:
            cells = [c for c in row if c]
            if len(cells) == 1:
                if m := _DATE.search(cells[0]):
                    dates.append(m.group(1))
                continue
            if header := _header(row):
                columns, width = header, len(row)
                continue
            fit = columns if columns and len(row) <= width else (_inferred(row) if columns else None)
            reading = _reading(row, fit) if fit else None
            if reading is None:
                unread += len(cells) >= 2
                continue
            readings.append(reading)
            day = row[fit["date"]].strip() if "date" in fit and fit["date"] < len(row) else ""
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?", day):
                dates.append(day.replace("T", " "))
    if not dates:
        dates = [m.group(1) for m in _DATE.finditer(text)]
    date = min(dates) if dates else ""
    return readings, (date if len(date) > 10 else f"{date} 00:00:00" if date else ""), unread


_TR = re.compile(r"<tr\b[^>]*>.*?</tr>", re.S | re.I)
_TAG = re.compile(r"<[^>]+>")


def without_rows(text: str, readings: list[dict[str, str]]) -> str:
    """`text` less every table row and line that carries a read reading's name
    and value: what a model still has to read. The HTML row, the markdown row
    and the text layer's copy of the same line all go, so the model is not
    asked to write out again what the rules already stored."""
    pairs = [(r["original_indicator"], r["value"]) for r in readings]

    def carries(chunk: str) -> bool:
        plain = html.unescape(_TAG.sub(" ", chunk))
        return any(name in plain and value in plain for name, value in pairs)

    text = _TR.sub(lambda m: "" if carries(m.group(0)) else m.group(0), text)
    return "\n".join(line for line in text.splitlines() if not carries(line))
