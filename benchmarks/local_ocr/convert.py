"""A candidate's raw output → what Mirobody's table reader takes (HTML or markdown tables).

`collect/files/services/table_indicators.py` reads `<table>` HTML, markdown
pipe tables and delimited lines. GLM-OCR, LightOnOCR and Qwen3-VL answer in
one of those, so they pass through unchanged. The others need a converter,
and each one here is what the model's own toolkit does after the model, so the
reader sees the table the model meant:

* `otsl`: PaddleOCR-VL and MinerU2.5 answer "Table Recognition:" in OTSL, a
  token grid (`<fcel>` a cell with text, `<ecel>` an empty one, `<lcel>` the
  cell to the left spans into this one, `<ucel>` the cell above does, `<xcel>`
  both, `<nl>` end of row). PaddleX and mineru-vl-utils (`otsl2html.py`) turn
  it into HTML with colspan/rowspan; this is a smaller rewrite of the same
  rule, not their code.
* `dots_layout`: dots.ocr's layout prompt answers one JSON list of elements
  (`bbox`, `category`, `text`), tables already HTML; the elements are written
  out in their order, a table as its HTML and the rest as text.
* `deepseek`: DeepSeek-OCR's grounding mode prefixes each block with
  `<|ref|>category<|/ref|><|det|>[[box]]<|/det|>`; the tags are dropped.

Every converter returns its input unchanged when the input is not in its
format, so a model that answered HTML after all is not damaged.
"""

from __future__ import annotations

import html
import json
import re

_OTSL = re.compile(r"(<fcel>|<ecel>|<lcel>|<ucel>|<xcel>|<nl>)")
_OTSL_ANY = re.compile(r"<(?:fcel|ecel|lcel|ucel|xcel|nl)>")


def _otsl_grid(block: str) -> list[list[tuple[str, str]]]:
    """Rows of (token, text) cells."""
    rows: list[list[tuple[str, str]]] = [[]]
    parts = _OTSL.split(block)
    i = 0
    while i < len(parts):
        part = parts[i]
        if part == "<nl>":
            rows.append([])
        elif part in ("<fcel>", "<ecel>", "<lcel>", "<ucel>", "<xcel>"):
            text = ""
            if part == "<fcel>" and i + 1 < len(parts) and not _OTSL.fullmatch(parts[i + 1] or "<nl>"):
                text = parts[i + 1].strip()
                i += 1
            rows[-1].append((part, text))
        i += 1
    rows = [r for r in rows if r]
    width = max((len(r) for r in rows), default=0)
    return [r + [("<ecel>", "")] * (width - len(r)) for r in rows]


def otsl_to_html(block: str) -> str:
    """One OTSL table as an HTML table, spans as colspan/rowspan."""
    grid = _otsl_grid(block)
    if not grid:
        return ""
    out = ["<table>"]
    for r, row in enumerate(grid):
        cells = []
        for c, (token, text) in enumerate(row):
            if token not in ("<fcel>", "<ecel>"):
                continue                      # covered by a span that starts elsewhere
            colspan = 1
            while c + colspan < len(row) and row[c + colspan][0] in ("<lcel>", "<xcel>"):
                colspan += 1
            rowspan = 1
            while r + rowspan < len(grid) and grid[r + rowspan][c][0] in ("<ucel>", "<xcel>"):
                rowspan += 1
            attrs = (f' colspan="{colspan}"' if colspan > 1 else "") + (f' rowspan="{rowspan}"' if rowspan > 1 else "")
            cells.append(f"<td{attrs}>{html.escape(text, quote=False)}</td>")
        out.append("<tr>" + "".join(cells) + "</tr>")
    out.append("</table>")
    return "".join(out)


def otsl(text: str) -> str:
    """Every OTSL run in `text` replaced by its HTML table. A run is the tokens
    with no blank line between them, through the text after its last token up
    to the end of that line. Cell text may hold `<` (a range `<5.0`), so a run
    is found by its tokens, never by "up to the next `<`"."""
    tokens = list(_OTSL_ANY.finditer(text))
    if not tokens:
        return text
    runs, start, last = [], tokens[0], tokens[0]
    for m in tokens[1:]:
        if "\n\n" in text[last.end():m.start()]:
            runs.append((start.start(), last.end()))
            start = m
        last = m
    runs.append((start.start(), last.end()))
    out, cursor = [], 0
    for a, b in runs:
        newline = text.find("\n", b)
        b = len(text) if newline < 0 else newline
        out.append(text[cursor:a])
        out.append("\n" + otsl_to_html(text[a:b]) + "\n")
        cursor = b
    out.append(text[cursor:])
    return "".join(out).strip()


def dots_layout(text: str) -> str:
    """dots.ocr's layout JSON as the page in reading order, tables as HTML."""
    raw = text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw)
    try:
        elements = json.loads(raw)
    except ValueError:
        return text
    if isinstance(elements, dict):
        elements = elements.get("layout") or elements.get("elements") or [elements]
    if not isinstance(elements, list):
        return text
    parts = []
    for element in elements:
        if not isinstance(element, dict):
            continue
        body = str(element.get("text") or "").strip()
        if not body or element.get("category") in ("Picture", "Page-header", "Page-footer"):
            continue
        parts.append(body)
    return "\n\n".join(parts)


_GROUNDING = re.compile(r"<\|ref\|>.*?<\|/ref\|><\|det\|>.*?<\|/det\|>\s*", re.S)


def deepseek(text: str) -> str:
    return _GROUNDING.sub("", text).strip()


CONVERTERS = {"otsl": otsl, "dots_layout": dots_layout, "deepseek": deepseek}


def convert(name: str | None, text: str) -> str:
    return CONVERTERS[name](text) if name else text
