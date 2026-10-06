r"""The reference `Ocr`: one image → its text, through the engine's vision client.

`extract.pdf_text` hands this only the pages whose text layer is empty, and
`extract.image_text` one downscaled photo at a time, never a whole document.
The provider is whichever key is configured (`utils.llm.unified_file_extract`
picks it); a consumer with its own vision model passes its own callable.

With `UTILS_OCR_MODEL` routed (a document-OCR model such as GLM-OCR, which
answers only its own task prompts) the image goes there instead, once per
prompt the entry declares: its text, then its tables as HTML, whose columns
`collect/files/services/table_indicators` reads without a model.

Every answer, from either path, is cleaned once here (`clean_answer`) before
anything reads or stores it, because the readers downstream take plain text
and HTML tables only, and the OCR models do not all answer in those. Measured
on the OCR benchmark (benchmarks/local_ocr, 2026-10-06/07): PaddleOCR-VL-1.6
and MinerU2.5 answer "Table Recognition:" in OTSL, which no reader parsed (the
table rules read 0 of PaddleOCR's 303 rows from it); PaddleOCR writes units and
flags as LaTeX (`\(\mu mol/L\)`, `6.49\(\uparrow\)`), which neither the unit
engine nor the value parser reads; and it looped one line until the token cap
on a scanned ECG page. GLM-OCR's answers hold none of these and pass unchanged.
"""

from __future__ import annotations

import html
import logging
import os
import re
import tempfile

logger = logging.getLogger(__name__)

#: The longest answer an OCR pass may write. Measured on the OCR benchmark
#: (2026-10-06): the longest of 150 answers by three OCR models was 1,656 tokens
#: (a 40-row lab table as HTML), and PaddleOCR-VL-1.6's loop on a scanned ECG
#: page ran to the benchmark's 8,192-token cap in 70 s. The product sent no cap,
#: so a loop ran on to the end of the model's context (16,384 tokens for the
#: OCR model in docker/local-models.ini), twice as long, for nothing.
OCR_MAX_TOKENS = 8192

OCR_PROMPT = """Extract and return ALL text content from this document/image.

Requirements:
1. Extract all text completely, do not omit anything
2. For tables, use the following format:
   - Each row on a separate line
   - Columns separated by |
   - Keep table headers
3. Preserve the original paragraph structure
4. Keep all values, units, dates, measurements exactly as shown
5. Anonymize personal identifiable information (PII):
   - ID number (身份证号): replace with "***"
   - Phone number: replace with "***"
   - Address: keep only city/district level, replace detailed address with "***"
   - Patient ID / Medical record number: replace with "***"
   - Keep name, age, gender, and medical-related dates (examination date, report date) as is

Return ONLY the extracted text content. Do not add any explanations, summaries, or commentary.
If there is no text, return an empty response."""

_SUFFIX = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp", "image/gif": ".gif"}


def _ocr_route():
    from mirobody.utils.config.llm import resolve_route

    return resolve_route("ocr")


async def _extract(image: bytes, mime: str, prompt: str, provider: str | None = None,
                   max_tokens: int | None = None) -> str:
    from mirobody.utils.llm import unified_file_extract

    with tempfile.NamedTemporaryFile(suffix=_SUFFIX.get(mime, ".png"), delete=False) as handle:
        handle.write(image)
        path = handle.name
    try:
        return clean_answer((await unified_file_extract(file_path=path, prompt=prompt, content_type=mime,
                                                        provider=provider, json_mode=False,
                                                        max_tokens=max_tokens)) or "")
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


async def vision_ocr(image: bytes, mime: str, *, prompt: str = OCR_PROMPT) -> str:
    """Text of one image: the OCR entry's passes when one is routed, else the
    vision provider. A pass that fails costs only itself: the text pass of a
    photo is still a document when its tables pass times out. When every pass
    fails, the last error is raised."""
    spec = _ocr_route()
    if spec is None:
        return await _extract(image, mime, prompt)
    parts, failure = [], None
    for name, task in spec.ocr_prompts.items():
        try:
            parts.append(await _extract(image, mime, task, spec.alias, OCR_MAX_TOKENS))
        except Exception as exc:
            logger.warning("ocr pass failed: pass=%s error_type=%s", name, type(exc).__name__)
            failure = exc
    if failure is not None and not parts:
        raise failure
    return "\n\n".join(part.strip() for part in parts if part.strip())


def table_ocr():
    """The tables pass alone, for PDF pages whose text layer is already exact;
    None when no OCR entry is routed or it declares no `tables` prompt."""
    spec = _ocr_route()
    if spec is None or "tables" not in spec.ocr_prompts:
        return None

    async def tables(image: bytes, mime: str) -> str:
        return (await _extract(image, mime, spec.ocr_prompts["tables"], spec.alias, OCR_MAX_TOKENS)).strip()

    return tables


# --- what an answer becomes before anything reads it ------------------------------------


def clean_answer(text: str) -> str:
    """One OCR answer as the readers take it: a decoding loop cut, OTSL tables
    as HTML tables, LaTeX as the characters it typesets. An answer with none
    of these (GLM-OCR's, a cloud model's plain text) comes back unchanged."""
    return _plain_math(_otsl_to_html(_cut_loops(text)))


#: A unit of text (a line, a cell, a phrase) repeated this many times in a row
#: is a decoding loop, not a page: a printed page holds far fewer rows (the
#: densest benchmark page has 40), no unit holding text occurs four times in a
#: row in the 150 benchmark answers, and the one loop repeated a line 2,722 times.
_LOOP_REPEATS = 64
_LOOP = re.compile(r"(.{1,200}?)\1{" + str(_LOOP_REPEATS - 1) + ",}", re.S)
_TAG = re.compile(r"<[^>]*>")


def _cut_loops(text: str) -> str:
    """`text` with each run of one repeated unit cut to one copy. A run of
    markup or punctuation alone (an empty table row, a rule of dashes) is
    layout and is kept."""
    out, cursor = [], 0
    for m in _LOOP.finditer(text):
        unit = m.group(1)
        if not re.search(r"\w", _TAG.sub("", unit)):
            continue
        out.append(text[cursor:m.start()] + unit)
        cursor = m.end()
        logger.warning("ocr answer looped, repeats cut: unit_chars=%d repeats=%d chars_cut=%d",
                       len(unit), len(m.group(0)) // len(unit), len(m.group(0)) - len(unit))
    if not out:
        return text
    out.append(text[cursor:])
    return "".join(out)


#: OTSL, the table language of PaddleOCR-VL and MinerU2.5: `<fcel>` a cell with
#: text, `<ecel>` an empty one, `<lcel>` the cell to the left spans into this
#: one, `<ucel>` the cell above does, `<xcel>` both, `<nl>` the end of a row.
_OTSL = re.compile(r"<(fcel|ecel|lcel|ucel|xcel|nl)>")


def _otsl_to_html(text: str) -> str:
    """Every OTSL run in `text` as an HTML table. A run is the tokens with no
    blank line between them, through the end of the line its last token is
    on. A cell's text may hold `<` (a bound, `< 8.0`), so a run is found by
    its tokens, never by "up to the next `<`"."""
    marks = list(_OTSL.finditer(text))
    if not marks:
        return text
    runs, start, last = [], marks[0], marks[0]
    for m in marks[1:]:
        if "\n\n" in text[last.end():m.start()]:
            runs.append((start.start(), last.end()))
            start = m
        last = m
    runs.append((start.start(), last.end()))
    out, cursor = [], 0
    for a, b in runs:
        newline = text.find("\n", b)
        b = len(text) if newline < 0 else newline
        out.append(text[cursor:a] + "\n" + _otsl_table(text[a:b]) + "\n")
        cursor = b
    out.append(text[cursor:])
    return "".join(out).strip()


def _otsl_table(block: str) -> str:
    """One OTSL run as an HTML table, a span as colspan or rowspan: the rule of
    PaddleX's and mineru-vl-utils' `otsl2html`, written here, not imported.
    Rows the model wrote short are padded at the end, the only place their
    missing cells can be put; a span token with nothing to span from (an
    `<lcel>` opening a row) is an empty cell, so the cells after it keep
    their columns; a token followed by text holds that text, so none is lost."""
    marks = list(_OTSL.finditer(block))
    rows: list[list[tuple[str, str]]] = [[]]
    for k, m in enumerate(marks):
        cell = block[m.end():marks[k + 1].start() if k + 1 < len(marks) else len(block)].strip()
        if m.group(1) == "nl":
            rows.append([])
        else:
            rows[-1].append(("fcel" if cell else m.group(1), cell))
    rows = [r for r in rows if r]
    width = max((len(r) for r in rows), default=0)
    grid = [r + [("ecel", "")] * (width - len(r)) for r in rows]
    for r, row in enumerate(grid):
        for c, (token, cell) in enumerate(row):
            orphan = ((token in ("lcel", "xcel") and c == 0) or (token in ("ucel", "xcel") and r == 0))
            if orphan:
                row[c] = ("ecel", cell)
    out = ["<table>"]
    for r, row in enumerate(grid):
        cells = []
        for c, (token, cell) in enumerate(row):
            if token not in ("fcel", "ecel"):
                continue
            colspan = 1
            while c + colspan < width and row[c + colspan][0] in ("lcel", "xcel"):
                colspan += 1
            rowspan = 1
            while r + rowspan < len(grid) and grid[r + rowspan][c][0] in ("ucel", "xcel"):
                rowspan += 1
            attrs = (f' colspan="{colspan}"' if colspan > 1 else "") + (f' rowspan="{rowspan}"' if rowspan > 1 else "")
            cells.append(f"<td{attrs}>{html.escape(cell, quote=False)}</td>")
        out.append("<tr>" + "".join(cells) + "</tr>")
    out.append("</table>")
    return "".join(out)


#: LaTeX control words an OCR model writes for a character a report prints.
#: PaddleOCR-VL-1.6 wrote `\mu`, `\times`, `\uparrow`, `\downarrow`, `\le`,
#: `\pm`, `\gamma` and `\dagger` on the benchmark pages; the rest are their
#: neighbours in the same reports (Greek letters of analyte names, comparators,
#: degrees). `\mu` is the Greek mu (U+03BC), which PaddleOCR's own text pass
#: and the printed truth use, not the micro sign.
_SYMBOLS = {
    "times": "×", "cdot": "·", "div": "÷", "pm": "±", "mp": "∓", "sim": "~", "approx": "≈",
    "le": "≤", "leq": "≤", "leqslant": "≤", "ge": "≥", "geq": "≥", "geqslant": "≥", "ne": "≠", "neq": "≠",
    "lt": "<", "gt": ">", "uparrow": "↑", "downarrow": "↓", "rightarrow": "→", "to": "→", "leftarrow": "←",
    "dagger": "†", "ddagger": "‡", "circ": "°", "degree": "°", "prime": "′", "infty": "∞", "permil": "‰",
    "ldots": "…", "cdots": "…", "dots": "…", "percent": "%",
    "alpha": "α", "beta": "β", "gamma": "γ", "delta": "δ", "Delta": "Δ", "epsilon": "ε", "kappa": "κ",
    "lambda": "λ", "mu": "μ", "nu": "ν", "pi": "π", "rho": "ρ", "sigma": "σ", "tau": "τ", "phi": "φ",
    "chi": "χ", "omega": "ω", "Omega": "Ω",
    "quad": " ", "qquad": " ",
}
#: Commands whose argument is the text itself (`\text{mmol/L}`).
_WRAPPERS = ("text", "textrm", "textnormal", "textbf", "textit", "mathrm", "mathbf", "mathit", "mathsf",
             "operatorname", "mbox", "boldsymbol")
_WRAPPED = re.compile(r"\\(?:" + "|".join(_WRAPPERS) + r")\s*\{([^{}]*)\}")
_FRAC = re.compile(r"\\[dt]?frac\s*\{([^{}]*)\}\s*\{([^{}]*)\}")
_SQRT = re.compile(r"\\sqrt\s*\{([^{}]*)\}")
_DOUBLE_BRACES = re.compile(r"\{\{([^{}]*)\}\}")
_SCRIPT = re.compile(r"([\^_])\{([^{}]*)\}")
_WORD = re.compile(r"\\([A-Za-z]+) ?")
_ESCAPED = re.compile(r"\\([%&#_{}$])")
_SPACING = re.compile(r"\\[,;:! ]")
#: Inline and display math: `\(..\)`, `\[..\]`, `$$..$$`, and `$..$` only when
#: what it holds is LaTeX, so a price (`$12.50 and $8`) stays a price.
_MATH = re.compile(r"\\\((.*?)\\\)|\\\[(.*?)\\\]|\$\$(.+?)\$\$|\$([^$\n]*[\\^_{][^$\n]*)\$", re.S)


def _plain_math(text: str) -> str:
    r"""`text` with LaTeX written as the characters it typesets: math
    delimiters dropped, control words as their symbols, `10^{9}` as `10^9`
    (the caret the reports and the unit engine use; a superscript ⁹ folds to
    a plain 9 under NFKC, and `240 10⁹/L` then reads as 240109),
    `\text{..}` unwrapped. A literal `\n` (PaddleOCR's line break inside a
    table cell) is a line break. Control words nobody prints are kept."""
    if "\\" not in text and "$" not in text:
        return text
    text = _MATH.sub(lambda m: _plain(next(g for g in m.groups() if g is not None), math=True), text)
    return _plain(text, math=False)


def _plain(text: str, *, math: bool) -> str:
    for _ in range(3):  # nested groups, innermost first
        text = _DOUBLE_BRACES.sub(r"{\1}", text)
        text = _WRAPPED.sub(r"\1", text)
        text = _FRAC.sub(r"\1/\2", text)
        text = _SQRT.sub(r"√\1", text)
    text = _WORD.sub(_word, text)
    text = _SCRIPT.sub(lambda m: (m.group(1) if m.group(1) == "^" and m.group(2).isalnum() else "") + m.group(2),
                       text)
    text = _ESCAPED.sub(r"\1", _SPACING.sub(lambda m: "" if m.group(0) == "\\!" else " ", text))
    return text.replace("{", "").replace("}", "") if math else text


def _word(m: re.Match) -> str:
    """One control word (with the space LaTeX swallows after it) as text."""
    name = m.group(1)
    if name in _SYMBOLS:
        return _SYMBOLS[name]
    if name.startswith("n"):
        # `\nThyroid`, `\n女`: a line break the model wrote as two characters.
        return "\n" + name[1:] + (" " if m.group(0).endswith(" ") else "")
    return m.group(0)
