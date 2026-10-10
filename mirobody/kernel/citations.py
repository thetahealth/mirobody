"""The citation format an answer uses, and the check that every number in it
can be traced to the rows it cites.

The format follows LongCite (arXiv 2409.02897): a claim that rests on the
record is wrapped in `<statement>`, and the evidence it rests on closes it in
`<cite>`:

    <statement>LDL fell from 3.8 to 2.9 mmol/L<cite>[r3][r9]</cite></statement>
    <statement>The clinic note asks for a recheck<cite>[/library/note.pdf#L12-L14]</cite></statement>
    Bring this trend to your doctor.

A cite is one of three kinds: a row a tool showed in this conversation
(`r3`), a reference passage (`ref:<source>:<id>`), or lines of a document
(`<path>#L12-L14`). Text outside a statement carries no number.

`parse` reads an answer into segments, `strip` removes the markup for a reader
that does not render it, and `check` returns every number that is not traced:
outside a statement, cited to nothing, or found neither in its cited rows nor
as a difference, ratio, percentage change, sum or mean of their values.
stdlib only: the agent, the benchmark and the training judge use one copy.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from itertools import permutations

STATEMENT_OPEN = "<statement>"
STATEMENT_CLOSE = "</statement>"
CITE_OPEN = "<cite>"
CITE_CLOSE = "</cite>"

ROW_ID = re.compile(r"^r[1-9]\d*$")
REF_ID = re.compile(r"^ref:[a-z0-9_]+:[^\s\]]+$")
LINES_ID = re.compile(r"^(?P<path>\S.*?)#L(?P<start>\d+)(?:-L?(?P<end>\d+))?$")

_CITE_TOKEN = re.compile(r"\[([^\[\]]+)\]")
_CLAIM_BOUNDARY = re.compile(r"[\n|。！？；]|[.!?;](?=\s)")
#: A fenced block (a chart's data) is drawn from the rows the prose cites.
_FENCE = re.compile(r"```.*?(?:```|\Z)", re.S)
_MARKUP = re.compile(r"</?statement>|<cite>.*?</cite>", re.S)

#: Dates, times, ranges of a reference interval and unit exponents carry
#: digits that are not values; they are blanked before numbers are read.
_NOT_VALUES = re.compile(
    r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}"           # 2026-08-07
    r"|(?:19|20)\d{2}[-/](?:0?[1-9]|1[0-2])(?![\d.])"  # 2025-11
    r"|\d{4}\s*年|\d{1,2}\s*月|\d{1,2}\s*[日号]"  # 2026 年 8 月 7 日
    r"|\b\d{1,2}:\d{2}(?::\d{2})?\b"            # 07:30
    r"|[*^×x]\s*10\s*[\^*]?\s*\d+"              # the x10^9 of 6.5x10^9
    r"|\b10\s*[\^*]\s*\d+"                     # 10^9/L, 10*9/L
    r"|\b(?:19|20)\d{2}\b"                      # a bare year
)
_NUMBER = re.compile(r"(?<![\w.])[-+−]?\d+(?:[.,]\d+)?(?![\w])")


@dataclass(frozen=True)
class Segment:
    """A stretch of an answer: a statement with its cites, or plain text."""

    text: str
    cites: tuple[str, ...] = ()
    statement: bool = False


@dataclass(frozen=True)
class Problem:
    """One number, or one cite, that the check could not trace."""

    kind: str  # uncited_number | statement_without_cite | unknown_cite | unsupported_number
    text: str
    detail: str = ""


@dataclass
class _Reader:
    text: str
    pos: int = 0
    out: list[Segment] = field(default_factory=list)


def parse(answer: str) -> list[Segment]:
    """The answer as segments, in order. Tolerant: an unclosed statement runs
    to the next one or to the end, and a `<cite>` outside a statement cites
    the plain text since the previous segment, as LongCite's citations do."""
    reader = _Reader(answer or "")
    plain: list[str] = []
    while reader.pos < len(reader.text):
        start = reader.text.find(STATEMENT_OPEN, reader.pos)
        loose = reader.text.find(CITE_OPEN, reader.pos)
        if loose != -1 and (start == -1 or loose < start):
            plain.append(reader.text[reader.pos:loose])
            cites, reader.pos = _read_cites(reader.text, loose)
            before, claim = _split_claim("".join(plain))
            _flush(reader.out, before)
            _flush(reader.out, claim, cites, statement=True)
            plain = []
            continue
        if start == -1:
            plain.append(reader.text[reader.pos:])
            break
        plain.append(reader.text[reader.pos:start])
        _flush(reader.out, "".join(plain))
        plain = []
        body_start = start + len(STATEMENT_OPEN)
        end = _statement_end(reader.text, body_start)
        body = reader.text[body_start:end]
        cite_at = body.rfind(CITE_OPEN)
        cites: tuple[str, ...] = ()
        if cite_at != -1:
            cites, _ = _read_cites(body, cite_at)
            body = body[:cite_at]
        _flush(reader.out, body, cites, statement=True)
        reader.pos = end + (len(STATEMENT_CLOSE) if reader.text.startswith(STATEMENT_CLOSE, end) else 0)
    _flush(reader.out, "".join(plain))
    return reader.out


def _split_claim(text: str) -> tuple[str, str]:
    """A loose cite cites its own sentence, line or table cell, not everything
    since the last statement: `| 4.60 <cite>[r5]</cite> | 4.45 <cite>[r6]` is
    two claims."""
    ends = list(_CLAIM_BOUNDARY.finditer(text))
    if not ends:
        return "", text
    cut = ends[-1].end()
    return text[:cut], text[cut:]


def _statement_end(text: str, body_start: int) -> int:
    close = text.find(STATEMENT_CLOSE, body_start)
    reopen = text.find(STATEMENT_OPEN, body_start)
    candidates = [i for i in (close, reopen) if i != -1]
    return min(candidates) if candidates else len(text)


def _read_cites(text: str, cite_at: int) -> tuple[tuple[str, ...], int]:
    body_start = cite_at + len(CITE_OPEN)
    close = text.find(CITE_CLOSE, body_start)
    end = close if close != -1 else len(text)
    ids = tuple(token.strip() for token in _CITE_TOKEN.findall(text[body_start:end]) if token.strip())
    return ids, (end + len(CITE_CLOSE) if close != -1 else end)


def _flush(out: list[Segment], text: str, cites: tuple[str, ...] = (), *, statement: bool = False) -> None:
    if text.strip() or cites:
        out.append(Segment(text=text, cites=cites, statement=statement))


def strip(answer: str) -> str:
    """The answer without its markup, for a reader that does not render it."""
    return _MARKUP.sub("", answer or "")


def cite_kind(cite: str) -> str:
    """`row`, `ref`, `lines`, or `unknown`."""
    if ROW_ID.match(cite):
        return "row"
    if REF_ID.match(cite):
        return "ref"
    if LINES_ID.match(cite):
        return "lines"
    return "unknown"


def numbers(text: str) -> list[float]:
    """The values written in `text`: dates, times, years and unit exponents
    left out, `,` read as a decimal mark when it is one."""
    blanked = _NOT_VALUES.sub(" ", text or "")
    out = []
    for token in _NUMBER.findall(blanked):
        token = token.replace("−", "-").lstrip("+")
        if "," in token:
            whole, _, frac = token.partition(",")
            token = f"{whole}{frac}" if len(frac) == 3 else f"{whole}.{frac}"
        out.append(float(token))
    return out


def check(answer: str, support: Mapping[str, Collection[float]], *,
          known: Collection[str] | None = None) -> list[Problem]:
    """Every untraced number and unknown row cite in `answer`.

    `support` maps a row cite to the values that row showed (value, range
    bounds, count, min, max, mean...). `known` adds cites that resolve but
    carry no values (reference passages, document lines); a `ref:` or lines
    cite not in it is unknown only when `known` is given. Fenced blocks (a
    chart's data) are not read.
    """
    problems: list[Problem] = []
    for segment in parse(_FENCE.sub("\n", answer or "")):
        values = numbers(segment.text)
        if not segment.statement:
            problems.extend(Problem("uncited_number", segment.text.strip(), _fmt(v)) for v in values)
            continue
        if values and not segment.cites:
            problems.append(Problem("statement_without_cite", segment.text.strip()))
            continue
        cited: list[float] = []
        rows = 0
        for cite in segment.cites:
            kind = cite_kind(cite)
            if kind == "row":
                if cite not in support:
                    problems.append(Problem("unknown_cite", segment.text.strip(), cite))
                    continue
                rows += 1
                cited.extend(support[cite])
            elif known is not None and cite not in known:
                problems.append(Problem("unknown_cite", segment.text.strip(), cite))
        if not rows:
            # A document or reference statement, whose words are checked by
            # reading, or one whose only rows were unknown (reported above).
            continue
        for value in values:
            if not _traced(value, cited):
                problems.append(Problem("unsupported_number", segment.text.strip(), _fmt(value)))
    return problems


def _traced(value: float, cited: Iterable[float]) -> bool:
    cited = list(cited)
    if any(_close(value, c) for c in cited):
        return True
    derived: list[float] = []
    for a, b in permutations(cited, 2):
        derived += [a - b, a + b]
        if b:
            derived += [a / b, (a - b) / b * 100, a / b * 100]
    if cited:
        derived.append(sum(cited) / len(cited))
        derived.append(sum(cited))
    return any(_close(value, d) or _close(-value, d) for d in derived)


def _close(written: float, exact: float) -> bool:
    """Equal within 1%, or within the rounding of what was written."""
    if abs(written - exact) <= max(0.01, abs(exact) * 0.01):
        return True
    decimals = len(_fmt(written).partition(".")[2])
    return abs(written - exact) <= 0.5 * 10 ** -decimals


def _fmt(value: float) -> str:
    return f"{value:.10g}"


__all__ = [
    "CITE_CLOSE",
    "CITE_OPEN",
    "LINES_ID",
    "REF_ID",
    "ROW_ID",
    "STATEMENT_CLOSE",
    "STATEMENT_OPEN",
    "Problem",
    "Segment",
    "check",
    "cite_kind",
    "numbers",
    "parse",
    "strip",
]
