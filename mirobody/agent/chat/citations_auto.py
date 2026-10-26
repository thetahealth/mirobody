"""Attach a source only when the visible claim can be attributed unambiguously.

A matching number alone does not establish provenance. Automatic citations require
compatible units and a named source metric or an unambiguous generic continuation;
explicit encounter dates must agree too. Unknown names and ambiguous rows stay uncited.
Fenced data and existing citations are preserved. Uncited figures are an audit signal,
not a verdict that a derived number is false.
"""
from __future__ import annotations

import re
from typing import Any

from mirobody.units import normalize_unit

_NUM = re.compile(r"(?<![A-Za-z0-9_.])[-+−]?\d+(?:,\d{3})*(?:\.\d+)?(?:[eE][-+]?\d+)?")
_CITE = re.compile(r"<cite>.*?</cite>", re.S)
_FENCE = re.compile(r"```.*?```", re.S)
_RID = re.compile(r"^r\d+$")
_UNIT = re.compile(r"[ \t]*([\wµμ%°/\[\]{][\wµμ%°/^*.\-\[\]{}]{0,47})")
_PROSE_AFTER_VALUE = frozenset({"to", "from", "between", "and", "on", "at", "in", "for",
                               "before", "after", "today", "this", "the", "was", "is"})
#: One prose word is also a measured unit: "in" is inches after a value ("70 in") and a
#: preposition in running text ("7.659 in the first third"). The word after it decides.
_AMBIGUOUS_UNITS = frozenset({"in"})
#: Words that follow a preposition and never follow a unit.
_PROSE_FOLLOW = frozenset({
    "the", "a", "an", "this", "that", "these", "those", "which", "what", "both", "each",
    "every", "some", "any", "all", "most", "many", "few", "several", "one", "two", "my", "his",
    "her", "their", "our", "your", "its", "order", "total", "general", "particular", "addition",
    "comparison", "contrast", "fact", "practice", "detail", "short", "summary", "terms", "term",
    "case", "cases", "front", "between", "place", "response",
})
_CONSTANTS = re.compile(r"^\(constants:\s*(.*?)\)\s*$")
#: Keep numeric stats and observed-day trend values tied to their source row.
_FIELDS = ("value", "avg", "min", "max", "sum", "first", "last", "change",
           "first_third_mean", "last_third_mean", "delta")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_DATE_TIME = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ][0-9:.+Z-]+)?|\d{4}年\d{1,2}月\d{1,2}日|\d{1,2}:\d{2}(?::\d{2})?")
_GENERIC = re.compile(r"(?:\s|[:：*()=]|latest|previous|last|result|value|was|is|the|at|and|average|mean|maximum|minimum|sum|最新|上次|之前|结果|数值|是|为|均值|平均|最大|最小|总和)*", re.I)
_BINDING = re.compile(r"(?:\s|[:：*()=]|is|was|at|averaged|average|mean|maximum|minimum|sum|measured|recorded|result|value|fell|rose|from|to|by|and|的|是|为|均值|平均|测得|结果|数值|最大|最小|总和)*", re.I)


def _tolerance(target: float) -> float:
    """How close two numbers must be to be the same reading, in the value's own scale.

    The floor used to be 0.011 absolute, which for a concentration of 0.003 g/L makes every
    figure in the row "match": a row with min=0.002, avg=0.003, max=0.004 endorsed "0.01 g/L".
    The tolerance now follows the magnitude, with only a tiny floor for floating point.
    """
    return max(abs(target) * 0.002, 1e-9)


def _cells(line: str) -> list[str]:
    cells, current = [], []
    index = 0
    while index < len(line):
        char = line[index]
        if char == "\\" and index + 1 < len(line):
            index += 1
            current.append("\n" if line[index] == "n" else line[index])
        elif char == "|":
            cells.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        index += 1
    return [*cells, "".join(current).strip()]


def rows_from_result(content: str) -> list[dict[str, Any]]:
    """Decode compact record tables, hoisted constants and aggregate value fields."""
    records: list[dict[str, str]] = []
    constants: dict[str, str] = {}
    header: list[str] | None = None

    def flush() -> None:
        if header is None and _RID.fullmatch(constants.get("rid", "")):
            records.append(dict(constants))

    for line in [*content.splitlines(), ""]:
        stripped = line.strip()
        if not stripped:
            flush()
            constants, header = {}, None
            continue
        match = _CONSTANTS.match(stripped)
        if match:
            flush()
            constants, header = {}, None
            for item in re.split(r",\s+(?=[A-Za-z_]\w*=)", match.group(1)):
                if "=" in item:
                    key, value = item.split("=", 1)
                    if key in constants:
                        constants = {}
                        break
                    constants[key] = value
            continue
        parts = _cells(stripped)
        if "rid" in parts and any(k in parts or k in constants for k in _FIELDS):
            header = parts
            continue
        if header is not None and len(parts) == len(header):
            row = {**constants, **dict(zip(header, parts, strict=True))}
            if _RID.fullmatch(row.get("rid", "")):
                records.append(row)

    parsed = []
    for row in records:
        for field in _FIELDS:
            try:
                value = float(row[field])
            except (KeyError, TypeError, ValueError):
                continue
            parsed.append({**row, "_value": value, "_field": field})
    return parsed


def _aliases(row: dict[str, Any]) -> set[str]:
    return {str(row.get(key) or "").strip().casefold() for key in ("name", "indicator")} - {""}


def _looks_prepositional(rest: str) -> bool:
    """Whether the word after a value opens a phrase, not a unit.

    "7.659 in the first third" is prose; "Height is 70 in." is inches. A unit is never followed
    by a determiner, a number or a proper noun, so those settle it.
    """
    match = re.match(r"[ \t]*([A-Za-z0-9][\w-]*)", rest)
    if match is None:
        return False
    word = match.group(1)
    if word[0].isdigit() or word[0].isupper():
        return True
    return word.casefold() in _PROSE_FOLLOW


def _unit_after(text: str, start: int) -> tuple[int, str]:
    """The unit written after a number, through the wrappers a report puts around it.

    "(mg/dL)" and "**mg/dL**" are the same claim as "mg/dL": reading only the first token
    returns nothing for them, and an empty unit is accepted as "no claim", so a mmol/L row
    could be cited to a sentence that explicitly said mg/dL.
    """
    stripped = text[start:start + 40].lstrip()
    wrapper = len(text[start:start + 40]) - len(stripped)
    while stripped[:1] in ("(", "[", "*", "_", '"', "'", "``", "`"):
        stripped, wrapper = stripped[1:], wrapper + 1
    match = _UNIT.match(stripped)
    if match is None:
        return start, ""
    token = match.group(1).rstrip(".")
    folded = token.casefold()
    # An explicit unit must be validated even when the same letters also read as prose: dropping
    # "in" let "Height is 70 in." cite a 70 cm record as if the sentence had stated no unit.
    if folded in _PROSE_AFTER_VALUE and not (
            folded in _AMBIGUOUS_UNITS and not _looks_prepositional(stripped[match.end(1):])):
        return start, ""
    return start + wrapper + match.start(1) + len(token), token


def _binding_text(text: str) -> str:
    text = _DATE_TIME.sub("", text)
    out, cursor = [], 0
    for match in _NUM.finditer(text):
        if match.start() < cursor:
            continue
        end, unit = _unit_after(text, match.end())
        out.append(text[cursor:match.start()])
        out.append(" ")
        cursor = end if unit and normalize_unit(unit) else match.end()
    return "".join([*out, text[cursor:]]).strip()


def _date_matches(row: dict[str, Any], dates: set[str]) -> bool:
    bounds = {str(row.get(k) or "")[:10] for k in ("first_date", "last_date")} - {""}
    if bounds and len(bounds) == 2:
        return bounds == dates
    observed = {str(row.get(k) or "")[:10] for k in ("time", "date", "period")} - {""}
    return len(dates) == 1 and bool(dates & (observed | bounds))


def _unit_matches(row: dict[str, Any], unit: str) -> bool:
    if not unit:
        return True
    wanted = normalize_unit(unit)
    if wanted is None:
        return False
    recorded = normalize_unit(str(row.get("unit") or ""))
    # A generic count gains its entity only from the named metric, never from
    # dimensional equivalence between unrelated counted things.
    return wanted == recorded or (recorded == "{#}" and wanted.startswith("{") and wanted.endswith("}")
                                  and any(normalize_unit(n) == wanted for n in _aliases(row)))


#: The words an answer uses for each statistic a row reports.
_STATISTIC_WORDS = {
    "maximum": "max", "max": "max", "highest": "max", "peak": "max", "最高": "max", "最大": "max",
    "minimum": "min", "min": "min", "lowest": "min", "最低": "min", "最小": "min",
    "average": "avg", "mean": "avg", "averaged": "avg", "平均": "avg", "均值": "avg",
    "sum": "sum", "total": "sum", "总和": "sum", "合计": "sum",
    "median": "median",
}


def _statistic_before(window: str) -> str:
    """The field an answer is talking about, or "" when it names no statistic."""
    recent = window[-60:]
    found = [(match.start(), field) for word, field in _STATISTIC_WORDS.items()
             for match in re.finditer(rf"(?<![A-Za-z]){re.escape(word)}(?![A-Za-z])", recent)]
    if not found:
        return ""
    position, field = max(found)
    return field


#: Verbs a sentence uses to state a direction, with the sign they assert.
_CHANGE_WORDS = {
    "fell": -1, "fall": -1, "falls": -1, "falling": -1, "fallen": -1, "dropped": -1, "drop": -1,
    "drops": -1, "dropping": -1, "decreased": -1, "decrease": -1, "decreases": -1, "declined": -1,
    "decline": -1, "declines": -1, "reduced": -1, "reduce": -1, "reduction": -1, "lowered": -1,
    "down": -1, "lost": -1, "shrank": -1, "shrunk": -1,
    "rose": 1, "rise": 1, "rises": 1, "rising": 1, "risen": 1, "increased": 1, "increase": 1,
    "increases": 1, "gained": 1, "gain": 1, "gains": 1, "grew": 1, "grow": 1, "grows": 1,
    "climbed": 1, "up": 1, "higher": 1,
}
_ZH_CHANGE = {"下降": -1, "降低": -1, "减少": -1, "下跌": -1, "回落": -1, "减轻": -1,
              "上升": 1, "升高": 1, "增加": 1, "上涨": 1, "增长": 1}
_CLAIM_WORDS = set(_CHANGE_WORDS) | {
    "is", "was", "at", "measured", "recorded", "had", "a", "an", "the", "change", "difference",
    "delta", "loss", "of", "by", "from", "to", "and", "between", "about", "approximately",
    "roughly", "sharply", "slightly", "nearly", "around",
}
# A role such as "fell by" permits its grammar, never an unknown metric
# between the source's name and the number.
_CLAIM_BINDING = re.compile(
    rf"(?:\s|[:：*()=]|(?<![A-Za-z])(?:{'|'.join(sorted(_CLAIM_WORDS, key=len, reverse=True))})(?![A-Za-z])"
    rf"|{'|'.join(sorted(_ZH_CHANGE, key=len, reverse=True))}|了|约|近|幅度为|幅度约|幅度|从|到|至|的|是|为)*",
    re.I,
)
_CHANGE_VERB = "|".join(sorted(map(re.escape, _CHANGE_WORDS), key=len, reverse=True))
#: "fell by 2", "dropped sharply by 2", and "a decrease of 2" state an amount of change.
_CHANGE_BY = re.compile(rf"\b(?P<verb>{_CHANGE_VERB})\b[^.;:!?\d]{{0,24}}?\bby\s*$", re.I)
_CHANGE_OF = re.compile(r"\b(?:change|difference|delta|decrease|increase|drop|rise|gain|loss|"
                        r"reduction|decline)\s+of\s*$", re.I)
_ZH_CHANGE_BEFORE = re.compile(r"(?:下降|降低|减少|下跌|回落|减轻|上升|升高|增加|上涨|增长)"
                               r"(?:了|约|近|幅度为|幅度约|幅度)?\s*$")
_FIRST_BOUND = re.compile(r"(?:^|[^\w])(?:from|从|between)\s*(?:about\s+|approximately\s+|roughly\s+|~|约)?$", re.I)
_LAST_BOUND = re.compile(r"(?:^|[^\w])(?:to|到|至)\s*(?:about\s+|approximately\s+|roughly\s+|~|约)?$", re.I)


def _direction(window: str) -> int:
    """The direction a sentence states anywhere in its clause, or 0 when it states none."""
    for word in re.findall(r"[A-Za-z]+", window):
        sign = _CHANGE_WORDS.get(word.casefold())
        if sign:
            return sign
    for word, sign in _ZH_CHANGE.items():
        if word in window:
            return sign
    return 0


def _claim(before: str, context: str) -> tuple[str, int] | None:
    """What this number is in its own clause: an amount of change, or one end of a change.

    A change claim binds to the record's `delta`/`change` field and must agree with the stated
    sign; an endpoint claim binds to `first`/`last` (or the thirds' means) and must agree with
    the direction the two ends actually moved. When the clause is ambiguous the answer stays
    uncited: "Sleep fell by 6 h." was citing the last-third mean of 6 while the change was -2.
    """
    window = before[-80:].casefold()
    after = context[len(before):][:80].casefold()
    direction = _direction(window)
    if _FIRST_BOUND.search(window):
        opens_a_pair = bool(re.search(r"\bto\b|到|至|\band\b", after)) or "between" in window
        if direction or opens_a_pair:
            return "first", direction
    if _LAST_BOUND.search(window) and any(word in window for word in ("from", "从", "between")):
        return "last", direction
    if re.search(r"\band\s*$", window) and "between" in window:
        return "last", direction
    match = _CHANGE_BY.search(window)
    if match:
        return "change", _CHANGE_WORDS.get(match.group("verb").casefold(), 0) or direction
    if _CHANGE_OF.search(window) or _ZH_CHANGE_BEFORE.search(before):
        return "change", direction
    return None


def _number(value: Any) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _endpoints_agree(row: dict[str, Any], direction: int) -> bool:
    """Whether the row's two ends moved the way the sentence says they did."""
    if not direction:
        return True
    first = _number(row.get("first_third_mean"))
    if first is None:
        first = _number(row.get("first"))
    last = _number(row.get("last_third_mean"))
    if last is None:
        last = _number(row.get("last"))
    if first is None or last is None:
        return True
    return (last - first) * direction > 0


def _candidates(rows: list[dict[str, Any]], value: float,
                claim: tuple[str, int] | None, *, signed: bool) -> list[dict[str, Any]]:
    """Rows whose value the sentence's number states, honouring a change's sign.

    "Sleep fell by 2" states a magnitude of 2 with a direction, so the record's -2 delta is the
    matching row; matching only the signed number left every true change claim uncited.
    """
    targets = {value}
    if claim and claim[0] == "change" and not signed:
        if claim[1] < 0:
            targets = {-value}
        elif claim[1] > 0:
            targets = {value}
        else:
            targets = {value, -value}
    return [row for row in rows
            if any(abs(target - row["_value"]) <= _tolerance(row["_value"]) for target in targets)]


def _choose(before: str, context: str, unit: str, candidates: list[dict[str, Any]],
            names: set[str], claim: tuple[str, int] | None = None) -> dict[str, Any] | None:
    # The field belongs in the key: an average and a minimum that happen to be the same number
    # are two claims about one row, and collapsing them kept whichever came last - which is how
    # "Body weight averaged 70" resolved to the row's minimum.
    candidates = list({(r["rid"], r.get("_field", ""), r["_value"], r.get("unit", "")): r
                       for r in candidates}.values())
    original_count = len({r["rid"] for r in candidates})
    dates = set(_DATE.findall(context))
    dates.update(f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
                 for y, m, d in re.findall(r"(\d{4})年(\d{1,2})月(\d{1,2})日", context))
    if dates:
        candidates = [r for r in candidates if _date_matches(r, dates)]
    window = re.sub(r"<[^>]+>", "", before).casefold()
    matched = [(window.rfind(name) + len(name), len(name), name) for name in names if name in window]
    name_end, _, nearest = max(matched, default=(-1, 0, ""))
    named = [r for r in candidates if nearest and nearest in _aliases(r)]
    grammar = _CLAIM_BINDING if claim is not None else _BINDING
    if nearest:
        if not grammar.fullmatch(_binding_text(window[name_end:])):
            return None
        candidates = named
    elif original_count != 1 or not (grammar if claim is not None else _GENERIC).fullmatch(
            _binding_text(window)):
        return None
    candidates = [r for r in candidates if _unit_matches(r, unit)]
    # A change sentence states the movement, not either end it moved between.
    if claim is not None:
        kind, direction = claim
        if kind == "change":
            candidates = [r for r in candidates if r.get("_field") in ("delta", "change")]
            if direction:
                candidates = [r for r in candidates if r["_value"] * direction > 0]
        else:
            fields = ("first", "first_third_mean") if kind == "first" else ("last", "last_third_mean")
            candidates = [r for r in candidates
                          if r.get("_field") in fields and _endpoints_agree(r, direction)]
        if not candidates:
            return None
    # A sentence that names a statistic means that field.
    operation = window[:name_end - len(nearest)] + " " + window[name_end:] if nearest else window
    statistic = _statistic_before(operation)
    if statistic:
        by_field = [r for r in candidates if r.get("_field") == statistic]
        if not by_field:
            return None
        candidates = by_field
    return candidates[0] if len({r["rid"] for r in candidates}) == 1 else None


def attach_citations(text: str, rows: list[dict[str, Any]]) -> tuple[str, int, list[str]]:
    """Return rewritten text, inserted citation count and figures left unattributed."""
    if not text:
        return text, 0, []
    regions = [m.span() for pattern in (_FENCE, _CITE, _DATE_TIME) for m in pattern.finditer(text)]
    names = {name for row in rows for name in _aliases(row)}
    out: list[str] = []
    cursor, cited = 0, 0
    uncited = []
    boundaries = [m.start() for m in re.finditer(r"[\n;；，。!?,]|\.(?!\d)|(?<!\d)\.", text)]
    for match in _NUM.finditer(text):
        if match.start() < cursor or any(start <= match.start() < end for start, end in regions):
            continue
        if re.match(r"[A-Za-z]", text[match.end():]):
            continue
        value = float(match.group().replace(",", "").replace("−", "-"))
        end, unit = _unit_after(text, match.end())
        left = max([0, *(i + 1 for i in boundaries if i < match.start())])
        right = min([len(text), *(i for i in boundaries if i >= end)])
        before = text[max(left, match.start() - 120):match.start()]
        context = text[max(left, match.start() - 120):min(right, end + 80)]
        claim = _claim(before, context)
        signed = match.group().startswith(("+", "-", "−"))
        chosen = _choose(before, context, unit, _candidates(rows, value, claim, signed=signed), names, claim)
        out.append(text[cursor:end])
        if chosen is not None:
            if not text[end:end + 6].startswith("<cite>"):
                out.append(f"<cite>[{chosen['rid']}]</cite>")
                cited += 1
        else:
            uncited.append(match.group())
        cursor = end
    out.append(text[cursor:])
    return "".join(out), cited, uncited


def collect_rows(transcript: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for block in transcript:
        if block.get("type") == "tool_result" and block.get("status") in (None, "ok"):
            rows.extend(rows_from_result(str(block.get("content") or "")))
    return rows
