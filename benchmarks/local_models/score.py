"""The automatic checks: a Q&A transcript against its case, a document's
stored readings against its printed rows, a journal sentence's entries
against the entries it states. Pure functions; run.py calls them and
`python score.py <results/size>` re-scores saved transcripts after a change
here, without asking any model again.
"""

from __future__ import annotations

import json
import re
import sys
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import read_json, write_json

# --- text ----------------------------------------------------------------------------

# Bounded by ASCII letters and digits only: in "增加约0.46公斤" the CJK neighbours
# are words to `\w`, and a number written against them was not read.
_NUM = re.compile(r"(?<![A-Za-z0-9_.])[-−]?\d{1,3}(?:,\d{3})+(?:\.\d+)?(?![A-Za-z0-9_])"
                  r"|(?<![A-Za-z0-9_.])[-−]?\d+(?:\.\d+)?")
_CJK = re.compile(r"[㐀-鿿]")
_LATIN = re.compile(r"[A-Za-z]")
_CHART = re.compile(r"```vis-chart\s*(.*?)```", re.S)
_FENCE = re.compile(r"```.*?```", re.S)

#: Dates and times in the forms an answer writes them; their digits are a
#: date, not a value, and are taken out before numbers are read.
_MONTHS = ("january|february|march|april|may|june|july|august|september|october|november|december|"
           "jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec")
_DATE_FORMS = [
    re.compile(r"\b(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})\b"),
    re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*[日号]"),
    re.compile(rf"\b({_MONTHS})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s+(\d{{4}})\b", re.I),
    re.compile(rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+({_MONTHS})\.?,?\s+(\d{{4}})\b", re.I),
]
_DATE_NOISE = [
    # A year and a month: "7317.0", a day's step count as a tool prints it, is not one.
    re.compile(r"\b(?:19|20)\d{2}[-/.](?:0?[1-9]|1[0-2])(?:[-/.]\d{1,2})?\b(?!\.\d)"),
    re.compile(r"\d{4}\s*年(?:\s*\d{1,2}\s*月)?(?:\s*\d{1,2}\s*[日号])?"),
    re.compile(r"\d{1,2}\s*月(?:\s*\d{1,2}\s*[日号])?(?:\s*[-–至到~]\s*\d{1,2}\s*月(?:\s*\d{1,2}\s*[日号])?)?"),
    re.compile(rf"\b(?:{_MONTHS})\.?\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,?\s+\d{{4}})?\b", re.I),
    re.compile(rf"\b\d{{1,2}}(?:st|nd|rd|th)?\s+(?:{_MONTHS})\b(?:,?\s+\d{{4}})?", re.I),
    re.compile(rf"\b(?:{_MONTHS})\.?\s+\d{{4}}\b", re.I),
    re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\b"),
    re.compile(r"\b(?:19|20)\d{2}\b"),
]
_MONTH_NO = {m: i for i, names in enumerate(
    [("jan", "january"), ("feb", "february"), ("mar", "march"), ("apr", "april"), ("may",), ("jun", "june"),
     ("jul", "july"), ("aug", "august"), ("sep", "sept", "september"), ("oct", "october"), ("nov", "november"),
     ("dec", "december")], start=1) for m in names}


def nfkc(text: str) -> str:
    return unicodedata.normalize("NFKC", text or "")


def numbers(text: str) -> list[float]:
    out = []
    for m in _NUM.finditer(nfkc(text)):
        token = m.group(0).replace(",", "").replace("−", "-")
        try:
            out.append(float(token))
        except ValueError:
            continue
    return out


def strip_dates(text: str) -> str:
    text = nfkc(text)
    for pattern in _DATE_NOISE:
        text = pattern.sub(" ", text)
    return text


def dates_in(text: str) -> set[str]:
    """ISO dates an answer names, in any of the forms it writes them."""
    text = nfkc(text)
    found = set()
    for i, pattern in enumerate(_DATE_FORMS):
        for m in pattern.finditer(text):
            try:
                if i in (0, 1):
                    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                elif i == 2:
                    mo, d, y = _MONTH_NO[m.group(1).lower()], int(m.group(2)), int(m.group(3))
                else:
                    d, mo, y = int(m.group(1)), _MONTH_NO[m.group(2).lower()], int(m.group(3))
                found.add(date(y, mo, d).isoformat())
            except (ValueError, KeyError):
                continue
    return found


def language_of(text: str) -> str:
    """zh when CJK characters are at least a fifth of the letters, else en.
    Charts, code and tables of names are not prose, so fences are dropped."""
    prose = _FENCE.sub(" ", text or "")
    cjk, latin = len(_CJK.findall(prose)), len(_LATIN.findall(prose))
    if cjk + latin == 0:
        return ""
    return "zh" if cjk >= 0.2 * (cjk + latin) else "en"


def charts(text: str) -> list[dict]:
    out = []
    for block in _CHART.findall(text or ""):
        try:
            spec = json.loads(block)
        except ValueError:
            out.append({"valid": False})
            continue
        data = spec.get("data") if isinstance(spec, dict) else None
        values = []
        for point in data or []:
            if isinstance(point, dict):
                values += [v for k, v in point.items() if k in ("value", "y") and isinstance(v, (int, float))]
        out.append({"valid": bool(values), "type": spec.get("type") if isinstance(spec, dict) else None,
                    "points": len(data or []), "values": values})
    return out


# --- the transcript ------------------------------------------------------------------


def answer_of(blocks: list[dict]) -> str:
    return "".join(b.get("text", "") for b in blocks if b.get("type") == "text")


def tool_calls(blocks: list[dict]) -> list[dict]:
    return [b for b in blocks if b.get("type") == "tool_call"]


def called_names(blocks: list[dict]) -> list[str]:
    """The tools a turn called, including those called from inside `eval`
    (the code interpreter: `await tools.queryHealthIndicators({...})`), which
    reach the same tool as a direct call."""
    out = []
    for b in tool_calls(blocks):
        out.append(b.get("name", ""))
        if b.get("name") == "eval":
            code = str((b.get("args") or {}).get("code") or "")
            for m in re.finditer(r"\btools\.([A-Za-z_]\w*)\s*\(", code):
                out.append(re.sub(r"(?<!^)(?=[A-Z])", "_", m.group(1)).lower())
    return out


def tool_text(blocks: list[dict]) -> str:
    """Everything the tools returned this turn, as text."""
    parts = []
    for b in blocks:
        if b.get("type") == "tool_result":
            content = b.get("content")
            parts.append(content if isinstance(content, str) else json.dumps(content, ensure_ascii=False))
    return "\n".join(parts)


def _supported(x: float, pool: list[float]) -> bool:
    """`x` is a tool number as written or rounded, a difference of two, a
    percentage change between two, or minutes as hours."""
    ax = abs(x)
    decimals = len(str(ax).split(".")[1]) if "." in str(ax) and not str(ax).endswith(".0") else 0
    tol = 0.5 * 10 ** -decimals + 1e-9
    for p in pool:
        if abs(abs(p) - ax) <= tol or abs(abs(p) / 60 - ax) <= max(tol, 0.05):
            return True
    if len(pool) > 400:
        return False
    for i, a in enumerate(pool):
        for b in pool[i + 1:]:
            if abs(abs(a - b) - ax) <= tol:
                return True
            for base, other in ((a, b), (b, a)):
                if base and abs(abs((other - base) / base * 100) - ax) <= tol:
                    return True
    return False


def unsupported_numbers(answer: str, tools: str, question: str) -> list[float]:
    """Numbers in the answer that no tool result holds, after dates, times,
    list markers and small counts are set aside (the rule is in README.md)."""
    pool = sorted(set(numbers(strip_dates(tools))))
    asked = set(numbers(question))
    text = strip_dates(answer)
    text = re.sub(r"(?m)^\s*\d+[.)、]\s", " ", text)          # list markers
    text = re.sub(r"\brs\d+\b", " ", text, flags=re.I)        # dbSNP ids
    text = re.sub(r"(?i)\b(?:hba1c|a1c|apo\s?a1|apob|t3|t4|ldl-c|hdl-c|co2|spo2|cyp\w+|slco1b1|\w+\*\d+)\b", " ", text)
    out = []
    for x in numbers(text):
        if x in asked or (float(x).is_integer() and 0 <= x <= 12):
            continue
        if not _supported(x, pool):
            out.append(x)
    return out


def _number_present(fact: dict, nums: list[float]) -> bool:
    targets = fact.get("any") or [fact["value"]]
    return any(abs(abs(n) - abs(t)) <= fact["tol"] + 1e-9 for t in targets for n in nums)


def _durations(text: str) -> list[float]:
    """Durations an answer states, in minutes."""
    text = nfkc(text).lower()
    out = []
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*(?:h|hr|hrs|hours?|小时)\s*(?:and\s*)?(\d+(?:\.\d+)?)\s*(?:m|min|mins|minutes?|分钟?)",
                         text):
        out.append(float(m.group(1)) * 60 + float(m.group(2)))
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*(?:h|hr|hrs|hours?|小时)(?![\w\d])", text):
        out.append(float(m.group(1)) * 60)
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*(?:min|mins|minutes?|分钟)", text):
        out.append(float(m.group(1)))
    return out


def _genotype_present(gt: str, text: str) -> bool:
    a, b = gt[0], gt[1]
    forms = {a + b, b + a, f"{a}/{b}", f"{b}/{a}", f"{a};{b}", f"{b};{a}", f"{a}|{b}", f"{b}|{a}"}
    return any(re.search(rf"(?<![A-Za-z]){re.escape(f)}(?![A-Za-z])", text) for f in forms)


def fact_present(fact: dict, answer: str, nums: list[float]) -> bool:
    kind = fact["kind"]
    if kind == "number":
        return _number_present(fact, nums)
    if kind == "words":
        low = nfkc(answer).lower().replace("\u2019", "'")        # couldn’t, as GPT models write it
        return any(w.lower() in low for w in fact["any"])
    if kind == "date":
        return fact["value"] in dates_in(answer)
    if kind == "duration":
        return any(abs(d - m) <= fact["tol"] for d in _durations(answer) for m in fact["minutes"])
    if kind == "genotype":
        return _genotype_present(fact["value"], nfkc(answer))
    raise ValueError(kind)


def score_case(case: dict, run: dict) -> dict:
    """The automatic checks for one answered case."""
    blocks = run.get("blocks") or []
    answer = answer_of(blocks)
    called = called_names(blocks)
    tools = tool_text(blocks)
    finished = run.get("finish_reason") == "stop" and not run.get("timeout") and not run.get("error")
    answered = finished and bool(answer.strip()) and run.get("finish_reason") != "empty"
    nums = numbers(strip_dates(answer))
    facts = [{"label": f["label"], "ok": fact_present(f, answer, nums)} for f in case["facts"]]
    drawn = [c for c in charts(answer) if c.get("valid")]
    lang = language_of(answer)
    invented = unsupported_numbers(answer, tools, case["question"]) if answered else []
    chart_values = [v for c in drawn for v in c["values"]]
    pool = numbers(strip_dates(tools))
    chart_unsupported = [v for v in chart_values if not _supported(v, pool)]
    checks = {
        "answered": answered,
        "tool": any(t in called for t in case["expect_tools"]),
        "facts": all(f["ok"] for f in facts),
        "language": lang == case["lang"],
        "chart": bool(drawn) if case["chart"] else True,
    }
    return {
        "id": case["id"],
        "pass": all(checks.values()),
        "checks": checks,
        "facts": facts,
        "tools_called": called,
        "answer_language": lang,
        "charts": len(drawn),
        "chart_points": sum(c["points"] for c in drawn),
        "chart_values_unsupported": chart_unsupported,
        "unsupported_numbers": invented,
        "seconds": run.get("seconds"),
        "finish_reason": run.get("finish_reason"),
        "timeout": bool(run.get("timeout")),
    }


# --- extraction ----------------------------------------------------------------------

_DASHES = re.compile(r"\s*(?:--|–|—|~|～|至|-)\s*")


def norm_name(text: str) -> str:
    return re.sub(r"[\s()（）\[\]【】:：,，._\-/]", "", nfkc(text).casefold())


def norm_unit(text: str) -> str:
    t = nfkc(text).casefold().replace("µ", "u").replace("μ", "u").replace(" ", "")
    return {"次/分": "/min", "bpm": "/min", "℃": "°c", "count/min": "/min"}.get(t, t)


def norm_range(text: str) -> str:
    t = nfkc(text).strip().strip("()（）[]").replace(" ", "").replace("\n", "")
    t = re.sub(r"(?<=\d)一(?=\d)", "-", t)
    return _DASHES.sub("-", t)


def norm_value(text: str) -> str:
    return re.sub(r"[↑↓*]|(?<=\d)\s*[HL]$", "", nfkc(text).strip()).strip()


def _num(text: str) -> float | None:
    m = re.fullmatch(r"[<>≤≥]?\s*([-+]?\d+(?:\.\d+)?)", norm_value(text))
    return float(m.group(1)) if m else None


#: Flag words a cell prints after its value ("7.49mmol/L偏高").
_FLAG_WORDS = re.compile(r"(?:偏高|偏低|升高|降低|阳性|阴性|异常|正常|high|low|[HL↑↓*])$", re.I)


def _cell_number(stored: str, unit: str) -> float | None:
    """The number of a stored value that kept its whole printed cell, value,
    unit and flag together, as a CSV prints `7.49mmol/L偏高`: the reading's
    number is right and its unit is stored apart, so it is the printed row."""
    m = re.match(r"\s*([<>≤≥]?\s*[-+]?\d+(?:\.\d+)?)(?![\d.])\s*(.*)$", nfkc(stored))
    if not m:
        return None
    rest = m.group(2).strip()
    u = nfkc(unit).strip()
    if u and rest.casefold().startswith(u.casefold()):
        rest = rest[len(u):].strip()
    rest = _FLAG_WORDS.sub("", rest).strip()
    return _num(m.group(1)) if not rest else None


def _same_value(stored: str, printed: str, unit: str = "") -> bool:
    a, b = _num(stored), _num(printed)
    if a is None and b is not None:
        a = _cell_number(stored, unit)
    if a is not None and b is not None:
        return abs(a - b) < 1e-9
    x, y = norm_value(stored).casefold(), norm_value(printed).casefold()
    if not y:
        return False
    if x == y:
        return True
    # A pair kept with its unit, as "111/65mmHg" for a printed 111/65.
    rest = x[len(y):].strip() if x.startswith(y) else None
    u = nfkc(unit).strip().casefold()
    if rest and u and rest.startswith(u):
        rest = rest[len(u):].strip()
    return rest is not None and not _FLAG_WORDS.sub("", rest).strip()


def _names_match(stored: dict, name: str, loincs: set[str]) -> bool:
    a, b = norm_name(stored.get("name", "")), norm_name(name)
    if a and b and (a == b or (min(len(a), len(b)) >= 2 and (a in b or b in a))):
        return True
    return bool(stored.get("code")) and stored["code"] in loincs


def _abnormal(flag: str) -> str:
    f = nfkc(flag or "").strip().casefold()
    if not f:
        return ""
    return "0" if f in ("n", "normal", "正常", "-", "0") else "1"


def score_document(truth: dict, stored: list[dict]) -> dict:
    """One document's stored readings against its printed rows.

    A printed row is found when a stored reading from the file carries its
    value and its name (or its LOINC). A two-reading row (a blood pressure
    printed 111/65) is found when both readings are, or one reading holds the
    pair. `previous` columns are not scored: they are the earlier visit's.
    """
    rows = truth["printed_rows"]
    readings = truth["readings"]
    used: set[int] = set()
    out_rows = []
    for i, row in enumerate(rows):
        if not row.get("readable", True):
            out_rows.append({"row": i, "readable": False})
            continue
        own = [readings[j] for j in row["readings"] if readings[j]["role"] == "current"] or \
              [readings[j] for j in row["readings"]]
        loincs = {r["loinc"] for r in own if r.get("loinc")}
        hits: list[int] = []
        whole = next((k for k, s in enumerate(stored) if k not in used and _same_value(s.get("value", ""), row["item_value"], s.get("unit", ""))
                      and _names_match(s, row["item_name"], loincs)), None)
        if whole is not None:
            hits = [whole]
        elif len(own) > 1:
            for r in own:
                k = next((k for k, s in enumerate(stored) if k not in used and k not in hits
                          and _same_value(s.get("value", ""), r["value_text"], s.get("unit", ""))
                          and (_names_match(s, row["item_name"], {r.get("loinc", "")}) or s.get("code") == r.get("loinc"))),
                         None)
                if k is None:
                    hits = []
                    break
                hits.append(k)
        entry: dict[str, Any] = {"row": i, "name": row["item_name"], "value": row["item_value"], "found": bool(hits)}
        if hits:
            used.update(hits)
            s = stored[hits[0]]
            ranges = {norm_range(row["item_range"])} | {norm_range(v) for v in (row.get("alternatives") or {})
                                                          .get("item_range", [])}
            entry.update({
                "unit_ok": norm_unit(s.get("unit", "")) == norm_unit(row["item_unit"]),
                "range_ok": norm_range(s.get("ref", "")) in ranges,
                "flag_ok": (None if row["is_abnormal"] == "" else _abnormal(s.get("flag", "")) == row["is_abnormal"]),
                "stored": {k: s.get(k) for k in ("name", "value", "unit", "ref", "flag", "date", "extractor")},
            })
        out_rows.append(entry)
    previous = {(norm_value(r["value_text"])) for r in readings if r["role"] == "previous"}
    extra = [s for k, s in enumerate(stored) if k not in used and norm_value(s.get("value", "")) not in previous]
    distractors = [norm_name(d.get("text", "")) for d in truth.get("distractors") or [] if isinstance(d, dict)]
    found = [r for r in out_rows if r.get("found")]
    readable = [r for r in out_rows if r.get("readable", True)]
    flags = [r for r in found if r["flag_ok"] is not None]
    dates = sorted({s.get("date") for s in stored if s.get("date")})
    return {
        "rows": len(readable),
        "found": len(found),
        "unit_ok": sum(r["unit_ok"] for r in found),
        "range_ok": sum(r["range_ok"] for r in found),
        "flag_ok": sum(r["flag_ok"] for r in flags),
        "flag_rows": len(flags),
        "stored": len(stored),
        "extra": len(extra),
        "extra_distractors": sum(any(norm_name(s.get("name", "")) and norm_name(s.get("name", "")) in d
                                     and norm_name(s.get("value", "")) in d for d in distractors) for s in extra),
        "dates_stored": dates,
        "date_ok": bool(dates) and all(d in acceptable_dates(truth) for d in dates),
        "by_extractor": _count(s.get("extractor") or "?" for s in stored),
        "detail": out_rows,
        "extra_rows": [{k: s.get(k) for k in ("name", "value", "unit", "extractor")} for s in extra],
    }


def acceptable_dates(truth: dict) -> set[str]:
    """The days a document's readings may be filed under: the encounter, and,
    when the document prints no day of collection, testing or receipt, the
    day it says it reported. A print date is never right."""
    days = set(truth["encounter_dates"])
    printed = truth.get("dates") or []
    if not any(d["role"] in ("collected", "tested", "received") for d in printed):
        days |= {d["iso"][:10] for d in printed if d["role"] == "reported"}
    return days


def _count(items) -> dict[str, int]:
    out: dict[str, int] = {}
    for i in items:
        out[i] = out.get(i, 0) + 1
    return out


# --- journal -------------------------------------------------------------------------


def score_sentence(sentence: dict, written: list[dict]) -> dict:
    """The entries one sentence states against what the journal wrote."""
    used: set[int] = set()
    out = []
    for e in sentence["expected"]:
        hit = None
        for k, w in enumerate(written):
            if k in used:
                continue
            if e["kind"] == "measurement":
                if w.get("kind") == "measurement" and _num(str(w.get("value", ""))) is not None \
                        and abs(_num(str(w["value"])) - float(e["value"])) < 1e-9:
                    hit = k
                    break
            elif w.get("kind") == "symptom":
                text = norm_name(w.get("text", ""))
                name = norm_name(e["name"])
                coded_right = e.get("icpc3") and w.get("code") == e["icpc3"]
                if coded_right or (name and (name in text or text in name)):
                    hit = k
                    break
        row = {"kind": e["kind"], "expected": e.get("name") or f"{e.get('key')} {e.get('value')}", "found": hit is not None}
        if hit is not None:
            used.add(hit)
            w = written[hit]
            if e["kind"] == "symptom":
                if e.get("expect") == "coded":
                    row["code_ok"] = w.get("code") == e["icpc3"]
                else:
                    row["code_ok"] = not w.get("coded")
                row["code"] = w.get("code")
            row["written"] = {k: w.get(k) for k in ("kind", "text", "value", "unit", "code", "display")}
        out.append(row)
    extra = [w for k, w in enumerate(written) if k not in used and w.get("kind") != "note"]
    notes = [w for w in written if w.get("kind") == "note"]
    found = [r for r in out if r["found"]]
    coded = [r for r in found if "code_ok" in r]
    return {
        "expected": len(out),
        "found": len(found),
        "code_ok": sum(r["code_ok"] for r in coded),
        "coded_checked": len(coded),
        "extra": len(extra),
        "notes": len(notes),
        "exact": len(found) == len(out) and not extra and all(r.get("code_ok", True) for r in found),
        "detail": out,
        "extra_entries": [{k: w.get(k) for k in ("kind", "text", "value", "unit", "code")} for w in extra],
    }


# --- re-scoring saved results --------------------------------------------------------


def rescore(size_dir: Path, cases_path: Path) -> None:
    cases = {json.loads(line)["id"]: json.loads(line) for line in cases_path.read_text(encoding="utf-8").splitlines()
             if line.strip()}
    qa = read_json(size_dir / "qa.json")
    for run in qa["runs"]:
        run["score"] = score_case(cases[run["id"]], run)
    write_json(size_dir / "qa.json", qa)
    print(f"rescored {len(qa['runs'])} answers in {size_dir}")


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    for arg in sys.argv[1:] or [str(p) for p in sorted((here / "results").glob("*/qa.json"))]:
        target = Path(arg)
        rescore(target if target.is_dir() else target.parent, here / "cases.jsonl")
