"""Build 100 source-grounded development cases from a pinned mirobody-gen build.

Expected facts are selected from rendered-row truth, device records, genotype
sites, explicit medication changes and journal entries. No Mirobody result is
used as an oracle. The generated manifest records source hashes and unpassed
upstream audits; this is a regression set, not an independent clinical test.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import HERE, Corpus, write_json
from provenance import (AS_OF, GENERATOR_COMMIT, SUITE_ID, corpus_manifest, digest_json,
                        medication_versions, validate_cases, validate_corpus)

QHI = ["query_health_indicators"]
READ = ["read_file", "query_health_indicators"]
NO_DATA = ["no record", "no data", "not recorded", "not found", "could not find", "no result", "no reading",
           "no matching", "nothing on file", "not on file", "没有", "未找到", "暂无", "无记录", "未记录", "未查到", "查不到"]
UNCERTAIN = ["cannot", "can't", "not enough", "insufficient", "does not prove", "doesn't prove", "not evidence",
             "unknown", "cannot determine", "unable", "不能", "无法", "不足", "不代表", "不等于", "不确定"]
# Phrases read beside the words above (`score.fact_present`, on lowercased text).
# "There is no genotype file" and "No dose of amlodipine is recorded" hold none of
# NO_DATA, but a bare "no" also opens "No, your insulin is 7.2": a record noun must
# follow within three plain words, none of them a determiner or possessive.
_RECORD = r"(?:records?|results?|values?|entr(?:y|ies)|readings?|data|doses?|files?|uploads?|logs?|levels?|measurements?)"
_GAP = r"(?:(?!(?:a|an|the|your|my|this|that)\b)[a-z-]+\s+){0,3}"
NO_DATA_PATTERNS = [
    rf"\bno\s+{_GAP}{_RECORD}\b",
    (r"(?:\b(?:i|we|you)\s+(?:do|did|could)(?:\s+not|n't)\s+(?:see|find|have)|\bthere\s+(?:is|are|was|were)(?:\s+not|n't))"
     rf"\s+any\s+{_GAP}{_RECORD}\b"),
]
# "A no-call does not establish a normal genotype", "does not mean you didn't take".
UNCERTAIN_PATTERNS = [(r"\b(?:does|do|did|can|could|would|is|are)(?:\s+not|n't)\s+(?:by itself\s+|necessarily\s+)?"
                       r"(?:establish|mean|prove|show|confirm|imply|guarantee|rule out)\b")]
ALIASES = {
    "weight": ["weight", "体重"], "steps": ["steps", "step count", "步数", "步"],
    "rhr": ["resting heart rate", "resting pulse", "静息心率"], "sleep": ["sleep", "睡眠"],
    "sbp": ["systolic", "收缩压", "高压"],
    "dbp": ["diastolic", "舒张压", "低压"],
    "hr": ["heart rate", "pulse", "心率", "脉搏"],
    "hgb": ["hemoglobin", "haemoglobin", "hgb", "hb", "血红蛋白"],
    "glu": ["glucose", "glu", "血糖"], "hba1c": ["hba1c", "a1c", "糖化血红蛋白"],
    "alt": ["alt", "alanine", "谷丙", "丙氨酸"], "ast": ["ast", "aspartate", "谷草", "天门冬氨酸"],
    "ldl": ["ldl", "低密度"], "hdl": ["hdl", "高密度"], "tsh": ["tsh", "促甲状腺"],
    "ft4": ["ft4", "free t4", "游离甲状腺素"], "urine_pro": ["urine protein", "urinary protein", "尿蛋白"],
    "temp": ["temperature", "temp", "体温"], "pulse": ["pulse", "heart rate", "心率", "脉搏"],
    "spo2": ["spo2", "oxygen", "血氧"], "bmi": ["bmi", "body mass", "体重指数"],
    "qtc": ["qtc", "corrected qt", "校正qt"], "crea": ["creatinine", "肌酐"],
    "egfr": ["egfr", "glomerular", "肾小球"], "crp": ["crp", "c-reactive", "c反应"],
}
DISPLAY_ZH = {"steps": "步数", "sbp": "收缩压", "dbp": "舒张压", "hr": "心率",
              "crp": "C 反应蛋白", "qtc": "校正 QT 间期（QTc）"}


def metric_label(key: str, lang: str) -> str:
    return DISPLAY_ZH.get(key, ALIASES[key][-1]) if lang == "zh" else ALIASES[key][0]


def number(value: float, label: str, *, tol: float = .05, anchors: list[str] | None = None,
           source: list | None = None, day: str | None = None) -> dict:
    out = {"kind": "number", "value": round(float(value), 6), "tol": tol, "label": label}
    if anchors:
        out["scope"] = {"anchors": list(anchors)}
    if day:
        out.setdefault("scope", {})["date"] = day
    if source:
        out["source"] = source
    return out


# The resolution an answer naturally states each quantity in. A correctly rounded
# value is within half a step of the exact one, so that is the tolerance: 9585.57
# steps read as "9586" passes, a neighbouring day's 112 mmHg for 113 does not. A
# difference of two rounded means can be off by a whole step (53.43 and 53.86 bpm
# read as 53 and 54 differ by 1, against 0.43).
STEP = {"steps": 1, "sbp": 1, "dbp": 1, "rhr": 1, "weight": .1, "lb": .1, "percent": .1, "hours": .1}
LB_PER_KG = 1 / .45359237
# Sleep is stated in hours or minutes, so one bound must admit one-decimal hours
# (47.6 h for 2858.25 min is 2.25 min off) and stays far below a missed night.
SLEEP_TOL_MINUTES = 60 * STEP["hours"] / 2


def half_step(quantity: str) -> float:
    return STEP[quantity] / 2


def duration(minutes: float, label: str, *, anchors: list[str] | None = None) -> dict:
    """A length of time in any unit the answer chooses; `score._durations` reads h/min."""
    out = {"kind": "duration", "minutes": [round(float(minutes), 6)], "tol": SLEEP_TOL_MINUTES, "label": label}
    if anchors:
        out["scope"] = {"anchors": list(anchors)}
    return out


def aggregate(metric: str, value: float, label: str, *, anchors: list[str]) -> dict:
    """A device total or mean; the questions that sum or average sleep leave its unit to the answer."""
    if metric == "sleep":
        return duration(value, label, anchors=anchors)
    return number(value, label, tol=half_step(metric), anchors=anchors)


def right_of(facts: list[dict]) -> str:
    return "; ".join(f["label"] + "=" + str(f.get("value", f.get("any", f.get("minutes", "")))) for f in facts)


def words(values: list[str], label: str, *, anchors: list[str] | None = None,
          patterns: list[str] | None = None) -> dict:
    out = {"kind": "words", "any": values, "label": label}
    if patterns:
        out["patterns"] = list(patterns)
    if anchors:
        out["scope"] = {"anchors": list(anchors)}
    return out


def day_fact(day: str, label: str = "observation date") -> dict:
    return {"kind": "date", "value": day, "label": label}


def genotype(value: str, rsid: str) -> dict:
    return {"kind": "genotype", "value": value, "label": rsid, "scope": {"anchors": [rsid]}}


def doc_rows(corpus: Corpus, file: str, keys: set[str] | None = None) -> list[dict]:
    f = corpus.files[file]
    rows = []
    for i, reading in enumerate(f["readings"]):
        row = f["printed_rows"][reading["printed_row"]]
        if reading["role"] == "previous" or not row.get("readable", True) or row.get("unreadable_fields"):
            continue
        if keys and reading["key"] not in keys:
            continue
        rows.append({**reading, "file": file, "row": reading["printed_row"], "reading": i,
                     "name": row["item_name"], "unit": row["item_unit"], "range": row["item_range"],
                     "abnormal": row["is_abnormal"]})
    return rows


def row_fact(row: dict, *, converted: float | None = None, tol: float = .05) -> dict:
    return number(row["canonical_value"] if converted is None else converted, row["key"], tol=tol,
                  anchors=ALIASES.get(row["key"], [row["name"], row["key"]]), source=[row["file"], row["row"]])


def device_series(corpus: Corpus, person: str, metric: str, tz: str) -> list[tuple[datetime, float]]:
    records = [r for r in corpus.devices[person]["records"] if r["metric"] == metric]
    if metric != "sleep":
        return sorted((datetime.fromisoformat(r["time"]).astimezone(ZoneInfo(tz)), float(r["value"])) for r in records)
    payloads = [r for file in corpus.devices[person]["files"]
                for r in json.loads(corpus.path(file).read_text(encoding="utf-8"))["records"]]
    intervals = {(r["indicator"], r["time"]): r for r in payloads if r.get("end_time")}
    out = []
    for row in records:
        source = intervals[(row["indicator"], row["time"])]
        start = datetime.fromisoformat(source["time"]).astimezone(ZoneInfo(tz))
        end = datetime.fromisoformat(source["end_time"]).astimezone(ZoneInfo(tz))
        if end <= start or end - start >= timedelta(hours=24):
            raise ValueError("Sleep oracle requires one nonoverlapping nighttime interval per source record")
        out.append((start - timedelta(hours=18), (end - start).total_seconds() / 60))
    if len({t.date() for t, _ in out}) != len(out):
        raise ValueError("Sleep oracle needs interval union for multiple records in the same night")
    return sorted(out)


class Builder:
    def __init__(self, corpus: Corpus):
        self.c = corpus
        self.cases: list[dict] = []
        self.people = {p: {"tz": ("UTC" if p in {"p002", "p006", "p007", "p008"} else "Asia/Shanghai"),
                           "lang": "en" if p in {"p002", "p006", "p007", "p008"} else "zh",
                           "genomics": p in corpus.genomics, "medications": p in corpus.medications,
                           "documents": []} for p in corpus.people}

    def select(self, person: str, *, kind: str | None = None, keys: tuple[str, ...] = (),
               tier: str | None = None, fmt: str | None = None, hand: bool | None = False) -> dict:
        files = [f for f in self.c.files.values() if f["person_id"] == person
                 and (kind is None or f["kind"] == kind) and (tier is None or f["tier"] == tier)
                 and (fmt is None or f["format"] == fmt)
                 and (hand is None or bool(f.get("handwriting")) == hand)
                 and set(keys) <= {r["key"] for r in doc_rows(self.c, f["file"])}]
        if not files:
            raise ValueError(f"No source matches {person}, {kind}, {keys}, {tier}, {fmt}, {hand}")
        found = max(files, key=lambda f: (max(f["encounter_dates"] or [""]), f["file"]))
        documents = self.people[person]["documents"]
        if found["file"] not in documents:
            documents.append(found["file"])
        return found

    def row(self, doc: dict, key: str) -> dict:
        found = doc_rows(self.c, doc["file"], {key})
        if len(found) != 1:
            raise ValueError(f"Expected one current {key} row in {doc['file']}, got {len(found)}")
        return found[0]

    def add(self, slug: str, person: str, lang: str, domain: str, question: str, facts: list[dict],
            evidence: list[dict], *, tools: list[str] = QHI, chart: bool = False, depends_on: str | None = None,
            tags: tuple[str, ...] = (), forbidden: tuple[str, ...] = ()) -> dict:
        for fact in facts:
            own = set(fact.get("scope", {}).get("anchors", []))
            if own and fact["kind"] in {"number", "genotype"}:
                competitors = {anchor for other in facts if other is not fact and other["kind"] in {"number", "genotype"}
                               for anchor in other.get("scope", {}).get("anchors", [])} - own
                if competitors:
                    fact["scope"]["other_anchors"] = sorted(competitors)
        case = {"id": f"h100-{slug}", "suite_id": SUITE_ID, "synthetic": True, "person": person,
                "lang": lang, "domain": domain, "question": question, "facts": facts, "evidence": evidence,
                "sources": sorted({e["file"] for e in evidence}), "expect_tools": tools, "chart": chart,
                "tags": list(tags), "as_of": AS_OF, "forbidden": list(forbidden),
                "right": right_of(facts)}
        if depends_on:
            case["depends_on"] = depends_on
        self.cases.append(case)
        return case

    def dev(self, person: str, metric: str, start: str, end: str) -> tuple[list[tuple[datetime, float]], list[dict]]:
        rows = [(t, v) for t, v in device_series(self.c, person, metric, self.people[person]["tz"])
                if start <= t.date().isoformat() <= end]
        if not rows:
            raise ValueError(f"Empty device oracle: {person} {metric} {start} {end}")
        ref = {"file": "devices.jsonl", "person_id": person, "metric": metric,
               "start": start, "end": end, "tz": self.people[person]["tz"], "records": len(rows),
               "day_boundary": "18:00" if metric == "sleep" else "00:00",
               "value_policy": "source_interval_minutes" if metric == "sleep" else "reported_numeric_value"}
        if metric == "sleep":
            ref["payload_files"] = self.c.devices[person]["files"]
        return rows, [ref]


def device_cases(b: Builder) -> None:
    # Windows avoid the selected documents' overlapping measurements. The
    # final validator checks this again after document selection is complete.
    specs = [
        ("latest-weight", "p001", "zh", "weight", "2026-08-01", "2026-09-01", "latest"),
        ("latest-resting", "p002", "en", "rhr", "2026-08-01", "2026-08-31", "latest"),
        ("weight-extremes", "p003", "zh", "weight", "2026-08-01", "2026-08-31", "extremes"),
        ("weekly-steps-total", "p002", "en", "steps", "2026-08-01", "2026-08-07", "sum"),
        ("monthly-step-mean", "p001", "zh", "steps", "2026-07-01", "2026-07-31", "mean"),
        ("sleep-hours", "p002", "en", "sleep", "2026-07-01", "2026-07-31", "hours"),
        ("systolic-range", "p003", "zh", "sbp", "2026-08-01", "2026-08-31", "extremes"),
        ("resting-change", "p006", "en", "rhr", "2026-08-01", "2026-08-31", "change"),
        ("weight-span", "p005", "zh", "weight", "2026-07-01", "2026-07-31", "span"),
        ("weight-median", "p007", "en", "weight", "2026-08-01", "2026-08-31", "median"),
        ("steps-threshold", "p001", "zh", "steps", "2026-08-01", "2026-08-14", "threshold"),
        ("sleep-missing-days", "p002", "en", "sleep", "2026-08-01", "2026-08-31", "missing"),
        ("steps-92-days", "p003", "zh", "steps", "2026-06-01", "2026-08-31", "sum"),
        ("leap-day-window", "p002", "en", "steps", "2024-02-28", "2024-03-01", "sum"),
        ("previous-calendar-month", "p001", "zh", "steps", "2026-08-01", "2026-08-31", "relative"),
        ("month-boundary", "p006", "en", "rhr", "2026-07-31", "2026-08-01", "points"),
        ("blood-pressure-pair", "p003", "zh", "sbp", "2026-08-01", "2026-08-31", "bp"),
        ("kilograms-to-pounds", "p008", "en", "weight", "2026-08-01", "2026-09-01", "pounds"),
        ("weight-percent-change", "p004", "zh", "weight", "2026-08-01", "2026-08-31", "percent"),
        ("resting-week-chart", "p002", "en", "rhr", "2026-08-01", "2026-08-07", "chart"),
        ("steps-week-chart", "p006", "zh", "steps", "2026-08-01", "2026-08-07", "chart"),
        ("missing-weight-count", "p007", "en", "weight", "2026-08-01", "2026-08-31", "count"),
        ("diastolic-average", "p003", "zh", "dbp", "2026-08-01", "2026-08-31", "mean"),
        ("sleep-observed-total", "p002", "en", "sleep", "2026-08-01", "2026-08-07", "sum"),
    ]
    for slug, person, lang, metric, start, end, op in specs:
        rows, evidence = b.dev(person, metric, start, end)
        values = [v for _, v in rows]
        label = metric_label(metric, lang)
        window = f"{start} 至 {end}" if lang == "zh" else f"{start} through {end}, inclusive"
        anchor = ALIASES[metric]
        base = f"请查询 {window} 的{label}。" if lang == "zh" else f"Check my {label} for {window}. "
        if metric == "sleep":
            base += "Use source sleep intervals; label nights by the evening date of the 18:00-based sleep window. "
        tol = None if metric == "sleep" else half_step(metric)
        facts = []
        extra = {"zh": "", "en": ""}
        if op == "latest":
            facts = [number(values[-1], label, tol=tol, anchors=anchor), day_fact(rows[-1][0].date().isoformat())]
            extra = {"zh": "最新一条的数值和日期是什么？", "en": "What was the latest reading and its date?"}
        elif op in {"mean", "median", "hours", "sum", "relative"}:
            val = (statistics.median(values) if op == "median" else sum(values) if op in {"sum", "relative"}
                   else statistics.fmean(values) / (60 if op == "hours" else 1))
            facts = [number(val, label, tol=half_step("hours"), anchors=anchor) if op == "hours"
                     else aggregate(metric, val, label, anchors=anchor)]
            extra = {"zh": "请给出所有已记录数据的总和。" if op in {"sum", "relative"} else "所有已记录数值的平均值是多少？",
                     "en": {"mean": "Give the mean of the recorded readings.", "median": "Give the median of the recorded readings.",
                            "hours": "Give the mean sleep duration in hours per recorded night; do not count missing nights as zero.",
                            "sum": "Give the sum over all recorded days, with its unit.", "relative": "Give the total."}[op]}
            if op == "relative":
                base = "本题以 2026-09-02 为今天。请查上一个完整自然月的步数，说明起止日期。"
                facts += [day_fact(start, "window start"), day_fact(end, "window end")]
        elif op == "extremes":
            facts = [number(min(values), "minimum", tol=tol, anchors=["minimum", "lowest", "最小", "最低"]),
                     number(max(values), "maximum", tol=tol, anchors=["maximum", "highest", "最大", "最高"])]
            extra = {"zh": "分别给出最低值和最高值。", "en": "Give the minimum and maximum, clearly labelled."}
        elif op in {"change", "percent"}:
            change = values[-1] - values[0]
            magnitude = change / values[0] * 100 if op == "percent" else change
            facts = [number(values[0], "first", tol=tol, anchors=["first", "start", "最早", "首", "初始"]),
                     number(values[-1], "last", tol=tol, anchors=["last", "latest", "最后", "末", "最新"]),
                     number(magnitude, "signed change", tol=half_step("percent" if op == "percent" else metric)),
                     words(["increase", "rose", "higher", "增加", "上升", "升高"] if change > 0 else
                           ["decrease", "fell", "lower", "减少", "下降", "降低"], "change direction")]
            extra = {"zh": "列出首末值，算出带正负号的变化百分比（末值减首值，再除以首值），并说明升降。", "en": "Give the first and last values and their signed change (last minus first)."}
        elif op == "span":
            facts = [day_fact(rows[0][0].date().isoformat(), "first observed day"),
                     day_fact(rows[-1][0].date().isoformat(), "last observed day"), number(len(rows), "observed count", tol=0)]
            extra = {"zh": "实际有记录的第一天、最后一天及记录条数分别是多少？", "en": "Give the observed dates and count."}
        elif op == "threshold":
            facts = [number(sum(v >= 10000 for v in values), "days at least 10000 steps", tol=0)]
            extra = {"zh": "有多少天达到至少 10000 步（含恰好 10000）？", "en": "Count days with at least 10000 steps."}
        elif op in {"missing", "count"}:
            n = len({t.date() for t, _ in rows})
            facts = [number(n, "recorded days", tol=0)]
            if op == "missing":
                missing = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1 - n
                facts.append(number(missing, "unrecorded days", tol=0, anchors=["missing", "unrecorded", "no record"]))
            extra = {"zh": "有多少天有记录？", "en": "How many days have records? " +
                     ("How many have no record? Missing is not zero sleep." if op == "missing" else "Do not count missing days as zero weight.")}
        elif op in {"points", "chart"}:
            facts = [number(v, t.date().isoformat(), tol=tol, day=t.date().isoformat()) for t, v in rows]
            extra = {"zh": "按日期升序列出每天数值并画折线图。", "en": "List each date with its value in ascending order." +
                     (" Also draw a line chart." if op == "chart" else " Keep both boundary days.")}
        elif op == "bp":
            t, systolic = rows[-1]
            diastolic = next(v for dt, v in device_series(b.c, person, "dbp", b.people[person]["tz"]) if dt == t)
            facts = [number(systolic, "systolic", tol=half_step("sbp"), anchors=ALIASES["sbp"]),
                     number(diastolic, "diastolic", tol=half_step("dbp"), anchors=ALIASES["dbp"]), day_fact(t.date().isoformat())]
            evidence.append({**evidence[0], "metric": "dbp"})
            extra = {"zh": "最近一次血压的高压、低压和日期分别是什么？请区分高低压。", "en": "Give the latest pressure pair."}
        elif op == "pounds":
            facts = [number(values[-1], "weight kg", tol=half_step("weight"), anchors=["kg", "kilogram"]),
                     number(values[-1] * LB_PER_KG, "weight lb", tol=half_step("lb"), anchors=["lb", "pound"])]
            extra = {"zh": "换算为磅。", "en": "Convert the latest weight to pounds and include the original kilograms."}
        b.add(slug, person, lang, "device", base + extra[lang], facts, evidence,
              chart=op == "chart", tags=(op, metric, "deterministic_arithmetic"))


def laboratory_cases(b: Builder) -> None:
    for person, lang in [("p001", "zh"), ("p002", "en"), ("p005", "zh"), ("p007", "en")]:
        doc = b.select(person, kind="checkup_book", tier="T0", keys=("hgb", "glu", "alt", "ast", "urine_pro"))
        name, day = Path(doc["file"]).name, doc["encounter_dates"][0]
        prefix = f"请以文件 {name}（{day}）为准，" if lang == "zh" else f"Use my report {name}, dated {day}. "
        operations = {
            "p001": ("result", "range", "pair", "convert", "qualitative"),
            "p002": ("hba1c", "flag", "trend", "convert", "qualitative"),
            "p005": ("result", "range", "pair", "inequality", "classification"),
            "p007": ("result", "excess", "ratio", "compare", "qualitative"),
        }
        for op in operations[person]:
            evidence = [{"file": doc["file"], "person_id": person, "rows": []}]
            if op in {"result", "hba1c"}:
                row = b.row(doc, "hba1c" if op == "hba1c" else "glu")
                facts = [row_fact(row), words([row["unit"]], "printed unit"), day_fact(day)]
                question = "空腹血糖的数值、单位和采样日期是什么？不要拿参考范围当结果。" if lang == "zh" else "Give the fasting glucose result, unit and collection date, not the reference interval."
                if op == "hba1c":
                    question = "Give the HbA1c result, its unit and collection date. Keep it separate from fasting glucose."
                evidence[0]["rows"] = [row["row"]]
            elif op in {"flag", "excess", "classification", "inequality"}:
                key = {"flag": "glu", "excess": "alt", "classification": "hgb", "inequality": "ldl"}[op]
                row = b.row(doc, key)
                bounds = [float(n) for n in re.findall(r"\d+(?:\.\d+)?", row["range"])]
                high = bounds[-1]
                value = row["canonical_value"]
                facts = [row_fact(row)]
                if op == "flag":
                    facts += [words(["high", "above", "elevated"] if value > high else ["within", "normal"], "printed flag interpretation")]
                    question = "Does this report flag fasting glucose high? Give its actual result and interpret its own flag, not the HbA1c result."
                elif op == "excess":
                    if value <= high:
                        raise ValueError("The above-range task requires a genuinely elevated source result")
                    facts += [number(high, "reference upper limit", anchors=["upper", "limit", "reference"]),
                              number(value - high, "excess above upper limit", anchors=["above", "exceed", "over", "difference"])]
                    question = "How far is ALT above this report's upper reference limit? State ALT, the upper limit and the excess, keeping the three numbers distinct."
                elif op == "inequality":
                    facts += [number(high, "strict reference upper bound", anchors=["上限", "小于", "低于", "<"]),
                              words(["低于", "小于", "未超过"] if value < high else ["高于", "超过"], "relation to bound")]
                    question = "LDL-C 的参考范围用了不等号。请给出结果、这个上限，并说明我的值在上限哪一侧。"
                else:
                    within = bounds[0] <= value <= high
                    facts += [words(["范围内", "正常", "未超出"] if within else ["范围外", "异常", "超出"], "within printed interval")]
                    question = "血红蛋白相对本报告的参考范围是否在范围内？列出数值，并区分参考范围判断与疾病诊断。"
                evidence[0]["rows"] = [row["row"]]
            elif op in {"ratio", "compare"}:
                alt, ast = b.row(doc, "alt"), b.row(doc, "ast")
                facts = [row_fact(alt), row_fact(ast)]
                if op == "ratio":
                    facts += [number(ast["canonical_value"] / alt["canonical_value"], "AST/ALT ratio", tol=.01,
                                     anchors=["ratio", "AST/ALT"])]
                    question = "Calculate AST divided by ALT using this report. Show both source values and the ratio; this is arithmetic, not a new diagnosis."
                else:
                    winner = "ALT" if alt["canonical_value"] > ast["canonical_value"] else "AST"
                    facts += [words([winner], "higher enzyme", anchors=["higher", "larger", "greater"]),
                              number(abs(alt["canonical_value"] - ast["canonical_value"]), "absolute difference", tol=.05)]
                    question = "Which numerical result is higher, ALT or AST, and by how many U/L? Do not infer relative clinical severity from the numerical ordering."
                evidence[0]["rows"] = [alt["row"], ast["row"]]
            elif op == "trend":
                earlier_doc = b.select(person, kind="lab_slip", keys=("glu", "hba1c"), fmt="jpg")
                before, after = b.row(earlier_doc, "glu"), b.row(doc, "glu")
                facts = [number(r["canonical_value"], r["observed"], day=r["observed"],
                                source=[r["file"], r["row"]]) for r in (before, after)]
                facts.append(number(abs(after["canonical_value"] - before["canonical_value"]), "glucose change magnitude", tol=.01))
                question = (f"Compare fasting glucose here with {Path(earlier_doc['file']).name}. "
                            "Give each date/value pair and the absolute change; do not claim the change proves a treatment effect.")
                evidence[0]["rows"] = [after["row"]]
                evidence.append({"file": earlier_doc["file"], "person_id": person, "rows": [before["row"]]})
            elif op == "range":
                row = b.row(doc, "hgb")
                facts = [row_fact(row), {"kind": "range", "value": row["range"], "label": "printed hemoglobin reference interval",
                                         "scope": {"anchors": ALIASES["hgb"]}, "source": [doc["file"], row["row"]]}]
                question = "血红蛋白测量值和本报告自己的参考范围分别是多少？" if lang == "zh" else "What are the hemoglobin result and this report's own reference interval?"
                evidence[0]["rows"] = [row["row"]]
            elif op == "pair":
                rows = [b.row(doc, key) for key in ("alt", "ast")]
                facts = [row_fact(r) for r in rows]
                question = "分别列出 ALT 与 AST，标清名称和数值，别把两项互换。" if lang == "zh" else "List ALT and AST separately with their values. Keep each value attached to its test."
                evidence[0]["rows"] = [r["row"] for r in rows]
            elif op == "convert":
                row = b.row(doc, "hgb")
                # Generator hgb truth is g/L; the independent unit identity is
                # 1 dL = 0.1 L. The rendered row may use a different spelling.
                if row["unit_ucum"] != "g/L":
                    raise ValueError("The hemoglobin conversion oracle requires canonical g/L")
                facts = [number(row["canonical_value"] / 10, "hemoglobin g/dL", tol=.01, anchors=ALIASES["hgb"],
                                source=[doc["file"], row["row"]]), words(["g/dL", "g/dl", "克/分升"], "target unit")]
                question = "把血红蛋白换成 g/dL，说明换算后的单位。" if lang == "zh" else "Express hemoglobin in g/dL and label the converted unit."
                evidence[0]["rows"] = [row["row"]]
            else:
                row = b.row(doc, "urine_pro")
                raw = row["value_text"]
                options = [raw] + (["negative", "阴性"] if raw in {"阴性", "(-)", "negative"} else [])
                facts = [words(options, "qualitative urine protein", anchors=ALIASES["urine_pro"])]
                question = "尿蛋白的原始定性结果是什么？别把阴性变成数值零。" if lang == "zh" else "What is the qualitative urine protein result? Do not turn a negative result into numeric zero."
                evidence[0]["rows"] = [row["row"]]
            b.add(f"{person}-lab-{op}", person, lang, "laboratory", prefix + question, facts, evidence,
                  tools=READ, tags=(op, "printed_evidence"))


def document_cases(b: Builder) -> None:
    selections = [
        ("photo-glucose", "p002", "en", "lab_slip", ("glu", "hba1c"), "jpg", False, "glu"),
        ("handwritten-vitals", "p003", "zh", "outpatient_record", ("temp", "spo2"), "jpg", True, "spo2"),
        ("handwritten-form", "p005", "zh", "lab_slip", ("bmi", "glu"), "jpg", True, "bmi"),
        ("handwritten-correction", "p007", "en", "home_log", ("weight",), "jpg", True, "weight"),
        ("screenshot-ecg", "p007", "zh", "ecg_report", ("qtc",), "png", False, "qtc"),
        ("spreadsheet-thyroid", "p005", "en", "lab_slip", ("tsh", "ft4"), "xlsx", False, "tsh"),
        ("scan-kidney", "p008", "en", "lab_slip", ("crea", "egfr"), "pdf", False, "crea"),
        ("previous-column", "p001", "zh", "lab_slip", ("crp",), "jpg", False, "crp"),
        ("bilingual-source", "p005", "en", "checkup_book", ("hgb",), "pdf", False, "hgb"),
        ("report-finding", "p002", "en", "checkup_book", ("hgb",), "pdf", False, "finding"),
        ("report-not-a-diagnosis", "p005", "zh", "checkup_book", ("hgb",), "pdf", False, "finding"),
        ("photo-vital-pair", "p003", "zh", "outpatient_record", ("sbp", "dbp"), "jpg", True, "pair"),
    ]
    for slug, person, lang, kind, keys, fmt, hand, key in selections:
        doc = b.select(person, kind=kind, keys=keys, fmt=fmt, hand=hand)
        name = Path(doc["file"]).name
        prefix = f"读取文件 {name}，" if lang == "zh" else f"Read {name}. "
        refs = {"file": doc["file"], "person_id": person, "capture": doc["scene"],
                "format": doc["format"], "handwriting": (doc.get("handwriting") or {}).get("tier")}
        if key == "finding":
            finding = doc["findings"][0]
            facts = [words([finding["summary"], finding["surface"]], "recorded narrative finding")]
            section = {"dental": "口腔", "ecg": "心电图", "abd_us": "腹部超声", "eye": "眼科"}.get(finding["where"], finding["where"])
            question = (f"报告中{section}检查的小结写了什么？只复述报告，不推断新的诊断。" if lang == "zh" else
                        f"What finding is recorded under {finding['where']}? Cite the report and distinguish it from a new diagnosis.")
            refs["finding_id"] = finding["id"]
        elif key == "pair":
            rows = [b.row(doc, k) for k in ("sbp", "dbp")]
            facts = [row_fact(r) for r in rows]
            question = "手写血压的收缩压和舒张压分别是多少？" if lang == "zh" else "Read the handwritten blood-pressure pair."
            refs["rows"] = [r["row"] for r in rows]
        elif slug == "handwritten-correction":
            correction = doc["handwriting"]["corrections"][0]
            row = next(r for r in doc_rows(b.c, doc["file"], {key}) if r["row"] == correction["row"])
            facts = [row_fact(row), day_fact(row["observed"])]
            question = "The weight log contains a crossed-out value. What is the corrected weight and its date? Identify the correction, not the crossed-out reading."
            refs["rows"] = [row["row"]]
        else:
            row = b.row(doc, key)
            facts = [row_fact(row), day_fact(row["observed"])]
            label = metric_label(key, lang)
            question = (f"{label}的本次结果和测量日期是什么？" if lang == "zh" else
                        f"What is the current {label} result and observation date?")
            if slug == "previous-column":
                question += "区分本次和上次结果，不能把打印日期当采样日期。"
            if slug == "spreadsheet-thyroid":
                row2 = b.row(doc, "ft4")
                facts.append(row_fact(row2))
                question += " Include free T4 separately; do not interchange TSH and free T4."
            refs["rows"] = [f["source"][1] for f in facts if f.get("source")]
        # The readings stored from the file at upload are the product's reading of it,
        # as for laboratory reports; the value and date facts pin the reading.
        b.add(slug, person, lang, "document", prefix + question, facts, [refs], tools=READ,
              tags=(doc["tier"], "ocr" if doc["scene"] or hand else "text_layer", "source_reading"))


def genetic_cases(b: Builder) -> None:
    for person, lang in [("p004", "zh"), ("p008", "en")]:
        g = b.c.genomics[person]
        sites = {s["rsid"]: s for s in g["sites"]}
        for rsid in ("rs671", "rs4149056", "rs4244285"):
            site = sites[rsid]
            question = (f"我的基因文件中 {rsid} 的原始基因型是什么？写明位点，不推断患病。" if lang == "zh" else
                        f"What genotype was recorded for {rsid}? Name the site; do not turn it into a disease diagnosis.")
            b.add(f"{person}-{rsid}", person, lang, "genetics", question, [genotype(site["genotype_raw"], rsid)],
                  [{"file": "genomics.jsonl", "person_id": person, "rsids": [rsid]}], tools=["query_genetic_data"],
                  tags=("off_catalog" if not site["in_catalog"] else "catalog", "genotype"))
        site = next(s for s in g["sites"] if s["call_status"] == "no_call")
        question = (f"基因文件里的 {site['rsid']} 是否有有效检测结果？能把未检出分型当作正常吗？" if lang == "zh" else
                    f"Does {site['rsid']} have a valid call in my file? Can a no-call establish a normal genotype?")
        b.add(f"{person}-no-call", person, lang, "genetics", question,
              [words(["no-call", "no_call", "no call", "未分型", "未判定", "未检出", "缺失", "无有效", "没有有效"], "no-call"),
               words(UNCERTAIN, "no-call is not normal", patterns=UNCERTAIN_PATTERNS)],
              [{"file": "genomics.jsonl", "person_id": person, "rsids": [site["rsid"]]}], tools=["query_genetic_data"], tags=("no_call",))
        gene, drug, rsid = ("SLCO1B1", "simvastatin", "rs4149056") if lang == "zh" else ("CYP2C19", "clopidogrel", "rs4244285")
        question = (f"结合我的药物基因组结果，{gene} 与 {drug} 有什么关联？请核实 {rsid} 基因型，说明检测覆盖限制，不自行给我改剂量。" if lang == "zh" else
                    f"Check my pharmacogenomic record for {gene} and {drug}. Verify {rsid}, explain coverage limits, and do not prescribe a dose change.")
        b.add(f"{person}-pgx-evidence", person, lang, "genetics", question,
              [genotype(sites[rsid]["genotype_raw"], rsid), words([gene], "gene named"),
               words(["coverage", "incomplete", "limited", "未覆盖", "覆盖", "不完整", "有限"], "coverage limitation")],
              [{"file": "genomics.jsonl", "person_id": person, "rsids": [rsid], "gene": gene}],
              tools=["query_pharmacogenomics"], tags=("pgx", "coverage", "human_review_for_clinical_claims"))
        question = ("我的消费级基因报告能排除所有药物不良反应吗？查一下药物基因组覆盖，说明数据局限。" if lang == "zh" else
                    "Can my consumer genotype file rule out every adverse drug reaction? Check pharmacogenomic coverage and explain the limits.")
        b.add(f"{person}-pgx-limits", person, lang, "genetics", question,
              [words(UNCERTAIN, "cannot rule out all reactions", patterns=UNCERTAIN_PATTERNS), words(["coverage", "cover", "覆盖", "检测范围"], "scope of assay")],
              [{"file": "genomics.jsonl", "person_id": person, "sites": g["n_sites"], "no_calls": g["n_no_call"]}],
              tools=["query_pharmacogenomics"], tags=("pgx", "negative_evidence"))


def short_names(plan: dict) -> list[str]:
    """The name a person says: "levothyroxine" for Levothyroxine Sodium, 左甲状腺素 for 左甲状腺素钠片."""
    en = re.sub(r"\s+(?:sodium|potassium|calcium|hydrochloride|besylate)$", "", plan["generic"])
    zh = re.sub(r"^(?:盐酸|苯磺酸)|[钠钾钙]?(?:片|胶囊)$", "", plan["name_zh"])
    return [n for n in (en, zh) if n not in (plan["generic"], plan["name_zh"])]


def medication_cases(b: Builder) -> None:
    specs = [("p002", "en", 0, "regimen"), ("p003", "zh", 0, "start"), ("p004", "zh", 0, "regimen"),
             ("p005", "en", 0, "history"), ("p005", "zh", 0, "current"), ("p005", "en", 1, "prn"),
             ("p008", "en", 0, "adherence"), ("p005", "zh", 1, "course")]
    for person, lang, index, op in specs:
        plan = b.c.medications[person]["plans"][index]
        label = plan["name_zh"] if lang == "zh" else plan["name_en"]
        anchors = list(dict.fromkeys([plan["name_en"], plan["generic"], plan["name_zh"], *short_names(plan)]))
        versions = medication_versions(plan)
        strength, unit = versions[-1]["strength"].split()
        facts = [words(anchors, "medication name")]
        if op in {"regimen", "current"}:
            facts += [number(float(strength), "dose", tol=0, anchors=anchors),
                      words([unit, "mcg", "微克"] if unit == "ug" else [unit, "毫克"], "dose unit")]
            freq = plan["frequency"]
            facts.append(words(["twice", "2 times", "2次", "两次"] if freq == "bid" else
                               ["once", "1 time", "1次", "一次"], "frequency"))
            question = f"在 2026-09-01 我的{label}用药计划中，每次剂量及每日频次是什么？" if lang == "zh" else f"As of 2026-09-01, what dose and frequency does my {label} plan record?"
        elif op == "start":
            facts.append(day_fact(plan["start"], "course start"))
            question = f"我的{label}计划开始于哪一天？只说明记录，不把计划当成已服药。"
        elif op == "history":
            version = versions[0]
            amount = float(version["strength"].split()[0])
            facts += [number(amount, "historical dose", tol=0, anchors=anchors), day_fact(version["end"], "version end")]
            question = f"What dose did my {label} plan record at its start, and on which date did that dose period end? Use the historical version, not the later dose."
        elif op == "prn":
            facts += [number(float(strength), "PRN dose", tol=0, anchors=anchors), words(["as needed", "prn"], "PRN")]
            question = f"Was my historical {label} course scheduled every day or as needed? Give the dose per intake; do not invent clock times."
        elif op == "course":
            facts += [day_fact(plan["start"], "course start"), day_fact(plan["end"], "course end")]
            question = f"我的{label}短疗程记录是从哪天到哪天？不要把它说成目前仍在持续的用药。"
        else:
            facts += [words(NO_DATA, "no intake log", patterns=NO_DATA_PATTERNS),
                      words(UNCERTAIN, "no log does not prove no intake", patterns=UNCERTAIN_PATTERNS)]
            question = f"Did I actually take {label} on 2026-08-31? Check intake logs; a plan alone cannot prove ingestion or a missed dose."
        b.add(f"{person}-med-{op}", person, lang, "medication", question, facts,
              [{"file": "medications.jsonl", "person_id": person, "plan_id": plan["plan_id"], "projection": "dated_schedule_versions"}],
              tools=["query_medications"], tags=(op, "plan_is_not_intake"))


def journal_cases(b: Builder) -> None:
    for person, lang in [("p001", "zh"), ("p002", "en")]:
        entries = b.c.journal_of(person)
        grouped = defaultdict(list)
        for i, entry in enumerate(entries):
            for symptom in entry["expected"]:
                if symptom["kind"] == "symptom":
                    grouped[symptom["symptom_id"]].append((entry, symptom))
        choices = sorted(grouped, key=lambda s: (-len(grouped[s]), s))[:4]
        for op, symptom_id in zip(("latest", "first", "count", "period"), choices, strict=True):
            source = sorted(grouped[symptom_id], key=lambda s: s[0]["date"])
            name = source[0][1]["name"]
            evidence = [{"file": "journal.jsonl", "person_id": person, "symptom_id": symptom_id,
                         "dates": [s[0]["date"] for s in source], "source_kind": "self_reported"}]
            if op in {"first", "latest"}:
                selected = source[0 if op == "first" else -1][0]["date"]
                facts = [day_fact(selected)]
                question = (f"我的日记里{'第一次' if op == 'first' else '最近一次'}提到“{name}”是哪天？请把自述与确诊区分开。" if lang == "zh" else
                            f"When did I {'first' if op == 'first' else 'most recently'} report {name} in my journal? This is a report of a symptom, not a diagnosis.")
            elif op == "count":
                facts = [number(len(source), "recorded entries", tol=0)]
                question = f"日记中一共记录过多少条“{name}”？按已记录条目计数，不推断持续天数。" if lang == "zh" else f"How many journal entries record {name}? Count recorded entries, not assumed symptom-days."
            else:
                year = source[-1][0]["date"][:4]
                within = [s for s in source if s[0]["date"].startswith(year)]
                facts = [number(len(within), "entries in selected year", tol=0), day_fact(within[-1][0]["date"], "latest in year")]
                question = f"只看 {year} 年的日记，“{name}”记录过几次，最后一次是哪天？" if lang == "zh" else f"Within {year} only, how many entries mention {name}, and what is the last recorded date?"
                evidence[0]["year"] = year
            b.add(f"{person}-journal-{op}", person, lang, "journal", question, facts, evidence, tags=(op, "self_reported"))


def absence_cases(b: Builder) -> None:
    specs = [
        ("no-genomics", "p001", "en", "What is my rs4244285 genotype? Check whether I have a genotype file; do not borrow another person's result.",
         ["query_genetic_data"], "genomics.jsonl"),
        ("no-sleep-device", "p004", "zh", "2026 年 8 月我的睡眠时长是多少？先核实有无记录，没有就说没有。", QHI, "devices.jsonl"),
        ("before-observation", "p007", "en", "What was my weight on 2019-01-01? Do not use a later reading to fill the missing date.", QHI, "devices.jsonl"),
        ("no-medication-plan", "p006", "en", "Which medication plans are on file for me? Do not infer prescriptions from diagnoses or measurements.", ["query_medications"], "medications.jsonl"),
        ("future-is-not-normal", "p002", "zh", "请查 2035-01-01 的血糖记录。没有记录能否说那天血糖正常？", QHI, "files.jsonl"),
        ("unmeasured-analyte", "p007", "en", "Is there an insulin result in my uploaded reports? If absent, say so rather than substituting glucose.", QHI, "files.jsonl"),
    ]
    for slug, person, lang, question, tools, source in specs:
        facts = [words(NO_DATA, "absence explicitly reported", patterns=NO_DATA_PATTERNS)]
        if slug == "future-is-not-normal":
            facts.append(words(UNCERTAIN, "absence is not normal", patterns=UNCERTAIN_PATTERNS))
        b.add(slug, person, lang, "absence", question, facts,
              [{"file": source, "person_id": person, "assertion": "no matching records in the loaded scope"}],
              tools=tools, tags=("missingness", "abstention", "scope"))


def conversation_cases(b: Builder) -> None:
    # Both turns are real scored rows: 90 independent starts + five two-turn
    # conversations = exactly 100 HTTP chat requests, with no hidden warmups.
    specs = [("p001", "zh", "steps"), ("p002", "en", "rhr"), ("p003", "zh", "dbp"),
             ("p002", "en", "sleep"), ("p005", "zh", "weight")]
    for i, (person, lang, metric) in enumerate(specs, 1):
        start, end = ("2026-07-24", "2026-07-31") if i == 3 else ("2026-07-01", "2026-07-07")
        first, evidence = b.dev(person, metric, start, end)
        second, evidence2 = b.dev(person, metric, "2026-07-08", "2026-07-14") if i in (1, 2) else (first, evidence)
        a, z = statistics.fmean(v for _, v in first), statistics.fmean(v for _, v in second)
        label = metric_label(metric, lang)
        question = (f"{start} 到 {end} 的{label}平均值是多少？按所有原始测量条目计算，不把缺失计为零。" if lang == "zh" else
                    f"What was my mean {label} from 2026-07-01 through 2026-07-07? Average recorded days only.")
        if metric == "sleep":
            question += " Use source intervals and the evening date of the 18:00-based sleep window."
        if i == 2:
            question += " Keep this baseline window and remember the comparison label Cedar window."
        case = b.add(f"conversation-{i}-start", person, lang, "conversation", question,
                     [aggregate(metric, a, label, anchors=ALIASES[metric])], evidence, tags=("context", "start"))
        if i in (1, 2):
            question = ("那接下来的七天呢？和刚才那七天相比是高还是低？给出新均值和差值。" if lang == "zh" else
                        "And the following seven days? Give the new mean and how much it rose or fell compared with the period we just discussed.")
            direction = ["higher", "rose", "increase", "上升", "高", "增加"] if z > a else ["lower", "fell", "decrease", "下降", "低", "减少"]
            facts = [number(z, "following-week mean", tol=half_step(metric)),
                     number(abs(z - a), "difference", tol=STEP[metric]), words(direction, "direction")]
            refs = evidence + evidence2
        elif i == 3:
            question = "那刚才这段时间内最后一次舒张压是多少？请同时给出这次测量的日期。"
            facts = [number(first[-1][1], "last diastolic pressure", tol=half_step(metric), anchors=ALIASES[metric]),
                     day_fact(first[-1][0].date().isoformat())]
            refs = evidence
        elif i == 4:
            question = "What was the total sleep duration over that same period, in hours? Sum recorded nights only."
            facts = [number(sum(v for _, v in first) / 60, "total sleep hours", tol=half_step("hours"), anchors=ALIASES[metric])]
            refs = evidence
        elif i == 5:
            question = "把刚才这个平均体重换算成磅，并保留公斤值作为对照。"
            # A mean already rounded to 0.1 kg carries that error into pounds.
            facts = [number(a * LB_PER_KG, "mean pounds", tol=round(half_step("weight") * LB_PER_KG + half_step("lb"), 2),
                            anchors=["磅", "lb", "pound"]),
                     number(a, "mean kg", tol=half_step("weight"), anchors=["公斤", "千克", "kg"])]
            refs = evidence
        if i == 2:
            question += " Also repeat the original baseline start/end dates and the comparison label I asked you to remember."
            facts += [day_fact(start, "original baseline start"), day_fact(end, "original baseline end"),
                      {**words(["Cedar window"], "comparison label from the first user request"), "origin": "prior_user_request"}]
        followup = b.add(f"conversation-{i}-followup", person, lang, "conversation", question,
                         facts, refs, tools=QHI + ["python"], depends_on=case["id"], tags=("context", "followup", "date_resolution"))
        if i == 2:
            followup["long_context"] = {"minimum_summaries": 2, "max_pressure_turns": 16,
                                        "source_files": b.c.devices[person]["files"]}


def validate_source_scope(b: Builder) -> None:
    for case in b.cases:
        if case["domain"] not in {"device", "conversation"}:
            continue
        for ref in case["evidence"]:
            for file in b.people[case["person"]]["documents"]:
                collisions = [r for r in doc_rows(b.c, file, {ref["metric"]})
                              if ref["start"] <= r["observed"] <= ref["end"]]
                if collisions:
                    raise ValueError(f"Device oracle overlaps a loaded document: {case['id']} {file}")
    insulin = [r for f in b.people["p007"]["documents"] for r in doc_rows(b.c, f) if r["key"] == "insulin"]
    if insulin or "p001" in b.c.genomics or "p006" in b.c.medications:
        raise ValueError("An absence precondition is not true")


def localize_cases(b: Builder) -> None:
    """The task mix stays fixed; localization changes requests and aliases only."""
    translations = {
        "weight-extremes": ("en", "Check my weight from 2026-08-01 through 2026-08-31. Give the minimum and maximum, clearly labelled."),
        "monthly-step-mean": ("en", "What was my mean daily step count in July 2026? Average recorded days only."),
        "systolic-range": ("en", "For 2026-08-01 through 2026-08-31, what are the minimum and maximum systolic blood-pressure measurements? Use original measurement entries."),
        "weight-percent-change": ("en", "Give my first and last recorded weights in August 2026 and their signed percentage change: (last minus first) divided by first, times 100. State the direction."),
        "p001-lab-result": ("en", "Use {file}. Give the fasting glucose result, its unit and collection date, not the reference interval."),
        "p005-lab-range": ("en", "Use {file}. What are the hemoglobin result and this report's own reference interval?"),
        "p005-lab-classification": ("en", "Use {file}. Is hemoglobin within this report's reference interval? Include the result and distinguish a range comparison from a disease diagnosis."),
        "p004-rs4149056": ("en", "What genotype was recorded for rs4149056 in my genetic file? Name the site and do not infer a disease diagnosis."),
        "p004-pgx-evidence": ("en", "Check my pharmacogenomic record for SLCO1B1 and simvastatin. Verify rs4149056, explain assay coverage limits and do not prescribe a dose change."),
        "p001-journal-period": ("en", None),
        "previous-calendar-month": ("ja", "今日は 2026-09-02 とします。前の完全な暦月の歩数を調べ、期間の開始日・終了日と合計を示してください。"),
        "p001-lab-convert": ("ja", "ファイル {file} のヘモグロビン値を g/dL に換算し、換算後の単位を明記してください。"),
        "handwritten-vitals": ("ja", "ファイル {file} を読み、手書きの SpO2（血中酸素飽和度）の値と測定日を示してください。"),
        "p004-rs671": ("ja", "私の遺伝子ファイルで rs671 の遺伝子型は何ですか。座位を明記し、病気の診断と混同しないでください。"),
        "p003-med-start": ("ja", "私のアトルバスタチン（Atorvastatin）の服薬計画は何日に始まりましたか。計画の記録と実際の服用を区別してください。"),
        "blood-pressure-pair": ("zh-TW", "請查 2026-08-01 至 2026-08-31 最近一次的血壓。收縮壓、舒張壓和量測日期分別是甚麼？請明確標示高壓與低壓。"),
        "p005-lab-inequality": ("zh-TW", "請讀取 {file}。LDL-C 的參考範圍用了不等號，請列出結果、參考上限，並說明數值在上限哪一側。"),
        "handwritten-form": ("zh-TW", "請讀取 {file}，手寫表格中的身體質量指數（BMI）和量測日期分別是甚麼？請以本次記錄為準。"),
        "p004-no-call": ("zh-TW", None),
        "p001-journal-latest": ("zh-TW", None),
    }
    traditional = str.maketrans({"体": "體", "压": "壓", "缩": "縮", "张": "張", "红": "紅", "过": "過",
                                "录": "錄", "测": "測", "检": "檢", "数": "數", "无": "無", "没": "沒",
                                "为": "為", "区": "區", "间": "間", "围": "圍", "内": "內", "认": "認",
                                "药": "藥", "遗": "遺", "剂": "劑", "断": "斷", "处": "處", "于": "於",
                                "参": "參", "与": "與", "对": "對", "称": "稱", "并": "並", "应": "應",
                                "这": "這", "说": "說", "够": "夠", "类": "類", "型": "型", "单": "單",
                                "记": "記", "里": "裡", "梦": "夢", "请": "請", "确": "確", "诊": "診",
                                "开": "開", "别": "別", "号": "號"})
    japanese = {"hgb": ["ヘモグロビン", "血色素"], "spo2": ["SpO2", "血中酸素飽和度", "酸素飽和度"]}
    for case in b.cases:
        case["lang"] = "zh-CN" if case["lang"] == "zh" else case["lang"]
        slug = case["id"].removeprefix("h100-")
        if slug not in translations:
            continue
        language, question = translations[slug]
        case["lang"] = language
        if slug == "p001-journal-period":
            ref = case["evidence"][0]
            name = next(e["name"] for s in b.c.journal_of(case["person"]) for e in s["expected"]
                        if e.get("symptom_id") == ref["symptom_id"])
            question = f"In {ref['year']} only, how many journal entries mention the symptom labelled “{name}”, and what is the last date?"
        elif slug == "p004-no-call":
            rsid = case["evidence"][0]["rsids"][0]
            question = f"我的基因檔案中 {rsid} 是否有有效分型結果？未能分型可以當作正常嗎？請說明證據的限制。"
        elif slug == "p001-journal-latest":
            question = case["question"].translate(traditional)
        case["question"] = question.format(file=Path(case["evidence"][0]["file"]).name)
        for fact in case["facts"]:
            if language == "zh-TW":
                if fact["kind"] == "words":
                    fact["any"] = list(dict.fromkeys(fact["any"] + [s.translate(traditional) for s in fact["any"]]))
                if fact.get("scope"):
                    for field in ("anchors", "other_anchors"):
                        values = fact["scope"].get(field, [])
                        if values:
                            fact["scope"][field] = list(dict.fromkeys(values + [s.translate(traditional) for s in values]))
                if fact.get("scope") and fact["label"] == "bmi":
                    fact["scope"]["anchors"].append("身體質量指數")
                if fact["kind"] == "words" and fact["label"] == "no-call":
                    fact["any"] += ["未能分型", "無法分型"]
            elif language == "ja":
                if fact.get("scope") and fact["label"] in japanese:
                    fact["scope"]["anchors"] += japanese[fact["label"]]
                if slug == "p001-lab-convert" and fact.get("scope"):
                    fact["scope"]["anchors"] += japanese["hgb"]
                if slug == "previous-calendar-month" and fact.get("scope"):
                    fact["scope"]["anchors"] += ["歩数", "合計"]
                if fact["kind"] == "words" and fact["label"] == "medication name":
                    fact["any"] += ["アトルバスタチン"]
            elif language == "en" and fact["kind"] == "words" and fact["label"] == "within printed interval":
                fact["any"] += ["within", "normal", "in range"]
        case["right"] = right_of(case["facts"])
    for config in b.people.values():
        if config["lang"] == "zh":
            config["lang"] = "zh-CN"


def build(corpus: Corpus, *, generator_commit: str) -> tuple[list[dict], dict]:
    b = Builder(corpus)
    for function in (device_cases, laboratory_cases, document_cases, genetic_cases, medication_cases,
                     journal_cases, absence_cases, conversation_cases):
        function(b)
    validate_source_scope(b)
    localize_cases(b)
    if Counter(c["lang"] for c in b.cases) != {"en": 60, "zh-CN": 30, "ja": 5, "zh-TW": 5}:
        raise ValueError("Expected 60 en, 30 zh-CN, 5 ja and 5 zh-TW questions")
    if len({c["question"] for c in b.cases}) != 100:
        raise ValueError("Repeated question text")
    # A small balanced slice spans every family and includes one whole chain.
    smoke_ids = {
        "latest-weight", "weekly-steps-total", "sleep-hours", "steps-92-days", "leap-day-window", "diastolic-average",
        "p001-lab-range", "p002-lab-trend", "p005-lab-inequality", "p007-lab-excess", "p007-lab-ratio",
        "photo-glucose", "handwritten-vitals", "handwritten-form", "handwritten-correction",
        "p004-rs671", "p004-pgx-evidence", "p008-no-call", "p008-pgx-limits",
        "p002-med-regimen", "p005-med-history", "p008-med-adherence",
        "p001-journal-latest", "p002-journal-count", "p002-journal-period",
        "no-genomics", "no-sleep-device", "before-observation", "conversation-1-start", "conversation-1-followup",
    }
    for case in b.cases:
        case["smoke"] = case["id"].removeprefix("h100-") in smoke_ids
    if sum(c["smoke"] for c in b.cases) != 30:
        raise ValueError("Smoke selection must contain 30 existing cases")
    for cfg in b.people.values():
        cfg["documents"].sort()
    extraction = sorted({e["file"] for c in b.cases if c["domain"] == "document" for e in c["evidence"]})
    plan = {"provenance": corpus_manifest(corpus, b.people, generator_commit=generator_commit),
            "cases_sha256": digest_json(b.cases), "qa_people": b.people,
            "counts": {"questions": 100, "smoke": 30, "languages": dict(Counter(c["lang"] for c in b.cases)),
                       "families": dict(Counter(c["domain"] for c in b.cases)), "conversations": 5,
                       "long_context_cases": 1, "max_pressure_turns": 16,
                       "selected_documents": sum(len(cfg["documents"]) for cfg in b.people.values())},
            "extraction_docs": [{"file": f, "label": f"{corpus.files[f]['kind']} / {corpus.files[f]['tier']} / {corpus.files[f]['scene']}"}
                                for f in extraction],
            "journal_sentences": []}
    # The optional sentence-extraction pass is separate from the 100 questions.
    plan["journal_sentences"] = [s for lang in ("en", "zh")
                                 for s in [s for s in corpus.journal if s["lang"] == lang][:5]]
    validate_cases(b.cases, plan)
    return b.cases, plan


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--generator", type=Path, help="pinned mirobody-gen checkout; required when regenerating")
    ap.add_argument("--out", type=Path, default=HERE)
    ap.add_argument("--check", action="store_true", help="verify frozen cases and all input bytes without rewriting")
    ap.add_argument("--review-packets", type=Path, help="optional source/evidence packets for offline mirobody-gen review-scenarios")
    args = ap.parse_args()
    corpus = Corpus(args.corpus)
    if args.check:
        cases = [json.loads(s) for s in (args.out / "cases.jsonl").read_text(encoding="utf-8").splitlines()]
        plan = json.loads((args.out / "plan.json").read_text(encoding="utf-8"))
        validate_cases(cases, plan)
        validate_corpus(corpus, plan)
        regenerated, _ = build(corpus, generator_commit=plan["provenance"]["generator_commit"])
        if regenerated != cases:
            raise ValueError("Frozen questions differ from the current source-derived builder")
    else:
        if not args.generator:
            ap.error("--generator is required to pin the actual source revision")
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.generator, text=True).strip()
        dirty = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=args.generator, text=True).strip()
        if not commit.startswith(GENERATOR_COMMIT) or dirty:
            ap.error(f"Use a clean mirobody-gen checkout at {GENERATOR_COMMIT}; found {commit[:12]}, dirty={bool(dirty)}")
        cases, plan = build(corpus, generator_commit=commit)
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "cases.jsonl").write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in cases), encoding="utf-8")
        write_json(args.out / "plan.json", plan)
    if args.review_packets:
        packets = [{"case_id": case["id"], "synthetic": True, "purpose": "stress",
                    "facts": [{"id": f"e{i}", "text": json.dumps(fact, ensure_ascii=False, sort_keys=True)}
                              for i, fact in enumerate(case["evidence"] + case["facts"])],
                    "questions": [{"id": case["id"], "text": case["question"]}]} for case in cases]
        args.review_packets.parent.mkdir(parents=True, exist_ok=True)
        args.review_packets.write_text("".join(json.dumps(p, ensure_ascii=False) + "\n" for p in packets), encoding="utf-8")
    print(json.dumps(plan["counts"], ensure_ascii=False))
    print("Upstream verification:", plan["provenance"]["audit"]["overall"], "(preserved; not a release certification)")


if __name__ == "__main__":
    main()
