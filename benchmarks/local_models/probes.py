"""Small source-derived capability probes; incomplete runs are not quality grades.

Use the pinned loading plan and its existing benchmark account. Gold is computed
from generator truth, never from product responses. Only disjoint windows whose
answers differ beyond their tolerances become window-discrimination probes.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from cases import ALIASES, device_series, metric_label
from common import Corpus, read_json, write_json
from provenance import AS_OF, digest_json, record_identity, validate_corpus
import score
from mirobody import units

ABSENCE = {"glu": ("blood glucose", "2345-7", "1558-6"),
           "insulin": ("insulin", "20448-7"), "peak_flow": ("peak flow", "19935-6")}
ABSENT = re.compile(r"\b(?:no (?:[\w-]+ ){0,3}(?:records?|readings?|measurements?|results?|data)"
                    r"|not recorded|never recorded|not on file)\b|未记录|没有(?:相关)?(?:记录|数据)|无(?:相关)?记录")


def windows(points: list[tuple[str, float]], blocked: set[str]) -> list[list[tuple[str, float]]]:
    """One bounded window in each third, avoiding overlapping document truth."""
    out = []
    for i, width in enumerate((7, 14, 21)):
        segment = points[i * len(points) // 3:(i + 1) * len(points) // 3]
        width = min(width, len(segment))
        if width < 7:
            return []
        candidates = [segment[j:j + width] for j in range(len(segment) - width + 1)
                      if not any(segment[j][0] <= day <= segment[j + width - 1][0] for day in blocked)]
        if not candidates:
            return []
        # Prefer a different numeric answer, independently of model responses.
        chosen = (max(candidates, key=lambda c: min(abs(statistics.fmean(v for _, v in c)
                                                       - statistics.fmean(v for _, v in previous)) for previous in out))
                  if out else candidates[0])
        out.append(chosen)
    return out


def distinct(probes: list[dict]) -> list[dict]:
    """A repeated answer must not pass a window probe by luck."""
    kept: list[dict] = []
    for probe in probes:
        if all(abs(probe["gold"] - old["gold"]) > probe["tol"] + old["tol"] for old in kept):
            kept.append(probe)
    return kept if len(kept) >= 2 else []


def build(corpus: Corpus, person: str, config: dict, *, max_metrics: int = 2) -> tuple[list[dict], list[dict]]:
    device = corpus.devices.get(person) or {}
    records = device.get("records") or []
    loaded = [row for file in config["documents"] for row in corpus.files[file].get("readings", [])]
    probes, skipped = [], []
    used = 0
    for metric in sorted({r["metric"] for r in records}):
        if used >= max_metrics:
            break
        source = [r for r in records if r["metric"] == metric]
        units = {str(r.get("unit") or "") for r in source}
        if len(units) != 1 or not next(iter(units)):
            skipped.append({"metric": metric, "reason": "no single recorded unit"})
            continue
        try:
            points = [(when.date().isoformat(), value) for when, value in device_series(corpus, person, metric, config["tz"])]
        except (ValueError, KeyError, TypeError, OverflowError):
            skipped.append({"metric": metric, "reason": "unsupported source timestamps or intervals"})
            continue
        if (len(points) < 30 or any(not math.isfinite(v) for _, v in points)
                or len({day for day, _ in points}) != len(points)):
            skipped.append({"metric": metric, "reason": "need 30 finite, unambiguous local days"})
            continue
        codes = {r.get("loinc") for r in source} - {None, ""}
        blocked = {str(r["observed"])[:10] for r in loaded if r.get("key") == metric or r.get("loinc") in codes}
        blocks = windows(points, blocked)
        if not blocks:
            skipped.append({"metric": metric, "reason": "no three disjoint windows without document overlap"})
            continue
        unit = "min" if metric == "sleep" else next(iter(units))
        label = metric_label(metric, "en") if metric in ALIASES else str(source[0]["indicator"])
        candidates: dict[str, list[dict]] = defaultdict(list)
        for block in blocks:
            start, end = block[0][0], block[-1][0]
            values = [v for _, v in block]
            basis = ("Use source sleep intervals in minutes, with nights labelled by the evening date "
                     "of the 18:00-based sleep window. " if metric == "sleep" else "")
            for op, capability, value, request in (
                ("mean", "C3", statistics.fmean(values), f"What was my average {label}, in {unit},"),
                ("days", "C4", len(values), f"How many distinct local days have a {label} record"),
                ("max", "C4", max(values), f"What was my highest {label}, in {unit},"),
            ):
                candidates[op].append({
                    "capability": capability, "operation": op, "metric": metric, "unit": "days" if op == "days" else unit,
                    "start": start, "end": end, "gold": round(value, 6),
                    "tol": 0 if op == "days" else max(0.05, abs(value) * 0.01),
                    "question": f"{request} from {start} through {end}? {basis}Reply with one value and cite its source.",
                    "evidence": {"file": "devices.jsonl", "person": person, "tz": config["tz"],
                                 "points": block, "policy": "source_sleep_interval_minutes" if metric == "sleep" else "recorded_value"},
                })
        accepted_groups = {op: distinct(group) for op, group in candidates.items()}
        if not accepted_groups["mean"]:
            skipped.append({"metric": metric, "reason": "window answers overlap within tolerance"})
            continue
        for operation, group in candidates.items():
            accepted = accepted_groups[operation]
            probes.extend(accepted)
            if not accepted:
                skipped.append({"metric": metric, "operation": operation, "reason": "window answers overlap within tolerance"})
        used += 1
    # Absence concerns all loaded truth, including short series and unreadable
    # document rows; an uncertain source never establishes that a metric is absent.
    journal = [r for r in corpus.journal if r["person_id"] == person]
    presence = json.dumps([records, loaded, journal], ensure_ascii=False).casefold()
    for metric, aliases in ABSENCE.items():
        names = [metric, *aliases, *ALIASES.get(metric, [])]
        if not any(name.casefold() in presence for name in names):
            probes.append({"capability": "C7", "operation": "absence", "metric": metric,
                           "gold": None, "tol": None, "expect_absent": True,
                           "question": f"Check all my loaded health records for {aliases[0]}. If it was never recorded, say so. Do not substitute another measurement.",
                           "evidence": {"files": ["devices.jsonl", *config["documents"], "journal.jsonl"],
                                        "person": person, "assertion": "no source mentions this metric or its known aliases/codes"}})
            break
    else:
        skipped.append({"capability": "C7", "reason": "no proven absence among candidate metrics"})
    for i, probe in enumerate(probes, 1):
        probe["id"] = f"{person}-probe-{i:02d}"
    return probes, skipped


def answer_number(text: str) -> float | None:
    values = score.numbers(score.strip_dates(score.prose_of(text)))
    unique = set(values)
    return next(iter(unique)) if len(unique) == 1 and all(math.isfinite(v) for v in values) else None


_STATED_UNIT = re.compile(
    r"[-−+]?\d[\d,.]*\s*"
    r"((?:[([{\"'`_][ \t]*)*(?:[^\W\d_]|[%°µμ/])[\w/%°µμ.*^+\-\[\](){}\"'`]*"
    r"(?:[ \t]*[)\]}\"'`_])*)"
)
_UNIT_WRAPPERS = {"(": ")", "[": "]", "{": "}", '"': '"', "'": "'", "`": "`", "_": "_"}


def stated_unit(token: str) -> str | None:
    token = token.strip().rstrip(".")
    while token:
        # UCUM brackets can belong to the unit, so resolve before peeling prose wrappers.
        normalized = units.normalize_unit(token)
        if normalized is not None:
            return normalized
        if _UNIT_WRAPPERS.get(token[0]) != token[-1]:
            break
        token = token[1:-1].strip()
    return None


def units_match(text: str, expected: str) -> bool:
    target = units.normalize_unit(expected)
    prose = score.strip_dates(score.prose_of(text)).replace("*", "")
    stated = [stated_unit(m.group(1)) for m in _STATED_UNIT.finditer(prose)]
    return not target or all(unit is None or unit == target for unit in stated)


def grade(probe: dict, turn: dict) -> dict:
    text = score.answer_of(turn.get("blocks", []))
    result = {**probe, "answer": text, "turn": turn, "ok": False, "cited": False}
    failed = any(turn.get(k) for k in ("error", "timeout", "configuration_error"))
    if failed or turn.get("finish_reason") != "stop" or not text.strip():
        return {**result, "status": "run_failure", "detail": "turn did not complete with a nonempty answer"}
    trace = score.trace_of(text, score.tool_text(turn.get("blocks", [])))
    cited = (trace["row_cites"] > 0 and not any(trace[k] for k in (
        "made_up", "uncited_numbers", "unsupported_numbers", "unclosed_statements")))
    got = answer_number(text)
    if probe.get("expect_absent"):
        prose = score.prose_of(text).casefold()
        absent = bool(ABSENT.search(prose))
        # A denial plus a numeric reading is contradictory, regardless of the
        # location of the denial or whether the fabricated value has a cite.
        ok = absent and not score.numbers(score.strip_dates(prose))
        detail = "absence without a fabricated numeric reading" if ok else "absence not established or contradicted"
    else:
        ok = (got is not None and abs(got - probe["gold"]) <= probe["tol"]
              and units_match(text, probe.get("unit", "")))
        detail = f"got {got!r}, expected {probe['gold']} +/- {probe['tol']}"
    return {**result, "status": "completed", "ok": ok, "cited": cited, "citation_trace": trace, "detail": detail}


def summarize(results: list[dict]) -> dict:
    counts: dict[str, dict] = {}
    for result in results:
        for capability in [result["capability"], *(["C6"] if result.get("gold") is not None else [])]:
            row = counts.setdefault(capability, {"selected": 0, "completed": 0, "passed": 0,
                                                 "run_failure": 0, "not_attempted": 0})
            row["selected"] += 1
            if result["status"] != "completed":
                row["not_attempted" if result["status"] == "not_attempted" else "run_failure"] += 1
                continue
            row["completed"] += 1
            row["passed"] += bool(result["cited"] if capability == "C6" else result["ok"])
    return counts


def validate_account(plan: dict, person: str, entry: dict) -> None:
    if (entry.get("person") != person or entry.get("record_sha256") != record_identity(plan)
            or not entry.get("loaded_at") or "devices" not in entry.get("steps", {})):
        raise ValueError("Account is not a completed load of this person and source plan; load it before probing")


def main() -> int:
    import common
    import requests
    import run as bench
    from load import LOAD_STATE

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--corpus", required=True, type=Path)
    ap.add_argument("--plan", required=True, type=Path)
    ap.add_argument("--person", required=True)
    ap.add_argument("--email", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--timeout", type=float, default=300)
    ap.add_argument("--max-metrics", type=int, choices=(1, 2, 3), default=2)
    ap.add_argument("--dry-run", action="store_true", help="save source-derived questions without account or model calls")
    args = ap.parse_args()
    corpus, plan = Corpus(args.corpus), read_json(args.plan)
    validate_corpus(corpus, plan)
    config = plan["qa_people"][args.person]
    probes, skipped = build(corpus, args.person, config, max_metrics=args.max_metrics)
    packet = {"person": args.person, "email": args.email, "source_sha256": plan["provenance"]["sha256"],
              "record_sha256": record_identity(plan), "tz": config["tz"], "as_of": AS_OF,
              "case_manifest": probes, "cases_sha256": digest_json(probes), "skipped": skipped,
              "dry_run": args.dry_run, "complete": False, "results": []}
    write_json(args.out, packet)
    if not any(p["capability"] == "C3" for p in probes):
        print(f"No discriminating C3 windows; skipped-source reasons saved to {args.out}", file=sys.stderr)
        return 2
    if args.dry_run:
        print(f"wrote {len(probes)} source-derived probes to {args.out}; no model calls")
        return 0
    entry = read_json(LOAD_STATE, {}).get(args.email, {})
    validate_account(plan, args.person, entry)
    account = common.sign_in(args.email, tz=config["tz"], lang="en")
    packet["results"] = [{**p, "status": "not_attempted", "ok": False, "cited": False, "detail": "not attempted"}
                         for p in probes]
    packet["capabilities"] = summarize(packet["results"])
    write_json(args.out, packet)
    for index, probe in enumerate(probes):
        try:
            turn = bench.ask(account, probe["question"], args.timeout, as_of=AS_OF)
        except (common.ApiError, requests.RequestException) as exc:
            turn = {"error": type(exc).__name__, "finish_reason": "error", "blocks": []}
        result = grade(probe, turn)
        packet["results"][index] = result
        packet["capabilities"] = summarize(packet["results"])
        write_json(args.out, packet)
        print(f"{result['status']} {probe['id']} {probe['capability']} ok={result['ok']} cited={result['cited']}", flush=True)
        if result["status"] == "run_failure":
            break
    packet["complete"] = True
    packet["capabilities"] = summarize(packet["results"])
    write_json(args.out, packet)
    return int(any(not r["ok"] or (r.get("gold") is not None and not r["cited"]) for r in packet["results"]))


if __name__ == "__main__":
    raise SystemExit(main())
