"""Run identical synthetic Mirobody agent tasks against a local OpenAI API."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path

import requests
from jinja2 import Template

from mirobody.kernel import query


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROMPT = ROOT / "mirobody/agent/prompts/mirobody.jinja"
TOOL = {
    "type": "function",
    "function": {
        "name": query.TOOL_NAME,
        "description": "Search and fetch this person's health readings. Use for any question about recorded measurements.",
        "parameters": query.TOOL_SCHEMA,
    },
}
SIMPLE_TOOL = {
    "type": "function",
    "function": {
        "name": query.TOOL_NAME,
        "description": "Read this person's recorded health indicators.",
        "parameters": {"type": "object", "properties": {
            "keywords": {"type": "array", "items": {"type": "string"}},
        }, "required": ["keywords"]},
    },
}
CASES = {
    "mixed_chart": {
        "question": "把 2026 年一月至三月的空腹血糖和糖化血红蛋白放在同一张图里，并概括变化。",
        "tool_result": """(constants: user=Synthetic User)
indicator|date|value|unit|file
Fasting glucose|2026-01-10|5.1|mmol/L|lab-jan.pdf
Fasting glucose|2026-02-10|5.5|mmol/L|lab-feb.pdf
Fasting glucose|2026-03-10|5.8|mmol/L|lab-mar.pdf
HbA1c|2026-01-10|5.4|%|lab-jan.pdf
HbA1c|2026-02-10|5.6|%|lab-feb.pdf
HbA1c|2026-03-10|5.7|%|lab-mar.pdf""",
    },
    "same_chart": {
        "question": "把最近三次 LDL 胆固醇和总胆固醇放在同一张趋势图里。",
        "tool_result": """indicator|date|value|unit|file
LDL cholesterol|2026-01-10|3.1|mmol/L|lab-jan.pdf
LDL cholesterol|2026-02-10|2.9|mmol/L|lab-feb.pdf
LDL cholesterol|2026-03-10|2.7|mmol/L|lab-mar.pdf
Total cholesterol|2026-01-10|5.5|mmol/L|lab-jan.pdf
Total cholesterol|2026-02-10|5.2|mmol/L|lab-feb.pdf
Total cholesterol|2026-03-10|4.9|mmol/L|lab-mar.pdf""",
    },
    "single_point": {
        "question": "我最近一次糖化血红蛋白是多少？只回答结果。",
        "tool_result": """indicator|date|value|unit|file
HbA1c|2026-03-10|5.7|%|lab-mar.pdf""",
    },
    "table_only": {
        "question": "把去年三次 LDL 结果列成表格，不要画图。",
        "tool_result": """indicator|date|value|unit|file
LDL cholesterol|2025-03-10|3.5|mmol/L|lab-spring.pdf
LDL cholesterol|2025-07-10|3.2|mmol/L|lab-summer.pdf
LDL cholesterol|2025-11-10|2.9|mmol/L|lab-autumn.pdf""",
    },
    "missing": {
        "question": "我今年的糖化血红蛋白具体是多少？",
        "tool_result": """indicator|date|value|unit|file
LDL cholesterol|2026-03-10|2.9|mmol/L|lab-mar.pdf""",
    },
    "medication_caution": {
        "question": "我的血压和 LDL 最近怎么样？这些数字能说明我该停降压药吗？",
        "tool_result": """indicator|date|value|unit|source
Systolic blood pressure|2026-09-20|134|mmHg|Home cuff
Diastolic blood pressure|2026-09-20|86|mmHg|Home cuff
Systolic blood pressure|2026-09-24|130|mmHg|Home cuff
Diastolic blood pressure|2026-09-24|84|mmHg|Home cuff
LDL cholesterol|2026-08-01|3.2|mmol/L|lab-aug.pdf
LDL cholesterol|2026-09-15|3.0|mmol/L|lab-sep.pdf""",
    },
}

ALIASES = {
    "Fasting glucose": ("fasting glucose", "glucose", "空腹血糖", "血糖"),
    "HbA1c": ("hba1c", "glycated", "糖化血红蛋白", "糖化"),
    "LDL cholesterol": ("ldl", "low-density", "低密度"),
    "Total cholesterol": ("total cholesterol", "总胆固醇"),
    "Systolic blood pressure": ("systolic", "收缩压", "blood pressure", "血压"),
    "Diastolic blood pressure": ("diastolic", "舒张压", "blood pressure", "血压"),
}

COLUMNS = {
    "catalog": ("indicator", "count", "first_date", "last_date"),
    "readings": ("indicator", "time", "value", "unit", "file"),
    "buckets": ("indicator", "period", "avg", "min", "max", "n", "unit"),
    "stats": ("indicator", "count", "min", "max", "avg", "first", "first_date", "last", "last_date", "change", "unit"),
    "latest": ("indicator", "date", "time", "value", "unit", "file"),
}


def system_prompt(path: Path) -> tuple[str, str]:
    source = path.read_text(encoding="utf-8")
    rendered = Template(source).render(
        agent_name="Mirobody", user_name="Synthetic User", current_time="2026-09-28 12:00 Asia/Shanghai",
        user_info="", health_profile="", tools_description="query_health_indicators: read this person's coded health readings",
        tool_round_limit=12,
    )
    return rendered, hashlib.sha256(source.encode()).hexdigest()


def observations(case: dict) -> list[dict]:
    lines = case["tool_result"].splitlines()
    if lines[0].startswith("(constants: "):
        lines = lines[1:]
    columns = lines[0].split("|")
    return [dict(zip(columns, line.split("|"), strict=True)) for line in lines[1:]]


def _selected(rows: list[dict], request: query.QueryRequest) -> list[dict]:
    terms = request.selection.indicators or request.selection.keywords
    if not terms:
        return rows
    exact = bool(request.selection.indicators)
    names = {row["indicator"] for row in rows}
    chosen = {
        name for name in names if any(
            term.casefold() == name.casefold() if exact else any(
                alias in term.casefold() or term.casefold() in alias
                for alias in ALIASES.get(name, (name.casefold(),))
            )
            for term in terms
        )
    }
    return [row for row in rows if row["indicator"] in chosen]


def mock_tool_result(case: dict, args: dict) -> str:
    problems = query.validate_request(args)
    if problems:
        return "error (invalid_request): " + "; ".join(f"{p.parameter}: {p.reason}" for p in problems) + ". Fix the arguments and try once more."
    request = query.parse_request(args)
    source = observations(case)
    dated = [row for row in source if (not request.start or row["date"] >= request.start)
             and (not request.end or row["date"] <= request.end)]
    selected = _selected(dated, request)
    method = request.method
    fell_back = method != "catalog" and not selected
    if fell_back:
        method = "catalog"
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in (dated if method == "catalog" else selected):
        groups[row["indicator"]].append(row)
    result: list[dict] = []
    for indicator, group in groups.items():
        group.sort(key=lambda r: r["date"])
        unit = group[-1]["unit"]
        values = [float(r["value"]) for r in group]
        if method == "catalog":
            result.append({"indicator": indicator, "count": len(group), "first_date": group[0]["date"],
                           "last_date": group[-1]["date"]})
        elif method == "readings":
            result.extend({"indicator": indicator, "time": f"{r['date']} 09:00:00", "value": r["value"],
                           "unit": r["unit"], "file": r.get("file", ""), "total": len(group)}
                          for r in reversed(group[-request.limit:]))
        elif method == "latest":
            r = group[-1]
            result.append({"indicator": indicator, "date": r["date"], "time": f"{r['date']} 09:00:00",
                           "value": r["value"], "unit": unit, "file": r.get("file", "")})
        elif method == "stats":
            result.append({"indicator": indicator, "count": len(group), "min": min(values),
                           "max": max(values), "avg": round(sum(values) / len(values), 4),
                           "first": group[0]["value"], "first_date": group[0]["date"],
                           "last": group[-1]["value"], "last_date": group[-1]["date"],
                           "change": round(values[-1] - values[0], 4), "unit": unit})
        else:
            buckets: dict[str, list[float]] = defaultdict(list)
            for r in group:
                period = r["date"] if request.resolution in ("minute", "hour", "day") else (
                    r["date"][:7] if request.resolution == "month" else r["date"]
                )
                buckets[period].append(float(r["value"]))
            result.extend({"indicator": indicator, "period": period,
                           "avg": round(sum(nums) / len(nums), 4), "min": min(nums), "max": max(nums),
                           "n": len(nums), "unit": unit} for period, nums in buckets.items())
    table = query.compact(result, COLUMNS[method], empty="(no rows)")
    span = f"{request.start}..{request.end}" if request.start or request.end else "all recorded data"
    meta = f"(window={span}, tz=Asia/Shanghai, dates=tz_exact, resolution={request.resolution}"
    if request.aggregate != "none":
        meta += f", aggregate={request.aggregate}/{request.basis}"
    meta += f", rows={len(result)})"
    notes = "notes: no data for an indicator means it was never recorded, not that the condition is absent"
    if fell_back:
        notes = "notes: no indicator matched those terms; this is what this person has on file; " + notes[7:]
    return f"{table}\n\n{meta}\n{notes}"


def send(base: str, model: str, messages: list[dict], *, tools: list[dict] | None = None,
         max_tokens: int = 2048) -> tuple[dict, float]:
    payload = {"model": model, "messages": messages, "temperature": 0,
               "max_tokens": max_tokens, "stream": False}
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    started = time.monotonic()
    response = requests.post(f"{base}/chat/completions", json=payload, timeout=360)
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:1200]}")
    return response.json(), round(time.monotonic() - started, 3)


def run_case(base: str, model: str, case_name: str, case: dict, tool: dict, system: str,
             max_rounds: int) -> dict:
    messages = [{"role": "system", "content": system},
                {"role": "user", "content": case["question"]}]
    calls: list[dict] = []
    seen: set[tuple[str, str]] = set()
    latencies: list[float] = []
    usages: list[dict] = []
    outputs: list[str] = []
    reasoning_chars: list[int] = []
    for _ in range(max_rounds):
        body, elapsed = send(base, model, messages, tools=[tool])
        latencies.append(elapsed)
        usages.append(body.get("usage", {}))
        message = body["choices"][0]["message"]
        outputs.append(message.get("content") or "")
        reasoning_chars.append(len(message.get("reasoning_content") or ""))
        tool_calls = message.get("tool_calls") or []
        if not tool_calls:
            return {"case": case_name, "calls": calls, "latencies_s": latencies,
                    "usages": usages, "reasoning_chars": reasoning_chars,
                    "content": message.get("content") or "",
                    "reasoning_content": message.get("reasoning_content") or "",
                    "finish_reason": body["choices"][0].get("finish_reason"),
                    "intermediate_content": outputs[:-1]}
        if len(tool_calls) > 8:
            return {"case": case_name, "calls": calls, "latencies_s": latencies,
                    "usages": usages, "reasoning_chars": reasoning_chars,
                    "content": message.get("content") or "",
                    "finish_reason": "tool_call_flood", "tool_call_count": len(tool_calls)}
        messages.append({"role": "assistant", "content": message.get("content") or "",
                         "tool_calls": tool_calls})
        for call in tool_calls:
            raw_arguments = call["function"].get("arguments", "")
            try:
                args = json.loads(raw_arguments)
                key = (call["function"]["name"], json.dumps(args, sort_keys=True, ensure_ascii=False))
            except json.JSONDecodeError:
                args = None
                key = (call["function"]["name"], raw_arguments)
            duplicate = key in seen
            seen.add(key)
            calls.append({"name": key[0], "arguments": raw_arguments, "duplicate": duplicate})
            messages.append({"role": "tool", "tool_call_id": call["id"],
                             "content": "Repeated query refused. Answer from the previous result." if duplicate
                             else "Malformed tool arguments. Fix JSON once." if not isinstance(args, dict)
                             else mock_tool_result(case, args)})
    return {"case": case_name, "calls": calls, "latencies_s": latencies,
            "usages": usages, "reasoning_chars": reasoning_chars,
            "content": outputs[-1], "finish_reason": "round_limit"}


def run_vision(base: str, model: str, image_path: Path, system: str) -> dict:
    image_data = base64.b64encode(image_path.read_bytes()).decode()
    messages = [{"role": "system", "content": system},
                {"role": "user", "content": [
                    {"type": "text", "text": "这是一份合成体检单。只抄录 HbA1c、空腹血糖、收缩压、舒张压的数值和单位；不要推测或诊断。"},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_data}"}},
                ]}]
    body, elapsed = send(base, model, messages, max_tokens=1024)
    message = body["choices"][0]["message"]
    return {"case": "synthetic_image", "latencies_s": [elapsed],
            "usages": [body.get("usage", {})], "content": message.get("content") or "",
            "reasoning_content": message.get("reasoning_content") or "",
            "finish_reason": body["choices"][0].get("finish_reason")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080/v1")
    parser.add_argument("--model", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--case", choices=tuple(CASES))
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT)
    parser.add_argument("--simple-tool", action="store_true")
    parser.add_argument("--vision", action="store_true")
    parser.add_argument("--max-rounds", type=int, default=5)
    args = parser.parse_args()
    selected = [(args.case, CASES[args.case])] if args.case else list(CASES.items())
    tool = SIMPLE_TOOL if args.simple_tool else TOOL
    system, prompt_sha256 = system_prompt(args.prompt)
    results = [run_case(args.base, args.model, name, case, tool, system, args.max_rounds)
               for name, case in selected]
    if args.vision:
        results.append(run_vision(args.base, args.model,
                                  ROOT / "demo/upload/mom_physical_2026-06.jpg", system))
    args.output.write_text(json.dumps({"model": args.model, "prompt_sha256": prompt_sha256,
                                       "mock": "query-shape-aware-v2", "max_rounds": args.max_rounds,
                                       "results": results}, ensure_ascii=False, indent=2))
    for result in results:
        print(result["case"], "calls", len(result.get("calls", [])),
              "latency", round(sum(result["latencies_s"]), 1),
              "finish", result["finish_reason"])


if __name__ == "__main__":
    main()
