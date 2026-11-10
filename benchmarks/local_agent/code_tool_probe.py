"""Compare code languages on synthetic records through the production harness.

Cloud models infer remotely; Python and JavaScript execute only in local
networkless Docker containers. The Python arm uses the production `code_exec`
Python engine (`code_tool.python_code`), under the arm name its recorded runs
use. Both arms read original Envelope artifacts through ToolRuntime.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import json
import logging
import math
import re
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from langchain.tools import ToolRuntime
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import StructuredTool, tool
from deepagents.backends import StateBackend

from mirobody.agent import harness
from mirobody.agent.models.clients import build_chat_model
from mirobody.agent.python_sandbox import LocalPythonSandbox
from mirobody.agent.code_tool import COMPUTATION_NOTE, python_code, source_results
from mirobody.agent.tool_loader import _envelope_wrapper
from mirobody.agent.tools._base import RecordTool
from mirobody.kernel import tools
from mirobody.utils.config import Config
from mirobody.utils.config.llm import chat_entries

#: The production Python engine under this study's name for its arm: the
#: recorded runs in code_tool_language_runs.json, and their replay, key on it.
python_exec = python_code.model_copy(update={"name": "python_exec"})

SYSTEM = """You analyze synthetic health readings, using only the supplied tools.
Query the readings first, then use the code tool for every requested calculation.
Original rows are injected as data; never paste a display table or literal
readings into code. Each row has indicator, date, value, unit and rid.
Use the standard library or built-ins; there are no external packages.
Print a JSON object with the exact result labels requested in the question.
After code succeeds, return that same JSON object as the final answer.
The result labels are names, never additional input data.
"""


def _d(start: str, days: int) -> str:
    return (date.fromisoformat(start) + timedelta(days=days)).isoformat()


ROWS: list[dict] = []


def add(indicator: str, when: str, value: float, unit: str) -> None:
    ROWS.append({"indicator": indicator, "date": when, "value": value, "unit": unit})


def build_rows() -> None:
    if ROWS:
        return
    # Resting heart rate, every third day, March through August.
    for i in range(61):
        add("Resting heart rate", _d("2026-03-01", 3 * i),
            round(45.0 + 0.8 * math.sin(i / 3.0) + 0.02 * i, 1), "bpm")
    # One month of nightly sleep.
    for i in range(31):
        add("Sleep duration", f"2026-07-{i + 1:02d}", 407 + ((i * 7) % 23) - 11, "min")
    # Weekly weight, January through August.
    for week in range(32):
        add("Weight", _d("2026-01-05", 7 * week),
            round(84.2 + 0.035 * week + 0.15 * math.sin(week), 2), "kg")
    # Blood pressure twice a week, April through June.
    for i in range(24):
        when = _d("2026-04-03", 3 * i)
        add("Systolic blood pressure", when, 112 + (i % 5), "mmHg")
        add("Diastolic blood pressure", when, 68 + (i % 4), "mmHg")
    # Three HbA1c results.
    for when, value in (("2024-04-10", 6.6), ("2025-04-05", 6.1), ("2026-03-31", 6.1)):
        add("HbA1c", when, value, "%")
    # Daily steps in August.
    for i in range(31):
        add("Steps", f"2026-08-{i + 1:02d}", 2000 + ((i * 977) % 10000), "steps")
    # Monthly LDL, January through June.
    for month in range(1, 7):
        add("LDL cholesterol", f"2026-{month:02d}-10",
            round(3.3 - 0.1 * month + 0.05 * (month % 2), 2), "mmol/L")
    # Monthly fasting glucose, January through June.
    for month in range(1, 7):
        add("Fasting glucose", f"2026-{month:02d}-10", 95 + (month * 3) % 10, "mg/dL")


ALIASES = {
    "Resting heart rate": ("resting heart rate", "rhr", "静息心率", "心率"),
    "Sleep duration": ("sleep", "睡眠"),
    "Weight": ("weight", "体重"),
    "Systolic blood pressure": ("systolic", "收缩压", "blood pressure", "血压"),
    "Diastolic blood pressure": ("diastolic", "舒张压", "blood pressure", "血压"),
    "HbA1c": ("hba1c", "glycated", "糖化血红蛋白", "糖化"),
    "Steps": ("steps", "步数", "走路"),
    "LDL cholesterol": ("ldl", "低密度脂蛋白", "低密度"),
    "Fasting glucose": ("fasting glucose", "空腹血糖", "血糖"),
}


def select_rows(keywords: list[str]) -> list[dict]:
    if not keywords:
        return list(ROWS)
    chosen: set[str] = set()
    for row in ROWS:
        haystack = " ".join((row["indicator"],) + ALIASES.get(row["indicator"], ())).casefold()
        if any(str(k).casefold() in haystack for k in keywords):
            chosen.add(row["indicator"])
    return [row for row in ROWS if row["indicator"] in chosen]


def monthly_mean(indicator: str, month: str) -> float:
    values = [float(r["value"]) for r in ROWS if r["indicator"] == indicator and r["date"].startswith(month)]
    return round(sum(values) / len(values), 4)


def mean(indicator: str, start: str = "", end: str = "") -> float:
    values = [float(r["value"]) for r in ROWS
              if r["indicator"] == indicator and (not start or r["date"] >= start)
              and (not end or r["date"] <= end)]
    return round(sum(values) / len(values), 4)


def _case(case_id: str, question: str, keywords: list[str], expect: list[dict]) -> dict:
    return {"id": case_id, "question": question, "keywords": keywords, "expect": expect}


def cases() -> list[dict]:
    build_rows()
    jan_weight = monthly_mean("Weight", "2026-01")
    aug_weight = monthly_mean("Weight", "2026-08")
    ldl_first = float(next(r["value"] for r in ROWS if r["indicator"] == "LDL cholesterol"
                           and r["date"].startswith("2026-01")))
    ldl_last = float(next(r["value"] for r in ROWS if r["indicator"] == "LDL cholesterol"
                          and r["date"].startswith("2026-06")))
    glucose = mean("Fasting glucose", "2026-01-01", "2026-06-30")
    steps = [float(r["value"]) for r in ROWS if r["indicator"] == "Steps" and r["date"].startswith("2026-08")]
    hba1c = [float(r["value"]) for r in ROWS if r["indicator"] == "HbA1c"]
    return [
        _case("rhr_monthly",
              "我 2026 年 3 月到 8 月的静息心率月均值分别是多少？请按月列出。",
              ["静息心率"],
              [{"label": month, "value": monthly_mean("Resting heart rate", month), "tol": 0.06}
               for month in ("2026-03", "2026-04", "2026-05", "2026-06", "2026-07", "2026-08")]),
        _case("sleep_july",
              "我 2026 年 7 月平均每晚睡多少分钟？",
              ["睡眠"],
              [{"label": "July mean sleep", "value": mean("Sleep duration", "2026-07-01", "2026-07-31"),
                "tol": 2.0}]),
        _case("weight_change",
              "我 2026 年 1 月和 8 月的平均体重分别是多少？这期间变化了多少公斤？",
              ["体重"],
              [{"label": "January mean", "value": jan_weight, "tol": 0.1},
               {"label": "August mean", "value": aug_weight, "tol": 0.1},
               {"label": "change kg", "value": round(aug_weight - jan_weight, 4), "tol": 0.15}]),
        _case("bp_q2",
              "我 2026 年 4 到 6 月的平均收缩压和舒张压分别是多少？",
              ["血压"],
              [{"label": "mean systolic",
                "value": mean("Systolic blood pressure", "2026-04-01", "2026-06-30"), "tol": 0.6},
               {"label": "mean diastolic",
                "value": mean("Diastolic blood pressure", "2026-04-01", "2026-06-30"), "tol": 0.6}]),
        _case("hba1c_delta",
              "我最近三次糖化血红蛋白分别是多少？从第一次到最后一次变化了多少？",
              ["糖化血红蛋白"],
              [{"label": "first HbA1c", "value": hba1c[0], "tol": 0.05},
               {"label": "second HbA1c", "value": hba1c[1], "tol": 0.05},
               {"label": "third HbA1c", "value": hba1c[2], "tol": 0.05},
               {"label": "delta", "value": round(hba1c[-1] - hba1c[0], 4), "tol": 0.05}]),
        _case("steps_august",
              "我 2026 年 8 月一共走了多少步？日均多少步？",
              ["步数"],
              [{"label": "total steps", "value": sum(steps), "tol": 1},
               {"label": "daily mean", "value": round(sum(steps) / len(steps), 4), "tol": 2}]),
        _case("ldl_pct",
              "我 2026 年 1 月到 6 月的 LDL 胆固醇从第一次到最后一次变化了百分之几？",
              ["LDL"],
              [{"label": "first LDL", "value": ldl_first, "tol": 0.03},
               {"label": "last LDL", "value": ldl_last, "tol": 0.03},
               {"label": "percent change", "value": round((ldl_last - ldl_first) / ldl_first * 100, 4),
                "tol": 1.0}]),
        _case("glucose_convert",
              "我 2026 年上半年空腹血糖的平均值是多少 mg/dL？换算成 mmol/L 是多少？",
              ["空腹血糖"],
              [{"label": "mean mg/dL", "value": glucose, "tol": 0.6},
               {"label": "mean mmol/L", "value": round(glucose / 18.018, 4), "tol": 0.06}]),
    ]

class SyntheticReadings(RecordTool):
    """The controlled fixture; the production envelope adapter renders its result."""

    TOOL_NAME = "query_health_indicators"

    async def query_health_indicators(self, user_info: dict, keywords: list[str]):
        return await self.envelope(user_info, keywords=keywords)

    def columns(self, args):
        return ("rid", "indicator", "date", "value", "unit")

    async def _run(self, caller_id, args):
        rows = [dict(row, rid=f"r{ROWS.index(row) + 1}") for row in select_rows(args.get("keywords", []))]
        return tools.Envelope(tools.STATUS_OK, data=rows,
                              meta=tools.Meta(tz="UTC", view="raw", row_count=len(rows)))


def query_tool():
    service = SyntheticReadings()
    wrapper = _envelope_wrapper(service.query_health_indicators, {"user_id": "synthetic"}, COMPUTATION_NOTE)
    return StructuredTool.from_function(
        coroutine=wrapper, name="query_health_indicators",
        description="Read synthetic recorded readings. Pass indicator names or synonyms in keywords.",
        args_schema={"type": "object", "properties": {
            "keywords": {"type": "array", "items": {"type": "string"}}}, "required": ["keywords"]},
        response_format="content_and_artifact",
    )


def run_javascript(code, inputs, timeout):
    sandbox = LocalPythonSandbox(image="node:22-slim", timeout=timeout)
    try:
        sandbox.start()
        prelude = """(() => {
const inputs = JSON.parse(require('fs').readFileSync('/workspace/data.json', 'utf8'));
globalThis.data = inputs.data;
globalThis.results = inputs.results;
})();
"""
        uploaded = sandbox.upload_files([
            ("/workspace/data.json", json.dumps(inputs, ensure_ascii=False).encode()),
            ("/workspace/main.cjs", (prelude + code).encode()),
        ])
        if any(result.error for result in uploaded):
            raise RuntimeError("synthetic JavaScript inputs could not be transferred")
        return sandbox.execute("node /workspace/main.cjs")
    finally:
        sandbox.close()
        sandbox._client.close()


@tool(response_format="content_and_artifact")
async def javascript_exec(code: str, runtime: ToolRuntime, timeout_s: int = 15) -> tuple[str, dict]:
    """Calculate in local JavaScript on original queried rows, using console.log(...) for output."""
    results = source_results(runtime.state.get("messages", []), computation_tool="javascript_exec")
    inputs = {"data": results[-1]["data"], "results": results}
    response = await asyncio.to_thread(run_javascript, code, inputs, max(1, min(timeout_s, 30)))
    text = response.output.strip() or "(no output; use console.log(...))"
    if response.exit_code != 0:
        text = f"error (exit {response.exit_code}): {text}"
    if response.truncated:
        text += "\n[Output truncated]"
    return text, {"status": tools.STATUS_OK if response.exit_code == 0 else tools.STATUS_ERROR,
                  "exit_code": response.exit_code,
                  "source_tool_call_ids": [r["tool_call_id"] for r in results]}


javascript_exec.description = python_exec.description.replace("Python", "JavaScript").replace(
    "print(...)", "console.log(...)").replace("standard library", "standard built-ins").split("For example:")[0]


def json_objects(text):
    decoder = json.JSONDecoder()
    found = []
    for match in re.finditer(r"\{", text):
        try:
            value, _ = decoder.raw_decode(text[match.start():])
        except json.JSONDecodeError:
            try:
                value = ast.literal_eval(text[match.start():].splitlines()[0])
            except (ValueError, SyntaxError):
                continue
        if isinstance(value, dict):
            found.append(value)
    return found


def matches(expected, text):
    for result in json_objects(text):
        values = {key: (value.get("value", value.get("mean", value.get("monthly_mean_bpm")))
                       if isinstance(value, dict) else value)
                  for key, value in result.items()}
        if all(isinstance(values.get(item["label"]), (int, float))
               and not isinstance(values[item["label"]], bool)
               and math.isfinite(values[item["label"]])
               and abs(values[item["label"]] - item["value"]) <= item["tol"] for item in expected):
            return True
    return False


def references_inputs(code, code_name):
    if code_name == "python_exec":
        try:
            return any((isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
                        and node.id in {"data", "results"})
                       or (isinstance(node, ast.Constant) and node.value == "/workspace/data.json")
                       for node in ast.walk(ast.parse(code)))
        except SyntaxError:
            return False
    return bool(re.search(r"\b(?:data|results)\b", code))


def score(case, messages, code_name):
    answer = next((m.text for m in reversed(messages) if isinstance(m, AIMessage) and not m.tool_calls), "")
    query_ids = set()
    code_calls = code_errors = 0
    calculated = False
    code_by_id = {call["id"]: call.get("args", {}).get("code", "")
                  for message in messages if isinstance(message, AIMessage) for call in message.tool_calls}
    for message in messages:
        if not isinstance(message, ToolMessage):
            continue
        if message.name == "query_health_indicators" and isinstance(message.artifact, tools.Envelope):
            if message.artifact.status != tools.STATUS_ERROR:
                query_ids.add(message.tool_call_id)
        if message.name != code_name:
            continue
        code_calls += 1
        artifact = message.artifact if isinstance(message.artifact, dict) else {}
        successful = artifact.get("exit_code") == 0 and artifact.get("status") != tools.STATUS_ERROR
        code_errors += not successful
        sources = artifact.get("source_tool_call_ids", [])
        if (successful and sources and set(sources) <= query_ids
                and references_inputs(code_by_id.get(message.tool_call_id, ""), code_name)
                and matches(case["expect"], str(message.content))):
            calculated = True
    accurate = matches(case["expect"], answer)
    return {"passed": accurate and calculated, "answer_accurate": accurate, "calculated": calculated,
            "query_calls": len(query_ids), "code_calls": code_calls, "code_errors": code_errors, "answer": answer}


async def run_case(client, case, arm, *, max_calls=8, timeout=240):
    code = (python_exec.model_copy(update={"description": python_exec.description.split("For example:")[0]})
            if arm == "python" else javascript_exec)
    graph = harness.assemble(
        model=client, tools=[query_tool(), code], backend=StateBackend(), system_prompt=SYSTEM,
        middleware=harness.standard_middleware(
            model_call_limit=max_calls, retry_limit=1,
            tool_call_limits={"query_health_indicators": 5, code.name: 5},
        ),
        model_call_limit=max_calls,
        excluded_native_tools={"delete", "write_file", "edit_file", "execute", "ls", "read_file", "glob", "grep"},
    )
    labels = [item["label"] for item in case["expect"]]
    prompt = case["question"] + "\nPrint and then answer a JSON object using exactly these result labels: " + json.dumps(labels)
    started = time.monotonic()
    try:
        result = await asyncio.wait_for(graph.ainvoke({"messages": [HumanMessage(prompt)]}), timeout)
        messages = result["messages"]
        outcome = score(case, messages, code.name)
        calls = []
        for message in messages:
            if isinstance(message, AIMessage):
                calls.extend(message.tool_calls)
            elif isinstance(message, ToolMessage) and message.name == code.name:
                calls.append({"result_for": message.tool_call_id, "content": str(message.content),
                              "artifact": message.artifact if isinstance(message.artifact, dict) else None})
        return {"case": case["id"], "arm": arm, "score": outcome, "calls": calls,
                "seconds": round(time.monotonic() - started, 2)}
    except Exception as exc:
        return {"case": case["id"], "arm": arm, "score": {
            "passed": False, "answer_accurate": False, "calculated": False,
            "query_calls": 0, "code_calls": 0, "code_errors": 0, "answer": ""},
            "exception_type": type(exc).__name__, "http_status": getattr(exc, "status_code", None),
            "seconds": round(time.monotonic() - started, 2)}


def summary(results):
    grouped = defaultdict(list)
    for result in results:
        grouped[(result["entry"], result["arm"])].append(result)
    return [
        {"entry": entry, "arm": arm, "cases": len(rows),
         "passed": sum(r["score"]["passed"] for r in rows),
         "answer_accurate": sum(r["score"]["answer_accurate"] for r in rows),
         "calculated": sum(r["score"]["calculated"] for r in rows),
         "code_calls": sum(r["score"]["code_calls"] for r in rows),
         "code_errors": sum(r["score"]["code_errors"] for r in rows),
         "endpoint_failures": sum("exception_type" in r for r in rows),
         "mean_seconds": round(sum(r["seconds"] for r in rows) / len(rows), 2)}
        for (entry, arm), rows in sorted(grouped.items())
    ]


async def run(args):
    Config.load_dotenv(".env")
    Config(["config.yaml"], read_only=["config.yaml"])
    entries = chat_entries()
    clients = {}
    selected = [c for c in cases() if not args.cases or c["id"] in args.cases.split(",")]
    for alias in args.entries.split(","):
        alias = alias.strip()
        if alias not in entries:
            raise ValueError(f"unknown MODELS entry: {alias}")
        entry = {**entries[alias], "timeout": 120, "max_retries": 0}
        if alias == "local":
            entry["base_url"] = args.local_base
        clients[alias] = build_chat_model(entry, alias=alias, thinking="medium")
    semaphore = asyncio.Semaphore(args.concurrency)
    results = []

    async def one(alias, case, arm, repetition):
        async with semaphore:
            result = await run_case(clients[alias], case, arm, max_calls=args.max_calls, timeout=args.timeout)
            result.update(entry=alias, model=entries[alias]["model"], repetition=repetition)
            results.append(result)
            mark = "PASS" if result["score"]["passed"] else "FAIL"
            print(f"{alias:14s} {arm:6s} {case['id']:18s} run={repetition} {mark} "
                  f"code={result['score']['code_calls']} errors={result['score']['code_errors']} "
                  f"{result['seconds']}s", flush=True)
            if args.out:
                args.out.write_text(json.dumps({"summary": summary(results), "results": results},
                                                ensure_ascii=False, indent=2), encoding="utf-8")

    await asyncio.gather(*(one(alias, case, arm, repetition)
                           for repetition in range(1, args.runs + 1)
                           for case in selected for alias in clients for arm in args.arms.split(",")))
    print(json.dumps(summary(results), ensure_ascii=False, indent=2))


async def replay_case(result, case):
    """Check generated code against doubled source values without another model call."""
    lookups = {}
    outputs = {call["result_for"]: call for call in result.get("calls", []) if "result_for" in call}
    service = SyntheticReadings()
    attempts = []
    for call in result.get("calls", []):
        if call.get("name") == "query_health_indicators":
            lookups[call["id"]] = await service.envelope({"user_id": "synthetic"}, **call.get("args", {}))
            continue
        if call.get("name") not in {"python_exec", "javascript_exec"}:
            continue
        output = outputs.get(call["id"], {})
        artifact = output.get("artifact") or {}
        ids = artifact.get("source_tool_call_ids", [])
        if artifact.get("exit_code") != 0 or not ids or any(source not in lookups for source in ids):
            continue
        if not matches(case["expect"], output.get("content", "")):
            continue
        snapshots = [{"tool": "query_health_indicators", "tool_call_id": source, **asdict(lookups[source])}
                     for source in ids]
        for snapshot in snapshots:
            for row in snapshot["data"] or []:
                if isinstance(row.get("value"), (int, float)) and not isinstance(row["value"], bool):
                    row["value"] *= 2
        inputs = {"data": snapshots[-1]["data"], "results": snapshots}
        expected = [{**item, "value": item["value"] if item["label"] == "percent change" else item["value"] * 2,
                     "tol": item["tol"] * 2} for item in case["expect"]]
        try:
            timeout = max(1, min(int(call.get("args", {}).get("timeout_s", 15)), 30))
            if result["arm"] == "python":
                response = await asyncio.to_thread(run_python, call["args"]["code"], inputs, timeout)
            else:
                response = await asyncio.to_thread(run_javascript, call["args"]["code"], inputs, timeout)
            verified = response.exit_code == 0 and matches(expected, response.output)
            attempts.append({"tool_call_id": call["id"], "verified": verified,
                             "exit_code": response.exit_code, "output": response.output})
            if verified:
                break
        except Exception as exc:
            attempts.append({"tool_call_id": call["id"], "verified": False,
                             "exception_type": type(exc).__name__})
    verified = any(attempt["verified"] for attempt in attempts)
    result["score"]["before_replay_passed"] = result["score"]["passed"]
    result["score"]["calculated"] = verified
    result["score"]["passed"] = result["score"]["answer_accurate"] and verified
    result["replay"] = {"value_multiplier": 2, "verified": verified, "attempts": attempts}
    return result


def run_python(code, inputs, timeout):
    return LocalPythonSandbox(timeout=timeout).run(code, inputs)


async def replay_report(path, out, concurrency=4):
    report = json.loads(path.read_text(encoding="utf-8"))
    by_case = {case["id"]: case for case in cases()}
    semaphore = asyncio.Semaphore(concurrency)

    async def one(result):
        async with semaphore:
            checked = await replay_case(result, by_case[result["case"]])
            mark = "PASS" if checked["score"]["passed"] else "FAIL"
            print(f"replay {checked['entry']:14s} {checked['arm']:6s} {checked['case']:18s} "
                  f"run={checked['repetition']} {mark}", flush=True)
            return checked

    results = await asyncio.gather(*(one(result) for result in report["results"]))
    out.write_text(json.dumps({"summary": summary(results), "results": results},
                              ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary(results), ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entries", default="local,deepseek,claude-sonnet,gpt")
    parser.add_argument("--local-base", default="http://127.0.0.1:8088/v1")
    parser.add_argument("--arms", default="python,js", choices=["python", "js", "python,js", "js,python"])
    parser.add_argument("--cases", default="")
    parser.add_argument("--runs", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--max-calls", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--replay-report", type=Path, help="replay stored generated code on doubled synthetic values")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    if args.replay_report:
        asyncio.run(replay_report(args.replay_report, args.out, args.concurrency))
    else:
        asyncio.run(run(args))


if __name__ == "__main__":
    main()
