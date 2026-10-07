"""Each model alone, through the llama.cpp router only (no app, no database):
memory on a fresh process and llama-server's own prompt and generation
speed on one fixed request shape.

    python benchmarks/local_models/speed.py                  # every size's answering model, then GLM-OCR
    python benchmarks/local_models/speed.py minicpm5-1b minicpm5-2b

The request is a tool result of 31 days of readings and a three-part
question, sent three times with different numbers so the prompt cache
(`--cache-ram`) cannot reuse a prefix; at most 400 tokens are generated. The
speeds are llama-server's `timings`; whether the reply is right is the
questions' business (run.py), not this script's. Writes results/speed.json.
Stops, keeping what it has, when common.guard says the machine is short of
disk or memory.
"""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (
    OCR_MODEL, RESULTS, ROUTER, SIZES, ResourceStop, guard, llama_processes, llama_version, now_iso, router_status,
    write_json,
)

REPEATS = 3
MAX_TOKENS = 400


def unload_all() -> None:
    for model, s in router_status().items():
        if s in ("loaded", "loading"):
            requests.post(ROUTER + "/models/unload", json={"model": model}, timeout=60)
    t0 = time.monotonic()
    while any(s in ("loaded", "loading", "unloading") for s in router_status().values()):
        if time.monotonic() - t0 > 120:
            break
        time.sleep(2)
    time.sleep(3)


def load(model: str) -> float:
    t0 = time.monotonic()
    requests.post(ROUTER + "/models/load", json={"model": model}, timeout=60)
    while (s := router_status().get(model)) != "loaded":
        if s == "failed" or time.monotonic() - t0 > 1800:
            raise RuntimeError(f"{model}: {s}")
        time.sleep(2)
    return round(time.monotonic() - t0, 1)


def child(model: str) -> dict | None:
    procs = [p for p in llama_processes() if p["alias"] == model]
    return {k: procs[0][k] for k in ("rss_mb", "footprint_mb")} if procs else None


def messages(rep: int) -> list[dict]:
    rng = random.Random(100 + rep)
    rows = [{"date": f"2026-07-{d:02d}", "resting_hr": rng.randint(55, 72), "steps": rng.randint(2000, 14000),
             "sleep_min": rng.randint(330, 500)} for d in range(1, 32)]
    tool = json.dumps({"indicator": "daily summary", "unit": {"resting_hr": "bpm", "sleep_min": "min"}, "rows": rows})
    return [{"role": "system", "content": "You are a health-data assistant. Answer only from the tool result."},
            {"role": "user", "content": f"Request {rep}. Tool result:\n{tool}\n\nWhat was my average resting heart "
                                        "rate in July 2026, which day had the most steps, and how long did I sleep "
                                        "on average? Answer in three short sentences."}]


def ask(model: str, rep: int) -> dict:
    t0 = time.monotonic()
    r = requests.post(ROUTER + "/v1/chat/completions", timeout=900, json={
        "model": model, "messages": messages(rep), "max_tokens": MAX_TOKENS, "temperature": 0, "seed": 7})
    body = r.json()
    t = body.get("timings") or {}
    return {"wall_s": round(time.monotonic() - t0, 2), "prompt_tokens": t.get("prompt_n"),
            "prompt_tok_s": round(t.get("prompt_per_second") or 0, 1), "generated_tokens": t.get("predicted_n"),
            "gen_tok_s": round(t.get("predicted_per_second") or 0, 1)}


def main() -> None:
    models = sys.argv[1:] or [SIZES[s] for s in ("tiny", "small")] + [OCR_MODEL]
    out = {"started": now_iso(), "llama_cpp": llama_version(), "router": ROUTER, "repeats": REPEATS,
           "max_tokens": MAX_TOKENS, "models": {}}
    path = RESULTS / "speed.json"
    try:
        for model in models:
            entry: dict = {"resources_before": guard(f"before {model}")}
            unload_all()
            entry["load_s"] = load(model)
            time.sleep(5)
            entry["memory_loaded"] = child(model)
            if model != OCR_MODEL:                     # the reader is measured by the extraction part
                entry["requests"] = [ask(model, rep) for rep in range(REPEATS)]
                entry["memory_after"] = child(model)
                entry["gen_tok_s"] = sorted(r["gen_tok_s"] for r in entry["requests"])[REPEATS // 2]
                entry["prompt_tok_s"] = sorted(r["prompt_tok_s"] for r in entry["requests"])[REPEATS // 2]
            print(f"{model}: {json.dumps({k: v for k, v in entry.items() if k != 'requests'})}", flush=True)
            out["models"][model] = entry
            write_json(path, out)
    except ResourceStop as e:
        out["stopped"] = str(e)
        print(f"STOP: {e}", flush=True)
    unload_all()
    out["finished"] = now_iso()
    write_json(path, out)


if __name__ == "__main__":
    main()
