"""Ask a stronger model whether an answer actually answers the question.

The structural checks in `score.py` are deliberately literal: a number must appear, bind to
a label, cite a real rid. That keeps them reproducible, and it also rejects correct answers
phrased differently. This is the second reader for those cases.

It is a *review*, not a replacement, and it is built to be auditable:

* the case list defaults to the manifest frozen in `qa.json` (``case_manifest``), so a gold
  edited after the run cannot silently change what was judged;
* the evidence packet carries every tool result of the turn and of the prior turns
  (``prior_blocks``), untruncated up to a generous cap, because a figure printed in an
  earlier turn is still supported;
* the facts keep their kind, tolerance, unit and duration, so "47.6 hours" for a duration
  gold is judged on meaning rather than string equality;
* the judge is told to separate a value the record shows, a value derived from record
  values (a ratio, a difference, a mean, a conversion), and a value from outside;
* a reply is accepted only if it is the documented shape; anything else is an
  *evaluator failure*, never a silent pass, and the summary reports the full denominator.

    export JUDGE_BASE_URL=https://api.deepseek.com/v1
    export JUDGE_API_KEY=...
    python benchmarks/local_models/judge.py results/full --model deepseek-chat
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

#: Bumped whenever the prompt or the packet changes, so cached verdicts from an older
#: rubric are never reused under the new one.
PROMPT_VERSION = "2026-10-25.1"

#: A packet above this is cut with a note the judge can see; it is far beyond any answer
#: this suite produces and exists so one runaway tool result cannot exceed a model window.
MAX_EVIDENCE_CHARS = 200_000

PROMPT = """You are reviewing one answer from a personal health-record assistant.

Judge the answer against the record the assistant had, not against your own medical
knowledge. Reply with ONE JSON object and nothing else.

Question:
{question}

Expected facts. Every one must be stated (numbers allow the tolerance shown; a duration may
be given in any unit that matches the same length of time):
{facts}

The record the assistant could use, from this turn and from earlier turns of the same
conversation:
{evidence}

The assistant's answer:
{answer}

How to judge a number:
- "recorded"   the record shows this value (possibly in another unit or after conversion);
- "derived"    the answer computed it from record values - a ratio, a difference, a mean, a
               total, a percentage change, a unit conversion - and the inputs are in the
               record. A derived value is correct when the arithmetic is right, even though
               that exact number is not printed anywhere;
- "outside"    neither the record nor a computation over it produces it. This is the one
               that counts as unsupported.

Reply shape:
{{"correct": true|false,
  "missing": ["expected facts the answer does not state"],
  "unsupported": ["numbers that are neither recorded nor derived"],
  "note": "one short sentence when correct is false, naming the fault"}}

A differently worded but complete answer is correct. An answer that mixes several plans or
subjects contradicts itself is not correct even when the numbers it quotes are real."""


def _key() -> str:
    for name in ("JUDGE_API_KEY", "DEEPSEEK_API_KEY", "OPENAI_API_KEY"):
        value = os.environ.get(name)
        if value:
            return value
    raise SystemExit("set JUDGE_API_KEY (or DEEPSEEK_API_KEY / OPENAI_API_KEY)")


def _base_url() -> str:
    return (os.environ.get("JUDGE_BASE_URL") or "https://api.deepseek.com/v1").rstrip("/")


# ----------------------------------------------------------------------------- evidence

def facts_text(case: dict) -> str:
    """Every expected fact with the detail the judge needs to compare numbers."""
    lines = []
    for fact in case.get("facts") or []:
        label = fact.get("label") or "fact"
        kind = fact.get("kind") or ""
        if kind == "number" and fact.get("value") is not None:
            unit = fact.get("unit") or ""
            tol = fact.get("tol")
            extra = f" unit={unit}" if unit else ""
            extra += f" tol=±{tol}" if isinstance(tol, (int, float)) else ""
            lines.append(f"- {label}: {fact['value']} ({kind}{extra})")
        elif kind == "date" and fact.get("value"):
            lines.append(f"- {label}: {fact['value']} (date)")
        elif "any" in fact:
            lines.append(f"- {label}: one of {fact['any']}")
        else:
            lines.append(f"- {label}: {kind or 'stated'}")
    return "\n".join(lines) or "- (none recorded)"


def _tool_blocks(run: dict) -> list[dict]:
    blocks = list(run.get("prior_blocks") or []) + list(run.get("blocks") or [])
    return [b for b in blocks if b.get("type") == "tool_result"]


def evidence_text(run: dict) -> str:
    """Every tool result of this turn and of the earlier ones, in order."""
    chunks = []
    for index, block in enumerate(_tool_blocks(run), start=1):
        status = block.get("status") or "ok"
        name = block.get("name") or block.get("tool") or "tool"
        content = str(block.get("content") or "")
        chunks.append(f"[{index}] {name} ({status})\n{content}")
    text = "\n\n".join(chunks)
    if len(text) > MAX_EVIDENCE_CHARS:
        text = text[:MAX_EVIDENCE_CHARS] + "\n\n[evidence truncated by the harness]"
    return text or "(no tool result)"


def answer_text(run: dict) -> str:
    return "".join(str(b.get("text") or "") for b in run.get("blocks") or [] if b.get("type") == "text")


# ----------------------------------------------------------------------------- verdicts

VERDICT_TYPES = {"correct": bool, "missing": list, "unsupported": list}


def validate(verdict: Any) -> dict[str, Any]:
    """The documented shape or an exception: a malformed reply is not a verdict."""
    if not isinstance(verdict, dict):
        raise ValueError("verdict is not an object")
    for field, expected in VERDICT_TYPES.items():
        if field not in verdict:
            raise ValueError(f"verdict lacks {field}")
        if not isinstance(verdict[field], expected):
            raise ValueError(f"verdict.{field} is not {expected.__name__}")
    note = verdict.get("note", "")
    if not isinstance(note, str):
        raise ValueError("verdict.note is not a string")
    return {"correct": verdict["correct"], "missing": list(verdict["missing"]),
            "unsupported": list(verdict["unsupported"]), "note": note}


def cache_key(case: dict, run: dict, answer: str, model: str) -> str:
    """Everything a verdict depends on: the rubric, the judge, the gold and the evidence."""
    payload = {
        "rubric": PROMPT_VERSION,
        "model": model,
        "endpoint": _base_url(),
        "case": {"id": case.get("id"), "question": case.get("question"), "facts": case.get("facts")},
        "evidence": evidence_text(run),
        "answer": answer,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def ask(prompt: str, model: str, timeout: int = 180) -> dict[str, Any]:
    body = json.dumps({"model": model, "temperature": 0, "max_tokens": 500,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    request = urllib.request.Request(
        f"{_base_url()}/chat/completions", data=body,
        headers={"Authorization": f"Bearer {_key()}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as handle:
        payload = json.loads(handle.read())
    content = ((payload.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    start, end = content.find("{"), content.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("judge did not answer with JSON")
    return validate(json.loads(content[start:end + 1]))


# ----------------------------------------------------------------------------- driver

def saved_manifest(run_data: dict) -> list[dict]:
    """The case list frozen with the run, so later edits to `cases.jsonl` cannot leak in."""
    manifest = run_data.get("case_manifest") or []
    if not manifest:
        raise SystemExit("qa.json carries no case_manifest; re-run the benchmark or pass --cases-file")
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path, help="a results directory holding qa.json")
    ap.add_argument("--model", default="deepseek-chat")
    ap.add_argument("--cases-file", type=Path, default=None,
                    help="override the frozen manifest (recorded as a new evaluation version)")
    ap.add_argument("--limit", type=int, default=0, help="judge at most N unanswered cases (0 = all)")
    ap.add_argument("--force", action="store_true", help="ignore the cache")
    args = ap.parse_args()

    run_data = json.loads((args.results / "qa.json").read_text())
    manifest = saved_manifest(run_data)
    cases = {c["id"]: c for c in manifest}
    if args.cases_file:
        cases = {c["id"]: c for c in (json.loads(line) for line in args.cases_file.open() if line.strip())}
        print("note: judging against an explicit --cases-file, not the frozen manifest", file=sys.stderr)

    selected = run_data.get("selected_ids") or [r.get("id") for r in run_data.get("runs") or []]
    cache_path = args.results / "judge.json"
    cache = {} if args.force or not cache_path.exists() else json.loads(cache_path.read_text())

    counts = {"selected": len(selected), "attempted": 0, "answered": 0, "judged": 0,
              "unknown_case": 0, "evaluator_failure": 0, "correct": 0}
    for run in run_data.get("runs") or []:
        if run.get("id") not in selected:
            continue
        counts["attempted"] += 1
        answer = answer_text(run).strip()
        if not answer:
            continue
        counts["answered"] += 1
        case = cases.get(run["id"])
        if not case:
            counts["unknown_case"] += 1
            continue
        if args.limit and counts["judged"] + counts["evaluator_failure"] >= args.limit:
            break

        key = cache_key(case, run, answer, args.model)
        cached = cache.get(run["id"])
        if cached and cached.get("key") == key and not args.force:
            counts["judged"] += 1
            counts["correct"] += 1 if cached.get("correct") else 0
            continue

        prompt = PROMPT.format(question=case.get("question", ""), facts=facts_text(case),
                               evidence=evidence_text(run), answer=answer)
        try:
            verdict = ask(prompt, args.model)
        except (urllib.error.URLError, ValueError, TimeoutError) as error:
            counts["evaluator_failure"] += 1
            print(f"  {run['id']}: evaluator failure ({type(error).__name__}: {error})", file=sys.stderr)
            continue
        verdict.update({"key": key, "model": args.model, "endpoint": _base_url(),
                        "rubric": PROMPT_VERSION})
        cache[run["id"]] = verdict
        counts["judged"] += 1
        counts["correct"] += 1 if verdict["correct"] else 0
        cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=1))

    print(json.dumps(counts, ensure_ascii=False))
    print(f"judged {counts['judged']}/{counts['answered']} answered with {args.model}: "
          f"{counts['correct']} correct, {counts['judged'] - counts['correct']} not correct, "
          f"{counts['evaluator_failure']} evaluator failures")
    for case_id, verdict in sorted(cache.items()):
        if verdict.get("correct") is False:
            print(f"  {case_id}: {verdict.get('note') or ''} missing={verdict.get('missing')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
