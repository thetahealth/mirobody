"""Run the evaluation for one model size.

    python benchmarks/local_models/run.py --size small --corpus <mirobody-gen build>

1. Switches the stack to the size (`POST /api/setup`, as the first-run page
   does), waits until the router reports the answering model and GLM-OCR
   `loaded`, and gives the worker time to read the saved choice.
2. Records the memory the two models' llama-server processes hold.
3. **Questions** (`qa`): every case of cases.jsonl, in order, each in a fresh
   chat session against the Q&A accounts load.py filled, scored by score.py.
4. **Extraction** (`extraction`): the plan's documents into a fresh account,
   one at a time, each scored against its printed rows.
5. **Journal** (`journal`): the plan's sentences through
   `POST /api/v1/journal/sentence` into a fresh account, scored against the
   entries each states.

Writes results/<size>/ (meta.json, qa.json, extraction.json, journal.json,
summary.md) and records the environment in results/run.json. `--parts` runs a
subset, `--cases` some questions, `--out` writes elsewhere so a rerun can be
compared with the stored one (`report.py --diff`).
"""

from __future__ import annotations

import argparse
import json
import re
import platform
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import (
    APP, HERE, OCR_MODEL, RESULTS, ROOT, ROUTER, ROUTER_FROM_APP, SIZES, Account, Corpus, ResourceStop, bench_email,
    git_dirty, git_head, gguf_in_cache, guard, llama_version, memory_snapshot, model_files, now_iso, qa_email,
    read_json, read_jsonl, resources, router_status, setup_token, sha256, sign_in, swapouts, wait_loaded,
    wait_tasks_drained, write_json,
)
from load import LOAD_STATE, settings_for, stored_readings, upload_document
from score import acceptable_dates, rescore, score_case, score_document, score_sentence

QUESTION_TIMEOUT = 600
#: After a timeout the turn keeps running on the server; the next question
#: waits for it to end so it is not measured against a busy model.
LATE_WAIT = 900
SETTLE = 35
MEMORY_EVERY = 20


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


#: Why a part stopped early (common.guard), if one did. The part keeps what
#: it finished, and no later part starts.
STOPPED: list[str] = []


def check(where: str) -> bool:
    """False, with the reason in STOPPED, when the machine is short of disk
    or memory (common.guard)."""
    try:
        guard(where)
        return True
    except ResourceStop as e:
        STOPPED.append(str(e))
        log(f"STOP: {e}")
        return False


# --- the size ------------------------------------------------------------------------


def switch(size: str) -> dict:
    agent = SIZES[size]
    status = router_status()
    if agent not in status:
        raise SystemExit(f"the router at {ROUTER} does not serve {agent}; start it with docker/local-models.ini")
    t0 = time.monotonic()
    # Unload everything first. With --models-max 2 the router would otherwise
    # evict whichever model was used least recently, which can be the document
    # reader; and a child that has been answering holds its prompt cache
    # (llama-server --cache-ram, 8 GiB by default), so memory is measured on
    # fresh processes.
    for model, s in status.items():
        if s in ("loaded", "loading"):
            requests.post(ROUTER + "/models/unload", json={"model": model}, timeout=60)
            log(f"unloaded {model}")
    t_unload = time.monotonic()
    while any(v in ("loaded", "loading", "unloading") for v in router_status().values()):
        if time.monotonic() - t_unload > 120:
            break
        time.sleep(2)
    control = sign_in(bench_email("control", "switch"), lang="en")
    r = requests.post(APP + "/api/setup", headers={**control.headers(), "X-Setup-Token": setup_token()},
                      json={"mode": "local", "base_url": ROUTER_FROM_APP, "model": agent, "ocr_model": OCR_MODEL},
                      timeout=300).json()
    if r.get("code") != 0:
        raise SystemExit(f"setup refused {agent}: {r.get('msg')}")
    loaded_s = wait_loaded([agent, OCR_MODEL], log=log)
    log(f"{agent} and {OCR_MODEL} loaded in {loaded_s:.0f} s; waiting {SETTLE} s for the worker")
    time.sleep(SETTLE)
    state = requests.get(APP + "/api/setup", headers={"X-Setup-Token": setup_token()}, timeout=60).json()["data"]
    if state.get("chat_model") != agent:
        raise SystemExit(f"the app answers with {state.get('chat_model')}, not {agent}")
    return {"agent": agent, "ocr": OCR_MODEL, "switch_s": round(time.monotonic() - t0, 1),
            "load_s": round(loaded_s, 1), "local_models": state.get("local", {}).get("models")}


def ensure_loaded(size: str, *, ocr: bool = True) -> None:
    """The router may have been restarted under a run; ask for the size's
    models again and wait for them. Without `ocr`, GLM-OCR is unloaded: the
    questions read stored text, and the extraction documents' OCR text is
    already stored by `warm_ocr`, so neither calls it."""
    want = [SIZES[size]] + ([OCR_MODEL] if ocr else [])
    status = router_status()
    if not ocr and status.get(OCR_MODEL) in ("loaded", "loading"):
        requests.post(ROUTER + "/models/unload", json={"model": OCR_MODEL}, timeout=60)
        log(f"unloaded {OCR_MODEL} for this pass")
    for model in want:
        if status.get(model) != "loaded":
            requests.post(ROUTER + "/models/load", json={"model": model}, timeout=60)
    wait_loaded(want, log=log)


def describe_models(size: str, *, hashes: bool) -> list[dict]:
    out = []
    for model in (SIZES[size], OCR_MODEL):
        info = model_files(model)
        info.pop("args", None)
        files = []
        if info.get("model_path"):
            paths = [Path(info["model_path"])] + ([Path(info["mmproj_path"])] if info.get("mmproj_path") else [])
        elif info.get("hf_repo"):
            paths = gguf_in_cache(info["hf_repo"], info.get("hf_file"))
        else:
            paths = []
        for p in paths:
            real = p.resolve()
            entry = {"file": p.name, "bytes": real.stat().st_size if real.exists() else None}
            if real.parent.name == "blobs" and len(real.name) == 64:
                entry["sha256"] = real.name            # the Hub names an LFS blob by its SHA-256
            elif hashes and real.exists():
                entry["sha256"] = sha256(real)
            files.append(entry)
        info["files"] = files
        out.append(info)
    return out


class MemorySampler(threading.Thread):
    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.samples: list[dict] = []
        self._stop = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                snap = memory_snapshot()
                self.samples.append({"at": snap["at"], "rss_mb": snap["children_rss_mb"],
                                     "footprint_mb": snap["children_footprint_mb"]})
            except Exception:
                pass
            self._stop.wait(MEMORY_EVERY)

    def stop(self) -> dict:
        self._stop.set()
        rss = [s["rss_mb"] for s in self.samples]
        fp = [s["footprint_mb"] for s in self.samples if s["footprint_mb"] is not None]
        return {"samples": len(self.samples), "peak_rss_mb": max(rss, default=None),
                "peak_footprint_mb": max(fp, default=None)}


# --- questions -----------------------------------------------------------------------


def ask(account: Account, question: str, timeout: float) -> dict:
    """One question in a fresh session, streamed to the end or the timeout."""
    created = account.post("/api/session", {"query_user_id": account.user_id})
    session_id = (created.get("data") or {}).get("session_id") or created.get("session_id")
    blocks: list[dict] = []
    out: dict[str, Any] = {"session_id": session_id, "started": now_iso()}
    first_text = None
    finish = None
    try:
        for _ in range(12):
            t0 = time.monotonic()          # a wait for the rate limit's window is not the answer's time
            r = account.session.post(APP + "/api/chat", headers=account.headers(), stream=True, timeout=(30, 120),
                                     json={"question": question, "session_id": session_id,
                                           "query_user_id": account.user_id})
            if r.status_code != 429:
                break
            r.close()
            out["rate_limited"] = out.get("rate_limited", 0) + 1
            time.sleep(10)
        with r:
            for line in r.iter_lines(decode_unicode=True):
                if time.monotonic() - t0 > timeout:
                    out["timeout"] = True
                    break
                if not line or not line.startswith("data:"):
                    continue
                block = json.loads(line[5:].strip())
                kind = block.get("type")
                if kind == "heartbeat":
                    continue
                if kind == "text" and first_text is None:
                    first_text = time.monotonic() - t0
                if kind == "end":
                    finish = block.get("finish_reason")
                    break
                if kind == "error":
                    out["error"] = block.get("message")
                _merge(blocks, block)
    except requests.RequestException as e:
        out["error"] = f"{type(e).__name__}: {e}"[:300]
    out.update({"seconds": round(time.monotonic() - t0, 1), "first_text_s": round(first_text, 1) if first_text else None,
                "finish_reason": finish, "blocks": blocks})
    if out.get("timeout") or (finish is None and not out.get("error")):
        out["late_s"] = _wait_answer(account, session_id)
    return out


def _merge(blocks: list[dict], block: dict) -> None:
    """Join streamed text and reasoning, as the server's transcript does; keep
    only the reasoning's length (the answer and the tools are what is scored)."""
    kind = block.get("type")
    if kind in ("text", "reasoning") and blocks and blocks[-1].get("type") == kind:
        field = "text" if kind == "text" else "reasoning"
        blocks[-1][field] = blocks[-1].get(field, "") + block.get(field, "")
        return
    blocks.append(dict(block))


def _wait_answer(account: Account, session_id: str) -> float | None:
    t0 = time.monotonic()
    while time.monotonic() - t0 < LATE_WAIT:
        time.sleep(15)
        try:
            history = account.get("/api/history", session_id=session_id)["data"]["history"]
        except Exception:
            continue
        if any(m.get("role") == "assistant" for m in history):
            return round(time.monotonic() - t0, 1)
    return None


#: How an upstream rate limit reaches the chat: OpenRouter answers 200 with an
#: error and no choices, which the agent meets as a TypeError, or as the SDK's
#: APIError, or a 429. The app logs only the error's type (no text leaves its
#: log), so an APIError is taken as one; on 2026-10-07 the host probe made at
#: the same minutes was answered "temporarily rate-limited upstream".
_RATE_LIMITED = re.compile(r"rate|429|TypeError|NoneType|temporarily|APIError", re.I)


def run_questions(size: str, cases: list[dict], timeout: float, plan: dict, *, ocr: bool = True,
                  save=None, retries: int = 0) -> list[dict]:
    """Every case in order; `save(runs)` after each, so a run cut short keeps
    the answers it has."""
    state = read_json(LOAD_STATE, {})
    accounts: dict[str, Account] = {}
    for person, cfg in plan["qa_people"].items():
        if qa_email(person) in state:
            accounts[person] = sign_in(qa_email(person), tz=cfg["tz"], lang=cfg["lang"])
    runs = []
    for i, case in enumerate(cases, 1):
        account = accounts.get(case["person"])
        if account is None:
            raise SystemExit(f"no Q&A account for {case['person']}: run load.py --qa first")
        if not check(f"before question {i} ({case['id']})"):
            break
        log(f"[{i}/{len(cases)}] {case['id']}")
        before = swapouts()
        result = ask(account, case["question"], timeout)
        if result.get("error") and not result.get("timeout"):
            log(f"  error: {result['error']}; checking the router and asking once more")
            if size in SIZES:
                ensure_loaded(size, ocr=ocr)
            first = result
            result = ask(account, case["question"], timeout)
            result["retried_after"] = {k: first.get(k) for k in ("error", "seconds", "finish_reason")}
        limited: list[dict] = []
        while (retries and len(limited) < retries and result.get("error") and not result.get("timeout")
               and _RATE_LIMITED.search(str(result["error"]))):
            wait = 60 * 2 ** len(limited)
            log(f"  rate-limited ({str(result['error'])[:60]}); asking again in {wait} s")
            limited.append({k: result.get(k) for k in ("error", "seconds", "finish_reason")})
            time.sleep(wait)
            result = ask(account, case["question"], timeout)
        if limited:
            result["rate_limited_attempts"] = limited
        after = swapouts()
        result.update({"id": case["id"], "person": case["person"], "question": case["question"],
                       "swapouts": after - before if before is not None and after is not None else None})
        for b in result["blocks"]:
            if b.get("type") == "reasoning":
                b["reasoning_chars"] = len(b.pop("reasoning", ""))
        result["score"] = score_case(case, result)
        s = result["score"]
        log(f"  {'PASS' if s['pass'] else 'fail'} in {result['seconds']:.0f} s; tools {s['tools_called']}; "
            f"facts {sum(f['ok'] for f in s['facts'])}/{len(s['facts'])}; unsupported {s['unsupported_numbers']}")
        runs.append(result)
        if save:
            save(runs)
    return runs


# --- extraction and journal ----------------------------------------------------------


def warm_ocr(corpus: Corpus, plan: dict) -> dict:
    """Upload each extraction document once, into an account of its own,
    before any size is measured on it.

    The app keeps the text it read off a file and reuses it for any later
    upload of the same bytes (`file_abstract_extractor._read_original_text_cache`,
    keyed by SHA-256), so only the first upload of a document runs GLM-OCR.
    Without this pass the first size measured would pay for the OCR and every
    later one would not. With it, every size reads the same OCR text, and the
    OCR's own time is recorded here once: it is the same model for every size.
    """
    state = read_json(LOAD_STATE, {})
    warm = state.setdefault("ocr-warm", {})
    account = None
    for d in plan["extraction_docs"]:
        if warm.get(d["file"], {}).get("status") == "done":
            continue
        if not check(f"before the first upload of {Path(d['file']).name}"):
            break
        account = account or sign_in(bench_email("ocr", "warm"), tz="Asia/Shanghai", lang="zh")
        log(f"first upload (OCR) of {Path(d['file']).name}")
        warm[d["file"]] = {**upload_document(account, corpus.path(d["file"]), log=log),
                           "agent_model": ",".join(m for m, s in router_status().items()
                                                   if s == "loaded" and m != OCR_MODEL and "/" not in m)}
        write_json(LOAD_STATE, state)
    return warm


#: The app's log lines that say a model call failed while a document was read.
_MODEL_ERRORS = ("Structured output API error", "returned an error, and a failed call is not retried")
#: The ones that cost readings: the extraction itself, or one of its pages, got no answer.
_LOST_READINGS = re.compile(r"produced 0 rows: indicator extraction failed|LLM returned empty response for text "
                            r"extraction|Text extraction failed|read \d+ pages, [1-9]\d* unanswered")


def model_errors(since: str) -> tuple[int, int]:
    """Failed model calls in the app's log since `since` (UTC, ISO), and how
    many of them cost readings. An upstream rate limit reaches the app as an
    answer with no choices. A failed title or date call costs no reading and
    is not a reason to read the document again."""
    from load import _app_log

    lines = _app_log(since).splitlines()
    return (sum(any(m in line for m in _MODEL_ERRORS) for line in lines),
            sum(bool(_LOST_READINGS.search(line)) for line in lines))


def _fresh_account(name: str) -> Account:
    account = sign_in(bench_email("ext", name), tz="Asia/Shanghai", lang="zh")
    settings_for(account, {"sex": "male", "birth_year": 1970})
    return account


def run_extraction(size: str, corpus: Corpus, plan: dict, stamp: str, retries: int = 0) -> dict:
    """Each document into a fresh account. With `retries`, a document whose
    reading met a failed model call is read again, after a growing wait, into
    an account of its own (the same bytes in the same account would meet the
    first upload's readings as duplicates); every attempt is recorded."""
    from datetime import UTC, datetime

    warm = warm_ocr(corpus, plan)
    if STOPPED:
        return {"documents": [], "stopped": STOPPED[-1]}
    account = _fresh_account(f"{size}-{stamp}")
    docs = []
    for d in plan["extraction_docs"]:
        if not check(f"before extracting {Path(d['file']).name}"):
            break
        log(f"extract {d['label']}: {Path(d['file']).name}")
        acct, attempts = account, []
        for attempt in range(retries + 1):
            since = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            up = upload_document(acct, corpus.path(d["file"]), log=log)
            errors, lost = model_errors(since) if retries else (0, 0)
            attempts.append({"account": acct.email, "file_key": up["file_key"], "seconds": up["seconds"],
                             "indicators_count": up.get("indicators_count"), "model_errors": errors,
                             "lost_readings_errors": lost})
            if not lost or attempt == retries:
                break
            wait = 60 * 2 ** attempt
            log(f"  {errors} failed model call(s), {lost} costing readings; reading it again in {wait} s")
            time.sleep(wait)
            acct = _fresh_account(f"{size}-{stamp}-{Path(d['file']).stem[-8:]}-r{attempt + 1}")
        docs.append({**d, **up, "account": acct.email, "attempts": attempts})
    by_key: dict[str, list[dict]] = {}
    for email in dict.fromkeys(d["account"] for d in docs):
        for r in stored_readings(sign_in(email, tz="Asia/Shanghai", lang="zh")):
            by_key.setdefault(r.get("file_key") or "", []).append(r)
    for d in docs:
        truth = corpus.files[d["file"]]
        d["score"] = score_document(truth, by_key.get(d["file_key"], []))
        d["report_date_ok"] = d.get("report_date", "")[:10] in acceptable_dates(truth)
        s = d["score"]
        log(f"  {Path(d['file']).name}: {s['found']}/{s['rows']} rows, unit {s['unit_ok']}, range {s['range_ok']}, "
            f"{s['extra']} extra, {d['seconds']:.0f} s")
    return {"account": account.email, "user_id": account.user_id, "documents": docs,
            "first_upload": {d["file"]: {k: warm.get(d["file"], {}).get(k) for k in ("seconds", "filed_s", "agent_model")}
                             for d in docs}}


def run_journal(size: str, plan: dict, stamp: str, retries: int = 0) -> dict:
    account = sign_in(bench_email("jnl", f"{size}-{stamp}"), tz="Asia/Shanghai", lang="zh")
    out = []
    for s in plan["journal_sentences"]:
        if not check("before a journal sentence"):
            break
        body = {"text": s["text"], "observed_at": f"{s['date']}T12:00:00+08:00", "tz": "Asia/Shanghai"}
        errors: list[str] = []
        for attempt in range(retries + 1):
            t0 = time.monotonic()
            try:
                r = account.post("/api/v1/journal/sentence", body, timeout=900)
                data, error = r.get("data") or {}, ""
            except Exception as e:
                data, error = {}, f"{type(e).__name__}: {e}"[:300]
            seconds = round(time.monotonic() - t0, 1)
            # 502 is "the sentence could not be read": the model call failed and
            # nothing was written, so asking again cannot write twice.
            if not error or "code 502" not in error or attempt == retries:
                break
            errors.append(error)
            wait = 30 * 2 ** attempt
            log(f"journal {s['text'][:30]}: {error[:60]}; again in {wait} s")
            time.sleep(wait)
        score = score_sentence(s, data.get("written") or [])
        log(f"journal {s['text'][:30]}: {score['found']}/{score['expected']} entries, {score['extra']} extra, "
            f"{seconds:.0f} s{' ' + error if error else ''}")
        out.append({"text": s["text"], "lang": s["lang"], "date": s["date"], "seconds": seconds, "error": error,
                    "response": data, "score": score, "failed_attempts": errors})
    return {"account": account.email, "user_id": account.user_id, "sentences": out}


def rescore_saved(size_dir: Path, corpus: Corpus, plan: dict) -> None:
    """Score a size's saved results again with the current score.py, asking
    no model: the transcripts, the extraction account's stored readings (read
    again over HTTP) and the journal's written entries."""
    if (size_dir / "qa.json").exists():
        rescore(size_dir, HERE / "cases.jsonl")
    ext = read_json(size_dir / "extraction.json")
    if ext and ext.get("account"):
        by_key: dict[str, list[dict]] = {}
        for email in dict.fromkeys(d.get("account") or ext["account"] for d in ext["documents"]):
            for r in stored_readings(sign_in(email, tz="Asia/Shanghai", lang="zh")):
                by_key.setdefault(r.get("file_key") or "", []).append(r)
        for d in ext["documents"]:
            truth = corpus.files[d["file"]]
            d["score"] = score_document(truth, by_key.get(d["file_key"], []))
            d["report_date_ok"] = d.get("report_date", "")[:10] in acceptable_dates(truth)
        write_json(size_dir / "extraction.json", ext)
        log(f"rescored {len(ext['documents'])} documents of {ext['account']}")
    jnl = read_json(size_dir / "journal.json")
    if jnl:
        planned = {s["text"]: s for s in plan["journal_sentences"]}
        for s in jnl["sentences"]:
            s["score"] = score_sentence(planned[s["text"]], (s.get("response") or {}).get("written") or [])
        write_json(size_dir / "journal.json", jnl)


# --- the summary ---------------------------------------------------------------------


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    k = (len(values) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return round(values[lo] + (values[hi] - values[lo]) * (k - lo), 1)


def summarize(size_dir: Path) -> dict:
    meta = read_json(size_dir / "meta.json", {})
    qa = read_json(size_dir / "qa.json", {"runs": []})
    ext = read_json(size_dir / "extraction.json", {"documents": []})
    jnl = read_json(size_dir / "journal.json", {"sentences": []})
    runs = qa["runs"]
    times = [r["seconds"] for r in runs]
    docs = ext["documents"]
    sents = jnl["sentences"]
    found = sum(d["score"]["found"] for d in docs)
    return {
        "size": meta.get("size"), "agent": meta.get("agent"),
        "qa_pass": sum(r["score"]["pass"] for r in runs), "qa_n": len(runs),
        "qa_checks": {k: sum(r["score"]["checks"][k] for r in runs) for k in
                      ("answered", "tool", "facts", "language", "chart")},
        "qa_unsupported_numbers": sum(len(r["score"]["unsupported_numbers"]) for r in runs),
        "qa_answers_with_unsupported": sum(bool(r["score"]["unsupported_numbers"]) for r in runs),
        "qa_timeouts": sum(r["score"]["timeout"] for r in runs),
        "qa_median_s": round(statistics.median(times), 1) if times else None, "qa_p90_s": pct(times, 0.9),
        "ext_rows": sum(d["score"]["rows"] for d in docs), "ext_found": found,
        "ext_unit_ok": sum(d["score"]["unit_ok"] for d in docs), "ext_range_ok": sum(d["score"]["range_ok"] for d in docs),
        "ext_extra": sum(d["score"]["extra"] for d in docs),
        "ext_dates_ok": sum(d.get("report_date_ok", False) for d in docs), "ext_docs": len(docs),
        "ext_median_s": round(statistics.median([d["seconds"] for d in docs]), 1) if docs else None,
        "ext_failed": sum(d["status"] != "done" for d in docs),
        "jnl_expected": sum(s["score"]["expected"] for s in sents), "jnl_found": sum(s["score"]["found"] for s in sents),
        "jnl_code_ok": sum(s["score"]["code_ok"] for s in sents),
        "jnl_coded_checked": sum(s["score"]["coded_checked"] for s in sents),
        "jnl_extra": sum(s["score"]["extra"] for s in sents), "jnl_exact": sum(s["score"]["exact"] for s in sents),
        "jnl_n": len(sents), "jnl_errors": sum(bool(s["error"]) for s in sents),
        "jnl_median_s": round(statistics.median([s["seconds"] for s in sents]), 1) if sents else None,
        "memory_idle_rss_mb": (meta.get("memory_idle") or {}).get("children_rss_mb"),
        "memory_idle_footprint_mb": (meta.get("memory_idle") or {}).get("children_footprint_mb"),
        "memory_peak_rss_mb": (meta.get("memory_run") or {}).get("peak_rss_mb"),
        "memory_peak_footprint_mb": (meta.get("memory_run") or {}).get("peak_footprint_mb"),
    }


def write_summary(size_dir: Path, cases: list[dict]) -> None:
    s = summarize(size_dir)
    qa = read_json(size_dir / "qa.json", {"runs": []})
    lines = [f"# {s['size']}: {s['agent']}", "",
             ("Generated by run.py from the JSON beside it. Automatic checks only; Claude Code's grades are in "
              "grades.json and ../summary.md."), "",
             "| Part | Result |", "| --- | --- |",
             f"| Questions passed (automatic) | {s['qa_pass']}/{s['qa_n']} |",
             (f"| answered / right tool / facts / language / chart | {s['qa_checks']['answered']} / "
              f"{s['qa_checks']['tool']} / {s['qa_checks']['facts']} / {s['qa_checks']['language']} / "
              f"{s['qa_checks']['chart']} |"),
             f"| Numbers in no tool result | {s['qa_unsupported_numbers']} in {s['qa_answers_with_unsupported']} answers |",
             f"| Seconds per answer, median / p90 | {s['qa_median_s']} / {s['qa_p90_s']} |",
             f"| Timeouts ({QUESTION_TIMEOUT} s) | {s['qa_timeouts']} |",
             f"| Extraction: printed rows found | {s['ext_found']}/{s['ext_rows']} in {s['ext_docs']} documents |",
             f"| … unit / range as printed | {s['ext_unit_ok']} / {s['ext_range_ok']} of {s['ext_found']} |",
             f"| … readings stored that are not printed rows | {s['ext_extra']} |",
             f"| … report date right | {s['ext_dates_ok']}/{s['ext_docs']} |",
             f"| … seconds per document, median | {s['ext_median_s']} |",
             f"| Journal: entries found | {s['jnl_found']}/{s['jnl_expected']} in {s['jnl_n']} sentences |",
             (f"| … coded right / extra entries / sentences exact | {s['jnl_code_ok']}/{s['jnl_coded_checked']} / "
              f"{s['jnl_extra']} / {s['jnl_exact']}/{s['jnl_n']} |"),
             f"| … seconds per sentence, median | {s['jnl_median_s']} |",
             (f"| llama-server memory, both models loaded: RSS / footprint (MB) | {s['memory_idle_rss_mb']} / "
              f"{s['memory_idle_footprint_mb']} (peak while answering {s['memory_peak_rss_mb']} / "
              f"{s['memory_peak_footprint_mb']}) |"), "",
             "| Case | Pass | Checks failed | Facts | Unsupported numbers | Seconds |",
             "| --- | --- | --- | --- | --- | --- |"]
    for r in qa["runs"]:
        sc = r["score"]
        failed = ", ".join(k for k, v in sc["checks"].items() if not v) or "—"
        facts = f"{sum(f['ok'] for f in sc['facts'])}/{len(sc['facts'])}"
        lines.append(f"| {r['id']} | {'yes' if sc['pass'] else 'no'} | {failed} | {facts} | "
                     f"{', '.join(str(x) for x in sc['unsupported_numbers']) or '—'} | {r['seconds']:.0f} |")
    (size_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# --- a cloud reference ---------------------------------------------------------------


def openrouter_usage() -> float | None:
    """What the OpenRouter key has spent so far, in USD (`GET /api/v1/key`), when
    OPENROUTER_API_KEY is in this process's environment. The key goes to
    openrouter.ai and nowhere else. A key other callers use at the same time
    makes the difference between two readings an upper bound."""
    key = __import__("os").environ.get("OPENROUTER_API_KEY", "")
    if not key:
        return None
    try:
        r = requests.get("https://openrouter.ai/api/v1/key", headers={"Authorization": f"Bearer {key}"}, timeout=30)
        return float((r.json().get("data") or {}).get("usage"))
    except (requests.RequestException, ValueError, TypeError):
        return None


def openrouter_usage_settled(wait: float = 30, limit: float = 300) -> float | None:
    """The key's spend once it stops moving: OpenRouter books a request's
    cost some seconds to a minute after the answer (measured 2026-10-06: a
    part's spend read at its end was a seventh of the spend a minute later)."""
    last = openrouter_usage()
    if last is None:
        return None
    t0 = time.monotonic()
    while time.monotonic() - t0 < limit:
        time.sleep(wait)
        now = openrouter_usage()
        if now is not None and abs(now - last) < 1e-9:
            return now
        last = now if now is not None else last
    return last


def overlay_entries(ref: str) -> dict:
    """The two entries a reference's eval-only configuration adds
    (refs/<ref>.llm.yaml, refs/overlays.py), when it has one."""
    path = HERE / "refs" / f"{ref}.llm.yaml"
    if not path.exists():
        return {}
    import yaml

    models = yaml.safe_load(path.read_text(encoding="utf-8"))["MODELS"]
    return {"file": str(path.relative_to(ROOT)), "ref-chat": models["ref-chat"], "ref-utils": models["ref-utils"]}


def probe_hosts(entries: dict) -> dict:
    """One small request per surface with exactly the entry's model and
    extra_body, and the `provider` OpenRouter says answered. The utility probe
    carries a temperature and a response_format, as every utility call does."""
    key = __import__("os").environ.get("OPENROUTER_API_KEY", "")
    out: dict[str, Any] = {}
    for surface in ("ref-chat", "ref-utils"):
        e = entries.get(surface)
        if not e or not key:
            continue
        body: dict[str, Any] = {"model": e["model"], "max_tokens": 200, **(e.get("extra_body") or {}),
                                "messages": [{"role": "user", "content": "Return the JSON object {\"ok\": true}."}]}
        if surface == "ref-utils":
            body["temperature"] = 0.1
            body["response_format"] = {"type": "json_object"} if e.get("response_format") == "json_object" else {
                "type": "json_schema", "json_schema": {"name": "probe", "strict": True, "schema": {
                    "type": "object", "additionalProperties": False, "required": ["ok"],
                    "properties": {"ok": {"type": "boolean"}}}}}
            if e.get("reasoning_effort"):
                body["reasoning_effort"] = e["reasoning_effort"]
        for attempt in range(6):
            try:
                d = requests.post("https://openrouter.ai/api/v1/chat/completions", json=body, timeout=180,
                                  headers={"Authorization": f"Bearer {key}"}).json()
            except (requests.RequestException, ValueError) as ex:
                d = {"error": {"message": type(ex).__name__}}
            if "error" not in d:
                out[surface] = {"provider": d.get("provider"), "model": d.get("model"), "attempts": attempt + 1}
                break
            out[surface] = {"error": str(d["error"].get("message"))[:200], "attempts": attempt + 1}
            time.sleep(15 * (attempt + 1))
    return out


def reference_state() -> dict:
    """The models the stack answers and extracts with, as its setup API says."""
    state = requests.get(APP + "/api/setup", headers={"X-Setup-Token": setup_token()}, timeout=60).json()["data"]
    return {"chat_model": state.get("chat_model"),
            "providers": [{k: p.get(k) for k in ("key", "set", "chat_model", "utils_model")}
                          for p in state.get("providers") or [] if p.get("set")],
            "local": {k: (state.get("local") or {}).get(k) for k in ("configured", "base_url", "models")}}


# --- main ----------------------------------------------------------------------------


#: The mirobody-gen commit the corpus is built at (README.md, "Reproduce").
#: A build does not record it, so a checkout that has moved on since is
#: reported beside it, not instead of it; the JSONL hashes tell the builds apart.
GEN_COMMIT = "248df0f2aead8051c81ef1f7a9fa431e627e8791"


def environment(corpus: Corpus | None) -> dict:
    gen = Path(__import__("os").environ.get("MIROBODY_GEN_DIR", str(ROOT.parent / "mirobody-gen")))
    people = len(corpus.people) if corpus else None
    return {
        "mirobody_commit": git_head(ROOT), "mirobody_dirty": git_dirty(ROOT),
        "mirobody_gen": {"seed": 7, "people": people, "commit": GEN_COMMIT,
                         "repo_head_at_run": git_head(gen) if gen.exists() else "",
                         "build": f"mirobody-gen build --seed 7 --people {people} --out <dir> --render",
                         "corpus": str(corpus.dir) if corpus else "",
                         "sha256": {p.name: sha256(p) for p in sorted(corpus.dir.glob("*.jsonl"))} if corpus else {}},
        "llama_cpp": llama_version(),
        "router": {"preset": "docker/local-models.ini", "models_max": 2},
        "machine": {"chip": _sysctl("machdep.cpu.brand_string"), "memory_gb": round(int(_sysctl("hw.memsize") or 0) / 2**30),
                    "os": f"{platform.system()} {platform.mac_ver()[0] or platform.release()}",
                    "docker_vm": _colima()},
        "timeouts": {"question_s": QUESTION_TIMEOUT, "late_wait_s": LATE_WAIT, "upload_s": 1800, "journal_s": 900,
                     "settle_s": SETTLE},
    }


def _sysctl(name: str) -> str:
    try:
        return subprocess.run(["sysctl", "-n", name], capture_output=True, text=True).stdout.strip()
    except OSError:
        return ""


def _colima() -> str:
    try:
        out = subprocess.run(["colima", "list", "--json"], capture_output=True, text=True, timeout=20).stdout
        vm = json.loads(out.splitlines()[0])
        return f"colima {vm.get('cpus')} CPU, {round(int(vm.get('memory', 0)) / 2**30)} GB"
    except Exception:
        return ""


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the local-model evaluation for one size.")
    who = ap.add_mutually_exclusive_group(required=True)
    who.add_argument("--size", choices=sorted(SIZES))
    who.add_argument("--ref", help="a reference the stack is already configured for (a cloud model, by env); "
                                   "results/ref-<REF>, no model switching and no llama-server")
    ap.add_argument("--corpus", required=True, help="the mirobody-gen build the cases and plan came from")
    ap.add_argument("--parts", default="qa,extraction,journal")
    ap.add_argument("--cases", default="", help="comma-separated case ids (default: all, in file order)")
    ap.add_argument("--out", default="", help="default results/<size>")
    ap.add_argument("--no-switch", action="store_true", help="the size is already set up and loaded")
    ap.add_argument("--no-ocr", action="store_true",
                    help="with --no-switch: run with GLM-OCR unloaded, to leave memory to the answering model")
    ap.add_argument("--timeout", type=float, default=QUESTION_TIMEOUT)
    ap.add_argument("--no-hash", action="store_true", help="skip the GGUF sha256 (slow on a large model)")
    ap.add_argument("--overlay", default="",
                    help="with --ref: the refs/<overlay>.llm.yaml the stack runs, when the results' name differs "
                         "(ref-gpt-6-luna-rules runs the gpt-6-luna overlay)")
    ap.add_argument("--name", default="",
                    help="with --size: the results' name, e.g. small-v2, so a rerun on new code keeps the old run")
    ap.add_argument("--merge", action="store_true",
                    help="with --cases: keep the saved answers to the other cases in qa.json")
    ap.add_argument("--retries", type=int, default=0,
                    help="read a document or sentence again, after a growing wait, when its model call failed "
                         "(an upstream rate limit); every attempt is recorded")
    ap.add_argument("--rescore", action="store_true",
                    help="score the saved results again with the current score.py; asks no model")
    args = ap.parse_args()

    corpus = Corpus(args.corpus)
    parts = set(args.parts.split(","))
    label = args.name or args.size or f"ref-{args.ref}"
    out = Path(args.out) if args.out else RESULTS / label
    out.mkdir(parents=True, exist_ok=True)
    cases = read_jsonl(HERE / "cases.jsonl")
    if args.cases:
        wanted = args.cases.split(",")
        cases = [c for c in cases if c["id"] in wanted]
    plan = json.loads((HERE / "plan.json").read_text(encoding="utf-8"))
    stamp = time.strftime("%Y%m%d%H%M")
    if args.rescore:
        rescore_saved(out, corpus, plan)
        write_summary(out, cases)
        run_path = (out.parent if args.out else RESULTS) / "run.json"
        run = read_json(run_path, {}) or {}
        group = "sizes" if args.size else "references"
        if label in run.get(group, {}):
            run[group][label]["summary"] = summarize(out)
            write_json(run_path, run)
        print((out / "summary.md").read_text(encoding="utf-8"))
        return

    meta = read_json(out / "meta.json", {}) or {}
    meta.pop("stopped", None)              # a previous pass's; this one says its own
    if not check("before switching"):
        raise SystemExit(STOPPED[-1])
    if args.ref:
        state = reference_state()
        overlay = overlay_entries(args.overlay or args.ref)
        if overlay:
            state["overlay"] = overlay
            state["host_probe"] = probe_hosts(overlay)
            log(f"host probe: {state['host_probe']}")
        meta.update({"size": label, "reference": args.ref, "agent": state["chat_model"], "models": state,
                     "started": meta.get("started") or now_iso(), "parts": sorted(set(meta.get("parts") or []) | parts)})
        log(f"reference {args.ref}: the stack answers with {state['chat_model']}")
    else:
        meta.update({"size": label, "agent": SIZES[args.size], "started": meta.get("started") or now_iso(),
                     "parts": sorted(set(meta.get("parts") or []) | parts)})
        if not args.no_switch:
            meta["switch"] = switch(args.size)
        else:
            ensure_loaded(args.size, ocr=not args.no_ocr)
        meta["models"] = describe_models(args.size, hashes=not args.no_hash)
    # A reference runs no llama-server of its own; any on the machine is someone else's.
    meta["memory_idle"] = None if args.ref else memory_snapshot()
    usage = {"start": openrouter_usage_settled() if args.ref else None}
    meta.setdefault("passes", []).append({
        "parts": sorted(parts), "started": now_iso(), "resources": resources(),
        "mirobody_commit": git_head(ROOT), "mirobody_dirty": git_dirty(ROOT),
        "loaded": [] if args.ref else sorted(m for m, s in router_status().items() if s == "loaded"),
        "memory": meta["memory_idle"]})
    if meta["memory_idle"]:
        log(f"memory, both loaded: RSS {meta['memory_idle']['children_rss_mb']} MB, "
            f"footprint {meta['memory_idle']['children_footprint_mb']} MB")
    write_json(out / "meta.json", meta)

    sampler = MemorySampler()
    sampler.start()
    if "qa" in parts:
        kept: list[dict] = []
        if args.merge:
            # Keep the saved answers to the cases this pass does not ask.
            asked = {c["id"] for c in cases}
            kept = [r for r in (read_json(out / "qa.json", {}) or {}).get("runs", []) if r["id"] not in asked]
        order = {c["id"]: i for i, c in enumerate(read_jsonl(HERE / "cases.jsonl"))}

        def save(runs: list[dict]) -> None:
            merged = sorted(kept + runs, key=lambda r: order.get(r["id"], len(order)))
            write_json(out / "qa.json", {"size": label, "agent": meta["agent"], "cases": "cases.jsonl",
                                         "runs": merged, **({"stopped": STOPPED[-1]} if STOPPED else {})})

        save(run_questions(label, cases, args.timeout, plan, ocr=not args.no_ocr, save=save, retries=args.retries))
        usage["qa"] = openrouter_usage_settled() if args.ref else None
        meta["memory_run"] = None if args.ref else sampler.stop()
        meta["passes"][-1]["memory_run"] = meta["memory_run"]
        meta["passes"][-1]["memory_after_qa"] = {**({} if args.ref else memory_snapshot()), **resources()}
    sampler.stop()
    if "extraction" in parts and not STOPPED:
        write_json(out / "extraction.json", run_extraction(label, corpus, plan, stamp, retries=args.retries))
        usage["extraction"] = openrouter_usage_settled() if args.ref else None
    if "journal" in parts and not STOPPED:
        write_json(out / "journal.json", run_journal(label, plan, stamp, retries=args.retries))
        usage["journal"] = openrouter_usage_settled() if args.ref else None
    meta["background_tasks_drained_s"] = round(wait_tasks_drained(log=log), 1)
    meta["finished"] = now_iso()
    meta["passes"][-1]["finished"] = meta["finished"]
    if usage["start"] is not None:
        # The host probe's few cents are booked before "start" settles.
        # Each part's cost is the key's spend at its end less at the one before.
        marks = [("start", usage["start"])] + [(k, v) for k, v in usage.items() if k != "start" and v is not None]
        meta["passes"][-1]["openrouter_usd"] = {
            "readings": usage, "parts": {k: round(v - marks[i][1], 6) for i, (k, v) in enumerate(marks[1:])},
            "total": round(marks[-1][1] - usage["start"], 6)}
    if STOPPED:
        meta["stopped"] = STOPPED[-1]
    write_json(out / "meta.json", meta)
    write_summary(out, cases)

    run_path = (out.parent if args.out else RESULTS) / "run.json"
    run = read_json(run_path, {}) or {}
    run.update(environment(corpus))
    run.setdefault("sizes" if args.size else "references", {})[label] = {"started": meta["started"], "finished": meta["finished"],
                                              "parts": meta["parts"], "models": meta["models"],
                                              "passes": [{k: p.get(k) for k in ("parts", "started", "finished",
                                                                                "mirobody_commit", "mirobody_dirty")}
                                                         for p in meta.get("passes") or []],
                                              "summary": summarize(out)}
    write_json(run_path, run)
    log(f"done: {out}")
    print((out / "summary.md").read_text(encoding="utf-8"))
    if STOPPED:
        raise SystemExit(f"stopped early: {STOPPED[-1]}")


if __name__ == "__main__":
    main()
