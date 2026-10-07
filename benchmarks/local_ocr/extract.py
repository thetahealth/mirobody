"""End to end: what the product would store from one OCR model's pages.

    MIROBODY_SRC=<checkout> PYTHONPATH=<mirobody-gen> python benchmarks/local_ocr/extract.py \
        --model glm-ocr-q8 --corpus <mirobody-gen build with clean/>

Reads the OCR outputs `run.py` stored (results/<model>/pages/), builds each
page's document text exactly as `run.py` scores it, and hands it to
`IndicatorExtractor.extract_indicators_from_text(..., save_to_db=False)` from
MIROBODY_SRC: the table rules, then the text model for whatever they left,
the merge and the de-duplication, as an upload runs them. The text model is
MiniCPM5-2B (the default local size) on its own `llama-server`, started here
the way `docker/local-models.ini` runs it (32k context, two slots on one KV
pool, 1 GiB prompt cache) plus a fixed seed, so two OCR models' pages are
read by the same sampler. The stored readings are aligned to the page's
printed truth as `run.py` aligns the rule rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import requests

import run
from run import (
    HERE,
    RESULTS,
    align,
    breakdowns,
    log,
    page_text,
    prepare,
    range_ok,
    resources_low,
    split_unmatched,
    unit_ok,
    value_ok,
    write_json,
)

#: The line mirobody-gen prints on every page (`--no-banner` drops it). MiniCPM5-2B
#: reads "NOT A REAL PATIENT RECORD" literally: given what the table rules left
#: of an English check-up page (p002_2023-08-08_e01a p5, MinerU's text) it
#: answered `non_health_related` with no rows at server seeds 7, 8 and 9, and
#: with the line removed `medical_report` with all 26. `--strip-banner` hands
#: the product the page without it, as a real report would be.
BANNER = re.compile(r"SYNTHETIC SAMPLE.{0,12}GENERATED DATA.{0,6}NOT A REAL PATIENT RECORD", re.I)
TEXT_MODEL_REPO = "openbmb/MiniCPM5-2B-GGUF"
TEXT_MODEL_FILE = "MiniCPM5-2B-Q4_K_M.gguf"
OCR_PORT_PLACEHOLDER = "http://127.0.0.1:8187/v1"


def text_model_path(arg: str | None) -> Path:
    if arg:
        return Path(arg)
    hub = Path.home() / ".cache/huggingface/hub" / ("models--" + TEXT_MODEL_REPO.replace("/", "--"))
    found = sorted(hub.glob(f"snapshots/*/{TEXT_MODEL_FILE}"))
    if not found:
        raise SystemExit(f"{TEXT_MODEL_FILE} not in the Hugging Face cache; pass --text-model-gguf")
    return found[0]


def start_text_server(gguf: Path, port: int, log_path: Path, seed: int) -> subprocess.Popen:
    args = ["llama-server", "-m", str(gguf), "--host", "127.0.0.1", "--port", str(port), "-c", "32768",
            "-np", "2", "-kvu", "--cache-ram", "1024", "-ngl", "99", "--jinja", "--seed", str(seed)]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(log_path, "a")
    handle.write(f"\n# {' '.join(args)}\n")
    handle.flush()
    proc = subprocess.Popen(args, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    t0 = time.monotonic()
    while time.monotonic() - t0 < 600:
        if proc.poll() is not None:
            raise SystemExit(f"text model server exited; see {log_path}")
        try:
            if requests.get(f"http://127.0.0.1:{port}/health", timeout=5).status_code == 200:
                return proc
        except requests.RequestException:
            pass
        time.sleep(1)
    raise SystemExit("text model server did not become healthy")


def stop(proc: subprocess.Popen | None) -> None:
    if proc and proc.poll() is None:
        os.killpg(proc.pid, 15)
        try:
            proc.wait(30)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, 9)


async def configure(text_base: str) -> None:
    """Mirobody's config from MIROBODY_SRC with the local entries pointed at
    this run's servers. The OCR address only has to be set: the extractor asks
    whether an OCR route exists (its tables are then read by rule), never
    calls it."""
    os.environ.update({
        "LOCAL_BASE_URL": text_base, "LOCAL_MODEL": "minicpm5-2b",
        "LOCAL_OCR_BASE_URL": OCR_PORT_PLACEHOLDER, "LOCAL_OCR_MODEL": "glm-ocr",
        "UTILS_TEXT_MODEL": "local-utils", "UTILS_OCR_MODEL": "local-ocr",
    })
    cwd = os.getcwd()
    os.chdir(run.ROOT)
    try:
        from mirobody.utils.config.config import Config

        await Config.init()
    finally:
        os.chdir(cwd)
    from mirobody.utils.config.llm import resolve_route

    text, ocr = resolve_route("text"), resolve_route("ocr")
    if text is None or text.alias != "local-utils" or ocr is None:
        raise SystemExit(f"routes not local: text={getattr(text, 'alias', None)} ocr={getattr(ocr, 'alias', None)}")


def stored_value(indicator: dict) -> str:
    """The value text `save_indicators_to_db` would write for a row, from the
    MIROBODY_SRC being measured: from 490a0e1 on, a flag printed after the
    number is moved out (`indicator_store.value_and_flag`); before, the
    model's value was written as it came."""
    try:
        from mirobody.collect.files.services.indicator_store import value_and_flag
    except ImportError:
        return str(indicator.get("value") or "")
    return value_and_flag(indicator)[0]


def score(page: dict, indicators: list[dict]) -> dict:
    truth = page["rows"]
    preds = [{"item_name": str(i.get("original_indicator") or ""), "item_value": stored_value(i),
              "item_unit": str(i.get("unit") or ""), "item_range": str(i.get("reference_range") or "")}
             for i in indicators]
    pairs = align(truth, preds)
    checks = {"item_value": value_ok, "item_unit": unit_ok, "item_range": range_ok}
    ok = dict.fromkeys(checks, 0)
    errors = []
    for i, j, _ in pairs:
        good = {f: check(preds[j], truth[i]) for f, check in checks.items()}
        for f, g in good.items():
            ok[f] += g
        if not all(good.values()):
            errors.append({"truth": {k: truth[i][k] for k in ("item_name", "item_value", "item_unit", "item_range")},
                           "stored": preds[j], "wrong": [f[5:] for f, g in good.items() if not g]})
    repeats, elsewhere, false = split_unmatched(preds, pairs, page)
    loose = sum(value_ok(preds[j], truth[i], loose=True) for i, j, _ in pairs)
    return {"truth_rows": len(truth), "stored": len(preds), "matched": len(pairs), "value_ok": ok["item_value"],
            "value_ok_flag_forgiven": loose,
            "unit_ok": ok["item_unit"], "range_ok": ok["item_range"], "wrong_value": len(pairs) - ok["item_value"],
            "false": len(false), "elsewhere": len(elsewhere), "repeats": len(repeats), "errors": errors,
            "false_rows": false, "elsewhere_rows": elsewhere, "repeat_rows": repeats}


SUMS = ("truth_rows", "stored", "matched", "value_ok", "value_ok_flag_forgiven", "unit_ok", "range_ok", "wrong_value",
        "false", "elsewhere", "repeats", "seconds")


def aggregate(scores: list[dict]) -> dict:
    c = {k: sum(s.get(k, 0) or 0 for s in scores) for k in SUMS}

    def rate(a, b):
        return round(c[a] / c[b], 3) if c[b] else None

    return {"pages": len(scores), **{k: round(v, 1) if k == "seconds" else v for k, v in c.items()},
            "row_recall": rate("matched", "truth_rows"), "correct_recall": rate("value_ok", "truth_rows"),
            "correct_recall_flag_forgiven": rate("value_ok_flag_forgiven", "truth_rows"),
            "value_exact": rate("value_ok", "matched"), "unit_exact": rate("unit_ok", "matched"),
            "range_exact": rate("range_ok", "matched"),
            "model_pages": sum(1 for s in scores if s.get("llm")),
            "empty_pages": sum(1 for s in scores if s["truth_rows"] and not s["stored"]),
            "seconds_per_page": round(c["seconds"] / len(scores), 1) if scores else None,
            "failed_pages": sum(1 for s in scores if s.get("error"))}


async def main_async(args) -> None:
    specs = json.loads((HERE / "models.json").read_text(encoding="utf-8"))
    spec = specs[args.model]
    sample = json.loads(Path(args.sample).read_text(encoding="utf-8"))
    corpus = Path(args.corpus)
    records = {json.loads(line)["doc_id"]: json.loads(line)
               for line in (corpus / "files.jsonl").read_text(encoding="utf-8").splitlines()}
    entries = [e for e in sample["pages"] if not args.pages or e["id"] in args.pages]
    # Seed 7 is the run every model gets; another seed is a repeat, to see how
    # much of a difference between two OCR models the text model's sampling
    # (temperature 0.1 in `_llm_extract_one`) could make on its own.
    tag = (f"extract-{run.git_head(run.ROOT)[:7]}" + ("" if args.seed == 7 else f"-seed{args.seed}")
           + ("-nobanner" if args.strip_banner else ""))
    base = RESULTS / args.model / args.set if args.set else RESULTS / args.model
    out_dir = base / tag
    proc = None
    if not args.rescore:
        text_base = args.text_base or f"http://127.0.0.1:{args.port}/v1"
        await configure(text_base)
        from mirobody.collect.files.services.indicator_extractor import IndicatorExtractor
        from mirobody.collect.files.services.table_indicators import left_for_model, table_indicators, without_rows

        try:
            for n, entry in enumerate(entries, 1):
                path = out_dir / f"{entry['id']}.json"
                raw = base / "pages" / f"{entry['id']}.json"
                if (path.exists() and not args.force) or not raw.exists():
                    continue
                if (why := resources_low(args)):
                    raise SystemExit(f"stopped before {entry['id']}: {why}; rerun to resume")
                page = prepare(corpus, entry, records[entry["doc_id"]])
                text, _, _ = page_text(page, json.loads(raw.read_text(encoding="utf-8"))["outputs"], spec)
                if args.strip_banner:
                    text = BANNER.sub("", text)
                rows, _, unread = table_indicators(text)
                llm = not (rows and not unread and not left_for_model(without_rows(text, rows) if rows else text))
                if llm and proc is None and not args.text_base:
                    proc = start_text_server(text_model_path(args.text_model_gguf), args.port,
                                             RESULTS / "minicpm5-2b.server.log", args.seed)
                t0 = time.monotonic()
                error = None
                try:
                    indicators, result, _ = await IndicatorExtractor.extract_indicators_from_text(
                        text, user_id=0, file_name=entry["id"], save_to_db=False)
                except Exception as exc:  # the product would report this upload as failed
                    indicators, result, error = [], None, f"{type(exc).__name__}: {str(exc)[:200]}"
                seconds = round(time.monotonic() - t0, 2)
                write_json(path, {"id": entry["id"], "llm": llm, "seconds": seconds, "error": error,
                                  "indicators": indicators,
                                  "date_time": ((result or {}).get("content_info") or {}).get("date_time")
                                  if isinstance(result, dict) else None,
                                  "content_type": result.get("content_type") if isinstance(result, dict) else None})
                log(f"{n}/{len(entries)} {entry['id']} {'llm' if llm else 'rules'} {seconds}s "
                    f"{len(indicators)} rows{' ERR ' + error if error else ''}")
        finally:
            stop(proc)

    scores, pages = {}, []
    for entry in entries:
        path = out_dir / f"{entry['id']}.json"
        if not path.exists():
            continue
        page = prepare(corpus, entry, records[entry["doc_id"]])
        pages.append(page)
        stored = json.loads(path.read_text(encoding="utf-8"))
        scores[entry["id"]] = {"tier": entry["tier"], "llm": stored["llm"], "seconds": stored["seconds"],
                               "error": stored["error"], **score(page, stored["indicators"])}
    summary = {"model": args.model, "text_model": f"{TEXT_MODEL_REPO}:{TEXT_MODEL_FILE}",
               "mirobody_src": str(run.ROOT), "mirobody_commit": run.git_head(run.ROOT),
               **breakdowns(pages, scores, aggregate)}
    summary["seed"] = args.seed
    write_json(base / f"{tag}_scores.json", scores)
    write_json(base / f"{tag}_summary.json", summary)
    o = summary["overall"]
    log(f"{args.model} end to end: recall {o['row_recall']} correct {o['correct_recall']} value {o['value_exact']} "
        f"false {o['false']} wrong {o['wrong_value']}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--model", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--sample", default=str(HERE / "sample.json"))
    ap.add_argument("--set", default="", help="results under results/<model>/<set>/, as run.py --set")
    ap.add_argument("--port", type=int, default=8188)
    ap.add_argument("--text-base", help="an already running text model server's /v1 address")
    ap.add_argument("--text-model-gguf", help="MiniCPM5-2B GGUF path (default: the Hugging Face cache)")
    ap.add_argument("--pages", nargs="*")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--rescore", action="store_true")
    ap.add_argument("--seed", type=int, default=7, help="the text model server's sampling seed")
    ap.add_argument("--strip-banner", action="store_true", help="drop the generator's SYNTHETIC banner from the text")
    ap.add_argument("--min-free-gb", type=float, default=0)
    ap.add_argument("--min-free-mem", type=float, default=0)
    asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
