"""Measure one document-OCR model the way Mirobody uses it, on synthetic pages with row-level truth.

    PYTHONPATH=.:<mirobody-gen> python benchmarks/local_ocr/run.py --model glm-ocr-q8 \
        --corpus <mirobody-gen build with clean/> --models-dir <dir with the GGUFs>

For each page of `sample.json`:

1. The page as the product hands it to the OCR model: a PDF page rendered by
   `documents.extract._pdf_pages` (150 dpi), a photo through
   `extract.downscale_image`, then `render.fit_image` to 1536 px JPEG, the
   size `utils/llm/file_processors/media._read_and_optimize_image` sends.
2. One request per pass of the model's prompts (`models.json`), image first
   and the prompt after it, as `media._build_vision_message` builds it. A
   text-layer PDF page gets only the tables pass, after its text layer, as
   `extract.pdf_text` does; a scan or photo gets every pass, joined as
   `ocr.vision_ocr` joins them.
3. A model whose tables are not HTML or markdown has them converted
   (`convert.py`), then the page text goes through `table_indicators`,
   `without_rows` and `left_for_model`, the rule path of
   `indicator_extractor`, and the rows it stores are aligned to the page's
   printed truth with mirobody-gen's own aligner.

The model runs in its own `llama-server` (one slot, 16k context, prompt cache
off so the memory measured is the model's), started and stopped here.
Raw outputs land in results/<model>/pages/ before anything is scored, so
`--rescore` scores again without a model.
"""

from __future__ import annotations

import argparse
import base64
import collections
import datetime as dt
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any

import requests

HERE = Path(__file__).resolve().parent
#: The Mirobody source whose reader is measured: this checkout, or the one
#: MIROBODY_SRC names (the branch the product will ship from).
ROOT = Path(os.environ.get("MIROBODY_SRC") or HERE.parents[1]).resolve()
RESULTS = HERE / "results"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT))

from convert import convert
from mirobody.collect.files.services.table_indicators import (
    _TABLE_BLOCK,
    _html_tables,
    _markdown_tables,
    left_for_model,
    table_indicators,
    without_rows,
)
from mirobody import translate
from mirobody.documents import extract, render
from mirobody.units import normalize_unit

try:
    from mirobody.documents.ocr import clean_answer
except ImportError:  # a Mirobody from before e2044e9: the OCR answer was used as it came
    clean_answer = None

try:
    from mirobody_gen.harness.score import align, name_similarity
except ImportError:  # pragma: no cover - the README says to put mirobody-gen on PYTHONPATH
    sys.exit("mirobody-gen is not importable: add its checkout to PYTHONPATH (see README.md)")

#: Page labels a sample may carry besides its tier; each gets its own breakdown.
GROUPS = ("language", "kind", "capture")
#: The edge `media._read_and_optimize_image` fits every OCR image to.
SEND_EDGE = 1536
SEND_QUALITY = 85
CTX = 16384
#: The product sends no max_tokens; a model that loops would run to the end of
#: its context. Capped here so a loop costs minutes, not the run; a capped
#: answer is recorded with finish_reason "length".
MAX_TOKENS = 8192
#: The `local-ocr` entry's timeout.
TIMEOUT = 600


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def norm(s: str) -> str:
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", s or ""))


# --- the pages ------------------------------------------------------------------------


def clean_pages(corpus: Path, doc_id: str) -> list[str]:
    import pypdfium2 as pdfium

    doc = pdfium.PdfDocument((corpus / "clean" / f"{doc_id}.pdf").read_bytes())
    try:
        return [doc[i].get_textpage().get_text_range() or "" for i in range(len(doc))]
    finally:
        doc.close()


def page_truth(record: dict, pages: list[str], index: int) -> tuple[list[dict], list[str]]:
    """The printed rows and the narrative strings on page `index` (0-based).

    A row is on the page whose clean text layer holds its name and its value.
    A row found on two pages (a check-up's summary repeats an abnormal one)
    goes to the page its table is on: the page the table's unambiguous rows
    are on."""
    texts = [norm(p) for p in pages]
    rows = record["printed_rows"]
    candidates = [[k for k, t in enumerate(texts) if norm(r["item_name"]) in t and norm(r["item_value"]) in t]
                  for r in rows]
    table_pages: dict[int, set[int]] = collections.defaultdict(set)
    for r, c in zip(rows, candidates, strict=True):
        if len(c) == 1:
            table_pages[r["table"]].add(c[0])
    on_page = []
    for r, c in zip(rows, candidates, strict=True):
        if not c:
            continue
        home = sorted(set(c) & table_pages[r["table"]]) or c
        if home[0] == index and r["readable"]:
            on_page.append(r)
    strings = []
    for block in record.get("blocks") or []:
        for s in block.get("printed") or []:
            for line in str(s).splitlines():
                n = norm(line)
                if len(n) >= 4 and n in texts[index]:
                    strings.append(line.strip())
    return on_page, strings


def prepare(corpus: Path, entry: dict, record: dict) -> dict:
    """The image the product would send for this page, its text layer when it
    has one, and the page's truth."""
    data = (corpus / entry["file"]).read_bytes()
    if sha256_bytes(data) != entry["sha256"]:
        raise SystemExit(f"{entry['file']}: not the sampled file (sha256 differs); rebuild the corpus from the seed")
    index = entry["page"] - 1
    layer = None
    if record["format"] == "pdf":
        texts, to_ocr, layered = extract._pdf_pages(data, min_page_text=extract.MIN_PAGE_TEXT, dpi=extract.RENDER_DPI,
                                                     render_all=True)
        image = dict(to_ocr + layered)[index]
        if texts[index]:
            layer = texts[index]
    else:
        mime = "image/png" if entry["file"].endswith(".png") else "image/jpeg"
        image, _ = extract.downscale_image(data, mime)
    sent, stats = render.fit_image(image, max_edge=SEND_EDGE, quality=SEND_QUALITY)
    clean = clean_pages(corpus, record["doc_id"])
    rows, strings = page_truth(record, clean, index)
    return {
        "clean": clean[index],
        # Rows that look like readings and are not (a patient field, "异常项目数 0"): stored, they are false.
        "distractors": {fold(d["text"].split()[0]) for d in record.get("distractors") or [] if d.get("text")},
        "id": entry["id"], "doc_id": entry["doc_id"], "page": entry["page"], "tier": entry["tier"],
        "kind": record["kind"], "pdf": record["format"] == "pdf", "layer": layer, "image": sent,
        "image_size": list(stats.get("optimized_dimensions") or []), "rows": rows, "strings": strings,
        "series": record["kind"] == "home_log",
        # What a sample may group its pages by besides the tier (the handwriting
        # set: language, kind of document, capture scene).
        "group": {k: entry[k] for k in GROUPS if k in entry},
    }


# --- the server -----------------------------------------------------------------------


def model_paths(spec: dict, models_dir: Path) -> dict[str, Path]:
    """The GGUFs (and a template file) under `models_dir/<org>__<repo>/`, or in
    the Hugging Face cache."""
    out = {}
    for key in ("model_file", "mmproj_file", "template_file"):
        name = spec.get(key)
        if not name:
            continue
        local = models_dir / spec["repo"].replace("/", "__") / name
        if not local.exists():
            hub = Path.home() / ".cache/huggingface/hub" / ("models--" + spec["repo"].replace("/", "--"))
            found = sorted(hub.glob(f"snapshots/*/{name}"))
            if found:
                local = found[0]
        if not local.exists():
            raise SystemExit(f"{name} not found under {models_dir} or the Hugging Face cache; see README.md")
        out[key] = local
    return out


class Server:
    def __init__(self, spec: dict, paths: dict[str, Path], port: int, log_path: Path):
        self.spec, self.paths, self.port, self.log_path = spec, paths, port, log_path
        self.proc: subprocess.Popen | None = None
        self.base = f"http://127.0.0.1:{port}"

    def start(self) -> float:
        args = ["llama-server", "-m", str(self.paths["model_file"]), "--mmproj", str(self.paths["mmproj_file"]),
                "--host", "127.0.0.1", "--port", str(self.port), "-c", str(CTX), "-ngl", "99", "-np", "1",
                "--jinja", "--cache-ram", "0"]
        if "template_file" in self.paths:
            args += ["--chat-template-file", str(self.paths["template_file"])]
        args += self.spec.get("server_args", [])
        t0 = time.monotonic()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self.log_path, "a")
        handle.write(f"\n# {dt.datetime.now().isoformat()} {' '.join(args)}\n")
        handle.flush()
        self.proc = subprocess.Popen(args, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
        while time.monotonic() - t0 < 600:
            if self.proc.poll() is not None:
                raise SystemExit(f"llama-server exited ({self.proc.returncode}); see {self.log_path}")
            try:
                if requests.get(self.base + "/health", timeout=5).status_code == 200:
                    return time.monotonic() - t0
            except requests.RequestException:
                pass
            time.sleep(1)
        self.stop()
        raise SystemExit("llama-server did not become healthy in 600 s")

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            os.killpg(self.proc.pid, signal.SIGTERM)
            try:
                self.proc.wait(30)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait(10)
        self.proc = None

    def memory(self) -> dict[str, Any]:
        """`ps` RSS and macOS `footprint` (phys_footprint counts the Metal
        buffers RSS leaves out) of the server process, in MiB."""
        if not self.proc:
            return {}
        pid = self.proc.pid
        out: dict[str, Any] = {}
        ps = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        if ps:
            out["rss_mib"] = round(int(ps) / 1024, 1)
        if Path("/usr/bin/footprint").exists():
            fp = subprocess.run(["/usr/bin/footprint", "-f", "bytes", str(pid)], capture_output=True, text=True,
                                timeout=60).stdout
            for key in ("phys_footprint", "phys_footprint_peak"):
                m = re.search(rf"{key}:\s*([\d,]+)\s*B", fp)
                if m:
                    out[f"{key}_mib"] = round(int(m.group(1).replace(",", "")) / 1048576, 1)
        return out

    def chat(self, image: bytes, prompt: str) -> dict[str, Any]:
        content: list[dict] = [{"type": "image_url",
                                "image_url": {"url": "data:image/jpeg;base64," + base64.b64encode(image).decode()}}]
        if prompt:
            content.append({"type": "text", "text": prompt})
        payload = {"messages": [{"role": "user", "content": content}], "max_tokens": MAX_TOKENS,
                   **self.spec.get("sampling", {})}
        t0 = time.monotonic()
        try:
            r = requests.post(self.base + "/v1/chat/completions", json=payload, timeout=TIMEOUT)
            body = r.json()
        except (requests.RequestException, ValueError) as exc:
            return {"error": type(exc).__name__, "seconds": round(time.monotonic() - t0, 2), "raw": ""}
        seconds = round(time.monotonic() - t0, 2)
        if r.status_code != 200 or "choices" not in body:
            return {"error": f"HTTP {r.status_code}: {str(body)[:300]}", "seconds": seconds, "raw": ""}
        choice = body["choices"][0]
        return {"raw": choice["message"].get("content") or "", "finish_reason": choice.get("finish_reason"),
                "seconds": seconds, "usage": body.get("usage"), "timings": body.get("timings")}


# --- a shared machine ---------------------------------------------------------------------


def memory_free_percent() -> float | None:
    """macOS `memory_pressure`'s system-wide free percentage; None elsewhere."""
    try:
        out = subprocess.run(["memory_pressure"], capture_output=True, text=True, timeout=60).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r"free percentage:\s*(\d+)%", out)
    return float(m.group(1)) if m else None


def resources_low(args) -> str | None:
    """Why the run must stop now (disk or memory below the flags' floor), or None."""
    if args.min_free_gb:
        free = shutil.disk_usage(HERE).free / 2**30
        if free < args.min_free_gb:
            return f"{free:.1f} GiB free on disk, below {args.min_free_gb}"
    if args.min_free_mem:
        pct = memory_free_percent()
        if pct is not None and pct < args.min_free_mem:
            return f"{pct:.0f}% of memory free, below {args.min_free_mem}%"
    return None


# --- whether a stored field is the printed one ------------------------------------------
#
# Compared as the product stores it, not as text: `observations.prepare` keeps
# the value text but charts `translate.parse_value(value, unit)`, so "42.4 %"
# is 42.4 with its unit and right, and "6.49↑" parses as narrative, holds no
# number, and is wrong. A unit is right when it normalizes to the printed one
# (in its own cell or after the number). A range is compared as printed, less
# spacing, brackets and the dash or tilde between its ends: `parse_range`
# cannot be the judge, it reads "35.0--45.0" as -45 to 35.


def _alternatives(ref: dict, field: str) -> list[str]:
    alts = (ref.get("alternatives") or {}).get(field) or []
    return [ref[field], *(alts if isinstance(alts, list) else [alts])]


_GLUED_FLAG = re.compile(r"\s*(?:↑↑|↓↓|↑|↓|偏高|偏低|H|L)\s*$")


def value_ok(pred: dict, ref: dict, loose: bool = False) -> bool:
    """`loose`: a flag glued after the number ("5.73↑") forgiven, which the
    product does not forgive (it stores that value as narrative, no number)."""
    want = translate.parse_value(ref["item_value"], ref["item_unit"])
    printed = pred.get("item_value") or ""
    if loose:
        printed = _GLUED_FLAG.sub("", printed)
    got = translate.parse_value(printed, pred.get("item_unit") or "")
    if want.value_kind == "quantity":
        return (got.value_kind == "quantity" and got.value_num is not None
                and abs(got.value_num - want.value_num) < 1e-9 and got.comparator == want.comparator)
    return fold(pred.get("item_value") or "") == fold(ref["item_value"])


def unit_ok(pred: dict, ref: dict) -> bool:
    printed = pred.get("item_unit") or translate.parse_value(pred.get("item_value") or "", "").unit_tail or ""
    for want in _alternatives(ref, "item_unit"):
        a, b = normalize_unit(printed), normalize_unit(want)
        if (a is not None and a == b) or fold(printed) == fold(want):
            return True
    return False


def _range_text(s: str) -> str:
    s = fold(s)
    return s[1:-1] if s[:1] == "(" and s[-1:] == ")" else s


def printed_on_page(pred: dict, page: dict) -> bool:
    """Whether a stored row that matches no printed table row is still on the
    page: a line of the clean page holds its name and its value (a check-up's
    "DOB值 3.1‰" in a narrative block, an outpatient record's "HbA1c 6.1").
    Such a row is a reading the truth does not list, not a false one."""
    name = fold(pred.get("item_name") or "")
    if name in page["distractors"]:
        return False
    relaxed = fold(re.sub(r"[(（][^)）]*[)）]", "", pred.get("item_name") or ""))
    number = re.match(r"\s*[<>≤≥]?\s*([-+]?\d+(?:\.\d+)?)", pred.get("item_value") or "")
    value = fold(number.group(1) if number else (pred.get("item_value") or ""))
    if not value or not (name or relaxed):
        return False
    if not number:
        # A finding as printed, possibly over several lines: its label and its
        # text on the page, or a text long enough to be the page's own whatever
        # label the model put on it ("腹部超声（肝、胆、胰、脾、双肾）肝脏" for the
        # liver line of an ultrasound report).
        whole = fold(page["clean"])
        named = name in whole or (len(relaxed) >= 2 and relaxed in whole)
        return value in whole and (named or len(value) >= 4)
    for line in page["clean"].splitlines():
        line = fold(line)
        if value in line and ((name and name in line) or (len(relaxed) >= 2 and relaxed in line)):
            return True
    return False


def split_unmatched(preds: list[dict], pairs: list[tuple[int, int, float]], page: dict
                    ) -> tuple[list[dict], list[dict], list[dict]]:
    """(stored rows that repeat a printed row already stored, stored rows
    printed elsewhere on the page, stored rows not printed at all). A repeat
    holds the value of a printed row another stored row matched, under a name
    alike or containing the other (`血红蛋白` beside `血红蛋白（HGB）`)."""
    matched = {j for _, j, _ in pairs}
    taken = [page["rows"][i] for i, _, _ in pairs]

    def repeat(r: dict) -> bool:
        for t in taken:
            if not value_ok(r, t, loose=True):
                continue
            a = fold(re.sub(r"[(（][^)）]*[)）]", "", r.get("item_name") or ""))
            b = fold(re.sub(r"[(（][^)）]*[)）]", "", t["item_name"]))
            if (a and b and (a in b or b in a)) or name_similarity(r.get("item_name") or "", t["item_name"]) > 0:
                return True
        return False

    rest = [preds[j] for j in range(len(preds)) if j not in matched]
    repeats = [r for r in rest if repeat(r)]
    rest = [r for r in rest if not repeat(r)]
    elsewhere = [r for r in rest if printed_on_page(r, page)]
    return repeats, elsewhere, [r for r in rest if not printed_on_page(r, page)]


def range_ok(pred: dict, ref: dict) -> bool:
    return any(_range_text(pred.get("item_range") or "") == _range_text(want) for want in _alternatives(ref, "item_range"))


# --- one page through the product's rule path --------------------------------------------


_SPECIAL = re.compile(r"<\|[a-z_]{1,24}\|>")


def page_text(page: dict, outputs: dict[str, dict], spec: dict) -> tuple[str, str, str]:
    """(the document text the product would extract, the OCR's text alone, the
    tables pass alone), each pass converted to what the reader takes."""
    def clean(name: str, raw: str) -> str:
        # A server run with --special (the model's table tokens are special
        # tokens) also prints its end-of-turn token: `<|im_end|>`. The product
        # has no step for it; it is cut here for every commit alike.
        if spec.get("special_tokens"):
            raw = _SPECIAL.sub("", raw)
        # From e2044e9 the product cleans every OCR answer itself (OTSL to HTML,
        # LaTeX to text, loops cut): that is what is measured, not convert.py.
        if clean_answer is not None:
            return clean_answer(raw)
        return convert(spec.get("convert") if name == "tables" else None, raw)

    converted = {name: clean(name, out.get("raw") or "").strip() for name, out in outputs.items()}
    tables = converted.get("tables", "")
    ocr_text = "\n\n".join(t for t in converted.values() if t)
    if page["layer"] is not None:
        body = page["layer"] + (f"\n\n{tables}" if tables else "")
    else:
        body = ocr_text
    text = f"--- page {page['page']} ---\n{body}" if page["pdf"] else body
    return text, ocr_text, tables


def fold(s: str, spaces: bool = False) -> str:
    """Compared as printed, less the differences a reader would not call a
    misread: width, case, spaces (kept as one, when `spaces`, so two numbers
    stay two), and the dash or tilde of a range."""
    s = unicodedata.normalize("NFKC", s or "").lower()
    s = re.sub(r"\s+", " " if spaces else "", s).strip()
    return re.sub(r"[~～—–一－]|--", "-", s)


def ocr_rows(truth: list[dict], text: str) -> dict[str, int]:
    """How many printed rows the OCR output reproduces, read with no rule:
    a table row (any table, any header) or a line that holds the row's name
    and its value, and of those how many also hold its unit and its range.
    This is the OCR's own quality; the rule path's is `score_page`'s."""
    units = [" | ".join(row) for table in _html_tables(text) + _markdown_tables(text) for row in table]
    units += _TABLE_BLOCK.sub("\n", text).splitlines()
    units = [(fold(u), fold(u, spaces=True)) for u in units if u.strip()]
    found = complete = 0
    for r in truth:
        name, relaxed = fold(r["item_name"]), fold(re.sub(r"[(（][^)）]*[)）]", "", r["item_name"]))
        value = re.compile(r"(?<![\d.])" + re.escape(fold(r["item_value"], spaces=True)) + r"(?![\d.])")
        rows = [u for u, spaced in units if (name in u or (len(relaxed) >= 2 and relaxed in u)) and value.search(spaced)]
        if not rows:
            continue
        found += 1
        if any(fold(r["item_unit"]) in u and fold(r["item_range"]) in u for u in rows):
            complete += 1
    return {"ocr_found": found, "ocr_complete": complete}


def values_found(truth: list[dict], text: str) -> tuple[int, int]:
    """(printed values the OCR text holds, printed values), each value counted
    as often as the page prints it and found as a whole token: "119/70" is a
    blood pressure, not the 70 of a date, and "5.6" is not inside "15.62"."""
    want = collections.Counter(fold(r["item_value"], spaces=True) for r in truth if r["item_value"].strip())
    flat = fold(text, spaces=True)
    found = 0
    for value, n in want.items():
        hits = re.findall(r"(?<![\d.:/-])" + re.escape(value) + r"(?![\d.:/%-])", flat)
        found += min(n, len(hits))
    return found, sum(want.values())


def score_page(page: dict, outputs: dict[str, dict], spec: dict) -> dict[str, Any]:
    text, ocr_text, tables = page_text(page, outputs, spec)
    readings, _date, unread = table_indicators(text)
    rest = without_rows(text, readings) if readings else text
    left = left_for_model(rest)
    rules_only = bool(readings) and not unread and not left
    preds = [{"item_name": r["original_indicator"], "item_value": r["value"], "item_unit": r["unit"],
              "item_range": r["reference_range"]} for r in readings]
    truth = page["rows"]
    pairs = align(truth, preds)

    n_value = n_unit = n_range = 0
    errors = []
    for i, j, _ in pairs:
        ref, pred = truth[i], preds[j]
        v, u, g = value_ok(pred, ref), unit_ok(pred, ref), range_ok(pred, ref)
        n_value, n_unit, n_range = n_value + v, n_unit + u, n_range + g
        if not (v and u and g):
            errors.append({"truth": {k: ref[k] for k in ("item_name", "item_value", "item_unit", "item_range")},
                           "stored": pred, "wrong": [f for f, good in (("value", v), ("unit", u), ("range", g))
                                                     if not good]})
    repeats, elsewhere, false = split_unmatched(preds, pairs, page)
    out: dict[str, Any] = {
        "truth_rows": len(truth), "stored": len(preds), "matched": len(pairs), "value_ok": n_value,
        "unit_ok": n_unit, "range_ok": n_range, "wrong_value": len(pairs) - n_value, "false": len(false),
        "elsewhere": len(elsewhere), "repeats": len(repeats),
        "unread": unread, "rules_only": rules_only, "left_chars": len(left), "errors": errors,
        "false_rows": false,
        "seconds": round(sum(o.get("seconds", 0) for o in outputs.values()), 2),
        "failed_passes": [n for n, o in outputs.items() if o.get("error")],
        "capped_passes": [n for n, o in outputs.items() if o.get("finish_reason") == "length"],
    }
    out.update(ocr_rows(truth, tables if page["layer"] is not None else ocr_text))
    if page["layer"] is None:
        # What the OCR itself got right, before any table rule: the printed
        # values found in its text, and the narrative lines.
        out["values_found"], out["values_total"] = values_found(truth, ocr_text)
        flat = norm(ocr_text)
        out["strings_total"] = len(page["strings"])
        out["strings_found"] = sum(1 for s in page["strings"] if norm(s) in flat)
        alone, _, _ = table_indicators(tables) if tables else ([], "", 0)
        alone_preds = [{"item_name": r["original_indicator"], "item_value": r["value"]} for r in alone]
        out["tables_alone_matched"] = len(align(truth, alone_preds))
        out["tables_alone_value_ok"] = sum(value_ok({**alone_preds[j], "item_unit": alone[j]["unit"]}, truth[i])
                                           for i, j, _ in align(truth, alone_preds))
    return out


SUMS = ("truth_rows", "stored", "matched", "value_ok", "unit_ok", "range_ok", "wrong_value", "false", "elsewhere",
        "repeats", "unread",
        "left_chars", "values_total", "values_found", "strings_total", "strings_found", "tables_alone_matched",
        "tables_alone_value_ok", "ocr_found", "ocr_complete", "seconds")


def aggregate(scores: list[dict]) -> dict[str, Any]:
    c: collections.Counter = collections.Counter()
    for s in scores:
        for k in SUMS:
            c[k] += s.get(k, 0) or 0
    n = len(scores)

    def rate(a, b):
        return round(c[a] / c[b], 3) if c[b] else None

    return {
        "pages": n, **{k: round(c[k], 2) if k == "seconds" else c[k] for k in SUMS},
        "ocr_found": c["ocr_found"], "ocr_row_recall": rate("ocr_found", "truth_rows"),
        "ocr_complete_recall": rate("ocr_complete", "truth_rows"),
        "row_recall": rate("matched", "truth_rows"), "correct_recall": rate("value_ok", "truth_rows"),
        "value_exact": rate("value_ok", "matched"), "unit_exact": rate("unit_ok", "matched"),
        "range_exact": rate("range_ok", "matched"),
        "rules_only_pages": sum(1 for s in scores if s["rules_only"]),
        "values_in_ocr": rate("values_found", "values_total"), "strings_in_ocr": rate("strings_found", "strings_total"),
        "tables_alone_recall": rate("tables_alone_matched", "truth_rows"),
        "seconds_per_page": round(c["seconds"] / n, 1) if n else None,
        "failed_passes": sum(len(s["failed_passes"]) for s in scores),
        "capped_passes": sum(len(s["capped_passes"]) for s in scores),
    }


def breakdowns(pages: list[dict], scores: dict[str, dict], aggregate) -> dict[str, Any]:
    """`overall` and `by_tier` leave the series (home logs) out, which the table
    rules leave to the text model by design; `series` is them alone; `all` and
    the `*_all`/group breakdowns count every page."""
    def agg(keep) -> dict:
        return aggregate([scores[p["id"]] for p in pages if p["id"] in scores and keep(p)])

    out = {"overall": agg(lambda p: not p["series"]), "series": agg(lambda p: p["series"]), "all": agg(lambda p: True),
           "by_tier": {t: agg(lambda p, t=t: p["tier"] == t and not p["series"]) for t in sorted({p["tier"] for p in pages})},
           "by_tier_all": {t: agg(lambda p, t=t: p["tier"] == t) for t in sorted({p["tier"] for p in pages})}}
    for key in GROUPS:
        values = sorted({p["group"][key] for p in pages if key in p["group"]})
        if values:
            out[f"by_{key}"] = {v: agg(lambda p, k=key, v=v: p["group"].get(k) == v) for v in values}
    return out


def summarize(pages: list[dict], scores: dict[str, dict]) -> dict[str, Any]:
    out = breakdowns(pages, scores, aggregate)
    out["all_pages_seconds"] = round(sum(s["seconds"] for s in scores.values()), 1)
    return out


# --- the run ----------------------------------------------------------------------------


def git_head(path: Path) -> str:
    """The checkout's commit, or a snapshot's: a copy of a clean checkout taken
    so a branch that moves during a long run cannot change the code under it
    records the commit it was copied at in `.commit`."""
    if (path / ".commit").exists():
        return (path / ".commit").read_text().strip()
    return subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def git_dirty(path: Path) -> bool | None:
    if (path / ".commit").exists():
        return None
    out = subprocess.run(["git", "-C", str(path), "status", "--porcelain", "--untracked-files=no"],
                         capture_output=True, text=True).stdout
    return bool(out.strip())


def environment(args) -> dict[str, Any]:
    llama = subprocess.run(["llama-server", "--version"], capture_output=True, text=True)
    chip = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True).stdout.strip()
    mem = subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout.strip()
    return {
        "llama_cpp": " ".join((llama.stdout + llama.stderr).strip().splitlines()[:2]),
        "mirobody_src": str(ROOT), "mirobody_commit": git_head(ROOT), "mirobody_dirty": git_dirty(ROOT),
        "machine": f"{chip or platform.processor()}, {int(mem) // 2**30 if mem else '?'} GiB, "
                   f"{platform.system()} {platform.mac_ver()[0] or platform.release()}",
        "python": platform.python_version(),
    }


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--model", required=True, help="an id from models.json")
    ap.add_argument("--corpus", required=True, help="a mirobody-gen build made by clean_pdfs.py (has clean/)")
    ap.add_argument("--sample", default=str(HERE / "sample.json"), help="the pages to run (sample.json's shape)")
    ap.add_argument("--set", default="", help="results go to results/<model>/<set>/ (default: results/<model>/)")
    ap.add_argument("--models-dir", default=str(HERE / "models"), help="GGUFs under <dir>/<org>__<repo>/")
    ap.add_argument("--port", type=int, default=8187)
    ap.add_argument("--pages", nargs="*", help="only these page ids")
    ap.add_argument("--rescore", action="store_true", help="score the stored outputs again; no model is run")
    ap.add_argument("--force", action="store_true", help="run pages that already have stored outputs")
    ap.add_argument("--min-free-gb", type=float, default=0, help="stop when the disk has less free space")
    ap.add_argument("--min-free-mem", type=float, default=0, help="stop when less memory is free (percent, macOS)")
    args = ap.parse_args()

    specs = json.loads((HERE / "models.json").read_text(encoding="utf-8"))
    spec = specs[args.model]
    sample = json.loads(Path(args.sample).read_text(encoding="utf-8"))
    corpus = Path(args.corpus)
    records = {}
    for line in (corpus / "files.jsonl").read_text(encoding="utf-8").splitlines():
        r = json.loads(line)
        records[r["doc_id"]] = r
    entries = [e for e in sample["pages"] if not args.pages or e["id"] in args.pages]
    pages = [prepare(corpus, e, records[e["doc_id"]]) for e in entries]
    out_dir = RESULTS / args.model / args.set if args.set else RESULTS / args.model
    raw_dir = out_dir / "pages"

    meta_path = out_dir / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    if not args.rescore:
        paths = model_paths(spec, Path(args.models_dir))
        server = Server(spec, paths, args.port, out_dir / "server.log")
        todo = [p for p in pages if args.force or not (raw_dir / f"{p['id']}.json").exists()]
        log(f"{args.model}: {len(todo)} of {len(pages)} pages to run")
        memory: list[dict] = []
        try:
            if (why := resources_low(args)):
                raise SystemExit(f"not started: {why}")
            load = server.start()
            idle = server.memory()
            log(f"loaded in {load:.1f}s, {idle}")
            for n, page in enumerate(todo, 1):
                if (why := resources_low(args)):
                    raise SystemExit(f"stopped before {page['id']}: {why}; stored pages are kept, rerun to resume")
                passes = {"tables": spec["passes"]["tables"]} if page["layer"] is not None else spec["passes"]
                outputs = {name: server.chat(page["image"], prompt) for name, prompt in passes.items()}
                mem = server.memory()
                memory.append(mem)
                write_json(raw_dir / f"{page['id']}.json", {
                    "id": page["id"], "tier": page["tier"], "image_sha256": sha256_bytes(page["image"]),
                    "image_size": page["image_size"], "text_layer": page["layer"] is not None,
                    "outputs": {k: {"prompt": passes[k], **v} for k, v in outputs.items()}, "memory": mem})
                log(f"{n}/{len(todo)} {page['id']} {page['tier']} "
                    f"{', '.join(f'{k} {v.get('seconds')}s' + (' ERR' if v.get('error') else '') for k, v in outputs.items())}")
            final = server.memory()
        finally:
            server.stop()
        if todo:
            meta = {
                "model": args.model, "spec": spec, "files": {k: {"name": p.name, "bytes": p.stat().st_size,
                                                                 "sha256": sha256_file(p)} for k, p in paths.items()},
                "server": {"ctx": CTX, "slots": 1, "cache_ram": 0, "max_tokens": MAX_TOKENS},
                "load_seconds": round(load, 1), "memory_idle": idle, "memory_final": final,
                "memory_max_rss_mib": max((m.get("rss_mib", 0) for m in memory), default=None),
                "memory_max_footprint_mib": max((m.get("phys_footprint_mib", 0) for m in memory), default=None),
                "run_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
                "pages_run": len(todo), "command": " ".join(sys.argv),
            }
            write_json(meta_path, meta)

    scores = {}
    for page in pages:
        path = raw_dir / f"{page['id']}.json"
        if not path.exists():
            continue
        stored = json.loads(path.read_text(encoding="utf-8"))
        scores[page["id"]] = {"tier": page["tier"], **score_page(page, stored["outputs"], spec)}
    scored_pages = [p for p in pages if p["id"] in scores]
    summary = {"model": args.model, "name": spec["name"], **summarize(scored_pages, scores),
               "memory": {k: meta.get(k) for k in ("memory_idle", "memory_final", "memory_max_rss_mib",
                                                    "memory_max_footprint_mib", "load_seconds")},
               "files": meta.get("files")}
    # Scored per Mirobody commit: the table rules are the product's, and a
    # rule change rescored here must not overwrite what the previous commit read.
    commit = git_head(ROOT)[:7]
    summary["mirobody_commit"] = git_head(ROOT)
    write_json(out_dir / f"scores-{commit}.json", scores)
    write_json(out_dir / f"summary-{commit}.json", summary)

    run_path = RESULTS / "run.json"
    run = json.loads(run_path.read_text()) if run_path.exists() else {}
    if "corpus" in run:                    # the layout before there was more than one sample
        run = {"printed": run}
    entry = run.setdefault(args.set or "printed", {})
    entry["corpus"] = sample["corpus"]
    entry["sample"] = Path(args.sample).name
    entry.setdefault("models", {})
    if meta:
        entry["models"][args.model] = {
            "repo": spec["repo"], "files": meta.get("files"), "run_at": meta.get("run_at"),
            "command": meta.get("command"), "scored_with": environment(args)}
    write_json(run_path, run)
    o = summary["overall"]
    log(f"{args.model}: recall {o['row_recall']} value {o['value_exact']} false {o['false']} "
        f"unread {o['unread']} {o['seconds_per_page']}s/page")


if __name__ == "__main__":
    main()
