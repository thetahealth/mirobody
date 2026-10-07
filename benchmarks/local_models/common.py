"""What load.py, cases.py and run.py share: the stack's addresses, an HTTP
client that signs in, the corpus reader, the model router and the database.

Nothing here imports mirobody: the benchmark talks to a running stack over its
HTTP API, as the web client does, so it measures what a person gets. The one
exception is `psql`, which reads the extractor of each stored reading and the
task queue; neither is exposed over HTTP.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RESULTS = HERE / "results"

APP = os.environ.get("MIROBODY_URL", "http://localhost:18260").rstrip("/")
#: The llama.cpp router as this machine reaches it, and as the app (in Docker)
#: reaches it. The second is what the setup page saves.
ROUTER = os.environ.get("LLAMA_ROUTER", "http://127.0.0.1:8080").rstrip("/")
ROUTER_FROM_APP = os.environ.get("LLAMA_ROUTER_FROM_APP", "http://host.docker.internal:8080/v1")
#: Where `docker compose` finds the stack (the checkout with compose.yaml and .env).
COMPOSE_DIR = Path(os.environ.get("MIROBODY_COMPOSE_DIR", str(ROOT)))

#: Every benchmark account signs in with this. A local test stack only.
PASSWORD = os.environ.get("BENCH_PASSWORD", "local-models-bench")
EMAIL_DOMAIN = "bench.mirobody.local"

#: The sizes of 1.5.4 (config.llm.yaml LOCAL_SETUP) and the preset section
#: each names. The document reader is the same for all of them.
SIZES = {"tiny": "minicpm5-1b", "small": "minicpm5-2b", "large": "qwen3.8-27b"}
OCR_MODEL = "glm-ocr"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def setup_token() -> str:
    """`SETUP_TOKEN` from the environment, else from the stack's `.env`."""
    token = os.environ.get("SETUP_TOKEN", "")
    if token:
        return token
    env = COMPOSE_DIR / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith("SETUP_TOKEN="):
                return line.split("=", 1)[1].strip().strip("'\"")
    raise SystemExit("SETUP_TOKEN is not set and no .env next to compose.yaml holds it")


# --- the app -------------------------------------------------------------------------


class ApiError(RuntimeError):
    pass


@dataclass
class Account:
    """A signed-in benchmark account. `tz` and `lang` are what its web client
    would send as X-Timezone and X-Language."""

    email: str
    tz: str = "Asia/Shanghai"
    lang: str = "zh"
    token: str = ""
    user_id: str = ""
    session: requests.Session = field(default_factory=requests.Session, repr=False)

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}", "X-Timezone": self.tz, "X-Language": self.lang}

    def get(self, path: str, **params: Any) -> Any:
        # A GET is safe to repeat. Polling every few seconds meets uvicorn
        # closing an idle keep-alive connection at the moment it is reused,
        # which surfaces as RemoteDisconnected.
        for attempt in range(3):
            try:
                r = self.session.get(APP + path, params=params, headers=self.headers(), timeout=120)
                return _body(r, path)
            except requests.ConnectionError:
                if attempt == 2:
                    raise
                time.sleep(2)

    def post(self, path: str, body: Any = None, *, timeout: float = 120, **kw: Any) -> Any:
        # config.yaml REQUEST_RATE_LIMITER allows six /api/session and six
        # /api/chat a minute per caller; a small model answers faster than
        # that, so a 429 waits for the window instead of failing the run.
        for _ in range(12):
            r = self.session.post(APP + path, json=body, headers=self.headers(), timeout=timeout, **kw)
            if r.status_code != 429:
                break
            time.sleep(10)
        return _body(r, path)

    def put(self, path: str, body: Any) -> Any:
        r = self.session.put(APP + path, json=body, headers=self.headers(), timeout=120)
        return _body(r, path)


def _body(r: requests.Response, path: str) -> Any:
    try:
        data = r.json()
    except ValueError as e:
        raise ApiError(f"{path}: HTTP {r.status_code}, not JSON") from e
    if r.status_code >= 400:
        raise ApiError(f"{path}: HTTP {r.status_code}: {str(data)[:300]}")
    if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] != 0:
        raise ApiError(f"{path}: code {data['code']}: {data.get('msg')}")
    return data


_SIGNED_IN: dict[str, str] = {}


def sign_in(email: str, *, tz: str = "Asia/Shanghai", lang: str = "zh") -> Account:
    """Sign in to `email` with the benchmark password, registering it the
    first time. A token is reused within a process: the login and register
    paths are rate-limited per address (config.yaml REQUEST_RATE_LIMITER)."""
    account = Account(email=email, tz=tz, lang=lang)
    token = _SIGNED_IN.get(email)
    if not token:
        body = {"email": email, "password": PASSWORD}
        r = _auth("/password/login", body)
        if r.get("code") != 0:
            r = _auth("/password/register", body)
        if r.get("code") != 0:
            raise ApiError(f"cannot sign in {email}: {r.get('msg')}")
        token = _SIGNED_IN[email] = r["data"]["access_token"]
    account.token = token
    account.user_id = str(_jwt_subject(token))
    return account


def _auth(path: str, body: dict) -> dict:
    for _ in range(8):
        r = requests.post(APP + path, json=body, timeout=30)
        if r.status_code != 429:
            return r.json()
        time.sleep(15)
    raise ApiError(f"{path}: still rate-limited after two minutes")


def _jwt_subject(token: str) -> str:
    import base64

    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))["sub"]


def bench_email(kind: str, name: str) -> str:
    return f"{kind}-{name}@{EMAIL_DOMAIN}".lower()


#: The Q&A accounts' generation. A new plan (other people or documents), or a
#: pipeline that reads the same documents differently, is a new generation, so
#: no run reads a record loaded by another. qa2: loaded at f76bcbd (results
#: small, tiny, ref-deepseek-v4.1-flash); qa3: the same plan loaded again at
#: 4e3c06f, after the extraction fixes (the -v2 runs and the cloud references);
#: qa4: loaded again at 958fae5, after round 2's extraction changes (small-v3).
QA_GENERATION = "qa4"


def qa_email(person: str) -> str:
    return bench_email(QA_GENERATION, person)


# --- the corpus (a mirobody-gen build) -----------------------------------------------


class Corpus:
    """A mirobody-gen build directory: files.jsonl, manifest.jsonl,
    people.jsonl, devices.jsonl, genomics.jsonl, journal.jsonl."""

    def __init__(self, path: Path | str):
        self.dir = Path(path).resolve()
        if not (self.dir / "files.jsonl").exists():
            raise SystemExit(f"{self.dir} is not a mirobody-gen build (no files.jsonl); see README.md")
        self.files = {f["file"]: f for f in read_jsonl(self.dir / "files.jsonl")}
        self.people = {p["person_id"]: p for p in read_jsonl(self.dir / "people.jsonl")}
        self.devices = {d["person_id"]: d for d in read_jsonl(self.dir / "devices.jsonl")}
        self.genomics = {g["person_id"]: g for g in read_jsonl(self.dir / "genomics.jsonl")}
        self.journal = read_jsonl(self.dir / "journal.jsonl")

    def path(self, rel: str) -> Path:
        return self.dir / rel

    def journal_of(self, person: str) -> list[dict]:
        return [j for j in self.journal if j["person_id"] == person]


# --- the model router ----------------------------------------------------------------


def router_models() -> dict[str, dict]:
    """id -> the router's entry (status, args)."""
    r = requests.get(ROUTER + "/models", timeout=10)
    r.raise_for_status()
    return {m["id"]: m for m in r.json().get("data") or []}


def router_status() -> dict[str, str]:
    return {k: (v.get("status") or {}).get("value", "") for k, v in router_models().items()}


def wait_loaded(model_ids: list[str], timeout: float = 3600, log=print) -> float:
    """Seconds until every id is `loaded`. Raises on `failed` or timeout."""
    t0 = time.monotonic()
    last = None
    while True:
        status = router_status()
        now = {m: status.get(m, "missing") for m in model_ids}
        if now != last:
            log(f"  router: {now}")
            last = now
        if all(s == "loaded" for s in now.values()):
            return time.monotonic() - t0
        if any(s in ("failed", "missing") for s in now.values()) and time.monotonic() - t0 > 30:
            raise RuntimeError(f"router: {now}")
        if time.monotonic() - t0 > timeout:
            raise TimeoutError(f"router did not load {model_ids} in {timeout:.0f} s: {now}")
        time.sleep(5)


def model_files(model_id: str) -> dict[str, Any]:
    """What the router runs for `model_id`: the Hugging Face repo:quant or the
    local GGUF path, from the arguments it starts the child with."""
    entry = router_models().get(model_id) or {}
    args = (entry.get("status") or {}).get("args") or []
    out: dict[str, Any] = {"id": model_id, "args": args}
    for flag, key in (("--hf-repo", "hf_repo"), ("--hf-file", "hf_file"), ("--model", "model_path"), ("-m", "model_path"),
                      ("--mmproj", "mmproj_path"), ("--ctx-size", "ctx_size"), ("--parallel", "parallel")):
        if flag in args:
            out[key] = args[args.index(flag) + 1]
    return out


def gguf_in_cache(hf_repo: str, hf_file: str | None = None) -> list[Path]:
    """The GGUF files of `owner/name:QUANT` in the Hugging Face cache."""
    repo, _, quant = hf_repo.partition(":")
    snap = Path.home() / ".cache/huggingface/hub" / ("models--" + repo.replace("/", "--")) / "snapshots"
    found = sorted(p for p in snap.glob("*/*.gguf"))
    if hf_file:
        return [p for p in found if p.name == hf_file] + [p for p in found if p.name.startswith("mmproj")]
    if quant:
        return [p for p in found if quant.lower() in p.name.lower() or p.name.startswith("mmproj")]
    return found


def sha256(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def llama_processes() -> list[dict[str, Any]]:
    """The llama-server processes: the router and the one child per loaded
    model, with resident set size (`ps -o rss`) and, on macOS, the physical
    footprint (`footprint`), which counts Metal buffers that RSS does not."""
    out = subprocess.run(["ps", "-axo", "pid=,ppid=,rss=,command="], capture_output=True, text=True).stdout
    procs = []
    for line in out.splitlines():
        parts = line.split(None, 3)
        if len(parts) < 4 or "llama-server" not in parts[3]:
            continue
        pid, ppid, rss, cmd = int(parts[0]), int(parts[1]), int(parts[2]), parts[3]
        if not cmd.split()[0].endswith("llama-server"):
            continue                                   # a shell that started it
        alias = ""
        m = re.search(r"--alias\s+(\S+)", cmd)
        if m:
            alias = m.group(1)
        procs.append({"pid": pid, "ppid": ppid, "rss_mb": round(rss / 1024, 1), "alias": alias,
                      "role": "child" if "--port 0" in cmd or alias else "router"})
    for p in procs:
        p["footprint_mb"] = _footprint_mb(p["pid"])
    return procs


def _footprint_mb(pid: int) -> float | None:
    if not Path("/usr/bin/footprint").exists():
        return None
    try:
        out = subprocess.run(["/usr/bin/footprint", "-f", "bytes", str(pid)], capture_output=True, text=True,
                             timeout=60).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r"phys_footprint:\s*([\d,]+)\s*B", out) or re.search(r"Footprint:\s*([\d,]+)\s*B", out)
    if not m:
        return None
    return round(int(m.group(1).replace(",", "")) / 1024 / 1024, 1)


def memory_snapshot() -> dict[str, Any]:
    procs = llama_processes()
    children = [p for p in procs if p["role"] == "child"]
    return {
        "at": now_iso(),
        "processes": procs,
        "children_rss_mb": round(sum(p["rss_mb"] for p in children), 1),
        "children_footprint_mb": (round(sum(p["footprint_mb"] or 0 for p in children), 1)
                                  if children and all(p["footprint_mb"] is not None for p in children) else None),
        "swap_used_mb": _swap_used_mb(),
    }


#: A run stops, keeping what it has, below these. Swap files live on the
#: data volume: on 2026-10-06, on a 16 GB machine, swap reached 24 GB, the
#: disk filled and the Docker VM's disk aborted its journal, taking the
#: stack's database with it.
MIN_FREE_DISK_GB = float(os.environ.get("BENCH_MIN_FREE_DISK_GB", "5"))
MIN_FREE_MEMORY_PCT = float(os.environ.get("BENCH_MIN_FREE_MEMORY_PCT", "10"))


class ResourceStop(RuntimeError):
    pass


def resources() -> dict[str, Any]:
    """Free disk where this checkout lives, and macOS's free-memory
    percentage (`memory_pressure`; None elsewhere)."""
    import shutil

    free_gb = shutil.disk_usage(HERE).free / 2**30          # GiB, as df -h reports it
    pct = None
    try:
        out = subprocess.run(["memory_pressure"], capture_output=True, text=True, timeout=30).stdout
        m = re.search(r"free percentage:\s*(\d+)%", out)
        pct = int(m.group(1)) if m else None
    except (OSError, subprocess.TimeoutExpired):
        pass
    return {"disk_free_gb": round(free_gb, 1), "memory_free_pct": pct, "swap_used_mb": _swap_used_mb()}


def _short(r: dict[str, Any]) -> bool:
    return r["disk_free_gb"] < MIN_FREE_DISK_GB or (r["memory_free_pct"] is not None
                                                   and r["memory_free_pct"] < MIN_FREE_MEMORY_PCT)


def guard(where: str) -> dict[str, Any]:
    """Raise ResourceStop when free disk or free memory is under its floor,
    and still is 30 s later: a model paging in dips memory for a moment."""
    r = resources()
    if _short(r):
        time.sleep(30)
        r = resources()
    if _short(r):
        raise ResourceStop(f"{where}: {r['disk_free_gb']} GB disk free (floor {MIN_FREE_DISK_GB}), "
                           f"{r['memory_free_pct']}% memory free (floor {MIN_FREE_MEMORY_PCT}), "
                           f"swap {r['swap_used_mb']} MB")
    return r


def swapouts() -> int | None:
    """macOS's count of pages swapped out since boot (`vm_stat`); a rise
    while a question is answered means the answer was timed under swapping."""
    try:
        out = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=10).stdout
    except OSError:
        return None
    m = re.search(r"Swapouts:\s*(\d+)", out)
    return int(m.group(1)) if m else None


def _swap_used_mb() -> float | None:
    """macOS swap in use, from `sysctl vm.swapusage`; None elsewhere."""
    try:
        out = subprocess.run(["sysctl", "-n", "vm.swapusage"], capture_output=True, text=True, timeout=10).stdout
    except OSError:
        return None
    m = re.search(r"used\s*=\s*([\d.]+)M", out)
    return float(m.group(1)) if m else None


# --- the database (read-only, for what HTTP does not expose) ------------------------


def psql_json(sql: str) -> Any:
    """Run one SELECT in the stack's Postgres and return its JSON value. The
    statement must yield one row, one column of json."""
    cmd = ["docker", "compose", "exec", "-T", "pg", "psql", "-U", os.environ.get("PG_USER", "holistic_user"),
           "-d", os.environ.get("PG_DBNAME", "holistic_db"), "-At", "-v", "ON_ERROR_STOP=1",
           "-c", "set search_path=theta_ai,public", "-c", sql]
    out = subprocess.run(cmd, cwd=COMPOSE_DIR, capture_output=True, text=True, timeout=120)
    if out.returncode != 0:
        raise RuntimeError(f"psql failed: {out.stderr.strip()[:300]}")
    lines = [line for line in out.stdout.splitlines() if line.strip() and line.strip() != "SET"]
    return json.loads(lines[-1]) if lines else None


def psql_exec(sql: str) -> str:
    """Run one statement that changes the stack's database; its status line."""
    cmd = ["docker", "compose", "exec", "-T", "pg", "psql", "-U", os.environ.get("PG_USER", "holistic_user"),
           "-d", os.environ.get("PG_DBNAME", "holistic_db"), "-At", "-v", "ON_ERROR_STOP=1",
           "-c", "set search_path=theta_ai,public", "-c", sql]
    out = subprocess.run(cmd, cwd=COMPOSE_DIR, capture_output=True, text=True, timeout=120)
    if out.returncode != 0:
        raise RuntimeError(f"psql failed: {out.stderr.strip()[:300]}")
    return out.stdout.strip().splitlines()[-1] if out.stdout.strip() else ""


def pending_tasks() -> int:
    """Background tasks not yet done (the profile refresh an upload queues)."""
    n = psql_json("select to_json(count(*)) from th_task_queue where failed_at is null")
    return int(n or 0)


def wait_tasks_drained(timeout: float = 1800, log=print) -> float:
    t0 = time.monotonic()
    while True:
        n = pending_tasks()
        if n == 0:
            return time.monotonic() - t0
        if time.monotonic() - t0 > timeout:
            log(f"  {n} background task(s) still pending after {timeout:.0f} s; going on")
            return time.monotonic() - t0
        time.sleep(10)


def git_head(path: Path) -> str:
    out = subprocess.run(["git", "-C", str(path), "rev-parse", "HEAD"], capture_output=True, text=True)
    return out.stdout.strip()


def git_dirty(path: Path) -> bool:
    out = subprocess.run(["git", "-C", str(path), "status", "--porcelain", "--untracked-files=no"],
                         capture_output=True, text=True)
    return bool(out.stdout.strip())


def llama_version() -> str:
    try:
        out = subprocess.run(["llama-server", "--version"], capture_output=True, text=True, timeout=30)
    except OSError:
        return ""
    return " ".join((out.stdout + out.stderr).split("\n")[0:2]).strip()
