"""Whether a model server serves a model that can see, whether it has
loaded it yet, and how much context one request to it gets.

For entries whose address is configuration (`LOCAL_BASE_URL`): the model behind
it can be swapped for a text-only one (MiniCPM5-2B, a quant served without its
mmproj), and the entry would still say `supports_image: true`. Measured on
llama.cpp b11269:

* a single-model server answers `/props` with `modalities.vision`;
* the router answers `/props?model=<id>` the same way, but LOADS the model to
  do it (13 GB for a flag), so it is asked only once `/models` says the model
  is loaded; before that its launch args or preset say whether it has an mmproj;
* Ollama lists `capabilities` at `/api/show`.

Anything else says nothing, and the entry's own `supports_image` stands.
"""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.parse
import urllib.request
from typing import Any

_TTL_SEC = 300.0
#: How long "the server does not say" stands: a router that has not loaded the
#: model yet says nothing, and says it once the setup page has loaded it.
_UNKNOWN_TTL_SEC = 30.0
_TIMEOUT_SEC = 3.0
#: How long a router's status for a model stands. Every chat turn asks, and a
#: model that finished loading must not read as loading for long after.
_STATUS_TTL_SEC = 5.0
_cache: dict[tuple[str, str], tuple[float, bool | None]] = {}
_status_cache: dict[tuple[str, str], tuple[float, str | None]] = {}
_context_cache: dict[tuple[str, str], tuple[float, int | None]] = {}
_refreshing: set[tuple[str, str]] = set()
_lock = threading.Lock()


def _fetch(url: str, body: dict | None = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data, {"Content-Type": "application/json"} if data else {})
    with urllib.request.urlopen(request, timeout=_TIMEOUT_SEC) as response:
        return json.load(response)


def _vision_of(props: Any) -> bool | None:
    modalities = props.get("modalities") if isinstance(props, dict) else None
    if isinstance(modalities, dict) and "vision" in modalities:
        return bool(modalities["vision"])
    return None


def _listed(root: str, model: str) -> dict | None:
    """`model`'s row in the server's `/models` listing, or None."""
    listing = _fetch(f"{root}/models").get("data") or []
    return next((m for m in listing if m.get("id") == model), None)


def _ask(root: str, model: str) -> bool | None:
    try:
        props = _fetch(f"{root}/props")
    except Exception:
        props = None
    if isinstance(props, dict) and props.get("role") == "router":
        row = _listed(root, model)
        if row is None:
            return None
        status = row.get("status") or {}
        if status.get("value") == "loaded":
            return _vision_of(_fetch(f"{root}/props?model={urllib.parse.quote(model)}"))
        described = " ".join(map(str, status.get("args") or [])) + "\n" + str(status.get("preset") or "")
        return True if "mmproj" in described else None
    seen = _vision_of(props)
    if seen is not None:
        return seen
    try:
        capabilities = _fetch(f"{root}/api/show", {"model": model}).get("capabilities")
    except Exception:
        return None
    return "vision" in capabilities if isinstance(capabilities, list) else None


def _read(key: tuple[str, str]) -> bool | None:
    try:
        answer = _ask(key[0].rstrip("/").removesuffix("/v1"), key[1])
    except Exception:
        answer = None
    _cache[key] = (time.monotonic(), answer)
    return answer


def _refresh(key: tuple[str, str]) -> None:
    try:
        _read(key)
    finally:
        with _lock:
            _refreshing.discard(key)


def served_vision(base_url: str, model: str, *, fresh: bool = False) -> bool | None:
    """True or False when the server says whether `model` takes images; None
    when it does not say or cannot be reached. Cached for five minutes (30 s
    for None). Callers run on the event loop, and a server that does not
    answer costs up to three timeouts: so a stale answer is returned at once
    and re-read beside the caller, and only the first question in a process
    waits for the server. `fresh`, for a caller off the event loop, reads
    again rather than take an expired answer or an unknown one."""
    if not base_url or not model:
        return None
    key = (base_url, model)
    hit = _cache.get(key)
    expired = hit is not None and time.monotonic() - hit[0] >= (_TTL_SEC if hit[1] is not None else _UNKNOWN_TTL_SEC)
    if hit is None or (fresh and (hit[1] is None or expired)):
        return _read(key)
    if time.monotonic() - hit[0] >= (_TTL_SEC if hit[1] is not None else _UNKNOWN_TTL_SEC):
        with _lock:
            start = key not in _refreshing
            _refreshing.add(key)
        if start:
            threading.Thread(target=_refresh, args=(key,), daemon=True).start()
    return hit[1]


def _context_of_args(args: list) -> int | None:
    """The context one request gets from llama-server's launch args: `c`, or
    `c` divided between the slots when they do not share one KV pool."""
    args = [str(a) for a in args]
    value = {flag: args[i + 1] for i, flag in enumerate(args[:-1]) if args[i + 1].isdigit()}
    context = int(value.get("--ctx-size") or value.get("-c") or 0)
    if not context:
        return None
    slots = int(value.get("--parallel") or value.get("-np") or 1)
    return context if "--kv-unified" in args or slots <= 1 else context // slots


_PRESET_CONTEXT = re.compile(r"^\s*(?:c|ctx-size)\s*=\s*(\d+)\s*$", re.M)


def _ask_context(root: str, model: str) -> int | None:
    try:
        props = _fetch(f"{root}/props")
    except Exception:
        props = None
    if isinstance(props, dict) and props.get("role") == "router":
        # The router lists a model's launch args before it loads it.
        status = (_listed(root, model) or {}).get("status") or {}
        found = _context_of_args(status.get("args") or [])
        if found:
            return found
        match = _PRESET_CONTEXT.search(str(status.get("preset") or ""))
        return int(match.group(1)) if match else None
    settings = props.get("default_generation_settings") if isinstance(props, dict) else None
    context = settings.get("n_ctx") if isinstance(settings, dict) else None
    return context if isinstance(context, int) and context > 0 else None


def served_context(base_url: str, model: str) -> int | None:
    """The tokens one request to `model` may hold on the server behind
    `base_url`; None when it does not say (Ollama, a cloud API) or cannot be
    reached. Cached for five minutes (30 s for None). It blocks for up to two
    timeouts, so async callers run it in a thread."""
    if not base_url or not model:
        return None
    key = (base_url, model)
    hit = _context_cache.get(key)
    if hit is not None and time.monotonic() - hit[0] < (_TTL_SEC if hit[1] is not None else _UNKNOWN_TTL_SEC):
        return hit[1]
    try:
        answer = _ask_context(base_url.rstrip("/").removesuffix("/v1"), model)
    except Exception:
        answer = None
    _context_cache[key] = (time.monotonic(), answer)
    return answer


def served_status(base_url: str, model: str) -> str | None:
    """What llama.cpp's router reports for `model`: `unloaded`, `loading`
    (its download included) or `loaded`. None from a server that reports no
    status (a single-model server, Ollama) or cannot be reached. Cached for
    five seconds; it blocks for up to one timeout, so async callers run it in
    a thread."""
    if not base_url or not model:
        return None
    key = (base_url, model)
    hit = _status_cache.get(key)
    if hit is not None and time.monotonic() - hit[0] < _STATUS_TTL_SEC:
        return hit[1]
    try:
        row = _listed(base_url.rstrip("/").removesuffix("/v1"), model)
    except Exception:
        row = None
    status = (row or {}).get("status")
    value = status.get("value") if isinstance(status, dict) else None
    answer = str(value) if value else None
    _status_cache[key] = (time.monotonic(), answer)
    return answer


#: How long work that needs a local model waits for a router that is loading
#: it before saying so: a load from disk takes seconds, a download minutes to
#: hours. Before this an upload waited on the request itself: 307 s for the
#: summary, as long again for the readings, the row reading 已处理 meanwhile,
#: and the reason was "InternalServerError" (#275, a stalled download).
LOAD_WAIT_SEC = 120.0
_POLL_SEC = 5.0
#: When a wait last gave up on a model, by (address, model).
_gave_up: dict[tuple[str, str], float] = {}


class ModelNotReady(RuntimeError):
    """A local model the router is still downloading or loading, or could not
    load. The message names the model and the fix, for the person whose
    upload needed it."""

    def __init__(self, model: str, *, failed: bool = False) -> None:
        if failed:
            message = (f"the local model {model} failed to load: its download did not finish or the file is "
                       "damaged (the model server's log says which); upload the file again once it loads")
        else:
            message = (f"the local model {model} is still downloading or loading; upload the file again once "
                       "the setup page shows it ready")
        super().__init__(message)
        self.model = model


def still_loading(spec: Any) -> bool:
    """Whether `spec` runs on a local model server whose router is still
    downloading or loading its model. An `unloaded` model is left to the
    request, which is what starts its load. Blocks for up to one timeout."""
    if spec is None or not (getattr(spec, "base_url_env", "") and getattr(spec, "base_url", "")):
        return False
    return served_status(spec.base_url, spec.model) == "loading"


async def wait_until_loaded(spec: Any, *, wait: float | None = None) -> None:
    """Return once the router no longer reports `spec`'s model loading (or
    never did); raise `ModelNotReady` if it still is after `wait` seconds,
    `LOAD_WAIT_SEC` by default."""
    import asyncio

    limit = LOAD_WAIT_SEC if wait is None else wait
    key = (getattr(spec, "base_url", ""), getattr(spec, "model", ""))
    deadline = time.monotonic() + limit
    while await asyncio.to_thread(still_loading, spec):
        # One wait per model, not one per caller: a scan's two OCR passes
        # waited it out in turn (240 s), and every further page would have.
        recently = time.monotonic() - _gave_up.get(key, float("-inf")) < limit
        if recently or time.monotonic() >= deadline:
            _gave_up[key] = time.monotonic()
            raise ModelNotReady(spec.model)
        await asyncio.sleep(_POLL_SEC)
    _gave_up.pop(key, None)


def failed_to_load(spec: Any, exc: BaseException) -> bool:
    """Whether `exc` is llama.cpp's router answering that `spec`'s model
    failed to load ("model name=glm-ocr failed to load", HTTP 500), which the
    upload reported as "InternalServerError"."""
    return (bool(getattr(spec, "base_url_env", "")) and getattr(exc, "status_code", None) == 500
            and "failed to load" in str(exc))


def sees(spec: Any) -> bool:
    """Whether a resolved route can be sent an image: a declared `false` is
    final; an address from configuration asks the server; otherwise the
    entry's word, with no word meaning yes (the vision surface's old rule)."""
    if getattr(spec, "supports_image", None) is False:
        return False
    if getattr(spec, "base_url_env", "") and getattr(spec, "base_url", ""):
        seen = served_vision(spec.base_url, spec.model)
        if seen is not None:
            return seen
    return True


__all__ = ["LOAD_WAIT_SEC", "ModelNotReady", "failed_to_load", "sees", "served_context", "served_status",
           "served_vision", "still_loading", "wait_until_loaded"]
