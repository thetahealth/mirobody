"""Whether a model server serves a model that can see, and whether it has
loaded it yet.

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


def served_vision(base_url: str, model: str) -> bool | None:
    """True or False when the server says whether `model` takes images; None
    when it does not say or cannot be reached. Cached for five minutes (30 s
    for None). Callers run on the event loop, and a server that does not
    answer costs up to three timeouts: so a stale answer is returned at once
    and re-read beside the caller, and only the first question in a process
    waits for the server."""
    if not base_url or not model:
        return None
    key = (base_url, model)
    hit = _cache.get(key)
    if hit is None:
        return _read(key)
    if time.monotonic() - hit[0] >= (_TTL_SEC if hit[1] is not None else _UNKNOWN_TTL_SEC):
        with _lock:
            start = key not in _refreshing
            _refreshing.add(key)
        if start:
            threading.Thread(target=_refresh, args=(key,), daemon=True).start()
    return hit[1]


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


__all__ = ["sees", "served_status", "served_vision"]
