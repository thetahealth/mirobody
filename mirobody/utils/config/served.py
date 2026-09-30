"""Whether a model server serves a model that can see.

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
import time
import urllib.parse
import urllib.request
from typing import Any

_TTL_SEC = 300.0
_TIMEOUT_SEC = 3.0
_cache: dict[tuple[str, str], tuple[float, bool | None]] = {}


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


def _ask(root: str, model: str) -> bool | None:
    try:
        props = _fetch(f"{root}/props")
    except Exception:
        props = None
    if isinstance(props, dict) and props.get("role") == "router":
        listing = _fetch(f"{root}/models").get("data") or []
        row = next((m for m in listing if m.get("id") == model), None)
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


def served_vision(base_url: str, model: str) -> bool | None:
    """True or False when the server says whether `model` takes images; None
    when it does not say or cannot be reached. Cached for five minutes."""
    if not base_url or not model:
        return None
    key = (base_url, model)
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < _TTL_SEC:
        return hit[1]
    try:
        answer = _ask(base_url.rstrip("/").removesuffix("/v1"), model)
    except Exception:
        answer = None
    _cache[key] = (time.monotonic(), answer)
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


__all__ = ["sees", "served_vision"]
