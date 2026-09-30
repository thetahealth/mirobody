"""The first-run page's API: choose a vendor key, or run every model locally.

`GET /api/setup` says whether a model is still needed and what each choice
takes; it returns no saved value. `POST /api/setup` changes where health data
is sent, so it takes the setup token the app prints to its log at startup
(or `SETUP_TOKEN` from `.env`, which `deploy.sh` generates): whoever can read
the deployment's log is whoever deployed it. A key is saved only after one
real request through it succeeds; a local address only when the server there
serves the models the local entries name.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import secrets
import time
import urllib.request
from typing import Any, Literal

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel

from mirobody.server.envelope import err, ok

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/setup", tags=["setup"])

#: What the page calls each vendor. The key names and where to get one are
#: `KEYS_URL`; which model a key turns on is the MODELS table.
_LABELS = {
    "OPENROUTER_API_KEY": "OpenRouter",
    "DASHSCOPE_API_KEY": "Alibaba Cloud Model Studio (Qwen)",
    "GOOGLE_API_KEY": "Google Gemini",
    "OPENAI_API_KEY": "OpenAI",
    "ANTHROPIC_API_KEY": "Anthropic",
    "DEEPSEEK_API_KEY": "DeepSeek",
}

_token = os.environ.get("SETUP_TOKEN") or secrets.token_urlsafe(18)
_failures: dict[str, list[float]] = {}
_MAX_FAILURES, _FAILURE_WINDOW_SEC = 10, 600
_env_lock = asyncio.Lock()


def setup_token() -> str:
    return _token


def _refused(client: str) -> bool:
    now = time.monotonic()
    recent = [t for t in _failures.get(client, []) if now - t < _FAILURE_WINDOW_SEC]
    _failures[client] = recent
    return len(recent) >= _MAX_FAILURES


def _local_setup() -> dict[str, Any]:
    from mirobody.utils.config.config import global_config

    cfg = global_config()
    value = cfg.get("LOCAL_SETUP") if cfg else None
    return value if isinstance(value, dict) else {}


def _entry_for_key(name: str) -> tuple[str, dict] | None:
    from mirobody.utils.config.llm import chat_entries

    for entry_name, entry in chat_entries().items():
        if str(entry.get("api_key") or "") == name:
            return entry_name, entry
    return None


def _local_models() -> dict[str, str]:
    from mirobody.utils.config.llm import model_entries

    table = model_entries()
    return {role: str((table.get(entry) or {}).get("model") or "")
            for role, entry in (("agent", "local"), ("utils", "local-utils"), ("ocr", "local-ocr"))}


def _served(base_url: str) -> dict[str, str]:
    """Model id -> status as the server reports it: llama.cpp's router says
    unloaded, loading or loaded; any other server just lists what it serves."""
    root = base_url.rstrip("/").removesuffix("/v1")
    for url, router_listing in ((root + "/models", True), (base_url.rstrip("/") + "/models", False)):
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                import json

                rows = json.load(response).get("data") or []
        except Exception:
            continue
        return {str(m.get("id")): (str((m.get("status") or {}).get("value") or "ready") if router_listing else "ready")
                for m in rows}
    return {}


def _load(base_url: str, model: str) -> None:
    """Ask llama.cpp's router to fetch and load a model now rather than on the
    first question. Another server has no such call, which is fine."""
    import json

    root = base_url.rstrip("/").removesuffix("/v1")
    request = urllib.request.Request(root + "/models/load", json.dumps({"model": model}).encode(),
                                     {"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(request, timeout=10).close()
    except Exception as e:
        logger.info("model not preloaded: error_type=%s", type(e).__name__)


@router.get("")
async def setup_state() -> Any:
    from mirobody.utils.config import settings
    from mirobody.utils.config.llm import KEYS_URL, chat_default

    fixed = settings.set_in_environment()
    providers = []
    for name, url in KEYS_URL.items():
        found = _entry_for_key(name)
        providers.append({"key": name, "label": _LABELS.get(name, name), "get_key_url": url,
                          "model": str(found[1].get("model") or "") if found else "",
                          "set": bool(os.environ.get(name)), "in_env_file": name in fixed})
    local = _local_setup()
    base_url = os.environ.get("LOCAL_BASE_URL") or str(local.get("base_url") or "")
    models = _local_models()
    status = await asyncio.to_thread(_served, base_url) if os.environ.get("LOCAL_BASE_URL") else {}
    return ok({
        "needed": chat_default() is None,
        "chat_model": chat_default() or "",
        "providers": providers,
        "local": {
            "configured": bool(os.environ.get("LOCAL_BASE_URL")),
            "base_url": base_url,
            "models": models,
            "status": {role: status.get(model, "missing") for role, model in models.items() if model} if status else {},
            "download_gb": local.get("download_gb"),
            "memory_gb": local.get("memory_gb"),
            "preset": local.get("preset") or "",
            "in_env_file": "LOCAL_BASE_URL" in fixed,
        },
    })


class SetupChoice(BaseModel):
    mode: Literal["key", "local"]
    name: str = ""
    value: str = ""
    base_url: str = ""
    ocr_base_url: str = ""


@router.post("")
async def setup_save(choice: SetupChoice, request: Request, x_setup_token: str = Header(default="")) -> Any:
    from mirobody.agent import probe, registry
    from mirobody.utils.config import settings
    from mirobody.utils.config.llm import KEYS_URL, chat_default

    client = request.client.host if request.client else "?"
    if _refused(client):
        return err(429, "Too many wrong setup tokens. Wait ten minutes.")
    if not x_setup_token or not hmac.compare_digest(x_setup_token, _token):
        _failures.setdefault(client, []).append(time.monotonic())
        return err(403, "The setup token is wrong. It is printed in the app's log at startup.")
    fixed = settings.set_in_environment()

    if choice.mode == "key":
        name, value = choice.name.strip(), choice.value.strip()
        if name not in KEYS_URL or not value:
            return err(400, "Choose a supported service and paste its key.")
        if name in fixed:
            return err(409, f"{name} is set in .env; change it there.")
        found = _entry_for_key(name)
        if not found:
            return err(400, f"No chat model uses {name}.")
        async with _env_lock:
            before = os.environ.get(name)
            os.environ[name] = value
            try:
                passed, detail = await asyncio.wait_for(probe._chat(found[0]), timeout=90)
            except Exception as e:
                passed, detail = False, type(e).__name__
            finally:
                if before is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = before
        if not passed:
            return err(400, f"The key did not work: {detail}")
        await settings.save({name: value})
    else:
        if "LOCAL_BASE_URL" in fixed:
            return err(409, "LOCAL_BASE_URL is set in .env; change it there.")
        base_url = (choice.base_url or str(_local_setup().get("base_url") or "")).strip()
        ocr_base_url = (choice.ocr_base_url or base_url).strip()
        if not base_url.startswith(("http://", "https://")):
            return err(400, "Give the address of the model server, e.g. http://host.docker.internal:8080/v1.")
        models = _local_models()
        served = {**await asyncio.to_thread(_served, base_url), **await asyncio.to_thread(_served, ocr_base_url)}
        missing = [m for m in models.values() if m and m not in served]
        if missing:
            listed = ", ".join(sorted(served)) or "nothing (is it running?)"
            return err(400, f"The server at {base_url} does not serve {', '.join(sorted(set(missing)))}; it serves {listed}.")
        await settings.save({"LOCAL_BASE_URL": base_url, "LOCAL_OCR_BASE_URL": ocr_base_url})
        for model in {models["agent"], models["ocr"]} - {""}:
            await asyncio.to_thread(_load, ocr_base_url if model == models["ocr"] else base_url, model)

    registry.reload_llm_clients()
    return ok({"needed": chat_default() is None, "chat_model": chat_default() or ""})
